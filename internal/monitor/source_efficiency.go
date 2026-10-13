package monitor

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	sourceEfficiencyCacheTTL = time.Minute
	sourceEfficiencyMaxIDs   = store.MaxLearningSources
	sourceEfficiencyBatch    = store.MaxSourceQualityReadBatch
	sourceEfficiencyTimeout  = 5 * time.Second
)

type sourceEfficiencyIdentity struct {
	ID              string
	EffectiveScheme string
}

type sourceEfficiencyQualitySnapshot struct {
	identities []sourceEfficiencyIdentity
	quality    map[string]store.SourceQuality
	capturedAt time.Time
}

// sourceEfficiencyState retains only one bounded current-catalog snapshot.
// The mutex also coalesces concurrent report and stats-publish reads.
type sourceEfficiencyState struct {
	mu       sync.Mutex
	snapshot sourceEfficiencyQualitySnapshot
}

// sourceEfficiencyMetricHelp is consumed by the Prometheus exposition writer.
// Every source label is a fixed-width hash; source URLs and endpoint data are
// never used as labels.
var sourceEfficiencyMetricHelp = map[string]string{
	"freeproxyapi_source_unattributed_validated":                  "Validated records with missing or retired source provenance.",
	"freeproxyapi_source_unattributed_routing_eligible":           "Routing-filter matches with missing or retired source provenance.",
	"freeproxyapi_source_admitted_total":                          "Candidate IDs newly admitted to the tracked set by source.",
	"freeproxyapi_source_admission_refreshes_total":               "Completed whole-refresh admission observations for source.",
	"freeproxyapi_source_http_attempts_total":                     "HTTP attempts attributed to source fetches.",
	"freeproxyapi_source_response_bytes_total":                    "Response bytes attributed to source fetches.",
	"freeproxyapi_source_fetch_work_milliseconds_total":           "Fetch duration milliseconds accumulated for source.",
	"freeproxyapi_source_probe_completions_total":                 "Committed candidate probe completions attributed to source.",
	"freeproxyapi_source_probe_successful_completions_total":      "Committed successful candidate probe completions attributed to source.",
	"freeproxyapi_source_probe_work_milliseconds_total":           "Committed candidate probe duration milliseconds accumulated for source.",
	"freeproxyapi_source_validated":                               "Currently validated candidates attributed to source.",
	"freeproxyapi_source_routing_eligible":                        "Currently validated candidates meeting live strict router evidence attributed to source.",
	"freeproxyapi_source_zero_admission_streak":                   "Completed consecutive source refresh observations with zero new admissions.",
	"freeproxyapi_source_efficiency_quality_snapshot_age_seconds": "Age of the source quality snapshot used for efficiency metrics.",
	"freeproxyapi_source_efficiency_counts_snapshot_age_seconds":  "Age of the validated-source count snapshot used for efficiency metrics.",
	"freeproxyapi_source_efficiency_counts_observed":              "Whether source-attributed validated counts were observed in the current shared snapshot.",
}

var sourceEfficiencyMetricType = map[string]string{
	"freeproxyapi_source_unattributed_validated":                  "gauge",
	"freeproxyapi_source_unattributed_routing_eligible":           "gauge",
	"freeproxyapi_source_admitted_total":                          "counter",
	"freeproxyapi_source_admission_refreshes_total":               "counter",
	"freeproxyapi_source_http_attempts_total":                     "counter",
	"freeproxyapi_source_response_bytes_total":                    "counter",
	"freeproxyapi_source_fetch_work_milliseconds_total":           "counter",
	"freeproxyapi_source_probe_completions_total":                 "counter",
	"freeproxyapi_source_probe_successful_completions_total":      "counter",
	"freeproxyapi_source_probe_work_milliseconds_total":           "counter",
	"freeproxyapi_source_validated":                               "gauge",
	"freeproxyapi_source_routing_eligible":                        "gauge",
	"freeproxyapi_source_zero_admission_streak":                   "gauge",
	"freeproxyapi_source_efficiency_quality_snapshot_age_seconds": "gauge",
	"freeproxyapi_source_efficiency_counts_snapshot_age_seconds":  "gauge",
	"freeproxyapi_source_efficiency_counts_observed":              "gauge",
}

type sourceEfficiencyReport struct {
	UnattributedValidated  *int64                         `json:"unattributed_validated,omitempty"`
	UnattributedRouting    *int64                         `json:"unattributed_routing_eligible,omitempty"`
	Status                 string                         `json:"status"`
	CheckedAt              string                         `json:"checked_at"`
	QualityCapturedAt      string                         `json:"quality_captured_at,omitempty"`
	QualityAgeSeconds      *int64                         `json:"quality_age_seconds,omitempty"`
	QualityStale           bool                           `json:"quality_stale"`
	QualityError           string                         `json:"quality_error,omitempty"`
	SourceCountsObserved   bool                           `json:"source_counts_observed"`
	SourceCountsAt         string                         `json:"source_counts_at,omitempty"`
	SourceCountsAgeSeconds *int64                         `json:"source_counts_age_seconds,omitempty"`
	Sources                []sourceEfficiencySourceReport `json:"sources"`
}

type sourceEfficiencySourceReport struct {
	QualityCorrupt        bool   `json:"quality_corrupt"`
	SourceID              string `json:"source_id"`
	EffectiveScheme       string `json:"effective_scheme,omitempty"`
	AdmissionMeasured     bool   `json:"admission_measured"`
	Admitted              *int64 `json:"admitted,omitempty"`
	AdmissionRefreshes    *int64 `json:"admission_refreshes,omitempty"`
	LastAdmitted          *int64 `json:"last_admitted,omitempty"`
	ZeroAdmissionStreak   *int64 `json:"zero_admission_streak,omitempty"`
	LastAdmissionAt       string `json:"last_admission_at,omitempty"`
	FetchCostMeasured     bool   `json:"fetch_cost_measured"`
	HTTPAttempts          *int64 `json:"http_attempts,omitempty"`
	NetworkBytes          *int64 `json:"network_bytes,omitempty"`
	FetchDurationMS       *int64 `json:"fetch_duration_ms,omitempty"`
	FetchCostObservations *int64 `json:"fetch_cost_observations,omitempty"`
	ProbeCostMeasured     bool   `json:"probe_cost_measured"`
	ProbeCompletions      *int64 `json:"probe_completions,omitempty"`
	ProbeSuccessful       *int64 `json:"probe_successful_completions,omitempty"`
	ProbeDurationMS       *int64 `json:"probe_duration_ms,omitempty"`
	Validated             *int64 `json:"validated,omitempty"`
	RoutingEligible       *int64 `json:"routing_eligible,omitempty"`
}

func (r *Runner) currentSourceEfficiencyIdentities() ([]sourceEfficiencyIdentity, error) {
	byID := make(map[string]string, min(len(r.config.SourceSpecs), sourceEfficiencyMaxIDs))
	for _, spec := range r.config.SourceSpecs {
		id := r.sourceID(spec)
		if _, exists := byID[id]; !exists {
			if len(byID) >= sourceEfficiencyMaxIDs {
				return nil, fmt.Errorf("source efficiency catalog exceeds bound")
			}
			byID[id] = strings.ToLower(strings.TrimSpace(spec.defaultScheme(r.config.DefaultProxyScheme)))
		}
	}
	ids := make([]string, 0, len(byID))
	for id := range byID {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	identities := make([]sourceEfficiencyIdentity, len(ids))
	for i, id := range ids {
		identities[i] = sourceEfficiencyIdentity{ID: id, EffectiveScheme: byID[id]}
	}
	return identities, nil
}

func sameSourceEfficiencyIdentities(left, right []sourceEfficiencyIdentity) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func (r *Runner) loadSourceEfficiencySnapshot(ctx context.Context, now time.Time) (sourceEfficiencyQualitySnapshot, error) {
	identities, err := r.currentSourceEfficiencyIdentities()
	if err != nil {
		return sourceEfficiencyQualitySnapshot{}, err
	}
	state := &r.sourceEfficiency
	if err := lockSourceEfficiency(ctx, &state.mu); err != nil {
		return sourceEfficiencyQualitySnapshot{
			identities: append([]sourceEfficiencyIdentity(nil), identities...),
			quality:    map[string]store.SourceQuality{},
		}, err
	}
	defer state.mu.Unlock()
	if sameSourceEfficiencyIdentities(state.snapshot.identities, identities) && !state.snapshot.capturedAt.IsZero() {
		age := now.Sub(state.snapshot.capturedAt)
		if age >= 0 && age < sourceEfficiencyCacheTTL {
			return state.snapshot, nil
		}
	}
	ids := make([]string, len(identities))
	for i, identity := range identities {
		ids[i] = identity.ID
	}
	redisStore := r.readStore()
	if len(identities) == 0 {
		snapshot := sourceEfficiencyQualitySnapshot{
			identities: []sourceEfficiencyIdentity{},
			quality:    map[string]store.SourceQuality{},
			capturedAt: now,
		}
		state.snapshot = snapshot
		return snapshot, nil
	}
	if redisStore == nil {
		return snapshotOnFailure(state.snapshot, identities), fmt.Errorf("source efficiency store unavailable")
	}
	quality := make(map[string]store.SourceQuality, len(ids))
	for start := 0; start < len(ids); start += sourceEfficiencyBatch {
		end := min(start+sourceEfficiencyBatch, len(ids))
		batch, loadErr := redisStore.LoadSourceQuality(ctx, ids[start:end], now)
		if loadErr != nil {
			return snapshotOnFailure(state.snapshot, identities), loadErr
		}
		for id, value := range batch {
			quality[id] = value
		}
	}
	snapshot := sourceEfficiencyQualitySnapshot{
		identities: append([]sourceEfficiencyIdentity(nil), identities...),
		quality:    quality,
		capturedAt: now,
	}
	state.snapshot = snapshot
	return snapshot, nil
}

func lockSourceEfficiency(ctx context.Context, mu *sync.Mutex) error {
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !mu.TryLock() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func snapshotOnFailure(cached sourceEfficiencyQualitySnapshot, requested []sourceEfficiencyIdentity) sourceEfficiencyQualitySnapshot {
	if sameSourceEfficiencyIdentities(cached.identities, requested) {
		return cached
	}
	return sourceEfficiencyQualitySnapshot{
		identities: append([]sourceEfficiencyIdentity(nil), requested...),
		quality:    map[string]store.SourceQuality{},
	}
}

func (r *Runner) sourceEfficiencyReport(ctx context.Context) sourceEfficiencyReport {
	now := time.Now().UTC()
	if r.nowFunc != nil {
		now = r.nowFunc()
	}
	checkedAt := now.UTC().Format(time.RFC3339)
	report := sourceEfficiencyReport{Status: "ok", CheckedAt: checkedAt, Sources: []sourceEfficiencySourceReport{}}
	if !r.config.SourceLearningEnabled {
		report.Status = "disabled"
		return report
	}
	loadCtx, cancel := context.WithTimeout(ctx, sourceEfficiencyTimeout)
	defer cancel()
	snapshot, err := r.loadSourceEfficiencySnapshot(loadCtx, now)
	if err != nil {
		report.Status = "unavailable"
		report.QualityError = "source quality unavailable"
		if len(snapshot.identities) == 0 {
			if identities, idErr := r.currentSourceEfficiencyIdentities(); idErr == nil {
				snapshot.identities = identities
			}
		}
	}
	if !snapshot.capturedAt.IsZero() {
		report.QualityCapturedAt = snapshot.capturedAt.UTC().Format(time.RFC3339)
		age := now.Sub(snapshot.capturedAt)
		if age < 0 {
			report.QualityStale = true
			age = 0
		} else if age >= sourceEfficiencyCacheTTL {
			report.QualityStale = true
		}
		seconds := int64(age / time.Second)
		report.QualityAgeSeconds = &seconds
	} else {
		report.QualityStale = true
	}
	slices := r.validatedSlicesSnapshot()
	now = time.Now().UTC()
	if r.nowFunc != nil {
		now = r.nowFunc()
	}
	report.CheckedAt = now.UTC().Format(time.RFC3339)
	countsAt := slices.SourceCountsAtMS
	if slices.SourceCountsObserved && countsAt > 0 && countsAt <= now.UnixMilli() {
		report.SourceCountsObserved = true
		report.UnattributedValidated = int64Pointer(slices.UnattributedValidated)
		report.UnattributedRouting = int64Pointer(slices.UnattributedRouting)
		report.SourceCountsAt = time.UnixMilli(countsAt).UTC().Format(time.RFC3339)
		age := int64((now.UnixMilli() - countsAt) / 1000)
		report.SourceCountsAgeSeconds = &age
	}
	for _, identity := range snapshot.identities {
		report.Sources = append(report.Sources, sourceEfficiencyReportEntry(identity, snapshot.quality[identity.ID], r.sourceProbeContract(), slices, report.SourceCountsObserved))
	}
	return report
}

func sourceEfficiencyReportEntry(identity sourceEfficiencyIdentity, quality store.SourceQuality, currentContract string, slices validatedSlices, countsObserved bool) sourceEfficiencySourceReport {
	entry := sourceEfficiencySourceReport{QualityCorrupt: quality.Corrupt, SourceID: identity.ID, EffectiveScheme: identity.EffectiveScheme}
	if !quality.Corrupt && (quality.AdmissionRefreshes > 0 || quality.Admitted > 0) {
		entry.AdmissionMeasured = true
		entry.Admitted = int64Pointer(quality.Admitted)
		entry.AdmissionRefreshes = int64Pointer(quality.AdmissionRefreshes)
		entry.LastAdmitted = int64Pointer(quality.LastAdmitted)
		entry.ZeroAdmissionStreak = int64Pointer(quality.ZeroAdmissionStreak)
		if !quality.LastAdmissionAt.IsZero() {
			entry.LastAdmissionAt = quality.LastAdmissionAt.UTC().Format(time.RFC3339)
		}
	}
	if !quality.Corrupt && quality.FetchCostObservations > 0 {
		entry.FetchCostMeasured = true
		entry.HTTPAttempts = int64Pointer(quality.HTTPAttempts)
		entry.NetworkBytes = int64Pointer(quality.NetworkBytes)
		entry.FetchDurationMS = int64Pointer(quality.FetchDurationMS)
		entry.FetchCostObservations = int64Pointer(quality.FetchCostObservations)
	}
	if !quality.Corrupt && quality.ProbeCompletions > 0 && validSourceProbeContract(currentContract) && quality.ProbeContract == currentContract {
		entry.ProbeCostMeasured = true
		entry.ProbeCompletions = int64Pointer(quality.ProbeCompletions)
		entry.ProbeSuccessful = int64Pointer(quality.ProbeSuccessfulCompletions)
		entry.ProbeDurationMS = int64Pointer(quality.ProbeDurationMS)
	}
	if countsObserved {
		entry.Validated = int64Pointer(slices.BySource[identity.ID])
		entry.RoutingEligible = int64Pointer(slices.RoutingBySource[identity.ID])
	}
	return entry
}

func validSourceProbeContract(contract string) bool {
	if len(contract) != 64 {
		return false
	}
	for _, r := range contract {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func int64Pointer(value int64) *int64 { return &value }

// publishSourceEfficiency publishes only the leader's bounded source view.
// Per-source validated counts come from the existing stats-leader scan result;
// this method never scans candidate hashes itself.
func (r *Runner) publishSourceEfficiency(ctx context.Context, slices validatedSlices, now time.Time) {
	if r.metrics == nil {
		return
	}
	if !r.config.SourceLearningEnabled || !r.config.StatsLeaderEnabled {
		r.metrics.ReplaceSourceGauges(nil)
		return
	}
	if r.config.StatsLeaderEnabled && !r.isStatsLeader() {
		r.metrics.ReplaceSourceGauges(nil)
		return
	}
	snapshot, err := r.loadSourceEfficiencySnapshot(ctx, now)
	if err != nil && ctx.Err() == nil {
		log.Printf("source efficiency snapshot unavailable")
	}
	gauges := make(map[string]int64)
	if !snapshot.capturedAt.IsZero() {
		qualityAge := int64(max(0, int64(now.Sub(snapshot.capturedAt)/time.Second)))
		gauges["freeproxyapi_source_efficiency_quality_snapshot_age_seconds"] = qualityAge
	}
	countsObserved := slices.SourceCountsObserved && slices.SourceCountsAtMS > 0 && slices.SourceCountsAtMS <= now.UnixMilli() && now.UnixMilli()-slices.SourceCountsAtMS <= statsAggregateMaxAge.Milliseconds()
	if countsObserved {
		countsAge := (now.UnixMilli() - slices.SourceCountsAtMS) / 1000
		gauges["freeproxyapi_source_efficiency_counts_snapshot_age_seconds"] = countsAge
		gauges["freeproxyapi_source_efficiency_counts_observed"] = 1
		gauges["freeproxyapi_source_unattributed_validated"] = slices.UnattributedValidated
		gauges["freeproxyapi_source_unattributed_routing_eligible"] = slices.UnattributedRouting
	} else {
		gauges["freeproxyapi_source_efficiency_counts_observed"] = 0
	}
	contract := r.sourceProbeContract()
	for _, identity := range snapshot.identities {
		quality := snapshot.quality[identity.ID]
		label := fmt.Sprintf("{source_id=%q}", identity.ID)
		if !quality.Corrupt && (quality.AdmissionRefreshes > 0 || quality.Admitted > 0) {
			gauges["freeproxyapi_source_admitted_total"+label] = quality.Admitted
			gauges["freeproxyapi_source_admission_refreshes_total"+label] = quality.AdmissionRefreshes
			gauges["freeproxyapi_source_zero_admission_streak"+label] = quality.ZeroAdmissionStreak
		}
		if !quality.Corrupt && quality.FetchCostObservations > 0 {
			gauges["freeproxyapi_source_http_attempts_total"+label] = quality.HTTPAttempts
			gauges["freeproxyapi_source_response_bytes_total"+label] = quality.NetworkBytes
			gauges["freeproxyapi_source_fetch_work_milliseconds_total"+label] = quality.FetchDurationMS
		}
		if !quality.Corrupt && quality.ProbeCompletions > 0 && validSourceProbeContract(contract) && quality.ProbeContract == contract {
			gauges["freeproxyapi_source_probe_completions_total"+label] = quality.ProbeCompletions
			gauges["freeproxyapi_source_probe_successful_completions_total"+label] = quality.ProbeSuccessfulCompletions
			gauges["freeproxyapi_source_probe_work_milliseconds_total"+label] = quality.ProbeDurationMS
		}
		if countsObserved {
			gauges["freeproxyapi_source_validated"+label] = slices.BySource[identity.ID]
			gauges["freeproxyapi_source_routing_eligible"+label] = slices.RoutingBySource[identity.ID]
		}
	}
	r.metrics.ReplaceSourceGauges(gauges)
}
