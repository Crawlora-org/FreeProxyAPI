package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const sourceRefreshTraceWriteTimeout = 250 * time.Millisecond
const maxSourceRefreshTraceJSONBytes = 8 << 20

type sourceRefreshTrace struct {
	RefreshID       string                       `json:"refresh_id"`
	Generation      uint64                       `json:"generation,omitempty"`
	CapturedAt      *time.Time                   `json:"captured_at,omitempty"`
	StartedAt       time.Time                    `json:"started_at"`
	CompletedAt     *time.Time                   `json:"completed_at,omitempty"`
	Status          string                       `json:"status"`
	Result          string                       `json:"result,omitempty"`
	Configured      int                          `json:"configured"`
	Selected        int                          `json:"selected"`
	Cooled          int                          `json:"cooled"`
	Exploring       int                          `json:"exploring"`
	AdmissionCooled int                          `json:"admission_cooled"`
	Decisions       []sourceRefreshTraceDecision `json:"decisions"`
}

type sourceRefreshTraceDecision struct {
	SourceID                      string     `json:"source_id"`
	Selected                      bool       `json:"selected"`
	Cooled                        bool       `json:"cooled"`
	Explored                      bool       `json:"explored"`
	AdmissionCooled               bool       `json:"admission_cooled"`
	TrustedPriorityProtected      bool       `json:"trusted_priority_protected"`
	CurrentPositiveProbeProtected bool       `json:"current_positive_probe_protected"`
	ProbeContractCurrent          bool       `json:"probe_contract_current"`
	PlanPosition                  *int       `json:"plan_position,omitempty"`
	Reasons                       []string   `json:"reasons"`
	FetchStatus                   string     `json:"fetch_status"`
	MergeStatus                   string     `json:"merge_status"`
	AdmissionObservationStatus    string     `json:"admission_observation_status"`
	AdmissionObservationReasons   []string   `json:"admission_observation_reasons,omitempty"`
	FetchAttempts                 int        `json:"fetch_attempts,omitempty"`
	HTTPAttempts                  int64      `json:"http_attempts,omitempty"`
	NetworkBytes                  int64      `json:"network_bytes,omitempty"`
	FetchDurationMS               int64      `json:"fetch_duration_ms,omitempty"`
	Parsed                        int64      `json:"parsed,omitempty"`
	AdmittedCandidates            int64      `json:"admitted_candidates,omitempty"`
	Truncated                     bool       `json:"truncated,omitempty"`
	FetchStartedAt                *time.Time `json:"fetch_started_at,omitempty"`
	FetchCompletedAt              *time.Time `json:"fetch_completed_at,omitempty"`
}

type sourceRefreshTraceSummary struct {
	RefreshID       string     `json:"refresh_id"`
	Generation      uint64     `json:"generation,omitempty"`
	CapturedAt      *time.Time `json:"captured_at,omitempty"`
	StartedAt       time.Time  `json:"started_at"`
	CompletedAt     *time.Time `json:"completed_at,omitempty"`
	Status          string     `json:"status"`
	Result          string     `json:"result,omitempty"`
	Configured      int        `json:"configured"`
	Selected        int        `json:"selected"`
	Cooled          int        `json:"cooled"`
	Exploring       int        `json:"exploring"`
	AdmissionCooled int        `json:"admission_cooled"`
}

type sourceRefreshTraceList struct {
	RetentionSeconds int                         `json:"retention_seconds"`
	MaxTraces        int                         `json:"max_traces"`
	Traces           []sourceRefreshTraceSummary `json:"traces"`
}

func newSourceRefreshTraceID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func (r *Runner) beginSourceRefreshTrace(ctx context.Context) string {
	id, err := newSourceRefreshTraceID()
	if err != nil {
		r.sourcePlan.mu.Lock()
		r.sourcePlan.trace = nil
		r.sourcePlan.traceDecisionIndex = nil
		r.sourcePlan.mu.Unlock()
		r.recordSourceRefreshTraceWriteFailure()
		return ""
	}
	now := r.nowFunc().UTC()
	trace := &sourceRefreshTrace{RefreshID: id, StartedAt: now, Status: "started", Decisions: []sourceRefreshTraceDecision{}}
	r.sourcePlan.mu.Lock()
	r.sourcePlan.trace = trace
	r.sourcePlan.traceDecisionIndex = make(map[string]int)
	r.sourcePlan.mu.Unlock()
	r.persistSourceRefreshTrace(ctx)
	return id
}

func (r *Runner) setSourceRefreshTracePlan(ctx context.Context, generation uint64, capturedAt time.Time, configured int, decisions []SourceLearningDecision) {
	r.sourcePlan.mu.Lock()
	if r.sourcePlan.trace == nil {
		r.sourcePlan.mu.Unlock()
		return
	}
	trace := r.sourcePlan.trace
	trace.Generation = generation
	captured := capturedAt.UTC()
	trace.CapturedAt = &captured
	trace.Configured = configured
	trace.Decisions = make([]sourceRefreshTraceDecision, 0, len(decisions))
	r.sourcePlan.traceDecisionIndex = make(map[string]int, len(decisions))
	for _, decision := range decisions {
		status, mergeStatus := "not_selected", "not_selected"
		admissionStatus := "not_selected"
		if decision.Selected {
			status, mergeStatus = "not_started", "not_started"
			admissionStatus = "pending"
			if generation == 0 {
				admissionStatus = "not_applicable"
			}
			trace.Selected++
		}
		if decision.Cooled {
			trace.Cooled++
		}
		if decision.Explored {
			trace.Exploring++
		}
		if decision.AdmissionCooled {
			trace.AdmissionCooled++
		}
		r.sourcePlan.traceDecisionIndex[decision.ID] = len(trace.Decisions)
		trace.Decisions = append(trace.Decisions, sourceRefreshTraceDecision{
			SourceID: decision.ID, Selected: decision.Selected, Cooled: decision.Cooled,
			Explored: decision.Explored, AdmissionCooled: decision.AdmissionCooled,
			TrustedPriorityProtected:      decision.TrustedPriorityProtected,
			CurrentPositiveProbeProtected: decision.CurrentPositiveProbeProtected,
			ProbeContractCurrent:          decision.ProbeContractCurrent,
			PlanPosition:                  decision.PlanPosition, Reasons: append([]string(nil), decision.Reasons...),
			FetchStatus: status, MergeStatus: mergeStatus, AdmissionObservationStatus: admissionStatus,
		})
	}
	r.sourcePlan.mu.Unlock()
	r.persistSourceRefreshTrace(ctx)
}

func (r *Runner) setSourceRefreshTraceGeneration(ctx context.Context, generation uint64, capturedAt time.Time, configured int) {
	r.sourcePlan.mu.Lock()
	if r.sourcePlan.trace == nil {
		r.sourcePlan.mu.Unlock()
		return
	}
	r.sourcePlan.trace.Generation = generation
	captured := capturedAt.UTC()
	r.sourcePlan.trace.CapturedAt = &captured
	r.sourcePlan.trace.Configured = configured
	r.sourcePlan.mu.Unlock()
	r.persistSourceRefreshTrace(ctx)
}

func (r *Runner) setLegacySourceRefreshTracePlan(ctx context.Context, specs []SourceSpec) {
	decisions := make([]SourceLearningDecision, 0, len(specs))
	seen := make(map[string]struct{}, len(specs))
	for _, spec := range specs {
		id := r.sourceID(spec)
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}
		position := len(decisions)
		decisions = append(decisions, SourceLearningDecision{ID: id, Selected: true, PlanPosition: &position, Reasons: []string{"learning_disabled"}})
	}
	r.setSourceRefreshTracePlan(ctx, 0, r.nowFunc(), len(specs), decisions)
}

func (r *Runner) recordSourceRefreshFetchStarted(spec SourceSpec) {
	id := r.sourceID(spec)
	r.sourcePlan.mu.Lock()
	if index, exists := r.sourcePlan.traceDecisionIndex[id]; exists && r.sourcePlan.trace != nil && index < len(r.sourcePlan.trace.Decisions) {
		decision := &r.sourcePlan.trace.Decisions[index]
		if decision.Selected && decision.FetchStatus == "not_started" {
			now := r.nowFunc().UTC()
			decision.FetchStatus = "started"
			decision.FetchStartedAt = &now
		}
	}
	r.sourcePlan.mu.Unlock()
}

func (r *Runner) recordSourceRefreshFetchResult(ctx context.Context, result sourceRefreshResult) {
	id := r.sourceID(result.spec)
	r.sourcePlan.mu.Lock()
	if index, exists := r.sourcePlan.traceDecisionIndex[id]; exists && r.sourcePlan.trace != nil && index < len(r.sourcePlan.trace.Decisions) {
		decision := &r.sourcePlan.trace.Decisions[index]
		if decision.Selected {
			now := r.nowFunc().UTC()
			decision.FetchCompletedAt = &now
			decision.FetchAttempts = result.fetchAttempts
			decision.HTTPAttempts = result.httpAttempts
			decision.NetworkBytes = result.networkBytes
			decision.FetchDurationMS = result.fetchDurationMS
			decision.Parsed = int64(result.parsed)
			decision.Truncated = result.truncated
			switch {
			case result.stage == sourceStageFetch && result.err != nil && (ctx.Err() != nil || errors.Is(result.err, context.Canceled) || errors.Is(result.err, context.DeadlineExceeded)):
				decision.FetchStatus = "cancelled"
			case result.stage == sourceStageFetch && result.err != nil:
				decision.FetchStatus = "failed"
			case result.truncated:
				decision.FetchStatus = "truncated"
			default:
				decision.FetchStatus = "fetched"
			}
			switch {
			case result.stage == sourceStageMerge && result.err != nil:
				decision.MergeStatus = "failed"
			case decision.FetchStatus == "cancelled" || decision.FetchStatus == "failed":
				decision.MergeStatus = "not_reached"
			default:
				decision.MergeStatus = "completed"
			}
			if decision.AdmissionObservationStatus != "not_applicable" {
				if decision.FetchStatus == "cancelled" {
					setAdmissionObservationOutcome(decision, "cancelled", "cancelled")
				} else {
					censored := false
					if decision.FetchStatus == "failed" {
						setAdmissionObservationOutcome(decision, "censored", "fetch_failed")
						censored = true
					}
					if decision.Truncated {
						setAdmissionObservationOutcome(decision, "censored", "truncated")
						censored = true
					}
					if decision.MergeStatus == "failed" {
						setAdmissionObservationOutcome(decision, "censored", "merge_failed")
						censored = true
					}
					if !censored && decision.MergeStatus == "completed" {
						decision.AdmissionObservationStatus = "pending"
					}
				}
			}
		}
	}
	r.sourcePlan.mu.Unlock()
}

func (r *Runner) recordSourceRefreshAdmissionDelta(sourceID string, added int64) {
	if added <= 0 {
		return
	}
	r.sourcePlan.mu.Lock()
	if index, exists := r.sourcePlan.traceDecisionIndex[sourceID]; exists && r.sourcePlan.trace != nil && index < len(r.sourcePlan.trace.Decisions) {
		r.sourcePlan.trace.Decisions[index].AdmittedCandidates += added
	}
	r.sourcePlan.mu.Unlock()
}

func (r *Runner) recordSourceRefreshAdmissionCensored(sourceID, reason string) {
	r.sourcePlan.mu.Lock()
	if index, exists := r.sourcePlan.traceDecisionIndex[sourceID]; exists && r.sourcePlan.trace != nil && index < len(r.sourcePlan.trace.Decisions) {
		decision := &r.sourcePlan.trace.Decisions[index]
		if decision.Selected && decision.AdmissionObservationStatus != "not_applicable" {
			setAdmissionObservationOutcome(decision, "censored", reason)
		}
	}
	r.sourcePlan.mu.Unlock()
}

func (r *Runner) recordSourceRefreshAdmissionRecorded(sourceIDs []string, failed bool) {
	r.sourcePlan.mu.Lock()
	defer r.sourcePlan.mu.Unlock()
	for _, sourceID := range sourceIDs {
		if index, exists := r.sourcePlan.traceDecisionIndex[sourceID]; exists && r.sourcePlan.trace != nil && index < len(r.sourcePlan.trace.Decisions) {
			decision := &r.sourcePlan.trace.Decisions[index]
			if failed {
				setAdmissionObservationOutcome(decision, "write_failed", "observation_write_failed")
			} else if decision.AdmissionObservationStatus == "pending" {
				decision.AdmissionObservationStatus = "recorded"
			}
		}
	}
}

func setAdmissionObservationOutcome(decision *sourceRefreshTraceDecision, status, reason string) {
	decision.AdmissionObservationStatus = status
	for _, existing := range decision.AdmissionObservationReasons {
		if existing == reason {
			return
		}
	}
	decision.AdmissionObservationReasons = append(decision.AdmissionObservationReasons, reason)
}

func (r *Runner) finishSourceRefreshTrace(ctx context.Context, originalCtx context.Context, err error) {
	r.sourcePlan.mu.Lock()
	if r.sourcePlan.trace == nil {
		r.sourcePlan.mu.Unlock()
		return
	}
	if err != nil {
		if originalCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			r.sourcePlan.trace.Status = "cancelled"
			r.sourcePlan.trace.Result = "cancelled"
			for i := range r.sourcePlan.trace.Decisions {
				decision := &r.sourcePlan.trace.Decisions[i]
				if decision.Selected {
					if decision.FetchStatus == "started" || decision.FetchStatus == "not_started" {
						decision.FetchStatus = "cancelled"
						decision.MergeStatus = "not_reached"
					}
					if decision.AdmissionObservationStatus == "pending" {
						setAdmissionObservationOutcome(decision, "cancelled", "cancelled")
					}
				}
			}
		} else {
			r.sourcePlan.trace.Status = "failed"
			r.sourcePlan.trace.Result = "error"
			for i := range r.sourcePlan.trace.Decisions {
				decision := &r.sourcePlan.trace.Decisions[i]
				if decision.Selected {
					if decision.FetchStatus == "started" || decision.FetchStatus == "not_started" {
						decision.FetchStatus = "failed"
						decision.MergeStatus = "not_reached"
						setAdmissionObservationOutcome(decision, "censored", "refresh_failed_before_fetch")
					} else if decision.AdmissionObservationStatus == "pending" {
						setAdmissionObservationOutcome(decision, "censored", "refresh_failed")
					}
				}
			}
		}
	} else {
		r.sourcePlan.trace.Status = "completed"
		if r.sourcePlan.trace.Selected == 0 {
			r.sourcePlan.trace.Result = "skipped"
		} else {
			r.sourcePlan.trace.Result = "ok"
			for _, decision := range r.sourcePlan.trace.Decisions {
				if decision.Selected && (decision.FetchStatus != "fetched" || (decision.AdmissionObservationStatus != "recorded" && decision.AdmissionObservationStatus != "not_applicable")) {
					r.sourcePlan.trace.Result = "partial"
					break
				}
			}
		}
	}
	completed := r.nowFunc().UTC()
	r.sourcePlan.trace.CompletedAt = &completed
	r.sourcePlan.mu.Unlock()
	r.persistSourceRefreshTrace(ctx)
}

func (r *Runner) persistSourceRefreshTrace(ctx context.Context) {
	trace := r.sourceRefreshTraceSnapshot()
	if trace.RefreshID == "" || r.store == nil {
		return
	}
	payload, err := encodeSourceRefreshTrace(trace)
	if err != nil {
		r.recordSourceRefreshTraceWriteFailure()
		return
	}
	summary, err := json.Marshal(summarizeSourceRefreshTrace(trace))
	if err != nil {
		r.recordSourceRefreshTraceWriteFailure()
		return
	}
	writeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sourceRefreshTraceWriteTimeout)
	defer cancel()
	if err := r.store.SaveSourceRefreshTrace(writeCtx, trace.RefreshID, payload, summary, trace.StartedAt); err != nil {
		r.recordSourceRefreshTraceWriteFailure()
	}
}

func (r *Runner) sourceRefreshTraceSnapshot() sourceRefreshTrace {
	r.sourcePlan.mu.RLock()
	defer r.sourcePlan.mu.RUnlock()
	if r.sourcePlan.trace == nil {
		return sourceRefreshTrace{}
	}
	trace := *r.sourcePlan.trace
	trace.Decisions = make([]sourceRefreshTraceDecision, len(r.sourcePlan.trace.Decisions))
	copy(trace.Decisions, r.sourcePlan.trace.Decisions)
	for i := range trace.Decisions {
		trace.Decisions[i].Reasons = append([]string(nil), trace.Decisions[i].Reasons...)
		trace.Decisions[i].AdmissionObservationReasons = append([]string(nil), trace.Decisions[i].AdmissionObservationReasons...)
	}
	return trace
}

func (r *Runner) recordSourceRefreshTraceWriteFailure() {
	if r.metrics != nil {
		r.metrics.AddSourceRefreshTraceWriteFailure()
	}
}

func summarizeSourceRefreshTrace(trace sourceRefreshTrace) sourceRefreshTraceSummary {
	return sourceRefreshTraceSummary{
		RefreshID: trace.RefreshID, Generation: trace.Generation, CapturedAt: trace.CapturedAt,
		StartedAt: trace.StartedAt, CompletedAt: trace.CompletedAt, Status: trace.Status,
		Result: trace.Result, Configured: trace.Configured, Selected: trace.Selected,
		Cooled: trace.Cooled, Exploring: trace.Exploring, AdmissionCooled: trace.AdmissionCooled,
	}
}

func decodeSourceRefreshTraceSummary(payload []byte, expectedID string) (sourceRefreshTraceSummary, error) {
	var summary sourceRefreshTraceSummary
	if err := json.Unmarshal(payload, &summary); err != nil {
		return sourceRefreshTraceSummary{}, errors.New("invalid source refresh trace summary")
	}
	if !validSourceRefreshTraceID(expectedID) || summary.RefreshID != expectedID || !validRefreshTraceStatus(summary.Status) || !validRefreshTraceResult(summary.Result) || summary.Configured < 0 || summary.Configured > store.MaxLearningSources || summary.Selected < 0 || summary.Selected > summary.Configured || summary.Cooled < 0 || summary.Cooled > summary.Configured || summary.Exploring < 0 || summary.Exploring > summary.Selected || summary.AdmissionCooled < 0 || summary.AdmissionCooled > summary.Configured {
		return sourceRefreshTraceSummary{}, errors.New("invalid source refresh trace summary")
	}
	if summary.Status == "started" {
		if summary.CompletedAt != nil || summary.Result != "" {
			return sourceRefreshTraceSummary{}, errors.New("invalid source refresh trace summary lifecycle")
		}
	} else if summary.CompletedAt == nil || summary.Result == "" {
		return sourceRefreshTraceSummary{}, errors.New("invalid source refresh trace summary lifecycle")
	}
	return summary, nil
}

func decodeSourceRefreshTrace(payload []byte) (sourceRefreshTrace, error) {
	if len(payload) == 0 {
		return sourceRefreshTrace{}, errors.New("invalid source refresh trace")
	}
	if len(payload) >= 2 && payload[0] == 0x1f && payload[1] == 0x8b {
		reader, err := gzip.NewReader(bytes.NewReader(payload))
		if err != nil {
			return sourceRefreshTrace{}, errors.New("invalid source refresh trace")
		}
		decompressed, readErr := io.ReadAll(io.LimitReader(reader, maxSourceRefreshTraceJSONBytes+1))
		closeErr := reader.Close()
		if readErr != nil || closeErr != nil || len(decompressed) > maxSourceRefreshTraceJSONBytes {
			return sourceRefreshTrace{}, errors.New("invalid source refresh trace")
		}
		payload = decompressed
	} else if len(payload) > maxSourceRefreshTraceJSONBytes {
		return sourceRefreshTrace{}, errors.New("invalid source refresh trace")
	}
	var trace sourceRefreshTrace
	if err := json.Unmarshal(payload, &trace); err != nil {
		return sourceRefreshTrace{}, fmt.Errorf("decode source refresh trace")
	}
	if !validSourceRefreshTraceID(trace.RefreshID) || !validRefreshTraceStatus(trace.Status) || !validRefreshTraceResult(trace.Result) || trace.Decisions == nil || len(trace.Decisions) > store.MaxLearningSources || trace.Configured < len(trace.Decisions) || trace.Configured > store.MaxLearningSources || trace.Selected < 0 || trace.Selected > trace.Configured {
		return sourceRefreshTrace{}, errors.New("invalid source refresh trace")
	}
	if trace.Status == "started" {
		if trace.CompletedAt != nil || trace.Result != "" {
			return sourceRefreshTrace{}, errors.New("invalid source refresh trace lifecycle")
		}
	} else if trace.CompletedAt == nil || trace.Result == "" {
		return sourceRefreshTrace{}, errors.New("invalid source refresh trace lifecycle")
	}
	selected, cooled, exploring, admissionCooled := 0, 0, 0, 0
	positions := make(map[int]struct{}, trace.Selected)
	for _, decision := range trace.Decisions {
		if !validSourceRefreshTraceID(decision.SourceID) || !validRefreshFetchStatus(decision.FetchStatus) || !validRefreshMergeStatus(decision.MergeStatus) || !validAdmissionObservationStatus(decision.AdmissionObservationStatus) || len(decision.Reasons) > 8 || len(decision.AdmissionObservationReasons) > 8 {
			return sourceRefreshTrace{}, errors.New("invalid source refresh trace decision")
		}
		if decision.Selected {
			selected++
			if decision.FetchStatus == "not_selected" || decision.MergeStatus == "not_selected" || decision.AdmissionObservationStatus == "not_selected" || decision.PlanPosition == nil || *decision.PlanPosition < 0 || *decision.PlanPosition >= trace.Selected {
				return sourceRefreshTrace{}, errors.New("invalid selected source trace decision")
			}
			if _, exists := positions[*decision.PlanPosition]; exists {
				return sourceRefreshTrace{}, errors.New("duplicate source refresh plan position")
			}
			positions[*decision.PlanPosition] = struct{}{}
		} else if decision.FetchStatus != "not_selected" || decision.MergeStatus != "not_selected" || decision.AdmissionObservationStatus != "not_selected" || decision.PlanPosition != nil || decision.Explored {
			return sourceRefreshTrace{}, errors.New("invalid omitted source trace decision")
		}
		if decision.Explored && (!decision.Selected || !decision.Cooled) {
			return sourceRefreshTrace{}, errors.New("invalid exploration trace decision")
		}
		if decision.Cooled {
			cooled++
		}
		if decision.Explored {
			if !decision.Cooled {
				return sourceRefreshTrace{}, errors.New("invalid exploration trace decision")
			}
			exploring++
		}
		if decision.AdmissionCooled {
			admissionCooled++
		}
		for _, reason := range decision.Reasons {
			switch reason {
			case "failure_cooldown", "low_yield_cooldown", "zero_admission_cooldown", "exploration", "learning_disabled", "quality_corrupt_fail_open", "active":
			default:
				return sourceRefreshTrace{}, errors.New("invalid source refresh trace reason")
			}
		}
		if decision.AdmissionCooled && !containsTraceReason(decision.Reasons, "zero_admission_cooldown") {
			return sourceRefreshTrace{}, errors.New("missing zero-admission trace reason")
		}
		if decision.Cooled && !containsTraceReason(decision.Reasons, "failure_cooldown") && !containsTraceReason(decision.Reasons, "low_yield_cooldown") && !containsTraceReason(decision.Reasons, "zero_admission_cooldown") {
			return sourceRefreshTrace{}, errors.New("missing source cooldown trace reason")
		}
		for _, reason := range decision.AdmissionObservationReasons {
			switch reason {
			case "truncated", "candidate_cap", "fetch_failed", "merge_failed", "cancelled", "observation_write_failed", "refresh_failed_before_fetch", "refresh_failed":
			default:
				return sourceRefreshTrace{}, errors.New("invalid source refresh trace admission reason")
			}
		}
	}
	if selected != trace.Selected || cooled != trace.Cooled || exploring != trace.Exploring || admissionCooled != trace.AdmissionCooled {
		return sourceRefreshTrace{}, errors.New("invalid source refresh trace counts")
	}
	return trace, nil
}

func containsTraceReason(reasons []string, want string) bool {
	for _, reason := range reasons {
		if reason == want {
			return true
		}
	}
	return false
}

func encodeSourceRefreshTrace(trace sourceRefreshTrace) ([]byte, error) {
	encoded, err := json.Marshal(trace)
	if err != nil {
		return nil, fmt.Errorf("encode source refresh trace")
	}
	if len(encoded) > maxSourceRefreshTraceJSONBytes {
		return nil, errors.New("source refresh trace exceeds size limit")
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(encoded); err != nil {
		return nil, fmt.Errorf("compress source refresh trace")
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("compress source refresh trace")
	}
	return compressed.Bytes(), nil
}

func validRefreshTraceStatus(status string) bool {
	switch status {
	case "started", "completed", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func validRefreshTraceResult(result string) bool {
	switch result {
	case "", "ok", "partial", "skipped", "error", "cancelled":
		return true
	default:
		return false
	}
}

func validRefreshFetchStatus(status string) bool {
	switch status {
	case "not_selected", "not_started", "started", "fetched", "truncated", "failed", "cancelled":
		return true
	default:
		return false
	}
}

func validRefreshMergeStatus(status string) bool {
	switch status {
	case "not_selected", "not_started", "not_reached", "completed", "failed":
		return true
	default:
		return false
	}
}

func validAdmissionObservationStatus(status string) bool {
	switch status {
	case "not_selected", "not_applicable", "pending", "recorded", "censored", "write_failed", "cancelled":
		return true
	default:
		return false
	}
}
