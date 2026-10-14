package monitor

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

// SourceLearningConfig controls opt-in ranking from sampled endpoint outcomes.
// Zero values select conservative defaults once Enabled is true.
type SourceLearningConfig struct {
	Enabled             bool
	MinSamples          float64
	HalfLife            time.Duration
	LowYieldThreshold   float64
	LowYieldCooldown    time.Duration
	ExplorationInterval uint64
	// ZeroAdmissionCooldown suppresses stale, low-confidence feeds that have
	// repeatedly completed a refresh without admitting any new candidates.
	// Zero disables this policy.
	ZeroAdmissionCooldown time.Duration
	// ZeroAdmissionRefreshes is the minimum consecutive full refresh outcomes
	// required before ZeroAdmissionCooldown can apply.
	ZeroAdmissionRefreshes int64
}

type SourceLearningCandidate struct {
	ID               string
	FallbackPriority int64
	Quality          store.SourceQuality
	// ProbeContractCurrent is set by the refresh snapshot loader after it
	// invalidates probe observations recorded under an older contract.
	ProbeContractCurrent bool
}

// RankedSource is one entry selected for the next source refresh. Priority is
// lower-is-earlier and can be combined with the configured source index using
// store.EncodeSourceStageScore. Cold candidates retain the ordering established
// by sourcePriority; learned samples affect ordering only after MinSamples.
type RankedSource struct {
	ID              string
	Priority        int64
	Score           float64
	Learned         bool
	Exploration     bool
	AdmissionCooled bool
}

// SourceLearningDecision records the planner's actual decision for one source.
// Reasons retain overlapping cooldowns; an explored source can therefore be
// selected while still reporting each cooldown that exploration bypassed.
type SourceLearningDecision struct {
	ID                            string
	Selected                      bool
	Cooled                        bool
	Explored                      bool
	AdmissionCooled               bool
	TrustedPriorityProtected      bool
	CurrentPositiveProbeProtected bool
	ProbeContractCurrent          bool
	PlanPosition                  *int
	Reasons                       []string
}

// DetailedSourceLearningPlan pairs the sources the refresh will actually
// fetch with a decision record for every unique configured source identity.
type DetailedSourceLearningPlan struct {
	Selected  []RankedSource
	Decisions []SourceLearningDecision
}

type scoredSource struct {
	candidate       SourceLearningCandidate
	score           float64
	learned         bool
	cooled          bool
	admissionCooled bool
	cooldownReasons []string
}

// StableSourceID shares store's URL-plus-scheme identity so the refresh planner
// and per-source Redis hash use the same identifier.
func StableSourceID(rawURL, defaultScheme string) string {
	return store.StableSourceID(rawURL, defaultScheme)
}

// SourceYield applies the decayed Beta(1,1)-smoothed success score and returns
// its effective sample count. A source remains cold until MinSamples, allowing
// the configured sourcePriority ordering to remain the fallback.
func SourceYield(q store.SourceQuality, now time.Time, config SourceLearningConfig) (score, effectiveSamples float64, learned bool) {
	if q.Corrupt {
		return 0.5, 0, false
	}
	minimum, halfLife := config.normalized()
	score, effectiveSamples = store.DecayedSourceYield(q, now, halfLife)
	return score, effectiveSamples, effectiveSamples >= minimum
}

// BuildSourceLearningPlan ranks configured sources without mutating input.
// Sources in failure or low-yield cooldown are omitted except on their stable
// one-in-N exploration refresh, so a bad early estimate cannot suppress a feed
// forever. With learning disabled, fallback priorities are returned unchanged.
func BuildSourceLearningPlan(candidates []SourceLearningCandidate, refreshNumber uint64, now time.Time, config SourceLearningConfig) []RankedSource {
	return BuildSourceLearningPlanDetailed(candidates, refreshNumber, now, config).Selected
}

// BuildSourceLearningPlanDetailed is the source planner used by refreshes and
// trace reporting. It returns the same ranked plan as BuildSourceLearningPlan
// plus one exact, reason-preserving decision for every unique candidate.
func BuildSourceLearningPlanDetailed(candidates []SourceLearningCandidate, refreshNumber uint64, now time.Time, config SourceLearningConfig) DetailedSourceLearningPlan {
	if len(candidates) == 0 {
		return DetailedSourceLearningPlan{}
	}
	minimum, halfLife := config.normalized()
	seen := make(map[string]struct{}, len(candidates))
	scored := make([]scoredSource, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.ID == "" {
			continue
		}
		if _, exists := seen[candidate.ID]; exists {
			continue
		}
		seen[candidate.ID] = struct{}{}
		score, samples := store.DecayedSourceYield(candidate.Quality, now, halfLife)
		if candidate.Quality.Corrupt {
			score, samples = 0.5, 0
		}
		eligible := config.Enabled && !candidate.Quality.Corrupt && samples >= minimum
		cooldownReasons := sourceCooldownReasons(candidate, now, config, eligible, score)
		admissionCooled := shouldCoolZeroAdmission(candidate, now, config)
		scored = append(scored, scoredSource{
			candidate: candidate, score: score, learned: eligible, cooled: len(cooldownReasons) > 0,
			admissionCooled: admissionCooled, cooldownReasons: cooldownReasons,
		})
	}
	if !config.Enabled {
		sort.SliceStable(scored, func(i, j int) bool {
			if scored[i].candidate.FallbackPriority != scored[j].candidate.FallbackPriority {
				return scored[i].candidate.FallbackPriority < scored[j].candidate.FallbackPriority
			}
			return false // stable sort preserves configured source order on ties
		})
		plan := make([]RankedSource, 0, len(scored))
		decisions := make([]SourceLearningDecision, 0, len(scored))
		for _, item := range scored {
			plan = append(plan, RankedSource{ID: item.candidate.ID, Priority: item.candidate.FallbackPriority})
			position := len(plan) - 1
			decisions = append(decisions, sourceLearningDecision(item, true, false, &position, []string{"learning_disabled"}))
		}
		return DetailedSourceLearningPlan{Selected: plan, Decisions: decisions}
	}

	interval := config.ExplorationInterval
	if interval == 0 {
		interval = 10
	}
	active := make([]scoredSource, 0, len(scored))
	exploring := make([]scoredSource, 0)
	for _, item := range scored {
		if item.cooled {
			if exploreThisRefresh(item.candidate.ID, refreshNumber, interval) {
				exploring = append(exploring, item)
			}
			continue
		}
		active = append(active, item)
	}
	less := func(items []scoredSource) func(int, int) bool {
		return func(i, j int) bool {
			a, b := items[i], items[j]
			if a.learned != b.learned {
				return a.learned
			}
			if a.learned && a.score != b.score {
				return a.score > b.score
			}
			if a.candidate.FallbackPriority != b.candidate.FallbackPriority {
				return a.candidate.FallbackPriority < b.candidate.FallbackPriority
			}
			return false // stable sort preserves configured source order on ties
		}
	}
	sort.SliceStable(active, less(active))
	sort.SliceStable(exploring, less(exploring))
	if len(exploring) > 0 {
		// Rotate the currently eligible exploration bucket once per full
		// interval. This lets same-bucket sources take turns when the bounded
		// exploration lane is smaller than the cooled catalog.
		start := int((refreshNumber / interval) % uint64(len(exploring)))
		exploring = append(exploring[start:], exploring[:start]...)
	}
	// Keep a bounded exploration lane near the front. A single exploratory
	// source is always first; larger plans add at most one exploratory source
	// per nine active sources, preventing a long recovery queue from consuming
	// the whole refresh while still giving cooled sources a chance under caps.
	maxExploration := max(1, (len(active)+8)/9)
	if len(exploring) > maxExploration {
		exploring = exploring[:maxExploration]
	}
	plan := make([]RankedSource, 0, len(active)+len(exploring))
	selectedByID := make(map[string]RankedSource, len(active)+len(exploring))
	appendSource := func(item scoredSource, isExploration bool) {
		ranked := RankedSource{
			ID: item.candidate.ID, Priority: int64(len(plan)), Score: item.score,
			Learned: item.learned, Exploration: isExploration, AdmissionCooled: item.admissionCooled,
		}
		plan = append(plan, ranked)
		selectedByID[item.candidate.ID] = ranked
	}
	exploreIndex := 0
	for i, item := range active {
		if i%9 == 0 && exploreIndex < len(exploring) {
			appendSource(exploring[exploreIndex], true)
			exploreIndex++
		}
		appendSource(item, false)
	}
	for ; exploreIndex < len(exploring); exploreIndex++ {
		appendSource(exploring[exploreIndex], true)
	}
	decisions := make([]SourceLearningDecision, 0, len(scored))
	for _, item := range scored {
		ranked, selected := selectedByID[item.candidate.ID]
		var position *int
		if selected {
			value := int(ranked.Priority)
			position = &value
		}
		reasons := append([]string(nil), item.cooldownReasons...)
		if ranked.Exploration {
			reasons = append(reasons, "exploration")
		}
		if len(item.cooldownReasons) == 0 {
			if item.candidate.Quality.Corrupt {
				reasons = append(reasons, "quality_corrupt_fail_open")
			} else {
				reasons = append(reasons, "active")
			}
		}
		decisions = append(decisions, sourceLearningDecision(item, selected, ranked.Exploration, position, reasons))
	}
	return DetailedSourceLearningPlan{Selected: plan, Decisions: decisions}
}

func sourceLearningDecision(item scoredSource, selected, explored bool, position *int, reasons []string) SourceLearningDecision {
	quality := item.candidate.Quality
	return SourceLearningDecision{
		ID:                            item.candidate.ID,
		Selected:                      selected,
		Cooled:                        item.cooled,
		Explored:                      explored,
		AdmissionCooled:               item.admissionCooled,
		TrustedPriorityProtected:      item.candidate.FallbackPriority <= 10,
		CurrentPositiveProbeProtected: item.candidate.ProbeContractCurrent && (quality.ProbePasses > 0 || quality.ProbeSuccessfulCompletions > 0),
		ProbeContractCurrent:          item.candidate.ProbeContractCurrent,
		PlanPosition:                  position,
		Reasons:                       reasons,
	}
}

func sourceCooldownReasons(candidate SourceLearningCandidate, now time.Time, config SourceLearningConfig, eligible bool, score float64) []string {
	if !config.Enabled || candidate.Quality.Corrupt {
		return nil
	}
	var reasons []string
	if candidate.Quality.CooldownUntil.After(now) {
		reasons = append(reasons, "failure_cooldown")
	}
	if eligible && config.LowYieldThreshold > 0 && score < config.LowYieldThreshold && config.LowYieldCooldown > 0 {
		if candidate.Quality.ProbeUpdatedAt.Add(config.LowYieldCooldown).After(now) {
			reasons = append(reasons, "low_yield_cooldown")
		}
	}
	if shouldCoolZeroAdmission(candidate, now, config) {
		reasons = append(reasons, "zero_admission_cooldown")
	}
	return reasons
}

func shouldCoolZeroAdmission(candidate SourceLearningCandidate, now time.Time, config SourceLearningConfig) bool {
	if !config.Enabled || config.ZeroAdmissionCooldown <= 0 || candidate.FallbackPriority <= 10 {
		return false
	}
	refreshes := config.ZeroAdmissionRefreshes
	if refreshes < 3 {
		refreshes = 3
	}
	quality := candidate.Quality
	if quality.Corrupt {
		return false
	}
	// A positive observed pass protects a cold but useful feed from the additional novelty cooldown.
	if quality.ProbePasses > 0 || quality.ProbeSuccessfulCompletions > 0 {
		return false
	}
	if quality.AdmissionRefreshes < refreshes || quality.ZeroAdmissionStreak < refreshes || quality.LastAdmitted != 0 {
		return false
	}
	if quality.LastAdmissionAt.IsZero() || quality.LastAdmissionAt.After(now) {
		return false
	}
	if !quality.LastAdmissionAt.Add(config.ZeroAdmissionCooldown).After(now) {
		return false
	}
	minimum, halfLife := config.normalized()
	score, samples := store.DecayedSourceYield(quality, now, halfLife)
	if samples >= minimum && config.LowYieldThreshold >= 0 && score >= config.LowYieldThreshold {
		return false
	}
	return true
}

func (c SourceLearningConfig) normalized() (float64, time.Duration) {
	minimum := c.MinSamples
	if minimum <= 0 {
		minimum = 8
	}
	halfLife := c.HalfLife
	if halfLife <= 0 {
		halfLife = 7 * 24 * time.Hour
	}
	return minimum, halfLife
}

func exploreThisRefresh(sourceID string, refreshNumber, interval uint64) bool {
	if interval <= 1 {
		return true
	}
	sum := sha256.Sum256([]byte(sourceID))
	bucket := binary.BigEndian.Uint64(sum[:8]) % interval
	return (refreshNumber+bucket)%interval == 0
}
