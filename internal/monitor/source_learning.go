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
}

type SourceLearningCandidate struct {
	ID               string
	FallbackPriority int64
	Quality          store.SourceQuality
}

// RankedSource is one entry selected for the next source refresh. Priority is
// lower-is-earlier and can be combined with the configured source index using
// store.EncodeSourceStageScore. Cold candidates retain the ordering established
// by sourcePriority; learned samples affect ordering only after MinSamples.
type RankedSource struct {
	ID          string
	Priority    int64
	Score       float64
	Learned     bool
	Exploration bool
}

type scoredSource struct {
	candidate SourceLearningCandidate
	score     float64
	learned   bool
	cooled    bool
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
	minimum, halfLife := config.normalized()
	score, effectiveSamples = store.DecayedSourceYield(q, now, halfLife)
	return score, effectiveSamples, effectiveSamples >= minimum
}

// BuildSourceLearningPlan ranks configured sources without mutating input.
// Sources in failure or low-yield cooldown are omitted except on their stable
// one-in-N exploration refresh, so a bad early estimate cannot suppress a feed
// forever. With learning disabled, fallback priorities are returned unchanged.
func BuildSourceLearningPlan(candidates []SourceLearningCandidate, refreshNumber uint64, now time.Time, config SourceLearningConfig) []RankedSource {
	if len(candidates) == 0 {
		return nil
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
		eligible := config.Enabled && samples >= minimum
		cooled := false
		if config.Enabled {
			cooled = candidate.Quality.CooldownUntil.After(now)
			if eligible && config.LowYieldThreshold > 0 && score < config.LowYieldThreshold && config.LowYieldCooldown > 0 {
				lowYieldUntil := candidate.Quality.ProbeUpdatedAt.Add(config.LowYieldCooldown)
				cooled = cooled || lowYieldUntil.After(now)
			}
		}
		scored = append(scored, scoredSource{candidate: candidate, score: score, learned: eligible, cooled: cooled})
	}
	if !config.Enabled {
		sort.SliceStable(scored, func(i, j int) bool {
			if scored[i].candidate.FallbackPriority != scored[j].candidate.FallbackPriority {
				return scored[i].candidate.FallbackPriority < scored[j].candidate.FallbackPriority
			}
			return false // stable sort preserves configured source order on ties
		})
		plan := make([]RankedSource, 0, len(scored))
		for _, item := range scored {
			plan = append(plan, RankedSource{ID: item.candidate.ID, Priority: item.candidate.FallbackPriority})
		}
		return plan
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
	appendSource := func(item scoredSource, isExploration bool) {
		plan = append(plan, RankedSource{
			ID: item.candidate.ID, Priority: int64(len(plan)), Score: item.score,
			Learned: item.learned, Exploration: isExploration,
		})
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
	return plan
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
