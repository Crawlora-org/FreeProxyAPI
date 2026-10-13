package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func sourceProbeContract(config Config) string {
	digest := sha256.Sum256([]byte(config.ProbeTarget + "\x00" + config.ProbeTargetMode + "\x00" + config.ProbeExpectedBody))
	return hex.EncodeToString(digest[:])
}

func (r *Runner) sourceProbeContract() string {
	if r.config.sourceProbeContract != "" {
		return r.config.sourceProbeContract
	}
	return sourceProbeContract(r.config)
}

type plannedSource struct {
	index           int
	priority, score int64
}
type sourceLearningReport struct {
	Prepared   bool   `json:"prepared"`
	Generation uint64 `json:"generation,omitempty"`
	Enabled    bool   `json:"enabled"`
	Configured int    `json:"configured"`
	Selected   int    `json:"selected"`
	Skipped    int    `json:"skipped"`
	Exploring  int    `json:"exploring"`
	Measured   int    `json:"measured"`
}
type sourcePlanState struct {
	mu     sync.RWMutex
	feeds  map[string]plannedSource
	report sourceLearningReport
}

func (r *Runner) sourceID(spec SourceSpec) string {
	return store.StableSourceID(spec.URL, spec.defaultScheme(r.config.DefaultProxyScheme))
}

func (r *Runner) prepareSourceLearning(ctx context.Context) (int, error) {
	if !r.config.SourceLearningEnabled {
		return len(r.config.SourceSpecs), nil
	}
	ids := make([]string, 0, len(r.config.SourceSpecs))
	indexes := make(map[string]int, len(r.config.SourceSpecs))
	for index, spec := range r.config.SourceSpecs {
		id := r.sourceID(spec)
		if _, exists := indexes[id]; exists {
			continue
		}
		indexes[id] = index
		ids = append(ids, id)
	}
	if err := r.store.SetActiveLearningSources(ctx, ids); err != nil {
		return 0, err
	}
	quality := make(map[string]store.SourceQuality, len(ids))
	now := r.nowFunc()
	for start := 0; start < len(ids); start += store.MaxSourceQualityReadBatch {
		batch, err := r.store.LoadSourceQuality(ctx, ids[start:min(start+store.MaxSourceQualityReadBatch, len(ids))], now)
		if err != nil {
			return 0, err
		}
		for id, q := range batch {
			if q.ProbeContract != r.sourceProbeContract() {
				q.ProbeSamples = 0
				q.ProbePasses = 0
				q.ProbeUpdatedAt = time.Time{}
				q.ProbeSeen = 0
			}
			quality[id] = q
		}
	}
	generation, err := r.store.NextSourceLearningRefresh(ctx)
	if err != nil {
		return 0, err
	}
	candidates := make([]SourceLearningCandidate, 0, len(ids))
	for _, id := range ids {
		candidates = append(candidates, SourceLearningCandidate{ID: id, FallbackPriority: sourcePriority(r.config.SourceSpecs[indexes[id]]), Quality: quality[id]})
	}
	plan := BuildSourceLearningPlan(candidates, generation, now, SourceLearningConfig{Enabled: true, MinSamples: 8, HalfLife: 7 * 24 * time.Hour, LowYieldThreshold: 0.02, LowYieldCooldown: 24 * time.Hour, ExplorationInterval: 10})
	feeds := make(map[string]plannedSource, len(plan))
	report := sourceLearningReport{Enabled: true, Prepared: true, Generation: generation, Configured: len(ids), Selected: len(plan), Skipped: len(ids) - len(plan)}
	for _, item := range plan {
		index := indexes[item.ID]
		score, err := store.EncodeSourceStageScore(item.Priority, index)
		if err != nil {
			return 0, err
		}
		feeds[item.ID] = plannedSource{index: index, priority: item.Priority, score: score}
		if item.Exploration {
			report.Exploring++
		}
		if item.Learned {
			report.Measured++
		}
	}
	r.sourcePlan.mu.Lock()
	r.sourcePlan.feeds, r.sourcePlan.report = feeds, report
	r.sourcePlan.mu.Unlock()
	return len(plan), nil
}

func (r *Runner) plannedSource(spec SourceSpec) (plannedSource, bool) {
	r.sourcePlan.mu.RLock()
	defer r.sourcePlan.mu.RUnlock()
	feed, exists := r.sourcePlan.feeds[r.sourceID(spec)]
	return feed, exists
}

func (r *Runner) sourceLearningSnapshot() sourceLearningReport {
	r.sourcePlan.mu.RLock()
	defer r.sourcePlan.mu.RUnlock()
	if !r.config.SourceLearningEnabled {
		return sourceLearningReport{}
	}
	report := r.sourcePlan.report
	report.Enabled = true
	if report.Configured == 0 {
		report.Configured = len(r.config.SourceSpecs)
	}
	return report
}

func (r *Runner) enqueueSourceBatch(ctx context.Context, batch []string, score int64, now time.Time) (store.UpsertCounts, error) {
	if !r.config.SourceLearningEnabled {
		return r.store.UpsertPrioritizedCounts(ctx, batch, sourceQueueTime(now, score))
	}
	priority, index, err := store.DecodeSourceStageScore(score)
	if err != nil || index < 0 || index >= len(r.config.SourceSpecs) || priority >= int64(len(r.config.SourceSpecs)) {
		return store.UpsertCounts{}, fmt.Errorf("invalid learned source stage score")
	}
	// Keep every ranked source immediately due while preserving the full
	// catalog order, including positions beyond the legacy 100ms band.
	due := now.Add(-time.Duration(max(100, len(r.config.SourceSpecs))-int(priority)) * time.Millisecond)
	return r.store.UpsertPrioritizedSourceCounts(ctx, batch, due, r.sourceID(r.config.SourceSpecs[index]))
}

func (r *Runner) recordLearningFetchFailure(ctx context.Context, spec SourceSpec) {
	if r.config.SourceLearningEnabled && ctx.Err() == nil {
		_ = r.store.RecordSourceFetchFailure(ctx, r.sourceID(spec), r.nowFunc())
	}
}
