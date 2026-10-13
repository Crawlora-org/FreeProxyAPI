package monitor

import (
	"fmt"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestSourceYieldUsesColdFallbackAndBayesianSamples(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	cfg := SourceLearningConfig{Enabled: true, MinSamples: 4, HalfLife: 24 * time.Hour}
	if score, samples, learned := SourceYield(store.SourceQuality{}, now, cfg); learned || score != 0.5 || samples != 0 {
		t.Fatalf("cold score=(%0.3f,%0.1f,%t), want prior and cold", score, samples, learned)
	}
	q := store.SourceQuality{ProbeSamples: 3, ProbePasses: 3, ProbeUpdatedAt: now}
	if score, samples, learned := SourceYield(q, now, cfg); learned || score <= 0.5 || samples != 3 {
		t.Fatalf("under-sampled score=(%0.3f,%0.1f,%t), want warm score but fallback status", score, samples, learned)
	}
	q.ProbeSamples, q.ProbePasses = 4, 4
	if score, samples, learned := SourceYield(q, now, cfg); !learned || score != 5.0/6.0 || samples != 4 {
		t.Fatalf("minimum-sample score=(%0.3f,%0.1f,%t)", score, samples, learned)
	}
	if score, samples, learned := SourceYield(q, now.Add(24*time.Hour), cfg); learned || samples != 2 || score != 3.0/4.0 {
		t.Fatalf("decayed score=(%0.3f,%0.1f,%t), want half samples and cold", score, samples, learned)
	}
}

func TestBuildSourceLearningPlanPreservesColdFallback(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	inputs := []SourceLearningCandidate{
		{ID: "ordinary", FallbackPriority: 100},
		{ID: "checked", FallbackPriority: 10},
		{ID: "warm", FallbackPriority: 100, Quality: store.SourceQuality{ProbeSamples: 2, ProbePasses: 2, ProbeUpdatedAt: now}},
	}
	plan := BuildSourceLearningPlan(inputs, 1, now, SourceLearningConfig{Enabled: true, MinSamples: 4})
	if len(plan) != 3 || plan[0].ID != "checked" || plan[1].ID != "ordinary" || plan[2].ID != "warm" {
		t.Fatalf("cold plan order = %+v, want fallback priorities with insufficient samples", plan)
	}
	inputs[0].Quality = store.SourceQuality{ProbeSamples: 16, ProbePasses: 15, ProbeUpdatedAt: now}
	inputs[1].Quality = store.SourceQuality{ProbeSamples: 16, ProbePasses: 8, ProbeUpdatedAt: now}
	plan = BuildSourceLearningPlan(inputs, 1, now, SourceLearningConfig{Enabled: true, MinSamples: 8})
	if len(plan) != 3 || plan[0].ID != "ordinary" || !plan[0].Learned {
		t.Fatalf("learned plan order = %+v, high-yield source should lead", plan)
	}
	if plan[0].Priority >= plan[1].Priority {
		t.Fatalf("learned priorities are not monotonic: %+v", plan)
	}
}

func TestSourceFailureCooldownExploresDeterministically(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	input := []SourceLearningCandidate{{
		ID: "temporarily-failed", FallbackPriority: 10,
		Quality: store.SourceQuality{CooldownUntil: now.Add(time.Hour)},
	}}
	cfg := SourceLearningConfig{Enabled: true, ExplorationInterval: 10}
	seenAt := -1
	for refresh := uint64(0); refresh < 10; refresh++ {
		plan := BuildSourceLearningPlan(input, refresh, now, cfg)
		if len(plan) == 1 {
			if !plan[0].Exploration {
				t.Fatalf("cooldown source selected outside exploration: %+v", plan)
			}
			if seenAt >= 0 {
				t.Fatalf("explored twice within one interval (%d, %d)", seenAt, refresh)
			}
			seenAt = int(refresh)
		}
	}
	if seenAt < 0 {
		t.Fatal("cooldown source was never explored")
	}
	input[0].Quality.CooldownUntil = now.Add(-time.Second)
	plan := BuildSourceLearningPlan(input, 11, now, cfg)
	if len(plan) != 1 || plan[0].Exploration {
		t.Fatalf("expired cooldown plan = %+v, want normal selection", plan)
	}
}

func TestLowYieldCooldownExploresByStableBucket(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	input := []SourceLearningCandidate{{
		ID: "low-yield", FallbackPriority: 10,
		Quality: store.SourceQuality{ProbeSamples: 64, ProbePasses: 1, ProbeUpdatedAt: now},
	}}
	cfg := SourceLearningConfig{Enabled: true, MinSamples: 8, HalfLife: time.Hour, LowYieldThreshold: .2, LowYieldCooldown: 24 * time.Hour}
	seen := 0
	for refresh := uint64(0); refresh < 10; refresh++ {
		plan := BuildSourceLearningPlan(input, refresh, now, cfg)
		if len(plan) == 1 {
			seen++
			if !plan[0].Exploration {
				t.Fatalf("low-yield source selected without exploration flag: %+v", plan)
			}
		}
	}
	if seen != 1 {
		t.Fatalf("exploration selections in ten refreshes = %d, want one", seen)
	}
}

func TestExplorationStaysNearFrontOfLargePlan(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	inputs := make([]SourceLearningCandidate, 80)
	for i := range inputs {
		inputs[i] = SourceLearningCandidate{ID: fmt.Sprintf("active-%03d", i), FallbackPriority: 20}
	}
	inputs = append(inputs, SourceLearningCandidate{
		ID: "recovering", FallbackPriority: 10,
		Quality: store.SourceQuality{CooldownUntil: now.Add(time.Hour)},
	})
	cfg := SourceLearningConfig{Enabled: true, ExplorationInterval: 10}
	var plan []RankedSource
	for refresh := uint64(0); refresh < 10; refresh++ {
		plan = BuildSourceLearningPlan(inputs, refresh, now, cfg)
		if len(plan) > len(inputs)-1 {
			break
		}
	}
	if len(plan) != len(inputs) {
		t.Fatalf("exploration was missing from all ten plans, len=%d", len(plan))
	}
	if plan[0].ID != "recovering" || !plan[0].Exploration {
		t.Fatalf("exploration source was starved behind active catalog: first=%+v", plan[0])
	}
}

func TestCooledSourcesRotateThroughBoundedExplorationLane(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	inputs := make([]SourceLearningCandidate, 0, 5)
	for i := 0; len(inputs) < 5; i++ {
		id := fmt.Sprintf("cooled-%03d", i)
		if exploreThisRefresh(id, 0, 10) {
			inputs = append(inputs, SourceLearningCandidate{
				ID: id, FallbackPriority: 10,
				Quality: store.SourceQuality{CooldownUntil: now.Add(time.Hour)},
			})
		}
	}
	cfg := SourceLearningConfig{Enabled: true, ExplorationInterval: 10}
	selected := make(map[string]struct{}, len(inputs))
	for epoch := uint64(0); epoch < uint64(len(inputs)); epoch++ {
		plan := BuildSourceLearningPlan(inputs, epoch*10, now, cfg)
		if len(plan) != 1 || !plan[0].Exploration {
			t.Fatalf("exploration epoch %d plan = %+v", epoch, plan)
		}
		selected[plan[0].ID] = struct{}{}
	}
	if len(selected) != len(inputs) {
		t.Fatalf("selected %d of %d same-bucket cooled sources", len(selected), len(inputs))
	}
}

func TestStableSourceIDsKeepParserHintsDistinct(t *testing.T) {
	httpID := StableSourceID(" https://feed.example/proxies?x=1 ", "http")
	socksID := StableSourceID("https://feed.example/proxies?x=1", "socks5")
	otherURLID := StableSourceID("https://feed.example/proxies?x=2", "http")
	if httpID == socksID || httpID == otherURLID || len(httpID) != 32 {
		t.Fatalf("source IDs not stable/distinct: %q %q %q", httpID, socksID, otherURLID)
	}
	if got := StableSourceID("https://feed.example/proxies", "HTTP"); got != StableSourceID("https://feed.example/proxies", "http") {
		t.Fatal("scheme hints should be case-normalized")
	}
}

func TestSourcePlanDeduplicatesIDs(t *testing.T) {
	now := time.Now()
	input := []SourceLearningCandidate{{ID: "same", FallbackPriority: 20}, {ID: "same", FallbackPriority: 10}, {ID: ""}}
	plan := BuildSourceLearningPlan(input, 0, now, SourceLearningConfig{Enabled: true})
	if len(plan) != 1 || plan[0].Priority != 0 {
		t.Fatalf("deduplicated plan = %+v", plan)
	}
}
