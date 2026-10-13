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

func TestZeroAdmissionCooldownRequiresFreshFullObservations(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	config := SourceLearningConfig{
		Enabled: true, MinSamples: 8, LowYieldThreshold: 0.02,
		ZeroAdmissionCooldown: time.Hour, ZeroAdmissionRefreshes: 3,
	}
	base := SourceLearningCandidate{ID: "duplicate-feed", FallbackPriority: 20}
	base.Quality = store.SourceQuality{
		AdmissionRefreshes:  3,
		LastAdmitted:        0,
		ZeroAdmissionStreak: 3,
		LastAdmissionAt:     now,
		UniqueIncremental:   0,
	}
	if !shouldCoolZeroAdmission(base, now, config) {
		t.Fatal("three completed zero-admission observations should start cooldown for a cold duplicate")
	}

	for name, mutate := range map[string]func(*store.SourceQuality){
		"legacy missing observation count": func(q *store.SourceQuality) { q.AdmissionRefreshes = 0 },
		"below consecutive threshold":      func(q *store.SourceQuality) { q.AdmissionRefreshes = 2; q.ZeroAdmissionStreak = 2 },
		"corrupt quality fields":           func(q *store.SourceQuality) { q.Corrupt = true },
		"latest refresh admitted records": func(q *store.SourceQuality) {
			q.LastAdmitted = 1
			q.ZeroAdmissionStreak = 0
		},
		"missing timestamp": func(q *store.SourceQuality) { q.LastAdmissionAt = time.Time{} },
		"future timestamp":  func(q *store.SourceQuality) { q.LastAdmissionAt = now.Add(time.Second) },
		"expired cooldown":  func(q *store.SourceQuality) { q.LastAdmissionAt = now.Add(-2 * time.Hour) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := base
			mutate(&candidate.Quality)
			if shouldCoolZeroAdmission(candidate, now, config) {
				t.Fatal("candidate with untrusted or insufficient observation should fail open")
			}
		})
	}
	if shouldCoolZeroAdmission(base, now, SourceLearningConfig{Enabled: true, ZeroAdmissionRefreshes: 3}) {
		t.Fatal("zero cooldown must disable admission cooling")
	}
	stricter := config
	stricter.ZeroAdmissionRefreshes = 5
	if shouldCoolZeroAdmission(base, now, stricter) {
		t.Fatal("configured five-refresh threshold must not cool after only three observations")
	}
}

func TestZeroAdmissionCooldownProtectsTrustedAndHighQualityFeeds(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	config := SourceLearningConfig{
		Enabled: true, MinSamples: 8, LowYieldThreshold: 0.02,
		ZeroAdmissionCooldown: 24 * time.Hour, ZeroAdmissionRefreshes: 3,
	}
	quality := store.SourceQuality{
		AdmissionRefreshes: 6, ZeroAdmissionStreak: 6,
		LastAdmissionAt: now, LastAdmitted: 0,
	}
	if shouldCoolZeroAdmission(SourceLearningCandidate{ID: "trusted", FallbackPriority: 10, Quality: quality}, now, config) {
		t.Fatal("trusted fallback priority must remain eligible despite duplicate admissions")
	}
	quality.ProbeSamples, quality.ProbePasses, quality.ProbeUpdatedAt = 16, 16, now
	if shouldCoolZeroAdmission(SourceLearningCandidate{ID: "high-quality", FallbackPriority: 20, Quality: quality}, now, config) {
		t.Fatal("measured source above the existing low-yield threshold must remain eligible")
	}
	quality.ProbeSamples, quality.ProbePasses = 128, 0
	if !shouldCoolZeroAdmission(SourceLearningCandidate{ID: "low-quality", FallbackPriority: 20, Quality: quality}, now, config) {
		t.Fatal("measured low-yield duplicate should remain eligible for admission cooldown")
	}
}

func TestAdmissionCooledSourcesExploreAndRotate(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	config := SourceLearningConfig{
		Enabled: true, ExplorationInterval: 10, ZeroAdmissionCooldown: time.Hour,
		ZeroAdmissionRefreshes: 3,
	}
	inputs := make([]SourceLearningCandidate, 0, 5)
	for i := 0; len(inputs) < 5; i++ {
		id := fmt.Sprintf("admission-cooled-%03d", i)
		if exploreThisRefresh(id, 0, 10) {
			inputs = append(inputs, SourceLearningCandidate{
				ID: id, FallbackPriority: 20,
				Quality: store.SourceQuality{
					AdmissionRefreshes: 3, ZeroAdmissionStreak: 3,
					LastAdmissionAt: now,
				},
			})
		}
	}
	selected := make(map[string]struct{}, len(inputs))
	for epoch := uint64(0); epoch < uint64(len(inputs)); epoch++ {
		plan := BuildSourceLearningPlan(inputs, epoch*10, now, config)
		if len(plan) != 1 || !plan[0].Exploration || !plan[0].AdmissionCooled {
			t.Fatalf("admission exploration epoch %d plan = %+v", epoch, plan)
		}
		selected[plan[0].ID] = struct{}{}
	}
	if len(selected) != len(inputs) {
		t.Fatalf("selected %d of %d admission-cooled feeds", len(selected), len(inputs))
	}
}

func TestAdmissionExplorationDoesNotStarveActivePlan(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	inputs := make([]SourceLearningCandidate, 80)
	for i := range inputs {
		inputs[i] = SourceLearningCandidate{ID: fmt.Sprintf("active-admission-%03d", i), FallbackPriority: 20}
	}
	cooled := SourceLearningCandidate{
		ID: "admission-duplicate", FallbackPriority: 20,
		Quality: store.SourceQuality{
			AdmissionRefreshes: 3, ZeroAdmissionStreak: 3, LastAdmissionAt: now,
		},
	}
	inputs = append(inputs, cooled)
	cfg := SourceLearningConfig{Enabled: true, ExplorationInterval: 10, ZeroAdmissionCooldown: time.Hour, ZeroAdmissionRefreshes: 3}
	var plan []RankedSource
	for refresh := uint64(0); refresh < 10; refresh++ {
		plan = BuildSourceLearningPlan(inputs, refresh, now, cfg)
		if len(plan) > len(inputs)-1 {
			break
		}
	}
	if len(plan) != len(inputs) || plan[0].ID != cooled.ID || !plan[0].Exploration || !plan[0].AdmissionCooled {
		t.Fatalf("admission exploration was starved or displaced the active plan: %+v", plan[:min(len(plan), 2)])
	}
}

func TestZeroAdmissionCooldownProtectsColdPositiveProbeEvidence(t *testing.T) {
	now := time.Unix(1800000000, 0)
	q := store.SourceQuality{AdmissionRefreshes: 3, ZeroAdmissionStreak: 3, LastAdmissionAt: now, ProbeSamples: 1, ProbePasses: 1, ProbeUpdatedAt: now}
	cfg := SourceLearningConfig{Enabled: true, ZeroAdmissionCooldown: time.Hour, ZeroAdmissionRefreshes: 3, LowYieldThreshold: 0.02}
	if shouldCoolZeroAdmission(SourceLearningCandidate{ID: "positive-cold", FallbackPriority: 100, Quality: q}, now, cfg) {
		t.Fatal("cooled useful cold feed")
	}
}

func TestCorruptQualityFailsOpenForEveryLearningCooldown(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	corrupt := store.SourceQuality{
		Corrupt:      true,
		ProbeSamples: 128, ProbePasses: 0, ProbeUpdatedAt: now,
		CooldownUntil:      now.Add(time.Hour),
		AdmissionRefreshes: 5, ZeroAdmissionStreak: 5, LastAdmissionAt: now,
	}
	config := SourceLearningConfig{
		Enabled: true, MinSamples: 8, LowYieldThreshold: 0.02, LowYieldCooldown: 24 * time.Hour,
		ZeroAdmissionCooldown: time.Hour, ZeroAdmissionRefreshes: 3,
	}
	if score, samples, learned := SourceYield(corrupt, now, config); learned || score != 0.5 || samples != 0 {
		t.Fatalf("corrupt SourceYield = (%0.3f,%0.1f,%t), want unmeasured prior", score, samples, learned)
	}
	inputs := []SourceLearningCandidate{
		{ID: "checked-fallback", FallbackPriority: 10},
		{ID: "corrupt", FallbackPriority: 80, Quality: corrupt},
	}
	plan := BuildSourceLearningPlan(inputs, 1, now, config)
	if len(plan) != 2 || plan[0].ID != "checked-fallback" || plan[1].ID != "corrupt" || plan[1].Learned || plan[1].Exploration || plan[1].AdmissionCooled {
		t.Fatalf("corrupt source did not fail open at fallback priority: %+v", plan)
	}
}
