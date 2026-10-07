package monitor

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

const bucketTestNS = "bucket-test:v1"

func newBucketTestRunner(t *testing.T, limit, chunk int, clock *testClock) (*miniredis.Miniredis, *Runner) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", bucketTestNS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	return mini, &Runner{
		store:         redisStore,
		metrics:       newMetrics(),
		worker:        "bucket-worker",
		nowFunc:       clock.now,
		workAvailable: newWakeNotifier(),
		config: Config{
			GlobalRequestsPerMinute: limit,
			ProbePermitChunk:        chunk,
			LeaseTTL:                30 * time.Second,
		},
	}
}

func budgetCounter(mini *miniredis.Miniredis, at time.Time) string {
	value, _ := mini.Get(bucketTestNS + ":probe-budget:" + at.UTC().Format("200601021504"))
	return value
}

func TestProbePermitChunkDefaults(t *testing.T) {
	cases := []struct{ workers, chunk, limit, want int }{
		{workers: 384, limit: 60000, want: 96},
		{workers: 8, limit: 60000, want: 10},
		{workers: 384, chunk: 25, limit: 60000, want: 25},
		{workers: 384, limit: 5, want: 5},
	}
	for _, tc := range cases {
		r := &Runner{config: Config{Workers: tc.workers, ProbePermitChunk: tc.chunk, GlobalRequestsPerMinute: tc.limit}}
		if got := r.probePermitChunk(); got != tc.want {
			t.Fatalf("probePermitChunk(%+v) = %d, want %d", tc, got, tc.want)
		}
	}
}

func TestProbePermitBucketRefillsInChunks(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	mini, runner := newBucketTestRunner(t, 25, 10, clock)
	ctx := context.Background()
	for i := 1; i <= 25; i++ {
		ok, err := runner.takeProbePermit(ctx)
		if err != nil || !ok {
			t.Fatalf("permit %d: ok=%t err=%v", i, ok, err)
		}
		// One Redis reservation per chunk; the final one is capped at the limit.
		want := map[int]string{1: "10", 10: "10", 11: "20", 20: "20", 21: "25", 25: "25"}
		if w, check := want[i]; check {
			if got := budgetCounter(mini, clock.now()); got != w {
				t.Fatalf("after permit %d counter = %q, want %q", i, got, w)
			}
		}
	}
	for i := 0; i < 3; i++ {
		if ok, err := runner.takeProbePermit(ctx); err != nil || ok {
			t.Fatalf("permit past limit: ok=%t err=%v", ok, err)
		}
	}
	if got := runner.metrics.budgetDenialsTotal.Load(); got != 3 {
		t.Fatalf("budget denials = %d, want 3", got)
	}
	if got := budgetCounter(mini, clock.now()); got != "25" {
		t.Fatalf("denials changed the counter to %q", got)
	}

	// Window rollover: leftover state is discarded and a new window reserves.
	clock.advance(time.Minute)
	if ok, err := runner.takeProbePermit(ctx); err != nil || !ok {
		t.Fatalf("permit in next window: ok=%t err=%v", ok, err)
	}
	if got := budgetCounter(mini, clock.now()); got != "10" {
		t.Fatalf("next window counter = %q, want 10", got)
	}
}

func TestReturnedPermitStaysInItsWindow(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 59, 0, time.UTC))
	mini, runner := newBucketTestRunner(t, 100, 1, clock)
	ctx := context.Background()
	permit, ok, err := runner.acquireProbePermit(ctx)
	if err != nil || !ok {
		t.Fatalf("acquire: ok=%t err=%v", ok, err)
	}
	runner.returnProbePermit(permit)
	if _, ok, _ := runner.acquireProbePermit(ctx); !ok || budgetCounter(mini, clock.now()) != "1" {
		t.Fatalf("returned permit was not reused locally: counter=%q", budgetCounter(mini, clock.now()))
	}
	stale, _, _ := runner.acquireProbePermit(ctx)
	clock.advance(2 * time.Second)
	runner.returnProbePermit(stale)
	if _, ok, _ := runner.acquireProbePermit(ctx); !ok {
		t.Fatal("acquire in new window failed")
	}
	if got := budgetCounter(mini, clock.now()); got != "1" {
		t.Fatalf("a permit from the closed window leaked into the new one: counter=%q", got)
	}
}

func TestProbePermitBucketNeverExceedsLimitAcrossReplicas(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	mini, first := newBucketTestRunner(t, 500, 7, clock)
	replicas := []*Runner{first}
	for i := 0; i < 3; i++ {
		redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", bucketTestNS)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = redisStore.Close() })
		replicas = append(replicas, &Runner{store: redisStore, metrics: newMetrics(), nowFunc: clock.now, config: first.config})
	}
	var mu sync.Mutex
	granted := 0
	var wg sync.WaitGroup
	for _, replica := range replicas {
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					ok, err := replica.takeProbePermit(context.Background())
					if err != nil {
						t.Errorf("takeProbePermit: %v", err)
						return
					}
					if !ok {
						return
					}
					mu.Lock()
					granted++
					mu.Unlock()
				}
			}()
		}
	}
	wg.Wait()
	if granted != 500 || budgetCounter(mini, clock.now()) != "500" {
		t.Fatalf("granted=%d counter=%q, want 500", granted, budgetCounter(mini, clock.now()))
	}
}

func TestClaimProbeBatchReturnsPermitsWhenNoWorkIsDue(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	mini, runner := newBucketTestRunner(t, 100, 10, clock)
	claims, outcome := runner.claimProbeBatch(context.Background(), 1)
	if len(claims) != 0 || outcome != claimNoWork {
		t.Fatalf("empty queue: claims=%+v outcome=%d, want none and claimNoWork", claims, outcome)
	}
	if runner.permits.tokens != 10 {
		t.Fatalf("local tokens = %d, want the full chunk back", runner.permits.tokens)
	}
	if got := budgetCounter(mini, clock.now()); got != "10" {
		t.Fatalf("counter = %q, want a single chunk reservation", got)
	}
}

func TestClaimProbeBatchDoesNotClaimOrReleaseWhenBudgetDenied(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	mini, runner := newBucketTestRunner(t, 1, 1, clock)
	if _, err := runner.store.Upsert(context.Background(), []string{"http://192.0.2.44:8080"}, clock.now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	members, err := mini.ZMembers(bucketTestNS + ":pending")
	if err != nil || len(members) != 1 {
		t.Fatalf("pending members = %v err=%v", members, err)
	}
	scoreBefore, _ := mini.ZScore(bucketTestNS+":pending", members[0])
	if ok, err := runner.takeProbePermit(context.Background()); err != nil || !ok {
		t.Fatalf("spend budget: ok=%t err=%v", ok, err)
	}

	claims, outcome := runner.claimProbeBatch(context.Background(), 1)
	if len(claims) != 0 || outcome != claimNoBudget {
		t.Fatalf("denied budget: claims=%+v outcome=%d, want none and claimNoBudget", claims, outcome)
	}
	scoreAfter, err := mini.ZScore(bucketTestNS+":pending", members[0])
	if err != nil || scoreAfter != scoreBefore {
		t.Fatalf("candidate was claimed/released and re-scored: before=%v after=%v err=%v", scoreBefore, scoreAfter, err)
	}
	if mini.Exists(bucketTestNS + ":leased") {
		if leased, _ := mini.ZMembers(bucketTestNS + ":leased"); len(leased) != 0 {
			t.Fatalf("candidate leased despite denied budget: %v", leased)
		}
	}
	if got := runner.metrics.budgetDenialsTotal.Load(); got != 1 {
		t.Fatalf("budget denials = %d, want 1", got)
	}

	// With budget in the next window the same claimer claims the candidate.
	clock.advance(time.Minute)
	claims, outcome = runner.claimProbeBatch(context.Background(), 1)
	if outcome != claimedWork || len(claims) != 1 || claims[0].ID != members[0] {
		t.Fatalf("next window claim: claims=%+v outcome=%d", claims, outcome)
	}
}
