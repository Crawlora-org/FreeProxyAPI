package monitor

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

// startClaimer runs claimLoop on a fresh feed. The returned stop cancels it and
// waits for it to exit.
func startClaimer(t *testing.T, runner *Runner, workers int, demand int) (feed *claimFeed, stop func()) {
	t.Helper()
	feed = newClaimFeed(workers)
	// Queue the demand before the claimer starts so it sees every idle worker
	// in its first look and the batching is deterministic.
	for i := 0; i < demand; i++ {
		feed.demand <- struct{}{}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.claimLoop(ctx, feed)
		close(done)
	}()
	stop = func() {
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("claimLoop did not exit after shutdown")
		}
	}
	t.Cleanup(stop)
	return feed, stop
}

func receive(t *testing.T, feed *claimFeed, n int) []store.Claim {
	t.Helper()
	var claims []store.Claim
	for i := 0; i < n; i++ {
		select {
		case c := <-feed.claims:
			claims = append(claims, c)
		case <-time.After(3 * time.Second):
			t.Fatalf("received %d of %d claims", len(claims), n)
		}
	}
	return claims
}

func dueCandidates(t *testing.T, runner *Runner, n int) {
	t.Helper()
	urls := make([]string, n)
	for i := range urls {
		urls[i] = fmt.Sprintf("http://192.0.2.%d:8080", i+1)
	}
	if _, err := runner.store.Upsert(context.Background(), urls, runner.nowFunc().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
}

func leasedAndPending(t *testing.T, runner *Runner) (leased, pending int64) {
	t.Helper()
	stats, err := runner.store.StatsAt(context.Background(), runner.nowFunc())
	if err != nil {
		t.Fatal(err)
	}
	return stats.Leased, stats.Pending
}

// The claimer leases for exactly the workers that are idle, in one call, and
// never more.
func TestClaimLoopClaimsForExactlyTheIdleWorkersInOneCall(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	_, runner := newBucketTestRunner(t, 1000, 10, clock)
	dueCandidates(t, runner, 10)

	feed, _ := startClaimer(t, runner, 64, 3)
	claims := receive(t, feed, 3)
	seen := map[string]bool{}
	for _, c := range claims {
		if seen[c.ID] {
			t.Fatalf("worker handed the same candidate twice: %+v", claims)
		}
		seen[c.ID] = true
	}
	if got := runner.metrics.claimBatches.Load(); got != 1 {
		t.Fatalf("claim calls = %d, want 1 for 3 idle workers", got)
	}
	time.Sleep(60 * time.Millisecond)
	if leased, pending := leasedAndPending(t, runner); leased != 3 || pending != 7 {
		t.Fatalf("leased=%d pending=%d, want 3 and 7: the claimer must not lease ahead of idle workers", leased, pending)
	}
}

// Many idle workers and nothing due cost one claim call, not one per worker,
// and the permits taken for it go back.
func TestClaimLoopMakesOneEmptyClaimForManyIdleWorkers(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	mini, runner := newBucketTestRunner(t, 1000, 10, clock)
	feed, _ := startClaimer(t, runner, 64, 20)

	waitFor(t, 3*time.Second, func() bool { return runner.metrics.claimEmpty.Load() == 1 }, "no empty claim was made")
	time.Sleep(80 * time.Millisecond)
	if got := runner.metrics.claimEmpty.Load(); got != 1 {
		t.Fatalf("empty claim calls = %d for 20 idle workers, want 1", got)
	}
	runner.permits.mu.Lock()
	tokens := runner.permits.tokens
	runner.permits.mu.Unlock()
	if tokens != 20 {
		t.Fatalf("local permits = %d, want all 20 back after an empty claim", tokens)
	}
	if got := budgetCounter(mini, clock.now()); got != "20" {
		t.Fatalf("budget counter = %q, want one reservation of 20 permits", got)
	}

	// Work arrives and the scheduler signals: the one waiting claimer wakes and
	// serves the idle workers.
	dueCandidates(t, runner, 1)
	runner.workAvailable.signal()
	claims := receive(t, feed, 1)
	if len(claims) != 1 {
		t.Fatalf("claims = %v", claims)
	}
}

func TestClaimLoopReleasesUndeliveredClaimsOnShutdown(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	_, runner := newBucketTestRunner(t, 1000, 10, clock)
	dueCandidates(t, runner, 3)

	_, stop := startClaimer(t, runner, 64, 3)
	waitFor(t, 3*time.Second, func() bool { return runner.metrics.claimBatches.Load() == 1 }, "the batch was not claimed")
	// No worker takes the claims. Shutting down must give them back.
	stop()
	if leased, pending := leasedAndPending(t, runner); leased != 0 || pending != 3 {
		t.Fatalf("after shutdown leased=%d pending=%d, want every undelivered claim released", leased, pending)
	}
}

func TestClaimLoopDoesNotClaimWhileTheControlProbeIsUnhealthy(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	_, runner := newBucketTestRunner(t, 1000, 10, clock)
	runner.accuracy.pausePoll = 2 * time.Millisecond
	runner.accuracy.controlUnhealthy.Store(true)
	dueCandidates(t, runner, 2)

	feed, _ := startClaimer(t, runner, 64, 2)
	time.Sleep(60 * time.Millisecond)
	if got := runner.metrics.claimBatches.Load() + runner.metrics.claimEmpty.Load(); got != 0 {
		t.Fatalf("made %d claim calls while the control probe was unhealthy", got)
	}
	if leased, _ := leasedAndPending(t, runner); leased != 0 {
		t.Fatalf("leased %d candidates while unhealthy", leased)
	}
	runner.accuracy.controlUnhealthy.Store(false)
	if claims := receive(t, feed, 2); len(claims) != 2 {
		t.Fatalf("claims after recovery = %v", claims)
	}
}

// A spent budget limits the batch to the permits that exist, and the rest of
// the idle workers wait for the next window instead of claiming without one.
func TestClaimLoopClaimsOnlyAsManyAsTheBudgetAllows(t *testing.T) {
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 5, 0, time.UTC))
	_, runner := newBucketTestRunner(t, 5, 5, clock)
	dueCandidates(t, runner, 10)

	feed, _ := startClaimer(t, runner, 64, 10)
	claims := receive(t, feed, 5)
	if len(claims) != 5 {
		t.Fatalf("claims = %v", claims)
	}
	waitFor(t, 3*time.Second, func() bool { return runner.metrics.budgetDenialsTotal.Load() >= 1 }, "the spent budget was not detected")
	time.Sleep(60 * time.Millisecond)
	if leased, pending := leasedAndPending(t, runner); leased != 5 || pending != 5 {
		t.Fatalf("leased=%d pending=%d, want 5 and 5 with the budget spent", leased, pending)
	}
	select {
	case c := <-feed.claims:
		t.Fatalf("claimed %+v with no budget left", c)
	default:
	}
}

// End to end with real workers: every due candidate is probed exactly once and
// committed, whatever the batch boundaries.
func TestRunProbeWorkersProbesEveryCandidateExactlyOnce(t *testing.T) {
	clock := newTestClock(time.Now())
	_, runner := newBucketTestRunner(t, 100000, 50, clock)
	runner.nowFunc = time.Now
	runner.scheduleChanged = newWakeNotifier()
	runner.config.ProbeTarget = "http://target.invalid/ok"
	runner.config.RequestTimeout = time.Second
	runner.config.MinSamplesForListing = 1
	runner.config.ValidatedRetestInterval = time.Hour
	runner.config.SampleRetestInterval = time.Hour
	runner.config.FailedRetestInterval = time.Hour
	runner.config.ListedFailureRetestInterval = time.Hour
	runner.config.MaxConsecutiveFailures = 10

	const candidates = 120
	dueCandidates(t, runner, candidates)

	var mu sync.Mutex
	probed := map[string]int{}
	runner.probeFunc = func(_ context.Context, proxyURL, _ string, _ time.Duration) probe.Result {
		time.Sleep(time.Millisecond)
		mu.Lock()
		probed[proxyURL]++
		mu.Unlock()
		return probe.Result{OK: true, StatusCode: 204, Duration: time.Millisecond}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.runProbeWorkers(ctx, 24)
		close(done)
	}()
	waitFor(t, 10*time.Second, func() bool {
		stats, err := runner.store.StatsAt(context.Background(), time.Now())
		return err == nil && stats.Validated == candidates
	}, "not every candidate was probed and committed")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("workers did not exit on shutdown")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(probed) != candidates {
		t.Fatalf("probed %d distinct candidates, want %d", len(probed), candidates)
	}
	for url, n := range probed {
		if n != 1 {
			t.Fatalf("%s was probed %d times", url, n)
		}
	}
	if got := runner.metrics.claimsTotal.Load(); got != candidates {
		t.Fatalf("claims = %d, want %d", got, candidates)
	}
	// Idle workers no longer poll for work one by one: far fewer claim calls
	// than candidates.
	calls := runner.metrics.claimBatches.Load() + runner.metrics.claimEmpty.Load()
	if calls >= candidates/2 {
		t.Fatalf("%d claim calls for %d candidates; batching is not working", calls, candidates)
	}
	if leased, _ := leasedAndPending(t, runner); leased != 0 {
		t.Fatalf("%d candidates still leased after shutdown", leased)
	}
}
