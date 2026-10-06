package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisClaimsAreExclusiveAndReclaimable(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := store.Upsert(ctx, []string{"http://proxy.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	schedule, err := store.ValidationSchedule(ctx, now)
	if err != nil || schedule.Leased != 0 || !schedule.HasNextDue || schedule.NextDueAt.After(now) {
		t.Fatalf("initial validation schedule: %+v err=%v", schedule, err)
	}
	first, ok, err := store.ClaimDue(ctx, "one", now, time.Second)
	if err != nil || !ok {
		t.Fatalf("first ClaimDue: claim=%+v ok=%t err=%v", first, ok, err)
	}
	schedule, err = store.ValidationSchedule(ctx, now)
	if err != nil || schedule.Leased != 1 {
		t.Fatalf("running validation schedule: %+v err=%v", schedule, err)
	}
	if _, ok, err := store.ClaimDue(ctx, "two", now, time.Second); err != nil || ok {
		t.Fatalf("second ClaimDue should be empty: ok=%t err=%v", ok, err)
	}
	if _, err := store.ReclaimExpired(ctx, now.Add(2*time.Second), 10); err != nil {
		t.Fatalf("ReclaimExpired: %v", err)
	}
	second, ok, err := store.ClaimDue(ctx, "two", now.Add(2*time.Second), time.Second)
	if err != nil || !ok || second.ID != first.ID {
		t.Fatalf("reclaimed ClaimDue: claim=%+v ok=%t err=%v", second, ok, err)
	}
	outcome, err := store.Complete(ctx, second, now.Add(2*time.Second), RetestPolicy{ValidatedAfter: time.Hour}, true, 204, 10*time.Millisecond, "", OutcomeMeta{})
	if err != nil || !outcome.Committed {
		t.Fatalf("Complete: outcome=%+v err=%v", outcome, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Validated != 1 || stats.Leased != 0 {
		t.Fatalf("Stats: stats=%+v err=%v", stats, err)
	}
	schedule, err = store.ValidationSchedule(ctx, now.Add(2*time.Second))
	if err != nil || schedule.Leased != 0 || !schedule.HasNextDue || !schedule.NextDueAt.After(now.Add(2*time.Second)) {
		t.Fatalf("rescheduled validation: %+v err=%v", schedule, err)
	}
}

func TestRedisUpsertDeduplicatesAcrossSourcesBeforeClaims(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	duplicate := "http://proxy.example.net:8080"

	added, err := store.Upsert(ctx, []string{duplicate, "socks5://proxy.example.net:1080"}, now)
	if err != nil || added != 2 {
		t.Fatalf("first Upsert added=%d err=%v", added, err)
	}
	added, err = store.Upsert(ctx, []string{duplicate, "http://other.example.net:8080"}, now)
	if err != nil || added != 1 {
		t.Fatalf("second Upsert added=%d err=%v", added, err)
	}

	stats, err := store.Stats(ctx)
	if err != nil || stats.Candidates != 3 || stats.Pending != 3 {
		t.Fatalf("deduplicated stats: %+v err=%v", stats, err)
	}
	claims := make(map[string]bool, 3)
	for len(claims) < 3 {
		claim, ok, err := store.ClaimDue(ctx, "dedupe-test", now, time.Minute)
		if err != nil {
			t.Fatalf("ClaimDue: %v", err)
		}
		if !ok {
			t.Fatalf("ClaimDue stopped before all unique candidates were claimed: %d", len(claims))
		}
		if claims[claim.URL] {
			t.Fatalf("duplicate claim for %q", claim.URL)
		}
		claims[claim.URL] = true
	}
}

func TestRedisUpsertPreservesActiveLeaseAndPendingSchedule(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	activeURL := "http://active.example.net:8080"
	futureURL := "http://future.example.net:8080"
	newURL := "http://new.example.net:8080"
	if _, err := store.Upsert(ctx, []string{activeURL, futureURL}, now); err != nil {
		t.Fatalf("initial Upsert: %v", err)
	}
	futureID := proxyID(futureURL)
	futureDue := now.Add(6 * time.Hour)
	mini.ZAdd(store.pendingKey(), float64(futureDue.UnixMilli()), futureID)

	active, found, err := store.ClaimDue(ctx, "worker-one", now, time.Hour)
	if err != nil || !found || active.URL != activeURL {
		t.Fatalf("ClaimDue: claim=%+v found=%t err=%v", active, found, err)
	}
	leaseScore, err := mini.ZScore(store.leasedKey(), active.ID)
	if err != nil {
		t.Fatalf("leased score: %v", err)
	}

	refreshedAt := now.Add(10 * time.Minute)
	added, err := store.Upsert(ctx, []string{activeURL, futureURL, newURL}, refreshedAt)
	if err != nil || added != 1 {
		t.Fatalf("refresh Upsert: added=%d err=%v", added, err)
	}
	if zsetContains(t, mini, store.pendingKey(), active.ID) {
		t.Fatal("refresh put actively leased candidate back in pending")
	}
	if got, err := mini.ZScore(store.leasedKey(), active.ID); err != nil || got != leaseScore {
		t.Fatalf("active lease changed: score=%v err=%v, want %v", got, err, leaseScore)
	}
	if got := mini.HGet(store.proxyKey(active.ID), "lease_token"); got != active.Token {
		t.Fatalf("active token changed: got %q want %q", got, active.Token)
	}
	if got, err := mini.ZScore(store.pendingKey(), futureID); err != nil || got != float64(futureDue.UnixMilli()) {
		t.Fatalf("existing due time changed: score=%v err=%v", got, err)
	}
	newID := proxyID(newURL)
	if got, err := mini.ZScore(store.pendingKey(), newID); err != nil || got != float64(refreshedAt.UnixMilli()) {
		t.Fatalf("new candidate schedule: score=%v err=%v", got, err)
	}
	// last_seen_at_ms was never read; refreshes no longer write it.
	if mini.Exists(store.proxyKey(active.ID)) && mini.HGet(store.proxyKey(active.ID), "last_seen_at_ms") != "" {
		t.Fatal("refresh wrote unused last_seen_at_ms")
	}

	outcome, err := store.Complete(ctx, active, refreshedAt, RetestPolicy{ValidatedAfter: time.Hour}, true, 204, time.Millisecond, "", OutcomeMeta{})
	if err != nil || !outcome.Committed {
		t.Fatalf("Complete after refresh: outcome=%+v err=%v", outcome, err)
	}
}

func TestRedisClaimDueSkipsExistingActiveLease(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	urls := []string{"http://active.example.net:8080", "http://next.example.net:8080"}
	if _, err := store.Upsert(ctx, urls, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	active, found, err := store.ClaimDue(ctx, "worker-one", now, time.Hour)
	if err != nil || !found {
		t.Fatalf("first ClaimDue: found=%t err=%v", found, err)
	}
	mini.ZAdd(store.pendingKey(), float64(now.UnixMilli()-1), active.ID)

	next, found, err := store.ClaimDue(ctx, "worker-two", now, time.Hour)
	if err != nil || !found || next.ID == active.ID {
		t.Fatalf("defensive ClaimDue: claim=%+v found=%t err=%v", next, found, err)
	}
	if got := mini.HGet(store.proxyKey(active.ID), "lease_token"); got != active.Token {
		t.Fatalf("active token overwritten: got %q want %q", got, active.Token)
	}
	if zsetContains(t, mini, store.pendingKey(), active.ID) {
		t.Fatal("polluted pending entry was not removed")
	}
}

func zsetContains(t *testing.T, mini *miniredis.Miniredis, key, member string) bool {
	t.Helper()
	members, err := mini.ZMembers(key)
	if err != nil {
		return false
	}
	for _, candidate := range members {
		if candidate == member {
			return true
		}
	}
	return false
}

func TestRedisClaimDuePropagatesRedisError(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	mini.SetError("LOADING Redis is loading the dataset in memory")
	if _, found, err := store.ClaimDue(context.Background(), "worker", time.Now(), time.Minute); err == nil || found {
		t.Fatalf("ClaimDue should propagate Redis error: found=%t err=%v", found, err)
	}
}

func TestRedisStatsAtReportsDueAndNextPendingWork(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	if _, err := store.Upsert(ctx, []string{
		"http://due.example.net:8080",
		"http://future.example.net:8080",
	}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	if err := store.SetSourceRefreshStats(ctx, 42, 456, 7); err != nil {
		t.Fatalf("SetSourceRefreshStats: %v", err)
	}
	futureID := proxyID("http://future.example.net:8080")
	mini.ZAdd(store.pendingKey(), float64(now.Add(time.Hour).UnixMilli()), futureID)

	stats, err := store.StatsAt(ctx, now)
	if err != nil {
		t.Fatalf("StatsAt: %v", err)
	}
	if stats.SourceUnique != 42 || stats.SourceParsed != 456 || stats.SourcesSucceeded != 7 || stats.Pending != 2 || stats.PendingDue != 1 {
		t.Fatalf("unexpected pending counts: %+v", stats)
	}
	if !stats.HasNextDue || !stats.NextDueAt.Equal(now) {
		t.Fatalf("unexpected next due: %+v", stats)
	}

	claim, found, err := store.ClaimDue(ctx, "stats-test", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("ClaimDue: claim=%+v found=%t err=%v", claim, found, err)
	}
	stats, err = store.StatsAt(ctx, now)
	if err != nil {
		t.Fatalf("StatsAt after claim: %v", err)
	}
	if stats.PendingDue != 0 || !stats.HasNextDue || !stats.NextDueAt.Equal(now.Add(time.Hour)) {
		t.Fatalf("unexpected future-only schedule: %+v", stats)
	}
}

func TestRedisGlobalPermit(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	now := time.Now()
	for i := 0; i < 2; i++ {
		allowed, err := store.TakePermit(context.Background(), now, 2)
		if err != nil || !allowed {
			t.Fatalf("permit %d: allowed=%t err=%v", i, allowed, err)
		}
	}
	allowed, err := store.TakePermit(context.Background(), now, 2)
	if err != nil || allowed {
		t.Fatalf("third permit: allowed=%t err=%v", allowed, err)
	}
}

func TestRedisConcurrentClaimsAreExclusive(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()

	const total = 40
	urls := make([]string, 0, total)
	for i := 0; i < total; i++ {
		urls = append(urls, fmt.Sprintf("http://proxy%d.example.net:8080", i))
	}
	if _, err := store.Upsert(ctx, urls, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	const workers = 8
	var (
		mu      sync.Mutex
		claimed []string
		wg      sync.WaitGroup
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				mu.Lock()
				done := len(claimed) >= total
				mu.Unlock()
				if done {
					return
				}
				claim, ok, err := store.ClaimDue(ctx, fmt.Sprintf("worker-%d", worker), now, time.Minute)
				if err != nil {
					t.Errorf("ClaimDue: %v", err)
					return
				}
				if !ok {
					time.Sleep(time.Millisecond)
					continue
				}
				mu.Lock()
				claimed = append(claimed, claim.ID)
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()

	seen := make(map[string]int, total)
	for _, id := range claimed {
		seen[id]++
	}
	if len(seen) != total {
		t.Fatalf("claimed %d unique candidates, want %d", len(seen), total)
	}
	for id, count := range seen {
		if count != 1 {
			t.Fatalf("candidate %s claimed %d times concurrently", id, count)
		}
	}
}

func TestRedisCompleteRejectsStaleToken(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := store.Upsert(ctx, []string{"http://proxy.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	first, ok, err := store.ClaimDue(ctx, "one", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first ClaimDue: ok=%t err=%v", ok, err)
	}
	stale := Claim{ID: first.ID, URL: first.URL, Token: "expired-token"}
	outcome, err := store.Complete(ctx, stale, now.Add(time.Second), RetestPolicy{ValidatedAfter: time.Hour}, true, 204, time.Millisecond, "", OutcomeMeta{})
	if err != nil || outcome.Committed {
		t.Fatalf("stale Complete: outcome=%+v err=%v", outcome, err)
	}
	// The stale attempt must not have scheduled a retest or validated anything.
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Validated != 0 || stats.Pending != 0 || stats.Leased != 1 {
		t.Fatalf("stats after stale complete: %+v", stats)
	}
	if _, err := store.ReclaimExpired(ctx, now.Add(2*time.Minute), 10); err != nil {
		t.Fatalf("ReclaimExpired: %v", err)
	}
	again, ok, err := store.ClaimDue(ctx, "two", now.Add(2*time.Minute), time.Minute)
	if err != nil || !ok || again.ID != first.ID {
		t.Fatalf("reclaimed candidate should be claimable again: ok=%t err=%v claim=%+v", ok, err, again)
	}
}

func TestRedisReleaseReturnsClaimToPending(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := store.Upsert(ctx, []string{"http://proxy.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	first, ok, err := store.ClaimDue(ctx, "one", now, time.Hour)
	if err != nil || !ok {
		t.Fatalf("ClaimDue: ok=%t err=%v", ok, err)
	}
	released, err := store.Release(ctx, first, now.Add(time.Second))
	if err != nil || !released {
		t.Fatalf("Release: released=%t err=%v", released, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Pending != 1 || stats.Leased != 0 {
		t.Fatalf("stats after release: %+v err=%v", stats, err)
	}
	// A second release for an already-released lease is a harmless no-op.
	released, err = store.Release(ctx, first, now.Add(2*time.Second))
	if err != nil || released {
		t.Fatalf("double Release: released=%t err=%v", released, err)
	}
	next, ok, err := store.ClaimDue(ctx, "two", now.Add(2*time.Second), time.Hour)
	if err != nil || !ok || next.ID != first.ID {
		t.Fatalf("released candidate re-claimable: ok=%t err=%v claim=%+v", ok, err, next)
	}
}

func TestRedisBudgetWindowResets(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	windowStart := time.Date(2026, time.August, 23, 12, 0, 30, 0, time.UTC)

	for i := 0; i < 3; i++ {
		allowed, err := store.TakePermit(ctx, windowStart, 3)
		if err != nil || !allowed {
			t.Fatalf("permit %d in first window: allowed=%t err=%v", i, allowed, err)
		}
	}
	if allowed, err := store.TakePermit(ctx, windowStart, 3); err != nil || allowed {
		t.Fatalf("fourth permit should be denied: allowed=%t err=%v", allowed, err)
	}
	// A different minute must open a fresh window without touching the old key.
	nextWindow := windowStart.Add(time.Minute)
	if allowed, err := store.TakePermit(ctx, nextWindow, 3); err != nil || !allowed {
		t.Fatalf("permit in next window: allowed=%t err=%v", allowed, err)
	}
	mini.FastForward(3 * time.Minute)
	if allowed, err := store.TakePermit(ctx, nextWindow.Add(time.Minute), 3); err != nil || !allowed {
		t.Fatalf("permit after expiry sweep: allowed=%t err=%v", allowed, err)
	}
}

func TestRedisUpsertHandlesLargeBatches(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()

	const total = upsertBatch*2 + 37
	urls := make([]string, 0, total)
	for i := 0; i < total; i++ {
		urls = append(urls, fmt.Sprintf("http://bulk%d.example.net:%d", i, 8080+i%1000))
	}
	added, err := store.Upsert(ctx, urls, now)
	if err != nil || added != total {
		t.Fatalf("Upsert added=%d err=%v", added, err)
	}
	stats, err := store.Stats(ctx)
	if err != nil || stats.Candidates != total || stats.Pending != total {
		t.Fatalf("stats after bulk upsert: %+v err=%v", stats, err)
	}
}
