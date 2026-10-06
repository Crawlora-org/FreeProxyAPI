package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newTombstoneTestStore(t *testing.T) (*miniredis.Miniredis, *Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return mini, s
}

// failUntilGone upserts url at now, then claims and fails it until the store
// evicts or discards it, returning the final outcome.
func failUntilGone(t *testing.T, s *Redis, rawURL string, now time.Time, policy RetestPolicy) CompleteResult {
	t.Helper()
	ctx := context.Background()
	counts, err := s.UpsertCounts(ctx, []string{rawURL}, now)
	if err != nil || counts.Added != 1 || counts.Tombstoned != 0 {
		t.Fatalf("UpsertCounts at %s: %+v err=%v", now, counts, err)
	}
	for attempt := 0; attempt < 20; attempt++ {
		claimAt := now.Add(time.Duration(attempt) * 24 * time.Hour)
		claim, found, err := s.ClaimDue(ctx, "w", claimAt, time.Minute)
		if err != nil || !found {
			t.Fatalf("ClaimDue attempt %d: found=%t err=%v", attempt, found, err)
		}
		outcome, err := s.Complete(ctx, claim, now, policy, false, 0, time.Millisecond, "timeout", OutcomeMeta{})
		if err != nil {
			t.Fatalf("Complete attempt %d: %v", attempt, err)
		}
		if outcome.Evicted || outcome.Discarded {
			return outcome
		}
	}
	t.Fatal("candidate was never evicted")
	return CompleteResult{}
}

func TestRedisEvictionTombstoneBacksOffExponentially(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	ctx := context.Background()
	rawURL := "http://dead.example.net:8080"
	id := proxyID(rawURL)
	policy := RetestPolicy{
		ValidatedAfter: time.Hour, FailedAfter: time.Hour, MaxConsecutiveFailures: 2,
		EvictedBackoffBase: 2 * time.Hour, EvictedBackoffMax: 5 * time.Hour,
	}
	// Retention = 2 probes * 1h * 2 = 4h.
	start := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)

	out := failUntilGone(t, s, rawURL, start, policy)
	if !out.Evicted || out.Strikes != 1 || out.BlockedFor != 2*time.Hour {
		t.Fatalf("first eviction outcome = %+v", out)
	}
	wantValue := fmt.Sprintf("1:%d", start.Add(2*time.Hour).UnixMilli())
	if got, _ := mini.Get(s.tombstoneKey(id)); got != wantValue {
		t.Fatalf("tombstone = %q, want %q", got, wantValue)
	}
	if ttl := mini.TTL(s.tombstoneKey(id)); ttl != 2*time.Hour+4*time.Hour {
		t.Fatalf("tombstone TTL = %s, want 6h", ttl)
	}

	// While blocked, a refresh must not touch any candidate state.
	blockedAt := start.Add(time.Hour)
	counts, err := s.UpsertPrioritizedCounts(ctx, []string{rawURL, "http://fresh.example.net:8080"}, blockedAt)
	if err != nil || counts.Added != 1 || counts.Tombstoned != 1 {
		t.Fatalf("blocked UpsertPrioritizedCounts = %+v err=%v", counts, err)
	}
	if member, _ := mini.SIsMember(s.candidatesKey(), id); member {
		t.Fatal("tombstoned endpoint was added to candidates")
	}
	if mini.Exists(s.proxyKey(id)) {
		t.Fatal("tombstoned endpoint hash was written")
	}
	if zsetContains(t, mini, s.pendingKey(), id) {
		t.Fatal("tombstoned endpoint was scheduled")
	}
	mini.Del(s.proxyKey(proxyID("http://fresh.example.net:8080")))
	mini.SRem(s.candidatesKey(), proxyID("http://fresh.example.net:8080"))
	mini.ZRem(s.pendingKey(), proxyID("http://fresh.example.net:8080"))

	// After the block the endpoint may return, and strike memory survives.
	mini.FastForward(2*time.Hour + time.Minute)
	second := start.Add(2*time.Hour + time.Minute)
	out = failUntilGone(t, s, rawURL, second, policy)
	if out.Strikes != 2 || out.BlockedFor != 4*time.Hour {
		t.Fatalf("second eviction outcome = %+v, want strikes 2 blocked 4h", out)
	}
	if ttl := mini.TTL(s.tombstoneKey(id)); ttl != 8*time.Hour {
		t.Fatalf("second tombstone TTL = %s, want 8h", ttl)
	}

	mini.FastForward(4*time.Hour + time.Minute)
	third := second.Add(4*time.Hour + time.Minute)
	out = failUntilGone(t, s, rawURL, third, policy)
	if out.Strikes != 3 || out.BlockedFor != 5*time.Hour {
		t.Fatalf("third eviction outcome = %+v, want strikes 3 capped at 5h", out)
	}

	// Once the key expires the strike count starts over.
	mini.FastForward(11 * time.Hour)
	fresh := third.Add(11 * time.Hour)
	if mini.Exists(s.tombstoneKey(id)) {
		t.Fatal("tombstone did not expire")
	}
	out = failUntilGone(t, s, rawURL, fresh, policy)
	if out.Strikes != 1 || out.BlockedFor != 2*time.Hour {
		t.Fatalf("post-expiry eviction outcome = %+v", out)
	}
}

func TestRedisDiscardWritesTombstone(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	rawURL := "http://discard-tomb.example.net:8080"
	policy := RetestPolicy{
		ValidatedAfter: time.Hour, FailedAfter: 3 * time.Hour, DiscardFailedCandidates: true,
		EvictedBackoffBase: time.Hour, EvictedBackoffMax: 24 * time.Hour,
	}
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	out := failUntilGone(t, s, rawURL, now, policy)
	if !out.Discarded || out.Strikes != 1 || out.BlockedFor != time.Hour {
		t.Fatalf("discard outcome = %+v", out)
	}
	// Eviction disabled: retention uses one probe, 1 * 3h * 2 = 6h.
	if ttl := mini.TTL(s.tombstoneKey(proxyID(rawURL))); ttl != 7*time.Hour {
		t.Fatalf("discard tombstone TTL = %s, want 7h", ttl)
	}
}

func TestRedisTombstonesDisabledWithoutBase(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	rawURL := "http://no-tomb.example.net:8080"
	policy := RetestPolicy{FailedAfter: time.Hour, MaxConsecutiveFailures: 1}
	out := failUntilGone(t, s, rawURL, time.Now(), policy)
	if !out.Evicted || out.Strikes != 0 {
		t.Fatalf("outcome = %+v", out)
	}
	if mini.Exists(s.tombstoneKey(proxyID(rawURL))) {
		t.Fatal("tombstone written with zero backoff base")
	}
}

func TestRedisPrioritizeSkipsTombstonedCandidate(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	tombURL := "http://tomb-prio.example.net:8080"
	controlURL := "http://control-prio.example.net:8080"
	if _, err := s.Upsert(ctx, []string{tombURL, controlURL}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	future := float64(now.Add(time.Hour).UnixMilli())
	mini.ZAdd(s.pendingKey(), future, proxyID(tombURL))
	mini.ZAdd(s.pendingKey(), future, proxyID(controlURL))
	// A hash can coexist with a blocking tombstone after a re-add race.
	if err := mini.Set(s.tombstoneKey(proxyID(tombURL)), fmt.Sprintf("1:%d", now.Add(time.Hour).UnixMilli())); err != nil {
		t.Fatal(err)
	}
	counts, err := s.UpsertPrioritizedCounts(ctx, []string{tombURL, controlURL}, now)
	if err != nil || counts.Added != 0 || counts.Tombstoned != 0 {
		t.Fatalf("UpsertPrioritizedCounts = %+v err=%v (existing candidates are refreshed, not skipped)", counts, err)
	}
	if score, _ := mini.ZScore(s.pendingKey(), proxyID(tombURL)); score != future {
		t.Fatalf("tombstoned candidate promoted to %v", score)
	}
	if score, _ := mini.ZScore(s.pendingKey(), proxyID(controlURL)); score != float64(now.UnixMilli()) {
		t.Fatalf("control candidate score = %v, want promotion to now", score)
	}
}

func TestRetestJitterBounds(t *testing.T) {
	base := 10 * time.Hour
	exact := RetestPolicy{}
	if got := exact.jitter(base); got != base {
		t.Fatalf("zero jitter = %s", got)
	}
	low := RetestPolicy{JitterPct: 10, JitterInt64N: func(int64) int64 { return 0 }}
	if got := low.jitter(base); got != 9*time.Hour {
		t.Fatalf("minimum jitter = %s, want 9h", got)
	}
	high := RetestPolicy{JitterPct: 10, JitterInt64N: func(n int64) int64 { return n - 1 }}
	if got := high.jitter(base); got != 11*time.Hour {
		t.Fatalf("maximum jitter = %s, want 11h", got)
	}
	random := RetestPolicy{JitterPct: 50}
	for i := 0; i < 1000; i++ {
		if got := random.jitter(base); got < 5*time.Hour || got > 15*time.Hour {
			t.Fatalf("random jitter %s outside ±50%%", got)
		}
	}
}

func TestRedisCompleteAppliesRetestJitter(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	policy := RetestPolicy{
		ValidatedAfter: 10 * time.Hour, FailedAfter: 20 * time.Hour, MaxConsecutiveFailures: 5,
		JitterPct: 10, JitterInt64N: func(int64) int64 { return 0 },
	}
	if _, err := s.Upsert(ctx, []string{"http://jitter.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	claim, found, err := s.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("ClaimDue: found=%t err=%v", found, err)
	}
	if _, err := s.Complete(ctx, claim, now, policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil {
		t.Fatalf("Complete ok: %v", err)
	}
	if score, _ := mini.ZScore(s.pendingKey(), claim.ID); score != float64(now.Add(9*time.Hour).UnixMilli()) {
		t.Fatalf("validated due = %v, want now+9h", score)
	}
	mini.ZAdd(s.pendingKey(), 0, claim.ID)
	claim, found, err = s.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("re-claim: found=%t err=%v", found, err)
	}
	if _, err := s.Complete(ctx, claim, now, policy, false, 0, time.Millisecond, "timeout", OutcomeMeta{}); err != nil {
		t.Fatalf("Complete failed: %v", err)
	}
	if score, _ := mini.ZScore(s.pendingKey(), claim.ID); score != float64(now.Add(18*time.Hour).UnixMilli()) {
		t.Fatalf("failed due = %v, want now+18h", score)
	}
}

func TestRedisRunOncePermanentMarkerNeverExpires(t *testing.T) {
	mini, s := newTombstoneTestStore(t)
	ctx := context.Background()
	runs := 0
	job := func(context.Context) error { runs++; return nil }
	if won, err := s.RunOncePermanent(ctx, "perm", time.Hour, job); !won || err != nil {
		t.Fatalf("first RunOncePermanent: won=%t err=%v", won, err)
	}
	marker := "test:v1:once:perm"
	if !mini.Exists(marker) || mini.TTL(marker) != 0 {
		t.Fatalf("marker exists=%t ttl=%s, want permanent", mini.Exists(marker), mini.TTL(marker))
	}
	mini.FastForward(30 * 24 * time.Hour)
	if won, err := s.RunOncePermanent(ctx, "perm", time.Hour, job); won || err != nil || runs != 1 {
		t.Fatalf("second RunOncePermanent: won=%t err=%v runs=%d", won, err, runs)
	}

	// A marker left by the former expiring implementation is made permanent.
	legacy := "test:v1:once:legacy"
	if err := mini.Set(legacy, "old"); err != nil {
		t.Fatal(err)
	}
	mini.SetTTL(legacy, time.Hour)
	if won, err := s.RunOncePermanent(ctx, "legacy", time.Hour, job); won || err != nil || runs != 1 {
		t.Fatalf("legacy RunOncePermanent: won=%t err=%v runs=%d", won, err, runs)
	}
	if mini.TTL(legacy) != 0 {
		t.Fatalf("legacy marker TTL = %s, want persisted", mini.TTL(legacy))
	}
}
