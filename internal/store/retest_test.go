package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestRedisTieredRetestScheduling(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: 48 * time.Hour, MaxConsecutiveFailures: 3}

	if _, err := store.Upsert(ctx, []string{"http://ok.example.net:8080", "http://bad.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	okClaim, found, err := store.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("claim ok candidate: found=%t err=%v", found, err)
	}
	if _, err := store.Complete(ctx, okClaim, now, policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil {
		t.Fatalf("complete ok: %v", err)
	}
	wantOkScore := float64(now.Add(time.Hour).UnixMilli())
	if score, err := mini.ZScore("test:v1:pending", okClaim.ID); err != nil || score != wantOkScore {
		t.Fatalf("validated reschedule score = %v (err=%v), want %v", score, err, wantOkScore)
	}
	if fails := mini.HGet("test:v1:proxy:"+okClaim.ID, "consecutive_failures"); fails != "0" {
		t.Fatalf("consecutive_failures after ok = %q, want 0", fails)
	}

	// Re-probe the already-validated candidate and fail it: validated-set
	// membership must track the LATEST outcome, so a failure removes it. This
	// is a regression guard - an earlier script rewrite dropped exactly this
	// SREM and stale corpses polluted every validated-set slice.
	mini.ZAdd("test:v1:pending", 0, okClaim.ID)
	badClaim, found, err := store.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found || badClaim.ID != okClaim.ID {
		t.Fatalf("re-claim validated candidate: found=%t err=%v claim=%+v", found, err, badClaim)
	}
	if _, err := store.Complete(ctx, badClaim, now, policy, false, 0, time.Millisecond, "timeout", OutcomeMeta{}); err != nil {
		t.Fatalf("complete failed: %v", err)
	}
	wantBadScore := float64(now.Add(48 * time.Hour).UnixMilli())
	if score, err := mini.ZScore("test:v1:pending", badClaim.ID); err != nil || score != wantBadScore {
		t.Fatalf("failed reschedule score = %v (err=%v), want %v", score, err, wantBadScore)
	}
	if member, _ := mini.SIsMember("test:v1:validated", badClaim.ID); member {
		t.Fatal("failed candidate must not be in validated set")
	}
}

func TestRedisEvictionAfterConsecutiveFailures(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour, MaxConsecutiveFailures: 3}

	if _, err := store.Upsert(ctx, []string{"http://corpse.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// Seed: one success puts it in validated; the three failures that follow
	// must strip it back out (and finally evict the hash entirely).
	claim, found, err := store.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("seed claim: found=%t err=%v", found, err)
	}
	if out, err := store.Complete(ctx, claim, now, policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil || !out.Committed {
		t.Fatalf("seed complete: %+v err=%v", out, err)
	}
	for attempt := 1; attempt <= 3; attempt++ {
		// The seeded success rescheduled one validated interval out; each
		// subsequent failed completion reschedules one failed interval out.
		attemptNow := now.Add(time.Duration(attempt) * time.Hour)
		c, found, err := store.ClaimDue(ctx, "w", attemptNow, time.Minute)
		if err != nil || !found {
			t.Fatalf("attempt %d claim: found=%t err=%v", attempt, found, err)
		}
		claim = c
		outcome, err := store.Complete(ctx, claim, attemptNow, policy, false, 0, time.Millisecond, "", OutcomeMeta{})
		if err != nil {
			t.Fatalf("attempt %d complete: %v", attempt, err)
		}
		if outcome.Evicted != (attempt == 3) {
			t.Fatalf("attempt %d: evicted=%t", attempt, outcome.Evicted)
		}
		if attempt < 3 {
			fails := mini.HGet("test:v1:proxy:"+claim.ID, "consecutive_failures")
			if fails != fmt.Sprintf("%d", attempt) {
				t.Fatalf("attempt %d: consecutive_failures=%q", attempt, fails)
			}
		}
	}

	if mini.Exists("test:v1:proxy:" + claim.ID) {
		t.Fatal("evicted candidate hash still exists")
	}
	if member, _ := mini.SIsMember("test:v1:candidates", claim.ID); member {
		t.Fatal("evicted candidate still in candidates set")
	}
	if member, _ := mini.SIsMember("test:v1:validated", claim.ID); member {
		t.Fatal("evicted candidate still in validated set")
	}
	if score, _ := mini.ZScore("test:v1:pending", claim.ID); score != 0 {
		t.Fatal("evicted candidate still scheduled in pending")
	}
	stats, err := store.Stats(ctx)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Candidates != 0 || stats.Pending != 0 || stats.Leased != 0 {
		t.Fatalf("stats after eviction: %+v", stats)
	}
}

func TestRedisDiscardFailedCandidateIsAtomic(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour, DiscardFailedCandidates: true}
	url := "http://discard.example.net:8080"

	if _, err := store.Upsert(ctx, []string{url}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	id := proxyID(url)
	stale, err := store.Complete(ctx, Claim{ID: id, Token: "stale-token"}, now, policy, false, 0, time.Second, "should not delete", OutcomeMeta{})
	if err != nil {
		t.Fatalf("stale Complete: %v", err)
	}
	if stale.Committed || stale.Discarded || !mini.Exists(store.proxyKey(id)) {
		t.Fatalf("stale discard outcome = %+v; candidate exists=%t", stale, mini.Exists(store.proxyKey(id)))
	}
	claim, found, err := store.ClaimDue(ctx, "burst-worker", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("ClaimDue: found=%t err=%v", found, err)
	}
	outcome, err := store.Complete(ctx, claim, now, policy, false, 0, time.Second, "large failure detail", OutcomeMeta{Country: "US"})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !outcome.Committed || !outcome.Discarded || outcome.Evicted {
		t.Fatalf("discard outcome = %+v", outcome)
	}
	if mini.Exists(store.proxyKey(id)) {
		t.Fatal("discarded candidate hash still exists")
	}
	if member, _ := mini.SIsMember(store.candidatesKey(), id); member {
		t.Fatal("discarded candidate still in candidates set")
	}
	if member, _ := mini.SIsMember(store.validatedKey(), id); member {
		t.Fatal("discarded candidate still in validated set")
	}
	if score, _ := mini.ZScore(store.pendingKey(), id); score != 0 {
		t.Fatal("discarded candidate still scheduled in pending")
	}
}

func TestRedisForcePendingDueNow(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	urls := []string{
		"http://future-one.example.net:8080",
		"http://future-two.example.net:8080",
	}
	if _, err := store.Upsert(ctx, urls, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	future := float64(now.Add(2 * time.Hour).UnixMilli())
	for _, url := range urls {
		mini.ZAdd(store.pendingKey(), future, proxyID(url))
	}
	count, err := store.ForcePendingDueNow(ctx)
	if err != nil || count != int64(len(urls)) {
		t.Fatalf("ForcePendingDueNow: count=%d err=%v", count, err)
	}
	dueLimit := float64(time.Now().Add(time.Second).UnixMilli())
	for _, url := range urls {
		if score, err := mini.ZScore(store.pendingKey(), proxyID(url)); err != nil || score > dueLimit {
			t.Fatalf("pending score for %q = %v (err=%v), want due by %v", url, score, err, dueLimit)
		}
	}
}

func TestRedisRunOnceWithErrorReleasesMarker(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	marker := "run-once-error-test"
	if won, err := store.RunOnceWithError(ctx, marker, time.Hour, func(context.Context) error {
		return errors.New("transient failure")
	}); !won || err == nil {
		t.Fatalf("first RunOnceWithError: won=%t err=%v", won, err)
	}
	if won, err := store.RunOnceWithError(ctx, marker, time.Hour, func(context.Context) error {
		return nil
	}); !won || err != nil {
		t.Fatalf("retry RunOnceWithError: won=%t err=%v", won, err)
	}
}

func TestRedisRequeueValidatedPreservesLeaseAndSkipsEvicted(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC)
	policy := RetestPolicy{ValidatedAfter: time.Hour}
	urls := []string{
		"http://active.example.net:8080",
		"http://healthy.example.net:8080",
		"http://evicted.example.net:8080",
	}
	if _, err := store.Upsert(ctx, urls, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	for range urls {
		claim, found, err := store.ClaimDue(ctx, "seed", now, time.Minute)
		if err != nil || !found {
			t.Fatalf("seed ClaimDue: found=%t err=%v", found, err)
		}
		if _, err := store.Complete(ctx, claim, now, policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil {
			t.Fatalf("seed Complete: %v", err)
		}
	}

	activeID := proxyID(urls[0])
	mini.ZAdd(store.pendingKey(), 0, activeID)
	active, found, err := store.ClaimDue(ctx, "active-worker", now, time.Hour)
	if err != nil || !found || active.ID != activeID {
		t.Fatalf("active ClaimDue: claim=%+v found=%t err=%v", active, found, err)
	}
	leaseScore, err := mini.ZScore(store.leasedKey(), active.ID)
	if err != nil {
		t.Fatalf("active lease score: %v", err)
	}

	evictedID := proxyID(urls[2])
	mini.Del(store.proxyKey(evictedID))
	mini.SRem(store.candidatesKey(), evictedID)
	mini.ZRem(store.pendingKey(), evictedID)
	if member, _ := mini.SIsMember(store.validatedKey(), evictedID); !member {
		t.Fatal("test setup lost stale validated member")
	}

	n, err := store.RequeueValidatedNow(ctx, now.Add(10*time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("RequeueValidatedNow: requeued=%d err=%v", n, err)
	}
	if zsetContains(t, mini, store.pendingKey(), active.ID) {
		t.Fatal("requeue put actively leased candidate in pending")
	}
	if got, err := mini.ZScore(store.leasedKey(), active.ID); err != nil || got != leaseScore {
		t.Fatalf("active lease changed: score=%v err=%v", got, err)
	}
	if got := mini.HGet(store.proxyKey(active.ID), "lease_token"); got != active.Token {
		t.Fatalf("active token changed: got %q want %q", got, active.Token)
	}
	if zsetContains(t, mini, store.pendingKey(), evictedID) {
		t.Fatal("requeue resurrected evicted candidate")
	}
	if member, _ := mini.SIsMember(store.validatedKey(), evictedID); member {
		t.Fatal("stale validated member was not cleaned up")
	}
	healthyID := proxyID(urls[1])
	if got, err := mini.ZScore(store.pendingKey(), healthyID); err != nil || got != 0 {
		t.Fatalf("healthy candidate score=%v err=%v, want 0", got, err)
	}

	outcome, err := store.Complete(ctx, active, now.Add(10*time.Minute), policy, true, 204, time.Millisecond, "", OutcomeMeta{})
	if err != nil || !outcome.Committed {
		t.Fatalf("Complete after requeue: outcome=%+v err=%v", outcome, err)
	}
}

func TestRedisSuccessResetsFailureStreak(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Minute, FailedAfter: time.Hour, MaxConsecutiveFailures: 3}

	if _, err := store.Upsert(ctx, []string{"http://flaky.example.net:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	for i := 0; i < 2; i++ {
		claim, found, err := store.ClaimDue(ctx, "w", now.Add(time.Duration(i)*time.Hour), time.Minute)
		if err != nil || !found {
			t.Fatalf("cycle %d claim: found=%t err=%v", i, found, err)
		}
		if _, err := store.Complete(ctx, claim, now.Add(time.Duration(i)*time.Hour), policy, false, 0, time.Millisecond, "", OutcomeMeta{}); err != nil {
			t.Fatalf("cycle %d complete: %v", i, err)
		}
	}
	recovery, found, err := store.ClaimDue(ctx, "w", now.Add(2*time.Hour), time.Minute)
	if err != nil || !found {
		t.Fatalf("recovery claim: found=%t err=%v", found, err)
	}
	if _, err := store.Complete(ctx, recovery, now.Add(2*time.Hour), policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil {
		t.Fatalf("recovery complete: %v", err)
	}
	if fails := mini.HGet("test:v1:proxy:"+recovery.ID, "consecutive_failures"); fails != "0" {
		t.Fatalf("consecutive_failures after recovery = %q, want 0", fails)
	}
}

func TestRedisCompletionScoringAndGeoAnnotations(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Minute, FailedAfter: time.Minute}

	if _, err := store.Upsert(ctx, []string{"http://192.0.2.77:8080"}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}

	// 7 successes then 3 failures -> results10 full window, ratio 70%.
	for i := 0; i < 7; i++ {
		claim, found, err := store.ClaimDue(ctx, "w", now.Add(time.Duration(i)*time.Hour), time.Minute)
		if err != nil || !found {
			t.Fatalf("cycle %d claim: found=%t err=%v", i, found, err)
		}
		outcome, err := store.Complete(ctx, claim, now.Add(time.Duration(i)*time.Hour), policy, true, 204, 100*time.Millisecond, "", OutcomeMeta{Country: "US", ASN: "AS64512"})
		if err != nil || !outcome.Committed {
			t.Fatalf("cycle %d complete: outcome=%+v err=%v", i, outcome, err)
		}
	}
	for i := 7; i < 10; i++ {
		claim, found, err := store.ClaimDue(ctx, "w", now.Add(time.Duration(i)*time.Hour), time.Minute)
		if err != nil || !found {
			t.Fatalf("cycle %d claim: found=%t err=%v", i, found, err)
		}
		if _, err := store.Complete(ctx, claim, now.Add(time.Duration(i)*time.Hour), policy, false, 0, time.Millisecond, "timeout", OutcomeMeta{Country: "US", ASN: "AS64512"}); err != nil {
			t.Fatalf("cycle %d complete: %v", i, err)
		}
	}

	hashKey := "test:v1:proxy:" + hashForTest(t, store, "http://192.0.2.77:8080")
	if got := mini.HGet(hashKey, "results10"); got != "1111111000" {
		t.Fatalf("results10 = %q", got)
	}
	if got := mini.HGet(hashKey, "ok_ratio_pct"); got != "70" {
		t.Fatalf("ok_ratio_pct = %q, want 70", got)
	}
	if got := mini.HGet(hashKey, "country"); got != "US" {
		t.Fatalf("country = %q", got)
	}
	if got := mini.HGet(hashKey, "asn"); got != "AS64512" {
		t.Fatalf("asn = %q", got)
	}
	ewma := mini.HGet(hashKey, "latency_ewma_ms")
	if ewma == "" {
		t.Fatal("latency_ewma_ms missing")
	}

	// An empty country must not wipe the previous annotation.
	claim, found, err := store.ClaimDue(ctx, "w", now.Add(10*time.Hour), time.Minute)
	if err != nil || !found {
		t.Fatalf("post-window claim: found=%t err=%v", found, err)
	}
	if _, err := store.Complete(ctx, claim, now.Add(10*time.Hour), policy, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil {
		t.Fatalf("post-window complete: %v", err)
	}
	if got := mini.HGet(hashKey, "country"); got != "US" {
		t.Fatalf("country after empty lookup = %q, want US preserved", got)
	}

	// A success that leaves the rolling ratio below 80% must not list the
	// candidate. Force membership to inspect the scored slice.
	if member, _ := mini.SIsMember("test:v1:validated", claim.ID); member {
		t.Fatal("success at 70% ratio must not be validated")
	}
	mini.SAdd("test:v1:validated", claim.ID)
	details, err := store.ValidatedDetails(ctx)
	if err != nil || len(details) != 1 {
		t.Fatalf("ValidatedDetails: %+v err=%v", details, err)
	}
	// The post-window success shifted results10 to "1111110001" (70%) and
	// pulled the EWMA toward its 1ms latency.
	if details[0].Country != "US" || details[0].OkRatioPct != 70 || details[0].LatencyMs != 70 {
		t.Fatalf("validated slice = %+v", details[0])
	}
}

// hashForTest recomputes the candidate ID for a canonical URL via the store's
// own ID derivation.
func hashForTest(t *testing.T, s *Redis, rawURL string) string {
	t.Helper()
	return proxyID(rawURL)
}

func TestRedisQueryValidatedFilters(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	now := time.Now()
	policy := RetestPolicy{ValidatedAfter: time.Hour}

	urls := []string{
		"http://fast-us.example.net:8080",    // US, fast
		"http://slow-id.example.net:8080",    // ID, slow
		"socks5://elite-de.example.net:1080", // DE elite
	}
	if _, err := store.Upsert(ctx, urls, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	metaByURL := map[string]struct {
		meta    OutcomeMeta
		latency time.Duration
	}{
		"http://fast-us.example.net:8080":    {OutcomeMeta{Country: "US", ExitCountry: "CA"}, 120 * time.Millisecond},
		"http://slow-id.example.net:8080":    {OutcomeMeta{Country: "ID"}, 1500 * time.Millisecond},
		"socks5://elite-de.example.net:1080": {OutcomeMeta{Country: "DE", Anonymity: "elite"}, 300 * time.Millisecond},
	}
	for range urls {
		claim, found, err := store.ClaimDue(ctx, "w", now, time.Minute)
		if err != nil || !found {
			t.Fatalf("claim: found=%t err=%v", found, err)
		}
		want, ok := metaByURL[claim.URL]
		if !ok {
			t.Fatalf("unexpected claimed URL %q", claim.URL)
		}
		if _, err := store.Complete(ctx, claim, now, policy, true, 204, want.latency, "", want.meta); err != nil {
			t.Fatalf("complete %q: %v", claim.URL, err)
		}
	}

	all, err := store.QueryValidated(ctx, ProxyFilter{})
	if err != nil || len(all) != 3 {
		t.Fatalf("unfiltered query: %d results err=%v", len(all), err)
	}

	us, err := store.QueryValidated(ctx, ProxyFilter{Country: "US"})
	if err != nil || len(us) != 1 || us[0].Country != "US" || us[0].LatencyMs != 120 {
		t.Fatalf("country filter: %+v err=%v", us, err)
	}

	canada, err := store.QueryValidated(ctx, ProxyFilter{ExitCountry: "CA"})
	if err != nil || len(canada) != 1 || canada[0].ExitCountry != "CA" {
		t.Fatalf("exit country filter: %+v err=%v", canada, err)
	}

	fast, err := store.QueryValidated(ctx, ProxyFilter{MaxLatencyMs: 500})
	if err != nil || len(fast) != 2 {
		t.Fatalf("latency filter expected 2, got %d err=%v", len(fast), err)
	}

	eliteEU, err := store.QueryValidated(ctx, ProxyFilter{Anonymity: "elite", Country: "DE"})
	if err != nil || len(eliteEU) != 1 || eliteEU[0].Scheme != "socks5" || eliteEU[0].URL == "" {
		t.Fatalf("combined filter: %+v err=%v", eliteEU, err)
	}

	reliable, err := store.QueryValidated(ctx, ProxyFilter{MinRatioPct: 80})
	if err != nil || len(reliable) != 3 {
		t.Fatalf("ratio filter expected 3, got %d err=%v", len(reliable), err)
	}

	limited, err := store.QueryValidated(ctx, ProxyFilter{Limit: 1})
	if err != nil || len(limited) != 1 {
		t.Fatalf("limit: got %d err=%v", len(limited), err)
	}
}

type queryCommandAudit struct {
	smembers      int
	sscan         int
	hmget         int
	duplicateScan bool
}

func (h *queryCommandAudit) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *queryCommandAudit) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		h.observe(cmd)
		if err == nil && h.duplicateScan && cmd.Name() == "sscan" {
			if scan, ok := cmd.(*redis.ScanCmd); ok {
				page, cursor := scan.Val()
				if len(page) > 0 {
					scan.SetVal(append(page, page[0]), cursor)
				}
			}
		}
		return err
	}
}

func (h *queryCommandAudit) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		for _, cmd := range cmds {
			h.observe(cmd)
		}
		return err
	}
}

func (h *queryCommandAudit) observe(cmd redis.Cmder) {
	switch cmd.Name() {
	case "smembers":
		h.smembers++
	case "sscan":
		h.sscan++
	case "hmget":
		h.hmget++
	}
}

func (h *queryCommandAudit) reset() {
	h.smembers = 0
	h.sscan = 0
	h.hmget = 0
	h.duplicateScan = false
}

func TestRedisQueryValidatedPositiveLimitUsesBoundedScan(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	const total = 601
	for i := 0; i < total; i++ {
		rawURL := fmt.Sprintf("http://query-%03d.example.net:8080", i)
		id := proxyID(rawURL)
		mini.SAdd(store.candidatesKey(), id)
		mini.SAdd(store.validatedKey(), id)
		country := ""
		if i%100 == 0 {
			country = "US"
		}
		mini.HSet(store.proxyKey(id),
			"url", rawURL,
			"country", country,
			"latency_ewma_ms", "100",
			"ok_ratio_pct", "100",
			"last_checked_at_ms", "1")
	}

	// Limit zero retains the documented unlimited behavior.
	all, err := store.QueryValidated(context.Background(), ProxyFilter{})
	if err != nil || len(all) != total {
		t.Fatalf("unlimited query: got=%d err=%v", len(all), err)
	}

	audit := &queryCommandAudit{}
	store.client.AddHook(audit)
	one, err := store.QueryValidated(context.Background(), ProxyFilter{Limit: 1})
	if err != nil || len(one) != 1 {
		t.Fatalf("limited query: got=%d err=%v", len(one), err)
	}
	if audit.smembers != 0 || audit.sscan == 0 {
		t.Fatalf("positive limit commands: SMEMBERS=%d SSCAN=%d", audit.smembers, audit.sscan)
	}
	if audit.hmget > 1 {
		t.Fatalf("limit=1 read %d hashes from %d validated members", audit.hmget, total)
	}

	audit.reset()
	filtered, err := store.QueryValidated(context.Background(), ProxyFilter{Country: "US", Limit: 3})
	if err != nil || len(filtered) != 3 {
		t.Fatalf("filtered scan: got=%d err=%v", len(filtered), err)
	}
	for _, result := range filtered {
		if result.Country != "US" {
			t.Fatalf("filtered scan returned %+v", result)
		}
	}
	if audit.smembers != 0 || audit.sscan == 0 || audit.hmget > total {
		t.Fatalf("filtered commands: SMEMBERS=%d SSCAN=%d HMGET=%d", audit.smembers, audit.sscan, audit.hmget)
	}

	// Redis documents that SSCAN can repeat members. Force one duplicate per
	// page and verify the response still contains unique candidates.
	audit.reset()
	audit.duplicateScan = true
	page, err := store.QueryValidated(context.Background(), ProxyFilter{Limit: 200})
	if err != nil || len(page) != 200 {
		t.Fatalf("duplicate scan query: got=%d err=%v", len(page), err)
	}
	seen := make(map[string]struct{}, len(page))
	for _, result := range page {
		if _, duplicate := seen[result.URL]; duplicate {
			t.Fatalf("duplicate SSCAN result %q", result.URL)
		}
		seen[result.URL] = struct{}{}
	}

	// A huge positive value controls only the output count; it must not be
	// trusted as an allocation size when the actual inventory is small.
	audit.reset()
	huge, err := store.QueryValidated(context.Background(), ProxyFilter{Limit: math.MaxInt})
	if err != nil || len(huge) != total {
		t.Fatalf("huge limit query: got=%d err=%v", len(huge), err)
	}
	if audit.smembers != 0 || audit.sscan == 0 {
		t.Fatalf("huge positive limit commands: SMEMBERS=%d SSCAN=%d", audit.smembers, audit.sscan)
	}
}
