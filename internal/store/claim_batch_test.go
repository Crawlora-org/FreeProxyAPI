package store

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newBatchTestStore(t *testing.T, prefix string) (*miniredis.Miniredis, *Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return mini, s
}

func upsertN(t *testing.T, s *Redis, n int, due time.Time) []string {
	t.Helper()
	urls := make([]string, 0, n)
	for i := 0; i < n; i++ {
		urls = append(urls, fmt.Sprintf("http://batch%03d.example.net:8080", i))
	}
	if _, err := s.Upsert(context.Background(), urls, due); err != nil {
		t.Fatal(err)
	}
	return urls
}

func TestClaimDueBatchLeasesDistinctDueCandidates(t *testing.T) {
	_, s := newBatchTestStore(t, "batch:v1")
	ctx := context.Background()
	now := time.Now()
	upsertN(t, s, 10, now)

	first, err := s.ClaimDueBatch(ctx, "w1", now, time.Minute, 4)
	if err != nil || len(first) != 4 {
		t.Fatalf("first batch: %d claims, err=%v, want 4", len(first), err)
	}
	seenID, seenToken := map[string]bool{}, map[string]bool{}
	for _, c := range first {
		if c.ID == "" || c.URL == "" || c.Token == "" {
			t.Fatalf("incomplete claim %+v", c)
		}
		if seenID[c.ID] || seenToken[c.Token] {
			t.Fatalf("duplicate id or token in one batch: %+v", c)
		}
		seenID[c.ID], seenToken[c.Token] = true, true
	}
	stats, err := s.Stats(ctx)
	if err != nil || stats.Leased != 4 || stats.Pending != 6 {
		t.Fatalf("after first batch: leased=%d pending=%d err=%v, want 4 and 6", stats.Leased, stats.Pending, err)
	}

	// Fewer due than asked for: the rest, and then nothing.
	second, err := s.ClaimDueBatch(ctx, "w2", now, time.Minute, 10)
	if err != nil || len(second) != 6 {
		t.Fatalf("second batch: %d claims, err=%v, want the remaining 6", len(second), err)
	}
	for _, c := range second {
		if seenID[c.ID] {
			t.Fatalf("candidate %s was claimed twice", c.ID)
		}
	}
	if third, err := s.ClaimDueBatch(ctx, "w3", now, time.Minute, 10); err != nil || len(third) != 0 {
		t.Fatalf("third batch: %d claims, err=%v, want none", len(third), err)
	}
}

func TestClaimDueBatchOnlyClaimsDueCandidates(t *testing.T) {
	_, s := newBatchTestStore(t, "batch-due:v1")
	ctx := context.Background()
	now := time.Now()
	if _, err := s.Upsert(ctx, []string{"http://due1.example.net:8080", "http://due2.example.net:8080"}, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Upsert(ctx, []string{"http://later.example.net:8080"}, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	claims, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, 10)
	if err != nil || len(claims) != 2 {
		t.Fatalf("claimed %d, err=%v, want only the 2 due candidates", len(claims), err)
	}
	for _, c := range claims {
		if c.URL == "http://later.example.net:8080" {
			t.Fatalf("claimed a candidate that is not due yet: %+v", c)
		}
	}
}

// Each token in the batch is a real lease: Complete accepts it, and rejects a
// token from another claim.
func TestClaimDueBatchTokensAreValidLeases(t *testing.T) {
	mini, s := newBatchTestStore(t, "batch-tok:v1")
	ctx := context.Background()
	now := time.Now()
	upsertN(t, s, 3, now)
	claims, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, 3)
	if err != nil || len(claims) != 3 {
		t.Fatalf("claimed %d, err=%v", len(claims), err)
	}
	for _, c := range claims {
		key := "batch-tok:v1:proxy:" + c.ID
		if got := mini.HGet(key, "lease_token"); got != c.Token {
			t.Fatalf("stored lease token %q, claim holds %q", got, c.Token)
		}
		if got := mini.HGet(key, "lease_owner"); got != "w" {
			t.Fatalf("lease owner %q, want w", got)
		}
		wantExpiry := strconv.FormatInt(now.Add(time.Minute).UnixMilli(), 10)
		if got := mini.HGet(key, "lease_expires_at_ms"); got != wantExpiry {
			t.Fatalf("lease expiry %q, want %q", got, wantExpiry)
		}
	}
	wrong := claims[0]
	wrong.Token = claims[1].Token
	if out, err := s.Complete(ctx, wrong, now, RetestPolicy{ValidatedAfter: time.Hour}, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil || out.Committed {
		t.Fatalf("completed with another claim's token: out=%+v err=%v", out, err)
	}
	for _, c := range claims {
		if out, err := s.Complete(ctx, c, now, RetestPolicy{ValidatedAfter: time.Hour}, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil || !out.Committed {
			t.Fatalf("Complete(%s): out=%+v err=%v", c.ID, out, err)
		}
	}
}

func TestClaimDueBatchReportsRetests(t *testing.T) {
	_, s := newBatchTestStore(t, "batch-retest:v1")
	ctx := context.Background()
	now := time.Now()
	upsertN(t, s, 1, now)
	first, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, 1)
	if err != nil || len(first) != 1 || first[0].Retest {
		t.Fatalf("first claim: %+v err=%v, want a first probe", first, err)
	}
	if out, err := s.Complete(ctx, first[0], now, RetestPolicy{ValidatedAfter: time.Minute}, true, 204, time.Millisecond, "", OutcomeMeta{}); err != nil || !out.Committed {
		t.Fatalf("Complete: %+v err=%v", out, err)
	}
	again, err := s.ClaimDueBatch(ctx, "w", now.Add(2*time.Minute), time.Minute, 1)
	if err != nil || len(again) != 1 || !again[0].Retest {
		t.Fatalf("second claim: %+v err=%v, want Retest=true", again, err)
	}
}

// Pending entries with no hash, or already leased, are dropped without being
// claimed and without stopping the batch from filling.
func TestClaimDueBatchSkipsStaleEntriesAndStillFills(t *testing.T) {
	mini, s := newBatchTestStore(t, "batch-stale:v1")
	ctx := context.Background()
	now := time.Now()
	for i := 0; i < 5; i++ {
		if _, err := mini.ZAdd("batch-stale:v1:pending", float64(now.Add(-time.Hour).UnixMilli()), fmt.Sprintf("ghost%d", i)); err != nil {
			t.Fatal(err)
		}
	}
	upsertN(t, s, 3, now)
	claims, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, 3)
	if err != nil || len(claims) != 3 {
		t.Fatalf("claimed %d, err=%v, want 3 real candidates past the 5 stale entries", len(claims), err)
	}
	if members, _ := mini.ZMembers("batch-stale:v1:pending"); len(members) != 0 {
		t.Fatalf("stale pending entries were not cleaned: %v", members)
	}
}

func TestClaimDueBatchIsBounded(t *testing.T) {
	_, s := newBatchTestStore(t, "batch-max:v1")
	ctx := context.Background()
	now := time.Now()
	upsertN(t, s, MaxClaimBatch+30, now)
	claims, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, 100000)
	if err != nil || len(claims) != MaxClaimBatch {
		t.Fatalf("claimed %d, err=%v, want the cap of %d", len(claims), err, MaxClaimBatch)
	}
	for _, n := range []int{0, -3} {
		if got, err := s.ClaimDueBatch(ctx, "w", now, time.Minute, n); err != nil || len(got) != 0 {
			t.Fatalf("n=%d claimed %d, err=%v, want nothing", n, len(got), err)
		}
	}
}

// Several replicas claiming in batches never lease the same candidate twice.
func TestClaimDueBatchIsExclusiveAcrossConcurrentClaimers(t *testing.T) {
	_, s := newBatchTestStore(t, "batch-conc:v1")
	ctx := context.Background()
	now := time.Now()
	const total = 400
	upsertN(t, s, total, now)

	var (
		mu      sync.Mutex
		claimed = map[string]int{}
		wg      sync.WaitGroup
	)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				claims, err := s.ClaimDueBatch(ctx, fmt.Sprintf("worker-%d", worker), now, time.Minute, 17)
				if err != nil {
					t.Errorf("ClaimDueBatch: %v", err)
					return
				}
				if len(claims) == 0 {
					return
				}
				mu.Lock()
				for _, c := range claims {
					claimed[c.ID]++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	if len(claimed) != total {
		t.Fatalf("claimed %d distinct candidates, want %d", len(claimed), total)
	}
	for id, n := range claimed {
		if n != 1 {
			t.Fatalf("candidate %s was claimed %d times", id, n)
		}
	}
}
