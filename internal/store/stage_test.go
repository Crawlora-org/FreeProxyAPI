package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestSourceStageDrainsUniqueURLs(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "stage:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 10, []string{"http://a.example:80", "http://b.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 100, []string{"http://b.example:80", "http://c.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 100, []string{"http://c.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 5, []string{"http://c.example:80"}); err != nil {
		t.Fatal(err)
	}
	count, err := s.SourceStageCount(ctx, key)
	if err != nil || count != 3 {
		t.Fatalf("stage count=%d err=%v, want 3", count, err)
	}
	var got []string
	if err := s.DrainSourceStage(ctx, key, func(batch []string) error {
		got = append(got, batch...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 {
		t.Fatalf("drained %d URLs: %v", len(got), got)
	}
	if got[0] != "http://c.example:80" || got[1] != "http://a.example:80" || got[2] != "http://b.example:80" {
		t.Fatalf("priority order = %v, want high-priority endpoints first", got)
	}
	if added, err := s.Upsert(ctx, got, time.Now()); err != nil || added != 3 {
		t.Fatalf("upsert added=%d err=%v", added, err)
	}
}

func TestSourceStageDrainsPrioritizedBatches(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "stage-priority:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	key, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 20, []string{"http://ordinary.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, key, 10, []string{"http://checked.example:80", "http://checked-two.example:80"}); err != nil {
		t.Fatal(err)
	}
	var priorities []int64
	var batches [][]string
	if err := s.DrainSourceStagePrioritized(ctx, key, func(priority int64, batch []string) error {
		priorities = append(priorities, priority)
		batches = append(batches, append([]string(nil), batch...))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(priorities) != 2 || priorities[0] != 10 || priorities[1] != 20 {
		t.Fatalf("priorities=%v, want [10 20]", priorities)
	}
	if len(batches[0]) != 2 || batches[0][0] != "http://checked-two.example:80" || batches[0][1] != "http://checked.example:80" {
		t.Fatalf("high-priority batch=%v", batches[0])
	}
}

func TestMergeSourceStageMovesEachMemberOnceAtLowestPriority(t *testing.T) {
	mini, s := newTestStore(t, "stage-merge:v1")
	ctx := context.Background()
	previous := sourceMergeBatch
	sourceMergeBatch = 3 // force several merge passes
	t.Cleanup(func() { sourceMergeBatch = previous })

	shared, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	checked, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	// Duplicates inside one feed collapse in its private SET.
	if err := s.AddSourceMembers(ctx, raw, []string{"http://a.example:80", "http://b.example:80", "http://a.example:80", "http://c.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddSourceMembers(ctx, checked, []string{"http://b.example:80", "http://d.example:80"}); err != nil {
		t.Fatal(err)
	}
	if mini.TTL(raw) != sourceStageTTL {
		t.Fatalf("private stage ttl = %v, want %v", mini.TTL(raw), sourceStageTTL)
	}
	if moved, err := s.MergeSourceStage(ctx, raw, shared, 100); err != nil || moved != 3 {
		t.Fatalf("merge raw moved=%d err=%v, want 3", moved, err)
	}
	if moved, err := s.MergeSourceStage(ctx, checked, shared, 10); err != nil || moved != 2 {
		t.Fatalf("merge checked moved=%d err=%v, want 2", moved, err)
	}
	if mini.Exists(raw) || mini.Exists(checked) {
		t.Fatal("merge left members in a private stage")
	}
	if ttl := mini.TTL(shared); ttl != sourceStageTTL {
		t.Fatalf("shared stage ttl = %v, want %v", ttl, sourceStageTTL)
	}
	// A later, higher-priority-number merge must not demote an endpoint.
	if err := s.AddSourceMembers(ctx, raw, []string{"http://d.example:80"}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.MergeSourceStage(ctx, raw, shared, 100); err != nil {
		t.Fatal(err)
	}
	if count, err := s.SourceStageCount(ctx, shared); err != nil || count != 4 {
		t.Fatalf("shared count=%d err=%v, want 4", count, err)
	}
	got := map[string]int64{}
	if err := s.DrainSourceStagePrioritized(ctx, shared, func(priority int64, batch []string) error {
		for _, member := range batch {
			got[member] = priority
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	want := map[string]int64{"http://a.example:80": 100, "http://b.example:80": 10, "http://c.example:80": 100, "http://d.example:80": 10}
	for member, priority := range want {
		if got[member] != priority {
			t.Fatalf("priorities=%v, want %v", got, want)
		}
	}
	if mini.Exists(shared) {
		t.Fatal("drain left the shared stage populated")
	}
}

func TestClaimDueReportsRetest(t *testing.T) {
	_, s := newTestStore(t, "claim-kind:v1")
	ctx := context.Background()
	now := time.UnixMilli(1_700_000_000_000)
	if _, err := s.Upsert(ctx, []string{"http://first.example:80"}, now); err != nil {
		t.Fatal(err)
	}
	claim, found, err := s.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("first claim found=%t err=%v", found, err)
	}
	if claim.Retest {
		t.Fatal("never-checked candidate claimed as a retest")
	}
	outcome, err := s.Complete(ctx, claim, now, RetestPolicy{FailedAfter: -time.Second, MaxConsecutiveFailures: 10}, false, 0, time.Second, "connect_timeout", OutcomeMeta{})
	if err != nil || !outcome.Committed {
		t.Fatalf("complete outcome=%+v err=%v", outcome, err)
	}
	claim, found, err = s.ClaimDue(ctx, "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("retest claim found=%t err=%v", found, err)
	}
	if !claim.Retest {
		t.Fatal("previously checked candidate not reported as a retest")
	}
}
