package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestStore(t *testing.T, namespace string) (*miniredis.Miniredis, *Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", namespace)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return mini, s
}

func TestBackfillCountriesSkipsMissingRecords(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	mini.HSet(s.proxyKey("present"), "url", "http://present.example:80")
	err := s.BackfillCountries(context.Background(), [][3]string{
		{"present", "US", "AS1"},
		{"evicted", "DE", "AS2"},
	})
	if err != nil {
		t.Fatalf("BackfillCountries: %v", err)
	}
	if got := mini.HGet(s.proxyKey("present"), "country"); got != "US" {
		t.Fatalf("present country = %q", got)
	}
	if got := mini.HGet(s.proxyKey("present"), "asn"); got != "AS1" {
		t.Fatalf("present asn = %q", got)
	}
	if mini.Exists(s.proxyKey("evicted")) {
		t.Fatal("backfill recreated an evicted candidate hash")
	}
}

func TestSourceStageTTLArmedAndRefreshedDuringDrain(t *testing.T) {
	mini, s := newTestStore(t, "stage-ttl:v1")
	ctx := context.Background()
	previous := sourceStageDrainPage
	sourceStageDrainPage = 2
	t.Cleanup(func() { sourceStageDrainPage = previous })

	key, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	var urls []string
	for i := 0; i < 7; i++ {
		urls = append(urls, fmt.Sprintf("http://stage-%d.example:80", i))
	}
	if err := s.StageSourceURLs(ctx, key, 1, urls); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{key} {
		if ttl := mini.TTL(k); ttl != sourceStageTTL {
			t.Fatalf("stage key %q ttl = %v, want %v", k, ttl, sourceStageTTL)
		}
	}

	// Each batch takes 20 minutes; four passes total 80 minutes, far longer
	// than the 30 minute staging TTL.
	var drained []string
	if err := s.DrainSourceStage(ctx, key, func(batch []string) error {
		drained = append(drained, batch...)
		mini.FastForward(20 * time.Minute)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(drained) != len(urls) {
		t.Fatalf("drained %d of %d staged URLs: %v", len(drained), len(urls), drained)
	}
}

func TestRunOnceWithErrorRenewsLockWhileRunning(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	lockKey := "test:v1:once:renew:lock"
	const ttl = 600 * time.Millisecond
	won, err := s.RunOnceWithError(context.Background(), "renew", ttl, func(ctx context.Context) error {
		for i := 0; i < 3; i++ {
			// At least one renewal tick (ttl/3) lands in each wait, so the
			// lock survives a cumulative fast-forward well beyond ttl.
			time.Sleep(450 * time.Millisecond)
			mini.FastForward(500 * time.Millisecond)
			if !mini.Exists(lockKey) {
				return fmt.Errorf("lock expired during iteration %d", i)
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
		}
		return nil
	})
	if !won || err != nil {
		t.Fatalf("RunOnceWithError: won=%t err=%v", won, err)
	}
	if !mini.Exists("test:v1:once:renew") {
		t.Fatal("completion marker missing")
	}
	if mini.Exists(lockKey) {
		t.Fatal("lock not released after completion")
	}
}

func TestRunOnceWithErrorCancelsJobWhenLockLost(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	lockKey := "test:v1:once:lost:lock"
	won, err := s.RunOnceWithError(context.Background(), "lost", 300*time.Millisecond, func(ctx context.Context) error {
		if err := mini.Set(lockKey, "other-owner"); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
			return errors.New("job context was not cancelled")
		}
	})
	if !won || !errors.Is(err, context.Canceled) || !errors.Is(err, ErrOnceLockLost) {
		t.Fatalf("RunOnceWithError: won=%t err=%v", won, err)
	}
	if mini.Exists("test:v1:once:lost") {
		t.Fatal("completion marker written after lock loss")
	}
	if got, _ := mini.Get(lockKey); got != "other-owner" {
		t.Fatalf("new owner's lock was modified: %q", got)
	}
}

// zscanEvictHook removes a member from pending right after a ZSCAN page is
// read, simulating a claim racing with ForcePendingDueNow.
type zscanEvictHook struct {
	mini   *miniredis.Miniredis
	key    string
	member string
}

func (h *zscanEvictHook) DialHook(next redis.DialHook) redis.DialHook { return next }
func (h *zscanEvictHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() == "zscan" {
			_, _ = h.mini.ZRem(h.key, h.member)
		}
		return err
	}
}
func (h *zscanEvictHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func TestForcePendingDueNowDoesNotReaddClaimedMembers(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	mini.ZAdd(s.pendingKey(), 1e15, "kept")
	mini.ZAdd(s.pendingKey(), 1e15, "claimed")
	s.client.AddHook(&zscanEvictHook{mini: mini, key: s.pendingKey(), member: "claimed"})
	if _, err := s.ForcePendingDueNow(context.Background()); err != nil {
		t.Fatalf("ForcePendingDueNow: %v", err)
	}
	if zsetContains(t, mini, s.pendingKey(), "claimed") {
		t.Fatal("claimed member was re-added to pending")
	}
	if score, err := mini.ZScore(s.pendingKey(), "kept"); err != nil || score >= 1e15 {
		t.Fatalf("kept member score=%v err=%v", score, err)
	}
}

func TestTakePermitArmsAndRepairsExpiry(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	now := time.Date(2026, time.August, 23, 12, 0, 30, 0, time.UTC)
	key := "test:v1:probe-budget:202608231200"
	if ok, err := s.TakePermit(context.Background(), now, 5); err != nil || !ok {
		t.Fatalf("TakePermit: ok=%t err=%v", ok, err)
	}
	if ttl := mini.TTL(key); ttl != 2*time.Minute {
		t.Fatalf("budget ttl = %v", ttl)
	}
	// A counter left without TTL (e.g. crash between INCR and EXPIRE in the
	// old implementation) is repaired on the next permit.
	if err := s.client.Persist(context.Background(), key).Err(); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.TakePermit(context.Background(), now, 5); err != nil || !ok {
		t.Fatalf("TakePermit: ok=%t err=%v", ok, err)
	}
	if ttl := mini.TTL(key); ttl != 2*time.Minute {
		t.Fatalf("repaired budget ttl = %v", ttl)
	}
}

func TestValidatedDetailsSkipsDeletedRecords(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	mini.HSet(s.proxyKey("alive"), "country", "US", "ok_ratio_pct", "100")
	mini.SAdd(s.validatedKey(), "alive", "ghost")
	details, err := s.ValidatedDetails(context.Background())
	if err != nil {
		t.Fatalf("ValidatedDetails: %v", err)
	}
	if len(details) != 1 || details[0].Country != "US" {
		t.Fatalf("details = %+v, want only the live record", details)
	}
}

func TestValidatedWholeSetReadsUseSScan(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	const total = 2503
	for i := 0; i < total; i++ {
		id := fmt.Sprintf("id-%04d", i)
		mini.SAdd(s.validatedKey(), id)
		mini.SAdd(s.candidatesKey(), id)
		mini.HSet(s.proxyKey(id), "url", "http://"+id+".example:80", "ok_ratio_pct", "100", "last_status", "ok")
	}
	audit := &queryCommandAudit{duplicateScan: true}
	s.client.AddHook(audit)
	ctx := context.Background()

	members, err := s.ValidatedMembers(ctx)
	if err != nil || len(members) != total {
		t.Fatalf("ValidatedMembers: got=%d err=%v", len(members), err)
	}
	details, err := s.ValidatedDetails(ctx)
	if err != nil || len(details) != total {
		t.Fatalf("ValidatedDetails: got=%d err=%v", len(details), err)
	}
	all, err := s.QueryValidated(ctx, ProxyFilter{})
	if err != nil || len(all) != total {
		t.Fatalf("QueryValidated: got=%d err=%v", len(all), err)
	}
	if pruned, err := s.PruneFailedValidated(ctx, 1); err != nil || pruned != 0 {
		t.Fatalf("PruneFailedValidated: pruned=%d err=%v", pruned, err)
	}
	requeued, err := s.RequeueValidatedNow(ctx, time.Now())
	if err != nil || requeued != total {
		t.Fatalf("RequeueValidatedNow: got=%d err=%v", requeued, err)
	}
	if audit.smembers != 0 || audit.sscan < 5 {
		t.Fatalf("whole-set reads: SMEMBERS=%d SSCAN=%d", audit.smembers, audit.sscan)
	}
}
