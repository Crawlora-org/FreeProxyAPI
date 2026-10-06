package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newPermitLeaderStore(t *testing.T) (*miniredis.Miniredis, *Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return mini, s
}

func TestReservePermitsNeverExceedsLimitUnderConcurrency(t *testing.T) {
	mini, s := newPermitLeaderStore(t)
	now := time.Date(2026, 9, 14, 10, 30, 15, 0, time.UTC)
	const limit = 1000
	var mu sync.Mutex
	total := 0
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				granted, err := s.ReservePermits(context.Background(), now, limit, 7)
				if err != nil {
					t.Errorf("ReservePermits: %v", err)
					return
				}
				if granted < 0 || granted > 7 {
					t.Errorf("granted %d outside [0,7]", granted)
					return
				}
				if granted == 0 {
					return
				}
				mu.Lock()
				total += granted
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if total != limit {
		t.Fatalf("granted %d permits, want exactly %d", total, limit)
	}
	key := "test:v1:probe-budget:202609141030"
	if got, _ := mini.Get(key); got != "1000" {
		t.Fatalf("window counter = %q, want 1000", got)
	}
	if ttl := mini.TTL(key); ttl <= 0 || ttl > 2*time.Minute {
		t.Fatalf("window TTL = %v", ttl)
	}
}

func TestReservePermitsCapsAtRemainingAndRepairsExpiry(t *testing.T) {
	mini, s := newPermitLeaderStore(t)
	ctx := context.Background()
	now := time.Date(2026, 9, 14, 10, 31, 0, 0, time.UTC)
	key := "test:v1:probe-budget:202609141031"
	// A counter left without expiry (older writer) is capped and repaired.
	if err := mini.Set(key, "95"); err != nil {
		t.Fatal(err)
	}
	if granted, err := s.ReservePermits(ctx, now, 100, 10); err != nil || granted != 5 {
		t.Fatalf("ReservePermits near limit: granted=%d err=%v", granted, err)
	}
	if ttl := mini.TTL(key); ttl <= 0 {
		t.Fatalf("expiry not repaired: ttl=%v", ttl)
	}
	if granted, err := s.ReservePermits(ctx, now, 100, 10); err != nil || granted != 0 {
		t.Fatalf("ReservePermits at limit: granted=%d err=%v", granted, err)
	}
	// TakePermit denials over-increment the shared counter; reservations
	// must still grant nothing rather than a negative count.
	if allowed, err := s.TakePermit(ctx, now, 100); err != nil || allowed {
		t.Fatalf("TakePermit at limit: allowed=%t err=%v", allowed, err)
	}
	if granted, err := s.ReservePermits(ctx, now, 100, 10); err != nil || granted != 0 {
		t.Fatalf("ReservePermits over limit: granted=%d err=%v", granted, err)
	}
	if got, _ := mini.Get(key); got != "101" {
		t.Fatalf("counter = %q, want 101", got)
	}
	// The next window is independent.
	if granted, err := s.ReservePermits(ctx, now.Add(time.Minute), 100, 10); err != nil || granted != 10 {
		t.Fatalf("ReservePermits next window: granted=%d err=%v", granted, err)
	}
	if _, err := s.ReservePermits(ctx, now, 0, 10); err == nil {
		t.Fatal("non-positive limit should fail")
	}
}

func TestStatsLeaderLeaseAndAggregateFailover(t *testing.T) {
	mini, s := newPermitLeaderStore(t)
	ctx := context.Background()
	ttl := 90 * time.Second
	if held, err := s.AcquireStatsLeader(ctx, "a", ttl); err != nil || !held {
		t.Fatalf("a acquire: held=%t err=%v", held, err)
	}
	if held, err := s.AcquireStatsLeader(ctx, "b", ttl); err != nil || held {
		t.Fatalf("b should lose while a holds: held=%t err=%v", held, err)
	}
	mini.FastForward(60 * time.Second)
	if held, err := s.AcquireStatsLeader(ctx, "a", ttl); err != nil || !held {
		t.Fatalf("a renew: held=%t err=%v", held, err)
	}
	if got := mini.TTL("test:v1:stats-leader"); got != ttl {
		t.Fatalf("renewed TTL = %v, want %v", got, ttl)
	}
	if written, err := s.PublishValidatedAggregate(ctx, "b", []byte(`{"x":1}`), 5*time.Minute); err != nil || written {
		t.Fatalf("non-holder publish: written=%t err=%v", written, err)
	}
	if written, err := s.PublishValidatedAggregate(ctx, "a", []byte(`{"x":2}`), 5*time.Minute); err != nil || !written {
		t.Fatalf("holder publish: written=%t err=%v", written, err)
	}
	if payload, found, err := s.ValidatedAggregate(ctx); err != nil || !found || string(payload) != `{"x":2}` {
		t.Fatalf("aggregate read: payload=%s found=%t err=%v", payload, found, err)
	}

	// a stops renewing: the lease expires and b takes over.
	mini.FastForward(91 * time.Second)
	if held, err := s.AcquireStatsLeader(ctx, "b", ttl); err != nil || !held {
		t.Fatalf("b failover: held=%t err=%v", held, err)
	}
	if held, err := s.AcquireStatsLeader(ctx, "a", ttl); err != nil || held {
		t.Fatalf("a after failover: held=%t err=%v", held, err)
	}
	if written, err := s.PublishValidatedAggregate(ctx, "a", []byte(`{"x":3}`), 5*time.Minute); err != nil || written {
		t.Fatalf("deposed leader publish: written=%t err=%v", written, err)
	}

	// A graceful release hands over immediately; a stale token cannot release.
	if err := s.ReleaseStatsLeader(ctx, "a"); err != nil {
		t.Fatal(err)
	}
	if held, _ := s.AcquireStatsLeader(ctx, "a", ttl); held {
		t.Fatal("stale token released the current lease")
	}
	if err := s.ReleaseStatsLeader(ctx, "b"); err != nil {
		t.Fatal(err)
	}
	if held, err := s.AcquireStatsLeader(ctx, "a", ttl); err != nil || !held {
		t.Fatalf("acquire after release: held=%t err=%v", held, err)
	}

	mini.FastForward(5 * time.Minute)
	if _, found, err := s.ValidatedAggregate(ctx); err != nil || found {
		t.Fatalf("aggregate should expire: found=%t err=%v", found, err)
	}
}
