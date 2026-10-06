package store

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRedisSourceLockFinishRearmsOrReleases(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	const key = "test:v1:source-refresh-lock"

	token, ok, err := store.TrySourceLock(ctx, 30*time.Minute)
	if err != nil || !ok || token == "" {
		t.Fatalf("first TrySourceLock: token=%q ok=%t err=%v", token, ok, err)
	}
	if _, ok, err := store.TrySourceLock(ctx, 30*time.Minute); err != nil || ok {
		t.Fatalf("second TrySourceLock while held: ok=%t err=%v", ok, err)
	}

	if err := store.FinishSourceLock(ctx, "stale-token", 0); err != nil {
		t.Fatalf("FinishSourceLock stale: %v", err)
	}
	if !mini.Exists(key) || mini.TTL(key) != 30*time.Minute {
		t.Fatalf("stale token changed the lock: exists=%t ttl=%v", mini.Exists(key), mini.TTL(key))
	}

	if err := store.FinishSourceLock(ctx, token, 5*time.Minute); err != nil {
		t.Fatalf("FinishSourceLock hold: %v", err)
	}
	if got := mini.TTL(key); got != 5*time.Minute {
		t.Fatalf("lock ttl after finish = %v, want 5m", got)
	}

	if err := store.FinishSourceLock(ctx, token, -time.Second); err != nil {
		t.Fatalf("FinishSourceLock release: %v", err)
	}
	if mini.Exists(key) {
		t.Fatal("non-positive hold did not release the lock")
	}
	if _, ok, err := store.TrySourceLock(ctx, 30*time.Minute); err != nil || !ok {
		t.Fatalf("TrySourceLock after release: ok=%t err=%v", ok, err)
	}
}

func TestRedisSourceLockRenewChecksToken(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	token, ok, err := store.TrySourceLock(ctx, time.Minute)
	if err != nil || !ok {
		t.Fatalf("TrySourceLock: token=%q ok=%t err=%v", token, ok, err)
	}
	if renewed, err := store.RenewSourceLock(ctx, "stale-token", 5*time.Minute); err != nil || renewed {
		t.Fatalf("stale renewal: renewed=%t err=%v", renewed, err)
	}
	if renewed, err := store.RenewSourceLock(ctx, token, 5*time.Minute); err != nil || !renewed {
		t.Fatalf("owner renewal: renewed=%t err=%v", renewed, err)
	}
	if got := mini.TTL("test:v1:source-refresh-lock"); got != 5*time.Minute {
		t.Fatalf("renewed TTL = %v, want 5m", got)
	}
}

func TestRedisPruneFailedValidatedKeepsStableCandidates(t *testing.T) {
	mini := miniredis.RunT(t)
	store, err := NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer store.Close()

	records := map[string][]string{
		"stable-after-blip": {"last_status", "failed", "ok_ratio_pct", "90"},
		"at-threshold":      {"last_status", "failed", "ok_ratio_pct", "80"},
		"unstable":          {"last_status", "failed", "ok_ratio_pct", "66.666666666667"},
		"healthy":           {"last_status", "ok", "ok_ratio_pct", "100"},
		"legacy-failed":     {"last_status", "failed"},
		"legacy-ok":         {"last_status", "ok"},
	}
	for id, fields := range records {
		mini.HSet("test:v1:proxy:"+id, fields...)
		if _, err := mini.SAdd("test:v1:validated", id); err != nil {
			t.Fatalf("SAdd: %v", err)
		}
	}

	pruned, err := store.PruneFailedValidated(context.Background(), 1)
	if err != nil || pruned != 2 {
		t.Fatalf("PruneFailedValidated = %d, %v; want 2", pruned, err)
	}
	members, err := mini.Members("test:v1:validated")
	if err != nil {
		t.Fatalf("Members: %v", err)
	}
	want := []string{"at-threshold", "healthy", "legacy-ok", "stable-after-blip"}
	if len(members) != len(want) {
		t.Fatalf("validated members = %v, want %v", members, want)
	}
	for i := range want {
		if members[i] != want[i] {
			t.Fatalf("validated members = %v, want %v", members, want)
		}
	}
}
