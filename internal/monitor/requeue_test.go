package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func newRequeueTestRunner(t *testing.T, enabled bool) (*miniredis.Miniredis, *Runner) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "requeue-test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	return mini, &Runner{
		store:           redisStore,
		metrics:         newMetrics(),
		config:          Config{RequeuePendingOnStart: enabled},
		nowFunc:         time.Now,
		scheduleChanged: newWakeNotifier(),
	}
}

func TestRequeuePendingOnStartDisabledDoesNothing(t *testing.T) {
	mini, runner := newRequeueTestRunner(t, false)
	future := float64(time.Now().Add(time.Hour).UnixMilli())
	mini.ZAdd("requeue-test:v1:pending", future, "candidate")
	if err := runner.requeuePendingOnStart(context.Background()); err != nil {
		t.Fatalf("requeuePendingOnStart: %v", err)
	}
	if score, _ := mini.ZScore("requeue-test:v1:pending", "candidate"); score != future {
		t.Fatalf("disabled requeue changed score to %v", score)
	}
	if mini.Exists("requeue-test:v1:once:" + pendingRequeueMarker) {
		t.Fatal("disabled requeue wrote a marker")
	}
}

func TestRequeuePendingOnStartRunsOnceForever(t *testing.T) {
	mini, runner := newRequeueTestRunner(t, true)
	ctx := context.Background()
	pending := "requeue-test:v1:pending"
	future := float64(time.Now().Add(time.Hour).UnixMilli())
	mini.ZAdd(pending, future, "candidate")
	if err := runner.requeuePendingOnStart(ctx); err != nil {
		t.Fatalf("first requeue: %v", err)
	}
	if score, _ := mini.ZScore(pending, "candidate"); score >= future {
		t.Fatalf("first requeue left score %v", score)
	}
	marker := "requeue-test:v1:once:" + pendingRequeueMarker
	if !mini.Exists(marker) || mini.TTL(marker) != 0 {
		t.Fatalf("marker exists=%t ttl=%s, want permanent", mini.Exists(marker), mini.TTL(marker))
	}

	// A later deploy, well past the former 24h marker lifetime, must not
	// force the pending set due again.
	mini.FastForward(48 * time.Hour)
	mini.ZAdd(pending, future, "candidate")
	if err := runner.requeuePendingOnStart(ctx); err != nil {
		t.Fatalf("second requeue: %v", err)
	}
	if score, _ := mini.ZScore(pending, "candidate"); score != future {
		t.Fatalf("second requeue changed score to %v", score)
	}
}
