package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func TestControlProbeStateMachine(t *testing.T) {
	runner := &Runner{metrics: newMetrics(), config: Config{ControlProbeFailureThreshold: 3}, workAvailable: newWakeNotifier()}
	failure := errors.New("origin 429")
	for i := 1; i <= 2; i++ {
		runner.recordControlResult(failure)
		if !runner.controlHealthy() {
			t.Fatalf("unhealthy after %d failures, threshold is 3", i)
		}
	}
	// A success before the threshold resets the streak.
	runner.recordControlResult(nil)
	runner.recordControlResult(failure)
	runner.recordControlResult(failure)
	if !runner.controlHealthy() {
		t.Fatal("success must reset the consecutive failure count")
	}
	wake := runner.workAvailable.channel()
	runner.recordControlResult(failure)
	if runner.controlHealthy() || runner.accuracy.metrics.controlUnhealthy.Load() != 1 {
		t.Fatal("third consecutive failure must mark control unhealthy")
	}
	runner.recordControlResult(failure)
	if runner.controlHealthy() {
		t.Fatal("stays unhealthy while failing")
	}
	runner.recordControlResult(nil)
	if !runner.controlHealthy() || runner.accuracy.metrics.controlUnhealthy.Load() != 0 {
		t.Fatal("one success must recover")
	}
	select {
	case <-wake:
	default:
		t.Fatal("recovery must wake idle workers")
	}
	if got := runner.accuracy.metrics.controlChecks[0].Load(); got != 6 {
		t.Fatalf("failed checks = %d, want 6", got)
	}
}

func TestWaitForControlHealthyPausesUntilRecovery(t *testing.T) {
	runner := &Runner{metrics: newMetrics(), config: Config{ControlProbeFailureThreshold: 1}}
	runner.accuracy.pausePoll = 2 * time.Millisecond
	runner.recordControlResult(errors.New("down"))
	done := make(chan bool, 1)
	go func() { done <- runner.waitForControlHealthy(context.Background()) }()
	select {
	case <-done:
		t.Fatal("worker resumed while control unhealthy")
	case <-time.After(30 * time.Millisecond):
	}
	runner.recordControlResult(nil)
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("wait reported shutdown after recovery")
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not resume after recovery")
	}

	runner.recordControlResult(errors.New("down"))
	ctx, cancel := context.WithCancel(context.Background())
	go func() { done <- runner.waitForControlHealthy(ctx) }()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("cancelled wait must report false")
		}
	case <-time.After(time.Second):
		t.Fatal("paused worker ignored shutdown")
	}
}

func TestControlProbeLoopDrivesStateAndStopsOnShutdown(t *testing.T) {
	runner := &Runner{metrics: newMetrics(), config: Config{ControlProbeFailureThreshold: 2, ControlProbeInterval: time.Millisecond, RequestTimeout: time.Second}}
	// mode: 0 fail, 1 succeed, 2 block until shutdown.
	var mode atomic.Int64
	runner.accuracy.controlCheckFunc = func(ctx context.Context) error {
		switch mode.Load() {
		case 0:
			return errors.New("egress broken")
		case 1:
			return nil
		default:
			<-ctx.Done()
			return ctx.Err()
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		runner.controlProbeLoop(ctx)
		close(stopped)
	}()
	waitFor(t, 2*time.Second, func() bool { return !runner.controlHealthy() }, "control never became unhealthy")
	mode.Store(1)
	waitFor(t, 2*time.Second, runner.controlHealthy, "control never recovered")

	// A check blocked in flight must not hold up shutdown.
	mode.Store(2)
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("control loop did not stop on shutdown")
	}
	if !runner.controlHealthy() {
		t.Fatal("shutdown cancellation must not count as a failure")
	}
}

func TestWorkerDoesNotCommitFailuresWhileControlUnhealthy(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "control-test:v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	now := time.Now()
	if _, err := redisStore.Upsert(context.Background(), []string{"http://192.0.2.10:8080"}, now.Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	runner := &Runner{
		store:   redisStore,
		metrics: newMetrics(),
		config: Config{
			ProbeTarget: "http://target.invalid/success.txt", RequestTimeout: time.Second,
			LeaseTTL: time.Minute, GlobalRequestsPerMinute: 100, ControlProbeFailureThreshold: 1,
			MaxConsecutiveFailures: 1, FailedRetestInterval: time.Hour,
		},
		nowFunc:         time.Now,
		scheduleChanged: newWakeNotifier(),
		workAvailable:   newWakeNotifier(),
	}
	runner.accuracy.pausePoll = 2 * time.Millisecond
	var probes atomic.Int64
	// The origin breaks while this probe is in flight.
	runner.probeFunc = func(context.Context, string, string, time.Duration) probe.Result {
		probes.Add(1)
		runner.recordControlResult(errors.New("origin down"))
		return probe.Result{Error: "timeout"}
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runner.workerLoop(ctx)
		close(done)
	}()
	waitFor(t, 2*time.Second, func() bool { return runner.accuracy.metrics.suppressedOutcomes.Load() == 1 }, "failed outcome was not suppressed")
	// The released candidate is due again, but the paused worker must not
	// claim it.
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not exit while paused")
	}
	if got := probes.Load(); got != 1 {
		t.Fatalf("probes = %d, want 1 (worker must pause)", got)
	}
	stats, err := redisStore.StatsAt(context.Background(), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if stats.Candidates != 1 || stats.Pending != 1 || stats.Leased != 0 {
		t.Fatalf("candidate must be released, not evicted: %+v", stats)
	}
	for _, key := range mini.Keys() {
		if strings.Contains(key, ":proxy:") {
			if status := mini.HGet(key, "last_status"); status != "" {
				t.Fatalf("suppressed failure was committed: last_status=%q", status)
			}
		}
	}
}

func TestCheckControlResponse(t *testing.T) {
	respond := func(status int, body string) *http.Response {
		rec := httptest.NewRecorder()
		rec.WriteHeader(status)
		_, _ = io.WriteString(rec, body)
		return rec.Result()
	}
	if err := checkControlResponse(respond(200, "success\n"), "standard", "success"); err != nil {
		t.Fatalf("matching body: %v", err)
	}
	if err := checkControlResponse(respond(200, "rate limited"), "standard", "success"); err == nil {
		t.Fatal("body mismatch must fail")
	}
	if err := checkControlResponse(respond(429, "success"), "standard", "success"); err == nil {
		t.Fatal("429 must fail")
	}
	if err := checkControlResponse(respond(200, `{"origin":"1.2.3.4"}`), "echo", "success"); err != nil {
		t.Fatalf("echo mode ignores expected body: %v", err)
	}
}

func TestAccuracyMetricsWrapper(t *testing.T) {
	runner := &Runner{metrics: newMetrics(), config: Config{ControlProbeFailureThreshold: 1}}
	handler := withAccuracyMetrics(runner.metrics.Handler(), &runner.accuracy.metrics)
	get := func(method string) *http.Response {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(method, "/metrics", nil))
		return rec.Result()
	}
	body, _ := io.ReadAll(get(http.MethodGet).Body)
	if !strings.Contains(string(body), "freeproxyapi_claims_total") || !strings.Contains(string(body), "freeproxyapi_control_probe_healthy 1\n") {
		t.Fatalf("exposition missing base or accuracy metrics:\n%s", body)
	}
	runner.recordControlResult(errors.New("down"))
	response := get(http.MethodGet)
	body, _ = io.ReadAll(response.Body)
	if !strings.Contains(string(body), "freeproxyapi_control_probe_healthy 0\n") {
		t.Fatal("gauge must drop to 0 while unhealthy")
	}
	if response.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("Content-Length %s != body %d", response.Header.Get("Content-Length"), len(body))
	}
	head := get(http.MethodHead)
	headBody, _ := io.ReadAll(head.Body)
	if len(headBody) != 0 || head.Header.Get("Content-Length") != strconv.Itoa(len(body)) {
		t.Fatalf("HEAD: body=%d content-length=%s", len(headBody), head.Header.Get("Content-Length"))
	}
	if code := get(http.MethodPost).StatusCode; code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d", code)
	}
}
