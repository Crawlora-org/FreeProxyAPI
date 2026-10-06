package monitor

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
	"github.com/alicebob/miniredis/v2"
)

const testLockKey = "test:v1:source-refresh-lock"

// trackedLaunch mirrors Run's goBackground so tests can assert that every
// refresh and retry goroutine exits.
func trackedLaunch() (func(func()), func(t *testing.T, timeout time.Duration)) {
	var wg sync.WaitGroup
	launch := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	wait := func(t *testing.T, timeout time.Duration) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(timeout):
			t.Fatal("refresh/retry goroutines did not exit")
		}
	}
	return launch, wait
}

func TestCanceledRefreshRecordsCanceledAndReleasesLockImmediately(t *testing.T) {
	mini := miniredis.RunT(t)
	sourceURL, release := blockingSource(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": [%q],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr(), sourceURL))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.refresh(ctx) }()
	waitFor(t, 5*time.Second, func() bool { return mini.Exists(testLockKey) }, "refresh never acquired the lock")

	// Shutdown arrives mid-fetch; the feed then delivers into a canceled
	// context so staging fails the same way a rollout kill does.
	cancel()
	release()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled refresh returned nil")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled refresh did not return")
	}

	if mini.Exists(testLockKey) {
		t.Fatalf("lock still held after canceled refresh (ttl %v), want immediate release", mini.TTL(testLockKey))
	}
	if got := runner.metrics.sourceRefreshTotal[3].Load(); got != 1 {
		t.Fatalf("canceled refreshes = %d, want 1", got)
	}
	if got := runner.metrics.sourceRefreshTotal[1].Load(); got != 0 {
		t.Fatalf("error refreshes = %d, want 0 for a canceled refresh", got)
	}
	if got := runner.lastRefreshSnapshot().Result; got == "error" {
		t.Fatal("canceled refresh set last refresh result to error")
	}
}

func TestCanceledLockAttemptRecordsCanceled(t *testing.T) {
	mini := miniredis.RunT(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file:///nonexistent/proxies.txt"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr()))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runner.refresh(ctx)
	if err == nil {
		t.Fatal("refresh with canceled context returned nil")
	}
	var lockErr *sourceLockError
	if errors.As(err, &lockErr) {
		t.Fatal("canceled lock attempt must not be retried as a lock error")
	}
	if got := runner.metrics.sourceRefreshTotal[3].Load(); got != 1 {
		t.Fatalf("canceled = %d, want 1", got)
	}
	if got := runner.metrics.sourceRefreshTotal[1].Load(); got != 0 {
		t.Fatalf("error = %d, want 0", got)
	}
}

func TestIncompleteRefreshKeepsFailureHoldAndRecordsError(t *testing.T) {
	mini := miniredis.RunT(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file:///nonexistent/proxies.txt"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr()))
	if err := runner.refresh(context.Background()); err == nil {
		t.Fatal("refresh with no reachable source should fail")
	}
	if ttl := mini.TTL(testLockKey); ttl <= 0 || ttl > sourceRefreshFailureHold {
		t.Fatalf("lock TTL = %v, want the %v failure hold", ttl, sourceRefreshFailureHold)
	}
	if got := runner.metrics.sourceRefreshTotal[1].Load(); got != 1 {
		t.Fatalf("error = %d, want 1", got)
	}
	if got := runner.metrics.sourceRefreshTotal[3].Load(); got != 0 {
		t.Fatalf("canceled = %d, want 0", got)
	}
	if got := runner.lastRefreshSnapshot().Result; got != "error" {
		t.Fatalf("last refresh result = %q, want error", got)
	}
}

func TestLockErrorRetriesWithBackoffUntilRedisRecovers(t *testing.T) {
	mini := miniredis.RunT(t)
	inventory := writeTempFile(t, t.TempDir(), "proxies.txt", "proxy.example.net:8080\n")
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file://%s"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr(), inventory))
	var attemptsMu sync.Mutex
	var attempts []int
	runner.refreshState.delay = func(attempt int) time.Duration {
		attemptsMu.Lock()
		attempts = append(attempts, attempt)
		attemptsMu.Unlock()
		return 10 * time.Millisecond
	}

	mini.SetError("ERR injected outage")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	launch, wait := trackedLaunch()
	if !runner.startRefresh(ctx, launch) {
		t.Fatal("refresh did not start")
	}
	waitFor(t, 5*time.Second, func() bool { return runner.metrics.sourceRefreshTotal[1].Load() >= 3 }, "lock errors were not retried")
	mini.SetError("")
	waitFor(t, 5*time.Second, func() bool { return runner.metrics.sourceRefreshTotal[0].Load() == 1 }, "refresh did not succeed after Redis recovered")

	// A successful refresh ends the retry chain.
	time.Sleep(100 * time.Millisecond)
	runner.refreshState.mu.Lock()
	attempt, pending := runner.refreshState.attempt, runner.refreshState.pending
	runner.refreshState.mu.Unlock()
	if attempt != 0 || pending {
		t.Fatalf("retry state after success: attempt=%d pending=%t", attempt, pending)
	}
	if got := runner.metrics.sourceRefreshTotal[0].Load(); got != 1 {
		t.Fatalf("ok refreshes = %d, want exactly 1", got)
	}
	attemptsMu.Lock()
	for i, a := range attempts {
		if a != i+1 {
			t.Fatalf("retry attempts = %v, want consecutive from 1", attempts)
		}
	}
	attemptsMu.Unlock()
	cancel()
	wait(t, 5*time.Second)
}

func TestLockRetryStopsOnShutdownWithoutLeaks(t *testing.T) {
	mini := miniredis.RunT(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file:///nonexistent/proxies.txt"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr()))
	runner.refreshState.delay = func(int) time.Duration { return time.Hour }
	mini.SetError("ERR injected outage")

	ctx, cancel := context.WithCancel(context.Background())
	launch, wait := trackedLaunch()
	runner.startRefresh(ctx, launch)
	waitFor(t, 5*time.Second, func() bool {
		runner.refreshState.mu.Lock()
		defer runner.refreshState.mu.Unlock()
		return runner.refreshState.pending
	}, "retry was never scheduled")

	// A ticker refresh during the backoff joins the existing chain instead of
	// starting a second one.
	waitFor(t, 5*time.Second, func() bool { return !runner.refreshing.Load() }, "first refresh did not finish")
	runner.startRefresh(ctx, launch)
	waitFor(t, 5*time.Second, func() bool {
		runner.refreshState.mu.Lock()
		defer runner.refreshState.mu.Unlock()
		return runner.refreshState.attempt == 2
	}, "ticker lock error did not advance the attempt")

	cancel()
	wait(t, 5*time.Second)
	runner.refreshState.mu.Lock()
	pending := runner.refreshState.pending
	runner.refreshState.mu.Unlock()
	if pending {
		t.Fatal("retry still pending after shutdown")
	}
}

func TestLockRetryResetsOnContentionOrSuccess(t *testing.T) {
	runner := &Runner{metrics: newMetrics(), scheduleChanged: newWakeNotifier()}
	runner.refreshState.attempt = 3
	runner.scheduleLockRetry(context.Background(), func(func()) { t.Fatal("no retry expected") }, nil)
	if runner.refreshState.attempt != 0 {
		t.Fatalf("attempt = %d after non-lock outcome, want 0", runner.refreshState.attempt)
	}
}

func TestSourceLockRetryDelaySchedule(t *testing.T) {
	want := []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute}
	for i, w := range want {
		if got := sourceLockRetryDelay(i+1, 0.5); got != w {
			t.Fatalf("attempt %d delay = %v, want %v", i+1, got, w)
		}
	}
	for attempt := 1; attempt <= 100; attempt++ {
		for _, jitter := range []float64{0, 0.999} {
			got := sourceLockRetryDelay(attempt, jitter)
			if got <= 0 || got > sourceLockRetryCap {
				t.Fatalf("attempt %d jitter %v delay = %v out of bounds", attempt, jitter, got)
			}
		}
	}
	if got := sourceLockRetryDelay(1, 0); got != 12*time.Second {
		t.Fatalf("min jitter delay = %v, want 12s", got)
	}
}

func TestClassifySourceFetchError(t *testing.T) {
	cases := []struct {
		err  error
		want string
	}{
		{fmt.Errorf("fetch source: %w", &url.Error{Op: "Get", URL: "https://x", Err: context.Canceled}), "canceled"},
		{fmt.Errorf("fetch source: %w", &url.Error{Op: "Get", URL: "https://x", Err: context.DeadlineExceeded}), "timeout"},
		{fmt.Errorf("fetch source: %w", &net.DNSError{Err: "no such host", Name: "x", IsNotFound: true}), "dns"},
		{fmt.Errorf("fetch source: %w", &net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}), "refused"},
		{errors.New("source returned 404 Not Found"), "http_status"},
		{fmt.Errorf("%w: limit is 10 bytes", source.ErrSourceTooLarge), "too_large"},
		{fmt.Errorf("fetch source: %w", x509.UnknownAuthorityError{}), "tls"},
		{fmt.Errorf("decode JSON proxy feed array: %w", errors.New("bad")), "parse"},
		{errors.New("boom"), "other"},
	}
	for _, c := range cases {
		if got := classifySourceFetchError(c.err); got != c.want {
			t.Fatalf("classify(%v) = %q, want %q", c.err, got, c.want)
		}
	}
}

func TestSourceFetchRetryDelay(t *testing.T) {
	if got, retry := sourceFetchRetryDelay(&source.HTTPStatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: 12 * time.Second}, 0.5); !retry || got != 12*time.Second {
		t.Fatalf("429 retry = %s, %t; want 12s, true", got, retry)
	}
	if got, retry := sourceFetchRetryDelay(&source.HTTPStatusError{StatusCode: http.StatusNotFound}, 0.5); retry || got != 0 {
		t.Fatalf("404 retry = %s, %t; want 0s, false", got, retry)
	}
	if got, retry := sourceFetchRetryDelay(context.DeadlineExceeded, 0); !retry || got != 1600*time.Millisecond {
		t.Fatalf("timeout retry = %s, %t; want 1.6s, true", got, retry)
	}
	if got, retry := sourceFetchRetryDelay(context.Canceled, 0.5); retry || got != 0 {
		t.Fatalf("canceled retry = %s, %t; want 0s, false", got, retry)
	}
	if got, retry := sourceFetchRetryDelay(&source.HTTPStatusError{StatusCode: http.StatusTooManyRequests, RetryAfter: sourceFetchRetryAfterCap + time.Second}, 0.5); retry || got != 0 {
		t.Fatalf("long 429 retry = %s, %t; want 0s, false", got, retry)
	}
	pathErr := &os.PathError{Op: "open", Path: "/missing", Err: syscall.ENOENT}
	if got, retry := sourceFetchRetryDelay(pathErr, 0.5); retry || got != 0 {
		t.Fatalf("missing file retry = %s, %t; want 0s, false", got, retry)
	}
}

func TestSourceFailureHostDropsPathAndQuery(t *testing.T) {
	cases := map[string]string{
		"https://User:pw@Raw.GitHubusercontent.com:443/a/b.txt?token=x": "raw.githubusercontent.com",
		"file:///data/proxies.txt":                                      "file",
		"::bad":                                                         "other",
	}
	for raw, want := range cases {
		if got := sourceFailureHost(raw); got != want {
			t.Fatalf("host(%q) = %q, want %q", raw, got, want)
		}
	}
}

func TestSourceFailureMetricLabelsAndHostCap(t *testing.T) {
	m := newMetrics()
	m.RecordSourceFetchFailureBySource("feeds.example.net", "timeout")
	m.RecordSourceFetchFailureBySource("feeds.example.net", "timeout")
	m.RecordSourceFetchFailureBySource("feeds.example.net", "made-up")
	for i := 0; i < sourceFailureHostCap+50; i++ {
		m.RecordSourceFetchFailureBySource(fmt.Sprintf("h%d.example.net", i), "dns")
	}
	if got := len(m.sourceFailures.counts); got != sourceFailureHostCap+1 {
		t.Fatalf("distinct hosts = %d, want cap %d plus other", got, sourceFailureHostCap)
	}
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, line := range []string{
		`freeproxyapi_source_fetch_failures_by_source_total{host="feeds.example.net",reason="timeout"} 2`,
		`freeproxyapi_source_fetch_failures_by_source_total{host="feeds.example.net",reason="other"} 1`,
		// feeds.example.net took one slot, so 51 hosts overflow into other.
		`freeproxyapi_source_fetch_failures_by_source_total{host="other",reason="dns"} 51`,
		`freeproxyapi_source_fetch_failures_by_reason_total{reason="timeout"} 2`,
		fmt.Sprintf(`freeproxyapi_source_fetch_failures_by_reason_total{reason="dns"} %d`, sourceFailureHostCap+50),
		`freeproxyapi_source_fetch_failures_by_reason_total{reason="other"} 1`,
		`freeproxyapi_source_refreshes_total{result="canceled"} 0`,
		"# TYPE freeproxyapi_source_fetch_failures_by_source_total counter",
	} {
		if !strings.Contains(body, line) {
			t.Fatalf("metrics missing %q", line)
		}
	}
}

func TestRefreshSourceRecordsPerSourceFailure(t *testing.T) {
	mini := miniredis.RunT(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file:///nonexistent/proxies.txt"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr()))
	_ = runner.refresh(context.Background())
	if got := runner.metrics.sourceFailures.counts["file"]["other"]; got != 1 {
		t.Fatalf("per-source failures = %v, want file/other=1", runner.metrics.sourceFailures.counts)
	}
}
