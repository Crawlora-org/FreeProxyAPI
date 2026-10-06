package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := listener.Addr().(*net.TCPAddr)
	listener.Close()
	return addr.Port
}

func writeTempFile(t *testing.T, dir, name, body string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func getJSON(t *testing.T, url string, out any) int {
	t.Helper()
	response, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if out != nil && response.StatusCode == http.StatusOK {
		if err := json.Unmarshal(body, out); err != nil {
			t.Fatalf("decode %s: %v (%s)", url, err, body)
		}
	}
	return response.StatusCode
}

func TestRunnerRefreshesInventoryAndServesHealth(t *testing.T) {
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	inventory := writeTempFile(t, dir, "proxies.txt",
		"# synthetic inventory\nproxy1.example.net:8080\nsocks5://proxy2.example.net:1080\nnot-a-proxy\n")
	port := freePort(t)
	configPath := writeTempFile(t, dir, "monitor.json", fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file://%s"],
		"network_validation_enabled": false,
		"listen_addr": "127.0.0.1:%d",
		"fetch_interval": "1h"
	}`, mini.Addr(), inventory, port))

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	runner, err := NewRunner(config, "test-worker")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer runner.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	waitFor(t, 5*time.Second, func() bool {
		response, err := http.Get(base + "/livez")
		if err != nil {
			return false // server not accepting yet
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, "livez never became ready")

	var ready struct {
		Ready bool `json:"ready"`
	}
	if status := getJSON(t, base+"/readyz", &ready); status != http.StatusOK || !ready.Ready {
		t.Fatalf("readyz: status=%d payload=%+v", status, ready)
	}

	var report reportPayload
	waitFor(t, 5*time.Second, func() bool {
		if getJSON(t, base+"/report", &report) != http.StatusOK {
			return false
		}
		return report.Counts.Candidates == 2 && report.Counts.Pending == 2
	}, "report never showed the two accepted candidates")

	var stats struct {
		Status    string           `json:"status"`
		Total     int64            `json:"total"`
		ByCountry map[string]int64 `json:"by_country"`
	}
	if status := getJSON(t, base+"/stats", &stats); status != http.StatusOK || stats.Status != "ok" || stats.Total != 0 {
		t.Fatalf("stats: status=%d payload=%+v", status, stats)
	}
	var echo echoPayload
	if status := getJSON(t, base+"/get?probe=1", &echo); status != http.StatusOK || echo.Origin == "" || echo.URL == "" || echo.Args["probe"][0] != "1" {
		t.Fatalf("echo: status=%d payload=%+v", status, echo)
	}
	var statsEcho echoPayload
	if status := getJSON(t, base+"/stats?echo=1&probe=2", &statsEcho); status != http.StatusOK || statsEcho.Origin == "" || statsEcho.Args["probe"][0] != "2" {
		t.Fatalf("stats echo: status=%d payload=%+v", status, statsEcho)
	}

	proxiesResponse, err := http.Get(base + "/proxies?country=US&min_ratio_pct=80&limit=10")
	if err != nil {
		t.Fatalf("GET /proxies: %v", err)
	}
	proxiesBody, _ := io.ReadAll(proxiesResponse.Body)
	proxiesResponse.Body.Close()
	if proxiesResponse.StatusCode != http.StatusOK {
		t.Fatalf("proxies: status=%d body=%s", proxiesResponse.StatusCode, proxiesBody)
	}
	if got := proxiesResponse.Header.Get("Cache-Control"); got != "public, max-age=30, s-maxage=30" {
		t.Fatalf("proxies Cache-Control = %q", got)
	}
	homeResponse, err := http.Get(base + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	homeBody, _ := io.ReadAll(homeResponse.Body)
	homeResponse.Body.Close()
	if homeResponse.StatusCode != http.StatusOK || !strings.Contains(string(homeBody), "FreeProxyAPI") || strings.Contains(string(homeBody), "googletagmanager") {
		t.Fatalf("homepage: status=%d body=%s", homeResponse.StatusCode, homeBody)
	}
	if got := homeResponse.Header.Get("Cache-Control"); got != "public, max-age=60, s-maxage=60" {
		t.Fatalf("homepage Cache-Control = %q", got)
	}
	for _, path := range []string{"/icon.png", "/favicon.png"} {
		iconResponse, err := http.Get(base + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		iconBody, _ := io.ReadAll(iconResponse.Body)
		iconResponse.Body.Close()
		if iconResponse.StatusCode != http.StatusOK || iconResponse.Header.Get("Content-Type") != "image/png" || len(iconBody) == 0 {
			t.Fatalf("icon %s: status=%d content-type=%q bytes=%d", path, iconResponse.StatusCode, iconResponse.Header.Get("Content-Type"), len(iconBody))
		}
	}
	dashboardResponse, err := http.Get(base + "/dashboard")
	if err != nil {
		t.Fatalf("GET /dashboard: %v", err)
	}
	dashboardBody, _ := io.ReadAll(dashboardResponse.Body)
	dashboardResponse.Body.Close()
	if dashboardResponse.StatusCode != http.StatusOK || !strings.Contains(string(dashboardBody), "Operations dashboard") || !strings.Contains(string(dashboardBody), `href="/favicon.png"`) {
		t.Fatalf("dashboard: status=%d body=%s", dashboardResponse.StatusCode, dashboardBody)
	}
	if got := dashboardResponse.Header.Get("Cache-Control"); got != "public, max-age=60, s-maxage=60" {
		t.Fatalf("dashboard Cache-Control = %q", got)
	}
	dashboardDataResponse, err := http.Get(base + "/dashboard/data")
	if err != nil {
		t.Fatalf("GET /dashboard/data: %v", err)
	}
	dashboardDataBody, _ := io.ReadAll(dashboardDataResponse.Body)
	dashboardDataResponse.Body.Close()
	if dashboardDataResponse.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(dashboardDataBody), "not configured") {
		t.Fatalf("dashboard data without Prometheus: status=%d body=%s", dashboardDataResponse.StatusCode, dashboardDataBody)
	}
	if got := dashboardDataResponse.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("dashboard data Cache-Control = %q", got)
	}

	wantMetrics := []string{
		`freeproxyapi_source_refreshes_total{result="ok"} 1`,
		"freeproxyapi_source_records_added_total 2",
		"freeproxyapi_source_records_rejected_total 1",
		"freeproxyapi_source_unique 2",
		"freeproxyapi_source_records_parsed 2",
		"freeproxyapi_sources_succeeded 1",
	}
	var metricsBody []byte
	// The startup refresh runs beside the main loop, so its final metric
	// updates can land just after the candidates become visible in Redis.
	waitFor(t, 5*time.Second, func() bool {
		metricsResponse, err := http.Get(base + "/metrics")
		if err != nil {
			t.Fatalf("GET metrics: %v", err)
		}
		metricsBody, _ = io.ReadAll(metricsResponse.Body)
		metricsResponse.Body.Close()
		for _, want := range wantMetrics {
			if !strings.Contains(string(metricsBody), want) {
				return false
			}
		}
		return true
	}, "metrics never showed the completed startup refresh")

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestRunnerReleasesLeaseOnShutdown(t *testing.T) {
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	inventory := writeTempFile(t, dir, "proxies.txt", "proxy.example.net:8080\n")
	configPath := writeTempFile(t, dir, "monitor.json", fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file://%s"],
		"probe_target": "https://probe.example.net/healthz",
		"network_validation_enabled": true,
		"listen_addr": "-",
		"workers": 1,
		"lease_ttl": "1h",
		"fetch_interval": "1h"
	}`, mini.Addr(), inventory))

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	runner, err := NewRunner(config, "test-worker")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer runner.Close()
	probeStarted := make(chan struct{})
	shutdownObserved := make(chan struct{})
	runner.probeFunc = func(ctx context.Context, proxyURL, targetURL string, timeout time.Duration) probe.Result {
		close(probeStarted)
		<-ctx.Done()
		close(shutdownObserved)
		return probe.Result{Error: "context canceled"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-probeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("candidate probe never started")
	}

	cancel()
	<-shutdownObserved
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after cancellation")
	}

	stats, err := runner.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("final Stats: %v", err)
	}
	if stats.Pending != 1 || stats.Leased != 0 || stats.Validated != 0 {
		t.Fatalf("inflight claim was not released on shutdown: %+v", stats)
	}
}

func waitFor(t *testing.T, timeout time.Duration, check func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal(message + " (timed out)")
}

func TestNewRunnerDefaultsProbeFunc(t *testing.T) {
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	configPath := writeTempFile(t, dir, "monitor.json", fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file:///data/proxies.txt"],
		"network_validation_enabled": true,
		"probe_target": "https://probe.example.net/healthz",
		"listen_addr": "-"
	}`, mini.Addr()))

	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	runner, err := NewRunner(config, "test-worker")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	defer runner.Close()
	if runner.probeFunc == nil {
		t.Fatal("NewRunner left probeFunc nil; production workers would panic on first claim")
	}
}

func newBudgetTestRunner(t *testing.T, limit int) *Runner {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "budget-test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	return &Runner{
		store:   redisStore,
		metrics: newMetrics(),
		config: Config{
			AnonymityCheckURL:       "https://echo-one.invalid/get,https://echo-two.invalid/get",
			RequestTimeout:          time.Second,
			GlobalRequestsPerMinute: limit,
		},
		nowFunc: func() time.Time { return time.Date(2026, 9, 13, 12, 0, 0, 0, time.UTC) },
	}
}

func TestProxyAnonymityFailoverTakesPermitBeforeEveryRequest(t *testing.T) {
	runner := newBudgetTestRunner(t, 1)
	var calls atomic.Int64
	runner.echoCheckFunc = func(context.Context, string, string, string, time.Duration) probe.AnonymityResult {
		calls.Add(1)
		return probe.AnonymityResult{}
	}

	result := runner.checkProxyAnonymity(context.Background(), "http://192.0.2.10:8080")
	if result.Class != "" {
		t.Fatalf("unexpected classification: %+v", result)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("echo requests = %d, want 1 before second URL is denied", got)
	}
	if got := runner.metrics.budgetDenialsTotal.Load(); got != 1 {
		t.Fatalf("budget denials = %d, want 1", got)
	}
}

func TestDirectEchoFailoverTakesPermitBeforeEveryRequest(t *testing.T) {
	runner := newBudgetTestRunner(t, 1)
	var calls atomic.Int64
	runner.echoRefreshFunc = func(context.Context, string) (string, error) {
		calls.Add(1)
		return "", errors.New("fixture failure")
	}

	if runner.refreshRealIP(context.Background()) {
		t.Fatal("direct echo unexpectedly succeeded")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("direct echo requests = %d, want 1 before second URL is denied", got)
	}
	if got := runner.metrics.budgetDenialsTotal.Load(); got != 1 {
		t.Fatalf("budget denials = %d, want 1", got)
	}
}

// blockingSource returns a file:// feed backed by a named pipe. Opening and
// reading it block (regardless of context cancellation) until release writes
// a record and closes the pipe, so tests control exactly when a refresh can
// finish. The fetcher refuses loopback HTTP, so an httptest server cannot be
// used here.
func blockingSource(t *testing.T) (sourceURL string, release func()) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "blocking.txt")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Skipf("named pipes unavailable: %v", err)
	}
	var once sync.Once
	releaseFn := func() {
		once.Do(func() {
			// A blocking write-only open rendezvouses with the fetcher's read
			// open, so the record is delivered even if release runs before the
			// refresh reaches the pipe; closing the writer then yields EOF. It
			// runs in a goroutine so release never blocks the test itself.
			go func() {
				pipe, err := os.OpenFile(path, os.O_WRONLY, 0)
				if err != nil {
					return
				}
				_, _ = io.WriteString(pipe, "blocked.example.net:8080\n")
				_ = pipe.Close()
			}()
		})
	}
	t.Cleanup(releaseFn)
	return "file://" + path, releaseFn
}

func loadTestRunner(t *testing.T, body string) *Runner {
	t.Helper()
	configPath := writeTempFile(t, t.TempDir(), "monitor.json", body)
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	runner, err := NewRunner(config, "test-worker")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	t.Cleanup(func() { _ = runner.Close() })
	return runner
}

func TestStartRefreshNeverOverlapsOnOneReplica(t *testing.T) {
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

	var wg sync.WaitGroup
	launch := func(fn func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn()
		}()
	}
	ctx := context.Background()
	if !runner.startRefresh(ctx, launch) {
		t.Fatal("first refresh did not start")
	}
	// The first refresh cannot complete until release, so it is still running.
	if runner.startRefresh(ctx, launch) {
		t.Fatal("second refresh started while the first was still running")
	}
	if got := runner.metrics.sourceRefreshTotal[2].Load(); got != 1 {
		t.Fatalf("skipped refreshes = %d, want 1", got)
	}
	release()
	wg.Wait()
	if runner.refreshing.Load() {
		t.Fatal("refresh guard still held after the refresh finished")
	}
	if got := runner.metrics.sourceRefreshTotal[0].Load(); got != 1 {
		t.Fatalf("ok refreshes = %d, want 1", got)
	}
}

func TestRunnerStartsWorkersBeforeStartupRefreshAndDrainsNotReady(t *testing.T) {
	mini := miniredis.RunT(t)
	sourceURL, release := blockingSource(t)
	port := freePort(t)
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": [%q],
		"probe_target": "https://probe.example.net/healthz",
		"network_validation_enabled": true,
		"listen_addr": "127.0.0.1:%d",
		"workers": 1,
		"lease_ttl": "1h",
		"fetch_interval": "1h"
	}`, mini.Addr(), sourceURL, port))
	// A candidate already pending from an earlier refresh must be probed even
	// while the startup refresh is still stuck on a slow feed.
	if _, err := runner.store.UpsertPrioritized(context.Background(), []string{"http://pending.example.net:8080"}, time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("seed pending: %v", err)
	}
	probeStarted := make(chan struct{})
	finishProbe := make(chan struct{})
	var probeOnce sync.Once
	runner.probeFunc = func(ctx context.Context, proxyURL, targetURL string, timeout time.Duration) probe.Result {
		probeOnce.Do(func() { close(probeStarted) })
		<-finishProbe // ignore cancellation so the drain window stays observable
		return probe.Result{Error: "context canceled"}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// The startup refresh is blocked on the pipe until release, so a probe
	// starting here proves workers do not wait for it.
	select {
	case <-probeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not probe pending work while the startup refresh was blocked")
	}
	if !runner.refreshing.Load() {
		t.Fatal("startup refresh unexpectedly finished before release")
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	if status := getJSON(t, base+"/readyz", nil); status != http.StatusOK {
		t.Fatalf("readyz before shutdown = %d, want 200", status)
	}

	cancel()
	waitFor(t, 5*time.Second, func() bool {
		return getJSON(t, base+"/readyz", nil) == http.StatusServiceUnavailable
	}, "readyz never reported draining")
	select {
	case err := <-done:
		t.Fatalf("Run returned before its worker finished draining: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(finishProbe)
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after draining")
	}
}

func TestRunnerWaitsForBackgroundJobsBeforeReturning(t *testing.T) {
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	inventory := writeTempFile(t, dir, "proxies.txt", "proxy.example.net:8080\n")
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file://%s"],
		"network_validation_enabled": false,
		"anonymity_check_url": "https://echo.example.net/get",
		"listen_addr": "-",
		"fetch_interval": "1h"
	}`, mini.Addr(), inventory))
	if runner.echo == nil {
		t.Fatal("fixture expected an echo client to enable maintainRealIP")
	}
	refreshStarted := make(chan struct{})
	finishRefresh := make(chan struct{})
	var exited atomic.Bool
	runner.echoRefreshFunc = func(context.Context, string) (string, error) {
		close(refreshStarted)
		<-finishRefresh // simulate a background job still using Redis-backed state
		exited.Store(true)
		return "", errors.New("fixture failure")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("maintainRealIP never started")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned while maintainRealIP was still running: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(finishRefresh)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not stop after background job finished")
	}
	if !exited.Load() {
		t.Fatal("Run returned before the background job exited")
	}
}

// TestRefreshReleasesLockQuicklyAfterFailure guards against a refresh that
// never completed (every source unreachable, same as a replica killed
// mid-fetch by a rolling deploy) stranding every other replica behind the
// cluster-wide lock for the rest of the fetch interval.
func TestRefreshReleasesLockQuicklyAfterFailure(t *testing.T) {
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
		t.Fatal("refresh with no reachable source should return an error")
	}

	lockKey := "test:v1:source-refresh-lock"
	if ttl := mini.TTL(lockKey); ttl <= 0 || ttl > sourceRefreshFailureHold {
		t.Fatalf("lock TTL after a failed refresh = %v, want a short hold around %v (not the ~54m fetch-interval gate)", ttl, sourceRefreshFailureHold)
	}

	mini.FastForward(sourceRefreshFailureHold)
	if _, locked, err := runner.store.TrySourceLock(context.Background(), sourceRefreshLockTTL); err != nil {
		t.Fatalf("TrySourceLock: %v", err)
	} else if !locked {
		t.Fatal("lock from a failed refresh was still held after its failure hold elapsed")
	}
}

func TestRefreshQueuesNothingFromSourceThatFailsMidStream(t *testing.T) {
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	good := writeTempFile(t, dir, "good.txt", "good1.example.net:8080\ngood2.example.net:8080\n")
	var oversized strings.Builder
	for i := 0; i < 5000; i++ { // > 2 staging batches before the byte limit trips
		fmt.Fprintf(&oversized, "bad%d.example.net:8080\n", i)
	}
	bad := writeTempFile(t, dir, "bad.txt", oversized.String())
	runner := loadTestRunner(t, fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "test:v1",
		"sources": ["file://%s", "file://%s"],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"source_max_bytes": %d,
		"fetch_interval": "1h"
	}`, mini.Addr(), good, bad, oversized.Len()-10))

	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	stats, err := runner.store.Stats(context.Background())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.Candidates != 2 || stats.Pending != 2 {
		t.Fatalf("failed source leaked candidates into the queue: %+v", stats)
	}
	if got := runner.metrics.sourceParsedGauge.Load(); got != 2 {
		t.Fatalf("parsed = %d, want only the successful source's 2 records", got)
	}
	if got := runner.metrics.sourceUniqueGauge.Load(); got != 2 {
		t.Fatalf("unique = %d, want 2", got)
	}
	if got := runner.metrics.sourceFetchFailures.Load(); got != 1 {
		t.Fatalf("fetch failures = %d, want 1", got)
	}
	for _, key := range mini.Keys() {
		if strings.Contains(key, "source-stage") {
			t.Fatalf("staging key %q was not cleaned up", key)
		}
	}
}

func TestAggregateValidatedSlicesKeepsLargestASNs(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "asn-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	add := func(id, asn string) {
		mini.SAdd("asn-test:validated", id)
		mini.HSet("asn-test:proxy:"+id, "country", "US", "asn", asn)
	}
	// 80 singleton ASNs sort lexically before the large ones, so a
	// first-seen cap would drop the heavy hitters.
	for i := 0; i < 80; i++ {
		add(fmt.Sprintf("small-%d", i), fmt.Sprintf("AS1%03d", i))
	}
	for i := 0; i < 5; i++ {
		add(fmt.Sprintf("big-%d", i), "AS9999")
	}
	for i := 0; i < 3; i++ {
		add(fmt.Sprintf("mid-%d", i), "AS8888")
	}
	add("unknown-1", "")

	runner := &Runner{store: redisStore, metrics: newMetrics()}
	slices := runner.aggregateValidatedSlices(context.Background())
	if len(slices.ByASN) != maxASNSeries {
		t.Fatalf("ASN series = %d, want %d", len(slices.ByASN), maxASNSeries)
	}
	if slices.ByASN["AS9999"] != 5 || slices.ByASN["AS8888"] != 3 {
		t.Fatalf("largest ASNs missing: AS9999=%d AS8888=%d", slices.ByASN["AS9999"], slices.ByASN["AS8888"])
	}
	if _, ok := slices.ByASN["unknown"]; ok {
		t.Fatal("unknown ASN should not be counted")
	}
	// Ties (count 1) are broken by ASN string: the lowest 62 singletons stay.
	if _, ok := slices.ByASN["AS1061"]; !ok {
		t.Fatal("tie-break should keep AS1061")
	}
	if _, ok := slices.ByASN["AS1062"]; ok {
		t.Fatal("tie-break should drop AS1062")
	}
}

func TestSourceQueueTimeOrdersPriorityClasses(t *testing.T) {
	base := time.Date(2026, time.January, 1, 0, 0, 0, 0, time.UTC)
	checked := sourceQueueTime(base, 10)
	ordinary := sourceQueueTime(base, 20)
	unknown := sourceQueueTime(base, 100)
	if !checked.Before(ordinary) || !ordinary.Before(unknown) {
		t.Fatalf("queue times checked=%s ordinary=%s unknown=%s", checked, ordinary, unknown)
	}
	if !sourceQueueTime(base, -1).Before(checked) || !sourceQueueTime(base, 101).Equal(unknown) {
		t.Fatal("queue priority bounds were not clamped")
	}
}
