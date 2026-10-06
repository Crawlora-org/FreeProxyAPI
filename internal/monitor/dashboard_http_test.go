package monitor

import (
	"bytes"
	"encoding/json"
	"image"
	_ "image/png"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	body, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

// fakeDashboardPrometheus answers every dashboard query with an empty success
// result, except the health batch, which returns fixed values. It can fail
// every request or hold requests until released.
type fakeDashboardPrometheus struct {
	server   *httptest.Server
	requests atomic.Int64
	fail     atomic.Bool
	gate     chan struct{}
}

func newFakeDashboardPrometheus(t *testing.T, gate chan struct{}) *fakeDashboardPrometheus {
	t.Helper()
	fake := &fakeDashboardPrometheus{gate: gate}
	fake.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fake.requests.Add(1)
		if fake.gate != nil {
			<-fake.gate
		}
		if fake.fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		resultType := "vector"
		if r.URL.Path == "/api/v1/query_range" {
			resultType = "matrix"
		}
		result := []any{}
		if strings.Contains(r.URL.Query().Get("query"), "\"https_checks_1h\"") {
			result = []any{
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "https_checks_1h"}, "value": []any{1, "1208"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "https_ok_1h"}, "value": []any{1, "302"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "tamper_checks_1h"}, "value": []any{1, "823"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "tampered_1h"}, "value": []any{1, "70"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "internal_api_failures_15m"}, "value": []any{1, "0"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "gost_router_available"}, "value": []any{1, "2"}},
				map[string]any{"metric": map[string]string{dashboardMetricLabel: "gost_router_desired"}, "value": []any{1, "2"}},
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": result}})
	}))
	t.Cleanup(fake.server.Close)
	return fake
}

func startDashboardTestServer(t *testing.T, prometheusURL string) (*healthServer, *Runner) {
	t.Helper()
	runner := &Runner{prometheus: newPrometheusClient(prometheusURL), metrics: newMetrics()}
	health, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = health.Shutdown() })
	return health, runner
}

func getDashboardData(t *testing.T, health *healthServer, rawRange string) (*httptest.ResponseRecorder, dashboardPayload) {
	t.Helper()
	recorder := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/data?range="+rawRange, nil))
	var payload dashboardPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode dashboard body %q: %v", recorder.Body.String(), err)
	}
	return recorder, payload
}

func expireDashboardCache(runner *Runner, key string) {
	client := runner.prometheus
	client.cacheMu.Lock()
	entry := client.cache[key]
	entry.expiresAt = time.Now().Add(-time.Second)
	client.cache[key] = entry
	client.cacheMu.Unlock()
	client.totalsMu.Lock()
	client.totalsExpiresAt = time.Now().Add(-time.Second)
	client.totalsMu.Unlock()
}

func TestDashboardDataHTTPCacheHitExpiryAndHeaders(t *testing.T) {
	fake := newFakeDashboardPrometheus(t, nil)
	health, runner := startDashboardTestServer(t, fake.server.URL)

	first, payload := getDashboardData(t, health, "1h")
	if first.Code != http.StatusOK || payload.Status != "ok" {
		t.Fatalf("dashboard data: status=%d body=%s", first.Code, first.Body.String())
	}
	if got := first.Header().Get("Cache-Control"); got != "public, max-age=15, s-maxage=15" {
		t.Fatalf("Cache-Control = %q", got)
	}
	if dashboardCacheFreshness != 15*time.Second {
		t.Fatalf("server cache freshness = %s, want 15s", dashboardCacheFreshness)
	}
	var raw struct {
		Health map[string]*float64 `json:"health"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"https_checks_1h", "https_pass_rate_1h", "tamper_checks_1h", "tampered_1h", "internal_api_failures_15m", "gost_router_available", "gost_router_desired"} {
		if value, ok := raw.Health[field]; !ok || value == nil {
			t.Fatalf("health field %q missing or null: %s", field, first.Body.String())
		}
	}
	if rate := *raw.Health["https_pass_rate_1h"]; rate < 24.99 || rate > 25.01 {
		t.Fatalf("https_pass_rate_1h = %v, want 25", rate)
	}
	t.Logf("dashboard health sample: %s", mustJSON(t, payload.Health))
	cold := fake.requests.Load()
	if cold != 6 {
		t.Fatalf("cold refresh Prometheus requests = %d, want 6", cold)
	}

	second, _ := getDashboardData(t, health, "1h")
	if fake.requests.Load() != cold || second.Body.String() != first.Body.String() {
		t.Fatalf("cache miss within TTL: requests=%d body changed=%t", fake.requests.Load(), second.Body.String() != first.Body.String())
	}

	expireDashboardCache(runner, "1h")
	third, _ := getDashboardData(t, health, "1h")
	if third.Code != http.StatusOK || fake.requests.Load() != 2*cold {
		t.Fatalf("expired cache not refreshed: status=%d requests=%d", third.Code, fake.requests.Load())
	}
}

func TestDashboardDataHTTPConcurrentRequestsShareOneFetch(t *testing.T) {
	gate := make(chan struct{})
	fake := newFakeDashboardPrometheus(t, gate)
	health, _ := startDashboardTestServer(t, fake.server.URL)

	const clients = 16
	var wg sync.WaitGroup
	codes := make(chan int, clients)
	for i := 0; i < clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder := httptest.NewRecorder()
			health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/dashboard/data?range=6h", nil))
			codes <- recorder.Code
		}()
	}
	deadline := time.Now().Add(2 * time.Second)
	for fake.requests.Load() < 6 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	// Give any request that failed to join the flight time to issue its own
	// Prometheus queries before releasing them all.
	time.Sleep(100 * time.Millisecond)
	close(gate)
	wg.Wait()
	close(codes)
	for code := range codes {
		if code != http.StatusOK {
			t.Fatalf("concurrent dashboard status = %d", code)
		}
	}
	if got := fake.requests.Load(); got != 6 {
		t.Fatalf("Prometheus requests for %d concurrent clients = %d, want one shared fetch of 6", clients, got)
	}
}

func TestDashboardDataHTTPErrorAndStalePaths(t *testing.T) {
	fake := newFakeDashboardPrometheus(t, nil)
	health, runner := startDashboardTestServer(t, fake.server.URL)

	fake.fail.Store(true)
	failed, payload := getDashboardData(t, health, "24h")
	if failed.Code != http.StatusServiceUnavailable || payload.Status != "unavailable" || payload.Error != "dashboard data unavailable" {
		t.Fatalf("cold failure: status=%d body=%s", failed.Code, failed.Body.String())
	}
	if got := failed.Header().Get("Cache-Control"); got != "no-store" {
		t.Fatalf("failure Cache-Control = %q, want no-store", got)
	}

	fake.fail.Store(false)
	expireDashboardCache(runner, "24h")
	ok, payload := getDashboardData(t, health, "24h")
	if ok.Code != http.StatusOK || payload.Status != "ok" || payload.Health == nil {
		t.Fatalf("recovery: status=%d body=%s", ok.Code, ok.Body.String())
	}

	fake.fail.Store(true)
	for attempt := 1; attempt <= 2; attempt++ {
		expireDashboardCache(runner, "24h")
		stale, payload := getDashboardData(t, health, "24h")
		if stale.Code != http.StatusOK || payload.Status != "degraded" || !payload.Stale || payload.Health == nil {
			t.Fatalf("stale attempt %d: status=%d body=%s", attempt, stale.Code, stale.Body.String())
		}
		if got := stale.Header().Get("Cache-Control"); got != dashboardDataCacheControl {
			t.Fatalf("stale Cache-Control = %q", got)
		}
	}
}

func TestDashboardHealthNullsMissingSeries(t *testing.T) {
	body := mustJSON(t, buildDashboardHealth(nil))
	for _, field := range []string{"https_checks_1h", "https_pass_rate_1h", "tamper_checks_1h", "tampered_1h", "internal_api_failures_15m", "gost_router_available", "gost_router_desired"} {
		if !strings.Contains(body, `"`+field+`":null`) {
			t.Fatalf("missing series did not produce null %s: %s", field, body)
		}
	}
	zeroChecks := buildDashboardHealth([]prometheusVector{
		{Metric: map[string]string{dashboardMetricLabel: "https_checks_1h"}, Value: []json.RawMessage{json.RawMessage("1"), json.RawMessage(`"0"`)}},
	})
	if zeroChecks.HTTPSChecks1h == nil || *zeroChecks.HTTPSChecks1h != 0 || zeroChecks.HTTPSPassRate1h != nil {
		t.Fatalf("zero HTTPS checks should report 0 checks and null pass rate: %s", mustJSON(t, zeroChecks))
	}
	noOK := buildDashboardHealth([]prometheusVector{
		{Metric: map[string]string{dashboardMetricLabel: "https_checks_1h"}, Value: []json.RawMessage{json.RawMessage("1"), json.RawMessage(`"10"`)}},
	})
	if noOK.HTTPSPassRate1h == nil || *noOK.HTTPSPassRate1h != 0 {
		t.Fatalf("checks without ok series should report 0%% pass rate: %s", mustJSON(t, noOK))
	}
}

func TestStatsIncludesAnonymityAndLatencyBands(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "stats-fields-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	for id, values := range map[string][]string{
		"a": {"US", "elite", "150"},
		"b": {"US", "elite", "350"},
		"c": {"GB", "anonymous", "700"},
		"d": {"CA", "transparent", "1500"},
		"e": {"CA", "", "0"},
	} {
		mini.SAdd("stats-fields-test:validated", id)
		mini.HSet("stats-fields-test:proxy:"+id,
			"country", values[0], "exit_country", values[0], "anonymity", values[1],
			"latency_ewma_ms", values[2], "ok_ratio_pct", "90")
	}
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	recorder := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("stats: status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var payload struct {
		Status          string           `json:"status"`
		Total           int64            `json:"total"`
		ByCountry       map[string]int64 `json:"by_country"`
		StableByCountry map[string]int64 `json:"stable_by_country"`
		ByAnonymity     map[string]int64 `json:"by_anonymity"`
		LatencyBands    map[string]int64 `json:"latency_bands"`
		CheckedAt       string           `json:"checked_at"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	t.Logf("stats sample: %s", strings.TrimSpace(recorder.Body.String()))
	if payload.Status != "ok" || payload.Total != 5 || payload.ByCountry["US"] != 2 || payload.CheckedAt == "" {
		t.Fatalf("existing stats fields changed: %s", recorder.Body.String())
	}
	wantAnonymity := map[string]int64{"elite": 2, "anonymous": 1, "transparent": 1, "unknown": 1}
	if mustJSON(t, payload.ByAnonymity) != mustJSON(t, wantAnonymity) {
		t.Fatalf("by_anonymity = %v, want %v", payload.ByAnonymity, wantAnonymity)
	}
	var bandTotal int64
	for _, band := range []string{"<200ms", "200-500ms", "500-1000ms", ">1000ms"} {
		if payload.LatencyBands[band] != 1 {
			t.Fatalf("latency_bands[%q] = %d: %v", band, payload.LatencyBands[band], payload.LatencyBands)
		}
		bandTotal += payload.LatencyBands[band]
	}
	if bandTotal+payload.LatencyBands["unknown"] != payload.Total {
		t.Fatalf("latency bands do not add up to total: %v", payload.LatencyBands)
	}
	if body := recorder.Body.String(); strings.Contains(body, "latency_ewma_ms") || strings.Contains(body, "proxy:") {
		t.Fatalf("stats leaked per-proxy detail: %s", body)
	}
}

func TestSocialShareServedUnderDashboardAndImagesStaySmall(t *testing.T) {
	health, _ := startDashboardTestServer(t, "http://prometheus.invalid")
	for _, path := range []string{"/social-share.png", "/dashboard/social-share.png"} {
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK || recorder.Header().Get("Content-Type") != "image/png" || !bytes.Equal(recorder.Body.Bytes(), socialSharePNG) {
			t.Fatalf("%s: status=%d type=%q bytes=%d", path, recorder.Code, recorder.Header().Get("Content-Type"), recorder.Body.Len())
		}
	}
	for _, tc := range []struct {
		name          string
		body          []byte
		maxBytes      int
		width, height int
	}{
		{name: "social-share.png", body: socialSharePNG, maxBytes: 200_000, width: 1200, height: 630},
		{name: "icon.png", body: iconPNG, maxBytes: 30_000, width: 180, height: 180},
	} {
		config, format, err := image.DecodeConfig(bytes.NewReader(tc.body))
		if err != nil || format != "png" {
			t.Fatalf("%s: decode format=%q err=%v", tc.name, format, err)
		}
		if config.Width != tc.width || config.Height != tc.height || len(tc.body) > tc.maxBytes {
			t.Fatalf("%s: %dx%d %d bytes, want %dx%d <= %d bytes", tc.name, config.Width, config.Height, len(tc.body), tc.width, tc.height, tc.maxBytes)
		}
	}
}
