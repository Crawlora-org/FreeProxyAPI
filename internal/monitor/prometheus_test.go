package monitor

import (
	"context"
	"encoding/json"
	"errors"
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

func TestDashboardDataAggregatesPrometheusSamples(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	defer redisStore.Close()
	if err := redisStore.SetSourceRefreshStats(context.Background(), 42, 456, 7); err != nil {
		t.Fatalf("SetSourceRefreshStats: %v", err)
	}

	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		query := r.URL.Query().Get("query")
		var body any
		if r.URL.Path == "/api/v1/query_range" {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "matrix",
				"result": []any{
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "validated"}, "values": [][]any{{1, "5"}, {2, "7"}}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "stable"}, "values": [][]any{{1, "4"}, {2, "6"}}},
				},
			}}
		} else if strings.Contains(query, "validated_by_country") && strings.Contains(query, "source_unique") {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated", "instance": "replica-1"}, "value": []any{1, "10"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated", "instance": "replica-2"}, "value": []any{1, "9"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated_stable", "instance": "replica-1"}, "value": []any{1, "8"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_source_unique", "instance": "replica-1"}, "value": []any{1, "42"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_source_records_parsed", "instance": "replica-1"}, "value": []any{1, "99"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_sources_succeeded", "instance": "replica-1"}, "value": []any{1, "4"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_inflight_probes", "instance": "replica-1"}, "value": []any{1, "2"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_inflight_probes", "instance": "replica-2"}, "value": []any{1, "1"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_pending_due", "instance": "replica-1"}, "value": []any{1, "3"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_pending_due", "instance": "replica-2"}, "value": []any{1, "5"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_pending_next_due_in_seconds", "instance": "replica-1"}, "value": []any{1, "0"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_pending_next_due_in_seconds", "instance": "replica-2"}, "value": []any{1, "12"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated_by_country", "country": "US"}, "value": []any{1, "7"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated_by_country", "country": "US", "instance": "replica-2"}, "value": []any{1, "6"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated_latency_band", "band": "200-500ms"}, "value": []any{1, "4"}},
				},
			}}
		} else if strings.Contains(query, "\"probe_results\"") {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "probe_results", "outcome": "ok"}, "value": []any{1, "1"}},
				},
			}}
		} else if strings.Contains(query, "\"processed_records\"") {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "processed_records"}, "value": []any{1, "12"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "validation_attempts"}, "value": []any{1, "5"}},
				},
			}}
		} else if strings.Contains(query, "\"https_checks_1h\"") {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "https_checks_1h"}, "value": []any{1, "200"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "https_ok_1h"}, "value": []any{1, "50"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "tamper_checks_1h"}, "value": []any{1, "80"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "tampered_1h"}, "value": []any{1, "4"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "internal_api_failures_15m"}, "value": []any{1, "0"}},
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "gost_router_available"}, "value": []any{1, "2"}},
				},
			}}
		} else if strings.Contains(query, "validation_upload_per_second") {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{dashboardMetricLabel: "gost_upload_per_second"}, "value": []any{1, "3"}},
				},
			}}
		} else {
			body = map[string]any{"status": "success", "data": map[string]any{
				"resultType": "vector",
				"result": []any{
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated", "instance": "replica-1"}, "value": []any{1, "10"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated", "instance": "replica-2"}, "value": []any{1, "9"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_validated_stable", "instance": "replica-1"}, "value": []any{1, "8"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_inflight_probes", "instance": "replica-1"}, "value": []any{1, "2"}},
					map[string]any{"metric": map[string]string{"__name__": "freeproxyapi_inflight_probes", "instance": "replica-2"}, "value": []any{1, "1"}},
				},
			}}
		}
		if err := json.NewEncoder(w).Encode(body); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	runner := &Runner{prometheus: newPrometheusClient(server.URL), store: redisStore}
	payload, err := runner.dashboardData(context.Background(), dashboardRange("1h"))
	if err != nil {
		t.Fatalf("dashboardData: %v", err)
	}
	if payload.Current["validated"] != 10 || payload.Current["stable"] != 8 || payload.Current["source_unique"] != 42 || payload.Current["source_records_parsed"] != 456 || payload.Current["sources_succeeded"] != 7 || payload.Current["inflight_probes"] != 3 {
		t.Fatalf("current = %#v", payload.Current)
	}
	if !strings.Contains(dashboardMetricsQuery(), "pending_due|pending_next_due_in_seconds") {
		t.Fatalf("current query misses pending schedule gauges: %s", dashboardMetricsQuery())
	}
	if !strings.Contains(dashboardRatesQuery(), "source_fetch_failures_by_reason_total") {
		t.Fatalf("rate query misses source failure reason metric: %s", dashboardRatesQuery())
	}
	if got, ok := payload.Current["pending_due"]; !ok || got != 5 {
		t.Fatalf("pending_due = %v (present=%t), want max across replicas 5", got, ok)
	}
	if got, ok := payload.Current["pending_next_due_in_seconds"]; !ok || got != 12 {
		t.Fatalf("pending_next_due_in_seconds = %v (present=%t), want max across replicas 12", got, ok)
	}
	if payload.Distributions["country"][0].Label != "US" || payload.Distributions["country"][0].Value != 7 {
		t.Fatalf("country distribution = %#v", payload.Distributions["country"])
	}
	if len(payload.Distributions["latency_band"]) != 1 || payload.Distributions["latency_band"][0].Label != "200-500ms" {
		t.Fatalf("latency distribution = %#v", payload.Distributions["latency_band"])
	}
	if payload.Rates["probe_results_ok"] != 1 {
		t.Fatalf("probe rate = %#v", payload.Rates)
	}
	if payload.Totals["processed_records"] != 12 {
		t.Fatalf("processed records total = %#v, want 12", payload.Totals)
	}
	if payload.Totals["validation_attempts"] == 0 || !payload.Traffic["gost"].Upload.Available {
		t.Fatalf("traffic totals = %#v traffic = %#v", payload.Totals, payload.Traffic)
	}
	if len(payload.Trends["validated"]) != 2 || payload.Range != "1h" {
		t.Fatalf("trend = %#v range=%q", payload.Trends["validated"], payload.Range)
	}
	health := payload.Health
	if health == nil || health.HTTPSChecks1h == nil || *health.HTTPSChecks1h != 200 ||
		health.HTTPSPassRate1h == nil || *health.HTTPSPassRate1h != 25 ||
		health.TamperChecks1h == nil || *health.TamperChecks1h != 80 ||
		health.Tampered1h == nil || *health.Tampered1h != 4 ||
		health.InternalAPIFailures15m == nil || *health.InternalAPIFailures15m != 0 ||
		health.GostRouterAvailable == nil || *health.GostRouterAvailable != 2 ||
		health.GostRouterDesired != nil {
		t.Fatalf("health = %s", mustJSON(t, health))
	}
	if got := requests.Load(); got != 6 {
		t.Fatalf("Prometheus requests = %d, want 6 batched requests", got)
	}
}

func TestDashboardQueriesRunConcurrentlyAndShareTotalsAcrossRanges(t *testing.T) {
	var requests, totalsRequests, trafficRequests, healthRequests, active, maximum atomic.Int64
	allArrived := make(chan struct{})
	var arrivedOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		query := r.URL.Query().Get("query")
		if strings.Contains(query, "\"processed_records\"") {
			totalsRequests.Add(1)
		}
		if strings.Contains(query, "validation_upload_per_second") {
			trafficRequests.Add(1)
		}
		if strings.Contains(query, "\"https_checks_1h\"") {
			healthRequests.Add(1)
		}
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		// A cold refresh issues six requests; hold each until all six are in
		// flight so a sequential implementation shows a maximum of one.
		if current >= 6 {
			arrivedOnce.Do(func() { close(allArrived) })
		}
		select {
		case <-allArrived:
		case <-time.After(2 * time.Second):
		}
		resultType := "vector"
		if r.URL.Path == "/api/v1/query_range" {
			resultType = "matrix"
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": resultType, "result": []any{}}})
	}))
	defer server.Close()

	runner := &Runner{prometheus: newPrometheusClient(server.URL)}
	if _, err := runner.dashboardData(context.Background(), dashboardRange("1h")); err != nil {
		t.Fatalf("dashboardData 1h: %v", err)
	}
	if got := maximum.Load(); got != 6 {
		t.Fatalf("maximum concurrent Prometheus requests = %d, want 6", got)
	}
	if _, err := runner.dashboardData(context.Background(), dashboardRange("24h")); err != nil {
		t.Fatalf("dashboardData 24h: %v", err)
	}
	if requests.Load() != 9 || totalsRequests.Load() != 1 || trafficRequests.Load() != 1 || healthRequests.Load() != 1 {
		t.Fatalf("requests=%d totals=%d traffic=%d health=%d, want 9 total with shared totals/traffic/health", requests.Load(), totalsRequests.Load(), trafficRequests.Load(), healthRequests.Load())
	}

	// Expired shared totals are refreshed once by the next range refresh.
	runner.prometheus.totalsMu.Lock()
	runner.prometheus.totalsExpiresAt = time.Now().Add(-time.Second)
	runner.prometheus.totalsMu.Unlock()
	if _, err := runner.dashboardData(context.Background(), dashboardRange("6h")); err != nil {
		t.Fatalf("dashboardData 6h: %v", err)
	}
	if totalsRequests.Load() != 2 || trafficRequests.Load() != 2 {
		t.Fatalf("expired totals not refreshed: totals=%d traffic=%d", totalsRequests.Load(), trafficRequests.Load())
	}
}

func TestDashboardCacheCoalescesAndCanonicalizesRanges(t *testing.T) {
	client := newPrometheusClient("http://prometheus.invalid")
	var calls atomic.Int64
	started := make(chan struct{})
	release := make(chan struct{})
	load := func(context.Context, dashboardRangeSpec) (dashboardPayload, error) {
		if calls.Add(1) == 1 {
			close(started)
		}
		<-release
		return dashboardPayload{Status: "ok"}, nil
	}
	const waiters = 20
	var wg sync.WaitGroup
	wg.Add(waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			defer wg.Done()
			if _, err := client.cachedDashboard(context.Background(), dashboardRange("attacker-value"), load); err != nil {
				t.Errorf("cachedDashboard: %v", err)
			}
		}()
	}
	<-started
	close(release)
	wg.Wait()
	if _, err := client.cachedDashboard(context.Background(), dashboardRange("another-value"), load); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", calls.Load())
	}
	for _, raw := range []string{"1h", "24h"} {
		if _, err := client.cachedDashboard(context.Background(), dashboardRange(raw), load); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.cache) != 3 {
		t.Fatalf("canonical cache keys = %d, want 3", len(client.cache))
	}
}

func TestDashboardCanceledWaiterDoesNotCancelSharedRefresh(t *testing.T) {
	client := newPrometheusClient("http://prometheus.invalid")
	started := make(chan struct{})
	release := make(chan struct{})
	load := func(ctx context.Context, _ dashboardRangeSpec) (dashboardPayload, error) {
		close(started)
		select {
		case <-release:
			return dashboardPayload{Status: "ok"}, nil
		case <-ctx.Done():
			return dashboardPayload{}, ctx.Err()
		}
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { _, err := client.cachedDashboard(firstCtx, dashboardRange("1h"), load); first <- err }()
	<-started
	second := make(chan error, 1)
	go func() {
		_, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), load)
		second <- err
	}()
	cancel()
	if err := <-first; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v", err)
	}
	close(release)
	if err := <-second; err != nil {
		t.Fatalf("shared refresh was poisoned: %v", err)
	}
}

func TestDashboardFailureUsesLastSuccessfulSnapshot(t *testing.T) {
	client := newPrometheusClient("http://prometheus.invalid")
	success := func(context.Context, dashboardRangeSpec) (dashboardPayload, error) {
		return dashboardPayload{Status: "ok", Current: map[string]float64{"validated": 7}}, nil
	}
	if _, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), success); err != nil {
		t.Fatalf("initial dashboard refresh: %v", err)
	}

	client.cacheMu.Lock()
	entry := client.cache["1h"]
	entry.expiresAt = time.Now().Add(-time.Second)
	client.cache["1h"] = entry
	client.cacheMu.Unlock()

	failure := func(context.Context, dashboardRangeSpec) (dashboardPayload, error) {
		return dashboardPayload{}, errors.New("Prometheus unavailable")
	}
	payload, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), failure)
	if err != nil {
		t.Fatalf("stale dashboard refresh: %v", err)
	}
	if payload.Status != "degraded" || !payload.Stale || payload.Current["validated"] != 7 {
		t.Fatalf("stale payload = %#v", payload)
	}
}

func TestDashboardFailureCooldownAndGlobalRefreshBound(t *testing.T) {
	client := newPrometheusClient("http://prometheus.invalid")
	var failures atomic.Int64
	fail := func(context.Context, dashboardRangeSpec) (dashboardPayload, error) {
		failures.Add(1)
		return dashboardPayload{}, errors.New("upstream detail")
	}
	if _, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), fail); err == nil {
		t.Fatal("expected refresh failure")
	}
	if _, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), fail); err == nil || failures.Load() != 1 {
		t.Fatalf("failure cooldown missed: calls=%d err=%v", failures.Load(), err)
	}
	time.Sleep(dashboardFailureCooldown + 50*time.Millisecond)
	if _, err := client.cachedDashboard(context.Background(), dashboardRange("1h"), fail); err == nil || failures.Load() != 2 {
		t.Fatalf("refresh did not recover after cooldown: calls=%d err=%v", failures.Load(), err)
	}

	client = newPrometheusClient("http://prometheus.invalid")
	var active, maximum atomic.Int64
	release := make(chan struct{})
	load := func(context.Context, dashboardRangeSpec) (dashboardPayload, error) {
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		<-release
		active.Add(-1)
		return dashboardPayload{Status: "ok"}, nil
	}
	var wg sync.WaitGroup
	for _, raw := range []string{"1h", "6h", "24h"} {
		wg.Add(1)
		go func(raw string) {
			defer wg.Done()
			_, _ = client.cachedDashboard(context.Background(), dashboardRange(raw), load)
		}(raw)
	}
	deadline := time.Now().Add(time.Second)
	for maximum.Load() < dashboardRefreshConcurrency && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if maximum.Load() != dashboardRefreshConcurrency {
		t.Fatalf("maximum concurrent refreshes = %d", maximum.Load())
	}
	close(release)
	wg.Wait()
	if maximum.Load() > dashboardRefreshConcurrency {
		t.Fatalf("refresh concurrency exceeded bound: %d", maximum.Load())
	}
	if got := publicDashboardError(errors.New("secret upstream message")); got != "dashboard data unavailable" {
		t.Fatalf("public error leaked detail: %q", got)
	}
}
