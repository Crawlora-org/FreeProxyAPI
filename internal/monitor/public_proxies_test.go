package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func TestAggregateStableCountriesUsesObservedExitCountry(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "stable-country-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()

	for id, values := range map[string][]string{
		"stable-us":      {"CA", "US", "80"},
		"stable-gb":      {"DE", "GB", "95"},
		"unstable-us":    {"CA", "US", "79"},
		"stable-unknown": {"CA", "", "90"},
	} {
		mini.SAdd("stable-country-test:validated", id)
		mini.HSet("stable-country-test:proxy:"+id,
			"country", values[0], "exit_country", values[1], "ok_ratio_pct", values[2],
			"latency_ewma_ms", "100")
	}

	runner := &Runner{store: redisStore, metrics: newMetrics()}
	slices := runner.aggregateValidatedSlices(context.Background())
	if slices.Stable != 3 {
		t.Fatalf("stable=%d, want 3", slices.Stable)
	}
	if got := slices.StableByCountry["US"]; got != 1 {
		t.Fatalf("stable US=%d, want 1", got)
	}
	if got := slices.StableByCountry["GB"]; got != 1 {
		t.Fatalf("stable GB=%d, want 1", got)
	}
	if _, ok := slices.StableByCountry["CA"]; ok {
		t.Fatalf("stable countries should use exit country, got CA")
	}

	runner.setValidatedSlices(slices)
	health, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	response := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/stats", nil))
	var payload statsPayload
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &payload) != nil {
		t.Fatalf("stats: status=%d body=%s", response.Code, response.Body.String())
	}
	if got := payload.StableByCountry["US"]; got != 1 {
		t.Fatalf("stats stable US=%d, want 1", got)
	}
}

func TestPublicProxiesLimitsAndExitCountry(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "public-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	for i := 0; i < 1205; i++ {
		id := fmt.Sprint(i)
		exit, entry, ratio := "US", "CA", "90"
		if i >= 1200 {
			exit, entry = "CA", "US"
		}
		if i == 1199 {
			ratio = "70"
		}
		mini.SAdd("public-test:validated", id)
		mini.HSet("public-test:proxy:"+id, "url", "http://proxy"+id+".example:8080", "country", entry, "exit_country", exit, "ok_ratio_pct", ratio)
	}
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	for _, tc := range []struct {
		query   string
		count   int
		exit    string
		limit   int
		offset  int
		hasMore bool
	}{
		{"exit_country=us&min_ratio_pct=80&limit=50", 50, "US", 50, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=10", 10, "US", 10, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=100", 100, "US", 100, 0, true},
		// An omitted, zero, negative, or invalid limit is the default page.
		{"exit_country=US&min_ratio_pct=80", publicProxyDefaultLimit, "US", publicProxyDefaultLimit, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=0", publicProxyDefaultLimit, "US", publicProxyDefaultLimit, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=-5", publicProxyDefaultLimit, "US", publicProxyDefaultLimit, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=lots", publicProxyDefaultLimit, "US", publicProxyDefaultLimit, 0, true},
		// An explicit limit up to the maximum is honored; more is clamped.
		{"exit_country=US&min_ratio_pct=80&limit=1000", publicProxyMaxLimit, "US", publicProxyMaxLimit, 0, true},
		{"exit_country=US&min_ratio_pct=80&limit=2000", publicProxyMaxLimit, "US", publicProxyMaxLimit, 0, true},
		{"exit_country=US&min_ratio_pct=80&offset=1000", 100, "US", publicProxyDefaultLimit, 1000, true},
		{"exit_country=US&min_ratio_pct=80&offset=1100", 99, "US", publicProxyDefaultLimit, 1100, false},
		{"exit_country=US&min_ratio_pct=80&limit=100&offset=1150", 49, "US", 100, 1150, false},
		{"exit_country=US&min_ratio_pct=80&limit=10&offset=1189", 10, "US", 10, 1189, false},
		{"exit_country=US&min_ratio_pct=80&offset=5000", 0, "US", publicProxyDefaultLimit, 5000, false},
		{"exit_country=US&min_ratio_pct=95&limit=50", 0, "US", 50, 0, false},
		{"country=US&limit=50", 5, "CA", 50, 0, false},
		{"country=US&geo_mismatch=true", 5, "CA", publicProxyDefaultLimit, 0, false},
	} {
		t.Run(tc.query, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/proxies?"+tc.query, nil))
			var data publicProxiesPayload
			if recorder.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
			if err := json.Unmarshal(recorder.Body.Bytes(), &data); err != nil {
				t.Fatal(err)
			}
			if data.Count != tc.count || len(data.Proxies) != tc.count {
				t.Fatalf("got %d proxies, want %d", data.Count, tc.count)
			}
			if data.Limit != tc.limit || data.Offset != tc.offset || data.HasMore != tc.hasMore {
				t.Fatalf("page = limit %d offset %d has_more %v, want %d %d %v", data.Limit, data.Offset, data.HasMore, tc.limit, tc.offset, tc.hasMore)
			}
			for _, proxy := range data.Proxies {
				if proxy.ExitCountry != tc.exit {
					t.Fatalf("wrong exit country: %+v", proxy)
				}
			}
		})
	}
}

func TestPublicProxiesPagesCoverAllMatchesAndRejectBadOffsets(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "public-page-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	const total = 250
	for i := 0; i < total; i++ {
		id := fmt.Sprint(i)
		mini.SAdd("public-page-test:validated", id)
		mini.HSet("public-page-test:proxy:"+id, "url", "http://proxy"+id+".example:8080", "country", "US", "exit_country", "US", "ok_ratio_pct", "90")
	}
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	if health.server.IdleTimeout != httpIdleTimeout || health.server.MaxHeaderBytes != httpMaxHeaderBytes {
		t.Fatalf("server idle=%s maxHeader=%d", health.server.IdleTimeout, health.server.MaxHeaderBytes)
	}

	seen := make(map[string]struct{}, total)
	for offset := 0; ; offset += 100 {
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, fmt.Sprintf("/proxies?exit_country=US&limit=100&offset=%d", offset), nil))
		var data publicProxiesPayload
		if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &data) != nil {
			t.Fatalf("offset %d: status=%d body=%s", offset, recorder.Code, recorder.Body.String())
		}
		for _, proxy := range data.Proxies {
			if _, dup := seen[proxy.URL]; dup {
				t.Fatalf("offset %d repeated %s", offset, proxy.URL)
			}
			seen[proxy.URL] = struct{}{}
		}
		if !data.HasMore {
			break
		}
	}
	if len(seen) != total {
		t.Fatalf("pages covered %d proxies, want %d", len(seen), total)
	}

	for _, offset := range []string{"-1", "abc", fmt.Sprint(publicProxyMaxOffset + 1)} {
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/proxies?offset="+offset, nil))
		if recorder.Code != http.StatusBadRequest || recorder.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Fatalf("offset=%s: status=%d nosniff=%q", offset, recorder.Code, recorder.Header().Get("X-Content-Type-Options"))
		}
	}
}

func TestPublicProxiesAllowsCORS(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "public-cors-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	request := httptest.NewRequest(http.MethodGet, "/proxies?exit_country=US&min_ratio_pct=80", nil)
	request.Header.Set("Origin", "https://example.com")
	response := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("GET /proxies: status=%d body=%s", response.Code, response.Body.String())
	}
	if got := response.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("Access-Control-Allow-Origin = %q", got)
	}
	cacheHit := httptest.NewRecorder()
	cacheRequest := httptest.NewRequest(http.MethodGet, "/proxies?exit_country=US&min_ratio_pct=80", nil)
	cacheRequest.Header.Set("Origin", "https://example.com")
	health.server.Handler.ServeHTTP(cacheHit, cacheRequest)
	if cacheHit.Code != http.StatusOK || cacheHit.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("cached GET /proxies: status=%d cors=%q", cacheHit.Code, cacheHit.Header().Get("Access-Control-Allow-Origin"))
	}

	head := httptest.NewRecorder()
	headRequest := httptest.NewRequest(http.MethodHead, "/proxies?exit_country=US&min_ratio_pct=80", nil)
	headRequest.Header.Set("Origin", "https://example.com")
	health.server.Handler.ServeHTTP(head, headRequest)
	if head.Code != http.StatusOK || head.Body.Len() != 0 || head.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("HEAD /proxies: status=%d body=%d cors=%q", head.Code, head.Body.Len(), head.Header().Get("Access-Control-Allow-Origin"))
	}

	preflight := httptest.NewRecorder()
	preflightRequest := httptest.NewRequest(http.MethodOptions, "/proxies", nil)
	preflightRequest.Header.Set("Origin", "https://example.com")
	preflightRequest.Header.Set("Access-Control-Request-Method", http.MethodGet)
	health.server.Handler.ServeHTTP(preflight, preflightRequest)
	if preflight.Code != http.StatusNoContent {
		t.Fatalf("OPTIONS /proxies: status=%d body=%s", preflight.Code, preflight.Body.String())
	}
	if got := preflight.Header().Get("Access-Control-Allow-Methods"); got != "GET, HEAD, OPTIONS" {
		t.Fatalf("Access-Control-Allow-Methods = %q", got)
	}
	if got := preflight.Header().Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("preflight Access-Control-Allow-Origin = %q", got)
	}
	if got := preflight.Header().Get("Access-Control-Allow-Headers"); got != "Accept, Content-Type" {
		t.Fatalf("Access-Control-Allow-Headers = %q", got)
	}
	if got := preflight.Header().Get("Access-Control-Max-Age"); got != strconv.Itoa(publicProxyCORSMaxAge) {
		t.Fatalf("Access-Control-Max-Age = %q", got)
	}

	methodNotAllowed := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(methodNotAllowed, httptest.NewRequest(http.MethodPost, "/proxies", nil))
	if methodNotAllowed.Code != http.StatusMethodNotAllowed || methodNotAllowed.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("POST /proxies: status=%d cors=%q", methodNotAllowed.Code, methodNotAllowed.Header().Get("Access-Control-Allow-Origin"))
	}
	stats := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(stats, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if stats.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("/stats unexpectedly allowed CORS: %q", stats.Header().Get("Access-Control-Allow-Origin"))
	}
}
