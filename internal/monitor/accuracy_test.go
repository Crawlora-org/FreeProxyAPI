package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestLoadConfigAccuracyDefaults(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.ProbeExpectedBody != "" {
		t.Errorf("ProbeExpectedBody = %q, want empty", config.ProbeExpectedBody)
	}
	if config.ListedFailureRetestInterval != 5*time.Minute || config.SampleRetestInterval != time.Minute || config.MinSamplesForListing != 3 {
		t.Errorf("accuracy defaults wrong: %+v", config)
	}
	policy := config.RetestPolicy()
	if policy.ListedFailureAfter != 5*time.Minute || policy.SampleRetestAfter != time.Minute || policy.MinSamplesForListing != 3 {
		t.Errorf("RetestPolicy = %+v", policy)
	}
}

func TestLoadConfigAccuracyValues(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///data/proxies.txt"],
		"probe_expected_body": "success\n",
		"listed_failure_retest_interval": "2m",
		"sample_retest_interval": "30s",
		"min_samples_for_listing": 10
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.ProbeExpectedBody != "success" || config.ListedFailureRetestInterval != 2*time.Minute ||
		config.SampleRetestInterval != 30*time.Second || config.MinSamplesForListing != 10 {
		t.Errorf("accuracy values wrong: %+v", config)
	}

	t.Setenv("FREEPROXYAPI_PROBE_EXPECTED_BODY", " ok ")
	config, err = LoadConfig(writeConfig(t, `{"sources": ["file:///data/proxies.txt"], "probe_expected_body": "success"}`))
	if err != nil || config.ProbeExpectedBody != "ok" {
		t.Fatalf("env override: body=%q err=%v", config.ProbeExpectedBody, err)
	}
}

func TestLoadConfigAccuracyValidation(t *testing.T) {
	for _, tc := range []struct {
		field string
		value string
		want  string
	}{
		{"listed_failure_retest_interval", `"0s"`, "listed_failure_retest_interval"},
		{"listed_failure_retest_interval", `"-5m"`, "listed_failure_retest_interval"},
		{"sample_retest_interval", `"soon"`, "sample_retest_interval"},
		{"sample_retest_interval", `"0"`, "sample_retest_interval"},
		{"min_samples_for_listing", `11`, "min_samples_for_listing"},
		{"min_samples_for_listing", `-1`, "min_samples_for_listing"},
	} {
		_, err := LoadConfig(writeConfig(t, `{"sources": ["file:///data/proxies.txt"], "`+tc.field+`": `+tc.value+`}`))
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s=%s: err=%v, want mention of %s", tc.field, tc.value, err, tc.want)
		}
	}
}

func TestParseProxyMaxAge(t *testing.T) {
	for raw, want := range map[string]int64{
		"":      0,
		"30m":   30 * 60 * 1000,
		"1800":  1800 * 1000,
		" 90s ": 90 * 1000,
		"720h":  720 * 3600 * 1000,
	} {
		got, err := parseProxyMaxAge(raw)
		if err != nil || got != want {
			t.Errorf("parseProxyMaxAge(%q) = %d, %v; want %d", raw, got, err, want)
		}
	}
	for _, raw := range []string{"0", "0s", "-5m", "-1", "abc", "1ns", "721h", "99999999999"} {
		if _, err := parseProxyMaxAge(raw); err == nil {
			t.Errorf("parseProxyMaxAge(%q) accepted", raw)
		}
	}
}

func TestPublicProxyCacheKeyIncludesMaxAge(t *testing.T) {
	if publicProxyCacheKey(store.ProxyFilter{}, 0) == publicProxyCacheKey(store.ProxyFilter{MaxAgeMs: 1000}, 0) {
		t.Fatal("max_age must be part of the public cache key")
	}
}

func TestPublicProxiesMaxAgeFilter(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "age-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	nowMs := time.Now().UnixMilli()
	for id, lastOK := range map[string]int64{"fresh": nowMs - 60_000, "stale": nowMs - 2*3600_000, "never": 0} {
		mini.SAdd("age-test:validated", id)
		mini.HSet("age-test:proxy:"+id, "url", "http://"+id+".example:8080", "ok_ratio_pct", "100", "last_status", "ok")
		if lastOK > 0 {
			mini.HSet("age-test:proxy:"+id, "last_ok_at_ms", strconv.FormatInt(lastOK, 10))
		}
	}
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	for _, tc := range []struct {
		query string
		count int
	}{
		{"", 3},
		{"max_age=30m", 1},
		{"max_age=1800", 1},
		{"max_age=3h", 2},
	} {
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/proxies?"+tc.query, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("%q: status=%d body=%s", tc.query, recorder.Code, recorder.Body.String())
		}
		var data publicProxiesPayload
		if err := json.Unmarshal(recorder.Body.Bytes(), &data); err != nil {
			t.Fatal(err)
		}
		if data.Count != tc.count {
			t.Fatalf("%q: count=%d want %d", tc.query, data.Count, tc.count)
		}
		if tc.query == "max_age=30m" {
			if p := data.Proxies[0]; p.LastStatus != "ok" || p.LastOkAt != nowMs-60_000 {
				t.Fatalf("freshness fields = %+v", p)
			}
			if !strings.Contains(recorder.Body.String(), `"last_ok_at_ms"`) || !strings.Contains(recorder.Body.String(), `"last_status"`) {
				t.Fatalf("response missing freshness fields: %s", recorder.Body.String())
			}
		}
	}

	for _, bad := range []string{"max_age=abc", "max_age=0", "max_age=-5m"} {
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/proxies?"+bad, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "max_age") {
			t.Fatalf("%q: status=%d body=%s", bad, recorder.Code, recorder.Body.String())
		}
	}
}

func TestInternalAPIRejectsInvalidMaxAge(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "internal-age-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	mux := http.NewServeMux()
	const token = "0123456789abcdef-token"
	registerInternalAPI(mux, &Runner{store: redisStore, metrics: newMetrics()}, token)
	for query, want := range map[string]int{"max_age=bogus": http.StatusBadRequest, "max_age=10m": http.StatusOK} {
		request := httptest.NewRequest(http.MethodGet, "/internal/api/v1/proxies?"+query, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		recorder := httptest.NewRecorder()
		mux.ServeHTTP(recorder, request)
		if recorder.Code != want {
			t.Fatalf("%q: status=%d want %d body=%s", query, recorder.Code, want, recorder.Body.String())
		}
	}
}

func TestInternalAPICountsQueryFailures(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "internal-failure-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	runner := &Runner{store: redisStore, metrics: newMetrics()}
	mux := http.NewServeMux()
	const token = "0123456789abcdef-token"
	registerInternalAPI(mux, runner, token)
	mux.Handle("/metrics", runner.metrics.Handler())
	mini.Close()

	request := httptest.NewRequest(http.MethodGet, "/internal/api/v1/proxies", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d want 503 body=%s", recorder.Code, recorder.Body.String())
	}
	if got := runner.metrics.internalAPIFailures.Load(); got != 1 {
		t.Fatalf("internal API failures = %d, want 1", got)
	}

	recorder = httptest.NewRecorder()
	mux.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"# HELP freeproxyapi_internal_api_query_failures_total ",
		"# TYPE freeproxyapi_internal_api_query_failures_total counter\n",
		"\nfreeproxyapi_internal_api_query_failures_total 1\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestStandardProbeFuncUsesExpectedBody(t *testing.T) {
	if standardProbeFunc("", 3*time.Second) == nil || standardProbeFunc("success", 3*time.Second) == nil {
		t.Fatal("standardProbeFunc returned nil")
	}
}
