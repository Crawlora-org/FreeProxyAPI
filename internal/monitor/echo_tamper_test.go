package monitor

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestCheckProxyAnonymityKeepsTamperOnlyVerdict(t *testing.T) {
	runner := newBudgetTestRunner(t, 10)
	calls := 0
	runner.echoCheckFunc = func(_ context.Context, _, echoURL, _ string, _ time.Duration) probe.AnonymityResult {
		calls++
		if strings.Contains(echoURL, "echo-one") {
			return probe.AnonymityResult{TamperChecked: true, Tampered: true, TamperReason: "body"}
		}
		return probe.AnonymityResult{}
	}
	result := runner.checkProxyAnonymity(context.Background(), "http://192.0.2.10:8080")
	if calls != 2 || result.Class != "" || !result.TamperChecked || !result.Tampered {
		t.Fatalf("calls=%d result=%+v", calls, result)
	}
}

func TestRecordEchoTamperSetsMetaAndMetrics(t *testing.T) {
	runner := &Runner{metrics: newMetrics()}
	meta := store.OutcomeMeta{}
	runner.recordEchoTamper(probe.AnonymityResult{Class: "elite"}, &meta)
	if meta.TamperChecked {
		t.Fatalf("unchecked result must not record tamper meta: %+v", meta)
	}
	runner.recordEchoTamper(probe.AnonymityResult{Class: "elite", TamperChecked: true}, &meta)
	if !meta.TamperChecked || meta.Tampered {
		t.Fatalf("clean meta = %+v", meta)
	}
	meta = store.OutcomeMeta{}
	runner.recordEchoTamper(probe.AnonymityResult{Class: "elite", TamperChecked: true, Tampered: true, TamperReason: "header:server"}, &meta)
	if !meta.TamperChecked || !meta.Tampered {
		t.Fatalf("tampered meta = %+v", meta)
	}

	var b strings.Builder
	runner.accuracy.metrics.appendTo(&b)
	for _, line := range []string{
		"# TYPE freeproxyapi_echo_tamper_results_total counter\n",
		"freeproxyapi_echo_tamper_results_total{outcome=\"clean\"} 1\n",
		"freeproxyapi_echo_tamper_results_total{outcome=\"tampered\"} 1\n",
		"freeproxyapi_echo_tamper_reasons_total{reason=\"header\"} 1\n",
		"freeproxyapi_echo_tamper_reasons_total{reason=\"body\"} 0\n",
	} {
		if !strings.Contains(b.String(), line) {
			t.Fatalf("exposition missing %q:\n%s", line, b.String())
		}
	}
}

func TestProxyFilterParsesExcludeTampered(t *testing.T) {
	for _, path := range []string{"/proxies?exclude_tampered=true", "/internal/api/v1/proxies?exclude_tampered=true&anonymity=elite"} {
		filter, err := proxyFilter(httptest.NewRequest("GET", path, nil))
		if err != nil || !filter.ExcludeTampered {
			t.Fatalf("%s: filter=%+v err=%v", path, filter, err)
		}
	}
	filter, err := proxyFilter(httptest.NewRequest("GET", "/proxies?exclude_tampered=false", nil))
	if err != nil || filter.ExcludeTampered {
		t.Fatalf("exclude_tampered=false parsed as %+v err=%v", filter, err)
	}
	if publicProxyCacheKey(store.ProxyFilter{ExcludeTampered: true}, 0) == publicProxyCacheKey(store.ProxyFilter{}, 0) {
		t.Fatal("exclude_tampered must change the cache key")
	}
}
