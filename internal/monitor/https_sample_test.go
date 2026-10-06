package monitor

import (
	"context"
	"errors"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func newHTTPSSampleRunner(t *testing.T, limit int, outcome probe.HTTPSResult) (*Runner, *atomic.Int64) {
	t.Helper()
	runner := newBudgetTestRunner(t, limit)
	runner.config.HTTPSProbeTarget = "https://https-target.invalid/success.txt"
	runner.config.HTTPSProbeEvery = 4
	runner.accuracy.sampleIntN = func(int) int { return 0 }
	var calls atomic.Int64
	runner.accuracy.httpsProbeFunc = func(context.Context, string) probe.HTTPSResult {
		calls.Add(1)
		return outcome
	}
	return runner, &calls
}

func TestSampleHTTPSRecordsOutcome(t *testing.T) {
	runner, calls := newHTTPSSampleRunner(t, 10, probe.HTTPSResult{OK: true, TunnelScheme: "http", ProxyTLSFallback: true})
	meta := store.OutcomeMeta{}
	runner.sampleHTTPS(context.Background(), "https://192.0.2.10:443", probe.Result{OK: true}, &meta)
	if calls.Load() != 1 || !meta.HTTPSChecked || !meta.HTTPSOK || meta.HTTPSTunnelScheme != "http" {
		t.Fatalf("calls=%d meta=%+v", calls.Load(), meta)
	}
	if runner.accuracy.metrics.httpsResults[1].Load() != 1 || runner.accuracy.metrics.httpsProxyTLSFallback.Load() != 1 {
		t.Fatal("https metrics not recorded")
	}

	failed, _ := newHTTPSSampleRunner(t, 10, probe.HTTPSResult{Error: "CONNECT rejected"})
	meta = store.OutcomeMeta{}
	failed.sampleHTTPS(context.Background(), "http://192.0.2.10:8080", probe.Result{OK: true}, &meta)
	if !meta.HTTPSChecked || meta.HTTPSOK || meta.HTTPSTunnelScheme != "" {
		t.Fatalf("failed sample meta=%+v", meta)
	}
}

func TestSampleHTTPSSkips(t *testing.T) {
	cases := map[string]func(r *Runner) (probe.Result, context.Context){
		"disabled": func(r *Runner) (probe.Result, context.Context) {
			r.config.HTTPSProbeTarget = ""
			return probe.Result{OK: true}, context.Background()
		},
		"standard probe failed": func(r *Runner) (probe.Result, context.Context) {
			return probe.Result{Error: "timeout"}, context.Background()
		},
		"not sampled": func(r *Runner) (probe.Result, context.Context) {
			r.accuracy.sampleIntN = func(int) int { return 3 }
			return probe.Result{OK: true}, context.Background()
		},
		"shutdown": func(r *Runner) (probe.Result, context.Context) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return probe.Result{OK: true}, ctx
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			runner, calls := newHTTPSSampleRunner(t, 10, probe.HTTPSResult{OK: true})
			result, ctx := setup(runner)
			meta := store.OutcomeMeta{}
			runner.sampleHTTPS(ctx, "http://192.0.2.10:8080", result, &meta)
			if calls.Load() != 0 || meta.HTTPSChecked {
				t.Fatalf("calls=%d meta=%+v", calls.Load(), meta)
			}
		})
	}
}

func TestSampleHTTPSTakesBudgetPermit(t *testing.T) {
	runner, calls := newHTTPSSampleRunner(t, 1, probe.HTTPSResult{OK: true})
	if allowed, err := runner.takeProbePermit(context.Background()); err != nil || !allowed {
		t.Fatalf("seed permit: allowed=%t err=%v", allowed, err)
	}
	meta := store.OutcomeMeta{}
	runner.sampleHTTPS(context.Background(), "http://192.0.2.10:8080", probe.Result{OK: true}, &meta)
	if calls.Load() != 0 || meta.HTTPSChecked {
		t.Fatal("exhausted budget must skip the HTTPS request")
	}
	if runner.accuracy.metrics.httpsBudgetSkipped.Load() != 1 {
		t.Fatal("budget skip not counted")
	}
}

func TestSampleHTTPSFailureIgnoredWhileControlUnhealthy(t *testing.T) {
	runner, calls := newHTTPSSampleRunner(t, 10, probe.HTTPSResult{Error: "timeout"})
	runner.config.ControlProbeFailureThreshold = 1
	runner.recordControlResult(errors.New("origin down"))
	meta := store.OutcomeMeta{}
	runner.sampleHTTPS(context.Background(), "http://192.0.2.10:8080", probe.Result{OK: true}, &meta)
	if calls.Load() != 1 || meta.HTTPSChecked {
		t.Fatalf("failure during control outage must not be recorded: meta=%+v", meta)
	}
}

func TestProxyFilterParsesHTTPS(t *testing.T) {
	filter, err := proxyFilter(httptest.NewRequest("GET", "/proxies?https=1&anonymity=elite", nil))
	if err != nil || !filter.HTTPS || filter.Anonymity != "elite" {
		t.Fatalf("filter=%+v err=%v", filter, err)
	}
	if publicProxyCacheKey(filter, 0) == publicProxyCacheKey(store.ProxyFilter{Anonymity: "elite"}, 0) {
		t.Fatal("https filter must change the cache key")
	}
	aged := filter
	aged.ClassificationMaxAgeMs = int64(time.Hour / time.Millisecond)
	if publicProxyCacheKey(filter, 0) == publicProxyCacheKey(aged, 0) {
		t.Fatal("classification max age must change the cache key")
	}
}
