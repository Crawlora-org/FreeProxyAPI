package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
)

func TestMetricsExposeProbeOutcomesByKindAndReason(t *testing.T) {
	m := newMetrics()
	m.RecordProbeOutcome(false, probe.CodeConnectTimeout, false, 3*time.Second)
	m.RecordProbeOutcome(false, probe.CodeConnectTimeout, false, 3*time.Second)
	m.RecordProbeOutcome(true, probe.CodeOK, true, 200*time.Millisecond)
	m.RecordProbeOutcome(true, "not-a-code", false, time.Second)
	m.RecordProbeOutcome(true, probe.CodeOK, false, time.Second) // inconsistent input

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		"# TYPE freeproxyapi_probe_outcomes_total counter",
		`freeproxyapi_probe_outcomes_total{kind="first",outcome="failed",reason="connect_timeout"} 2`,
		`freeproxyapi_probe_outcomes_total{kind="retest",outcome="ok",reason="ok"} 1`,
		`freeproxyapi_probe_outcomes_total{kind="retest",outcome="failed",reason="other"} 2`,
		`freeproxyapi_probe_outcomes_total{kind="first",outcome="ok",reason="ok"} 0`,
		"# TYPE freeproxyapi_probe_outcome_duration_seconds histogram",
		`freeproxyapi_probe_outcome_duration_seconds_bucket{kind="first",outcome="failed",le="2"} 0`,
		`freeproxyapi_probe_outcome_duration_seconds_bucket{kind="first",outcome="failed",le="5"} 2`,
		`freeproxyapi_probe_outcome_duration_seconds_count{kind="retest",outcome="ok"} 1`,
		// Existing families keep their names and semantics.
		"# TYPE freeproxyapi_probe_results_total counter",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	if strings.Contains(text, `outcome="ok",reason="connect_timeout"`) || strings.Contains(text, `outcome="failed",reason="ok"`) {
		t.Fatalf("impossible outcome/reason combination exposed:\n%s", text)
	}
	// Bounded cardinality: 2 kinds x (1 ok + every failure reason).
	if got, want := strings.Count(text, "freeproxyapi_probe_outcomes_total{"), 2*len(probe.ErrorCodes()); got != want {
		t.Fatalf("outcome series = %d, want %d", got, want)
	}
}

func TestProbeDetailStoresShortCode(t *testing.T) {
	long := probe.Result{Error: `Get "http://probe.example/healthz?_fpa=00112233": proxyconnect tcp: dial tcp 203.0.113.9:8080: i/o timeout`}
	if got := probeDetail(long); got != probe.CodeConnectTimeout {
		t.Fatalf("probeDetail = %q, want %q", got, probe.CodeConnectTimeout)
	}
	if got := probeDetail(probe.Result{OK: true, StatusCode: 204}); got != "" {
		t.Fatalf("successful probe detail = %q, want empty", got)
	}
	for _, code := range probe.ErrorCodes() {
		if len(code) > 32 {
			t.Fatalf("code %q exceeds 32 bytes", code)
		}
	}
}
