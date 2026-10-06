package monitor

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMetricsExposeDynamicGaugeFamilies(t *testing.T) {
	m := newMetrics()
	m.SetLabeledGauge(`freeproxyapi_validated_by_country{country="US"}`, 7)
	m.SetSourceUnique(123)
	m.SetSourceRefreshStats(456, 7)
	m.AddProbeTraffic(12, 34)
	m.AddDiscarded(3)
	m.AddSourceFetchRetries(2)
	m.AddSourceFetchRecovered(1)

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, want := range []string{
		"# TYPE freeproxyapi_validated_by_country gauge",
		`freeproxyapi_validated_by_country{country="US"} 7`,
		"freeproxyapi_source_unique 123",
		"freeproxyapi_source_records_parsed 456",
		"freeproxyapi_sources_succeeded 7",
		"freeproxyapi_probe_upload_bytes_total 12",
		"freeproxyapi_probe_download_bytes_total 34",
		"freeproxyapi_candidates_discarded_total 3",
		`freeproxyapi_source_fetch_failures_by_reason_total{reason="timeout"} 0`,
		"freeproxyapi_source_fetch_retries_total 2",
		"freeproxyapi_source_fetch_recovered_total 1",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}

func TestMetricsReplaceLabeledGaugesIsAtomicForScrapes(t *testing.T) {
	m := newMetrics()
	const perSet = 40
	sets := [2]map[string]int64{{}, {}}
	for i := 0; i < perSet; i++ {
		sets[0][fmt.Sprintf(`freeproxyapi_validated_by_country{country="A%02d"}`, i)] = 1
		sets[1][fmt.Sprintf(`freeproxyapi_validated_by_asn{asn="AS%02d"}`, i)] = 2
	}
	m.ReplaceLabeledGauges(sets[0])

	stop := make(chan struct{})
	var writer sync.WaitGroup
	writer.Add(1)
	go func() {
		defer writer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				m.ReplaceLabeledGauges(sets[i%2])
			}
		}
	}()
	for i := 0; i < 300; i++ {
		recorder := httptest.NewRecorder()
		m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		text := recorder.Body.String()
		country := strings.Count(text, "freeproxyapi_validated_by_country{")
		asn := strings.Count(text, "freeproxyapi_validated_by_asn{")
		if !(country == perSet && asn == 0) && !(country == 0 && asn == perSet) {
			close(stop)
			writer.Wait()
			t.Fatalf("scrape %d saw partial labeled set: country=%d asn=%d", i, country, asn)
		}
	}
	close(stop)
	writer.Wait()
}

func TestMetricsLabeledCounterFamiliesOmitUnlabeledTotal(t *testing.T) {
	m := newMetrics()
	m.RecordSourceRefresh("ok")
	m.RecordProbeResult(true)
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		"# TYPE freeproxyapi_source_refreshes_total counter",
		`freeproxyapi_source_refreshes_total{result="ok"} 1`,
		"# TYPE freeproxyapi_probe_results_total counter",
		`freeproxyapi_probe_results_total{outcome="ok"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "freeproxyapi_source_refreshes_total ") || strings.HasPrefix(line, "freeproxyapi_probe_results_total ") {
			t.Fatalf("unlabeled total would double-count under sum(): %q", line)
		}
	}
	if strings.Contains(text, "# TYPE freeproxyapi_probe_results_total gauge") {
		t.Fatalf("labeled counter family re-typed as gauge:\n%s", text)
	}
}

func TestMetricsConcurrentFixedCountersAndReads(t *testing.T) {
	m := newMetrics()
	const writers = 16
	const iterations = 100
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				m.RecordSourceRefresh("ok")
				m.RecordProbeResult(j%2 == 0)
				recorder := httptest.NewRecorder()
				m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
			}
		}()
	}
	wg.Wait()
	if got := m.sourceRefreshTotal[0].Load(); got != writers*iterations {
		t.Fatalf("source refresh count = %d, want %d", got, writers*iterations)
	}
	if got := m.probeResultsTotal[0].Load() + m.probeResultsTotal[1].Load(); got != writers*iterations {
		t.Fatalf("probe result count = %d, want %d", got, writers*iterations)
	}
}

func TestMetricsInflightUsesAtomicDeltas(t *testing.T) {
	m := newMetrics()
	const workers = 64
	start := make(chan struct{})
	release := make(chan struct{})
	entered := make(chan struct{}, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			m.AddInflight(1)
			entered <- struct{}{}
			<-release
			m.AddInflight(-1)
		}()
	}
	close(start)
	for i := 0; i < workers; i++ {
		<-entered
	}
	if got := m.inflightGauge.Load(); got != workers {
		t.Fatalf("inflight = %d, want %d", got, workers)
	}
	close(release)
	wg.Wait()
	if got := m.inflightGauge.Load(); got != 0 {
		t.Fatalf("final inflight = %d, want 0", got)
	}
}

func TestMetricsExposePendingSchedule(t *testing.T) {
	m := newMetrics()
	now := time.Date(2026, 9, 13, 12, 0, 10, 0, time.UTC)
	m.SetPendingSchedule(3, now.Add(-25*time.Second), true, now)

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, want := range []string{
		"freeproxyapi_pending_due 3",
		"freeproxyapi_pending_oldest_due_age_seconds 25",
		"freeproxyapi_pending_next_due_in_seconds 0",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}

	m.SetPendingSchedule(0, now.Add(time.Hour), true, now)
	recorder = httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, _ = io.ReadAll(recorder.Result().Body)
	text = string(body)
	for _, want := range []string{
		"freeproxyapi_pending_due 0",
		"freeproxyapi_pending_oldest_due_age_seconds 0",
		"freeproxyapi_pending_next_due_in_seconds 3600",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("future schedule metrics missing %q:\n%s", want, text)
		}
	}
}

func TestMetricsExposeTargetLabeledProbeMetrics(t *testing.T) {
	m := newMetrics()
	m.RecordProbeResultForTarget("https://HTTPBIN.org/get?probe=redacted", true, 150*time.Millisecond)
	m.RecordProbeResultForTarget("https://httpbin.org/get", false, 2*time.Second)

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(recorder.Result().Body)
	if err != nil {
		t.Fatal(err)
	}

	text := string(body)
	for _, want := range []string{
		`freeproxyapi_probe_results_by_target_total{outcome="ok",target="https://httpbin.org/get"} 1`,
		`freeproxyapi_probe_results_by_target_total{outcome="failed",target="https://httpbin.org/get"} 1`,
		`freeproxyapi_probe_duration_seconds_bucket{le="0.25",target="https://httpbin.org/get"} 1`,
		`freeproxyapi_probe_duration_seconds_count{target="https://httpbin.org/get"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}
