package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func newSourceEfficiencyTestRunner(t *testing.T, specs []SourceSpec) (*Runner, *miniredis.Miniredis, *time.Time) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "source-efficiency-test:v1")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	now := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	runner := &Runner{
		config: Config{SourceLearningEnabled: true, SourceSpecs: specs, DefaultProxyScheme: "http", StatsLeaderEnabled: true},
		store:  redisStore, metrics: newMetrics(),
		nowFunc: func() time.Time { return now },
	}
	return runner, mini, &now
}

func TestSourceEfficiencyReportIsBoundedAndDoesNotExposeURLs(t *testing.T) {
	ctx := context.Background()
	spec := SourceSpec{URL: "https://secret-source.example/feed?token=must-not-escape", Scheme: "http"}
	runner, mini, now := newSourceEfficiencyTestRunner(t, []SourceSpec{spec})
	id := runner.sourceID(spec)
	if _, err := runner.store.UpsertPrioritizedSourceCounts(ctx, []string{"http://proxy.example:8080"}, *now, id); err != nil {
		t.Fatal(err)
	}
	if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 1}, *now); err != nil {
		t.Fatal(err)
	}
	if err := runner.store.RecordSourceFetchCost(ctx, id, 2, 4096, 75, *now); err != nil {
		t.Fatal(err)
	}
	mini.HSet("source-efficiency-test:v1:source-quality:"+id,
		"probe_contract", runner.sourceProbeContract(),
		"probe_completions", "3", "probe_successful_completions", "2", "probe_duration_ms", "55")
	first := runner.sourceEfficiencyReport(ctx)
	if first.Status != "ok" || len(first.Sources) != 1 || first.Sources[0].SourceID != id || first.Sources[0].EffectiveScheme != "http" {
		t.Fatalf("report identity/status = %+v", first)
	}
	entry := first.Sources[0]
	if !entry.AdmissionMeasured || entry.Admitted == nil || *entry.Admitted != 1 || entry.AdmissionRefreshes == nil || *entry.AdmissionRefreshes != 1 {
		t.Fatalf("admission report = %+v", entry)
	}
	if !entry.FetchCostMeasured || entry.HTTPAttempts == nil || *entry.HTTPAttempts != 2 || entry.NetworkBytes == nil || *entry.NetworkBytes != 4096 {
		t.Fatalf("fetch-cost report = %+v", entry)
	}
	if !entry.ProbeCostMeasured || entry.ProbeCompletions == nil || *entry.ProbeCompletions != 3 || entry.ProbeSuccessful == nil || *entry.ProbeSuccessful != 2 {
		t.Fatalf("probe-cost report = %+v", entry)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-source.example", "must-not-escape", "proxy.example", "http://", "https://"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("report leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestSourceEfficiencyLegacyFieldsRemainUnmeasuredAndCountsOmitWhenUnknown(t *testing.T) {
	ctx := context.Background()
	spec := SourceSpec{URL: "https://source.example/feed", Scheme: "socks5"}
	runner, _, now := newSourceEfficiencyTestRunner(t, []SourceSpec{spec})
	id := runner.sourceID(spec)
	if err := runner.store.RecordSourceRefreshSuccess(ctx, id, 3, 2, *now); err != nil {
		t.Fatal(err)
	}
	runner.setValidatedSlices(validatedSlices{
		BySource: map[string]int64{id: 4}, RoutingBySource: map[string]int64{id: 3},
		SourceCountsObserved: false,
	})
	report := runner.sourceEfficiencyReport(ctx)
	if report.Status != "ok" || len(report.Sources) != 1 {
		t.Fatalf("report = %+v", report)
	}
	entry := report.Sources[0]
	if entry.AdmissionMeasured || entry.Admitted != nil || entry.FetchCostMeasured || entry.HTTPAttempts != nil || entry.ProbeCostMeasured || entry.ProbeCompletions != nil {
		t.Fatalf("legacy counters were presented as measured: %+v", entry)
	}
	if report.SourceCountsObserved || entry.Validated != nil || entry.RoutingEligible != nil {
		t.Fatalf("unobserved source counts were emitted: report=%+v", report)
	}
}

func TestSourceEfficiencyBatchesCatalogReadsAndEnforcesBound(t *testing.T) {
	ctx := context.Background()
	specs := make([]SourceSpec, store.MaxSourceQualityReadBatch+1)
	for i := range specs {
		specs[i] = SourceSpec{URL: "https://source-" + strconv.Itoa(i) + ".example/feed", Scheme: "http"}
	}
	runner, _, _ := newSourceEfficiencyTestRunner(t, specs)
	report := runner.sourceEfficiencyReport(ctx)
	if report.Status != "ok" || len(report.Sources) != len(specs) {
		t.Fatalf("batched source report count=%d status=%q", len(report.Sources), report.Status)
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "source-0.example") || strings.Contains(string(encoded), "/feed") {
		t.Fatal("bounded source report exposed configured feed URL content")
	}
	tooMany := make([]SourceSpec, sourceEfficiencyMaxIDs+1)
	for i := range tooMany {
		tooMany[i] = SourceSpec{URL: "https://bounded-" + strconv.Itoa(i) + ".example/feed", Scheme: "http"}
	}
	runner.config.SourceSpecs = tooMany
	bounded := runner.sourceEfficiencyReport(ctx)
	if bounded.Status != "unavailable" || bounded.QualityError == "" || len(bounded.Sources) != 0 {
		t.Fatalf("oversized source catalog was not rejected: %+v", bounded)
	}
}

func TestSourceEfficiencyReportToleratesUnsetTestClock(t *testing.T) {
	runner, _, _ := newSourceEfficiencyTestRunner(t, nil)
	runner.nowFunc = nil
	report := runner.sourceEfficiencyReport(context.Background())
	if report.Status != "ok" || report.CheckedAt == "" {
		t.Fatalf("report with default wall clock = %+v", report)
	}
}

func TestSourceEfficiencyCacheRefreshAndCountFreshness(t *testing.T) {
	ctx := context.Background()
	spec := SourceSpec{URL: "https://source.example/feed", Scheme: "http"}
	runner, _, now := newSourceEfficiencyTestRunner(t, []SourceSpec{spec})
	id := runner.sourceID(spec)
	if err := runner.store.RecordSourceFetchCost(ctx, id, 1, 10, 5, *now); err != nil {
		t.Fatal(err)
	}
	first := runner.sourceEfficiencyReport(ctx)
	if first.Sources[0].NetworkBytes == nil || *first.Sources[0].NetworkBytes != 10 {
		t.Fatalf("initial report = %+v", first.Sources[0])
	}
	if err := runner.store.RecordSourceFetchCost(ctx, id, 1, 20, 5, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	*now = now.Add(30 * time.Second)
	cached := runner.sourceEfficiencyReport(ctx)
	if cached.Sources[0].NetworkBytes == nil || *cached.Sources[0].NetworkBytes != 10 || cached.QualityStale {
		t.Fatalf("one-minute cache was not used: %+v", cached)
	}
	*now = now.Add(31 * time.Second)
	refreshed := runner.sourceEfficiencyReport(ctx)
	if refreshed.Sources[0].NetworkBytes == nil || *refreshed.Sources[0].NetworkBytes != 30 || refreshed.QualityStale {
		t.Fatalf("expired quality cache was not refreshed: %+v", refreshed)
	}
	runner.setValidatedSlices(validatedSlices{
		BySource: map[string]int64{id: 7}, RoutingBySource: map[string]int64{id: 4},
		SourceCountsObserved: true, SourceCountsAtMS: now.Add(-2 * time.Minute).UnixMilli(),
	})
	counts := runner.sourceEfficiencyReport(ctx)
	if !counts.SourceCountsObserved || counts.Sources[0].Validated == nil || *counts.Sources[0].Validated != 7 ||
		counts.Sources[0].RoutingEligible == nil || *counts.Sources[0].RoutingEligible != 4 || counts.SourceCountsAgeSeconds == nil || *counts.SourceCountsAgeSeconds != 120 {
		t.Fatalf("observed stale counts lost their value/freshness: %+v", counts)
	}
}

func TestSourceEfficiencyMetricsOnlyPublishOnLeaderAndReplaceStaleSeries(t *testing.T) {
	ctx := context.Background()
	spec := SourceSpec{URL: "https://source.example/feed", Scheme: "http"}
	runner, _, now := newSourceEfficiencyTestRunner(t, []SourceSpec{spec})
	id := runner.sourceID(spec)
	if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 0}, *now); err != nil {
		t.Fatal(err)
	}
	series := func(metric string) string { return metric + `{source_id="` + id + `"}` }
	// Followers must clear old source series instead of exporting stale gauges.
	runner.metrics.ReplaceSourceGauges(map[string]int64{series("freeproxyapi_source_admitted_total"): 999})
	runner.publishSourceEfficiency(ctx, validatedSlices{}, *now)
	if exposition := metricsExposition(t, runner.metrics.Handler()); strings.Contains(exposition, "freeproxyapi_source_admitted_total") {
		t.Fatalf("follower retained source series: %s", exposition)
	}
	runner.setStatsLeader(true)
	runner.setValidatedSlices(validatedSlices{
		BySource: map[string]int64{id: 5}, RoutingBySource: map[string]int64{id: 2},
		SourceCountsObserved: true, SourceCountsAtMS: now.UnixMilli(),
	})
	runner.publishSourceEfficiency(ctx, runner.validatedSlicesSnapshot(), *now)
	exposition := metricsExposition(t, runner.metrics.Handler())
	for _, want := range []string{
		series("freeproxyapi_source_admitted_total"),
		series("freeproxyapi_source_validated"),
		series("freeproxyapi_source_routing_eligible"),
		`freeproxyapi_source_efficiency_counts_observed 1`,
	} {
		if !strings.Contains(exposition, want) {
			t.Fatalf("source exposition missing %q:\n%s", want, exposition)
		}
	}
	for metric := range sourceEfficiencyMetricHelp {
		if sourceEfficiencyMetricType[metric] == "" {
			t.Fatalf("metric %q lacks a type", metric)
		}
	}
}

func metricsExposition(t *testing.T, handler interface {
	ServeHTTP(http.ResponseWriter, *http.Request)
}) string {
	t.Helper()
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	return response.Body.String()
}

func TestSourceEfficiencyMatchingLegacyContractDoesNotInventProbeCost(t *testing.T) {
	r, mini, now := newSourceEfficiencyTestRunner(t, []SourceSpec{{URL: "https://feeds.example/one", Scheme: "http"}})
	id := r.sourceID(r.config.SourceSpecs[0])
	mini.HSet("source-efficiency-test:v1:source-quality:"+id, "probe_contract", r.sourceProbeContract(), "probe_samples", "12", "probe_passes", "1", "probe_updated_ms", strconv.FormatInt(now.UnixMilli(), 10))
	row := r.sourceEfficiencyReport(context.Background()).Sources[0]
	if row.ProbeCostMeasured || row.ProbeCompletions != nil {
		t.Fatal("matching legacy samples were labeled as measured cost")
	}
}
func TestSourceEfficiencyDisabledDoesNotReadOrEmitSources(t *testing.T) {
	r, _, _ := newSourceEfficiencyTestRunner(t, []SourceSpec{{URL: "https://feeds.example/one", Scheme: "http"}})
	r.config.SourceLearningEnabled = false
	report := r.sourceEfficiencyReport(context.Background())
	if report.Status != "disabled" || len(report.Sources) != 0 {
		t.Fatal("disabled instrumentation exposed source metrics")
	}
}

func TestSourceAttributionAggregateIncludesUnattributedAndRetiredRows(t *testing.T) {
	spec := SourceSpec{URL: "https://feeds.example/one", Scheme: "http"}
	r, mini, now := newSourceEfficiencyTestRunner(t, []SourceSpec{spec})
	sid := r.sourceID(spec)
	for _, id := range []string{"known", "retired", "missing"} {
		mini.SAdd("source-efficiency-test:v1:validated", id)
		mini.HSet("source-efficiency-test:v1:proxy:"+id, "country", "US", "ok_ratio_pct", "100", "success_streak", "3", "https_ok", "1", "https_checked_at_ms", strconv.FormatInt(now.UnixMilli(), 10), "tampered", "0", "tamper_checked_at_ms", strconv.FormatInt(now.UnixMilli(), 10))
	}
	mini.HSet("source-efficiency-test:v1:proxy:known", "source_id", sid)
	mini.HSet("source-efficiency-test:v1:proxy:retired", "source_id", strings.Repeat("a", 32))
	mini.HSet("source-efficiency-test:v1:proxy:missing", "https_ok", "0")
	slices := r.aggregateValidatedSlices(context.Background())
	if slices.BySource[sid] != 1 || slices.UnattributedValidated != 2 || slices.RoutingBySource[sid] != 1 || slices.UnattributedRouting != 1 {
		t.Fatalf("attribution=%+v", slices)
	}
	r.setValidatedSlices(slices)
	report := r.sourceEfficiencyReport(context.Background())
	if report.UnattributedValidated == nil || *report.UnattributedValidated != 2 || report.UnattributedRouting == nil || *report.UnattributedRouting != 1 {
		t.Fatalf("report=%+v", report)
	}
}
