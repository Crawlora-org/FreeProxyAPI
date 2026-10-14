package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestRefreshTraceLinksExplorationDecisionToFetchedSource(t *testing.T) {
	dir := t.TempDir()
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	var cooled SourceSpec
	for i := 0; ; i++ {
		candidate := syntheticSource(t, dir, fmt.Sprintf("cooled-%d.txt", i), "192.0.2.201:8080\n")
		id := StableSourceID(candidate.URL, candidate.defaultScheme("http"))
		if exploreThisRefresh(id, 1, 10) {
			cooled = candidate
			break
		}
	}
	active := syntheticSource(t, dir, "active.txt", "192.0.2.202:8080\n")
	runner, _ := newSourceLearningRunner(t, "trace-lifecycle:v1", []SourceSpec{cooled, active}, true)
	runner.config.SourceLearningZeroAdmissionCooldown = time.Hour
	runner.config.SourceLearningZeroAdmissionRefreshes = 3
	ctx := context.Background()
	cooledID := runner.sourceID(cooled)
	for i := 1; i <= 3; i++ {
		if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{cooledID: 0}, base.Add(time.Duration(i-4)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	if err := runner.store.RecordSourceFetchFailure(ctx, cooledID, base); err != nil {
		t.Fatal(err)
	}
	if err := runner.refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	trace := runner.sourceRefreshTraceSnapshot()
	if len(trace.RefreshID) != 32 || trace.Generation != 1 || trace.Status != "completed" || trace.Result != "ok" || trace.CompletedAt == nil {
		t.Fatalf("refresh lifecycle trace = %+v", trace)
	}
	var decision *sourceRefreshTraceDecision
	for i := range trace.Decisions {
		if trace.Decisions[i].SourceID == cooledID {
			decision = &trace.Decisions[i]
			break
		}
	}
	if decision == nil {
		t.Fatal("exploratory source missing from exact decision set")
	}
	if !decision.Selected || !decision.Cooled || !decision.AdmissionCooled || !decision.Explored || decision.FetchStatus != "fetched" || decision.FetchAttempts != 1 || decision.HTTPAttempts != 0 || decision.AdmissionObservationStatus != "recorded" {
		t.Fatalf("exploratory fetch decision = %+v", *decision)
	}
	if strings.Join(decision.Reasons, ",") != "failure_cooldown,zero_admission_cooldown,exploration" {
		t.Fatalf("overlapping reasons = %v", decision.Reasons)
	}
	quality, err := runner.store.LoadSourceQuality(ctx, []string{cooledID}, base)
	if err != nil || quality[cooledID].LastAdmitted == 0 || quality[cooledID].ZeroAdmissionStreak != 0 {
		t.Fatalf("exploration did not import/reset the source after planning: quality=%+v err=%v", quality[cooledID], err)
	}
}

func TestRefreshTraceRecordsFailedCancelledAndCensoredOutcomes(t *testing.T) {
	t.Run("failed", func(t *testing.T) {
		dir := t.TempDir()
		bad := SourceSpec{URL: "file://" + dir + "/missing.txt", Scheme: "http"}
		runner, _ := newSourceLearningRunner(t, "trace-failed:v1", []SourceSpec{bad}, true)
		if err := runner.refresh(context.Background()); err == nil {
			t.Fatal("refresh with only a failed feed succeeded")
		}
		trace := runner.sourceRefreshTraceSnapshot()
		if trace.Status != "failed" || trace.Result != "error" || trace.Generation != 1 || len(trace.Decisions) != 1 || trace.Decisions[0].FetchStatus != "failed" || trace.Decisions[0].MergeStatus != "not_reached" || trace.Decisions[0].AdmissionObservationStatus != "censored" || strings.Join(trace.Decisions[0].AdmissionObservationReasons, ",") != "fetch_failed" {
			t.Fatalf("failed refresh trace = %+v", trace)
		}
	})

	t.Run("partial_failure", func(t *testing.T) {
		dir := t.TempDir()
		good := syntheticSource(t, dir, "good.txt", "192.0.2.210:8080\n")
		bad := SourceSpec{URL: "file://" + filepath.Join(dir, "missing-partial.txt"), Scheme: "http"}
		runner, _ := newSourceLearningRunner(t, "trace-partial-failure:v1", []SourceSpec{good, bad}, true)
		if err := runner.refresh(context.Background()); err != nil {
			t.Fatalf("one failed feed should not abort other source imports: %v", err)
		}
		trace := runner.sourceRefreshTraceSnapshot()
		if trace.Status != "completed" || trace.Result != "partial" || len(trace.Decisions) != 2 {
			t.Fatalf("partial source failure trace = %+v", trace)
		}
		statuses := make(map[string]string, len(trace.Decisions))
		for _, decision := range trace.Decisions {
			statuses[decision.SourceID] = decision.FetchStatus
		}
		if statuses[runner.sourceID(good)] != "fetched" || statuses[runner.sourceID(bad)] != "failed" {
			t.Fatalf("per-source fetch outcomes = %v", statuses)
		}
	})

	t.Run("cancelled", func(t *testing.T) {
		spec := SourceSpec{URL: "file:///synthetic/cancelled-feed", Scheme: "http"}
		runner, _ := newSourceLearningRunner(t, "trace-cancelled:v1", []SourceSpec{spec}, true)
		ctx, cancel := context.WithCancel(context.Background())
		calls := 0
		runner.nowFunc = func() time.Time {
			calls++
			if calls == 3 { // begin, planner snapshot, then first selected fetch start
				cancel()
			}
			return time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
		}
		if err := runner.refresh(ctx); err == nil {
			t.Fatal("cancelled refresh returned nil")
		}
		trace := runner.sourceRefreshTraceSnapshot()
		if trace.Status != "cancelled" || trace.Result != "cancelled" || len(trace.Decisions) != 1 || trace.Decisions[0].FetchStatus != "cancelled" || trace.Decisions[0].AdmissionObservationStatus != "cancelled" {
			t.Fatalf("cancelled refresh trace = %+v", trace)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		dir := t.TempDir()
		var filename string
		for i := 0; i < 1000; i++ {
			candidateName := fmt.Sprintf("truncated-%d.txt", i)
			candidateURL := "file://" + filepath.Join(dir, candidateName)
			if exploreThisRefresh(StableSourceID(candidateURL, "http"), 1, 10) {
				filename = candidateName
				break
			}
		}
		if filename == "" {
			t.Fatal("could not find a source ID in the generation-1 exploration bucket")
		}
		spec := syntheticSource(t, dir, filename, feedLines("trace-truncated", 12))
		runner, _ := newSourceLearningRunner(t, "trace-truncated:v1", []SourceSpec{spec}, true)
		runner.config.SourceLearningZeroAdmissionCooldown = time.Hour
		runner.config.SourceLearningZeroAdmissionRefreshes = 3
		runner.config.MaxRecordsPerSource = 2
		ctx := context.Background()
		id := runner.sourceID(spec)
		base := runner.nowFunc()
		for i := 1; i <= 3; i++ {
			if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 0}, base.Add(time.Duration(i-4)*time.Minute)); err != nil {
				t.Fatal(err)
			}
		}
		if err := runner.refresh(context.Background()); err != nil {
			t.Fatalf("truncated refresh: %v", err)
		}
		trace := runner.sourceRefreshTraceSnapshot()
		if trace.Status != "completed" || trace.Result != "partial" || len(trace.Decisions) != 1 || !trace.Decisions[0].Explored || !trace.Decisions[0].AdmissionCooled || trace.Decisions[0].FetchStatus != "truncated" || !trace.Decisions[0].Truncated || trace.Decisions[0].AdmittedCandidates != 2 || trace.Decisions[0].AdmissionObservationStatus != "censored" || strings.Join(trace.Decisions[0].AdmissionObservationReasons, ",") != "truncated" {
			t.Fatalf("truncated refresh trace = %+v", trace)
		}
		quality, err := runner.store.LoadSourceQuality(ctx, []string{id}, base)
		if err != nil || quality[id].Admitted != 2 || quality[id].AdmissionRefreshes != 3 || quality[id].ZeroAdmissionStreak != 0 || quality[id].LastAdmitted != 0 || !quality[id].LastAdmissionAt.Equal(base.Add(-time.Minute)) {
			t.Fatalf("partial positive truncation changed full-observation history incorrectly: quality=%+v err=%v", quality[id], err)
		}
	})

	t.Run("candidate_cap", func(t *testing.T) {
		runner, mini, _ := capRunner(t, `,"max_candidates":1`, feedLines("trace-candidate-cap", 12))
		runner.config.SourceLearningEnabled = true
		runner.config.MaxCandidates = 0
		if err := runner.refresh(context.Background()); err != nil {
			t.Fatalf("initial refresh: %v", err)
		}
		runner.config.MaxCandidates = 1
		mini.FastForward(time.Hour)
		if err := runner.refresh(context.Background()); err != nil {
			t.Fatalf("candidate-capped refresh: %v", err)
		}
		trace := runner.sourceRefreshTraceSnapshot()
		if trace.Status != "completed" || trace.Result != "partial" || len(trace.Decisions) != 1 || trace.Decisions[0].FetchStatus != "fetched" || trace.Decisions[0].AdmissionObservationStatus != "censored" || strings.Join(trace.Decisions[0].AdmissionObservationReasons, ",") != "candidate_cap" {
			t.Fatalf("candidate-capped refresh trace = %+v", trace)
		}
		id := runner.sourceID(runner.config.SourceSpecs[0])
		quality, err := runner.store.LoadSourceQuality(context.Background(), []string{id}, time.Now())
		if err != nil || quality[id].AdmissionRefreshes != 1 {
			t.Fatalf("candidate-capped source recorded a full admission observation: quality=%+v err=%v", quality[id], err)
		}
	})

}

func TestTraceWriteFailureDoesNotInterruptImports(t *testing.T) {
	dir := t.TempDir()
	spec := syntheticSource(t, dir, "trace-fail-open.txt", "192.0.2.203:8080\n")
	runner, mini := newSourceLearningRunner(t, "trace-write-fail:v1", []SourceSpec{spec}, true)
	if err := mini.Set("trace-write-fail:v1:source-refresh-traces", "wrong-type"); err != nil {
		t.Fatal(err)
	}
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("telemetry write failure interrupted refresh: %v", err)
	}
	stats, err := runner.store.Stats(context.Background())
	if err != nil || stats.Candidates != 1 {
		t.Fatalf("candidate import after trace failure: stats=%+v err=%v", stats, err)
	}
	metrics := httptest.NewRecorder()
	runner.metrics.Handler().ServeHTTP(metrics, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	foundFailureCount := false
	for _, line := range strings.Split(metrics.Body.String(), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "freeproxyapi_source_refresh_trace_write_failures_total" && fields[1] != "0" {
			foundFailureCount = true
			break
		}
	}
	if !foundFailureCount {
		t.Fatal("trace write failure metric was not exposed")
	}
}

func TestSourceRefreshTraceEncodingBoundsMaximumCatalog(t *testing.T) {
	decisions := make([]sourceRefreshTraceDecision, store.MaxLearningSources)
	selectedCount, cooled, exploring, admissionCooled := 0, 0, 0, 0
	for i := range decisions {
		isCooled := i%3 == 0
		isExplored := isCooled && i%30 == 0
		selected := !isCooled || isExplored
		fetchStatus, mergeStatus, admissionStatus := "not_selected", "not_selected", "not_selected"
		var position *int
		if selected {
			positionValue := selectedCount
			position = &positionValue
			selectedCount++
			fetchStatus, mergeStatus, admissionStatus = "fetched", "completed", "recorded"
		}
		reasons := []string{"active"}
		if isCooled {
			cooled++
			admissionCooled++
			reasons = []string{"zero_admission_cooldown"}
		}
		if isExplored {
			exploring++
			reasons = append(reasons, "exploration")
		}
		decisions[i] = sourceRefreshTraceDecision{
			SourceID: fmt.Sprintf("%032x", i+1), Selected: selected, Cooled: isCooled,
			Explored: isExplored, AdmissionCooled: isCooled,
			TrustedPriorityProtected: i%100 == 0, ProbeContractCurrent: true,
			PlanPosition: position, Reasons: reasons, FetchStatus: fetchStatus,
			MergeStatus: mergeStatus, AdmissionObservationStatus: admissionStatus,
		}
	}
	trace := sourceRefreshTrace{
		RefreshID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Generation: 1, StartedAt: time.Now().UTC(),
		Status: "completed", Result: "ok", Configured: len(decisions), Selected: selectedCount, Cooled: cooled, Exploring: exploring, AdmissionCooled: admissionCooled,
		Decisions: decisions,
	}
	completed := time.Now().UTC()
	trace.CompletedAt = &completed
	payload, err := encodeSourceRefreshTrace(trace)
	if err != nil {
		t.Fatalf("encode maximum catalog trace: %v", err)
	}
	if len(payload) > 1<<20 {
		t.Fatalf("compressed maximum catalog trace is %d bytes, exceeds Redis record limit", len(payload))
	}
	decoded, err := decodeSourceRefreshTrace(payload)
	if err != nil || len(decoded.Decisions) != store.MaxLearningSources {
		t.Fatalf("decode maximum catalog trace returned %d decisions, err=%v", len(decoded.Decisions), err)
	}
}

func TestSourceRefreshTraceRouteIsPrivateAndRejectsCorruption(t *testing.T) {
	runner, _, _ := newSourceEfficiencyTestRunner(t, []SourceSpec{{URL: "file:///synthetic/trace-route", Scheme: "http"}})
	runner.config.AdminListenAddr = "127.0.0.1:0"
	const traceID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	now := time.Now().UTC()
	summary, err := json.Marshal(sourceRefreshTraceSummary{RefreshID: traceID, StartedAt: now, Status: "started"})
	if err != nil {
		t.Fatal(err)
	}
	if err := runner.store.SaveSourceRefreshTrace(context.Background(), traceID, []byte("not-json"), summary, now); err != nil {
		t.Fatal(err)
	}
	server, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown()
	public := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(public, httptest.NewRequest(http.MethodGet, "/report/source-refreshes?refresh_id="+traceID, nil))
	if public.Code != http.StatusNotFound {
		t.Fatalf("public trace route status=%d, want 404", public.Code)
	}
	list := httptest.NewRecorder()
	server.admin.Handler.ServeHTTP(list, httptest.NewRequest(http.MethodGet, "/report/source-refreshes", nil))
	if list.Code != http.StatusOK || strings.Contains(list.Body.String(), "not-json") || strings.Contains(list.Body.String(), "decisions") {
		t.Fatalf("trace summary list status=%d body=%s", list.Code, list.Body.String())
	}
	admin := httptest.NewRecorder()
	server.admin.Handler.ServeHTTP(admin, httptest.NewRequest(http.MethodGet, "/report/source-refreshes?refresh_id="+traceID, nil))
	if admin.Code != http.StatusServiceUnavailable || strings.Contains(admin.Body.String(), "not-json") || strings.Contains(admin.Body.String(), "source_id") {
		t.Fatalf("corrupt trace response status=%d body=%s", admin.Code, admin.Body.String())
	}
	if strings.Contains(admin.Body.String(), "http") || strings.Contains(admin.Body.String(), "proxy") {
		t.Fatalf("corrupt trace failure leaked payload data: %s", admin.Body.String())
	}
}

func TestSourceRefreshTraceRouteRequiresLoopbackWithoutAdminListener(t *testing.T) {
	runner, _, _ := newSourceEfficiencyTestRunner(t, []SourceSpec{{URL: "file:///synthetic/trace-loopback", Scheme: "http"}})
	runner.config.AdminListenAddr = ""
	server, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Shutdown()
	remote := httptest.NewRequest(http.MethodGet, "/report/source-refreshes", nil)
	remote.RemoteAddr = "203.0.113.12:30000"
	blocked := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(blocked, remote)
	if blocked.Code != http.StatusNotFound {
		t.Fatalf("non-loopback trace route status=%d, want 404", blocked.Code)
	}
	local := httptest.NewRequest(http.MethodGet, "/report/source-refreshes", nil)
	local.RemoteAddr = "127.0.0.1:30001"
	allowed := httptest.NewRecorder()
	server.server.Handler.ServeHTTP(allowed, local)
	if allowed.Code != http.StatusOK {
		t.Fatalf("loopback trace route status=%d body=%s, want 200", allowed.Code, allowed.Body.String())
	}
}
