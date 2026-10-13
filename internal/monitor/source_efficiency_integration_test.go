package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestSourceAdmissionCooldownUsesStoredFullRefreshObservations(t *testing.T) {
	ctx := context.Background()
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	runner, _, clock := newSourceEfficiencyTestRunner(t, []SourceSpec{{URL: "file:///synthetic/feed", Scheme: "http"}})
	*clock = now
	id := runner.sourceID(runner.config.SourceSpecs[0])
	candidate := SourceLearningCandidate{ID: id, FallbackPriority: 20}
	config := SourceLearningConfig{Enabled: true, MinSamples: 8, ZeroAdmissionCooldown: time.Hour, ZeroAdmissionRefreshes: 3, ExplorationInterval: 10}
	for i := 1; i <= 3; i++ {
		if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 0}, now.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	quality, err := runner.store.LoadSourceQuality(ctx, []string{id}, now.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	candidate.Quality = quality[id]
	plan := BuildSourceLearningPlan([]SourceLearningCandidate{candidate}, 1, now.Add(3*time.Minute), config)
	if len(plan) != 0 {
		t.Fatalf("three stored zero admissions should cool source: %+v", plan)
	}
	// The stable exploration slot still admits the cooled feed periodically.
	for generation := uint64(1); generation < 20; generation++ {
		plan = BuildSourceLearningPlan([]SourceLearningCandidate{candidate}, generation, now.Add(3*time.Minute), config)
		if len(plan) == 1 {
			if !plan[0].Exploration || !plan[0].AdmissionCooled {
				t.Fatalf("exploration plan = %+v", plan[0])
			}
			break
		}
	}
	if len(plan) != 1 {
		t.Fatal("cooled source was never selected for deterministic exploration")
	}
	if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 1}, now.Add(4*time.Minute)); err != nil {
		t.Fatal(err)
	}
	quality, err = runner.store.LoadSourceQuality(ctx, []string{id}, now.Add(4*time.Minute))
	if err != nil || quality[id].ZeroAdmissionStreak != 0 || quality[id].LastAdmitted != 1 {
		t.Fatalf("positive admission did not reset stored streak: %+v err=%v", quality[id], err)
	}
	candidate.Quality = quality[id]
	if plan = BuildSourceLearningPlan([]SourceLearningCandidate{candidate}, 2, now.Add(4*time.Minute), config); len(plan) != 1 || plan[0].AdmissionCooled {
		t.Fatalf("positive admission remained cooled: %+v", plan)
	}
	// Legacy quality with fewer than three measured observations fails open.
	if err := runner.store.RecordSourceAdmissionRefresh(ctx, map[string]int64{id: 0}, now.Add(5*time.Minute)); err != nil {
		t.Fatal(err)
	}
	quality, err = runner.store.LoadSourceQuality(ctx, []string{id}, now.Add(5*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	legacy := SourceLearningCandidate{ID: "legacy-feed", FallbackPriority: 0, Quality: store.SourceQuality{ZeroAdmissionStreak: 2, LastAdmissionAt: now}}
	if got := BuildSourceLearningPlan([]SourceLearningCandidate{legacy}, 1, now.Add(5*time.Minute), config); len(got) != 1 || got[0].AdmissionCooled {
		t.Fatalf("legacy source with fewer than three observations was cooled: %+v", got)
	}
}

func TestCappedRefreshDoesNotRecordAdmissionObservation(t *testing.T) {
	runner, mini, _ := capRunner(t, `, "max_candidates": 1`, feedLines("cap", 12))
	runner.config.MaxCandidates = 0
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	runner.config.MaxCandidates = 1
	runner.config.SourceLearningEnabled = true
	mini.FastForward(time.Hour)
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("capped refresh: %v", err)
	}
	id := runner.sourceID(runner.config.SourceSpecs[0])
	quality, err := runner.store.LoadSourceQuality(context.Background(), []string{id}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if quality[id].AdmissionRefreshes != 0 {
		t.Fatalf("capped source recorded %d admission observations", quality[id].AdmissionRefreshes)
	}
}

func TestSourceReportRouteIsAdminOnly(t *testing.T) {
	public, err := startHealthServer("127.0.0.1:0", adminTestRunner(t, Config{AdminListenAddr: "127.0.0.1:0"}))
	if err != nil {
		t.Fatal(err)
	}
	defer public.Shutdown()
	request := httptest.NewRequest(http.MethodGet, "/report/sources", nil)
	response := httptest.NewRecorder()
	public.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("public source report status=%d, want 404", response.Code)
	}
	response = httptest.NewRecorder()
	public.admin.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/report/sources", nil))
	if response.Code == http.StatusNotFound {
		t.Fatalf("admin source report route missing: %s", response.Body.String())
	}
}

func TestTruncatedRefreshDoesNotRecordAdmissionObservation(t *testing.T) {
	runner, _, _ := capRunner(t, `, "max_records_per_source": 2`, feedLines("truncated", 12))
	runner.config.SourceLearningEnabled = true
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	id := runner.sourceID(runner.config.SourceSpecs[0])
	quality, err := runner.store.LoadSourceQuality(context.Background(), []string{id}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if quality[id].AdmissionRefreshes != 0 {
		t.Fatalf("truncated source recorded %d admission observations", quality[id].AdmissionRefreshes)
	}
}
