package monitor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func newSourceLearningRunner(t *testing.T, namespace string, specs []SourceSpec, enabled bool) (*Runner, *miniredis.Miniredis) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", namespace)
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	runner := &Runner{
		config: Config{
			SourceSpecs: specs, SourceLearningEnabled: enabled, DefaultProxyScheme: "http",
			FetchInterval: time.Hour, SourceRequestTimeout: time.Second, SourceMaxBytes: 1 << 20,
		},
		store:   redisStore,
		fetch:   source.NewFetcher(time.Second, 1<<20),
		metrics: newMetrics(),
		nowFunc: func() time.Time { return base },
	}
	return runner, mini
}

func syntheticSource(t *testing.T, dir, name, contents string) SourceSpec {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return SourceSpec{URL: "file://" + path, Scheme: "http"}
}

func TestSourceLearningRefreshUnionsFeedsAndAttachesOriginBeforeClaim(t *testing.T) {
	dir := t.TempDir()
	first := syntheticSource(t, dir, "first.txt", "192.0.2.101:8080\n192.0.2.102:8080\n")
	second := syntheticSource(t, dir, "second.txt", "192.0.2.102:8080\n192.0.2.103:8080\n")
	runner, mini := newSourceLearningRunner(t, "learning-refresh:v1", []SourceSpec{first, second}, true)
	ctx := context.Background()
	if err := runner.refresh(ctx); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	stats, err := runner.store.Stats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if stats.SourceUnique != 3 || stats.SourceParsed != 4 || stats.SourcesSucceeded != 2 {
		t.Fatalf("refresh stats=%+v, want 3 unique / 4 parsed / 2 sources", stats)
	}
	id1, id2 := runner.sourceID(first), runner.sourceID(second)
	quality, err := runner.store.LoadSourceQuality(ctx, []string{id1, id2}, runner.nowFunc())
	if err != nil {
		t.Fatal(err)
	}
	if quality[id1].RefreshSuccesses != 1 || quality[id2].RefreshSuccesses != 1 || quality[id1].Parsed != 2 || quality[id2].Parsed != 2 {
		t.Fatalf("per-source success stats=%+v", quality)
	}
	introduced := quality[id1].UniqueIncremental + quality[id2].UniqueIncremental
	if introduced != 3 || quality[id1].DuplicateRecords+quality[id2].DuplicateRecords != 1 {
		t.Fatalf("merge contribution stats=%+v; introduced=%d, want union of 3 and one cross-feed duplicate", quality, introduced)
	}

	claims, err := runner.store.ClaimDueBatch(ctx, "source-learning-test", runner.nowFunc(), time.Minute, 10)
	if err != nil || len(claims) != 3 {
		t.Fatalf("ClaimDueBatch returned %d claims, err=%v; want three unique candidates", len(claims), err)
	}
	seen := make(map[string]bool, 3)
	for _, claim := range claims {
		if seen[claim.URL] {
			t.Fatalf("duplicate candidate claimed: %s", claim.URL)
		}
		seen[claim.URL] = true
		storedID := mini.HGet("learning-refresh:v1:proxy:"+claim.ID, "source_id")
		if storedID == "" {
			t.Fatalf("source_id missing before first claim for %s", claim.URL)
		}
		if claim.URL == "http://192.0.2.102:8080" && storedID != id1 {
			t.Fatalf("shared endpoint origin=%s, want lower-ranked source %s", storedID, id1)
		}
	}
	if len(seen) != 3 || !seen["http://192.0.2.101:8080"] || !seen["http://192.0.2.102:8080"] || !seen["http://192.0.2.103:8080"] {
		t.Fatalf("claimed union=%v", seen)
	}
	if active, err := mini.SMembers(runner.store.SourceQualityKeyPrefix() + "active"); err != nil || len(active) != 2 {
		t.Fatalf("active source set=%v, want both configured feeds", active)
	}
}

func TestPrepareSourceLearningChunksLargeConfiguredCatalog(t *testing.T) {
	const sourceCount = store.MaxSourceQualityReadBatch + 17
	specs := make([]SourceSpec, sourceCount)
	for i := range specs {
		specs[i] = SourceSpec{URL: fmt.Sprintf("file:///synthetic/no-fetch-%04d.txt", i), Scheme: "http"}
	}
	runner, mini := newSourceLearningRunner(t, "learning-catalog:v1", specs, true)
	selected, err := runner.prepareSourceLearning(context.Background())
	if err != nil || selected != sourceCount {
		t.Fatalf("prepareSourceLearning selected=%d err=%v, want %d", selected, err, sourceCount)
	}
	if active, err := mini.SCard(runner.store.SourceQualityKeyPrefix() + "active"); err != nil || active != sourceCount {
		t.Fatalf("active source count=%d, want %d", active, sourceCount)
	}
	if generation, err := runner.store.NextSourceLearningRefresh(context.Background()); err != nil || generation != 2 {
		t.Fatalf("refresh generation=%d err=%v; prepare should consume generation 1", generation, err)
	}
	feed, ok := runner.plannedSource(specs[sourceCount-1])
	if !ok || feed.index != sourceCount-1 {
		t.Fatalf("last planned source=%+v found=%t", feed, ok)
	}
	priority, index, err := store.DecodeSourceStageScore(feed.score)
	if err != nil || priority != int64(sourceCount-1) || index != sourceCount-1 {
		t.Fatalf("last stage score decoded=(%d,%d,%v)", priority, index, err)
	}
}

func TestCooledAllSourcesSkipRefreshAndPreserveAggregateStats(t *testing.T) {
	dir := t.TempDir()
	var spec SourceSpec
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	for i := 0; ; i++ {
		candidate := SourceSpec{URL: fmt.Sprintf("file://%s/missing-%d.txt", dir, i), Scheme: "http"}
		id := store.StableSourceID(candidate.URL, candidate.defaultScheme("http"))
		if !exploreThisRefresh(id, 1, 10) && !exploreThisRefresh(id, 2, 10) {
			spec = candidate
			break
		}
	}
	runner, mini := newSourceLearningRunner(t, "learning-cooldown:v1", []SourceSpec{spec}, true)
	ctx := context.Background()
	id := runner.sourceID(spec)
	if err := runner.store.SetSourceRefreshStats(ctx, 17, 23, 1); err != nil {
		t.Fatal(err)
	}
	if err := runner.store.RecordSourceFetchFailure(ctx, id, base); err != nil {
		t.Fatal(err)
	}
	runner.setLastRefresh(refreshReport{At: "previous", Result: "ok", Accepted: 9})
	if selected, err := runner.prepareSourceLearning(ctx); err != nil || selected != 0 {
		t.Fatalf("all-cooled plan selected=%d err=%v, want no source", selected, err)
	}
	// prepareSourceLearning consumed generation 1; generation 2 has no
	// exploration allowance for this deterministic source ID.
	if generation, _ := mini.Get("learning-cooldown:v1:source-learning-refresh"); generation != "1" {
		t.Fatalf("prepared generation=%s, want 1", generation)
	}
	mini.FastForward(31 * time.Second)
	if err := runner.refresh(ctx); err != nil {
		t.Fatalf("cooled refresh should skip cleanly: %v", err)
	}
	stats, err := runner.store.Stats(ctx)
	if err != nil || stats.SourceUnique != 17 || stats.SourceParsed != 23 || stats.SourcesSucceeded != 1 {
		t.Fatalf("skipped refresh changed aggregate stats: %+v err=%v", stats, err)
	}
	if report := runner.lastRefreshSnapshot(); report != (refreshReport{At: "previous", Result: "ok", Accepted: 9}) {
		t.Fatalf("skipped refresh changed last report: %+v", report)
	}
}

func TestFailedFileSourceGetsCooldownAndDisabledLearningKeepsLegacyPath(t *testing.T) {
	dir := t.TempDir()
	bad := SourceSpec{URL: "file://" + filepath.Join(dir, "missing-feed.txt"), Scheme: "http"}
	runner, _ := newSourceLearningRunner(t, "learning-file-failure:v1", []SourceSpec{bad}, true)
	ctx := context.Background()
	if err := runner.refresh(ctx); err == nil {
		t.Fatal("missing file feed should fail a refresh with no successful sources")
	}
	quality, err := runner.store.LoadSourceQuality(ctx, []string{runner.sourceID(bad)}, runner.nowFunc())
	if err != nil {
		t.Fatal(err)
	}
	if quality[runner.sourceID(bad)].FetchFailures != 1 || quality[runner.sourceID(bad)].Parsed != 0 || !quality[runner.sourceID(bad)].CooldownUntil.After(runner.nowFunc()) {
		t.Fatalf("failed source quality=%+v", quality[runner.sourceID(bad)])
	}

	good := syntheticSource(t, dir, "legacy.txt", "192.0.2.111:8080\n")
	legacy, mini := newSourceLearningRunner(t, "learning-disabled:v1", []SourceSpec{good}, false)
	if selected, err := legacy.prepareSourceLearning(ctx); err != nil || selected != 1 {
		t.Fatalf("disabled prepare selected=%d err=%v", selected, err)
	}
	if err := legacy.refresh(ctx); err != nil {
		t.Fatalf("legacy refresh: %v", err)
	}
	url := "http://192.0.2.111:8080"
	claims, err := legacy.store.ClaimDueBatch(ctx, "legacy-test", legacy.nowFunc(), time.Minute, 2)
	if err != nil || len(claims) != 1 || claims[0].URL != url {
		t.Fatalf("legacy claims=%+v err=%v", claims, err)
	}
	if got := mini.HGet("learning-disabled:v1:proxy:"+claims[0].ID, "source_id"); got != "" {
		t.Fatalf("disabled learning attached provenance %q", got)
	}
	if mini.Exists(legacy.store.SourceQualityKeyPrefix() + "active") {
		t.Fatal("disabled learning wrote the active source catalog")
	}
	if mini.Exists(legacy.store.SourceQualityKeyPrefix() + legacy.sourceID(good)) {
		t.Fatal("disabled learning wrote source quality stats")
	}
}

func TestSourceLearningIDUsesPerSourceParserHintInRunner(t *testing.T) {
	specHTTP := SourceSpec{URL: "file:///synthetic/hints.txt", Scheme: "http"}
	specSOCKS := SourceSpec{URL: specHTTP.URL, Scheme: "socks5"}
	runner, mini := newSourceLearningRunner(t, "learning-hints:v1", []SourceSpec{specHTTP, specSOCKS}, true)
	selected, err := runner.prepareSourceLearning(context.Background())
	if err != nil || selected != 2 {
		t.Fatalf("prepare selected=%d err=%v", selected, err)
	}
	if runner.sourceID(specHTTP) == runner.sourceID(specSOCKS) {
		t.Fatal("parser hints shared a source identity")
	}
	if active, err := mini.SCard(runner.store.SourceQualityKeyPrefix() + "active"); err != nil || active != 2 {
		t.Fatalf("active source count=%d, want distinct parser-hint identities", active)
	}
	if strings.TrimSpace(runner.sourceID(specHTTP)) == "" {
		t.Fatal("source identity unexpectedly empty")
	}
}

func TestPrepareSourceLearningContractResetDoesNotReuseOldProbeSuccessForAdmissionCooldown(t *testing.T) {
	base := time.Date(2026, 10, 13, 12, 0, 0, 0, time.UTC)
	var spec SourceSpec
	for i := 0; ; i++ {
		candidate := SourceSpec{URL: fmt.Sprintf("https://feeds.example.test/contract-reset-%d", i), Scheme: "http"}
		id := store.StableSourceID(candidate.URL, "http")
		if !exploreThisRefresh(id, 1, 10) {
			spec = candidate
			break
		}
	}

	for _, tc := range []struct {
		name           string
		storedContract func(current string) string
		wantSelected   int
		wantCooled     int
	}{
		{
			name: "old contract success no longer protects",
			storedContract: func(current string) string {
				old := strings.Repeat("a", 64)
				if old == current {
					return strings.Repeat("b", 64)
				}
				return old
			},
			wantSelected: 0,
			wantCooled:   1,
		},
		{
			name:           "matching contract success still protects",
			storedContract: func(current string) string { return current },
			wantSelected:   1,
			wantCooled:     0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner, mini := newSourceLearningRunner(t, "learning-contract-cooldown:"+tc.name, []SourceSpec{spec}, true)
			runner.config.SourceLearningZeroAdmissionCooldown = time.Hour
			runner.config.SourceLearningZeroAdmissionRefreshes = 3
			id := runner.sourceID(spec)
			key := runner.store.SourceQualityKeyPrefix() + id
			currentContract := runner.sourceProbeContract()
			mini.HSet(key,
				"probe_contract", tc.storedContract(currentContract),
				"probe_seen", "1", "probe_samples", "1", "probe_passes", "1",
				"probe_updated_ms", fmt.Sprint(base.UnixMilli()),
				"probe_completions", "1", "probe_successful_completions", "1", "probe_duration_ms", "250",
			)
			for observation := 0; observation < 3; observation++ {
				if err := runner.store.RecordSourceAdmissionRefresh(context.Background(), map[string]int64{id: 0}, base); err != nil {
					t.Fatalf("record completed admission observation: %v", err)
				}
			}
			before := sourceQualityHashSnapshot(t, mini, key)

			selected, err := runner.prepareSourceLearning(context.Background())
			if err != nil || selected != tc.wantSelected {
				t.Fatalf("prepareSourceLearning selected=%d err=%v, want %d", selected, err, tc.wantSelected)
			}
			if report := runner.sourceLearningSnapshot(); report.AdmissionCooled != tc.wantCooled {
				t.Fatalf("admission-cooled report=%+v, want %d", report, tc.wantCooled)
			}
			after := sourceQualityHashSnapshot(t, mini, key)
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("planning mutated stored quality hash: before=%v after=%v", before, after)
			}

			quality, err := runner.store.LoadSourceQuality(context.Background(), []string{id}, base)
			if err != nil {
				t.Fatal(err)
			}
			stored := quality[id]
			if stored.ProbeCompletions != 1 || stored.ProbeSuccessfulCompletions != 1 || stored.ProbeDurationMS != 250 {
				t.Fatalf("stored probe-cost fields changed: %+v", stored)
			}
		})
	}
}

func sourceQualityHashSnapshot(t *testing.T, mini *miniredis.Miniredis, key string) map[string]string {
	t.Helper()
	fields, err := mini.HKeys(key)
	if err != nil {
		t.Fatalf("HKeys(%q): %v", key, err)
	}
	snapshot := make(map[string]string, len(fields))
	for _, field := range fields {
		snapshot[field] = mini.HGet(key, field)
	}
	return snapshot
}
