package monitor

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

// capRunner builds a runner whose only sources are the given local feeds.
func capRunner(t *testing.T, extra string, feeds ...string) (*Runner, *miniredis.Miniredis, []string) {
	t.Helper()
	mini := miniredis.RunT(t)
	dir := t.TempDir()
	var sources, paths []string
	for i, body := range feeds {
		path := writeTempFile(t, dir, fmt.Sprintf("feed%d.txt", i), body)
		paths = append(paths, path)
		sources = append(sources, fmt.Sprintf("%q", "file://"+path))
	}
	configPath := writeTempFile(t, dir, "monitor.json", fmt.Sprintf(`{
		"redis_url": "redis://%s/0",
		"namespace": "caps:v1",
		"sources": [%s],
		"network_validation_enabled": false,
		"listen_addr": "-",
		"fetch_interval": "1h"%s
	}`, mini.Addr(), strings.Join(sources, ","), extra))
	config, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	runner, err := NewRunner(config, "caps-worker")
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	t.Cleanup(func() { runner.Close() })
	return runner, mini, paths
}

func feedLines(prefix string, n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "%s%d.example.net:8080\n", prefix, i)
	}
	return b.String()
}

func candidateCount(t *testing.T, r *Runner) int64 {
	t.Helper()
	stats, err := r.store.Stats(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return stats.Candidates
}

func TestRefreshTruncatesFeedAtMaxRecordsPerSource(t *testing.T) {
	runner, _, _ := capRunner(t, `, "max_records_per_source": 7`, feedLines("big", 50), feedLines("small", 3))
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	// 7 from the oversized feed plus the complete small feed.
	if got := candidateCount(t, runner); got != 10 {
		t.Fatalf("candidates = %d, want 10 (7 capped + 3)", got)
	}
	if got := runner.metrics.sourcesTruncated.Load(); got != 1 {
		t.Fatalf("sources truncated = %d, want 1", got)
	}
}

func TestRefreshWithoutCapIngestsWholeFeed(t *testing.T) {
	runner, _, _ := capRunner(t, "", feedLines("all", 40))
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if got := candidateCount(t, runner); got != 40 {
		t.Fatalf("candidates = %d, want 40 under the default cap", got)
	}
	if runner.metrics.sourcesTruncated.Load() != 0 {
		t.Fatal("a small feed must not be reported as truncated")
	}
}

func TestRefreshStopsAddingCandidatesAtMaxCandidates(t *testing.T) {
	runner, mini, feeds := capRunner(t, `, "max_candidates": 5`, feedLines("a", 20))
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	first := candidateCount(t, runner)
	if first < 5 || first > 20 {
		t.Fatalf("first refresh candidates = %d, want at least the cap and at most the feed", first)
	}
	// Once the set is at or above the cap, a feed full of new endpoints adds
	// nothing, and the skipped endpoints are counted.
	if err := os.WriteFile(feeds[0], []byte(feedLines("b", 20)), 0o600); err != nil {
		t.Fatal(err)
	}
	mini.FastForward(2 * time.Hour) // expire the cluster-wide refresh lock
	if err := runner.refresh(context.Background()); err != nil {
		t.Fatalf("second refresh: %v", err)
	}
	if got := candidateCount(t, runner); got != first {
		t.Fatalf("candidates grew from %d to %d past max_candidates", first, got)
	}
	if runner.metrics.candidatesCapped.Load() == 0 {
		t.Fatal("capped endpoints were not counted")
	}
}

func TestLoadConfigSourceCapValidation(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil || config.MaxRecordsPerSource != defaultMaxRecordsPerSource || config.MaxCandidates != 0 {
		t.Fatalf("defaults = %d %d, %v", config.MaxRecordsPerSource, config.MaxCandidates, err)
	}
	for _, bad := range []string{`"max_records_per_source": -1`, `"max_records_per_source": 5000001`, `"max_candidates": -1`} {
		if _, err := LoadConfig(writeConfig(t, `{"sources":["file:///data/proxies.txt"],`+bad+`}`)); err == nil {
			t.Errorf("config with %s was accepted", bad)
		}
	}
}
