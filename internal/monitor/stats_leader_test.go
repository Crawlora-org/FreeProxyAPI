package monitor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

const leaderTestNS = "leader-test:v1"

type testClock struct{ ms atomic.Int64 }

func newTestClock(at time.Time) *testClock {
	c := &testClock{}
	c.ms.Store(at.UnixMilli())
	return c
}

func (c *testClock) now() time.Time          { return time.UnixMilli(c.ms.Load()).UTC() }
func (c *testClock) advance(d time.Duration) { c.ms.Add(d.Milliseconds()) }

func newLeaderTestRunner(t *testing.T, mini *miniredis.Miniredis, worker string, clock *testClock) *Runner {
	t.Helper()
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", leaderTestNS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = redisStore.Close() })
	return &Runner{
		store:   redisStore,
		metrics: newMetrics(),
		worker:  worker,
		nowFunc: clock.now,
		config: Config{
			StatsLeaderEnabled: true,
			StatsLeaderTTL:     defaultStatsLeaderTTL,
			StatsAggregateTTL:  defaultStatsAggregateTTL,
		},
	}
}

func seedValidated(mini *miniredis.Miniredis, id, country, asn string, latency, ratio int) {
	mini.SAdd(leaderTestNS+":validated", id)
	mini.HSet(leaderTestNS+":proxy:"+id, "country", country, "exit_country", country, "asn", asn,
		"anonymity", "elite", "latency_ewma_ms", fmt.Sprint(latency), "ok_ratio_pct", fmt.Sprint(ratio))
}

func validatedExposition(t *testing.T, r *Runner) string {
	t.Helper()
	rec := httptest.NewRecorder()
	r.metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	var lines []string
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if strings.Contains(line, "freeproxyapi_validated") {
			lines = append(lines, line)
		}
	}
	return strings.Join(lines, "\n")
}

func TestStatsFollowerUsesLeaderAggregateWithIdenticalMetrics(t *testing.T) {
	mini := miniredis.RunT(t)
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC))
	seedValidated(mini, "a", "US", "AS1", 120, 90)
	seedValidated(mini, "b", "US", "AS2", 700, 50)
	seedValidated(mini, "c", "DE", "AS1", 300, 85)
	leader := newLeaderTestRunner(t, mini, "leader", clock)
	follower := newLeaderTestRunner(t, mini, "follower", clock)

	leader.publishStats(context.Background())
	follower.publishStats(context.Background())
	if !leader.isStatsLeader() || follower.isStatsLeader() {
		t.Fatalf("leadership: leader=%t follower=%t", leader.isStatsLeader(), follower.isStatsLeader())
	}
	want := leader.validatedSlicesSnapshot()
	if want.ByCountry["US"] != 2 || want.Stable != 2 || want.BandKnown != 3 {
		t.Fatalf("unexpected leader aggregate: %+v", want)
	}
	if got := follower.validatedSlicesSnapshot(); !reflect.DeepEqual(got, want) {
		t.Fatalf("follower slices = %+v, want %+v", got, want)
	}
	leaderMetrics, followerMetrics := validatedExposition(t, leader), validatedExposition(t, follower)
	if leaderMetrics == "" || leaderMetrics != followerMetrics {
		t.Fatalf("validated metrics differ:\nleader:\n%s\nfollower:\n%s", leaderMetrics, followerMetrics)
	}

	// The follower reads the shared copy rather than scanning: an entry it
	// could only see by scanning does not appear until the leader republishes.
	seedValidated(mini, "d", "FR", "AS3", 100, 95)
	follower.publishStats(context.Background())
	if got := follower.validatedSlicesSnapshot(); got.ByCountry["FR"] != 0 {
		t.Fatalf("follower scanned instead of reading the shared aggregate: %+v", got)
	}
	leader.publishStats(context.Background())
	follower.publishStats(context.Background())
	if got := follower.validatedSlicesSnapshot(); got.ByCountry["FR"] != 1 {
		t.Fatalf("follower missed republished aggregate: %+v", got)
	}
}

func TestStatsFollowerFallsBackWhenAggregateStaleOrMissing(t *testing.T) {
	mini := miniredis.RunT(t)
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC))
	seedValidated(mini, "a", "US", "AS1", 120, 90)
	leader := newLeaderTestRunner(t, mini, "leader", clock)
	follower := newLeaderTestRunner(t, mini, "follower", clock)
	leader.publishStats(context.Background())

	seedValidated(mini, "b", "GB", "AS2", 150, 90)
	clock.advance(statsAggregateMaxAge + time.Second)
	follower.publishStats(context.Background())
	if follower.isStatsLeader() {
		t.Fatal("follower took a lease that is still held")
	}
	if got := follower.validatedSlicesSnapshot(); got.ByCountry["GB"] != 1 || got.ByCountry["US"] != 1 {
		t.Fatalf("stale aggregate did not trigger a local scan: %+v", got)
	}

	// Missing aggregate while another replica holds the lease: scan locally.
	mini.Del(leaderTestNS + ":validated-aggregate")
	seedValidated(mini, "c", "JP", "AS3", 150, 90)
	follower.publishStats(context.Background())
	if got := follower.validatedSlicesSnapshot(); got.ByCountry["JP"] != 1 {
		t.Fatalf("missing aggregate did not trigger a local scan: %+v", got)
	}
}

func TestStatsLeaderFailoverAndGracefulHandover(t *testing.T) {
	mini := miniredis.RunT(t)
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC))
	seedValidated(mini, "a", "US", "AS1", 120, 90)
	first := newLeaderTestRunner(t, mini, "first", clock)
	second := newLeaderTestRunner(t, mini, "second", clock)

	first.publishStats(context.Background())
	second.publishStats(context.Background())
	if !first.isStatsLeader() || second.isStatsLeader() {
		t.Fatal("first replica should lead")
	}

	// The leader stops publishing; once its lease lapses the next publish on
	// another replica takes over and the old leader becomes a follower.
	mini.FastForward(defaultStatsLeaderTTL + time.Second)
	clock.advance(defaultStatsLeaderTTL + time.Second)
	seedValidated(mini, "b", "CA", "AS2", 120, 90)
	second.publishStats(context.Background())
	if !second.isStatsLeader() {
		t.Fatal("second replica did not take over the expired lease")
	}
	first.publishStats(context.Background())
	if first.isStatsLeader() {
		t.Fatal("deposed leader still believes it leads")
	}
	if got := first.validatedSlicesSnapshot(); got.ByCountry["CA"] != 1 {
		t.Fatalf("deposed leader did not adopt the new leader's aggregate: %+v", got)
	}

	// Draining releases the lease so the next publish elsewhere takes over
	// without waiting out the TTL.
	second.draining.Store(true)
	second.publishStats(context.Background())
	if second.isStatsLeader() {
		t.Fatal("draining leader kept the lease")
	}
	first.publishStats(context.Background())
	if !first.isStatsLeader() {
		t.Fatal("lease was not handed over after a graceful release")
	}
}

func TestStatsLeaderDisabledScansOnEveryReplica(t *testing.T) {
	mini := miniredis.RunT(t)
	clock := newTestClock(time.Date(2026, 9, 14, 10, 0, 0, 0, time.UTC))
	seedValidated(mini, "a", "US", "AS1", 120, 90)
	runner := newLeaderTestRunner(t, mini, "solo", clock)
	runner.config.StatsLeaderEnabled = false
	runner.publishStats(context.Background())
	if runner.isStatsLeader() || mini.Exists(leaderTestNS+":stats-leader") || mini.Exists(leaderTestNS+":validated-aggregate") {
		t.Fatal("disabled stats leader touched lease or aggregate keys")
	}
	if got := runner.validatedSlicesSnapshot(); got.ByCountry["US"] != 1 {
		t.Fatalf("local scan missing: %+v", got)
	}
}

func TestLoadConfigStatsLeaderAndPermitChunkKeys(t *testing.T) {
	dir := t.TempDir()
	defaults := writeTempFile(t, dir, "defaults.json", `{"sources": ["file:///dev/null"]}`)
	config, err := LoadConfig(defaults)
	if err != nil {
		t.Fatal(err)
	}
	if !config.StatsLeaderEnabled || config.StatsLeaderTTL != defaultStatsLeaderTTL ||
		config.StatsAggregateTTL != defaultStatsAggregateTTL || config.ProbePermitChunk != 0 {
		t.Fatalf("unexpected defaults: %+v", config)
	}
	custom := writeTempFile(t, dir, "custom.json", `{"sources": ["file:///dev/null"],
		"stats_leader_enabled": false, "stats_leader_ttl": "2m", "stats_aggregate_ttl": "10m", "probe_permit_chunk": 50}`)
	config, err = LoadConfig(custom)
	if err != nil {
		t.Fatal(err)
	}
	if config.StatsLeaderEnabled || config.StatsLeaderTTL != 2*time.Minute ||
		config.StatsAggregateTTL != 10*time.Minute || config.ProbePermitChunk != 50 {
		t.Fatalf("unexpected custom config: %+v", config)
	}
	for name, body := range map[string]string{
		"short-lease":     `{"sources": ["file:///dev/null"], "stats_leader_ttl": "30s"}`,
		"short-aggregate": `{"sources": ["file:///dev/null"], "stats_aggregate_ttl": "59s"}`,
		"negative-chunk":  `{"sources": ["file:///dev/null"], "probe_permit_chunk": -1}`,
	} {
		if _, err := LoadConfig(writeTempFile(t, dir, name+".json", body)); err == nil {
			t.Fatalf("%s: expected a validation error", name)
		}
	}
}
