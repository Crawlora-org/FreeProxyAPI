package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestStableSourceIDAndStageScoreEncoding(t *testing.T) {
	url := " https://example.test/feed?token=redacted "
	idHTTP := StableSourceID(url, "http")
	if idHTTP != StableSourceID("https://example.test/feed?token=redacted", "HTTP") {
		t.Fatal("trimmed URL and normalized scheme should be stable")
	}
	if idHTTP == StableSourceID("https://example.test/feed?token=other", "http") || idHTTP == StableSourceID("https://example.test/feed?token=redacted", "socks5") {
		t.Fatal("distinct URL or parser hint collided")
	}

	first, err := EncodeSourceStageScore(20, 7)
	if err != nil {
		t.Fatal(err)
	}
	second, err := EncodeSourceStageScore(20, 8)
	if err != nil {
		t.Fatal(err)
	}
	otherPriority, err := EncodeSourceStageScore(21, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !(first < second && second < otherPriority) {
		t.Fatalf("encoded scores = %d %d %d, want priority then source index", first, second, otherPriority)
	}
	priority, index, err := DecodeSourceStageScore(second)
	if err != nil || priority != 20 || index != 8 {
		t.Fatalf("decoded score=(%d,%d,%v), want (20,8,nil)", priority, index, err)
	}
	if _, err := EncodeSourceStageScore(1, int(sourceStageScoreStride)); err == nil {
		t.Fatal("accepted out-of-range source index")
	}
}

func TestSharedStageKeepsSelectedOriginOnDeduplication(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-origin:v1")
	stage, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	url := "http://duplicate.example:8080"
	lateScore, _ := EncodeSourceStageScore(4, 8)
	selectedScore, _ := EncodeSourceStageScore(4, 2)
	if err := s.StageSourceURLs(ctx, stage, lateScore, []string{url}); err != nil {
		t.Fatal(err)
	}
	if err := s.StageSourceURLs(ctx, stage, selectedScore, []string{url}); err != nil {
		t.Fatal(err)
	}
	if err := s.DrainSourceStagePrioritized(ctx, stage, func(score int64, batch []string) error {
		priority, index, err := DecodeSourceStageScore(score)
		if err != nil || priority != 4 || index != 2 || len(batch) != 1 || batch[0] != url {
			t.Fatalf("drained origin score=%d decoded=(%d,%d,%v) batch=%v", score, priority, index, err, batch)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestAttachSourceProvenanceKeepsFirstExistingOrigin(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-provenance:v1")
	urls := []string{"http://origin-a.example:8080", "http://origin-b.example:8080"}
	if _, err := s.Upsert(ctx, urls, time.Now()); err != nil {
		t.Fatal(err)
	}
	orphanURL := "http://orphan.example:8080"
	if err := s.client.HSet(ctx, s.proxyKey(proxyID(orphanURL)), "url", orphanURL).Err(); err != nil {
		t.Fatal(err)
	}
	first := StableSourceID("https://feeds.example/a", "http")
	second := StableSourceID("https://feeds.example/b", "http")
	if err := s.AttachSourceProvenance(ctx, append(urls, "http://missing.example:8080", orphanURL), first); err != nil {
		t.Fatal(err)
	}
	if err := s.AttachSourceProvenance(ctx, urls, second); err != nil {
		t.Fatal(err)
	}
	for _, rawURL := range urls {
		got, err := s.client.HGet(ctx, s.proxyKey(proxyID(rawURL)), "source_id").Result()
		if err != nil || got != first {
			t.Fatalf("source_id(%s)=%q err=%v, want first origin %q", rawURL, got, err, first)
		}
	}
	if exists, err := s.client.Exists(ctx, s.proxyKey(proxyID("http://missing.example:8080"))).Result(); err != nil || exists != 0 {
		t.Fatalf("provenance attached ghost candidate: exists=%d err=%v", exists, err)
	}
	if value, err := s.client.HGet(ctx, s.proxyKey(proxyID(orphanURL)), "source_id").Result(); err != redis.Nil {
		t.Fatalf("orphan hash got provenance %q, err=%v", value, err)
	}
	if err := s.AttachSourceProvenance(ctx, urls, "not-a-source-id"); err == nil {
		t.Fatal("accepted malformed source ID")
	}
}

func TestUpsertSourceProvenanceIsPresentBeforeFirstClaimAndPreservesLease(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-upsert:v1")
	now := time.Unix(1_800_000_000, 0)
	url := "http://first-check.example:8080"
	id := StableSourceID("https://feeds.example/first", "http")
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	counts, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now, id)
	if err != nil || counts.Added != 1 {
		t.Fatalf("source upsert=%+v err=%v", counts, err)
	}
	claim, ok, err := s.ClaimDue(ctx, "worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("first claim=(%+v,%t,%v)", claim, ok, err)
	}
	// The source ID must already be on the leased hash when the first probe
	// completes, so the inline completion hook can attribute the first result.
	gotID, err := s.client.HGet(ctx, s.proxyKey(claim.ID), "source_id").Result()
	if err != nil || gotID != id {
		t.Fatalf("source_id before first completion=%q err=%v", gotID, err)
	}
	completed, err := s.Complete(ctx, claim, now, RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour, MaxConsecutiveFailures: 3}, true, 200, time.Millisecond, "", OutcomeMeta{SourceLearning: true})
	if err != nil || !completed.Committed {
		t.Fatalf("first Complete=%+v err=%v", completed, err)
	}
	quality, err := s.LoadSourceQuality(ctx, []string{id}, now)
	if err != nil || quality[id].ProbeSamples != 1 || quality[id].ProbePasses != 1 {
		t.Fatalf("first outcome quality=%+v err=%v", quality[id], err)
	}

	leasedURL := "http://already-leased.example:8080"
	if _, err := s.Upsert(ctx, []string{leasedURL}, now); err != nil {
		t.Fatal(err)
	}
	leased, ok, err := s.ClaimDue(ctx, "worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("leased claim=(%+v,%t,%v)", leased, ok, err)
	}
	secondID := StableSourceID("https://feeds.example/second", "http")
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{leasedURL}, now.Add(time.Second), secondID); err != nil {
		t.Fatal(err)
	}
	token, err := s.client.HGet(ctx, s.proxyKey(leased.ID), "lease_token").Result()
	if err != nil || token != leased.Token {
		t.Fatalf("lease token after source upsert=%q err=%v, want %q", token, err, leased.Token)
	}
	gotID, err = s.client.HGet(ctx, s.proxyKey(leased.ID), "source_id").Result()
	if err != nil || gotID != secondID {
		t.Fatalf("leased candidate source_id=%q err=%v", gotID, err)
	}
}

func TestSourceUpsertDoesNotCreateTombstonedGhost(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-learning-tombstone:v1")
	now := time.Unix(1_800_000_000, 0)
	url := "http://blocked.example:8080"
	id := StableSourceID("https://feeds.example/blocked", "http")
	if err := mini.Set(s.tombstoneKey(proxyID(url)), fmt.Sprintf("1:%d", now.Add(time.Hour).UnixMilli())); err != nil {
		t.Fatal(err)
	}
	counts, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now, id)
	if err != nil || counts.Tombstoned != 1 || counts.Added != 0 {
		t.Fatalf("tombstoned source upsert=%+v err=%v", counts, err)
	}
	if exists, err := s.client.Exists(ctx, s.proxyKey(proxyID(url))).Result(); err != nil || exists != 0 {
		t.Fatalf("tombstoned ghost exists=%d err=%v", exists, err)
	}
}

func TestSourceRefreshStatsRequireSuccessfulStageMerge(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-merge:v1")
	id := StableSourceID("https://feeds.example/list", "http")
	now := time.Unix(1_800_000_000, 0)

	// A partial source stage without a completed merge records only the fetch
	// failure; staged members do not contribute parsed or unique counters.
	private, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	if err := s.AddSourceMembers(ctx, private, []string{"http://one.example:80", "http://one.example:80"}); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordSourceFetchFailure(ctx, id, now); err != nil {
		t.Fatal(err)
	}
	q, err := s.LoadSourceQuality(ctx, []string{id}, now)
	if err != nil {
		t.Fatal(err)
	}
	if q[id].FetchFailures != 1 || q[id].Parsed != 0 || q[id].UniqueIncremental != 0 {
		t.Fatalf("failed stage quality = %+v", q[id])
	}

	shared, err := s.NewSourceStageKey()
	if err != nil {
		t.Fatal(err)
	}
	moved, err := s.MergeSourceStage(ctx, private, shared, 20)
	if err != nil || moved != 1 {
		t.Fatalf("MergeSourceStage moved=%d err=%v", moved, err)
	}
	if err := s.RecordSourceRefreshSuccess(ctx, id, 4, moved, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	q, err = s.LoadSourceQuality(ctx, []string{id}, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if q[id].RefreshFetches != 2 || q[id].RefreshSuccesses != 1 || q[id].FetchFailures != 1 ||
		q[id].Parsed != 4 || q[id].UniqueIncremental != 1 || q[id].DuplicateRecords != 3 ||
		q[id].ConsecutiveFailures != 0 || !q[id].CooldownUntil.IsZero() {
		t.Fatalf("merged source quality = %+v", q[id])
	}
	if ttl := miniTTL(t, s.client, s.sourceQualityKey(id)); ttl <= 0 || ttl > sourceQualityTTL {
		t.Fatalf("source quality TTL = %s", ttl)
	}
}

func TestSourceFailureCooldownAndBackoff(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-backoff:v1")
	id := StableSourceID("https://feeds.example/down", "http")
	start := time.Unix(1_800_000_000, 0)
	for i := 0; i < 3; i++ {
		if err := s.RecordSourceFetchFailure(ctx, id, start.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatal(err)
		}
	}
	q, err := s.LoadSourceQuality(ctx, []string{id}, start.Add(3*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	quality := q[id]
	if quality.RefreshFetches != 3 || quality.FetchFailures != 3 || quality.ConsecutiveFailures != 3 {
		t.Fatalf("failure counts = %+v", quality)
	}
	want := start.Add(2 * time.Minute).Add(4 * time.Minute)
	if !quality.CooldownUntil.Equal(want) {
		t.Fatalf("cooldown until = %s, want %s", quality.CooldownUntil, want)
	}
}

func TestLoadSourceQualityIsBoundedAndToleratesCorruption(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-read:v1")
	ids := make([]string, MaxSourceQualityReadBatch+1)
	for i := range ids {
		ids[i] = StableSourceID(fmt.Sprintf("https://feeds.example/%d", i), "http")
	}
	if _, err := s.LoadSourceQuality(ctx, ids, time.Now()); err != ErrSourceLearningBatchTooLarge {
		t.Fatalf("oversized read error = %v, want batch-limit error", err)
	}
	if err := s.client.HSet(ctx, s.sourceQualityKey(ids[0]),
		"refresh_fetches", "garbage", "probe_samples", "NaN", "probe_passes", "2", "cooldown_until_ms", "-1").Err(); err != nil {
		t.Fatal(err)
	}
	loaded, err := s.LoadSourceQuality(ctx, []string{ids[0], ids[0], "malformed"}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || !loaded[ids[0]].Corrupt || loaded[ids[0]].RefreshFetches != 0 || loaded[ids[0]].ProbeSamples != 0 {
		t.Fatalf("corrupt source decode = %+v", loaded)
	}
	if !loaded[ids[0]].CooldownUntil.IsZero() {
		t.Fatal("negative timestamp should be ignored")
	}
}

func TestSourceProbeLuaSamplesInitialAndDecaysRetests(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-probes:v1")
	id := StableSourceID("https://feeds.example/probe", "http")
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	proxyKey := s.proxyKey(proxyID("http://candidate.example:80"))
	if err := s.client.HSet(ctx, proxyKey, "source_id", id).Err(); err != nil {
		t.Fatal(err)
	}
	script := redis.NewScript(`
local key = KEYS[1]
local status = ARGV[1]
local now_ms = ARGV[2]
` + SourceProbeLearningLuaSnippet + `
return 1
`)
	invoke := func(status string, at time.Time) {
		t.Helper()
		args := make([]interface{}, 30)
		args[0] = status
		args[1] = at.UnixMilli()
		args[27] = "1"
		args[28] = "16"
		args[29] = (24 * time.Hour).Milliseconds()
		keys := []string{proxyKey, "probe:k2", "probe:k3", "probe:k4", "probe:k5", s.SourceQualityKeyPrefix()}
		if err := script.Run(ctx, s.client, keys, args...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	start := time.Unix(1_800_000_000, 0)
	invoke("ok", start)
	q, err := s.LoadSourceQuality(ctx, []string{id}, start)
	if err != nil || q[id].ProbeSamples != 1 || q[id].ProbePasses != 1 {
		t.Fatalf("initial sample=%+v err=%v", q[id], err)
	}
	// Existing last_checked_at_ms marks each result as a retest. Fifteen
	// retests do not sample; the sixteenth adds one observation.
	if err := s.client.HSet(ctx, proxyKey, "last_checked_at_ms", start.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 16; i++ {
		invoke("failed", start.Add(time.Duration(i)*time.Minute))
	}
	q, err = s.LoadSourceQuality(ctx, []string{id}, start.Add(16*time.Minute))
	if err != nil || q[id].ProbeSamples < 1.98 || q[id].ProbeSamples > 2.01 || q[id].ProbePasses < 0.99 || q[id].ProbePasses > 1.0 {
		t.Fatalf("sampled retest=%+v err=%v", q[id], err)
	}
	aged, samples := DecayedSourceYield(q[id], start.Add(24*time.Hour), 24*time.Hour)
	if samples <= 0.9 || samples >= 1.1 || aged >= 0.7 {
		t.Fatalf("decayed source yield=(%0.3f,%0.3f)", aged, samples)
	}
}

func TestActiveSourceCatalogBoundsProbeAttributionAndRefreshGeneration(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-learning-active:v1")
	id := StableSourceID("https://feeds.example/active", "http")
	base := time.Unix(1_800_000_000, 0)
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	activeKey := s.SourceQualityKeyPrefix() + "active"
	if active, err := mini.SMembers(activeKey); err != nil || len(active) != 1 || active[0] != id {
		t.Fatalf("active source set=%v", active)
	}
	if ttl := mini.TTL(activeKey); ttl <= 0 || ttl > 24*time.Hour {
		t.Fatalf("active source TTL=%s", ttl)
	}
	first, err := s.NextSourceLearningRefresh(ctx)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.NextSourceLearningRefresh(ctx)
	if err != nil || first != 1 || second != 2 {
		t.Fatalf("refresh generations=(%d,%d), err=%v", first, second, err)
	}
	if ttl := mini.TTL(s.prefix + ":source-learning-refresh"); ttl <= 0 || ttl > 30*24*time.Hour {
		t.Fatalf("refresh generation TTL=%s", ttl)
	}

	qualityKey := s.sourceQualityKey(id)
	if err := s.client.HSet(ctx, qualityKey, "probe_seen", "15", "probe_samples", "1", "probe_passes", "1", "probe_updated_ms", base.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.client.PExpire(ctx, qualityKey, 48*time.Hour).Err(); err != nil {
		t.Fatal(err)
	}
	proxyKey := s.proxyKey(proxyID("http://198.51.100.22:8080"))
	if err := s.client.HSet(ctx, proxyKey, "source_id", id, "last_checked_at_ms", base.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	script := sourceProbeTestScript()
	invoke := func(at time.Time) {
		t.Helper()
		args := make([]interface{}, 30)
		args[0], args[1] = "failed", at.UnixMilli()
		args[27], args[28], args[29] = "1", "16", (24 * time.Hour).Milliseconds()
		keys := []string{proxyKey, "active:k2", "active:k3", "active:k4", "active:k5", s.SourceQualityKeyPrefix()}
		if err := script.Run(ctx, s.client, keys, args...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	// At the sixteenth retest while active, the sample is recorded and TTL is
	// extended. After source removal, the same hook must leave both untouched.
	invoke(base.Add(time.Minute))
	if got := mini.TTL(qualityKey); got < 29*24*time.Hour {
		t.Fatalf("active source sample did not refresh retention TTL: %s", got)
	}
	if err := s.SetActiveLearningSources(ctx, nil); err != nil {
		t.Fatal(err)
	}
	mini.FastForward(29 * 24 * time.Hour)
	beforeTTL := mini.TTL(qualityKey)
	beforeSeen := mini.HGet(qualityKey, "probe_seen")
	beforeSamples := mini.HGet(qualityKey, "probe_samples")
	beforeUpdated := mini.HGet(qualityKey, "probe_updated_ms")
	invoke(base.Add(30 * 24 * time.Hour))
	if got := mini.TTL(qualityKey); got != beforeTTL {
		t.Fatalf("removed source stats TTL changed from %s to %s", beforeTTL, got)
	}
	if mini.HGet(qualityKey, "probe_seen") != beforeSeen || mini.HGet(qualityKey, "probe_samples") != beforeSamples || mini.HGet(qualityKey, "probe_updated_ms") != beforeUpdated {
		t.Fatalf("removed source probe data changed: before=(%s,%s,%s), after=(%s,%s,%s)", beforeSeen, beforeSamples, beforeUpdated, mini.HGet(qualityKey, "probe_seen"), mini.HGet(qualityKey, "probe_samples"), mini.HGet(qualityKey, "probe_updated_ms"))
	}
	mini.FastForward(2 * 24 * time.Hour)
	if mini.Exists(qualityKey) {
		t.Fatal("removed source statistics did not expire")
	}
}

func TestSourceProbeContractChangeStartsFreshOutcomeEpoch(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-learning-contract:v1")
	id := StableSourceID("https://feeds.example/probe-contract", "http")
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	qualityKey := s.sourceQualityKey(id)
	oldContract, newContract := strings.Repeat("a", 64), strings.Repeat("b", 64)
	base := time.Unix(1_800_000_000, 0)
	if err := s.client.HSet(ctx, qualityKey,
		"refresh_fetches", "7", "refresh_successes", "6", "fetch_failures", "1",
		"parsed", "900", "unique_incremental", "800", "duplicate_records", "100",
		"probe_seen", "128", "probe_samples", "100", "probe_passes", "100",
		"probe_updated_ms", base.Add(-24*time.Hour).UnixMilli(), "probe_contract", oldContract).Err(); err != nil {
		t.Fatal(err)
	}
	proxyKey := s.proxyKey(proxyID("http://198.51.100.33:8080"))
	if err := s.client.HSet(ctx, proxyKey, "source_id", id, "last_checked_at_ms", base.Add(-time.Hour).UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	script := sourceProbeTestScript()
	invoke := func(enabled string, contract string) {
		t.Helper()
		args := make([]interface{}, 32)
		args[0], args[1] = "failed", base.UnixMilli()
		args[27], args[28], args[29], args[31] = enabled, "16", (24 * time.Hour).Milliseconds(), contract
		keys := []string{proxyKey, "contract:k2", "contract:k3", "contract:k4", "contract:k5", s.SourceQualityKeyPrefix()}
		if err := script.Run(ctx, s.client, keys, args...).Err(); err != nil {
			t.Fatal(err)
		}
	}
	invoke("1", newContract)
	quality, err := s.LoadSourceQuality(ctx, []string{id}, base)
	if err != nil {
		t.Fatal(err)
	}
	q := quality[id]
	if q.ProbeContract != newContract || q.ProbeSamples != 1 || q.ProbePasses != 0 || q.ProbeSeen != 1 || !q.ProbeUpdatedAt.Equal(base) {
		t.Fatalf("contract change inherited old outcome data: %+v", q)
	}
	if q.RefreshFetches != 7 || q.RefreshSuccesses != 6 || q.FetchFailures != 1 || q.Parsed != 900 || q.UniqueIncremental != 800 || q.DuplicateRecords != 100 {
		t.Fatalf("contract change reset refresh aggregates: %+v", q)
	}

	// A disabled hook must leave both the prior contract and high score intact,
	// even if the optional contract argument is populated.
	offID := StableSourceID("https://feeds.example/probe-contract-disabled", "http")
	if err := s.SetActiveLearningSources(ctx, []string{id, offID}); err != nil {
		t.Fatal(err)
	}
	offQualityKey := s.sourceQualityKey(offID)
	if err := s.client.HSet(ctx, offQualityKey, "probe_seen", "32", "probe_samples", "24", "probe_passes", "23", "probe_updated_ms", base.UnixMilli(), "probe_contract", oldContract).Err(); err != nil {
		t.Fatal(err)
	}
	offProxyKey := s.proxyKey(proxyID("http://198.51.100.34:8080"))
	if err := s.client.HSet(ctx, offProxyKey, "source_id", offID, "last_checked_at_ms", base.UnixMilli()).Err(); err != nil {
		t.Fatal(err)
	}
	args := make([]interface{}, 32)
	args[0], args[1] = "failed", base.Add(time.Minute).UnixMilli()
	args[27], args[28], args[29], args[31] = "0", "16", (24 * time.Hour).Milliseconds(), newContract
	keys := []string{offProxyKey, "contract-off:k2", "contract-off:k3", "contract-off:k4", "contract-off:k5", s.SourceQualityKeyPrefix()}
	if err := script.Run(ctx, s.client, keys, args...).Err(); err != nil {
		t.Fatal(err)
	}
	unchanged, err := s.LoadSourceQuality(ctx, []string{offID}, base.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if unchanged[offID].ProbeContract != oldContract || unchanged[offID].ProbeSamples != 24 || unchanged[offID].ProbePasses != 23 || unchanged[offID].ProbeSeen != 32 {
		t.Fatalf("disabled hook changed contract outcome fields: %+v", unchanged[offID])
	}
}

func sourceProbeTestScript() *redis.Script {
	return redis.NewScript(`
local key = KEYS[1]
local status = ARGV[1]
local now_ms = ARGV[2]
` + SourceProbeLearningLuaSnippet + `
return 1
`)
}

func miniTTL(t *testing.T, client *redis.Client, key string) time.Duration {
	t.Helper()
	ttl, err := client.TTL(context.Background(), key).Result()
	if err != nil {
		t.Fatal(err)
	}
	return ttl
}

func TestSourceProvenanceReassignsOnlyRemovedOrigin(t *testing.T) {
	mini, s := newTestStore(t, "source-reassignment:v1")
	ctx := context.Background()
	a := StableSourceID("https://a.example.test/list", "http")
	b := StableSourceID("https://b.example.test/list", "http")
	proxyURL := "http://192.0.2.10:8080"
	if err := s.SetActiveLearningSources(ctx, []string{a, b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{proxyURL}, time.Now(), a); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{proxyURL}, time.Now(), b); err != nil {
		t.Fatal(err)
	}
	if got := mini.HGet(s.proxyKey(proxyID(proxyURL)), "source_id"); got != a {
		t.Fatalf("active first origin=%s, want %s", got, a)
	}
	if err := s.SetActiveLearningSources(ctx, []string{b}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{proxyURL}, time.Now(), b); err != nil {
		t.Fatal(err)
	}
	if got := mini.HGet(s.proxyKey(proxyID(proxyURL)), "source_id"); got != b {
		t.Fatalf("removed origin not replaced: %s", got)
	}
}
