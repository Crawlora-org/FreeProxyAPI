package store

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func completeQualitySample(t *testing.T, s *Redis, id string, now time.Time, ok bool, meta OutcomeMeta) {
	t.Helper()
	if _, err := s.client.ZAdd(context.Background(), s.pendingKey(), redis.Z{Score: float64(now.Add(-time.Second).UnixMilli()), Member: id}).Result(); err != nil {
		t.Fatal(err)
	}
	claim, found, err := s.ClaimDue(context.Background(), "quality", now, time.Minute)
	if err != nil || !found || claim.ID != id {
		t.Fatalf("ClaimDue found=%t claim=%+v err=%v", found, claim, err)
	}
	status := 0
	if ok {
		status = 204
	}
	if _, err := s.Complete(context.Background(), claim, now, RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour, MinSamplesForListing: 1}, ok, status, time.Millisecond, "", meta); err != nil {
		t.Fatal(err)
	}
}

func TestCompleteMaintainsBoundedObservedReliability(t *testing.T) {
	mini, s := newTestStore(t, "quality-history:v1")
	ctx := context.Background()
	proxyURL := "http://history.example.net:8080"
	id := proxyID(proxyURL)
	if _, err := s.Upsert(ctx, []string{proxyURL}, time.Now()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Hour).Add(-30 * time.Minute)
	points := []struct {
		at time.Time
		ok bool
	}{
		{now.Add(-8 * 24 * time.Hour), true},
		{now.Add(-6 * 24 * time.Hour), true},
		{now.Add(-12 * time.Hour), false},
		{now.Add(-time.Hour), true},
		{now, true},
	}
	for _, p := range points {
		completeQualitySample(t, s, id, p.at, p.ok, OutcomeMeta{})
	}
	queryTime := time.Now()
	proxies, err := s.QueryValidated(ctx, ProxyFilter{})
	if err != nil || len(proxies) != 1 {
		t.Fatalf("QueryValidated count=%d err=%v", len(proxies), err)
	}
	got := proxies[0]
	if got.SuccessStreak == nil || *got.SuccessStreak != 2 {
		t.Fatalf("success streak = %v, want 2", got.SuccessStreak)
	}
	if got.Reliability24hSamples != 3 || got.Reliability24hPct == nil || *got.Reliability24hPct != 67 {
		t.Fatalf("24h observed reliability = %d/%v, want 2/3 rounded to 67%%", got.Reliability24hSamples, got.Reliability24hPct)
	}
	if got.Reliability7dSamples != 4 || got.Reliability7dPct == nil || *got.Reliability7dPct != 75 {
		t.Fatalf("7d observed reliability = %d/%v, want 3/4", got.Reliability7dSamples, got.Reliability7dPct)
	}
	if got.ReliabilityFirstSampleAt != points[0].at.UnixMilli() || absInt64(got.ReliabilityCoverageStartAt-queryTime.Add(-7*24*time.Hour).UnixMilli()) > 2*time.Second.Milliseconds() {
		t.Fatalf("history boundaries = first:%d (want %d) coverage:%d (want near %d)", got.ReliabilityFirstSampleAt, points[0].at.UnixMilli(), got.ReliabilityCoverageStartAt, queryTime.Add(-7*24*time.Hour).UnixMilli())
	}
	key := s.proxyKey(id)
	buckets := mini.HGet(key, "reliability_buckets")
	if len(strings.Split(buckets, ",")) > 168 {
		t.Fatalf("reliability history has too many buckets: %d", len(strings.Split(buckets, ",")))
	}
	filtered, err := s.QueryValidated(ctx, ProxyFilter{MinReliability7dSamples: 5})
	if err != nil || len(filtered) != 0 {
		t.Fatalf("minimum observed sample filter returned %d err=%v", len(filtered), err)
	}
}

func TestCompleteStartsSparseHistoryOnFirstSuccessAndBoundsIt(t *testing.T) {
	mini, s := newTestStore(t, "quality-sparse:v1")
	ctx := context.Background()
	proxyURL := "http://sparse.example.net:8080"
	id := proxyID(proxyURL)
	if _, err := s.Upsert(ctx, []string{proxyURL}, time.Now()); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Hour)
	completeQualitySample(t, s, id, now, false, OutcomeMeta{})
	key := s.proxyKey(id)
	if mini.HGet(key, "reliability_buckets") != "" || mini.HGet(key, "reliability_started_at_ms") != "" {
		t.Fatal("initial failure allocated reliability history")
	}
	for i := 0; i < 175; i++ {
		completeQualitySample(t, s, id, now.Add(time.Duration(i+1)*time.Hour), true, OutcomeMeta{})
	}
	encoded := mini.HGet(key, "reliability_buckets")
	if got := len(strings.Split(encoded, ",")); got != 168 {
		t.Fatalf("retained buckets = %d, want cap 168", got)
	}
	if raw := mini.HGet(key, "success_streak"); raw != "175" {
		t.Fatalf("success_streak = %q, want 175", raw)
	}
}

func TestExitMetadataChangesAndStableExitUseMeasuredObservations(t *testing.T) {
	mini, s := newTestStore(t, "quality-exit:v1")
	ctx := context.Background()
	ids := map[string]string{}
	for _, name := range []string{"changing", "stable", "unknown-hosting"} {
		proxyURL := "http://" + name + ".example.net:8080"
		ids[name] = proxyID(proxyURL)
		if _, err := s.Upsert(ctx, []string{proxyURL}, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now().Truncate(time.Hour)
	falseValue := false
	trueValue := true
	completeQualitySample(t, s, ids["changing"], now.Add(-3*time.Hour), true, OutcomeMeta{ExitIP: "198.51.100.1", ExitCountry: "US", ExitASN: "AS64501", ExitOrg: "First Org", Hosting: &falseValue})
	completeQualitySample(t, s, ids["changing"], now.Add(-2*time.Hour), true, OutcomeMeta{ExitIP: "198.51.100.1", ExitCountry: "US", ExitASN: "AS64501", ExitOrg: "First Org", Hosting: &falseValue})
	// A failed check without an actual IP must not refresh exit measurement time.
	completeQualitySample(t, s, ids["changing"], now.Add(-90*time.Minute), false, OutcomeMeta{ExitASN: "AS99999", ExitOrg: "wrong tuple", Hosting: &trueValue})
	if got, want := mini.HGet(s.proxyKey(ids["changing"]), "exit_checked_at_ms"), strconv.FormatInt(now.Add(-2*time.Hour).UnixMilli(), 10); got != want {
		t.Fatalf("missing exit measurement refreshed timestamp to %q, want %q", got, want)
	}
	completeQualitySample(t, s, ids["changing"], now.Add(-time.Hour), true, OutcomeMeta{ExitIP: "203.0.113.7", ExitCountry: "", ExitASN: "", ExitOrg: "", Hosting: nil})
	completeQualitySample(t, s, ids["changing"], now, true, OutcomeMeta{ExitIP: "203.0.113.7", ExitASN: "", ExitOrg: "", Hosting: nil})
	completeQualitySample(t, s, ids["stable"], now.Add(-2*time.Hour), true, OutcomeMeta{ExitIP: "192.0.2.77", ExitASN: "AS64502", ExitOrg: "Stable Org", Hosting: &trueValue})
	completeQualitySample(t, s, ids["stable"], now, true, OutcomeMeta{ExitIP: "192.0.2.77", ExitASN: "AS64502", ExitOrg: "Stable Org", Hosting: &trueValue})
	completeQualitySample(t, s, ids["unknown-hosting"], now.Add(-time.Hour), true, OutcomeMeta{ExitIP: "192.0.2.88", ExitASN: "AS64503"})
	completeQualitySample(t, s, ids["unknown-hosting"], now, true, OutcomeMeta{ExitIP: "192.0.2.88", ExitASN: "AS64503"})

	all, err := s.QueryValidated(ctx, ProxyFilter{})
	if err != nil || len(all) != 3 {
		t.Fatalf("QueryValidated count=%d err=%v", len(all), err)
	}
	byHost := map[string]QueryProxy{}
	for _, p := range all {
		byHost[p.URL] = p
	}
	changing := byHost["http://changing.example.net:8080"]
	if changing.ExitIP != "203.0.113.7" || changing.ExitASN != "" || changing.ExitOrg != "" || changing.Hosting != nil {
		t.Fatalf("new IP carried prior enrichment: %+v", changing)
	}
	if changing.ExitIPChanges == nil || *changing.ExitIPChanges != 1 || changing.ExitIPSamples == nil || *changing.ExitIPSamples != 4 {
		var changes, samples int64
		if changing.ExitIPChanges != nil {
			changes = *changing.ExitIPChanges
		}
		if changing.ExitIPSamples != nil {
			samples = *changing.ExitIPSamples
		}
		t.Fatalf("exit observations = changes:%d samples:%d", changes, samples)
	}
	if got := mini.HGet(s.proxyKey(ids["changing"]), "exit_checked_at_ms"); got != strconv.FormatInt(now.UnixMilli(), 10) {
		t.Fatalf("exit checked timestamp = %q, want latest actual IP observation", got)
	}
	if mini.HGet(s.proxyKey(ids["changing"]), "exit_country") != "" {
		t.Fatal("new IP without exit country retained the previous IP country")
	}
	if _, err := s.QueryValidated(ctx, ProxyFilter{StableExit: true}); err != nil {
		t.Fatal(err)
	}
	stable, err := s.QueryValidated(ctx, ProxyFilter{StableExit: true})
	if err != nil || len(stable) != 2 {
		t.Fatalf("stable-exit filter returned %d err=%v; changing IP and no-measurement history must not match", len(stable), err)
	}
	byASN, err := s.QueryValidated(ctx, ProxyFilter{ExitASN: "AS64502"})
	if err != nil || len(byASN) != 1 || byASN[0].URL != "http://stable.example.net:8080" {
		t.Fatalf("exit ASN filter = %+v err=%v", byASN, err)
	}
	withoutHosting, err := s.QueryValidated(ctx, ProxyFilter{ExcludeHosting: true})
	if err != nil || len(withoutHosting) != 2 {
		t.Fatalf("exclude-hosting must retain unknown and explicitly false: %d err=%v", len(withoutHosting), err)
	}
	if _, err := s.QueryValidated(ctx, ProxyFilter{ExitASN: "AS64502", ClassificationMaxAgeMs: time.Hour.Milliseconds()}); err != nil {
		t.Fatal(err)
	}
	tooOld, err := s.QueryValidated(ctx, ProxyFilter{ExitASN: "AS64502", ClassificationMaxAgeMs: time.Minute.Milliseconds()})
	if err != nil || len(tooOld) != 0 {
		t.Fatalf("stale exit ASN filter returned %d err=%v", len(tooOld), err)
	}
}

func absInt64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func TestQueryValidatedTreatsFutureQualityMeasurementsAsUnknown(t *testing.T) {
	mini, s := newTestStore(t, "quality-future:v1")
	now := time.Now()
	futureMs := strconv.FormatInt(now.Add(48*time.Hour).UnixMilli(), 10)
	futureHour := now.Add(49*time.Hour).Unix() / int64(time.Hour/time.Second)
	id := "future"
	mini.HSet(s.proxyKey(id),
		"url", "http://future.example.net:8080", "country", "US", "last_checked_at_ms", futureMs,
		"success_streak", "5", "exit_ip", "203.0.113.9", "exit_asn", "AS64500", "exit_org", "Example Cloud",
		"exit_hosting", "1", "exit_checked_at_ms", futureMs,
		"reliability_started_at_ms", futureMs,
		"reliability_buckets", strconv.FormatInt(futureHour, 10)+":4:4")
	mini.SAdd("quality-future:v1:validated", id)
	got, err := s.QueryValidated(context.Background(), ProxyFilter{})
	if err != nil || len(got) != 1 {
		t.Fatalf("query count=%d err=%v", len(got), err)
	}
	entry := got[0]
	if entry.ExitIP != "" || entry.ExitASN != "" || entry.ExitOrg != "" || entry.Hosting != nil {
		t.Fatalf("future exit classification was trusted: %+v", entry)
	}
	if entry.SuccessStreak != nil || entry.Reliability7dSamples != 0 || entry.Reliability7dPct != nil || entry.ReliabilityFirstSampleAt != 0 {
		t.Fatalf("future quality history was trusted: %+v", entry)
	}
	_ = mini
}
