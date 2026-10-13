package store

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestSourceFetchCostRecordsBoundedAggregates(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-fetch-cost:v1")
	id := StableSourceID("https://feeds.example/cost", "http")
	now := time.Unix(1_800_000_000, 0)
	if err := s.RecordSourceFetchCost(ctx, id, 2, 4096, 125, now); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordSourceFetchCost(ctx, id, 1, 1024, 75, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	quality, err := s.LoadSourceQuality(ctx, []string{id}, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	got := quality[id]
	if got.HTTPAttempts != 3 || got.NetworkBytes != 5120 || got.FetchDurationMS != 200 || got.FetchCostObservations != 2 {
		t.Fatalf("fetch cost = %+v", got)
	}
	if ttl := miniTTL(t, s.client, s.sourceQualityKey(id)); ttl <= 0 || ttl > sourceQualityTTL {
		t.Fatalf("source quality TTL = %s", ttl)
	}
	for _, input := range []struct {
		attempts, bytes, duration int64
	}{
		{-1, 0, 0}, {0, -1, 0}, {0, 0, -1},
		{maxSourceHTTPAttempts + 1, 0, 0}, {0, maxSourceNetworkBytes + 1, 0},
		{0, 0, maxSourceCostDurationMS + 1},
	} {
		if err := s.RecordSourceFetchCost(ctx, id, input.attempts, input.bytes, input.duration, now); err == nil {
			t.Fatalf("accepted invalid fetch cost %+v", input)
		}
	}
	quality, err = s.LoadSourceQuality(ctx, []string{id}, now.Add(time.Second))
	if err != nil || quality[id].HTTPAttempts != 3 || quality[id].FetchCostObservations != 2 {
		t.Fatalf("invalid observations changed counters: quality=%+v err=%v", quality[id], err)
	}
}

func TestSourceAdmissionCountsOnlyActualInsertionsAndTracksCompletedRefreshes(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-admission:v1")
	now := time.Unix(1_800_000_000, 0)
	first := StableSourceID("https://feeds.example/first", "http")
	second := StableSourceID("https://feeds.example/second", "http")
	if err := s.SetActiveLearningSources(ctx, []string{first, second}); err != nil {
		t.Fatal(err)
	}
	urls := []string{"http://one.example:8080", "http://two.example:8080"}
	counts, err := s.UpsertPrioritizedSourceCounts(ctx, urls, now, first)
	if err != nil || counts.Added != 2 {
		t.Fatalf("initial source upsert=%+v err=%v", counts, err)
	}
	// Reimporting present candidates updates their schedule/provenance but does
	// not count as a new admission.
	counts, err = s.UpsertPrioritizedSourceCounts(ctx, urls, now.Add(time.Second), first)
	if err != nil || counts.Added != 0 {
		t.Fatalf("duplicate source upsert=%+v err=%v", counts, err)
	}
	claim, ok, err := s.ClaimDue(ctx, "worker", now.Add(time.Second), time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim before leased duplicate=(%+v,%t,%v)", claim, ok, err)
	}
	counts, err = s.UpsertPrioritizedSourceCounts(ctx, []string{urls[0]}, now.Add(2*time.Second), first)
	if err != nil || counts.Added != 0 {
		t.Fatalf("leased duplicate upsert=%+v err=%v", counts, err)
	}
	leaseToken, err := s.client.HGet(ctx, s.proxyKey(claim.ID), "lease_token").Result()
	if err != nil || leaseToken != claim.Token {
		t.Fatalf("lease token after duplicate admission=%q err=%v", leaseToken, err)
	}
	blocked := "http://blocked.example:8080"
	if err := mini.Set(s.tombstoneKey(proxyID(blocked)), fmt.Sprintf("1:%d", now.Add(time.Hour).UnixMilli())); err != nil {
		t.Fatal(err)
	}
	counts, err = s.UpsertPrioritizedSourceCounts(ctx, []string{blocked}, now, second)
	if err != nil || counts.Added != 0 || counts.Tombstoned != 1 {
		t.Fatalf("tombstoned source upsert=%+v err=%v", counts, err)
	}
	quality, err := s.LoadSourceQuality(ctx, []string{first, second}, now)
	if err != nil || quality[first].Admitted != 2 || quality[second].Admitted != 0 {
		t.Fatalf("actual admissions first=%+v second=%+v err=%v", quality[first], quality[second], err)
	}

	// A completed-refresh report updates the observed latest values and zero
	// streaks; it does not double-increment the actual SADD-derived total.
	if err := s.RecordSourceAdmissionRefresh(ctx, map[string]int64{first: 2, second: 0}, now.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordSourceAdmissionRefresh(ctx, map[string]int64{first: 0, second: 0}, now.Add(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := s.RecordSourceAdmissionRefresh(ctx, map[string]int64{first: 1}, now.Add(4*time.Second)); err != nil {
		t.Fatal(err)
	}
	quality, err = s.LoadSourceQuality(ctx, []string{first, second}, now.Add(4*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if got := quality[first]; got.Admitted != 2 || got.AdmissionRefreshes != 3 || got.LastAdmitted != 1 || got.ZeroAdmissionStreak != 0 || !got.LastAdmissionAt.Equal(now.Add(4*time.Second)) {
		t.Fatalf("first admission state = %+v", got)
	}
	if got := quality[second]; got.Admitted != 0 || got.AdmissionRefreshes != 2 || got.LastAdmitted != 0 || got.ZeroAdmissionStreak != 2 || !got.LastAdmissionAt.Equal(now.Add(3*time.Second)) {
		t.Fatalf("second admission state = %+v", got)
	}
	if ttl := miniTTL(t, s.client, s.sourceQualityKey(first)); ttl <= 0 || ttl > sourceQualityTTL {
		t.Fatalf("source quality TTL = %s", ttl)
	}
}

func TestCorruptAdmissionTelemetryNeverBlocksCandidateImports(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-admission-corrupt:v1")
	now := time.Unix(1_800_000_000, 0)
	id := StableSourceID("https://feeds.example/corrupt-admission", "http")
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		seed       func(key string)
		wantStored string
	}{
		{
			name:       "malformed counter",
			seed:       func(key string) { mini.HSet(key, "admitted", "not-an-integer") },
			wantStored: "not-an-integer",
		},
		{
			name:       "counter at exact cap",
			seed:       func(key string) { mini.HSet(key, "admitted", fmt.Sprint(int64(1<<53-1))) },
			wantStored: fmt.Sprint(int64(1<<53 - 1)),
		},
		{
			name: "wrong-type quality key",
			seed: func(key string) {
				if err := mini.Set(key, "legacy non-hash value"); err != nil {
					t.Fatal(err)
				}
			},
			wantStored: "legacy non-hash value",
		},
	}
	for index, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url := fmt.Sprintf("http://admission-%d.example:8080", index)
			idKey := s.sourceQualityKey(id)
			mini.Del(idKey)
			tc.seed(idKey)
			counts, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now.Add(time.Duration(index)*time.Second), id)
			if err != nil || counts.Added != 1 || counts.Tombstoned != 0 {
				t.Fatalf("corrupt telemetry blocked upsert: counts=%+v err=%v", counts, err)
			}
			candidateID := proxyID(url)
			exists, err := s.client.SIsMember(ctx, s.candidatesKey(), candidateID).Result()
			if err != nil || !exists {
				t.Fatalf("candidate membership=%t err=%v", exists, err)
			}
			if due, err := s.client.ZScore(ctx, s.pendingKey(), candidateID).Result(); err != nil || due != float64(now.Add(time.Duration(index)*time.Second).UnixMilli()) {
				t.Fatalf("pending score=%v err=%v", due, err)
			}
			if got, err := s.client.HGet(ctx, s.proxyKey(candidateID), "source_id").Result(); err != nil || got != id {
				t.Fatalf("candidate provenance=%q err=%v", got, err)
			}
			var stored string
			if tc.name == "wrong-type quality key" {
				stored, err = mini.Get(idKey)
			} else {
				stored = mini.HGet(idKey, "admitted")
			}
			if err != nil || stored != tc.wantStored {
				t.Fatalf("corrupt admission counter changed: got=%q err=%v want=%q", stored, err, tc.wantStored)
			}
		})
	}
}

func TestLegacySourceQualityHasUnmeasuredEfficiencyFields(t *testing.T) {
	quality := decodeSourceQuality(map[string]string{"refresh_fetches": "1"}, time.Unix(1_800_000_000, 0))
	if quality.Admitted != 0 || quality.AdmissionRefreshes != 0 || quality.LastAdmitted != 0 || quality.ZeroAdmissionStreak != 0 || !quality.LastAdmissionAt.IsZero() ||
		quality.HTTPAttempts != 0 || quality.NetworkBytes != 0 || quality.FetchDurationMS != 0 || quality.FetchCostObservations != 0 ||
		quality.ProbeCompletions != 0 || quality.ProbeSuccessfulCompletions != 0 || quality.ProbeDurationMS != 0 || quality.Corrupt {
		t.Fatalf("legacy hash should decode efficiency values as unmeasured zeroes: %+v", quality)
	}
}

func TestSourceAdmissionRefreshRejectsInvalidBatchWithoutPartialWrites(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-admission-invalid:v1")
	now := time.Unix(1_800_000_000, 0)
	valid := StableSourceID("https://feeds.example/valid", "http")
	if err := s.RecordSourceAdmissionRefresh(ctx, map[string]int64{valid: 1}, now); err != nil {
		t.Fatal(err)
	}
	for _, counts := range []map[string]int64{
		{valid: -1}, {"invalid": 1},
	} {
		if err := s.RecordSourceAdmissionRefresh(ctx, counts, now.Add(time.Second)); err == nil {
			t.Fatalf("accepted invalid admission report %v", counts)
		}
	}
	tooMany := make(map[string]int64, MaxSourceQualityReadBatch+1)
	for i := 0; i <= MaxSourceQualityReadBatch; i++ {
		tooMany[StableSourceID(fmt.Sprintf("https://feeds.example/%d", i), "http")] = 0
	}
	if err := s.RecordSourceAdmissionRefresh(ctx, tooMany, now.Add(time.Second)); err == nil {
		t.Fatal("accepted oversized admission batch")
	}
	quality, err := s.LoadSourceQuality(ctx, []string{valid}, now)
	if err != nil || quality[valid].AdmissionRefreshes != 1 || quality[valid].LastAdmitted != 1 {
		t.Fatalf("invalid reports partially changed state: quality=%+v err=%v", quality[valid], err)
	}
}

func TestEfficiencyCounterPreflightRejectsExponentCountersWithoutPartialWrites(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-efficiency-exponent:v1")
	now := time.Unix(1_800_000_000, 0)
	id := StableSourceID("https://feeds.example/cost-exponent", "http")
	key := s.sourceQualityKey(id)
	mini.HSet(key, "http_attempts", "11", "network_bytes", "1e3")
	if err := s.RecordSourceFetchCost(ctx, id, 1, 10, 1, now); err == nil {
		t.Fatal("accepted exponential-notation fetch-cost counter")
	}
	if got := mini.HGet(key, "http_attempts"); got != "11" {
		t.Fatalf("fetch preflight partially incremented earlier field: %q", got)
	}
	if got := mini.HGet(key, "network_bytes"); got != "1e3" {
		t.Fatalf("fetch preflight rewrote malformed field: %q", got)
	}
	if got := mini.HGet(key, "fetch_cost_observations"); got != "" {
		t.Fatalf("fetch preflight created observations field: %q", got)
	}

	first := StableSourceID("https://feeds.example/admission-first", "http")
	second := StableSourceID("https://feeds.example/admission-second", "http")
	goodID, badID := first, second
	if goodID > badID {
		goodID, badID = badID, goodID
	}
	mini.HSet(s.sourceQualityKey(badID), "admission_refreshes", "1e3")
	if err := s.RecordSourceAdmissionRefresh(ctx, map[string]int64{goodID: 1, badID: 0}, now); err == nil {
		t.Fatal("accepted exponential-notation admission counter")
	}
	if got := mini.HGet(s.sourceQualityKey(goodID), "admission_refreshes"); got != "" {
		t.Fatalf("admission preflight partially observed earlier key: %q", got)
	}
	if got := mini.HGet(s.sourceQualityKey(badID), "admission_refreshes"); got != "1e3" {
		t.Fatalf("admission preflight rewrote malformed field: %q", got)
	}
}

func TestCommittedProbeCostIsLeaseGuardedAndContractScoped(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "source-probe-cost:v1")
	now := time.Unix(1_800_000_000, 0)
	url := "http://probe-cost.example:8080"
	id := StableSourceID("https://feeds.example/probe-cost", "http")
	contractA := strings.Repeat("a", 64)
	contractB := strings.Repeat("b", 64)
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now, id); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := s.ClaimDue(ctx, "worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim=(%+v,%t,%v)", claim, ok, err)
	}
	if _, err := s.Complete(ctx, claim, now, RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, true, 200, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contractA, SourceProbeDurationMS: 17,
	}); err != nil {
		t.Fatal(err)
	}
	// Wrong lease tokens cannot update efficiency costs.
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now.Add(time.Hour), id); err != nil {
		t.Fatal(err)
	}
	claim, ok, err = s.ClaimDue(ctx, "worker", now.Add(time.Hour), time.Minute)
	if err != nil || !ok {
		t.Fatalf("second claim=(%+v,%t,%v)", claim, ok, err)
	}
	stale := claim
	stale.Token = "wrong-token"
	result, err := s.Complete(ctx, stale, now.Add(time.Hour), RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, false, 502, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contractA, SourceProbeDurationMS: 50,
	})
	if err != nil || result.Committed {
		t.Fatalf("stale completion=%+v err=%v", result, err)
	}
	// A contract change resets both sampled yield and probe-cost counters, then
	// records the current committed result under the new contract.
	result, err = s.Complete(ctx, claim, now.Add(time.Hour), RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, false, 502, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contractB, SourceProbeDurationMS: 23,
	})
	if err != nil || !result.Committed {
		t.Fatalf("contract-change completion=%+v err=%v", result, err)
	}
	quality, err := s.LoadSourceQuality(ctx, []string{id}, now.Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	got := quality[id]
	if got.ProbeContract != contractB || got.ProbeCompletions != 1 || got.ProbeSuccessfulCompletions != 0 || got.ProbeDurationMS != 23 || got.ProbeSamples != 1 || got.ProbePasses != 0 {
		t.Fatalf("post-contract probe quality=%+v", got)
	}
	if ttl := miniTTL(t, s.client, s.sourceQualityKey(id)); ttl <= 0 || ttl > sourceQualityTTL {
		t.Fatalf("source quality TTL = %s", ttl)
	}
	// Invalid duration metadata must not block a completed probe or alter cost
	// counters. It is omitted rather than leaving the leased candidate stuck.
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now.Add(2*time.Hour), id); err != nil {
		t.Fatal(err)
	}
	claim, ok, err = s.ClaimDue(ctx, "worker", now.Add(2*time.Hour), time.Minute)
	if err != nil || !ok {
		t.Fatalf("invalid-duration claim=(%+v,%t,%v)", claim, ok, err)
	}
	result, err = s.Complete(ctx, claim, now.Add(2*time.Hour), RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, true, 200, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contractB, SourceProbeDurationMS: maxSourceCostDurationMS + 1,
	})
	if err != nil || !result.Committed {
		t.Fatalf("invalid-duration completion=%+v err=%v", result, err)
	}
	quality, err = s.LoadSourceQuality(ctx, []string{id}, now.Add(2*time.Hour))
	if err != nil || quality[id].ProbeCompletions != 1 || quality[id].ProbeSuccessfulCompletions != 0 || quality[id].ProbeDurationMS != 23 {
		t.Fatalf("invalid duration changed probe costs: quality=%+v err=%v", quality[id], err)
	}

	// Disabled attribution (source removed from active catalog) does not add
	// probe-cost evidence even for a valid lease and contract.
	if err := s.SetActiveLearningSources(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now.Add(3*time.Hour), id); err != nil {
		t.Fatal(err)
	}
	claim, ok, err = s.ClaimDue(ctx, "worker", now.Add(3*time.Hour), time.Minute)
	if err != nil || !ok {
		t.Fatalf("inactive claim=(%+v,%t,%v)", claim, ok, err)
	}
	if _, err := s.Complete(ctx, claim, now.Add(3*time.Hour), RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, true, 200, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contractB, SourceProbeDurationMS: 41,
	}); err != nil {
		t.Fatal(err)
	}
	quality, err = s.LoadSourceQuality(ctx, []string{id}, now.Add(3*time.Hour))
	if err != nil || quality[id].ProbeCompletions != 1 || quality[id].ProbeDurationMS != 23 {
		t.Fatalf("inactive source changed probe costs: quality=%+v err=%v", quality[id], err)
	}
}

func TestMalformedProbeCostCounterDoesNotLoseCommittedOutcome(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-probe-cost-exponent:v1")
	now := time.Unix(1_800_000_000, 0)
	url := "http://probe-cost-counter.example:8080"
	id := StableSourceID("https://feeds.example/probe-cost-counter", "http")
	contract := strings.Repeat("a", 64)
	if err := s.SetActiveLearningSources(ctx, []string{id}); err != nil {
		t.Fatal(err)
	}
	mini.HSet(s.sourceQualityKey(id), "probe_contract", contract, "probe_completions", "1e3")
	if _, err := s.UpsertPrioritizedSourceCounts(ctx, []string{url}, now, id); err != nil {
		t.Fatal(err)
	}
	claim, ok, err := s.ClaimDue(ctx, "worker", now, time.Minute)
	if err != nil || !ok {
		t.Fatalf("claim=(%+v,%t,%v)", claim, ok, err)
	}
	result, err := s.Complete(ctx, claim, now, RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour}, true, 200, time.Millisecond, "", OutcomeMeta{
		SourceLearning: true, SourceLearningContract: contract, SourceProbeDurationMS: 25,
	})
	if err != nil || !result.Committed {
		t.Fatalf("malformed cost telemetry prevented committed outcome: result=%+v err=%v", result, err)
	}
	if got := mini.HGet(s.proxyKey(claim.ID), "last_status"); got != "ok" {
		t.Fatalf("candidate outcome was not recorded: last_status=%q", got)
	}
	if got := mini.HGet(s.proxyKey(claim.ID), "lease_token"); got != "" {
		t.Fatalf("candidate lease was not cleared: token=%q", got)
	}
	if got := mini.HGet(s.sourceQualityKey(id), "probe_completions"); got != "1e3" {
		t.Fatalf("malformed probe cost counter changed: %q", got)
	}
	if got := mini.HGet(s.sourceQualityKey(id), "probe_successful_completions"); got != "" {
		t.Fatalf("malformed probe counter wrote success total: %q", got)
	}
	if got := mini.HGet(s.sourceQualityKey(id), "probe_duration_ms"); got != "" {
		t.Fatalf("malformed probe counter wrote duration: %q", got)
	}
}

func TestValidatedDetailsExposeOnlyRoutingEvidenceAndFailClosed(t *testing.T) {
	ctx := context.Background()
	mini, s := newTestStore(t, "source-routing-evidence:v1")
	now := time.Unix(1_800_000_000, 0)
	nowMS := now.UnixMilli()
	seed := func(id string, values ...string) {
		t.Helper()
		mini.SAdd(s.validatedKey(), id)
		args := append([]string{"ok_ratio_pct", "80"}, values...)
		mini.HSet(s.proxyKey(id), args...)
	}
	seed("fresh", "source_id", StableSourceID("https://feeds.example/new", "http"), "success_streak", "1", "https_ok", "1", "https_checked_at_ms", fmt.Sprint(nowMS), "tampered", "0", "tamper_checked_at_ms", fmt.Sprint(nowMS))
	seed("stale", "source_id", StableSourceID("https://feeds.example/stale", "http"), "success_streak", "3", "https_ok", "1", "https_checked_at_ms", fmt.Sprint(now.Add(-25*time.Hour).UnixMilli()), "tampered", "0", "tamper_checked_at_ms", fmt.Sprint(nowMS))
	seed("future", "source_id", StableSourceID("https://feeds.example/future", "http"), "success_streak", "2", "https_ok", "1", "https_checked_at_ms", fmt.Sprint(now.Add(time.Second).UnixMilli()), "tampered", "0", "tamper_checked_at_ms", fmt.Sprint(nowMS))
	seed("unknown", "source_id", StableSourceID("https://feeds.example/unknown", "http"), "success_streak", "1", "https_ok", "true", "https_checked_at_ms", fmt.Sprint(nowMS), "tampered", "0", "tamper_checked_at_ms", fmt.Sprint(nowMS))
	seed("dirty", "source_id", StableSourceID("https://feeds.example/dirty", "http"), "success_streak", "1", "https_ok", "1", "https_checked_at_ms", fmt.Sprint(nowMS), "tampered", "1", "tamper_checked_at_ms", fmt.Sprint(nowMS))
	infos, err := s.ValidatedDetails(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eligible := map[string]bool{}
	bySource := map[string]int{}
	for _, info := range infos {
		eligible[info.SourceID] = info.RoutingEligible(now, 24*time.Hour)
		if info.SourceID != "" {
			bySource[info.SourceID]++
		}
	}
	freshSource := StableSourceID("https://feeds.example/new", "http")
	if bySource[freshSource] != 1 || !eligible[freshSource] {
		t.Fatalf("fresh source evidence count=%d eligible=%v", bySource[freshSource], eligible[freshSource])
	}
	for _, id := range []string{"stale", "future", "unknown", "dirty"} {
		sourceID := StableSourceID("https://feeds.example/"+id, "http")
		if eligible[sourceID] {
			t.Fatalf("ineligible evidence fixture %q passed routing gate", id)
		}
	}
	// The method has no last_checked_at gate; its API is exactly the listed
	// evidence tuple plus ratio and streak.
	noLastCheck := ValidatedInfo{OkRatioPct: 80, SuccessStreak: ptr(int64(1)), HTTPSOK: ptr(true), HTTPSCheckedAt: nowMS, Tampered: ptr(false), TamperCheckedAt: nowMS}
	if !noLastCheck.RoutingEligible(now, 24*time.Hour) {
		t.Fatal("routing eligibility added an unrelated last-check timestamp gate")
	}
	if noLastCheck.RoutingEligible(now, time.Hour) == false {
		t.Fatal("fresh routing evidence should remain eligible with a stricter age bound")
	}
	missingTimes := noLastCheck
	missingTimes.HTTPSCheckedAt, missingTimes.TamperCheckedAt = 0, 0
	if missingTimes.RoutingEligible(now, 0) {
		t.Fatal("zero max age must not trust missing check timestamps")
	}
}

func ptr[T any](value T) *T { return &value }
