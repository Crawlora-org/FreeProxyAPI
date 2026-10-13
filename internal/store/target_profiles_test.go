package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func completeWithTargetProfile(t *testing.T, s *Redis, rawURL string, at time.Time, update *TargetProfileUpdate) (Claim, CompleteResult) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.Upsert(ctx, []string{rawURL}, at); err != nil {
		t.Fatal(err)
	}
	if err := s.client.ZAdd(ctx, s.pendingKey(), redisZ(proxyID(rawURL), at.Add(-time.Millisecond).UnixMilli())).Err(); err != nil {
		t.Fatal(err)
	}
	claim, found, err := s.ClaimDue(ctx, "target-profile-test", at, time.Minute)
	if err != nil || !found || claim.URL != rawURL {
		t.Fatalf("ClaimDue found=%t claim=%+v err=%v", found, claim, err)
	}
	result, err := s.Complete(ctx, claim, at, RetestPolicy{ValidatedAfter: time.Hour, FailedAfter: time.Hour, MinSamplesForListing: 1}, true, 204, time.Millisecond, "", OutcomeMeta{TargetProfiles: update})
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	return claim, result
}

func profileResult(fingerprint, state string, checkedAt time.Time) TargetProfileResult {
	return TargetProfileResult{Fingerprint: fingerprint, State: state, CheckedAt: checkedAt.UnixMilli(), HTTPStatus: 204, LatencyMs: 12}
}

func TestCompleteStoresAndFiltersFreshTargetProfile(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "target-profile-filter:v1")
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	fingerprint := strings.Repeat("a", 64)
	url := "http://192.0.2.101:8080"
	completeWithTargetProfile(t, s, url, base, &TargetProfileUpdate{
		Active: map[string]string{"catalog": fingerprint},
		Checks: map[string]TargetProfileResult{"catalog": profileResult(fingerprint, "pass", base)},
	})

	got, err := s.QueryValidated(ctx, ProxyFilter{TargetProfile: "catalog", TargetProfileFingerprint: fingerprint, ClassificationMaxAgeMs: int64(24 * time.Hour / time.Millisecond)})
	if err != nil || len(got) != 1 || got[0].URL != url {
		t.Fatalf("matching target profile query = %d, err=%v", len(got), err)
	}
	got, err = s.QueryValidated(ctx, ProxyFilter{TargetProfile: "catalog", TargetProfileFingerprint: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("mismatched fingerprint returned %d candidates", len(got))
	}
	stored, err := s.client.HGet(ctx, s.proxyKey(proxyID(url)), "target_profiles").Result()
	if err != nil {
		t.Fatal(err)
	}
	profiles := FreshTargetProfiles(stored, time.Now().UnixMilli(), int64(24*time.Hour/time.Millisecond))
	if profiles["catalog"].Fingerprint != fingerprint || profiles["catalog"].State != "pass" {
		t.Fatalf("stored profiles = %+v", profiles)
	}
}

func TestTargetProfileQueryRejectsExpiredFutureAndUnknownResults(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "target-profile-freshness:v1")
	now := time.Now().Add(-time.Hour).Truncate(time.Minute)
	fingerprint := strings.Repeat("c", 64)
	cases := []struct {
		name  string
		state string
		at    time.Time
	}{
		{name: "expired", state: "pass", at: now.Add(-2 * time.Hour)},
		{name: "future", state: "pass", at: time.Now().Add(time.Hour)},
		{name: "unknown", state: "unknown", at: now},
		{name: "failed", state: "fail", at: now},
	}
	for i, tc := range cases {
		url := fmt.Sprintf("http://192.0.2.%d:8080", 102+i)
		checkedAt := tc.at
		completeWithTargetProfile(t, s, url, now, &TargetProfileUpdate{
			Active: map[string]string{"catalog": fingerprint},
			Checks: map[string]TargetProfileResult{"catalog": profileResult(fingerprint, tc.state, checkedAt)},
		})
	}
	filter := ProxyFilter{TargetProfile: "catalog", TargetProfileFingerprint: fingerprint, ClassificationMaxAgeMs: int64(time.Hour / time.Millisecond)}
	got, err := s.QueryValidated(ctx, filter)
	if err != nil || len(got) != 0 {
		t.Fatalf("non-fresh/non-pass profile query returned %d candidates, err=%v", len(got), err)
	}

	// The same checks remain excluded when evaluated individually, so a passing
	// sibling cannot hide which unsafe state caused a match.
	for i, tc := range cases {
		url := fmt.Sprintf("http://192.0.2.%d:8080", 102+i)
		id := proxyID(url)
		got, err := s.queryValidatedIDs(ctx, []string{id}, filter, make(map[string]struct{}))
		if err != nil || len(got) != 0 {
			t.Errorf("%s profile matched: count=%d err=%v", tc.name, len(got), err)
		}
	}
}

func TestTargetProfileNameReusePrunesOldFingerprint(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "target-profile-reuse:v1")
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	url := "http://192.0.2.106:8080"
	oldFP, newFP := strings.Repeat("1", 64), strings.Repeat("2", 64)
	completeWithTargetProfile(t, s, url, base, &TargetProfileUpdate{
		Active: map[string]string{"production": oldFP},
		Checks: map[string]TargetProfileResult{"production": profileResult(oldFP, "pass", base)},
	})
	completeWithTargetProfile(t, s, url, base.Add(time.Second), &TargetProfileUpdate{
		Active: map[string]string{"production": newFP},
		Checks: map[string]TargetProfileResult{},
	})
	stored, err := s.client.HGet(ctx, s.proxyKey(proxyID(url)), "target_profiles").Result()
	if err != nil {
		t.Fatal(err)
	}
	var profiles map[string]TargetProfileResult
	if err := json.Unmarshal([]byte(stored), &profiles); err != nil {
		t.Fatalf("stored profile JSON %q: %v", stored, err)
	}
	if stored != "{}" || len(profiles) != 0 {
		t.Fatalf("empty active observations were not encoded as an object: %q / %+v", stored, profiles)
	}
	got, err := s.QueryValidated(ctx, ProxyFilter{TargetProfile: "production", TargetProfileFingerprint: newFP})
	if err != nil || len(got) != 0 {
		t.Fatalf("new fingerprint matched stale observation: count=%d err=%v", len(got), err)
	}
}

func TestTargetProfileLimitsMalformedAndLegacyJSONAreSafe(t *testing.T) {
	active := make(map[string]string, MaxTargetProfiles+1)
	for i := 0; i <= MaxTargetProfiles; i++ {
		active[fmt.Sprintf("profile-%d", i)] = strings.Repeat("a", 64)
	}
	if encoded := encodeTargetProfileUpdate(&TargetProfileUpdate{Active: active}); encoded != "" {
		t.Fatalf("oversized active target update encoded %q", encoded)
	}
	if got := FreshTargetProfiles("not-json", time.Now().UnixMilli(), 0); got != nil {
		t.Fatalf("malformed stored JSON returned profiles: %+v", got)
	}
	tooMany, err := json.Marshal(active)
	if err != nil {
		t.Fatal(err)
	}
	if got := FreshTargetProfiles(string(tooMany), time.Now().UnixMilli(), 0); got != nil {
		t.Fatalf("legacy JSON with >%d profiles returned data", MaxTargetProfiles)
	}

	ctx := context.Background()
	_, s := newTestStore(t, "target-profile-legacy:v1")
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	url := "http://192.0.2.107:8080"
	if _, err := s.Upsert(ctx, []string{url}, base); err != nil {
		t.Fatal(err)
	}
	key := s.proxyKey(proxyID(url))
	if err := s.client.HSet(ctx, key, "target_profiles", "{broken legacy json").Err(); err != nil {
		t.Fatal(err)
	}
	completeWithTargetProfile(t, s, url, base.Add(time.Second), &TargetProfileUpdate{
		Active: map[string]string{"current": strings.Repeat("b", 64)},
		Checks: map[string]TargetProfileResult{"current": profileResult(strings.Repeat("b", 64), "pass", base.Add(time.Second))},
	})
	stored, err := s.client.HGet(ctx, key, "target_profiles").Result()
	if err != nil {
		t.Fatal(err)
	}
	var profiles map[string]TargetProfileResult
	if err := json.Unmarshal([]byte(stored), &profiles); err != nil {
		t.Fatalf("Complete did not recover malformed legacy JSON: %q (%v)", stored, err)
	}
	if profiles["current"].State != "pass" {
		t.Fatalf("malformed legacy data blocked current result: %+v", profiles)
	}
}

func TestStaleLeaseCannotUpdateTargetProfiles(t *testing.T) {
	ctx := context.Background()
	_, s := newTestStore(t, "target-profile-lease:v1")
	base := time.Now().Add(-time.Hour).Truncate(time.Minute)
	url := "http://192.0.2.108:8080"
	fingerprint := strings.Repeat("d", 64)
	claim, _ := completeWithTargetProfile(t, s, url, base, &TargetProfileUpdate{
		Active: map[string]string{"current": fingerprint},
		Checks: map[string]TargetProfileResult{"current": profileResult(fingerprint, "pass", base)},
	})

	if _, err := s.client.ZAdd(ctx, s.pendingKey(), redisZ(proxyID(url), base.Add(time.Second).UnixMilli())).Result(); err != nil {
		t.Fatal(err)
	}
	current, found, err := s.ClaimDue(ctx, "current", base.Add(2*time.Second), time.Minute)
	if err != nil || !found {
		t.Fatalf("second ClaimDue found=%t err=%v", found, err)
	}
	before, err := s.client.HGet(ctx, s.proxyKey(claim.ID), "target_profiles").Result()
	if err != nil {
		t.Fatal(err)
	}
	stale := current
	stale.Token = "stale-token"
	result, err := s.Complete(ctx, stale, base.Add(3*time.Second), RetestPolicy{}, true, 204, time.Millisecond, "", OutcomeMeta{TargetProfiles: &TargetProfileUpdate{
		Active: map[string]string{"current": strings.Repeat("e", 64)},
		Checks: map[string]TargetProfileResult{"current": profileResult(strings.Repeat("e", 64), "pass", base.Add(3*time.Second))},
	}})
	if err != nil || result.Committed {
		t.Fatalf("stale Complete=%+v err=%v", result, err)
	}
	after, err := s.client.HGet(ctx, s.proxyKey(claim.ID), "target_profiles").Result()
	if err != nil || after != before {
		t.Fatalf("stale lease changed profiles from %q to %q, err=%v", before, after, err)
	}
}

func redisZ(member string, score int64) redis.Z {
	return redis.Z{Member: member, Score: float64(score)}
}
