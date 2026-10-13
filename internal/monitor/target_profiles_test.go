package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func targetProfileFixture(name string) TargetProfile {
	return TargetProfile{Name: name, URL: "https://profile.example.test/health", ExpectedBody: "healthy"}
}

func newTargetProfileTestRunner(t *testing.T, budget int, profiles []TargetProfile) *Runner {
	t.Helper()
	runner := newBudgetTestRunner(t, budget)
	runner.config.TargetProfiles = append([]TargetProfile(nil), profiles...)
	runner.config.TargetProbeEvery = 1
	runner.accuracy.sampleIntN = func(int) int { return 0 }
	runner.targets.controlFunc = func(context.Context, TargetProfile) error { return nil }
	return runner
}

func targetResult(t *testing.T, meta store.OutcomeMeta, name string) store.TargetProfileResult {
	t.Helper()
	if meta.TargetProfiles == nil {
		t.Fatal("target profile update was not recorded")
	}
	result, ok := meta.TargetProfiles.Checks[name]
	if !ok {
		t.Fatalf("target profile %q missing from checks: %+v", name, meta.TargetProfiles)
	}
	return result
}

func TestTargetProfileConfigValidationAndBounds(t *testing.T) {
	profiles := make([]TargetProfile, store.MaxTargetProfiles)
	for i := range profiles {
		profiles[i] = targetProfileFixture(fmt.Sprintf("site-%d", i))
	}
	valid := loadConfigWithTargetProfiles(t, profiles, 1)
	if len(valid.TargetProfiles) != store.MaxTargetProfiles || valid.TargetProbeEvery != 1 {
		t.Fatalf("loaded target profile config=%+v", valid)
	}

	tooMany := append(append([]TargetProfile(nil), profiles...), targetProfileFixture("extra"))
	if _, err := LoadConfig(writeConfig(t, targetProfileConfigJSON(t, tooMany, 1))); err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("accepted %d profiles: err=%v", len(tooMany), err)
	}
	if got := loadConfigWithTargetProfiles(t, nil, 0).TargetProbeEvery; got != defaultTargetProbeEvery {
		t.Fatalf("default target_probe_every=%d, want %d", got, defaultTargetProbeEvery)
	}
	if _, err := LoadConfig(writeConfig(t, targetProfileConfigJSON(t, profiles[:1], 1001))); err == nil || !strings.Contains(err.Error(), "target_probe_every") {
		t.Fatalf("accepted out-of-range target_probe_every: err=%v", err)
	}

	for _, tc := range []struct {
		name     string
		profiles []TargetProfile
	}{
		{name: "empty required body", profiles: []TargetProfile{{Name: "site", URL: "https://example.test/health"}}},
		{name: "oversized body", profiles: []TargetProfile{{Name: "site", URL: "https://example.test/health", ExpectedBody: strings.Repeat("x", int(probe.MaxResponseBytes)+1)}}},
		{name: "invalid name", profiles: []TargetProfile{{Name: "Site", URL: "https://example.test/health", ExpectedBody: "ok"}}},
		{name: "duplicate name", profiles: []TargetProfile{targetProfileFixture("same"), targetProfileFixture("same")}},
		{name: "name too long", profiles: []TargetProfile{{Name: "a" + strings.Repeat("b", 32), URL: "https://example.test/health", ExpectedBody: "ok"}}},
		{name: "URL credentials", profiles: []TargetProfile{{Name: "site", URL: "https://user:pass@example.test/health", ExpectedBody: "ok"}}},
		{name: "URL fragment", profiles: []TargetProfile{{Name: "site", URL: "https://example.test/health#fragment", ExpectedBody: "ok"}}},
		{name: "unsupported URL scheme", profiles: []TargetProfile{{Name: "site", URL: "ftp://example.test/health", ExpectedBody: "ok"}}},
		{name: "missing URL host", profiles: []TargetProfile{{Name: "site", URL: "https:///health", ExpectedBody: "ok"}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateTargetProfiles(append([]TargetProfile(nil), tc.profiles...)); err == nil {
				t.Fatal("invalid target profile config accepted")
			}
		})
	}
	maxBody := []TargetProfile{{Name: "site", URL: "https://example.test/health", ExpectedBody: strings.Repeat("x", int(probe.MaxResponseBytes))}}
	if err := validateTargetProfiles(maxBody); err != nil {
		t.Fatalf("maximum bounded body rejected: %v", err)
	}
}

func targetProfileConfigJSON(t *testing.T, profiles []TargetProfile, every int) string {
	t.Helper()
	data, err := json.Marshal(map[string]any{
		"sources":            []string{"file:///data/proxies.txt"},
		"target_profiles":    profiles,
		"target_probe_every": every,
	})
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func loadConfigWithTargetProfiles(t *testing.T, profiles []TargetProfile, every int) Config {
	t.Helper()
	config, err := LoadConfig(writeConfig(t, targetProfileConfigJSON(t, profiles, every)))
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func TestSampleTargetProfileUsesOneBudgetPermitAndRecordsBytes(t *testing.T) {
	profile := targetProfileFixture("residential")
	runner := newTargetProfileTestRunner(t, 1, []TargetProfile{profile})
	var calls atomic.Int64
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		calls.Add(1)
		return probe.Result{OK: true, StatusCode: http.StatusOK, Duration: 37 * time.Millisecond, UploadBytes: 123, DownloadBytes: 456}
	}
	meta := store.OutcomeMeta{}
	runner.sampleTargetProfile(context.Background(), "http://proxy.example:8080", probe.Result{OK: true}, &meta)

	got := targetResult(t, meta, profile.Name)
	if got.State != "pass" || got.Fingerprint != profile.fingerprint() || got.CheckedAt != runner.nowFunc().UnixMilli() || got.HTTPStatus != http.StatusOK || got.LatencyMs != 37 || got.ErrorCode != "" {
		t.Fatalf("target profile result=%+v", got)
	}
	if meta.TargetProfiles.Active[profile.Name] != profile.fingerprint() || len(meta.TargetProfiles.Checks) != 1 {
		t.Fatalf("target profile update=%+v", meta.TargetProfiles)
	}
	if calls.Load() != 1 || runner.metrics.budgetDenialsTotal.Load() != 0 {
		t.Fatalf("probe calls=%d budget denials=%d", calls.Load(), runner.metrics.budgetDenialsTotal.Load())
	}
	if runner.metrics.probeUploadBytes.Load() != 123 || runner.metrics.probeDownloadBytes.Load() != 456 {
		t.Fatalf("probe bytes upload=%d download=%d", runner.metrics.probeUploadBytes.Load(), runner.metrics.probeDownloadBytes.Load())
	}
	if allowed, err := runner.store.TakePermit(context.Background(), runner.nowFunc(), 1); err != nil || allowed {
		t.Fatalf("sample did not consume exactly one permit: allowed=%t err=%v", allowed, err)
	}
}

func TestSampleTargetProfileMapsContentFailureAndTransportTimeout(t *testing.T) {
	profile := targetProfileFixture("site")
	for _, tc := range []struct {
		name   string
		result probe.Result
		state  string
		code   string
	}{
		{name: "HTTP 200 content mismatch", result: probe.Result{StatusCode: http.StatusOK, Error: "unexpected response body"}, state: "fail", code: probe.CodeUnexpectedBody},
		{name: "transport timeout", result: probe.Result{Error: "Get: context deadline exceeded", Duration: time.Second}, state: "unknown", code: probe.CodeTimeout},
	} {
		t.Run(tc.name, func(t *testing.T) {
			runner := newTargetProfileTestRunner(t, 2, []TargetProfile{profile})
			runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result { return tc.result }
			meta := store.OutcomeMeta{}
			runner.sampleTargetProfile(context.Background(), "http://proxy.example:8080", probe.Result{OK: true}, &meta)
			got := targetResult(t, meta, profile.Name)
			if got.State != tc.state || got.ErrorCode != tc.code {
				t.Fatalf("target profile result=%+v want state=%s code=%s", got, tc.state, tc.code)
			}
		})
	}
}

func TestUnhealthyTargetOriginRecordsUnknownWithoutProxyPermit(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 1, []TargetProfile{profile})
	runner.targets.controlFunc = func(context.Context, TargetProfile) error { return errors.New("origin unavailable") }
	var probeCalls atomic.Int64
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		probeCalls.Add(1)
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	meta := store.OutcomeMeta{}
	runner.sampleTargetProfile(context.Background(), "http://proxy.example:8080", probe.Result{OK: true}, &meta)
	got := targetResult(t, meta, profile.Name)
	if got.State != "unknown" || got.ErrorCode != "origin_unavailable" || probeCalls.Load() != 0 {
		t.Fatalf("origin outage result=%+v probe calls=%d", got, probeCalls.Load())
	}
	if allowed, err := runner.store.TakePermit(context.Background(), runner.nowFunc(), 1); err != nil || !allowed {
		t.Fatalf("origin control spent proxy permit: allowed=%t err=%v", allowed, err)
	}
	if got := runner.metrics.budgetDenialsTotal.Load(); got != 0 {
		t.Fatalf("budget denials=%d, want 0", got)
	}
}

func TestTargetProfileBudgetDenialSkipsProbeAndCommit(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 1, []TargetProfile{profile})
	if allowed, err := runner.takeProbePermit(context.Background()); err != nil || !allowed {
		t.Fatalf("seed permit: allowed=%t err=%v", allowed, err)
	}
	var probeCalls atomic.Int64
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		probeCalls.Add(1)
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	meta := store.OutcomeMeta{}
	runner.sampleTargetProfile(context.Background(), "http://proxy.example:8080", probe.Result{OK: true}, &meta)
	if meta.TargetProfiles != nil || probeCalls.Load() != 0 || runner.metrics.budgetDenialsTotal.Load() != 1 {
		t.Fatalf("budget skip meta=%+v probe calls=%d denials=%d", meta.TargetProfiles, probeCalls.Load(), runner.metrics.budgetDenialsTotal.Load())
	}
}

func TestCanceledTargetProbeDoesNotCommitProfileUpdate(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 2, []TargetProfile{profile})
	ctx, cancel := context.WithCancel(context.Background())
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		cancel()
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	meta := store.OutcomeMeta{}
	runner.sampleTargetProfile(ctx, "http://proxy.example:8080", probe.Result{OK: true}, &meta)
	if meta.TargetProfiles != nil {
		t.Fatalf("canceled target probe committed metadata: %+v", meta.TargetProfiles)
	}
}

func TestTargetProfilesAreSampledRoundRobin(t *testing.T) {
	profiles := []TargetProfile{targetProfileFixture("alpha"), targetProfileFixture("beta")}
	runner := newTargetProfileTestRunner(t, 4, profiles)
	var sampled []string
	runner.targets.probeFunc = func(_ context.Context, _ string, profile TargetProfile) probe.Result {
		sampled = append(sampled, profile.Name)
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	for i := 0; i < 3; i++ {
		meta := store.OutcomeMeta{}
		runner.sampleTargetProfile(context.Background(), "http://proxy.example:8080", probe.Result{OK: true}, &meta)
		if meta.TargetProfiles == nil || len(meta.TargetProfiles.Checks) != 1 {
			t.Fatalf("sample %d metadata=%+v", i, meta.TargetProfiles)
		}
	}
	if got, want := strings.Join(sampled, ","), "alpha,beta,alpha"; got != want {
		t.Fatalf("sample order=%q, want %q", got, want)
	}
}

func TestConcurrentTargetControlProbeIsCoalesced(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 2, []TargetProfile{profile})
	entered := make(chan struct{})
	release := make(chan struct{})
	var controlCalls, proxyCalls atomic.Int64
	runner.targets.controlFunc = func(context.Context, TargetProfile) error {
		if controlCalls.Add(1) == 1 {
			close(entered)
		}
		<-release
		return nil
	}
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		proxyCalls.Add(1)
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	firstMeta, secondMeta := store.OutcomeMeta{}, store.OutcomeMeta{}
	firstDone := make(chan struct{})
	go func() {
		runner.sampleTargetProfile(context.Background(), "http://first-proxy.example:8080", probe.Result{OK: true}, &firstMeta)
		close(firstDone)
	}()
	<-entered
	runner.sampleTargetProfile(context.Background(), "http://second-proxy.example:8080", probe.Result{OK: true}, &secondMeta)
	second := targetResult(t, secondMeta, profile.Name)
	if second.State != "unknown" || second.ErrorCode != "origin_unavailable" {
		t.Fatalf("concurrent second control result=%+v", second)
	}
	close(release)
	<-firstDone
	first := targetResult(t, firstMeta, profile.Name)
	if first.State != "pass" || controlCalls.Load() != 1 || proxyCalls.Load() != 1 {
		t.Fatalf("first result=%+v control calls=%d proxy calls=%d", first, controlCalls.Load(), proxyCalls.Load())
	}
}

func TestTargetProfileWithinLeaseSkipsWhenLeaseIsSpent(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 1, []TargetProfile{profile})
	runner.config.LeaseTTL = 5 * time.Second
	var controlCalls, proxyCalls atomic.Int64
	runner.targets.controlFunc = func(context.Context, TargetProfile) error {
		controlCalls.Add(1)
		return nil
	}
	runner.targets.probeFunc = func(context.Context, string, TargetProfile) probe.Result {
		proxyCalls.Add(1)
		return probe.Result{OK: true, StatusCode: http.StatusOK}
	}
	basicResult := probe.Result{OK: true, StatusCode: http.StatusNoContent}
	meta := store.OutcomeMeta{}
	runner.sampleTargetProfileWithinLease(context.Background(), "http://proxy.example:8080", basicResult, &meta, time.Now().Add(-10*time.Second))
	if controlCalls.Load() != 0 || proxyCalls.Load() != 0 || meta.TargetProfiles != nil {
		t.Fatalf("spent lease did optional work: control=%d proxy=%d meta=%+v", controlCalls.Load(), proxyCalls.Load(), meta.TargetProfiles)
	}
	if allowed, err := runner.store.TakePermit(context.Background(), runner.nowFunc(), 1); err != nil || !allowed {
		t.Fatalf("spent lease consumed a proxy permit: allowed=%t err=%v", allowed, err)
	}
	if !basicResult.OK || basicResult.StatusCode != http.StatusNoContent {
		t.Fatalf("basic result changed: %+v", basicResult)
	}
}

func TestTargetProfileLeaseDeadlineCancelsOnlyOptionalCheck(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 2, []TargetProfile{profile})
	runner.config.LeaseTTL = 3 * time.Second
	probeStarted := make(chan struct{})
	runner.targets.probeFunc = func(ctx context.Context, _ string, _ TargetProfile) probe.Result {
		close(probeStarted)
		<-ctx.Done()
		return probe.Result{Error: context.DeadlineExceeded.Error(), Duration: time.Second}
	}
	parent, cancelParent := context.WithCancel(context.Background())
	defer cancelParent()
	basicResult := probe.Result{OK: true, StatusCode: http.StatusNoContent}
	meta := store.OutcomeMeta{}
	started := time.Now().Add(-1500 * time.Millisecond) // about 500ms remains after the commit margin
	runner.sampleTargetProfileWithinLease(parent, "http://proxy.example:8080", basicResult, &meta, started)
	select {
	case <-probeStarted:
	default:
		t.Fatal("optional probe did not start inside the remaining lease")
	}
	if parent.Err() != nil {
		t.Fatalf("optional lease deadline canceled the parent context: %v", parent.Err())
	}
	if meta.TargetProfiles != nil {
		t.Fatalf("deadline-bound optional result was committed: %+v", meta.TargetProfiles)
	}
	if !basicResult.OK || basicResult.StatusCode != http.StatusNoContent {
		t.Fatalf("basic result changed after optional timeout: %+v", basicResult)
	}
}

func TestCanceledTargetControlDoesNotPoisonRetry(t *testing.T) {
	profile := targetProfileFixture("site")
	runner := newTargetProfileTestRunner(t, 1, []TargetProfile{profile})
	entered := make(chan struct{})
	var calls atomic.Int64
	runner.targets.controlFunc = func(ctx context.Context, _ TargetProfile) error {
		if calls.Add(1) == 1 {
			close(entered)
			<-ctx.Done()
			return ctx.Err()
		}
		return nil
	}
	parent, cancelParent := context.WithCancel(context.Background())
	firstDone := make(chan bool, 1)
	go func() { firstDone <- runner.targetControlHealthy(parent, profile) }()
	<-entered
	cancelParent()
	if healthy := <-firstDone; healthy {
		t.Fatal("canceled control probe was reported healthy")
	}
	if healthy := runner.targetControlHealthy(context.Background(), profile); !healthy {
		t.Fatal("canceled control probe poisoned the next healthy check")
	}
	if calls.Load() != 2 {
		t.Fatalf("control calls=%d, want canceled attempt plus retry", calls.Load())
	}
}

func TestProxyFilterParsesQualityFieldsAndSeparatesCacheKeys(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/proxies?exit_asn=64500&exclude_hosting=true&stable_exit=true&min_success_streak=7&min_reliability_7d_pct=91&min_reliability_7d_samples=15&target_profile=residential", nil)
	filter, err := proxyFilter(request)
	if err != nil {
		t.Fatal(err)
	}
	if filter.ExitASN != "AS64500" || !filter.ExcludeHosting || !filter.StableExit || filter.MinSuccessStreak != 7 || filter.MinReliability7dPct != 91 || filter.MinReliability7dSamples != 15 || filter.TargetProfile != "residential" {
		t.Fatalf("parsed quality filter=%+v", filter)
	}
	base := store.ProxyFilter{}
	variants := []store.ProxyFilter{
		{ExitASN: "AS64500"}, {ExcludeHosting: true}, {StableExit: true},
		{MinSuccessStreak: 7}, {MinReliability7dPct: 91}, {MinReliability7dSamples: 15},
		{TargetProfile: "residential"}, {TargetProfile: "residential", TargetProfileFingerprint: strings.Repeat("a", 64)},
	}
	for _, variant := range variants {
		if publicProxyCacheKey(base, 0) == publicProxyCacheKey(variant, 0) {
			t.Fatalf("filter omitted from public cache key: %+v", variant)
		}
	}
	if publicProxyCacheKey(store.ProxyFilter{TargetProfile: "residential"}, 0) == publicProxyCacheKey(store.ProxyFilter{TargetProfile: "residential", TargetProfileFingerprint: strings.Repeat("b", 64)}, 0) {
		t.Fatal("target profile fingerprint must isolate cache entries")
	}
	for _, query := range []string{
		"min_success_streak=-1", "min_reliability_7d_pct=101", "min_reliability_7d_samples=1000000001",
	} {
		if _, err := proxyFilter(httptest.NewRequest(http.MethodGet, "/proxies?"+query, nil)); err == nil {
			t.Errorf("accepted invalid quality query %q", query)
		}
	}
}

func TestInternalAPIRequiresConfiguredFreshTargetProfileFingerprint(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "target-api:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	profile := targetProfileFixture("residential")
	nowMs := time.Now().UnixMilli()
	seed := func(id string, result store.TargetProfileResult) {
		t.Helper()
		encoded, err := json.Marshal(map[string]store.TargetProfileResult{"residential": result})
		if err != nil {
			t.Fatal(err)
		}
		mini.SAdd("target-api:v1:validated", id)
		mini.HSet("target-api:v1:proxy:"+id,
			"url", "http://"+id+".example.test:8080", "country", "US", "ok_ratio_pct", "100",
			"last_status", "ok", "target_profiles", string(encoded))
	}
	seed("matching", store.TargetProfileResult{Fingerprint: profile.fingerprint(), State: "pass", CheckedAt: nowMs - 1_000})
	seed("old-fingerprint", store.TargetProfileResult{Fingerprint: strings.Repeat("0", 64), State: "pass", CheckedAt: nowMs - 1_000})
	seed("expired", store.TargetProfileResult{Fingerprint: profile.fingerprint(), State: "pass", CheckedAt: nowMs - int64(2*time.Hour/time.Millisecond)})
	seed("future", store.TargetProfileResult{Fingerprint: profile.fingerprint(), State: "pass", CheckedAt: nowMs + 60_000})
	runner := &Runner{
		store: redisStore, metrics: newMetrics(),
		config: Config{TargetProfiles: []TargetProfile{profile}, ClassificationMaxAge: time.Hour},
	}
	mux := http.NewServeMux()
	const token = "0123456789abcdef-target-profile"
	registerInternalAPI(mux, runner, token)

	request := httptest.NewRequest(http.MethodGet, "/internal/api/v1/proxies?target_profile=residential", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	var payload struct {
		Count   int                `json:"count"`
		Proxies []store.QueryProxy `json:"proxies"`
	}
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &payload) != nil {
		t.Fatalf("target profile API: status=%d body=%s", response.Code, response.Body.String())
	}
	if payload.Count != 1 || len(payload.Proxies) != 1 || !strings.Contains(payload.Proxies[0].URL, "matching.example.test") {
		t.Fatalf("target profile result=%+v", payload)
	}
	matching := payload.Proxies[0].TargetProfiles[profile.Name]
	if matching.State != "pass" || matching.Fingerprint != "" || matching.CheckedAt <= 0 {
		t.Fatalf("matching profile response=%+v; expected fresh pass without exposing fingerprint", matching)
	}

	unknown := httptest.NewRequest(http.MethodGet, "/internal/api/v1/proxies?target_profile=unknown", nil)
	unknown.Header.Set("Authorization", "Bearer "+token)
	badResponse := httptest.NewRecorder()
	mux.ServeHTTP(badResponse, unknown)
	if badResponse.Code != http.StatusBadRequest || !strings.Contains(badResponse.Body.String(), "not configured") {
		t.Fatalf("unknown target profile: status=%d body=%s", badResponse.Code, badResponse.Body.String())
	}
}
