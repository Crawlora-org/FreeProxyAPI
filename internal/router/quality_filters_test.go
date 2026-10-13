package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestSyncOnceSendsQualityFilterQueryParameters(t *testing.T) {
	want := map[string]string{
		"exit_asn": "AS64500", "exclude_hosting": "true", "stable_exit": "true",
		"min_success_streak": "4", "min_reliability_7d_pct": "95",
		"min_reliability_7d_samples": "20", "target_profile": "residential",
	}
	got := make(map[string]string)
	proxy := completeQualityProxy(time.Now().Add(-time.Second).UnixMilli())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			for name := range want {
				got[name] = r.URL.Query().Get(name)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{proxy}})
		case "/config/reload":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	opts := SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true, OutputFile: filepath.Join(t.TempDir(), "gost.json"),
		GOSTAPIURL: server.URL, GOSTAPIAddress: ":18080", GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", ExitASN: want["exit_asn"],
		ExcludeHosting: true, StableExit: true, MinSuccessStreak: 4, MinReliability7dPct: 95,
		MinReliability7dSamples: 20, TargetProfile: want["target_profile"],
	}
	if err := SyncOnce(context.Background(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	for name, expected := range want {
		if got[name] != expected {
			t.Errorf("query %s=%q, want %q", name, got[name], expected)
		}
	}
}

func TestIgnoredQualityFiltersWithdrawPreviousPoolWhenEvidenceIsMissing(t *testing.T) {
	queryCount, reloadCount := 0, 0
	nowMs := time.Now().UnixMilli()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			queryCount++
			proxies := []store.QueryProxy{completeQualityProxy(nowMs - 1000)}
			if queryCount > 1 {
				// A compatible older API ignores quality query parameters and
				// returns a proxy without the evidence needed to satisfy them.
				proxies = []store.QueryProxy{{URL: "http://192.0.2.11:8080"}}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"count": len(proxies), "proxies": proxies})
		case "/config/reload":
			reloadCount++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	output := filepath.Join(t.TempDir(), "gost.json")
	opts := SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true, OutputFile: output,
		GOSTAPIURL: server.URL, GOSTAPIAddress: ":18080", GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", ExitASN: "AS64500", ExcludeHosting: true,
		StableExit: true, MinSuccessStreak: 4, MinReliability7dPct: 95,
		MinReliability7dSamples: 12, TargetProfile: "residential",
	}
	state := newSyncer()
	if err := state.sync(t.Context(), server.Client(), opts); err != nil {
		t.Fatalf("initial matching quality response: %v", err)
	}
	if err := state.sync(t.Context(), server.Client(), opts); err == nil {
		t.Fatal("response missing quality evidence unexpectedly kept the pool")
	}
	data, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Services) != 0 || len(config.Chains) != 0 || reloadCount != 2 {
		t.Fatalf("unsafe quality pool retained: services=%d chains=%d reloads=%d", len(config.Services), len(config.Chains), reloadCount)
	}
}

func TestStableExitAndTargetProfileWithdrawOnAPIError(t *testing.T) {
	for _, filter := range []string{"stable-exit", "target-profile"} {
		t.Run(filter, func(t *testing.T) {
			queryCount, reloadCount := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/proxies":
					queryCount++
					if queryCount == 1 {
						_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{{URL: "http://192.0.2.10:8080"}}})
						return
					}
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
				case "/config/reload":
					reloadCount++
					w.WriteHeader(http.StatusNoContent)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			output := filepath.Join(t.TempDir(), "gost.json")
			opts := SyncOptions{
				APIURL: server.URL + "/proxies", PublicAPI: true, OutputFile: output,
				GOSTAPIURL: server.URL, GOSTAPIAddress: ":18080", GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
				ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
			}
			state := newSyncer()
			if err := state.sync(t.Context(), server.Client(), opts); err != nil {
				t.Fatal(err)
			}
			if filter == "stable-exit" {
				opts.StableExit = true
			} else {
				opts.TargetProfile = "residential"
			}
			if err := state.sync(t.Context(), server.Client(), opts); err == nil {
				t.Fatal("API error was not returned")
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			var config gostConfig
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			if len(config.Services) != 0 || len(config.Chains) != 0 || reloadCount != 2 {
				t.Fatalf("%s API failure kept pool: services=%d chains=%d reloads=%d", filter, len(config.Services), len(config.Chains), reloadCount)
			}
		})
	}
}

func completeQualityProxy(nowMs int64) store.QueryProxy {
	hosting, stable, streak, reliability := false, true, int64(6), int64(97)
	return store.QueryProxy{
		URL: "http://192.0.2.10:8080", ExitASN: "64500", Hosting: &hosting,
		StableExit: &stable, ExitCheckedAt: nowMs,
		SuccessStreak: &streak, Reliability7dPct: &reliability, Reliability7dSamples: 30,
		TargetProfiles: map[string]store.TargetProfileResult{
			"residential": {State: "pass", CheckedAt: nowMs},
		},
	}
}

func TestFilterQualityResultsEnforcesEvidenceAndUnknownHostingSemantics(t *testing.T) {
	nowMs := time.Now().UnixMilli()
	pass := completeQualityProxy(nowMs)
	unknownHosting := pass
	unknownHosting.Hosting = nil
	hosted := pass
	hostedValue := true
	hosted.Hosting = &hostedValue
	noStableEvidence := pass
	noStableEvidence.ExitCheckedAt = 0
	noStableEvidence.StableExit = nil
	unstable := pass
	stableFalse := false
	unstable.StableExit = &stableFalse
	belowStreak := pass
	shortStreak := int64(2)
	belowStreak.SuccessStreak = &shortStreak
	noStreak := pass
	noStreak.SuccessStreak = nil
	noReliability := pass
	noReliability.Reliability7dPct = nil
	belowReliability := pass
	lowReliability := int64(80)
	belowReliability.Reliability7dPct = &lowReliability
	invalidReliability := pass
	tooHighReliability := int64(101)
	invalidReliability.Reliability7dPct = &tooHighReliability
	belowSampleCount := pass
	belowSampleCount.Reliability7dSamples = 5
	noProfile := pass
	noProfile.TargetProfiles = nil
	failingProfile := pass
	profile := failingProfile.TargetProfiles["residential"]
	profile.State = "fail"
	failingProfile.TargetProfiles = map[string]store.TargetProfileResult{"residential": profile}
	futureProfile := pass
	profile = futureProfile.TargetProfiles["residential"]
	profile.CheckedAt = nowMs + 1
	futureProfile.TargetProfiles = map[string]store.TargetProfileResult{"residential": profile}
	for _, tc := range []struct {
		name   string
		filter SyncOptions
		proxy  store.QueryProxy
		want   bool
	}{
		{name: "canonical exit ASN", filter: SyncOptions{ExitASN: "AS64500"}, proxy: pass, want: true},
		{name: "exit ASN mismatch", filter: SyncOptions{ExitASN: "AS64501"}, proxy: pass, want: false},
		{name: "unknown hosting remains eligible", filter: SyncOptions{ExcludeHosting: true}, proxy: unknownHosting, want: true},
		{name: "known hosting excluded", filter: SyncOptions{ExcludeHosting: true}, proxy: hosted, want: false},
		{name: "stable exit needs timestamp", filter: SyncOptions{StableExit: true}, proxy: noStableEvidence, want: false},
		{name: "unstable exit rejected", filter: SyncOptions{StableExit: true}, proxy: unstable, want: false},
		{name: "success streak needs evidence", filter: SyncOptions{MinSuccessStreak: 4}, proxy: noStreak, want: false},
		{name: "short success streak rejected", filter: SyncOptions{MinSuccessStreak: 4}, proxy: belowStreak, want: false},
		{name: "reliability needs evidence", filter: SyncOptions{MinReliability7dPct: 95}, proxy: noReliability, want: false},
		{name: "below reliability rejected", filter: SyncOptions{MinReliability7dPct: 95}, proxy: belowReliability, want: false},
		{name: "impossible reliability rejected", filter: SyncOptions{MinReliability7dPct: 95}, proxy: invalidReliability, want: false},
		{name: "sample count enforced", filter: SyncOptions{MinReliability7dSamples: 10}, proxy: belowSampleCount, want: false},
		{name: "target profile needs pass evidence", filter: SyncOptions{TargetProfile: "residential"}, proxy: noProfile, want: false},
		{name: "target profile fail rejected", filter: SyncOptions{TargetProfile: "residential"}, proxy: failingProfile, want: false},
		{name: "future target profile rejected", filter: SyncOptions{TargetProfile: "residential"}, proxy: futureProfile, want: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := filterQualityResults([]store.QueryProxy{tc.proxy}, tc.filter, nowMs)
			if (len(got) == 1) != tc.want {
				t.Fatalf("matched=%d, want %v", len(got), tc.want)
			}
		})
	}
}
