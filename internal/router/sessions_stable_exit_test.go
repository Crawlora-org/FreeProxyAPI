package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func stableSessionProxy(host, exitIP string, stable *bool, samples *int64, checkedAt int64) store.QueryProxy {
	return store.QueryProxy{
		URL: "http://" + host + ":8080", Country: "US", LastStatus: "ok",
		ExitIP: exitIP, StableExit: stable, ExitIPSamples: samples, ExitCheckedAt: checkedAt,
	}
}

func TestSessionStableExitSelectsMeasuredCandidatesWithoutFilteringNormalPool(t *testing.T) {
	now := time.Now()
	checkedAt := now.Add(-time.Hour).UnixMilli()
	stable, unstable := true, false
	twoSamples, oneSample := int64(2), int64(1)
	proxies := []store.QueryProxy{
		stableSessionProxy("qualified-a.example", "8.8.8.8", &stable, &twoSamples, checkedAt),
		stableSessionProxy("qualified-b.example", "1.1.1.1", &stable, &twoSamples, checkedAt),
		stableSessionProxy("unstable.example", "9.9.9.9", &unstable, &twoSamples, checkedAt),
		stableSessionProxy("unknown.example", "4.2.2.2", nil, &twoSamples, checkedAt),
		stableSessionProxy("few-samples.example", "4.2.2.3", &stable, &oneSample, checkedAt),
		stableSessionProxy("stale.example", "4.2.2.4", &stable, &twoSamples, now.Add(-sessionStableExitMaxAge-time.Hour).UnixMilli()),
		stableSessionProxy("future.example", "4.2.2.5", &stable, &twoSamples, now.Add(time.Minute).UnixMilli()),
		stableSessionProxy("invalid-ip.example", "not-an-ip", &stable, &twoSamples, checkedAt),
	}
	queryHadGlobalStableFilter := false
	reloads := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			_, queryHadGlobalStableFilter = r.URL.Query()["stable_exit"]
			_ = json.NewEncoder(w).Encode(map[string]any{"count": len(proxies), "proxies": proxies})
		case "/config/reload":
			reloads++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	output := filepath.Join(t.TempDir(), "gost.json")
	opts := SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true, OutputFile: output,
		GOSTAPIURL: server.URL, GOSTAPIAddress: ":18080",
		GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000,
		SessionStableExit: true,
	}
	state := newSyncer()
	state.now = func() time.Time { return now }
	if err := state.sync(context.Background(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	if queryHadGlobalStableFilter {
		t.Fatal("session stable-exit option narrowed the global API pool")
	}
	config := readSessionConfig(t, output)
	var allChain *gostChain
	for i := range config.Chains {
		if config.Chains[i].Name == "all-validated" {
			allChain = &config.Chains[i]
			break
		}
	}
	if allChain == nil || len(allChain.Hops) != 1 || len(allChain.Hops[0].Nodes) != len(proxies) {
		t.Fatalf("ordinary pool was narrowed by session stability: %+v", allChain)
	}
	chain := sessionChain(t, config, "alpha")
	if len(chain.Hops) != 1 || len(chain.Hops[0].Nodes) != 1 {
		t.Fatalf("stable session chain=%+v, want exactly one node", chain)
	}
	selected := chain.Hops[0].Nodes[0].Addr
	if selected != "qualified-a.example:8080" && selected != "qualified-b.example:8080" {
		t.Fatalf("session selected ineligible node %q", selected)
	}
	if reloads != 1 {
		t.Fatalf("GOST reloads=%d, want 1", reloads)
	}
}

func TestSessionStableExitRejectsMissingStaleFutureAndUnusableEvidence(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	trueValue := true
	twoSamples := int64(2)
	good := stableSessionProxy("good.example", "8.8.8.8", &trueValue, &twoSamples, now.Add(-time.Hour).UnixMilli())
	falseValue := false
	oneSample := int64(1)
	for _, tc := range []struct {
		name  string
		proxy store.QueryProxy
		want  bool
	}{
		{name: "valid measured exit", proxy: good, want: true},
		{name: "missing stable verdict", proxy: stableSessionProxy("missing-stable.example", "8.8.8.8", nil, &twoSamples, good.ExitCheckedAt)},
		{name: "explicitly unstable", proxy: stableSessionProxy("unstable.example", "8.8.8.8", &falseValue, &twoSamples, good.ExitCheckedAt)},
		{name: "empty exit IP", proxy: stableSessionProxy("empty-ip.example", "", &trueValue, &twoSamples, good.ExitCheckedAt)},
		{name: "invalid exit IP", proxy: stableSessionProxy("invalid-ip.example", "bad-ip", &trueValue, &twoSamples, good.ExitCheckedAt)},
		{name: "private exit IP", proxy: stableSessionProxy("private-ip.example", "10.0.0.1", &trueValue, &twoSamples, good.ExitCheckedAt)},
		{name: "missing sample count", proxy: stableSessionProxy("missing-samples.example", "8.8.8.8", &trueValue, nil, good.ExitCheckedAt)},
		{name: "one exit sample", proxy: stableSessionProxy("one-sample.example", "8.8.8.8", &trueValue, &oneSample, good.ExitCheckedAt)},
		{name: "missing timestamp", proxy: stableSessionProxy("missing-time.example", "8.8.8.8", &trueValue, &twoSamples, 0)},
		{name: "stale timestamp", proxy: stableSessionProxy("stale.example", "8.8.8.8", &trueValue, &twoSamples, now.Add(-sessionStableExitMaxAge-time.Millisecond).UnixMilli())},
		{name: "future timestamp", proxy: stableSessionProxy("future.example", "8.8.8.8", &trueValue, &twoSamples, now.Add(time.Second).UnixMilli())},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := sessionHasStableExit(tc.proxy, now); got != tc.want {
				t.Fatalf("sessionHasStableExit=%t, want %t", got, tc.want)
			}
		})
	}
}

func TestIneligibleStableSessionPinStaysWithheldUntilTTL(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	stable, unstable := true, false
	twoSamples := int64(2)
	oldPin := stableSessionProxy("pinned.example", "8.8.8.8", &unstable, &twoSamples, now.UnixMilli())
	eligible := stableSessionProxy("alternate.example", "1.1.1.1", &stable, &twoSamples, now.UnixMilli())
	state := &syncer{sessionPins: map[string]sessionPin{
		"alpha": {URL: oldPin.URL, ExitIP: oldPin.ExitIP, ExpiresAt: now.Add(time.Minute)},
	}}
	opts := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000, SessionStableExit: true}
	if routes := reconcileSessionListeners(state, opts, []store.QueryProxy{oldPin, eligible}, now); len(routes) != 0 {
		t.Fatalf("stable session failed over before expiry: %+v", routes)
	}
	if pin := state.sessionPins["alpha"]; pin.URL != oldPin.URL || !pin.ExpiresAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("ineligible pin was not held through TTL: %+v", pin)
	}
	now = now.Add(time.Minute + time.Millisecond)
	routes := reconcileSessionListeners(state, opts, []store.QueryProxy{oldPin, eligible}, now)
	if len(routes) != 1 || routes[0].Proxy.URL != eligible.URL {
		t.Fatalf("expired pin did not reassign from stable candidates: %+v", routes)
	}
}

func TestNoEligibleStableSessionHasNoRouteOrPin(t *testing.T) {
	now := time.Unix(1_900_000_000, 0)
	state := &syncer{}
	opts := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: time.Minute, SessionBasePort: 10000, SessionStableExit: true}
	unknown := stableSessionProxy("unknown.example", "8.8.8.8", nil, nil, 0)
	if routes := reconcileSessionListeners(state, opts, []store.QueryProxy{unknown}, now); len(routes) != 0 {
		t.Fatalf("session routed without stable exit evidence: %+v", routes)
	}
	if len(state.sessionPins) != 0 {
		t.Fatalf("created pin without an eligible candidate: %+v", state.sessionPins)
	}
}

func TestSessionStableExitDoesNotChangeNormalPoolWhenDisabled(t *testing.T) {
	now := time.Now()
	proxy := stableSessionProxy("current.example", "8.8.8.8", nil, nil, 0)
	state := &syncer{}
	opts := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: time.Minute, SessionBasePort: 10000}
	routes := reconcileSessionListeners(state, opts, []store.QueryProxy{proxy}, now)
	if len(routes) != 1 || routes[0].Proxy.URL != proxy.URL {
		t.Fatalf("disabled gate changed default candidate eligibility: %+v", routes)
	}
}
