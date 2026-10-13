package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func sessionTestProxy(host, exitIP, status string) store.QueryProxy {
	return store.QueryProxy{
		URL: "http://" + host + ":8080", Country: "US", ExitCountry: "US",
		ExitIP: exitIP, LastStatus: status,
	}
}

func TestNamedSessionPortMapIsStableAndValidationIsBounded(t *testing.T) {
	ports := SessionPortMap([]string{"zeta", "alpha", "beta"}, 10000)
	want := map[string]int{"alpha": 10000, "beta": 10001, "zeta": 10002}
	if !reflect.DeepEqual(ports, want) {
		t.Fatalf("session ports=%v, want %v", ports, want)
	}
	parsed, err := ParseSessionIDs(" zeta,alpha ")
	if err != nil || !reflect.DeepEqual(parsed, []string{"zeta", "alpha"}) {
		t.Fatalf("parsed IDs=%v err=%v", parsed, err)
	}
	for _, raw := range []string{"bad.id", "-leading", "会话", "", "one,,two", strings.Repeat("a", 33)} {
		if raw == "" { // An empty option disables named sessions.
			if ids, err := ParseSessionIDs(raw); err != nil || len(ids) != 0 {
				t.Fatalf("empty ID list=%v err=%v", ids, err)
			}
			continue
		}
		if _, err := ParseSessionIDs(raw); err == nil {
			t.Errorf("ParseSessionIDs(%q) accepted invalid ID input", raw)
		}
	}
	for _, tc := range []struct {
		name string
		ids  []string
		ttl  time.Duration
		port int
	}{
		{name: "duplicate", ids: []string{"same", "same"}, ttl: time.Minute, port: 10000},
		{name: "too many", ids: makeSessionIDs(maxNamedSessions + 1), ttl: time.Minute, port: 10000},
		{name: "zero TTL", ids: []string{"a"}, ttl: 0, port: 10000},
		{name: "port overflow", ids: []string{"a", "b"}, ttl: time.Minute, port: 65535},
		{name: "listener collision", ids: []string{"a"}, ttl: time.Minute, port: 3129},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := ValidateSessionOptions(tc.ids, tc.ttl, tc.port, []string{"US"}); err == nil {
				t.Fatal("invalid session options accepted")
			}
		})
	}
}

func makeSessionIDs(count int) []string {
	ids := make([]string, count)
	for i := range ids {
		ids[i] = "s" + strconv.Itoa(i)
	}
	return ids
}

func TestSessionPortValidationUsesListenerIntervalsAndAPIAddress(t *testing.T) {
	ttl := time.Minute
	if err := ValidateSessionOptions([]string{"alpha"}, ttl, 3129, nil); err == nil {
		t.Fatal("session port overlapped BuildConfig's default US listener")
	}
	if err := ValidateSessionOptions([]string{"alpha", "beta"}, ttl, 3000, nil); err != nil {
		t.Fatalf("non-overlapping low session ports rejected: %v", err)
	}
	if err := ValidateSessionOptions([]string{"alpha"}, ttl, 18080, nil, "127.0.0.1:18080"); err == nil {
		t.Fatal("session port collided with GOST API listener")
	}
	if err := ValidateSessionOptions([]string{"alpha", "beta"}, ttl, 18079, nil, ":18080"); err == nil {
		t.Fatal("session port range crossed GOST API listener")
	}
	if err := ValidateSessionOptions([]string{"alpha"}, ttl, 65535, nil); err != nil {
		t.Fatalf("single session at port 65535 rejected: %v", err)
	}
	if err := ValidateSessionOptions([]string{"alpha", "beta"}, ttl, 65535, nil); err == nil {
		t.Fatal("session port range beyond 65535 accepted")
	}
}

func TestSessionPinsSurviveRefreshReorderingAndChangeAfterTTL(t *testing.T) {
	poolA := sessionTestProxy("a.example", "198.51.100.10", "ok")
	poolB := sessionTestProxy("b.example", "198.51.100.11", "ok")
	responses := [][]store.QueryProxy{{poolA, poolB}, {poolB, poolA}, {poolB}}
	queryCount, reloadCount := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			proxies := responses[queryCount]
			queryCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"proxies": proxies})
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
		GOSTAPIURL: server.URL, GOSTAPIAddress: ":18080",
		GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000,
	}
	now := time.UnixMilli(1_900_000_000_000)
	state := newSyncer()
	state.now = func() time.Time { return now }
	client := server.Client()
	if err := state.sync(t.Context(), client, opts); err != nil {
		t.Fatal(err)
	}
	pinned := state.sessionPins["alpha"]
	if pinned.URL == "" || pinned.ExitIP == "" || !pinned.ExpiresAt.Equal(now.Add(opts.SessionTTL)) {
		t.Fatalf("initial pin=%+v", pinned)
	}
	if err := state.sync(t.Context(), client, opts); err != nil {
		t.Fatal(err)
	}
	if got := state.sessionPins["alpha"]; got != pinned {
		t.Fatalf("reordered pool changed pin: before=%+v after=%+v", pinned, got)
	}
	config := readSessionConfig(t, output)
	if got := sessionService(t, config, "alpha").Addr; got != ":10000" {
		t.Fatalf("session listener address=%q, want :10000", got)
	}
	now = now.Add(opts.SessionTTL + time.Millisecond)
	if err := state.sync(t.Context(), client, opts); err != nil {
		t.Fatal(err)
	}
	if got := state.sessionPins["alpha"]; got.URL != poolB.URL || !got.ExpiresAt.Equal(now.Add(opts.SessionTTL)) {
		t.Fatalf("expired pin was not reassigned: %+v", got)
	}
	if reloadCount != 2 {
		t.Fatalf("reloads=%d, want 2", reloadCount)
	}
}

func TestSessionPinsFailClosedForMissingFailedOrChangedExit(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	base := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000, CountryField: "entry"}
	for _, tc := range []struct {
		name string
		pool []store.QueryProxy
	}{
		{name: "missing", pool: []store.QueryProxy{sessionTestProxy("other.example", "198.51.100.20", "ok")}},
		{name: "failed", pool: []store.QueryProxy{sessionTestProxy("pin.example", "198.51.100.10", "failed"), sessionTestProxy("other.example", "198.51.100.20", "ok")}},
		{name: "changed exit", pool: []store.QueryProxy{sessionTestProxy("pin.example", "198.51.100.99", "ok"), sessionTestProxy("other.example", "198.51.100.20", "ok")}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			state := &syncer{sessionPins: map[string]sessionPin{
				"alpha": {URL: "http://pin.example:8080", ExitIP: "198.51.100.10", ExpiresAt: now.Add(time.Minute)},
			}}
			if routes := reconcileSessionListeners(state, base, tc.pool, now); len(routes) != 0 {
				t.Fatalf("invalid pin got session routes: %+v", routes)
			}
			if got := state.sessionPins["alpha"]; got.URL != "http://pin.example:8080" || !got.ExpiresAt.Equal(now.Add(time.Minute)) {
				t.Fatalf("failed pin did not remain held through TTL: %+v", got)
			}
		})
	}
}

func TestSessionLearnsExitIPWhenItFirstBecomesObservable(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	state := &syncer{sessionPins: map[string]sessionPin{
		"alpha": {URL: "http://pin.example:8080", ExpiresAt: now.Add(time.Minute)},
	}}
	opts := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000}
	proxy := sessionTestProxy("pin.example", "198.51.100.10", "ok")
	routes := reconcileSessionListeners(state, opts, []store.QueryProxy{proxy}, now)
	if len(routes) != 1 || state.sessionPins["alpha"].ExitIP != proxy.ExitIP {
		t.Fatalf("first observed exit did not update pin: routes=%+v pin=%+v", routes, state.sessionPins["alpha"])
	}
}

func TestSessionFailoverIsOptIn(t *testing.T) {
	now := time.UnixMilli(1_900_000_000_000)
	state := &syncer{sessionPins: map[string]sessionPin{
		"alpha": {URL: "http://pin.example:8080", ExitIP: "198.51.100.10", ExpiresAt: now.Add(time.Minute)},
	}}
	opts := SyncOptions{Sessions: []string{"alpha"}, SessionTTL: 10 * time.Minute, SessionBasePort: 10000}
	pool := []store.QueryProxy{sessionTestProxy("other.example", "198.51.100.20", "ok")}
	if routes := reconcileSessionListeners(state, opts, pool, now); len(routes) != 0 {
		t.Fatalf("default mode failed over: %+v", routes)
	}
	opts.SessionFailover = true
	routes := reconcileSessionListeners(state, opts, pool, now)
	if len(routes) != 1 || routes[0].Proxy.URL != pool[0].URL || state.sessionPins["alpha"].URL != pool[0].URL || !state.sessionPins["alpha"].ExpiresAt.Equal(now.Add(opts.SessionTTL)) {
		t.Fatalf("opt-in failover route=%+v pin=%+v", routes, state.sessionPins["alpha"])
	}
}

func TestBuildConfigAddsAuthenticatedOneProxySessionChain(t *testing.T) {
	proxy := sessionTestProxy("pin.example", "198.51.100.10", "ok")
	data, err := BuildConfig([]store.QueryProxy{proxy}, BuildOptions{
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", APIUsername: "api-user", APIPassword: "api-pass",
		SessionListeners: []SessionListener{{ID: "alpha", Port: 10000, Proxy: proxy}},
	})
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	service := sessionService(t, config, "alpha")
	if service.Addr != ":10000" || service.Handler.Auth.Username != "proxy-user" || service.Handler.Auth.Password != "proxy-pass" || service.Handler.Chain != "session-alpha" {
		t.Fatalf("session service=%+v", service)
	}
	chain := sessionChain(t, config, "alpha")
	if len(chain.Hops) != 1 || len(chain.Hops[0].Nodes) != 1 || chain.Hops[0].Nodes[0].Addr != "pin.example:8080" {
		t.Fatalf("session chain=%+v", chain)
	}
	if err := validateSessionListeners([]SessionListener{{ID: "../bad", Port: 10000, Proxy: proxy}}, nil); err == nil {
		t.Fatal("invalid session listener ID accepted")
	}
	if _, err := BuildConfig([]store.QueryProxy{proxy}, BuildOptions{
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", APIUsername: "api-user", APIPassword: "api-pass",
		APIAddress: "127.0.0.1:10000", SessionListeners: []SessionListener{{ID: "alpha", Port: 10000, Proxy: proxy}},
	}); err == nil || !strings.Contains(err.Error(), "collides") {
		t.Fatalf("BuildConfig did not catch GOST API port collision: %v", err)
	}
}

func TestQueryFailureDropsOnlySessionListeners(t *testing.T) {
	queryCount, reloadCount := 0, 0
	proxy := sessionTestProxy("pin.example", "198.51.100.10", "ok")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			queryCount++
			if queryCount == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"proxies": []store.QueryProxy{proxy}})
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
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", Sessions: []string{"alpha"},
	}
	state := newSyncer()
	if err := state.sync(t.Context(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	if err := state.sync(t.Context(), server.Client(), opts); err == nil {
		t.Fatal("query failure was not returned")
	}
	config := readSessionConfig(t, output)
	if len(config.Services) != 2 || len(config.Chains) != 2 {
		t.Fatalf("query failure did not remove only session listeners: services=%+v chains=%+v", config.Services, config.Chains)
	}
	for _, service := range config.Services {
		if strings.HasPrefix(service.Name, "session-") {
			t.Fatalf("session listener remains after query failure: %+v", service)
		}
	}
	if reloadCount != 2 {
		t.Fatalf("reloads=%d, want 2", reloadCount)
	}
}

func readSessionConfig(t *testing.T, path string) gostConfig {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func sessionService(t *testing.T, config gostConfig, id string) gostService {
	t.Helper()
	for _, service := range config.Services {
		if service.Name == "session-"+id {
			return service
		}
	}
	t.Fatalf("session service %q not found", id)
	return gostService{}
}

func sessionChain(t *testing.T, config gostConfig, id string) gostChain {
	t.Helper()
	for _, chain := range config.Chains {
		if chain.Name == "session-"+id {
			return chain
		}
	}
	t.Fatalf("session chain %q not found", id)
	return gostChain{}
}
