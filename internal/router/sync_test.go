package router

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestSyncOnceWritesConfigAndRetriesReload(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("0123456789abcdef-token"), 0600); err != nil {
		t.Fatal(err)
	}
	var reloads int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/internal/api/v1/proxies":
			if got := r.Header.Get("Authorization"); got != "Bearer 0123456789abcdef-token" {
				t.Errorf("bearer token = %q", got)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"count":   1,
				"proxies": []store.QueryProxy{{URL: "socks5://192.0.2.10:1080", Country: "US"}},
			})
		case "/config/reload":
			user, pass, ok := r.BasicAuth()
			if !ok || user != "api-user" || pass != "api-pass" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			reloads++
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	outputFile := filepath.Join(t.TempDir(), "runtime", "gost.json")
	opts := SyncOptions{
		APIURL:          server.URL + "/internal/api/v1/proxies",
		TokenFile:       tokenFile,
		OutputFile:      outputFile,
		GOSTAPIURL:      server.URL,
		GOSTAPIAddress:  ":18080",
		GOSTAPIUsername: "api-user",
		GOSTAPIPassword: "api-pass",
		ProxyUsername:   "proxy-user",
		ProxyPassword:   "proxy-pass",
		MinimumRatioPct: 80,
	}
	client := server.Client()
	if err := SyncOnce(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	if err := SyncOnce(context.Background(), client, opts); err != nil {
		t.Fatal(err)
	}
	if reloads != 2 {
		t.Fatalf("reload count = %d, want 2", reloads)
	}
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("GOST config is empty")
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.API.Addr != ":18080" {
		t.Fatalf("GOST API address = %q, want :18080", config.API.Addr)
	}
}

func TestSyncOncePublicAPIUsesValidatedFiltersWithoutToken(t *testing.T) {
	var sawQuery bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			if got := r.Header.Get("Authorization"); got != "" {
				t.Errorf("public API authorization = %q, want empty", got)
			}
			query := r.URL.Query()
			for key, want := range map[string]string{
				"country": "US", "exit_country": "CA", "asn": "AS64500",
				"anonymity": "elite", "geo_mismatch": "true", "max_latency_ms": "500",
				"min_ratio_pct": "90", "limit": "25",
			} {
				if got := query.Get(key); got != want {
					t.Errorf("query %s = %q, want %q", key, got, want)
				}
			}
			sawQuery = true
			_ = json.NewEncoder(w).Encode(map[string]any{
				"count":   1,
				"proxies": []store.QueryProxy{{URL: "socks5://192.0.2.10:1080", Country: "US", ExitCountry: "CA"}},
			})
		case "/config/reload":
			user, pass, ok := r.BasicAuth()
			if !ok || user != "api-user" || pass != "api-pass" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	opts := SyncOptions{
		APIURL:           server.URL + "/proxies",
		PublicAPI:        true,
		OutputFile:       filepath.Join(t.TempDir(), "runtime", "gost.json"),
		GOSTAPIURL:       server.URL,
		GOSTAPIAddress:   ":18080",
		GOSTAPIUsername:  "api-user",
		GOSTAPIPassword:  "api-pass",
		ProxyUsername:    "proxy-user",
		ProxyPassword:    "proxy-pass",
		Country:          "US",
		ExitCountry:      "CA",
		ASN:              "AS64500",
		Anonymity:        "elite",
		GeoMismatch:      true,
		MaximumLatencyMs: 500,
		MinimumRatioPct:  90,
		Limit:            25,
	}
	if err := SyncOnce(context.Background(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	if !sawQuery {
		t.Fatal("public API was not queried")
	}
}

func TestSyncReloadsOnlyOnChangeOrAfterFailure(t *testing.T) {
	var (
		mu         sync.Mutex
		proxyURL   = "socks5://192.0.2.10:1080"
		reloads    int
		failReload = true
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/proxies":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"proxies": []store.QueryProxy{{URL: proxyURL, Country: "US"}},
			})
		case "/config/reload":
			reloads++
			if failReload {
				http.Error(w, "starting", http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	opts := SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true,
		OutputFile: filepath.Join(t.TempDir(), "gost.json"), GOSTAPIURL: server.URL,
		GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
	}
	state := newSyncer()
	ctx := context.Background()
	step := func(wantErr bool, wantReloads int) {
		t.Helper()
		err := state.sync(ctx, server.Client(), opts)
		if (err != nil) != wantErr {
			t.Fatalf("sync error = %v, wantErr %t", err, wantErr)
		}
		mu.Lock()
		defer mu.Unlock()
		if reloads != wantReloads {
			t.Fatalf("reloads = %d, want %d", reloads, wantReloads)
		}
	}
	step(true, 1) // first write; GOST not ready
	mu.Lock()
	failReload = false
	mu.Unlock()
	step(false, 2) // unchanged config, but previous reload failed
	step(false, 2) // unchanged and reloaded: no reload
	mu.Lock()
	proxyURL = "socks5://192.0.2.11:1080"
	mu.Unlock()
	step(false, 3) // changed config
	step(false, 3)
}

func TestSyncRejectsTruncatedAPIResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"proxies":[`))
		_, _ = w.Write(bytes.Repeat([]byte(" "), maxAPIResponseBytes))
		_, _ = w.Write([]byte(`]}`))
	}))
	defer server.Close()
	err := SyncOnce(context.Background(), server.Client(), SyncOptions{
		APIURL: server.URL, PublicAPI: true, OutputFile: filepath.Join(t.TempDir(), "gost.json"),
		GOSTAPIUsername: "u", GOSTAPIPassword: "p", ProxyUsername: "u", ProxyPassword: "p",
	})
	if err == nil || !strings.Contains(err.Error(), "byte limit") {
		t.Fatalf("error = %v, want explicit size-limit error", err)
	}
}

func TestRunWritesBootstrapConfigWhileAPIUnavailable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Stop Run after its first refresh attempt.
		cancel()
		http.Error(w, "query failed", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	outputFile := filepath.Join(t.TempDir(), "runtime", "gost.json")
	err := Run(ctx, SyncOptions{
		APIURL: server.URL, PublicAPI: true, OutputFile: outputFile,
		GOSTAPIURL: server.URL, GOSTAPIAddress: "127.0.0.1:18080",
		GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		RefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("bootstrap config not written: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := raw["services"]; ok {
		t.Fatalf("bootstrap config must not open proxy listeners: %s", data)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if config.API.Addr != "127.0.0.1:18080" || config.API.Auth.Username != "api-user" || config.API.Auth.Password != "api-pass" {
		t.Fatalf("bootstrap API = %+v", config.API)
	}
	if info, err := os.Stat(outputFile); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("bootstrap config mode = %v, %v; want 0600", info.Mode().Perm(), err)
	}
}

func TestBootstrapConfigKeepsExistingConfig(t *testing.T) {
	outputFile := filepath.Join(t.TempDir(), "gost.json")
	existing := []byte(`{"services":[{"name":"unified-http"}]}` + "\n")
	if err := os.WriteFile(outputFile, existing, 0600); err != nil {
		t.Fatal(err)
	}
	err := writeBootstrapConfig(SyncOptions{
		OutputFile: outputFile, GOSTAPIAddress: "127.0.0.1:18080",
		GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	if data, _ := os.ReadFile(outputFile); !bytes.Equal(data, existing) {
		t.Fatalf("existing config overwritten: %s", data)
	}
}

func TestBuildBootstrapConfigRequiresAPICredentials(t *testing.T) {
	if _, err := BuildBootstrapConfig(BuildOptions{APIUsername: "api-user"}); err == nil {
		t.Fatal("BuildBootstrapConfig accepted a missing API password")
	}
}
