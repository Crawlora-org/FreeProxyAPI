package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func strictProxy(rawURL string) store.QueryProxy {
	clean := false
	return store.QueryProxy{URL: rawURL, Tampered: &clean, TamperCheckedAt: time.Now().UnixMilli()}
}

func TestFilterStrictTamperResultsRejectsUnknownTamperedAndInvalidHTTPS(t *testing.T) {
	nowMs := int64(1_000_000)
	clean, tampered, httpsOK := false, true, true
	proxies := []store.QueryProxy{
		{URL: "http://clean.example:8080", Tampered: &clean, TamperCheckedAt: nowMs - 1},
		{URL: "http://unknown.example:8080"},
		{URL: "http://tampered.example:8080", Tampered: &tampered, TamperCheckedAt: nowMs - 1},
		{URL: "http://future.example:8080", Tampered: &clean, TamperCheckedAt: nowMs + 1},
		{URL: "http://https-unchecked.example:8080", Tampered: &clean, TamperCheckedAt: nowMs - 1},
		{URL: "http://https-pass.example:8080", Tampered: &clean, TamperCheckedAt: nowMs - 1, HTTPSOK: &httpsOK, HTTPSCheckedAt: nowMs - 1},
	}
	got := filterStrictTamperResults(proxies, false, nowMs)
	if len(got) != 3 || got[0].URL != proxies[0].URL || got[2].URL != proxies[5].URL {
		t.Fatalf("strict tamper results = %+v", got)
	}
	got = filterStrictTamperResults(proxies, true, nowMs)
	if len(got) != 1 || got[0].URL != proxies[5].URL {
		t.Fatalf("strict tamper + HTTPS results = %+v", got)
	}
}

func TestSyncOnceSendsExcludeTampered(t *testing.T) {
	for _, exclude := range []bool{true, false} {
		var got string
		var present bool
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/proxies":
				got = r.URL.Query().Get("exclude_tampered")
				_, present = r.URL.Query()["exclude_tampered"]
				_ = json.NewEncoder(w).Encode(map[string]any{
					"count":   1,
					"proxies": []store.QueryProxy{{URL: "http://192.0.2.10:8080", Country: "US"}},
				})
			case "/config/reload":
				w.WriteHeader(http.StatusNoContent)
			default:
				http.NotFound(w, r)
			}
		}))
		opts := SyncOptions{
			APIURL:          server.URL + "/proxies",
			PublicAPI:       true,
			OutputFile:      filepath.Join(t.TempDir(), "gost.json"),
			GOSTAPIURL:      server.URL,
			GOSTAPIAddress:  ":18080",
			GOSTAPIUsername: "api-user",
			GOSTAPIPassword: "api-pass",
			ProxyUsername:   "proxy-user",
			ProxyPassword:   "proxy-pass",
			ExcludeTampered: exclude,
		}
		err := SyncOnce(context.Background(), server.Client(), opts)
		server.Close()
		if err != nil {
			t.Fatal(err)
		}
		if exclude && got != "true" {
			t.Fatalf("exclude_tampered = %q, want true", got)
		}
		if !exclude && present {
			t.Fatalf("disabled exclude_tampered must not be sent, got %q", got)
		}
	}
}

func TestStrictTamperWithdrawsProxyListenersOnEmptyOrFailedQuery(t *testing.T) {
	for _, failure := range []string{"empty", "api-error", "ignored-filter"} {
		t.Run(failure, func(t *testing.T) {
			queryCount, reloadCount := 0, 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/proxies":
					queryCount++
					if queryCount == 1 || failure == "api-error" && queryCount == 3 {
						_ = json.NewEncoder(w).Encode(map[string]any{
							"count": 1, "proxies": []store.QueryProxy{strictProxy("http://192.0.2.10:8080")},
						})
						return
					}
					if failure == "api-error" {
						http.Error(w, "unavailable", http.StatusServiceUnavailable)
						return
					}
					if failure == "ignored-filter" {
						_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{{URL: "http://192.0.2.11:8080"}}})
						return
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "proxies": []store.QueryProxy{}})
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
				RequireTamperChecked: true,
			}
			state := newSyncer()
			if err := state.sync(context.Background(), server.Client(), opts); err != nil {
				t.Fatalf("initial pool sync: %v", err)
			}
			if err := state.sync(context.Background(), server.Client(), opts); err == nil {
				t.Fatal("empty or failed strict query should report its cause")
			}
			if failure == "api-error" {
				// Reinstall a valid pool, then lose the API. A failed query must
				// withdraw it just like an empty successful result.
				if err := state.sync(context.Background(), server.Client(), opts); err != nil {
					t.Fatalf("restore pool sync: %v", err)
				}
				if err := state.sync(context.Background(), server.Client(), opts); err == nil {
					t.Fatal("failed strict query should report its cause")
				}
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			var config map[string]json.RawMessage
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			if _, ok := config["services"]; ok {
				t.Fatalf("withdrawn config still has proxy services: %s", data)
			}
			if _, ok := config["chains"]; ok {
				t.Fatalf("withdrawn config still has proxy chains: %s", data)
			}
			wantReloads := 2
			if failure == "api-error" {
				wantReloads = 4
			}
			if reloadCount != wantReloads {
				t.Fatalf("GOST reloads=%d, want %d (%s)", reloadCount, wantReloads, fmt.Sprint(failure))
			}
		})
	}
}

func TestStrictTamperWithdrawalRetriesFailedGOSTReload(t *testing.T) {
	queryCount, reloadCount := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			queryCount++
			if queryCount == 1 {
				_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{strictProxy("http://192.0.2.10:8080")}})
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "proxies": []store.QueryProxy{}})
		case "/config/reload":
			reloadCount++
			if reloadCount == 2 {
				http.Error(w, "reload failed", http.StatusInternalServerError)
				return
			}
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
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", RequireTamperChecked: true,
	}
	state := newSyncer()
	if err := state.sync(context.Background(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	err := state.sync(context.Background(), server.Client(), opts)
	if err == nil || !strings.Contains(err.Error(), "reload GOST API-only config") || !state.reloadPending {
		t.Fatalf("withdrawal reload error=%v pending=%v, want retryable reload failure", err, state.reloadPending)
	}
	if err := state.sync(context.Background(), server.Client(), opts); err == nil || strings.Contains(err.Error(), "reload GOST API-only config") {
		t.Fatalf("subsequent empty result should retry and complete withdrawal: %v", err)
	}
	if state.reloadPending || reloadCount != 3 {
		t.Fatalf("reload pending=%v reload count=%d, want false/3", state.reloadPending, reloadCount)
	}
}

func TestStrictTamperWithdrawsOnTokenAndURLSetupFailures(t *testing.T) {
	for _, failure := range []string{"token", "url"} {
		t.Run(failure, func(t *testing.T) {
			reloads := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/proxies":
					_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{strictProxy("http://192.0.2.10:8080")}})
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
				ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass", RequireTamperChecked: true,
			}
			state := newSyncer()
			if err := state.sync(context.Background(), server.Client(), opts); err != nil {
				t.Fatal(err)
			}
			wantError := "parse API URL"
			if failure == "token" {
				opts.PublicAPI = false
				opts.TokenFile = filepath.Join(t.TempDir(), "missing-token")
				wantError = "read API token"
			} else {
				opts.APIURL = "http://%gh&"
			}
			err := state.sync(context.Background(), server.Client(), opts)
			if err == nil || !strings.Contains(err.Error(), wantError) {
				t.Fatalf("setup failure error=%v, want %q", err, wantError)
			}
			data, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			var config map[string]json.RawMessage
			if err := json.Unmarshal(data, &config); err != nil {
				t.Fatal(err)
			}
			if _, ok := config["services"]; ok || reloads != 2 {
				t.Fatalf("setup failure retained pool or missed reloads=%d: %s", reloads, data)
			}
		})
	}
}

func TestSyncOnceSendsStrictTamperAndHTTPSFilters(t *testing.T) {
	var requireTamperChecked, https string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/proxies":
			requireTamperChecked = r.URL.Query().Get("require_tamper_checked")
			https = r.URL.Query().Get("https")
			proxy := strictProxy("http://192.0.2.10:8080")
			httpsOK := true
			proxy.HTTPSOK = &httpsOK
			proxy.HTTPSCheckedAt = proxy.TamperCheckedAt
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 1, "proxies": []store.QueryProxy{proxy}})
		case "/config/reload":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	opts := SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true,
		OutputFile: filepath.Join(t.TempDir(), "gost.json"), GOSTAPIURL: server.URL,
		GOSTAPIAddress: ":18080", GOSTAPIUsername: "api-user", GOSTAPIPassword: "api-pass",
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		RequireTamperChecked: true, HTTPS: true,
	}
	if err := SyncOnce(context.Background(), server.Client(), opts); err != nil {
		t.Fatal(err)
	}
	if requireTamperChecked != "true" || https != "true" {
		t.Fatalf("query strict=%q https=%q, want true/true", requireTamperChecked, https)
	}
}
