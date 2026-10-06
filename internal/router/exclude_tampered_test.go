package router

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

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
