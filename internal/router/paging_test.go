package router

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

// pagedAPI is a fake public /proxies endpoint over a fixed list. Like the real
// one it clamps limit to maxPage, skips offset matches, and reports has_more.
type pagedAPI struct {
	mu       sync.Mutex
	total    int
	maxPage  int
	requests []url.Values
	// overlap makes every page after the first start one proxy early, as
	// happens when the validated pool shifts between two requests.
	overlap bool
	// failPage, when positive, answers that 1-based page with HTTP 503.
	failPage int
	server   *httptest.Server
}

func newPagedAPI(t *testing.T, total, maxPage int) *pagedAPI {
	t.Helper()
	api := &pagedAPI{total: total, maxPage: maxPage}
	api.server = httptest.NewServer(http.HandlerFunc(api.serve))
	t.Cleanup(api.server.Close)
	return api
}

func (a *pagedAPI) serve(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	a.requests = append(a.requests, r.URL.Query())
	page := len(a.requests)
	a.mu.Unlock()
	if a.failPage > 0 && page == a.failPage {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > a.maxPage {
		limit = a.maxPage
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	if a.overlap && offset > 0 {
		offset--
	}
	end := offset + limit
	if end > a.total {
		end = a.total
	}
	proxies := []store.QueryProxy{}
	for i := offset; i < end; i++ {
		proxies = append(proxies, proxyN(i))
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"count": len(proxies), "proxies": proxies, "limit": limit, "offset": offset, "has_more": end < a.total,
	})
}

func proxyN(i int) store.QueryProxy {
	return store.QueryProxy{URL: fmt.Sprintf("http://203.0.113.%d:%d", i%250+1, 8000+i), Country: "US"}
}

func (a *pagedAPI) queries() []url.Values {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]url.Values(nil), a.requests...)
}

func fetchAll(t *testing.T, api *pagedAPI, opts SyncOptions) ([]store.QueryProxy, error) {
	t.Helper()
	opts.PublicAPI = true
	opts = opts.defaults()
	base, err := url.Parse(api.server.URL + "/proxies")
	if err != nil {
		t.Fatal(err)
	}
	return fetchProxies(context.Background(), api.server.Client(), opts, base, url.Values{"min_ratio_pct": {"80"}}, "")
}

func TestFetchProxiesFollowsHasMoreWhenServerCapsPageSize(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	got, err := fetchAll(t, api, SyncOptions{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 250 {
		t.Fatalf("got %d proxies, want all 250", len(got))
	}
	queries := api.queries()
	if len(queries) != 3 {
		t.Fatalf("made %d requests, want 3 pages", len(queries))
	}
	for i, wantOffset := range []string{"", "100", "200"} {
		if got := queries[i].Get("offset"); got != wantOffset {
			t.Errorf("page %d offset = %q, want %q", i+1, got, wantOffset)
		}
		if got := queries[i].Get("limit"); got != "1000" {
			t.Errorf("page %d limit = %q, want the same 1000 on every page", i+1, got)
		}
		if got := queries[i].Get("min_ratio_pct"); got != "80" {
			t.Errorf("page %d lost its filter: min_ratio_pct = %q", i+1, got)
		}
	}
}

// A server that returns everything at once, as the public API does today and
// the internal API always does, still costs exactly one request with no offset.
func TestFetchProxiesMakesOneRequestWhenServerReturnsEverything(t *testing.T) {
	api := newPagedAPI(t, 554, 1000)
	got, err := fetchAll(t, api, SyncOptions{Limit: 1000})
	if err != nil || len(got) != 554 {
		t.Fatalf("got %d proxies, err=%v, want 554", len(got), err)
	}
	queries := api.queries()
	if len(queries) != 1 || queries[0].Has("offset") {
		t.Fatalf("requests = %v, want one request with no offset", queries)
	}
}

func TestFetchProxiesStopsAtTheLimit(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	got, err := fetchAll(t, api, SyncOptions{Limit: 150})
	if err != nil || len(got) != 150 {
		t.Fatalf("got %d proxies, err=%v, want exactly the limit of 150", len(got), err)
	}
	if n := len(api.queries()); n != 2 {
		t.Fatalf("made %d requests, want 2 (no page beyond the limit)", n)
	}
}

func TestFetchProxiesDeduplicatesOverlappingPages(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	api.overlap = true
	got, err := fetchAll(t, api, SyncOptions{Limit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, p := range got {
		if seen[p.URL] {
			t.Fatalf("duplicate %s in %d proxies", p.URL, len(got))
		}
		seen[p.URL] = true
	}
	if len(got) < 245 {
		t.Fatalf("got %d proxies, want about 250 after removing the overlap", len(got))
	}
}

func TestFetchProxiesDoesNotLoopOnAnEmptyPage(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"proxies":[],"has_more":true}`))
	}))
	defer server.Close()
	base, _ := url.Parse(server.URL)
	opts := SyncOptions{PublicAPI: true}.defaults()
	got, err := fetchProxies(context.Background(), server.Client(), opts, base, url.Values{}, "")
	if err != nil || len(got) != 0 || calls != 1 {
		t.Fatalf("got %d proxies, err=%v after %d requests, want 0 proxies after 1 request", len(got), err, calls)
	}
}

func TestFetchProxiesIsBoundedByMaxFetchPages(t *testing.T) {
	api := newPagedAPI(t, 100000, 1)
	got, err := fetchAll(t, api, SyncOptions{Limit: 100000})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(api.queries()); n != maxFetchPages || len(got) != maxFetchPages {
		t.Fatalf("made %d requests and got %d proxies, want %d of each", n, len(got), maxFetchPages)
	}
}

func TestFetchProxiesFailsWholeRefreshWhenALaterPageFails(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	api.failPage = 2
	got, err := fetchAll(t, api, SyncOptions{Limit: 1000})
	if err == nil || !strings.Contains(err.Error(), "page 2") || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("err = %v, want a page 2 HTTP 503 error", err)
	}
	if got != nil {
		t.Fatalf("returned %d proxies from a failed refresh, want none", len(got))
	}
}

// A failed later page must leave the last generated GOST config in place
// rather than replacing it with a partial pool.
func TestSyncKeepsPreviousConfigWhenALaterPageFails(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	api.failPage = 2
	output := filepath.Join(t.TempDir(), "gost.json")
	if err := os.WriteFile(output, []byte("previous config"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := SyncOnce(context.Background(), api.server.Client(), SyncOptions{
		APIURL: api.server.URL + "/proxies", PublicAPI: true, OutputFile: output,
		GOSTAPIUsername: "u", GOSTAPIPassword: "p", ProxyUsername: "u", ProxyPassword: "p",
		RequestTimeout: 5 * time.Second,
	})
	if err == nil {
		t.Fatal("sync succeeded although page 2 failed")
	}
	if data, _ := os.ReadFile(output); string(data) != "previous config" {
		t.Fatalf("config was replaced by a partial pool: %q", data)
	}
}

// End to end: with the server capping pages, the generated config contains
// proxies from every page, including the last.
func TestSyncOncePagesIntoTheGeneratedConfig(t *testing.T) {
	api := newPagedAPI(t, 250, 100)
	mux := http.NewServeMux()
	mux.HandleFunc("/proxies", api.serve)
	mux.HandleFunc("/config/reload", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	server := httptest.NewServer(mux)
	defer server.Close()
	output := filepath.Join(t.TempDir(), "gost.json")
	err := SyncOnce(context.Background(), server.Client(), SyncOptions{
		APIURL: server.URL + "/proxies", PublicAPI: true, OutputFile: output, GOSTAPIURL: server.URL,
		GOSTAPIUsername: "u", GOSTAPIPassword: "p", ProxyUsername: "u", ProxyPassword: "p",
		RequestTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	config, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	first, last := strings.TrimPrefix(proxyN(0).URL, "http://"), strings.TrimPrefix(proxyN(249).URL, "http://")
	if !strings.Contains(string(config), first) || !strings.Contains(string(config), last) {
		t.Fatalf("generated config is missing the first (%s) or last (%s) proxy", first, last)
	}
}
