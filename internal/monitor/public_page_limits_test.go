package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

// startLimitTestServer serves n validated US proxies with the given config.
func startLimitTestServer(t *testing.T, prefix string, n int, config Config) (*healthServer, *Runner) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { redisStore.Close() })
	for i := 0; i < n; i++ {
		id := fmt.Sprint(i)
		mini.SAdd(prefix+":validated", id)
		mini.HSet(prefix+":proxy:"+id, "url", "http://proxy"+id+".example:8080", "country", "US", "ok_ratio_pct", "100")
	}
	runner := &Runner{store: redisStore, metrics: newMetrics(), config: config}
	health, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { health.Shutdown() })
	return health, runner
}

func getProxies(t *testing.T, health *healthServer, query string) publicProxiesPayload {
	t.Helper()
	request := httptest.NewRequest(http.MethodGet, "/proxies?"+query, nil)
	request.RemoteAddr = "198.51.100.77:1234"
	recorder := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("%s: status=%d body=%s", query, recorder.Code, recorder.Body.String())
	}
	var data publicProxiesPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	return data
}

// Lowering public_max_limit is how the hosted service moves to small pages: an
// over-limit request is clamped, and an omitted one is the smaller of the
// default and the maximum.
func TestPublicMaxLimitIsConfigurable(t *testing.T) {
	for _, tc := range []struct {
		name  string
		max   int
		query string
		limit int
	}{
		{"default maximum allows 1000", 0, "limit=1000", 1000},
		{"maximum 100 clamps 1000", 100, "limit=1000", 100},
		{"maximum 100 keeps 60", 100, "limit=60", 60},
		{"maximum 100 default page", 100, "", 100},
		{"maximum 40 lowers the default page", 40, "", 40},
		{"maximum 40 clamps 100", 40, "limit=100", 40},
	} {
		t.Run(tc.name, func(t *testing.T) {
			health, _ := startLimitTestServer(t, "limit-cfg", 300, Config{PublicMaxLimit: tc.max})
			data := getProxies(t, health, tc.query)
			if data.Limit != tc.limit || data.Count != min(tc.limit, 300) {
				t.Fatalf("limit=%d count=%d, want limit %d", data.Limit, data.Count, tc.limit)
			}
		})
	}
}

// A client that follows has_more with offset gets every proxy exactly once,
// whatever the page size. This is what proxy-router does when the maximum is
// lowered.
func TestPublicProxiesPageThroughEverythingWithASmallMaximum(t *testing.T) {
	const total = 437
	health, _ := startLimitTestServer(t, "limit-walk", total, Config{PublicMaxLimit: 100})
	seen := map[string]bool{}
	offset, pages := 0, 0
	for {
		data := getProxies(t, health, fmt.Sprintf("limit=1000&offset=%d", offset))
		pages++
		for _, p := range data.Proxies {
			if seen[p.URL] {
				t.Fatalf("duplicate %s on page %d", p.URL, pages)
			}
			seen[p.URL] = true
		}
		if !data.HasMore {
			break
		}
		offset += data.Count
		if pages > 20 {
			t.Fatal("paging did not terminate")
		}
	}
	if len(seen) != total || pages != 5 {
		t.Fatalf("read %d proxies in %d pages, want %d in 5", len(seen), pages, total)
	}
}

// Only requests for more than the default page count as large, so the metric
// reaches zero exactly when nothing depends on large pages any more.
func TestLargePageRequestsAreCounted(t *testing.T) {
	health, runner := startLimitTestServer(t, "limit-metric", 150, Config{})
	for _, query := range []string{"", "limit=10", "limit=100", "limit=0", "limit=abc"} {
		getProxies(t, health, query)
	}
	if got := runner.metrics.publicLargePageRequests.Load(); got != 0 {
		t.Fatalf("counted %d large-page requests for requests at or below the default page", got)
	}
	for _, query := range []string{"limit=101", "limit=1000", "limit=5000"} {
		getProxies(t, health, query)
	}
	if got := runner.metrics.publicLargePageRequests.Load(); got != 3 {
		t.Fatalf("counted %d large-page requests, want 3", got)
	}
	// A cached repeat is still a request for a large page.
	getProxies(t, health, "limit=1000")
	if got := runner.metrics.publicLargePageRequests.Load(); got != 4 {
		t.Fatalf("a cache hit was not counted: %d, want 4", got)
	}

	recorder := httptest.NewRecorder()
	runner.metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"# TYPE freeproxyapi_public_large_page_requests_total counter\n",
		"\nfreeproxyapi_public_large_page_requests_total 4\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestPublicMaxLimitConfig(t *testing.T) {
	load := func(extra string) (Config, error) {
		return LoadConfig(writeConfig(t, `{"sources":["file:///data/proxies.txt"]`+extra+`}`))
	}
	for _, tc := range []struct {
		extra string
		want  int
	}{
		{"", publicProxyMaxLimit},
		{`,"public_max_limit":0`, publicProxyMaxLimit},
		{`,"public_max_limit":100`, 100},
		{`,"public_max_limit":1`, 1},
		{`,"public_max_limit":1000`, 1000},
	} {
		config, err := load(tc.extra)
		if err != nil || config.PublicMaxLimit != tc.want {
			t.Errorf("%q: PublicMaxLimit = %d, err=%v, want %d", tc.extra, config.PublicMaxLimit, err, tc.want)
		}
	}
	for _, extra := range []string{`,"public_max_limit":-1`, `,"public_max_limit":1001`} {
		if _, err := load(extra); err == nil || !strings.Contains(err.Error(), "public_max_limit") {
			t.Errorf("%q: err = %v, want a public_max_limit error", extra, err)
		}
	}
	// A Config built without LoadConfig still serves the default maximum.
	if got := (Config{}).publicMaxLimit(); got != publicProxyMaxLimit {
		t.Errorf("zero Config maximum = %d, want %d", got, publicProxyMaxLimit)
	}
}
