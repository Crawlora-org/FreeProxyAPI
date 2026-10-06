package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

// staleClock is a clock tests can move; background refreshes read it from
// another goroutine.
type staleClock struct {
	mu sync.Mutex
	t  time.Time
}

func newStaleClock() *staleClock { return &staleClock{t: time.Unix(1_000_000, 0)} }

func (c *staleClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *staleClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// waitIdle waits until the cache has no load in flight.
func waitIdle(t *testing.T, c *publicProxyResponseCache) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		idle := len(c.inFlight) == 0
		c.mu.Unlock()
		if idle {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("cache still has a load in flight")
}

func staleTestCache(clock *staleClock) *publicProxyResponseCache {
	cache := newPublicProxyResponseCache(4, 1<<20, 1<<20, publicProxyCacheTTL, clock.now)
	cache.serveStale(publicProxyStaleWindow)
	return cache
}

func TestPublicProxyCacheServesStaleAndRefreshesOnce(t *testing.T) {
	clock := newStaleClock()
	cache := staleTestCache(clock)
	var stale atomic.Int64
	cache.onStale = func() { stale.Add(1) }
	key := publicProxyCacheKey(store.ProxyFilter{Country: "US"}, 0)
	cache.put(key, []byte(`{"v":1}`))
	clock.advance(publicProxyCacheTTL + time.Second)

	var loads atomic.Int64
	release := make(chan struct{})
	load := func(context.Context) ([]byte, error) {
		loads.Add(1)
		<-release
		return []byte(`{"v":2}`), nil
	}
	// Every caller inside the window is answered at once from the old entry,
	// even though the refresh has not finished.
	for i := 0; i < 5; i++ {
		body, remaining, cached, err := cache.getOrLoad(context.Background(), key, load)
		if err != nil || string(body) != `{"v":1}` || remaining != 0 || !cached {
			t.Fatalf("call %d: body=%s remaining=%s cached=%t err=%v, want the stale body with no TTL", i, body, remaining, cached, err)
		}
	}
	if got := stale.Load(); got != 5 {
		t.Fatalf("onStale ran %d times, want 5", got)
	}
	close(release)
	waitIdle(t, cache)
	if got := loads.Load(); got != 1 {
		t.Fatalf("refresh ran %d times, want exactly 1", got)
	}
	body, remaining, cached, err := cache.getOrLoad(context.Background(), key, load)
	if err != nil || string(body) != `{"v":2}` || remaining <= 0 || !cached {
		t.Fatalf("after refresh: body=%s remaining=%s cached=%t err=%v, want the fresh body", body, remaining, cached, err)
	}
	if got := stale.Load(); got != 5 {
		t.Fatalf("a fresh hit was counted as stale (%d)", got)
	}
}

func TestPublicProxyCacheKeepsServingStaleWhenRefreshFails(t *testing.T) {
	clock := newStaleClock()
	cache := staleTestCache(clock)
	var failures atomic.Int64
	cache.onRefreshFailure = func(error) { failures.Add(1) }
	key := publicProxyCacheKey(store.ProxyFilter{Country: "US"}, 0)
	cache.put(key, []byte(`{"v":1}`))
	clock.advance(publicProxyCacheTTL + time.Second)

	boom := errors.New("redis is down")
	var loads atomic.Int64
	load := func(context.Context) ([]byte, error) {
		loads.Add(1)
		return nil, boom
	}
	for i := 0; i < 3; i++ {
		body, _, cached, err := cache.getOrLoad(context.Background(), key, load)
		if err != nil || string(body) != `{"v":1}` || !cached {
			t.Fatalf("attempt %d: body=%s cached=%t err=%v, want the stale body", i, body, cached, err)
		}
		waitIdle(t, cache)
	}
	if loads.Load() != 3 || failures.Load() != 3 {
		t.Fatalf("loads=%d failures=%d, want a refresh attempt per request (3 each)", loads.Load(), failures.Load())
	}

	// Past the stale window the entry is gone and the failure is visible.
	clock.advance(publicProxyStaleWindow)
	if _, _, _, err := cache.getOrLoad(context.Background(), key, load); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the load error once nothing can be served", err)
	}
	if cache.order.Len() != 0 || cache.bytes != 0 {
		t.Fatalf("entry kept past the stale window: entries=%d bytes=%d", cache.order.Len(), cache.bytes)
	}
}

func TestPublicProxyCacheWithoutStaleWindowLoadsSynchronously(t *testing.T) {
	clock := newStaleClock()
	cache := newPublicProxyResponseCache(4, 1<<20, 1<<20, publicProxyCacheTTL, clock.now)
	key := publicProxyCacheKey(store.ProxyFilter{Country: "US"}, 0)
	cache.put(key, []byte(`{"v":1}`))
	clock.advance(publicProxyCacheTTL + time.Second)
	boom := errors.New("redis is down")
	if _, _, _, err := cache.getOrLoad(context.Background(), key, func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v: an expired entry must not be served when stale serving is off", err)
	}
}

func TestPublicProxyCacheNeverServesStaleForAnUnknownKey(t *testing.T) {
	cache := staleTestCache(newStaleClock())
	boom := errors.New("redis is down")
	key := publicProxyCacheKey(store.ProxyFilter{Country: "ZZ"}, 0)
	if _, _, _, err := cache.getOrLoad(context.Background(), key, func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the load error for a key that was never cached", err)
	}
}

func TestPublicProxyCacheControlForStaleResponses(t *testing.T) {
	if got := publicProxyCacheControlFor(0, true); got != publicProxyStaleCacheControl {
		t.Fatalf("stale Cache-Control = %q, want %q", got, publicProxyStaleCacheControl)
	}
	if got := publicProxyCacheControlFor(19*time.Second+800*time.Millisecond, true); got != "public, max-age=19, s-maxage=19" {
		t.Fatalf("fresh hit Cache-Control = %q", got)
	}
	if got := publicProxyCacheControlFor(0, false); got != publicProxyCacheControl {
		t.Fatalf("miss Cache-Control = %q", got)
	}
}

// The timeouts only make sense in this order; a change that breaks it brings
// back 503s for scans that would have succeeded.
func TestPublicProxyTimeoutsAreOrdered(t *testing.T) {
	if publicProxyLoadTimeout <= readyzTimeout {
		t.Fatalf("load timeout %s must exceed the %s readiness probe budget", publicProxyLoadTimeout, readyzTimeout)
	}
	if publicProxyRequestTimeout <= publicProxyLoadTimeout+publicProxyLoadSlotWait {
		t.Fatalf("request timeout %s must exceed load timeout %s plus slot wait %s", publicProxyRequestTimeout, publicProxyLoadTimeout, publicProxyLoadSlotWait)
	}
	// proxy-router waits 15s for a response by default.
	if publicProxyRequestTimeout >= 15*time.Second {
		t.Fatalf("request timeout %s must stay below proxy-router's 15s client timeout", publicProxyRequestTimeout)
	}
	if publicProxyRequestTimeout >= httpWriteTimeout {
		t.Fatalf("request timeout %s must stay below the %s write timeout", publicProxyRequestTimeout, httpWriteTimeout)
	}
}

func startStaleTestServer(t *testing.T, prefix string) (*miniredis.Miniredis, *healthServer, *Runner, *staleClock) {
	t.Helper()
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { redisStore.Close() })
	mini.SAdd(prefix+":validated", "one")
	mini.HSet(prefix+":proxy:one", "url", "http://one.example:8080", "country", "US", "ok_ratio_pct", "100")
	runner := &Runner{store: redisStore, metrics: newMetrics()}
	health, err := startHealthServer("127.0.0.1:0", runner)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { health.Shutdown() })
	clock := newStaleClock()
	health.handlers.publicCache.now = clock.now
	health.handlers.statsCache.now = clock.now
	return mini, health, runner, clock
}

func serve(health *healthServer, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.RemoteAddr = "198.51.100.10:1234"
	response := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(response, request)
	return response
}

// A Redis outage after a query was served must not turn that query into a 503:
// the previous response keeps being served, marked stale, until the stale
// window passes. Only then does the failure become visible, with Retry-After.
func TestPublicProxiesServesStaleDuringRedisOutage(t *testing.T) {
	mini, health, runner, clock := startStaleTestServer(t, "stale-outage")
	first := serve(health, "/proxies?country=US&limit=5")
	if first.Code != http.StatusOK || !strings.Contains(first.Body.String(), "one.example") {
		t.Fatalf("initial response: status=%d body=%s", first.Code, first.Body.String())
	}
	if first.Header().Get("Warning") != "" {
		t.Fatalf("a fresh response carried a stale warning: %q", first.Header().Get("Warning"))
	}

	mini.Close()
	clock.advance(publicProxyCacheTTL + time.Second)
	stale := serve(health, "/proxies?country=US&limit=5")
	if stale.Code != http.StatusOK || stale.Body.String() != first.Body.String() {
		t.Fatalf("outage response: status=%d body=%s, want the previous body", stale.Code, stale.Body.String())
	}
	if got := stale.Header().Get("Cache-Control"); got != publicProxyStaleCacheControl {
		t.Fatalf("stale Cache-Control = %q, want %q", got, publicProxyStaleCacheControl)
	}
	if got := stale.Header().Get("Warning"); !strings.HasPrefix(got, "110 ") {
		t.Fatalf("stale Warning = %q, want a 110 warning", got)
	}
	if got := runner.metrics.publicStaleResponses.Load(); got != 1 {
		t.Fatalf("stale responses counted = %d, want 1", got)
	}
	if got := runner.metrics.publicQueryFailures.Load(); got != 0 {
		t.Fatalf("a stale response was counted as a failure (%d)", got)
	}
	waitIdle(t, health.handlers.publicCache)
	if got := runner.metrics.publicRefreshFailures.Load(); got != 1 {
		t.Fatalf("refresh failures counted = %d, want 1", got)
	}

	clock.advance(publicProxyStaleWindow)
	failed := serve(health, "/proxies?country=US&limit=5")
	if failed.Code != http.StatusServiceUnavailable || !strings.Contains(failed.Body.String(), "query failed") {
		t.Fatalf("after the stale window: status=%d body=%s, want 503 query failed", failed.Code, failed.Body.String())
	}
	if got := failed.Header().Get("Retry-After"); got != "5" {
		t.Fatalf("Retry-After = %q, want 5", got)
	}
	if got := runner.metrics.publicQueryFailures.Load(); got != 1 {
		t.Fatalf("query failures counted = %d, want 1", got)
	}
}

// With Redis healthy, an expired entry is answered at once and replaced in the
// background, so the next request sees the new data with a normal TTL.
func TestPublicProxiesRefreshesInBackgroundAfterTTL(t *testing.T) {
	mini, health, _, clock := startStaleTestServer(t, "stale-refresh")
	first := serve(health, "/proxies?country=US&limit=5")
	if first.Code != http.StatusOK || strings.Contains(first.Body.String(), "two.example") {
		t.Fatalf("initial response: status=%d body=%s", first.Code, first.Body.String())
	}
	mini.SAdd("stale-refresh:validated", "two")
	mini.HSet("stale-refresh:proxy:two", "url", "http://two.example:8080", "country", "US", "ok_ratio_pct", "100")

	clock.advance(publicProxyCacheTTL + time.Second)
	stale := serve(health, "/proxies?country=US&limit=5")
	if stale.Code != http.StatusOK || stale.Body.String() != first.Body.String() {
		t.Fatalf("response after TTL: status=%d body=%s, want the old body immediately", stale.Code, stale.Body.String())
	}
	waitIdle(t, health.handlers.publicCache)

	fresh := serve(health, "/proxies?country=US&limit=5")
	if fresh.Code != http.StatusOK || !strings.Contains(fresh.Body.String(), "two.example") {
		t.Fatalf("response after the refresh: status=%d body=%s, want the new proxy", fresh.Code, fresh.Body.String())
	}
	if got := fresh.Header().Get("Cache-Control"); got == publicProxyStaleCacheControl || fresh.Header().Get("Warning") != "" {
		t.Fatalf("refreshed response still looks stale: Cache-Control=%q Warning=%q", got, fresh.Header().Get("Warning"))
	}
}

func TestPublicQueryFailuresAreCountedAndAdvertiseRetry(t *testing.T) {
	mini, health, runner, _ := startStaleTestServer(t, "stale-miss")
	mini.Close()
	for _, path := range []string{"/proxies?country=US&limit=5", "/stats"} {
		response := serve(health, path)
		if response.Code != http.StatusServiceUnavailable {
			t.Fatalf("%s: status=%d, want 503 for a key that was never cached", path, response.Code)
		}
		if got := response.Header().Get("Retry-After"); got != "5" {
			t.Fatalf("%s: Retry-After = %q, want 5", path, got)
		}
	}
	if got := runner.metrics.publicQueryFailures.Load(); got != 2 {
		t.Fatalf("query failures counted = %d, want 2", got)
	}

	recorder := httptest.NewRecorder()
	runner.metrics.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{
		"# TYPE freeproxyapi_public_query_failures_total counter\n",
		"\nfreeproxyapi_public_query_failures_total 2\n",
		"# TYPE freeproxyapi_public_stale_responses_total counter\n",
		"\nfreeproxyapi_public_stale_responses_total 0\n",
		"# TYPE freeproxyapi_public_refresh_failures_total counter\n",
		"\nfreeproxyapi_public_refresh_failures_total 0\n",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}
