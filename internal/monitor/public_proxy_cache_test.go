package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func TestPublicProxyCacheCanonicalFilterKeyAndExpiry(t *testing.T) {
	now := time.Unix(1_000, 0)
	cache := newPublicProxyResponseCache(2, 1024, 512, publicProxyCacheTTL, func() time.Time { return now })
	filter := store.ProxyFilter{
		Country: "US", ExitCountry: "CA", ASN: "AS123", Anonymity: "elite",
		GeoMismatch: true, MaxLatencyMs: 200, MinRatioPct: 90, Limit: 10,
	}
	key := publicProxyCacheKey(filter, 0)
	cache.put(key, []byte(`{"count":1}`))

	if _, remaining, ok := cache.get(key); !ok || remaining != publicProxyCacheTTL {
		t.Fatal("cached successful response was not found")
	}
	now = now.Add(10*time.Second + 200*time.Millisecond)
	if _, remaining, ok := cache.get(key); !ok || remaining != 19*time.Second+800*time.Millisecond {
		t.Fatalf("cache remaining TTL = %s, want 19.8s", remaining)
	}
	if got := publicProxyCacheControlFor(19*time.Second+800*time.Millisecond, true); got != "public, max-age=19, s-maxage=19" {
		t.Fatalf("cached Cache-Control = %q", got)
	}
	now = now.Add(19*time.Second + 800*time.Millisecond)
	if _, _, ok := cache.get(key); ok {
		t.Fatal("expired response was returned from cache")
	}
	if cache.order.Len() != 0 || cache.bytes != 0 {
		t.Fatalf("expired entry was retained: entries=%d bytes=%d", cache.order.Len(), cache.bytes)
	}

	if publicProxyCacheKey(filter, 0) == publicProxyCacheKey(store.ProxyFilter{Country: "US", ExitCountry: "CA", ASN: "AS123", Anonymity: "elite", GeoMismatch: true, MaxLatencyMs: 200, MinRatioPct: 90, Limit: 11}, 0) {
		t.Fatal("limit was omitted from cache key")
	}
	if publicProxyCacheKey(filter, 0) == publicProxyCacheKey(filter, 10) {
		t.Fatal("offset was omitted from cache key")
	}
	if publicProxyCacheKey(store.ProxyFilter{Limit: -1}, -1) != publicProxyCacheKey(store.ProxyFilter{}, 0) {
		t.Fatal("non-positive limits and offsets did not use the same cache key")
	}
	if len(publicProxyCacheKey(store.ProxyFilter{ASN: strings.Repeat("x", 1<<20)}, 0)) != 32 {
		t.Fatal("cache key size is not bounded")
	}
}

func TestPublicProxyCacheEntryAdmitsFullPage(t *testing.T) {
	proxies := make([]store.QueryProxy, publicProxyMaxLimit)
	for i := range proxies {
		proxies[i] = store.QueryProxy{
			URL: "socks5://255.255.255.255:65535", Scheme: "socks5", Country: "US", ExitCountry: "GB",
			GeoMismatch: true, ASN: "AS4294967295 " + strings.Repeat("o", 64), Anonymity: "anonymous",
			LatencyMs: 99_999, JitterMs: 99_999, OkRatioPct: 100, LastCheckedAt: 1_900_000_000_000,
		}
	}
	body, err := json.Marshal(publicProxiesPayload{Count: len(proxies), Proxies: proxies, Limit: publicProxyMaxLimit, Offset: publicProxyMaxOffset, HasMore: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(body)+1 > publicProxyCacheEntrySize || publicProxyCacheEntrySize > publicProxyCacheBytes {
		t.Fatalf("full page of %d bytes is not cacheable (entry limit %d, cache %d)", len(body)+1, publicProxyCacheEntrySize, publicProxyCacheBytes)
	}
}

func TestPublicProxyCacheCanceledLeaderDoesNotFailWaiters(t *testing.T) {
	cache := newPublicProxyResponseCache(2, 1024, 512, publicProxyCacheTTL, time.Now)
	key := publicProxyCacheKey(store.ProxyFilter{Country: "US"}, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	defer cancelLeader()
	leader := make(chan error, 1)
	go func() {
		_, _, _, err := cache.getOrLoad(leaderCtx, key, func(loadCtx context.Context) ([]byte, error) {
			close(started)
			<-release
			if err := loadCtx.Err(); err != nil {
				return nil, err
			}
			if _, ok := loadCtx.Deadline(); !ok {
				return nil, errors.New("shared load has no deadline")
			}
			return []byte(`{"count":1}`), nil
		})
		leader <- err
	}()
	<-started
	waiter := make(chan error, 1)
	go func() {
		body, _, _, err := cache.getOrLoad(context.Background(), key, func(context.Context) ([]byte, error) {
			return nil, errors.New("follower ran a duplicate load")
		})
		if err == nil && string(body) != `{"count":1}` {
			err = errors.New("unexpected body " + string(body))
		}
		waiter <- err
	}()
	cancelLeader()
	close(release)
	if err := <-waiter; err != nil {
		t.Fatalf("waiter failed after leader cancellation: %v", err)
	}
	if err := <-leader; err != nil {
		t.Fatalf("leader load was canceled with its request: %v", err)
	}
}

func TestPublicEndpointsServeWhenFirstCallerCanceled(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "cancel-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	mini.SAdd("cancel-test:validated", "one")
	mini.HSet("cancel-test:proxy:one", "url", "http://proxy.example:8080", "country", "US", "exit_country", "US", "ok_ratio_pct", "100")
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	for _, path := range []string{"/proxies?country=US", "/stats"} {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		first := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, path, nil).WithContext(canceled))
		if first.Code != http.StatusOK {
			t.Fatalf("%s: canceled first caller poisoned the shared load: status=%d body=%s", path, first.Code, first.Body.String())
		}
		waiter := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(waiter, httptest.NewRequest(http.MethodGet, path, nil))
		if waiter.Code != http.StatusOK || waiter.Body.String() != first.Body.String() {
			t.Fatalf("%s: waiter status=%d body=%s", path, waiter.Code, waiter.Body.String())
		}
		if got := waiter.Header().Get("X-Content-Type-Options"); got != "nosniff" {
			t.Fatalf("%s: X-Content-Type-Options = %q", path, got)
		}
	}
}

func TestPublicProxyCacheCoalescesConcurrentMisses(t *testing.T) {
	cache := newPublicProxyResponseCache(2, 1024, 512, publicProxyCacheTTL, time.Now)
	key := publicProxyCacheKey(store.ProxyFilter{Country: "US"}, 0)
	started := make(chan struct{})
	release := make(chan struct{})
	results := make(chan error, 2)
	loads := 0
	var loadsMu sync.Mutex
	load := func(context.Context) ([]byte, error) {
		loadsMu.Lock()
		loads++
		loadsMu.Unlock()
		close(started)
		<-release
		return []byte(`{"count":1}`), nil
	}
	go func() {
		_, _, _, err := cache.getOrLoad(context.Background(), key, load)
		results <- err
	}()
	<-started
	go func() {
		_, _, _, err := cache.getOrLoad(context.Background(), key, func(context.Context) ([]byte, error) {
			return nil, errors.New("follower ran a duplicate load")
		})
		results <- err
	}()
	close(release)
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	loadsMu.Lock()
	defer loadsMu.Unlock()
	if loads != 1 {
		t.Fatalf("loads=%d, want 1", loads)
	}
}

func TestPublicProxiesCacheUsesNormalizedFilterAndStillRateLimits(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "public-cache-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	mini.SAdd("public-cache-test:validated", "one")
	mini.HSet("public-cache-test:proxy:one", "url", "http://proxy.example:8080", "country", "US", "ok_ratio_pct", "100")
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	first := httptest.NewRequest(http.MethodGet, "/proxies?country=us&limit=1", nil)
	first.RemoteAddr = "198.51.100.10:1234"
	firstResponse := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(firstResponse, first)
	if firstResponse.Code != http.StatusOK || !strings.Contains(firstResponse.Body.String(), "proxy.example") {
		t.Fatalf("initial response: status=%d body=%s", firstResponse.Code, firstResponse.Body.String())
	}

	mini.SRem("public-cache-test:validated", "one")
	second := httptest.NewRequest(http.MethodGet, "/proxies?limit=1&country=US", nil)
	second.RemoteAddr = "198.51.100.10:1234"
	secondResponse := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(secondResponse, second)
	if secondResponse.Code != http.StatusOK || secondResponse.Body.String() != firstResponse.Body.String() {
		t.Fatalf("equivalent request missed cache: status=%d body=%s", secondResponse.Code, secondResponse.Body.String())
	}

	for i := 0; i < publicProxyRequestsPerMin-2; i++ {
		request := httptest.NewRequest(http.MethodGet, "/proxies?country=US&limit=1", nil)
		request.RemoteAddr = "198.51.100.10:1234"
		response := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("cached request %d: status=%d", i+3, response.Code)
		}
	}
	request := httptest.NewRequest(http.MethodGet, "/proxies?country=US&limit=1", nil)
	request.RemoteAddr = "198.51.100.10:1234"
	response := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("rate limit was bypassed by cache: status=%d body=%s", response.Code, response.Body.String())
	}
}
