package monitor

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestProxyFilterValidatesClosedSets(t *testing.T) {
	for _, query := range []string{
		"country=USA", "country=U", "country=1A", "country=%24%28id%29",
		"exit_country=U1", "exit_country=%00%00",
		"anonymity=bogus", "anonymity=elite%3Bid",
		"asn=abc", "asn=AS", "asn=AS0", "asn=-5", "asn=99999999999", "asn=AS1.5",
	} {
		if _, err := proxyFilter(httptest.NewRequest(http.MethodGet, "/proxies?"+query, nil)); err == nil {
			t.Errorf("proxyFilter(%q) succeeded, want error", query)
		}
	}
}

func TestProxyFilterCanonicalizesAndClamps(t *testing.T) {
	filter, err := proxyFilter(httptest.NewRequest(http.MethodGet,
		"/proxies?country=us&exit_country=Ca&anonymity=Elite&asn=as015169&max_latency_ms=99999999999&min_ratio_pct=5000", nil))
	if err != nil {
		t.Fatal(err)
	}
	if filter.Country != "US" || filter.ExitCountry != "CA" || filter.Anonymity != "elite" || filter.ASN != "AS15169" {
		t.Fatalf("filter not canonicalized: %+v", filter)
	}
	if filter.MaxLatencyMs != proxyMaxLatencyFilterMs || filter.MinRatioPct != proxyMaxRatioFilterPct {
		t.Fatalf("numeric filters not clamped: %+v", filter)
	}
	same, err := proxyFilter(httptest.NewRequest(http.MethodGet, "/proxies?asn=15169&country=US", nil))
	if err != nil {
		t.Fatal(err)
	}
	other, err := proxyFilter(httptest.NewRequest(http.MethodGet, "/proxies?country=us&asn=AS15169", nil))
	if err != nil {
		t.Fatal(err)
	}
	if publicProxyCacheKey(same, 0) != publicProxyCacheKey(other, 0) {
		t.Fatal("equivalent asn spellings must share a cache key")
	}
	// Garbage numerics keep their historical meaning: the filter is ignored.
	loose, err := proxyFilter(httptest.NewRequest(http.MethodGet, "/proxies?max_latency_ms=abc&min_ratio_pct=-3", nil))
	if err != nil || loose.MaxLatencyMs != 0 || loose.MinRatioPct != 0 {
		t.Fatalf("non-numeric filters = %+v, %v; want ignored", loose, err)
	}
}

func TestPublicProxyLoadSlotsLeaveHeadroom(t *testing.T) {
	for pool, want := range map[int]int{0: 1, 1: 1, 2: 1, 8: 4, 12: 6, 64: publicProxyMaxLoadSlots} {
		if got := publicProxyLoadSlots(pool); got != want {
			t.Errorf("publicProxyLoadSlots(%d) = %d, want %d", pool, got, want)
		}
	}
}

func TestPublicProxyCacheShedsMissesBeyondLoadSlots(t *testing.T) {
	cache := newPublicProxyResponseCache(64, 1<<20, 1<<10, time.Minute, nil)
	cache.limitLoads(2)
	keyFor := func(n int) [32]byte { return publicProxyCacheKey(store.ProxyFilter{MaxLatencyMs: int64(n)}, 0) }

	release := make(chan struct{})
	started := make(chan struct{}, 2)
	var wg sync.WaitGroup
	for n := 1; n <= 2; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, _, err := cache.getOrLoad(context.Background(), keyFor(n), func(context.Context) ([]byte, error) {
				started <- struct{}{}
				<-release
				return []byte("ok"), nil
			})
			if err != nil {
				t.Errorf("load %d: %v", n, err)
			}
		}()
	}
	<-started
	<-started

	// A third distinct miss finds both slots held and is shed, not queued
	// behind the slow scans and not run unbounded.
	begin := time.Now()
	loaded := false
	_, _, _, err := cache.getOrLoad(context.Background(), keyFor(3), func(context.Context) ([]byte, error) {
		loaded = true
		return []byte("x"), nil
	})
	if !errors.Is(err, errPublicProxyBusy) || loaded {
		t.Fatalf("third miss: err=%v loaded=%t, want errPublicProxyBusy without loading", err, loaded)
	}
	if elapsed := time.Since(begin); elapsed > 2*time.Second {
		t.Fatalf("shedding took %s, want a short bounded wait", elapsed)
	}

	// A caller for a key already in flight coalesces onto it instead of
	// needing a slot.
	coalesced := make(chan error, 1)
	go func() {
		_, _, _, err := cache.getOrLoad(context.Background(), keyFor(1), func(context.Context) ([]byte, error) {
			t.Error("coalesced caller must not start its own load")
			return nil, nil
		})
		coalesced <- err
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if err := <-coalesced; err != nil {
		t.Fatalf("coalesced caller: %v", err)
	}

	// Slots are released: new misses and cache hits both work afterwards.
	if _, _, cached, err := cache.getOrLoad(context.Background(), keyFor(3), func(context.Context) ([]byte, error) { return []byte("x"), nil }); err != nil || cached {
		t.Fatalf("miss after release: cached=%t err=%v", cached, err)
	}
	if _, _, cached, err := cache.getOrLoad(context.Background(), keyFor(3), nil); err != nil || !cached {
		t.Fatalf("hit after load: cached=%t err=%v", cached, err)
	}
}

func TestPublicProxiesShedsDistinctMissesButServesCacheHits(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "shed-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	mini.SAdd("shed-test:validated", "one")
	mini.HSet("shed-test:proxy:one", "url", "http://proxy.example:8080", "country", "US", "ok_ratio_pct", "100")
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	get := func(ip, query string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodGet, "/proxies?"+query, nil)
		request.RemoteAddr = ip + ":1234"
		response := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(response, request)
		return response
	}
	if response := get("198.51.100.1", "country=US&limit=1"); response.Code != http.StatusOK {
		t.Fatalf("warm cache: status=%d body=%s", response.Code, response.Body.String())
	}

	// Hold every load slot as a stuck scan would.
	slots := cap(health.handlers.publicCache.loadSlots)
	for i := 0; i < slots; i++ {
		health.handlers.publicCache.loadSlots <- struct{}{}
	}
	shed := get("198.51.100.2", "asn=AS64496&limit=1")
	if shed.Code != http.StatusServiceUnavailable || shed.Header().Get("Retry-After") == "" || !strings.Contains(shed.Body.String(), "busy") {
		t.Fatalf("distinct miss while saturated: status=%d retry=%q body=%s", shed.Code, shed.Header().Get("Retry-After"), shed.Body.String())
	}
	if hit := get("198.51.100.3", "country=US&limit=1"); hit.Code != http.StatusOK || !strings.Contains(hit.Body.String(), "proxy.example") {
		t.Fatalf("cache hit while saturated: status=%d body=%s", hit.Code, hit.Body.String())
	}
	for i := 0; i < slots; i++ {
		<-health.handlers.publicCache.loadSlots
	}
	if after := get("198.51.100.4", "asn=AS64496&limit=1"); after.Code != http.StatusOK {
		t.Fatalf("miss after slots freed: status=%d body=%s", after.Code, after.Body.String())
	}
}
