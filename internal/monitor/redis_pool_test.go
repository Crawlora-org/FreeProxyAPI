package monitor

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

// startDelayedRedisProxy forwards TCP to target and delays every server reply
// by delay, standing in for a busy single-threaded Redis whose event loop
// adds latency to each round trip.
func startDelayedRedisProxy(t *testing.T, target string, delay time.Duration) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	track := func(c net.Conn) {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}
	t.Cleanup(func() {
		_ = listener.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			_ = c.Close()
		}
	})
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			server, err := net.Dial("tcp", target)
			if err != nil {
				_ = client.Close()
				continue
			}
			track(client)
			track(server)
			go func() {
				_, _ = io.Copy(server, client)
				_ = server.Close()
			}()
			go func() {
				defer client.Close()
				buf := make([]byte, 32<<10)
				for {
					n, err := server.Read(buf)
					if n > 0 {
						time.Sleep(delay)
						if _, werr := client.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	return listener.Addr().String()
}

// saturateRedisPool runs n goroutines issuing back-to-back Redis round trips,
// like probe workers claiming and completing work, until the test ends.
func saturateRedisPool(t *testing.T, redisStore *store.Redis, n int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				_ = redisStore.Ping(ctx)
			}
		}()
	}
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	deadline := time.Now().Add(5 * time.Second)
	for redisStore.PoolStats().PendingRequests < uint32(n/2) {
		if time.Now().After(deadline) {
			t.Fatalf("worker pool never saturated: %+v", redisStore.PoolStats())
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// newSaturatedPools returns a tiny worker pool held busy by many goroutines
// and a separate API pool, both reaching miniredis through a delaying proxy.
func newSaturatedPools(t *testing.T, namespace string) (workers, api *store.Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	addr := startDelayedRedisProxy(t, mini.Addr(), 25*time.Millisecond)
	url := "redis://" + addr + "/0"
	workers, err := store.NewRedisWithPool(url, namespace, store.PoolOptions{PoolSize: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = workers.Close() })
	api, err = store.NewRedisWithPool(url, namespace, apiRedisPoolOptions(Config{RedisAPIPoolSize: 2}))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = api.Close() })
	// 128 goroutines on 2 connections with 25ms round trips queue each new
	// command for roughly 1.6s behind worker traffic.
	saturateRedisPool(t, workers, 128)
	return workers, api
}

func TestSaturatedWorkerPoolStarvesSharedReadsButNotAPIPool(t *testing.T) {
	workers, api := newSaturatedPools(t, "pool-isolation-test")
	filter := store.ProxyFilter{Limit: 10}

	shared := &Runner{store: workers}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	started := time.Now()
	_, err := shared.readStore().QueryValidated(ctx, filter)
	cancel()
	if err == nil {
		t.Fatal("query through the saturated shared worker pool succeeded; reproduction did not starve it")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shared-pool query error = %v, want context deadline exceeded", err)
	}
	t.Logf("shared worker pool: query failed after %s: %v", time.Since(started).Round(time.Millisecond), err)

	isolated := &Runner{store: workers, apiStore: api}
	if isolated.readStore() != api {
		t.Fatal("readStore must prefer the dedicated API client")
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	started = time.Now()
	if _, err := isolated.readStore().QueryValidated(ctx, filter); err != nil {
		t.Fatalf("query through the API pool failed while workers saturate their pool: %v", err)
	}
	t.Logf("dedicated API pool: query succeeded in %s", time.Since(started).Round(time.Millisecond))
}

func TestInternalAPIUsesDedicatedPoolUnderWorkerSaturation(t *testing.T) {
	workers, api := newSaturatedPools(t, "pool-internal-api-test")
	const token = "0123456789abcdef-token"
	mux := http.NewServeMux()
	registerInternalAPI(mux, &Runner{store: workers, apiStore: api, metrics: newMetrics()}, token)

	request := httptest.NewRequest(http.MethodGet, "/internal/api/v1/proxies", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	recorder := httptest.NewRecorder()
	started := time.Now()
	mux.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d want 200 body=%s", recorder.Code, recorder.Body.String())
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("internal API took %s under worker saturation", elapsed)
	}
}

func TestAPIRedisPoolFailsFastBelowHandlerDeadlines(t *testing.T) {
	options := apiRedisPoolOptions(Config{})
	if options.PoolSize != defaultRedisAPIPoolSize || options.MinIdleConns != apiRedisMinIdleConns {
		t.Fatalf("api pool options = %+v", options)
	}
	if options.PoolTimeout <= 0 || options.PoolTimeout >= readyzTimeout || options.PoolTimeout >= internalAPIQueryTimeout {
		t.Fatalf("api pool timeout %s must be positive and below handler deadlines", options.PoolTimeout)
	}
	worker := workerRedisPoolOptions(Config{Workers: 768, RedisPoolSize: 20})
	if worker.PoolSize != 20 || worker.MinIdleConns != 5 || worker.PoolTimeout != 0 {
		t.Fatalf("worker pool options = %+v", worker)
	}
}

func TestMetricsExposeRedisPoolStats(t *testing.T) {
	m := newMetrics()
	m.RegisterRedisPool("workers", func() store.PoolStats {
		return store.PoolStats{Size: 20, Hits: 11, Misses: 7, Timeouts: 1, WaitCount: 3, WaitDurationNs: 1_500_000_000, StaleConns: 4, TotalConns: 20, IdleConns: 2, PendingRequests: 9}
	})
	m.RegisterRedisPool("api", func() store.PoolStats { return store.PoolStats{Size: 8, TotalConns: 2, IdleConns: 2} })

	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		"# TYPE freeproxyapi_redis_pool_size gauge\n",
		`freeproxyapi_redis_pool_size{pool="api"} 8`,
		`freeproxyapi_redis_pool_size{pool="workers"} 20`,
		`freeproxyapi_redis_pool_connections{pool="workers"} 20`,
		`freeproxyapi_redis_pool_idle_connections{pool="workers"} 2`,
		`freeproxyapi_redis_pool_pending_requests{pool="workers"} 9`,
		"# TYPE freeproxyapi_redis_pool_hits_total counter\n",
		`freeproxyapi_redis_pool_hits_total{pool="workers"} 11`,
		`freeproxyapi_redis_pool_misses_total{pool="workers"} 7`,
		`freeproxyapi_redis_pool_waits_total{pool="workers"} 3`,
		`freeproxyapi_redis_pool_wait_seconds_total{pool="workers"} 1.5`,
		`freeproxyapi_redis_pool_timeouts_total{pool="workers"} 1`,
		`freeproxyapi_redis_pool_stale_connections_total{pool="workers"} 4`,
		`freeproxyapi_redis_pool_timeouts_total{pool="api"} 0`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
	if got := strings.Count(text, "# TYPE freeproxyapi_redis_pool_waits_total "); got != 1 {
		t.Fatalf("waits family TYPE emitted %d times", got)
	}
	if strings.Index(text, `freeproxyapi_redis_pool_size{pool="api"}`) > strings.Index(text, `freeproxyapi_redis_pool_size{pool="workers"}`) {
		t.Fatal("pool series must be sorted by pool label")
	}

	// Registering an existing name replaces its source instead of duplicating.
	m.RegisterRedisPool("api", func() store.PoolStats { return store.PoolStats{Size: 16} })
	recorder = httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if text := recorder.Body.String(); strings.Count(text, `freeproxyapi_redis_pool_size{pool="api"}`) != 1 || !strings.Contains(text, `freeproxyapi_redis_pool_size{pool="api"} 16`) {
		t.Fatalf("re-registered pool not replaced:\n%s", text)
	}
}

func TestMetricsRedisPoolStatsFromRealClient(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedisWithPool("redis://"+mini.Addr()+"/0", "pool-metrics-test", store.PoolOptions{PoolSize: 3})
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	if err := redisStore.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	m := newMetrics()
	m.RegisterRedisPool("workers", redisStore.PoolStats)
	recorder := httptest.NewRecorder()
	m.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	text := recorder.Body.String()
	for _, want := range []string{
		`freeproxyapi_redis_pool_size{pool="workers"} 3`,
		`freeproxyapi_redis_pool_connections{pool="workers"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("metrics missing %q:\n%s", want, text)
		}
	}
}

func TestLoadConfigRedisPoolSizes(t *testing.T) {
	defaults, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if defaults.RedisPoolSize != minAutoRedisPoolSize || defaults.RedisAPIPoolSize != defaultRedisAPIPoolSize {
		t.Fatalf("default pools: workers=%d api=%d", defaults.RedisPoolSize, defaults.RedisAPIPoolSize)
	}
	for workers, want := range map[int]int{1: 10, 320: 10, 512: 16, 768: 24, 2000: 32} {
		if got := autoRedisPoolSize(workers); got != want {
			t.Errorf("autoRedisPoolSize(%d) = %d, want %d", workers, got, want)
		}
	}
	auto, err := LoadConfig(writeConfig(t, `{"sources":["file:///d/p.txt"],"workers":768}`))
	if err != nil || auto.RedisPoolSize != 24 {
		t.Fatalf("auto pool for 768 workers = %d err=%v", auto.RedisPoolSize, err)
	}
	explicit, err := LoadConfig(writeConfig(t, `{"sources":["file:///d/p.txt"],"workers":768,"redis_pool_size":256,"redis_api_pool_size":64}`))
	if err != nil || explicit.RedisPoolSize != 256 || explicit.RedisAPIPoolSize != 64 {
		t.Fatalf("explicit pools: workers=%d api=%d err=%v", explicit.RedisPoolSize, explicit.RedisAPIPoolSize, err)
	}
	for name, body := range map[string]string{
		"negative worker pool":  `{"sources":["file:///d/p.txt"],"redis_pool_size":-1}`,
		"oversized worker pool": `{"sources":["file:///d/p.txt"],"redis_pool_size":257}`,
		"negative api pool":     `{"sources":["file:///d/p.txt"],"redis_api_pool_size":-1}`,
		"oversized api pool":    `{"sources":["file:///d/p.txt"],"redis_api_pool_size":65}`,
	} {
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("%s: LoadConfig accepted an unsafe pool size", name)
		}
	}
}

// TestLiveLocalMonitorConfigRedisConnectionBudget keeps the production
// per-replica pools well below Redis maxclients at the live replica count.
func TestLiveLocalMonitorConfigRedisConnectionBudget(t *testing.T) {
	config, err := LoadConfig(filepath.Join("..", "..", "k8s", "overlays", "live-local", "monitor.json"))
	if err != nil {
		t.Fatalf("LoadConfig live-local: %v", err)
	}
	if config.RedisPoolSize != 20 || config.RedisAPIPoolSize != 8 {
		t.Fatalf("live-local redis pools: workers=%d api=%d", config.RedisPoolSize, config.RedisAPIPoolSize)
	}
	const liveReplicas, redisMaxClients = 32, 10000
	if total := liveReplicas * (config.RedisPoolSize + config.RedisAPIPoolSize); total > redisMaxClients/4 {
		t.Fatalf("live-local Redis connection budget %d exceeds a quarter of maxclients", total)
	}
}
