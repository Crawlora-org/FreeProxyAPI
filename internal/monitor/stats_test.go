package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func TestStatsSortsCountriesByCount(t *testing.T) {
	body, err := json.Marshal(statsPayload{
		ByCountry:       sortedCounts{"US": 3, "GB": 2, "CA": 1},
		StableByCountry: sortedCounts{"US": 2, "CA": 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	output := string(body)
	if strings.Index(output, `"US":3`) > strings.Index(output, `"GB":2`) ||
		strings.Index(output, `"GB":2`) > strings.Index(output, `"CA":1`) {
		t.Fatalf("countries were not sorted by descending count: %s", output)
	}
}

func TestStatsCacheExpiresAndAvoidsRepeatedRedisReads(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "stats-cache-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	for id, values := range map[string][]string{
		"us-1": {"US", "US", "90"},
		"us-2": {"US", "US", "90"},
		"us-3": {"US", "US", "90"},
		"gb-1": {"GB", "GB", "90"},
		"ca-1": {"CA", "CA", "90"},
	} {
		mini.SAdd("stats-cache-test:validated", id)
		mini.HSet("stats-cache-test:proxy:"+id,
			"country", values[0], "exit_country", values[1], "ok_ratio_pct", values[2])
	}
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()

	first := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(first, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if first.Code != http.StatusOK {
		t.Fatalf("initial stats: status=%d body=%s", first.Code, first.Body.String())
	}
	if got := first.Header().Get("Cache-Control"); got != publicProxyCacheControl {
		t.Fatalf("initial stats Cache-Control = %q", got)
	}
	if output := first.Body.String(); strings.Index(output, `"US":3`) > strings.Index(output, `"GB":1`) {
		t.Fatalf("stats countries were not ordered by count: %s", output)
	}

	mini.FlushAll()
	second := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(second, httptest.NewRequest(http.MethodGet, "/stats", nil))
	if second.Code != http.StatusOK || second.Body.String() != first.Body.String() {
		t.Fatalf("cached stats response changed: status=%d body=%s", second.Code, second.Body.String())
	}
	if !strings.HasPrefix(second.Header().Get("Cache-Control"), "public, max-age=") {
		t.Fatalf("cached stats Cache-Control = %q", second.Header().Get("Cache-Control"))
	}
}
