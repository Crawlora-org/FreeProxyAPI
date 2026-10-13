package monitor

import (
	"net/http/httptest"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestProxyFilterParsesRequireTamperChecked(t *testing.T) {
	for _, path := range []string{"/proxies?require_tamper_checked=true", "/internal/api/v1/proxies?require_tamper_checked=true&https=true"} {
		filter, err := proxyFilter(httptest.NewRequest("GET", path, nil))
		if err != nil || !filter.RequireTamperChecked {
			t.Fatalf("%s: filter=%+v err=%v", path, filter, err)
		}
	}
	filter, err := proxyFilter(httptest.NewRequest("GET", "/proxies?require_tamper_checked=false", nil))
	if err != nil || filter.RequireTamperChecked {
		t.Fatalf("require_tamper_checked=false parsed as %+v err=%v", filter, err)
	}
	if publicProxyCacheKey(store.ProxyFilter{RequireTamperChecked: true}, 0) == publicProxyCacheKey(store.ProxyFilter{}, 0) {
		t.Fatal("require_tamper_checked must change the cache key")
	}
}
