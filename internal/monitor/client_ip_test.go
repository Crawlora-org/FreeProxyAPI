package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
	"github.com/alicebob/miniredis/v2"
)

func TestClientIPResolverIgnoresUntrustedForwardingHeaders(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/proxies", nil)
	request.RemoteAddr = "198.51.100.9:1234"
	request.Header.Set("CF-Connecting-IP", "203.0.113.1")
	request.Header.Set("X-Forwarded-For", "203.0.113.2")
	if got := newClientIPResolver(nil).resolve(request); got != "198.51.100.9" {
		t.Fatalf("resolve = %q, want socket peer", got)
	}
}

func TestClientIPResolverUsesTrustedSanitizedHeaders(t *testing.T) {
	resolver := newClientIPResolver([]netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	request := httptest.NewRequest(http.MethodGet, "/get", nil)
	request.RemoteAddr = "10.1.2.3:443"
	request.Header.Set("CF-Connecting-IP", "2001:0db8::1")
	if got := resolver.resolve(request); got != "2001:db8::1" {
		t.Fatalf("CF resolve = %q", got)
	}
	request.Header.Del("CF-Connecting-IP")
	request.Header.Set("X-Forwarded-For", "192.0.2.8, 203.0.113.7, 10.2.3.4")
	if got := resolver.resolve(request); got != "203.0.113.7" {
		t.Fatalf("XFF trusted-chain resolve = %q", got)
	}
}

func TestClientIPResolverRejectsMalformedTrustedHeader(t *testing.T) {
	resolver := newClientIPResolver([]netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	request := httptest.NewRequest(http.MethodGet, "/get", nil)
	request.RemoteAddr = "10.1.2.3:443"
	request.Header.Set("CF-Connecting-IP", "victim.example")
	if got := resolver.resolve(request); got != "10.1.2.3" {
		t.Fatalf("malformed header resolve = %q", got)
	}
}

func TestEchoUsesConfiguredClientIPResolver(t *testing.T) {
	resolver := newClientIPResolver([]netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	request := httptest.NewRequest(http.MethodGet, "/get", nil)
	request.RemoteAddr = "10.1.2.3:443"
	request.Header.Set("CF-Connecting-IP", "192.0.2.44")
	recorder := httptest.NewRecorder()
	writeEcho(recorder, request, resolver)
	var payload echoPayload
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Origin != "192.0.2.44" {
		t.Fatalf("echo origin = %q", payload.Origin)
	}
}

func TestEchoReflectsOnlyCallerSuppliedForwardedFor(t *testing.T) {
	resolver := newClientIPResolver([]netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")})
	cases := []struct {
		name   string
		remote string
		cf     string
		xff    string
		origin string
		want   string
	}{
		{name: "transparent proxy leak behind cloudflare", remote: "10.1.2.3:443", cf: "203.0.113.9", xff: "198.51.100.7, 203.0.113.9", origin: "203.0.113.9", want: "198.51.100.7"},
		{name: "ingress-only chain", remote: "10.1.2.3:443", cf: "203.0.113.9", xff: "203.0.113.9", origin: "203.0.113.9", want: ""},
		{name: "trailing trusted hops", remote: "10.1.2.3:443", xff: "198.51.100.7, 203.0.113.9, 10.2.3.4", origin: "203.0.113.9", want: "198.51.100.7"},
		{name: "client not appended by ingress", remote: "10.1.2.3:443", cf: "203.0.113.9", xff: "198.51.100.7", origin: "203.0.113.9", want: "198.51.100.7"},
		{name: "untrusted peer echoes whole header", remote: "198.51.100.9:1234", xff: "198.51.100.7, 203.0.113.9", origin: "198.51.100.9", want: "198.51.100.7, 203.0.113.9"},
		{name: "malformed caller entry is kept", remote: "10.1.2.3:443", cf: "203.0.113.9", xff: "unknown, 203.0.113.9", origin: "203.0.113.9", want: "unknown"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, "/get", nil)
			request.RemoteAddr = tc.remote
			if tc.cf != "" {
				request.Header.Set("CF-Connecting-IP", tc.cf)
			}
			request.Header.Set("X-Forwarded-For", tc.xff)
			recorder := httptest.NewRecorder()
			writeEcho(recorder, request, resolver)
			var payload echoPayload
			if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
				t.Fatal(err)
			}
			if payload.Origin != tc.origin {
				t.Fatalf("origin = %q, want %q", payload.Origin, tc.origin)
			}
			got, ok := payload.Headers["X-Forwarded-For"]
			if got != tc.want || ok != (tc.want != "") {
				t.Fatalf("X-Forwarded-For = %q (present %v), want %q", got, ok, tc.want)
			}
		})
	}
}

func TestEchoBoundsForwardedFor(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/get", nil)
	request.RemoteAddr = "198.51.100.9:1234"
	request.Header.Set("X-Forwarded-For", strings.Repeat("198.51.100.7, ", 100))
	headers := echoedProxyHeaders(request, newClientIPResolver(nil))
	if got := len(headers["X-Forwarded-For"]); got != maxEchoedForwardedFor {
		t.Fatalf("echoed X-Forwarded-For length = %d, want %d", got, maxEchoedForwardedFor)
	}
}

func TestPublicProxyLimiterCannotBeBypassedBySpoofedHeaders(t *testing.T) {
	mini := miniredis.RunT(t)
	redisStore, err := store.NewRedis("redis://"+mini.Addr()+"/0", "client-ip-test")
	if err != nil {
		t.Fatal(err)
	}
	defer redisStore.Close()
	health, err := startHealthServer("127.0.0.1:0", &Runner{store: redisStore, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	for i := 0; i <= publicProxyRequestsPerMin; i++ {
		request := httptest.NewRequest(http.MethodGet, "/proxies", nil)
		request.RemoteAddr = "198.51.100.9:1234"
		request.Header.Set("CF-Connecting-IP", netip.AddrFrom4([4]byte{203, 0, 113, byte(i)}).String())
		recorder := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(recorder, request)
		want := http.StatusOK
		if i == publicProxyRequestsPerMin {
			want = http.StatusTooManyRequests
		}
		if recorder.Code != want {
			t.Fatalf("request %d status = %d, want %d", i+1, recorder.Code, want)
		}
	}
}
