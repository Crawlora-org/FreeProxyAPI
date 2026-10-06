package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestRedactURLsDropsCredentialsPathAndQuery(t *testing.T) {
	for in, want := range map[string]string{
		`Get "https://user:pw@feeds.example.com/list/SECRETKEY?token=abc#frag": EOF`: `Get "https://feeds.example.com": EOF`,
		"fetch http://a.example/x?key=1, then http://b.example/y.":                   "fetch http://a.example, then http://b.example.",
		"no url here":         "no url here",
		"bad http://[::1 end": "bad <url> end",
	} {
		if got := redactURLs(in); got != want {
			t.Errorf("redactURLs(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestRedactSourceErrorKeepsUnwrapAndHidesSecrets(t *testing.T) {
	cause := context.DeadlineExceeded
	wrapped := &url.Error{Op: "Get", URL: "https://feeds.example.com/v1/SECRETKEY/list?token=abc", Err: cause}
	redacted := redactSourceError(fmt.Errorf("fetch source: %w", wrapped))
	if strings.Contains(redacted.Error(), "SECRETKEY") || strings.Contains(redacted.Error(), "token") {
		t.Fatalf("redacted error still leaks the URL: %v", redacted)
	}
	if !errors.Is(redacted, context.DeadlineExceeded) {
		t.Fatal("redaction must preserve errors.Is for retry classification")
	}
	var urlErr *url.Error
	if !errors.As(redacted, &urlErr) {
		t.Fatal("redaction must preserve errors.As")
	}
	plain := errors.New("boom")
	if redactSourceError(plain) != plain {
		t.Fatal("errors without URLs must be returned unchanged")
	}
	if redactSourceError(nil) != nil {
		t.Fatal("nil must stay nil")
	}
}

func TestSourceLogLabelHidesPathAndQuery(t *testing.T) {
	if got := sourceLogLabel("https://api.example.com/KEY123/list?token=abc"); got != "https://api.example.com" {
		t.Fatalf("label = %q", got)
	}
	if got := sourceLogLabel("file:///data/proxies.txt"); strings.Contains(got, "proxies") {
		t.Fatalf("file label = %q", got)
	}
}

func TestHTMLPagesSendCSPAndSecurityHeaders(t *testing.T) {
	for _, tc := range []struct {
		name  string
		page  []byte
		extra bool
	}{{"homepage", homepageHTML, false}, {"dashboard", dashboardHTML, false}} {
		page := newHTMLPage(tc.page, pageOptions{})
		for _, forbidden := range []string{"'unsafe-inline' 'sha", "script-src 'unsafe-inline'", "unsafe-eval", "googletagmanager", "default-src *"} {
			if strings.Contains(page.csp, forbidden) {
				t.Errorf("%s CSP contains %q: %s", tc.name, forbidden, page.csp)
			}
		}
		scriptSrc := strings.SplitN(strings.SplitN(page.csp, "script-src ", 2)[1], ";", 2)[0]
		if scriptSrc == "'self'" || !strings.Contains(scriptSrc, "'sha256-") {
			t.Errorf("%s script-src must allow its inline scripts by hash: %q", tc.name, scriptSrc)
		}
		for _, want := range []string{"default-src 'none'", "frame-ancestors 'none'", "base-uri 'none'", "connect-src 'self'"} {
			if !strings.Contains(page.csp, want) {
				t.Errorf("%s CSP missing %q", tc.name, want)
			}
		}
		response := httptest.NewRecorder()
		serveHTML(response, httptest.NewRequest(http.MethodGet, "/", nil), "/", page)
		if response.Header().Get("Content-Security-Policy") != page.csp ||
			response.Header().Get("X-Content-Type-Options") != "nosniff" ||
			response.Header().Get("Referrer-Policy") == "" {
			t.Errorf("%s response headers = %v", tc.name, response.Header())
		}
	}
}

func TestCSPHashesMatchEveryExecutableInlineScript(t *testing.T) {
	body := []byte(`<script type="application/ld+json">{"a":1}</script><script>alert(1)</script><script src="/x.js"></script><script>
 two() </script>`)
	csp := contentSecurityPolicy(body, false, false)
	if got := strings.Count(csp, "'sha256-"); got != 2 {
		t.Fatalf("hashes = %d, want 2 (ld+json and src scripts excluded): %s", got, csp)
	}
	withGA := contentSecurityPolicy(body, true, false)
	if !strings.Contains(withGA, "script-src 'self' ") || !strings.Contains(withGA, "https://www.googletagmanager.com") || !strings.Contains(withGA, "analytics") {
		t.Fatalf("analytics CSP missing hosts: %s", withGA)
	}
	if strings.Contains(csp, "google") {
		t.Fatalf("default CSP names Google: %s", csp)
	}
}

func TestCSPAllowsCloudflareBeaconOnlyWhenConfigured(t *testing.T) {
	body := []byte("<script>x()</script>")
	if csp := contentSecurityPolicy(body, false, false); strings.Contains(csp, "cloudflare") {
		t.Fatalf("default CSP names Cloudflare: %s", csp)
	}
	csp := contentSecurityPolicy(body, false, true)
	scriptSrc := strings.SplitN(strings.SplitN(csp, "script-src ", 2)[1], ";", 2)[0]
	if !strings.Contains(scriptSrc, "https://static.cloudflareinsights.com") {
		t.Fatalf("script-src missing the beacon host: %s", scriptSrc)
	}
	if strings.Contains(csp, "connect-src 'self' https://") {
		t.Fatalf("beacon must not widen connect-src: %s", csp)
	}
}

func TestLoadConfigCloudflareWebAnalytics(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil || config.CloudflareWebAnalytics {
		t.Fatalf("default = %t, %v; want off", config.CloudflareWebAnalytics, err)
	}
	config, err = LoadConfig(writeConfig(t, `{"sources":["file:///data/proxies.txt"],"cloudflare_web_analytics":true}`))
	if err != nil || !config.CloudflareWebAnalytics {
		t.Fatalf("configured = %t, %v", config.CloudflareWebAnalytics, err)
	}
}
