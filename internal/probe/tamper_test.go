package probe

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

// cloudflareEchoHeaders mirrors the header names measured on direct fetches
// of the live /get endpoint.
func cloudflareEchoHeaders() http.Header {
	h := http.Header{}
	for name, value := range map[string]string{
		"Alt-Svc": `h3=":443"; ma=86400`, "Cache-Control": "no-store", "Cf-Cache-Status": "DYNAMIC",
		"Cf-Ray": "a3b12b4f6fed3eb0-HKG", "Connection": "keep-alive", "Content-Length": "154",
		"Content-Type": "application/json", "Date": "Mon, 14 Sep 2026 17:39:07 GMT", "Nel": "{}",
		"Report-To": "{}", "Server": "cloudflare", "X-Content-Type-Options": "nosniff",
	} {
		h.Set(name, value)
	}
	return h
}

func TestEchoTamperReason(t *testing.T) {
	const nonce = "0123456789abcdef01234567"
	requested, err := withEchoNonce("http://freeproxyapi.crawlora.net/get", nonce)
	if err != nil {
		t.Fatal(err)
	}
	echoBody := func(args, rawURL string) string {
		return `{"args":` + args + `,"headers":{},"origin":"198.51.100.7","url":"` + rawURL + `"}`
	}
	cleanBody := echoBody(`{"fpa_nonce":["`+nonce+`"]}`, "http://freeproxyapi.crawlora.net/get?fpa_nonce="+nonce)
	withHeaders := func(mutate func(http.Header)) http.Header {
		h := cloudflareEchoHeaders()
		mutate(h)
		return h
	}
	baseline := newHeaderBaseline(cloudflareEchoHeaders())

	cases := []struct {
		name     string
		headers  http.Header
		body     string
		baseline *headerBaseline
		want     string
	}{
		{name: "clean echo", headers: cloudflareEchoHeaders(), body: cleanBody, baseline: baseline},
		{
			name:    "httpbin string args and https url",
			headers: cloudflareEchoHeaders(), baseline: baseline,
			body: echoBody(`{"fpa_nonce":"`+nonce+`"}`, "https://FreeProxyAPI.crawlora.net/get?fpa_nonce="+nonce),
		},
		{
			name:    "wrong nonce",
			headers: cloudflareEchoHeaders(), baseline: baseline, want: "nonce",
			body: echoBody(`{"fpa_nonce":["ffffffffffffffffffffffff"]}`, "http://freeproxyapi.crawlora.net/get?fpa_nonce="+nonce),
		},
		{name: "non-JSON body", headers: cloudflareEchoHeaders(), baseline: baseline, body: "<html>ok</html>", want: "body"},
		{name: "appended content", headers: cloudflareEchoHeaders(), baseline: baseline, body: cleanBody + "<script></script>", want: "body"},
		{
			name:    "url path rewritten",
			headers: cloudflareEchoHeaders(), baseline: baseline, want: "url",
			body: echoBody(`{"fpa_nonce":["`+nonce+`"]}`, "http://freeproxyapi.crawlora.net/other?fpa_nonce="+nonce),
		},
		{
			name:    "injected header",
			headers: withHeaders(func(h http.Header) { h.Set("X-Ad-Inject", "1") }),
			body:    cleanBody, baseline: baseline, want: "header:x-ad-inject",
		},
		{
			name:    "duplicate server header",
			headers: withHeaders(func(h http.Header) { h.Add("Server", "josephwcarrillo.actor: always") }),
			body:    cleanBody, baseline: baseline, want: "header:server",
		},
		{
			name: "benign proxy headers",
			headers: withHeaders(func(h http.Header) {
				h.Set("Via", "1.1 squid")
				h.Set("X-Cache", "MISS from squid")
				h.Set("X-Cache-Lookup", "MISS from squid:3128")
				h.Set("Age", "0")
				h.Set("Proxy-Connection", "keep-alive")
				h.Set("Accept-Ranges", "bytes")
			}),
			body: cleanBody, baseline: baseline,
		},
		{
			name:    "no baseline skips header check",
			headers: withHeaders(func(h http.Header) { h.Add("Server", "josephwcarrillo.actor") }),
			body:    cleanBody,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := echoTamperReason(requested, nonce, tc.headers, tc.body, tc.baseline); got != tc.want {
				t.Fatalf("echoTamperReason = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestHeaderBaselineIsUnionOfRecentSamples(t *testing.T) {
	first := cloudflareEchoHeaders()
	second := cloudflareEchoHeaders()
	second.Set("Speculation-Rules", "x")
	baseline := newHeaderBaseline(first, second)
	proxied := cloudflareEchoHeaders()
	proxied.Set("Speculation-Rules", "x")
	if got := headerTamperReason(proxied, baseline); got != "" {
		t.Fatalf("header seen in an earlier direct fetch flagged: %q", got)
	}

	client := &EchoClient{}
	requested, _ := url.Parse("http://echo.example/get")
	for i := 0; i < echoBaselineSamples+1; i++ {
		h := cloudflareEchoHeaders()
		if i == 0 {
			h.Set("X-Old", "1")
		}
		client.recordBaseline("http://echo.example/get", h, true)
	}
	eligible, current := client.tamperProfile("http://echo.example/get", requested)
	if !eligible || current == nil {
		t.Fatal("recorded echo endpoint must be eligible with a baseline")
	}
	if _, ok := current.names["x-old"]; ok {
		t.Fatal("baseline must forget samples older than echoBaselineSamples")
	}
	client.recordBaseline("http://echo.example/get", cloudflareEchoHeaders(), false)
	if eligible, _ := client.tamperProfile("http://echo.example/get", requested); eligible {
		t.Fatal("endpoint whose direct fetch does not echo the nonce must be skipped")
	}
	plain, _ := url.Parse("http://ip.example/")
	if eligible, _ := (&EchoClient{}).tamperProfile("http://ip.example/", plain); eligible {
		t.Fatal("unobserved non-/get endpoint must be skipped")
	}
}

// monitorStyleEcho replicates the monitor's writeEcho payload shape
// (internal/monitor/health.go): args, headers, origin, and url built from the
// request host and URI.
func monitorStyleEcho() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Server", "cloudflare")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"args":    r.URL.Query(),
			"headers": map[string]string{},
			"origin":  "198.51.100.7",
			"url":     "http://" + r.Host + r.URL.RequestURI(),
		})
	})
}

// forwardProxy is a minimal absolute-URI HTTP proxy that lets mutate rewrite
// the upstream response headers and body before relaying them.
func forwardProxy(t *testing.T, status int, mutate func(http.Header, []byte) []byte) *httptest.Server {
	t.Helper()
	upstream := &http.Transport{Proxy: nil}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		out, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		out.Header = r.Header.Clone()
		resp, err := upstream.RoundTrip(out)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		for name, values := range resp.Header {
			if strings.EqualFold(name, "Content-Length") {
				continue
			}
			w.Header()[name] = values
		}
		if mutate != nil {
			body = mutate(w.Header(), body)
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(server.Close)
	t.Cleanup(upstream.CloseIdleConnections)
	return server
}

func TestEchoClientCheckDetectsTamperingEndToEnd(t *testing.T) {
	echo := httptest.NewServer(monitorStyleEcho())
	defer echo.Close()
	echoURL := echo.URL + "/get"

	passThrough := forwardProxy(t, http.StatusOK, nil)
	injectServer := forwardProxy(t, http.StatusOK, func(h http.Header, body []byte) []byte {
		h.Add("Server", "josephwcarrillo.actor")
		h.Add("Server", "josephwcarrillo.actor: always")
		return body
	})
	rewriteBody := forwardProxy(t, http.StatusOK, func(_ http.Header, body []byte) []byte {
		return []byte(strings.Replace(string(body), "fpa_nonce", "fpa_nonc3", -1))
	})
	blockPage := forwardProxy(t, http.StatusForbidden, func(h http.Header, _ []byte) []byte {
		h.Set("Server", "Beaver")
		h.Set("Content-Type", "text/html")
		return []byte("<html>blocked</html>")
	})

	client := newEchoClient(time.Second, plainDial)
	ctx := context.Background()

	// Before any direct fetch the /get endpoint is body-checked only, so the
	// injected Server header is not yet visible.
	if got := client.Check(ctx, injectServer.URL, echoURL, "203.0.113.8", time.Second); !got.TamperChecked || got.Tampered {
		t.Fatalf("no-baseline check = %+v, want body-only clean", got)
	}
	if _, err := client.RefreshRealIP(ctx, echoURL); err != nil {
		t.Fatalf("direct echo fetch: %v", err)
	}

	cases := []struct {
		name        string
		proxy       string
		wantChecked bool
		wantTamper  bool
		wantReason  string
		wantClass   bool
	}{
		{name: "pass-through proxy", proxy: passThrough.URL, wantChecked: true, wantClass: true},
		{name: "server injection", proxy: injectServer.URL, wantChecked: true, wantTamper: true, wantReason: "header:server", wantClass: true},
		{name: "body rewrite", proxy: rewriteBody.URL, wantChecked: true, wantTamper: true, wantReason: "nonce", wantClass: true},
		{name: "403 block page", proxy: blockPage.URL},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := client.Check(ctx, tc.proxy, echoURL, "203.0.113.8", time.Second)
			if got.TamperChecked != tc.wantChecked || got.Tampered != tc.wantTamper || got.TamperReason != tc.wantReason {
				t.Fatalf("Check = %+v", got)
			}
			if (got.Class != "") != tc.wantClass {
				t.Fatalf("classification = %q, want classified=%t", got.Class, tc.wantClass)
			}
		})
	}
}
