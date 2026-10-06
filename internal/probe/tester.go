// Package probe performs bounded HTTP proxy checks.
package probe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

const (
	// ExpectedStatusMin/Max bound what counts as a successful probe. Anything
	// outside is recorded as a failure without retrying. Redirects are
	// failures: a proxy answering with 3xx is typically a captive portal or
	// login page rather than a working tunnel to the target.
	ExpectedStatusMin = 200
	ExpectedStatusMax = 299

	// MaxResponseBytes bounds how much of the probe target's body is read.
	// The audit only needs headers; oversized bodies fail the check instead of
	// being drained silently.
	MaxResponseBytes = 64 << 10
)

type Result struct {
	OK            bool
	StatusCode    int
	Duration      time.Duration
	Error         string
	UploadBytes   int64
	DownloadBytes int64
	Anonymity     AnonymityResult
}

// Test sends one GET through a credential-free proxy to the configured target.
// It never follows redirects and bounds each request by timeout. HTTP,
// HTTPS, SOCKS4, SOCKS4A, SOCKS5, and SOCKS5H proxy schemes are supported;
// every tunnel handshake is time-bounded and refuses unsafe addresses.
func Test(parent context.Context, proxyURL, targetURL string, timeout time.Duration) Result {
	return runProbe(parent, proxyURL, targetURL, timeout, endpoint.DialPublicContext)
}

// TestExpectBody is Test with an additional content check: when expectedBody
// is non-empty, a 2xx response only passes if its bounded body, with trailing
// whitespace removed, equals expectedBody. This rejects captive portals, ad
// injectors, and caches that answer 2xx with unrelated content.
func TestExpectBody(parent context.Context, proxyURL, targetURL string, timeout time.Duration, expectedBody string) Result {
	return runProbeMode(parent, proxyURL, targetURL, timeout, "", false, expectedBody, endpoint.DialPublicContext)
}

// TestEcho sends one GET through a proxy to an echo endpoint and uses that
// response for both reachability and anonymity classification. The endpoint
// must return HTTP 200 and contain a caller-visible IP in a httpbin-style
// payload or another parseable form.
func TestEcho(parent context.Context, proxyURL, targetURL string, timeout time.Duration, originIP string) Result {
	return runProbeEcho(parent, proxyURL, targetURL, timeout, originIP, endpoint.DialPublicContext)
}

// publicHopDialer builds the production proxy-hop dialer with a TCP connect
// timeout. It is a variable only so tests can observe the plumbed timeout
// without dialing a real or unsafe address.
var publicHopDialer = func(connectTimeout time.Duration) dialFunc {
	return endpoint.PublicDialer{ConnectTimeout: connectTimeout}.DialContext
}

// TestWithConnectTimeout is Test with the TCP connect to the proxy hop bounded
// by connectTimeout (zero leaves only the request timeout). TLS to an https
// proxy and SOCKS handshakes keep their own limits.
func TestWithConnectTimeout(parent context.Context, proxyURL, targetURL string, timeout, connectTimeout time.Duration) Result {
	return runProbe(parent, proxyURL, targetURL, timeout, publicHopDialer(connectTimeout))
}

// TestEchoWithConnectTimeout is TestEcho with the proxy-hop TCP connect
// bounded by connectTimeout.
func TestEchoWithConnectTimeout(parent context.Context, proxyURL, targetURL string, timeout, connectTimeout time.Duration, originIP string) Result {
	return runProbeEcho(parent, proxyURL, targetURL, timeout, originIP, publicHopDialer(connectTimeout))
}

// TestExpectBodyWithConnectTimeout is the production standard-mode probe: the
// proxy-hop TCP connect is bounded by connectTimeout and, when expectedBody is
// non-empty, the trimmed response body must equal it (see TestExpectBody).
func TestExpectBodyWithConnectTimeout(parent context.Context, proxyURL, targetURL string, timeout, connectTimeout time.Duration, expectedBody string) Result {
	return runProbeMode(parent, proxyURL, targetURL, timeout, "", false, expectedBody, publicHopDialer(connectTimeout))
}

func runProbe(parent context.Context, proxyURL, targetURL string, timeout time.Duration, dialPublic dialFunc) (result Result) {
	return runProbeMode(parent, proxyURL, targetURL, timeout, "", false, "", dialPublic)
}

func runProbeEcho(parent context.Context, proxyURL, targetURL string, timeout time.Duration, originIP string, dialPublic dialFunc) (result Result) {
	return runProbeMode(parent, proxyURL, targetURL, timeout, originIP, true, "", dialPublic)
}

// cacheBusterParam is the query parameter appended to standard-mode probe
// URLs so intermediary caches cannot answer on behalf of a dead upstream.
const cacheBusterParam = "_fpa"

// withCacheBuster appends a random cacheBusterParam value to rawURL while
// preserving any existing query parameters.
func withCacheBuster(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	value := cacheBusterParam + "=" + hex.EncodeToString(token[:])
	if parsed.RawQuery == "" {
		parsed.RawQuery = value
	} else {
		parsed.RawQuery += "&" + value
	}
	return parsed.String(), nil
}

func runProbeMode(parent context.Context, proxyURL, targetURL string, timeout time.Duration, originIP string, echo bool, expectedBody string, dialPublic dialFunc) (result Result) {
	started := time.Now()
	traffic := &probeTraffic{}
	defer func() {
		result.Duration = time.Since(started)
		result.UploadBytes = traffic.upload.Load()
		result.DownloadBytes = traffic.download.Load()
	}()
	proxy, err := url.Parse(proxyURL)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	scheme := strings.ToLower(proxy.Scheme)
	if proxy.Host == "" {
		result.Error = "proxy URL has no host"
		return result
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	transport := &http.Transport{
		TLSHandshakeTimeout:   timeout / 2,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
	}
	dialCounted := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialPublic(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: conn, traffic: traffic}, nil
	}
	switch scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(proxy)
		transport.DialContext = dialCounted
	case "socks4", "socks4a":
		transport.DialContext = socksDialContext(dialCounted, proxy.Hostname(), proxy.Port(), 4, scheme == "socks4a")
	case "socks5", "socks5h":
		transport.DialContext = socksDialContext(dialCounted, proxy.Hostname(), proxy.Port(), 5, scheme == "socks5h")
	default:
		result.Error = fmt.Sprintf("unsupported proxy scheme %q", scheme)
		return result
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	requestURL := targetURL
	if !echo {
		busted, err := withCacheBuster(targetURL)
		if err != nil {
			result.Error = err.Error()
			return result
		}
		requestURL = busted
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	request.Header.Set("User-Agent", "FreeProxyAPI/0.1 (proxy-probe)")
	if !echo {
		request.Header.Set("Cache-Control", "no-cache")
		request.Header.Set("Pragma", "no-cache")
	}
	response, err := client.Do(request)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer response.Body.Close()
	result.StatusCode = response.StatusCode
	if echo && response.StatusCode != http.StatusOK {
		result.Error = fmt.Sprintf("echo endpoint returned unexpected HTTP status %d", response.StatusCode)
		return result
	}
	if !echo && (response.StatusCode < ExpectedStatusMin || response.StatusCode > ExpectedStatusMax) {
		result.Error = fmt.Sprintf("unexpected HTTP status %d", response.StatusCode)
		return result
	}
	drained, err := readBoundedResponse(ctx, response.Body)
	if err != nil {
		result.Error = fmt.Sprintf("read probe response: %v", err)
		return result
	}
	if echo {
		result.Anonymity = classifyEchoResponse(response.StatusCode, response.Header, string(drained), originIP)
		if result.Anonymity.Class == "" {
			result.Error = "echo response contained no caller IP"
			return result
		}
	} else if expectedBody != "" && strings.TrimRight(string(drained), " \t\r\n") != expectedBody {
		result.Error = "unexpected response body"
		return result
	}
	result.OK = true
	return result
}

func readBoundedResponse(ctx context.Context, body io.Reader) ([]byte, error) {
	drained, err := io.ReadAll(io.LimitReader(body, MaxResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if int64(len(drained)) > MaxResponseBytes {
		return nil, fmt.Errorf("%s-byte response limit exceeded", strconv.Itoa(MaxResponseBytes))
	}
	return drained, nil
}

type probeTraffic struct {
	upload   atomic.Int64
	download atomic.Int64
}

type countingConn struct {
	net.Conn
	traffic *probeTraffic
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.traffic.download.Add(int64(n))
	}
	return n, err
}

func (c *countingConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if n > 0 {
		c.traffic.upload.Add(int64(n))
	}
	return n, err
}
