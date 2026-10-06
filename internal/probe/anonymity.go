package probe

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

// Anonymity classes recorded for a proxy candidate.
const (
	AnonymityElite       = "elite"       // exit IP differs from ours and origin does not leak
	AnonymityAnonymous   = "anonymous"   // exit IP differs but origin leaks via headers
	AnonymityTransparent = "transparent" // our own address is visible as the exit or leaked outright
)

var ipv4Re = regexp.MustCompile(`\b((?:\d{1,3}\.){3}\d{1,3})\b`)

// extractIP finds the caller-visible address in an echo payload. It prefers
// the structured JSON "origin" field (httpbin-style), then any IPv4 literal,
// then any IPv6 literal. IPv6 support matters for exits that only announce
// AAAA connectivity.
func extractIP(payload string) netip.Addr {
	var doc struct {
		Origin string `json:"origin"`
	}
	if err := json.Unmarshal([]byte(payload), &doc); err == nil && doc.Origin != "" {
		if addr, err := netip.ParseAddr(strings.TrimSpace(doc.Origin)); err == nil {
			return addr.Unmap()
		}
	}
	if m := ipv4Re.FindStringSubmatch(payload); len(m) > 1 {
		if addr, err := netip.ParseAddr(m[1]); err == nil {
			return addr
		}
	}
	for _, token := range strings.FieldsFunc(payload, func(r rune) bool {
		switch r {
		case ' ', '\t', '\n', '\r', '"', ',', '<', '>', '(', ')', '[', ']':
			return true
		}
		return false
	}) {
		token = strings.Trim(token, ".")
		if strings.ContainsAny(token, ":") {
			if addr, err := netip.ParseAddr(token); err == nil && addr.IsValid() {
				return addr.Unmap()
			}
		}
	}
	return netip.Addr{}
}

// AnonymityResult summarizes one echo-endpoint observation made through a
// proxy. A zero Class means the observation failed and callers must not
// record it as a class at all.
type AnonymityResult struct {
	Class  string
	ExitIP string

	// TamperChecked reports that the echo request returned 200 and was
	// compared against the expected echo semantics. Tampered is meaningful
	// only when TamperChecked is true; TamperReason is a short code such as
	// "body", "nonce", "url", or "header:server".
	TamperChecked bool
	Tampered      bool
	TamperReason  string
}

// EchoClient classifies proxies against an operator-configured echo endpoint.
// The endpoint URL is configured through anonymity_check_url.
type EchoClient struct {
	direct     *http.Client
	dialPublic dialFunc

	// baselines holds recent direct-fetch observations per echo URL; see
	// echoBaseline.
	baselineMu sync.Mutex
	baselines  map[string]*echoBaseline
}

func NewEchoClient(timeout time.Duration) *EchoClient {
	return newEchoClient(timeout, endpoint.DialPublicContext)
}

func newEchoClient(timeout time.Duration, dialPublic dialFunc) *EchoClient {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	if dialPublic == nil {
		dialPublic = endpoint.DialPublicContext
	}
	return &EchoClient{
		dialPublic: dialPublic,
		direct: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:               nil,
				DialContext:         dialPublic,
				TLSHandshakeTimeout: timeout / 2,
				DisableKeepAlives:   true,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

// RefreshRealIP learns the monitor's own direct-egress address by fetching the
// echo endpoint without a proxy. Best-effort: until it succeeds,
// classification degrades to header-leak detection only.
//
// The same direct fetch (with a tamper nonce) also refreshes the header
// baseline and echo-semantics profile used by tamper detection in Check.
func (e *EchoClient) RefreshRealIP(ctx context.Context, echoURL string) (string, error) {
	nonce := newEchoNonce()
	requested, err := withEchoNonce(echoURL, nonce)
	if err != nil {
		return "", err
	}
	body, headers, err := e.fetchDirect(ctx, requested.String())
	if err != nil {
		return "", err
	}
	e.recordBaseline(echoURL, headers, echoBodyTamperReason(requested, nonce, body) == "")
	addr := extractIP(body)
	if !addr.IsValid() {
		return "", fmt.Errorf("echo response contained no IP address")
	}
	return addr.String(), nil
}

// Check routes one GET to the echo endpoint through proxyURL and classifies
// what the far side saw relative to the provided origin address.
func (e *EchoClient) Check(parent context.Context, proxyURL, echoURL, originIP string, timeout time.Duration) AnonymityResult {
	parsed, err := url.Parse(proxyURL)
	if err != nil || parsed.Host == "" {
		return AnonymityResult{}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	transport := &http.Transport{
		TLSHandshakeTimeout:   timeout / 2,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
	}
	dialPublic := e.dialPublic
	if dialPublic == nil {
		dialPublic = endpoint.DialPublicContext
	}
	scheme := strings.ToLower(parsed.Scheme)
	switch scheme {
	case "http", "https":
		transport.Proxy = http.ProxyURL(parsed)
		transport.DialContext = dialPublic
	case "socks4", "socks4a":
		transport.DialContext = socksDialContext(dialPublic, parsed.Hostname(), parsed.Port(), 4, scheme == "socks4a")
	case "socks5", "socks5h":
		transport.DialContext = socksDialContext(dialPublic, parsed.Hostname(), parsed.Port(), 5, scheme == "socks5h")
	default:
		return AnonymityResult{}
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	nonce := newEchoNonce()
	requested, err := withEchoNonce(echoURL, nonce)
	if err != nil {
		return AnonymityResult{}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, requested.String(), nil)
	if err != nil {
		return AnonymityResult{}
	}
	req.Header.Set("User-Agent", "FreeProxyAPI/0.1 (anonymity-probe)")
	resp, err := client.Do(req)
	if err != nil {
		return AnonymityResult{}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// Block pages and captive portals fail here and are never recorded
		// as tampering.
		return AnonymityResult{}
	}
	bounded, err := readBoundedResponse(ctx, resp.Body)
	if err != nil {
		return AnonymityResult{}
	}
	result := classifyEchoResponse(resp.StatusCode, resp.Header, string(bounded), originIP)
	if eligible, baseline := e.tamperProfile(echoURL, requested); eligible {
		reason := echoTamperReason(requested, nonce, resp.Header, string(bounded), baseline)
		result.TamperChecked = true
		result.Tampered = reason != ""
		result.TamperReason = reason
	}
	return result
}

func classifyEchoResponse(status int, headers http.Header, payload, originIP string) AnonymityResult {
	if status != http.StatusOK {
		return AnonymityResult{}
	}
	exitAddr := extractIP(payload)
	if !exitAddr.IsValid() {
		return AnonymityResult{}
	}
	exitIP := exitAddr.String()

	haystack := strings.ToLower(dumpHeaders(headers) + "\n" + payload)
	originAddr := netip.Addr{}
	if originIP != "" {
		if parsed, perr := netip.ParseAddr(strings.TrimSpace(originIP)); perr == nil {
			originAddr = parsed.Unmap()
		}
	}
	leakHeaders := containsEchoHeader(haystack, "via") || containsEchoHeader(haystack, "forwarded") ||
		containsEchoHeader(haystack, "x-forwarded-for") || containsEchoHeader(haystack, "x-real-ip")
	switch {
	case !originAddr.IsValid():
		// No known origin: fall back to header-presence heuristics only.
		if leakHeaders {
			return AnonymityResult{Class: AnonymityAnonymous, ExitIP: exitIP}
		}
		return AnonymityResult{Class: AnonymityElite, ExitIP: exitIP}
	case originAddr.Compare(exitAddr) == 0 || containsAddr(haystack, originAddr):
		// Our address is either the exit itself or forwarded to the target in
		// X-Forwarded-For, Forwarded, Via, or a similar header.
		return AnonymityResult{Class: AnonymityTransparent, ExitIP: exitIP}
	case leakHeaders:
		return AnonymityResult{Class: AnonymityAnonymous, ExitIP: exitIP}
	default:
		return AnonymityResult{Class: AnonymityElite, ExitIP: exitIP}
	}
}

// containsAddr reports whether addr appears in the lowercased haystack as a
// whole address, so 1.2.3.4 does not match inside 11.2.3.45 while forms such
// as "for=1.2.3.4:5678" and "[2001:db8::1]" still match.
func containsAddr(haystack string, addr netip.Addr) bool {
	needle := strings.ToLower(addr.String())
	for offset := 0; offset < len(haystack); {
		i := strings.Index(haystack[offset:], needle)
		if i < 0 {
			return false
		}
		start := offset + i
		end := start + len(needle)
		if !addrContinues(haystack, start-1, -1, addr.Is4()) && !addrContinues(haystack, end, 1, addr.Is4()) {
			return true
		}
		offset = start + 1
	}
	return false
}

// addrContinues reports whether the byte at i extends the address literal
// that ends (dir=1) or starts (dir=-1) next to it.
func addrContinues(s string, i, dir int, v4 bool) bool {
	if i < 0 || i >= len(s) {
		return false
	}
	c := s[i]
	isDigit := func(b byte) bool { return b >= '0' && b <= '9' }
	if v4 {
		if isDigit(c) {
			return true
		}
		// A dot only continues the address when another octet follows it.
		next := i + dir
		return c == '.' && next >= 0 && next < len(s) && isDigit(s[next])
	}
	return isDigit(c) || (c >= 'a' && c <= 'f') || c == ':'
}

func containsEchoHeader(haystack, name string) bool {
	name = strings.ToLower(name)
	return strings.Contains(haystack, name+":") || strings.Contains(haystack, "\""+name+"\"")
}

func (e *EchoClient) fetchDirect(ctx context.Context, url string) (string, http.Header, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", nil, err
	}
	req.Header.Set("User-Agent", "FreeProxyAPI/0.1 (anonymity-self-check)")
	resp, err := e.direct.Do(req)
	if err != nil {
		return "", nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", nil, fmt.Errorf("echo endpoint returned %s", resp.Status)
	}
	bounded, err := readBoundedResponse(ctx, resp.Body)
	if err != nil {
		return "", nil, err
	}
	return string(bounded), resp.Header, nil
}

func dumpHeaders(h http.Header) string {
	var b strings.Builder
	for name, values := range h {
		for _, v := range values {
			b.WriteString(strings.ToLower(name))
			b.WriteString(": ")
			b.WriteString(v)
			b.WriteByte('\n')
		}
	}
	return b.String()
}
