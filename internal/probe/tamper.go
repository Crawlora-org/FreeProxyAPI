package probe

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

// Tamper detection for the anonymity echo check. Every echo request carries a
// random nonce; a proxy that answers 200 but rewrites the body (the nonce is
// not echoed back exactly) or injects response headers that a direct fetch of
// the same endpoint never returns is flagged as tampered. Tampered proxies are
// annotated, never delisted.

// EchoNonceParam is the query parameter carrying the per-request tamper nonce.
const EchoNonceParam = "fpa_nonce"

// echoBaselineSamples bounds how many recent direct fetches form the header
// baseline. The union over several fetches absorbs natural CDN header
// variance without letting a header seen once long ago stay trusted forever.
const echoBaselineSamples = 8

// benignProxyHeaders are response headers an honest HTTP proxy or cache may
// add; they never count as injection. Framing headers are included because a
// proxy may legitimately re-frame a body (chunked vs. Content-Length), and
// Accept-Ranges because caching proxies routinely advertise range support.
var benignProxyHeaders = map[string]struct{}{
	"accept-ranges":     {},
	"via":               {},
	"x-cache":           {},
	"x-cache-lookup":    {},
	"x-cache-hits":      {},
	"age":               {},
	"cache-status":      {},
	"proxy-connection":  {},
	"connection":        {},
	"keep-alive":        {},
	"proxy-agent":       {},
	"content-length":    {},
	"transfer-encoding": {},
}

// singletonEchoHeaders must not carry more values through a proxy than the
// direct baseline did (for example a second injected Server line).
var singletonEchoHeaders = []string{"Server", "Content-Type"}

// headerBaseline is the union of header names, and the largest value count
// of each singleton header, over recent direct fetches.
type headerBaseline struct {
	names  map[string]struct{}
	counts map[string]int
}

type echoBaseline struct {
	samples []http.Header
	// echoes reports whether the latest direct fetch honored echo semantics
	// (nonce in args and url). When false, tamper checks are skipped for this
	// endpoint so a non-echo service is never flagged wholesale.
	echoes bool
}

func (e *EchoClient) recordBaseline(echoURL string, headers http.Header, echoes bool) {
	e.baselineMu.Lock()
	defer e.baselineMu.Unlock()
	if e.baselines == nil {
		e.baselines = map[string]*echoBaseline{}
	}
	b := e.baselines[echoURL]
	if b == nil {
		b = &echoBaseline{}
		e.baselines[echoURL] = b
	}
	b.samples = append(b.samples, headers.Clone())
	if len(b.samples) > echoBaselineSamples {
		b.samples = b.samples[len(b.samples)-echoBaselineSamples:]
	}
	b.echoes = echoes
}

// tamperProfile reports whether tamper detection applies to echoURL and the
// header baseline to use (nil skips the header comparison). Endpoints with a
// direct observation follow what that fetch showed; before the first direct
// fetch only httpbin-compatible /get paths are checked, body only.
func (e *EchoClient) tamperProfile(echoURL string, requested *url.URL) (bool, *headerBaseline) {
	e.baselineMu.Lock()
	defer e.baselineMu.Unlock()
	b := e.baselines[echoURL]
	if b == nil {
		return strings.HasSuffix(requested.Path, "/get"), nil
	}
	if !b.echoes || len(b.samples) == 0 {
		return false, nil
	}
	return true, newHeaderBaseline(b.samples...)
}

func newHeaderBaseline(samples ...http.Header) *headerBaseline {
	out := &headerBaseline{names: map[string]struct{}{}, counts: map[string]int{}}
	for _, h := range samples {
		for name, values := range h {
			out.names[strings.ToLower(name)] = struct{}{}
			if n := len(values); n > out.counts[strings.ToLower(name)] {
				out.counts[strings.ToLower(name)] = n
			}
		}
	}
	return out
}

func newEchoNonce() string {
	var b [12]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// withEchoNonce returns echoURL with the tamper nonce query parameter set.
func withEchoNonce(echoURL, nonce string) (*url.URL, error) {
	parsed, err := url.Parse(echoURL)
	if err != nil {
		return nil, err
	}
	query := parsed.Query()
	query.Set(EchoNonceParam, nonce)
	parsed.RawQuery = query.Encode()
	return parsed, nil
}

// echoTamperReason returns "" for a clean echo response or a short reason
// code. baseline nil skips the header comparison.
func echoTamperReason(requested *url.URL, nonce string, headers http.Header, payload string, baseline *headerBaseline) string {
	if reason := echoBodyTamperReason(requested, nonce, payload); reason != "" {
		return reason
	}
	if baseline == nil {
		return ""
	}
	return headerTamperReason(headers, baseline)
}

// echoBodyTamperReason checks the httpbin-style echo contract: a JSON object
// whose args echo the nonce exactly and whose url names the requested host,
// path, and nonce. The url scheme is ignored because echo services behind TLS
// terminators may report https for a plain-HTTP request.
func echoBodyTamperReason(requested *url.URL, nonce, payload string) string {
	var doc struct {
		Args map[string]json.RawMessage `json:"args"`
		URL  *string                    `json:"url"`
	}
	if err := json.Unmarshal([]byte(payload), &doc); err != nil || doc.Args == nil {
		return "body"
	}
	if raw, ok := doc.Args[EchoNonceParam]; !ok || !echoedValueIs(raw, nonce) {
		return "nonce"
	}
	if doc.URL == nil {
		return "url"
	}
	echoed, err := url.Parse(strings.TrimSpace(*doc.URL))
	if err != nil || !strings.EqualFold(echoed.Hostname(), requested.Hostname()) ||
		normalizedPath(echoed.Path) != normalizedPath(requested.Path) ||
		echoed.Query().Get(EchoNonceParam) != nonce {
		return "url"
	}
	return ""
}

// echoedValueIs accepts both httpbin's single-string args and the monitor's
// list-valued args.
func echoedValueIs(raw json.RawMessage, want string) bool {
	var single string
	if json.Unmarshal(raw, &single) == nil {
		return single == want
	}
	var list []string
	return json.Unmarshal(raw, &list) == nil && len(list) == 1 && list[0] == want
}

func normalizedPath(p string) string {
	if p == "" {
		return "/"
	}
	return p
}

// headerTamperReason reports the first response header absent from the direct
// baseline (outside the benign allowlist), or a singleton header carrying more
// values than the baseline ever did.
func headerTamperReason(headers http.Header, baseline *headerBaseline) string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, strings.ToLower(name))
	}
	sort.Strings(names)
	for _, name := range names {
		if _, benign := benignProxyHeaders[name]; benign {
			continue
		}
		if _, known := baseline.names[name]; !known {
			return headerReason(name)
		}
	}
	for _, name := range singletonEchoHeaders {
		lower := strings.ToLower(name)
		if len(headers.Values(name)) > max(baseline.counts[lower], 1) {
			return headerReason(lower)
		}
	}
	return ""
}

func headerReason(name string) string {
	const maxName = 32
	if len(name) > maxName {
		name = name[:maxName]
	}
	return "header:" + name
}
