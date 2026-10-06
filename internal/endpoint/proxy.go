// Package endpoint parses proxy endpoints without dialing them.
package endpoint

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Proxy is a normalized, credential-free proxy endpoint.
type Proxy struct {
	Scheme string
	Host   string
	Port   uint16
}

// String returns the canonical credential-free endpoint form.
func (p Proxy) String() string {
	// JoinHostPort brackets IPv6 hosts; without them the URL does not parse.
	return p.Scheme + "://" + net.JoinHostPort(p.Host, strconv.Itoa(int(p.Port)))
}

// ParseFeed reads a line-delimited proxy inventory. Invalid records are counted
// rather than returned, so callers can safely report aggregate results.
func ParseFeed(r io.Reader, defaultScheme string) (accepted, rejected int, byScheme map[string]int, err error) {
	proxies, rejected, err := ParseFeedProxies(r, defaultScheme)
	if err != nil {
		return 0, rejected, nil, err
	}
	byScheme = make(map[string]int)
	for _, proxy := range proxies {
		byScheme[proxy.Scheme]++
	}
	return len(proxies), rejected, byScheme, nil
}

// ParseFeedProxies reads a local or fetched line-delimited inventory. Callers
// should retain endpoint strings only in protected state, never logs.
func ParseFeedProxies(r io.Reader, defaultScheme string) (proxies []Proxy, rejected int, err error) {
	rejected, err = VisitFeedProxies(r, defaultScheme, func(proxy Proxy) error {
		proxies = append(proxies, proxy)
		return nil
	})
	return proxies, rejected, err
}

// VisitFeedProxies parses a feed incrementally and invokes visit for each
// accepted endpoint. It keeps parsing memory bounded for very large text
// inventories while retaining the existing aggregate rejection count.
func VisitFeedProxies(r io.Reader, defaultScheme string, visit func(Proxy) error) (rejected int, err error) {
	if r == nil {
		return 0, fmt.Errorf("proxy feed reader is required")
	}
	if visit == nil {
		return 0, fmt.Errorf("proxy feed visitor is required")
	}

	reader := bufio.NewReader(r)
	first, err := firstNonSpaceByte(reader)
	if err != nil {
		if err == io.EOF {
			return 0, nil
		}
		return 0, fmt.Errorf("read proxy feed: %w", err)
	}
	if first == '{' || first == '[' {
		return visitJSONFeed(reader, defaultScheme, visit)
	}
	if first == '<' {
		return visitHTMLFeed(reader, defaultScheme, visit)
	}

	var line []byte
	for {
		var oversized bool
		line, oversized, err = readFeedLine(reader, line[:0])
		if len(line) > 0 || oversized {
			// A partial final line is only trusted at a clean EOF; a read error
			// (including the source byte limit) stops before visiting it.
			if err != nil && err != io.EOF {
				return rejected, fmt.Errorf("read proxy feed: %w", err)
			}
			if oversized {
				rejected++
			} else if record := strings.TrimSpace(string(line)); record != "" && !strings.HasPrefix(record, "#") {
				proxy, parseErr := ParseProxy(normalizeFeedRecord(record), defaultScheme)
				if parseErr != nil {
					rejected++
				} else if visitErr := visit(proxy); visitErr != nil {
					return rejected, visitErr
				}
			}
		}
		if err == io.EOF {
			return rejected, nil
		}
		if err != nil {
			return rejected, fmt.Errorf("read proxy feed: %w", err)
		}
	}
}

var structuredHTMLProxyRow = regexp.MustCompile(`(?is)<tr\b[^>]*data-type=["'](HTTP|HTTPS|SOCKS4|SOCKS5)["'][^>]*>.*?data-pp=["']([^"']+)["']`)

// genericHTMLProxyEndpoint finds bare IPv4 host:port records embedded in
// ordinary directory markup. The surrounding non-digit guards prevent
// accepting a substring of a malformed longer number; ParseProxy still
// applies the public-address and port validation.
var genericHTMLProxyEndpoint = regexp.MustCompile(`(?:^|[^0-9])((?:[0-9]{1,3}\.){3}[0-9]{1,3}:[0-9]{1,5})(?:[^0-9]|$)`)

// visitHTMLFeed accepts structured rows published by public proxy
// directories. The protocol comes from each row's data-type, never from a
// page-wide default. When a directory publishes bare records in ordinary
// markup, defaultScheme supplies the protocol. A document without recognized
// rows or endpoints is an error so an HTML error page cannot count as a
// healthy source refresh.
func visitHTMLFeed(r io.Reader, defaultScheme string, visit func(Proxy) error) (int, error) {
	payload, err := io.ReadAll(r)
	if err != nil {
		return 0, fmt.Errorf("read HTML proxy feed: %w", err)
	}
	matches := structuredHTMLProxyRow.FindAllSubmatch(payload, -1)
	rejected := 0
	for _, match := range matches {
		scheme := strings.ToLower(string(match[1]))
		proxy, parseErr := ParseProxy(string(match[2]), scheme)
		if parseErr != nil {
			rejected++
			continue
		}
		if err := visit(proxy); err != nil {
			return rejected, err
		}
	}
	if len(matches) > 0 {
		return rejected, nil
	}

	// Some live directories render a plain host:port table without protocol
	// metadata. Keep this fallback deliberately narrow: only IPv4 host:port
	// tokens are considered, and every token still passes ParseProxy.
	endpoints := genericHTMLProxyEndpoint.FindAllSubmatch(payload, -1)
	if len(endpoints) == 0 {
		return 0, fmt.Errorf("HTML proxy feed has no recognized proxy rows or endpoints")
	}
	for _, match := range endpoints {
		proxy, parseErr := ParseProxy(string(match[1]), defaultScheme)
		if parseErr != nil {
			rejected++
			continue
		}
		if err := visit(proxy); err != nil {
			return rejected, err
		}
	}
	return rejected, nil
}

// MaxFeedLineBytes bounds a single text feed record. Longer lines are counted
// as rejected records and skipped instead of aborting the whole feed.
const MaxFeedLineBytes = 1 << 20

// readFeedLine appends the next newline-terminated line (without the newline)
// to buf. When the line exceeds MaxFeedLineBytes, the rest of it is discarded
// and oversized is true. err is nil after a complete line, io.EOF at the end
// of input, or the underlying read error.
func readFeedLine(r *bufio.Reader, buf []byte) (line []byte, oversized bool, err error) {
	line = buf
	for {
		chunk, readErr := r.ReadSlice('\n')
		if !oversized {
			if len(line)+len(chunk) > MaxFeedLineBytes+1 { // +1 allows the newline itself
				oversized = true
				line = line[:0]
			} else {
				line = append(line, chunk...)
			}
		}
		switch readErr {
		case nil:
			line = bytes.TrimSuffix(line, []byte{'\n'})
			return line, oversized || len(line) > MaxFeedLineBytes, nil
		case bufio.ErrBufferFull:
			continue
		default:
			return line, oversized || len(line) > MaxFeedLineBytes, readErr
		}
	}
}

// normalizeFeedRecord extracts an endpoint from the two common annotated
// formats used by public proxy inventories: an endpoint followed by whitespace
// metadata, and an IPv4 endpoint followed by a colon-delimited country name.
// It also accepts the protocol://IPv4:port:country form used by some checked
// inventories. It deliberately leaves IPv6, hostnames, and all other shapes
// untouched so malformed records still go through ParseProxy's validation.
func normalizeFeedRecord(raw string) string {
	if fieldEnd := strings.IndexAny(raw, " \t"); fieldEnd >= 0 {
		raw = raw[:fieldEnd]
	}
	if schemeEnd := strings.Index(raw, "://"); schemeEnd >= 0 {
		prefix := raw[:schemeEnd+3]
		endpoint := raw[schemeEnd+3:]
		if strings.Count(endpoint, ":") != 2 {
			return raw
		}
		parts := strings.SplitN(endpoint, ":", 3)
		if _, err := netip.ParseAddr(parts[0]); err != nil {
			return raw
		}
		return prefix + parts[0] + ":" + parts[1]
	}
	if strings.Count(raw, ":") != 2 {
		return raw
	}
	parts := strings.SplitN(raw, ":", 3)
	if _, err := netip.ParseAddr(parts[0]); err != nil {
		return raw
	}
	return parts[0] + ":" + parts[1]
}

func firstNonSpaceByte(r *bufio.Reader) (byte, error) {
	for {
		value, err := r.ReadByte()
		if err != nil {
			return 0, err
		}
		if value == ' ' || value == '\t' || value == '\r' || value == '\n' {
			continue
		}
		if err := r.UnreadByte(); err != nil {
			return 0, err
		}
		return value, nil
	}
}

func visitJSONFeed(r io.Reader, defaultScheme string, visit func(Proxy) error) (int, error) {
	// JSON inventories in the configured set are paginated metadata feeds and
	// remain small; keep their established parser semantics while text feeds
	// use the incremental visitor above.
	proxies, rejected, err := parseJSONFeed(r, defaultScheme)
	if err != nil {
		return rejected, err
	}
	for _, proxy := range proxies {
		if err := visit(proxy); err != nil {
			return rejected, err
		}
	}
	return rejected, nil
}

// parseJSONFeed accepts paginated envelopes published by Geonode, ProxyLister,
// and equivalent providers, plus the array-of-records shape used by other
// machine-readable feeds.
// Structured records are normalized into the same credential-free endpoints
// as line-oriented feeds. A payload that is not valid JSON, or an envelope
// without a recognized record array, is an error rather than a rejected
// record, so a broken 200 response is not reported as a successful refresh.
func parseJSONFeed(r io.Reader, defaultScheme string) ([]Proxy, int, error) {
	payload, err := io.ReadAll(r)
	if err != nil {
		return nil, 0, fmt.Errorf("read JSON proxy feed: %w", err)
	}
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, 0, fmt.Errorf("JSON proxy feed is empty")
	}

	var records []json.RawMessage
	switch trimmed[0] {
	case '[':
		if err := json.Unmarshal(trimmed, &records); err != nil {
			return nil, 0, fmt.Errorf("decode JSON proxy feed array: %w", err)
		}
	case '{':
		var envelope struct {
			Data    json.RawMessage `json:"data"`
			Proxies json.RawMessage `json:"proxies"`
			Results json.RawMessage `json:"results"`
		}
		if err := json.Unmarshal(trimmed, &envelope); err != nil {
			return nil, 0, fmt.Errorf("decode JSON proxy feed envelope: %w", err)
		}
		payload := envelope.Data
		if len(payload) == 0 {
			payload = envelope.Proxies
		}
		if len(payload) == 0 {
			payload = envelope.Results
		}
		if len(payload) == 0 {
			return nil, 0, fmt.Errorf("JSON proxy feed envelope has no data, proxies, or results array")
		}
		if err := json.Unmarshal(payload, &records); err != nil {
			return nil, 0, fmt.Errorf("decode JSON proxy feed records: %w", err)
		}
	default:
		return nil, 0, fmt.Errorf("JSON proxy feed must be an array or an object envelope")
	}

	proxies := make([]Proxy, 0, len(records))
	rejected := 0
	for _, raw := range records {
		var endpoint string
		if err := json.Unmarshal(raw, &endpoint); err == nil {
			proxy, err := ParseProxy(endpoint, defaultScheme)
			if err != nil {
				rejected++
				continue
			}
			proxies = append(proxies, proxy)
			continue
		}

		var record struct {
			IP        string          `json:"ip"`
			IPAddress string          `json:"ip_address"`
			Addr      string          `json:"addr"`
			Host      string          `json:"host"`
			Port      json.RawMessage `json:"port"`
			Protocol  string          `json:"protocol"`
			Protocols []string        `json:"protocols"`
		}
		if err := json.Unmarshal(raw, &record); err != nil {
			rejected++
			continue
		}
		ip := strings.TrimSpace(record.IP)
		if ip == "" {
			ip = strings.TrimSpace(record.IPAddress)
		}
		if ip == "" {
			ip = strings.TrimSpace(record.Addr)
		}
		if ip == "" {
			ip = strings.TrimSpace(record.Host)
		}
		port := jsonPort(record.Port)
		if ip == "" || port == "" {
			rejected++
			continue
		}
		schemes := append([]string(nil), record.Protocols...)
		if len(schemes) == 0 && strings.TrimSpace(record.Protocol) != "" {
			schemes = []string{record.Protocol}
		}
		if len(schemes) == 0 {
			schemes = []string{defaultScheme}
		}
		for _, scheme := range schemes {
			scheme = strings.ToLower(strings.TrimSpace(scheme))
			rawProxy := scheme + "://" + net.JoinHostPort(ip, port)
			proxy, err := ParseProxy(rawProxy, defaultScheme)
			if err != nil {
				rejected++
				continue
			}
			proxies = append(proxies, proxy)
		}
	}
	return proxies, rejected, nil
}

func jsonPort(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return ""
	}
	switch value := value.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return string(value)
	default:
		return ""
	}
}

// ParseProxy accepts only supported proxy schemes, host:port endpoints, and no
// credentials. Literal unsafe IP addresses are rejected before any future dial.
func ParseProxy(raw, defaultScheme string) (Proxy, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return Proxy{}, fmt.Errorf("proxy endpoint is empty")
	}
	if !strings.Contains(raw, "://") {
		raw = strings.ToLower(strings.TrimSpace(defaultScheme)) + "://" + raw
	}

	u, err := url.ParseRequestURI(raw)
	if err != nil {
		return Proxy{}, fmt.Errorf("parse proxy endpoint: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if !supportedScheme(scheme) {
		return Proxy{}, fmt.Errorf("unsupported proxy scheme %q", u.Scheme)
	}
	if u.User != nil {
		return Proxy{}, fmt.Errorf("proxy credentials are not accepted")
	}
	if u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return Proxy{}, fmt.Errorf("proxy endpoint must not include a path, query, or fragment")
	}
	host := strings.TrimSpace(u.Hostname())
	if bare := strings.TrimSuffix(strings.ToLower(host), "."); bare == "" || bare == "localhost" || strings.HasSuffix(bare, ".localhost") {
		return Proxy{}, fmt.Errorf("proxy endpoint must have a public host")
	}
	host = normalizeIPv4Literal(host)
	if addr, parseAddrErr := netip.ParseAddr(host); parseAddrErr != nil {
		if err := checkHostname(strings.TrimSuffix(strings.ToLower(host), ".")); err != nil {
			return Proxy{}, err
		}
	} else if addr.Zone() != "" {
		return Proxy{}, fmt.Errorf("proxy endpoint must not use an IPv6 zone identifier")
	}
	port, err := parsePort(u.Port())
	if err != nil {
		return Proxy{}, err
	}
	if addr, parseAddrErr := netip.ParseAddr(host); parseAddrErr == nil && UnsafeIP(addr) {
		return Proxy{}, fmt.Errorf("proxy endpoint uses a non-public IP address")
	}

	return Proxy{Scheme: scheme, Host: strings.ToLower(host), Port: port}, nil
}

// checkHostname rejects hostnames that are not IP literals to Go but that
// other resolvers or wildcard DNS services map to an address: inet_aton-style
// numeric forms such as 2130706433, 127.1, or 0x7f.0.0.1 (a real top-level
// label is never numeric), and wildcard-DNS names that embed an unsafe IPv4
// address such as 127.0.0.1.nip.io, 10-0-0-1.sslip.io, or 7f000001.nip.io.
// Other hostnames are still accepted; their resolved addresses are checked by
// DialPublicContext at dial time. The host must be lowercased without a
// trailing dot.
func checkHostname(host string) error {
	if err := checkHostnameSyntax(host); err != nil {
		return err
	}
	labels := strings.Split(host, ".")
	if last := labels[len(labels)-1]; isNumericLabel(last) {
		return fmt.Errorf("proxy endpoint host is an ambiguous numeric address")
	}
	for i := range labels {
		if i+4 <= len(labels) {
			if addr, ok := octetsAddr(labels[i : i+4]); ok && UnsafeIP(addr) {
				return fmt.Errorf("proxy endpoint host embeds a non-public IP address")
			}
		}
		if parts := strings.Split(labels[i], "-"); len(parts) >= 4 {
			for j := 0; j+4 <= len(parts); j++ {
				if addr, ok := octetsAddr(parts[j : j+4]); ok && UnsafeIP(addr) {
					return fmt.Errorf("proxy endpoint host embeds a non-public IP address")
				}
			}
		}
		if len(labels[i]) == 8 {
			if value, err := strconv.ParseUint(labels[i], 16, 32); err == nil {
				addr := netip.AddrFrom4([4]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
				if UnsafeIP(addr) {
					return fmt.Errorf("proxy endpoint host embeds a non-public IP address")
				}
			}
		}
	}
	return nil
}

const (
	maxHostnameLength = 253
	maxHostnameLabel  = 63
)

// checkHostnameSyntax enforces strict LDH hostnames: dot-separated labels of
// 1-63 lowercase ASCII letters, digits, or hyphens that neither start nor end
// with a hyphen (internationalized names must be punycode). Feed data is
// untrusted and the host is echoed by the public API, so characters that are
// legal in a URL authority but meaningful to a shell or HTML consumer, such as
// `$ ( ) ; & ' " < > * , = ~`, percent-decoded bytes, and invalid UTF-8, are
// rejected rather than escaped. The host must already be lowercased without a
// trailing dot.
func checkHostnameSyntax(host string) error {
	if host == "" || len(host) > maxHostnameLength {
		return fmt.Errorf("proxy endpoint host has an invalid length")
	}
	labelLen := 0
	for i := 0; i < len(host); i++ {
		c := host[i]
		switch {
		case c == '.':
			if labelLen == 0 || host[i-1] == '-' {
				return fmt.Errorf("proxy endpoint host has an empty or malformed label")
			}
			labelLen = 0
			continue
		case c >= 'a' && c <= 'z', c >= '0' && c <= '9':
		case c == '-':
			if labelLen == 0 {
				return fmt.Errorf("proxy endpoint host has an empty or malformed label")
			}
		default:
			return fmt.Errorf("proxy endpoint host contains an invalid character")
		}
		labelLen++
		if labelLen > maxHostnameLabel {
			return fmt.Errorf("proxy endpoint host label is too long")
		}
	}
	if labelLen == 0 || host[len(host)-1] == '-' {
		return fmt.Errorf("proxy endpoint host has an empty or malformed label")
	}
	return nil
}

func isNumericLabel(label string) bool {
	if label == "" {
		return false
	}
	digits := label
	base := 10
	if len(label) > 2 && (label[:2] == "0x") {
		digits, base = label[2:], 16
	}
	_, err := strconv.ParseUint(digits, base, 64)
	return err == nil || errors.Is(err, strconv.ErrRange)
}

func octetsAddr(parts []string) (netip.Addr, bool) {
	var octets [4]byte
	for i, part := range parts {
		if part == "" || len(part) > 3 {
			return netip.Addr{}, false
		}
		value, err := strconv.ParseUint(part, 10, 8)
		if err != nil {
			return netip.Addr{}, false
		}
		octets[i] = byte(value)
	}
	return netip.AddrFrom4(octets), true
}

func normalizeIPv4Literal(host string) string {
	parts := strings.Split(host, ".")
	if len(parts) != 4 {
		return host
	}
	for i, part := range parts {
		if part == "" {
			return host
		}
		value, err := strconv.ParseUint(part, 10, 8)
		if err != nil {
			return host
		}
		parts[i] = strconv.FormatUint(value, 10)
	}
	return strings.Join(parts, ".")
}

// IsSupportedScheme reports whether the proxy scheme is one this package can
// parse and probe.
func IsSupportedScheme(scheme string) bool {
	return supportedScheme(scheme)
}

func supportedScheme(scheme string) bool {
	switch scheme {
	case "http", "https", "socks4", "socks4a", "socks5", "socks5h":
		return true
	default:
		return false
	}
}

func parsePort(raw string) (uint16, error) {
	if raw == "" {
		return 0, fmt.Errorf("proxy endpoint must include a port")
	}
	port, err := strconv.ParseUint(raw, 10, 16)
	if err != nil || port == 0 {
		return 0, fmt.Errorf("proxy endpoint has an invalid port")
	}
	return uint16(port), nil
}

// nonPublicPrefixes are special-purpose ranges the netip predicates do not
// cover but that are never reachable public proxy or source hosts.
var nonPublicPrefixes = []netip.Prefix{
	netip.MustParsePrefix("0.0.0.0/8"),     // "this network"
	netip.MustParsePrefix("100.64.0.0/10"), // carrier-grade NAT shared space
	netip.MustParsePrefix("192.0.0.0/24"),  // IETF protocol assignments
	netip.MustParsePrefix("198.18.0.0/15"), // benchmarking
	netip.MustParsePrefix("240.0.0.0/4"),   // reserved, including broadcast
	netip.MustParsePrefix("100::/64"),      // IPv6 discard-only
}

var (
	// NAT64 prefixes carry an IPv4 address in the low 32 bits.
	nat64Prefixes = []netip.Prefix{
		netip.MustParsePrefix("64:ff9b::/96"),
		netip.MustParsePrefix("64:ff9b:1::/48"),
	}
	// 6to4 carries an IPv4 address in bits 16-47.
	sixToFourPrefix = netip.MustParsePrefix("2002::/16")
)

// UnsafeIP reports whether the address is loopback, private, link-local,
// multicast, unspecified, another special-purpose range, or an IPv6
// translation of such an IPv4 address, and therefore never a safe audit
// endpoint.
func UnsafeIP(addr netip.Addr) bool {
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsLinkLocalUnicast() ||
		addr.IsMulticast() || addr.IsUnspecified() {
		return true
	}
	for _, prefix := range nonPublicPrefixes {
		if prefix.Contains(addr) {
			return true
		}
	}
	if embedded, ok := embeddedIPv4(addr); ok {
		return UnsafeIP(embedded)
	}
	return false
}

func embeddedIPv4(addr netip.Addr) (netip.Addr, bool) {
	if !addr.Is6() {
		return netip.Addr{}, false
	}
	b := addr.As16()
	for _, prefix := range nat64Prefixes {
		if prefix.Contains(addr) {
			return netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]}), true
		}
	}
	if sixToFourPrefix.Contains(addr) {
		return netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]}), true
	}
	return netip.Addr{}, false
}

// DialPublicContext resolves a hostname immediately before dialing and refuses
// unsafe literal or resolved addresses. It is used for source and proxy hops;
// the configured probe target is reached by the proxy, not directly by this
// dialer.
func DialPublicContext(ctx context.Context, network, address string) (net.Conn, error) {
	return PublicDialer{}.DialContext(ctx, network, address)
}

// PublicDialer applies the DialPublicContext address policy with an optional
// per-attempt TCP connect timeout. Proxy probes set ConnectTimeout so a
// black-holed endpoint fails in seconds instead of holding a worker for the
// whole request timeout; source fetches keep the zero value and stay bounded
// only by their request context. The timeout covers TCP connect only, never
// TLS or protocol handshakes.
type PublicDialer struct {
	ConnectTimeout time.Duration
}

func (d PublicDialer) netDialer() *net.Dialer {
	return &net.Dialer{Timeout: d.ConnectTimeout}
}

// DialContext dials address after refusing unsafe literal or resolved IPs.
// Each resolved candidate gets its own ConnectTimeout budget.
func (d PublicDialer) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	dialer := d.netDialer()
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if literal, err := netip.ParseAddr(host); err == nil {
		if UnsafeIP(literal) {
			return nil, fmt.Errorf("refusing non-public dial address")
		}
		return dialer.DialContext(ctx, network, net.JoinHostPort(literal.String(), port))
	}
	resolved, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, fmt.Errorf("resolve dial host: %w", err)
	}
	var lastErr error
	for _, candidate := range resolved {
		if UnsafeIP(candidate) {
			continue
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr != nil {
		return nil, fmt.Errorf("dial public address: %w", lastErr)
	}
	return nil, fmt.Errorf("dial host did not resolve to a public address")
}
