package monitor

import (
	"container/list"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	readyzTimeout = 3 * time.Second
	// internalAPIQueryTimeout bounds one internal proxy query. It stays below
	// proxy-router's 15s client timeout but allows for Redis contention while
	// every replica restarts and claims work at once.
	internalAPIQueryTimeout   = 10 * time.Second
	httpShutdown              = 5 * time.Second
	publicProxyRequestsPerMin = 60
	publicProxyRateWindow     = time.Minute
	publicProxyCacheControl   = "public, max-age=30, s-maxage=30"
	publicProxyCORSMaxAge     = 600
	// publicProxyMaxLimit caps one /proxies page. An omitted, zero, negative,
	// or larger limit returns up to this many results; use offset to page.
	publicProxyMaxLimit = 1000
	// publicProxyMaxOffset bounds the store work one page request can cause;
	// offset+limit results are read before the page is sliced out.
	publicProxyMaxOffset = 100_000
	// httpIdleTimeout reaps idle keep-alive connections. It is longer than
	// common ingress idle pools (cloudflared defaults to 90s) so the server
	// does not close a connection the ingress is about to reuse.
	httpIdleTimeout = 120 * time.Second
	// httpWriteTimeout bounds one response, from the end of the request headers
	// to the last byte. It is far above the slowest handler deadline (10s).
	httpWriteTimeout = 30 * time.Second
	// httpMaxHeaderBytes replaces the 1 MiB default. Legitimate requests carry
	// a few KiB of headers (browser, Cloudflare, bearer token).
	httpMaxHeaderBytes = 64 << 10
)

//go:embed homepage.html
var homepageHTML []byte

//go:embed dashboard.html
var dashboardHTML []byte

//go:embed icon.png
var iconPNG []byte

//go:embed favicon.png
var faviconPNG []byte

//go:embed social-share.png
var socialSharePNG []byte

// healthServer exposes the public homepage, dashboard, /get, /livez, /readyz,
// /report, /stats, and /metrics. Every JSON payload is aggregate counts and
// status only except /get, which intentionally echoes a caller-visible IP for
// operator-controlled proxy validation.
type healthServer struct {
	server   *http.Server
	handlers *healthHandlers
	// admin serves /metrics, /report, and the internal API when
	// admin_listen_addr is configured; nil otherwise.
	admin         *http.Server
	adminListener net.Listener
}

type livezReport struct {
	Status    string `json:"status"`
	CheckedAt string `json:"checked_at"`
}

type readyzReport struct {
	Ready     bool   `json:"ready"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
	CheckedAt string `json:"checked_at"`
}

type refreshReport struct {
	At       string `json:"at,omitempty"`
	Result   string `json:"result,omitempty"`
	Accepted int64  `json:"accepted,omitempty"`
	Rejected int64  `json:"rejected,omitempty"`
}

type validatedSlices struct {
	ByCountry       map[string]int64 `json:"by_country,omitempty"`
	StableByCountry map[string]int64 `json:"stable_by_country,omitempty"`
	ByASN           map[string]int64 `json:"by_asn,omitempty"`
	ByAnonymity     map[string]int64 `json:"by_anonymity,omitempty"`
	ByLatencyBand   map[string]int64 `json:"by_latency_band,omitempty"`
	Stable          int64            `json:"stable"`
	BandKnown       int64            `json:"-"`
	GeoMismatch     int64            `json:"geo_mismatch"`
}

type reportPayload struct {
	Status            string          `json:"status"`
	ValidationEnabled bool            `json:"network_validation_enabled"`
	UptimeSeconds     int64           `json:"uptime_seconds"`
	Counts            countsPayload   `json:"counts"`
	Validated         validatedSlices `json:"validated_slices"`
	BudgetPerMinute   int             `json:"probe_budget_per_minute"`
	LastSourceRefresh refreshReport   `json:"last_source_refresh"`
	CheckedAt         string          `json:"checked_at"`
}

type statsPayload struct {
	Status          string       `json:"status"`
	Total           int64        `json:"total"`
	Stable          int64        `json:"stable"`
	ByCountry       sortedCounts `json:"by_country"`
	StableByCountry sortedCounts `json:"stable_by_country"`
	// ByAnonymity counts validated proxies by anonymity class ("elite",
	// "anonymous", "transparent", "unknown", ...).
	ByAnonymity sortedCounts `json:"by_anonymity"`
	// LatencyBands counts validated proxies by latency band, keyed exactly as
	// latencyBand names them ("<200ms", "200-500ms", "500-1000ms", ">1000ms").
	LatencyBands sortedCounts `json:"latency_bands"`
	CheckedAt    string       `json:"checked_at"`
}

type sortedCounts map[string]int64

func (c sortedCounts) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	type countEntry struct {
		country string
		count   int64
	}
	entries := make([]countEntry, 0, len(c))
	for country, count := range c {
		entries = append(entries, countEntry{country: country, count: count})
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].count != entries[j].count {
			return entries[i].count > entries[j].count
		}
		return entries[i].country < entries[j].country
	})
	body := []byte{'{'}
	for i, entry := range entries {
		if i > 0 {
			body = append(body, ',')
		}
		key, _ := json.Marshal(entry.country)
		body = append(body, key...)
		body = append(body, ':')
		body = strconv.AppendInt(body, entry.count, 10)
	}
	return append(body, '}'), nil
}

// nonNilCounts returns counts as sortedCounts, substituting an empty map for
// nil so the public field is always a JSON object.
func nonNilCounts(counts map[string]int64) sortedCounts {
	if counts == nil {
		return sortedCounts{}
	}
	return sortedCounts(counts)
}

type echoPayload struct {
	Args    map[string][]string `json:"args"`
	Headers map[string]string   `json:"headers"`
	Origin  string              `json:"origin"`
	URL     string              `json:"url"`
}

type rateWindow struct {
	started time.Time
	count   int
	element *list.Element
}

type ipRateLimiter struct {
	mu         sync.Mutex
	limit      int
	window     time.Duration
	maxClients int
	clients    map[string]rateWindow
	order      *list.List
}

func newIPRateLimiter(limit int, window time.Duration) *ipRateLimiter {
	return &ipRateLimiter{
		limit:      limit,
		window:     window,
		maxClients: 10_000,
		clients:    make(map[string]rateWindow),
		order:      list.New(),
	}
}

func (l *ipRateLimiter) allow(ip string, now time.Time) bool {
	if ip == "" {
		ip = "unknown"
	}
	ip = rateLimitKey(ip)
	l.mu.Lock()
	defer l.mu.Unlock()
	l.cleanup(now, 64)
	entry, ok := l.clients[ip]
	if ok && (now.Sub(entry.started) >= l.window || now.Before(entry.started)) {
		l.order.Remove(entry.element)
		delete(l.clients, ip)
		ok = false
	}
	if !ok {
		// Refusing every unseen client once the table is full would let anyone
		// rotating source addresses lock out all new users. Evicting the oldest
		// window instead only lets that client start a fresh window early.
		for len(l.clients) >= l.maxClients && l.order.Len() > 0 {
			l.evictOldest()
		}
		entry = rateWindow{started: now, element: l.order.PushBack(ip)}
	}
	if entry.count >= l.limit {
		l.clients[ip] = entry
		return false
	}
	entry.count++
	l.clients[ip] = entry
	return true
}

func (l *ipRateLimiter) evictOldest() {
	front := l.order.Front()
	ip := front.Value.(string)
	if entry, ok := l.clients[ip]; ok && entry.element == front {
		delete(l.clients, ip)
	}
	l.order.Remove(front)
}

// rateLimitKey groups IPv6 clients by /64, the smallest allocation a single
// subscriber normally controls, so rotating addresses inside one allocation
// shares a window. IPv4 and unparseable values are used as-is.
func rateLimitKey(ip string) string {
	addr, err := netip.ParseAddr(ip)
	if err != nil || !addr.Is6() || addr.Is4In6() {
		return ip
	}
	prefix, err := addr.WithZone("").Prefix(64)
	if err != nil {
		return ip
	}
	return prefix.String()
}

func (l *ipRateLimiter) cleanup(now time.Time, budget int) {
	for budget > 0 {
		front := l.order.Front()
		if front == nil {
			return
		}
		ip := front.Value.(string)
		entry, ok := l.clients[ip]
		if !ok || entry.element != front {
			l.order.Remove(front)
			budget--
			continue
		}
		if now.Sub(entry.started) < l.window && !now.Before(entry.started) {
			return
		}
		delete(l.clients, ip)
		l.order.Remove(front)
		budget--
	}
}

type clientIPResolver struct {
	trusted []netip.Prefix
}

func newClientIPResolver(trusted []netip.Prefix) clientIPResolver {
	return clientIPResolver{trusted: append([]netip.Prefix(nil), trusted...)}
}

func (r clientIPResolver) resolve(request *http.Request) string {
	peer, ok := canonicalRemoteIP(request.RemoteAddr)
	if !ok {
		return "unknown"
	}
	if !r.isTrusted(peer) {
		return peer.String()
	}
	if value := strings.TrimSpace(request.Header.Get("CF-Connecting-IP")); value != "" {
		if ip, err := netip.ParseAddr(value); err == nil {
			return ip.Unmap().String()
		}
		return peer.String()
	}
	values := strings.Split(request.Header.Get("X-Forwarded-For"), ",")
	parsed := make([]netip.Addr, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		ip, err := netip.ParseAddr(value)
		if err != nil {
			return peer.String()
		}
		parsed = append(parsed, ip.Unmap())
	}
	for i := len(parsed) - 1; i >= 0; i-- {
		if !r.isTrusted(parsed[i]) {
			return parsed[i].String()
		}
	}
	if len(parsed) > 0 {
		return parsed[0].String()
	}
	return peer.String()
}

func (r clientIPResolver) isTrusted(ip netip.Addr) bool {
	for _, prefix := range r.trusted {
		if prefix.Contains(ip) {
			return true
		}
	}
	return false
}

func canonicalRemoteIP(remote string) (netip.Addr, bool) {
	host, _, err := net.SplitHostPort(strings.TrimSpace(remote))
	if err != nil {
		host = strings.TrimSpace(remote)
	}
	ip, err := netip.ParseAddr(host)
	return ip.Unmap(), err == nil
}

type countsPayload struct {
	Candidates int64 `json:"candidates"`
	Pending    int64 `json:"pending"`
	Leased     int64 `json:"leased"`
	Validated  int64 `json:"validated"`
}

// healthHandlers holds the shared state behind the public health server routes.
type healthHandlers struct {
	runner        *Runner
	publicLimiter *ipRateLimiter
	publicCache   *publicProxyResponseCache
	statsCache    *publicProxyResponseCache
	statsCacheKey [sha256.Size]byte
	clientIPs     clientIPResolver
}

func startHealthServer(addr string, runner *Runner) (*healthServer, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	publicCache := newPublicProxyResponseCache(publicProxyCacheEntries, publicProxyCacheBytes, publicProxyCacheEntrySize, publicProxyCacheTTL, time.Now)
	publicCache.limitLoads(publicProxyLoadSlots(runner.config.apiRedisPoolSize()))
	pageOpts := pageOptions{
		BaseURL:                runner.config.PublicBaseURL,
		MeasurementID:          runner.config.AnalyticsMeasurementID,
		PrivacyURL:             runner.config.PrivacyURL,
		GeoIPAttribution:       runner.config.GeoIPDBPath != "" || runner.config.GeoIPASNDBPath != "",
		CloudflareWebAnalytics: runner.config.CloudflareWebAnalytics,
	}
	homepage := newHTMLPage(homepageHTML, pageOpts)
	dashboard := newHTMLPage(dashboardHTML, pageOpts)
	h := &healthHandlers{
		runner:        runner,
		publicLimiter: newIPRateLimiter(publicProxyRequestsPerMin, publicProxyRateWindow),
		publicCache:   publicCache,
		statsCache:    newPublicProxyResponseCache(1, publicProxyCacheBytes, publicProxyCacheEntrySize, publicProxyCacheTTL, time.Now),
		statsCacheKey: publicProxyCacheKey(store.ProxyFilter{}, 0),
		clientIPs:     newClientIPResolver(runner.config.TrustedProxyCIDRs),
	}
	mux.HandleFunc("/icon.png", func(w http.ResponseWriter, r *http.Request) {
		servePNG(w, r, "/icon.png", iconPNG, "public, max-age=86400, immutable")
	})
	mux.HandleFunc("/favicon.png", func(w http.ResponseWriter, r *http.Request) {
		servePNG(w, r, "/favicon.png", faviconPNG, "public, max-age=86400, immutable")
	})
	mux.HandleFunc("/social-share.png", func(w http.ResponseWriter, r *http.Request) {
		servePNG(w, r, "/social-share.png", socialSharePNG, "public, max-age=86400, immutable")
	})
	// The Cloudflare edge only forwards /, /icon.png, /favicon.png, /get,
	// /stats, /proxies, and /dashboard/*, so pages reference the og image
	// under /dashboard. /social-share.png stays for direct origin access.
	mux.HandleFunc("/dashboard/social-share.png", func(w http.ResponseWriter, r *http.Request) {
		servePNG(w, r, "/dashboard/social-share.png", socialSharePNG, "public, max-age=86400, immutable")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, r, "/", homepage)
	})
	mux.HandleFunc("/livez", handleLivez)
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, r *http.Request) {
		serveHTML(w, r, "/dashboard", dashboard)
	})
	mux.HandleFunc("/dashboard/data", h.dashboardData)
	mux.HandleFunc("/get", h.echo)
	mux.HandleFunc("/readyz", h.readyz)
	mux.HandleFunc("/stats", h.stats)
	mux.HandleFunc("/proxies", h.proxies)

	// Operator endpoints: /metrics leaks feed hostnames and probe settings,
	// /report leaks budgets and uptime, and the internal API returns every
	// validated proxy. With admin_listen_addr they move to their own listener
	// (and are absent from the public one); without it they share the public
	// listener for compatibility, so the ingress must not forward them.
	registerAdmin := func(m *http.ServeMux) {
		m.HandleFunc("/report", h.report)
		m.Handle("/metrics", withAccuracyMetrics(runner.metrics.Handler(), &runner.accuracy.metrics))
		registerInternalAPI(m, runner, loadAPIToken(runner.config.InternalAPITokenFile))
	}
	var adminServer *http.Server
	var adminListener net.Listener
	if addr := runner.config.AdminListenAddr; addr != "" {
		adminListener, err = net.Listen("tcp", addr)
		if err != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("listen on admin address: %w", err)
		}
		adminMux := http.NewServeMux()
		registerAdmin(adminMux)
		adminMux.HandleFunc("/livez", handleLivez)
		adminMux.HandleFunc("/readyz", h.readyz)
		adminServer = newHTTPServer(adminMux)
	} else {
		registerAdmin(mux)
	}

	server := newHTTPServer(mux)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Printf("warning: health server stopped: %v", err)
		}
	}()
	if adminServer != nil {
		go func() {
			if err := adminServer.Serve(adminListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("warning: admin server stopped: %v", err)
			}
		}()
		log.Printf("admin endpoints (/metrics, /report, /internal/api) are served on %s", adminListener.Addr())
	} else {
		log.Printf("warning: admin_listen_addr is not set, so /metrics, /report, and /internal/api are served on the public listener %s; set admin_listen_addr (for example 127.0.0.1:9090) or keep your ingress from forwarding them", listener.Addr())
	}
	return &healthServer{server: server, handlers: h, admin: adminServer, adminListener: adminListener}, nil
}

// newHTTPServer applies the shared timeouts. Public responses are small and
// every handler has its own context deadline, so a fixed write deadline only
// bounds slow-reading clients, which would otherwise pin a goroutine and a
// socket each for as long as they keep the connection open.
func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
	}
}

// requireGetOrHead rejects methods other than GET and HEAD with 405 and
// reports whether the request may proceed.
func requireGetOrHead(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return false
	}
	return true
}

// serveHTML serves an embedded HTML page at exactly path.
func serveHTML(w http.ResponseWriter, r *http.Request, path string, page htmlPage) {
	if r.URL.Path != path {
		http.NotFound(w, r)
		return
	}
	if !requireGetOrHead(w, r) {
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=60, s-maxage=60")
	w.Header().Set("Content-Security-Policy", page.csp)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(page.body)
	}
}

func handleLivez(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, r, http.StatusOK, livezReport{Status: "ok", CheckedAt: time.Now().UTC().Format(time.RFC3339)})
}

func (h *healthHandlers) dashboardData(w http.ResponseWriter, r *http.Request) {
	if !requireGetOrHead(w, r) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	payload, err := h.runner.dashboardData(ctx, dashboardRange(r.URL.Query().Get("range")))
	if err != nil {
		writeJSON(w, r, http.StatusServiceUnavailable, dashboardPayload{
			Status:    "unavailable",
			Error:     publicDashboardError(err),
			CheckedAt: time.Now().UTC().Format(time.RFC3339),
		})
		return
	}
	// Successful (including stale "degraded") payloads match the server-side
	// cache lifetime so the edge and browsers can reuse them; errors above
	// stay no-store.
	w.Header().Set("Cache-Control", dashboardDataCacheControl)
	writeJSON(w, r, http.StatusOK, payload)
}

func (h *healthHandlers) echo(w http.ResponseWriter, r *http.Request) {
	if !requireGetOrHead(w, r) {
		return
	}
	writeEcho(w, r, h.clientIPs)
}

func (h *healthHandlers) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
	defer cancel()
	err := h.runner.readStore().Ping(ctx)
	status := http.StatusOK
	payload := readyzReport{Ready: true, Status: "ok", CheckedAt: time.Now().UTC().Format(time.RFC3339)}
	if err != nil {
		status = http.StatusServiceUnavailable
		payload.Ready = false
		payload.Status = "unavailable"
		payload.Error = "redis unavailable"
	}
	if h.runner.draining.Load() {
		status = http.StatusServiceUnavailable
		payload.Ready = false
		payload.Status = "draining"
	}
	writeJSON(w, r, status, payload)
}

func (h *healthHandlers) report(w http.ResponseWriter, r *http.Request) {
	runner := h.runner
	ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
	defer cancel()
	stats, err := runner.readStore().Stats(ctx)
	if err != nil {
		writeJSON(w, r, http.StatusServiceUnavailable, reportPayload{Status: "unavailable", CheckedAt: time.Now().UTC().Format(time.RFC3339)})
		return
	}
	// /report is unauthenticated and unrate-limited, so serve the aggregate
	// publishStats refreshes every 30s instead of scanning the whole
	// validated set in Redis on every request.
	slices := runner.validatedSlicesSnapshot()
	writeJSON(w, r, http.StatusOK, reportPayload{
		Status:            "ok",
		ValidationEnabled: runner.config.NetworkValidationEnabled,
		UptimeSeconds:     int64(time.Now().UTC().Sub(runner.startedAt).Seconds()),
		Counts: countsPayload{
			Candidates: stats.Candidates,
			Pending:    stats.Pending,
			Leased:     stats.Leased,
			Validated:  stats.Validated,
		},
		Validated:         slices,
		BudgetPerMinute:   runner.config.GlobalRequestsPerMinute,
		LastSourceRefresh: runner.lastRefreshSnapshot(),
		CheckedAt:         time.Now().UTC().Format(time.RFC3339),
	})
}

func (h *healthHandlers) stats(w http.ResponseWriter, r *http.Request) {
	if !requireGetOrHead(w, r) {
		return
	}
	// The Cloudflare tunnel currently exposes /stats but not /get. Keep
	// this explicit alias so the public echo endpoint works before the
	// tunnel ingress rule is updated.
	if r.URL.Query().Get("echo") == "1" {
		writeEcho(w, r, h.clientIPs)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
	defer cancel()
	body, remaining, cached, err := h.statsCache.getOrLoad(ctx, h.statsCacheKey, h.loadStatsBody)
	if err != nil {
		writeJSON(w, r, http.StatusServiceUnavailable, statsPayload{Status: "unavailable", CheckedAt: time.Now().UTC().Format(time.RFC3339)})
		return
	}
	writePublicJSONResponse(w, r, body, publicProxyCacheControlFor(remaining, cached))
}

// loadStatsBody builds the cached /stats JSON body.
func (h *healthHandlers) loadStatsBody(loadCtx context.Context) ([]byte, error) {
	if _, err := h.runner.readStore().Stats(loadCtx); err != nil {
		return nil, err
	}
	slices := h.runner.aggregateValidatedSlicesFrom(loadCtx, h.runner.readStore())
	byCountry := make(map[string]int64, len(slices.ByCountry))
	var total int64
	for country, count := range slices.ByCountry {
		byCountry[country] = count
		total += count
	}
	payload := statsPayload{
		Status:          "ok",
		Total:           total,
		Stable:          slices.Stable,
		ByCountry:       sortedCounts(byCountry),
		StableByCountry: sortedCounts(slices.StableByCountry),
		ByAnonymity:     nonNilCounts(slices.ByAnonymity),
		LatencyBands:    nonNilCounts(slices.ByLatencyBand),
		CheckedAt:       time.Now().UTC().Format(time.RFC3339),
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

func (h *healthHandlers) proxies(w http.ResponseWriter, r *http.Request) {
	setPublicProxyCORS(w)
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if !requireGetOrHead(w, r) {
		return
	}
	if !h.publicLimiter.allow(h.clientIPs.resolve(r), time.Now()) {
		w.Header().Set("Retry-After", "60")
		writeJSON(w, r, http.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		return
	}
	filter, offset, err := publicProxyPage(r)
	if err != nil {
		writeJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
		return
	}
	filter.ClassificationMaxAgeMs = h.runner.config.ClassificationMaxAge.Milliseconds()
	cacheKey := publicProxyCacheKey(filter, offset)
	ctx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
	defer cancel()
	body, remaining, cached, err := h.publicCache.getOrLoad(ctx, cacheKey, func(loadCtx context.Context) ([]byte, error) {
		return h.loadProxiesBody(loadCtx, filter, offset)
	})
	if err != nil {
		if errors.Is(err, errPublicProxyBusy) {
			w.Header().Set("Retry-After", "5")
			writeJSON(w, r, http.StatusServiceUnavailable, map[string]string{"error": "busy, retry shortly"})
			return
		}
		writeJSON(w, r, http.StatusServiceUnavailable, map[string]string{"error": "query failed"})
		return
	}
	writePublicJSONResponse(w, r, body, publicProxyCacheControlFor(remaining, cached))
}

// loadProxiesBody builds the cached /proxies JSON body for one page.
func (h *healthHandlers) loadProxiesBody(loadCtx context.Context, filter store.ProxyFilter, offset int) ([]byte, error) {
	query := filter
	// One extra result reveals whether another page exists.
	query.Limit = offset + filter.Limit + 1
	proxies, err := h.runner.readStore().QueryValidated(loadCtx, query)
	if err != nil {
		return nil, err
	}
	if len(proxies) > offset {
		proxies = proxies[offset:]
	} else {
		proxies = nil
	}
	hasMore := len(proxies) > filter.Limit
	if hasMore {
		proxies = proxies[:filter.Limit]
	}
	if proxies == nil {
		proxies = []store.QueryProxy{}
	}
	body, err := json.Marshal(publicProxiesPayload{
		Count:   len(proxies),
		Proxies: proxies,
		Limit:   filter.Limit,
		Offset:  offset,
		HasMore: hasMore,
	})
	if err != nil {
		return nil, err
	}
	return append(body, '\n'), nil
}

// proxyMaxAgeLimit bounds the max_age filter so the cutoff arithmetic cannot
// overflow and callers get a clear error for nonsensical values.
const proxyMaxAgeLimit = 30 * 24 * time.Hour

// parseProxyMaxAge parses the max_age query value as either a Go duration
// ("30m") or integer seconds ("1800"). An empty value disables the filter.
func parseProxyMaxAge(raw string) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	var age time.Duration
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds <= 0 || seconds > int64(proxyMaxAgeLimit/time.Second) {
			return 0, errInvalidMaxAge
		}
		age = time.Duration(seconds) * time.Second
	} else {
		parsed, perr := time.ParseDuration(raw)
		if perr != nil || parsed <= 0 || parsed > proxyMaxAgeLimit {
			return 0, errInvalidMaxAge
		}
		age = parsed
	}
	ms := age.Milliseconds()
	if ms <= 0 {
		return 0, errInvalidMaxAge
	}
	return ms, nil
}

var errInvalidMaxAge = fmt.Errorf("max_age must be a positive Go duration (for example 30m) or integer seconds, at most %s", proxyMaxAgeLimit)

// Bounds that keep the filter space small: every distinct value is its own
// cache key and, on a miss, its own scan of the validated set.
const (
	// proxyMaxLatencyFilterMs clamps max_latency_ms; probe timeouts are far
	// below ten minutes, so larger values already match every measured proxy.
	proxyMaxLatencyFilterMs = 10 * 60 * 1000
	// proxyMaxRatioFilterPct clamps min_ratio_pct to a valid percentage.
	proxyMaxRatioFilterPct = 100
)

var (
	errInvalidCountry   = errors.New("country and exit_country must be two-letter ISO country codes")
	errInvalidAnonymity = errors.New("anonymity must be one of elite, anonymous, or transparent")
	errInvalidASN       = errors.New("asn must be an AS number such as AS15169 or 15169")
)

// parseCountryFilter canonicalizes a two-letter country code. An empty value
// disables the filter.
func parseCountryFilter(raw string) (string, error) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" {
		return "", nil
	}
	if len(raw) != 2 || raw[0] < 'A' || raw[0] > 'Z' || raw[1] < 'A' || raw[1] > 'Z' {
		return "", errInvalidCountry
	}
	return raw, nil
}

// parseAnonymityFilter accepts the stored anonymity classes.
func parseAnonymityFilter(raw string) (string, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	switch raw {
	case "", probe.AnonymityElite, probe.AnonymityAnonymous, probe.AnonymityTransparent:
		return raw, nil
	}
	return "", errInvalidAnonymity
}

// parseASNFilter canonicalizes "15169" and "as15169" to the stored "AS15169"
// form so equivalent spellings share one cache key.
func parseASNFilter(raw string) (string, error) {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	if raw == "" {
		return "", nil
	}
	number, err := strconv.ParseUint(strings.TrimPrefix(raw, "AS"), 10, 32)
	if err != nil || number == 0 {
		return "", errInvalidASN
	}
	return "AS" + strconv.FormatUint(number, 10), nil
}

func proxyFilter(r *http.Request) (store.ProxyFilter, error) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	maxAgeMs, err := parseProxyMaxAge(q.Get("max_age"))
	if err != nil {
		return store.ProxyFilter{}, err
	}
	country, err := parseCountryFilter(q.Get("country"))
	if err != nil {
		return store.ProxyFilter{}, err
	}
	exitCountry, err := parseCountryFilter(q.Get("exit_country"))
	if err != nil {
		return store.ProxyFilter{}, err
	}
	asn, err := parseASNFilter(q.Get("asn"))
	if err != nil {
		return store.ProxyFilter{}, err
	}
	anonymity, err := parseAnonymityFilter(q.Get("anonymity"))
	if err != nil {
		return store.ProxyFilter{}, err
	}
	return store.ProxyFilter{
		Country:      country,
		ExitCountry:  exitCountry,
		ASN:          asn,
		Anonymity:    anonymity,
		GeoMismatch:  parseBool(q.Get("geo_mismatch")),
		MaxLatencyMs: min(parsePositive(q.Get("max_latency_ms")), proxyMaxLatencyFilterMs),
		MinRatioPct:  min(parsePositive(q.Get("min_ratio_pct")), proxyMaxRatioFilterPct),
		MaxAgeMs:     maxAgeMs,
		HTTPS:        parseBool(q.Get("https")),
		// exclude_tampered drops proxies whose fresh echo tamper check failed.
		ExcludeTampered: parseBool(q.Get("exclude_tampered")),
		Limit:           limit,
	}, nil
}

// publicProxiesPayload is the /proxies response. Count is the number of
// proxies in this page; HasMore reports whether offset+limit has more matches.
type publicProxiesPayload struct {
	Count   int                `json:"count"`
	Proxies []store.QueryProxy `json:"proxies"`
	Limit   int                `json:"limit"`
	Offset  int                `json:"offset"`
	HasMore bool               `json:"has_more"`
}

// publicProxyPage parses the public /proxies filter and page. The limit is
// always normalized into 1..publicProxyMaxLimit; an error is returned only for
// an invalid max_age or an offset that is not an integer in
// 0..publicProxyMaxOffset.
func publicProxyPage(r *http.Request) (store.ProxyFilter, int, error) {
	filter, err := proxyFilter(r)
	if err != nil {
		return filter, 0, err
	}
	if filter.Limit <= 0 || filter.Limit > publicProxyMaxLimit {
		filter.Limit = publicProxyMaxLimit
	}
	raw := strings.TrimSpace(r.URL.Query().Get("offset"))
	if raw == "" {
		return filter, 0, nil
	}
	offset, err := strconv.Atoi(raw)
	if err != nil || offset < 0 || offset > publicProxyMaxOffset {
		return filter, 0, fmt.Errorf("offset must be an integer from 0 to %d", publicProxyMaxOffset)
	}
	return filter, offset, nil
}

func publicProxyCacheControlFor(remaining time.Duration, cached bool) string {
	if !cached {
		return publicProxyCacheControl
	}
	seconds := int(remaining / time.Second)
	return fmt.Sprintf("public, max-age=%d, s-maxage=%d", seconds, seconds)
}

func writePublicJSONResponse(w http.ResponseWriter, r *http.Request, body []byte, cacheControl string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", cacheControl)
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

func setPublicProxyCORS(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type")
	w.Header().Set("Access-Control-Max-Age", strconv.Itoa(publicProxyCORSMaxAge))
}

func servePNG(w http.ResponseWriter, r *http.Request, path string, body []byte, cacheControl string) {
	if r.URL.Path != path {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "image/png")
	w.Header().Set("Cache-Control", cacheControl)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

// Shutdown stops accepting new connections and waits for in-flight handlers.
func (h *healthServer) Shutdown() error {
	if h == nil || h.server == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdown)
	defer cancel()
	err := h.server.Shutdown(ctx)
	if h.admin != nil {
		if adminErr := h.admin.Shutdown(ctx); err == nil {
			err = adminErr
		}
	}
	return err
}

// AdminAddr returns the admin listener address, or "" when none is configured.
func (h *healthServer) AdminAddr() string {
	if h == nil || h.adminListener == nil {
		return ""
	}
	return h.adminListener.Addr().String()
}

func writeJSON(w http.ResponseWriter, r *http.Request, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if w.Header().Get("Cache-Control") == "" {
		w.Header().Set("Cache-Control", "no-store")
	}
	body, err := json.Marshal(payload)
	if err != nil {
		http.Error(w, "encode failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(status)
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(append(body, '\n'))
}

// maxEchoedForwardedFor bounds the caller-supplied X-Forwarded-For value echoed
// back, so a hostile chain cannot inflate the response.
const maxEchoedForwardedFor = 256

func echoedProxyHeaders(r *http.Request, clientIPs clientIPResolver) map[string]string {
	// Do not echo ingress-managed identity headers such as CF-Connecting-IP or
	// the hops the ingress appends to X-Forwarded-For. They describe the tunnel
	// rather than a proxy leak and would make every request appear non-elite
	// behind the public hostname. X-Forwarded-For entries the caller sent are
	// echoed, because a transparent proxy most often leaks its client there.
	const names = "Via,Forwarded,X-Real-IP"
	result := make(map[string]string)
	for _, name := range strings.Split(names, ",") {
		if value := strings.TrimSpace(r.Header.Get(name)); value != "" {
			result[name] = value
		}
	}
	if value := clientIPs.callerForwardedFor(r); value != "" {
		result["X-Forwarded-For"] = value
	}
	return result
}

// callerForwardedFor returns the X-Forwarded-For entries the caller itself
// sent. From a trusted peer, the trailing trusted hops and the resolved client
// entry that the ingress appended are removed; from an untrusted peer the whole
// header came from the caller.
func (r clientIPResolver) callerForwardedFor(request *http.Request) string {
	var entries []string
	for _, header := range request.Header.Values("X-Forwarded-For") {
		for _, entry := range strings.Split(header, ",") {
			if entry = strings.TrimSpace(entry); entry != "" {
				entries = append(entries, entry)
			}
		}
	}
	if len(entries) == 0 {
		return ""
	}
	if peer, ok := canonicalRemoteIP(request.RemoteAddr); ok && r.isTrusted(peer) {
		client := r.resolve(request)
		for len(entries) > 0 {
			ip, err := netip.ParseAddr(entries[len(entries)-1])
			if err != nil {
				break
			}
			ip = ip.Unmap()
			if r.isTrusted(ip) {
				entries = entries[:len(entries)-1]
				continue
			}
			if ip.String() == client {
				entries = entries[:len(entries)-1]
			}
			break
		}
	}
	value := strings.Join(entries, ", ")
	if len(value) > maxEchoedForwardedFor {
		value = value[:maxEchoedForwardedFor]
	}
	return value
}

func requestURL(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil || strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https") {
		scheme = "https"
	}
	host := r.Host
	if host == "" {
		host = "localhost"
	}
	return scheme + "://" + host + r.URL.RequestURI()
}

func writeEcho(w http.ResponseWriter, r *http.Request, clientIPs clientIPResolver) {
	writeJSON(w, r, http.StatusOK, echoPayload{
		Args:    r.URL.Query(),
		Headers: echoedProxyHeaders(r, clientIPs),
		Origin:  clientIPs.resolve(r),
		URL:     requestURL(r),
	})
}

// loadAPIToken reads the internal-API bearer token from disk. An empty result
// disables the API entirely.
func loadAPIToken(path string) string {
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	if err != nil {
		log.Printf("warning: internal API disabled: read token file: %v", err)
		return ""
	}
	token := strings.TrimSpace(string(data))
	if len(token) < 16 {
		log.Printf("warning: internal API disabled: token shorter than 16 characters")
		return ""
	}
	return token
}

func registerInternalAPI(mux *http.ServeMux, runner *Runner, token string) {
	if token == "" {
		return
	}
	log.Printf("internal proxy query API enabled")
	mux.HandleFunc("/internal/api/v1/proxies", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) ||
			subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, prefix)), []byte(token)) != 1 {
			writeJSON(w, r, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		filter, err := proxyFilter(r)
		if err != nil {
			writeJSON(w, r, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		filter.ClassificationMaxAgeMs = runner.config.ClassificationMaxAge.Milliseconds()
		ctx, cancel := context.WithTimeout(r.Context(), internalAPIQueryTimeout)
		defer cancel()
		proxies, err := runner.readStore().QueryValidated(ctx, filter)
		if err != nil {
			log.Printf("internal proxy query failed: %v", err)
			runner.metrics.RecordInternalAPIQueryFailure()
			writeJSON(w, r, http.StatusServiceUnavailable, map[string]string{"error": "query failed"})
			return
		}
		if proxies == nil {
			proxies = []store.QueryProxy{}
		}
		writeJSON(w, r, http.StatusOK, map[string]any{"count": len(proxies), "proxies": proxies})
	})
}

func parsePositive(raw string) int64 {
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}
