package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	prometheusResponseLimit = 4 << 20
	// dashboardCacheFreshness is the shared per-range server cache lifetime.
	// dashboardDataCacheControl advertises the same lifetime to clients.
	dashboardCacheFreshness     = 15 * time.Second
	dashboardDataCacheControl   = "public, max-age=15, s-maxage=15"
	dashboardFailureCooldown    = time.Second
	dashboardRefreshTimeout     = 10 * time.Second
	dashboardRefreshConcurrency = 2
	dashboardMetricLabel        = "dashboard_metric"
)

var errPrometheusNotConfigured = errors.New("dashboard data source (Prometheus) is not configured")

type dashboardCacheEntry struct {
	payload   dashboardPayload
	err       error
	expiresAt time.Time
}

type dashboardFlight struct {
	done    chan struct{}
	payload dashboardPayload
	err     error
}

// dashboardTotals holds the long-window instant queries ([365d] totals and
// traffic windows up to 8760h). They do not depend on the selected chart
// range, so every range shares one cached copy instead of re-running them.
type dashboardTotals struct {
	totals  []prometheusVector
	traffic []prometheusVector
	// health holds the range-independent pipeline health instant queries.
	health []prometheusVector
}

type dashboardTotalsFlight struct {
	done   chan struct{}
	result dashboardTotals
	err    error
}

type prometheusClient struct {
	baseURL    string
	httpClient *http.Client
	cacheMu    sync.Mutex
	cache      map[string]dashboardCacheEntry
	flights    map[string]*dashboardFlight
	// lastGood keeps each range's most recent successful payload so failed
	// refreshes can keep serving it as a stale "degraded" snapshot.
	lastGood  map[string]dashboardPayload
	refreshes chan struct{}

	// totalsMu protects the shared range-independent totals cache and flight.
	totalsMu        sync.Mutex
	totals          dashboardTotals
	totalsExpiresAt time.Time
	totalsFlight    *dashboardTotalsFlight
}

func newPrometheusClient(baseURL string) *prometheusClient {
	if strings.TrimSpace(baseURL) == "" {
		return nil
	}
	return &prometheusClient{
		baseURL:    strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		httpClient: &http.Client{Timeout: 8 * time.Second},
		cache:      make(map[string]dashboardCacheEntry, 3),
		flights:    make(map[string]*dashboardFlight, 3),
		lastGood:   make(map[string]dashboardPayload, 3),
		refreshes:  make(chan struct{}, dashboardRefreshConcurrency),
	}
}

type prometheusVector struct {
	Metric map[string]string `json:"metric"`
	Value  []json.RawMessage `json:"value"`
}

type prometheusMatrix struct {
	Metric map[string]string   `json:"metric"`
	Values [][]json.RawMessage `json:"values"`
}

type prometheusQueryResponse struct {
	Status    string `json:"status"`
	Error     string `json:"error"`
	ErrorType string `json:"errorType"`
	Data      struct {
		ResultType string             `json:"resultType"`
		Result     []prometheusVector `json:"result"`
	} `json:"data"`
}

func (p *prometheusClient) query(ctx context.Context, expression string) ([]prometheusVector, error) {
	var response prometheusQueryResponse
	values := url.Values{"query": {expression}}
	if err := p.request(ctx, "/api/v1/query", values, &response); err != nil {
		return nil, err
	}
	if response.Data.ResultType != "vector" {
		return nil, fmt.Errorf("unexpected Prometheus result type %q", response.Data.ResultType)
	}
	return response.Data.Result, nil
}

func (p *prometheusClient) queryRange(ctx context.Context, expression string, spec dashboardRangeSpec) ([]prometheusMatrix, error) {
	var response struct {
		Status    string `json:"status"`
		Error     string `json:"error"`
		ErrorType string `json:"errorType"`
		Data      struct {
			ResultType string             `json:"resultType"`
			Result     []prometheusMatrix `json:"result"`
		} `json:"data"`
	}
	now := time.Now()
	values := url.Values{
		"query": {expression},
		"start": {strconv.FormatInt(now.Add(-spec.duration).Unix(), 10)},
		"end":   {strconv.FormatInt(now.Unix(), 10)},
		"step":  {strconv.FormatInt(int64(spec.step.Seconds()), 10)},
	}
	if err := p.request(ctx, "/api/v1/query_range", values, &response); err != nil {
		return nil, err
	}
	if response.Data.ResultType != "matrix" {
		return nil, fmt.Errorf("unexpected Prometheus range result type %q", response.Data.ResultType)
	}
	return response.Data.Result, nil
}

func (p *prometheusClient) request(ctx context.Context, path string, values url.Values, output any) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, p.baseURL+path+"?"+values.Encode(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("Accept", "application/json")
	response, err := p.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("request to Prometheus failed")
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("unexpected Prometheus HTTP status %d", response.StatusCode)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, prometheusResponseLimit))
	if err := decoder.Decode(output); err != nil {
		return fmt.Errorf("decode Prometheus response: %w", err)
	}
	var status string
	var apiError struct {
		Status string `json:"status"`
		Error  string `json:"error"`
	}
	data, err := json.Marshal(output)
	if err == nil {
		_ = json.Unmarshal(data, &apiError)
		status = apiError.Status
	}
	if status != "success" {
		if apiError.Error == "" {
			apiError.Error = "unknown query error"
		}
		return fmt.Errorf("query to Prometheus failed: %s", apiError.Error)
	}
	return nil
}

type dashboardRangeSpec struct {
	key      string
	duration time.Duration
	step     time.Duration
}

func dashboardRange(raw string) dashboardRangeSpec {
	switch raw {
	case "1h":
		return dashboardRangeSpec{key: "1h", duration: time.Hour, step: time.Minute}
	case "24h":
		return dashboardRangeSpec{key: "24h", duration: 24 * time.Hour, step: 15 * time.Minute}
	default:
		return dashboardRangeSpec{key: "6h", duration: 6 * time.Hour, step: 5 * time.Minute}
	}
}

type dashboardBucket struct {
	Label string  `json:"label"`
	Value float64 `json:"value"`
}

type dashboardPoint struct {
	Time  float64 `json:"time"`
	Value float64 `json:"value"`
}

type dashboardTrafficSummary struct {
	Available bool    `json:"available"`
	PerSecond float64 `json:"per_second"`
	Day       float64 `json:"day"`
	Week      float64 `json:"week"`
	Month     float64 `json:"month"`
	Total     float64 `json:"total"`
}

type dashboardTraffic struct {
	Upload   dashboardTrafficSummary `json:"upload"`
	Download dashboardTrafficSummary `json:"download"`
}

type dashboardValidation struct {
	State    string `json:"state"`
	InFlight int64  `json:"in_flight,omitempty"`
	NextAt   string `json:"next_at,omitempty"`
}

// dashboardHealth summarizes pipeline health from Prometheus instant queries.
// A field is null when its query returned no samples (or, for the pass rate,
// when there were no HTTPS checks in the window).
type dashboardHealth struct {
	HTTPSChecks1h          *float64 `json:"https_checks_1h"`
	HTTPSPassRate1h        *float64 `json:"https_pass_rate_1h"`
	TamperChecks1h         *float64 `json:"tamper_checks_1h"`
	Tampered1h             *float64 `json:"tampered_1h"`
	InternalAPIFailures15m *float64 `json:"internal_api_failures_15m"`
	GostRouterAvailable    *float64 `json:"gost_router_available"`
	GostRouterDesired      *float64 `json:"gost_router_desired"`
}

type dashboardPayload struct {
	Status        string                       `json:"status"`
	Error         string                       `json:"error,omitempty"`
	Stale         bool                         `json:"stale,omitempty"`
	CheckedAt     string                       `json:"checked_at"`
	Range         string                       `json:"range,omitempty"`
	Current       map[string]float64           `json:"current,omitempty"`
	Rates         map[string]float64           `json:"rates,omitempty"`
	Totals        map[string]float64           `json:"totals,omitempty"`
	Traffic       map[string]dashboardTraffic  `json:"traffic,omitempty"`
	Validation    dashboardValidation          `json:"validation"`
	Distributions map[string][]dashboardBucket `json:"distributions,omitempty"`
	Trends        map[string][]dashboardPoint  `json:"trends,omitempty"`
	Health        *dashboardHealth             `json:"health,omitempty"`
}

var dashboardHealthQueries = []struct {
	name string
	expr string
}{
	{name: "https_checks_1h", expr: `sum(increase(freeproxyapi_https_probe_results_total[1h]))`},
	{name: "https_ok_1h", expr: `sum(increase(freeproxyapi_https_probe_results_total{outcome="ok"}[1h]))`},
	{name: "tamper_checks_1h", expr: `sum(increase(freeproxyapi_echo_tamper_results_total[1h]))`},
	{name: "tampered_1h", expr: `sum(increase(freeproxyapi_echo_tamper_results_total{outcome="tampered"}[1h]))`},
	{name: "internal_api_failures_15m", expr: `sum(increase(freeproxyapi_internal_api_query_failures_total[15m]))`},
	{name: "gost_router_available", expr: `max(kube_deployment_status_replicas_available{namespace="freeproxyapi",deployment="gost-router"})`},
	{name: "gost_router_desired", expr: `max(kube_deployment_spec_replicas{namespace="freeproxyapi",deployment="gost-router"})`},
}

func dashboardHealthQuery() string {
	expressions := make([]string, 0, len(dashboardHealthQueries))
	for _, query := range dashboardHealthQueries {
		expressions = append(expressions, labelDashboardExpression(query.expr, query.name))
	}
	return joinDashboardExpressions(expressions)
}

// buildDashboardHealth decodes the labeled health vector. Missing series stay
// nil so the JSON field is null rather than a misleading zero.
func buildDashboardHealth(series []prometheusVector) *dashboardHealth {
	values := dashboardMetricValues(series)
	value := func(name string) *float64 {
		v, ok := values[name]
		if !ok {
			return nil
		}
		return &v
	}
	health := &dashboardHealth{
		HTTPSChecks1h:          value("https_checks_1h"),
		TamperChecks1h:         value("tamper_checks_1h"),
		Tampered1h:             value("tampered_1h"),
		InternalAPIFailures15m: value("internal_api_failures_15m"),
		GostRouterAvailable:    value("gost_router_available"),
		GostRouterDesired:      value("gost_router_desired"),
	}
	if total := health.HTTPSChecks1h; total != nil && *total > 0 {
		var ok float64
		if passed := value("https_ok_1h"); passed != nil {
			ok = *passed
		}
		rate := 100 * ok / *total
		health.HTTPSPassRate1h = &rate
	}
	return health
}

var dashboardRateQueries = []struct {
	name string
	expr string
}{
	{name: "probe_results", expr: `rate(freeproxyapi_probe_results_total[15m])`},
	{name: "source_refreshes", expr: `rate(freeproxyapi_source_refreshes_total[15m])`},
	{name: "source_records_added", expr: `rate(freeproxyapi_source_records_added_total[15m])`},
	{name: "source_records_rejected", expr: `rate(freeproxyapi_source_records_rejected_total[15m])`},
	{name: "source_fetch_failures", expr: `rate(freeproxyapi_source_fetch_failures_total[15m])`},
	{name: "source_fetch_failures_by_reason", expr: `rate(freeproxyapi_source_fetch_failures_by_reason_total[15m])`},
	{name: "source_fetch_retries", expr: `rate(freeproxyapi_source_fetch_retries_total[15m])`},
	{name: "source_fetch_recovered", expr: `rate(freeproxyapi_source_fetch_recovered_total[15m])`},
	{name: "claims", expr: `rate(freeproxyapi_claims_total[15m])`},
	{name: "budget_denials", expr: `rate(freeproxyapi_budget_denials_total[15m])`},
	{name: "lease_reclaims", expr: `rate(freeproxyapi_lease_reclaims_total[15m])`},
	{name: "result_conflicts", expr: `rate(freeproxyapi_result_conflicts_total[15m])`},
	{name: "candidates_evicted", expr: `rate(freeproxyapi_candidates_evicted_total[15m])`},
}

var dashboardTotalQueries = []struct {
	name string
	expr string
}{
	{name: "processed_records", expr: `sum(increase(freeproxyapi_source_records_added_total[365d]))`},
	{name: "validation_attempts", expr: `sum(increase(freeproxyapi_probe_results_total{outcome=~"ok|failed"}[365d]))`},
}

type dashboardTrafficQuerySpec struct {
	name string
	expr string
}

func labelDashboardExpression(expression, name string) string {
	return fmt.Sprintf(`label_replace(%s, "%s", "%s", "__name__", ".*")`, expression, dashboardMetricLabel, name)
}

func joinDashboardExpressions(expressions []string) string {
	return strings.Join(expressions, " or ")
}

func dashboardMetricsQuery() string {
	return `{__name__=~"freeproxyapi_(candidates|source_unique|source_records_parsed|sources_succeeded|pending|pending_due|pending_next_due_in_seconds|leased|validated|inflight_probes|validated_stable|geoip_enabled|validated_geo_mismatch|validated_by_country|validated_latency_band|validated_by_anonymity|validated_by_asn)"}`
}

func dashboardTrendsQuery() string {
	return joinDashboardExpressions([]string{
		labelDashboardExpression(`max(freeproxyapi_validated)`, "validated"),
		labelDashboardExpression(`max(freeproxyapi_validated_stable)`, "stable"),
	})
}

func dashboardRatesQuery() string {
	expressions := make([]string, 0, len(dashboardRateQueries))
	for _, query := range dashboardRateQueries {
		expressions = append(expressions, labelDashboardExpression(query.expr, query.name))
	}
	return joinDashboardExpressions(expressions)
}

func dashboardTotalsQuery() string {
	expressions := make([]string, 0, len(dashboardTotalQueries))
	for _, query := range dashboardTotalQueries {
		expressions = append(expressions, labelDashboardExpression(query.expr, query.name))
	}
	return joinDashboardExpressions(expressions)
}

func dashboardTrafficQueries() []dashboardTrafficQuerySpec {
	queries := []dashboardTrafficQuerySpec{
		{name: "validation_upload_per_second", expr: `sum(rate(freeproxyapi_probe_upload_bytes_total[5m]))`},
		{name: "validation_download_per_second", expr: `sum(rate(freeproxyapi_probe_download_bytes_total[5m]))`},
		{name: "gost_upload_per_second", expr: `sum(rate(container_network_transmit_bytes_total{namespace="freeproxyapi",pod=~"gost-router-.*",interface="eth0"}[5m]))`},
		{name: "gost_download_per_second", expr: `sum(rate(container_network_receive_bytes_total{namespace="freeproxyapi",pod=~"gost-router-.*",interface="eth0"}[5m]))`},
	}
	for _, target := range []struct {
		prefix string
		expr   string
	}{
		{prefix: "validation_upload", expr: `sum(increase(freeproxyapi_probe_upload_bytes_total[%s]))`},
		{prefix: "validation_download", expr: `sum(increase(freeproxyapi_probe_download_bytes_total[%s]))`},
		{prefix: "gost_upload", expr: `sum(increase(container_network_transmit_bytes_total{namespace="freeproxyapi",pod=~"gost-router-.*",interface="eth0"}[%s]))`},
		{prefix: "gost_download", expr: `sum(increase(container_network_receive_bytes_total{namespace="freeproxyapi",pod=~"gost-router-.*",interface="eth0"}[%s]))`},
	} {
		for _, window := range dashboardTrafficWindows {
			queries = append(queries, dashboardTrafficQuerySpec{
				name: target.prefix + "_" + window.name,
				expr: fmt.Sprintf(target.expr, window.duration),
			})
		}
	}
	return queries
}

func dashboardTrafficQuery() string {
	queries := dashboardTrafficQueries()
	expressions := make([]string, 0, len(queries))
	for _, query := range queries {
		expressions = append(expressions, labelDashboardExpression(query.expr, query.name))
	}
	return joinDashboardExpressions(expressions)
}

func dashboardMetricValues(series []prometheusVector) map[string]float64 {
	values := make(map[string]float64, len(series))
	for _, sample := range series {
		name := sample.Metric[dashboardMetricLabel]
		if name == "" {
			continue
		}
		if value, ok := prometheusSampleValue(sample.Value); ok {
			values[name] += value
		}
	}
	return values
}

func buildDashboardTrafficSummary(values map[string]float64, prefix string) dashboardTrafficSummary {
	var summary dashboardTrafficSummary
	if value, ok := values[prefix+"_per_second"]; ok {
		summary.Available = true
		summary.PerSecond = value
	}
	for _, window := range dashboardTrafficWindows {
		value, ok := values[prefix+"_"+window.name]
		if !ok {
			continue
		}
		summary.Available = true
		switch window.name {
		case "day":
			summary.Day = value
		case "week":
			summary.Week = value
		case "month":
			summary.Month = value
		case "total":
			summary.Total = value
		}
	}
	return summary
}

func (r *Runner) dashboardData(ctx context.Context, spec dashboardRangeSpec) (dashboardPayload, error) {
	if r.prometheus == nil {
		return dashboardPayload{}, errPrometheusNotConfigured
	}
	return r.prometheus.cachedDashboard(ctx, spec, r.dashboardDataFresh)
}

func (p *prometheusClient) cachedDashboard(ctx context.Context, spec dashboardRangeSpec, load func(context.Context, dashboardRangeSpec) (dashboardPayload, error)) (dashboardPayload, error) {
	spec = dashboardRange(spec.key)
	now := time.Now()
	p.cacheMu.Lock()
	if entry, ok := p.cache[spec.key]; ok && now.Before(entry.expiresAt) {
		p.cacheMu.Unlock()
		return entry.payload, entry.err
	}
	flight := p.flights[spec.key]
	if flight == nil {
		flight = &dashboardFlight{done: make(chan struct{})}
		p.flights[spec.key] = flight
		go p.refreshDashboard(spec, flight, load)
	}
	p.cacheMu.Unlock()
	select {
	case <-ctx.Done():
		return dashboardPayload{}, ctx.Err()
	case <-flight.done:
		return flight.payload, flight.err
	}
}

func (p *prometheusClient) refreshDashboard(spec dashboardRangeSpec, flight *dashboardFlight, load func(context.Context, dashboardRangeSpec) (dashboardPayload, error)) {
	ctx, cancel := context.WithTimeout(context.Background(), dashboardRefreshTimeout)
	defer cancel()
	select {
	case p.refreshes <- struct{}{}:
		defer func() { <-p.refreshes }()
	case <-ctx.Done():
		p.finishDashboardRefresh(spec, flight, dashboardPayload{}, ctx.Err())
		return
	}
	payload, err := load(ctx, spec)
	p.finishDashboardRefresh(spec, flight, payload, err)
}

func (p *prometheusClient) finishDashboardRefresh(spec dashboardRangeSpec, flight *dashboardFlight, payload dashboardPayload, err error) {
	ttl := dashboardCacheFreshness
	if err != nil {
		ttl = dashboardFailureCooldown
	}
	p.cacheMu.Lock()
	if err == nil && payload.Status == "ok" {
		p.lastGood[spec.key] = payload
	}
	if err != nil {
		// Serve the last successful snapshot, marked stale, for as long as
		// refreshes keep failing (not only for the first failure after it).
		if previous, ok := p.lastGood[spec.key]; ok {
			payload = previous
			payload.Status = "degraded"
			payload.Error = "dashboard data unavailable"
			payload.Stale = true
			err = nil
		}
	}
	flight.payload = payload
	flight.err = err
	p.cache[spec.key] = dashboardCacheEntry{payload: payload, err: err, expiresAt: time.Now().Add(ttl)}
	delete(p.flights, spec.key)
	close(flight.done)
	p.cacheMu.Unlock()
}

func publicDashboardError(err error) string {
	if errors.Is(err, errPrometheusNotConfigured) {
		return errPrometheusNotConfigured.Error()
	}
	return "dashboard data unavailable"
}

// sharedTotals returns the range-independent totals and traffic vectors,
// refreshing them at most once per dashboardCacheFreshness across all ranges.
// Concurrent callers share one flight; the flight runs detached from any one
// caller's cancellation. Failures are shared with current waiters but not
// cached.
func (p *prometheusClient) sharedTotals(ctx context.Context) (dashboardTotals, error) {
	p.totalsMu.Lock()
	if time.Now().Before(p.totalsExpiresAt) {
		result := p.totals
		p.totalsMu.Unlock()
		return result, nil
	}
	flight := p.totalsFlight
	if flight == nil {
		flight = &dashboardTotalsFlight{done: make(chan struct{})}
		p.totalsFlight = flight
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dashboardRefreshTimeout)
		go func() {
			defer cancel()
			result, err := p.loadTotals(loadCtx)
			p.totalsMu.Lock()
			flight.result, flight.err = result, err
			if err == nil {
				p.totals = result
				p.totalsExpiresAt = time.Now().Add(dashboardCacheFreshness)
			}
			p.totalsFlight = nil
			close(flight.done)
			p.totalsMu.Unlock()
		}()
	}
	p.totalsMu.Unlock()
	select {
	case <-ctx.Done():
		return dashboardTotals{}, ctx.Err()
	case <-flight.done:
		return flight.result, flight.err
	}
}

func (p *prometheusClient) loadTotals(ctx context.Context) (dashboardTotals, error) {
	var result dashboardTotals
	var totalsErr, trafficErr, healthErr error
	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		result.totals, totalsErr = p.query(ctx, dashboardTotalsQuery())
	}()
	go func() {
		defer wg.Done()
		result.traffic, trafficErr = p.query(ctx, dashboardTrafficQuery())
	}()
	go func() {
		defer wg.Done()
		result.health, healthErr = p.query(ctx, dashboardHealthQuery())
	}()
	wg.Wait()
	for _, err := range []error{totalsErr, trafficErr, healthErr} {
		if err != nil {
			return dashboardTotals{}, err
		}
	}
	return result, nil
}

// dashboardQueryResults holds the raw inputs one dashboard refresh fetched.
type dashboardQueryResults struct {
	current, rates []prometheusVector
	trends         []prometheusMatrix
	shared         dashboardTotals
	stats          store.Stats
	statsNow       time.Time
}

func (r *Runner) dashboardDataFresh(ctx context.Context, spec dashboardRangeSpec) (dashboardPayload, error) {
	results, err := r.fetchDashboardQueries(ctx, spec)
	if err != nil {
		return dashboardPayload{}, err
	}
	return r.buildDashboardPayload(spec, results), nil
}

// fetchDashboardQueries runs every dashboard query concurrently and returns
// the first error in a fixed order once all have finished.
func (r *Runner) fetchDashboardQueries(ctx context.Context, spec dashboardRangeSpec) (dashboardQueryResults, error) {
	// A cold refresh issues at most six Prometheus requests, all concurrently.
	// Each independent expression is tagged before being combined with OR so
	// its result can be decoded. The totals, traffic, and health requests are
	// shared across ranges (see sharedTotals), so a warm refresh for another
	// range issues only the current, trend, and rate requests.
	var (
		current, rates       []prometheusVector
		trends               []prometheusMatrix
		shared               dashboardTotals
		stats                store.Stats
		statsNow             = time.Now().UTC()
		currentErr, trendErr error
		ratesErr, sharedErr  error
		statsErr             error
		wg                   sync.WaitGroup
	)
	wg.Add(4)
	go func() {
		defer wg.Done()
		current, currentErr = r.prometheus.query(ctx, dashboardMetricsQuery())
	}()
	go func() {
		defer wg.Done()
		trends, trendErr = r.prometheus.queryRange(ctx, dashboardTrendsQuery(), spec)
	}()
	go func() {
		defer wg.Done()
		rates, ratesErr = r.prometheus.query(ctx, dashboardRatesQuery())
	}()
	go func() {
		defer wg.Done()
		shared, sharedErr = r.prometheus.sharedTotals(ctx)
	}()
	if r.store != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stats, statsErr = r.readStore().StatsAt(ctx, statsNow)
		}()
	}
	wg.Wait()
	for _, err := range []error{currentErr, trendErr, ratesErr, sharedErr, statsErr} {
		if err != nil {
			return dashboardQueryResults{}, err
		}
	}
	return dashboardQueryResults{
		current:  current,
		rates:    rates,
		trends:   trends,
		shared:   shared,
		stats:    stats,
		statsNow: statsNow,
	}, nil
}

// buildDashboardPayload assembles the dashboard response from fetched query
// results.
func (r *Runner) buildDashboardPayload(spec dashboardRangeSpec, results dashboardQueryResults) dashboardPayload {
	current, rates, trends := results.current, results.rates, results.trends
	stats, statsNow := results.stats, results.statsNow
	totals, traffic := results.shared.totals, results.shared.traffic

	payload := dashboardPayload{
		Status:        "ok",
		CheckedAt:     time.Now().UTC().Format(time.RFC3339),
		Range:         spec.key,
		Current:       make(map[string]float64),
		Rates:         make(map[string]float64),
		Totals:        make(map[string]float64),
		Traffic:       make(map[string]dashboardTraffic),
		Validation:    dashboardValidation{State: "idle"},
		Distributions: make(map[string][]dashboardBucket),
		Trends:        make(map[string][]dashboardPoint),
	}
	distributionValues := map[string]map[string]float64{
		"country": {}, "latency_band": {}, "anonymity": {}, "asn": {},
	}
	for _, sample := range current {
		metric := sample.Metric["__name__"]
		family := ""
		label := ""
		switch metric {
		case "freeproxyapi_validated_by_country":
			family, label = "country", sample.Metric["country"]
		case "freeproxyapi_validated_latency_band":
			family, label = "latency_band", sample.Metric["band"]
		case "freeproxyapi_validated_by_anonymity":
			family, label = "anonymity", sample.Metric["class"]
		case "freeproxyapi_validated_by_asn":
			family, label = "asn", sample.Metric["asn"]
		}
		if family != "" {
			value, ok := prometheusSampleValue(sample.Value)
			if label != "" && ok && value > distributionValues[family][label] {
				distributionValues[family][label] = value
			}
			continue
		}
		name := strings.TrimPrefix(metric, "freeproxyapi_")
		if name == "validated_stable" {
			name = "stable"
		}
		value, ok := prometheusSampleValue(sample.Value)
		if !ok {
			continue
		}
		if name == "inflight_probes" {
			payload.Current[name] += value
		} else if value > payload.Current[name] {
			payload.Current[name] = value
		}
	}
	if r.store != nil {
		now := statsNow
		// SourceUnique is persisted in Redis by the refresh owner. Reading it
		// here avoids stale max-across-replica values when a new rescan count
		// decreases before every replica's local Prometheus gauge catches up.
		payload.Current["source_unique"] = float64(stats.SourceUnique)
		payload.Current["source_records_parsed"] = float64(stats.SourceParsed)
		payload.Current["sources_succeeded"] = float64(stats.SourcesSucceeded)
		switch {
		case stats.Leased > 0:
			payload.Validation = dashboardValidation{State: "running", InFlight: stats.Leased}
		case stats.HasNextDue && !stats.NextDueAt.After(now):
			payload.Validation = dashboardValidation{State: "due"}
		case stats.HasNextDue:
			payload.Validation = dashboardValidation{State: "scheduled", NextAt: stats.NextDueAt.UTC().Format(time.RFC3339)}
		}
	}

	for family, values := range distributionValues {
		buckets := make([]dashboardBucket, 0, len(values))
		for label, value := range values {
			buckets = append(buckets, dashboardBucket{Label: label, Value: value})
		}
		sort.Slice(buckets, func(i, j int) bool {
			if buckets[i].Value == buckets[j].Value {
				return buckets[i].Label < buckets[j].Label
			}
			return buckets[i].Value > buckets[j].Value
		})
		limit := 12
		if family == "latency_band" || family == "anonymity" {
			limit = 8
		}
		if len(buckets) > limit {
			buckets = buckets[:limit]
		}
		payload.Distributions[family] = buckets
	}

	for _, sample := range rates {
		name := sample.Metric[dashboardMetricLabel]
		if name == "" {
			continue
		}
		label := sample.Metric["outcome"]
		if label == "" {
			label = sample.Metric["result"]
		}
		key := name
		if label != "" {
			key += "_" + label
		}
		if value, ok := prometheusSampleValue(sample.Value); ok {
			payload.Rates[key] += value
		}
	}
	for name, value := range dashboardMetricValues(totals) {
		payload.Totals[name] = value
	}
	trafficValues := dashboardMetricValues(traffic)
	payload.Traffic["validation"] = dashboardTraffic{
		Upload:   buildDashboardTrafficSummary(trafficValues, "validation_upload"),
		Download: buildDashboardTrafficSummary(trafficValues, "validation_download"),
	}
	payload.Traffic["gost"] = dashboardTraffic{
		Upload:   buildDashboardTrafficSummary(trafficValues, "gost_upload"),
		Download: buildDashboardTrafficSummary(trafficValues, "gost_download"),
	}
	trendValues := make(map[string][]prometheusMatrix, 2)
	for _, series := range trends {
		name := series.Metric[dashboardMetricLabel]
		if name != "" {
			trendValues[name] = append(trendValues[name], series)
		}
	}
	payload.Trends["validated"] = prometheusTrendPoints(trendValues["validated"])
	payload.Trends["stable"] = prometheusTrendPoints(trendValues["stable"])
	payload.Health = buildDashboardHealth(results.shared.health)
	return payload
}

var dashboardTrafficWindows = []struct {
	name     string
	duration string
}{
	{name: "day", duration: "24h"},
	{name: "week", duration: "168h"},
	{name: "month", duration: "720h"},
	{name: "total", duration: "8760h"},
}

func prometheusSampleValue(raw []json.RawMessage) (float64, bool) {
	if len(raw) < 2 {
		return 0, false
	}
	var value string
	if err := json.Unmarshal(raw[1], &value); err != nil {
		return 0, false
	}
	number, err := strconv.ParseFloat(value, 64)
	return number, err == nil
}

func prometheusTrendPoints(series []prometheusMatrix) []dashboardPoint {
	if len(series) == 0 {
		return []dashboardPoint{}
	}
	points := make([]dashboardPoint, 0, len(series[0].Values))
	for _, pair := range series[0].Values {
		if len(pair) < 2 {
			continue
		}
		var timestamp float64
		var value string
		if json.Unmarshal(pair[0], &timestamp) != nil || json.Unmarshal(pair[1], &value) != nil {
			continue
		}
		number, err := strconv.ParseFloat(value, 64)
		if err == nil {
			points = append(points, dashboardPoint{Time: timestamp, Value: number})
		}
	}
	return points
}
