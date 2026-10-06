package monitor

import (
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

var probeDurationBuckets = [...]float64{0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 15, 30}

// probeKinds labels whether a probe was a candidate's first check or a retest.
var probeKinds = [2]string{"first", "retest"}

// probeOutcomeLabels index the outcome dimension (0 failed, 1 ok) to match
// freeproxyapi_probe_results_total.
var probeOutcomeLabels = [2]string{"failed", "ok"}

// probeReasons is the bounded reason label set; unknown codes map to "other".
var probeReasons = probe.ErrorCodes()

var probeReasonIndex = func() map[string]int {
	index := make(map[string]int, len(probeReasons))
	for i, reason := range probeReasons {
		index[reason] = i
	}
	return index
}()

type durationHistogram struct {
	buckets [len(probeDurationBuckets)]atomic.Int64
	count   atomic.Int64
	sumNS   atomic.Int64
}

func (h *durationHistogram) observe(duration time.Duration) {
	if duration < 0 {
		duration = 0
	}
	seconds := duration.Seconds()
	for i, bucket := range probeDurationBuckets {
		if seconds <= bucket {
			h.buckets[i].Add(1)
		}
	}
	h.count.Add(1)
	h.sumNS.Add(duration.Nanoseconds())
}

type targetProbeMetrics struct {
	outcomes        [2]atomic.Int64
	durationBuckets [len(probeDurationBuckets)]atomic.Int64
	durationCount   atomic.Int64
	durationSumNS   atomic.Int64
}

// Metrics is a dependency-free Prometheus text-format collector covering the
// audit control surfaces: source refreshes, claims, lease reclaims, probe
// outcomes, and global-budget denials. Target-labeled probe metrics expose only
// the configured scheme, host, and path (never query strings or credentials).
type Metrics struct {
	sourceRefreshTotal      [4]atomic.Int64 // ok, error, skipped, canceled
	sourceFailures          sourceFailureMetrics
	sourceRecordsAdded      atomic.Int64
	sourceRecordsReject     atomic.Int64
	sourcesTruncated        atomic.Int64
	candidatesCapped        atomic.Int64
	sourceFetchFailures     atomic.Int64
	sourceFetchRetries      atomic.Int64
	sourceFetchRecovered    atomic.Int64
	claimsTotal             atomic.Int64
	reclaimsTotal           atomic.Int64
	probeResultsTotal       [2]atomic.Int64
	probeUploadBytes        atomic.Int64
	probeDownloadBytes      atomic.Int64
	budgetDenialsTotal      atomic.Int64
	resultConflictsTotal    atomic.Int64
	evictionsTotal          atomic.Int64
	discardedTotal          atomic.Int64
	sourceTombstoned        atomic.Int64
	internalAPIFailures     atomic.Int64
	publicQueryFailures     atomic.Int64
	publicStaleResponses    atomic.Int64
	publicLargePageRequests atomic.Int64
	publicRefreshFailures   atomic.Int64

	candidatesGauge       atomic.Int64
	sourceUniqueGauge     atomic.Int64
	sourceParsedGauge     atomic.Int64
	sourcesSucceededGauge atomic.Int64
	pendingGauge          atomic.Int64
	pendingDueGauge       atomic.Int64
	oldestDueAgeGauge     atomic.Int64
	nextDueInGauge        atomic.Int64
	leasedGauge           atomic.Int64
	validatedGauge        atomic.Int64
	inflightGauge         atomic.Int64
	stableGauge           atomic.Int64
	geoipGauge            atomic.Int64
	geoMismatch           atomic.Int64

	dynamicMu sync.Mutex
	dynamic   map[string]int64

	targetMu      sync.Mutex
	targetMetrics map[string]*targetProbeMetrics

	// probeOutcomes is indexed [kind][outcome][reason]; allocated lazily per
	// reason slice length so the reason set stays defined by probe.ErrorCodes.
	probeOutcomes  [2][2][]atomic.Int64
	probeDurations [2][2]durationHistogram

	redisPoolsMu sync.Mutex
	redisPools   []redisPoolSource
}

// redisPoolSource is one named Redis client whose pool counters are read at
// scrape time.
type redisPoolSource struct {
	name  string
	stats func() store.PoolStats
}

// RegisterRedisPool exposes a Redis client's connection-pool counters under
// pool=name. Registering a name again replaces its source.
func (m *Metrics) RegisterRedisPool(name string, stats func() store.PoolStats) {
	m.redisPoolsMu.Lock()
	defer m.redisPoolsMu.Unlock()
	for i := range m.redisPools {
		if m.redisPools[i].name == name {
			m.redisPools[i].stats = stats
			return
		}
	}
	m.redisPools = append(m.redisPools, redisPoolSource{name: name, stats: stats})
	sort.Slice(m.redisPools, func(i, j int) bool { return m.redisPools[i].name < m.redisPools[j].name })
}

// appendRedisPoolMetrics writes the freeproxyapi_redis_pool_* families. Pool
// wait counters show whether a client is starved for connections; the pool
// label separates worker traffic from HTTP reads.
func (m *Metrics) appendRedisPoolMetrics(b *strings.Builder) {
	m.redisPoolsMu.Lock()
	pools := append([]redisPoolSource(nil), m.redisPools...)
	m.redisPoolsMu.Unlock()
	if len(pools) == 0 {
		return
	}
	snapshots := make([]store.PoolStats, len(pools))
	for i, pool := range pools {
		snapshots[i] = pool.stats()
	}
	type family struct {
		name, help, kind string
		value            func(store.PoolStats) string
	}
	integer := func(v uint32) string { return strconv.FormatUint(uint64(v), 10) }
	families := []family{
		{"freeproxyapi_redis_pool_size", "Configured maximum connections in the Redis client pool.", "gauge", func(s store.PoolStats) string { return strconv.Itoa(s.Size) }},
		{"freeproxyapi_redis_pool_connections", "Open connections in the Redis client pool.", "gauge", func(s store.PoolStats) string { return integer(s.TotalConns) }},
		{"freeproxyapi_redis_pool_idle_connections", "Idle connections in the Redis client pool.", "gauge", func(s store.PoolStats) string { return integer(s.IdleConns) }},
		{"freeproxyapi_redis_pool_pending_requests", "Commands currently waiting for or holding a pooled Redis connection.", "gauge", func(s store.PoolStats) string { return integer(s.PendingRequests) }},
		{"freeproxyapi_redis_pool_hits_total", "Pool gets that found a free idle connection.", "counter", func(s store.PoolStats) string { return integer(s.Hits) }},
		{"freeproxyapi_redis_pool_misses_total", "Pool gets that found no free idle connection.", "counter", func(s store.PoolStats) string { return integer(s.Misses) }},
		{"freeproxyapi_redis_pool_waits_total", "Pool gets that had to wait for a connection slot.", "counter", func(s store.PoolStats) string { return integer(s.WaitCount) }},
		{"freeproxyapi_redis_pool_wait_seconds_total", "Total time spent waiting for a pooled Redis connection.", "counter", func(s store.PoolStats) string {
			return strconv.FormatFloat(float64(s.WaitDurationNs)/float64(time.Second), 'g', -1, 64)
		}},
		{"freeproxyapi_redis_pool_timeouts_total", "Pool gets that timed out waiting for a connection.", "counter", func(s store.PoolStats) string { return integer(s.Timeouts) }},
		{"freeproxyapi_redis_pool_stale_connections_total", "Stale connections removed from the pool.", "counter", func(s store.PoolStats) string { return integer(s.StaleConns) }},
	}
	for _, f := range families {
		fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.kind)
		for i, pool := range pools {
			fmt.Fprintf(b, "%s{pool=%q} %s\n", f.name, pool.name, f.value(snapshots[i]))
		}
	}
}

func newMetrics() *Metrics {
	m := &Metrics{
		dynamic:       map[string]int64{},
		targetMetrics: map[string]*targetProbeMetrics{},
	}
	for kind := range m.probeOutcomes {
		for outcome := range m.probeOutcomes[kind] {
			m.probeOutcomes[kind][outcome] = make([]atomic.Int64, len(probeReasons))
		}
	}
	return m
}

// RecordProbeOutcome records one probe by kind (first check or retest),
// outcome, and bounded failure reason, plus a latency histogram by kind and
// outcome. It complements, and never replaces, RecordProbeResultForTarget.
func (m *Metrics) RecordProbeOutcome(retest bool, reason string, ok bool, duration time.Duration) {
	kind := 0
	if retest {
		kind = 1
	}
	outcome := 0
	if ok {
		outcome = 1
		reason = probe.CodeOK
	} else if reason == probe.CodeOK {
		reason = probe.CodeOther
	}
	index, known := probeReasonIndex[reason]
	if !known {
		index = probeReasonIndex[probe.CodeOther]
	}
	m.probeOutcomes[kind][outcome][index].Add(1)
	m.probeDurations[kind][outcome].observe(duration)
}

func (m *Metrics) RecordSourceRefresh(result string) {
	index := 1
	switch result {
	case "ok":
		index = 0
	case "skipped":
		index = 2
	case "canceled":
		index = 3
	}
	m.sourceRefreshTotal[index].Add(1)
}

func (m *Metrics) AddSourceRecords(added, rejected int64) {
	if added > 0 {
		m.sourceRecordsAdded.Add(added)
	}
	if rejected > 0 {
		m.sourceRecordsReject.Add(rejected)
	}
}

// AddSourcesTruncated counts feeds cut off at max_records_per_source.
func (m *Metrics) AddSourcesTruncated(n int64) {
	if n > 0 {
		m.sourcesTruncated.Add(n)
	}
}

// AddCandidatesCapped counts staged endpoints skipped because the candidate
// set had reached max_candidates.
func (m *Metrics) AddCandidatesCapped(n int64) {
	if n > 0 {
		m.candidatesCapped.Add(n)
	}
}

func (m *Metrics) AddSourceFetchFailures(n int64) {
	if n > 0 {
		m.sourceFetchFailures.Add(n)
	}
}

// AddSourceFetchRetries counts bounded retry attempts made for transient feed
// failures. A retry is counted even when the second attempt also fails.
func (m *Metrics) AddSourceFetchRetries(n int64) {
	if n > 0 {
		m.sourceFetchRetries.Add(n)
	}
}

// AddSourceFetchRecovered counts feeds that succeeded after a retry.
func (m *Metrics) AddSourceFetchRecovered(n int64) {
	if n > 0 {
		m.sourceFetchRecovered.Add(n)
	}
}

func (m *Metrics) RecordClaim() { m.claimsTotal.Add(1) }

func (m *Metrics) AddReclaims(n int64) {
	if n > 0 {
		m.reclaimsTotal.Add(n)
	}
}

func (m *Metrics) RecordProbeResult(ok bool) {
	index := 0
	if ok {
		index = 1
	}
	m.probeResultsTotal[index].Add(1)
}

// RecordProbeResultForTarget records the legacy aggregate outcome and a
// target-labeled outcome plus latency histogram. Target labels are normalized
// to scheme://host/path so credentials and query strings never enter metrics.
func (m *Metrics) RecordProbeResultForTarget(target string, ok bool, duration time.Duration) {
	m.RecordProbeResult(ok)
	target = normalizeProbeTargetLabel(target)
	m.targetMu.Lock()
	targetMetrics := m.targetMetrics[target]
	if targetMetrics == nil {
		targetMetrics = &targetProbeMetrics{}
		m.targetMetrics[target] = targetMetrics
	}
	m.targetMu.Unlock()

	outcome := 0
	if ok {
		outcome = 1
	}
	targetMetrics.outcomes[outcome].Add(1)
	if duration < 0 {
		duration = 0
	}
	seconds := duration.Seconds()
	for i, bucket := range probeDurationBuckets {
		if seconds <= bucket {
			targetMetrics.durationBuckets[i].Add(1)
		}
	}
	targetMetrics.durationCount.Add(1)
	targetMetrics.durationSumNS.Add(duration.Nanoseconds())
}

func normalizeProbeTargetLabel(raw string) string {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || u.Scheme == "" || u.Hostname() == "" {
		return "unknown"
	}
	host := strings.ToLower(u.Hostname())
	if port := u.Port(); port != "" && !((u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443")) {
		host += ":" + port
	}
	path := u.EscapedPath()
	if path == "" {
		path = "/"
	}
	return strings.ToLower(u.Scheme) + "://" + host + path
}

// AddProbeTraffic records bytes observed on validation connections. These are
// wire bytes, including proxy handshakes and TLS framing, and are intentionally
// separate from the GOST pod-network traffic counters.
func (m *Metrics) AddProbeTraffic(upload, download int64) {
	if upload > 0 {
		m.probeUploadBytes.Add(upload)
	}
	if download > 0 {
		m.probeDownloadBytes.Add(download)
	}
}

func (m *Metrics) AddEvictions(n int64) {
	if n > 0 {
		m.evictionsTotal.Add(n)
	}
}

// AddSourceTombstoned counts refresh endpoints skipped by an active eviction
// tombstone.
func (m *Metrics) AddSourceTombstoned(n int64) {
	if n > 0 {
		m.sourceTombstoned.Add(n)
	}
}

func (m *Metrics) AddDiscarded(n int64) {
	if n > 0 {
		m.discardedTotal.Add(n)
	}
}

func (m *Metrics) RecordBudgetDenial()   { m.budgetDenialsTotal.Add(1) }
func (m *Metrics) RecordResultConflict() { m.resultConflictsTotal.Add(1) }

// RecordInternalAPIQueryFailure counts internal API requests answered with 503
// because the validated-set query failed.
func (m *Metrics) RecordInternalAPIQueryFailure() { m.internalAPIFailures.Add(1) }

// RecordPublicQueryFailure counts public /proxies and /stats requests answered
// with 503 because the query failed or was shed.
func (m *Metrics) RecordPublicQueryFailure() { m.publicQueryFailures.Add(1) }

// RecordPublicStaleResponse counts public /proxies and /stats responses served
// from an expired cache entry. This is routine: the first request after a TTL
// expires is answered from the old entry while it refreshes.
func (m *Metrics) RecordPublicStaleResponse() { m.publicStaleResponses.Add(1) }

// RecordPublicLargePageRequest counts public /proxies requests that asked for a
// page larger than publicProxyDefaultLimit. When this stops growing, no client
// depends on large pages and public_max_limit can be lowered.
func (m *Metrics) RecordPublicLargePageRequest() { m.publicLargePageRequests.Add(1) }

// RecordPublicRefreshFailure counts failed background refreshes of an expired
// public response. While they persist, clients are served older data.
func (m *Metrics) RecordPublicRefreshFailure() { m.publicRefreshFailures.Add(1) }

func (m *Metrics) SetCounts(candidates, pending, leased, validated int64) {
	m.candidatesGauge.Store(candidates)
	m.pendingGauge.Store(pending)
	m.leasedGauge.Store(leased)
	m.validatedGauge.Store(validated)
}

// SetSourceUnique publishes the cross-source unique count from the latest
// completed source refresh.
func (m *Metrics) SetSourceUnique(n int64) { m.sourceUniqueGauge.Store(n) }

// SetSourceRefreshStats publishes aggregate counts from the latest completed
// source refresh.
func (m *Metrics) SetSourceRefreshStats(parsed, succeeded int64) {
	m.sourceParsedGauge.Store(parsed)
	m.sourcesSucceededGauge.Store(succeeded)
}

// SetPendingSchedule publishes queue timing without exposing proxy
// identities. Values are whole seconds; zero means there is no pending item
// due now (or the queue is empty).
func (m *Metrics) SetPendingSchedule(pendingDue int64, nextDueAt time.Time, hasNextDue bool, now time.Time) {
	if pendingDue < 0 {
		pendingDue = 0
	}
	m.pendingDueGauge.Store(pendingDue)
	if !hasNextDue {
		m.oldestDueAgeGauge.Store(0)
		m.nextDueInGauge.Store(0)
		return
	}
	if nextDueAt.After(now) {
		m.oldestDueAgeGauge.Store(0)
		m.nextDueInGauge.Store(int64(nextDueAt.Sub(now) / time.Second))
		return
	}
	m.nextDueInGauge.Store(0)
	age := int64(now.Sub(nextDueAt) / time.Second)
	if age < 0 {
		age = 0
	}
	m.oldestDueAgeGauge.Store(age)
}

func (m *Metrics) SetInflight(n int64) { m.inflightGauge.Store(n) }

func (m *Metrics) AddInflight(delta int64) { m.inflightGauge.Add(delta) }

// SetStable publishes the count of validated candidates whose recent success
// ratio is at or above the stability threshold.
func (m *Metrics) SetStable(n int64) { m.stableGauge.Store(n) }

// SetGeoMismatch publishes the count of validated candidates whose observed
// exit country differs from their entry-country annotation.
func (m *Metrics) SetGeoMismatch(n int64) { m.geoMismatch.Store(n) }

// SetGeoipEnabled publishes whether a GeoIP database is loaded (1/0).
func (m *Metrics) SetGeoipEnabled(v bool) {
	if v {
		m.geoipGauge.Store(1)
	} else {
		m.geoipGauge.Store(0)
	}
}

// SetLabeledGauge publishes an aggregate gauge with a label selector, e.g.
// freeproxyapi_validated_by_country{country="US"}. Values are replaced each
// refresh; labels that disappear drop out of the exposition.
func (m *Metrics) SetLabeledGauge(series string, value int64) {
	m.dynamicMu.Lock()
	defer m.dynamicMu.Unlock()
	m.dynamic[series] = value
}

// ReplaceLabeledGauges atomically swaps the full set of dynamically labeled
// series. Series absent from the new set drop out of the exposition; scrapes
// observe either the previous complete set or the new one, never a mix.
func (m *Metrics) ReplaceLabeledGauges(series map[string]int64) {
	next := make(map[string]int64, len(series))
	for name, value := range series {
		next[name] = value
	}
	m.dynamicMu.Lock()
	m.dynamic = next
	m.dynamicMu.Unlock()
}

type metricsSample struct {
	name  string
	help  string
	kind  string
	value func() int64
}

// Handler serves the Prometheus text exposition format for GET and HEAD.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodHead)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		samples := []metricsSample{
			// Families with labeled series expose only HELP/TYPE for the bare
			// name (nil value): an extra unlabeled total double-counts under sum().
			{"freeproxyapi_source_refreshes_total", "Inventory source refresh attempts.", "counter", nil},
			{`freeproxyapi_source_refreshes_total{result="ok"}`, "", "", m.sourceRefreshTotal[0].Load},
			{`freeproxyapi_source_refreshes_total{result="error"}`, "", "", m.sourceRefreshTotal[1].Load},
			{`freeproxyapi_source_refreshes_total{result="skipped"}`, "", "", m.sourceRefreshTotal[2].Load},
			{`freeproxyapi_source_refreshes_total{result="canceled"}`, "", "", m.sourceRefreshTotal[3].Load},
			{"freeproxyapi_source_records_added_total", "Inventory records accepted into the candidate set.", "counter", m.sourceRecordsAdded.Load},
			{"freeproxyapi_source_records_rejected_total", "Inventory records rejected at parse time.", "counter", m.sourceRecordsReject.Load},
			{"freeproxyapi_sources_truncated_total", "Feeds cut off at max_records_per_source during a refresh.", "counter", m.sourcesTruncated.Load},
			{"freeproxyapi_candidates_capped_total", "Staged endpoints skipped because the candidate set reached max_candidates.", "counter", m.candidatesCapped.Load},
			{"freeproxyapi_source_fetch_failures_total", "Individual source fetches or parses that failed this process lifetime.", "counter", m.sourceFetchFailures.Load},
			{"freeproxyapi_source_fetch_retries_total", "Bounded retry attempts made after transient source fetch failures.", "counter", m.sourceFetchRetries.Load},
			{"freeproxyapi_source_fetch_recovered_total", "Sources that succeeded after a bounded fetch retry.", "counter", m.sourceFetchRecovered.Load},
			{"freeproxyapi_claims_total", "Candidates atomically claimed by this replica.", "counter", m.claimsTotal.Load},
			{"freeproxyapi_lease_reclaims_total", "Expired leases reclaimed back to pending.", "counter", m.reclaimsTotal.Load},
			{"freeproxyapi_probe_results_total", "Probe outcomes by outcome label.", "counter", nil},
			{`freeproxyapi_probe_results_total{outcome="failed"}`, "", "", m.probeResultsTotal[0].Load},
			{`freeproxyapi_probe_results_total{outcome="ok"}`, "", "", m.probeResultsTotal[1].Load},
			{"freeproxyapi_probe_upload_bytes_total", "Wire bytes uploaded during validation probes.", "counter", m.probeUploadBytes.Load},
			{"freeproxyapi_probe_download_bytes_total", "Wire bytes downloaded during validation probes.", "counter", m.probeDownloadBytes.Load},
			{"freeproxyapi_budget_denials_total", "Probe attempts denied by the Redis-wide request budget.", "counter", m.budgetDenialsTotal.Load},
			{"freeproxyapi_result_conflicts_total", "Result commits rejected because the lease token no longer matched.", "counter", m.resultConflictsTotal.Load},
			{"freeproxyapi_candidates_evicted_total", "Candidates evicted after reaching max_consecutive_failures.", "counter", m.evictionsTotal.Load},
			{"freeproxyapi_candidates_discarded_total", "Failed candidates discarded by the explicit burst/initial-sweep policy.", "counter", m.discardedTotal.Load},
			{"freeproxyapi_internal_api_query_failures_total", "Internal API proxy queries that failed and returned 503.", "counter", m.internalAPIFailures.Load},
			{"freeproxyapi_public_query_failures_total", "Public /proxies and /stats requests that failed or were shed and returned 503.", "counter", m.publicQueryFailures.Load},
			{"freeproxyapi_public_stale_responses_total", "Public /proxies and /stats responses served from an expired cache entry while it refreshes.", "counter", m.publicStaleResponses.Load},
			{"freeproxyapi_public_large_page_requests_total", "Public /proxies requests that asked for a page larger than 100 results.", "counter", m.publicLargePageRequests.Load},
			{"freeproxyapi_public_refresh_failures_total", "Background refreshes of expired public /proxies and /stats responses that failed.", "counter", m.publicRefreshFailures.Load},
			{"freeproxyapi_source_tombstoned_total", "Source endpoints skipped at refresh because their eviction backoff tombstone was still active.", "counter", m.sourceTombstoned.Load},
			{"freeproxyapi_candidates", "Candidates known to the audit set.", "gauge", m.candidatesGauge.Load},
			{"freeproxyapi_source_unique", "Unique canonical endpoints in the latest completed source refresh.", "gauge", m.sourceUniqueGauge.Load},
			{"freeproxyapi_source_records_parsed", "Valid records parsed in the latest completed source refresh.", "gauge", m.sourceParsedGauge.Load},
			{"freeproxyapi_sources_succeeded", "Sources fetched successfully in the latest completed source refresh.", "gauge", m.sourcesSucceededGauge.Load},
			{"freeproxyapi_pending", "Candidates waiting to be claimed.", "gauge", m.pendingGauge.Load},
			{"freeproxyapi_pending_due", "Pending candidates due for validation now.", "gauge", m.pendingDueGauge.Load},
			{"freeproxyapi_pending_oldest_due_age_seconds", "Age in seconds of the oldest pending candidate due for validation.", "gauge", m.oldestDueAgeGauge.Load},
			{"freeproxyapi_pending_next_due_in_seconds", "Seconds until the earliest pending candidate is due for validation.", "gauge", m.nextDueInGauge.Load},
			{"freeproxyapi_leased", "Candidates currently leased across all replicas.", "gauge", m.leasedGauge.Load},
			{"freeproxyapi_validated", "Candidates whose latest probe succeeded.", "gauge", m.validatedGauge.Load},
			{"freeproxyapi_inflight_probes", "Probes in flight on this replica.", "gauge", m.inflightGauge.Load},
			{"freeproxyapi_validated_stable", "Validated candidates with ok_ratio_pct >= 80.", "gauge", m.stableGauge.Load},
			{"freeproxyapi_geoip_enabled", "Whether a GeoIP database is loaded for country/ASN annotation.", "gauge", m.geoipGauge.Load},
			{"freeproxyapi_validated_geo_mismatch", "Validated candidates whose exit country differs from their entry country.", "gauge", m.geoMismatch.Load},
		}
		m.dynamicMu.Lock()
		dynamicNames := make([]string, 0, len(m.dynamic))
		for name := range m.dynamic {
			dynamicNames = append(dynamicNames, name)
		}
		sort.Strings(dynamicNames)
		for _, name := range dynamicNames {
			value := m.dynamic[name]
			samples = append(samples, metricsSample{name: name, value: func() int64 { return value }})
		}
		m.dynamicMu.Unlock()

		sort.Slice(samples, func(i, j int) bool { return samples[i].name < samples[j].name })

		var b strings.Builder
		emittedFamily := map[string]bool{}
		for _, s := range samples {
			family := s.name
			if idx := strings.IndexByte(family, '{'); idx >= 0 {
				family = family[:idx]
				if !emittedFamily[family] {
					fmt.Fprintf(&b, "# TYPE %s gauge\n", family)
					emittedFamily[family] = true
				}
			} else {
				fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n", s.name, s.help, s.name, s.kind)
			}
			emittedFamily[family] = true
			if s.value == nil {
				continue
			}
			fmt.Fprintf(&b, "%s %d\n", s.name, s.value())
		}
		m.appendTargetProbeMetrics(&b)
		m.appendProbeOutcomeMetrics(&b)
		m.appendSourceFailureMetrics(&b)
		m.appendRedisPoolMetrics(&b)
		body := b.String()
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		if r.Method == http.MethodHead {
			return
		}
		_, _ = w.Write([]byte(body))
	})
}

func (m *Metrics) appendTargetProbeMetrics(b *strings.Builder) {
	m.targetMu.Lock()
	targets := make([]string, 0, len(m.targetMetrics))
	for target := range m.targetMetrics {
		targets = append(targets, target)
	}
	sort.Strings(targets)
	metrics := make(map[string]*targetProbeMetrics, len(targets))
	for _, target := range targets {
		metrics[target] = m.targetMetrics[target]
	}
	m.targetMu.Unlock()
	if len(targets) == 0 {
		return
	}

	fmt.Fprintln(b, "# HELP freeproxyapi_probe_results_by_target_total Probe outcomes labeled by normalized probe target.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_probe_results_by_target_total counter")
	for _, target := range targets {
		for outcome, label := range []string{"failed", "ok"} {
			fmt.Fprintf(b, "freeproxyapi_probe_results_by_target_total{outcome=%q,target=%q} %d\n", label, target, metrics[target].outcomes[outcome].Load())
		}
	}

	fmt.Fprintln(b, "# HELP freeproxyapi_probe_duration_seconds Time spent probing through a candidate proxy, labeled by normalized probe target.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_probe_duration_seconds histogram")
	for _, target := range targets {
		targetMetrics := metrics[target]
		for i, bucket := range probeDurationBuckets {
			fmt.Fprintf(b, "freeproxyapi_probe_duration_seconds_bucket{le=%q,target=%q} %d\n", strconv.FormatFloat(bucket, 'g', -1, 64), target, targetMetrics.durationBuckets[i].Load())
		}
		count := targetMetrics.durationCount.Load()
		fmt.Fprintf(b, "freeproxyapi_probe_duration_seconds_bucket{le=\"+Inf\",target=%q} %d\n", target, count)
		fmt.Fprintf(b, "freeproxyapi_probe_duration_seconds_sum{target=%q} %s\n", target, strconv.FormatFloat(float64(targetMetrics.durationSumNS.Load())/float64(time.Second), 'g', -1, 64))
		fmt.Fprintf(b, "freeproxyapi_probe_duration_seconds_count{target=%q} %d\n", target, count)
	}
}

// ---------------------------------------------------------------------------
// Probe-accuracy metrics (control probe, HTTPS sampling). Kept in a separate
// block appended to this file; they are emitted by withAccuracyMetrics, which
// wraps the base exposition handler.
// ---------------------------------------------------------------------------

type accuracyMetrics struct {
	controlUnhealthy      atomic.Int64
	controlChecks         [2]atomic.Int64 // failed, ok
	suppressedOutcomes    atomic.Int64
	httpsResults          [2]atomic.Int64 // failed, ok
	httpsProxyTLSFallback atomic.Int64
	httpsBudgetSkipped    atomic.Int64
	echoTamperResults     [2]atomic.Int64 // clean, tampered
	echoTamperReasons     [4]atomic.Int64 // indexed by echoTamperReasonLabels
}

// echoTamperReasonLabels are the reason label values of
// freeproxyapi_echo_tamper_reasons_total; header:<name> reasons share "header"
// so attacker-chosen header names never become label values.
var echoTamperReasonLabels = [4]string{"body", "nonce", "url", "header"}

func (a *accuracyMetrics) recordEchoTamper(tampered bool, reason string) {
	if !tampered {
		a.echoTamperResults[0].Add(1)
		return
	}
	a.echoTamperResults[1].Add(1)
	if strings.HasPrefix(reason, "header:") {
		reason = "header"
	}
	for i, label := range echoTamperReasonLabels {
		if label == reason {
			a.echoTamperReasons[i].Add(1)
			return
		}
	}
}

func (a *accuracyMetrics) appendTo(b *strings.Builder) {
	fmt.Fprintln(b, "# HELP freeproxyapi_control_probe_healthy Whether this replica's direct control fetch of probe_target is healthy (1) or probe workers are paused (0).")
	fmt.Fprintln(b, "# TYPE freeproxyapi_control_probe_healthy gauge")
	fmt.Fprintf(b, "freeproxyapi_control_probe_healthy %d\n", 1-a.controlUnhealthy.Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_control_probe_checks_total Direct control probe checks by outcome.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_control_probe_checks_total counter")
	fmt.Fprintf(b, "freeproxyapi_control_probe_checks_total{outcome=\"failed\"} %d\n", a.controlChecks[0].Load())
	fmt.Fprintf(b, "freeproxyapi_control_probe_checks_total{outcome=\"ok\"} %d\n", a.controlChecks[1].Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_probe_outcomes_suppressed_total Failed probe outcomes released uncommitted because the control probe was unhealthy.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_probe_outcomes_suppressed_total counter")
	fmt.Fprintf(b, "freeproxyapi_probe_outcomes_suppressed_total %d\n", a.suppressedOutcomes.Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_https_probe_results_total Sampled HTTPS-capability checks by outcome.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_https_probe_results_total counter")
	fmt.Fprintf(b, "freeproxyapi_https_probe_results_total{outcome=\"failed\"} %d\n", a.httpsResults[0].Load())
	fmt.Fprintf(b, "freeproxyapi_https_probe_results_total{outcome=\"ok\"} %d\n", a.httpsResults[1].Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_https_probe_proxy_tls_fallback_total https:// proxies whose own TLS handshake failed and were retried as plain CONNECT proxies.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_https_probe_proxy_tls_fallback_total counter")
	fmt.Fprintf(b, "freeproxyapi_https_probe_proxy_tls_fallback_total %d\n", a.httpsProxyTLSFallback.Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_https_probe_budget_skipped_total Sampled HTTPS checks skipped because the global request budget was exhausted.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_https_probe_budget_skipped_total counter")
	fmt.Fprintf(b, "freeproxyapi_https_probe_budget_skipped_total %d\n", a.httpsBudgetSkipped.Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_echo_tamper_results_total Anonymity echo checks compared for traffic tampering, by outcome.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_echo_tamper_results_total counter")
	fmt.Fprintf(b, "freeproxyapi_echo_tamper_results_total{outcome=\"clean\"} %d\n", a.echoTamperResults[0].Load())
	fmt.Fprintf(b, "freeproxyapi_echo_tamper_results_total{outcome=\"tampered\"} %d\n", a.echoTamperResults[1].Load())
	fmt.Fprintln(b, "# HELP freeproxyapi_echo_tamper_reasons_total Tampered anonymity echo checks by first detected reason.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_echo_tamper_reasons_total counter")
	for i, label := range echoTamperReasonLabels {
		fmt.Fprintf(b, "freeproxyapi_echo_tamper_reasons_total{reason=%q} %d\n", label, a.echoTamperReasons[i].Load())
	}
}

// bufferedMetricsResponse captures the base handler's response so the
// accuracy block can be appended with a correct Content-Length.
type bufferedMetricsResponse struct {
	header http.Header
	status int
	body   strings.Builder
}

func (b *bufferedMetricsResponse) Header() http.Header { return b.header }

func (b *bufferedMetricsResponse) WriteHeader(status int) {
	if b.status == 0 {
		b.status = status
	}
}

func (b *bufferedMetricsResponse) Write(p []byte) (int, error) {
	if b.status == 0 {
		b.status = http.StatusOK
	}
	return b.body.Write(p)
}

// withAccuracyMetrics serves inner's exposition followed by the accuracy
// metrics block. HEAD requests get the same headers as GET without a body.
func withAccuracyMetrics(inner http.Handler, a *accuracyMetrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		innerRequest := r
		if r.Method == http.MethodHead {
			innerRequest = r.Clone(r.Context())
			innerRequest.Method = http.MethodGet
		}
		captured := &bufferedMetricsResponse{header: http.Header{}}
		inner.ServeHTTP(captured, innerRequest)
		status := captured.status
		if status == 0 {
			status = http.StatusOK
		}
		body := captured.body.String()
		if status == http.StatusOK {
			var b strings.Builder
			b.WriteString(body)
			a.appendTo(&b)
			body = b.String()
		}
		for name, values := range captured.header {
			w.Header()[name] = values
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(status)
		if r.Method != http.MethodHead {
			_, _ = w.Write([]byte(body))
		}
	})
}

// ---------------------------------------------------------------------------
// Source refresh robustness: per-source fetch failures.
// Host and reason label values are produced by sourceFailureHost and
// classifySourceFetchError (refresh_retry.go); hosts are capped at
// sourceFailureHostCap distinct values with an "other" bucket.
// ---------------------------------------------------------------------------

type sourceFailureMetrics struct {
	mu      sync.Mutex
	counts  map[string]map[string]int64 // host -> reason -> count
	reasons map[string]int64
}

// RecordSourceFetchFailureBySource counts one failed source fetch. Unknown
// reasons fall into "other", and hosts beyond the cardinality cap share the
// "other" host bucket.
func (m *Metrics) RecordSourceFetchFailureBySource(host, reason string) {
	if !isSourceFailureReason(reason) {
		reason = "other"
	}
	if host == "" {
		host = sourceFailureHostOther
	}
	s := &m.sourceFailures
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = map[string]map[string]int64{}
	}
	if s.reasons == nil {
		s.reasons = map[string]int64{}
	}
	reasons, ok := s.counts[host]
	if !ok {
		if host != sourceFailureHostOther && len(s.counts) >= sourceFailureHostCap {
			host = sourceFailureHostOther
			reasons = s.counts[host]
		}
		if reasons == nil {
			reasons = map[string]int64{}
			s.counts[host] = reasons
		}
	}
	reasons[reason]++
	s.reasons[reason]++
}

func isSourceFailureReason(reason string) bool {
	for _, known := range sourceFailureReasons {
		if reason == known {
			return true
		}
	}
	return false
}

func (m *Metrics) appendSourceFailureMetrics(b *strings.Builder) {
	s := &m.sourceFailures
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.counts) > 0 {
		hosts := make([]string, 0, len(s.counts))
		for host := range s.counts {
			hosts = append(hosts, host)
		}
		sort.Strings(hosts)
		fmt.Fprintln(b, "# HELP freeproxyapi_source_fetch_failures_by_source_total Source fetch failures by feed hostname and bounded failure reason.")
		fmt.Fprintln(b, "# TYPE freeproxyapi_source_fetch_failures_by_source_total counter")
		for _, host := range hosts {
			for _, reason := range sourceFailureReasons {
				if count, ok := s.counts[host][reason]; ok {
					fmt.Fprintf(b, "freeproxyapi_source_fetch_failures_by_source_total{host=%q,reason=%q} %d\n", host, reason, count)
				}
			}
		}
	}
	fmt.Fprintln(b, "# HELP freeproxyapi_source_fetch_failures_by_reason_total Source fetch failures by bounded reason.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_source_fetch_failures_by_reason_total counter")
	for _, reason := range sourceFailureReasons {
		fmt.Fprintf(b, "freeproxyapi_source_fetch_failures_by_reason_total{reason=%q} %d\n", reason, s.reasons[reason])
	}
}

// appendProbeOutcomeMetrics writes the kind/outcome/reason families. Every
// valid combination is emitted (ok only with reason="ok", failed with every
// other reason) so rate() sees series from process start.
func (m *Metrics) appendProbeOutcomeMetrics(b *strings.Builder) {
	fmt.Fprintln(b, "# HELP freeproxyapi_probe_outcomes_total Probe outcomes by kind (first check or retest), outcome, and bounded failure reason code.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_probe_outcomes_total counter")
	for kind, kindLabel := range probeKinds {
		for outcome, outcomeLabel := range probeOutcomeLabels {
			for index, reason := range probeReasons {
				if (outcome == 1) != (reason == probe.CodeOK) {
					continue
				}
				fmt.Fprintf(b, "freeproxyapi_probe_outcomes_total{kind=%q,outcome=%q,reason=%q} %d\n", kindLabel, outcomeLabel, reason, m.probeOutcomes[kind][outcome][index].Load())
			}
		}
	}

	fmt.Fprintln(b, "# HELP freeproxyapi_probe_outcome_duration_seconds Time spent probing through a candidate proxy, by kind and outcome.")
	fmt.Fprintln(b, "# TYPE freeproxyapi_probe_outcome_duration_seconds histogram")
	for kind, kindLabel := range probeKinds {
		for outcome, outcomeLabel := range probeOutcomeLabels {
			h := &m.probeDurations[kind][outcome]
			// Load buckets before count: observe increments buckets first, so
			// the +Inf value written here is never below a finite bucket.
			var buckets [len(probeDurationBuckets)]int64
			for i := range buckets {
				buckets[i] = h.buckets[i].Load()
			}
			count := h.count.Load()
			for i, bucket := range probeDurationBuckets {
				fmt.Fprintf(b, "freeproxyapi_probe_outcome_duration_seconds_bucket{kind=%q,outcome=%q,le=%q} %d\n", kindLabel, outcomeLabel, strconv.FormatFloat(bucket, 'g', -1, 64), buckets[i])
			}
			fmt.Fprintf(b, "freeproxyapi_probe_outcome_duration_seconds_bucket{kind=%q,outcome=%q,le=\"+Inf\"} %d\n", kindLabel, outcomeLabel, count)
			fmt.Fprintf(b, "freeproxyapi_probe_outcome_duration_seconds_sum{kind=%q,outcome=%q} %s\n", kindLabel, outcomeLabel, strconv.FormatFloat(float64(h.sumNS.Load())/float64(time.Second), 'g', -1, 64))
			fmt.Fprintf(b, "freeproxyapi_probe_outcome_duration_seconds_count{kind=%q,outcome=%q} %d\n", kindLabel, outcomeLabel, count)
		}
	}
}
