package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/geoip"
	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	// Keep refreshes concurrent enough to drain the live inventory within one
	// fetch interval while still bounding upstream and Redis pressure. With
	// hundreds of feeds and a 90s per-feed deadline, eight workers can leave a
	// refresh running past the old lock lifetime before it can commit stats.
	sourceRefreshWorkers = 32
	// The refresh heartbeat extends this lease while a refresh is alive. A
	// short crash fallback lets a replacement replica recover promptly instead
	// of waiting for a full source refresh interval.
	sourceRefreshLockTTL = 10 * time.Minute
	// How long an aborted refresh (never reached a completed drain) keeps
	// holding the cluster-wide lock. Short enough that a rolling deploy
	// killing the lock holder doesn't strand every other replica behind it
	// for the rest of the fetch interval; long enough that a persistent
	// failure (e.g. Redis unreachable) doesn't turn into a tight retry loop
	// hammering every configured source.
	sourceRefreshFailureHold = 30 * time.Second
	// Match the bounded Redis staging batch so large feeds do not spend most
	// of a refresh in avoidable round trips.
	sourceStageBatch = 2000
)

type Runner struct {
	config Config
	// store carries worker traffic: claims, completions, permits, refreshes,
	// leases, and background stats.
	store *store.Redis
	// apiStore is a separate, small client for HTTP reads so a busy worker
	// pool cannot make public or internal queries wait for a connection. Nil
	// in tests that build a Runner directly; readStore falls back to store.
	apiStore         *store.Redis
	fetch            *source.Fetcher
	geo              *geoip.Resolver
	echo             *probe.EchoClient
	prometheus       *prometheusClient
	realIPMu         sync.Mutex
	realIP           string
	worker           string
	metrics          *Metrics
	probeTargetLabel string
	startedAt        time.Time
	nowFunc          func() time.Time
	scheduleChanged  *wakeNotifier
	workAvailable    *wakeNotifier
	refreshMu        sync.Mutex
	lastRefresh      refreshReport
	slicesMu         sync.Mutex
	slices           validatedSlices
	// draining is set once Run's context is cancelled so /readyz stops
	// advertising the replica while workers and background jobs finish.
	draining atomic.Bool
	// refreshing guards against overlapping source refreshes on one replica.
	refreshing atomic.Bool
	// refreshState tracks the active refresh context and lock-error retries.
	refreshState refreshRobustness
	// probeFunc defaults to probe.Test and exists so tests can simulate
	// probes deterministically.
	probeFunc func(ctx context.Context, proxyURL, targetURL string, timeout time.Duration) probe.Result
	// Echo operations are injectable so budget/failover behavior can be tested
	// without weakening the production client's public-address dial policy.
	echoCheckFunc   func(context.Context, string, string, string, time.Duration) probe.AnonymityResult
	echoRefreshFunc func(context.Context, string) (string, error)
	// accuracy holds control-probe health and HTTPS sampling state.
	accuracy accuracyState

	// statsLeader is the shared-aggregate lease (stats_leader.go); permits is
	// the local global-budget bucket (budget.go). Both are zero-value ready.
	statsLeader statsLeaderState
	permits     permitBucket
}

// wakeNotifier is a broadcast edge-triggered notification. Replacing the
// channel before closing it wakes every current waiter without retaining a
// growing list of worker channels.
type wakeNotifier struct {
	mu sync.Mutex
	ch chan struct{}
}

func newWakeNotifier() *wakeNotifier { return &wakeNotifier{ch: make(chan struct{})} }

func (n *wakeNotifier) channel() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ch
}

func (n *wakeNotifier) signal() {
	n.mu.Lock()
	old := n.ch
	n.ch = make(chan struct{})
	close(old)
	n.mu.Unlock()
}

func waitForSignal(ctx context.Context, n *wakeNotifier) {
	select {
	case <-ctx.Done():
	case <-n.channel():
	}
}

func waitForSignalOrTimer(ctx context.Context, n *wakeNotifier, delay time.Duration) {
	if delay <= 0 {
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-n.channel():
	case <-timer.C:
	}
}

const (
	// apiRedisPoolTimeout bounds how long an HTTP read waits for a pooled
	// connection. It is below every handler deadline (readyzTimeout is the
	// shortest) so a saturated pool fails fast with a pool-timeout error
	// instead of consuming the whole request budget.
	apiRedisPoolTimeout = 2 * time.Second
	// apiRedisMinIdleConns keeps a couple of warm connections so readiness
	// probes and the first query after an idle period do not dial.
	apiRedisMinIdleConns = 2
)

// workerRedisPoolOptions sizes the worker client. A quarter of the pool stays
// warm so a wake-up burst does not dial many connections at once.
func workerRedisPoolOptions(config Config) store.PoolOptions {
	size := config.workerRedisPoolSize()
	return store.PoolOptions{PoolSize: size, MinIdleConns: size / 4}
}

// apiRedisPoolOptions sizes the HTTP read client.
func apiRedisPoolOptions(config Config) store.PoolOptions {
	size := config.apiRedisPoolSize()
	return store.PoolOptions{PoolSize: size, MinIdleConns: min(apiRedisMinIdleConns, size), PoolTimeout: apiRedisPoolTimeout}
}

func NewRunner(config Config, worker string) (*Runner, error) {
	redisStore, err := store.NewRedisWithPool(config.RedisURL, config.Namespace, workerRedisPoolOptions(config))
	if err != nil {
		return nil, err
	}
	apiStore, err := store.NewRedisWithPool(config.RedisURL, config.Namespace, apiRedisPoolOptions(config))
	if err != nil {
		_ = redisStore.Close()
		return nil, err
	}
	if worker == "" {
		worker, _ = os.Hostname()
	}
	geoResolver, err := geoip.Open(config.GeoIPDBPath, config.GeoIPASNDBPath)
	if err != nil {
		return nil, fmt.Errorf("open geoip database: %w", err)
	}
	if !geoResolver.Enabled() && config.GeoIPDBPath != "" {
		return nil, fmt.Errorf("geoip database disabled despite configured path")
	}
	log.Printf("FreeProxyAPI geoip enabled=%t", geoResolver.Enabled())
	var echoClient *probe.EchoClient
	if echoURLs := config.ClassificationEchoURLs(); len(echoURLs) > 0 {
		echoClient = probe.NewEchoClient(config.RequestTimeout)
	}
	runner := &Runner{
		config:           config,
		store:            redisStore,
		apiStore:         apiStore,
		fetch:            source.NewFetcher(config.SourceRequestTimeout, config.SourceMaxBytes),
		geo:              geoResolver,
		echo:             echoClient,
		prometheus:       newPrometheusClient(config.PrometheusURL),
		worker:           worker,
		metrics:          newMetrics(),
		probeTargetLabel: normalizeProbeTargetLabel(config.ProbeTarget),
		startedAt:        time.Now(),
		nowFunc:          time.Now,
		probeFunc:        standardProbeFunc(config.ProbeExpectedBody, config.ProbeConnectTimeout),
		scheduleChanged:  newWakeNotifier(),
		workAvailable:    newWakeNotifier(),
	}
	if echoClient != nil {
		runner.echoCheckFunc = echoClient.Check
		runner.echoRefreshFunc = echoClient.RefreshRealIP
	}
	runner.metrics.RegisterRedisPool("workers", redisStore.PoolStats)
	runner.metrics.RegisterRedisPool("api", apiStore.PoolStats)
	log.Printf("FreeProxyAPI redis pools workers=%d api=%d", config.workerRedisPoolSize(), config.apiRedisPoolSize())
	return runner, nil
}

func (r *Runner) setValidatedSlices(v validatedSlices) {
	r.slicesMu.Lock()
	defer r.slicesMu.Unlock()
	r.slices = v
}

func (r *Runner) validatedSlicesSnapshot() validatedSlices {
	r.slicesMu.Lock()
	defer r.slicesMu.Unlock()
	return r.slices
}

// lastRefreshSnapshot returns the most recent source-refresh outcome for /report.
func (r *Runner) lastRefreshSnapshot() refreshReport {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	return r.lastRefresh
}

func (r *Runner) setLastRefresh(report refreshReport) {
	r.refreshMu.Lock()
	defer r.refreshMu.Unlock()
	r.lastRefresh = report
}

// Close releases the Redis store and the geoip databases. It is nil-safe and
// reports every close failure.
func (r *Runner) Close() error {
	if r == nil {
		return nil
	}
	var storeErr, apiStoreErr error
	if r.store != nil {
		storeErr = r.store.Close()
	}
	if r.apiStore != nil {
		apiStoreErr = r.apiStore.Close()
	}
	return errors.Join(storeErr, apiStoreErr, r.geo.Close())
}

// readStore returns the Redis client for HTTP read paths: the dedicated API
// client when configured, otherwise the shared worker client.
func (r *Runner) readStore() *store.Redis {
	if r.apiStore != nil {
		return r.apiStore
	}
	return r.store
}

func (r *Runner) Run(ctx context.Context) error {
	if err := r.store.Ping(ctx); err != nil {
		return fmt.Errorf("connect Redis: %w", err)
	}
	log.Printf("FreeProxyAPI monitor started worker=%s validation_enabled=%t", r.worker, r.config.NetworkValidationEnabled)
	r.metrics.SetGeoipEnabled(r.geo.Enabled())
	r.draining.Store(false)

	// Every goroutine that may touch Redis is tracked so Run only returns (and
	// the caller only closes the store) after all of them have exited.
	var background sync.WaitGroup
	goBackground := func(fn func()) {
		background.Add(1)
		go func() {
			defer background.Done()
			fn()
		}()
	}
	var health *healthServer
	if listenAddr := r.config.ListenAddr; listenAddr != "" && listenAddr != "-" {
		var err error
		health, err = startHealthServer(listenAddr, r)
		if err != nil {
			return fmt.Errorf("start health server on %s: %w", listenAddr, err)
		}
		// Deferred before the wait below runs, so /readyz keeps serving 503
		// "draining" until every worker and background job has finished.
		defer health.Shutdown()
	}
	if r.echo != nil {
		goBackground(func() { r.maintainRealIP(ctx) })
	}

	// The pending set can be large, so do not make health/readiness wait for
	// the one-shot requeue scan. RunOnceWithError still ensures that only one
	// replica performs the scan, while the other replicas remain available.
	if r.config.RequeuePendingOnStart {
		goBackground(func() {
			if err := r.requeuePendingOnStart(ctx); err != nil && ctx.Err() == nil {
				log.Printf("pending requeue failed: %v", err)
			}
		})
	}

	goBackground(func() { r.enrichOnStart(ctx) })

	// The startup refresh can take minutes for a large inventory. Workers
	// sleep on workAvailable until scheduleLoop sees due work, so they can
	// start immediately and drain already-pending candidates meanwhile.
	r.publishStats(ctx)
	r.startRefresh(ctx, goBackground)
	if r.config.NetworkValidationEnabled {
		goBackground(func() { r.scheduleLoop(ctx) })
	}
	if r.config.NetworkValidationEnabled && r.config.ControlProbeEnabled {
		goBackground(func() { r.controlProbeLoop(ctx) })
	}

	var workers sync.WaitGroup
	if r.config.NetworkValidationEnabled {
		for i := 0; i < r.config.Workers; i++ {
			workers.Add(1)
			go func() {
				defer workers.Done()
				r.workerLoop(ctx)
			}()
		}
	}

	refreshTicker := time.NewTicker(r.config.FetchInterval)
	reclaimTicker := time.NewTicker(time.Minute)
	statsTicker := time.NewTicker(statsPublishInterval)
	defer refreshTicker.Stop()
	defer reclaimTicker.Stop()
	defer statsTicker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-refreshTicker.C:
			// Refreshes run beside the loop so lease reclaim and stats keep
			// their cadence; startRefresh skips a tick while one is running.
			r.startRefresh(ctx, goBackground)
		case <-reclaimTicker.C:
			reclaimed, err := r.store.ReclaimExpired(ctx, r.nowFunc(), 256)
			r.metrics.AddReclaims(reclaimed)
			if err != nil && ctx.Err() == nil {
				log.Printf("lease reclaim failed: %v", err)
			} else if reclaimed > 0 {
				log.Printf("lease reclaim count=%d", reclaimed)
				r.scheduleChanged.signal()
			}
		case <-statsTicker.C:
			r.publishStats(ctx)
		}
	}

	r.draining.Store(true)
	workers.Wait()
	background.Wait()
	r.metrics.SetInflight(0)
	r.publishStats(context.Background())
	return nil
}

// startRefresh launches one source refresh via launch unless a refresh is
// already running on this replica. It reports whether a refresh was started.
// A Redis error acquiring the cluster-wide lock schedules a backoff retry
// (see scheduleLockRetry) through the same launch function and guard.
func (r *Runner) startRefresh(ctx context.Context, launch func(func())) bool {
	return r.tryStartRefresh(ctx, launch, false)
}

func (r *Runner) publishStats(ctx context.Context) {
	publishCtx, cancel := context.WithTimeout(ctx, readyzTimeout)
	defer cancel()
	now := r.nowFunc()
	stats, err := r.store.StatsAt(publishCtx, now)
	if err != nil && ctx.Err() == nil {
		log.Printf("stats failed: %v", err)
		return
	}
	if err != nil {
		return
	}
	r.metrics.SetCounts(stats.Candidates, stats.Pending, stats.Leased, stats.Validated)
	r.metrics.SetSourceUnique(stats.SourceUnique)
	r.metrics.SetSourceRefreshStats(stats.SourceParsed, stats.SourcesSucceeded)
	r.metrics.SetPendingSchedule(stats.PendingDue, stats.NextDueAt, stats.HasNextDue, now)

	slices := r.resolveValidatedSlices(publishCtx, now)
	// Build the complete labeled set first and swap it in atomically so a
	// concurrent scrape never observes a partially refilled exposition.
	labeled := make(map[string]int64, len(slices.ByCountry)+len(slices.ByLatencyBand)+len(slices.ByASN)+len(slices.ByAnonymity))
	for country, count := range slices.ByCountry {
		labeled[fmt.Sprintf("freeproxyapi_validated_by_country{country=%q}", country)] = count
	}
	for band, count := range slices.ByLatencyBand {
		labeled[fmt.Sprintf("freeproxyapi_validated_latency_band{band=%q}", band)] = count
	}
	for asn, count := range slices.ByASN {
		labeled[fmt.Sprintf("freeproxyapi_validated_by_asn{asn=%q}", asn)] = count
	}
	for class, count := range slices.ByAnonymity {
		labeled[fmt.Sprintf("freeproxyapi_validated_by_anonymity{class=%q}", class)] = count
	}
	r.metrics.ReplaceLabeledGauges(labeled)
	r.metrics.SetStable(slices.Stable)
	r.metrics.SetGeoMismatch(slices.GeoMismatch)
	r.setValidatedSlices(slices)

	log.Printf("stats candidates=%d pending=%d leased=%d validated=%d", stats.Candidates, stats.Pending, stats.Leased, stats.Validated)
}

// scheduleLoop keeps workers asleep while the shared pending queue contains
// only future-dated retests. It polls infrequently as a safety net, but source
// refreshes, lease reclaims, and completed probes wake it immediately when the
// queue may have changed.
func (r *Runner) scheduleLoop(ctx context.Context) {
	for ctx.Err() == nil {
		now := r.nowFunc()
		schedule, err := r.store.ValidationSchedule(ctx, now)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("validation schedule failed: %v", err)
			waitForSignalOrTimer(ctx, r.scheduleChanged, time.Second)
			continue
		}

		if schedule.HasNextDue && !schedule.NextDueAt.After(now) {
			r.workAvailable.signal()
			// Allow a burst of workers to drain due work without making each
			// worker poll Redis independently.
			waitForSignalOrTimer(ctx, r.scheduleChanged, 250*time.Millisecond)
			continue
		}

		delay := 5 * time.Second
		if schedule.HasNextDue {
			until := schedule.NextDueAt.Sub(now)
			if until < delay {
				delay = until
			}
		}
		waitForSignalOrTimer(ctx, r.scheduleChanged, delay)
	}
}

// latencyBands are the fixed buckets used for validated-candidate slicing.
var latencyBands = []string{"<200ms", "200-500ms", "500-1000ms", ">1000ms"}

func latencyBand(ms int64) string {
	switch {
	case ms <= 0:
		return "unknown"
	case ms < 200:
		return "<200ms"
	case ms < 500:
		return "200-500ms"
	case ms < 1000:
		return "500-1000ms"
	default:
		return ">1000ms"
	}
}

// aggregateValidatedSlices summarizes the validated set into counts by country,
// latency band, and stability. Only aggregates cross this boundary; endpoint
// identities stay in Redis.
func (r *Runner) aggregateValidatedSlices(ctx context.Context) validatedSlices {
	return r.aggregateValidatedSlicesFrom(ctx, r.store)
}

// aggregateValidatedSlicesFrom is aggregateValidatedSlices reading through
// redisStore, so HTTP handlers can use the API client.
func (r *Runner) aggregateValidatedSlicesFrom(ctx context.Context, redisStore *store.Redis) validatedSlices {
	out := validatedSlices{
		ByCountry:       map[string]int64{},
		StableByCountry: map[string]int64{},
		ByASN:           map[string]int64{},
		ByAnonymity:     map[string]int64{},
		ByLatencyBand:   map[string]int64{},
	}
	details, err := redisStore.ValidatedDetails(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("validated slice fetch failed: %v", err)
		}
		// A busy Redis instance can make the bounded read time out while the
		// validator is still healthy. Keep the last complete aggregate rather
		// than presenting a transient empty pool to public stats/dashboard
		// consumers.
		return r.validatedSlicesSnapshot()
	}
	for _, d := range details {
		if d.Country != "" && d.ExitCountry != "" && d.Country != d.ExitCountry {
			out.GeoMismatch++
		}
		country := d.Country
		if country == "" {
			country = "unknown"
		}
		out.ByCountry[country]++
		asn := d.ASN
		if asn == "" {
			asn = "unknown"
		}
		if asn != "unknown" {
			out.ByASN[asn]++
		}
		anonymity := d.Anonymity
		if anonymity == "" {
			anonymity = "unknown"
		}
		out.ByAnonymity[anonymity]++
		band := latencyBand(d.LatencyMs)
		out.ByLatencyBand[band]++
		for _, known := range latencyBands {
			if band == known {
				out.BandKnown++
				break
			}
		}
		if d.OkRatioPct >= 80 {
			out.Stable++
			if d.ExitCountry != "" {
				out.StableByCountry[d.ExitCountry]++
			}
		}
	}
	out.ByASN = topCounts(out.ByASN, maxASNSeries)
	return out
}

// maxASNSeries bounds the ASN breakdown's metric cardinality.
const maxASNSeries = 64

// topCounts keeps the limit largest entries by count, breaking ties by key so
// the retained set is deterministic across publishes.
func topCounts(counts map[string]int64, limit int) map[string]int64 {
	if len(counts) <= limit {
		return counts
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		if counts[keys[i]] != counts[keys[j]] {
			return counts[keys[i]] > counts[keys[j]]
		}
		return keys[i] < keys[j]
	})
	out := make(map[string]int64, limit)
	for _, k := range keys[:limit] {
		out[k] = counts[k]
	}
	return out
}

// sourceRefreshGate is how long after a refresh starts the cluster-wide lock
// stays held. It is slightly shorter than the interval so the next ticker to
// fire after one interval always finds the lock free.
func sourceRefreshGate(interval time.Duration) time.Duration {
	return interval * 9 / 10
}

func (r *Runner) refresh(ctx context.Context) (err error) {
	// The lock is a renewable lease: its short TTL is the crash fallback while
	// the heartbeat keeps a long source pass protected from overlap.
	r.refreshState.active.Store(&ctx)
	defer r.refreshState.active.Store(nil)
	token, locked, lockErr := r.store.TrySourceLock(ctx, sourceRefreshLockTTL)
	if lockErr != nil {
		if ctx.Err() != nil {
			r.recordRefreshCanceled()
			return lockErr
		}
		r.recordRefreshError()
		return &sourceLockError{err: lockErr}
	}
	if !locked {
		r.metrics.RecordSourceRefresh("skipped")
		return sourceLockBusyError{}
	}
	refreshCtx, stopHeartbeat := r.startSourceLockHeartbeat(ctx, token)
	startedAt := time.Now()
	defer func() {
		// Stop renewing before changing the final lease state, so a tick that
		// races with completion cannot overwrite the completion hold.
		stopHeartbeat()
		// A refresh that actually completed holds the lock until shortly
		// before one fetch interval has passed since it started. Replica
		// tickers are arbitrarily phased, so this yields one cluster-wide
		// refresh per interval instead of waiting out the crash-fallback
		// TTL and skipping scheduled ticks.
		//
		// A refresh that never completed (the replica was terminated
		// mid-fetch by a rolling deploy, a fetch was canceled, Redis
		// errored) made no progress worth protecting. Holding the lock for
		// the rest of the interval in that case would just leave every
		// other replica sitting idle behind a lock nobody is using, so
		// release it after a short fixed backoff instead. A refresh
		// interrupted by shutdown releases immediately instead: the failure
		// is this replica exiting, not a broken dependency.
		hold := sourceRefreshFailureHold
		if err == nil {
			hold = sourceRefreshGate(r.config.FetchInterval) - time.Since(startedAt)
		}
		r.finishSourceLock(token, hold, err != nil && ctx.Err() != nil)
	}()
	stageKey, err := r.store.NewSourceStageKey()
	if err != nil {
		r.recordRefreshError()
		return err
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		_ = r.store.DeleteSourceStage(cleanupCtx, stageKey)
	}()

	results := r.startSourceRefreshWorkers(refreshCtx, stageKey)
	err = r.drainSourceRefresh(refreshCtx, stageKey, results)
	return err
}

// startSourceRefreshWorkers fans every configured source out to a bounded
// worker pool that stages it into stageKey. The returned channel is closed
// once every source has reported.
func (r *Runner) startSourceRefreshWorkers(ctx context.Context, stageKey string) <-chan sourceRefreshResult {
	jobs := make(chan SourceSpec)
	results := make(chan sourceRefreshResult, len(r.config.SourceSpecs))
	workerCount := sourceRefreshWorkers
	if len(r.config.SourceSpecs) < workerCount {
		workerCount = len(r.config.SourceSpecs)
	}
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for spec := range jobs {
				results <- r.refreshSource(ctx, spec, stageKey)
			}
		}()
	}
	go func() {
		for _, spec := range r.config.SourceSpecs {
			jobs <- spec
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()
	return results
}

// drainSourceRefresh collects per-source results, drains the shared stage
// into the validation queue, and records the refresh stats.
func (r *Runner) drainSourceRefresh(ctx context.Context, stageKey string, results <-chan sourceRefreshResult) error {
	var (
		totalRejected int64
		totalParsed   int64
		okSources     int
		lastErr       error
		mergeErr      error
	)
	for result := range results {
		if result.err != nil {
			if result.stage == sourceStageMerge {
				// A failed merge leaves the shared stage partially populated
				// by this source, so the pass must not drain it.
				mergeErr = result.err
				log.Printf("source %s failed source=%q: %v", result.stage, sourceLogLabel(result.spec.URL), result.err)
				continue
			}
			// One unreachable feed must not abort the remaining inventory;
			// mirror the legacy monitor, which only fails a pass when every
			// source fails.
			lastErr = result.err
			r.metrics.AddSourceFetchFailures(1)
			log.Printf("source %s failed source=%q: %v", result.stage, sourceLogLabel(result.spec.URL), result.err)
			continue
		}
		okSources++
		totalParsed += int64(result.parsed)
		totalRejected += int64(result.rejected)
	}
	if mergeErr != nil {
		r.recordRefreshError()
		return mergeErr
	}
	if okSources == 0 && lastErr != nil {
		r.recordRefreshError()
		return lastErr
	}
	uniqueCount, err := r.store.SourceStageCount(ctx, stageKey)
	if err != nil {
		r.recordRefreshError()
		return err
	}
	totalAdded := 0
	totalTombstoned := 0
	totalCapped := 0
	// room is how many new candidates may still be added before max_candidates;
	// -1 means uncapped. Batches arrive in priority order, so when room runs
	// out the lowest-priority feeds are the ones dropped. A batch may overshoot
	// by up to sourceStageBatch because only new records count against it.
	room := int64(-1)
	if limit := r.config.MaxCandidates; limit > 0 {
		stats, statsErr := r.store.Stats(ctx)
		if statsErr != nil {
			r.recordRefreshError()
			return statsErr
		}
		if room = limit - stats.Candidates; room < 0 {
			room = 0
		}
	}
	queueBase := r.nowFunc()
	err = r.store.DrainSourceStagePrioritized(ctx, stageKey, func(priority int64, batch []string) error {
		if room == 0 {
			totalCapped += len(batch)
			return nil
		}
		counts, err := r.store.UpsertPrioritizedCounts(ctx, batch, sourceQueueTime(queueBase, priority))
		if err != nil {
			return err
		}
		totalAdded += counts.Added
		if room > 0 {
			if room -= int64(counts.Added); room < 0 {
				room = 0
			}
		}
		totalTombstoned += counts.Tombstoned
		r.metrics.AddSourceTombstoned(int64(counts.Tombstoned))
		return nil
	})
	if err != nil {
		r.recordRefreshError()
		return err
	}
	if err := r.store.SetSourceRefreshStats(ctx, uniqueCount, totalParsed, int64(okSources)); err != nil {
		r.recordRefreshError()
		return err
	}
	if totalCapped > 0 {
		r.metrics.AddCandidatesCapped(int64(totalCapped))
		log.Printf("candidate cap reached max_candidates=%d: skipped %d staged endpoints", r.config.MaxCandidates, totalCapped)
	}
	r.metrics.RecordSourceRefresh("ok")
	r.metrics.SetSourceUnique(uniqueCount)
	r.metrics.SetSourceRefreshStats(totalParsed, int64(okSources))
	r.metrics.AddSourceRecords(int64(totalAdded), totalRejected)
	r.setLastRefresh(refreshReport{At: r.nowUTC(), Result: "ok", Accepted: int64(totalAdded), Rejected: totalRejected})
	log.Printf("source refresh sources=%d/%d parsed=%d unique=%d added=%d tombstoned=%d rejected=%d", okSources, len(r.config.SourceSpecs), totalParsed, uniqueCount, totalAdded, totalTombstoned, totalRejected)
	return nil
}

// sourcePriority keeps inventories that advertise recent checks ahead of
// giant raw lists. This affects only the order in which already-deduplicated
// endpoints enter the validation queue; it never bypasses the shared staging
// dedupe step.
func sourcePriority(spec SourceSpec) int64 {
	raw := strings.ToLower(spec.URL)
	switch {
	case strings.Contains(raw, "iplocate/free-proxy-list"),
		strings.Contains(raw, "clearproxy/checked-proxy-list"),
		strings.Contains(raw, "proxifly/free-proxy-list"),
		strings.Contains(raw, "charlespikachu/freeproxy"),
		strings.Contains(raw, "rapidproxylist.org"),
		strings.Contains(raw, "just-not-google/full-free-proxy"),
		strings.Contains(raw, "monosans/proxy-list"),
		strings.Contains(raw, "xyzs996/free-proxy-health-list"),
		strings.Contains(raw, "azestkingscrown/free_proxy_list"),
		strings.Contains(raw, "hproxy-com/free-proxy-list"),
		strings.Contains(raw, "relayglass/free-proxy-list"),
		strings.Contains(raw, "proxmint/free-proxy-list"),
		strings.Contains(raw, "watchttvv/free-proxy-list"),
		strings.Contains(raw, "freeproxies.nodemaven.com"),
		strings.Contains(raw, "roundproxies.com/api/get-free-proxies"),
		strings.Contains(raw, "papi.proxiware.com/proxies"),
		strings.Contains(raw, "m1noa/proxypool"),
		strings.Contains(raw, "moleway/free-proxy-list"),
		strings.Contains(raw, "mauricegift/free-proxies"),
		strings.Contains(raw, "pwnx0/proxy"),
		strings.Contains(raw, "skillter/proxygather"),
		strings.Contains(raw, "berkay-digital/proxy-scraper"),
		strings.Contains(raw, "ipparrot/proxy_ips"),
		strings.Contains(raw, "nikolait/free-proxy-list"),
		strings.Contains(raw, "naravid19/checked-proxies"),
		strings.Contains(raw, "ebrasha/abdal-proxy-hub"),
		strings.Contains(raw, "dinoz0rg/proxy-list/main/checked_proxies"),
		strings.Contains(raw, "ch4120n/ch4120n-proxy-list/main/proxies"),
		strings.Contains(raw, "z3a4/free-proxy-list"),
		strings.Contains(raw, "gnxd3rftt2we/working-proxy-list"),
		strings.Contains(raw, "gnxd3rftt2we/free-global-proxies"),
		strings.Contains(raw, "anutmagang/free-highquality-proxy-socks"),
		strings.Contains(raw, "cheagjihvg/simple-proxylist"),
		strings.Contains(raw, "jn-s3s.github.io/proxy-list"),
		strings.Contains(raw, "theriturajps/proxy-list"),
		strings.Contains(raw, "proxygenerator1/proxygenerator/main/stable"),
		strings.Contains(raw, "proxygenerator1/proxygenerator/main/forsites/"),
		strings.Contains(raw, "blacksnowdot0/proxy-pulse"),
		strings.Contains(raw, "anonymity=elite"),
		strings.Contains(raw, "pmix_checked"),
		strings.Contains(raw, "/checked_"):
		return 10
	case strings.Contains(raw, "geonode.com/api/proxy-list"),
		strings.Contains(raw, "proxyscrape"),
		strings.Contains(raw, "gfpcom/free-proxy-list"),
		strings.Contains(raw, "naeamwen/proxy-list"),
		strings.Contains(raw, "proxio"),
		strings.Contains(raw, "pxys-io/dailyproxylist"),
		strings.Contains(raw, "themiralay/proxy-list-world"),
		strings.Contains(raw, "bes-js/public-proxy-list"),
		strings.Contains(raw, "morawskidotmy/youtube-proxies"),
		strings.Contains(raw, "stormsia/proxy-list"),
		strings.Contains(raw, "mrmarble/proxy-list"),
		strings.Contains(raw, "casa-ls/proxy-list"),
		strings.Contains(raw, "tianndev/free-proxy"),
		strings.Contains(raw, "live"),
		strings.Contains(raw, "online"),
		strings.Contains(raw, "fresh"):
		return 20
	default:
		return 100
	}
}

// sourceQueueTime keeps the staging priority visible in the pending ZSET.
// Pending scores are milliseconds, so this small past-time band keeps every
// newly staged endpoint immediately due while checked sources sort ahead of
// ordinary and unknown feeds.
func sourceQueueTime(now time.Time, priority int64) time.Time {
	const maxPriority = int64(100)
	if priority < 0 {
		priority = 0
	}
	if priority > maxPriority {
		priority = maxPriority
	}
	return now.Add(-time.Duration(maxPriority-priority) * time.Millisecond)
}

type sourceRefreshResult struct {
	spec     SourceSpec
	stage    string
	parsed   int
	rejected int
	err      error
}

const (
	sourceStageFetch = "fetch"
	sourceStageMerge = "merge"
)

// errSourceRecordLimit stops a feed visitor once max_records_per_source is
// reached; it is not a failure.
var errSourceRecordLimit = errors.New("source record limit reached")

// refreshSource stages one feed into its own temporary SET and merges it into
// the shared refresh stage only after the whole feed streamed successfully, so
// a feed that fails mid-stream (or exceeds the byte limit) contributes nothing
// to the drain.
//
// Records stay in Redis rather than process memory: a feed can be up to
// source_max_bytes (32 MiB cap, roughly 1.5M short records) across 8
// concurrent workers, which as Go strings in a dedupe map would cost on the
// order of 100 MiB per feed. Staging keeps at most one 2000-record batch in
// memory. The private stage is a plain SET (one SADD per record; a feed has a
// single priority, so no per-record score) and the merge SPOPs it into the
// shared ZSET with ZADD LT, touching each record once instead of staging it
// into a set+zset pair twice and removing it from both.
func (r *Runner) refreshSource(ctx context.Context, spec SourceSpec, stageKey string) sourceRefreshResult {
	for attempt := 1; attempt <= sourceFetchAttempts; attempt++ {
		result := r.refreshSourceAttempt(ctx, spec, stageKey)
		if result.err == nil {
			if attempt > 1 {
				r.metrics.AddSourceFetchRecovered(1)
			}
			return result
		}
		if attempt == sourceFetchAttempts {
			r.recordSourceFetchFailure(spec.URL, result.err)
			return result
		}
		delay, retry := sourceFetchRetryDelay(result.err, rand.Float64())
		if !retry {
			r.recordSourceFetchFailure(spec.URL, result.err)
			return result
		}
		r.metrics.AddSourceFetchRetries(1)
		log.Printf("source fetch retry scheduled attempt=%d delay=%s reason=%s", attempt+1, delay, classifySourceFetchError(result.err))
		if err := waitSourceFetchRetry(ctx, delay); err != nil {
			result.err = err
			r.recordSourceFetchFailure(spec.URL, result.err)
			return result
		}
	}
	return sourceRefreshResult{spec: spec, stage: sourceStageFetch, err: context.Canceled}
}

func (r *Runner) refreshSourceAttempt(ctx context.Context, spec SourceSpec, stageKey string) sourceRefreshResult {
	result := sourceRefreshResult{spec: spec}
	sourceKey, err := r.store.NewSourceStageKey()
	if err != nil {
		return sourceRefreshResult{spec: spec, stage: sourceStageFetch, err: err}
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
		defer cancel()
		_ = r.store.DeleteSourceStage(cleanupCtx, sourceKey)
	}()
	truncated := false
	err = r.fetch.Stream(ctx, spec.URL, func(reader io.Reader) error {
		batch := make([]string, 0, sourceStageBatch)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			toStage := batch
			batch = make([]string, 0, sourceStageBatch)
			return r.store.AddSourceMembers(ctx, sourceKey, toStage)
		}
		var rejected int
		rejected, err := endpoint.VisitFeedProxies(reader, spec.defaultScheme(r.config.DefaultProxyScheme), func(proxy endpoint.Proxy) error {
			if limit := r.config.MaxRecordsPerSource; limit > 0 && result.parsed >= limit {
				// Stop reading: everything past the cap is ignored, and the
				// records already staged still merge like a complete feed.
				truncated = true
				return errSourceRecordLimit
			}
			result.parsed++
			batch = append(batch, proxy.String())
			if len(batch) >= sourceStageBatch {
				return flush()
			}
			return nil
		})
		result.rejected += rejected
		if errors.Is(err, errSourceRecordLimit) {
			err = nil
		}
		if err != nil {
			return err
		}
		return flush()
	})
	if truncated && err == nil {
		r.metrics.AddSourcesTruncated(1)
		log.Printf("source truncated source=%q limit=%d: ignoring the rest of the feed", sourceLogLabel(spec.URL), r.config.MaxRecordsPerSource)
	}
	if err != nil {
		return sourceRefreshResult{spec: spec, stage: sourceStageFetch, parsed: result.parsed, rejected: result.rejected, err: redactSourceError(err)}
	}
	if _, err := r.store.MergeSourceStage(ctx, sourceKey, stageKey, sourcePriority(spec)); err != nil {
		return sourceRefreshResult{spec: spec, stage: sourceStageMerge, parsed: result.parsed, rejected: result.rejected, err: redactSourceError(err)}
	}
	return result
}

// recordRefreshError records a failed refresh. A failure while the active
// refresh's context is canceled (shutdown or rollout) is recorded as
// "canceled" and leaves the last /report refresh outcome untouched.
func (r *Runner) recordRefreshError() {
	if r.refreshState.activeCanceled() {
		r.recordRefreshCanceled()
		return
	}
	r.metrics.RecordSourceRefresh("error")
	r.setLastRefresh(refreshReport{At: r.nowUTC(), Result: "error"})
}

func (r *Runner) nowUTC() string { return r.nowFunc().UTC().Format(time.RFC3339) }

func (r *Runner) workerLoop(ctx context.Context) {
	for {
		if ctx.Err() != nil || !r.waitForControlHealthy(ctx) {
			return
		}
		claim, claimed, stop := r.claimProbeWork(ctx)
		if stop {
			return
		}
		if !claimed {
			continue
		}
		r.metrics.RecordClaim()

		result := r.runProbe(ctx, claim.URL)
		r.metrics.AddProbeTraffic(result.UploadBytes, result.DownloadBytes)

		// Graceful shutdown: a probe cut short by cancellation releases its lease
		// immediately so another replica can pick it up, mirroring how the legacy
		// monitor released queued claims during drain.
		if result.Error != "" && errors.Is(ctx.Err(), context.Canceled) {
			r.releaseClaim(claim)
			return
		}

		if r.suppressOutcomeWhileUnhealthy(func() { r.releaseClaim(claim) }, result.OK) {
			continue
		}
		r.metrics.RecordProbeResultForTarget(r.probeTargetLabel, result.OK, result.Duration)
		r.metrics.RecordProbeOutcome(claim.Retest, probe.ClassifyError(result), result.OK, result.Duration)
		meta := r.probeOutcomeMeta(ctx, claim.URL, result)
		r.sampleHTTPS(ctx, claim.URL, result, &meta)
		if !r.commitProbeResult(ctx, claim, result, meta) {
			return
		}
	}
}

// claimProbeWork takes a probe permit and then claims one due candidate for
// it. claimed reports that the claim is ready to probe; stop reports that the
// worker should exit. When neither is set the worker loops again.
//
// The permit comes first so a spent budget never claims (and then releases,
// re-scoring to now and losing queue priority) a candidate. Permits come from
// the replica's local bucket; when no work is due the permit goes back to that
// bucket, so idle workers cannot exhaust the shared minute budget.
func (r *Runner) claimProbeWork(ctx context.Context) (claim store.Claim, claimed, stop bool) {
	permit, allowed, err := r.acquireProbePermit(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return store.Claim{}, false, true
		}
		log.Printf("probe budget failed: %v", err)
		time.Sleep(time.Second)
		return store.Claim{}, false, false
	}
	if !allowed {
		r.waitForBudgetWindow(ctx)
		return store.Claim{}, false, false
	}
	claim, found, err := r.store.ClaimDue(ctx, r.worker, r.nowFunc(), r.config.LeaseTTL)
	if err != nil {
		r.returnProbePermit(permit)
		if ctx.Err() != nil {
			return store.Claim{}, false, true
		}
		log.Printf("claim failed: %v", err)
		time.Sleep(time.Second)
		return store.Claim{}, false, false
	}
	if !found {
		r.returnProbePermit(permit)
		waitForSignal(ctx, r.workAvailable)
		return store.Claim{}, false, false
	}
	return claim, true, false
}

// standardProbeFunc returns the production standard-mode probe: the proxy-hop
// TCP connect is bounded by connectTimeout so dead endpoints fail fast instead
// of holding a worker for the whole request timeout, and the response body
// must match expectedBody when one is configured.
func standardProbeFunc(expectedBody string, connectTimeout time.Duration) func(context.Context, string, string, time.Duration) probe.Result {
	return func(ctx context.Context, proxyURL, targetURL string, timeout time.Duration) probe.Result {
		return probe.TestExpectBodyWithConnectTimeout(ctx, proxyURL, targetURL, timeout, connectTimeout, expectedBody)
	}
}

// runProbe probes one claimed proxy while counting it as in flight.
func (r *Runner) runProbe(ctx context.Context, proxyURL string) probe.Result {
	probeFn := r.probeFunc
	if probeFn == nil {
		probeFn = standardProbeFunc(r.config.ProbeExpectedBody, r.config.ProbeConnectTimeout)
	}
	r.metrics.AddInflight(1)
	defer r.metrics.AddInflight(-1)
	if r.config.ProbeTargetMode == "echo" {
		return probe.TestEchoWithConnectTimeout(ctx, proxyURL, r.config.ProbeTarget, r.config.RequestTimeout, r.config.ProbeConnectTimeout, r.currentRealIP())
	}
	return probeFn(ctx, proxyURL, r.config.ProbeTarget, r.config.RequestTimeout)
}

// probeOutcomeMeta classifies a probed proxy's geography and anonymity,
// running a separate anonymity check when the probe did not report one.
func (r *Runner) probeOutcomeMeta(ctx context.Context, proxyURL string, result probe.Result) store.OutcomeMeta {
	meta := store.OutcomeMeta{}
	if parsed, parseErr := url.Parse(proxyURL); parseErr == nil {
		meta.Country, meta.ASN = r.geo.Lookup(parsed.Hostname())
	}
	if result.Anonymity.Class != "" {
		meta.Anonymity = result.Anonymity.Class
		meta.ExitIP = result.Anonymity.ExitIP
		meta.ExitCountry, _ = r.geo.Lookup(result.Anonymity.ExitIP)
	} else if result.OK && r.echo != nil && r.config.AnonymityCheckURL != "" {
		anon := r.checkProxyAnonymity(ctx, proxyURL)
		if anon.Class != "" {
			meta.Anonymity = anon.Class
			meta.ExitIP = anon.ExitIP
			meta.ExitCountry, _ = r.geo.Lookup(anon.ExitIP)
		}
		r.recordEchoTamper(anon, &meta)
	}
	return meta
}

// recordEchoTamper copies an echo tamper-check result into meta and counts
// it. Tampered proxies are annotated only; listing is unaffected.
func (r *Runner) recordEchoTamper(anon probe.AnonymityResult, meta *store.OutcomeMeta) {
	if !anon.TamperChecked {
		return
	}
	meta.TamperChecked = true
	meta.Tampered = anon.Tampered
	r.accuracy.metrics.recordEchoTamper(anon.Tampered, anon.TamperReason)
}

// commitProbeResult completes the claim with the probe outcome. It returns
// false when the worker should exit.
func (r *Runner) commitProbeResult(ctx context.Context, claim store.Claim, result probe.Result, meta store.OutcomeMeta) bool {
	outcome, err := r.store.Complete(ctx, claim, r.nowFunc(), r.config.RetestPolicy(), result.OK, result.StatusCode, result.Duration, probeDetail(result), meta)
	if err != nil {
		if ctx.Err() != nil {
			r.releaseClaim(claim)
			return false
		}
		log.Printf("result commit failed: %v", err)
		return true
	}
	if outcome.Discarded {
		r.metrics.AddDiscarded(1)
		return true
	}
	if outcome.Evicted {
		r.metrics.AddEvictions(1)
		return true
	}
	if !outcome.Committed {
		r.metrics.RecordResultConflict()
	}
	r.scheduleChanged.signal()
	return true
}

// takeProbePermit takes one global-budget permit for a request that is always
// made (anonymity and direct echo checks) from the same local bucket as
// probe workers; denials are counted there.
func (r *Runner) takeProbePermit(ctx context.Context) (bool, error) {
	_, allowed, err := r.acquireProbePermit(ctx)
	return allowed, err
}

// budgetWakeJitter spreads budget-denied workers across the first seconds of
// the next window. Without it every worker on every replica wakes at the same
// minute boundary and hits Redis at once.
const budgetWakeJitter = 5 * time.Second

func (r *Runner) waitForBudgetWindow(ctx context.Context) {
	jitter := time.Duration(rand.Int64N(int64(budgetWakeJitter)))
	timer := time.NewTimer(budgetWindowDelay(r.nowFunc(), jitter))
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// budgetWindowDelay returns how long to sleep from now until the next
// per-minute budget window opens, plus jitter, with a 100ms floor.
func budgetWindowDelay(now time.Time, jitter time.Duration) time.Duration {
	now = now.UTC()
	delay := now.Truncate(time.Minute).Add(time.Minute).Sub(now) + jitter
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
	return delay
}

func (r *Runner) checkProxyAnonymity(ctx context.Context, proxyURL string) probe.AnonymityResult {
	check := r.echoCheckFunc
	if check == nil && r.echo != nil {
		check = r.echo.Check
	}
	if check == nil {
		return probe.AnonymityResult{}
	}
	origin := r.currentRealIP()
	// A 200 response that is not a usable echo (for example a replaced body)
	// has no class but still carries a tamper verdict; keep the first such
	// verdict if no endpoint yields a class.
	var tamperOnly probe.AnonymityResult
	for _, echoURL := range r.config.EchoURLs() {
		allowed, err := r.takeProbePermit(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("anonymity probe budget failed: %v", err)
			}
			return tamperOnly
		}
		if !allowed {
			return tamperOnly
		}
		result := check(ctx, proxyURL, echoURL, origin, r.config.RequestTimeout)
		if result.Class != "" {
			return result
		}
		if result.TamperChecked && !tamperOnly.TamperChecked {
			tamperOnly = result
		}
	}
	return tamperOnly
}

func (r *Runner) releaseClaim(claim store.Claim) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()
	_, _ = r.store.Release(releaseCtx, claim, r.nowFunc())
}

// probeDetail is the failure detail stored as last_error: a short stable
// probe.ClassifyError code rather than the raw transport error. Raw Go errors
// embed the target URL and addresses and routinely exceed Redis's 64-byte
// hash-max-listpack-value, which converts the whole candidate hash from a
// compact listpack into a hashtable.
func probeDetail(result probe.Result) string {
	if result.OK {
		return ""
	}
	return truncateDetail(probe.ClassifyError(result))
}

// truncateDetail keeps stored failure details short and credential-free.
func truncateDetail(detail string) string {
	const maxDetail = 32
	if len(detail) > maxDetail {
		return detail[:maxDetail]
	}
	return detail
}

const releaseTimeout = 5 * time.Second

const pendingRequeueMarker = "pending-requeue-on-start-v2"

func (r *Runner) requeuePendingOnStart(ctx context.Context) error {
	if !r.config.RequeuePendingOnStart {
		return nil
	}
	// The v2 marker runs once after the probe-target correction; candidates
	// deferred by the old echo target must be re-evaluated. The marker is
	// permanent: an expiring marker re-armed this sweep on every deploy after
	// it lapsed and forced the whole pending set due at once. 24h bounds only
	// the crash-fallback lock.
	didRequeue, err := r.store.RunOncePermanent(ctx, pendingRequeueMarker, 24*time.Hour, func(jobCtx context.Context) error {
		n, err := r.store.ForcePendingDueNow(jobCtx)
		if err == nil {
			log.Printf("pending requeue processed=%d", n)
		}
		return err
	})
	if err != nil {
		return err
	}
	if didRequeue {
		r.scheduleChanged.signal()
	}
	return nil
}

// enrichOnStart annotates the currently-validated tail with country/ASN data
// and priority-requeues it for immediate re-probe, so scoring and geo fields
// populate within minutes of a deploy instead of waiting out old intervals.
// Requeueing the full pending backlog is a separate explicitly enabled path
// with its own Redis one-shot lock. Each step runs at most once cluster-wide
// per marker TTL.
func (r *Runner) enrichOnStart(ctx context.Context) {
	if !r.geo.Enabled() {
		return
	}

	didEnrich, err := r.store.RunOnce(ctx, "geo-enrich-validated-v3", 24*time.Hour, func(jobCtx context.Context) {
		// Repair pass: sets polluted before the completion script enforced
		// latest-outcome membership may still contain failed candidates.
		if pruned, err := r.store.PruneFailedValidated(jobCtx, r.config.MinSamplesForListing); err != nil {
			log.Printf("warning: validated prune failed: %v", err)
		} else if pruned > 0 {
			log.Printf("validated prune removed=%d", pruned)
		}
		members, err := r.store.ValidatedMembers(jobCtx)
		if err != nil && jobCtx.Err() == nil {
			log.Printf("warning: validated member fetch failed: %v", err)
			return
		}
		entries := make([][3]string, 0, len(members))
		for _, m := range members {
			if m.URL == "" || (m.Country != "" && m.ASN != "") {
				continue
			}
			parsed, parseErr := url.Parse(m.URL)
			if parseErr != nil {
				continue
			}
			country, asn := r.geo.Lookup(parsed.Hostname())
			if country == "" && asn == "" {
				continue
			}
			entries = append(entries, [3]string{m.ID, country, asn})
		}
		if err := r.store.BackfillCountries(jobCtx, entries); err != nil && jobCtx.Err() == nil {
			log.Printf("warning: geo enrichment write failed: %v", err)
			return
		}
		log.Printf("geo enriched validated annotated=%d of %d", len(entries), len(members))
	})
	if err != nil {
		log.Printf("warning: geo enrich lock failed: %v", err)
	} else if didEnrich {
		log.Printf("geo enrich pass complete")
	}

	// Re-prioritize the tail when enrichment still has gaps to fill through
	// real probes (anonymity classes can only be earned at completion time).
	needProbe := 0
	if r.echo != nil {
		members, err := r.store.ValidatedMembers(ctx)
		if err == nil {
			for _, m := range members {
				if m.Anonymity == "" {
					needProbe++
				}
			}
		}
	}
	didRequeue, err := r.store.RunOnce(ctx, "validated-requeue-kick", time.Hour, func(jobCtx context.Context) {
		n, err := r.store.RequeueValidatedNow(jobCtx, r.nowFunc())
		if err != nil {
			log.Printf("warning: validated requeue failed: %v", err)
			return
		}
		log.Printf("validated requeue processed=%d missing_anonymity=%d", n, needProbe)
	})
	if err != nil && ctx.Err() == nil {
		log.Printf("warning: validated requeue lock failed: %v", err)
	} else if didRequeue && needProbe > 0 {
		log.Printf("validated requeue kick complete reason=anonymity-coverage")
	}
}

// maintainRealIP keeps a fresh copy of the monitor's own direct-egress address
// so anonymity classification can detect transparent exits.
func (r *Runner) maintainRealIP(ctx context.Context) {
	refresh := func() {
		refreshCtx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout*2)
		defer cancel()
		if !r.refreshRealIP(refreshCtx) && ctx.Err() == nil {
			log.Printf("anonymity self-check failed on all %d echo endpoints (will retry)", len(r.config.ClassificationEchoURLs()))
		}
	}
	refresh()
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
		}
	}
}

func (r *Runner) refreshRealIP(ctx context.Context) bool {
	refresh := r.echoRefreshFunc
	if refresh == nil && r.echo != nil {
		refresh = r.echo.RefreshRealIP
	}
	if refresh == nil {
		return false
	}
	for _, echoURL := range r.config.ClassificationEchoURLs() {
		allowed, err := r.takeProbePermit(ctx)
		if err != nil {
			if ctx.Err() == nil {
				log.Printf("anonymity self-check budget failed: %v", err)
			}
			return false
		}
		if !allowed {
			return false
		}
		ip, err := refresh(ctx, echoURL)
		if err != nil {
			continue
		}
		r.realIPMu.Lock()
		r.realIP = ip
		r.realIPMu.Unlock()
		return true
	}
	return false
}

func (r *Runner) currentRealIP() string {
	r.realIPMu.Lock()
	defer r.realIPMu.Unlock()
	return r.realIP
}
