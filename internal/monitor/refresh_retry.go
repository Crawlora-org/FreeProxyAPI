package monitor

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
)

const (
	// sourceLockRetryBase and sourceLockRetryCap bound the backoff used when
	// acquiring the cluster-wide source-refresh lock fails with a Redis error
	// (not ordinary contention): 15s, 30s, 60s, 120s, 240s, then 5m.
	sourceLockRetryBase = 15 * time.Second
	sourceLockRetryCap  = 5 * time.Minute
	// sourceCanceledReleaseTimeout bounds the immediate lock release a
	// shutting-down replica performs, so termination is never held up by a
	// slow or unreachable Redis.
	sourceCanceledReleaseTimeout = 2 * time.Second
	// Refreshes renew the cluster-wide lease periodically. If the owner dies,
	// the short lock TTL bounds how long other replicas wait before recovering.
	sourceRefreshLockRenewInterval = time.Minute
	// A source gets one retry. The retry is intentionally bounded so a slow
	// feed cannot consume the whole refresh window or create a retry storm.
	sourceFetchAttempts      = 2
	sourceFetchRetryBase     = 2 * time.Second
	sourceFetchRetryJitter   = 0.2
	sourceFetchRetryAfterCap = 2 * time.Minute
	sourceFetchRetryFallback = 5 * time.Second
)

// refreshResultCanceled labels a refresh that ended because the runner's
// context was canceled (shutdown or rollout), not because anything failed.
const refreshResultCanceled = "canceled"

// sourceLockError marks a Redis failure while acquiring the source-refresh
// lock, which startRefresh retries with backoff instead of waiting for the
// next fetch interval.
type sourceLockError struct{ err error }

func (e *sourceLockError) Error() string { return "acquire source refresh lock: " + e.err.Error() }
func (e *sourceLockError) Unwrap() error { return e.err }

// sourceLockBusyError distinguishes ordinary lock contention from a Redis
// error. Contention is still reported as a skipped refresh, but it also gets a
// bounded retry so a stale lock does not wait for the next ticker interval.
type sourceLockBusyError struct{}

func (sourceLockBusyError) Error() string { return "source refresh lock is held" }

// refreshRobustness holds per-replica state for refresh cancellation
// classification and lock-error retries.
type refreshRobustness struct {
	// active is the context of the refresh currently running on this replica,
	// so recordRefreshError can classify a failure caused by cancellation.
	active atomic.Pointer[context.Context]

	mu sync.Mutex
	// attempt counts consecutive lock-acquire Redis errors; zero means the
	// last refresh outcome was not a lock error.
	attempt int
	// pending is set while a retry goroutine is waiting on its timer, so at
	// most one retry chain exists per replica.
	pending bool
	// delay overrides sourceLockRetryDelay (tests only).
	delay func(attempt int) time.Duration
}

func (s *refreshRobustness) activeCanceled() bool {
	ctx := s.active.Load()
	return ctx != nil && (*ctx).Err() != nil
}

// sourceLockRetryDelay returns the capped exponential backoff for attempt
// (1-based) with +/-20% jitter, never exceeding sourceLockRetryCap.
func sourceLockRetryDelay(attempt int, jitter float64) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := sourceLockRetryCap
	if attempt <= 10 {
		if d := sourceLockRetryBase << (attempt - 1); d < sourceLockRetryCap {
			delay = d
		}
	}
	delay = time.Duration(float64(delay) * (0.8 + 0.4*jitter))
	if delay > sourceLockRetryCap {
		delay = sourceLockRetryCap
	}
	return delay
}

func (r *Runner) recordRefreshCanceled() {
	r.metrics.RecordSourceRefresh(refreshResultCanceled)
}

func sourceFetchRetryDelay(err error, jitter float64) (time.Duration, bool) {
	var statusErr *source.HTTPStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode != http.StatusTooManyRequests {
			return 0, false
		}
		delay := statusErr.RetryAfter
		if delay <= 0 {
			delay = sourceFetchRetryFallback
		}
		if delay > sourceFetchRetryAfterCap {
			return 0, false
		}
		return delay, true
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if errors.Is(err, context.Canceled) {
			return 0, false
		}
		return jitteredSourceRetryDelay(jitter), true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return jitteredSourceRetryDelay(jitter), true
	}
	// Only transport-level operation errors are transient here. In particular,
	// os.PathError also implements net.Error, but a missing local source file is
	// permanent and must not be retried.
	var netErr *net.OpError
	if errors.As(err, &netErr) {
		return jitteredSourceRetryDelay(jitter), true
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return jitteredSourceRetryDelay(jitter), true
	}
	return 0, false
}

func jitteredSourceRetryDelay(jitter float64) time.Duration {
	if jitter < 0 {
		jitter = 0
	}
	if jitter > 1 {
		jitter = 1
	}
	factor := 1 - sourceFetchRetryJitter + (2 * sourceFetchRetryJitter * jitter)
	return time.Duration(float64(sourceFetchRetryBase) * factor)
}

func waitSourceFetchRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// tryStartRefresh claims the per-replica refresh guard and launches one
// refresh. A retry that loses the guard to a running refresh is dropped
// silently: that refresh schedules its own retry if its lock attempt fails.
func (r *Runner) tryStartRefresh(ctx context.Context, launch func(func()), retry bool) bool {
	if !r.refreshing.CompareAndSwap(false, true) {
		if !retry {
			r.metrics.RecordSourceRefresh("skipped")
			log.Printf("source refresh skipped: previous refresh still running")
		}
		return false
	}
	launch(func() {
		err := r.refresh(ctx)
		var busyErr sourceLockBusyError
		if err != nil && ctx.Err() == nil && !errors.As(err, &busyErr) {
			log.Printf("source refresh failed: %v", err)
		}
		// Release the guard before scheduling a retry so the retry can
		// claim it; never via defer, which could clear a later run's guard.
		r.refreshing.Store(false)
		r.scheduleChanged.signal()
		if ctx.Err() == nil {
			r.publishStats(ctx)
		}
		r.scheduleLockRetry(ctx, launch, err)
	})
	return true
}

// scheduleLockRetry arms a backoff retry after a lock-acquire Redis error and
// resets the backoff after any other outcome (success, contention, or a
// failure after the lock was held). Retries stop on shutdown.
func (r *Runner) scheduleLockRetry(ctx context.Context, launch func(func()), err error) {
	state := &r.refreshState
	var lockErr *sourceLockError
	state.mu.Lock()
	var busyErr sourceLockBusyError
	if (!errors.As(err, &lockErr) && !errors.As(err, &busyErr)) || ctx.Err() != nil {
		state.attempt = 0
		state.mu.Unlock()
		return
	}
	state.attempt++
	attempt := state.attempt
	if state.pending {
		state.mu.Unlock()
		return
	}
	state.pending = true
	delayFn := state.delay
	state.mu.Unlock()

	delay := sourceLockRetryDelay(attempt, rand.Float64())
	if delayFn != nil {
		delay = delayFn(attempt)
	}
	if lockErr != nil {
		log.Printf("source refresh lock error; retrying attempt=%d delay=%s: %v", attempt, delay, lockErr.err)
	} else {
		log.Printf("source refresh lock busy; retrying attempt=%d delay=%s", attempt, delay)
	}
	launch(func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
		case <-timer.C:
		}
		state.mu.Lock()
		state.pending = false
		// attempt is reset once any refresh gets past lock acquisition, so a
		// stale retry does not run after the ticker already recovered.
		stale := state.attempt == 0
		state.mu.Unlock()
		if ctx.Err() != nil || stale {
			return
		}
		r.tryStartRefresh(ctx, launch, true)
	})
}

// startSourceLockHeartbeat returns a child context canceled if the refresh
// loses its Redis lease. The owner token is checked on every renewal, so a
// refresh cannot continue staging data after another replica has recovered a
// dead lease.
func (r *Runner) startSourceLockHeartbeat(ctx context.Context, token string) (context.Context, func()) {
	leaseCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(sourceRefreshLockRenewInterval)
		defer ticker.Stop()
		for {
			select {
			case <-leaseCtx.Done():
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), releaseTimeout)
				renewed, err := r.store.RenewSourceLock(renewCtx, token, sourceRefreshLockTTL)
				renewCancel()
				if err != nil {
					log.Printf("source refresh lock renewal failed: %v", err)
					cancel()
					return
				}
				if !renewed {
					log.Printf("source refresh lock ownership lost")
					cancel()
					return
				}
			}
		}
	}()
	return leaseCtx, func() {
		cancel()
		<-done
	}
}

// finishSourceLock re-arms the source-refresh lock to expire after hold. When
// canceled is set (the refresh was interrupted by shutdown) it instead
// releases the lock at once (token-checked) with a short background timeout,
// so another replica can refresh without waiting for the failure hold or the
// crash-fallback TTL.
func (r *Runner) finishSourceLock(token string, hold time.Duration, canceled bool) {
	timeout := releaseTimeout
	if canceled {
		hold = 0
		timeout = sourceCanceledReleaseTimeout
	}
	cleanupCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := r.store.FinishSourceLock(cleanupCtx, token, hold); err != nil {
		log.Printf("warning: source refresh lock release failed: %v", err)
	}
}

// Per-source fetch failure classification.

const (
	sourceFailureHostOther = "other"
	// sourceFailureHostCap bounds distinct host label values; further hosts
	// share the "other" bucket.
	sourceFailureHostCap = 512
)

// sourceFailureReasons is the fixed reason label set, in exposition order.
var sourceFailureReasons = []string{"canceled", "dns", "http_status", "other", "parse", "refused", "timeout", "tls", "too_large"}

// sourceFailureHost returns the lowercase hostname of a feed URL (never its
// path, query, or credentials); file:// sources report "file".
func sourceFailureHost(rawURL string) string {
	u, err := url.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return sourceFailureHostOther
	}
	if strings.EqualFold(u.Scheme, "file") {
		return "file"
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return sourceFailureHostOther
	}
	return host
}

// classifySourceFetchError maps a fetch/parse error to a bounded reason.
func classifySourceFetchError(err error) string {
	if err == nil {
		return "other"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	if errors.Is(err, source.ErrSourceTooLarge) {
		return "too_large"
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) && !dnsErr.IsTimeout {
		return "dns"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "refused"
	}
	var (
		certErr      *tls.CertificateVerificationError
		recordErr    tls.RecordHeaderError
		alertErr     tls.AlertError
		unknownCA    x509.UnknownAuthorityError
		hostnameErr  x509.HostnameError
		certInvalid  x509.CertificateInvalidError
		systemRootsE x509.SystemRootsError
	)
	if errors.As(err, &certErr) || errors.As(err, &recordErr) || errors.As(err, &alertErr) ||
		errors.As(err, &unknownCA) || errors.As(err, &hostnameErr) || errors.As(err, &certInvalid) ||
		errors.As(err, &systemRootsE) {
		return "tls"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "source returned "):
		return "http_status"
	case strings.Contains(msg, "tls: "):
		return "tls"
	case strings.Contains(msg, "proxy feed"):
		return "parse"
	}
	return "other"
}

// recordSourceFetchFailure counts one failed source fetch by feed host and
// failure reason.
func (r *Runner) recordSourceFetchFailure(rawURL string, err error) {
	r.metrics.RecordSourceFetchFailureBySource(sourceFailureHost(rawURL), classifySourceFetchError(err))
}
