package monitor

import (
	"context"
	"sync"
)

// minProbePermitChunk is the smallest automatic refill size. Chunks stay small
// relative to the budget: permits still held when a minute window ends are
// discarded (they were already counted in Redis), so the worst-case waste per
// window is one chunk per replica.
const minProbePermitChunk = 10

// permitBucket holds global-budget permits this replica reserved from the
// current Redis minute window. Its zero value is an empty bucket.
type permitBucket struct {
	mu     sync.Mutex
	window int64 // Unix minute the tokens belong to.
	tokens int
	// exhausted records that Redis granted nothing for window. The window
	// counter never decreases, so later requests are denied locally.
	exhausted bool
}

// probePermit identifies the window a permit was taken from so it can only be
// handed back into that same window.
type probePermit struct {
	window int64
}

func unixMinute(unixSeconds int64) int64 {
	minute := unixSeconds / 60
	if unixSeconds%60 < 0 {
		minute--
	}
	return minute
}

// probePermitChunk is how many permits one refill asks Redis for.
func (r *Runner) probePermitChunk() int {
	chunk := r.config.ProbePermitChunk
	if chunk <= 0 {
		chunk = max(minProbePermitChunk, r.config.Workers/4)
	}
	if limit := r.config.GlobalRequestsPerMinute; limit > 0 && chunk > limit {
		chunk = limit
	}
	return chunk
}

// acquireProbePermit takes one permit from the local bucket, refilling it
// from the Redis window counter when empty. Refills are serialized per replica
// so concurrent workers never over-reserve. ok=false means the cluster-wide
// budget for this minute is spent.
func (r *Runner) acquireProbePermit(ctx context.Context) (permit probePermit, ok bool, err error) {
	now := r.nowFunc()
	window := unixMinute(now.Unix())
	bucket := &r.permits
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.window != window {
		// Leftover permits belong to a closed window; they expire with it.
		bucket.window = window
		bucket.tokens = 0
		bucket.exhausted = false
	}
	if bucket.tokens > 0 {
		bucket.tokens--
		return probePermit{window: window}, true, nil
	}
	if bucket.exhausted {
		r.metrics.RecordBudgetDenial()
		return probePermit{}, false, nil
	}
	granted, err := r.store.ReservePermits(ctx, now, r.config.GlobalRequestsPerMinute, r.probePermitChunk())
	if err != nil {
		return probePermit{}, false, err
	}
	if granted <= 0 {
		bucket.exhausted = true
		r.metrics.RecordBudgetDenial()
		return probePermit{}, false, nil
	}
	bucket.tokens = granted - 1
	return probePermit{window: window}, true, nil
}

// returnProbePermit hands an unused permit back to the local bucket (never to
// Redis). A permit from a window that has since closed is dropped.
func (r *Runner) returnProbePermit(permit probePermit) {
	bucket := &r.permits
	bucket.mu.Lock()
	defer bucket.mu.Unlock()
	if bucket.window == permit.window && unixMinute(r.nowFunc().Unix()) == permit.window {
		bucket.tokens++
	}
}
