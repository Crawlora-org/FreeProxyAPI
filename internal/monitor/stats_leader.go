package monitor

import (
	"context"
	"encoding/json"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	// statsPublishInterval is the publishStats cadence in Run.
	statsPublishInterval = 30 * time.Second
	// defaultStatsLeaderTTL survives two missed publishes before failover.
	defaultStatsLeaderTTL = 90 * time.Second
	// defaultStatsAggregateTTL bounds how long a dead leader's aggregate lingers.
	defaultStatsAggregateTTL = 5 * time.Minute
	// statsAggregateMaxAge is the age past which followers stop trusting the
	// shared aggregate and scan the validated set themselves.
	statsAggregateMaxAge = 2 * statsPublishInterval
)

// statsLeaderState is this replica's stats-leader lease. Its zero value is a
// follower without a token.
type statsLeaderState struct {
	mu     sync.Mutex
	token  string
	leader bool
}

// sharedValidatedAggregate is the JSON document the stats leader stores in
// Redis. It carries only counts, never endpoint identities.
type sharedValidatedAggregate struct {
	ComputedAtMs    int64            `json:"computed_at_ms"`
	ByCountry       map[string]int64 `json:"by_country"`
	StableByCountry map[string]int64 `json:"stable_by_country"`
	ByASN           map[string]int64 `json:"by_asn"`
	ByAnonymity     map[string]int64 `json:"by_anonymity"`
	ByLatencyBand   map[string]int64 `json:"by_latency_band"`
	Stable          int64            `json:"stable"`
	BandKnown       int64            `json:"band_known"`
	GeoMismatch     int64            `json:"geo_mismatch"`
}

func newSharedValidatedAggregate(v validatedSlices, computedAt time.Time) sharedValidatedAggregate {
	return sharedValidatedAggregate{
		ComputedAtMs:    computedAt.UnixMilli(),
		ByCountry:       v.ByCountry,
		StableByCountry: v.StableByCountry,
		ByASN:           v.ByASN,
		ByAnonymity:     v.ByAnonymity,
		ByLatencyBand:   v.ByLatencyBand,
		Stable:          v.Stable,
		BandKnown:       v.BandKnown,
		GeoMismatch:     v.GeoMismatch,
	}
}

func (a sharedValidatedAggregate) slices() validatedSlices {
	orEmpty := func(m map[string]int64) map[string]int64 {
		if m == nil {
			return map[string]int64{}
		}
		return m
	}
	return validatedSlices{
		ByCountry:       orEmpty(a.ByCountry),
		StableByCountry: orEmpty(a.StableByCountry),
		ByASN:           orEmpty(a.ByASN),
		ByAnonymity:     orEmpty(a.ByAnonymity),
		ByLatencyBand:   orEmpty(a.ByLatencyBand),
		Stable:          a.Stable,
		BandKnown:       a.BandKnown,
		GeoMismatch:     a.GeoMismatch,
	}
}

func (r *Runner) statsLeaderTTL() time.Duration {
	if r.config.StatsLeaderTTL > 0 {
		return r.config.StatsLeaderTTL
	}
	return defaultStatsLeaderTTL
}

func (r *Runner) statsAggregateTTL() time.Duration {
	if r.config.StatsAggregateTTL > 0 {
		return r.config.StatsAggregateTTL
	}
	return defaultStatsAggregateTTL
}

// isStatsLeader reports whether this replica held the lease at its last publish.
func (r *Runner) isStatsLeader() bool {
	r.statsLeader.mu.Lock()
	defer r.statsLeader.mu.Unlock()
	return r.statsLeader.leader
}

func (r *Runner) statsLeaderToken() (string, error) {
	r.statsLeader.mu.Lock()
	defer r.statsLeader.mu.Unlock()
	if r.statsLeader.token == "" {
		token, err := store.NewLeaseToken()
		if err != nil {
			return "", err
		}
		r.statsLeader.token = token
	}
	return r.statsLeader.token, nil
}

func (r *Runner) setStatsLeader(leader bool) {
	r.statsLeader.mu.Lock()
	changed := r.statsLeader.leader != leader
	r.statsLeader.leader = leader
	r.statsLeader.mu.Unlock()
	if changed {
		log.Printf("stats leader worker=%s leader=%t", r.worker, leader)
	}
}

// resolveValidatedSlices returns the validated-set aggregate for this
// publish. The stats leader scans Redis and shares the result; followers use
// the shared copy while it is fresh and scan locally only when it is missing
// or older than statsAggregateMaxAge, so stats never go blank when a leader
// dies. Every replica therefore exposes the same validated_* gauge values.
func (r *Runner) resolveValidatedSlices(ctx context.Context, now time.Time) validatedSlices {
	if !r.config.StatsLeaderEnabled {
		return r.aggregateValidatedSlices(ctx)
	}
	token, err := r.statsLeaderToken()
	leader := false
	if err == nil {
		leader, err = r.store.AcquireStatsLeader(ctx, token, r.statsLeaderTTL())
	}
	if err != nil && ctx.Err() == nil {
		log.Printf("stats leader lease failed: %v", err)
	}
	r.setStatsLeader(leader)

	if leader {
		previous := r.validatedSlicesSnapshot()
		slices := r.aggregateValidatedSlices(ctx)
		// A failed scan returns the previous snapshot; do not republish it
		// under a fresh timestamp.
		if !sameSliceMaps(slices, previous) {
			r.publishSharedAggregate(ctx, token, slices, now)
		}
		if r.draining.Load() {
			releaseCtx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
			if err := r.store.ReleaseStatsLeader(releaseCtx, token); err != nil {
				log.Printf("stats leader release failed: %v", err)
			}
			cancel()
			r.setStatsLeader(false)
		}
		return slices
	}
	if shared, ok := r.readSharedAggregate(ctx, now); ok {
		return shared
	}
	return r.aggregateValidatedSlices(ctx)
}

func (r *Runner) publishSharedAggregate(ctx context.Context, token string, slices validatedSlices, now time.Time) {
	payload, err := json.Marshal(newSharedValidatedAggregate(slices, now))
	if err != nil {
		log.Printf("stats aggregate encode failed: %v", err)
		return
	}
	written, err := r.store.PublishValidatedAggregate(ctx, token, payload, r.statsAggregateTTL())
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("stats aggregate publish failed: %v", err)
		}
		return
	}
	if !written {
		r.setStatsLeader(false)
	}
}

// readSharedAggregate returns the leader's aggregate when it is present and
// no older than statsAggregateMaxAge.
func (r *Runner) readSharedAggregate(ctx context.Context, now time.Time) (validatedSlices, bool) {
	payload, found, err := r.store.ValidatedAggregate(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("stats aggregate read failed: %v", err)
		}
		return validatedSlices{}, false
	}
	if !found {
		return validatedSlices{}, false
	}
	var aggregate sharedValidatedAggregate
	if err := json.Unmarshal(payload, &aggregate); err != nil {
		log.Printf("stats aggregate decode failed: %v", err)
		return validatedSlices{}, false
	}
	age := now.Sub(time.UnixMilli(aggregate.ComputedAtMs))
	if age > statsAggregateMaxAge || age < -statsAggregateMaxAge {
		return validatedSlices{}, false
	}
	return aggregate.slices(), true
}

// sameSliceMaps reports whether a and b share their map storage, which is how
// aggregateValidatedSlices signals it fell back to the previous snapshot: a
// successful scan always allocates new maps.
func sameSliceMaps(a, b validatedSlices) bool {
	return reflect.ValueOf(a.ByCountry).UnsafePointer() == reflect.ValueOf(b.ByCountry).UnsafePointer()
}
