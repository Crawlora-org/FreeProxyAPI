// Package store coordinates Redis-backed monitor workers.
package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	mathrand "math/rand/v2"
	"net/url"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

type Redis struct {
	client *redis.Client
	prefix string
}

type Claim struct {
	ID    string
	URL   string
	Token string
	// Retest is true when the candidate had been checked before this claim.
	Retest bool
}

type Stats struct {
	Candidates       int64
	SourceUnique     int64
	SourceParsed     int64
	SourcesSucceeded int64
	Pending          int64
	PendingDue       int64
	Leased           int64
	Validated        int64
	NextDueAt        time.Time
	HasNextDue       bool
}

// ValidationSchedule describes the next probe work visible to all monitor
// replicas. The pending score is the earliest candidate due time; leased is
// the number of probes currently claimed by workers.
type ValidationSchedule struct {
	Leased     int64
	NextDueAt  time.Time
	HasNextDue bool
}

// PoolOptions sizes one Redis client's connection pool. Zero values keep the
// store defaults: go-redis's 10*GOMAXPROCS pool size (or a pool_size query
// parameter in the Redis URL), no pre-warmed idle connections, and a 30s pool
// wait.
type PoolOptions struct {
	PoolSize     int
	MinIdleConns int
	PoolTimeout  time.Duration
}

// PoolStats is a point-in-time copy of one client's connection-pool counters.
// Counters (Hits through StaleConns) are cumulative for the process lifetime.
type PoolStats struct {
	// Size is the configured maximum number of pooled connections.
	Size            int
	Hits            uint32
	Misses          uint32
	Timeouts        uint32
	WaitCount       uint32
	WaitDurationNs  int64
	StaleConns      uint32
	TotalConns      uint32
	IdleConns       uint32
	PendingRequests uint32
}

func NewRedis(redisURL, namespace string) (*Redis, error) {
	return NewRedisWithPool(redisURL, namespace, PoolOptions{})
}

// NewRedisWithPool is NewRedis with an explicit connection-pool size. Callers
// that share one process between long-running workers and latency-sensitive
// reads create one client per traffic class so neither can starve the other.
func NewRedisWithPool(redisURL, namespace string, pool PoolOptions) (*Redis, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	// Source refreshes can briefly queue behind Redis AOF rewrites while a
	// large deduplicated stage is drained. The go-redis defaults are tuned for
	// a low-latency cache and are too short for this durable queue workload.
	options.ReadTimeout = redisReadTimeout
	options.WriteTimeout = redisWriteTimeout
	options.PoolTimeout = redisPoolTimeout
	if pool.PoolSize > 0 {
		options.PoolSize = pool.PoolSize
	}
	if pool.MinIdleConns > 0 {
		options.MinIdleConns = pool.MinIdleConns
	}
	if pool.PoolTimeout > 0 {
		options.PoolTimeout = pool.PoolTimeout
	}
	if namespace == "" {
		namespace = "freeproxyapi:v1"
	}
	return &Redis{client: redis.NewClient(options), prefix: namespace}, nil
}

// PoolStats reports the client's connection-pool counters.
func (s *Redis) PoolStats() PoolStats {
	stats := s.client.PoolStats()
	return PoolStats{
		Size:            s.client.Options().PoolSize,
		Hits:            stats.Hits,
		Misses:          stats.Misses,
		Timeouts:        stats.Timeouts,
		WaitCount:       stats.WaitCount,
		WaitDurationNs:  stats.WaitDurationNs,
		StaleConns:      stats.StaleConns,
		TotalConns:      stats.TotalConns,
		IdleConns:       stats.IdleConns,
		PendingRequests: stats.PendingRequests,
	}
}

func (s *Redis) Close() error { return s.client.Close() }

func (s *Redis) Ping(ctx context.Context) error { return s.client.Ping(ctx).Err() }

func (s *Redis) candidatesKey() string   { return s.prefix + ":candidates" }
func (s *Redis) pendingKey() string      { return s.prefix + ":pending" }
func (s *Redis) leasedKey() string       { return s.prefix + ":leased" }
func (s *Redis) validatedKey() string    { return s.prefix + ":validated" }
func (s *Redis) sourceUniqueKey() string { return s.prefix + ":source-unique" }
func (s *Redis) sourceStatsKey() string  { return s.prefix + ":source-refresh-stats" }
func (s *Redis) proxyKey(id string) string {
	return s.prefix + ":proxy:" + id
}
func (s *Redis) tombstonePrefix() string       { return s.prefix + ":tomb:" }
func (s *Redis) tombstoneKey(id string) string { return s.tombstonePrefix() + id }

// upsertBatch bounds how many endpoints go into one Lua invocation so a very
// large feed cannot build a single enormous command while keeping refreshes
// practical for million-record inventories.
const upsertBatch = 2000

// sourceStageTTL must cover the longest bounded source refresh. The refresh
// owner stages feeds concurrently and commits the deduplicated batch only
// after every source has reported, so expiring this key mid-refresh would
// discard the inventory before source-unique stats are written.
const sourceStageTTL = 2 * time.Hour

const (
	redisReadTimeout  = 15 * time.Second
	redisWriteTimeout = 15 * time.Second
	redisPoolTimeout  = 30 * time.Second
)

// NewSourceStageKey returns an isolated, TTL-cleaned Redis staging key used to
// deduplicate one complete source refresh before its candidates are queued.
// The key contains no endpoint data and is safe to discard on any error path.
func (s *Redis) NewSourceStageKey() (string, error) {
	token, err := randomToken()
	if err != nil {
		return "", err
	}
	return s.prefix + ":source-stage:" + token, nil
}

// sourceStageDrainPage bounds how many staged entries one drain pass reads.
// It is a variable only so tests can force multi-pass drains cheaply.
var sourceStageDrainPage int64 = 10000

// sourceMergeBatch bounds how many members one merge script moves, keeping
// each Lua invocation short on a shared Redis instance.
var sourceMergeBatch int64 = 2000

// stageURLsScript adds one batch to a stage ZSET and (re)arms its TTL in the
// same atomic step, so a crash between batches can never leave a staging key
// without an expiry. ZADD LT inserts new members and only ever lowers an
// existing member's priority, so the ZSET is both the dedupe index and the
// drain order.
var stageURLsScript = redis.NewScript(`
for i = 3, #ARGV do
  redis.call('ZADD', KEYS[1], 'LT', ARGV[1], ARGV[i])
end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`)

// StageSourceURLs adds canonical endpoints to a temporary refresh stage (a
// ZSET scored by the lowest source priority seen). Redis performs the
// cross-source deduplication atomically; batches keep command and argument
// sizes bounded even when a feed contains millions of records. Lower
// priorities are drained first.
func (s *Redis) StageSourceURLs(ctx context.Context, key string, priority int64, urls []string) error {
	if len(urls) == 0 {
		return nil
	}
	for start := 0; start < len(urls); start += upsertBatch {
		end := start + upsertBatch
		if end > len(urls) {
			end = len(urls)
		}
		args := make([]interface{}, 2, 2+end-start)
		args[0] = priority
		args[1] = sourceStageTTL.Milliseconds()
		for _, rawURL := range urls[start:end] {
			args = append(args, rawURL)
		}
		if err := stageURLsScript.Run(ctx, s.client, []string{key}, args...).Err(); err != nil {
			return err
		}
	}
	return nil
}

// AddSourceMembers adds one feed's canonical endpoints to that feed's private
// SET and re-arms its TTL. A single feed has a single priority, so the private
// stage needs no score; it is merged into the shared stage only once the feed
// streamed completely (see MergeSourceStage).
func (s *Redis) AddSourceMembers(ctx context.Context, key string, urls []string) error {
	for start := 0; start < len(urls); start += upsertBatch {
		end := start + upsertBatch
		if end > len(urls) {
			end = len(urls)
		}
		members := make([]interface{}, 0, end-start)
		for _, rawURL := range urls[start:end] {
			members = append(members, rawURL)
		}
		if _, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.SAdd(ctx, key, members...)
			pipe.PExpire(ctx, key, sourceStageTTL)
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// mergeSourceStageScript moves up to ARGV[1] members from the private SET
// KEYS[1] into the shared stage ZSET KEYS[2] at priority ARGV[2] (never
// raising an existing lower priority) and re-arms both TTLs. SPOP removes as
// it moves, so each member is touched once and the private set shrinks while
// the shared stage grows.
var mergeSourceStageScript = redis.NewScript(`
local members = redis.call('SPOP', KEYS[1], ARGV[1])
for i = 1, #members do
  redis.call('ZADD', KEYS[2], 'LT', ARGV[2], members[i])
end
if #members > 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
  redis.call('PEXPIRE', KEYS[2], ARGV[3])
end
return #members
`)

// MergeSourceStage drains a feed's private SET into the shared refresh stage
// at the feed's priority and returns how many members were moved. A failure
// part-way leaves the shared stage partially merged; callers must then not
// drain it.
func (s *Redis) MergeSourceStage(ctx context.Context, sourceKey, stageKey string, priority int64) (int64, error) {
	var moved int64
	for {
		n, err := mergeSourceStageScript.Run(ctx, s.client, []string{sourceKey, stageKey},
			sourceMergeBatch, priority, sourceStageTTL.Milliseconds()).Int64()
		if err != nil {
			return moved, err
		}
		moved += n
		if n < sourceMergeBatch {
			return moved, nil
		}
	}
}

// SourceStageCount returns the number of unique canonical endpoints staged for
// a refresh, without exposing their identities to callers.
func (s *Redis) SourceStageCount(ctx context.Context, key string) (int64, error) {
	return s.client.ZCard(ctx, key).Result()
}

// DrainSourceStage visits the deduplicated refresh stage in bounded batches.
// The callback may upsert each batch; validation workers only see endpoints
// after they have passed through this cross-source dedupe stage.
func (s *Redis) DrainSourceStage(ctx context.Context, key string, fn func([]string) error) error {
	return s.DrainSourceStagePrioritized(ctx, key, func(_ int64, batch []string) error {
		return fn(batch)
	})
}

// DrainSourceStagePrioritized visits the deduplicated refresh stage in bounded
// priority-ordered batches. The priority is returned separately so callers
// can preserve it when they enqueue the endpoints for validation.
func (s *Redis) DrainSourceStagePrioritized(ctx context.Context, key string, fn func(int64, []string) error) error {
	for {
		// Re-arm the TTL before each page so a long drain cannot silently
		// lose candidates that have not been visited yet.
		if err := s.client.PExpire(ctx, key, sourceStageTTL).Err(); err != nil {
			return err
		}
		entries, err := s.client.ZRangeWithScores(ctx, key, 0, sourceStageDrainPage-1).Result()
		if err != nil {
			return err
		}
		if len(entries) == 0 {
			return nil
		}
		var (
			batchPriority int64
			batch         []string
		)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := fn(batchPriority, batch); err != nil {
				return err
			}
			batch = nil
			return nil
		}
		for _, entry := range entries {
			priority := int64(entry.Score)
			if len(batch) > 0 && priority != batchPriority {
				if err := flush(); err != nil {
					return err
				}
			}
			if len(batch) == 0 {
				batchPriority = priority
			}
			member, ok := entry.Member.(string)
			if !ok {
				return fmt.Errorf("source stage member has type %T", entry.Member)
			}
			batch = append(batch, member)
		}
		if err := flush(); err != nil {
			return err
		}
		values := make([]interface{}, len(entries))
		for i, entry := range entries {
			values[i] = entry.Member
		}
		if err := s.client.ZRem(ctx, key, values...).Err(); err != nil {
			return err
		}
	}
}

func (s *Redis) DeleteSourceStage(ctx context.Context, key string) error {
	return s.client.Del(ctx, key).Err()
}

// SetSourceRefreshStats records the aggregate results from the latest
// completed refresh so every replica can publish the same cluster-wide gauges.
func (s *Redis) SetSourceRefreshStats(ctx context.Context, unique, parsed, succeeded int64) error {
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Set(ctx, s.sourceUniqueKey(), unique, 0)
		pipe.HSet(ctx, s.sourceStatsKey(),
			"parsed", parsed,
			"succeeded", succeeded,
		)
		return nil
	})
	return err
}

// upsertScript adds or refreshes candidates. An endpoint that is not currently
// a candidate and whose eviction tombstone (KEYS[5] .. id, value
// "strikes:until_ms") is still blocking at ARGV[1] is skipped entirely: no
// SADD, HSET, or ZADD. An expired tombstone does not block, but its key stays
// until its TTL so the next eviction continues the strike count.
var upsertScript = redis.NewScript(`
local added = 0
local skipped = 0
local now_ms = tonumber(ARGV[1])
for i = 2, #ARGV, 2 do
  local id = ARGV[i]
  local key = KEYS[4] .. id
  local blocked = false
  local tomb = redis.call('GET', KEYS[5] .. id)
  if tomb and redis.call('SISMEMBER', KEYS[1], id) == 0 then
    local until_ms = tonumber(string.match(tomb, ':(%d+)$'))
    if until_ms and until_ms > now_ms then blocked = true end
  end
  if blocked then
    skipped = skipped + 1
  else
    added = added + redis.call('SADD', KEYS[1], id)
    redis.call('HSET', key, 'url', ARGV[i + 1])

    local leased = redis.call('ZSCORE', KEYS[3], id)
    local token = redis.call('HGET', key, 'lease_token') or ''
    if leased or token ~= '' then
      -- Repair any pending entry left by an older refresh implementation while
      -- preserving the active owner and its lease metadata.
      redis.call('ZREM', KEYS[2], id)
    elseif not redis.call('ZSCORE', KEYS[2], id) then
      redis.call('ZADD', KEYS[2], ARGV[1], id)
    end
  end
end
return {added, skipped}
`)

// UpsertCounts reports what one upsert call did. Tombstoned counts endpoints
// skipped because a recent eviction backoff is still active.
type UpsertCounts struct {
	Added      int
	Tombstoned int
}

// Upsert adds canonical proxy URLs and schedules new candidates immediately.
// Endpoints still blocked by an eviction tombstone are skipped.
func (s *Redis) Upsert(ctx context.Context, urls []string, now time.Time) (int, error) {
	counts, err := s.UpsertCounts(ctx, urls, now)
	return counts.Added, err
}

// UpsertCounts is Upsert that also reports how many endpoints were skipped
// by an active eviction tombstone.
func (s *Redis) UpsertCounts(ctx context.Context, urls []string, now time.Time) (UpsertCounts, error) {
	var counts UpsertCounts
	for start := 0; start < len(urls); start += upsertBatch {
		end := start + upsertBatch
		if end > len(urls) {
			end = len(urls)
		}
		args := make([]interface{}, 1, 1+(end-start)*2)
		args[0] = now.UnixMilli()
		for _, rawURL := range urls[start:end] {
			id := proxyID(rawURL)
			args = append(args, id, rawURL)
		}
		values, err := upsertScript.Run(ctx, s.client,
			[]string{s.candidatesKey(), s.pendingKey(), s.leasedKey(), s.prefix + ":proxy:", s.tombstonePrefix()}, args...).Int64Slice()
		if err != nil {
			return UpsertCounts{}, err
		}
		if len(values) != 2 {
			return UpsertCounts{}, fmt.Errorf("unexpected Redis upsert response")
		}
		counts.Added += int(values[0])
		counts.Tombstoned += int(values[1])
	}
	return counts, nil
}

// prioritizeUnvalidatedScript promotes never-checked candidates to ARGV[1].
// IDs start at ARGV[2]. An ID with a blocking eviction tombstone is never
// promoted, even if a hash exists for it.
var prioritizeUnvalidatedScript = redis.NewScript(`
local now_ms = tonumber(ARGV[1])
for i = 2, #ARGV do
  local id = ARGV[i]
  local key = KEYS[1] .. id
  local blocked = false
  local tomb = redis.call('GET', KEYS[4] .. id)
  if tomb then
    local until_ms = tonumber(string.match(tomb, ':(%d+)$'))
    if until_ms and until_ms > now_ms then blocked = true end
  end
  if not blocked
      and redis.call('EXISTS', key) == 1
      and (redis.call('HGET', key, 'last_checked_at_ms') or '') == ''
      and not redis.call('ZSCORE', KEYS[2], id) then
    redis.call('ZADD', KEYS[3], ARGV[1], id)
  end
end
return 1
`)

// UpsertPrioritized is used by a deduped source refresh. It promotes only
// candidates that have never been checked, leaving validated and failed
// retest schedules intact while allowing checked feeds to lead the first pass.
func (s *Redis) UpsertPrioritized(ctx context.Context, urls []string, now time.Time) (int, error) {
	counts, err := s.UpsertPrioritizedCounts(ctx, urls, now)
	return counts.Added, err
}

// UpsertPrioritizedCounts is UpsertPrioritized that also reports endpoints
// skipped by an active eviction tombstone.
func (s *Redis) UpsertPrioritizedCounts(ctx context.Context, urls []string, now time.Time) (UpsertCounts, error) {
	counts, err := s.UpsertCounts(ctx, urls, now)
	if err != nil {
		return UpsertCounts{}, err
	}
	for start := 0; start < len(urls); start += upsertBatch {
		end := start + upsertBatch
		if end > len(urls) {
			end = len(urls)
		}
		ids := make([]interface{}, 1, 1+end-start)
		ids[0] = now.UnixMilli()
		for _, rawURL := range urls[start:end] {
			ids = append(ids, proxyID(rawURL))
		}
		if err := prioritizeUnvalidatedScript.Run(ctx, s.client,
			[]string{s.prefix + ":proxy:", s.leasedKey(), s.pendingKey(), s.tombstonePrefix()}, ids...).Err(); err != nil {
			return UpsertCounts{}, err
		}
	}
	return counts, nil
}

var claimScript = redis.NewScript(`
-- A bounded loop also cleans pending entries accidentally left alongside an
-- active lease without allowing them to block all later due candidates.
for _ = 1, 100 do
  local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
  if #ids == 0 then return {} end
  local id = ids[1]
  if redis.call('ZREM', KEYS[1], id) == 1 then
    local key = KEYS[2] .. id
    local url = redis.call('HGET', key, 'url')
    local token = redis.call('HGET', key, 'lease_token') or ''
    if url and not redis.call('ZSCORE', KEYS[3], id) and token == '' then
      redis.call('HSET', key, 'lease_token', ARGV[2], 'lease_owner', ARGV[3], 'lease_expires_at_ms', ARGV[4])
      redis.call('ZADD', KEYS[3], ARGV[4], id)
      local checked = '0'
      if (redis.call('HGET', key, 'last_checked_at_ms') or '') ~= '' then checked = '1' end
      return {id, url, checked}
    end
  end
end
return {}
`)

// ClaimDue atomically leases one due candidate so multiple replicas cannot test
// it concurrently. Claim.Retest reports whether the candidate already has a
// recorded check (last_checked_at_ms), distinguishing first probes from retests.
func (s *Redis) ClaimDue(ctx context.Context, worker string, now time.Time, leaseTTL time.Duration) (Claim, bool, error) {
	token, err := randomToken()
	if err != nil {
		return Claim{}, false, err
	}
	expiresAt := now.Add(leaseTTL).UnixMilli()
	values, err := claimScript.Run(ctx, s.client,
		[]string{s.pendingKey(), s.prefix + ":proxy:", s.leasedKey()},
		now.UnixMilli(), token, worker, expiresAt).StringSlice()
	if err == redis.Nil {
		return Claim{}, false, nil
	}
	if err != nil {
		return Claim{}, false, err
	}
	if len(values) == 0 {
		return Claim{}, false, nil
	}
	if len(values) != 3 {
		return Claim{}, false, fmt.Errorf("unexpected Redis claim response")
	}
	return Claim{ID: values[0], URL: values[1], Token: token, Retest: values[2] == "1"}, true, nil
}

// MaxClaimBatch is the most candidates one ClaimDueBatch call leases. The claim
// runs as a single Lua script, during which Redis serves nothing else, so the
// batch is bounded: 64 claims is roughly a millisecond of script time.
const MaxClaimBatch = 64

// claimBatchScript leases up to ARGV[4] due candidates in one call. ARGV[5..]
// are the lease tokens, one per claim in the order claimed. It applies the same
// rules as claimScript to each candidate: a pending entry with no hash, an
// active lease, or an existing token is dropped without being claimed. The
// number of pending entries it looks at is bounded (the batch plus 100) so a
// run of stale entries cannot hold Redis.
var claimBatchScript = redis.NewScript(`
local want = tonumber(ARGV[4])
local claimed = 0
local popped = 0
local limit = want + 100
local out = {}
while claimed < want and popped < limit do
  local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, math.min(want - claimed, limit - popped))
  if #ids == 0 then break end
  for _, id in ipairs(ids) do
    popped = popped + 1
    if redis.call('ZREM', KEYS[1], id) == 1 then
      local key = KEYS[2] .. id
      local url = redis.call('HGET', key, 'url')
      local token = redis.call('HGET', key, 'lease_token') or ''
      if url and not redis.call('ZSCORE', KEYS[3], id) and token == '' then
        claimed = claimed + 1
        redis.call('HSET', key, 'lease_token', ARGV[4 + claimed], 'lease_owner', ARGV[2], 'lease_expires_at_ms', ARGV[3])
        redis.call('ZADD', KEYS[3], ARGV[3], id)
        local checked = '0'
        if (redis.call('HGET', key, 'last_checked_at_ms') or '') ~= '' then checked = '1' end
        out[#out + 1] = id
        out[#out + 1] = url
        out[#out + 1] = checked
      end
    end
  end
end
return out
`)

// ClaimDueBatch atomically leases up to n due candidates (at most MaxClaimBatch)
// in one Redis call, with the same semantics as n successful ClaimDue calls. It
// returns fewer than n, or none, when fewer are due. Every claim has its own
// lease token. One call replaces n round trips, which matters when many workers
// would otherwise each poll Redis for work.
func (s *Redis) ClaimDueBatch(ctx context.Context, worker string, now time.Time, leaseTTL time.Duration, n int) ([]Claim, error) {
	if n <= 0 {
		return nil, nil
	}
	if n > MaxClaimBatch {
		n = MaxClaimBatch
	}
	tokens := make([]string, n)
	args := make([]any, 0, 4+n)
	args = append(args, now.UnixMilli(), worker, now.Add(leaseTTL).UnixMilli(), n)
	for i := range tokens {
		token, err := randomToken()
		if err != nil {
			return nil, err
		}
		tokens[i] = token
		args = append(args, token)
	}
	values, err := claimBatchScript.Run(ctx, s.client,
		[]string{s.pendingKey(), s.prefix + ":proxy:", s.leasedKey()}, args...).StringSlice()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if len(values)%3 != 0 || len(values)/3 > n {
		return nil, fmt.Errorf("unexpected Redis batch claim response")
	}
	claims := make([]Claim, 0, len(values)/3)
	for i := 0; i+2 < len(values); i += 3 {
		claims = append(claims, Claim{ID: values[i], URL: values[i+1], Token: tokens[i/3], Retest: values[i+2] == "1"})
	}
	return claims, nil
}

// Complete atomically records a probe outcome under the lease token. Validated
// candidates are rescheduled after RetestPolicy.ValidatedAfter, failed ones
// after FailedAfter with a consecutive_failures counter; reaching
// MaxConsecutiveFailures evicts the candidate entirely (hash deleted, removed
// from candidates/validated/pending) so persistent corpses stop consuming
// budget forever.
//
// Each outcome also maintains stability history in the hash:
//
//	results10      last 10 outcomes as "1"/"0" characters
//	ok_ratio_pct   percentage of successes within results10
//	latency_ewma_ms / jitter_ewma_ms
//	               exponentially weighted latency average and mean absolute
//	               deviation (successful probes only)
//
// Country and ASN annotations are stored when non-empty and never overwritten
// by an empty value.
//
// Validated-set membership requires at least ARGV[18] (min samples) outcomes
// in results10 and ok_ratio_pct >= 80. Successes record last_ok_at_ms; a
// success below that bar is rescheduled at ARGV[19]. A failure that leaves
// the candidate listed is rescheduled at ARGV[17] instead of ARGV[10].
//
// Writing any of anonymity/exit_ip/exit_country (ARGV[13..15]) also sets
// anonymity_checked_at_ms. ARGV[20] https_ok ("1"/"0"), ARGV[21]
// https_checked_at_ms, and ARGV[22] https_tunnel_scheme record a sampled
// HTTPS-capability check; empty values leave the stored fields unchanged.
// ARGV[23] tampered ("1"/"0") and ARGV[24] tamper_checked_at_ms record an
// echo tamper check the same way.
var completeScript = redis.NewScript(`
local key = KEYS[1]
if redis.call('HGET', key, 'lease_token') ~= ARGV[1] then return 0 end
local status = ARGV[2]
local now_ms = ARGV[3]
local id = ARGV[7]
redis.call('ZREM', KEYS[2], id)

-- Burst/initial-sweep mode intentionally drops failed candidates after the
-- lease is verified. Do this before recording failure detail or history so a
-- large unsuccessful pass does not grow the Redis hash or AOF unnecessarily.
if status ~= 'ok' and ARGV[16] == '1' then
  redis.call('ZREM', KEYS[3], id)
  redis.call('SREM', KEYS[4], id)
  redis.call('SREM', KEYS[5], id)
  redis.call('DEL', key)
  return 3
end

redis.call('HSET', key,
  'lease_token', '', 'lease_owner', '', 'lease_expires_at_ms', '0',
  'last_checked_at_ms', now_ms, 'last_status', status,
  'last_http_status', ARGV[4], 'last_latency_ms', ARGV[5], 'last_error', ARGV[6])

local hist = redis.call('HGET', key, 'results10') or ''
if status == 'ok' then hist = hist .. '1' else hist = hist .. '0' end
if string.len(hist) > 10 then hist = string.sub(hist, -10) end
local oks = 0
for i = 1, string.len(hist) do
  if string.sub(hist, i, i) == '1' then oks = oks + 1 end
end
local ratio = math.floor(oks * 100 / string.len(hist) + 0.5)
local min_samples = tonumber(ARGV[18]) or 1
local listable = string.len(hist) >= min_samples and ratio >= 80

local lat = tonumber(ARGV[5]) or 0
local ewma = tonumber(redis.call('HGET', key, 'latency_ewma_ms') or '0')
local jitter = tonumber(redis.call('HGET', key, 'jitter_ewma_ms') or '0')

if status == 'ok' then
  if ewma > 0 then
    jitter = jitter * 0.7 + math.abs(lat - ewma) * 0.3
    ewma = ewma * 0.7 + lat * 0.3
  else
    ewma = lat
    jitter = 0
  end
  redis.call('HSET', key,
    'results10', hist, 'ok_ratio_pct', tostring(ratio),
    'latency_ewma_ms', string.format('%.0f', ewma),
    'jitter_ewma_ms', string.format('%.0f', jitter),
    'consecutive_failures', '0', 'last_ok_at_ms', now_ms)
  -- Listing requires both enough samples and a stable rolling ratio, so a
  -- single lucky success cannot publish a candidate. Candidates below the
  -- bar get a spaced follow-up (ARGV[19]) that lets the window converge
  -- ahead of the bulk queue without hammering the same endpoint.
  local next_due = ARGV[8]
  if listable then
    redis.call('SADD', KEYS[4], id)
  else
    redis.call('SREM', KEYS[4], id)
    next_due = ARGV[19]
  end
  redis.call('ZADD', KEYS[3], next_due, id)
else
  redis.call('HSET', key, 'results10', hist, 'ok_ratio_pct', tostring(ratio))
  -- A single transient failure must not remove a candidate whose rolling
  -- history is still stable. Keep the validated-set membership aligned with
  -- the same criterion used by the success branch.
  if listable then
    redis.call('SADD', KEYS[4], id)
  else
    redis.call('SREM', KEYS[4], id)
  end
  local failures = tonumber(redis.call('HINCRBY', key, 'consecutive_failures', 1))
  if tonumber(ARGV[9]) > 0 and failures >= tonumber(ARGV[9]) then
    redis.call('ZREM', KEYS[3], id)
    redis.call('SREM', KEYS[4], id)
    redis.call('SREM', KEYS[5], id)
    redis.call('DEL', key)
    return 2
  end
  -- A proxy that is still listed after this failure is re-probed soon
  -- (ARGV[17]) so a dead listed proxy is confirmed and delisted quickly.
  local failed_due = ARGV[10]
  if listable then failed_due = ARGV[17] end
  redis.call('ZADD', KEYS[3], failed_due, id)
end

if ARGV[11] ~= '' then redis.call('HSET', key, 'country', ARGV[11]) end
if ARGV[12] ~= '' then redis.call('HSET', key, 'asn', ARGV[12]) end
-- Anonymity/exit fields are measured together; any write refreshes
-- anonymity_checked_at_ms so readers can age out stale classifications.
local classified = false
if ARGV[13] ~= '' then redis.call('HSET', key, 'anonymity', ARGV[13]); classified = true end
if ARGV[14] ~= '' then redis.call('HSET', key, 'exit_ip', ARGV[14]); classified = true end
if ARGV[15] ~= '' then redis.call('HSET', key, 'exit_country', ARGV[15]); classified = true end
if classified then redis.call('HSET', key, 'anonymity_checked_at_ms', now_ms) end
-- ARGV[20] https_ok ('1'/'0', '' = unchanged), ARGV[21] https_checked_at_ms,
-- ARGV[22] https_tunnel_scheme ('' = unchanged).
if ARGV[20] ~= nil and ARGV[20] ~= '' then
  redis.call('HSET', key, 'https_ok', ARGV[20], 'https_checked_at_ms', ARGV[21])
end
if ARGV[22] ~= nil and ARGV[22] ~= '' then redis.call('HSET', key, 'https_tunnel_scheme', ARGV[22]) end
-- ARGV[23] tampered ('1'/'0', '' = unchanged), ARGV[24] tamper_checked_at_ms.
if ARGV[23] ~= nil and ARGV[23] ~= '' then
  redis.call('HSET', key, 'tampered', ARGV[23], 'tamper_checked_at_ms', ARGV[24])
end
return 1
`)

// RetestPolicy configures tiered rescheduling in Complete.
type RetestPolicy struct {
	ValidatedAfter          time.Duration
	FailedAfter             time.Duration
	MaxConsecutiveFailures  int // 0 disables eviction
	DiscardFailedCandidates bool
	// ListedFailureAfter reschedules a failed probe whose candidate remains
	// in the validated set; <= 0 falls back to FailedAfter.
	ListedFailureAfter time.Duration
	// SampleRetestAfter reschedules a successful probe whose candidate is not
	// yet listable; <= 0 re-queues it immediately.
	SampleRetestAfter time.Duration
	// MinSamplesForListing is the rolling-history length required for
	// validated-set membership; <= 0 is treated as 1.
	MinSamplesForListing int

	// JitterPct spreads ValidatedAfter and FailedAfter uniformly by up to
	// ±JitterPct percent (0 keeps exact intervals) so candidates checked
	// together do not come due together again.
	JitterPct int
	// JitterInt64N returns a uniform value in [0, n). Nil uses math/rand/v2;
	// tests inject a deterministic source.
	JitterInt64N func(n int64) int64

	// EvictedBackoffBase blocks re-adding an evicted or discarded endpoint for
	// base * 2^(strikes-1), capped at EvictedBackoffMax. Zero disables
	// tombstones.
	EvictedBackoffBase time.Duration
	EvictedBackoffMax  time.Duration
}

// jitter returns d moved by a uniform offset in [-JitterPct%, +JitterPct%].
func (p RetestPolicy) jitter(d time.Duration) time.Duration {
	if p.JitterPct <= 0 || d <= 0 {
		return d
	}
	pct := int64(p.JitterPct)
	if pct > 100 {
		pct = 100
	}
	span := int64(d) / 100 * pct
	if span <= 0 {
		return d
	}
	draw := p.JitterInt64N
	if draw == nil {
		draw = mathrand.Int64N
	}
	return d - time.Duration(span) + time.Duration(draw(2*span+1))
}

// tombstoneRetention is how long strike memory outlives the backoff window.
// It must cover the time a re-added endpoint needs to be evicted again
// (MaxConsecutiveFailures probes spaced by FailedAfter, plus jitter and refresh
// lag), otherwise the strike count would reset before the next eviction.
func (p RetestPolicy) tombstoneRetention() time.Duration {
	probes := p.MaxConsecutiveFailures
	if probes < 1 {
		probes = 1
	}
	return time.Duration(probes) * p.FailedAfter * 2
}

func (p RetestPolicy) minSamples() int {
	if p.MinSamplesForListing < 1 {
		return 1
	}
	return p.MinSamplesForListing
}

// CompleteResult reports whether the outcome was committed and whether the
// candidate was evicted after too many consecutive failures.
type CompleteResult struct {
	Committed bool
	Evicted   bool
	Discarded bool
	// Strikes and BlockedFor describe the tombstone written for an evicted or
	// discarded endpoint; both are zero when tombstones are disabled.
	Strikes    int64
	BlockedFor time.Duration
}

// tombstoneScript records one more eviction strike for KEYS[1], a STRING
// "strikes:until_ms". ARGV: now_ms, base_ms, max_ms, retention_ms. The block
// lasts base*2^(strikes-1) capped at max; the key expires after the block plus
// max(block, retention), so memory is bounded and self-cleaning.
var tombstoneScript = redis.NewScript(`
local now_ms = tonumber(ARGV[1])
local base = tonumber(ARGV[2])
local cap = tonumber(ARGV[3])
local retention = tonumber(ARGV[4])
local strikes = 0
local current = redis.call('GET', KEYS[1])
if current then strikes = tonumber(string.match(current, '^(%d+):')) or 0 end
strikes = strikes + 1
if strikes > 64 then strikes = 64 end
local backoff = base
for _ = 2, strikes do
  if backoff >= cap then break end
  backoff = backoff * 2
end
if backoff > cap then backoff = cap end
local ttl = backoff + math.max(backoff, retention)
redis.call('SET', KEYS[1], string.format('%.0f:%.0f', strikes, now_ms + backoff), 'PX', string.format('%.0f', ttl))
return {strikes, backoff}
`)

// OutcomeMeta carries enrichment annotations recorded alongside a probe
// outcome. Empty values never overwrite previously stored ones.
type OutcomeMeta struct {
	Country     string
	ASN         string
	Anonymity   string // elite | anonymous | transparent
	ExitIP      string
	ExitCountry string // GeoIP country of the observed exit IP

	// HTTPSChecked records a sampled HTTPS-capability check (https_ok and
	// https_checked_at_ms); when false the stored HTTPS fields are unchanged.
	HTTPSChecked bool
	HTTPSOK      bool
	// HTTPSTunnelScheme is the proxy hop scheme that carried a successful
	// HTTPS check ("http", "https", or a SOCKS scheme); empty leaves it as is.
	HTTPSTunnelScheme string

	// TamperChecked records an echo tamper check (tampered and
	// tamper_checked_at_ms); when false the stored tamper fields are unchanged.
	TamperChecked bool
	Tampered      bool
}

func (m OutcomeMeta) httpsArgs(now time.Time) (ok, checkedAt string) {
	if !m.HTTPSChecked {
		return "", ""
	}
	return boolArg(m.HTTPSOK), strconv.FormatInt(now.UnixMilli(), 10)
}

func (m OutcomeMeta) tamperArgs(now time.Time) (tampered, checkedAt string) {
	if !m.TamperChecked {
		return "", ""
	}
	return boolArg(m.Tampered), strconv.FormatInt(now.UnixMilli(), 10)
}

// Complete records the outcome of one claimed probe, maintaining stability
// history and optional annotations; see RetestPolicy and the completeScript
// documentation.
func (s *Redis) Complete(ctx context.Context, claim Claim, now time.Time, policy RetestPolicy, ok bool, statusCode int, latency time.Duration, detail string, meta OutcomeMeta) (CompleteResult, error) {
	status := "failed"
	if ok {
		status = "ok"
	}
	validatedDue := now.Add(policy.jitter(policy.ValidatedAfter)).UnixMilli()
	failedDue := now.Add(policy.jitter(policy.FailedAfter)).UnixMilli()
	// The listed-failure and sample follow-ups are jittered too, so proxies
	// that failed or were sampled together do not come due together again.
	listedFailureDue := failedDue
	if policy.ListedFailureAfter > 0 {
		listedFailureDue = now.Add(policy.jitter(policy.ListedFailureAfter)).UnixMilli()
	}
	sampleDue := int64(0)
	if policy.SampleRetestAfter > 0 {
		sampleDue = now.Add(policy.jitter(policy.SampleRetestAfter)).UnixMilli()
	}
	httpsOK, httpsCheckedAt := meta.httpsArgs(now)
	tampered, tamperCheckedAt := meta.tamperArgs(now)
	result, err := completeScript.Run(ctx, s.client,
		[]string{s.proxyKey(claim.ID), s.leasedKey(), s.pendingKey(), s.validatedKey(), s.candidatesKey()},
		claim.Token, status, now.UnixMilli(), statusCode, latency.Milliseconds(), detail,
		claim.ID, validatedDue,
		policy.MaxConsecutiveFailures, failedDue,
		meta.Country, meta.ASN, meta.Anonymity, meta.ExitIP, meta.ExitCountry,
		boolArg(policy.DiscardFailedCandidates),
		listedFailureDue, policy.minSamples(), sampleDue,
		httpsOK, httpsCheckedAt, meta.HTTPSTunnelScheme,
		tampered, tamperCheckedAt).Int()
	if err != nil {
		return CompleteResult{}, err
	}
	outcome := CompleteResult{Committed: result >= 1, Evicted: result == 2, Discarded: result == 3}
	if (outcome.Evicted || outcome.Discarded) && policy.EvictedBackoffBase > 0 {
		// The tombstone is a separate script so the shared completion script
		// keeps its argument layout. A crash between the two calls only loses
		// one backoff; the next eviction writes it again.
		strikes, blocked, err := s.writeTombstone(ctx, claim.ID, now, policy)
		if err != nil {
			return outcome, fmt.Errorf("write eviction tombstone: %w", err)
		}
		outcome.Strikes, outcome.BlockedFor = strikes, blocked
	}
	return outcome, nil
}

func (s *Redis) writeTombstone(ctx context.Context, id string, now time.Time, policy RetestPolicy) (int64, time.Duration, error) {
	maxBackoff := policy.EvictedBackoffMax
	if maxBackoff < policy.EvictedBackoffBase {
		maxBackoff = policy.EvictedBackoffBase
	}
	values, err := tombstoneScript.Run(ctx, s.client, []string{s.tombstoneKey(id)},
		now.UnixMilli(), policy.EvictedBackoffBase.Milliseconds(), maxBackoff.Milliseconds(),
		policy.tombstoneRetention().Milliseconds()).Int64Slice()
	if err != nil {
		return 0, 0, err
	}
	if len(values) != 2 {
		return 0, 0, fmt.Errorf("unexpected Redis tombstone response")
	}
	return values[0], time.Duration(values[1]) * time.Millisecond, nil
}

func boolArg(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

// ValidatedInfo is a slice of one validated candidate's routing-relevant
// attributes. It carries no endpoint identity.
type ValidatedInfo struct {
	Country     string
	ExitCountry string
	ASN         string
	Anonymity   string
	LatencyMs   int64
	JitterMs    int64
	OkRatioPct  int64
}

// ValidatedDetails returns country/latency/ratio attributes for every
// currently-validated candidate, in arbitrary order. Endpoint URLs are never
// included.
func (s *Redis) ValidatedDetails(ctx context.Context) ([]ValidatedInfo, error) {
	out := make([]ValidatedInfo, 0)
	err := s.scanValidated(ctx, func(ids []string) error {
		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, len(ids))
		for _, id := range ids {
			cmds = append(cmds, pipe.HMGet(ctx, s.proxyKey(id),
				"country", "exit_country", "asn", "anonymity",
				"latency_ewma_ms", "jitter_ewma_ms", "ok_ratio_pct"))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for _, cmd := range cmds {
			values, err := cmd.Result()
			if err != nil || len(values) != 7 || allNil(values) {
				// All-nil means the hash was deleted (evicted) after the scan;
				// counting it would report a ghost candidate.
				continue
			}
			info := ValidatedInfo{}
			if v, _ := values[0].(string); v != "" {
				info.Country = v
			}
			if v, _ := values[1].(string); v != "" {
				info.ExitCountry = v
			}
			if v, _ := values[2].(string); v != "" {
				info.ASN = v
			}
			if v, _ := values[3].(string); v != "" {
				info.Anonymity = v
			}
			if v, _ := values[4].(string); v != "" {
				info.LatencyMs, _ = strconv.ParseInt(v, 10, 64)
			}
			if v, _ := values[5].(string); v != "" {
				info.JitterMs, _ = strconv.ParseInt(v, 10, 64)
			}
			if v, _ := values[6].(string); v != "" {
				info.OkRatioPct, _ = strconv.ParseInt(v, 10, 64)
			}
			out = append(out, info)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func allNil(values []interface{}) bool {
	for _, v := range values {
		if v != nil {
			return false
		}
	}
	return true
}

// validatedScanCount is the SSCAN COUNT hint used when visiting the whole
// validated set. Each page stays well below the pipeline batch sizes used by
// callers, so a very large set never becomes one blocking SMEMBERS reply.
const validatedScanCount = 1000

// scanValidated visits the validated set in SSCAN pages. SSCAN may return a
// member more than once; repeats are filtered so callers see each ID once,
// matching the previous SMEMBERS semantics.
func (s *Redis) scanValidated(ctx context.Context, fn func([]string) error) error {
	seen := make(map[string]struct{})
	var cursor uint64
	for {
		page, next, err := s.client.SScan(ctx, s.validatedKey(), cursor, "", validatedScanCount).Result()
		if err != nil {
			return err
		}
		ids := make([]string, 0, len(page))
		for _, id := range page {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
		if len(ids) > 0 {
			if err := fn(ids); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

var reclaimScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
local reclaimed = 0
for _, id in ipairs(ids) do
  local key = KEYS[2] .. id
  if tonumber(redis.call('HGET', key, 'lease_expires_at_ms') or '0') <= tonumber(ARGV[1]) then
    redis.call('HSET', key, 'lease_token', '', 'lease_owner', '', 'lease_expires_at_ms', '0')
    redis.call('ZREM', KEYS[1], id)
    redis.call('ZADD', KEYS[3], ARGV[1], id)
    reclaimed = reclaimed + 1
  end
end
return reclaimed
`)

var releaseScript = redis.NewScript(`
local key = KEYS[1]
if redis.call('HGET', key, 'lease_token') ~= ARGV[1] then return 0 end
redis.call('HSET', key, 'lease_token', '', 'lease_owner', '', 'lease_expires_at_ms', '0')
if redis.call('ZREM', KEYS[2], ARGV[3]) == 1 then
  redis.call('ZADD', KEYS[3], ARGV[2], ARGV[3])
end
return 1
`)

// Release returns an owned claim to the pending queue immediately so a shutting
// down worker does not leave candidates waiting out the lease TTL. A claim whose
// lease was already lost or reclaimed is left untouched.
func (s *Redis) Release(ctx context.Context, claim Claim, now time.Time) (bool, error) {
	released, err := releaseScript.Run(ctx, s.client,
		[]string{s.proxyKey(claim.ID), s.leasedKey(), s.pendingKey()},
		claim.Token, now.UnixMilli(), claim.ID).Int()
	if err != nil {
		return false, err
	}
	return released == 1, nil
}

func (s *Redis) ReclaimExpired(ctx context.Context, now time.Time, limit int64) (int64, error) {
	if limit < 1 {
		limit = 1
	}
	return reclaimScript.Run(ctx, s.client,
		[]string{s.leasedKey(), s.prefix + ":proxy:", s.pendingKey()}, now.UnixMilli(), limit).Int64()
}

func (s *Redis) Stats(ctx context.Context) (Stats, error) {
	return s.StatsAt(ctx, time.Now())
}

func parseOptionalRedisInt(values []interface{}, index int, name string) (int64, error) {
	if index >= len(values) || values[index] == nil {
		return 0, nil
	}
	raw, ok := values[index].(string)
	if !ok || raw == "" {
		return 0, fmt.Errorf("parse %s source refresh count: unexpected Redis value", name)
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse %s source refresh count: %w", name, err)
	}
	return parsed, nil
}

// StatsAt returns queue and validation state as observed at now. Keeping the
// timestamp explicit makes due-work gauges deterministic in tests and keeps
// all queue timing calculations on the same clock sample.
func (s *Redis) StatsAt(ctx context.Context, now time.Time) (Stats, error) {
	pipe := s.client.Pipeline()
	candidates := pipe.SCard(ctx, s.candidatesKey())
	sourceUnique := pipe.Get(ctx, s.sourceUniqueKey())
	sourceStats := pipe.HMGet(ctx, s.sourceStatsKey(), "parsed", "succeeded")
	pending := pipe.ZCard(ctx, s.pendingKey())
	pendingDue := pipe.ZCount(ctx, s.pendingKey(), "-inf", strconv.FormatInt(now.UnixMilli(), 10))
	leased := pipe.ZCard(ctx, s.leasedKey())
	validated := pipe.SCard(ctx, s.validatedKey())
	next := pipe.ZRangeWithScores(ctx, s.pendingKey(), 0, 0)
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return Stats{}, err
	}
	unique := int64(0)
	if raw := sourceUnique.Val(); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return Stats{}, fmt.Errorf("parse source unique count: %w", err)
		}
		unique = parsed
	}
	parsed, err := parseOptionalRedisInt(sourceStats.Val(), 0, "parsed")
	if err != nil {
		return Stats{}, err
	}
	succeeded, err := parseOptionalRedisInt(sourceStats.Val(), 1, "succeeded")
	if err != nil {
		return Stats{}, err
	}
	stats := Stats{
		Candidates:       candidates.Val(),
		SourceUnique:     unique,
		SourceParsed:     parsed,
		SourcesSucceeded: succeeded,
		Pending:          pending.Val(),
		PendingDue:       pendingDue.Val(),
		Leased:           leased.Val(),
		Validated:        validated.Val(),
	}
	if values := next.Val(); len(values) > 0 {
		score := int64(values[0].Score)
		if score <= 0 {
			score = now.UnixMilli()
		}
		stats.NextDueAt = time.UnixMilli(score)
		stats.HasNextDue = true
	}
	return stats, nil
}

// ValidationSchedule returns the cluster-wide validation state without
// exposing candidate identities or endpoint URLs.
func (s *Redis) ValidationSchedule(ctx context.Context, now time.Time) (ValidationSchedule, error) {
	pipe := s.client.Pipeline()
	leased := pipe.ZCard(ctx, s.leasedKey())
	next := pipe.ZRangeWithScores(ctx, s.pendingKey(), 0, 0)
	if _, err := pipe.Exec(ctx); err != nil {
		return ValidationSchedule{}, err
	}
	schedule := ValidationSchedule{Leased: leased.Val()}
	if len(next.Val()) == 0 {
		return schedule, nil
	}
	score := int64(next.Val()[0].Score)
	if score <= 0 {
		score = now.UnixMilli()
	}
	schedule.NextDueAt = time.UnixMilli(score)
	schedule.HasNextDue = true
	return schedule, nil
}

func (s *Redis) sourceLockKey() string { return s.prefix + ":source-refresh-lock" }

// TrySourceLock prevents every replica from fetching the same inventory at the
// same time. Losing the lock is normal and not an error. The winner receives a
// token that must be passed to FinishSourceLock when its refresh ends; ttl is
// only a crash fallback.
func (s *Redis) TrySourceLock(ctx context.Context, ttl time.Duration) (string, bool, error) {
	token, err := randomToken()
	if err != nil {
		return "", false, err
	}
	won, err := s.setNX(ctx, s.sourceLockKey(), token, ttl)
	if err != nil || !won {
		return "", false, err
	}
	return token, true, nil
}

// setNX sets key only if it does not exist, with ttl as its expiry (none when
// ttl is zero), and reports whether this call created it. It replaces the
// deprecated go-redis SetNX with the equivalent SET ... NX form.
func (s *Redis) setNX(ctx context.Context, key, value string, ttl time.Duration) (bool, error) {
	err := s.client.SetArgs(ctx, key, value, redis.SetArgs{Mode: "NX", TTL: ttl}).Err()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	return err == nil, err
}

var finishSourceLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) ~= ARGV[1] then
  return 0
end
local hold = tonumber(ARGV[2])
if hold > 0 then
  return redis.call("pexpire", KEYS[1], hold)
end
return redis.call("del", KEYS[1])
`)

var renewSourceLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) ~= ARGV[1] then
  return 0
end
return redis.call("pexpire", KEYS[1], ARGV[2])
`)

// RenewSourceLock extends a refresh lease only when token still owns it. A
// false result means the lease expired or another owner recovered it.
func (s *Redis) RenewSourceLock(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	n, err := renewSourceLockScript.Run(ctx, s.client, []string{s.sourceLockKey()}, token, ttl.Milliseconds()).Int64()
	return n == 1, err
}

// FinishSourceLock re-arms the refresh lock held under token to expire after
// hold, so the cluster-wide refresh cadence follows the fetch interval rather
// than the crash-fallback TTL. A non-positive hold releases the lock at once.
// A lock that expired or now belongs to another replica is left untouched.
func (s *Redis) FinishSourceLock(ctx context.Context, token string, hold time.Duration) error {
	return finishSourceLockScript.Run(ctx, s.client, []string{s.sourceLockKey()}, token, hold.Milliseconds()).Err()
}

// TakePermit applies a simple Redis-wide per-minute ceiling across all workers.
// A denied permit performs no proxy or target network request.
func (s *Redis) TakePermit(ctx context.Context, now time.Time, limit int) (bool, error) {
	if limit < 1 {
		return false, fmt.Errorf("global request limit must be positive")
	}
	window := now.UTC().Format("200601021504")
	key := s.prefix + ":probe-budget:" + window
	count, err := takePermitScript.Run(ctx, s.client, []string{key}, (2 * time.Minute).Milliseconds()).Int64()
	if err != nil {
		return false, err
	}
	return count <= int64(limit), nil
}

// takePermitScript increments the window counter and arms its expiry in one
// atomic step. A counter found without a TTL (for example one written by an
// older non-atomic implementation) is repaired as well.
var takePermitScript = redis.NewScript(`
local count = redis.call('INCR', KEYS[1])
if count == 1 or redis.call('PTTL', KEYS[1]) < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[1])
end
return count
`)

// reservePermitsScript grants up to ARGV[2] permits from the window counter
// without ever pushing it past the limit ARGV[1]. A counter already at or
// above the limit (for example one over-incremented by TakePermit denials)
// grants nothing. The expiry is armed or repaired whenever the key exists.
var reservePermitsScript = redis.NewScript(`
local limit = tonumber(ARGV[1])
local want = tonumber(ARGV[2])
local used = tonumber(redis.call('GET', KEYS[1]) or '0')
local grant = limit - used
if grant > want then grant = want end
if grant <= 0 then
  if used > 0 and redis.call('PTTL', KEYS[1]) < 0 then
    redis.call('PEXPIRE', KEYS[1], ARGV[3])
  end
  return 0
end
redis.call('INCRBY', KEYS[1], grant)
if used == 0 or redis.call('PTTL', KEYS[1]) < 0 then
  redis.call('PEXPIRE', KEYS[1], ARGV[3])
end
return grant
`)

// ReservePermits atomically reserves up to want permits from the same
// per-minute window counter TakePermit uses and returns how many were granted
// (0 when the window is exhausted). Callers hand the granted permits out
// locally; permits left unused when the window ends simply expire, so the
// cluster-wide ceiling is never exceeded.
func (s *Redis) ReservePermits(ctx context.Context, now time.Time, limit, want int) (int, error) {
	if limit < 1 {
		return 0, fmt.Errorf("global request limit must be positive")
	}
	if want < 1 {
		return 0, nil
	}
	window := now.UTC().Format("200601021504")
	key := s.prefix + ":probe-budget:" + window
	granted, err := reservePermitsScript.Run(ctx, s.client, []string{key}, limit, want, (2 * time.Minute).Milliseconds()).Int()
	if err != nil {
		return 0, err
	}
	return granted, nil
}

func (s *Redis) statsLeaderKey() string        { return s.prefix + ":stats-leader" }
func (s *Redis) validatedAggregateKey() string { return s.prefix + ":validated-aggregate" }

// NewLeaseToken returns a random token identifying one lease holder.
func NewLeaseToken() (string, error) { return randomToken() }

var acquireStatsLeaderScript = redis.NewScript(`
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
  redis.call('PEXPIRE', KEYS[1], ARGV[2])
  return 1
end
if current then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
return 1
`)

// AcquireStatsLeader renews the stats-leader lease held under token or takes
// it when it is free, and reports whether token holds the lease afterwards.
// Losing is normal: another replica is publishing the shared aggregate.
func (s *Redis) AcquireStatsLeader(ctx context.Context, token string, ttl time.Duration) (bool, error) {
	if token == "" || ttl <= 0 {
		return false, fmt.Errorf("stats leader token and ttl are required")
	}
	held, err := acquireStatsLeaderScript.Run(ctx, s.client, []string{s.statsLeaderKey()}, token, ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return held == 1, nil
}

var releaseStatsLeaderScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// ReleaseStatsLeader drops the stats-leader lease if token still holds it, so
// a shutting-down leader hands over at the next publish instead of after the
// lease TTL.
func (s *Redis) ReleaseStatsLeader(ctx context.Context, token string) error {
	return releaseStatsLeaderScript.Run(ctx, s.client, []string{s.statsLeaderKey()}, token).Err()
}

var publishValidatedAggregateScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'PX', ARGV[3])
return 1
`)

// PublishValidatedAggregate stores the shared validated-set aggregate (an
// opaque, identity-free JSON document) only while token holds the stats-leader
// lease, so a leader that lost its lease cannot overwrite its successor.
func (s *Redis) PublishValidatedAggregate(ctx context.Context, token string, payload []byte, ttl time.Duration) (bool, error) {
	written, err := publishValidatedAggregateScript.Run(ctx, s.client,
		[]string{s.statsLeaderKey(), s.validatedAggregateKey()}, token, payload, ttl.Milliseconds()).Int()
	if err != nil {
		return false, err
	}
	return written == 1, nil
}

// ValidatedAggregate returns the shared validated-set aggregate, or found=false
// when no leader has published one within its TTL.
func (s *Redis) ValidatedAggregate(ctx context.Context) ([]byte, bool, error) {
	payload, err := s.client.Get(ctx, s.validatedAggregateKey()).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return payload, true, nil
}

func proxyID(rawURL string) string {
	sum := sha256.Sum256([]byte(rawURL))
	return hex.EncodeToString(sum[:])
}

func randomToken() (string, error) {
	data := make([]byte, 16)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

// RunOnce executes fn under a one-shot lock keyed by name, returning true if
// this caller won the right to run it. Used for cluster-wide one-time jobs
// (e.g. startup enrichment) so N replicas do not repeat the work.
func (s *Redis) RunOnce(ctx context.Context, name string, ttl time.Duration, fn func(context.Context)) (bool, error) {
	return s.RunOnceWithError(ctx, name, ttl, func(jobCtx context.Context) error {
		fn(jobCtx)
		return nil
	})
}

// RunOnceWithError executes fn under a one-shot lock and releases the lock
// when fn fails so a later replica can retry the operation. Contenders wait
// for the winner to finish, which makes it safe for startup barriers. The
// completion marker is retained for ttl, while lock cleanup verifies token
// ownership so an expired lock cannot be deleted by an older caller.
func (s *Redis) RunOnceWithError(ctx context.Context, name string, ttl time.Duration, fn func(context.Context) error) (bool, error) {
	return s.runOnce(ctx, name, ttl, false, fn)
}

// RunOncePermanent is RunOnceWithError whose completion marker never expires,
// for migrations that must not re-arm on a later deploy. ttl bounds only the
// crash-fallback lock. An existing marker left by an expiring run is
// persisted so it cannot re-arm either.
func (s *Redis) RunOncePermanent(ctx context.Context, name string, ttl time.Duration, fn func(context.Context) error) (bool, error) {
	return s.runOnce(ctx, name, ttl, true, fn)
}

func (s *Redis) runOnce(ctx context.Context, name string, ttl time.Duration, permanent bool, fn func(context.Context) error) (bool, error) {
	doneKey := s.prefix + ":once:" + name
	lockKey := doneKey + ":lock"
	markerTTL := ttl
	if permanent {
		markerTTL = 0
	}
	for {
		done, err := s.client.Exists(ctx, doneKey).Result()
		if err != nil {
			return false, err
		}
		if done > 0 {
			if permanent {
				if err := s.client.Persist(ctx, doneKey).Err(); err != nil {
					return false, err
				}
			}
			return false, nil
		}
		token, err := randomToken()
		if err != nil {
			return false, err
		}
		won, err := s.setNX(ctx, lockKey, token, ttl)
		if err != nil {
			return false, err
		}
		if won {
			jobCtx, cancel := context.WithCancel(ctx)
			lost := s.renewOnceLock(jobCtx, cancel, lockKey, token, ttl)
			err := fn(jobCtx)
			cancel()
			lockLost := <-lost
			if err == nil && lockLost {
				err = ErrOnceLockLost
			}
			if err != nil {
				if lockLost && !errors.Is(err, ErrOnceLockLost) {
					err = fmt.Errorf("%w: %w", ErrOnceLockLost, err)
				}
				if releaseErr := s.releaseOnceLock(lockKey, token); releaseErr != nil {
					return true, fmt.Errorf("%w (release one-shot lock: %v)", err, releaseErr)
				}
				return true, err
			}
			if err := s.client.Set(ctx, doneKey, token, markerTTL).Err(); err != nil {
				_ = s.releaseOnceLock(lockKey, token)
				return true, err
			}
			if err := s.releaseOnceLock(lockKey, token); err != nil {
				return true, err
			}
			return true, nil
		}

		timer := time.NewTimer(250 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return false, ctx.Err()
		case <-timer.C:
		}
	}
}

// ErrOnceLockLost reports that a one-shot job's lock expired or was taken by
// another owner while the job was running. The job context is cancelled and
// no completion marker is written.
var ErrOnceLockLost = errors.New("one-shot lock lost while job was running")

var renewOnceLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("pexpire", KEYS[1], ARGV[2])
end
return 0
`)

// renewOnceLock extends the lock every ttl/3 while ctx is live. If a renewal
// finds the lock no longer owned by token, it cancels the job. The returned
// channel yields whether the lock was lost once ctx is done. Transient Redis
// errors are retried on the next tick rather than treated as loss.
func (s *Redis) renewOnceLock(ctx context.Context, cancel context.CancelFunc, key, token string, ttl time.Duration) <-chan bool {
	result := make(chan bool, 1)
	if ttl <= 0 {
		// A lock without expiry needs no renewal.
		result <- false
		return result
	}
	interval := ttl / 3
	if interval <= 0 {
		interval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				result <- false
				return
			case <-ticker.C:
				renewed, err := renewOnceLockScript.Run(ctx, s.client, []string{key}, token, ttl.Milliseconds()).Int64()
				if err != nil {
					continue
				}
				if renewed == 0 {
					cancel()
					result <- true
					return
				}
			}
		}
	}()
	return result
}

var releaseOnceLockScript = redis.NewScript(`
if redis.call("get", KEYS[1]) == ARGV[1] then
  return redis.call("del", KEYS[1])
end
return 0
`)

func (s *Redis) releaseOnceLock(key, token string) error {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := releaseOnceLockScript.Run(cleanupCtx, s.client, []string{key}, token).Result()
	return err
}

// ForcePendingDueNow moves every currently pending candidate to the due-now
// score. It is intended for an explicitly enabled, one-shot burst sweep; the
// caller is responsible for guarding it with RunOnceWithError.
func (s *Redis) ForcePendingDueNow(ctx context.Context) (int64, error) {
	const batch = int64(500)
	now := float64(time.Now().UnixMilli())
	var cursor uint64
	var processed int64
	for {
		values, next, err := s.client.ZScan(ctx, s.pendingKey(), cursor, "*", batch).Result()
		if err != nil {
			return processed, err
		}
		if len(values) > 0 {
			pipe := s.client.Pipeline()
			for i := 0; i+1 < len(values); i += 2 {
				// XX: only update members still pending. A candidate claimed or
				// evicted since the scan page was read must not be re-added.
				pipe.ZAddXX(ctx, s.pendingKey(), redis.Z{Score: now, Member: values[i]})
			}
			if _, err := pipe.Exec(ctx); err != nil {
				return processed, err
			}
			processed += int64(len(values) / 2)
		}
		cursor = next
		if cursor == 0 {
			return processed, nil
		}
	}
}

// backfillCountriesScript writes annotations only to hashes that still exist,
// so a candidate evicted between the member read and this write is not
// recreated as a stray hash without url or schedule.
var backfillCountriesScript = redis.NewScript(`
local written = 0
for i = 1, #ARGV, 3 do
  local key = KEYS[1] .. ARGV[i]
  if redis.call('EXISTS', key) == 1 then
    redis.call('HSET', key, 'country', ARGV[i + 1])
    if ARGV[i + 2] ~= '' then redis.call('HSET', key, 'asn', ARGV[i + 2]) end
    written = written + 1
  end
end
return written
`)

// BackfillCountries sets country (and optional asn) on candidate hashes whose
// country field is missing. Inputs pair candidate IDs with values; hashes that
// disappeared mid-run are skipped silently.
func (s *Redis) BackfillCountries(ctx context.Context, entries [][3]string) error {
	const batch = 500
	for start := 0; start < len(entries); start += batch {
		end := start + batch
		if end > len(entries) {
			end = len(entries)
		}
		args := make([]interface{}, 0, (end-start)*3)
		for _, e := range entries[start:end] {
			args = append(args, e[0], e[1], e[2])
		}
		if err := backfillCountriesScript.Run(ctx, s.client, []string{s.prefix + ":proxy:"}, args...).Err(); err != nil {
			return err
		}
	}
	return nil
}

// RequeueValidatedNow moves every currently-validated candidate's pending due
// time forward to now, so the enrichment pass re-probes the valuable tail
// immediately instead of waiting out its previous interval.
var requeueValidatedScript = redis.NewScript(`
local requeued = 0
for i = 1, #ARGV do
  local id = ARGV[i]
  local key = KEYS[5] .. id
  if redis.call('SISMEMBER', KEYS[1], id) == 1 then
    if redis.call('SISMEMBER', KEYS[2], id) == 0 or redis.call('EXISTS', key) == 0 then
      -- A stale validated member must not recreate an evicted candidate.
      redis.call('SREM', KEYS[1], id)
      redis.call('ZREM', KEYS[3], id)
    else
      local leased = redis.call('ZSCORE', KEYS[4], id)
      local token = redis.call('HGET', key, 'lease_token') or ''
      if leased or token ~= '' then
        redis.call('ZREM', KEYS[3], id)
      else
        redis.call('ZADD', KEYS[3], 0, id)
        requeued = requeued + 1
      end
    end
  end
end
return requeued
`)

func (s *Redis) RequeueValidatedNow(ctx context.Context, now time.Time) (int64, error) {
	// Score 0 puts these structurally ahead of every past-due backlog item:
	// claims drain oldest-first, and an unchecked corpse's score is its
	// original upsert time (already in the past), so "now" would queue the
	// valuable validated tail behind hundreds of thousands of dead entries.
	_ = now
	var requeued int64
	const batch = 500
	err := s.scanValidated(ctx, func(ids []string) error {
		for start := 0; start < len(ids); start += batch {
			end := start + batch
			if end > len(ids) {
				end = len(ids)
			}
			n, err := requeueValidatedScript.Run(ctx, s.client,
				[]string{s.validatedKey(), s.candidatesKey(), s.pendingKey(), s.leasedKey(), s.prefix + ":proxy:"},
				toIface(ids[start:end])...).Int64()
			if err != nil {
				return err
			}
			requeued += n
		}
		return nil
	})
	return requeued, err
}

// ValidatedMember pairs a validated candidate's identity with the attributes
// enrichment needs. Endpoint URLs are operator-protected state and are never
// logged or exported by callers.
type ValidatedMember struct {
	ID        string
	URL       string
	Country   string
	ASN       string
	Anonymity string
}

// ValidatedMembers returns every currently-validated candidate with its
// endpoint URL and existing enrichment annotations.
func (s *Redis) ValidatedMembers(ctx context.Context) ([]ValidatedMember, error) {
	out := make([]ValidatedMember, 0)
	err := s.scanValidated(ctx, func(ids []string) error {
		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, len(ids))
		for _, id := range ids {
			cmds = append(cmds, pipe.HMGet(ctx, s.proxyKey(id), "url", "country", "asn", "anonymity"))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		for i, cmd := range cmds {
			values, err := cmd.Result()
			if err != nil || len(values) != 4 {
				continue
			}
			m := ValidatedMember{ID: ids[i]}
			m.URL, _ = values[0].(string)
			m.Country, _ = values[1].(string)
			m.ASN, _ = values[2].(string)
			m.Anonymity, _ = values[3].(string)
			out = append(out, m)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// PruneFailedValidated removes validated-set members that no longer meet the
// membership rule the completion script enforces: a rolling success ratio of
// at least 80%, so a single transient failure does not evict a stable
// candidate. Records written before the ratio existed fall back to their
// latest outcome. Returns the number pruned.
func (s *Redis) PruneFailedValidated(ctx context.Context, minSamples int) (int64, error) {
	var pruned int64
	err := s.scanValidated(ctx, func(ids []string) error {
		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, len(ids))
		for _, id := range ids {
			cmds = append(cmds, pipe.HMGet(ctx, s.proxyKey(id), "ok_ratio_pct", "last_status", "results10"))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return err
		}
		var dead []string
		for i, cmd := range cmds {
			values, _ := cmd.Result()
			if len(values) == 3 && belowValidatedThreshold(values[0], values[1], values[2], minSamples) {
				dead = append(dead, ids[i])
			}
		}
		if len(dead) > 0 {
			n, err := s.client.SRem(ctx, s.validatedKey(), toIface(dead)...).Result()
			if err != nil {
				return err
			}
			pruned += n
		}
		return nil
	})
	return pruned, err
}

// belowValidatedThreshold mirrors the completion script's validated-set rule
// for one record's ok_ratio_pct, last_status, and results10 values: a record
// whose rolling history is shorter than minSamples, or whose ratio is below
// 80, is not listable. Legacy records without a history are judged by ratio
// or latest outcome only.
func belowValidatedThreshold(ratio, status, results interface{}, minSamples int) bool {
	if hist, ok := results.(string); ok && hist != "" && len(hist) < minSamples {
		return true
	}
	if raw, ok := ratio.(string); ok && raw != "" {
		pct, err := strconv.ParseFloat(raw, 64)
		return err == nil && pct < 80
	}
	last, _ := status.(string)
	return last == "failed"
}

func toIface(ss []string) []interface{} {
	out := make([]interface{}, len(ss))
	for i, v := range ss {
		out[i] = v
	}
	return out
}

// ProxyFilter selects which validated candidates an internal API query
// returns. Country is the proxy endpoint country; ExitCountry is the observed
// country of the proxy's egress IP. Zero values match everything; Limit <= 0
// returns all matches. A positive Limit optionally restricts the result count.
type ProxyFilter struct {
	Country      string
	ExitCountry  string
	ASN          string
	Anonymity    string
	GeoMismatch  bool // true -> only candidates whose exit country differs from their entry country
	MaxLatencyMs int64
	MinRatioPct  int64
	// MaxAgeMs > 0 keeps only candidates whose last successful probe
	// (last_ok_at_ms) is at most this many milliseconds old; candidates with
	// no recorded success are excluded.
	MaxAgeMs int64
	// HTTPS -> only candidates whose latest sampled HTTPS check passed within
	// ClassificationMaxAgeMs (any age when that is zero).
	HTTPS bool
	// ExcludeTampered -> drop candidates whose latest echo tamper check,
	// within ClassificationMaxAgeMs, found tampering. Unknown stays included.
	ExcludeTampered bool
	// ClassificationMaxAgeMs > 0 treats anonymity/exit, HTTPS, and tamper
	// measurements older than this many milliseconds (or never timestamped) as
	// unknown: they are blanked in results and never match anonymity,
	// exit_country, geo_mismatch, https, or exclude_tampered filters. Zero
	// trusts stored values of any age.
	ClassificationMaxAgeMs int64
	Limit                  int
}

// QueryProxy is one entry of a filtered internal-API response. Unlike the
// aggregate surfaces this DOES include the credential-free endpoint URL: it is
// served only by the authenticated internal API, never by /report or /metrics.
type QueryProxy struct {
	URL           string `json:"url"`
	Scheme        string `json:"scheme"`
	Country       string `json:"country"`
	ExitCountry   string `json:"exit_country,omitempty"`
	GeoMismatch   bool   `json:"geo_mismatch"`
	ASN           string `json:"asn"`
	Anonymity     string `json:"anonymity"`
	LatencyMs     int64  `json:"latency_ms"`
	JitterMs      int64  `json:"jitter_ms"`
	OkRatioPct    int64  `json:"ok_ratio_pct"`
	LastCheckedAt int64  `json:"last_checked_at_ms"`
	LastStatus    string `json:"last_status"`
	LastOkAt      int64  `json:"last_ok_at_ms"`
	// AnonymityCheckedAt is when anonymity/exit fields were last measured.
	AnonymityCheckedAt int64 `json:"anonymity_checked_at_ms,omitempty"`
	// HTTPSOK is the latest fresh sampled HTTPS-capability result; nil when
	// never checked or older than the classification max age.
	HTTPSOK           *bool  `json:"https_ok,omitempty"`
	HTTPSCheckedAt    int64  `json:"https_checked_at_ms,omitempty"`
	HTTPSTunnelScheme string `json:"https_tunnel_scheme,omitempty"`
	// Tampered is the latest fresh echo tamper-check result; nil when never
	// checked or older than the classification max age.
	Tampered        *bool `json:"tampered,omitempty"`
	TamperCheckedAt int64 `json:"tamper_checked_at_ms,omitempty"`
}

const (
	// queryScanBatch is the read size for a small page, which is cheap when a
	// query ends after a few matches.
	queryScanBatch = 128
	// queryPipelineBatch is the read size once a query may need many records,
	// matching queryValidatedIDs's pipeline size.
	queryPipelineBatch = 500
	// queryScanCount is the SSCAN COUNT hint. A query that finds fewer matches
	// than its limit reads the whole validated set, and every SSCAN call and
	// pipeline is a Redis round trip, so ask for many members at a time. On the
	// hosted service (about 800 validated members) this took a full scan from
	// about 14 round trips to 3.
	queryScanCount = 1024
)

// queryReadSize returns how many scanned IDs to read in one pipeline. Without a
// filter every ID that is read is returned, so read only what is still needed.
// A filter can reject records, so read a full page, larger when the limit says
// many records are wanted.
func queryReadSize(f ProxyFilter, needed int) int {
	if hasProxyFilter(f) {
		if f.Limit > queryScanBatch {
			return queryPipelineBatch
		}
		return queryScanBatch
	}
	if needed > queryPipelineBatch {
		return queryPipelineBatch
	}
	return needed
}

// QueryValidated returns validated candidates matching the filter. Positive
// limits use bounded SSCAN pages so a small response does not materialize or
// preallocate for the whole validated set. Unlimited queries preserve the
// historical all-members behavior.
func (s *Redis) QueryValidated(ctx context.Context, f ProxyFilter) ([]QueryProxy, error) {
	limit := f.Limit
	if limit <= 0 {
		out := make([]QueryProxy, 0)
		returned := make(map[string]struct{})
		err := s.scanValidated(ctx, func(ids []string) error {
			var err error
			out, err = s.queryValidatedIDs(ctx, ids, f, returned, out...)
			return err
		})
		if err != nil {
			return nil, err
		}
		return out, nil
	}

	initialCapacity := limit
	if initialCapacity > queryScanBatch {
		initialCapacity = queryScanBatch
	}
	out := make([]QueryProxy, 0, initialCapacity)
	returned := make(map[string]struct{}, initialCapacity)
	var cursor uint64
	for {
		ids, next, err := s.client.SScan(ctx, s.validatedKey(), cursor, "", queryScanCount).Result()
		if err != nil {
			return nil, err
		}
		for start := 0; start < len(ids) && len(out) < limit; {
			readSize := queryReadSize(f, limit-len(out))
			end := start + readSize
			if end > len(ids) {
				end = len(ids)
			}
			var err error
			out, err = s.queryValidatedIDs(ctx, ids[start:end], f, returned, out...)
			if err != nil {
				return nil, err
			}
			start = end
		}
		if len(out) >= limit || next == 0 {
			return out, nil
		}
		cursor = next
	}
}

func hasProxyFilter(f ProxyFilter) bool {
	return f.Country != "" || f.ExitCountry != "" || f.ASN != "" || f.Anonymity != "" ||
		f.GeoMismatch || f.MaxLatencyMs > 0 || f.MinRatioPct > 0 || f.MaxAgeMs > 0 || f.HTTPS ||
		f.ExcludeTampered
}

// freshAt reports whether a measurement timestamp is usable under a
// classification max age (zero maxAgeMs trusts any value).
func freshAt(checkedAtMs, nowMs, maxAgeMs int64) bool {
	if maxAgeMs <= 0 {
		return true
	}
	return checkedAtMs > 0 && nowMs-checkedAtMs <= maxAgeMs
}

// queryValidatedIDs appends matching IDs to initial. A membership check after
// each hash read avoids returning records whose latest completion removed them
// from validated while a scan was in progress.
func (s *Redis) queryValidatedIDs(ctx context.Context, ids []string, f ProxyFilter, returned map[string]struct{}, initial ...QueryProxy) ([]QueryProxy, error) {
	out := initial
	if out == nil {
		out = make([]QueryProxy, 0)
	}
	const batch = queryPipelineBatch
	nowMs := time.Now().UnixMilli()
	var okCutoffMs int64
	if f.MaxAgeMs > 0 {
		okCutoffMs = nowMs - f.MaxAgeMs
	}
	for start := 0; start < len(ids) && (f.Limit <= 0 || len(out) < f.Limit); start += batch {
		end := start + batch
		if end > len(ids) {
			end = len(ids)
		}
		pipe := s.client.Pipeline()
		cmds := make([]*redis.SliceCmd, 0, end-start)
		members := make([]*redis.BoolCmd, 0, end-start)
		for _, id := range ids[start:end] {
			cmds = append(cmds, pipe.HMGet(ctx, s.proxyKey(id),
				"url", "country", "exit_country", "asn", "anonymity",
				"latency_ewma_ms", "jitter_ewma_ms", "ok_ratio_pct", "last_checked_at_ms",
				"last_status", "last_ok_at_ms",
				"anonymity_checked_at_ms", "https_ok", "https_checked_at_ms", "https_tunnel_scheme",
				"tampered", "tamper_checked_at_ms"))
			members = append(members, pipe.SIsMember(ctx, s.validatedKey(), id))
		}
		if _, err := pipe.Exec(ctx); err != nil {
			return nil, err
		}
		for i, cmd := range cmds {
			id := ids[start+i]
			if !members[i].Val() {
				continue
			}
			if returned != nil {
				if _, duplicate := returned[id]; duplicate {
					continue
				}
			}
			values, err := cmd.Result()
			if err != nil || len(values) != 17 {
				continue
			}
			str := func(idx int) string { v, _ := values[idx].(string); return v }
			num := func(idx int) int64 { v, _ := values[idx].(string); n, _ := strconv.ParseInt(v, 10, 64); return n }
			anonymityCheckedAt := num(11)
			anonymity := str(4)
			exitCountry := str(2)
			if !freshAt(anonymityCheckedAt, nowMs, f.ClassificationMaxAgeMs) {
				// Stale or never-timestamped classifications are unknown.
				anonymity, exitCountry = "", ""
			}
			var httpsOK *bool
			httpsCheckedAt := num(13)
			if raw := str(12); raw != "" && freshAt(httpsCheckedAt, nowMs, f.ClassificationMaxAgeMs) {
				v := raw == "1"
				httpsOK = &v
			}
			if f.HTTPS && (httpsOK == nil || !*httpsOK) {
				continue
			}
			var tampered *bool
			tamperCheckedAt := num(16)
			if raw := str(15); raw != "" && freshAt(tamperCheckedAt, nowMs, f.ClassificationMaxAgeMs) {
				v := raw == "1"
				tampered = &v
			}
			if f.ExcludeTampered && tampered != nil && *tampered {
				continue
			}
			entryCountry := str(1)
			mismatch := entryCountry != "" && exitCountry != "" && entryCountry != exitCountry
			if f.Country != "" && entryCountry != f.Country {
				continue
			}
			if f.ExitCountry != "" && exitCountry != f.ExitCountry {
				continue
			}
			if f.ASN != "" && str(3) != f.ASN {
				continue
			}
			if f.Anonymity != "" && anonymity != f.Anonymity {
				continue
			}
			if f.GeoMismatch && !mismatch {
				continue
			}
			latency := num(5)
			if f.MaxLatencyMs > 0 && (latency <= 0 || latency > f.MaxLatencyMs) {
				continue
			}
			ratio := num(7)
			if f.MinRatioPct > 0 && ratio < f.MinRatioPct {
				continue
			}
			lastOk := num(10)
			if f.MaxAgeMs > 0 && (lastOk <= 0 || lastOk < okCutoffMs) {
				continue
			}
			rawURL := str(0)
			if rawURL == "" {
				continue
			}
			// Re-validate at the output boundary: records written before a
			// parser tightening (or by an older build) stay in Redis, and
			// the URL is echoed to public consumers verbatim.
			if _, perr := endpoint.ParseProxy(rawURL, ""); perr != nil {
				continue
			}
			scheme := ""
			if parsed, perr := url.Parse(rawURL); perr == nil {
				scheme = parsed.Scheme
			}
			out = append(out, QueryProxy{
				URL: rawURL, Scheme: scheme,
				Country: entryCountry, ExitCountry: exitCountry, GeoMismatch: mismatch,
				ASN: str(3), Anonymity: anonymity,
				LatencyMs: latency, JitterMs: num(6), OkRatioPct: ratio,
				LastCheckedAt: num(8),
				LastStatus:    str(9), LastOkAt: lastOk,
				AnonymityCheckedAt: anonymityCheckedAt,
				HTTPSOK:            httpsOK, HTTPSCheckedAt: httpsCheckedAt,
				HTTPSTunnelScheme: str(14),
				Tampered:          tampered, TamperCheckedAt: tamperCheckedAt,
			})
			if returned != nil {
				returned[id] = struct{}{}
			}
			if f.Limit > 0 && len(out) >= f.Limit {
				break
			}
		}
	}
	return out, nil
}
