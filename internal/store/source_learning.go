package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	sourceQualityTTL          = 30 * 24 * time.Hour
	sourceFailureBaseCooldown = time.Minute
	sourceFailureMaxCooldown  = time.Hour
	maxSourceHTTPAttempts     = int64(1000)
	maxSourceNetworkBytes     = int64(1 << 30)
	maxSourceCostDurationMS   = int64(24 * time.Hour / time.Millisecond)
	maxSourceAdmissionBatch   = int64(1_000_000_000)
	sourceStageScoreStride    = int64(1 << 16)
	maxSourceStagePriority    = int64((1<<53)-1) / sourceStageScoreStride
	MaxSourceQualityReadBatch = 512
)

var ErrSourceLearningBatchTooLarge = errors.New("source quality read exceeds batch limit")

// SourceQuality contains bounded, aggregate-only evidence used to rank a feed.
// ProbeSamples and ProbePasses are exponentially decayed counters. Invalid
// stored fields are zeroed and reported through Corrupt rather than failing a
// whole refresh-plan read.
type SourceQuality struct {
	RefreshFetches             int64
	RefreshSuccesses           int64
	FetchFailures              int64
	ConsecutiveFailures        int64
	Parsed                     int64
	UniqueIncremental          int64
	DuplicateRecords           int64
	LastFetchAt                time.Time
	LastSuccessAt              time.Time
	CooldownUntil              time.Time
	Admitted                   int64
	AdmissionRefreshes         int64
	LastAdmitted               int64
	ZeroAdmissionStreak        int64
	LastAdmissionAt            time.Time
	HTTPAttempts               int64
	NetworkBytes               int64
	FetchDurationMS            int64
	FetchCostObservations      int64
	ProbeSeen                  int64
	ProbeSamples               float64
	ProbePasses                float64
	ProbeCompletions           int64
	ProbeSuccessfulCompletions int64
	ProbeDurationMS            int64
	ProbeUpdatedAt             time.Time
	ProbeContract              string
	Corrupt                    bool
}

// RecordSourceFetchCost adds bounded request-cost observations for one source
// refresh attempt sequence. The source hash retention is refreshed atomically.
func (s *Redis) RecordSourceFetchCost(ctx context.Context, sourceID string, httpAttempts, networkBytes, durationMS int64, now time.Time) error {
	if !validSourceID(sourceID) || now.UnixMilli() <= 0 ||
		httpAttempts < 0 || httpAttempts > maxSourceHTTPAttempts ||
		networkBytes < 0 || networkBytes > maxSourceNetworkBytes ||
		durationMS < 0 || durationMS > maxSourceCostDurationMS {
		return fmt.Errorf("invalid source fetch cost")
	}
	return recordSourceFetchCostScript.Run(ctx, s.client, []string{s.sourceQualityKey(sourceID)},
		httpAttempts, networkBytes, durationMS, sourceQualityTTL.Milliseconds()).Err()
}

var recordSourceFetchCostScript = redis.NewScript(`
local key = KEYS[1]
local fields = {'http_attempts', 'network_bytes', 'fetch_duration_ms', 'fetch_cost_observations'}
local increments = {tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3]), 1}
local cap = 9007199254740991
for i = 1, #fields do
  local raw = redis.call('HGET', key, fields[i]) or '0'
  local current = tonumber(raw)
  if not string.match(raw, '^%d+$') or not current or current < 0 or current ~= math.floor(current) or current + increments[i] > cap then
    return redis.error_reply('invalid source cost counter')
  end
end
for i = 1, #fields do redis.call('HINCRBY', key, fields[i], tostring(increments[i])) end
redis.call('PEXPIRE', key, ARGV[4])
return 1
`)

// RecordSourceAdmissionRefresh records the per-source admission result from
// one completed global refresh. Callers must include successful participating
// sources with zero admissions; failed or skipped feeds are omitted. Admitted
// is counted only by the upsert SADD script, so partial refreshes retain their
// actual insertions without being mistaken for completed observations.
func (s *Redis) RecordSourceAdmissionRefresh(ctx context.Context, admittedBySource map[string]int64, now time.Time) error {
	if len(admittedBySource) > MaxSourceQualityReadBatch || now.UnixMilli() <= 0 {
		return fmt.Errorf("invalid source admission refresh")
	}
	if len(admittedBySource) == 0 {
		return nil
	}
	ids := make([]string, 0, len(admittedBySource))
	for id, admitted := range admittedBySource {
		if !validSourceID(id) || admitted < 0 || admitted > maxSourceAdmissionBatch {
			return fmt.Errorf("invalid source admission refresh")
		}
		ids = append(ids, id)
	}
	sort.Strings(ids)
	keys := make([]string, len(ids))
	args := make([]interface{}, 0, len(ids)+2)
	args = append(args, now.UnixMilli(), sourceQualityTTL.Milliseconds())
	for i, id := range ids {
		keys[i] = s.sourceQualityKey(id)
		args = append(args, admittedBySource[id])
	}
	return recordSourceAdmissionRefreshScript.Run(ctx, s.client, keys, args...).Err()
}

var recordSourceAdmissionRefreshScript = redis.NewScript(`
local cap = 9007199254740991
for i = 1, #KEYS do
  local admitted = tonumber(ARGV[i + 2])
  if not admitted or admitted < 0 or admitted ~= math.floor(admitted) or admitted > 1000000000 then
    return redis.error_reply('invalid source admission count')
  end
  local fields = {'admission_refreshes', 'zero_admission_streak'}
  local increments = {1, admitted == 0 and 1 or 0}
  for j = 1, #fields do
    local raw = redis.call('HGET', KEYS[i], fields[j]) or '0'
    local current = tonumber(raw)
    if not string.match(raw, '^%d+$') or not current or current < 0 or current ~= math.floor(current) or current + increments[j] > cap then
      return redis.error_reply('invalid source admission counter')
    end
  end
end
for i = 1, #KEYS do
  local key = KEYS[i]
  local admitted = tonumber(ARGV[i + 2])
  redis.call('HINCRBY', key, 'admission_refreshes', 1)
  if admitted == 0 then
  redis.call('HINCRBY', key, 'zero_admission_streak', '1')
  else
  redis.call('HSET', key, 'zero_admission_streak', '0')
  end
  redis.call('HSET', key, 'last_admitted', ARGV[i + 2], 'last_admission_ms', ARGV[1])
  redis.call('PEXPIRE', key, ARGV[2])
end
return 1
`)

// StableSourceID identifies the exact trimmed feed URL and its parser scheme
// hint. Equivalent URLs with different query strings stay distinct, as do
// feeds whose bare-address rows use different schemes.
func StableSourceID(rawURL, defaultScheme string) string {
	rawURL = strings.TrimSpace(rawURL)
	defaultScheme = strings.ToLower(strings.TrimSpace(defaultScheme))
	value := strconv.Itoa(len(rawURL)) + ":" + rawURL + ":" + defaultScheme
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:16])
}

func validSourceID(id string) bool {
	if len(id) != 32 {
		return false
	}
	for _, r := range id {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

func (s *Redis) sourceQualityKey(id string) string { return s.prefix + ":source-quality:" + id }

// SourceQualityKeyPrefix returns the prefix used by source refresh and probe
// learning hashes. It is also passed as a declared Lua key to the completion
// script, which appends the validated source_id stored on each proxy hash.
func (s *Redis) SourceQualityKeyPrefix() string { return s.prefix + ":source-quality:" }

// EncodeSourceStageScore preserves learned priority first and deterministically
// breaks ties by the configured source index. The stride is large enough for
// source lists of up to 65,536 entries while keeping current priorities below
// Redis ZSET's exact integer range.
func EncodeSourceStageScore(priority int64, sourceIndex int) (int64, error) {
	if priority < 0 || priority > maxSourceStagePriority || int64(sourceIndex) < 0 || int64(sourceIndex) >= sourceStageScoreStride {
		return 0, fmt.Errorf("source stage score out of range")
	}
	return priority*sourceStageScoreStride + int64(sourceIndex), nil
}

// DecodeSourceStageScore reverses EncodeSourceStageScore.
func DecodeSourceStageScore(score int64) (priority int64, sourceIndex int, err error) {
	if score < 0 {
		return 0, 0, fmt.Errorf("source stage score out of range")
	}
	return score / sourceStageScoreStride, int(score % sourceStageScoreStride), nil
}

// AttachSourceProvenance sets source_id on existing candidate hashes only.
// HSETNX preserves the first selected source and missing/tombstoned candidates
// remain absent. Call after Upsert so newly accepted candidates already exist.
func (s *Redis) AttachSourceProvenance(ctx context.Context, urls []string, sourceID string) error {
	if !validSourceID(sourceID) {
		return fmt.Errorf("invalid source ID")
	}
	for start := 0; start < len(urls); start += upsertBatch {
		end := min(start+upsertBatch, len(urls))
		ids := make([]interface{}, 0, end-start)
		for _, rawURL := range urls[start:end] {
			ids = append(ids, proxyID(rawURL))
		}
		args := make([]interface{}, 0, len(ids)+2)
		args = append(args, s.prefix+":proxy:", sourceID)
		args = append(args, ids...)
		if err := attachSourceProvenanceScript.Run(ctx, s.client, []string{s.candidatesKey()}, args...).Err(); err != nil {
			return err
		}
	}
	return nil
}

var attachSourceProvenanceScript = redis.NewScript(`
local key_prefix = ARGV[1]
local source_id = ARGV[2]
for i = 3, #ARGV do
  local key = key_prefix .. ARGV[i]
  if redis.call('SISMEMBER', KEYS[1], ARGV[i]) == 1 and redis.call('EXISTS', key) == 1 then
    redis.call('HSETNX', key, 'source_id', source_id)
  end
end
return 1
`)

// RecordSourceRefreshSuccess records only feed output that survived a complete
// fetch, parse, and private-to-shared stage merge. Call it after MergeSourceStage
// succeeds; failed or partial feeds must use RecordSourceFetchFailure instead.
func (s *Redis) RecordSourceRefreshSuccess(ctx context.Context, sourceID string, parsed, uniqueIncremental int64, now time.Time) error {
	if !validSourceID(sourceID) {
		return fmt.Errorf("invalid source ID")
	}
	if parsed < 0 || uniqueIncremental < 0 || uniqueIncremental > parsed {
		return fmt.Errorf("invalid source refresh counts")
	}
	return recordSourceRefreshSuccessScript.Run(ctx, s.client, []string{s.sourceQualityKey(sourceID)},
		now.UnixMilli(), parsed, uniqueIncremental, parsed-uniqueIncremental, sourceQualityTTL.Milliseconds()).Err()
}

var recordSourceRefreshSuccessScript = redis.NewScript(`
local key = KEYS[1]
redis.call('HINCRBY', key, 'refresh_fetches', 1)
redis.call('HINCRBY', key, 'refresh_successes', 1)
redis.call('HINCRBY', key, 'parsed', ARGV[2])
redis.call('HINCRBY', key, 'unique_incremental', ARGV[3])
redis.call('HINCRBY', key, 'duplicate_records', ARGV[4])
redis.call('HSET', key,
  'last_fetch_ms', ARGV[1], 'last_success_ms', ARGV[1],
  'consecutive_failures', '0', 'cooldown_until_ms', '0')
redis.call('PEXPIRE', key, ARGV[5])
return 1
`)

// RecordSourceFetchFailure records one exhausted fetch attempt sequence and
// assigns a capped exponential cooldown. Partial parse counts are intentionally
// omitted because the private stage is discarded on every failed fetch.
func (s *Redis) RecordSourceFetchFailure(ctx context.Context, sourceID string, now time.Time) error {
	if !validSourceID(sourceID) {
		return fmt.Errorf("invalid source ID")
	}
	return recordSourceFetchFailureScript.Run(ctx, s.client, []string{s.sourceQualityKey(sourceID)},
		now.UnixMilli(), sourceFailureBaseCooldown.Milliseconds(), sourceFailureMaxCooldown.Milliseconds(), sourceQualityTTL.Milliseconds()).Err()
}

var recordSourceFetchFailureScript = redis.NewScript(`
local key = KEYS[1]
local consecutive = tonumber(redis.call('HGET', key, 'consecutive_failures') or '0') or 0
consecutive = math.min(consecutive + 1, 30)
local cooldown = tonumber(ARGV[2]) * math.pow(2, math.min(consecutive - 1, 10))
cooldown = math.min(cooldown, tonumber(ARGV[3]))
redis.call('HINCRBY', key, 'refresh_fetches', 1)
redis.call('HINCRBY', key, 'fetch_failures', 1)
redis.call('HSET', key,
  'consecutive_failures', tostring(consecutive),
  'last_fetch_ms', ARGV[1],
  'cooldown_until_ms', tostring(tonumber(ARGV[1]) + cooldown))
redis.call('PEXPIRE', key, ARGV[4])
return cooldown
`)

// LoadSourceQuality reads at most 512 per-source hashes in one Redis pipeline.
// Malformed numeric fields mark only their source corrupt and are ignored.
func (s *Redis) LoadSourceQuality(ctx context.Context, sourceIDs []string, now time.Time) (map[string]SourceQuality, error) {
	if len(sourceIDs) > MaxSourceQualityReadBatch {
		return nil, ErrSourceLearningBatchTooLarge
	}
	ids := make([]string, 0, len(sourceIDs))
	seen := make(map[string]struct{}, len(sourceIDs))
	for _, id := range sourceIDs {
		if !validSourceID(id) {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	result := make(map[string]SourceQuality, len(ids))
	if len(ids) == 0 {
		return result, nil
	}
	pipe := s.client.Pipeline()
	commands := make([]*redis.MapStringStringCmd, len(ids))
	for i, id := range ids {
		commands[i] = pipe.HGetAll(ctx, s.sourceQualityKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, err
	}
	for i, id := range ids {
		result[id] = decodeSourceQuality(commands[i].Val(), now)
	}
	return result, nil
}

func decodeSourceQuality(values map[string]string, now time.Time) SourceQuality {
	var q SourceQuality
	intField := func(name string) int64 {
		raw, ok := values[name]
		if !ok || raw == "" {
			return 0
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 0 {
			q.Corrupt = true
			return 0
		}
		return v
	}
	floatField := func(name string) float64 {
		raw, ok := values[name]
		if !ok || raw == "" {
			return 0
		}
		v, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
			q.Corrupt = true
			return 0
		}
		return v
	}
	timeField := func(name string) time.Time {
		millis := intField(name)
		if millis == 0 {
			return time.Time{}
		}
		return time.UnixMilli(millis)
	}
	q.RefreshFetches = intField("refresh_fetches")
	q.RefreshSuccesses = intField("refresh_successes")
	q.FetchFailures = intField("fetch_failures")
	q.ConsecutiveFailures = intField("consecutive_failures")
	q.Parsed = intField("parsed")
	q.UniqueIncremental = intField("unique_incremental")
	q.DuplicateRecords = intField("duplicate_records")
	q.LastFetchAt = timeField("last_fetch_ms")
	q.LastSuccessAt = timeField("last_success_ms")
	q.CooldownUntil = timeField("cooldown_until_ms")
	q.Admitted = intField("admitted")
	q.AdmissionRefreshes = intField("admission_refreshes")
	q.LastAdmitted = intField("last_admitted")
	q.ZeroAdmissionStreak = intField("zero_admission_streak")
	q.LastAdmissionAt = timeField("last_admission_ms")
	q.HTTPAttempts = intField("http_attempts")
	q.NetworkBytes = intField("network_bytes")
	q.FetchDurationMS = intField("fetch_duration_ms")
	q.FetchCostObservations = intField("fetch_cost_observations")
	q.ProbeSeen = intField("probe_seen")
	q.ProbeSamples = floatField("probe_samples")
	q.ProbePasses = floatField("probe_passes")
	q.ProbeCompletions = intField("probe_completions")
	q.ProbeSuccessfulCompletions = intField("probe_successful_completions")
	q.ProbeDurationMS = intField("probe_duration_ms")
	q.ProbeUpdatedAt = timeField("probe_updated_ms")
	if contract := values["probe_contract"]; contract != "" {
		if validProbeContract(contract) {
			q.ProbeContract = contract
		} else {
			q.Corrupt = true
		}
	}
	if q.ProbeUpdatedAt.After(now) {
		// Clock skew must not make a future-dated sample gain weight forever.
		q.ProbeUpdatedAt = now
	}
	return q
}

func validProbeContract(contract string) bool {
	if len(contract) != 64 {
		return false
	}
	for _, r := range contract {
		if !(r >= '0' && r <= '9') && !(r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}

// SourceProbeLearningLuaSnippet belongs immediately after the valid-lease
// check in completeScript, before any early discard/delete branch. The caller
// adds SourceQualityKeyPrefix as KEYS[6], then appends enabled,
// sampleEvery, halfLifeMS, and the optional 64-hex probe contract as
// ARGV[28..32], followed by probe duration at ARGV[33]. A contract change
// resets sampled outcomes and committed probe-cost counters before recording
// the first new-contract result; fetch and refresh counters remain. It samples
// each source's first completed probe and every Nth retest, while cost counters
// include every committed probe. The body adds no Redis round trip per probe.
const SourceProbeLearningLuaSnippet = `
if ARGV[28] == '1' then
local source_id = redis.call('HGET', key, 'source_id')
if source_id and string.len(source_id) == 32 and string.match(source_id, '^[0-9a-f]+$') and redis.call('SISMEMBER', KEYS[6] .. 'active', source_id) == 1 then
  local sample_every = tonumber(ARGV[29]) or 16
  local half_life_ms = tonumber(ARGV[30]) or 604800000
  sample_every = math.max(1, sample_every)
  half_life_ms = math.max(1, half_life_ms)
  local quality_key = KEYS[6] .. source_id
  local contract = ARGV[32] or ''
  local contract_changed = false
  if string.len(contract) == 64 and string.match(contract, '^[0-9a-f]+$') then
    local previous_contract = redis.call('HGET', quality_key, 'probe_contract') or ''
    if previous_contract ~= contract then
      redis.call('HSET', quality_key,
        'probe_seen', '0', 'probe_samples', '0', 'probe_passes', '0',
        'probe_updated_ms', '0', 'probe_completions', '0',
        'probe_successful_completions', '0', 'probe_duration_ms', '0',
        'probe_contract', contract)
      contract_changed = true
    end
  end
  local initial = redis.call('HEXISTS', key, 'last_checked_at_ms') == 0
  local seen = tonumber(redis.call('HGET', quality_key, 'probe_seen') or '0') or 0
  if not initial then
    seen = seen + 1
    redis.call('HSET', quality_key, 'probe_seen', tostring(seen))
  end
  if contract_changed or initial or seen % sample_every == 0 then
    local samples = tonumber(redis.call('HGET', quality_key, 'probe_samples') or '0') or 0
    local passes = tonumber(redis.call('HGET', quality_key, 'probe_passes') or '0') or 0
    local updated = tonumber(redis.call('HGET', quality_key, 'probe_updated_ms') or '0') or 0
    local elapsed = math.max(0, tonumber(now_ms) - updated)
    local decay = math.exp(-0.6931471805599453 * elapsed / half_life_ms)
    samples = samples * decay + 1
    if status == 'ok' then passes = passes * decay + 1 else passes = passes * decay end
    redis.call('HSET', quality_key,
      'probe_samples', tostring(samples),
      'probe_passes', tostring(passes),
      'probe_updated_ms', now_ms)
    redis.call('PEXPIRE', quality_key, 2592000000)
  end
  local probe_duration_ms = tonumber(ARGV[33])
  if string.len(contract) == 64 and string.match(contract, '^[0-9a-f]+$') and
      probe_duration_ms and probe_duration_ms >= 0 and probe_duration_ms <= 86400000 then
    local success_increment = status == 'ok' and 1 or 0
    local cap = 9007199254740991
    local completion_raw = redis.call('HGET', quality_key, 'probe_completions') or '0'
    local successful_raw = redis.call('HGET', quality_key, 'probe_successful_completions') or '0'
    local duration_raw = redis.call('HGET', quality_key, 'probe_duration_ms') or '0'
    local completion_count = tonumber(completion_raw)
    local successful_count = tonumber(successful_raw)
    local duration_total = tonumber(duration_raw)
    local valid_cost = string.match(completion_raw, '^%d+$') and
      string.match(successful_raw, '^%d+$') and string.match(duration_raw, '^%d+$') and
      completion_count and successful_count and duration_total and
      completion_count >= 0 and successful_count >= 0 and duration_total >= 0 and
      completion_count == math.floor(completion_count) and successful_count == math.floor(successful_count) and
      duration_total == math.floor(duration_total) and completion_count + 1 <= cap and
      successful_count + success_increment <= cap and duration_total + probe_duration_ms <= cap
    if valid_cost then
      redis.call('HINCRBY', quality_key, 'probe_completions', '1')
      if success_increment == 1 then redis.call('HINCRBY', quality_key, 'probe_successful_completions', '1') end
      redis.call('HINCRBY', quality_key, 'probe_duration_ms', tostring(probe_duration_ms))
      redis.call('PEXPIRE', quality_key, '2592000000')
    end
  end
end
end
`

func sourceProbeDurationArg(durationMS int64) int64 {
	if durationMS < 0 || durationMS > maxSourceCostDurationMS {
		return -1
	}
	return durationMS
}

// DecayedSourceYield applies the configured half-life at read time. Invalid or
// absent values naturally produce the Beta(1,1) prior of 0.5.
func DecayedSourceYield(q SourceQuality, now time.Time, halfLife time.Duration) (yield, effectiveSamples float64) {
	samples, passes := q.ProbeSamples, q.ProbePasses
	if !q.ProbeUpdatedAt.IsZero() && halfLife > 0 && now.After(q.ProbeUpdatedAt) {
		decay := math.Exp(-math.Ln2 * float64(now.Sub(q.ProbeUpdatedAt)) / float64(halfLife))
		samples *= decay
		passes *= decay
	}
	if passes > samples {
		passes = samples
	}
	return (passes + 1) / (samples + 2), samples
}
