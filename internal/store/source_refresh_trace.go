package store

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	MaxSourceRefreshTraces      = 64
	SourceRefreshTraceRetention = 72 * time.Hour
	maxSourceRefreshTraceBytes  = 1 << 20
	maxSourceRefreshTraceList   = MaxSourceRefreshTraces
)

var ErrSourceRefreshTraceNotFound = errors.New("source refresh trace not found")

func (s *Redis) sourceRefreshTraceIndexKey() string {
	return s.prefix + ":source-refresh-traces"
}

func (s *Redis) sourceRefreshTraceKey(id string) string {
	return s.prefix + ":source-refresh-trace:" + id
}

func (s *Redis) sourceRefreshTraceSummaryKey(id string) string {
	return s.prefix + ":source-refresh-trace-summary:" + id
}

var saveSourceRefreshTraceScript = redis.NewScript(`
local index = KEYS[1]
local payload = KEYS[2]
local summary = KEYS[3]
local id = ARGV[1]
local data = ARGV[2]
local captured = tonumber(ARGV[3])
local ttl = tonumber(ARGV[4])
local max = tonumber(ARGV[5])
local index_type = redis.call('TYPE', index).ok
if index_type ~= 'none' and index_type ~= 'zset' then
  return redis.error_reply('WRONGTYPE source refresh trace index is not a sorted set')
end
redis.call('SET', payload, data, 'PX', ttl)
redis.call('SET', summary, ARGV[8], 'PX', ttl)
redis.call('ZADD', index, captured, id)
local excess = redis.call('ZCARD', index) - max
if excess > 0 then
  local evicted = redis.call('ZRANGE', index, 0, excess - 1)
  for _, old_id in ipairs(evicted) do
    redis.call('DEL', ARGV[6] .. old_id)
    redis.call('DEL', ARGV[7] .. old_id)
  end
  redis.call('ZREMRANGEBYRANK', index, 0, excess - 1)
end
redis.call('PEXPIRE', index, ttl)
return 1
`)

// SaveSourceRefreshTrace stores an opaque bounded detail payload and compact
// summary, both indexed by capture time. The oldest keys are deleted when the
// index evicts their ID.
func (s *Redis) SaveSourceRefreshTrace(ctx context.Context, id string, payload, summary []byte, capturedAt time.Time) error {
	if !validSourceID(id) {
		return errors.New("invalid source refresh trace ID")
	}
	if len(payload) == 0 || len(payload) > maxSourceRefreshTraceBytes {
		return fmt.Errorf("source refresh trace payload size must be between 1 and %d bytes", maxSourceRefreshTraceBytes)
	}
	if len(summary) == 0 || len(summary) > 16<<10 {
		return errors.New("source refresh trace summary size must be between 1 and 16384 bytes")
	}
	if capturedAt.IsZero() || capturedAt.UnixMilli() <= 0 {
		return errors.New("invalid source refresh trace capture time")
	}
	_, err := saveSourceRefreshTraceScript.Run(ctx, s.client,
		[]string{s.sourceRefreshTraceIndexKey(), s.sourceRefreshTraceKey(id), s.sourceRefreshTraceSummaryKey(id)},
		id, payload, capturedAt.UnixMilli(), SourceRefreshTraceRetention.Milliseconds(),
		MaxSourceRefreshTraces, s.prefix+":source-refresh-trace:", s.prefix+":source-refresh-trace-summary:", summary).Result()
	if err != nil {
		return fmt.Errorf("save source refresh trace: %w", err)
	}
	return nil
}

// LoadSourceRefreshTraceSummary returns the compact summary for a listed ID.
func (s *Redis) LoadSourceRefreshTraceSummary(ctx context.Context, id string) ([]byte, error) {
	if !validSourceID(id) {
		return nil, errors.New("invalid source refresh trace ID")
	}
	_, err := s.client.ZScore(ctx, s.sourceRefreshTraceIndexKey(), id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrSourceRefreshTraceNotFound
		}
		return nil, fmt.Errorf("check source refresh trace index: %w", err)
	}
	summary, err := s.client.Get(ctx, s.sourceRefreshTraceSummaryKey(id)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrSourceRefreshTraceNotFound
		}
		return nil, fmt.Errorf("load source refresh trace summary: %w", err)
	}
	return summary, nil
}

// LoadSourceRefreshTrace returns the opaque detail bytes for an indexed trace.
func (s *Redis) LoadSourceRefreshTrace(ctx context.Context, id string) ([]byte, error) {
	if !validSourceID(id) {
		return nil, errors.New("invalid source refresh trace ID")
	}
	_, err := s.client.ZScore(ctx, s.sourceRefreshTraceIndexKey(), id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrSourceRefreshTraceNotFound
		}
		return nil, fmt.Errorf("check source refresh trace index: %w", err)
	}
	payload, err := s.client.Get(ctx, s.sourceRefreshTraceKey(id)).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrSourceRefreshTraceNotFound
		}
		return nil, fmt.Errorf("load source refresh trace: %w", err)
	}
	return payload, nil
}

// ListSourceRefreshTraceIDs returns the newest indexed IDs, newest first.
func (s *Redis) ListSourceRefreshTraceIDs(ctx context.Context, limit int) ([]string, error) {
	if limit < 0 || limit > maxSourceRefreshTraceList {
		return nil, fmt.Errorf("source refresh trace limit must be between 0 and %d", maxSourceRefreshTraceList)
	}
	if limit == 0 {
		return []string{}, nil
	}
	// Read at most the index's configured bound even if external corruption has
	// inserted arbitrary members, then expose only validated source IDs.
	ids, err := s.client.ZRangeArgs(ctx, redis.ZRangeArgs{
		Key: s.sourceRefreshTraceIndexKey(), Start: "0", Stop: strconv.Itoa(MaxSourceRefreshTraces - 1), Rev: true,
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("list source refresh trace IDs: %w", err)
	}
	valid := make([]string, 0, limit)
	for _, id := range ids {
		if validSourceID(id) {
			valid = append(valid, id)
			if len(valid) == limit {
				break
			}
		}
	}
	return valid, nil
}
