package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const MaxLearningSources = 4096

// SetActiveLearningSources bounds probe attribution to the current configured
// catalog. Removed sources stop rearming their statistics' retention TTL.
func (s *Redis) SetActiveLearningSources(ctx context.Context, ids []string) error {
	if len(ids) > MaxLearningSources {
		return fmt.Errorf("source learning supports at most %d sources", MaxLearningSources)
	}
	for _, id := range ids {
		if !validSourceID(id) {
			return fmt.Errorf("invalid source ID")
		}
	}
	key := s.SourceQualityKeyPrefix() + "active"
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.Del(ctx, key)
		for start := 0; start < len(ids); start += MaxSourceQualityReadBatch {
			end := min(start+MaxSourceQualityReadBatch, len(ids))
			values := make([]interface{}, 0, end-start)
			for _, id := range ids[start:end] {
				values = append(values, id)
			}
			pipe.SAdd(ctx, key, values...)
		}
		pipe.Expire(ctx, key, 24*time.Hour)
		return nil
	})
	return err
}

// NextSourceLearningRefresh advances one cluster-wide exploration generation.
// The existing source lease serializes refreshes across replicas.
func (s *Redis) NextSourceLearningRefresh(ctx context.Context) (uint64, error) {
	key := s.prefix + ":source-learning-refresh"
	var counter *redis.IntCmd
	_, err := s.client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		counter = pipe.Incr(ctx, key)
		pipe.Expire(ctx, key, 30*24*time.Hour)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return uint64(counter.Val()), nil
}
