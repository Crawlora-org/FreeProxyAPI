package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func newSourceRefreshTraceTestStore(t *testing.T) (*miniredis.Miniredis, *Redis) {
	t.Helper()
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "trace-test:v1")
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return mini, s
}

func traceTestID(n int) string { return fmt.Sprintf("%032x", n) }

func TestSourceRefreshTraceValidationAndLimits(t *testing.T) {
	_, s := newSourceRefreshTraceTestStore(t)
	ctx := context.Background()
	now := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	for _, id := range []string{"", "ABCDEF0123456789ABCDEF0123456789", "0123456789abcdef0123456789abcdeg", "0123456789abcdef0123456789abcde"} {
		if err := s.SaveSourceRefreshTrace(ctx, id, []byte(`{}`), []byte(`{}`), now); err == nil {
			t.Fatalf("SaveSourceRefreshTrace accepted invalid ID %q", id)
		}
		if _, err := s.LoadSourceRefreshTrace(ctx, id); err == nil {
			t.Fatalf("LoadSourceRefreshTrace accepted invalid ID %q", id)
		}
	}
	if err := s.SaveSourceRefreshTrace(ctx, traceTestID(1), nil, []byte(`{}`), now); err == nil {
		t.Fatal("SaveSourceRefreshTrace accepted an empty payload")
	}
	if err := s.SaveSourceRefreshTrace(ctx, traceTestID(1), []byte(`{}`), []byte(`{}`), time.Time{}); err == nil {
		t.Fatal("SaveSourceRefreshTrace accepted a zero capture time")
	}
	if err := s.SaveSourceRefreshTrace(ctx, traceTestID(1), []byte(strings.Repeat("x", maxSourceRefreshTraceBytes+1)), []byte(`{}`), now); err == nil {
		t.Fatal("SaveSourceRefreshTrace accepted an oversized payload")
	}
	for _, limit := range []int{-1, MaxSourceRefreshTraces + 1} {
		if _, err := s.ListSourceRefreshTraceIDs(ctx, limit); err == nil {
			t.Fatalf("ListSourceRefreshTraceIDs accepted limit %d", limit)
		}
	}
}

func TestSourceRefreshTraceRetentionAndPayloadEviction(t *testing.T) {
	mini, s := newSourceRefreshTraceTestStore(t)
	ctx := context.Background()
	base := time.Date(2026, 10, 14, 0, 0, 0, 0, time.UTC)
	for i := 1; i <= MaxSourceRefreshTraces+1; i++ {
		if err := s.SaveSourceRefreshTrace(ctx, traceTestID(i), []byte(fmt.Sprintf(`{"n":%d}`, i)), []byte(`{"summary":true}`), base.Add(time.Duration(i)*time.Second)); err != nil {
			t.Fatalf("SaveSourceRefreshTrace(%d): %v", i, err)
		}
	}
	ids, err := s.ListSourceRefreshTraceIDs(ctx, MaxSourceRefreshTraces)
	if err != nil {
		t.Fatalf("ListSourceRefreshTraceIDs: %v", err)
	}
	if len(ids) != MaxSourceRefreshTraces || ids[0] != traceTestID(MaxSourceRefreshTraces+1) || ids[len(ids)-1] != traceTestID(2) {
		t.Fatalf("retained IDs = %d entries, first %q last %q", len(ids), ids[0], ids[len(ids)-1])
	}
	if mini.Exists(s.sourceRefreshTraceKey(traceTestID(1))) {
		t.Fatal("evicted payload key still exists")
	}
	if mini.Exists(s.sourceRefreshTraceSummaryKey(traceTestID(1))) {
		t.Fatal("evicted summary key still exists")
	}
	if _, err := s.LoadSourceRefreshTrace(ctx, traceTestID(1)); !errors.Is(err, ErrSourceRefreshTraceNotFound) {
		t.Fatalf("LoadSourceRefreshTrace(evicted) error = %v", err)
	}
	if ttl := mini.TTL(s.sourceRefreshTraceKey(traceTestID(2))); ttl <= 0 || ttl > SourceRefreshTraceRetention {
		t.Fatalf("payload TTL = %s", ttl)
	}
	if ttl := mini.TTL(s.sourceRefreshTraceSummaryKey(traceTestID(2))); ttl <= 0 || ttl > SourceRefreshTraceRetention {
		t.Fatalf("summary TTL = %s", ttl)
	}
	if ttl := mini.TTL(s.sourceRefreshTraceIndexKey()); ttl <= 0 || ttl > SourceRefreshTraceRetention {
		t.Fatalf("index TTL = %s", ttl)
	}
	mini.FastForward(SourceRefreshTraceRetention)
	if mini.Exists(s.sourceRefreshTraceIndexKey()) || mini.Exists(s.sourceRefreshTraceKey(traceTestID(MaxSourceRefreshTraces+1))) || mini.Exists(s.sourceRefreshTraceSummaryKey(traceTestID(MaxSourceRefreshTraces+1))) {
		t.Fatal("trace index, payload, or summary survived retention")
	}
}

func TestSourceRefreshTraceMissingAndCorruptRedisTypes(t *testing.T) {
	mini, s := newSourceRefreshTraceTestStore(t)
	ctx := context.Background()
	id := traceTestID(7)
	if _, err := s.LoadSourceRefreshTrace(ctx, id); !errors.Is(err, ErrSourceRefreshTraceNotFound) {
		t.Fatalf("missing trace error = %v", err)
	}
	if err := s.SaveSourceRefreshTrace(ctx, id, []byte(`{"opaque":true}`), []byte(`{"summary":true}`), time.Now().UTC()); err != nil {
		t.Fatalf("SaveSourceRefreshTrace: %v", err)
	}
	got, err := s.LoadSourceRefreshTrace(ctx, id)
	if err != nil || string(got) != `{"opaque":true}` {
		t.Fatalf("LoadSourceRefreshTrace = %q, %v", got, err)
	}
	gotSummary, err := s.LoadSourceRefreshTraceSummary(ctx, id)
	if err != nil || string(gotSummary) != `{"summary":true}` {
		t.Fatalf("LoadSourceRefreshTraceSummary = %q, %v", gotSummary, err)
	}
	mini.Del(s.sourceRefreshTraceKey(id))
	if _, err := s.LoadSourceRefreshTrace(ctx, id); !errors.Is(err, ErrSourceRefreshTraceNotFound) {
		t.Fatalf("trace with missing payload error = %v", err)
	}
	mini.Set(s.sourceRefreshTraceKey(id), `{"opaque":true}`)
	mini.ZAdd(s.sourceRefreshTraceIndexKey(), float64(time.Now().Add(time.Hour).UnixMilli()), "https://invalid.example/feed")
	listed, err := s.ListSourceRefreshTraceIDs(ctx, MaxSourceRefreshTraces)
	if err != nil {
		t.Fatalf("ListSourceRefreshTraceIDs after invalid member: %v", err)
	}
	for _, listedID := range listed {
		if !validSourceID(listedID) {
			t.Fatalf("ListSourceRefreshTraceIDs exposed an invalid member %q", listedID)
		}
	}
	mini.Del(s.sourceRefreshTraceIndexKey())
	mini.Set(s.sourceRefreshTraceIndexKey(), "wrong-type")
	if _, err := s.ListSourceRefreshTraceIDs(ctx, 1); err == nil {
		t.Fatal("ListSourceRefreshTraceIDs hid a wrong-type index")
	}
	if err := s.SaveSourceRefreshTrace(ctx, traceTestID(8), []byte(`{}`), []byte(`{}`), time.Now().UTC()); err == nil {
		t.Fatal("SaveSourceRefreshTrace hid a wrong-type index")
	}

	// Restore an index entry and poison its payload key to cover GET wrong-type.
	mini.Del(s.sourceRefreshTraceIndexKey())
	if err := s.SaveSourceRefreshTrace(ctx, id, []byte(`{}`), []byte(`{}`), time.Now().UTC()); err != nil {
		t.Fatalf("restore index: %v", err)
	}
	mini.Del(s.sourceRefreshTraceKey(id))
	mini.HSet(s.sourceRefreshTraceKey(id), "field", "wrong-type")
	if _, err := s.LoadSourceRefreshTrace(ctx, id); err == nil {
		t.Fatal("LoadSourceRefreshTrace hid a wrong-type payload")
	}
}
