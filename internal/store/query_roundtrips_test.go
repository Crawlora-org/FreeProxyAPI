package store

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// roundTripAudit counts Redis round trips: every plain command and every
// pipeline is one. The handshake go-redis runs when it opens a connection
// (HELLO, CLIENT SETINFO) is not part of a query and is not counted.
type roundTripAudit struct{ trips atomic.Int64 }

func isConnectionSetup(cmd redis.Cmder) bool {
	name := cmd.Name()
	return name == "hello" || name == "client"
}

func (h *roundTripAudit) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		return next(ctx, network, addr)
	}
}

func (h *roundTripAudit) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if !isConnectionSetup(cmd) {
			h.trips.Add(1)
		}
		return next(ctx, cmd)
	}
}

func (h *roundTripAudit) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if len(cmds) > 0 && !isConnectionSetup(cmds[0]) {
			h.trips.Add(1)
		}
		return next(ctx, cmds)
	}
}

// seedValidated adds n validated proxies. Every third one has a 50% success
// ratio, so a min_ratio_pct=80 filter rejects a third of the set.
func seedValidated(t *testing.T, mini *miniredis.Miniredis, s *Redis, n int) (matching int) {
	t.Helper()
	for i := 0; i < n; i++ {
		rawURL := fmt.Sprintf("http://rt-%04d.example.net:8080", i)
		id := proxyID(rawURL)
		mini.SAdd(s.candidatesKey(), id)
		mini.SAdd(s.validatedKey(), id)
		ratio := "100"
		if i%3 == 0 {
			ratio = "50"
		} else {
			matching++
		}
		mini.HSet(s.proxyKey(id), "url", rawURL, "country", "US", "latency_ewma_ms", "100", "ok_ratio_pct", ratio, "last_checked_at_ms", "1")
	}
	return matching
}

func TestQueryReadSize(t *testing.T) {
	for _, tc := range []struct {
		name   string
		filter ProxyFilter
		needed int
		want   int
	}{
		{"no filter reads only what is needed", ProxyFilter{Limit: 10}, 10, 10},
		{"no filter caps at the pipeline size", ProxyFilter{Limit: 5000}, 5000, queryPipelineBatch},
		{"filter with a small limit reads a small page", ProxyFilter{Limit: 10, MinRatioPct: 80}, 10, queryScanBatch},
		{"filter at the small page limit stays small", ProxyFilter{Limit: queryScanBatch, MinRatioPct: 80}, queryScanBatch, queryScanBatch},
		{"filter with a large limit reads a full pipeline", ProxyFilter{Limit: 1001, MinRatioPct: 80}, 1001, queryPipelineBatch},
	} {
		if got := queryReadSize(tc.filter, tc.needed); got != tc.want {
			t.Errorf("%s: queryReadSize = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// A query that finds fewer matches than its limit must read the whole
// validated set. That is the hosted /proxies cache-miss shape (limit 1001, a
// filter, fewer matches), which used to take about 14 round trips for 831
// members and could exceed the handler's time budget on a slow Redis.
func TestQueryValidatedFullScanUsesFewRoundTrips(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "rt:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const total = 831
	matching := seedValidated(t, mini, s, total)

	audit := &roundTripAudit{}
	s.client.AddHook(audit)
	got, err := s.QueryValidated(context.Background(), ProxyFilter{MinRatioPct: 80, ExcludeTampered: true, Limit: 1001})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != matching {
		t.Fatalf("returned %d proxies, want %d matching", len(got), matching)
	}
	if trips := audit.trips.Load(); trips > 4 {
		t.Fatalf("full scan of %d members took %d Redis round trips, want at most 4", total, trips)
	}
}

// Chunk sizes only change how the scan is batched, never what it returns.
func TestQueryValidatedResultsIndependentOfPageSizes(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "rt2:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	const total = 1300
	matching := seedValidated(t, mini, s, total)

	for _, limit := range []int{1, 10, queryScanBatch, queryScanBatch + 1, queryPipelineBatch, queryPipelineBatch + 1, 1001, 5000} {
		for _, filter := range []ProxyFilter{{}, {MinRatioPct: 80}} {
			filter.Limit = limit
			got, err := s.QueryValidated(context.Background(), filter)
			if err != nil {
				t.Fatalf("limit=%d filter=%+v: %v", limit, filter, err)
			}
			available := total
			if filter.MinRatioPct > 0 {
				available = matching
			}
			want := limit
			if available < want {
				want = available
			}
			if len(got) != want {
				t.Errorf("limit=%d filter=%+v: got %d results, want %d", limit, filter, len(got), want)
			}
			seen := make(map[string]struct{}, len(got))
			for _, p := range got {
				if _, dup := seen[p.URL]; dup {
					t.Fatalf("limit=%d filter=%+v: duplicate %s", limit, filter, p.URL)
				}
				seen[p.URL] = struct{}{}
				if filter.MinRatioPct > 0 && p.OkRatioPct < filter.MinRatioPct {
					t.Fatalf("limit=%d: %s has ratio %d, below the filter", limit, p.URL, p.OkRatioPct)
				}
			}
		}
	}
}
