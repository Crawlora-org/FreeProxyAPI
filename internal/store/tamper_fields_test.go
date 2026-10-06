package store

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestCompletePersistsTamperCheck(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "tamper:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := s.Upsert(ctx, []string{"http://t.example.net:8080"}, now); err != nil {
		t.Fatal(err)
	}
	claim := completeOne(t, s, now, OutcomeMeta{Anonymity: "elite", TamperChecked: true, Tampered: true})
	key := "tamper:v1:proxy:" + claim.ID
	nowMs := strconv.FormatInt(now.UnixMilli(), 10)
	if mini.HGet(key, "tampered") != "1" || mini.HGet(key, "tamper_checked_at_ms") != nowMs {
		t.Fatalf("tamper fields not persisted: tampered=%q at=%q", mini.HGet(key, "tampered"), mini.HGet(key, "tamper_checked_at_ms"))
	}

	// A later probe without a tamper check leaves the fields unchanged.
	later := now.Add(time.Minute)
	mini.ZAdd("tamper:v1:pending", 0, claim.ID)
	completeOne(t, s, later, OutcomeMeta{})
	if mini.HGet(key, "tampered") != "1" || mini.HGet(key, "tamper_checked_at_ms") != nowMs {
		t.Fatal("meta without a tamper check must not change tamper fields")
	}

	// A clean check overwrites the flag and timestamp.
	mini.ZAdd("tamper:v1:pending", 0, claim.ID)
	completeOne(t, s, later, OutcomeMeta{TamperChecked: true, Tampered: false})
	if mini.HGet(key, "tampered") != "0" || mini.HGet(key, "tamper_checked_at_ms") != strconv.FormatInt(later.UnixMilli(), 10) {
		t.Fatal("clean tamper check not recorded")
	}
	if mini.HGet(key, "anonymity") != "elite" {
		t.Fatal("tamper check must not disturb anonymity fields")
	}
}

func TestQueryValidatedTamperFreshnessAndExcludeTampered(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "tamperq:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	nowMs := time.Now().UnixMilli()
	day := int64(24 * time.Hour / time.Millisecond)
	ms := func(v int64) string { return strconv.FormatInt(v, 10) }
	seed := func(id string, fields ...string) {
		base := []string{"url", "http://" + id + ".example.org:8080", "country", "US", "ok_ratio_pct", "100"}
		mini.HSet("tamperq:v1:proxy:"+id, append(base, fields...)...)
		if _, err := mini.SAdd("tamperq:v1:validated", id); err != nil {
			t.Fatal(err)
		}
	}
	seed("bad", "tampered", "1", "tamper_checked_at_ms", ms(nowMs-1000))
	seed("clean", "tampered", "0", "tamper_checked_at_ms", ms(nowMs-1000))
	seed("stale", "tampered", "1", "tamper_checked_at_ms", ms(nowMs-2*day))
	seed("legacy", "tampered", "1")
	seed("unknown")

	ctx := context.Background()
	query := func(f ProxyFilter) map[string]QueryProxy {
		t.Helper()
		got, err := s.QueryValidated(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]QueryProxy{}
		for _, p := range got {
			out[strings.TrimSuffix(strings.TrimPrefix(p.URL, "http://"), ".example.org:8080")] = p
		}
		return out
	}

	all := query(ProxyFilter{ClassificationMaxAgeMs: day})
	if len(all) != 5 {
		t.Fatalf("all = %d", len(all))
	}
	if p := all["bad"]; p.Tampered == nil || !*p.Tampered {
		t.Fatalf("fresh tampered flag must be exposed: %+v", p)
	}
	if p := all["clean"]; p.Tampered == nil || *p.Tampered {
		t.Fatalf("fresh clean flag must be exposed as false: %+v", p)
	}
	for _, id := range []string{"stale", "legacy", "unknown"} {
		if all[id].Tampered != nil {
			t.Fatalf("%s must report unknown tampered: %+v", id, all[id])
		}
	}

	cases := []struct {
		name   string
		filter ProxyFilter
		want   []string
	}{
		{name: "fresh only", filter: ProxyFilter{ExcludeTampered: true, ClassificationMaxAgeMs: day}, want: []string{"clean", "stale", "legacy", "unknown"}},
		{name: "bounded scan", filter: ProxyFilter{ExcludeTampered: true, ClassificationMaxAgeMs: day, Limit: 10}, want: []string{"clean", "stale", "legacy", "unknown"}},
		// Zero max age trusts stored values of any age, including legacy ones.
		{name: "any age", filter: ProxyFilter{ExcludeTampered: true}, want: []string{"clean", "unknown"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := query(tc.filter)
			if len(got) != len(tc.want) {
				t.Fatalf("matched %d, want %v", len(got), tc.want)
			}
			for _, id := range tc.want {
				if _, ok := got[id]; !ok {
					t.Fatalf("missing %s in %v", id, got)
				}
			}
		})
	}
	if !hasProxyFilter(ProxyFilter{ExcludeTampered: true}) {
		t.Fatal("exclude_tampered must count as a filter")
	}
}
