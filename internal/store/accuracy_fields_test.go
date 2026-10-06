package store

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func completeOne(t *testing.T, s *Redis, now time.Time, meta OutcomeMeta) Claim {
	t.Helper()
	claim, found, err := s.ClaimDue(context.Background(), "w", now, time.Minute)
	if err != nil || !found {
		t.Fatalf("claim: found=%t err=%v", found, err)
	}
	policy := RetestPolicy{ValidatedAfter: time.Hour}
	if _, err := s.Complete(context.Background(), claim, now, policy, true, 204, 100*time.Millisecond, "", meta); err != nil {
		t.Fatalf("complete: %v", err)
	}
	return claim
}

func TestCompletePersistsHTTPSAndClassificationTimestamps(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "acc:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	now := time.Now()
	if _, err := s.Upsert(ctx, []string{"http://a.example.net:8080"}, now); err != nil {
		t.Fatal(err)
	}
	claim := completeOne(t, s, now, OutcomeMeta{Anonymity: "elite", ExitCountry: "DE", HTTPSChecked: true, HTTPSOK: true, HTTPSTunnelScheme: "http"})
	key := "acc:v1:proxy:" + claim.ID
	nowMs := strconv.FormatInt(now.UnixMilli(), 10)
	if got := mini.HGet(key, "anonymity_checked_at_ms"); got != nowMs {
		t.Fatalf("anonymity_checked_at_ms = %q want %q", got, nowMs)
	}
	if mini.HGet(key, "https_ok") != "1" || mini.HGet(key, "https_checked_at_ms") != nowMs || mini.HGet(key, "https_tunnel_scheme") != "http" {
		t.Fatalf("https fields not persisted")
	}

	// A later probe with no measurements leaves every field unchanged.
	later := now.Add(time.Minute)
	mini.ZAdd("acc:v1:pending", 0, claim.ID)
	completeOne(t, s, later, OutcomeMeta{})
	if mini.HGet(key, "anonymity_checked_at_ms") != nowMs || mini.HGet(key, "https_checked_at_ms") != nowMs || mini.HGet(key, "https_ok") != "1" {
		t.Fatal("empty meta must not change measurement fields")
	}

	// A failed HTTPS sample overwrites the result and timestamp.
	mini.ZAdd("acc:v1:pending", 0, claim.ID)
	completeOne(t, s, later, OutcomeMeta{HTTPSChecked: true, HTTPSOK: false})
	laterMs := strconv.FormatInt(later.UnixMilli(), 10)
	if mini.HGet(key, "https_ok") != "0" || mini.HGet(key, "https_checked_at_ms") != laterMs {
		t.Fatalf("failed https sample not recorded: ok=%q at=%q", mini.HGet(key, "https_ok"), mini.HGet(key, "https_checked_at_ms"))
	}
	if mini.HGet(key, "https_tunnel_scheme") != "http" {
		t.Fatal("failed sample must not clear the last working tunnel scheme")
	}
}

func TestQueryValidatedClassificationFreshness(t *testing.T) {
	mini := miniredis.RunT(t)
	s, err := NewRedis("redis://"+mini.Addr()+"/0", "fresh:v1")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	nowMs := time.Now().UnixMilli()
	day := int64(24 * time.Hour / time.Millisecond)
	seed := func(id string, fields ...string) {
		base := []string{"url", "http://" + id + ".example.net:8080", "country", "US", "ok_ratio_pct", "100"}
		mini.HSet("fresh:v1:proxy:"+id, append(base, fields...)...)
		if _, err := mini.SAdd("fresh:v1:validated", id); err != nil {
			t.Fatal(err)
		}
	}
	ms := func(v int64) string { return strconv.FormatInt(v, 10) }
	seed("fresh", "anonymity", "elite", "exit_country", "DE", "anonymity_checked_at_ms", ms(nowMs-1000),
		"https_ok", "1", "https_checked_at_ms", ms(nowMs-1000))
	seed("stale", "anonymity", "elite", "exit_country", "DE", "anonymity_checked_at_ms", ms(nowMs-2*day),
		"https_ok", "1", "https_checked_at_ms", ms(nowMs-2*day))
	seed("legacy", "anonymity", "transparent", "exit_country", "FR")
	seed("httpsfail", "https_ok", "0", "https_checked_at_ms", ms(nowMs-1000))

	ctx := context.Background()
	query := func(f ProxyFilter) map[string]QueryProxy {
		t.Helper()
		f.ClassificationMaxAgeMs = day
		got, err := s.QueryValidated(ctx, f)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]QueryProxy{}
		for _, p := range got {
			out[p.URL] = p
		}
		return out
	}
	u := func(id string) string { return "http://" + id + ".example.net:8080" }

	all := query(ProxyFilter{})
	if len(all) != 4 {
		t.Fatalf("all = %d", len(all))
	}
	if p := all[u("fresh")]; p.Anonymity != "elite" || p.ExitCountry != "DE" || !p.GeoMismatch || p.HTTPSOK == nil || !*p.HTTPSOK {
		t.Fatalf("fresh entry blanked: %+v", p)
	}
	for _, id := range []string{"stale", "legacy"} {
		if p := all[u(id)]; p.Anonymity != "" || p.ExitCountry != "" || p.GeoMismatch || p.HTTPSOK != nil {
			t.Fatalf("%s entry must be unknown: %+v", id, p)
		}
	}
	if p := all[u("httpsfail")]; p.HTTPSOK == nil || *p.HTTPSOK {
		t.Fatalf("fresh https failure must report false: %+v", p)
	}

	for name, f := range map[string]ProxyFilter{
		"anonymity":    {Anonymity: "elite"},
		"exit_country": {ExitCountry: "DE"},
		"geo_mismatch": {GeoMismatch: true},
		"https":        {HTTPS: true},
	} {
		got := query(f)
		if len(got) != 1 {
			t.Fatalf("%s filter matched %d, want only fresh", name, len(got))
		}
		if _, ok := got[u("fresh")]; !ok {
			t.Fatalf("%s filter missed fresh entry", name)
		}
	}
	if got := query(ProxyFilter{Anonymity: "transparent"}); len(got) != 0 {
		t.Fatalf("legacy untimestamped class must not match: %d", len(got))
	}

	// Zero max age keeps the historical trust-any-age behavior.
	legacy, err := s.QueryValidated(ctx, ProxyFilter{Anonymity: "transparent"})
	if err != nil || len(legacy) != 1 {
		t.Fatalf("zero max age: %d err=%v", len(legacy), err)
	}
}
