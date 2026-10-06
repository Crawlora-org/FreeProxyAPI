package store

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// accuracyPolicy mirrors the production defaults for listing accuracy.
var accuracyPolicy = RetestPolicy{
	ValidatedAfter:       time.Hour,
	FailedAfter:          2 * time.Hour,
	ListedFailureAfter:   5 * time.Minute,
	SampleRetestAfter:    time.Minute,
	MinSamplesForListing: 3,
}

type listingHarness struct {
	t     *testing.T
	store *Redis
	id    string
	now   time.Time
}

// probe claims the single candidate at a time past any schedule and records
// the outcome, returning the completion timestamp.
func (h *listingHarness) probe(ok bool) time.Time {
	h.t.Helper()
	h.now = h.now.Add(3 * time.Hour)
	ctx := context.Background()
	claim, found, err := h.store.ClaimDue(ctx, "w", h.now, time.Minute)
	if err != nil || !found || claim.ID != h.id {
		h.t.Fatalf("ClaimDue: claim=%+v found=%t err=%v", claim, found, err)
	}
	outcome, err := h.store.Complete(ctx, claim, h.now, accuracyPolicy, ok, 204, 50*time.Millisecond, "", OutcomeMeta{})
	if err != nil || !outcome.Committed || outcome.Evicted {
		h.t.Fatalf("Complete: outcome=%+v err=%v", outcome, err)
	}
	return h.now
}

func newListingHarness(t *testing.T) (*listingHarness, func(string) float64, func() bool, func(string) string) {
	mini, s := newTestStore(t, "test:v1")
	url := "http://listing.example.net:8080"
	now := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	if _, err := s.Upsert(context.Background(), []string{url}, now); err != nil {
		t.Fatalf("Upsert: %v", err)
	}
	id := proxyID(url)
	score := func(string) float64 {
		got, err := mini.ZScore(s.pendingKey(), id)
		if err != nil {
			t.Fatalf("pending score: %v", err)
		}
		return got
	}
	listed := func() bool {
		member, _ := mini.SIsMember(s.validatedKey(), id)
		return member
	}
	field := func(name string) string { return mini.HGet(s.proxyKey(id), name) }
	return &listingHarness{t: t, store: s, id: id, now: now}, score, listed, field
}

func TestCompleteSingleSuccessIsNotListed(t *testing.T) {
	h, score, listed, field := newListingHarness(t)
	at := h.probe(true)
	if listed() {
		t.Fatal("a single success must not list the candidate")
	}
	if want := float64(at.Add(accuracyPolicy.SampleRetestAfter).UnixMilli()); score("") != want {
		t.Fatalf("follow-up score = %v, want sample retest %v", score(""), want)
	}
	if got := field("last_ok_at_ms"); got != strconv.FormatInt(at.UnixMilli(), 10) {
		t.Fatalf("last_ok_at_ms = %q", got)
	}
	if got := field("last_status"); got != "ok" {
		t.Fatalf("last_status = %q", got)
	}
	h.probe(true)
	if listed() {
		t.Fatal("two successes must not list the candidate with min samples 3")
	}
}

func TestCompleteListsAfterMinSamplesAndReportsFreshness(t *testing.T) {
	h, score, listed, _ := newListingHarness(t)
	h.probe(true)
	h.probe(true)
	at := h.probe(true)
	if !listed() {
		t.Fatal("three successes at 100% must list the candidate")
	}
	if want := float64(at.Add(accuracyPolicy.ValidatedAfter).UnixMilli()); score("") != want {
		t.Fatalf("listed score = %v, want validated retest %v", score(""), want)
	}
	proxies, err := h.store.QueryValidated(context.Background(), ProxyFilter{})
	if err != nil || len(proxies) != 1 {
		t.Fatalf("QueryValidated: %+v err=%v", proxies, err)
	}
	if proxies[0].LastStatus != "ok" || proxies[0].LastOkAt != at.UnixMilli() || proxies[0].LastCheckedAt != at.UnixMilli() {
		t.Fatalf("freshness fields = %+v", proxies[0])
	}
}

func TestCompleteListedFailureRetestsSoonThenDelists(t *testing.T) {
	h, score, listed, _ := newListingHarness(t)
	var lastOK time.Time
	for i := 0; i < 5; i++ {
		lastOK = h.probe(true)
	}
	if !listed() {
		t.Fatal("setup: candidate should be listed")
	}

	// 111110 -> 83%: still listed, so re-probe after the short interval.
	failedAt := h.probe(false)
	if !listed() {
		t.Fatal("one failure at 83% must keep the candidate listed")
	}
	if want := float64(failedAt.Add(accuracyPolicy.ListedFailureAfter).UnixMilli()); score("") != want {
		t.Fatalf("listed failure score = %v, want %v", score(""), want)
	}
	proxies, err := h.store.QueryValidated(context.Background(), ProxyFilter{})
	if err != nil || len(proxies) != 1 {
		t.Fatalf("QueryValidated: %+v err=%v", proxies, err)
	}
	if proxies[0].LastStatus != "failed" || proxies[0].LastOkAt != lastOK.UnixMilli() || proxies[0].LastCheckedAt != failedAt.UnixMilli() {
		t.Fatalf("freshness fields after failure = %+v", proxies[0])
	}

	// 1111100 -> 71%: removed, and the ordinary failed interval applies.
	failedAt = h.probe(false)
	if listed() {
		t.Fatal("failure dropping ratio below 80% must delist the candidate")
	}
	if want := float64(failedAt.Add(accuracyPolicy.FailedAfter).UnixMilli()); score("") != want {
		t.Fatalf("delisted failure score = %v, want %v", score(""), want)
	}
}

func TestQueryValidatedMaxAgeFilter(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	nowMs := time.Now().UnixMilli()
	records := map[string]string{
		"fresh": strconv.FormatInt(nowMs-int64(time.Minute/time.Millisecond), 10),
		"stale": strconv.FormatInt(nowMs-int64(2*time.Hour/time.Millisecond), 10),
		"never": "",
	}
	for id, lastOK := range records {
		mini.HSet(s.proxyKey(id), "url", "http://"+id+".example:80", "ok_ratio_pct", "100", "last_status", "ok")
		if lastOK != "" {
			mini.HSet(s.proxyKey(id), "last_ok_at_ms", lastOK)
		}
		mini.SAdd(s.validatedKey(), id)
	}
	ctx := context.Background()
	for _, tc := range []struct {
		maxAge time.Duration
		limit  int
		want   int
	}{
		{0, 0, 3},
		{30 * time.Minute, 0, 1},
		{30 * time.Minute, 5, 1},
		{3 * time.Hour, 0, 2},
	} {
		got, err := s.QueryValidated(ctx, ProxyFilter{MaxAgeMs: tc.maxAge.Milliseconds(), Limit: tc.limit})
		if err != nil || len(got) != tc.want {
			t.Fatalf("max age %s limit %d: got %d err=%v, want %d", tc.maxAge, tc.limit, len(got), err, tc.want)
		}
	}
}

func TestPruneFailedValidatedRequiresMinSamples(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	records := map[string][]string{
		"short-history":  {"last_status", "ok", "ok_ratio_pct", "100", "results10", "11"},
		"enough-samples": {"last_status", "ok", "ok_ratio_pct", "100", "results10", "111"},
		"legacy-no-hist": {"last_status", "ok", "ok_ratio_pct", "100"},
		"unstable":       {"last_status", "ok", "ok_ratio_pct", "75", "results10", "1101"},
	}
	for id, fields := range records {
		mini.HSet(s.proxyKey(id), fields...)
		mini.SAdd(s.validatedKey(), id)
	}
	pruned, err := s.PruneFailedValidated(context.Background(), 3)
	if err != nil || pruned != 2 {
		t.Fatalf("PruneFailedValidated = %d, %v; want 2", pruned, err)
	}
	for id, want := range map[string]bool{"short-history": false, "enough-samples": true, "legacy-no-hist": true, "unstable": false} {
		if member, _ := mini.SIsMember(s.validatedKey(), id); member != want {
			t.Fatalf("%s member=%t want %t", id, member, want)
		}
	}
}

func TestBelowValidatedThresholdSampleCount(t *testing.T) {
	for _, tc := range []struct {
		ratio, status, results interface{}
		min                    int
		want                   bool
	}{
		{"100", "ok", "11", 3, true},
		{"100", "ok", "111", 3, false},
		{"100", "ok", "1", 1, false},
		{"100", "ok", nil, 3, false},
		{"70", "ok", "1111111000", 3, true},
		{nil, "failed", nil, 3, true},
	} {
		if got := belowValidatedThreshold(tc.ratio, tc.status, tc.results, tc.min); got != tc.want {
			t.Fatalf("belowValidatedThreshold(%v,%v,%v,%d) = %t want %t", tc.ratio, tc.status, tc.results, tc.min, got, tc.want)
		}
	}
}

// TestQueryValidatedSkipsStoredHostileURLs covers records written before the
// hostname syntax check existed: they must not be served by either API.
func TestQueryValidatedSkipsStoredHostileURLs(t *testing.T) {
	mini, s := newTestStore(t, "test:v1")
	urls := map[string]string{
		"ok":      "http://proxy.example:80",
		"ok6":     "http://[2606:4700::1111]:80",
		"subst":   "http://$(id).evil.example:80",
		"semi":    "http://a;id;.evil.example:80",
		"zone":    "http://[fe80::1%eth0]:80",
		"private": "http://10.0.0.1:80",
		"garbage": "not a url",
	}
	for id, rawURL := range urls {
		mini.HSet(s.proxyKey(id), "url", rawURL, "ok_ratio_pct", "100", "last_status", "ok")
		mini.SAdd(s.validatedKey(), id)
	}
	ctx := context.Background()
	for _, limit := range []int{0, 1000} {
		got, err := s.QueryValidated(ctx, ProxyFilter{Limit: limit})
		if err != nil {
			t.Fatal(err)
		}
		served := make(map[string]bool)
		for _, proxy := range got {
			served[proxy.URL] = true
		}
		if len(got) != 2 || !served["http://proxy.example:80"] || !served["http://[2606:4700::1111]:80"] {
			t.Fatalf("limit %d: served %v, want only the two valid public endpoints", limit, served)
		}
	}
}
