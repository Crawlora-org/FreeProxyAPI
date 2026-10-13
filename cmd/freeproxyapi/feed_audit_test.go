package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

type auditFixtureFetcher struct {
	mu       sync.Mutex
	payloads map[string]string
	errors   map[string]error
	calls    map[string]int
}

func (f *auditFixtureFetcher) StreamMeasured(_ context.Context, rawURL string, visit func(io.Reader) error) (source.FetchStats, error) {
	f.mu.Lock()
	f.calls[rawURL]++
	payload := f.payloads[rawURL]
	err := f.errors[rawURL]
	f.mu.Unlock()
	stats := source.FetchStats{HTTPAttempts: 1, NetworkBytes: int64(len(payload)), Duration: 5 * time.Millisecond}
	if err != nil {
		return stats, err
	}
	err = visit(strings.NewReader(payload))
	return stats, err
}

func TestDecodeFeedAuditInputsRejectsUnsafeURLsBeforeFetching(t *testing.T) {
	options := testFeedAuditOptions()
	fetcher := &auditFixtureFetcher{calls: make(map[string]int)}
	inputs := []feedAuditInput{
		{URL: "https://feed.example/list", Scheme: "http"},
		{URL: "https://user:secret@feed.example/list", Scheme: "http"},
	}
	if _, err := runFeedAudit(context.Background(), inputs, options, fetcher); err == nil {
		t.Fatal("invalid batch was accepted")
	}
	if len(fetcher.calls) != 0 {
		t.Fatalf("invalid batch started requests: %#v", fetcher.calls)
	}

	for _, raw := range []string{
		"file:///tmp/feed",
		"ftp://feed.example/list",
		"https://user:pass@feed.example/list",
		"https://feed.example/list#fragment",
		"//feed.example/list",
	} {
		if err := validateFeedAuditURL(raw); err == nil {
			t.Errorf("validateFeedAuditURL(%q) succeeded", raw)
		}
	}
	for _, scheme := range []string{"auto", "https"} {
		data := []byte(`[{"url":"https://feed.example/list","scheme":"` + scheme + `"}]`)
		_, err := decodeFeedAuditInputs(data, 10)
		if err == nil {
			t.Errorf("decode accepted parser scheme %q", scheme)
		}
	}
	if _, err := decodeFeedAuditInputs([]byte(`[{"url":"https://feed.example/list","scheme":"SOCKS4"}]`), 10); err != nil {
		t.Errorf("decode rejected case-insensitive SOCKS4: %v", err)
	}
	decoded, err := decodeFeedAuditInputs([]byte(`[{"url":"https://feed.example/list"}]`), 10)
	if err != nil || len(decoded) != 1 || decoded[0].Scheme != "http" {
		t.Fatalf("default parser scheme = %#v, %v", decoded, err)
	}
}

func TestDecodeFeedAuditInputsRejectsMalformedRecords(t *testing.T) {
	for _, raw := range []string{
		`{ "url": "https://feed.example/list" }`,
		`[{"url":"https://feed.example/list","unexpected":true}]`,
		`[{"url":"https://feed.example/list"}] []`,
		`null`,
	} {
		if _, err := decodeFeedAuditInputs([]byte(raw), 10); err == nil {
			t.Errorf("decodeFeedAuditInputs accepted %s", raw)
		}
	}
	if _, err := decodeFeedAuditInputs([]byte(`[{"url":"https://one.example/list"},{"url":"https://two.example/list"}]`), 1); err == nil {
		t.Fatal("decodeFeedAuditInputs accepted too many source records")
	}
}

func TestRunFeedAuditAggregatesDeduplicatesAndHashesOnlyCompleteSet(t *testing.T) {
	const feedURL = "https://feed.example/inventory"
	fetcher := &auditFixtureFetcher{
		payloads: map[string]string{feedURL: "proxy.example:8080\nproxy.example:8080\nsocks5://proxy2.example:1080\ninvalid\n"},
		calls:    make(map[string]int),
	}
	results, err := runFeedAudit(context.Background(), []feedAuditInput{{URL: feedURL}}, testFeedAuditOptions(), fetcher)
	if err != nil || len(results) != 1 {
		t.Fatalf("runFeedAudit = %#v, %v", results, err)
	}
	result := results[0]
	if result.SourceID != store.StableSourceID(feedURL, "http") || result.InputIndex != 0 || result.Scheme != "http" {
		t.Fatalf("identity fields = %+v", result)
	}
	if !result.Eligible || result.Truncated || result.ParsedUnique != 2 || result.Rejected != 1 {
		t.Fatalf("feed counts = %+v", result)
	}
	if result.ByScheme["http"] != 1 || result.ByScheme["socks5"] != 1 || result.HTTPAttempts != 1 || result.NetworkBytes == 0 || result.DurationMillis != 5 || result.CacheHit {
		t.Fatalf("feed metrics = %+v", result)
	}
	if len(result.SetHash) != 64 {
		t.Fatalf("canonical-set hash = %q, want 64 hex chars", result.SetHash)
	}
	if _, err := runFeedAudit(context.Background(), []feedAuditInput{{URL: feedURL}}, testFeedAuditOptions(), fetcher); err != nil {
		t.Fatal(err)
	}
	if results[0].SetHash != result.SetHash {
		t.Fatal("canonical-set hash changed for identical contents")
	}
}

func TestRunFeedAuditMarksRecordLimitIneligible(t *testing.T) {
	const feedURL = "https://feed.example/limited"
	fetcher := &auditFixtureFetcher{
		payloads: map[string]string{feedURL: "proxy1.example:8080\nproxy2.example:8080\nproxy3.example:8080\n"},
		calls:    make(map[string]int),
	}
	options := testFeedAuditOptions()
	options.MaxRecords = 2
	results, err := runFeedAudit(context.Background(), []feedAuditInput{{URL: feedURL}}, options, fetcher)
	if err != nil || len(results) != 1 {
		t.Fatalf("runFeedAudit = %#v, %v", results, err)
	}
	if results[0].Eligible || !results[0].Truncated || results[0].ErrorCode != "record_limit" || results[0].ParsedUnique != 2 || results[0].SetHash != "" {
		t.Fatalf("truncated result = %+v", results[0])
	}
}

func TestRunFeedAuditUsesRedactedErrorCodes(t *testing.T) {
	const feedURL = "https://feed.example/unavailable"
	fetcher := &auditFixtureFetcher{
		errors: map[string]error{feedURL: &source.HTTPStatusError{StatusCode: 503, Status: "503 Service Unavailable"}},
		calls:  make(map[string]int),
	}
	results, err := runFeedAudit(context.Background(), []feedAuditInput{{URL: feedURL}}, testFeedAuditOptions(), fetcher)
	if err != nil || len(results) != 1 {
		t.Fatalf("runFeedAudit = %#v, %v", results, err)
	}
	if results[0].Eligible || results[0].ErrorCode != "http_status" || results[0].HTTPStatus != 503 {
		t.Fatalf("status result = %+v", results[0])
	}
	code, status := feedAuditErrorCode(fmt.Errorf("contains private source data"))
	if code != "fetch_or_parse_error" || status != 0 {
		t.Fatalf("generic error classification = %q/%d", code, status)
	}
}

func testFeedAuditOptions() feedAuditOptions {
	return feedAuditOptions{Timeout: 20 * time.Second, Concurrency: 2, MaxBytes: feedAuditMaxBytes, MaxRecords: feedAuditMaxRecords}
}

func TestFeedAuditRejectsEmptyFeed(t *testing.T) {
	result := auditOneFeed(context.Background(), 0, feedAuditInput{URL: "https://feeds.example/empty", Scheme: "http"}, feedAuditOptions{Timeout: time.Second, Concurrency: 1, MaxBytes: 1024, MaxRecords: 10}, emptyAuditFetcher{})
	if result.Eligible || result.ErrorCode != "empty_feed" {
		t.Fatalf("empty feed eligible: %+v", result)
	}
}

type emptyAuditFetcher struct{}

func (emptyAuditFetcher) StreamMeasured(ctx context.Context, u string, visit func(io.Reader) error) (source.FetchStats, error) {
	return source.FetchStats{}, visit(strings.NewReader(""))
}
