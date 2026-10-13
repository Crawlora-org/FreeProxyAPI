package source

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestStreamETag304ReingestsCachedBody(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if requests.Load() == 1 {
			w.Header().Set("ETag", `"v1"`)
			_, _ = io.WriteString(w, "proxy-v1\n")
			return
		}
		if got := r.Header.Get("If-None-Match"); got != `"v1"` {
			t.Errorf("If-None-Match = %q, want %q", got, `"v1"`)
		}
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()

	fetcher := newFetcher(2*time.Second, 1024, plainDialer)
	var visits []string
	visit := func(r io.Reader) error {
		body, err := io.ReadAll(r)
		if err == nil {
			visits = append(visits, string(body))
		}
		return err
	}
	for range 2 {
		if err := fetcher.Stream(context.Background(), server.URL, visit); err != nil {
			t.Fatalf("Stream: %v", err)
		}
	}
	if len(visits) != 2 || visits[0] != "proxy-v1\n" || visits[1] != visits[0] {
		t.Fatalf("visitor payloads = %#v, want cached payload replayed twice", visits)
	}
}

func TestStreamLastModifiedAndChanged200ReplaceCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
			_, _ = io.WriteString(w, "old\n")
		case 2:
			if got := r.Header.Get("If-Modified-Since"); got != "Wed, 21 Oct 2015 07:28:00 GMT" {
				t.Errorf("If-Modified-Since = %q", got)
			}
			w.Header().Set("ETag", `"v2"`)
			_, _ = io.WriteString(w, "new\n")
		case 3:
			if got := r.Header.Get("If-None-Match"); got != `"v2"` {
				t.Errorf("If-None-Match after replacement = %q", got)
			}
			if got := r.Header.Get("If-Modified-Since"); got != "" {
				t.Errorf("stale Last-Modified = %q", got)
			}
			w.WriteHeader(http.StatusNotModified)
		}
	}))
	defer server.Close()

	fetcher := newFetcher(2*time.Second, 1024, plainDialer)
	for i, want := range []string{"old\n", "new\n", "new\n"} {
		got, err := fetcher.Fetch(context.Background(), server.URL)
		if err != nil {
			t.Fatalf("Fetch %d: %v", i+1, err)
		}
		if string(got) != want {
			t.Fatalf("Fetch %d = %q, want %q", i+1, got, want)
		}
	}
}

func TestFailedVisitorPreservesCacheAndUncacheable200Invalidates(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			w.Header().Set("ETag", `"good"`)
			_, _ = io.WriteString(w, "good\n")
		case 2:
			w.Header().Set("ETag", `"bad"`)
			_, _ = io.WriteString(w, "bad\n")
		case 3:
			if got := r.Header.Get("If-None-Match"); got != `"good"` {
				t.Errorf("failed refresh replaced cache: If-None-Match = %q", got)
			}
			w.WriteHeader(http.StatusNotModified)
		case 4:
			_, _ = io.WriteString(w, "unvalidated\n")
		case 5:
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Errorf("uncacheable 200 left stale validator %q", got)
			}
			_, _ = io.WriteString(w, "again\n")
		}
	}))
	defer server.Close()
	fetcher := newFetcher(2*time.Second, 1024, plainDialer)
	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
	err := fetcher.Stream(context.Background(), server.URL, func(r io.Reader) error {
		_, _ = io.Copy(io.Discard, r)
		return fmt.Errorf("parse failed")
	})
	if err == nil || !strings.Contains(err.Error(), "parse failed") {
		t.Fatalf("failed visitor error = %v", err)
	}
	if got, err := fetcher.Fetch(context.Background(), server.URL); err != nil || string(got) != "good\n" {
		t.Fatalf("Fetch after failed refresh = %q, %v", got, err)
	}
	if got, err := fetcher.Fetch(context.Background(), server.URL); err != nil || string(got) != "unvalidated\n" {
		t.Fatalf("uncacheable Fetch = %q, %v", got, err)
	}
	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestSuccessfulPartialVisitorInvalidatesOldCache(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requests.Add(1) {
		case 1:
			w.Header().Set("ETag", `"old"`)
			_, _ = io.WriteString(w, "old body")
		case 2:
			w.Header().Set("ETag", `"new"`)
			_, _ = io.WriteString(w, "new body")
		case 3:
			if got := r.Header.Get("If-None-Match"); got != "" {
				t.Errorf("partial response left cached validator %q", got)
			}
			_, _ = io.WriteString(w, "third body")
		}
	}))
	defer server.Close()
	fetcher := newFetcher(2*time.Second, 1024, plainDialer)
	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
	if err := fetcher.Stream(context.Background(), server.URL, func(r io.Reader) error {
		var one [1]byte
		_, err := r.Read(one[:])
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
}

func TestCacheCaptureHonorsEntryAndConfiguredBounds(t *testing.T) {
	for _, maxBytes := range []int64{feedCacheEntryMaxBytes, feedCacheEntryMaxBytes + 20} {
		var conditional atomic.Bool
		payload := strings.Repeat("x", int(feedCacheEntryMaxBytes+1))
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conditional.Store(r.Header.Get("If-None-Match") != "")
			w.Header().Set("ETag", `"big"`)
			_, _ = io.WriteString(w, payload)
		}))
		fetcher := newFetcher(2*time.Second, maxBytes, plainDialer)
		err := fetcher.Stream(context.Background(), server.URL, func(r io.Reader) error {
			_, err := io.Copy(io.Discard, r)
			return err
		})
		if maxBytes <= int64(len(payload)) && err == nil {
			t.Fatal("oversized payload accepted")
		}
		if maxBytes > int64(len(payload)) && err != nil {
			t.Fatalf("cache-sized-independent body fetch: %v", err)
		}
		if maxBytes > int64(len(payload)) {
			if err := fetcher.Stream(context.Background(), server.URL, func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err }); err != nil {
				t.Fatal(err)
			}
			if conditional.Load() {
				t.Fatal("entry larger than cache cap acquired a validator")
			}
		}
		server.Close()
	}

}

func TestFeedCacheLRUBounds(t *testing.T) {
	fetcher := newFetcher(time.Second, feedCacheEntryMaxBytes, plainDialer)
	for i := 0; i < feedCacheMaxEntries+1; i++ {
		fetcher.putCached(fmt.Sprintf("key-%d", i), []byte("x"), fmt.Sprintf("etag-%d", i), "")
	}
	if fetcher.cacheLRU.Len() != feedCacheMaxEntries {
		t.Fatalf("entry count = %d, want %d", fetcher.cacheLRU.Len(), feedCacheMaxEntries)
	}
	if fetcher.getCached("key-0") != nil {
		t.Fatal("least recently used entry was retained")
	}
	if fetcher.getCached("key-1") == nil {
		t.Fatal("second-oldest entry was evicted unexpectedly")
	}

	body := make([]byte, feedCacheEntryMaxBytes)
	for i := 0; i < 17; i++ {
		fetcher.putCached(fmt.Sprintf("large-%d", i), body, fmt.Sprintf("large-etag-%d", i), "")
	}
	if fetcher.cacheLen > feedCacheMaxBytes || fetcher.cacheLRU.Len() > feedCacheMaxEntries {
		t.Fatalf("cache bounds exceeded: bytes=%d entries=%d", fetcher.cacheLen, fetcher.cacheLRU.Len())
	}
	if fetcher.getCached("large-0") != nil {
		t.Fatal("total-byte LRU did not evict oldest entry")
	}
	fetcher.putCached("too-big", make([]byte, feedCacheEntryMaxBytes+1), "too-big", "")
	if fetcher.getCached("too-big") != nil {
		t.Fatal("per-entry cache limit was exceeded")
	}
}

func TestFileStreamBypassesFeedCache(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "feed.txt")
	if err := os.WriteFile(path, []byte("file-body"), 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := newFetcher(time.Second, 1024, plainDialer)
	if _, err := fetcher.Fetch(context.Background(), "file://"+path); err != nil {
		t.Fatal(err)
	}
	if len(fetcher.cache) != 0 {
		t.Fatalf("file fetch created %d HTTP cache entries", len(fetcher.cache))
	}
}

func TestConcurrentFetchesShareCacheSafely(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if r.Header.Get("If-None-Match") == `"shared"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"shared"`)
		_, _ = io.WriteString(w, "shared\n")
	}))
	defer server.Close()
	fetcher := newFetcher(3*time.Second, 1024, plainDialer)
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			body, err := fetcher.Fetch(context.Background(), server.URL)
			if err != nil || string(body) != "shared\n" {
				t.Errorf("concurrent Fetch = %q, %v", body, err)
			}
		}()
	}
	wg.Wait()
	body, err := fetcher.Fetch(context.Background(), server.URL)
	if err != nil || string(body) != "shared\n" {
		t.Fatalf("cached Fetch = %q, %v", body, err)
	}
	if hits.Load() < 2 {
		t.Fatalf("request hits = %d, expected concurrent refresh and conditional fetch", hits.Load())
	}
}

func TestCache304WithoutBodyErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 1024, plainDialer)
	if _, err := fetcher.Fetch(context.Background(), server.URL); err == nil {
		t.Fatal("unsolicited 304 returned no error")
	}
}

func TestCachedReplayHonorsCancellation(t *testing.T) {
	var second atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if second.Swap(true) {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"cancel"`)
		_, _ = io.WriteString(w, "body")
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 1024, plainDialer)
	if _, err := fetcher.Fetch(context.Background(), server.URL); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := fetcher.Stream(ctx, server.URL, func(r io.Reader) error { _, err := io.ReadAll(r); return err })
	if err == nil {
		t.Fatal("cached replay ignored canceled context")
	}
	ctx, cancel = context.WithCancel(context.Background())
	err = fetcher.Stream(ctx, server.URL, func(r io.Reader) error {
		_, err := io.Copy(io.Discard, r)
		cancel()
		return err
	})
	cancel()
	if err == nil {
		t.Fatal("cached replay ignored cancellation during visitor")
	}
}
