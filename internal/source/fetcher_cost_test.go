package source

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStreamMeasuredHTTPAndConditionalCache(t *testing.T) {
	const payload = "203.0.113.1:8080\n"
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, payload)
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 1024, plainDialer)

	var first FetchStats
	var got string
	first, err := fetcher.StreamMeasured(context.Background(), server.URL, func(r io.Reader) error {
		b, err := io.ReadAll(r)
		got = string(b)
		return err
	})
	if err != nil || got != payload {
		t.Fatalf("first StreamMeasured = (%q, %v)", got, err)
	}
	if first.HTTPAttempts != 1 || first.NetworkBytes != int64(len(payload)) || first.CacheHit || first.Duration <= 0 {
		t.Fatalf("first stats = %+v", first)
	}

	second, err := fetcher.StreamMeasured(context.Background(), server.URL, func(r io.Reader) error {
		b, err := io.ReadAll(r)
		got = string(b)
		return err
	})
	if err != nil || got != payload {
		t.Fatalf("304 StreamMeasured = (%q, %v)", got, err)
	}
	if second.HTTPAttempts != 1 || second.NetworkBytes != 0 || !second.CacheHit || second.Duration <= 0 {
		t.Fatalf("304 stats = %+v", second)
	}
	if requests != 2 {
		t.Fatalf("requests = %d, want 2", requests)
	}
}

func TestStreamMeasuredPartialVisitorFailureCountsConsumedBytes(t *testing.T) {
	const payload = "a longer payload that should not be cached after failure"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		_, _ = io.WriteString(w, payload)
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 1024, plainDialer)
	wantErr := errors.New("parse failed")
	stats, err := fetcher.StreamMeasured(context.Background(), server.URL, func(r io.Reader) error {
		buf := make([]byte, 3)
		_, readErr := io.ReadFull(r, buf)
		if readErr != nil {
			return readErr
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want visitor error", err)
	}
	if stats.HTTPAttempts != 1 || stats.NetworkBytes != 3 || stats.CacheHit || stats.Duration <= 0 {
		t.Fatalf("stats = %+v, want one attempt and 3 consumed bytes", stats)
	}
	if len(fetcher.cache) != 0 {
		t.Fatal("failed visitor populated cache")
	}
}

func TestStreamMeasuredOversizeCountsLimitProbe(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("ETag", `"large"`)
		_, _ = io.WriteString(w, strings.Repeat("x", 32))
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 8, plainDialer)
	stats, err := fetcher.StreamMeasured(context.Background(), server.URL, func(r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	})
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("error = %v, want ErrSourceTooLarge", err)
	}
	if stats.HTTPAttempts != 1 || stats.NetworkBytes != 9 || stats.CacheHit {
		t.Fatalf("stats = %+v, want one attempt and limit+1 body bytes", stats)
	}
}

func TestStreamMeasuredRedirectAndUnsupportedScheme(t *testing.T) {
	var targetRequests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/target" {
			targetRequests++
			_, _ = io.WriteString(w, "unexpected")
			return
		}
		http.Redirect(w, r, "/target", http.StatusFound)
	}))
	defer server.Close()
	fetcher := newFetcher(time.Second, 1024, plainDialer)
	stats, err := fetcher.StreamMeasured(context.Background(), server.URL, func(r io.Reader) error { _, err := io.Copy(io.Discard, r); return err })
	if err == nil || stats.HTTPAttempts != 1 || stats.NetworkBytes != 0 || stats.CacheHit {
		t.Fatalf("redirect = stats %+v, error %v", stats, err)
	}
	if targetRequests != 0 {
		t.Fatalf("redirect target requests = %d, want 0", targetRequests)
	}
	stats, err = fetcher.StreamMeasured(context.Background(), "ftp://inventory.example/list", func(io.Reader) error { return nil })
	if err == nil || stats.HTTPAttempts != 0 || stats.NetworkBytes != 0 || stats.CacheHit {
		t.Fatalf("unsupported scheme = stats %+v, error %v", stats, err)
	}
}

func TestStreamMeasuredFileDoesNotCountNetwork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "feed.txt")
	if err := os.WriteFile(path, []byte("local feed"), 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := NewFetcher(time.Second, 1024)
	var got string
	stats, err := fetcher.StreamMeasured(context.Background(), "file://"+path, func(r io.Reader) error {
		b, err := io.ReadAll(r)
		got = string(b)
		return err
	})
	if err != nil || got != "local feed" {
		t.Fatalf("file stream = (%q, %v)", got, err)
	}
	if stats.HTTPAttempts != 0 || stats.NetworkBytes != 0 || stats.CacheHit || stats.Duration <= 0 {
		t.Fatalf("file stats = %+v", stats)
	}
}
