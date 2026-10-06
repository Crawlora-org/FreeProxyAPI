package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

func TestFetchFileSourceIsBounded(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	if err := os.WriteFile(path, []byte("proxy.example.net:8080\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := NewFetcher(time.Second, 1<<20)
	payload, err := fetcher.Fetch(context.Background(), "file://"+path)
	if err != nil {
		t.Fatalf("Fetch file: %v", err)
	}
	if string(payload) != "proxy.example.net:8080\n" {
		t.Fatalf("payload = %q", payload)
	}

	if _, err := fetcher.Fetch(context.Background(), "file:///nonexistent/proxies.txt"); err == nil {
		t.Fatal("Fetch accepted a missing file")
	}
}

func TestFetchFileSourceRejectsOversizedPayload(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "big.txt")
	if err := os.WriteFile(path, []byte(strings.Repeat("a", 4096)), 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := NewFetcher(time.Second, 1024)
	if _, err := fetcher.Fetch(context.Background(), "file://"+path); err == nil {
		t.Fatal("Fetch accepted an oversized file source")
	}
}

func TestFetchHTTPSourceRejectsRedirectsAndErrors(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			if !strings.HasPrefix(r.UserAgent(), "Mozilla/5.0") {
				t.Errorf("User-Agent = %q, want Mozilla-compatible source agent", r.UserAgent())
			}
			fmt.Fprintln(w, "proxy.example.net:8080")
		case "/redirect":
			http.Redirect(w, r, "/ok", http.StatusFound)
		case "/error":
			w.WriteHeader(http.StatusInternalServerError)
		case "/oversize":
			w.Write([]byte(strings.Repeat("b", 8192)))
		default:
			http.NotFound(w, r)
		}
	}))
	defer target.Close()

	fetcher := newFetcher(2*time.Second, 1024, plainDialer)
	ctx := context.Background()
	if _, err := fetcher.Fetch(ctx, target.URL+"/ok"); err != nil {
		t.Fatalf("Fetch ok: %v", err)
	}
	if _, err := fetcher.Fetch(ctx, target.URL+"/redirect"); err == nil {
		t.Fatal("Fetch followed a redirect; sources must be fetched directly")
	}
	if _, err := fetcher.Fetch(ctx, target.URL+"/error"); err == nil {
		t.Fatal("Fetch accepted a non-200 response")
	}
	if _, err := fetcher.Fetch(ctx, target.URL+"/oversize"); err == nil {
		t.Fatal("Fetch accepted an oversized HTTP payload")
	}
}

func TestFetchHTTPStatusErrorPreservesRetryAfter(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer target.Close()

	fetcher := newFetcher(time.Second, 1024, plainDialer)
	_, err := fetcher.Fetch(context.Background(), target.URL)
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("Fetch error = %v, want HTTPStatusError", err)
	}
	if statusErr.StatusCode != http.StatusTooManyRequests || statusErr.RetryAfter != 7*time.Second {
		t.Fatalf("HTTPStatusError = %+v, want status 429 and 7s retry delay", statusErr)
	}
}

func TestFetchRejectsUnsupportedScheme(t *testing.T) {
	fetcher := NewFetcher(time.Second, 1024)
	if _, err := fetcher.Fetch(context.Background(), "ftp://inventory.example.net/list.txt"); err == nil {
		t.Fatal("Fetch accepted an unsupported scheme")
	}
}

func plainDialer(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

type countingSource struct {
	remaining int
	read      int
}

func (s *countingSource) Read(p []byte) (int, error) {
	if s.remaining == 0 {
		return 0, io.EOF
	}
	n := len(p)
	if n > s.remaining {
		n = s.remaining
	}
	for i := range p[:n] {
		p[i] = 'a'
	}
	s.remaining -= n
	s.read += n
	return n, nil
}

func TestStreamStopsAtByteLimit(t *testing.T) {
	fetcher := NewFetcher(time.Second, 1024)
	source := &countingSource{remaining: 1 << 20}
	var delivered int
	err := fetcher.streamBounded(source, func(r io.Reader) error {
		buf := make([]byte, 300)
		for {
			n, err := r.Read(buf)
			delivered += n
			if err == io.EOF {
				return nil
			}
			if err != nil {
				return err
			}
		}
	})
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("error = %v, want ErrSourceTooLarge", err)
	}
	if delivered != 1024 {
		t.Fatalf("delivered %d bytes, want exactly the 1024-byte limit", delivered)
	}
	if source.read > 1025 {
		t.Fatalf("read %d bytes from source, want at most limit+1", source.read)
	}

	exact := &countingSource{remaining: 1024}
	if err := fetcher.streamBounded(exact, func(r io.Reader) error {
		_, err := io.ReadAll(r)
		return err
	}); err != nil {
		t.Fatalf("exact-limit body rejected: %v", err)
	}
}

func TestStreamLimitStopsFeedVisitorBeforePartialRecord(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "proxies.txt")
	feed := "203.0.113.10:8080\n203.0.113.11:8080\n"
	if err := os.WriteFile(path, []byte(feed), 0o600); err != nil {
		t.Fatal(err)
	}
	fetcher := NewFetcher(time.Second, int64(len("203.0.113.10:8080\n203.0.113.1")))
	var visited int
	err := fetcher.Stream(context.Background(), "file://"+path, func(r io.Reader) error {
		_, err := endpoint.VisitFeedProxies(r, "http", func(endpoint.Proxy) error {
			visited++
			return nil
		})
		return err
	})
	if !errors.Is(err, ErrSourceTooLarge) {
		t.Fatalf("error = %v, want ErrSourceTooLarge", err)
	}
	if visited != 1 {
		t.Fatalf("visited %d records, want 1 (truncated record must not be visited)", visited)
	}
}
