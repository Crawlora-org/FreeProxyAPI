// Package source fetches configured inventory sources.
package source

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

type Fetcher struct {
	client   *http.Client
	maxBytes int64
}

// HTTPStatusError reports a non-200 response from an inventory source. The
// retry-after value is parsed from the response header for callers handling
// rate limiting; it is never included in the error text.
type HTTPStatusError struct {
	StatusCode int
	Status     string
	RetryAfter time.Duration
}

func (e *HTTPStatusError) Error() string { return fmt.Sprintf("source returned %s", e.Status) }

func NewFetcher(timeout time.Duration, maxBytes int64) *Fetcher {
	return newFetcher(timeout, maxBytes, endpoint.DialPublicContext)
}

// newFetcher lets tests substitute a plain dialer to reach loopback fixtures;
// production always dials through endpoint.DialPublicContext so unsafe literal
// or resolved source addresses are refused.
func newFetcher(timeout time.Duration, maxBytes int64, dialContext func(ctx context.Context, network, address string) (net.Conn, error)) *Fetcher {
	if timeout <= 0 {
		timeout = 15 * time.Second
	}
	if maxBytes <= 0 {
		maxBytes = 1 << 20
	}
	return &Fetcher{
		maxBytes: maxBytes,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				Proxy:                 nil,
				DialContext:           dialContext,
				TLSHandshakeTimeout:   timeout / 2,
				ResponseHeaderTimeout: timeout,
				MaxIdleConns:          4,
				MaxIdleConnsPerHost:   2,
				IdleConnTimeout:       30 * time.Second,
			},
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return fmt.Errorf("source redirects are not allowed")
			},
		},
	}
}

// Fetch returns a bounded source payload.
func (f *Fetcher) Fetch(ctx context.Context, rawURL string) ([]byte, error) {
	var data []byte
	err := f.Stream(ctx, rawURL, func(reader io.Reader) error {
		var err error
		data, err = io.ReadAll(reader)
		return err
	})
	return data, err
}

// Stream fetches a source and visits its bounded body without retaining the
// whole feed. The callback must consume the reader before returning.
func (f *Fetcher) Stream(ctx context.Context, rawURL string, visit func(io.Reader) error) error {
	u, err := url.ParseRequestURI(strings.TrimSpace(rawURL))
	if err != nil {
		return fmt.Errorf("parse source URL: %w", err)
	}
	if visit == nil {
		return fmt.Errorf("source visitor is required")
	}
	switch u.Scheme {
	case "file":
		file, err := os.Open(u.Path)
		if err != nil {
			return fmt.Errorf("open source file: %w", err)
		}
		defer file.Close()
		return f.streamBounded(file, visit)
	case "http", "https":
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		// Some public feed providers, including Geonode, reject the default
		// curl-like agent even though their endpoint is public.
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; FreeProxyAPI/0.1; source-fetch)")
		req.Header.Set("Accept", "text/plain,application/json;q=0.9,*/*;q=0.1")
		response, err := f.client.Do(req)
		if err != nil {
			return fmt.Errorf("fetch source: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			return &HTTPStatusError{
				StatusCode: response.StatusCode,
				Status:     response.Status,
				RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
			}
		}
		return f.streamBounded(response.Body, visit)
	default:
		return fmt.Errorf("unsupported source URL scheme %q", u.Scheme)
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil || !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

func (f *Fetcher) streamBounded(reader io.Reader, visit func(io.Reader) error) error {
	bounded := &limitReader{reader: reader, max: f.maxBytes}
	if err := visit(bounded); err != nil {
		if bounded.exceeded {
			return bounded.limitError()
		}
		return err
	}
	if bounded.exceeded {
		return bounded.limitError()
	}
	return nil
}

// ErrSourceTooLarge reports a source body larger than the fetcher byte limit.
var ErrSourceTooLarge = errors.New("source exceeds byte limit")

// limitReader never yields more than max bytes. Once the source has more
// data than that, every Read fails immediately so a streaming visitor stops
// at the limit instead of consuming the rest of the body first.
type limitReader struct {
	reader   io.Reader
	max      int64
	count    int64
	exceeded bool
}

func (r *limitReader) limitError() error {
	return fmt.Errorf("%w: limit is %d bytes", ErrSourceTooLarge, r.max)
}

func (r *limitReader) Read(p []byte) (int, error) {
	if r.exceeded {
		return 0, r.limitError()
	}
	if len(p) == 0 {
		return 0, nil
	}
	if r.count >= r.max {
		// Probe for one more byte to distinguish an exact-size body from an
		// oversized one.
		var probe [1]byte
		for {
			n, err := r.reader.Read(probe[:])
			if n > 0 {
				r.exceeded = true
				return 0, r.limitError()
			}
			if err != nil {
				return 0, err
			}
		}
	}
	if remaining := r.max - r.count; int64(len(p)) > remaining {
		p = p[:remaining]
	}
	n, err := r.reader.Read(p)
	r.count += int64(n)
	return n, err
}
