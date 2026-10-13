// Package source fetches configured inventory sources.
package source

import (
	"bytes"
	"container/list"
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
	"sync"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

type Fetcher struct {
	client   *http.Client
	maxBytes int64
	cacheMu  sync.Mutex
	cache    map[string]*list.Element
	cacheLRU *list.List
	cacheLen int64
}

const (
	feedCacheMaxBytes      int64 = 16 << 20
	feedCacheMaxEntries          = 128
	feedCacheEntryMaxBytes int64 = 1 << 20
	feedCacheValidatorMax        = 1024
)

// feedCacheEntry and its body are immutable after insertion. A replay may
// retain an entry after LRU eviction without holding the cache lock.
type feedCacheEntry struct {
	key          string
	body         []byte
	etag         string
	lastModified string
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
		cache:    make(map[string]*list.Element),
		cacheLRU: list.New(),
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

// Stream fetches a source and visits its bounded body. HTTP feeds with usable
// validators may retain at most feedCacheEntryMaxBytes in the bounded cache;
// larger feeds stream without caching. The callback must consume the reader
// before returning.
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
		cacheKey := strings.TrimSpace(rawURL)
		cached := f.getCached(cacheKey)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return err
		}
		// Some public feed providers, including Geonode, reject the default
		// curl-like agent even though their endpoint is public.
		req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; FreeProxyAPI/0.1; source-fetch)")
		req.Header.Set("Accept", "text/plain,application/json;q=0.9,*/*;q=0.1")
		if cached != nil {
			if cached.etag != "" {
				req.Header.Set("If-None-Match", cached.etag)
			}
			if cached.lastModified != "" {
				req.Header.Set("If-Modified-Since", cached.lastModified)
			}
		}
		response, err := f.client.Do(req)
		if err != nil {
			return fmt.Errorf("fetch source: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode == http.StatusNotModified {
			if cached == nil {
				return fmt.Errorf("source returned 304 without a cached body")
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			err := f.streamBounded(&contextReader{ctx: ctx, reader: bytes.NewReader(cached.body)}, visit)
			if err != nil {
				return err
			}
			return ctx.Err()
		}
		if response.StatusCode != http.StatusOK {
			return &HTTPStatusError{
				StatusCode: response.StatusCode,
				Status:     response.Status,
				RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"), time.Now()),
			}
		}
		etag := validCacheValidator(response.Header.Get("ETag"))
		lastModified := validCacheValidator(response.Header.Get("Last-Modified"))
		captureLimit := feedCacheEntryMaxBytes
		if f.maxBytes < captureLimit {
			captureLimit = f.maxBytes
		}
		canCapture := (etag != "" || lastModified != "") &&
			(response.ContentLength < 0 || response.ContentLength <= captureLimit)
		if !canCapture {
			err := f.streamBounded(response.Body, visit)
			if err != nil {
				return err
			}
			f.invalidateCached(cacheKey)
			return nil
		}
		captured := &captureReader{reader: &limitReader{reader: response.Body, max: f.maxBytes}, max: captureLimit, cacheable: true}
		err = visit(captured)
		if err != nil {
			if captured.exceeded {
				return captured.limitError(f.maxBytes)
			}
			return err
		}
		if captured.exceeded {
			return captured.limitError(f.maxBytes)
		}
		if !captured.eof {
			f.invalidateCached(cacheKey)
			return nil
		}
		if captured.cacheable {
			f.putCached(cacheKey, captured.body, etag, lastModified)
		} else {
			f.invalidateCached(cacheKey)
		}
		return nil
	default:
		return fmt.Errorf("unsupported source URL scheme %q", u.Scheme)
	}
}

func validCacheValidator(value string) string {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > feedCacheValidatorMax {
		return ""
	}
	return value
}

func (f *Fetcher) getCached(key string) *feedCacheEntry {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	element := f.cache[key]
	if element == nil {
		return nil
	}
	f.cacheLRU.MoveToFront(element)
	return element.Value.(*feedCacheEntry)
}

func (f *Fetcher) putCached(key string, body []byte, etag, lastModified string) {
	if int64(len(body)) > feedCacheEntryMaxBytes || key == "" {
		return
	}
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.removeCachedLocked(key)
	entry := &feedCacheEntry{key: key, body: append([]byte(nil), body...), etag: etag, lastModified: lastModified}
	f.cache[key] = f.cacheLRU.PushFront(entry)
	f.cacheLen += int64(len(entry.body))
	for f.cacheLen > feedCacheMaxBytes || f.cacheLRU.Len() > feedCacheMaxEntries {
		f.removeCachedLocked(f.cacheLRU.Back().Value.(*feedCacheEntry).key)
	}
}

func (f *Fetcher) invalidateCached(key string) {
	f.cacheMu.Lock()
	defer f.cacheMu.Unlock()
	f.removeCachedLocked(key)
}

func (f *Fetcher) removeCachedLocked(key string) {
	element := f.cache[key]
	if element == nil {
		return
	}
	delete(f.cache, key)
	f.cacheLen -= int64(len(element.Value.(*feedCacheEntry).body))
	f.cacheLRU.Remove(element)
}

type captureReader struct {
	reader    io.Reader
	max       int64
	body      []byte
	cacheable bool
	eof       bool
	exceeded  bool
}

func (r *captureReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.cacheable {
		if int64(len(r.body))+int64(n) > r.max {
			r.cacheable = false
			r.body = nil
		} else {
			r.body = append(r.body, p[:n]...)
		}
	}
	if errors.Is(err, io.EOF) {
		r.eof = true
	}
	if errors.Is(err, ErrSourceTooLarge) {
		r.exceeded = true
	}
	return n, err
}

func (r *captureReader) limitError(max int64) error {
	return fmt.Errorf("%w: limit is %d bytes", ErrSourceTooLarge, max)
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(p)
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
