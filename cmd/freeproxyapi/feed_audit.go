package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/source"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	feedAuditInputMaxBytes = 1 << 20
	feedAuditMaxInputs     = 128
	feedAuditMaxBytes      = 32 << 20
	feedAuditMaxRecords    = 100_000
	feedAuditMaxWorkers    = 4
)

type feedAuditInput struct {
	URL    string `json:"url"`
	Scheme string `json:"scheme,omitempty"`
}

type feedAuditResult struct {
	SourceID       string         `json:"source_id"`
	InputIndex     int            `json:"input_index"`
	Scheme         string         `json:"scheme"`
	Eligible       bool           `json:"eligible"`
	Truncated      bool           `json:"truncated"`
	ParsedUnique   int            `json:"parsed_unique"`
	Rejected       int            `json:"rejected"`
	ByScheme       map[string]int `json:"by_scheme"`
	HTTPAttempts   int64          `json:"http_attempts"`
	NetworkBytes   int64          `json:"network_bytes"`
	DurationMillis int64          `json:"duration_ms"`
	CacheHit       bool           `json:"cache_hit"`
	SetHash        string         `json:"hash_of_canonical_set,omitempty"`
	ErrorCode      string         `json:"error_code,omitempty"`
	HTTPStatus     int            `json:"http_status,omitempty"`
}

type feedAuditFetcher interface {
	StreamMeasured(context.Context, string, func(io.Reader) error) (source.FetchStats, error)
}

type feedAuditOptions struct {
	Timeout     time.Duration
	Concurrency int
	MaxBytes    int64
	MaxRecords  int
}

func feedAuditCommand(args []string, out io.Writer) error {
	flags := flag.NewFlagSet("feed-audit", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	inputPath := flags.String("input", "/dev/stdin", "JSON array of feed URLs")
	timeout := flags.Duration("timeout", 20*time.Second, "maximum time per source")
	concurrency := flags.Int("concurrency", 2, "maximum concurrent feed requests (1-4)")
	maxBytes := flags.Int64("max-bytes", feedAuditMaxBytes, "maximum response bytes per source")
	maxRecords := flags.Int("max-records", feedAuditMaxRecords, "maximum accepted records per source")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	options := feedAuditOptions{Timeout: *timeout, Concurrency: *concurrency, MaxBytes: *maxBytes, MaxRecords: *maxRecords}
	if err := validateFeedAuditOptions(options); err != nil {
		return err
	}
	file, err := os.Open(*inputPath)
	if err != nil {
		return fmt.Errorf("could not open audit input")
	}
	defer file.Close()
	inputBytes, err := io.ReadAll(io.LimitReader(file, feedAuditInputMaxBytes+1))
	if err != nil || len(inputBytes) > feedAuditInputMaxBytes {
		return fmt.Errorf("audit input is unreadable or too large")
	}
	inputs, err := decodeFeedAuditInputs(inputBytes, feedAuditMaxInputs)
	if err != nil {
		return err
	}
	fetcher := source.NewFetcher(options.Timeout, options.MaxBytes)
	ctx, stop := signalContext()
	defer stop()
	results, err := runFeedAudit(ctx, inputs, options, fetcher)
	if err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	for _, result := range results {
		if err := encoder.Encode(result); err != nil {
			return fmt.Errorf("could not write audit result")
		}
	}
	return nil
}

// signalContext separates the command's cancellation handling from the audit
// runner so its validation and network behavior can be tested hermetically.
func signalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func validateFeedAuditOptions(options feedAuditOptions) error {
	if options.Timeout <= 0 || options.Timeout > 90*time.Second {
		return fmt.Errorf("-timeout must be positive and no greater than 90s")
	}
	if options.Concurrency < 1 || options.Concurrency > feedAuditMaxWorkers {
		return fmt.Errorf("-concurrency must be between 1 and 4")
	}
	if options.MaxBytes < 1 || options.MaxBytes > feedAuditMaxBytes {
		return fmt.Errorf("-max-bytes must be between 1 and 33554432")
	}
	if options.MaxRecords < 1 || options.MaxRecords > feedAuditMaxRecords {
		return fmt.Errorf("-max-records must be between 1 and 100000")
	}
	return nil
}

func decodeFeedAuditInputs(data []byte, maxInputs int) ([]feedAuditInput, error) {
	var inputs []feedAuditInput
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&inputs); err != nil || inputs == nil {
		return nil, fmt.Errorf("audit input must be a JSON array of {url, scheme} records")
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("audit input must contain exactly one JSON array")
	}
	if len(inputs) > maxInputs {
		return nil, fmt.Errorf("audit input has too many source records")
	}
	for i := range inputs {
		inputs[i].URL = strings.TrimSpace(inputs[i].URL)
		inputs[i].Scheme = strings.ToLower(strings.TrimSpace(inputs[i].Scheme))
		if inputs[i].Scheme == "" {
			inputs[i].Scheme = "http"
		}
		if inputs[i].Scheme != "http" && inputs[i].Scheme != "socks4" && inputs[i].Scheme != "socks5" {
			return nil, fmt.Errorf("audit input source %d has an unsupported parser scheme", i)
		}
		if err := validateFeedAuditURL(inputs[i].URL); err != nil {
			return nil, fmt.Errorf("audit input source %d has an invalid URL", i)
		}
	}
	return inputs, nil
}

func validateFeedAuditURL(raw string) error {
	if raw == "" || len(raw) > 8192 || strings.Contains(raw, "#") {
		return errors.New("invalid URL")
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.User != nil || u.Fragment != "" || u.Opaque != "" || u.Hostname() == "" {
		return errors.New("invalid URL")
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil && endpoint.UnsafeIP(ip) {
		return errors.New("non-public source address")
	}
	if strings.TrimSuffix(strings.ToLower(u.Hostname()), ".") == "localhost" {
		return errors.New("non-public source hostname")
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return errors.New("unsupported URL scheme")
	}
	if port := u.Port(); port != "" {
		parsedPort, err := strconv.ParseUint(port, 10, 16)
		if err != nil || parsedPort == 0 {
			return errors.New("invalid URL port")
		}
	}
	return nil
}

func runFeedAudit(ctx context.Context, inputs []feedAuditInput, options feedAuditOptions, fetcher feedAuditFetcher) ([]feedAuditResult, error) {
	if err := validateFeedAuditOptions(options); err != nil {
		return nil, err
	}
	if len(inputs) > feedAuditMaxInputs {
		return nil, fmt.Errorf("audit input has too many source records")
	}
	// Validate the entire batch before starting any request, including callers
	// that use this helper directly rather than decodeFeedAuditInputs.
	validated := make([]feedAuditInput, len(inputs))
	copy(validated, inputs)
	for i := range validated {
		validated[i].URL = strings.TrimSpace(validated[i].URL)
		validated[i].Scheme = strings.ToLower(strings.TrimSpace(validated[i].Scheme))
		if validated[i].Scheme == "" {
			validated[i].Scheme = "http"
		}
		if (validated[i].Scheme != "http" && validated[i].Scheme != "socks4" && validated[i].Scheme != "socks5") || validateFeedAuditURL(validated[i].URL) != nil {
			return nil, fmt.Errorf("audit input source %d is invalid", i)
		}
	}
	if fetcher == nil {
		return nil, fmt.Errorf("audit fetcher is required")
	}
	results := make([]feedAuditResult, len(validated))
	jobs := make(chan int)
	workers := options.Concurrency
	if workers > len(validated) {
		workers = len(validated)
	}
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for index := range jobs {
				results[index] = auditOneFeed(ctx, index, validated[index], options, fetcher)
			}
		}()
	}
	for i := range validated {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	return results, nil
}

func auditOneFeed(parent context.Context, index int, input feedAuditInput, options feedAuditOptions, fetcher feedAuditFetcher) feedAuditResult {
	result := feedAuditResult{
		SourceID:   store.StableSourceID(input.URL, input.Scheme),
		InputIndex: index,
		Scheme:     input.Scheme,
		ByScheme:   make(map[string]int),
	}
	ctx, cancel := context.WithTimeout(parent, options.Timeout)
	defer cancel()
	unique := make(map[string]string)
	acceptedRecords := 0
	stats, err := fetcher.StreamMeasured(ctx, input.URL, func(r io.Reader) error {
		rejected, parseErr := endpoint.VisitFeedProxies(r, input.Scheme, func(proxy endpoint.Proxy) error {
			acceptedRecords++
			canonical := proxy.String()
			hash := sha256.Sum256([]byte(canonical))
			unique[hex.EncodeToString(hash[:])] = canonical
			if acceptedRecords >= options.MaxRecords {
				result.Truncated = true
				return errFeedAuditRecordLimit
			}
			return nil
		})
		result.Rejected = rejected
		if errors.Is(parseErr, errFeedAuditRecordLimit) {
			return nil
		}
		return parseErr
	})
	result.HTTPAttempts = stats.HTTPAttempts
	result.NetworkBytes = stats.NetworkBytes
	result.DurationMillis = stats.Duration.Milliseconds()
	result.CacheHit = stats.CacheHit
	result.ParsedUnique = len(unique)
	for _, canonical := range unique {
		colon := strings.Index(canonical, "://")
		if colon > 0 {
			result.ByScheme[canonical[:colon]]++
		}
	}
	if err != nil {
		result.ErrorCode, result.HTTPStatus = feedAuditErrorCode(err)
		return result
	}
	if result.Truncated {
		result.ErrorCode = "record_limit"
		return result
	}
	if result.ParsedUnique == 0 {
		result.ErrorCode = "empty_feed"
		return result
	}
	result.Eligible = true
	result.SetHash = canonicalSetHash(unique)
	return result
}

var errFeedAuditRecordLimit = errors.New("feed audit record limit reached")

func feedAuditErrorCode(err error) (string, int) {
	if errors.Is(err, source.ErrSourceTooLarge) {
		return "byte_limit", 0
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", 0
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", 0
	}
	var statusErr *source.HTTPStatusError
	if errors.As(err, &statusErr) {
		return "http_status", statusErr.StatusCode
	}
	return "fetch_or_parse_error", 0
}

func canonicalSetHash(unique map[string]string) string {
	canonical := make([]string, 0, len(unique))
	for _, value := range unique {
		canonical = append(canonical, value)
	}
	sort.Strings(canonical)
	h := sha256.New()
	for _, value := range canonical {
		_, _ = io.WriteString(h, value)
		_, _ = io.WriteString(h, "\n")
	}
	return hex.EncodeToString(h.Sum(nil))
}
