package monitor

import (
	"encoding/json"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const (
	defaultNamespace       = "freeproxyapi:v1"
	defaultFetchInterval   = 15 * time.Minute
	defaultRequestTimeout  = 5 * time.Second
	defaultValidatedRetest = 6 * time.Hour
	defaultFailedRetest    = 48 * time.Hour
	maxConsecutiveFails    = 10
	defaultLeaseTTL        = 30 * time.Second
	defaultSourceMaxBytes  = int64(1 << 20)
	// defaultMaxRecordsPerSource sits above any legitimate feed (the largest
	// curated lists hold tens of thousands of endpoints) and well below what the
	// 32 MiB source_max_bytes ceiling admits (about 1.5M short records), so one
	// compromised list cannot flood Redis.
	defaultMaxRecordsPerSource = 250_000
	maxMaxRecordsPerSource     = 5_000_000
	defaultWorkers             = 2
	defaultGlobalPerMinute     = 30
	maxGlobalPerMinute         = 120000
	maxWorkersPerReplica       = 768
	defaultListenAddr          = ":8080"
	defaultProbeTargetMode     = "standard"

	defaultProbeConnectTimeout = 3 * time.Second
	defaultRetestJitterPct     = 10
	maxRetestJitterPct         = 50
	defaultEvictedBackoffBase  = 2 * time.Hour
	defaultEvictedBackoffMax   = 168 * time.Hour

	// defaultListedFailureRetest re-probes a failed proxy that is still listed
	// quickly, so a dead proxy leaves /proxies within minutes, not hours.
	defaultListedFailureRetest = 5 * time.Minute
	// defaultSampleRetest spaces follow-up probes for candidates that have
	// not yet collected enough samples to be listed.
	defaultSampleRetest = time.Minute
	// defaultMinSamplesForListing is the rolling-history length required
	// before a candidate can enter the validated set.
	defaultMinSamplesForListing = 3
	// maxMinSamplesForListing matches the results10 history window.
	maxMinSamplesForListing = 10
)

type Config struct {
	RedisURL                 string
	Namespace                string
	Sources                  []string
	SourceSpecs              []SourceSpec
	DefaultProxyScheme       string
	NetworkValidationEnabled bool
	ProbeTarget              string
	ProbeTargetMode          string
	// ProbeExpectedBody, when non-empty, must equal the standard-mode probe
	// response body after trailing whitespace is trimmed. Ignored in echo mode.
	ProbeExpectedBody           string
	ListenAddr                  string
	FetchInterval               time.Duration
	RequestTimeout              time.Duration
	SourceRequestTimeout        time.Duration
	ProbeConnectTimeout         time.Duration
	RetestJitterPct             int
	EvictedBackoffBase          time.Duration
	EvictedBackoffMax           time.Duration
	ValidatedRetestInterval     time.Duration
	FailedRetestInterval        time.Duration
	ListedFailureRetestInterval time.Duration
	SampleRetestInterval        time.Duration
	MinSamplesForListing        int
	MaxConsecutiveFailures      int
	DiscardFailedCandidates     bool
	RequeuePendingOnStart       bool
	GeoIPDBPath                 string
	GeoIPASNDBPath              string
	AnonymityCheckURL           string
	InternalAPITokenFile        string
	PrometheusURL               string
	// MaxRecordsPerSource caps how many records one feed may contribute per
	// refresh; the rest are ignored. MaxCandidates, when positive, stops new
	// candidates from being added once the candidate set reaches it.
	MaxRecordsPerSource int
	MaxCandidates       int64
	// CloudflareWebAnalytics allows Cloudflare's injected Web Analytics beacon
	// in the pages' Content-Security-Policy.
	CloudflareWebAnalytics bool
	// AdminListenAddr, when set, serves /metrics, /report, and the internal
	// API on a separate listener instead of ListenAddr. Empty keeps the legacy
	// layout where everything shares ListenAddr.
	AdminListenAddr string
	// PublicBaseURL is the public origin of this deployment, used for the
	// canonical, Open Graph, and code-sample URLs in the embedded pages.
	PublicBaseURL string
	// AnalyticsMeasurementID enables Google Analytics on the embedded pages
	// when set. Empty by default: nothing third-party is loaded.
	AnalyticsMeasurementID  string
	TrustedProxyCIDRs       []netip.Prefix
	LeaseTTL                time.Duration
	SourceMaxBytes          int64
	Workers                 int
	GlobalRequestsPerMinute int

	// HTTPSProbeTarget, when non-empty, enables a sampled HTTPS-capability
	// check through proxies after a successful standard probe.
	HTTPSProbeTarget       string
	HTTPSProbeExpectedBody string
	// HTTPSProbeEvery samples roughly one in N successful standard probes.
	HTTPSProbeEvery int
	// ControlProbe* configure the per-replica direct fetch of probe_target
	// that pauses workers while the probe origin itself is failing.
	ControlProbeEnabled          bool
	ControlProbeInterval         time.Duration
	ControlProbeFailureThreshold int
	// ClassificationMaxAge ages out anonymity/exit and HTTPS measurements in
	// /proxies results and filters.
	ClassificationMaxAge time.Duration
	// StatsLeaderEnabled makes one replica (holding a Redis lease for
	// StatsLeaderTTL) scan the validated set and share the aggregate for
	// StatsAggregateTTL; other replicas read it instead of scanning.
	StatsLeaderEnabled bool
	StatsLeaderTTL     time.Duration
	StatsAggregateTTL  time.Duration
	// ProbePermitChunk is how many global-budget permits a replica reserves
	// from Redis per refill; 0 selects max(10, workers/4).
	ProbePermitChunk int
	// RedisPoolSize caps the worker Redis client's connections per replica
	// (claims, completions, permits, refreshes, stats). LoadConfig resolves 0
	// to autoRedisPoolSize(Workers).
	RedisPoolSize int
	// RedisAPIPoolSize caps the separate Redis client that serves HTTP reads
	// (/proxies, the internal API, /stats, /report, /dashboard, /readyz), so
	// worker traffic cannot starve them. LoadConfig resolves 0 to 8.
	RedisAPIPoolSize int
}

const (
	// Worker pool auto-sizing: one connection per 32 workers, bounded. Workers
	// spend almost all of their time in probes, not Redis round trips, and
	// production Redis is single-threaded, so more connections add Redis
	// event-loop latency rather than throughput.
	autoRedisPoolWorkersPerConn = 32
	minAutoRedisPoolSize        = 10
	maxAutoRedisPoolSize        = 32
	maxRedisPoolSize            = 256
	defaultRedisAPIPoolSize     = 8
	maxRedisAPIPoolSize         = 64
)

// autoRedisPoolSize is the worker pool size used when redis_pool_size is 0.
func autoRedisPoolSize(workers int) int {
	return min(max(workers/autoRedisPoolWorkersPerConn, minAutoRedisPoolSize), maxAutoRedisPoolSize)
}

// workerRedisPoolSize returns the resolved worker pool size, also for Config
// values built without LoadConfig.
func (c Config) workerRedisPoolSize() int {
	if c.RedisPoolSize > 0 {
		return c.RedisPoolSize
	}
	return autoRedisPoolSize(c.Workers)
}

// apiRedisPoolSize returns the resolved HTTP read pool size.
func (c Config) apiRedisPoolSize() int {
	if c.RedisAPIPoolSize > 0 {
		return c.RedisAPIPoolSize
	}
	return defaultRedisAPIPoolSize
}

const (
	defaultHTTPSProbeEvery              = 4
	maxHTTPSProbeEvery                  = 1000
	defaultControlProbeInterval         = time.Minute
	defaultControlProbeFailureThreshold = 3
	maxControlProbeFailureThreshold     = 100
	defaultClassificationMaxAge         = 24 * time.Hour
)

// SourceSpec is one inventory source. The optional "scheme@" prefix
// (carried over from the legacy monitor) declares the default proxy scheme for
// bare host:port records inside that feed, e.g. "socks5@https://…/socks5.txt".
type SourceSpec struct {
	URL    string
	Scheme string
}

func (s SourceSpec) defaultScheme(fallback string) string {
	if s.Scheme != "" {
		return s.Scheme
	}
	return fallback
}

func parseSourceSpec(raw string) (SourceSpec, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return SourceSpec{}, fmt.Errorf("source spec is empty")
	}
	if at := strings.Index(trimmed, "@"); at > 0 {
		hint := strings.ToLower(strings.TrimSpace(trimmed[:at]))
		if endpoint.IsSupportedScheme(hint) && !strings.Contains(trimmed[:at], "://") {
			return SourceSpec{URL: strings.TrimSpace(trimmed[at+1:]), Scheme: hint}, nil
		}
	}
	return SourceSpec{URL: trimmed}, nil
}

type fileConfig struct {
	RedisURL                 string   `json:"redis_url"`
	Namespace                string   `json:"namespace"`
	Sources                  []string `json:"sources"`
	DefaultProxyScheme       string   `json:"default_proxy_scheme"`
	NetworkValidationEnabled bool     `json:"network_validation_enabled"`
	ProbeTarget              string   `json:"probe_target"`
	ProbeTargetMode          string   `json:"probe_target_mode"`
	ProbeExpectedBody        string   `json:"probe_expected_body"`
	ListenAddr               string   `json:"listen_addr"`
	FetchInterval            string   `json:"fetch_interval"`
	RequestTimeout           string   `json:"request_timeout"`
	SourceRequestTimeout     string   `json:"source_request_timeout"`
	ProbeConnectTimeout      string   `json:"probe_connect_timeout"`
	RetestJitterPct          *int     `json:"retest_jitter_pct"`
	EvictedBackoffBase       string   `json:"evicted_backoff_base"`
	EvictedBackoffMax        string   `json:"evicted_backoff_max"`
	ValidatedRetestInterval  string   `json:"validated_retest_interval"`
	FailedRetestInterval     string   `json:"failed_retest_interval"`
	ListedFailureRetest      string   `json:"listed_failure_retest_interval"`
	SampleRetestInterval     string   `json:"sample_retest_interval"`
	MinSamplesForListing     int      `json:"min_samples_for_listing"`
	MaxConsecutiveFailures   int      `json:"max_consecutive_failures"`
	DiscardFailedCandidates  bool     `json:"discard_failed_candidates"`
	RequeuePendingOnStart    bool     `json:"requeue_pending_on_start"`
	GeoIPDBPath              string   `json:"geoip_db_path"`
	GeoIPASNDBPath           string   `json:"geoip_asn_db_path"`
	AnonymityCheckURL        string   `json:"anonymity_check_url"`
	InternalAPITokenFile     string   `json:"internal_api_token_file"`
	PrometheusURL            string   `json:"prometheus_url"`
	AdminListenAddr          string   `json:"admin_listen_addr"`
	CloudflareWebAnalytics   bool     `json:"cloudflare_web_analytics"`
	MaxRecordsPerSource      int      `json:"max_records_per_source"`
	MaxCandidates            int64    `json:"max_candidates"`
	PublicBaseURL            string   `json:"public_base_url"`
	AnalyticsMeasurementID   string   `json:"analytics_measurement_id"`
	TrustedProxyCIDRs        []string `json:"trusted_proxy_cidrs"`
	LeaseTTL                 string   `json:"lease_ttl"`
	SourceMaxBytes           int64    `json:"source_max_bytes"`
	Workers                  int      `json:"workers"`
	GlobalRequestsPerMinute  int      `json:"global_requests_per_minute"`
	StatsLeaderEnabled       *bool    `json:"stats_leader_enabled"`
	StatsLeaderTTL           string   `json:"stats_leader_ttl"`
	StatsAggregateTTL        string   `json:"stats_aggregate_ttl"`
	ProbePermitChunk         int      `json:"probe_permit_chunk"`
	RedisPoolSize            int      `json:"redis_pool_size"`
	RedisAPIPoolSize         int      `json:"redis_api_pool_size"`

	HTTPSProbeTarget             string `json:"https_probe_target"`
	HTTPSProbeExpectedBody       string `json:"https_probe_expected_body"`
	HTTPSProbeEvery              int    `json:"https_probe_every"`
	ControlProbeEnabled          *bool  `json:"control_probe_enabled"`
	ControlProbeInterval         string `json:"control_probe_interval"`
	ControlProbeFailureThreshold int    `json:"control_probe_failure_threshold"`
	ClassificationMaxAge         string `json:"classification_max_age"`
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read monitor config: %w", err)
	}
	var file fileConfig
	if err := json.Unmarshal(data, &file); err != nil {
		return Config{}, fmt.Errorf("parse monitor config: %w", err)
	}
	config := Config{
		RedisURL:                 strings.TrimSpace(file.RedisURL),
		Namespace:                strings.TrimSpace(file.Namespace),
		Sources:                  append([]string(nil), file.Sources...),
		DefaultProxyScheme:       strings.ToLower(strings.TrimSpace(file.DefaultProxyScheme)),
		NetworkValidationEnabled: file.NetworkValidationEnabled,
		ProbeTarget:              strings.TrimSpace(file.ProbeTarget),
		ProbeTargetMode:          strings.ToLower(strings.TrimSpace(file.ProbeTargetMode)),
		ListenAddr:               strings.TrimSpace(file.ListenAddr),
		GeoIPDBPath:              strings.TrimSpace(file.GeoIPDBPath),
		GeoIPASNDBPath:           strings.TrimSpace(file.GeoIPASNDBPath),
		AnonymityCheckURL:        strings.TrimSpace(file.AnonymityCheckURL),
		InternalAPITokenFile:     strings.TrimSpace(file.InternalAPITokenFile),
		PrometheusURL:            strings.TrimRight(strings.TrimSpace(file.PrometheusURL), "/"),
		SourceMaxBytes:           file.SourceMaxBytes,
		Workers:                  file.Workers,
		GlobalRequestsPerMinute:  file.GlobalRequestsPerMinute,
		DiscardFailedCandidates:  file.DiscardFailedCandidates,
		RequeuePendingOnStart:    file.RequeuePendingOnStart,
	}
	applyStringEnv("FREEPROXYAPI_PROBE_TARGET", &config.ProbeTarget)
	applyStringEnv("FREEPROXYAPI_PROBE_TARGET_MODE", &config.ProbeTargetMode)
	config.ProbeExpectedBody = strings.TrimRight(file.ProbeExpectedBody, " \t\r\n")
	applyStringEnv("FREEPROXYAPI_PROBE_EXPECTED_BODY", &config.ProbeExpectedBody)
	config.ProbeTargetMode = strings.ToLower(strings.TrimSpace(config.ProbeTargetMode))
	if err := applyIntEnv("FREEPROXYAPI_WORKERS", &config.Workers); err != nil {
		return Config{}, err
	}
	if err := applyIntEnv("FREEPROXYAPI_GLOBAL_REQUESTS_PER_MINUTE", &config.GlobalRequestsPerMinute); err != nil {
		return Config{}, err
	}
	// The environment override keeps a Redis password (redis://:secret@host)
	// out of the config file and its ConfigMap.
	applyStringEnv("FREEPROXYAPI_REDIS_URL", &config.RedisURL)
	if config.RedisURL == "" {
		config.RedisURL = "redis://127.0.0.1:6379/0"
	}
	if config.Namespace == "" {
		config.Namespace = defaultNamespace
	}
	if config.PrometheusURL != "" {
		parsed, err := url.Parse(config.PrometheusURL)
		if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return Config{}, fmt.Errorf("prometheus_url must be an http(s) URL without credentials, query, or fragment")
		}
	}
	config.MaxRecordsPerSource = file.MaxRecordsPerSource
	if config.MaxRecordsPerSource == 0 {
		config.MaxRecordsPerSource = defaultMaxRecordsPerSource
	}
	if config.MaxRecordsPerSource < 0 || config.MaxRecordsPerSource > maxMaxRecordsPerSource {
		return Config{}, fmt.Errorf("max_records_per_source must be between 1 and %d (0 uses the default %d)", maxMaxRecordsPerSource, defaultMaxRecordsPerSource)
	}
	config.MaxCandidates = file.MaxCandidates
	if config.MaxCandidates < 0 {
		return Config{}, fmt.Errorf("max_candidates must not be negative (0 disables the cap)")
	}
	config.CloudflareWebAnalytics = file.CloudflareWebAnalytics
	config.AdminListenAddr = strings.TrimSpace(file.AdminListenAddr)
	applyStringEnv("FREEPROXYAPI_ADMIN_LISTEN_ADDR", &config.AdminListenAddr)
	config.PublicBaseURL = file.PublicBaseURL
	applyStringEnv("FREEPROXYAPI_PUBLIC_BASE_URL", &config.PublicBaseURL)
	if config.PublicBaseURL, err = normalizePublicBaseURL(config.PublicBaseURL); err != nil {
		return Config{}, err
	}
	config.AnalyticsMeasurementID = file.AnalyticsMeasurementID
	applyStringEnv("FREEPROXYAPI_ANALYTICS_MEASUREMENT_ID", &config.AnalyticsMeasurementID)
	if config.AnalyticsMeasurementID, err = validateAnalyticsMeasurementID(config.AnalyticsMeasurementID); err != nil {
		return Config{}, err
	}
	for i, raw := range file.TrustedProxyCIDRs {
		prefix, err := netip.ParsePrefix(strings.TrimSpace(raw))
		if err != nil {
			return Config{}, fmt.Errorf("trusted_proxy_cidrs[%d] must be a valid IP CIDR", i)
		}
		config.TrustedProxyCIDRs = append(config.TrustedProxyCIDRs, prefix.Masked())
	}
	if len(config.Sources) == 0 {
		return Config{}, fmt.Errorf("at least one source is required")
	}
	config.SourceSpecs = make([]SourceSpec, 0, len(config.Sources))
	for _, raw := range config.Sources {
		spec, err := parseSourceSpec(raw)
		if err != nil {
			return Config{}, fmt.Errorf("source %d: %w", len(config.SourceSpecs)+1, err)
		}
		config.SourceSpecs = append(config.SourceSpecs, spec)
	}
	if config.DefaultProxyScheme == "" {
		config.DefaultProxyScheme = "http"
	}
	if config.ProbeTargetMode == "" {
		config.ProbeTargetMode = defaultProbeTargetMode
	}
	if config.ProbeTargetMode != "standard" && config.ProbeTargetMode != "echo" {
		return Config{}, fmt.Errorf("probe_target_mode must be standard or echo")
	}
	if config.ListenAddr == "" {
		config.ListenAddr = defaultListenAddr
	}
	if config.AdminListenAddr != "" {
		if _, port, splitErr := net.SplitHostPort(config.AdminListenAddr); splitErr != nil || port == "" {
			return Config{}, fmt.Errorf("admin_listen_addr must be host:port, for example 127.0.0.1:9090")
		}
		if config.AdminListenAddr == config.ListenAddr || config.ListenAddr == "-" {
			return Config{}, fmt.Errorf("admin_listen_addr must differ from listen_addr and requires the public listener to be enabled")
		}
	}
	if config.FetchInterval, err = durationOrDefault(file.FetchInterval, defaultFetchInterval); err != nil {
		return Config{}, fmt.Errorf("fetch_interval: %w", err)
	}
	if config.RequestTimeout, err = durationOrDefault(file.RequestTimeout, defaultRequestTimeout); err != nil {
		return Config{}, fmt.Errorf("request_timeout: %w", err)
	}
	if config.SourceRequestTimeout, err = durationOrDefault(file.SourceRequestTimeout, config.RequestTimeout); err != nil {
		return Config{}, fmt.Errorf("source_request_timeout: %w", err)
	}
	connectDefault := defaultProbeConnectTimeout
	if connectDefault > config.RequestTimeout {
		connectDefault = config.RequestTimeout
	}
	if config.ProbeConnectTimeout, err = durationOrDefault(file.ProbeConnectTimeout, connectDefault); err != nil {
		return Config{}, fmt.Errorf("probe_connect_timeout: %w", err)
	}
	if config.ProbeConnectTimeout > config.RequestTimeout {
		return Config{}, fmt.Errorf("probe_connect_timeout must not exceed request_timeout")
	}
	config.RetestJitterPct = defaultRetestJitterPct
	if file.RetestJitterPct != nil {
		config.RetestJitterPct = *file.RetestJitterPct
	}
	if config.RetestJitterPct < 0 || config.RetestJitterPct > maxRetestJitterPct {
		return Config{}, fmt.Errorf("retest_jitter_pct must be between 0 and %d", maxRetestJitterPct)
	}
	if config.EvictedBackoffBase, err = durationOrDefault(file.EvictedBackoffBase, defaultEvictedBackoffBase); err != nil {
		return Config{}, fmt.Errorf("evicted_backoff_base: %w", err)
	}
	if config.EvictedBackoffMax, err = durationOrDefault(file.EvictedBackoffMax, defaultEvictedBackoffMax); err != nil {
		return Config{}, fmt.Errorf("evicted_backoff_max: %w", err)
	}
	if config.EvictedBackoffMax < config.EvictedBackoffBase {
		return Config{}, fmt.Errorf("evicted_backoff_max must not be less than evicted_backoff_base")
	}
	if config.ValidatedRetestInterval, err = durationOrDefault(file.ValidatedRetestInterval, defaultValidatedRetest); err != nil {
		return Config{}, fmt.Errorf("validated_retest_interval: %w", err)
	}
	if config.FailedRetestInterval, err = durationOrDefault(file.FailedRetestInterval, defaultFailedRetest); err != nil {
		return Config{}, fmt.Errorf("failed_retest_interval: %w", err)
	}
	if config.ListedFailureRetestInterval, err = durationOrDefault(file.ListedFailureRetest, defaultListedFailureRetest); err != nil {
		return Config{}, fmt.Errorf("listed_failure_retest_interval: %w", err)
	}
	if config.SampleRetestInterval, err = durationOrDefault(file.SampleRetestInterval, defaultSampleRetest); err != nil {
		return Config{}, fmt.Errorf("sample_retest_interval: %w", err)
	}
	if file.MinSamplesForListing == 0 {
		config.MinSamplesForListing = defaultMinSamplesForListing
	} else {
		config.MinSamplesForListing = file.MinSamplesForListing
	}
	if config.MinSamplesForListing < 1 || config.MinSamplesForListing > maxMinSamplesForListing {
		return Config{}, fmt.Errorf("min_samples_for_listing must be between 1 and %d", maxMinSamplesForListing)
	}
	if file.MaxConsecutiveFailures == 0 {
		config.MaxConsecutiveFailures = maxConsecutiveFails
	} else {
		config.MaxConsecutiveFailures = file.MaxConsecutiveFailures
	}
	if config.MaxConsecutiveFailures < 0 || config.MaxConsecutiveFailures > 100 {
		return Config{}, fmt.Errorf("max_consecutive_failures must be between 0 and 100")
	}
	if config.LeaseTTL, err = durationOrDefault(file.LeaseTTL, defaultLeaseTTL); err != nil {
		return Config{}, fmt.Errorf("lease_ttl: %w", err)
	}
	if config.SourceMaxBytes <= 0 {
		config.SourceMaxBytes = defaultSourceMaxBytes
	}
	if config.SourceMaxBytes > 32<<20 {
		return Config{}, fmt.Errorf("source_max_bytes exceeds 32 MiB safety limit")
	}
	if config.Workers <= 0 {
		config.Workers = defaultWorkers
	}
	if config.Workers > maxWorkersPerReplica {
		return Config{}, fmt.Errorf("workers exceeds %d-per-replica safety limit", maxWorkersPerReplica)
	}
	if config.GlobalRequestsPerMinute <= 0 {
		config.GlobalRequestsPerMinute = defaultGlobalPerMinute
	}
	if config.GlobalRequestsPerMinute > maxGlobalPerMinute {
		return Config{}, fmt.Errorf("global_requests_per_minute exceeds %d safety limit", maxGlobalPerMinute)
	}
	if config.NetworkValidationEnabled && config.ProbeTarget == "" {
		return Config{}, fmt.Errorf("probe_target is required when network validation is enabled")
	}
	config.StatsLeaderEnabled = true
	if file.StatsLeaderEnabled != nil {
		config.StatsLeaderEnabled = *file.StatsLeaderEnabled
	}
	if config.StatsLeaderTTL, err = durationOrDefault(file.StatsLeaderTTL, defaultStatsLeaderTTL); err != nil {
		return Config{}, fmt.Errorf("stats_leader_ttl: %w", err)
	}
	if config.StatsLeaderTTL <= statsPublishInterval {
		return Config{}, fmt.Errorf("stats_leader_ttl must exceed the %s stats publish interval", statsPublishInterval)
	}
	if config.StatsAggregateTTL, err = durationOrDefault(file.StatsAggregateTTL, defaultStatsAggregateTTL); err != nil {
		return Config{}, fmt.Errorf("stats_aggregate_ttl: %w", err)
	}
	if config.StatsAggregateTTL < statsAggregateMaxAge {
		return Config{}, fmt.Errorf("stats_aggregate_ttl must be at least %s", statsAggregateMaxAge)
	}
	config.ProbePermitChunk = file.ProbePermitChunk
	if config.ProbePermitChunk < 0 || config.ProbePermitChunk > maxGlobalPerMinute {
		return Config{}, fmt.Errorf("probe_permit_chunk must be between 0 and %d", maxGlobalPerMinute)
	}
	if file.RedisPoolSize < 0 || file.RedisPoolSize > maxRedisPoolSize {
		return Config{}, fmt.Errorf("redis_pool_size must be between 0 and %d", maxRedisPoolSize)
	}
	config.RedisPoolSize = file.RedisPoolSize
	config.RedisPoolSize = config.workerRedisPoolSize()
	if file.RedisAPIPoolSize < 0 || file.RedisAPIPoolSize > maxRedisAPIPoolSize {
		return Config{}, fmt.Errorf("redis_api_pool_size must be between 0 and %d", maxRedisAPIPoolSize)
	}
	config.RedisAPIPoolSize = file.RedisAPIPoolSize
	config.RedisAPIPoolSize = config.apiRedisPoolSize()
	if err := applyAccuracyConfig(&config, file); err != nil {
		return Config{}, err
	}
	return config, nil
}

// applyAccuracyConfig parses the HTTPS-sampling, control-probe, and
// classification-freshness settings.
func applyAccuracyConfig(config *Config, file fileConfig) error {
	var err error
	config.HTTPSProbeTarget = strings.TrimSpace(file.HTTPSProbeTarget)
	config.HTTPSProbeExpectedBody = strings.TrimRight(file.HTTPSProbeExpectedBody, " \t\r\n")
	if config.HTTPSProbeTarget != "" {
		parsed, perr := url.Parse(config.HTTPSProbeTarget)
		if perr != nil || parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return fmt.Errorf("https_probe_target must be an https URL without credentials")
		}
	}
	config.HTTPSProbeEvery = file.HTTPSProbeEvery
	if config.HTTPSProbeEvery == 0 {
		config.HTTPSProbeEvery = defaultHTTPSProbeEvery
	}
	if config.HTTPSProbeEvery < 1 || config.HTTPSProbeEvery > maxHTTPSProbeEvery {
		return fmt.Errorf("https_probe_every must be between 1 and %d", maxHTTPSProbeEvery)
	}
	config.ControlProbeEnabled = file.ControlProbeEnabled == nil || *file.ControlProbeEnabled
	if config.ControlProbeInterval, err = durationOrDefault(file.ControlProbeInterval, defaultControlProbeInterval); err != nil {
		return fmt.Errorf("control_probe_interval: %w", err)
	}
	if config.ControlProbeInterval < time.Second {
		return fmt.Errorf("control_probe_interval must be at least 1s")
	}
	config.ControlProbeFailureThreshold = file.ControlProbeFailureThreshold
	if config.ControlProbeFailureThreshold == 0 {
		config.ControlProbeFailureThreshold = defaultControlProbeFailureThreshold
	}
	if config.ControlProbeFailureThreshold < 1 || config.ControlProbeFailureThreshold > maxControlProbeFailureThreshold {
		return fmt.Errorf("control_probe_failure_threshold must be between 1 and %d", maxControlProbeFailureThreshold)
	}
	if config.ClassificationMaxAge, err = durationOrDefault(file.ClassificationMaxAge, defaultClassificationMaxAge); err != nil {
		return fmt.Errorf("classification_max_age: %w", err)
	}
	return nil
}

// ClassificationEchoURLs returns the echo endpoints used to learn the
// monitor's direct-egress address. In echo mode the probe target is the
// primary endpoint because it is also the endpoint used for classification.
func (c Config) ClassificationEchoURLs() []string {
	var out []string
	seen := map[string]bool{}
	appendURL := func(raw string) {
		u := strings.TrimSpace(raw)
		if u == "" || seen[u] {
			return
		}
		seen[u] = true
		out = append(out, u)
	}
	if c.ProbeTargetMode == "echo" {
		appendURL(c.ProbeTarget)
	}
	for _, u := range c.EchoURLs() {
		appendURL(u)
	}
	return out
}

func applyStringEnv(name string, target *string) {
	raw, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return
	}
	*target = strings.TrimSpace(raw)
}

func applyIntEnv(name string, target *int) error {
	raw, ok := os.LookupEnv(name)
	if !ok || strings.TrimSpace(raw) == "" {
		return nil
	}
	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("%s must be an integer", name)
	}
	*target = value
	return nil
}

func durationOrDefault(raw string, fallback time.Duration) (time.Duration, error) {
	if strings.TrimSpace(raw) == "" {
		return fallback, nil
	}
	duration, err := time.ParseDuration(raw)
	if err != nil || duration <= 0 {
		return 0, fmt.Errorf("must be a positive Go duration")
	}
	return duration, nil
}

// RetestPolicy returns the tiered rescheduling policy for probe outcomes.
func (c Config) RetestPolicy() store.RetestPolicy {
	policy := store.RetestPolicy{
		ValidatedAfter:          c.ValidatedRetestInterval,
		FailedAfter:             c.FailedRetestInterval,
		MaxConsecutiveFailures:  c.MaxConsecutiveFailures,
		DiscardFailedCandidates: c.DiscardFailedCandidates,
		JitterPct:               c.RetestJitterPct,
		EvictedBackoffBase:      c.EvictedBackoffBase,
		EvictedBackoffMax:       c.EvictedBackoffMax,
		ListedFailureAfter:      c.ListedFailureRetestInterval,
		SampleRetestAfter:       c.SampleRetestInterval,
		MinSamplesForListing:    c.MinSamplesForListing,
	}
	return policy
}

// EchoURLs splits anonymity_check_url into its ordered candidates. A
// comma-separated list provides automatic failover across public echo
// services; whitespace is tolerated around each entry.
func (c Config) EchoURLs() []string {
	var out []string
	seen := map[string]bool{}
	for _, raw := range strings.Split(c.AnonymityCheckURL, ",") {
		u := strings.TrimSpace(raw)
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		out = append(out, u)
	}
	return out
}
