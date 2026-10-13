package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/router"
)

func main() {
	opts, err := parseOptions(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "proxy-router:", err)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := router.Run(ctx, opts); err != nil {
		fmt.Fprintln(os.Stderr, "proxy-router:", err)
		os.Exit(1)
	}
}

// parseOptions reads flags with environment defaults. Malformed numeric,
// duration, or boolean environment values and missing GOST credentials are
// startup errors instead of silently falling back to defaults or failing
// every refresh.
func parseOptions(args []string, getenv func(string) string) (router.SyncOptions, error) {
	env := &envReader{getenv: getenv}
	flags := flag.NewFlagSet("proxy-router", flag.ContinueOnError)
	apiURL := flags.String("api-url", env.str("FREEPROXYAPI_URL", ""), "FreeProxyAPI proxy query URL (public mode defaults to https://freeproxyapi.crawlora.net/proxies)")
	publicAPI := flags.Bool("public-api", env.boolean("FREEPROXYAPI_PUBLIC", false), "query the public FreeProxyAPI endpoint without a bearer token")
	tokenFile := flags.String("token-file", env.str("FREEPROXYAPI_TOKEN_FILE", "/internal-api/token"), "FreeProxyAPI bearer-token file")
	outputFile := flags.String("output", env.str("GOST_CONFIG_FILE", "/runtime/gost.json"), "GOST JSON configuration output")
	gostAPIURL := flags.String("gost-api-url", env.str("GOST_API_URL", "http://127.0.0.1:18080"), "GOST Web API URL")
	gostAPIAddress := flags.String("gost-api-address", env.str("GOST_API_ADDRESS", "127.0.0.1:18080"), "GOST Web API listen address written to the generated config")
	gostAPIUser := flags.String("gost-api-user", getenv("GOST_API_USERNAME"), "GOST Web API username")
	proxyUser := flags.String("proxy-user", getenv("GOST_PROXY_USERNAME"), "unified proxy username")
	// Passwords are deliberately not accepted as flag values: argv is visible
	// to every local user through ps and /proc. Use the environment variable
	// or a file path instead.
	gostAPIPasswordFile := flags.String("gost-api-password-file", env.str("GOST_API_PASSWORD_FILE", ""), "file containing the GOST Web API password (alternative to GOST_API_PASSWORD)")
	proxyPasswordFile := flags.String("proxy-password-file", env.str("GOST_PROXY_PASSWORD_FILE", ""), "file containing the unified proxy password (alternative to GOST_PROXY_PASSWORD)")
	countryField := flags.String("country-field", env.str("GOST_COUNTRY_FIELD", "entry"), "country field: entry or exit")
	countries := flags.String("countries", env.str("GOST_COUNTRIES", "US"), "comma-separated country codes to expose on dedicated ports")
	refresh := flags.Duration("refresh", env.duration("GOST_REFRESH_INTERVAL", time.Minute), "configuration refresh interval")
	timeout := flags.Duration("timeout", env.duration("GOST_REQUEST_TIMEOUT", 15*time.Second), "FreeProxyAPI request timeout")
	minRatio := flags.Int64("min-ratio-pct", env.int64("GOST_MIN_RATIO_PCT", 80), "minimum validated success ratio")
	maxLatency := flags.Int64("max-latency-ms", env.int64("GOST_MAX_LATENCY_MS", 0), "optional maximum validated latency")
	country := flags.String("country", getenv("FREEPROXYAPI_COUNTRY"), "validated proxy endpoint country filter")
	exitCountry := flags.String("exit-country", getenv("FREEPROXYAPI_EXIT_COUNTRY"), "validated proxy exit country filter")
	exitASN := flags.String("exit-asn", getenv("FREEPROXYAPI_EXIT_ASN"), "observed proxy exit ASN filter")
	asn := flags.String("asn", getenv("FREEPROXYAPI_ASN"), "validated proxy ASN filter")
	anonymity := flags.String("anonymity", getenv("FREEPROXYAPI_ANONYMITY"), "validated proxy anonymity filter")
	targetProfile := flags.String("target-profile", env.str("FREEPROXYAPI_TARGET_PROFILE", ""), "proxy target profile filter")
	geoMismatch := flags.Bool("geo-mismatch", env.boolean("FREEPROXYAPI_GEO_MISMATCH", false), "only use proxies whose endpoint and exit countries differ")
	excludeHosting := flags.Bool("exclude-hosting", env.boolean("FREEPROXYAPI_EXCLUDE_HOSTING", false), "skip proxies explicitly classified as hosting")
	stableExit := flags.Bool("stable-exit", env.boolean("FREEPROXYAPI_STABLE_EXIT", false), "only use proxies with a stable observed exit IP")
	minSuccessStreak := flags.Int64("min-success-streak", env.int64("FREEPROXYAPI_MIN_SUCCESS_STREAK", 0), "minimum consecutive successful probes")
	minReliability7dPct := flags.Int64("min-reliability-7d-pct", env.int64("FREEPROXYAPI_MIN_RELIABILITY_7D_PCT", 0), "minimum seven-day observed reliability percentage")
	minReliability7dSamples := flags.Int64("min-reliability-7d-samples", env.int64("FREEPROXYAPI_MIN_RELIABILITY_7D_SAMPLES", 0), "minimum seven-day probe sample count")
	excludeTampered := flags.Bool("exclude-tampered", env.boolean("FREEPROXYAPI_EXCLUDE_TAMPERED", true), "skip proxies whose latest echo tamper check found modified traffic")
	requireTamperChecked := flags.Bool("require-tamper-checked", env.boolean("FREEPROXYAPI_REQUIRE_TAMPER_CHECKED", false), "only use proxies with a fresh, explicitly clean echo tamper check")
	https := flags.Bool("https", env.boolean("FREEPROXYAPI_HTTPS", false), "only use proxies with a fresh HTTPS check pass")
	sessionsRaw := flags.String("sessions", env.str("GOST_SESSIONS", ""), "comma-separated named sticky sessions, each exposed on its own listener port")
	sessionTTL := flags.Duration("session-ttl", env.duration("GOST_SESSION_TTL", 10*time.Minute), "how long named sessions keep their pinned upstream")
	sessionBasePort := flags.Int("session-base-port", env.integer("GOST_SESSION_BASE_PORT", 10000), "first port used by named session listeners")
	sessionFailover := flags.Bool("session-failover", env.boolean("GOST_SESSION_FAILOVER", false), "allow named sessions to move to another upstream before their pin expires")
	sessionStableExit := flags.Bool("session-stable-exit", env.boolean("GOST_SESSION_STABLE_EXIT", false), "assign named sessions only to proxies with fresh repeated exit-IP observations")
	limit := flags.Int("limit", env.integer("FREEPROXYAPI_LIMIT", 1000), "maximum validated proxies fetched per refresh")
	if err := flags.Parse(args); err != nil {
		return router.SyncOptions{}, err
	}
	gostAPIPassword := env.secret("GOST_API_PASSWORD", *gostAPIPasswordFile)
	proxyPassword := env.secret("GOST_PROXY_PASSWORD", *proxyPasswordFile)
	if len(env.errs) > 0 {
		return router.SyncOptions{}, errors.Join(env.errs...)
	}
	sessions, err := router.ParseSessionIDs(*sessionsRaw)
	if err != nil {
		return router.SyncOptions{}, err
	}
	opts := router.SyncOptions{
		APIURL:                  *apiURL,
		PublicAPI:               *publicAPI,
		TokenFile:               *tokenFile,
		OutputFile:              *outputFile,
		GOSTAPIURL:              *gostAPIURL,
		GOSTAPIAddress:          *gostAPIAddress,
		GOSTAPIUsername:         *gostAPIUser,
		GOSTAPIPassword:         gostAPIPassword,
		ProxyUsername:           *proxyUser,
		ProxyPassword:           proxyPassword,
		CountryField:            *countryField,
		Countries:               splitCSV(*countries),
		RefreshInterval:         *refresh,
		RequestTimeout:          *timeout,
		MinimumRatioPct:         *minRatio,
		MaximumLatencyMs:        *maxLatency,
		Country:                 strings.TrimSpace(*country),
		ExitCountry:             strings.TrimSpace(*exitCountry),
		ExitASN:                 strings.TrimSpace(*exitASN),
		ASN:                     strings.TrimSpace(*asn),
		Anonymity:               strings.TrimSpace(*anonymity),
		TargetProfile:           strings.TrimSpace(*targetProfile),
		GeoMismatch:             *geoMismatch,
		ExcludeHosting:          *excludeHosting,
		StableExit:              *stableExit,
		MinSuccessStreak:        *minSuccessStreak,
		MinReliability7dPct:     *minReliability7dPct,
		MinReliability7dSamples: *minReliability7dSamples,
		ExcludeTampered:         *excludeTampered,
		RequireTamperChecked:    *requireTamperChecked,
		HTTPS:                   *https,
		Sessions:                sessions,
		SessionTTL:              *sessionTTL,
		SessionBasePort:         *sessionBasePort,
		SessionFailover:         *sessionFailover,
		SessionStableExit:       *sessionStableExit,
		Limit:                   *limit,
	}
	return opts, validateOptions(opts)
}

// validateOptions checks settings every sync depends on. The four GOST
// credentials are required in both private and public API modes because the
// generated config always authenticates the proxy listeners and Web API.
func validateOptions(opts router.SyncOptions) error {
	var errs []error
	for _, required := range []struct{ value, name string }{
		{opts.GOSTAPIUsername, "GOST_API_USERNAME (-gost-api-user)"},
		{opts.GOSTAPIPassword, "GOST_API_PASSWORD (or GOST_API_PASSWORD_FILE / -gost-api-password-file)"},
		{opts.ProxyUsername, "GOST_PROXY_USERNAME (-proxy-user)"},
		{opts.ProxyPassword, "GOST_PROXY_PASSWORD (or GOST_PROXY_PASSWORD_FILE / -proxy-password-file)"},
	} {
		if required.value == "" {
			errs = append(errs, fmt.Errorf("%s is required", required.name))
		}
	}
	if opts.RefreshInterval <= 0 {
		errs = append(errs, fmt.Errorf("refresh interval must be positive"))
	}
	if opts.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("request timeout must be positive"))
	}
	for _, value := range []struct {
		name  string
		value int64
	}{
		{name: "minimum success streak", value: opts.MinSuccessStreak},
		{name: "minimum seven-day reliability sample count", value: opts.MinReliability7dSamples},
	} {
		if value.value < 0 {
			errs = append(errs, fmt.Errorf("%s must be non-negative", value.name))
		}
	}
	if opts.MinReliability7dPct < 0 || opts.MinReliability7dPct > 100 {
		errs = append(errs, fmt.Errorf("minimum seven-day reliability percentage must be from 0 to 100"))
	}
	if err := router.ValidateSessionOptions(opts.Sessions, opts.SessionTTL, opts.SessionBasePort, opts.Countries, opts.GOSTAPIAddress); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

func splitCSV(raw string) []string {
	var values []string
	for _, value := range strings.Split(raw, ",") {
		if value = strings.TrimSpace(value); value != "" {
			values = append(values, value)
		}
	}
	return values
}

type envReader struct {
	getenv func(string) string
	errs   []error
}

func (e *envReader) str(name, fallback string) string {
	if value := strings.TrimSpace(e.getenv(name)); value != "" {
		return value
	}
	return fallback
}

// secret returns a credential from the named environment variable or from
// file (the <name>_FILE variable or its flag). The raw environment value is
// used as-is; a file's trailing newline is stripped. Supplying both sources
// is an error so a stale variable cannot silently override a mounted secret.
func (e *envReader) secret(name, file string) string {
	value := e.getenv(name)
	if file == "" {
		return value
	}
	if value != "" {
		e.errs = append(e.errs, fmt.Errorf("set only one of %s and %s_FILE", name, name))
		return ""
	}
	content, err := os.ReadFile(file)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("read %s_FILE: %w", name, err))
		return ""
	}
	return strings.TrimRight(string(content), "\r\n")
}

func (e *envReader) duration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(e.getenv(name))
	if value == "" {
		return fallback
	}
	duration, err := time.ParseDuration(value)
	if err != nil || duration <= 0 {
		e.errs = append(e.errs, fmt.Errorf("%s must be a positive duration such as 1m, got %q", name, value))
		return fallback
	}
	return duration
}

func (e *envReader) int64(name string, fallback int64) int64 {
	value := strings.TrimSpace(e.getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s must be an integer, got %q", name, value))
		return fallback
	}
	return n
}

func (e *envReader) integer(name string, fallback int) int {
	value := strings.TrimSpace(e.getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		e.errs = append(e.errs, fmt.Errorf("%s must be an integer, got %q", name, value))
		return fallback
	}
	return n
}

func (e *envReader) boolean(name string, fallback bool) bool {
	value := strings.TrimSpace(e.getenv(name))
	switch strings.ToLower(value) {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	default:
		e.errs = append(e.errs, fmt.Errorf("%s must be a boolean (true/false), got %q", name, value))
		return fallback
	}
}
