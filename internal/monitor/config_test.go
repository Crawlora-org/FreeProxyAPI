package monitor

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "monitor.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const validConfig = `{
  "redis_url": "redis://127.0.0.1:6379/0",
  "sources": ["file:///data/proxies.txt"],
  "network_validation_enabled": false
}`

func TestLoadConfigAppliesSafetyDefaults(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.ListenAddr != ":8080" {
		t.Errorf("ListenAddr = %q, want :8080", config.ListenAddr)
	}
	if config.Namespace != defaultNamespace {
		t.Errorf("Namespace = %q", config.Namespace)
	}
	if config.FetchInterval != 15*time.Minute || config.LeaseTTL != 30*time.Second ||
		config.ValidatedRetestInterval != 6*time.Hour || config.FailedRetestInterval != 48*time.Hour ||
		config.MaxConsecutiveFailures != 10 || config.RequestTimeout != 5*time.Second ||
		config.SourceRequestTimeout != 5*time.Second {
		t.Errorf("duration defaults wrong: %+v", config)
	}
	if config.Workers != 2 || config.GlobalRequestsPerMinute != 30 || config.SourceMaxBytes != 1<<20 || config.ProbeTargetMode != defaultProbeTargetMode {
		t.Errorf("numeric defaults wrong: %+v", config)
	}
	if config.NetworkValidationEnabled {
		t.Error("network validation must default to disabled")
	}
	if config.DiscardFailedCandidates {
		t.Error("discard_failed_candidates must default to false")
	}
	if config.RequeuePendingOnStart {
		t.Error("requeue_pending_on_start must default to false")
	}
	if config.DefaultProxyScheme != "http" {
		t.Errorf("DefaultProxyScheme = %q", config.DefaultProxyScheme)
	}
}

func TestLoadConfigEnvironmentOverrides(t *testing.T) {
	t.Setenv("FREEPROXYAPI_PROBE_TARGET", " https://example.com/healthz ")
	t.Setenv("FREEPROXYAPI_PROBE_TARGET_MODE", " echo ")
	t.Setenv("FREEPROXYAPI_WORKERS", "14")
	t.Setenv("FREEPROXYAPI_GLOBAL_REQUESTS_PER_MINUTE", "540")
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///data/proxies.txt"],
		"probe_target": "https://old.example/healthz"
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.ProbeTarget != "https://example.com/healthz" || config.ProbeTargetMode != "echo" || config.Workers != 14 || config.GlobalRequestsPerMinute != 540 {
		t.Fatalf("environment overrides not applied: target=%q mode=%q workers=%d budget=%d", config.ProbeTarget, config.ProbeTargetMode, config.Workers, config.GlobalRequestsPerMinute)
	}
}

func TestLoadConfigRejectsInvalidEnvironmentOverride(t *testing.T) {
	t.Setenv("FREEPROXYAPI_WORKERS", "many")
	if _, err := LoadConfig(writeConfig(t, validConfig)); err == nil {
		t.Fatal("LoadConfig accepted a non-integer worker override")
	}
}

func TestLoadConfigValidatesPrometheusURL(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///data/proxies.txt"],
		"prometheus_url": " http://prometheus.monitoring:9090/ "
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.PrometheusURL != "http://prometheus.monitoring:9090" {
		t.Fatalf("PrometheusURL = %q", config.PrometheusURL)
	}
	for _, raw := range []string{
		"ftp://prometheus.monitoring:9090",
		"http://user:pass@prometheus.monitoring:9090",
		"http://prometheus.monitoring:9090/api?query=up",
	} {
		body := `{"sources":["file:///data/proxies.txt"],"prometheus_url":` + strconv.Quote(raw) + `}`
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("LoadConfig accepted invalid prometheus_url %q", raw)
		}
	}
}

func TestLoadConfigValidatesTrustedProxyCIDRs(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///data/proxies.txt"],
		"trusted_proxy_cidrs": ["10.0.0.9/24", "2001:db8::1/64"]
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(config.TrustedProxyCIDRs) != 2 || config.TrustedProxyCIDRs[0].String() != "10.0.0.0/24" || config.TrustedProxyCIDRs[1].String() != "2001:db8::/64" {
		t.Fatalf("TrustedProxyCIDRs = %v", config.TrustedProxyCIDRs)
	}
	if defaults, err := LoadConfig(writeConfig(t, validConfig)); err != nil || len(defaults.TrustedProxyCIDRs) != 0 {
		t.Fatalf("trusted proxy defaults must be empty: config=%+v err=%v", defaults, err)
	}
	if _, err := LoadConfig(writeConfig(t, `{"sources":["file:///d/p.txt"],"trusted_proxy_cidrs":["10.0.0.1"]}`)); err == nil {
		t.Fatal("LoadConfig accepted a trusted proxy address without a CIDR prefix")
	}
}

func TestLoadConfigEnforcesLimits(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///d/p.txt"],
		"workers": 768,
		"global_requests_per_minute": 120000
	}`))
	if err != nil {
		t.Fatalf("LoadConfig at global request safety limit: %v", err)
	}
	if config.GlobalRequestsPerMinute != 120000 || config.Workers != 768 {
		t.Fatalf("GlobalRequestsPerMinute = %d, Workers = %d, want 120000 and 768", config.GlobalRequestsPerMinute, config.Workers)
	}

	cases := map[string]string{
		"too many workers": `{
			"sources": ["file:///d/p.txt"],
			"workers": 769
		}`,
		"budget too high": `{
			"sources": ["file:///d/p.txt"],
			"global_requests_per_minute": 120001
		}`,
		"oversized payload limit": `{
			"sources": ["file:///d/p.txt"],
			"source_max_bytes": 40000000
		}`,
		"no sources": `{
			"sources": []
		}`,
		"validation without target": `{
			"sources": ["file:///d/p.txt"],
			"network_validation_enabled": true
		}`,
		"invalid probe target mode": `{
			"sources": ["file:///d/p.txt"],
			"probe_target_mode": "combined"
		}`,
		"bad duration": `{
			"sources": ["file:///d/p.txt"],
			"fetch_interval": "sometimes"
		}`,
	}
	for name, body := range cases {
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("%s: LoadConfig accepted an unsafe configuration", name)
		}
	}
}

func TestLoadConfigAcceptsBurstProfileAndDiscardFailedCandidates(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///d/p.txt"],
		"workers": 384,
		"global_requests_per_minute": 36000,
		"discard_failed_candidates": true
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.Workers != 384 || config.GlobalRequestsPerMinute != 36000 || !config.DiscardFailedCandidates {
		t.Fatalf("burst config = %+v", config)
	}
	if policy := config.RetestPolicy(); !policy.DiscardFailedCandidates {
		t.Fatal("discard policy did not preserve discard_failed_candidates")
	}
}

func TestLoadConfigRequeuesPendingOnStartWithoutDiscardingFailures(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": ["file:///d/p.txt"],
		"requeue_pending_on_start": true,
		"discard_failed_candidates": false
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if !config.RequeuePendingOnStart {
		t.Fatal("requeue_pending_on_start was not loaded")
	}
	if config.DiscardFailedCandidates {
		t.Fatal("discard_failed_candidates must remain false")
	}
	if policy := config.RetestPolicy(); policy.DiscardFailedCandidates {
		t.Fatal("requeue setting must not change the discard policy")
	}
}

func TestLoadConfigDeadProxyCostDefaults(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if config.ProbeConnectTimeout != 3*time.Second || config.RetestJitterPct != 10 ||
		config.EvictedBackoffBase != 2*time.Hour || config.EvictedBackoffMax != 168*time.Hour {
		t.Fatalf("defaults: connect=%s jitter=%d base=%s max=%s", config.ProbeConnectTimeout, config.RetestJitterPct, config.EvictedBackoffBase, config.EvictedBackoffMax)
	}
	policy := config.RetestPolicy()
	if policy.JitterPct != 10 || policy.EvictedBackoffBase != 2*time.Hour || policy.EvictedBackoffMax != 168*time.Hour {
		t.Fatalf("RetestPolicy = %+v", policy)
	}

	short, err := LoadConfig(writeConfig(t, `{"sources":["file:///d/p.txt"],"request_timeout":"2s","retest_jitter_pct":0}`))
	if err != nil {
		t.Fatalf("LoadConfig short timeout: %v", err)
	}
	if short.ProbeConnectTimeout != 2*time.Second {
		t.Fatalf("default connect timeout = %s, want clamped to request_timeout 2s", short.ProbeConnectTimeout)
	}
	separate, err := LoadConfig(writeConfig(t, `{"sources":["file:///d/p.txt"],"request_timeout":"10s","source_request_timeout":"20s"}`))
	if err != nil {
		t.Fatalf("LoadConfig separate source timeout: %v", err)
	}
	if separate.RequestTimeout != 10*time.Second || separate.SourceRequestTimeout != 20*time.Second {
		t.Fatalf("separate source timeout = request=%s source=%s, want 10s and 20s", separate.RequestTimeout, separate.SourceRequestTimeout)
	}
	if short.RetestJitterPct != 0 {
		t.Fatalf("explicit zero jitter = %d", short.RetestJitterPct)
	}

	for name, body := range map[string]string{
		"connect exceeds request": `{"sources":["file:///d/p.txt"],"request_timeout":"5s","probe_connect_timeout":"6s"}`,
		"source timeout invalid":  `{"sources":["file:///d/p.txt"],"source_request_timeout":"soon"}`,
		"connect not positive":    `{"sources":["file:///d/p.txt"],"probe_connect_timeout":"0s"}`,
		"jitter too high":         `{"sources":["file:///d/p.txt"],"retest_jitter_pct":51}`,
		"jitter negative":         `{"sources":["file:///d/p.txt"],"retest_jitter_pct":-1}`,
		"backoff max below base":  `{"sources":["file:///d/p.txt"],"evicted_backoff_base":"4h","evicted_backoff_max":"1h"}`,
		"backoff base invalid":    `{"sources":["file:///d/p.txt"],"evicted_backoff_base":"soon"}`,
	} {
		if _, err := LoadConfig(writeConfig(t, body)); err == nil {
			t.Errorf("%s: LoadConfig accepted invalid configuration", name)
		}
	}
}

func TestLiveLocalMonitorConfigClassifiesAnonymityViaOwnEcho(t *testing.T) {
	config, err := LoadConfig(filepath.Join("..", "..", "k8s", "overlays", "live-local", "monitor.json"))
	if err != nil {
		t.Fatalf("LoadConfig live-local: %v", err)
	}
	if config.ProbeTargetMode != "standard" {
		t.Fatalf("live-local probe_target_mode = %q, want standard", config.ProbeTargetMode)
	}
	// Plain HTTP on purpose: through a CONNECT tunnel an HTTP proxy cannot add
	// the Via or X-Forwarded-For headers the classification looks for.
	if got := config.EchoURLs(); len(got) != 1 || got[0] != "http://freeproxyapi.crawlora.net/get" {
		t.Fatalf("live-local anonymity echo URLs = %q", got)
	}
}

func TestLiveLocalMonitorConfigDisablesPendingRequeue(t *testing.T) {
	config, err := LoadConfig(filepath.Join("..", "..", "k8s", "overlays", "live-local", "monitor.json"))
	if err != nil {
		t.Fatalf("LoadConfig live-local: %v", err)
	}
	if config.RequeuePendingOnStart {
		t.Fatal("live-local must not force the whole pending set due on start")
	}
	if config.ProbeConnectTimeout != 3*time.Second || config.RetestJitterPct != 50 ||
		config.EvictedBackoffBase != 2*time.Hour || config.EvictedBackoffMax != 168*time.Hour {
		t.Fatalf("live-local dead-proxy settings: %+v", config)
	}
	if config.Workers != 768 || config.GlobalRequestsPerMinute != 90000 || config.FailedRetestInterval != time.Hour {
		t.Fatalf("live-local throughput settings: workers=%d budget=%d failed_retest=%s",
			config.Workers, config.GlobalRequestsPerMinute, config.FailedRetestInterval)
	}
	if config.HTTPSProbeTarget != "https://detectportal.firefox.com/success.txt" ||
		config.HTTPSProbeExpectedBody != "success" || config.HTTPSProbeEvery != 4 {
		t.Fatalf("live-local HTTPS probe settings: target=%q body=%q every=%d",
			config.HTTPSProbeTarget, config.HTTPSProbeExpectedBody, config.HTTPSProbeEvery)
	}
}

func TestLoadConfigAcceptsMoreThanFormerSourceCap(t *testing.T) {
	sources := make([]string, 129)
	for i := range sources {
		sources[i] = `"file:///d/p.txt"`
	}
	config, err := LoadConfig(writeConfig(t, `{"sources":[`+strings.Join(sources, ",")+`]}`))
	if err != nil {
		t.Fatalf("LoadConfig rejected an uncapped source list: %v", err)
	}
	if len(config.SourceSpecs) != len(sources) {
		t.Fatalf("source specs = %d, want %d", len(config.SourceSpecs), len(sources))
	}
}

func TestParseSourceSpecSchemeHint(t *testing.T) {
	cases := []struct {
		raw    string
		url    string
		scheme string
	}{
		{raw: "https://inventory.example.net/list.txt", url: "https://inventory.example.net/list.txt", scheme: ""},
		{raw: "file:///data/proxies.txt", url: "file:///data/proxies.txt", scheme: ""},
		{raw: "socks5@https://inventory.example.net/socks5.txt", url: "https://inventory.example.net/socks5.txt", scheme: "socks5"},
		// The hint only applies when no URL scheme precedes the "@".
		{raw: "http@https://inventory.example.net/http.txt", url: "https://inventory.example.net/http.txt", scheme: "http"},
	}
	for _, tc := range cases {
		spec, err := parseSourceSpec(tc.raw)
		if err != nil {
			t.Fatalf("parseSourceSpec(%q): %v", tc.raw, err)
		}
		if spec.URL != tc.url || spec.Scheme != tc.scheme {
			t.Errorf("parseSourceSpec(%q) = %+v, want url=%q scheme=%q", tc.raw, spec, tc.url, tc.scheme)
		}
	}
	// Unsupported hints fall back to whole-string URL parsing and are rejected
	// when the source fetcher handles the unsupported URL scheme.
	fallback, err := parseSourceSpec("ftp@https://inventory.example.net/x")
	if err != nil || fallback.URL != "ftp@https://inventory.example.net/x" {
		t.Errorf("unsupported hint should fall back to raw URL: %+v err=%v", fallback, err)
	}
}

func TestLoadConfigParsesPerSourceSchemes(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, `{
		"sources": [
			"https://inventory.example.net/plain.txt",
			"socks5@https://inventory.example.net/socks5.txt"
		]
	}`))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if len(config.SourceSpecs) != 2 {
		t.Fatalf("SourceSpecs = %+v", config.SourceSpecs)
	}
	if config.SourceSpecs[1].Scheme != "socks5" {
		t.Errorf("spec 1 scheme = %q, want socks5", config.SourceSpecs[1].Scheme)
	}
	if got := config.SourceSpecs[1].defaultScheme(config.DefaultProxyScheme); got != "socks5" {
		t.Errorf("defaultScheme = %q, want socks5", got)
	}
	if got := config.SourceSpecs[0].defaultScheme(config.DefaultProxyScheme); got != "http" {
		t.Errorf("fallback defaultScheme = %q, want http", got)
	}
}

func TestSourceSpecDefaultSchemeFallback(t *testing.T) {
	spec := SourceSpec{URL: "https://x.example.net/l"}
	if got := spec.defaultScheme("https"); got != "https" {
		t.Errorf("defaultScheme = %q", got)
	}
}

func TestEchoURLsSplitsAndDeduplicates(t *testing.T) {
	config := Config{AnonymityCheckURL: " https://a.example.net/get , https://b.example.net/get ,https://a.example.net/get, "}
	urls := config.EchoURLs()
	if len(urls) != 2 || urls[0] != "https://a.example.net/get" || urls[1] != "https://b.example.net/get" {
		t.Fatalf("EchoURLs = %#v", urls)
	}
	if got := (Config{AnonymityCheckURL: ""}).EchoURLs(); len(got) != 0 {
		t.Fatalf("empty URL list should be empty, got %#v", got)
	}
}

func TestClassificationEchoURLsUsesProbeTargetInEchoMode(t *testing.T) {
	config := Config{
		ProbeTarget:       " https://probe.example/get ",
		ProbeTargetMode:   "echo",
		AnonymityCheckURL: "https://fallback.example/get,https://probe.example/get",
	}
	got := config.ClassificationEchoURLs()
	want := []string{"https://probe.example/get", "https://fallback.example/get"}
	if len(got) != len(want) {
		t.Fatalf("ClassificationEchoURLs = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("ClassificationEchoURLs[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
