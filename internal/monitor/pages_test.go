package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestEmbeddedPagesContainNoDeploymentSpecificValues(t *testing.T) {
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		for _, forbidden := range []string{"crawlora.net", "G-XKJN7HVK4H"} {
			if strings.Contains(string(page), forbidden) {
				t.Errorf("%s embeds %q; it must come from configuration", name, forbidden)
			}
		}
	}
}

func TestRenderPageDefaultsLoadNothingThirdParty(t *testing.T) {
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		out := string(renderPage(page, pageOptions{}))
		for _, forbidden := range []string{"googletagmanager", "gtag", "dataLayer", "{{", "rel=\"canonical\"", "og:url", "og:image\"", "twitter:image\"", analyticsBegin, analyticsEnd} {
			if strings.Contains(out, forbidden) {
				t.Errorf("%s default render still contains %q", name, forbidden)
			}
		}
	}
	home := string(renderPage(homepageHTML, pageOptions{}))
	if !strings.Contains(home, pageOriginFallback+`</span><span class="string">/proxies`) || !strings.Contains(home, "location.origin") {
		t.Error("homepage samples must fall back to the visitor's origin without public_base_url")
	}
	if strings.Contains(home, "\"url\": \"/\"") {
		t.Error("JSON-LD url must be dropped, not emitted relative")
	}
}

func TestRenderPageUsesConfiguredBaseURLAndAnalytics(t *testing.T) {
	out := string(renderPage(homepageHTML, pageOptions{BaseURL: "https://proxies.example.org", MeasurementID: "G-TESTID1234"}))
	for _, want := range []string{
		`<link rel="canonical" href="https://proxies.example.org/">`,
		`content="https://proxies.example.org/dashboard/social-share.png"`,
		`"url": "https://proxies.example.org/"`,
		`https://proxies.example.org</span><span class="string">/proxies</span>`,
		`const measurementId = 'G-TESTID1234';`,
		"googletagmanager.com/gtag/js",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("render missing %q", want)
		}
	}
	if strings.Contains(out, "{{") || strings.Contains(out, analyticsBegin) {
		t.Error("render left template markers behind")
	}
	dash := string(renderPage(dashboardHTML, pageOptions{BaseURL: "https://proxies.example.org"}))
	if !strings.Contains(dash, `<link rel="canonical" href="https://proxies.example.org/dashboard">`) || strings.Contains(dash, "gtag") {
		t.Error("dashboard must get its canonical URL and no analytics when the ID is unset")
	}
}

func TestPublicBaseURLAndMeasurementIDValidation(t *testing.T) {
	for raw, want := range map[string]string{"": "", " https://a.example/ ": "https://a.example", "http://localhost:8080": "http://localhost:8080"} {
		if got, err := normalizePublicBaseURL(raw); err != nil || got != want {
			t.Errorf("normalizePublicBaseURL(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"ftp://a.example", "a.example", "https://u:p@a.example", "https://a.example/x", "https://a.example?x=1", "https://a.example#f", `https://a.example"><script>`, "https://a .example"} {
		if got, err := normalizePublicBaseURL(raw); err == nil {
			t.Errorf("normalizePublicBaseURL(%q) = %q, want error", raw, got)
		}
	}
	for _, raw := range []string{"G-ABCD1234", "G-1234567890"} {
		if _, err := validateAnalyticsMeasurementID(raw); err != nil {
			t.Errorf("validateAnalyticsMeasurementID(%q): %v", raw, err)
		}
	}
	for _, raw := range []string{"UA-1234-1", "g-abcd1234", "G-AB", "G-ABCD1234');alert(1);//", "G-"} {
		if _, err := validateAnalyticsMeasurementID(raw); err == nil {
			t.Errorf("validateAnalyticsMeasurementID(%q) succeeded, want error", raw)
		}
	}
}

func TestLoadConfigPublicBaseURLAndAnalytics(t *testing.T) {
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil || config.PublicBaseURL != "" || config.AnalyticsMeasurementID != "" {
		t.Fatalf("defaults = %q %q, %v; want both empty", config.PublicBaseURL, config.AnalyticsMeasurementID, err)
	}
	withKeys := `{"sources":["file:///data/proxies.txt"],"public_base_url":"https://proxies.example.org/","analytics_measurement_id":"G-FILEID123"}`
	config, err = LoadConfig(writeConfig(t, withKeys))
	if err != nil || config.PublicBaseURL != "https://proxies.example.org" || config.AnalyticsMeasurementID != "G-FILEID123" {
		t.Fatalf("file values = %q %q, %v", config.PublicBaseURL, config.AnalyticsMeasurementID, err)
	}
	t.Setenv("FREEPROXYAPI_PUBLIC_BASE_URL", "https://env.example.org")
	t.Setenv("FREEPROXYAPI_ANALYTICS_MEASUREMENT_ID", "G-ENVID1234")
	config, err = LoadConfig(writeConfig(t, withKeys))
	if err != nil || config.PublicBaseURL != "https://env.example.org" || config.AnalyticsMeasurementID != "G-ENVID1234" {
		t.Fatalf("env overrides = %q %q, %v", config.PublicBaseURL, config.AnalyticsMeasurementID, err)
	}
	t.Setenv("FREEPROXYAPI_PUBLIC_BASE_URL", "not a url")
	if _, err := LoadConfig(writeConfig(t, withKeys)); err == nil {
		t.Fatal("invalid public_base_url override was accepted")
	}
}

func TestHealthServerServesRenderedPages(t *testing.T) {
	get := func(config Config, path string) string {
		health, err := startHealthServer("127.0.0.1:0", &Runner{config: config, metrics: newMetrics()})
		if err != nil {
			t.Fatal(err)
		}
		defer health.Shutdown()
		response := httptest.NewRecorder()
		health.server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("%s: status %d", path, response.Code)
		}
		return response.Body.String()
	}
	for _, path := range []string{"/", "/dashboard"} {
		if body := get(Config{}, path); strings.Contains(body, "googletagmanager") || strings.Contains(body, "crawlora.net") {
			t.Errorf("default %s contains analytics or a hard-coded host", path)
		}
		body := get(Config{PublicBaseURL: "https://proxies.example.org", AnalyticsMeasurementID: "G-TESTID1234"}, path)
		if !strings.Contains(body, "proxies.example.org") || !strings.Contains(body, "G-TESTID1234") {
			t.Errorf("configured %s is missing the base URL or measurement ID", path)
		}
	}
}

func TestLoadConfigRedisURLEnvOverride(t *testing.T) {
	t.Setenv("FREEPROXYAPI_REDIS_URL", "redis://:s3cret@redis.internal:6379/2")
	config, err := LoadConfig(writeConfig(t, validConfig))
	if err != nil || config.RedisURL != "redis://:s3cret@redis.internal:6379/2" {
		t.Fatalf("RedisURL = %q, %v", config.RedisURL, err)
	}
}

func TestGeoIPAttributionOnlyWhenConfigured(t *testing.T) {
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		if out := string(renderPage(page, pageOptions{})); strings.Contains(out, "MaxMind") || strings.Contains(out, "{{") {
			t.Errorf("%s shows MaxMind attribution without a GeoIP database", name)
		}
		out := string(renderPage(page, pageOptions{GeoIPAttribution: true}))
		if !strings.Contains(out, "GeoLite2 Data created by") || !strings.Contains(out, "https://www.maxmind.com") || strings.Contains(out, "{{") {
			t.Errorf("%s is missing the MaxMind attribution when a GeoIP database is configured", name)
		}
	}
	// The served page follows the configuration.
	health, err := startHealthServer("127.0.0.1:0", &Runner{config: Config{GeoIPDBPath: "/geoip/GeoLite2-Country.mmdb"}, metrics: newMetrics()})
	if err != nil {
		t.Fatal(err)
	}
	defer health.Shutdown()
	response := httptest.NewRecorder()
	health.server.Handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if !strings.Contains(response.Body.String(), "MaxMind") {
		t.Error("served homepage lacks attribution although geoip_db_path is set")
	}
}
