package monitor

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/url"
	"regexp"
	"strings"
)

const (
	// pageOriginFallback stands in for the API origin in the homepage code
	// samples when public_base_url is unset; a small script swaps it for the
	// origin the visitor actually reached.
	pageOriginFallback = "https://your-host.example"

	analyticsBegin = "<!--analytics:begin-->"
	analyticsEnd   = "<!--analytics:end-->"
)

// measurementIDPattern matches a Google Analytics 4 measurement ID. The value
// is substituted into inline script, so it must stay within this charset.
var measurementIDPattern = regexp.MustCompile(`^G-[A-Z0-9]{4,20}$`)

// normalizePublicBaseURL validates public_base_url: an http(s) origin with no
// credentials, path, query, or fragment. It returns the origin without a
// trailing slash, or "" when unset.
func normalizePublicBaseURL(raw string) (string, error) {
	raw = strings.TrimRight(strings.TrimSpace(raw), "/")
	if raw == "" {
		return "", nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" ||
		strings.ContainsAny(raw, "\"'<>\\ \t\r\n") {
		return "", fmt.Errorf("public_base_url must be an http(s) origin such as https://proxies.example.com, without a path, credentials, query, or fragment")
	}
	return raw, nil
}

// validateAnalyticsMeasurementID validates analytics_measurement_id; empty
// disables analytics entirely.
func validateAnalyticsMeasurementID(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	if !measurementIDPattern.MatchString(raw) {
		return "", fmt.Errorf("analytics_measurement_id must look like G-XXXXXXXXXX")
	}
	return raw, nil
}

// renderPage specializes an embedded HTML page for this deployment.
//
//   - {{BASE_URL}} lines (canonical, Open Graph, JSON-LD) need an absolute URL,
//     so they are dropped unless public_base_url is set.
//   - {{API_ORIGIN}} in code samples becomes public_base_url, or a placeholder
//     that page script replaces with the visitor's own origin.
//   - The MaxMind attribution line is dropped unless a GeoIP database is configured.
//   - The analytics block, including its consent banner, is dropped unless
//     analytics_measurement_id is set, so a default deployment never loads a
//     third-party script or reports visitors. When it is set, Google Analytics
//     still loads only after the visitor accepts the banner.
//   - The banner's privacy link is dropped unless privacy_url is set.
//
// Both values are validated by LoadConfig, which keeps the substitution safe.
// pageOptions are the deployment-specific values rendered into the pages.
type pageOptions struct {
	BaseURL       string
	MeasurementID string
	// PrivacyURL is linked from the analytics consent banner; the link is
	// omitted when it is empty.
	PrivacyURL string
	// GeoIPAttribution shows MaxMind's required attribution; set it when a
	// GeoLite2 database is configured, since the pages then display derived
	// country and ASN data.
	GeoIPAttribution bool
	// CloudflareWebAnalytics allows the beacon script that Cloudflare injects
	// into proxied HTML pages when Web Analytics auto-install is on.
	CloudflareWebAnalytics bool
}

// geoipAttribution is the attribution text MaxMind's GeoLite2 terms require.
const geoipAttribution = `This product includes GeoLite2 Data created by <a href="https://www.maxmind.com">MaxMind</a>, available from https://www.maxmind.com.`

func renderPage(raw []byte, opts pageOptions) []byte {
	baseURL, measurementID := opts.BaseURL, opts.MeasurementID
	var out []string
	inAnalytics := false
	for _, line := range strings.Split(string(raw), "\n") {
		switch {
		case strings.Contains(line, analyticsBegin):
			inAnalytics = true
			continue
		case strings.Contains(line, analyticsEnd):
			inAnalytics = false
			continue
		case inAnalytics && measurementID == "":
			continue
		case baseURL == "" && strings.Contains(line, "{{BASE_URL}}"):
			continue
		case !opts.GeoIPAttribution && strings.Contains(line, "{{GEOIP_ATTRIBUTION}}"):
			continue
		case opts.PrivacyURL == "" && strings.Contains(line, "{{PRIVACY_URL}}"):
			continue
		}
		out = append(out, line)
	}
	origin := baseURL
	if origin == "" {
		origin = pageOriginFallback
	}
	return []byte(strings.NewReplacer(
		"{{BASE_URL}}", baseURL,
		"{{API_ORIGIN}}", origin,
		"{{API_ORIGIN_FALLBACK}}", pageOriginFallback,
		"{{MEASUREMENT_ID}}", measurementID,
		"{{GEOIP_ATTRIBUTION}}", geoipAttribution,
		"{{PRIVACY_URL}}", opts.PrivacyURL,
	).Replace(strings.Join(out, "\n")))
}

// htmlPage is a rendered embedded page with the Content-Security-Policy that
// matches its inline scripts.
type htmlPage struct {
	body []byte
	csp  string
}

func newHTMLPage(raw []byte, opts pageOptions) htmlPage {
	body := renderPage(raw, opts)
	return htmlPage{body: body, csp: contentSecurityPolicy(body, opts.MeasurementID != "", opts.CloudflareWebAnalytics)}
}

// inlineScript matches executable inline scripts: no src attribute and not a
// data block such as JSON-LD, which the browser never runs.
var inlineScript = regexp.MustCompile(`(?s)<script(\s[^>]*)?>(.*?)</script>`)

// contentSecurityPolicy allows only same-origin resources and the page's own
// inline scripts, identified by hash, so injected markup cannot execute.
// Inline styles stay allowed because the pages use style attributes. Analytics
// hosts are added only when a measurement ID is configured, and Cloudflare's
// beacon only when cloudflare_web_analytics is set.
func contentSecurityPolicy(body []byte, analytics, cloudflareBeacon bool) string {
	scriptSrc := []string{"'self'"}
	for _, match := range inlineScript.FindAllSubmatch(body, -1) {
		attrs := string(match[1])
		if strings.Contains(attrs, "src=") || strings.Contains(attrs, "type=\"application/ld+json\"") {
			continue
		}
		sum := sha256.Sum256(match[2])
		scriptSrc = append(scriptSrc, "'sha256-"+base64.StdEncoding.EncodeToString(sum[:])+"'")
	}
	imgSrc := []string{"'self'", "data:"}
	connectSrc := []string{"'self'"}
	if analytics {
		scriptSrc = append(scriptSrc, "https://www.googletagmanager.com")
		imgSrc = append(imgSrc, "https://www.google-analytics.com", "https://*.google-analytics.com", "https://*.googletagmanager.com")
		connectSrc = append(connectSrc, "https://www.google-analytics.com", "https://*.google-analytics.com", "https://*.analytics.google.com", "https://*.googletagmanager.com")
	}
	if cloudflareBeacon {
		// The beacon reports to the page's own /cdn-cgi/rum, covered by 'self'.
		scriptSrc = append(scriptSrc, "https://static.cloudflareinsights.com")
	}
	return strings.Join([]string{
		"default-src 'none'",
		"script-src " + strings.Join(scriptSrc, " "),
		"style-src 'self' 'unsafe-inline'",
		"img-src " + strings.Join(imgSrc, " "),
		"connect-src " + strings.Join(connectSrc, " "),
		"base-uri 'none'",
		"form-action 'self'",
		"frame-ancestors 'none'",
	}, "; ")
}

// normalizePrivacyURL validates privacy_url: an http(s) URL without
// credentials, query, or fragment-breaking characters. Unlike public_base_url it
// may carry a path. It returns "" when unset.
func normalizePrivacyURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", nil
	}
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" ||
		parsed.User != nil || strings.ContainsAny(raw, "\"'<>\\ \t\r\n") {
		return "", fmt.Errorf("privacy_url must be an http(s) URL without credentials or quote characters, such as https://example.com/privacy")
	}
	return raw, nil
}
