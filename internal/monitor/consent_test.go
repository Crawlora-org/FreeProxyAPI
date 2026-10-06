package monitor

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// consentScript extracts the executable analytics script from a rendered page.
func consentScript(t *testing.T, page string) string {
	t.Helper()
	for _, match := range regexp.MustCompile(`(?s)<script>(.*?)</script>`).FindAllStringSubmatch(page, -1) {
		if strings.Contains(match[1], "fpa-analytics-consent") {
			return match[1]
		}
	}
	t.Fatal("rendered page has no analytics consent script")
	return ""
}

func TestConsentScriptBehavior(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the consent script behavior test")
	}
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		t.Run(name, func(t *testing.T) {
			rendered := string(renderPage(page, pageOptions{MeasurementID: "G-TESTID1234", PrivacyURL: "https://example.org/privacy"}))
			script := filepath.Join(t.TempDir(), "consent.js")
			// Keep the placeholder: the harness substitutes the ID itself, as renderPage did.
			raw := strings.Replace(consentScript(t, rendered), "G-TESTID1234", "{{MEASUREMENT_ID}}", 1)
			if err := os.WriteFile(script, []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command(node, filepath.Join("testdata", "consent_harness.js"), script).CombinedOutput()
			if err != nil {
				t.Fatalf("harness failed: %v\n%s", err, out)
			}
			var results []struct {
				Name   string `json:"name"`
				OK     bool   `json:"ok"`
				Detail string `json:"detail"`
			}
			if err := json.Unmarshal(out, &results); err != nil {
				t.Fatalf("harness output is not JSON: %v\n%s", err, out)
			}
			if len(results) < 9 {
				t.Fatalf("only %d scenarios ran", len(results))
			}
			for _, result := range results {
				if !result.OK {
					t.Errorf("%s: %s", result.Name, result.Detail)
				}
			}
		})
	}
}

func TestAnalyticsNeverLoadsWithoutConsent(t *testing.T) {
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		out := string(renderPage(page, pageOptions{MeasurementID: "G-TESTID1234"}))
		for _, old := range []string{"setTimeout(activate", "activated = true", "const activate = "} {
			if strings.Contains(out, old) {
				t.Errorf("%s still has the old auto-activation code %q", name, old)
			}
		}
		// The loader is only reachable through load(), which only runs after consent.
		if strings.Count(out, "googletagmanager.com/gtag/js") != 1 {
			t.Errorf("%s must reference the GA loader exactly once", name)
		}
		for _, want := range []string{`id="consent"`, `role="dialog"`, "globalPrivacyControl", "doNotTrack", "fpa-analytics-consent", `id="consent-settings"`} {
			if !strings.Contains(out, want) {
				t.Errorf("%s missing %q", name, want)
			}
		}
		if strings.Contains(out, "{{") {
			t.Errorf("%s left template markers behind", name)
		}
	}
}

func TestConsentBannerAbsentWithoutAnalyticsID(t *testing.T) {
	for name, page := range map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML} {
		out := string(renderPage(page, pageOptions{PrivacyURL: "https://example.org/privacy"}))
		for _, forbidden := range []string{"consent", "localStorage", "Analytics settings", "googletagmanager"} {
			if strings.Contains(out, forbidden) {
				t.Errorf("%s shows %q although analytics is off", name, forbidden)
			}
		}
	}
}

func TestConsentPrivacyLinkOnlyWhenConfigured(t *testing.T) {
	with := string(renderPage(homepageHTML, pageOptions{MeasurementID: "G-TESTID1234", PrivacyURL: "https://example.org/privacy"}))
	if !strings.Contains(with, `<a href="https://example.org/privacy" rel="noopener">Privacy details</a>`) {
		t.Error("configured privacy URL is not linked")
	}
	without := string(renderPage(homepageHTML, pageOptions{MeasurementID: "G-TESTID1234"}))
	if strings.Contains(without, "Privacy details") || !strings.Contains(without, `id="consent-text"`) {
		t.Error("banner text must remain and the link must be dropped when privacy_url is unset")
	}
	// The paragraph must still close properly when the link line is dropped.
	text := without[strings.Index(without, `id="consent-text"`):]
	if !strings.Contains(text[:strings.Index(text, "consent-actions")], "</p>") {
		t.Error("consent paragraph lost its closing tag")
	}
}

func TestPrivacyURLValidationAndConfig(t *testing.T) {
	for raw, want := range map[string]string{"": "", " https://a.example/privacy ": "https://a.example/privacy", "http://a.example/p?x=1": "http://a.example/p?x=1"} {
		if got, err := normalizePrivacyURL(raw); err != nil || got != want {
			t.Errorf("normalizePrivacyURL(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"ftp://a.example", "a.example/privacy", "https://u:p@a.example", `https://a.example/"><script>`, "https://a.example/a b", "javascript:alert(1)"} {
		if got, err := normalizePrivacyURL(raw); err == nil {
			t.Errorf("normalizePrivacyURL(%q) = %q, want error", raw, got)
		}
	}
	body := `{"sources":["file:///data/proxies.txt"],"privacy_url":"https://file.example/privacy"}`
	config, err := LoadConfig(writeConfig(t, body))
	if err != nil || config.PrivacyURL != "https://file.example/privacy" {
		t.Fatalf("file value = %q, %v", config.PrivacyURL, err)
	}
	t.Setenv("FREEPROXYAPI_PRIVACY_URL", "https://env.example/privacy")
	if config, err = LoadConfig(writeConfig(t, body)); err != nil || config.PrivacyURL != "https://env.example/privacy" {
		t.Fatalf("env value = %q, %v", config.PrivacyURL, err)
	}
	t.Setenv("FREEPROXYAPI_PRIVACY_URL", "not a url")
	if _, err := LoadConfig(writeConfig(t, body)); err == nil {
		t.Fatal("invalid privacy_url override accepted")
	}
}
