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

var builtInPages = map[string][]byte{"homepage": homepageHTML, "dashboard": dashboardHTML}

// analyticsScript extracts the executable analytics script from a rendered page.
func analyticsScript(t *testing.T, page string) string {
	t.Helper()
	for _, match := range regexp.MustCompile(`(?s)<script>(.*?)</script>`).FindAllStringSubmatch(page, -1) {
		if strings.Contains(match[1], "googletagmanager.com/gtag/js") {
			return match[1]
		}
	}
	t.Fatal("rendered page has no analytics script")
	return ""
}

func TestAnalyticsLoadsOnEveryViewWhenConfigured(t *testing.T) {
	for name, page := range builtInPages {
		out := string(renderPage(page, pageOptions{MeasurementID: "G-TESTID1234", PrivacyURL: "https://example.org/privacy"}))
		if strings.Count(out, "googletagmanager.com/gtag/js") != 1 {
			t.Errorf("%s must reference the GA loader exactly once", name)
		}
		if !strings.Contains(out, "gtag('config', measurementId)") || !strings.Contains(out, "G-TESTID1234") {
			t.Errorf("%s does not configure GA with the measurement ID", name)
		}
		// There is no consent step, and no browser signal gates loading.
		for _, gone := range []string{"consent", `role="dialog"`, "localStorage", "globalPrivacyControl", "doNotTrack", "Analytics settings", "ga-disable-"} {
			if strings.Contains(out, gone) {
				t.Errorf("%s still contains consent code %q", name, gone)
			}
		}
		if strings.Contains(out, "{{") {
			t.Errorf("%s left template markers behind", name)
		}
	}
}

func TestAnalyticsAbsentWithoutMeasurementID(t *testing.T) {
	for name, page := range builtInPages {
		out := string(renderPage(page, pageOptions{PrivacyURL: "https://example.org/privacy"}))
		for _, forbidden := range []string{"googletagmanager", "gtag", "dataLayer", "localStorage", "privacy-link"} {
			if strings.Contains(out, forbidden) {
				t.Errorf("%s contains %q although analytics is off", name, forbidden)
			}
		}
	}
}

func TestPrivacyLinkOnlyWhenConfigured(t *testing.T) {
	with := string(renderPage(homepageHTML, pageOptions{MeasurementID: "G-TESTID1234", PrivacyURL: "https://example.org/privacy"}))
	if !strings.Contains(with, `<a class="privacy-link" href="https://example.org/privacy" rel="noopener">Privacy</a>`) {
		t.Error("configured privacy URL is not linked")
	}
	without := string(renderPage(homepageHTML, pageOptions{MeasurementID: "G-TESTID1234"}))
	if strings.Contains(without, `<a class="privacy-link"`) || !strings.Contains(without, "googletagmanager.com/gtag/js") {
		t.Error("the link must be dropped, and analytics kept, when privacy_url is unset")
	}
}

// The script runs against a stub DOM: with no stored choice and the browser
// signals that used to suppress analytics set, it must still load GA.
func TestAnalyticsScriptBehavior(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node is not installed; skipping the analytics script behavior test")
	}
	const harness = `
const fs = require('fs');
const appended = [];
const calls = [];
const window = {};
const document = {
  createElement: (tag) => ({ tag }),
  head: { appendChild: (el) => appended.push(el) },
};
const navigator = { globalPrivacyControl: true, doNotTrack: '1' };
const fn = new Function('window', 'document', 'navigator', fs.readFileSync(process.argv[2], 'utf8'));
fn(window, document, navigator);
window.dataLayer.forEach((entry) => calls.push(Array.from(entry)));
console.log(JSON.stringify({
  scripts: appended.map((el) => ({ tag: el.tag, src: el.src, async: el.async })),
  calls: calls.map((c) => [c[0], c[0] === 'config' ? c[1] : null]),
}));
`
	for name, page := range builtInPages {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			scriptPath := filepath.Join(dir, "analytics.js")
			harnessPath := filepath.Join(dir, "harness.js")
			rendered := string(renderPage(page, pageOptions{MeasurementID: "G-TESTID1234"}))
			if err := os.WriteFile(scriptPath, []byte(analyticsScript(t, rendered)), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(harnessPath, []byte(harness), 0o600); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command(node, harnessPath, scriptPath).CombinedOutput()
			if err != nil {
				t.Fatalf("harness failed: %v\n%s", err, out)
			}
			var got struct {
				Scripts []struct {
					Tag   string `json:"tag"`
					Src   string `json:"src"`
					Async bool   `json:"async"`
				} `json:"scripts"`
				Calls [][2]any `json:"calls"`
			}
			if err := json.Unmarshal(out, &got); err != nil {
				t.Fatalf("harness output is not JSON: %v\n%s", err, out)
			}
			if len(got.Scripts) != 1 || got.Scripts[0].Tag != "script" || !got.Scripts[0].Async ||
				got.Scripts[0].Src != "https://www.googletagmanager.com/gtag/js?id=G-TESTID1234" {
				t.Errorf("appended scripts = %+v, want one async GA loader for G-TESTID1234", got.Scripts)
			}
			if len(got.Calls) != 2 || got.Calls[0][0] != "js" || got.Calls[1][0] != "config" || got.Calls[1][1] != "G-TESTID1234" {
				t.Errorf("gtag calls = %v, want js then config G-TESTID1234", got.Calls)
			}
		})
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
