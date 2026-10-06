package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func fakeEnv(values map[string]string) func(string) string {
	return func(name string) string { return values[name] }
}

func credentialEnv() map[string]string {
	return map[string]string{
		"GOST_API_USERNAME":   "api-user",
		"GOST_API_PASSWORD":   "api-pass",
		"GOST_PROXY_USERNAME": "proxy-user",
		"GOST_PROXY_PASSWORD": "proxy-pass",
	}
}

func writeSecret(t *testing.T, content string) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(file, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func TestParseOptionsAcceptsValidEnvironment(t *testing.T) {
	env := credentialEnv()
	env["GOST_REFRESH_INTERVAL"] = "2m"
	env["FREEPROXYAPI_LIMIT"] = "250"
	env["FREEPROXYAPI_PUBLIC"] = "true"
	opts, err := parseOptions(nil, fakeEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	if opts.RefreshInterval != 2*time.Minute || opts.Limit != 250 || !opts.PublicAPI {
		t.Fatalf("options = %+v", opts)
	}
	if opts.GOSTAPIPassword != "api-pass" || opts.ProxyPassword != "proxy-pass" {
		t.Fatalf("passwords not read from environment: %+v", opts)
	}
}

func TestParseOptionsRejectsMalformedEnvironment(t *testing.T) {
	for name, value := range map[string]string{
		"GOST_REFRESH_INTERVAL":     "60",
		"GOST_REQUEST_TIMEOUT":      "-1s",
		"GOST_MIN_RATIO_PCT":        "eighty",
		"GOST_MAX_LATENCY_MS":       "1.5",
		"FREEPROXYAPI_LIMIT":        "lots",
		"FREEPROXYAPI_PUBLIC":       "maybe",
		"FREEPROXYAPI_GEO_MISMATCH": "yep",
	} {
		t.Run(name, func(t *testing.T) {
			env := credentialEnv()
			env[name] = value
			_, err := parseOptions(nil, fakeEnv(env))
			if err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("error = %v, want one naming %s", err, name)
			}
		})
	}
}

func TestParseOptionsRequiresCredentials(t *testing.T) {
	env := credentialEnv()
	delete(env, "GOST_PROXY_PASSWORD")
	if _, err := parseOptions(nil, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST_PROXY_PASSWORD") {
		t.Fatalf("error = %v, want missing proxy password", err)
	}
	// An empty password file is still a missing credential.
	if _, err := parseOptions([]string{"-proxy-password-file", writeSecret(t, "\n")}, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST_PROXY_PASSWORD") {
		t.Fatalf("error = %v, want missing proxy password for empty file", err)
	}
	if _, err := parseOptions([]string{"-refresh", "0s"}, fakeEnv(credentialEnv())); err == nil {
		t.Fatal("zero refresh interval accepted")
	}
}

func TestParseOptionsRejectsPasswordFlags(t *testing.T) {
	for _, name := range []string{"-proxy-password", "-gost-api-password"} {
		if _, err := parseOptions([]string{name, "from-flag"}, fakeEnv(credentialEnv())); err == nil {
			t.Errorf("%s accepted a password on the command line", name)
		}
	}
}

func TestParseOptionsReadsPasswordFiles(t *testing.T) {
	env := credentialEnv()
	delete(env, "GOST_API_PASSWORD")
	delete(env, "GOST_PROXY_PASSWORD")
	env["GOST_API_PASSWORD_FILE"] = writeSecret(t, "api-from-file\n")
	opts, err := parseOptions([]string{"-proxy-password-file", writeSecret(t, "proxy-from-file\r\n")}, fakeEnv(env))
	if err != nil {
		t.Fatal(err)
	}
	if opts.GOSTAPIPassword != "api-from-file" || opts.ProxyPassword != "proxy-from-file" {
		t.Fatalf("passwords = %q/%q", opts.GOSTAPIPassword, opts.ProxyPassword)
	}
}

func TestParseOptionsPasswordFileErrors(t *testing.T) {
	env := credentialEnv()
	delete(env, "GOST_PROXY_PASSWORD")
	missing := filepath.Join(t.TempDir(), "absent")
	if _, err := parseOptions([]string{"-proxy-password-file", missing}, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST_PROXY_PASSWORD_FILE") {
		t.Fatalf("error = %v, want unreadable password file", err)
	}
	// Supplying both sources is ambiguous and rejected.
	env = credentialEnv()
	env["GOST_API_PASSWORD_FILE"] = writeSecret(t, "other")
	if _, err := parseOptions(nil, fakeEnv(env)); err == nil || !strings.Contains(err.Error(), "GOST_API_PASSWORD") {
		t.Fatalf("error = %v, want conflicting password sources", err)
	}
}
