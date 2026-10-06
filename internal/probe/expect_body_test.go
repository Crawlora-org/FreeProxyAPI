package probe

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunProbeExpectedBody(t *testing.T) {
	for _, tc := range []struct {
		name     string
		body     string
		expected string
		wantOK   bool
	}{
		{"exact match", "success", "success", true},
		{"trailing newline trimmed", "success\n", "success", true},
		{"trailing whitespace trimmed", "success \r\n\t", "success", true},
		{"leading whitespace not trimmed", "  success", "success", false},
		{"captive portal body", "<html>login</html>", "success", false},
		{"empty body", "", "success", false},
		{"status only when unset", "anything at all", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			result := runProbeMode(context.Background(), server.URL, server.URL+"/success.txt", 5*time.Second, "", false, tc.expected, plainDial)
			if result.OK != tc.wantOK {
				t.Fatalf("OK=%t want %t (err=%q)", result.OK, tc.wantOK, result.Error)
			}
			if !tc.wantOK && result.Error != "unexpected response body" {
				t.Fatalf("error = %q, want unexpected response body", result.Error)
			}
		})
	}
}

func TestRunProbeStandardAddsCacheBuster(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []*http.Request
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Clone(context.Background()))
		mu.Unlock()
		_, _ = w.Write([]byte("success\n"))
	}))
	defer server.Close()

	for i := 0; i < 2; i++ {
		result := runProbeMode(context.Background(), server.URL, server.URL+"/success.txt?keep=1&other=a%20b", 5*time.Second, "", false, "success", plainDial)
		if !result.OK {
			t.Fatalf("probe %d failed: %+v", i, result)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(requests) != 2 {
		t.Fatalf("got %d requests, want 2", len(requests))
	}
	seen := map[string]bool{}
	for _, r := range requests {
		q := r.URL.Query()
		if r.URL.Path != "/success.txt" || q.Get("keep") != "1" || q.Get("other") != "a b" {
			t.Fatalf("existing query not preserved: %s", r.URL.String())
		}
		buster := q.Get(cacheBusterParam)
		if _, err := hex.DecodeString(buster); err != nil || len(buster) != 16 {
			t.Fatalf("cache buster = %q, want 16 hex chars", buster)
		}
		seen[buster] = true
		if got := r.Header.Get("Cache-Control"); got != "no-cache" {
			t.Fatalf("Cache-Control = %q", got)
		}
		if got := r.Header.Get("Pragma"); got != "no-cache" {
			t.Fatalf("Pragma = %q", got)
		}
	}
	if len(seen) != 2 {
		t.Fatal("cache buster must differ between probes")
	}
}

func TestWithCacheBusterWithoutQuery(t *testing.T) {
	got, err := withCacheBuster("http://detectportal.example/success.txt")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(got, "http://detectportal.example/success.txt?"+cacheBusterParam+"=") {
		t.Fatalf("withCacheBuster = %q", got)
	}
}

func TestRunProbeEchoUnaffectedByCacheBuster(t *testing.T) {
	var (
		mu      sync.Mutex
		rawQS   []string
		headers []http.Header
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		rawQS = append(rawQS, r.URL.RawQuery)
		headers = append(headers, r.Header.Clone())
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"origin":"203.0.113.50","headers":{}}`))
	}))
	defer server.Close()
	_ = runProbeEcho(context.Background(), server.URL, server.URL+"/get", 5*time.Second, "198.51.100.1", plainDial)
	mu.Lock()
	defer mu.Unlock()
	if len(rawQS) != 1 {
		t.Fatalf("got %d requests, want 1", len(rawQS))
	}
	if rawQS[0] != "" {
		t.Fatalf("echo probe query = %q, want none", rawQS[0])
	}
	if headers[0].Get("Cache-Control") != "" || headers[0].Get("Pragma") != "" {
		t.Fatalf("echo probe must not send cache headers: %v", headers[0])
	}
}
