package probe

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func plainDial(ctx context.Context, network, address string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(ctx, network, address)
}

func TestRunProbeStatusBoundaries(t *testing.T) {
	// Note: a raw 199 cannot be exercised here — Go's HTTP server rewrites
	// unknown 1xx codes to 200 on the wire.
	for _, status := range []int{200, 204, 299, 301, 302, 304, 307, 399, 400, 500} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if status >= 300 && status < 400 {
				w.Header().Set("Location", "http://portal.example.net/login")
			}
			w.WriteHeader(status)
		}))
		result := runProbe(context.Background(), server.URL, server.URL+"/target", 5*time.Second, plainDial)
		server.Close()
		want := status >= ExpectedStatusMin && status <= ExpectedStatusMax
		if result.OK != want {
			t.Fatalf("status %d: OK=%t err=%q", status, result.OK, result.Error)
		}
		if want && result.StatusCode != status {
			t.Fatalf("status %d: recorded %d", status, result.StatusCode)
		}
	}
}

func TestRunProbeFailsWhenBodyExceedsLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(strings.Repeat("x", int(MaxResponseBytes)+16)))
	}))
	defer server.Close()
	result := runProbe(context.Background(), server.URL, server.URL+"/target", 5*time.Second, plainDial)
	if result.OK || !strings.Contains(result.Error, "limit exceeded") {
		t.Fatalf("oversized body should fail: %+v", result)
	}
}

func TestRunProbeFailsWhenBodyReadTimesOutAndMeasuresFullRequest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "10")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	timeout := 75 * time.Millisecond
	result := runProbe(context.Background(), server.URL, "http://192.0.2.10/target", timeout, plainDial)
	if result.OK || result.Error == "" {
		t.Fatalf("partial timed-out body should fail: %+v", result)
	}
	if result.Duration < timeout/2 {
		t.Fatalf("duration %s stopped before body read completed", result.Duration)
	}
}

func TestRunProbeRejectsUnsupportedScheme(t *testing.T) {
	result := runProbe(context.Background(), "gopher://proxy.example.net:70", "http://192.0.2.10/", time.Second, plainDial)
	if result.OK || !strings.Contains(result.Error, "unsupported proxy scheme") {
		t.Fatalf("unsupported scheme should fail: %+v", result)
	}
}

// TestRunProbeThroughHTTPProxy exercises the standard-library forward-proxy path
// end to end against a loopback fixture.
func TestRunProbeThroughHTTPProxy(t *testing.T) {
	var seenTarget string
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer target.Close()

	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Keep the response behind the request write long enough for the
		// asynchronous transport writer to update the traffic counter.
		time.Sleep(10 * time.Millisecond)
		if r.URL.Path == "/target" {
			seenTarget = r.URL.String()
			w.WriteHeader(http.StatusNoContent)
			return
		}
		// Absolute-form request arrives here when used as an HTTP proxy.
		w.WriteHeader(http.StatusNoContent)
	}))
	defer proxy.Close()
	result := runProbe(context.Background(), proxy.URL, target.URL+"/target", 5*time.Second, plainDial)
	if !result.OK || result.StatusCode != http.StatusNoContent {
		t.Fatalf("probe through HTTP proxy failed: %+v", result)
	}
	// Standard-mode probes append a random cache-buster query parameter.
	if !strings.HasPrefix(seenTarget, target.URL+"/target?"+cacheBusterParam+"=") {
		t.Fatalf("target handler not reached: %q", seenTarget)
	}
	if result.UploadBytes <= 0 || result.DownloadBytes <= 0 {
		t.Fatalf("probe traffic was not counted: upload=%d download=%d", result.UploadBytes, result.DownloadBytes)
	}
}

func TestRunProbeEchoThroughHTTPProxy(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"origin":"198.51.100.7","headers":{}}`))
	}))
	defer server.Close()

	result := runProbeEcho(context.Background(), server.URL, server.URL+"/get", 5*time.Second, "203.0.113.8", plainDial)
	if !result.OK || result.StatusCode != http.StatusOK {
		t.Fatalf("echo probe failed: %+v", result)
	}
	if result.Anonymity.Class != AnonymityElite || result.Anonymity.ExitIP != "198.51.100.7" {
		t.Fatalf("unexpected echo classification: %+v", result.Anonymity)
	}
	if requests != 1 {
		t.Fatalf("echo probe made %d requests, want 1", requests)
	}
}

func TestRunProbeEchoRequiresCallerIP(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	result := runProbeEcho(context.Background(), server.URL, server.URL+"/get", 5*time.Second, "203.0.113.8", plainDial)
	if result.OK || !strings.Contains(result.Error, "no caller IP") {
		t.Fatalf("echo probe without caller IP should fail: %+v", result)
	}
}
