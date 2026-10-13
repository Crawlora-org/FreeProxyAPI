package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestProbeTargetContentContract(t *testing.T) {
	_, health, _, _ := startStaleTestServer(t, "probe-target-content")
	t.Run("GET returns the exact body without cacheability", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, "/get?probe_content=1", nil)
		health.server.Handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK || recorder.Body.String() != "FreeProxyAPI probe OK\n" {
			t.Fatalf("GET status=%d body=%q", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("Content-Type=%q", got)
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control=%q", got)
		}
	})

	t.Run("HEAD returns matching metadata without a body", func(t *testing.T) {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodHead, "/get?probe_content=1", nil)
		health.server.Handler.ServeHTTP(recorder, request)

		if recorder.Code != http.StatusOK || recorder.Body.Len() != 0 {
			t.Fatalf("HEAD status=%d body=%q", recorder.Code, recorder.Body.String())
		}
		if got := recorder.Header().Get("Content-Length"); got != "22" {
			t.Fatalf("Content-Length=%q, want 22", got)
		}
		if got := recorder.Header().Get("Content-Type"); got != "text/plain; charset=utf-8" {
			t.Fatalf("Content-Type=%q", got)
		}
		if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
			t.Fatalf("Cache-Control=%q", got)
		}
	})
}

func TestProbeTargetRejectsOtherMethodsAndPaths(t *testing.T) {
	_, health, _, _ := startStaleTestServer(t, "probe-target-methods")
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{method: http.MethodPost, path: "/get?probe_content=1", status: http.StatusMethodNotAllowed},
		{method: http.MethodOptions, path: "/get?probe_content=1", status: http.StatusMethodNotAllowed},
		{method: http.MethodGet, path: "/probe.txt", status: http.StatusNotFound},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(tc.method, tc.path, nil)
			health.server.Handler.ServeHTTP(recorder, request)
			if recorder.Code != tc.status {
				t.Fatalf("status=%d, want %d", recorder.Code, tc.status)
			}
			if tc.status == http.StatusMethodNotAllowed {
				if got := recorder.Header().Get("Allow"); got != "GET, HEAD" {
					t.Fatalf("Allow=%q", got)
				}
				if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
					t.Fatalf("Cache-Control=%q", got)
				}
			}
		})
	}
}

func TestProbeContentModeLeavesNormalEchoUntouched(t *testing.T) {
	_, health, _, _ := startStaleTestServer(t, "probe-target-echo")
	for _, tc := range []struct {
		path       string
		wantStatic bool
	}{
		{path: "/get?probe=1"},
		{path: "/get?probe_content=0"},
		{path: "/get?probe_content=1&probe=2", wantStatic: true},
		{path: "/get?probe_content=1&probe_content=0"},
	} {
		recorder := httptest.NewRecorder()
		request := httptest.NewRequest(http.MethodGet, tc.path, nil)
		health.server.Handler.ServeHTTP(recorder, request)
		if recorder.Code != http.StatusOK {
			t.Fatalf("GET %s status=%d body=%q", tc.path, recorder.Code, recorder.Body.String())
		}
		if tc.wantStatic {
			if recorder.Body.String() != "FreeProxyAPI probe OK\n" {
				t.Fatalf("GET %s body=%q", tc.path, recorder.Body.String())
			}
			if got := recorder.Header().Get("Cache-Control"); got != "no-store" {
				t.Fatalf("GET %s Cache-Control=%q", tc.path, got)
			}
			continue
		}
		if !strings.Contains(recorder.Body.String(), `"args"`) || strings.Contains(recorder.Body.String(), "FreeProxyAPI probe OK") {
			t.Fatalf("GET %s did not use the regular echo: %q", tc.path, recorder.Body.String())
		}
	}
}
