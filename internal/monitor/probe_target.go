package monitor

import (
	"net/http"
	"strconv"
)

const probeTargetBody = "FreeProxyAPI probe OK\n"

// isProbeContentRequest reserves a single exact query value on the existing
// /get route. All ordinary echo requests keep their previous behavior.
func isProbeContentRequest(r *http.Request) bool {
	values, ok := r.URL.Query()["probe_content"]
	return ok && len(values) == 1 && values[0] == "1"
}

// probeTarget is a small, stable content contract for explicitly configured
// target profiles. no-store prevents a cache from satisfying the proxy check.
func (h *healthHandlers) probeTarget(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if !requireGetOrHead(w, r) {
		return
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(probeTargetBody)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write([]byte(probeTargetBody))
	}
}
