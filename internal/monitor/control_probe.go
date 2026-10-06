package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
)

// controlPausePoll is how often a paused worker re-checks control health.
const controlPausePoll = 250 * time.Millisecond

// accuracyState holds probe-accuracy runtime state: the per-replica control
// probe health, HTTPS sampling hooks, and their metrics. The zero value is a
// healthy control state with production probe functions.
type accuracyState struct {
	controlUnhealthy atomic.Bool
	controlFailures  atomic.Int64
	metrics          accuracyMetrics

	// Test hooks; nil selects the production implementation.
	controlCheckFunc func(ctx context.Context) error
	httpsProbeFunc   func(ctx context.Context, proxyURL string) probe.HTTPSResult
	sampleIntN       func(n int) int
	pausePoll        time.Duration
}

// controlProbeLoop fetches the probe target directly (no proxy) every
// control_probe_interval. It exits as soon as ctx is cancelled; an in-flight
// check is bounded by request_timeout and aborted by the same context.
func (r *Runner) controlProbeLoop(ctx context.Context) {
	interval := r.config.ControlProbeInterval
	if interval <= 0 {
		interval = defaultControlProbeInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		r.runControlProbe(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (r *Runner) runControlProbe(ctx context.Context) {
	timeout := r.config.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	check := r.accuracy.controlCheckFunc
	if check == nil {
		check = r.directControlCheck
	}
	checkCtx, cancel := context.WithTimeout(ctx, timeout)
	err := check(checkCtx)
	cancel()
	if ctx.Err() != nil {
		// Shutdown is not an origin failure.
		return
	}
	r.recordControlResult(err)
}

// recordControlResult advances the control state machine: threshold
// consecutive failures mark the replica unhealthy; one success recovers it.
func (r *Runner) recordControlResult(err error) {
	a := &r.accuracy
	if err == nil {
		a.controlFailures.Store(0)
		a.metrics.controlChecks[1].Add(1)
		if a.controlUnhealthy.CompareAndSwap(true, false) {
			a.metrics.controlUnhealthy.Store(0)
			log.Printf("control probe recovered: resuming probe workers target=%s", r.probeTargetLabel)
			if r.workAvailable != nil {
				r.workAvailable.signal()
			}
		}
		return
	}
	a.metrics.controlChecks[0].Add(1)
	failures := a.controlFailures.Add(1)
	threshold := int64(r.config.ControlProbeFailureThreshold)
	if threshold < 1 {
		threshold = defaultControlProbeFailureThreshold
	}
	if failures >= threshold && a.controlUnhealthy.CompareAndSwap(false, true) {
		a.metrics.controlUnhealthy.Store(1)
		log.Printf("control probe unhealthy after %d consecutive failures: pausing probe workers target=%s err=%s", failures, r.probeTargetLabel, truncateDetail(err.Error()))
		return
	}
	if !a.controlUnhealthy.Load() {
		log.Printf("control probe failed consecutive=%d threshold=%d err=%s", failures, threshold, truncateDetail(err.Error()))
	}
}

func (r *Runner) controlHealthy() bool { return !r.accuracy.controlUnhealthy.Load() }

// waitForControlHealthy blocks a worker while the control probe is unhealthy,
// polling in small increments. It returns false when ctx ends.
func (r *Runner) waitForControlHealthy(ctx context.Context) bool {
	poll := r.accuracy.pausePoll
	if poll <= 0 {
		poll = controlPausePoll
	}
	for !r.controlHealthy() {
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return ctx.Err() == nil
}

// suppressOutcomeWhileUnhealthy releases a failed probe's claim instead of
// committing it while the probe origin itself is failing, so an origin or
// egress outage cannot evict working proxies. It reports whether the outcome
// was suppressed.
func (r *Runner) suppressOutcomeWhileUnhealthy(claimRelease func(), ok bool) bool {
	if ok || r.controlHealthy() {
		return false
	}
	claimRelease()
	r.accuracy.metrics.suppressedOutcomes.Add(1)
	return true
}

// directControlCheck fetches probe_target without a proxy through the public
// dialer and applies the standard-mode status and body checks.
func (r *Runner) directControlCheck(ctx context.Context) error {
	target, err := url.Parse(r.config.ProbeTarget)
	if err != nil || target.Host == "" {
		return fmt.Errorf("invalid probe target")
	}
	query := target.Query()
	query.Set("_fpa_control", strconv.FormatInt(time.Now().UnixNano(), 36))
	target.RawQuery = query.Encode()
	transport := &http.Transport{
		DialContext:           endpoint.PublicDialer{ConnectTimeout: r.config.ProbeConnectTimeout}.DialContext,
		TLSHandshakeTimeout:   r.config.RequestTimeout,
		ResponseHeaderTimeout: r.config.RequestTimeout,
		DisableKeepAlives:     true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	request.Header.Set("User-Agent", "FreeProxyAPI/0.1 (control-probe)")
	request.Header.Set("Cache-Control", "no-cache")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return checkControlResponse(response, r.config.ProbeTargetMode, r.config.ProbeExpectedBody)
}

func checkControlResponse(response *http.Response, mode, expectedBody string) error {
	if response.StatusCode < probe.ExpectedStatusMin || response.StatusCode > probe.ExpectedStatusMax {
		return fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, probe.MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("read control response: %w", err)
	}
	if len(body) > probe.MaxResponseBytes {
		return errors.New("control response too large")
	}
	if mode != "echo" && expectedBody != "" && strings.TrimRight(string(body), " \t\r\n") != expectedBody {
		return errors.New("unexpected response body")
	}
	return nil
}
