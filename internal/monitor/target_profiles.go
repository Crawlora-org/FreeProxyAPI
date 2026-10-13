package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const defaultTargetProbeEvery = 16

var targetProfileNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

// TargetProfile describes a small response an operator owns and permits the
// monitor to fetch. Profiles have no defaults and cannot be supplied by clients.
type TargetProfile struct {
	Name         string `json:"name"`
	URL          string `json:"url"`
	ExpectedBody string `json:"expected_body"`
}

func (p TargetProfile) fingerprint() string {
	digest := sha256.Sum256([]byte(p.URL + "\x00" + p.ExpectedBody))
	return hex.EncodeToString(digest[:])
}

func validateTargetProfiles(profiles []TargetProfile) error {
	if len(profiles) > store.MaxTargetProfiles {
		return fmt.Errorf("target_profiles allows at most %d profiles", store.MaxTargetProfiles)
	}
	seen := map[string]bool{}
	for i := range profiles {
		p := &profiles[i]
		p.Name = strings.TrimSpace(p.Name)
		p.URL = strings.TrimSpace(p.URL)
		p.ExpectedBody = strings.TrimRight(p.ExpectedBody, " \t\r\n")
		if !targetProfileNamePattern.MatchString(p.Name) || seen[p.Name] {
			return fmt.Errorf("target_profiles requires unique lowercase names (at most 32 characters)")
		}
		seen[p.Name] = true
		u, err := url.Parse(p.URL)
		if err != nil || u.Host == "" || u.User != nil || u.Fragment != "" || (u.Scheme != "http" && u.Scheme != "https") {
			return fmt.Errorf("target_profiles %s requires an HTTP(S) URL without credentials or fragment", p.Name)
		}
		if p.ExpectedBody == "" || len(p.ExpectedBody) > int(probe.MaxResponseBytes) {
			return fmt.Errorf("target_profiles %s requires a bounded nonempty expected_body", p.Name)
		}
	}
	return nil
}

type targetControlEntry struct {
	at      time.Time
	healthy bool
	running bool
}
type targetProfileState struct {
	mu          sync.Mutex
	control     map[string]targetControlEntry
	next        atomic.Uint64
	probeFunc   func(context.Context, string, TargetProfile) probe.Result
	controlFunc func(context.Context, TargetProfile) error
}

func (r *Runner) targetControlHealthy(ctx context.Context, profile TargetProfile) bool {
	a := &r.targets
	now := r.nowFunc()
	interval := r.config.ControlProbeInterval
	if interval <= 0 {
		interval = defaultControlProbeInterval
	}
	id := profile.fingerprint()
	a.mu.Lock()
	if a.control == nil {
		a.control = map[string]targetControlEntry{}
	}
	entry := a.control[id]
	if !entry.at.IsZero() && now.Sub(entry.at) < interval {
		a.mu.Unlock()
		return entry.healthy
	}
	if entry.running {
		a.mu.Unlock()
		return false
	}
	entry.running = true
	a.control[id] = entry
	a.mu.Unlock()
	check := a.controlFunc
	if check == nil {
		check = r.directTargetProfileControl
	}
	checkCtx, cancel := context.WithTimeout(ctx, r.config.RequestTimeout)
	err := check(checkCtx, profile)
	cancel()
	a.mu.Lock()
	if ctx.Err() != nil {
		delete(a.control, id)
	} else {
		a.control[id] = targetControlEntry{at: now, healthy: err == nil}
	}
	a.mu.Unlock()
	return err == nil
}

func (r *Runner) directTargetProfileControl(ctx context.Context, profile TargetProfile) error {
	target, err := url.Parse(profile.URL)
	if err != nil {
		return err
	}
	query := target.Query()
	query.Set("_fpa_control", fmt.Sprint(r.nowFunc().UnixNano()))
	target.RawQuery = query.Encode()
	transport := &http.Transport{
		DialContext:         endpoint.PublicDialer{ConnectTimeout: r.config.ProbeConnectTimeout}.DialContext,
		TLSHandshakeTimeout: r.config.RequestTimeout, ResponseHeaderTimeout: r.config.RequestTimeout, DisableKeepAlives: true,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("User-Agent", "FreeProxyAPI/0.1 (target-control)")
	req.Header.Set("Cache-Control", "no-cache")
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	return checkControlResponse(response, "standard", profile.ExpectedBody)
}

// Additional target work stays inside the remaining claim lease, leaving a
// small margin for the already completed base result to commit.
func (r *Runner) sampleTargetProfileWithinLease(ctx context.Context, proxyURL string, result probe.Result, meta *store.OutcomeMeta, started time.Time) {
	if r.config.LeaseTTL <= 0 {
		r.sampleTargetProfile(ctx, proxyURL, result, meta)
		return
	}
	remaining := r.config.LeaseTTL - time.Since(started) - time.Second
	if remaining <= 0 {
		return
	}
	checkCtx, cancel := context.WithTimeout(ctx, remaining)
	defer cancel()
	r.sampleTargetProfile(checkCtx, proxyURL, result, meta)
}

func (r *Runner) sampleTargetProfile(ctx context.Context, proxyURL string, result probe.Result, meta *store.OutcomeMeta) {
	profiles := r.config.TargetProfiles
	if !result.OK || len(profiles) == 0 || ctx.Err() != nil {
		return
	}
	every := r.config.TargetProbeEvery
	if every < 1 {
		every = defaultTargetProbeEvery
	}
	draw := r.accuracy.sampleIntN
	if draw == nil {
		draw = rand.IntN
	}
	if every > 1 && draw(every) != 0 {
		return
	}
	profile := profiles[(r.targets.next.Add(1)-1)%uint64(len(profiles))]
	observation := store.TargetProfileResult{Fingerprint: profile.fingerprint(), State: "unknown", CheckedAt: r.nowFunc().UnixMilli()}
	if !r.targetControlHealthy(ctx, profile) {
		if ctx.Err() != nil {
			return
		}
		observation.ErrorCode = "origin_unavailable"
	} else {
		allowed, err := r.takeProbePermit(ctx)
		if err != nil || !allowed {
			return
		}
		check := r.targets.probeFunc
		if check == nil {
			check = func(ctx context.Context, proxyURL string, profile TargetProfile) probe.Result {
				return probe.TestExpectBodyWithConnectTimeout(ctx, proxyURL, profile.URL, r.config.RequestTimeout, r.config.ProbeConnectTimeout, profile.ExpectedBody)
			}
		}
		checked := check(ctx, proxyURL, profile)
		r.metrics.AddProbeTraffic(checked.UploadBytes, checked.DownloadBytes)
		if ctx.Err() != nil {
			return
		}
		observation.CheckedAt = r.nowFunc().UnixMilli()
		observation.HTTPStatus = checked.StatusCode
		observation.LatencyMs = checked.Duration.Milliseconds()
		if checked.OK {
			observation.State = "pass"
		} else {
			observation.ErrorCode = probe.ClassifyError(checked)
			if checked.StatusCode > 0 {
				observation.State = "fail"
			}
		}
	}
	active := make(map[string]string, len(profiles))
	for _, p := range profiles {
		active[p.Name] = p.fingerprint()
	}
	meta.TargetProfiles = &store.TargetProfileUpdate{Active: active, Checks: map[string]store.TargetProfileResult{profile.Name: observation}}
}
