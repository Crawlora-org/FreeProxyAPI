package monitor

import (
	"context"
	"log"
	"math/rand/v2"

	"github.com/Crawlora-org/FreeProxyAPI/internal/probe"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

// sampleHTTPS runs the sampled HTTPS-capability check after a successful
// probe and records its outcome in meta. Roughly one in https_probe_every
// successes is sampled; each check spends one global budget permit and is
// skipped (not queued) when the budget is exhausted.
func (r *Runner) sampleHTTPS(ctx context.Context, proxyURL string, result probe.Result, meta *store.OutcomeMeta) {
	if r.config.HTTPSProbeTarget == "" || !result.OK || ctx.Err() != nil {
		return
	}
	every := r.config.HTTPSProbeEvery
	if every < 1 {
		every = defaultHTTPSProbeEvery
	}
	if every > 1 {
		draw := r.accuracy.sampleIntN
		if draw == nil {
			draw = rand.IntN
		}
		if draw(every) != 0 {
			return
		}
	}
	allowed, err := r.takeProbePermit(ctx)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("https probe budget failed: %v", err)
		}
		return
	}
	if !allowed {
		r.accuracy.metrics.httpsBudgetSkipped.Add(1)
		return
	}
	check := r.accuracy.httpsProbeFunc
	if check == nil {
		check = func(ctx context.Context, proxyURL string) probe.HTTPSResult {
			return probe.TestHTTPS(ctx, proxyURL, r.config.HTTPSProbeTarget, r.config.RequestTimeout, r.config.ProbeConnectTimeout, r.config.HTTPSProbeExpectedBody)
		}
	}
	outcome := check(ctx, proxyURL)
	r.metrics.AddProbeTraffic(outcome.UploadBytes, outcome.DownloadBytes)
	if ctx.Err() != nil {
		return
	}
	if outcome.ProxyTLSFallback {
		r.accuracy.metrics.httpsProxyTLSFallback.Add(1)
	}
	if !outcome.OK && !r.controlHealthy() {
		// The origin may be failing for everyone; do not mark HTTPS broken.
		return
	}
	meta.HTTPSChecked = true
	meta.HTTPSOK = outcome.OK
	if outcome.OK {
		meta.HTTPSTunnelScheme = outcome.TunnelScheme
		r.accuracy.metrics.httpsResults[1].Add(1)
	} else {
		r.accuracy.metrics.httpsResults[0].Add(1)
	}
}
