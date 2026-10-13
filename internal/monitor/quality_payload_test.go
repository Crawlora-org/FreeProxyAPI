package monitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func qualityPayloadTargetProfile() TargetProfile {
	return TargetProfile{Name: "residential", URL: "https://profile.example.test/health", ExpectedBody: "healthy"}
}

func TestMaximumPublicProxyPageWithQualityMetadataFitsCache(t *testing.T) {
	const rows = publicProxyMaxLimit
	profiles := make(map[string]store.TargetProfileResult, store.MaxTargetProfiles)
	for i := 0; i < store.MaxTargetProfiles; i++ {
		name := fmt.Sprintf("p%02d%s", i, strings.Repeat("x", 29))
		if len(name) != 32 {
			t.Fatalf("profile fixture name length=%d, want 32", len(name))
		}
		profiles[name] = store.TargetProfileResult{
			State:      []string{"pass", "fail", "unknown"}[i%3],
			CheckedAt:  1_900_000_000_000,
			HTTPStatus: []int{200, 599, 0}[i%3],
			LatencyMs:  999_999,
			ErrorCode:  strings.Repeat("e", 32),
			// Fingerprints are removed by filterActiveTargetProfiles before
			// the public response is marshaled.
			Fingerprint: "",
		}
	}
	escapedOrg := strings.Repeat("\"\\\n\t", 64)
	if len(escapedOrg) != 256 {
		t.Fatalf("organization fixture has %d source bytes, want 256", len(escapedOrg))
	}
	changed, falseValue, percent := int64(999_999_999), false, int64(100)
	checkedAt := int64(1_900_000_000_000)
	proxies := make([]store.QueryProxy, rows)
	for i := range proxies {
		host := maximumDNSName(i)
		proxyURL := "http://" + host + ":65535"
		if _, err := endpoint.ParseProxy(proxyURL, ""); err != nil {
			t.Fatalf("synthetic source URL %q is invalid: %v", proxyURL, err)
		}
		if parsed, err := url.Parse(proxyURL); err != nil || parsed.Hostname() != host {
			t.Fatalf("synthetic source URL parse host=%q err=%v", parsed.Hostname(), err)
		}
		proxies[i] = store.QueryProxy{
			TargetProfiles:             profiles,
			URL:                        proxyURL,
			Scheme:                     "http",
			Country:                    "US",
			ExitCountry:                "ZZ",
			ExitIP:                     "203.0.113.255",
			ExitCheckedAt:              checkedAt,
			StableExit:                 &falseValue,
			ExitASN:                    "AS4294967295",
			ExitOrg:                    escapedOrg,
			Hosting:                    &falseValue,
			ExitIPChanges:              &changed,
			ExitIPChangedAt:            checkedAt,
			ExitIPSamples:              &changed,
			GeoMismatch:                true,
			ASN:                        "AS4294967295",
			Anonymity:                  "transparent",
			LatencyMs:                  999_999,
			JitterMs:                   999_999,
			OkRatioPct:                 100,
			LastCheckedAt:              checkedAt,
			LastStatus:                 "failed",
			LastOkAt:                   checkedAt,
			SuccessStreak:              &changed,
			Reliability24hSamples:      999_999_999,
			Reliability24hPct:          &percent,
			Reliability7dSamples:       999_999_999,
			Reliability7dPct:           &percent,
			ReliabilityFirstSampleAt:   checkedAt,
			ReliabilityCoverageStartAt: checkedAt,
			AnonymityCheckedAt:         checkedAt,
			HTTPSOK:                    &falseValue,
			HTTPSCheckedAt:             checkedAt,
			HTTPSTunnelScheme:          "https",
			Tampered:                   &falseValue,
			TamperCheckedAt:            checkedAt,
		}
	}
	body, err := json.Marshal(publicProxiesPayload{
		Count: rows, Proxies: proxies, Limit: rows, Offset: 0, HasMore: false,
	})
	if err != nil {
		t.Fatal(err)
	}
	body = append(body, '\n')
	if len(body) > publicProxyCacheEntrySize {
		t.Fatalf("maximum quality page is %d bytes, exceeds per-entry limit %d", len(body), publicProxyCacheEntrySize)
	}
	if len(body) > publicProxyCacheBytes {
		t.Fatalf("maximum quality page is %d bytes, exceeds total cache limit %d", len(body), publicProxyCacheBytes)
	}

	cache := newPublicProxyResponseCache(1, publicProxyCacheBytes, publicProxyCacheEntrySize, publicProxyCacheTTL, time.Now)
	key := publicProxyCacheKey(store.ProxyFilter{TargetProfile: "residential", StableExit: true}, 0)
	cache.put(key, body)
	cached, remaining, ok := cache.get(key)
	if !ok || remaining <= 0 || !bytes.Equal(cached, body) {
		t.Fatalf("maximum quality page did not round-trip through cache: hit=%t remaining=%s body=%d bytes", ok, remaining, len(body))
	}
	t.Logf("maximum public page: %d bytes; per-entry headroom: %d bytes", len(body), publicProxyCacheEntrySize-len(body))
}

func maximumDNSName(index int) string {
	first := fmt.Sprintf("p%04d", index)
	first += strings.Repeat("a", 63-len(first))
	return strings.Join([]string{first, strings.Repeat("b", 63), strings.Repeat("c", 63), strings.Repeat("d", 61)}, ".")
}

func TestStrictQualityQueriesDoNotServeStalePublicCacheEntries(t *testing.T) {
	for _, tc := range []struct {
		name  string
		query string
		setup func(*Runner)
	}{
		{name: "stable exit", query: "stable_exit=true"},
		{
			name:  "target profile",
			query: "target_profile=residential",
			setup: func(runner *Runner) {
				runner.config.TargetProfiles = []TargetProfile{qualityPayloadTargetProfile()}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mini, health, runner, clock := startStaleTestServer(t, "quality-stale-"+strings.ReplaceAll(tc.name, " ", "-"))
			if tc.setup != nil {
				tc.setup(runner)
			}
			path := "/proxies?limit=5&" + tc.query
			first := serve(health, path)
			if first.Code != http.StatusOK {
				t.Fatalf("initial query: status=%d body=%s", first.Code, first.Body.String())
			}
			mini.Close()
			clock.advance(publicProxyCacheTTL + time.Second)
			response := serve(health, path)
			if response.Code != http.StatusServiceUnavailable {
				t.Fatalf("strict quality query served stale response: status=%d body=%s", response.Code, response.Body.String())
			}
			if got := runner.metrics.publicStaleResponses.Load(); got != 0 {
				t.Fatalf("strict quality query stale responses=%d, want 0", got)
			}
		})
	}
}
