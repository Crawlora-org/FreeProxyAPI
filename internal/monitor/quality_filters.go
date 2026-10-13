package monitor

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func parseQualityInteger(raw, name string, maximum int64) (int64, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 || value > maximum {
		return 0, fmt.Errorf("%s must be an integer from 0 to %d", name, maximum)
	}
	return value, nil
}

func (r *Runner) bindTargetProfile(filter *store.ProxyFilter) error {
	if filter.TargetProfile == "" {
		return nil
	}
	for _, profile := range r.config.TargetProfiles {
		if profile.Name == filter.TargetProfile {
			filter.TargetProfileFingerprint = profile.fingerprint()
			return nil
		}
	}
	return fmt.Errorf("target_profile is not configured")
}

func (r *Runner) filterActiveTargetProfiles(proxies []store.QueryProxy) {
	active := make(map[string]string, len(r.config.TargetProfiles))
	for _, p := range r.config.TargetProfiles {
		active[p.Name] = p.fingerprint()
	}
	for i := range proxies {
		for name, result := range proxies[i].TargetProfiles {
			if fingerprint, found := active[name]; !found || result.Fingerprint != fingerprint {
				delete(proxies[i].TargetProfiles, name)
				continue
			}
			// Fingerprints bind stored checks to configuration but do not add
			// useful per-row information to the public response.
			result.Fingerprint = ""
			proxies[i].TargetProfiles[name] = result
		}
		if len(proxies[i].TargetProfiles) == 0 {
			proxies[i].TargetProfiles = nil
		}
	}
}
