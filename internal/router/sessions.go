package router

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

const maxNamedSessions = 128

const sessionStableExitMaxAge = 24 * time.Hour

// SessionListener describes one authenticated GOST listener pinned to one
// validated upstream. Its ID and port are operator controlled and validated
// before they become GOST service or chain names.
type SessionListener struct {
	ID    string
	Port  int
	Proxy store.QueryProxy
}

type sessionPin struct {
	URL       string
	ExitIP    string
	ExpiresAt time.Time
}

// ParseSessionIDs parses comma-separated ASCII session IDs.
func ParseSessionIDs(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	ids := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		id := strings.TrimSpace(part)
		if !validSessionID(id) {
			return nil, fmt.Errorf("invalid session ID %q: use 1-32 ASCII letters, digits, underscore, or hyphen, starting with a letter or digit", id)
		}
		ids = append(ids, id)
	}
	if err := validateSessionIDs(ids); err != nil {
		return nil, err
	}
	return ids, nil
}

// ValidateSessionOptions bounds named session count, IDs, TTL, and listener
// ports. Optional API addresses let callers reject Web API listener collisions.
func ValidateSessionOptions(ids []string, ttl time.Duration, basePort int, countries []string, apiAddresses ...string) error {
	if err := validateSessionIDs(ids); err != nil {
		return err
	}
	if ttl <= 0 {
		return fmt.Errorf("session TTL must be positive")
	}
	if basePort < 1024 || basePort > 65535 {
		return fmt.Errorf("session base port must be from 1024 to 65535")
	}
	if len(ids) > 0 && basePort+len(ids)-1 > 65535 {
		return fmt.Errorf("session listener ports exceed 65535")
	}
	countryList := normalizeCountries(countries)
	if len(countryList) == 0 {
		countryList = []string{"US"} // BuildConfig defaults to a US listener.
	}
	lastPort := basePort + len(ids) - 1
	lastCountryPort := 3128 + len(countryList)
	if len(ids) > 0 && basePort <= lastCountryPort && lastPort >= 3128 {
		return fmt.Errorf("session listener ports %d-%d overlap normal or country listeners 3128-%d", basePort, lastPort, lastCountryPort)
	}
	if len(ids) > 0 && len(apiAddresses) > 0 {
		if apiPort, ok := listenPort(apiAddresses[0]); ok && basePort <= apiPort && lastPort >= apiPort {
			return fmt.Errorf("session listener ports %d-%d overlap GOST API port %d", basePort, lastPort, apiPort)
		}
	}
	return nil
}

func listenPort(address string) (int, bool) {
	_, rawPort, err := net.SplitHostPort(address)
	if err != nil {
		return 0, false
	}
	port, err := strconv.Atoi(rawPort)
	return port, err == nil && port >= 1 && port <= 65535
}

func validateSessionIDs(ids []string) error {
	if len(ids) > maxNamedSessions {
		return fmt.Errorf("at most %d named sessions are allowed", maxNamedSessions)
	}
	seen := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if !validSessionID(id) {
			return fmt.Errorf("invalid session ID %q", id)
		}
		if _, ok := seen[id]; ok {
			return fmt.Errorf("duplicate session ID %q", id)
		}
		seen[id] = struct{}{}
	}
	return nil
}

func validSessionID(id string) bool {
	if len(id) == 0 || len(id) > 32 {
		return false
	}
	for i := 0; i < len(id); i++ {
		c := id[i]
		alpha := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
		digit := c >= '0' && c <= '9'
		if !(alpha || digit || (i > 0 && (c == '_' || c == '-'))) {
			return false
		}
	}
	return true
}

// SessionPortMap gives IDs ports in sorted-ID order, independent of the
// operator's input order. It assumes IDs and basePort have been validated.
func SessionPortMap(ids []string, basePort int) map[string]int {
	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)
	ports := make(map[string]int, len(sorted))
	for index, id := range sorted {
		ports[id] = basePort + index
	}
	return ports
}

func reconcileSessionListeners(state *syncer, opts SyncOptions, proxies []store.QueryProxy, now time.Time) []SessionListener {
	if len(opts.Sessions) == 0 {
		state.sessionPins = nil
		return nil
	}
	if state.sessionPins == nil {
		state.sessionPins = make(map[string]sessionPin, len(opts.Sessions))
	}
	active := make(map[string]struct{}, len(opts.Sessions))
	for _, id := range opts.Sessions {
		active[id] = struct{}{}
	}
	for id := range state.sessionPins {
		if _, ok := active[id]; !ok {
			delete(state.sessionPins, id)
		}
	}

	candidates := make(map[string]store.QueryProxy, len(proxies))
	for _, proxy := range proxies {
		if strings.EqualFold(strings.TrimSpace(proxy.LastStatus), "failed") || proxy.URL == "" {
			continue
		}
		if opts.SessionStableExit && !sessionHasStableExit(proxy, now) {
			continue
		}
		country := proxy.Country
		if strings.EqualFold(opts.CountryField, "exit") {
			country = proxy.ExitCountry
		}
		if _, _, err := buildNode(proxy, normalizeCountry(country)); err != nil {
			continue
		}
		candidates[proxy.URL] = proxy
	}
	ordered := make([]store.QueryProxy, 0, len(candidates))
	for _, proxy := range candidates {
		ordered = append(ordered, proxy)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].URL < ordered[j].URL })
	ports := SessionPortMap(opts.Sessions, opts.SessionBasePort)
	ids := append([]string(nil), opts.Sessions...)
	sort.Strings(ids)
	listeners := make([]SessionListener, 0, len(ids))
	for _, id := range ids {
		pin, exists := state.sessionPins[id]
		if exists && now.Before(pin.ExpiresAt) {
			proxy, available := candidates[pin.URL]
			valid := available
			if valid && pin.ExitIP != "" && proxy.ExitIP != "" && pin.ExitIP != proxy.ExitIP {
				valid = false
			}
			if valid {
				if proxy.ExitIP != "" {
					pin.ExitIP = proxy.ExitIP
					state.sessionPins[id] = pin
				}
				listeners = append(listeners, SessionListener{ID: id, Port: ports[id], Proxy: proxy})
				continue
			}
			if !opts.SessionFailover || len(ordered) == 0 {
				continue
			}
			proxy = ordered[sessionCandidateIndex(id, len(ordered))]
			pin = sessionPin{URL: proxy.URL, ExitIP: proxy.ExitIP, ExpiresAt: now.Add(opts.SessionTTL)}
			state.sessionPins[id] = pin
			listeners = append(listeners, SessionListener{ID: id, Port: ports[id], Proxy: proxy})
			continue
		}
		if len(ordered) == 0 {
			delete(state.sessionPins, id)
			continue
		}
		proxy := ordered[sessionCandidateIndex(id, len(ordered))]
		state.sessionPins[id] = sessionPin{URL: proxy.URL, ExitIP: proxy.ExitIP, ExpiresAt: now.Add(opts.SessionTTL)}
		listeners = append(listeners, SessionListener{ID: id, Port: ports[id], Proxy: proxy})
	}
	return listeners
}

// sessionHasStableExit keeps the evidence gate local to named-session
// selection. It does not narrow the ordinary unified or country pool.
func sessionHasStableExit(proxy store.QueryProxy, now time.Time) bool {
	if proxy.StableExit == nil || !*proxy.StableExit || proxy.ExitIPSamples == nil || *proxy.ExitIPSamples < 2 || proxy.ExitCheckedAt <= 0 {
		return false
	}
	nowMs := now.UnixMilli()
	if proxy.ExitCheckedAt > nowMs || nowMs-proxy.ExitCheckedAt > sessionStableExitMaxAge.Milliseconds() {
		return false
	}
	ip, err := netip.ParseAddr(strings.TrimSpace(proxy.ExitIP))
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	return ip.IsGlobalUnicast() && !ip.IsPrivate() && !ip.IsLoopback() && !ip.IsLinkLocalUnicast() && !ip.IsUnspecified() && !ip.IsMulticast()
}

func sessionCandidateIndex(id string, candidateCount int) int {
	if candidateCount <= 1 {
		return 0
	}
	digest := sha256.Sum256([]byte(id))
	return int(binary.BigEndian.Uint64(digest[:8]) % uint64(candidateCount))
}

// dropSessionListeners removes named-session services and chains from the
// currently written config after a refresh failure. The normal pool stays as
// it was, while no listener can keep routing a stale session pin.
func (s *syncer) dropSessionListeners(ctx context.Context, client *http.Client, opts SyncOptions) error {
	data, err := os.ReadFile(opts.OutputFile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read GOST config to remove session listeners: %w", err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return fmt.Errorf("parse GOST config to remove session listeners: %w", err)
	}
	changed := false
	services := config.Services[:0]
	for _, service := range config.Services {
		if strings.HasPrefix(service.Name, "session-") {
			changed = true
			continue
		}
		services = append(services, service)
	}
	config.Services = services
	chains := config.Chains[:0]
	for _, chain := range config.Chains {
		if strings.HasPrefix(chain.Name, "session-") {
			changed = true
			continue
		}
		chains = append(chains, chain)
	}
	config.Chains = chains
	if !changed && !s.reloadPending {
		return nil
	}
	updated, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal GOST config without session listeners: %w", err)
	}
	if _, err := writeIfChanged(opts.OutputFile, append(updated, '\n')); err != nil {
		return fmt.Errorf("write GOST config without session listeners: %w", err)
	}
	s.reloadPending = true
	if err := reloadGOST(ctx, client, opts); err != nil {
		return fmt.Errorf("reload GOST config without session listeners: %w", err)
	}
	s.reloadPending = false
	return nil
}
