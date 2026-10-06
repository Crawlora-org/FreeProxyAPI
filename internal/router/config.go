// Package router renders country-aware GOST v3 forwarding configurations.
package router

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

type BuildOptions struct {
	CountryField   string
	Countries      []string
	ProxyUsername  string
	ProxyPassword  string
	APIUsername    string
	APIPassword    string
	APIAddress     string
	Selector       string
	MaxFails       int
	FailTimeout    string
	RequestRetries int
}

type gostConfig struct {
	Services []gostService `json:"services,omitempty"`
	Chains   []gostChain   `json:"chains,omitempty"`
	API      gostAPI       `json:"api"`
}

type gostService struct {
	Name     string       `json:"name"`
	Addr     string       `json:"addr"`
	Handler  gostHandler  `json:"handler"`
	Listener gostListener `json:"listener"`
}

type gostHandler struct {
	Type    string   `json:"type"`
	Auth    gostAuth `json:"auth"`
	Chain   string   `json:"chain"`
	Retries int      `json:"retries,omitempty"`
}

type gostListener struct {
	Type string `json:"type"`
}

type gostChain struct {
	Name string    `json:"name"`
	Hops []gostHop `json:"hops"`
}

type gostHop struct {
	Name     string       `json:"name"`
	Selector gostSelector `json:"selector"`
	Nodes    []gostNode   `json:"nodes"`
}

type gostSelector struct {
	Strategy    string `json:"strategy"`
	MaxFails    int    `json:"maxFails"`
	FailTimeout string `json:"failTimeout"`
}

type gostNode struct {
	Name      string        `json:"name"`
	Addr      string        `json:"addr"`
	Connector gostConnector `json:"connector"`
	Dialer    gostDialer    `json:"dialer"`
}

type gostConnector struct {
	Type string `json:"type"`
}

type gostDialer struct {
	Type string   `json:"type"`
	TLS  *gostTLS `json:"tls,omitempty"`
}

type gostTLS struct {
	Secure     bool   `json:"secure"`
	ServerName string `json:"serverName,omitempty"`
}

type gostAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type gostAPI struct {
	Addr string   `json:"addr"`
	Auth gostAuth `json:"auth"`
}

// BuildConfig creates an authenticated all-country service on port 3128 and
// one service per requested country starting at port 3129. Explicit ports make
// country selection reliable for normal HTTP proxy clients, including CONNECT.
func BuildConfig(proxies []store.QueryProxy, opts BuildOptions) ([]byte, error) {
	opts = withDefaults(opts)
	if opts.ProxyUsername == "" || opts.ProxyPassword == "" {
		return nil, fmt.Errorf("GOST proxy credentials are required")
	}
	if opts.APIUsername == "" || opts.APIPassword == "" {
		return nil, fmt.Errorf("GOST API credentials are required")
	}
	if opts.Selector != "round" && opts.Selector != "rand" && opts.Selector != "fifo" && opts.Selector != "hash" && opts.Selector != "parallel" {
		return nil, fmt.Errorf("unsupported GOST selector %q", opts.Selector)
	}
	if len(proxies) == 0 {
		return nil, fmt.Errorf("cannot render GOST config without validated proxies")
	}

	allNodes := buildNodes(proxies, opts, "")
	if len(allNodes) == 0 {
		return nil, fmt.Errorf("none of the validated proxies can be represented by GOST")
	}
	services := []gostService{makeService("unified-http", 3128, "all-validated", opts)}
	chains := []gostChain{makeChain("all-validated", "all-country-pool", allNodes, opts)}
	for index, country := range normalizeCountries(opts.Countries) {
		nodes := buildNodes(proxies, opts, country)
		if len(nodes) == 0 {
			continue
		}
		chainName := "country-" + strings.ToLower(country)
		services = append(services, makeService(country+"-http", 3129+index, chainName, opts))
		chains = append(chains, makeChain(chainName, strings.ToLower(country)+"-pool", nodes, opts))
	}

	config := gostConfig{
		Services: services,
		Chains:   chains,
		API:      gostAPI{Addr: opts.APIAddress, Auth: gostAuth{Username: opts.APIUsername, Password: opts.APIPassword}},
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal GOST config: %w", err)
	}
	return append(data, '\n'), nil
}

// BuildBootstrapConfig renders a config that only enables the authenticated
// GOST Web API. GOST can start from it before the first successful sync; the
// proxy listeners open on the first reload, so a readiness probe on them stays
// false until real upstreams exist.
func BuildBootstrapConfig(opts BuildOptions) ([]byte, error) {
	opts = withDefaults(opts)
	if opts.APIUsername == "" || opts.APIPassword == "" {
		return nil, fmt.Errorf("GOST API credentials are required")
	}
	config := gostConfig{
		API: gostAPI{Addr: opts.APIAddress, Auth: gostAuth{Username: opts.APIUsername, Password: opts.APIPassword}},
	}
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal GOST bootstrap config: %w", err)
	}
	return append(data, '\n'), nil
}

func makeService(name string, port int, chain string, opts BuildOptions) gostService {
	return gostService{
		Name: name,
		Addr: fmt.Sprintf(":%d", port),
		Handler: gostHandler{
			Type:    "http",
			Auth:    gostAuth{Username: opts.ProxyUsername, Password: opts.ProxyPassword},
			Chain:   chain,
			Retries: opts.RequestRetries,
		},
		Listener: gostListener{Type: "tcp"},
	}
}

func makeChain(name, hopName string, nodes []gostNode, opts BuildOptions) gostChain {
	return gostChain{
		Name: name,
		Hops: []gostHop{{
			Name:     hopName,
			Selector: gostSelector{Strategy: opts.Selector, MaxFails: opts.MaxFails, FailTimeout: opts.FailTimeout},
			Nodes:    nodes,
		}},
	}
}

func withDefaults(opts BuildOptions) BuildOptions {
	if opts.CountryField == "" {
		opts.CountryField = "entry"
	}
	if len(opts.Countries) == 0 {
		opts.Countries = []string{"US"}
	}
	if opts.APIAddress == "" {
		opts.APIAddress = "127.0.0.1:18080"
	}
	if opts.Selector == "" {
		opts.Selector = "round"
	}
	if opts.MaxFails <= 0 {
		opts.MaxFails = 2
	}
	if opts.FailTimeout == "" {
		opts.FailTimeout = "30s"
	}
	if opts.RequestRetries < 0 {
		opts.RequestRetries = 0
	}
	return opts
}

func buildNodes(proxies []store.QueryProxy, opts BuildOptions, onlyCountry string) []gostNode {
	nodes := make([]gostNode, 0, len(proxies))
	seen := make(map[string]struct{}, len(proxies))
	for _, proxy := range proxies {
		country := proxy.Country
		if strings.EqualFold(opts.CountryField, "exit") {
			country = proxy.ExitCountry
		}
		country = normalizeCountry(country)
		if onlyCountry != "" && country != onlyCountry {
			continue
		}
		node, key, err := buildNode(proxy, country)
		if err != nil {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		nodes = append(nodes, node)
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return nodes
}

// buildNode re-validates each upstream with endpoint.ParseProxy before it is
// written into the GOST config, because GOST dials these hosts directly and
// the API response is not trusted to be safe. Unsafe IP literals, localhost
// forms, inet_aton-style numeric hosts, and wildcard-DNS names embedding an
// unsafe IPv4 are dropped. Ordinary hostnames are kept because feeds may
// legitimately carry them; GOST resolves those itself.
func buildNode(proxy store.QueryProxy, country string) (gostNode, string, error) {
	if !strings.Contains(proxy.URL, "://") {
		return gostNode{}, "", fmt.Errorf("invalid proxy URL")
	}
	parsed, err := endpoint.ParseProxy(proxy.URL, "")
	if err != nil {
		return gostNode{}, "", fmt.Errorf("invalid proxy URL: %w", err)
	}
	scheme := parsed.Scheme
	connector := ""
	switch scheme {
	case "http", "https":
		connector = "http"
	case "socks4", "socks4a":
		connector = "socks4"
	case "socks5", "socks5h":
		connector = "socks5"
	default:
		return gostNode{}, "", fmt.Errorf("unsupported proxy scheme %q", scheme)
	}
	addr := net.JoinHostPort(parsed.Host, strconv.Itoa(int(parsed.Port)))
	dialer := gostDialer{Type: "tcp"}
	if scheme == "https" {
		dialer = gostDialer{Type: "tls", TLS: &gostTLS{Secure: true, ServerName: parsed.Host}}
	}
	return gostNode{
		Name:      nodeName(country, scheme, addr),
		Addr:      addr,
		Connector: gostConnector{Type: connector},
		Dialer:    dialer,
	}, scheme + "://" + addr, nil
}

func normalizeCountry(raw string) string {
	country := strings.ToUpper(strings.TrimSpace(raw))
	if len(country) != 2 || country[0] < 'A' || country[0] > 'Z' || country[1] < 'A' || country[1] > 'Z' {
		return ""
	}
	return country
}

func normalizeCountries(raw []string) []string {
	seen := make(map[string]struct{}, len(raw))
	var countries []string
	for _, value := range raw {
		country := normalizeCountry(value)
		if country == "" {
			continue
		}
		if _, ok := seen[country]; ok {
			continue
		}
		seen[country] = struct{}{}
		countries = append(countries, country)
	}
	sort.Strings(countries)
	return countries
}

func nodeName(country, scheme, host string) string {
	label := country
	if label == "" {
		label = "unknown"
	}
	name := label + "-" + scheme + "-" + host
	name = strings.NewReplacer(".", "-", ":", "-", "[", "", "]", "").Replace(name)
	return strings.ToLower(name)
}
