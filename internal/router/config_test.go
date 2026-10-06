package router

import (
	"encoding/json"
	"testing"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestBuildConfigRoutesByCountryAndSupportsMixedSchemes(t *testing.T) {
	data, err := BuildConfig([]store.QueryProxy{
		{URL: "http://192.0.2.10:8080", Country: "US", Scheme: "http"},
		{URL: "socks5://192.0.2.11:1080", Country: "DE", Scheme: "socks5"},
		{URL: "https://192.0.2.12:8443", Country: "US", Scheme: "https"},
	}, BuildOptions{
		Countries:     []string{"US", "DE"},
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		APIUsername: "api-user", APIPassword: "api-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if got := config.Services[0].Handler.Auth.Username; got != "proxy-user" {
		t.Fatalf("proxy auth username = %q", got)
	}
	if got := len(config.Services); got != 3 {
		t.Fatalf("service count = %d, want 3", got)
	}
	if got := len(config.Chains[0].Hops[0].Nodes); got != 3 {
		t.Fatalf("all-country node count = %d, want 3", got)
	}
	serviceByName := make(map[string]gostService, len(config.Services))
	chainByName := make(map[string]gostChain, len(config.Chains))
	for _, service := range config.Services {
		serviceByName[service.Name] = service
	}
	for _, chain := range config.Chains {
		chainByName[chain.Name] = chain
	}
	if serviceByName["US-http"].Addr != ":3130" || serviceByName["DE-http"].Addr != ":3129" {
		t.Fatalf("country service ports = US:%q DE:%q", serviceByName["US-http"].Addr, serviceByName["DE-http"].Addr)
	}
	if got := len(chainByName["country-us"].Hops[0].Nodes); got != 2 {
		t.Fatalf("US node count = %d, want 2", got)
	}
	if got := len(chainByName["country-de"].Hops[0].Nodes); got != 1 {
		t.Fatalf("DE node count = %d, want 1", got)
	}
	var sawTLS bool
	for _, node := range config.Chains[0].Hops[0].Nodes {
		if node.Dialer.Type == "tls" && node.Dialer.TLS != nil && node.Dialer.TLS.Secure {
			sawTLS = true
		}
	}
	if !sawTLS {
		t.Fatal("HTTPS upstream TLS dialer missing")
	}
}

func TestBuildConfigCanRouteByExitCountry(t *testing.T) {
	data, err := BuildConfig([]store.QueryProxy{{
		URL: "socks4://192.0.2.20:1080", Country: "NL", ExitCountry: "US",
	}}, BuildOptions{
		CountryField: "exit", Countries: []string{"US"},
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		APIUsername: "api-user", APIPassword: "api-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if got := len(config.Chains[1].Hops[0].Nodes); got != 1 {
		t.Fatalf("exit-country node count = %d, want 1", got)
	}
	if config.Services[1].Name != "US-http" {
		t.Fatalf("country service = %q, want US-http", config.Services[1].Name)
	}
}

func TestBuildConfigRevalidatesUpstreamHosts(t *testing.T) {
	data, err := BuildConfig([]store.QueryProxy{
		{URL: "http://localhost.:8080", Country: "US"},
		{URL: "http://127.0.0.1.nip.io:8080", Country: "US"},
		{URL: "http://0x7f.0.0.1:8080", Country: "US"},
		{URL: "http://2130706433:8080", Country: "US"},
		{URL: "http://10.0.0.5:8080", Country: "US"},
		{URL: "socks5://[fd00::1]:1080", Country: "US"},
		{URL: "http://user:pass@203.0.113.9:8080", Country: "US"},
		{URL: "203.0.113.8:8080", Country: "US"},
		{URL: "socks5://[2606:4700::1111]:1080", Country: "US"},
		{URL: "https://proxy.example.net:8443", Country: "US"},
	}, BuildOptions{
		Countries:     []string{"US"},
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		APIUsername: "api-user", APIPassword: "api-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	var config gostConfig
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	nodes := config.Chains[0].Hops[0].Nodes
	addrs := make(map[string]gostNode, len(nodes))
	for _, node := range nodes {
		addrs[node.Addr] = node
	}
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v, want only the public IPv6 and hostname upstreams", nodes)
	}
	if _, ok := addrs["[2606:4700::1111]:1080"]; !ok {
		t.Fatalf("IPv6 upstream missing or not bracketed: %+v", nodes)
	}
	host, ok := addrs["proxy.example.net:8443"]
	if !ok || host.Dialer.TLS == nil || host.Dialer.TLS.ServerName != "proxy.example.net" {
		t.Fatalf("hostname HTTPS upstream missing or wrong TLS: %+v", nodes)
	}
}

func TestBuildConfigFailsWhenAllUpstreamsUnsafe(t *testing.T) {
	_, err := BuildConfig([]store.QueryProxy{{URL: "http://127.0.0.1:8080", Country: "US"}}, BuildOptions{
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		APIUsername: "api-user", APIPassword: "api-pass",
	})
	if err == nil {
		t.Fatal("BuildConfig rendered a config from only unsafe upstreams")
	}
}
