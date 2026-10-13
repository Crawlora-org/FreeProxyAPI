package router

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/store"
)

func TestBuildNodeUsesOnlyFreshVerifiedHTTSTunnelScheme(t *testing.T) {
	now := time.Date(2026, 10, 13, 5, 0, 0, 0, time.UTC)
	pass, fail := true, false
	url := "https://proxy.example.net:8443"
	tests := []struct {
		name       string
		proxy      store.QueryProxy
		wantScheme string
		wantConn   string
		wantDialer string
		wantTLS    bool
	}{
		{
			name:       "fresh verified plain CONNECT fallback",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &pass, HTTPSCheckedAt: now.UnixMilli(), HTTPSTunnelScheme: "http"},
			wantScheme: "http", wantConn: "http", wantDialer: "tcp",
		},
		{
			name:       "unmeasured keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "failed measurement keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &fail, HTTPSCheckedAt: now.UnixMilli(), HTTPSTunnelScheme: "http"},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "missing timestamp keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &pass, HTTPSTunnelScheme: "http"},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "stale measurement keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &pass, HTTPSCheckedAt: now.Add(-25 * time.Hour).UnixMilli(), HTTPSTunnelScheme: "http"},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "future timestamp keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &pass, HTTPSCheckedAt: now.Add(time.Second).UnixMilli(), HTTPSTunnelScheme: "http"},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "unsupported verified scheme keeps configured HTTPS",
			proxy:      store.QueryProxy{URL: url, HTTPSOK: &pass, HTTPSCheckedAt: now.UnixMilli(), HTTPSTunnelScheme: "socks5"},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "fresh verified TLS for HTTP URL",
			proxy:      store.QueryProxy{URL: "http://proxy.example.net:8443", HTTPSOK: &pass, HTTPSCheckedAt: now.UnixMilli(), HTTPSTunnelScheme: " HTTPS "},
			wantScheme: "https", wantConn: "http", wantDialer: "tls", wantTLS: true,
		},
		{
			name:       "SOCKS transport ignores HTTP tunnel metadata",
			proxy:      store.QueryProxy{URL: "socks5://proxy.example.net:1080", HTTPSOK: &pass, HTTPSCheckedAt: now.UnixMilli(), HTTPSTunnelScheme: "http"},
			wantScheme: "socks5", wantConn: "socks5", wantDialer: "tcp",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			node, key, err := buildNodeAt(tc.proxy, "US", now)
			if err != nil {
				t.Fatalf("buildNodeAt: %v", err)
			}
			parsedAddr := node.Addr
			if got, want := node.Connector.Type, tc.wantConn; got != want {
				t.Fatalf("connector type = %q, want %q", got, want)
			}
			if got, want := node.Dialer.Type, tc.wantDialer; got != want {
				t.Fatalf("dialer type = %q, want %q", got, want)
			}
			if tc.wantTLS {
				if node.Dialer.TLS == nil || !node.Dialer.TLS.Secure || node.Dialer.TLS.ServerName != "proxy.example.net" {
					t.Fatalf("TLS dialer = %+v, want verified server name", node.Dialer.TLS)
				}
			} else if node.Dialer.TLS != nil {
				t.Fatalf("plain dialer unexpectedly has TLS settings: %+v", node.Dialer.TLS)
			}
			if got, want := node.Name, nodeName("US", tc.wantScheme, parsedAddr); got != want {
				t.Fatalf("node name = %q, want %q", got, want)
			}
			if got, want := key, tc.wantScheme+"://"+parsedAddr; got != want {
				t.Fatalf("dedupe key = %q, want %q", got, want)
			}
		})
	}
}

func TestVerifiedFallbackHTTPSAliasDeduplicatesAsPlainHTTP(t *testing.T) {
	checkedAt := time.Now().Add(-time.Minute).UnixMilli()
	verified := true
	nodes := buildNodes([]store.QueryProxy{
		{URL: "https://proxy.example.net:8443", Country: "US", HTTPSOK: &verified, HTTPSCheckedAt: checkedAt, HTTPSTunnelScheme: "http"},
		{URL: "http://proxy.example.net:8443", Country: "US"},
	}, BuildOptions{}, "")
	if len(nodes) != 1 {
		t.Fatalf("node count = %d, want one deduplicated HTTP endpoint: %+v", len(nodes), nodes)
	}
	if nodes[0].Name != nodeName("US", "http", "proxy.example.net:8443") || nodes[0].Dialer.Type != "tcp" || nodes[0].Dialer.TLS != nil {
		t.Fatalf("fallback node = %+v, want plain HTTP transport and name", nodes[0])
	}
}

func TestBuildConfigSerializesDisable4aOnlyForPlainSOCKS4(t *testing.T) {
	now := time.Now()
	verified := true
	data, err := BuildConfig([]store.QueryProxy{
		{URL: "socks4://plain.example.net:1080", Country: "US"},
		{URL: "socks4a://remote.example.net:1081", Country: "US"},
		{URL: "http://http.example.net:8080", Country: "US"},
		{URL: "socks5://socks5.example.net:1082", Country: "US"},
		{URL: "https://fallback.example.net:8443", Country: "US", HTTPSOK: &verified, HTTPSCheckedAt: now.Add(-time.Minute).UnixMilli(), HTTPSTunnelScheme: "http"},
	}, BuildOptions{
		ProxyUsername: "proxy-user", ProxyPassword: "proxy-pass",
		APIUsername: "api-user", APIPassword: "api-pass",
	})
	if err != nil {
		t.Fatal(err)
	}
	var config struct {
		Chains []struct {
			Hops []struct {
				Nodes []struct {
					Addr      string          `json:"addr"`
					Name      string          `json:"name"`
					Connector json.RawMessage `json:"connector"`
					Dialer    gostDialer      `json:"dialer"`
				} `json:"nodes"`
			} `json:"hops"`
		} `json:"chains"`
	}
	if err := json.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	if len(config.Chains) == 0 || len(config.Chains[0].Hops) == 0 {
		t.Fatalf("GOST config has no all-pool nodes: %s", data)
	}
	nodes := make(map[string]struct {
		Name      string
		Connector json.RawMessage
		Dialer    gostDialer
	}, len(config.Chains[0].Hops[0].Nodes))
	for _, node := range config.Chains[0].Hops[0].Nodes {
		nodes[node.Addr] = struct {
			Name      string
			Connector json.RawMessage
			Dialer    gostDialer
		}{node.Name, node.Connector, node.Dialer}
	}
	for _, tc := range []struct {
		addr          string
		name          string
		connectorType string
		metadata      map[string]any
		dialerType    string
		wantServer    string
	}{
		{"plain.example.net:1080", "us-socks4-plain-example-net-1080", "socks4", map[string]any{"disable4a": true}, "tcp", ""},
		{"remote.example.net:1081", "us-socks4a-remote-example-net-1081", "socks4", nil, "tcp", ""},
		{"http.example.net:8080", "us-http-http-example-net-8080", "http", nil, "tcp", ""},
		{"socks5.example.net:1082", "us-socks5-socks5-example-net-1082", "socks5", nil, "tcp", ""},
		{"fallback.example.net:8443", "us-http-fallback-example-net-8443", "http", nil, "tcp", ""},
	} {
		t.Run(tc.addr, func(t *testing.T) {
			node, ok := nodes[tc.addr]
			if !ok {
				t.Fatalf("node %q missing from serialized config", tc.addr)
			}
			if node.Name != tc.name {
				t.Fatalf("node name = %q, want %q", node.Name, tc.name)
			}
			var connector struct {
				Type     string         `json:"type"`
				Metadata map[string]any `json:"metadata"`
			}
			if err := json.Unmarshal(node.Connector, &connector); err != nil {
				t.Fatal(err)
			}
			var connectorFields map[string]json.RawMessage
			if err := json.Unmarshal(node.Connector, &connectorFields); err != nil {
				t.Fatal(err)
			}
			if connector.Type != tc.connectorType {
				t.Fatalf("connector type = %q, want %q", connector.Type, tc.connectorType)
			}
			metadataJSON, metadataPresent := connectorFields["metadata"]
			if tc.metadata == nil && metadataPresent {
				t.Fatalf("unexpected connector metadata: %s", metadataJSON)
			}
			if tc.metadata != nil && !metadataPresent {
				t.Fatal("expected connector metadata to be serialized")
			}
			if len(connector.Metadata) != len(tc.metadata) {
				t.Fatalf("connector metadata = %#v, want %#v", connector.Metadata, tc.metadata)
			}
			for key, value := range tc.metadata {
				if connector.Metadata[key] != value {
					t.Fatalf("connector metadata[%q] = %#v, want %#v", key, connector.Metadata[key], value)
				}
			}
			if node.Dialer.Type != tc.dialerType {
				t.Fatalf("dialer type = %q, want %q", node.Dialer.Type, tc.dialerType)
			}
			if tc.wantServer != "" && (node.Dialer.TLS == nil || node.Dialer.TLS.ServerName != tc.wantServer) {
				t.Fatalf("TLS dialer = %+v, want server name %q", node.Dialer.TLS, tc.wantServer)
			}
		})
	}
}
