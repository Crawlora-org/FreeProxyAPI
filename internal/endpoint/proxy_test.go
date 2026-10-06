package endpoint

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestParseProxy(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want bool
	}{
		{name: "host port defaults to http", raw: "proxy.example.net:8080", want: true},
		{name: "supported socks", raw: "socks5://proxy.example.net:1080", want: true},
		{name: "zero-padded IPv4 is canonicalized", raw: "http://001.002.003.004:8080", want: true},
		{name: "credentials rejected", raw: "http://user:secret@proxy.example.net:8080"},
		{name: "private IPv4 rejected", raw: "http://192.168.1.10:8080"},
		{name: "loopback IPv6 rejected", raw: "socks5://[::1]:1080"},
		{name: "missing port rejected", raw: "https://proxy.example.net"},
		{name: "unsupported scheme", raw: "ftp://proxy.example.net:21"},
		{name: "public IPv6 accepted", raw: "http://[2606:4700::1111]:8080", want: true},
		{name: "NAT64 of public IPv4 accepted", raw: "http://[64:ff9b::808:808]:8080", want: true},
		{name: "localhost with trailing dot rejected", raw: "http://localhost.:8080"},
		{name: "localhost subdomain rejected", raw: "http://api.localhost:8080"},
		{name: "this-network IPv4 rejected", raw: "http://0.1.2.3:8080"},
		{name: "carrier-grade NAT rejected", raw: "http://100.64.0.1:8080"},
		{name: "benchmarking range rejected", raw: "http://198.18.0.1:8080"},
		{name: "reserved IPv4 rejected", raw: "http://240.0.0.1:8080"},
		{name: "broadcast rejected", raw: "http://255.255.255.255:8080"},
		{name: "IPv4-mapped loopback rejected", raw: "http://[::ffff:127.0.0.1]:8080"},
		{name: "NAT64 link-local metadata rejected", raw: "http://[64:ff9b::a9fe:a9fe]:8080"},
		{name: "6to4 loopback rejected", raw: "http://[2002:7f00:1::]:8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseProxy(tt.raw, "http")
			if (err == nil) != tt.want {
				t.Fatalf("ParseProxy(%q) error = %v, want success=%t", tt.raw, err, tt.want)
			}
		})
	}

	proxy, err := ParseProxy("http://001.002.003.004:8080", "http")
	if err != nil || proxy.String() != "http://1.2.3.4:8080" {
		t.Fatalf("zero-padded IPv4 = %q, %v; want canonical endpoint", proxy.String(), err)
	}

	v6, err := ParseProxy("SOCKS5://[2606:4700::1111]:1080", "http")
	if err != nil || v6.String() != "socks5://[2606:4700::1111]:1080" {
		t.Fatalf("IPv6 endpoint = %q, %v; want bracketed canonical form", v6.String(), err)
	}
	roundTrip, err := ParseProxy(v6.String(), "http")
	if err != nil || roundTrip != v6 {
		t.Fatalf("IPv6 endpoint did not round-trip: %+v, %v", roundTrip, err)
	}
}

func TestParseFeedReportsOnlyAggregates(t *testing.T) {
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader("# comment\nproxy.example.net:8080\ninvalid\nsocks5://proxy.example.net:1080\n203.0.113.10:3128:United States\nhttp://198.51.100.9:8080:US\n198.51.100.8:8080 metadata\n"), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 5 || rejected != 1 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 5 and 1", accepted, rejected)
	}
	if byScheme["http"] != 4 || byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseGeonodeJSONFeed(t *testing.T) {
	payload := `{"data":[
                {"ip":"203.0.113.10","port":"8080","protocols":["http","https"]},
                {"ip":"203.0.113.11","port":1080,"protocols":["socks5"]},
                {"ip":"192.168.1.10","port":"8080","protocols":["http"]}
        ],"page":3,"total":3}`
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader(payload), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 3 || rejected != 1 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 3 and 1", accepted, rejected)
	}
	if byScheme["http"] != 1 || byScheme["https"] != 1 || byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseJSONFeedAlternateIPAddressFields(t *testing.T) {
	payload := `{"proxies":[
                {"ip_address":"203.0.113.12","port":8080,"protocol":"http"},
                {"addr":"203.0.113.13","port":"1080","protocol":"socks5"}
        ]}`
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader(payload), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 2 || rejected != 0 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 2 and 0", accepted, rejected)
	}
	if byScheme["http"] != 1 || byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseJSONFeedResultsEnvelope(t *testing.T) {
	payload := `{"results":[
                {"ip_address":"203.0.113.17","port":8080,"protocols":["HTTP"]},
                {"ip_address":"203.0.113.18","port":1080,"protocols":["SOCKS5"]}
        ],"count":2}`
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader(payload), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 2 || rejected != 0 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 2 and 0", accepted, rejected)
	}
	if byScheme["http"] != 1 || byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseJSONFeedHostField(t *testing.T) {
	payload := `[{"host":"203.0.113.16","port":1080,"protocol":"socks5"}]`
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader(payload), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 1 || rejected != 0 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 1 and 0", accepted, rejected)
	}
	if byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseJSONFeedStringEnvelope(t *testing.T) {
	payload := `{"proxies":[
                "203.0.113.14:8080",
                "socks5://203.0.113.15:1080",
                "http://192.168.1.10:8080"
        ]}`
	accepted, rejected, byScheme, err := ParseFeed(strings.NewReader(payload), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 2 || rejected != 1 {
		t.Fatalf("ParseFeed counts = accepted %d rejected %d, want 2 and 1", accepted, rejected)
	}
	if byScheme["http"] != 1 || byScheme["socks5"] != 1 {
		t.Fatalf("ParseFeed schemes = %#v", byScheme)
	}
}

func TestParseFeedRejectsMalformedJSON(t *testing.T) {
	for name, payload := range map[string]string{
		"truncated array":      `[{"ip":"203.0.113.10","port":8080}`,
		"truncated envelope":   `{"data":[{"ip":"203.0.113.10"`,
		"empty envelope":       `{"page":1,"total":0}`,
		"non-array records":    `{"data":{"ip":"203.0.113.10","port":8080}}`,
		"html error in object": `{<html>`,
	} {
		t.Run(name, func(t *testing.T) {
			accepted, _, _, err := ParseFeed(strings.NewReader(payload), "http")
			if err == nil {
				t.Fatalf("ParseFeed(%q) accepted=%d, want error", payload, accepted)
			}
		})
	}
	// An explicitly empty record array is a valid, empty feed.
	accepted, rejected, _, err := ParseFeed(strings.NewReader(`{"data":[]}`), "http")
	if err != nil || accepted != 0 || rejected != 0 {
		t.Fatalf("empty data array = %d/%d, %v; want 0/0, nil", accepted, rejected, err)
	}
}

func TestParseHTMLFeedUsesRowProtocol(t *testing.T) {
	feed := `<table><tr data-type="HTTP"><td><button data-pp="192.0.2.10:8080">Copy</button></td></tr>
<tr data-type="SOCKS5"><td><button data-pp="192.0.2.11:1080">Copy</button></td></tr></table>`
	proxies, rejected, err := ParseFeedProxies(strings.NewReader(feed), "http")
	if err != nil {
		t.Fatalf("ParseFeedProxies: %v", err)
	}
	if rejected != 0 {
		t.Fatalf("rejected = %d, want 0", rejected)
	}
	got := []string{proxies[0].String(), proxies[1].String()}
	want := []string{"http://192.0.2.10:8080", "socks5://192.0.2.11:1080"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("proxies = %#v, want %#v", got, want)
	}
}

func TestParseHTMLFeedUsesDefaultProtocolForBareEndpoints(t *testing.T) {
	feed := `<html><body><div>203.0.113.10:8080</div><div>198.51.100.11:1080</div></body></html>`
	proxies, rejected, err := ParseFeedProxies(strings.NewReader(feed), "socks5")
	if err != nil {
		t.Fatalf("ParseFeedProxies: %v", err)
	}
	if rejected != 0 {
		t.Fatalf("rejected = %d, want 0", rejected)
	}
	got := []string{proxies[0].String(), proxies[1].String()}
	want := []string{"socks5://203.0.113.10:8080", "socks5://198.51.100.11:1080"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("proxies = %#v, want %#v", got, want)
	}
}

func TestParseHTMLFeedRejectsUnrecognizedDocument(t *testing.T) {
	_, _, err := ParseFeedProxies(strings.NewReader("<html><body>temporary error</body></html>"), "http")
	if err == nil {
		t.Fatal("ParseFeedProxies accepted HTML without proxy rows")
	}
}

func TestVisitFeedSkipsOversizedLine(t *testing.T) {
	feed := "203.0.113.10:8080\n" + strings.Repeat("x", MaxFeedLineBytes+10) + "\n203.0.113.11:8080\r\n203.0.113.12:8080"
	accepted, rejected, _, err := ParseFeed(strings.NewReader(feed), "http")
	if err != nil {
		t.Fatalf("ParseFeed returned error: %v", err)
	}
	if accepted != 3 || rejected != 1 {
		t.Fatalf("counts = accepted %d rejected %d, want 3 and 1", accepted, rejected)
	}
}

type failingReader struct {
	data string
	err  error
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.data == "" {
		return 0, r.err
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func TestVisitFeedStopsOnReadErrorWithoutPartialLine(t *testing.T) {
	readErr := errors.New("limit reached")
	var visited []string
	_, err := VisitFeedProxies(&failingReader{data: "203.0.113.10:8080\n203.0.113.1", err: readErr}, "http", func(p Proxy) error {
		visited = append(visited, p.String())
		return nil
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("error = %v, want wrapped read error", err)
	}
	if len(visited) != 1 || visited[0] != "http://203.0.113.10:8080" {
		t.Fatalf("visited = %v, want only the complete line", visited)
	}
}

func TestParseProxyRejectsAmbiguousHostnames(t *testing.T) {
	for _, raw := range []string{
		"http://2130706433:8080",
		"http://0x7f.0.0.1:8080",
		"http://0x7f000001:8080",
		"http://127.1:8080",
		"http://127.0.0.1.nip.io:8080",
		"http://app.10-0-0-1.sslip.io:8080",
		"http://7f000001.nip.io:8080",
		"http://localhost.:8080",
		"http://99999999999999999999999:8080",
	} {
		if proxy, err := ParseProxy(raw, "http"); err == nil {
			t.Errorf("ParseProxy(%q) = %+v, want error", raw, proxy)
		}
	}
	for _, raw := range []string{
		"http://proxy.example.net:8080",
		"http://203.0.113.10.nip.io:8080",
		"http://cdn-1.example.com:8080",
		"http://proxy.example.net.:8080",
	} {
		if _, err := ParseProxy(raw, "http"); err != nil {
			t.Errorf("ParseProxy(%q) error = %v, want success", raw, err)
		}
	}
}

func TestParseProxyRejectsHostileHostnames(t *testing.T) {
	for _, raw := range []string{
		"http://$(id).evil.example:80",
		"http://a;id;.evil.example:80",
		"http://a&&id.evil.example:80",
		"http://a`id`.evil.example:80",
		"http://a'b.example:80",
		"http://a\"b.example:80",
		"http://a<b>.example:80",
		"http://a*b.example:80",
		"http://a,b.example:80",
		"http://a=b.example:80",
		"http://a~b.example:80",
		"http://a!b.example:80",
		"http://a+b.example:80",
		"http://a_b.example:80",
		"http://a%ffb.example:80",
		"http://a%20b.example:80",
		"http://a%2eb.example:80",
		"http://bücher.example:80",
		"http://-lead.example:80",
		"http://trail-.example:80",
		"http://a..example:80",
		"http://.example:80",
		"http://" + strings.Repeat("a", 64) + ".example:80",
		"http://" + strings.Repeat("a.", 130) + "example:80",
		"http://[2001:db8::1%25eth0]:80",
		"http://[fe80::1%25en0]:80",
	} {
		if proxy, err := ParseProxy(raw, "http"); err == nil {
			t.Errorf("ParseProxy(%q) = %+v, want error", raw, proxy)
		}
	}
	for _, raw := range []string{
		"http://xn--bcher-kva.example:80",
		"http://a-b.example:80",
		"http://a1.b2.example.co.uk:80",
		"http://" + strings.Repeat("a", 63) + ".example:80",
		"http://UPPER.Example.NET:80",
	} {
		proxy, err := ParseProxy(raw, "http")
		if err != nil {
			t.Errorf("ParseProxy(%q) error = %v, want success", raw, err)
			continue
		}
		if proxy.Host != strings.ToLower(proxy.Host) {
			t.Errorf("ParseProxy(%q) host %q is not lowercased", raw, proxy.Host)
		}
	}
}
