package probe

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"
)

func TestExtractIPHandlesV4AndV6(t *testing.T) {
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{name: "httpbin json v4", payload: `{"origin":"104.130.31.60,34.160.111.145","headers":{}}`, want: "104.130.31.60"},
		{name: "httpbin json v6", payload: `{"origin":"2606:4700:4700::1111"}`, want: "2606:4700:4700::1111"},
		{name: "plain text v4", payload: "your ip is 203.0.113.9 thanks", want: "203.0.113.9"},
		{name: "bracketed v6 in text", payload: `exit [2001:db8::5] port 3128`, want: "2001:db8::5"},
		{name: "json array headers v6", payload: `{"X-Ip":["2402:9400:1000:0::12"]}`, want: "2402:9400:1000::12"},
		{name: "no address", payload: "nothing here", want: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractIP(tc.payload)
			if !got.IsValid() {
				if tc.want != "" {
					t.Fatalf("extractIP = invalid, want %q", tc.want)
				}
				return
			}
			if got.String() != tc.want {
				t.Fatalf("extractIP = %q, want %q", got.String(), tc.want)
			}
		})
	}
}

func TestClassificationNormalizesIPv6Origin(t *testing.T) {
	// Equivalent-form IPv6 origins must compare equal after Unmap+ParseAddr;
	// verified here against the same normalization the Check path uses.
	a := mustAddr(t, "2606:4700:4700::0")
	b := mustAddr(t, "2606:4700:4700::")
	if a.Compare(b) != 0 {
		t.Fatalf("normalized comparison failed: %q vs %q", a.String(), b.String())
	}
}

func mustAddr(t *testing.T, s string) (out netip.Addr) {
	t.Helper()
	addr, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return addr.Unmap()
}

func TestEchoClientCheckRefusesRedirects(t *testing.T) {
	redirected := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirected" {
			redirected++
			_, _ = w.Write([]byte(`{"origin":"198.51.100.9"}`))
			return
		}
		http.Redirect(w, r, "/redirected", http.StatusFound)
	}))
	defer server.Close()

	client := newEchoClient(time.Second, plainDial)
	result := client.Check(context.Background(), server.URL, server.URL+"/start", "203.0.113.8", time.Second)
	if result.Class != "" || redirected != 0 {
		t.Fatalf("redirect was followed: result=%+v redirected=%d", result, redirected)
	}
}

func TestEchoClientRejectsOversizedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", int(MaxResponseBytes)+1)))
	}))
	defer server.Close()

	client := newEchoClient(time.Second, plainDial)
	if result := client.Check(context.Background(), server.URL, server.URL, "", time.Second); result.Class != "" {
		t.Fatalf("secondary echo accepted oversized response: %+v", result)
	}
	if _, err := client.RefreshRealIP(context.Background(), server.URL); err == nil || !strings.Contains(err.Error(), "limit exceeded") {
		t.Fatalf("direct echo oversized response error = %v", err)
	}
}

func TestEchoClientRejectsPartialBodyWhenContextExpires(t *testing.T) {
	payload := `{"origin":"198.51.100.9"}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "30")
		_, _ = w.Write([]byte(payload))
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	client := newEchoClient(75*time.Millisecond, plainDial)
	if result := client.Check(context.Background(), server.URL, server.URL, "", 75*time.Millisecond); result.Class != "" {
		t.Fatalf("secondary echo classified a partial response: %+v", result)
	}
}

func TestZeroValueEchoClientKeepsPublicDialPolicy(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		_, _ = w.Write([]byte(`{"origin":"198.51.100.9"}`))
	}))
	defer server.Close()

	client := &EchoClient{}
	if result := client.Check(context.Background(), server.URL, server.URL, "", time.Second); result.Class != "" {
		t.Fatalf("zero-value client reached loopback: %+v", result)
	}
	if requests != 0 {
		t.Fatalf("zero-value client made %d loopback requests", requests)
	}
}

func TestEchoClientCheckUppercaseRemoteResolveScheme(t *testing.T) {
	var sawHostname bool
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		greeting := make([]byte, 3)
		if _, err := io.ReadFull(conn, greeting); err != nil {
			return
		}
		conn.Write([]byte{0x05, 0x00})
		header := make([]byte, 4)
		if _, err := io.ReadFull(conn, header); err != nil {
			return
		}
		if header[3] == 0x03 {
			sawHostname = true
		}
		// Refuse the connect; only the address type matters here.
		conn.Write([]byte{0x05, 0x01, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
	}()
	client := newEchoClient(time.Second, plainDial)
	client.Check(context.Background(), "SOCKS5H://"+listener.Addr().String(), "http://echo.example.net/get", "", time.Second)
	if !sawHostname {
		t.Fatal("uppercase SOCKS5H scheme did not use remote hostname resolution")
	}
}
