package probe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// connectProxyHandler is a minimal CONNECT proxy that tunnels to the
// requested loopback address.
func connectProxyHandler(t *testing.T, connects *atomic.Int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect {
			http.Error(w, "CONNECT only", http.StatusMethodNotAllowed)
			return
		}
		connects.Add(1)
		upstream, err := net.Dial("tcp", r.Host)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Errorf("response writer cannot hijack")
			_ = upstream.Close()
			return
		}
		client, _, err := hijacker.Hijack()
		if err != nil {
			_ = upstream.Close()
			return
		}
		if _, err := io.WriteString(client, "HTTP/1.1 200 Connection established\r\n\r\n"); err != nil {
			_ = upstream.Close()
			_ = client.Close()
			return
		}
		go func() { _, _ = io.Copy(upstream, client); _ = upstream.Close() }()
		go func() { _, _ = io.Copy(client, upstream); _ = client.Close() }()
	})
}

func tlsTarget(t *testing.T, body string) (*httptest.Server, *x509.CertPool) {
	t.Helper()
	target := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(target.Close)
	pool := x509.NewCertPool()
	pool.AddCert(target.Certificate())
	return target, pool
}

func TestHTTPSProbeThroughConnectProxy(t *testing.T) {
	for _, tc := range []struct {
		name, body, expected string
		wantOK               bool
	}{
		{"pass", "success\n", "success", true},
		{"body mismatch", "<html>portal</html>", "success", false},
		{"any body when unset", "whatever", "", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, pool := tlsTarget(t, tc.body)
			var connects atomic.Int64
			proxy := httptest.NewServer(connectProxyHandler(t, &connects))
			defer proxy.Close()
			result := runHTTPSProbe(context.Background(), proxy.URL, target.URL+"/success.txt", 5*time.Second, tc.expected, plainDial, pool)
			if result.OK != tc.wantOK {
				t.Fatalf("OK=%t want %t (err=%q)", result.OK, tc.wantOK, result.Error)
			}
			if connects.Load() != 1 {
				t.Fatalf("CONNECT requests = %d, want 1", connects.Load())
			}
			if result.TunnelScheme != "http" || result.ProxyTLSFallback {
				t.Fatalf("tunnel = %q fallback=%t", result.TunnelScheme, result.ProxyTLSFallback)
			}
			if !tc.wantOK && result.Error != "unexpected response body" {
				t.Fatalf("error = %q", result.Error)
			}
			if result.UploadBytes == 0 || result.DownloadBytes == 0 {
				t.Fatalf("traffic not counted: %+v", result)
			}
		})
	}
}

func TestHTTPSProbeFailsUntrustedTarget(t *testing.T) {
	target, _ := tlsTarget(t, "success")
	var connects atomic.Int64
	proxy := httptest.NewServer(connectProxyHandler(t, &connects))
	defer proxy.Close()
	// An empty pool trusts nothing: an interception proxy with its own cert
	// would look exactly like this.
	result := runHTTPSProbe(context.Background(), proxy.URL, target.URL, 5*time.Second, "success", plainDial, x509.NewCertPool())
	if result.OK {
		t.Fatal("untrusted target certificate must fail")
	}
}

func TestHTTPSProbeRejectsRefusedConnect(t *testing.T) {
	target, pool := tlsTarget(t, "success")
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no tunnels", http.StatusForbidden)
	}))
	defer proxy.Close()
	result := runHTTPSProbe(context.Background(), proxy.URL, target.URL, 5*time.Second, "success", plainDial, pool)
	if result.OK || result.Error == "" {
		t.Fatalf("refused CONNECT must fail: %+v", result)
	}
}

func TestHTTPSProbeOverTLSProxy(t *testing.T) {
	target, pool := tlsTarget(t, "success")
	var connects atomic.Int64
	proxy := httptest.NewTLSServer(connectProxyHandler(t, &connects))
	defer proxy.Close()
	// httptest servers share one certificate, so the pool trusts the proxy.
	proxyURL := "https://" + proxy.Listener.Addr().String()
	result := runHTTPSProbe(context.Background(), proxyURL, target.URL, 5*time.Second, "success", plainDial, pool)
	if !result.OK || result.TunnelScheme != "https" || result.ProxyTLSFallback {
		t.Fatalf("TLS proxy: %+v", result)
	}
}

func TestHTTPSProbeFallsBackForPlainProxyLabeledHTTPS(t *testing.T) {
	target, pool := tlsTarget(t, "success")
	var connects atomic.Int64
	proxy := httptest.NewServer(connectProxyHandler(t, &connects))
	defer proxy.Close()
	proxyURL := "https://" + proxy.Listener.Addr().String()
	result := runHTTPSProbe(context.Background(), proxyURL, target.URL, 5*time.Second, "success", plainDial, pool)
	if !result.OK || !result.ProxyTLSFallback || result.TunnelScheme != "http" {
		t.Fatalf("fallback: %+v", result)
	}
}

func TestFallbackToPlainConnectClassification(t *testing.T) {
	if !fallbackToPlainConnect(&proxyTLSError{err: x509.UnknownAuthorityError{}}) {
		t.Error("unknown CA on proxy hop should fall back")
	}
	if !fallbackToPlainConnect(&proxyTLSError{err: tls.RecordHeaderError{Msg: "first record does not look like a TLS handshake"}}) {
		t.Error("non-TLS record should fall back")
	}
	if fallbackToPlainConnect(x509.UnknownAuthorityError{}) {
		t.Error("target TLS failure must not fall back")
	}
	if fallbackToPlainConnect(&proxyTLSError{err: errors.New("connection reset")}) {
		t.Error("generic proxy failure must not fall back")
	}
}

// socks5TunnelServer accepts one no-auth SOCKS5 CONNECT per connection and
// splices it to the requested address.
func socks5TunnelServer(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				buf := make([]byte, 262)
				if _, err := io.ReadFull(conn, buf[:3]); err != nil {
					_ = conn.Close()
					return
				}
				_, _ = conn.Write([]byte{0x05, 0x00})
				if _, err := io.ReadFull(conn, buf[:4]); err != nil || buf[3] != 0x01 {
					_ = conn.Close()
					return
				}
				if _, err := io.ReadFull(conn, buf[:6]); err != nil {
					_ = conn.Close()
					return
				}
				addr := net.JoinHostPort(net.IP(buf[:4]).String(), strconv.Itoa(int(binary.BigEndian.Uint16(buf[4:6]))))
				upstream, err := net.Dial("tcp", addr)
				if err != nil {
					_, _ = conn.Write([]byte{0x05, 0x05, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
					_ = conn.Close()
					return
				}
				_, _ = conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 0, 0, 0, 0, 0, 0})
				go func() { _, _ = io.Copy(upstream, conn); _ = upstream.Close() }()
				_, _ = io.Copy(conn, upstream)
				_ = conn.Close()
			}(conn)
		}
	}()
	return listener.Addr().String()
}

func TestHTTPSProbeThroughSOCKS5(t *testing.T) {
	target, pool := tlsTarget(t, "success")
	addr := socks5TunnelServer(t)
	result := runHTTPSProbe(context.Background(), "socks5://"+addr, target.URL, 5*time.Second, "success", plainDial, pool)
	if !result.OK || result.TunnelScheme != "socks5" {
		t.Fatalf("socks5 https: %+v", result)
	}
}

func TestHTTPSProbeRequiresHTTPSTarget(t *testing.T) {
	result := runHTTPSProbe(context.Background(), "http://192.0.2.1:8080", "http://example.invalid/", time.Second, "", plainDial, nil)
	if result.OK || result.Error == "" {
		t.Fatalf("plain-HTTP target must be rejected: %+v", result)
	}
}
