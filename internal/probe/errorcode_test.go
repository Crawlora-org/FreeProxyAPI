package probe

import (
	"context"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
)

func TestClassifyErrorMessages(t *testing.T) {
	cases := []struct {
		name   string
		result Result
		want   string
	}{
		{"ok", Result{OK: true, StatusCode: 200}, CodeOK},
		{"proxyconnect dial timeout", Result{Error: `Get "http://probe.example/healthz?_fpa=0011": proxyconnect tcp: dial tcp 203.0.113.9:8080: i/o timeout`}, CodeConnectTimeout},
		{"socks connect timeout", Result{Error: `Get "http://probe.example/": connect proxy: dial tcp 203.0.113.9:1080: i/o timeout`}, CodeConnectTimeout},
		{"header timeout", Result{Error: `Get "http://probe.example/": net/http: timeout awaiting response headers`}, CodeTimeout},
		{"context deadline", Result{Error: `Get "http://probe.example/": context deadline exceeded`}, CodeTimeout},
		{"body read deadline", Result{Error: `read probe response: context deadline exceeded`}, CodeTimeout},
		{"tls handshake timeout", Result{Error: `Get "https://probe.example/": net/http: TLS handshake timeout`}, CodeHandshakeTimeout},
		{"socks handshake timeout", Result{Error: `Get "http://probe.example/": SOCKS handshake: context deadline exceeded`}, CodeHandshakeTimeout},
		{"refused", Result{Error: `Get "http://probe.example/": proxyconnect tcp: dial tcp 203.0.113.9:8080: connect: connection refused`}, CodeRefused},
		{"reset", Result{Error: `Get "http://probe.example/": read tcp 10.0.0.1:5555->203.0.113.9:8080: read: connection reset by peer`}, CodeReset},
		{"eof", Result{Error: `Get "http://probe.example/": EOF`}, CodeEOF},
		{"unreachable", Result{Error: `Get "http://probe.example/": proxyconnect tcp: dial tcp 203.0.113.9:8080: connect: no route to host`}, CodeUnreachable},
		{"dns", Result{Error: `Get "http://probe.example/": proxyconnect tcp: resolve dial host: lookup proxy.example: no such host`}, CodeDNS},
		{"tls", Result{Error: `Get "https://probe.example/": tls: failed to verify certificate: x509: certificate signed by unknown authority`}, CodeTLS},
		{"https to http", Result{Error: `Get "https://probe.example/": http: server gave HTTP response to HTTPS client`}, CodeTLS},
		{"407 status", Result{StatusCode: 407, Error: "unexpected HTTP status 407"}, CodeProxyAuth},
		{"connect auth", Result{Error: `Get "https://probe.example/": Proxy Authentication Required`}, CodeProxyAuth},
		{"connect forbidden", Result{Error: `Get "https://probe.example/": Forbidden`}, CodeConnectRejected},
		{"302", Result{StatusCode: 302, Error: "unexpected HTTP status 302"}, CodeStatus3xx},
		{"403", Result{StatusCode: 403, Error: "unexpected HTTP status 403"}, CodeStatus4xx},
		{"503", Result{StatusCode: 503, Error: "unexpected HTTP status 503"}, CodeStatus5xx},
		{"status text only", Result{Error: "echo endpoint returned unexpected HTTP status 502"}, CodeStatus5xx},
		{"body", Result{StatusCode: 200, Error: "unexpected response body"}, CodeUnexpectedBody},
		{"echo body", Result{StatusCode: 200, Error: "echo response contained no caller IP"}, CodeUnexpectedBody},
		{"socks rejected", Result{Error: `Get "http://probe.example/": SOCKS5 connect failed: code=0x05`}, CodeSOCKS},
		{"too large", Result{StatusCode: 200, Error: "read probe response: 65536-byte response limit exceeded"}, CodeTooLarge},
		{"malformed", Result{Error: `Get "http://probe.example/": net/http: HTTP/1.x transport connection broken: malformed HTTP response "SSH-2.0"`}, CodeBadResponse},
		{"blocked", Result{Error: `Get "http://probe.example/": proxyconnect tcp: refusing non-public dial address`}, CodeBlocked},
		{"invalid", Result{Error: `unsupported proxy scheme "ftp"`}, CodeInvalidProxy},
		{"canceled", Result{Error: `Get "http://probe.example/": context canceled`}, CodeCanceled},
		{"empty failure", Result{}, CodeOther},
		{"unknown", Result{Error: "something odd happened"}, CodeOther},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyError(tc.result); got != tc.want {
				t.Fatalf("ClassifyError(%q, %d) = %q, want %q", tc.result.Error, tc.result.StatusCode, got, tc.want)
			}
		})
	}
}

func TestClassifyErrTypedErrors(t *testing.T) {
	dialTimeout := &net.OpError{Op: "dial", Net: "tcp", Err: os.ErrDeadlineExceeded}
	cases := []struct {
		name string
		err  error
		want string
	}{
		{"nil", nil, CodeOK},
		{"canceled", fmt.Errorf("wrap: %w", context.Canceled), CodeCanceled},
		{"dial timeout", fmt.Errorf("proxyconnect: %w", dialTimeout), CodeConnectTimeout},
		{"read timeout", &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}, CodeTimeout},
		{"deadline", fmt.Errorf("read probe response: %w", context.DeadlineExceeded), CodeTimeout},
		{"socks handshake deadline", fmt.Errorf("SOCKS handshake: %w", context.DeadlineExceeded), CodeHandshakeTimeout},
		{"refused", &net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.ECONNREFUSED)}, CodeRefused},
		{"reset", &net.OpError{Op: "read", Err: os.NewSyscallError("read", syscall.ECONNRESET)}, CodeReset},
		{"unreachable", &net.OpError{Op: "dial", Err: os.NewSyscallError("connect", syscall.EHOSTUNREACH)}, CodeUnreachable},
		{"dns", fmt.Errorf("resolve dial host: %w", &net.DNSError{Err: "no such host", Name: "x.example", IsNotFound: true}), CodeDNS},
		{"x509", fmt.Errorf("tls: %w", x509.UnknownAuthorityError{}), CodeTLS},
		{"eof", fmt.Errorf("wrap: %w", io.EOF), CodeEOF},
		{"socks eof", fmt.Errorf("read SOCKS5 reply: %w", io.ErrUnexpectedEOF), CodeSOCKS},
		{"fallback", errors.New("unexpected HTTP status 404"), CodeStatus4xx},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ClassifyErr(tc.err); got != tc.want {
				t.Fatalf("ClassifyErr(%v) = %q, want %q", tc.err, got, tc.want)
			}
		})
	}
}

func TestErrorCodesAreShortAndUnique(t *testing.T) {
	seen := map[string]bool{}
	for _, code := range ErrorCodes() {
		if len(code) == 0 || len(code) > 32 {
			t.Fatalf("code %q length %d outside 1..32", code, len(code))
		}
		if seen[code] {
			t.Fatalf("duplicate code %q", code)
		}
		seen[code] = true
	}
}
