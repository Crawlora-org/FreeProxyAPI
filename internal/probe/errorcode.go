package probe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"syscall"
)

// Error codes are short, stable, bounded identifiers for probe outcomes. They
// are stored as a candidate's last_error (keeping the Redis hash under
// hash-max-listpack-value) and used as the metrics reason label, so the set
// must stay small and never contain endpoint data.
const (
	CodeOK               = "ok"
	CodeConnectTimeout   = "connect_timeout"   // TCP connect to the proxy hop timed out
	CodeHandshakeTimeout = "handshake_timeout" // TLS or SOCKS handshake timed out
	CodeTimeout          = "timeout"           // request/response deadline after connect
	CodeRefused          = "refused"
	CodeReset            = "reset" // connection reset or broken pipe
	CodeEOF              = "eof"   // proxy closed the connection without a response
	CodeUnreachable      = "unreachable"
	CodeDNS              = "dns"
	CodeTLS              = "tls"
	CodeProxyAuth        = "proxy_auth"       // 407 or CONNECT answered with auth required
	CodeConnectRejected  = "connect_rejected" // CONNECT answered with another non-2xx status
	CodeStatus3xx        = "bad_status_3xx"
	CodeStatus4xx        = "bad_status_4xx"
	CodeStatus5xx        = "bad_status_5xx"
	CodeBadResponse      = "bad_response" // malformed HTTP from the proxy
	CodeUnexpectedBody   = "unexpected_body"
	CodeSOCKS            = "socks_handshake"
	CodeTooLarge         = "too_large"
	CodeBlocked          = "blocked_address" // refused by the public-address dial policy
	CodeInvalidProxy     = "invalid_proxy"
	CodeCanceled         = "canceled"
	CodeOther            = "other"
)

var errorCodes = []string{
	CodeOK, CodeConnectTimeout, CodeHandshakeTimeout, CodeTimeout, CodeRefused,
	CodeReset, CodeEOF, CodeUnreachable, CodeDNS, CodeTLS, CodeProxyAuth,
	CodeConnectRejected, CodeStatus3xx, CodeStatus4xx, CodeStatus5xx,
	CodeBadResponse, CodeUnexpectedBody, CodeSOCKS, CodeTooLarge, CodeBlocked,
	CodeInvalidProxy, CodeCanceled, CodeOther,
}

// ErrorCodes returns every code ClassifyError can return, in a fixed order.
func ErrorCodes() []string { return append([]string(nil), errorCodes...) }

// ClassifyError maps a probe result to a short stable code. Result carries
// its failure as text, so classification relies on the stable wording of Go's
// net, net/http, crypto/tls, and this package's own errors.
func ClassifyError(result Result) string {
	if result.OK {
		return CodeOK
	}
	if code := statusCode(result.StatusCode); code != "" {
		return code
	}
	return classifyMessage(result.Error)
}

// ClassifyErr maps an error value to a code, preferring typed inspection
// (errors.Is/As) and falling back to message matching for errors that were
// flattened while wrapping (for example proxyconnect failures).
func ClassifyErr(err error) string {
	if err == nil {
		return CodeOK
	}
	msg := strings.ToLower(err.Error())
	switch {
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	case errors.Is(err, syscall.ECONNREFUSED):
		return CodeRefused
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, syscall.EPIPE):
		return CodeReset
	case errors.Is(err, syscall.EHOSTUNREACH), errors.Is(err, syscall.ENETUNREACH), errors.Is(err, syscall.EHOSTDOWN):
		return CodeUnreachable
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return CodeDNS
	}
	var opErr *net.OpError
	isTimeout := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded)
	var timeoutErr interface{ Timeout() bool }
	if errors.As(err, &timeoutErr) && timeoutErr.Timeout() {
		isTimeout = true
	}
	if isTimeout {
		if isHandshake(msg) {
			return CodeHandshakeTimeout
		}
		if errors.As(err, &opErr) && opErr.Op == "dial" {
			return CodeConnectTimeout
		}
		return timeoutPhase(msg)
	}
	var (
		unknownAuthority x509.UnknownAuthorityError
		hostnameErr      x509.HostnameError
		certInvalid      x509.CertificateInvalidError
		recordHeader     tls.RecordHeaderError
		alert            tls.AlertError
	)
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostnameErr) || errors.As(err, &certInvalid) ||
		errors.As(err, &recordHeader) || errors.As(err, &alert) {
		return CodeTLS
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		if strings.Contains(msg, "socks") {
			return CodeSOCKS
		}
		return CodeEOF
	}
	return classifyMessage(err.Error())
}

func statusCode(status int) string {
	switch {
	case status == http.StatusProxyAuthRequired:
		return CodeProxyAuth
	case status >= 300 && status < 400:
		return CodeStatus3xx
	case status >= 400 && status < 500:
		return CodeStatus4xx
	case status >= 500 && status < 600:
		return CodeStatus5xx
	}
	return ""
}

func isHandshake(msg string) bool {
	return strings.Contains(msg, "tls handshake") || strings.Contains(msg, "socks handshake") ||
		strings.Contains(msg, "socks4") || strings.Contains(msg, "socks5")
}

// timeoutPhase distinguishes a timeout during the proxy-hop connect from one
// after it. net maps a dial deadline to "i/o timeout" under "dial tcp".
func timeoutPhase(msg string) string {
	if strings.Contains(msg, "dial tcp") || strings.Contains(msg, "connect proxy") || strings.Contains(msg, "dial public address") {
		return CodeConnectTimeout
	}
	return CodeTimeout
}

func classifyMessage(raw string) string {
	msg := strings.ToLower(strings.TrimSpace(raw))
	if msg == "" {
		return CodeOther
	}
	has := func(parts ...string) bool {
		for _, part := range parts {
			if strings.Contains(msg, part) {
				return true
			}
		}
		return false
	}
	switch {
	case has("operation was canceled", "context canceled"):
		return CodeCanceled
	case has("i/o timeout", "deadline exceeded", "timeout awaiting", "client.timeout exceeded", "handshake timeout"):
		if isHandshake(msg) {
			return CodeHandshakeTimeout
		}
		return timeoutPhase(msg)
	case has("connection refused"):
		return CodeRefused
	case has("connection reset", "broken pipe", "forcibly closed"):
		return CodeReset
	case has("no route to host", "network is unreachable", "host is down"):
		return CodeUnreachable
	case has("no such host", "resolve dial host", "server misbehaving", "lookup "):
		return CodeDNS
	case has("refusing non-public", "did not resolve to a public", "must be a public ipv4"):
		return CodeBlocked
	case has("socks"):
		return CodeSOCKS
	case has("tls:", "x509:", "certificate", "server gave http response to https client"):
		return CodeTLS
	case has("response limit exceeded"):
		return CodeTooLarge
	case has("unexpected response body", "contained no caller ip"):
		return CodeUnexpectedBody
	case has("unexpected http status", "unexpected status"):
		return statusFromMessage(msg)
	case has("malformed http", "bogus", "http: server closed", "invalid header", "transfer-encoding"):
		return CodeBadResponse
	case has("unsupported proxy scheme", "proxy url has no host", "invalid tunnel port", "parse "):
		return CodeInvalidProxy
	case has("eof"):
		return CodeEOF
	}
	// An http.Transport CONNECT answered with a non-200 status fails with the
	// bare status text (e.g. `Get "https://t": Forbidden`).
	if idx := strings.LastIndex(msg, ": "); idx >= 0 {
		tail := msg[idx+2:]
		if tail == strings.ToLower(http.StatusText(http.StatusProxyAuthRequired)) {
			return CodeProxyAuth
		}
		for status := 300; status < 600; status++ {
			if text := http.StatusText(status); text != "" && tail == strings.ToLower(text) {
				return CodeConnectRejected
			}
		}
	}
	return CodeOther
}

func statusFromMessage(msg string) string {
	fields := strings.Fields(msg)
	for i := len(fields) - 1; i >= 0; i-- {
		if status, err := strconv.Atoi(fields[i]); err == nil {
			if code := statusCode(status); code != "" {
				return code
			}
			break
		}
	}
	return CodeOther
}
