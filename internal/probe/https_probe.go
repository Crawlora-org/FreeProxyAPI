package probe

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// HTTPSResult is the outcome of one sampled HTTPS-capability check through a
// proxy. TunnelScheme records which proxy hop actually carried the tunnel:
// "http" for a plain CONNECT, "https" for CONNECT over TLS to the proxy, or the
// SOCKS scheme. ProxyTLSFallback reports that an https:// proxy failed its own
// TLS handshake (non-TLS record or untrusted certificate) and the check was
// retried as a plain-HTTP CONNECT proxy.
type HTTPSResult struct {
	OK               bool
	Error            string
	TunnelScheme     string
	ProxyTLSFallback bool
	Duration         time.Duration
	UploadBytes      int64
	DownloadBytes    int64
}

// TestHTTPS verifies that a proxy can carry an end-to-end TLS session: the
// request travels through a CONNECT tunnel (http/https proxies) or a SOCKS
// tunnel, TLS to targetURL is verified against the system roots, and when
// expectedBody is non-empty the trimmed response body must equal it. The
// proxy-hop TCP connect is bounded by connectTimeout and refuses unsafe
// addresses.
func TestHTTPS(parent context.Context, proxyURL, targetURL string, timeout, connectTimeout time.Duration, expectedBody string) HTTPSResult {
	return runHTTPSProbe(parent, proxyURL, targetURL, timeout, expectedBody, publicHopDialer(connectTimeout), nil)
}

// proxyTLSError marks a failed TLS handshake with an https:// proxy itself (as
// opposed to the tunneled target), so the caller can retry as plain CONNECT.
type proxyTLSError struct{ err error }

func (e *proxyTLSError) Error() string { return "proxy TLS handshake: " + e.err.Error() }
func (e *proxyTLSError) Unwrap() error { return e.err }

// fallbackToPlainConnect reports whether a proxy TLS failure looks like a
// plain-HTTP proxy mislabeled as https:// (non-TLS first record) or a proxy
// presenting an untrusted/mismatched certificate.
func fallbackToPlainConnect(err error) bool {
	var tlsErr *proxyTLSError
	if !errors.As(err, &tlsErr) {
		return false
	}
	var recordErr tls.RecordHeaderError
	var unknownCA x509.UnknownAuthorityError
	var hostErr x509.HostnameError
	var invalidErr x509.CertificateInvalidError
	var verifyErr *tls.CertificateVerificationError
	return errors.As(err, &recordErr) || errors.As(err, &unknownCA) || errors.As(err, &hostErr) ||
		errors.As(err, &invalidErr) || errors.As(err, &verifyErr)
}

func runHTTPSProbe(parent context.Context, proxyURL, targetURL string, timeout time.Duration, expectedBody string, dialPublic dialFunc, rootCAs *x509.CertPool) HTTPSResult {
	started := time.Now()
	proxy, err := url.Parse(proxyURL)
	if err != nil {
		return HTTPSResult{Error: err.Error()}
	}
	scheme := strings.ToLower(proxy.Scheme)
	result := httpsAttempt(parent, proxy, scheme, targetURL, timeout, expectedBody, dialPublic, rootCAs)
	if scheme == "https" && !result.OK && fallbackToPlainConnect(result.err) {
		retry := httpsAttempt(parent, proxy, "http", targetURL, timeout, expectedBody, dialPublic, rootCAs)
		retry.ProxyTLSFallback = true
		retry.UploadBytes += result.UploadBytes
		retry.DownloadBytes += result.DownloadBytes
		if !retry.OK {
			retry.Error = fmt.Sprintf("%s; plain CONNECT retry: %s", result.Error, retry.Error)
		}
		result = retry
	}
	result.Duration = time.Since(started)
	return result.HTTPSResult
}

type httpsAttemptResult struct {
	HTTPSResult
	err error
}

func httpsAttempt(parent context.Context, proxy *url.URL, scheme, targetURL string, timeout time.Duration, expectedBody string, dialPublic dialFunc, rootCAs *x509.CertPool) (result httpsAttemptResult) {
	traffic := &probeTraffic{}
	defer func() {
		result.UploadBytes = traffic.upload.Load()
		result.DownloadBytes = traffic.download.Load()
		if result.err != nil && result.Error == "" {
			result.Error = result.err.Error()
		}
	}()
	result.TunnelScheme = scheme
	target, err := url.Parse(targetURL)
	if err != nil {
		result.err = err
		return result
	}
	if !strings.EqualFold(target.Scheme, "https") || target.Host == "" {
		result.err = fmt.Errorf("https probe target must be an https URL")
		return result
	}
	if proxy.Host == "" {
		result.err = fmt.Errorf("proxy URL has no host")
		return result
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	dialCounted := func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dialPublic(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: conn, traffic: traffic}, nil
	}
	transport := &http.Transport{
		TLSClientConfig:       &tls.Config{RootCAs: rootCAs, MinVersion: tls.VersionTLS12},
		TLSHandshakeTimeout:   timeout / 2,
		ResponseHeaderTimeout: timeout,
		DisableKeepAlives:     true,
	}
	switch scheme {
	case "http":
		transport.DialContext = connectTunnelDial(dialCounted, defaultPort(proxy, "80"), nil)
	case "https":
		transport.DialContext = connectTunnelDial(dialCounted, defaultPort(proxy, "443"),
			&tls.Config{ServerName: proxy.Hostname(), RootCAs: rootCAs, MinVersion: tls.VersionTLS12})
	case "socks4", "socks4a":
		transport.DialContext = socksDialContext(dialCounted, proxy.Hostname(), proxy.Port(), 4, scheme == "socks4a")
	case "socks5", "socks5h":
		transport.DialContext = socksDialContext(dialCounted, proxy.Hostname(), proxy.Port(), 5, scheme == "socks5h")
	default:
		result.err = fmt.Errorf("unsupported proxy scheme %q", scheme)
		return result
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	requestURL, err := withCacheBuster(targetURL)
	if err != nil {
		result.err = err
		return result
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, requestURL, nil)
	if err != nil {
		result.err = err
		return result
	}
	request.Header.Set("User-Agent", "FreeProxyAPI/0.1 (proxy-probe)")
	request.Header.Set("Cache-Control", "no-cache")
	request.Header.Set("Pragma", "no-cache")
	response, err := client.Do(request)
	if err != nil {
		result.err = err
		return result
	}
	defer response.Body.Close()
	if response.StatusCode < ExpectedStatusMin || response.StatusCode > ExpectedStatusMax {
		result.err = fmt.Errorf("unexpected HTTP status %d", response.StatusCode)
		return result
	}
	body, err := readBoundedResponse(ctx, response.Body)
	if err != nil {
		result.err = fmt.Errorf("read https probe response: %w", err)
		return result
	}
	if expectedBody != "" && strings.TrimRight(string(body), " \t\r\n") != expectedBody {
		result.err = errors.New("unexpected response body")
		return result
	}
	result.OK = true
	return result
}

func defaultPort(u *url.URL, fallback string) string {
	if port := u.Port(); port != "" {
		return net.JoinHostPort(u.Hostname(), port)
	}
	return net.JoinHostPort(u.Hostname(), fallback)
}

// maxConnectResponseBytes bounds the CONNECT reply read from a proxy.
const maxConnectResponseBytes = 16 << 10

// connectTunnelDial returns a DialContext that opens a CONNECT tunnel through
// an HTTP proxy at proxyAddr (host:port). When proxyTLS is non-nil the proxy
// hop is wrapped in TLS first and a handshake failure is reported as
// *proxyTLSError. The whole handshake is bounded like the SOCKS path.
func connectTunnelDial(dialProxy dialFunc, proxyAddr string, proxyTLS *tls.Config) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("unsupported CONNECT network %q", network)
		}
		raw, err := dialProxy(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("connect proxy: %w", err)
		}
		deadline := time.Now().Add(socksConnectTimeout)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		if err := raw.SetDeadline(deadline); err != nil {
			_ = raw.Close()
			return nil, err
		}
		stopCancelWatch := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Now()) })
		conn, err := performConnect(ctx, raw, address, proxyTLS)
		watchStopped := stopCancelWatch()
		if err != nil {
			_ = raw.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("CONNECT handshake: %w", ctxErr)
			}
			return nil, err
		}
		if !watchStopped {
			_ = conn.Close()
			return nil, fmt.Errorf("CONNECT handshake: %w", ctx.Err())
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}
}

func performConnect(ctx context.Context, raw net.Conn, address string, proxyTLS *tls.Config) (net.Conn, error) {
	conn := raw
	if proxyTLS != nil {
		tlsConn := tls.Client(raw, proxyTLS)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			return nil, &proxyTLSError{err: err}
		}
		conn = tlsConn
	}
	request := "CONNECT " + address + " HTTP/1.1\r\nHost: " + address + "\r\nUser-Agent: FreeProxyAPI/0.1 (proxy-probe)\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		return nil, fmt.Errorf("write CONNECT: %w", err)
	}
	reader := bufio.NewReader(&limitedConnReader{conn: conn, remaining: maxConnectResponseBytes})
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return nil, fmt.Errorf("read CONNECT reply: %w", err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("CONNECT rejected: HTTP %d", response.StatusCode)
	}
	if reader.Buffered() > 0 {
		// A proxy must not send tunnel bytes before the client's TLS hello.
		return nil, errors.New("CONNECT reply followed by unexpected data")
	}
	return conn, nil
}

// limitedConnReader caps how many CONNECT-reply bytes are read from a proxy.
type limitedConnReader struct {
	conn      net.Conn
	remaining int
}

func (r *limitedConnReader) Read(p []byte) (int, error) {
	if r.remaining <= 0 {
		return 0, errors.New("CONNECT reply too large")
	}
	if len(p) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.conn.Read(p)
	r.remaining -= n
	return n, err
}
