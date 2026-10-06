package probe

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/Crawlora-org/FreeProxyAPI/internal/endpoint"
)

const (
	socksConnectTimeout = 10 * time.Second
	socksReadTimeout    = 10 * time.Second
	maxHostnameLength   = 255
)

// dialFunc dials one TCP hop. Production passes endpoint.DialPublicContext so
// unsafe literal or resolved addresses are refused; tests may substitute a
// plain dialer to reach loopback fixtures.
type dialFunc func(ctx context.Context, network, address string) (net.Conn, error)

// socksDialContext returns a DialContext that tunnels every connection through
// the given credential-free SOCKS proxy. When remoteResolve is true (socks4a,
// socks5h) the hostname is sent to the proxy instead of resolved locally.
//
// The proxy hop itself is dialed through the supplied dialFunc, which refuses
// unsafe proxy addresses before any handshake bytes are written.
func socksDialContext(dialProxy dialFunc, proxyHost, proxyPort string, version int, remoteResolve bool) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		if network != "tcp" && network != "tcp4" && network != "tcp6" {
			return nil, fmt.Errorf("unsupported SOCKS network %q", network)
		}
		host, portText, err := net.SplitHostPort(address)
		if err != nil {
			return nil, fmt.Errorf("split tunnel address: %w", err)
		}
		port, err := strconv.Atoi(portText)
		if err != nil || port <= 0 || port > 65535 {
			return nil, fmt.Errorf("invalid tunnel port %q", portText)
		}

		conn, err := dialProxy(ctx, "tcp", net.JoinHostPort(proxyHost, proxyPort))
		if err != nil {
			return nil, fmt.Errorf("connect proxy: %w", err)
		}

		// Bound the handshake unconditionally. It runs inside DialContext before
		// http.Transport owns the conn, so transport-level cancellation cannot
		// reach it; a proxy that accepts TCP but never replies must not pin a
		// goroutine forever. Same lesson the legacy monitor learned.
		deadline := time.Now().Add(socksConnectTimeout)
		if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
			deadline = ctxDeadline
		}
		if err := conn.SetDeadline(deadline); err != nil {
			_ = conn.Close()
			return nil, err
		}
		// Cancellation (not only the deadline) must interrupt blocked handshake
		// reads and writes.
		stopCancelWatch := context.AfterFunc(ctx, func() {
			_ = conn.SetDeadline(time.Now())
		})

		var handshakeErr error
		switch version {
		case 4:
			handshakeErr = performSOCKS4Handshake(ctx, conn, host, uint16(port), remoteResolve)
		case 5:
			handshakeErr = performSOCKS5Handshake(ctx, conn, host, uint16(port), remoteResolve)
		default:
			handshakeErr = fmt.Errorf("unsupported SOCKS version %d", version)
		}
		watchStopped := stopCancelWatch()
		if handshakeErr != nil {
			_ = conn.Close()
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, fmt.Errorf("SOCKS handshake: %w", ctxErr)
			}
			return nil, handshakeErr
		}
		if !watchStopped {
			// The context ended after the handshake finished; the deadline may
			// already have been forced into the past.
			_ = conn.Close()
			return nil, fmt.Errorf("SOCKS handshake: %w", ctx.Err())
		}
		_ = conn.SetDeadline(time.Time{})
		return conn, nil
	}
}

// performSOCKS4Handshake speaks SOCKS4, or the SOCKS4a extension when
// remoteResolve is true. Credentials are never sent: the audit inventory is
// credential-free by construction.
func performSOCKS4Handshake(ctx context.Context, conn net.Conn, host string, port uint16, remoteResolve bool) error {
	if len(host) > maxHostnameLength {
		return fmt.Errorf("SOCKS target hostname too long")
	}
	request := []byte{0x04, 0x01, byte(port >> 8), byte(port)}
	ip4 := net.ParseIP(host).To4()
	switch {
	case ip4 != nil:
		request = append(request, ip4...)
	case remoteResolve:
		// SOCKS4a: sentinel IP 0.0.0.1 tells the proxy the hostname follows the
		// (empty) user ID.
		request = append(request, 0x00, 0x00, 0x00, 0x01)
	default:
		resolved, err := lookupPublicIPv4(ctx, host)
		if err != nil {
			return err
		}
		request = append(request, resolved...)
	}
	request = append(request, 0x00) // empty user ID terminator
	if remoteResolve && ip4 == nil {
		request = append(request, []byte(host)...)
		request = append(request, 0x00)
	}

	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write SOCKS4 request: %w", err)
	}
	response := make([]byte, 8)
	if _, err := io.ReadFull(conn, response); err != nil {
		return fmt.Errorf("read SOCKS4 reply: %w", err)
	}
	if response[1] != 0x5a {
		return fmt.Errorf("SOCKS4 connect rejected: code=0x%02x", response[1])
	}
	return nil
}

// performSOCKS5Handshake performs a no-auth SOCKS5 CONNECT. GSSAPI and
// username/password methods are intentionally not offered.
func performSOCKS5Handshake(ctx context.Context, conn net.Conn, host string, port uint16, remoteResolve bool) error {
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return fmt.Errorf("write SOCKS5 greeting: %w", err)
	}
	greeting := make([]byte, 2)
	if _, err := io.ReadFull(conn, greeting); err != nil {
		return fmt.Errorf("read SOCKS5 greeting reply: %w", err)
	}
	if greeting[0] != 0x05 {
		return fmt.Errorf("unexpected SOCKS5 greeting version 0x%02x", greeting[0])
	}
	if greeting[1] != 0x00 {
		return fmt.Errorf("SOCKS5 proxy rejected no-auth method: method=0x%02x", greeting[1])
	}

	atyp := byte(0x01)
	var addrBytes []byte
	ip := net.ParseIP(host)
	switch {
	case ip != nil && ip.To4() != nil:
		addrBytes = ip.To4()
	case ip != nil:
		atyp = 0x04
		addrBytes = ip.To16()
	case remoteResolve:
		if len(host) == 0 || len(host) > maxHostnameLength {
			return fmt.Errorf("SOCKS target hostname too long")
		}
		atyp = 0x03
		addrBytes = append([]byte{byte(len(host))}, host...)
	default:
		resolved, err := lookupPublicIPv4(ctx, host)
		if err != nil {
			return err
		}
		addrBytes = resolved
	}

	request := make([]byte, 0, 7+len(addrBytes))
	request = append(request, 0x05, 0x01, 0x00, atyp)
	request = append(request, addrBytes...)
	var portBytes [2]byte
	binary.BigEndian.PutUint16(portBytes[:], port)
	request = append(request, portBytes[:]...)
	if _, err := conn.Write(request); err != nil {
		return fmt.Errorf("write SOCKS5 connect: %w", err)
	}

	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return fmt.Errorf("read SOCKS5 reply: %w", err)
	}
	if header[1] != 0x00 {
		return fmt.Errorf("SOCKS5 connect failed: code=0x%02x", header[1])
	}
	var bindLen int
	switch header[3] {
	case 0x01:
		bindLen = 4
	case 0x03:
		bindLen = -1 // length-prefixed domain, read below
	case 0x04:
		bindLen = 16
	default:
		return fmt.Errorf("unexpected SOCKS5 reply address type 0x%02x", header[3])
	}
	if bindLen < 0 {
		lengthByte := make([]byte, 1)
		if _, err := io.ReadFull(conn, lengthByte); err != nil {
			return fmt.Errorf("read SOCKS5 reply domain length: %w", err)
		}
		bindLen = int(lengthByte[0])
	}
	trailer := make([]byte, bindLen+2)
	if _, err := io.ReadFull(conn, trailer); err != nil {
		return fmt.Errorf("read SOCKS5 reply trailer: %w", err)
	}
	return nil
}

func lookupPublicIPv4(ctx context.Context, host string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("resolve SOCKS4 target: %w", err)
	}
	if literal, err := netip.ParseAddr(host); err == nil {
		if literal.Is4() && !endpoint.UnsafeIP(literal) {
			octets := literal.As4()
			return octets[:], nil
		}
		return nil, fmt.Errorf("SOCKS4 target must be a public IPv4 or resolvable host")
	}
	addrs, err := net.DefaultResolver.LookupNetIP(ctx, "ip4", host)
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, fmt.Errorf("resolve SOCKS4 target: %w", contextErr)
	}
	if err != nil {
		return nil, fmt.Errorf("resolve SOCKS4 target: %w", err)
	}
	for _, addr := range addrs {
		if !endpoint.UnsafeIP(addr) {
			octets := addr.As4()
			return octets[:], nil
		}
	}
	return nil, fmt.Errorf("SOCKS4 target did not resolve to a public IPv4 address")
}
