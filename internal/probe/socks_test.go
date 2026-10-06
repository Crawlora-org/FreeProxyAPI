package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// fakeSOCKS4Server speaks just enough SOCKS4/4A on a net.Pipe to verify the
// client handshake byte-for-byte. The pipe is synchronous, so the server must
// drain the full request (including any 4a hostname suffix) before replying.
func fakeSOCKS4Server(t *testing.T, server net.Conn, replyCode byte) {
	t.Helper()
	request := make([]byte, 8)
	if _, err := io.ReadFull(server, request); err != nil {
		t.Errorf("read socks4 request: %v", err)
		return
	}
	if request[0] != 0x04 || request[1] != 0x01 {
		t.Errorf("unexpected socks4 header: % x", request[:2])
	}
	port := binary.BigEndian.Uint16(request[2:4])
	if port == 0 {
		t.Errorf("socks4 request carried port 0")
	}
	sentinel := bytes.Equal(request[4:8], []byte{0, 0, 0, 1})
	buf := make([]byte, 1)
	nullsSeen := 0
	if !sentinel {
		nullsSeen = 1 // plain SOCKS4 ends after the empty user-ID NUL
	}
	for nullsSeen < 2 {
		if _, err := io.ReadFull(server, buf); err != nil {
			t.Errorf("read socks4 suffix: %v", err)
			return
		}
		if buf[0] == 0x00 {
			nullsSeen++
		}
	}
	response := []byte{0x00, replyCode, 0x00, 0x00, 0, 0, 0, 0}
	if _, err := server.Write(response); err != nil {
		t.Errorf("write socks4 reply: %v", err)
	}
}

func newPipe(t *testing.T) (net.Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	deadline := time.Now().Add(2 * time.Second)
	_ = client.SetDeadline(deadline)
	_ = server.SetDeadline(deadline)
	t.Cleanup(func() { client.Close(); server.Close() })
	return client, server
}

func TestPerformSOCKS4AHandshakeSuccess(t *testing.T) {
	client, server := newPipe(t)
	go fakeSOCKS4Server(t, server, 0x5a)

	if err := performSOCKS4Handshake(context.Background(), client, "target.example.net", 80, true); err != nil {
		t.Fatalf("socks4a handshake failed: %v", err)
	}
}

func TestPerformSOCKS4HandshakeRejection(t *testing.T) {
	client, server := newPipe(t)
	go fakeSOCKS4Server(t, server, 0x5b)

	err := performSOCKS4Handshake(context.Background(), client, "target.example.net", 80, true)
	if err == nil || !strings.Contains(err.Error(), "code=0x5b") {
		t.Fatalf("expected rejection error, got %v", err)
	}
}

func TestPerformSOCKS4LiteralIPFraming(t *testing.T) {
	client, server := newPipe(t)

	done := make(chan error, 1)
	go func() {
		request := make([]byte, 9)
		if _, err := io.ReadFull(server, request); err != nil {
			done <- err
			return
		}
		// VER CMD PORT 192.0.2.1 USERID(0x00)
		if request[0] != 0x04 || request[1] != 0x01 {
			done <- errors.New("bad header")
			return
		}
		port := binary.BigEndian.Uint16(request[2:4])
		if port != 8080 {
			done <- errors.New("bad port")
			return
		}
		if !bytes.Equal(request[4:8], net.ParseIP("192.0.2.1").To4()) {
			done <- errors.New("bad ip")
			return
		}
		if request[8] != 0x00 {
			done <- errors.New("missing user-id terminator")
			return
		}
		server.Write([]byte{0x00, 0x5a, 0x00, 0x00, 0, 0, 0, 0})
		done <- nil
	}()

	if err := performSOCKS4Handshake(context.Background(), client, "192.0.2.1", 8080, true); err != nil {
		t.Fatalf("literal-ip handshake failed: %v", err)
	}
	if err := <-done; err != nil {
		t.Fatalf("server-side framing check failed: %v", err)
	}
}

func TestPerformSOCKS4LocalResolveRejectsUnsafeHostnames(t *testing.T) {
	client, server := newPipe(t)
	defer server.Close()
	// No server traffic expected: localhost must be refused before any bytes.
	if err := performSOCKS4Handshake(context.Background(), client, "localhost", 80, false); err == nil {
		t.Fatal("local resolve accepted a loopback hostname")
	}
}

func TestPerformSOCKS4LocalResolveHonorsCanceledContext(t *testing.T) {
	client, server := newPipe(t)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := performSOCKS4Handshake(ctx, client, "canceled-resolution.invalid", 80, false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("local resolution error = %v, want context canceled", err)
	}
}

func TestPerformSOCKS4LocalResolveHonorsExpiredDeadline(t *testing.T) {
	client, server := newPipe(t)
	defer server.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	err := performSOCKS4Handshake(ctx, client, "deadline-resolution.invalid", 80, false)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("local resolution error = %v, want deadline exceeded", err)
	}
}

func fakeSOCKS5Server(t *testing.T, server net.Conn, methodReply byte, connectReply byte) {
	t.Helper()
	greeting := make([]byte, 3)
	if _, err := io.ReadFull(server, greeting); err != nil {
		t.Errorf("read greeting: %v", err)
		return
	}
	if greeting[0] != 0x05 || greeting[1] != 0x01 || greeting[2] != 0x00 {
		t.Errorf("unexpected greeting: % x", greeting)
		return
	}
	if _, err := server.Write([]byte{0x05, methodReply}); err != nil {
		t.Errorf("write method reply: %v", err)
		return
	}
	if methodReply != 0x00 {
		return
	}
	header := make([]byte, 4)
	if _, err := io.ReadFull(server, header); err != nil {
		t.Errorf("read connect header: %v", err)
		return
	}
	if header[0] != 0x05 || header[1] != 0x01 || header[2] != 0x00 {
		t.Errorf("unexpected connect header: % x", header)
		return
	}
	var addrLen int
	switch header[3] {
	case 0x01:
		addrLen = 4
	case 0x03:
		lenByte := make([]byte, 1)
		if _, err := io.ReadFull(server, lenByte); err != nil {
			t.Errorf("read domain length: %v", err)
			return
		}
		addrLen = int(lenByte[0])
	default:
		t.Errorf("unexpected atyp 0x%02x", header[3])
		return
	}
	trailer := make([]byte, addrLen+2)
	if _, err := io.ReadFull(server, trailer); err != nil {
		t.Errorf("read address trailer: %v", err)
		return
	}
	server.Write([]byte{0x05, connectReply, 0x00, 0x01, 192, 0, 2, 9, 0x00, 0x50})
}

func TestPerformSOCKS5HandshakeDomainAndSuccess(t *testing.T) {
	client, server := newPipe(t)
	go fakeSOCKS5Server(t, server, 0x00, 0x00)

	if err := performSOCKS5Handshake(context.Background(), client, "target.example.net", 443, true); err != nil {
		t.Fatalf("socks5 handshake failed: %v", err)
	}
}

func TestPerformSOCKS5MethodRejected(t *testing.T) {
	client, server := newPipe(t)
	go fakeSOCKS5Server(t, server, 0x02, 0x00)

	err := performSOCKS5Handshake(context.Background(), client, "target.example.net", 443, true)
	if err == nil || !strings.Contains(err.Error(), "method=0x02") {
		t.Fatalf("expected auth-method rejection, got %v", err)
	}
}

func TestPerformSOCKS5ConnectFailedCode(t *testing.T) {
	client, server := newPipe(t)
	go fakeSOCKS5Server(t, server, 0x00, 0x01)

	err := performSOCKS5Handshake(context.Background(), client, "192.0.2.20", 443, false)
	if err == nil || !strings.Contains(err.Error(), "code=0x01") {
		t.Fatalf("expected connect failure code, got %v", err)
	}
}

// startFakeSOCKSProxy runs a minimal TCP SOCKS proxy on loopback that accepts a
// no-auth CONNECT and then replies with canned HTTP bytes, letting runProbe be
// exercised end to end through a real socket.
func startFakeSOCKSProxy(t *testing.T, version int) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if version == 5 {
			greeting := make([]byte, 3)
			if _, err := io.ReadFull(conn, greeting); err != nil {
				return
			}
			if _, err := conn.Write([]byte{0x05, 0x00}); err != nil {
				return
			}
			header := make([]byte, 4)
			if _, err := io.ReadFull(conn, header); err != nil {
				return
			}
			addrLen := map[byte]int{0x01: 4, 0x03: -1, 0x04: 16}[header[3]]
			if addrLen < 0 {
				lengthByte := make([]byte, 1)
				if _, err := io.ReadFull(conn, lengthByte); err != nil {
					return
				}
				addrLen = int(lengthByte[0])
			}
			trailer := make([]byte, addrLen+2)
			if _, err := io.ReadFull(conn, trailer); err != nil {
				return
			}
			conn.Write([]byte{0x05, 0x00, 0x00, 0x01, 192, 0, 2, 9, 0x00, 0x50})
		} else {
			request := make([]byte, 9)
			if _, err := io.ReadFull(conn, request); err != nil {
				return
			}
			// Consume optional SOCKS4a hostname suffix up to NUL.
			if bytes.Equal(request[4:8], []byte{0, 0, 0, 1}) {
				buf := make([]byte, 1)
				for {
					if _, err := io.ReadFull(conn, buf); err != nil || buf[0] == 0x00 {
						break
					}
				}
			}
			conn.Write([]byte{0x00, 0x5a, 0x00, 0x00, 0, 0, 0, 0})
		}
		// Read the tunneled HTTP request before answering so the fixture is
		// deterministic even under -race scheduling.
		request := make([]byte, 4096)
		for {
			n, err := conn.Read(request)
			if err != nil || bytes.Contains(request[:n], []byte("\r\n\r\n")) {
				break
			}
		}
		io.WriteString(conn, "HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
	}()
	return listener.Addr().String()
}

func TestRunProbeEndToEndThroughSOCKSProxies(t *testing.T) {
	cases := []struct {
		name    string
		version int
		scheme  string
		target  string
	}{
		{name: "socks5h remote resolve", version: 5, scheme: "socks5h", target: "http://target.example.net/target"},
		{name: "socks5 literal ip", version: 5, scheme: "socks5", target: "http://192.0.2.10:8080/target"},
		{name: "socks4a remote resolve", version: 4, scheme: "socks4a", target: "http://target.example.net/target"},
		{name: "socks4 literal ip", version: 4, scheme: "socks4", target: "http://192.0.2.10:8080/target"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			proxyAddr := startFakeSOCKSProxy(t, tc.version)
			result := runProbe(context.Background(),
				fmt.Sprintf("%s://%s", tc.scheme, proxyAddr),
				tc.target, 5*time.Second, plainDial)
			if !result.OK || result.StatusCode != 204 {
				t.Fatalf("probe via %s failed: %+v", tc.scheme, result)
			}
		})
	}
}

func TestSOCKSDialHonorsContextCancellationDuringHandshake(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn // never reply
		}
	}()
	t.Cleanup(func() {
		select {
		case conn := <-accepted:
			conn.Close()
		default:
		}
	})

	host, port, _ := net.SplitHostPort(listener.Addr().String())
	dial := socksDialContext(plainDial, host, port, 5, true)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(50*time.Millisecond, cancel)
	started := time.Now()
	_, err = dial(ctx, "tcp", "target.example.net:80")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("dial error = %v, want context canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("handshake ignored cancellation for %s", elapsed)
	}
}
