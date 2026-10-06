package endpoint

import (
	"context"
	"testing"
	"time"
)

func TestPublicDialerConnectTimeout(t *testing.T) {
	if got := (PublicDialer{ConnectTimeout: 3 * time.Second}).netDialer().Timeout; got != 3*time.Second {
		t.Fatalf("probe dialer timeout = %s, want 3s", got)
	}
	// Source fetches use DialPublicContext, which must keep no connect
	// timeout of its own.
	if got := (PublicDialer{}).netDialer().Timeout; got != 0 {
		t.Fatalf("default dialer timeout = %s, want 0", got)
	}
}

func TestPublicDialerKeepsAddressPolicy(t *testing.T) {
	for _, dial := range []func(context.Context, string, string) (interface{ Close() error }, error){
		func(ctx context.Context, n, a string) (interface{ Close() error }, error) {
			return PublicDialer{ConnectTimeout: time.Second}.DialContext(ctx, n, a)
		},
		func(ctx context.Context, n, a string) (interface{ Close() error }, error) {
			return DialPublicContext(ctx, n, a)
		},
	} {
		conn, err := dial(context.Background(), "tcp", "127.0.0.1:9")
		if err == nil {
			_ = conn.Close()
			t.Fatal("dialer accepted a loopback address")
		}
	}
}
