package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWithConnectTimeoutPlumbsHopDialerTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	original := publicHopDialer
	t.Cleanup(func() { publicHopDialer = original })
	var got []time.Duration
	publicHopDialer = func(connectTimeout time.Duration) dialFunc {
		got = append(got, connectTimeout)
		return plainDial
	}

	result := TestWithConnectTimeout(context.Background(), server.URL, server.URL+"/target", 5*time.Second, 1500*time.Millisecond)
	if !result.OK {
		t.Fatalf("probe through fixture failed: %+v", result)
	}
	if len(got) != 1 || got[0] != 1500*time.Millisecond {
		t.Fatalf("hop dialer timeouts = %v, want [1.5s]", got)
	}
}
