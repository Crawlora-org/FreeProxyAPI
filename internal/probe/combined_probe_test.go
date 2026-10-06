package probe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// TestExpectBodyWithConnectTimeoutCombinesBodyCheckAndHopDialer guards the
// production standard-mode probe: it must both enforce the expected body and
// bound the proxy-hop connect, not just one of the two.
func TestExpectBodyWithConnectTimeoutCombinesBodyCheckAndHopDialer(t *testing.T) {
	var sawCacheBuster bool
	body := "success\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, cacheBusterParam+"=") {
			sawCacheBuster = true
		}
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	original := publicHopDialer
	t.Cleanup(func() { publicHopDialer = original })
	var got []time.Duration
	publicHopDialer = func(connectTimeout time.Duration) dialFunc {
		got = append(got, connectTimeout)
		return plainDial
	}

	result := TestExpectBodyWithConnectTimeout(context.Background(), server.URL, server.URL+"/success.txt", 5*time.Second, 1500*time.Millisecond, "success")
	if !result.OK {
		t.Fatalf("matching body failed: %+v", result)
	}
	if len(got) != 1 || got[0] != 1500*time.Millisecond {
		t.Fatalf("hop dialer timeouts = %v, want [1.5s]", got)
	}
	if !sawCacheBuster {
		t.Fatal("standard probe did not send the cache-buster parameter")
	}

	body = "<html>router login</html>"
	result = TestExpectBodyWithConnectTimeout(context.Background(), server.URL, server.URL+"/success.txt", 5*time.Second, 1500*time.Millisecond, "success")
	if result.OK || !strings.Contains(result.Error, "unexpected response body") {
		t.Fatalf("mismatched body result = %+v, want unexpected response body failure", result)
	}
	if len(got) != 2 || got[1] != 1500*time.Millisecond {
		t.Fatalf("hop dialer timeouts after second probe = %v", got)
	}
}
