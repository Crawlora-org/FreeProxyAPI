package probe

import (
	"net/http"
	"testing"
)

func TestClassifyEchoResponseAnonymityClasses(t *testing.T) {
	cases := []struct {
		name    string
		origin  string
		headers http.Header
		payload string
		want    string
	}{
		{name: "elite", origin: "198.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"Host":"echo.example"}}`, want: AnonymityElite},
		{name: "anonymous via header without our address", origin: "198.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"Via":"1.1 squid"}}`, want: AnonymityAnonymous},
		{name: "transparent exit equals origin", origin: "198.51.100.7", payload: `{"origin":"198.51.100.7","headers":{}}`, want: AnonymityTransparent},
		{name: "transparent forwarded origin", origin: "198.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"198.51.100.7"}}`, want: AnonymityTransparent},
		{name: "transparent forwarded origin with port", origin: "198.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"Forwarded":"for=198.51.100.7:4711"}}`, want: AnonymityTransparent},
		{name: "transparent origin ends a sentence", origin: "198.51.100.7", payload: "ip 203.0.113.9\nclient 198.51.100.7.", want: AnonymityTransparent},
		{name: "longer address is not a leak", origin: "198.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"198.51.100.71"}}`, want: AnonymityAnonymous},
		{name: "prefixed address is not a leak", origin: "98.51.100.7", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"198.51.100.7"}}`, want: AnonymityAnonymous},
		{name: "transparent IPv6 forwarded origin", origin: "2001:DB8::7", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"[2001:db8::7]"}}`, want: AnonymityTransparent},
		{name: "longer IPv6 is not a leak", origin: "2001:db8::7", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"2001:db8::7a"}}`, want: AnonymityAnonymous},
		{name: "unknown origin falls back to headers", origin: "", payload: `{"origin":"203.0.113.9","headers":{"X-Forwarded-For":"198.51.100.7"}}`, want: AnonymityAnonymous},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			headers := tc.headers
			if headers == nil {
				headers = http.Header{}
			}
			got := classifyEchoResponse(http.StatusOK, headers, tc.payload, tc.origin)
			if got.Class != tc.want {
				t.Fatalf("class = %q, want %q (exit=%q)", got.Class, tc.want, got.ExitIP)
			}
		})
	}
}
