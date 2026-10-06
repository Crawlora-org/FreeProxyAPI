package geoip

import "testing"

func TestDisabledResolverReturnsEmpty(t *testing.T) {
	r, err := Open("", "")
	if err != nil {
		t.Fatalf("Open(\"\"): %v", err)
	}
	if r.Enabled() {
		t.Fatal("empty path must produce a disabled resolver")
	}
	country, asn := r.Lookup("192.0.2.10")
	if country != "" || asn != "" {
		t.Fatalf("disabled lookup returned %q/%q", country, asn)
	}
}

func TestLookupIgnoresHostnamesAndBadInput(t *testing.T) {
	// Enabled() is false here, but the hostname short-circuit must come first
	// regardless of database presence.
	r := &Resolver{}
	for _, host := range []string{"proxy.example.net", "", "[::1]:80"} {
		if country, asn := r.Lookup(host); country != "" || asn != "" {
			t.Errorf("Lookup(%q) = %q/%q, want empty", host, country, asn)
		}
	}
}

func TestOpenRejectsMissingDatabase(t *testing.T) {
	if _, err := Open("/nonexistent/GeoLite2-Country.mmdb", ""); err == nil {
		t.Fatal("Open accepted a missing database file")
	}
}

func TestCloseIsSafeOnDisabledResolver(t *testing.T) {
	var nilResolver *Resolver
	if err := nilResolver.Close(); err != nil {
		t.Fatalf("nil Close: %v", err)
	}
	r, _ := Open("", "")
	if err := r.Close(); err != nil {
		t.Fatalf("disabled Close: %v", err)
	}
	if country, asn := r.Lookup("192.0.2.10"); country != "" || asn != "" {
		t.Fatalf("lookup after Close = %q/%q", country, asn)
	}
}
