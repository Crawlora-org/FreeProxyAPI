package geoip

import (
	"os"
	"testing"
)

// TestOpenRealDatabase is an opt-in integration check against a locally
// downloaded mmdb file; set TEST_GEOIP_DB to run it.
func TestOpenRealDatabase(t *testing.T) {
	path := os.Getenv("TEST_GEOIP_DB")
	if path == "" {
		t.Skip("TEST_GEOIP_DB not set")
	}
	r, err := Open(path, "")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	country, _ := r.Lookup("8.8.8.8")
	if country == "" {
		t.Fatal("lookup of 8.8.8.8 returned an empty country against a real database")
	}
	t.Logf("8.8.8.8 -> %s", country)
}

// TestOpenRealDatabaseASNFailureAndClose checks the ASN-open error path and
// Close against a real primary database; set TEST_GEOIP_DB to run it.
func TestOpenRealDatabaseASNFailureAndClose(t *testing.T) {
	path := os.Getenv("TEST_GEOIP_DB")
	if path == "" {
		t.Skip("TEST_GEOIP_DB not set")
	}
	if _, err := Open(path, "/nonexistent/GeoLite2-ASN.mmdb"); err == nil {
		t.Fatal("Open accepted a missing ASN database")
	}
	r, err := Open(path, "")
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if r.Enabled() {
		t.Fatal("resolver still enabled after Close")
	}
}
