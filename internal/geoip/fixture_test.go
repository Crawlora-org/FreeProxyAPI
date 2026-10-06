package geoip

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The geoip2/maxminddb dependencies can only read databases, so these tests
// assemble a minimal MaxMind DB in memory: an IPv4 search tree with one node
// whose two records both point at the same data record, so every IPv4 address
// resolves to that record.

func mmdbString(s string) []byte {
	if len(s) >= 29 {
		panic("mmdbString: extended sizes not supported")
	}
	return append([]byte{0x40 | byte(len(s))}, s...)
}

func mmdbUint16(v uint16) []byte { return []byte{0xA2, byte(v >> 8), byte(v)} }

func mmdbUint32(v uint32) []byte {
	return []byte{0xC4, byte(v >> 24), byte(v >> 16), byte(v >> 8), byte(v)}
}

func mmdbMap(pairs ...[]byte) []byte {
	if len(pairs)%2 != 0 || len(pairs)/2 >= 29 {
		panic("mmdbMap: bad pair count")
	}
	out := []byte{0xE0 | byte(len(pairs)/2)}
	for _, part := range pairs {
		out = append(out, part...)
	}
	return out
}

func buildMMDB(databaseType string, record []byte) []byte {
	const nodeCount = 1
	var buf bytes.Buffer
	// 24-bit records; value nodeCount+16+0 points at data-section offset 0.
	pointer := uint32(nodeCount + 16)
	for range 2 {
		buf.Write([]byte{byte(pointer >> 16), byte(pointer >> 8), byte(pointer)})
	}
	buf.Write(make([]byte, 16)) // data section separator
	buf.Write(record)
	buf.WriteString("\xAB\xCD\xEFMaxMind.com")
	buf.Write(mmdbMap(
		mmdbString("node_count"), mmdbUint32(nodeCount),
		mmdbString("record_size"), mmdbUint16(24),
		mmdbString("ip_version"), mmdbUint16(4),
		mmdbString("database_type"), mmdbString(databaseType),
		mmdbString("binary_format_major_version"), mmdbUint16(2),
		mmdbString("binary_format_minor_version"), mmdbUint16(0),
	))
	return buf.Bytes()
}

func writeFixture(t *testing.T, name string, content []byte) string {
	t.Helper()
	file := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(file, content, 0o600); err != nil {
		t.Fatal(err)
	}
	return file
}

func countryDB(t *testing.T) string {
	record := mmdbMap(
		mmdbString("country"), mmdbMap(mmdbString("iso_code"), mmdbString("US")),
		mmdbString("autonomous_system_number"), mmdbUint32(64500),
	)
	return writeFixture(t, "country.mmdb", buildMMDB("GeoLite2-Country", record))
}

func asnDB(t *testing.T, asn uint32) string {
	record := mmdbMap(mmdbString("autonomous_system_number"), mmdbUint32(asn))
	return writeFixture(t, "asn.mmdb", buildMMDB("GeoLite2-ASN", record))
}

func TestLookupWithCountryAndASNDatabases(t *testing.T) {
	r, err := Open(countryDB(t), asnDB(t, 15169))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if !r.Enabled() {
		t.Fatal("resolver with a database must be enabled")
	}
	for _, host := range []string{"192.0.2.10", "10.0.0.1", "127.0.0.1"} {
		if country, asn := r.Lookup(host); country != "US" || asn != "AS15169" {
			t.Errorf("Lookup(%q) = %q/%q, want US/AS15169", host, country, asn)
		}
	}
	// Hostnames, malformed input, and IPv6 against an IPv4-only database all
	// degrade to empty annotations rather than errors.
	for _, host := range []string{"proxy.example.net", "", "999.1.1.1", "192.0.2.10:8080", "2001:db8::1"} {
		if country, asn := r.Lookup(host); country != "" || asn != "" {
			t.Errorf("Lookup(%q) = %q/%q, want empty", host, country, asn)
		}
	}
}

func TestLookupCountryOnlyDatabaseSkipsASN(t *testing.T) {
	// A Country database rejects ASN queries (InvalidMethodError); the
	// resolver must still return the country.
	r, err := Open(countryDB(t), "")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if country, asn := r.Lookup("198.51.100.7"); country != "US" || asn != "" {
		t.Fatalf("Lookup = %q/%q, want US/empty", country, asn)
	}
}

func TestLookupIgnoresZeroASN(t *testing.T) {
	r, err := Open(countryDB(t), asnDB(t, 0))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if country, asn := r.Lookup("203.0.113.1"); country != "US" || asn != "" {
		t.Fatalf("Lookup = %q/%q, want US/empty", country, asn)
	}
}

func TestOpenASNErrors(t *testing.T) {
	country := countryDB(t)
	_, err := Open(country, filepath.Join(t.TempDir(), "missing-asn.mmdb"))
	if err == nil || !strings.Contains(err.Error(), "open ASN database") {
		t.Fatalf("missing ASN error = %v", err)
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing ASN error %v does not wrap os.ErrNotExist", err)
	}
	garbage := writeFixture(t, "garbage.mmdb", []byte("not a maxmind database"))
	if _, err := Open(country, garbage); err == nil {
		t.Fatal("Open accepted a corrupt ASN database")
	}
}

func TestOpenRejectsInvalidPrimaryDatabase(t *testing.T) {
	if _, err := Open(filepath.Join(t.TempDir(), "absent.mmdb"), ""); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing primary error = %v, want os.ErrNotExist", err)
	}
	if _, err := Open(writeFixture(t, "garbage.mmdb", []byte("garbage")), ""); err == nil {
		t.Fatal("Open accepted a corrupt primary database")
	}
	if _, err := Open(t.TempDir(), ""); err == nil {
		t.Fatal("Open accepted a directory as a database")
	}
	unknown := writeFixture(t, "unknown.mmdb", buildMMDB("Example-Unknown", mmdbMap()))
	if _, err := Open(unknown, ""); err == nil {
		t.Fatal("Open accepted an unknown database type")
	}
}

func TestCloseIsIdempotent(t *testing.T) {
	r, err := Open(countryDB(t), asnDB(t, 15169))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for i := range 3 {
		if err := r.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i+1, err)
		}
	}
	if r.Enabled() {
		t.Fatal("resolver still enabled after Close")
	}
	if country, asn := r.Lookup("192.0.2.10"); country != "" || asn != "" {
		t.Fatalf("lookup after Close = %q/%q", country, asn)
	}
}

func TestZeroValueAndNilResolver(t *testing.T) {
	var nilResolver *Resolver
	if nilResolver.Enabled() {
		t.Fatal("nil resolver reports enabled")
	}
	if country, asn := nilResolver.Lookup("192.0.2.10"); country != "" || asn != "" {
		t.Fatalf("nil lookup = %q/%q", country, asn)
	}
	for range 2 {
		if err := nilResolver.Close(); err != nil {
			t.Fatalf("nil Close: %v", err)
		}
		if err := (&Resolver{}).Close(); err != nil {
			t.Fatalf("zero-value Close: %v", err)
		}
	}
}

func TestItoa(t *testing.T) {
	for v, want := range map[uint64]string{
		0:              "0",
		7:              "7",
		15169:          "15169",
		math.MaxUint64: "18446744073709551615",
	} {
		if got := itoa(v); got != want {
			t.Errorf("itoa(%d) = %q, want %q", v, got, want)
		}
	}
}
