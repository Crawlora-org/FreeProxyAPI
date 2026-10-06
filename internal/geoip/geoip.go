// Package geoip annotates candidate endpoints with country and ASN data from
// an operator-supplied MaxMind-format database. The database itself is never
// bundled: operators obtain GeoLite2 (or equivalent) under its own license and
// mount it at a path configured via geoip_db_path. Without it the resolver is
// disabled and lookups return empty strings.
package geoip

import (
	"fmt"
	"net"
	"net/netip"

	"github.com/oschwald/geoip2-golang"
)

type Resolver struct {
	reader    *geoip2.Reader // country/city database (optional)
	asnReader *geoip2.Reader // ASN database (optional)
}

// Open loads a country-or-city mmdb file plus an optional separate ASN
// database. An empty primary path returns a disabled resolver; the ASN path is
// optional and its lookups degrade gracefully when absent or mismatched.
func Open(path, asnPath string) (*Resolver, error) {
	if path == "" {
		return &Resolver{}, nil
	}
	reader, err := geoip2.Open(path)
	if err != nil {
		return nil, err
	}
	resolver := &Resolver{reader: reader}
	if asnPath != "" {
		asnReader, err := geoip2.Open(asnPath)
		if err != nil {
			_ = reader.Close()
			return nil, fmt.Errorf("open ASN database: %w", err)
		}
		resolver.asnReader = asnReader
	}
	return resolver, nil
}

func (r *Resolver) Enabled() bool { return r != nil && r.reader != nil }

// Close releases the open databases. It is safe on nil or disabled
// resolvers, and lookups after Close return empty strings.
func (r *Resolver) Close() error {
	if r == nil {
		return nil
	}
	var firstErr error
	if r.asnReader != nil {
		firstErr = r.asnReader.Close()
		r.asnReader = nil
	}
	if r.reader != nil {
		if err := r.reader.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		r.reader = nil
	}
	return firstErr
}

// Lookup resolves country ISO code and "AS<number>" for an endpoint host.
// Hostnames are not resolved here — probes reach targets through the proxy, so
// only literal IP endpoints (the vast majority of inventories) get annotated.
func (r *Resolver) Lookup(host string) (country string, asn string) {
	if !r.Enabled() {
		return "", ""
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !addr.IsValid() {
		return "", ""
	}
	var ip net.IP
	if addr.Is4() {
		octets := addr.As4()
		ip = net.IP(octets[:])
	} else {
		octets := addr.As16()
		ip = net.IP(octets[:])
	}
	countryRecord, err := r.reader.Country(ip)
	if err == nil {
		country = countryRecord.Country.IsoCode
	}
	asnSource := r.asnReader
	if asnSource == nil {
		asnSource = r.reader // combined databases carry ASN records too
	}
	asnRecord, err := asnSource.ASN(ip)
	if err == nil && asnRecord.AutonomousSystemNumber > 0 {
		asn = "AS" + itoa(uint64(asnRecord.AutonomousSystemNumber))
	}
	return country, asn
}

func itoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var digits [20]byte
	i := len(digits)
	for v > 0 {
		i--
		digits[i] = byte('0' + v%10)
		v /= 10
	}
	return string(digits[i:])
}
