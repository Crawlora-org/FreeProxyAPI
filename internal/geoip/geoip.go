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
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/oschwald/geoip2-golang"
)

type Resolver struct {
	reader    *geoip2.Reader // country/city database (optional)
	asnReader *geoip2.Reader // ASN database (optional)
}

// Details are best-effort annotations for one literal IP. Hosting remains nil
// when the ASN organization does not contain a well-known hosting term; lack
// of a match is not evidence that an address is residential.
type Details struct {
	Country      string
	ASN          string
	Organization string
	Hosting      *bool
}

// Open loads independently optional country/city and ASN databases. Without
// either path the resolver is disabled; an ASN-only database needs no country
// database to provide provider annotations.
func Open(path, asnPath string) (*Resolver, error) {
	resolver := &Resolver{}
	if path != "" {
		reader, err := geoip2.Open(path)
		if err != nil {
			return nil, err
		}
		resolver.reader = reader
	}
	if asnPath != "" {
		reader, err := geoip2.Open(asnPath)
		if err != nil {
			_ = resolver.Close()
			return nil, fmt.Errorf("open ASN database: %w", err)
		}
		resolver.asnReader = reader
	}
	return resolver, nil
}

func (r *Resolver) Enabled() bool { return r != nil && (r.reader != nil || r.asnReader != nil) }

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
	details := r.LookupDetails(host)
	return details.Country, details.ASN
}

// LookupDetails resolves country, ASN, and ASN organization for a literal IP.
// Hostnames are not resolved here. Hosting is true only when the ASN
// organization matches a known hosting/cloud/data-center term, and otherwise
// remains unknown.
func (r *Resolver) LookupDetails(host string) Details {
	var details Details
	if !r.Enabled() {
		return details
	}
	addr, err := netip.ParseAddr(host)
	if err != nil || !addr.IsValid() {
		return details
	}
	var ip net.IP
	if addr.Is4() {
		octets := addr.As4()
		ip = net.IP(octets[:])
	} else {
		octets := addr.As16()
		ip = net.IP(octets[:])
	}
	if r.reader != nil {
		countryRecord, err := r.reader.Country(ip)
		if err == nil {
			details.Country = countryRecord.Country.IsoCode
		}
	}
	asnSource := r.asnReader
	if asnSource == nil {
		asnSource = r.reader // combined databases carry ASN records too
	}
	asnRecord, err := asnSource.ASN(ip)
	if err == nil && asnRecord.AutonomousSystemNumber > 0 {
		details.ASN = "AS" + itoa(uint64(asnRecord.AutonomousSystemNumber))
	}
	if err == nil {
		details.Organization = strings.TrimSpace(strings.Map(func(r rune) rune {
			if unicode.IsControl(r) {
				return -1
			}
			return r
		}, asnRecord.AutonomousSystemOrganization))
		if len(details.Organization) > 256 {
			details.Organization = details.Organization[:256]
			for !utf8.ValidString(details.Organization) {
				details.Organization = details.Organization[:len(details.Organization)-1]
			}
		}
		if isKnownHostingOrganization(details.Organization) {
			hosting := true
			details.Hosting = &hosting
		}
	}
	return details
}

func isKnownHostingOrganization(org string) bool {
	org = strings.ToLower(org)
	for _, term := range []string{
		"hosting", "cloud", "data center", "datacenter", "colocation", "colocrossing",
		"amazon", "google cloud", "microsoft azure", "digitalocean",
		"hetzner", "vultr", "linode", "leaseweb", "oracle cloud",
	} {
		if strings.Contains(org, term) {
			return true
		}
	}
	for _, word := range strings.FieldsFunc(org, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		if word == "aws" || word == "ovh" || word == "colo" {
			return true
		}
	}
	return false
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
