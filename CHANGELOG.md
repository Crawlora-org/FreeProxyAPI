# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [0.1.0] - 2026-10-06

First public release.

### Security
- Proxy hostnames must be plain ASCII hostnames or IP literals. Feed entries
  containing shell or HTML metacharacters, percent-decoded bytes, or IPv6 zone
  IDs are rejected, and stored URLs are re-validated before the API returns
  them.
- `/proxies` validates `country`, `exit_country`, `asn`, and `anonymity`,
  clamps numeric filters, and bounds concurrent uncached queries so rotating a
  filter value cannot exhaust the Redis read pool. Excess queries return `503`
  with `Retry-After`.
- The built-in pages send a hash-based Content-Security-Policy, `nosniff`, and
  a Referrer-Policy.
- Source URLs are logged as `scheme://host`, and URLs are redacted from fetch
  errors.
- Responses have a 30s write timeout.

### Added
- `admin_listen_addr` serves `/metrics`, `/report`, and the internal API on a
  separate listener.
- `public_base_url` and `analytics_measurement_id` configure the built-in
  pages. Analytics are off by default.
- `max_records_per_source` and `max_candidates` bound feed intake, with
  `freeproxyapi_sources_truncated_total` and
  `freeproxyapi_candidates_capped_total`.
- `FREEPROXYAPI_REDIS_URL` overrides `redis_url`.
- `cloudflare_web_analytics` lets Cloudflare's injected Web Analytics beacon
  through the pages' Content-Security-Policy.
- The built-in pages show MaxMind's GeoLite2 attribution when a GeoIP database
  is configured.
- HTTP API reference, privacy notes, acceptable-use policy, and contributor
  documentation.

### Changed
- The built-in pages no longer embed the hosted service's analytics ID or
  canonical URLs.
- Malformed `country`, `exit_country`, `asn`, or `anonymity` filters now return
  `400` instead of matching nothing; `anonymity` is case-insensitive and `asn`
  accepts `15169` as well as `AS15169`.
