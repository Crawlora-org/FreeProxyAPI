# Changelog

All notable changes are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Removed
- `terraform/` (Rackspace Spot provisioning and its kubeconfig helper) and
  `scripts/refresh-kubeconfig.sh` are no longer part of the repository, and
  `terraform/` is gitignored so local infrastructure code is never committed.
  `scripts/ci-kubeconfig.sh` now requires `--admin-kubeconfig PATH` and works
  with any cluster.

### Changed
- `/proxies` returns 100 results when `limit` is omitted, zero, negative, or
  invalid (it was up to 1000). A larger explicit `limit` is still honored up to
  the maximum page size. Page with `offset` while `has_more` is true. Page sizes
  above 100 are deprecated: the maximum on the hosted service will be lowered to
  100 once `freeproxyapi_public_large_page_requests_total` stops growing.
- `proxy-router` follows the public API's paging. It still asks for the whole
  `FREEPROXYAPI_LIMIT` first, so against today's API it makes one request, and
  it continues with `offset` when the API serves smaller pages. A failed page
  keeps the previous GOST config instead of replacing it with a partial pool.
- Image pins in `k8s/base`, `private-cluster`, `cluster-smoke`, `gost-router`,
  `gost-public-sidecar` and `docker-compose.gost-public.yml` move to
  `20261006180336`, the first build whose `proxy-router` follows `/proxies`
  paging. The `gost-router` overlay also relies on `proxy-router` writing its
  own GOST bootstrap config, which older pins lack. Against a server capped at
  100 results per page, the previous router built a config with 100 of 437
  proxies and this one builds all 437. The `live-local` pin is left alone: the
  deploy job rewrites it on every deploy, so production renders are unchanged.
- Google Analytics, when enabled with `analytics_measurement_id`, now loads on
  every page view of the built-in pages. Before, it loaded after the first
  interaction or after 3.5 seconds. There is no consent banner, and Global
  Privacy Control and Do Not Track are not honored. Leave the ID empty to load
  nothing. New `privacy_url` option adds a small "Privacy" link to the pages.

### Fixed
- `/proxies` returned `503 {"error":"query failed"}` on cache misses that scan
  the whole validated set, such as the `limit=1000` query `proxy-router` makes
  when fewer proxies match than requested. On the hosted service that scan took
  about 14 Redis round trips and, on a slow Redis, exceeded the handler's 3
  second budget; failed loads were not cached, so every retry paid again.
  - The scan now uses larger `SSCAN` pages and pipelines: 3 round trips for
    about 800 members.
  - Loads get their own 8 second budget instead of the 3 second readiness
    probe's, and the request waits for it.
  - `/proxies` and `/stats` answer an expired entry from the previous response
    (up to five minutes old) while one background refresh replaces it, so a
    slow or failing Redis no longer turns a recently served query into an
    error. Stale responses carry a short `Cache-Control` and a `Warning: 110`
    header, and `503` responses now send `Retry-After: 5`.
  - New metrics `freeproxyapi_public_query_failures_total`,
    `freeproxyapi_public_stale_responses_total` and
    `freeproxyapi_public_refresh_failures_total`, and alerts
    `RelayAuditPublicAPIQueryFailures` and `RelayAuditPublicAPIRefreshFailing`.
    Public 503s were not counted anywhere before, so nothing alerted on them.
- `k8s/overlays/gost-public-sidecar` copied a bootstrap config with fixed
  `bootstrap`/`bootstrap` API credentials, while `proxy-router` reloaded GOST
  with the credentials from the `gost-public-auth` Secret, so every reload
  failed with HTTP 401 and the proxy listeners never opened unless the Secret
  used those exact values. The overlay now lets `proxy-router` write the
  bootstrap config with the Secret's credentials, adds a `gost` startup probe,
  and pins an image that includes that behavior. Removed the unused
  `bootstrap-configmap.yaml` and `examples/gost-public-bootstrap.json`.

### Added
- `public_max_limit` sets the largest `/proxies` page (default 1000, range
  1 to 1000), and `freeproxyapi_public_large_page_requests_total` counts
  requests for more than 100 results, so the maximum can be lowered once nothing
  depends on large pages. See "Lowering the public page size" in
  `docs/deployment.md`.
- `k8s/ci-deployer/rbac.yaml` and a step-by-step procedure in
  `docs/deployment.md` for creating the namespace-scoped CI deployer identity
  behind the `KUBE_CONFIG_B64` secret, and `scripts/ci-kubeconfig.sh`, an
  admin-run script that automates it with an admin kubeconfig for any cluster.

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
