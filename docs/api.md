# HTTP API reference

The monitor listens on `listen_addr` (default `:8080`). Examples use the hosted
service; replace the host with your own. All responses are JSON unless noted.
Availability can change after a response is cached, so treat results as a
snapshot, not a guarantee.

## `GET /proxies`

Public, validated proxies. Results are cached for 30 seconds, rate limited to
60 requests per minute per client IP (`429` with `Retry-After`), and served with
`Access-Control-Allow-Origin: *`.

```sh
curl 'http://localhost:8080/proxies?exit_asn=AS15169&stable_exit=1&min_reliability_7d_pct=90&min_reliability_7d_samples=20&limit=2'
```

The example assumes a local monitor running a version with these optional
filters. Hosted-service filter availability depends on its deployment.

### Parameters

| Parameter | Type | Notes |
| --- | --- | --- |
| `country` | two letters | Country of the proxy's own IP (entry country). Case-insensitive. |
| `exit_country` | two letters | Country of the exit IP observed by the echo check. Unknown or stale values never match. |
| `exit_asn` | `AS15169` or `15169` | ASN of the observed exit IP. Unknown or stale values never match. |
| `asn` | `AS15169` or `15169` | Autonomous system of the proxy's IP. |
| `anonymity` | `elite`, `anonymous`, `transparent` | Anonymity class from the echo check. Unknown or stale values never match. |
| `geo_mismatch` | bool | Only proxies whose exit country differs from their entry country. |
| `exclude_hosting` | bool | Drop exits explicitly tagged as hosting by the ASN-name heuristic. Unknown classifications stay included; an unrecognized name does not mean residential. |
| `stable_exit` | bool | Require at least two actual exit-IP observations in the last seven days and no observed change in that period. Missing observations do not count as stable. |
| `min_success_streak` | integer | Minimum consecutive successful probes. A failure resets the streak to zero. |
| `min_reliability_7d_pct` | 0–100 | Minimum observed-success percentage over available samples in the last seven days. |
| `min_reliability_7d_samples` | integer | Minimum number of actual probe samples in the last seven days. Use with the percentage filter to require a useful evidence base. |
| `target_profile` | configured name | Require a fresh passing result for this operator-configured target profile. An unconfigured name returns `400`. |
| `max_latency_ms` | integer | Maximum smoothed latency. Capped at 600000. |
| `min_ratio_pct` | integer 0–100 | Minimum rolling success ratio. |
| `https` | bool | Only proxies whose latest sampled HTTPS/CONNECT check passed within `classification_max_age`. |
| `exclude_tampered` | bool | Drop proxies whose latest echo check within `classification_max_age` found modified traffic. Unchecked proxies stay included. |
| `require_tamper_checked` | bool | Require an explicitly clean, timestamped echo tamper check within `classification_max_age`. Modified, unchecked, and expired results never match. Defaults to false. |
| `max_age` | duration or seconds | Only proxies whose last successful check is at most this old (for example `30m` or `1800`; maximum `720h`). |
| `limit` | 1–1000 | Page size. Omitted, zero, negative, or invalid values return 100. Larger values are clamped to the maximum page size, which is 1000 and may be lowered (see [Paging](#paging)). The response `limit` is the size that was used. |
| `offset` | 0–100000 | Skip this many matches. Use with `has_more` to page. |

Booleans accept `1`, `true`, `yes`, or `on`. A malformed `country`,
`exit_country`, `asn`, `anonymity`, `max_age`, or `offset` returns `400`.
Malformed `min_success_streak`, `min_reliability_7d_pct`, or
`min_reliability_7d_samples` values return `400`; the percentage must be from
0 to 100 and the integer thresholds from 0 to 1,000,000,000. A malformed or
unconfigured `target_profile` returns `400`. A non-numeric `max_latency_ms` or
`min_ratio_pct` is ignored. Pages are best-effort because the validated pool
changes between requests.

### Paging

The default page is 100 results. To read more, request the next page with
`offset` while `has_more` is `true`; advance `offset` by `count`, the number of
proxies the previous page returned. Each page counts against the rate limit, and
pages are best-effort: the validated pool changes between requests, so a proxy
can appear on two pages or be skipped, and clients should deduplicate by `url`.

**Page sizes above 100 are deprecated.** Requests for more than 100 results are
still honored today, up to 1000, but the maximum page size on the hosted service
will be lowered to 100 once clients stop relying on large pages. Do not assume a
`limit` larger than 100 is honored: read `limit` in the response and follow
`has_more`. The `proxy-router` in this repository already does.

### Response

```json
{
  "count": 1,
  "limit": 2,
  "offset": 0,
  "has_more": true,
  "proxies": [
    {
      "url": "http://203.0.113.10:8080",
      "scheme": "http",
      "country": "US",
      "exit_country": "US",
      "exit_ip": "198.51.100.4",
      "exit_asn": "AS64496",
      "exit_org": "Example Hosting",
      "hosting": true,
      "exit_checked_at_ms": 1790000000000,
      "exit_ip_changes": 0,
      "exit_ip_samples": 3,
      "stable_exit": true,
      "geo_mismatch": false,
      "asn": "AS64496",
      "anonymity": "elite",
      "latency_ms": 412,
      "jitter_ms": 35,
      "ok_ratio_pct": 90,
      "last_checked_at_ms": 1790000000000,
      "last_status": "ok",
      "last_ok_at_ms": 1790000000000,
      "success_streak": 5,
      "reliability_24h_samples": 12,
      "reliability_24h_pct": 92,
      "reliability_7d_samples": 24,
      "reliability_7d_pct": 92,
      "reliability_first_sample_at_ms": 1789500000000,
      "reliability_coverage_start_at_ms": 1789395200000,
      "anonymity_checked_at_ms": 1790000000000,
      "https_ok": true,
      "https_checked_at_ms": 1790000000000,
      "https_tunnel_scheme": "http",
      "tampered": false,
      "tamper_checked_at_ms": 1790000000000,
      "target_profiles": {
        "catalog": {
          "state": "pass",
          "checked_at_ms": 1790000000000,
          "http_status": 204,
          "latency_ms": 125
        }
      }
    }
  ]
}
```

(`203.0.113.0/24` is a documentation range; real responses list real endpoints.)

- `anonymity`, `exit_country`, `https_*`, and `tampered` are omitted or empty
  when never measured or older than `classification_max_age` (default 24h).
- `exit_ip`, `exit_asn`, `exit_org`, `hosting`, and `stable_exit` describe a
  fresh observed exit measurement and are omitted when it is missing or older
  than `classification_max_age`. `hosting: true` comes from recognized ASN
  organization terms; unknown is omitted and is not evidence of a residential
  exit. The first observed exit IP is not counted as a change. An absent later
  observation does not establish stability.
- Reliability fields count completed probe observations in sparse UTC-hour
  buckets. History begins with the first successful sample; later successes
  and failures count, while missing hours add no failures. The hour-aligned
  window edges can differ from an exact rolling 24-hour or seven-day boundary
  by less than one hour. `reliability_*_pct` and `success_streak` are omitted
  when legacy records have no corresponding measurement; percentage fields are
  also omitted when the sample count is zero. `reliability_coverage_start_at_ms`
  is the later of the first successful sample and the seven-day window start;
  it does not claim uninterrupted monitoring.
- `stable_exit` is true only after at least two measured IP observations in the
  last seven days with no observed IP change in that window. A lack of recent
  observations is unknown, not stable.
- `target_profiles` contains only active configured profile names whose URL
  and expected-body fingerprint still matches. Results are `pass`, `fail`, or
  `unknown`; stale and removed profiles are omitted. Profile checks are
  opt-in operator configuration, with no built-in third-party targets.
- `tampered: true` means the echo check found modified headers or a violated
  body contract. Separate tamper annotations do not affect listing; a combined
  echo body-contract failure counts as a failed reachability sample. Use
  `exclude_tampered` to remove fresh modified results from a query.
- `require_tamper_checked=true` selects only measured clean echoes. Combine it
  with `https=true` to also require a passing HTTPS/CONNECT check. It keeps the
  normal 30-second response cache and request coalescing, but does not serve
  expired cached responses during a failed or slow refresh. Freshness is
  evaluated when the response is generated; results remain a snapshot.
- Anonymity, exit country, and tamper results come from a plain-HTTP echo that
  the proxy itself can influence: treat them as hints, not proof.
- `url` is always a plain `scheme://host:port` with an ASCII hostname or IP
  literal. Quote it when passing it to a shell.

### Errors

| Status | Meaning |
| --- | --- |
| `400` | A filter or paging value is invalid. |
| `429` | Per-IP rate limit exceeded. Wait `Retry-After` seconds. |
| `503` | The query failed (it has up to about 9 seconds), or too many distinct uncached queries are running. Retry after `Retry-After` (5 seconds). Only a query that was not served recently can fail this way; see [Stale responses](#stale-responses). |

### Stale responses

A response is cached for 30 seconds. After that, the first request for the same
query is answered at once from the previous response while one background
refresh replaces it, and a refresh that fails does not turn the query into an
error. Such a response is up to five minutes past its normal 30 seconds. It is
sent with `Cache-Control: public, max-age=5, s-maxage=5` and
`Warning: 110 - "Response is Stale"`, so clients and CDNs ask again within
seconds. A query that has not been served in the last five minutes is loaded
before it is answered, and that load is the only case that returns `503`.
`/stats` behaves the same way.

## `GET /stats`

Aggregate counts, never proxy URLs: `total`, `stable`, `by_country`,
`stable_by_country`, `by_anonymity`, and `latency_bands` (`<200ms`,
`200-500ms`, `500-1000ms`, `>1000ms`, `unknown`), plus `checked_at`. Cached for
30 seconds, with the same [stale responses](#stale-responses) as `/proxies`.
`GET /stats?echo=1` returns the same body as `/get`.

## `GET /get`

httpbin-compatible echo of the caller-visible address and headers
(`args`, `headers`, `origin`, `url`). Used by validators for anonymity checks.
Nothing is stored.

## Health and dashboard

| Endpoint | Description |
| --- | --- |
| `/livez` | Liveness. |
| `/readyz` | Readiness; `503` when Redis is unavailable or the process is draining. |
| `/get?probe_content=1` | Stable `FreeProxyAPI probe OK` plaintext for explicitly configured target profiles; `GET`/`HEAD`, `text/plain; charset=utf-8`, `Cache-Control: no-store`. Other `/get` queries retain the normal caller-IP echo behavior. |
| `/` and `/dashboard` | Built-in homepage and operations dashboard. |
| `/dashboard/data?range=1h\|6h\|24h` | Dashboard JSON backed by Prometheus; cached 15 seconds per range. |

## Operator endpoints

`/metrics` (Prometheus), `/report` (monitor status and validation counts, never
proxy URLs) and the internal API `/internal/api/v1/proxies` (bearer token, off
unless `internal_api_token_file` is set) are for operators. Serve them on
`admin_listen_addr` and keep them off the public internet; see the
[README](../README.md#configuration) and [deployment guide](deployment.md).
