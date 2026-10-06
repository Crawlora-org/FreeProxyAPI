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
curl 'https://freeproxyapi.crawlora.net/proxies?exit_country=US&min_ratio_pct=80&limit=2'
```

### Parameters

| Parameter | Type | Notes |
| --- | --- | --- |
| `country` | two letters | Country of the proxy's own IP (entry country). Case-insensitive. |
| `exit_country` | two letters | Country of the exit IP observed by the echo check. Unknown or stale values never match. |
| `asn` | `AS15169` or `15169` | Autonomous system of the proxy's IP. |
| `anonymity` | `elite`, `anonymous`, `transparent` | Anonymity class from the echo check. Unknown or stale values never match. |
| `geo_mismatch` | bool | Only proxies whose exit country differs from their entry country. |
| `max_latency_ms` | integer | Maximum smoothed latency. Capped at 600000. |
| `min_ratio_pct` | integer 0–100 | Minimum rolling success ratio. |
| `https` | bool | Only proxies whose latest sampled HTTPS/CONNECT check passed within `classification_max_age`. |
| `exclude_tampered` | bool | Drop proxies whose latest echo check within `classification_max_age` found modified traffic. Unchecked proxies stay included. |
| `max_age` | duration or seconds | Only proxies whose last successful check is at most this old (for example `30m` or `1800`; maximum `720h`). |
| `limit` | 1–1000 | Page size. Omitted, zero, invalid, or larger values return up to 1000. |
| `offset` | 0–100000 | Skip this many matches. Use with `has_more` to page. |

Booleans accept `1`, `true`, `yes`, or `on`. A malformed `country`,
`exit_country`, `asn`, `anonymity`, `max_age`, or `offset` returns `400`.
A non-numeric `max_latency_ms` or `min_ratio_pct` is ignored. Pages are
best-effort because the validated pool changes between requests.

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
      "geo_mismatch": false,
      "asn": "AS64496",
      "anonymity": "elite",
      "latency_ms": 412,
      "jitter_ms": 35,
      "ok_ratio_pct": 90,
      "last_checked_at_ms": 1790000000000,
      "last_status": "ok",
      "last_ok_at_ms": 1790000000000,
      "anonymity_checked_at_ms": 1790000000000,
      "https_ok": true,
      "https_checked_at_ms": 1790000000000,
      "https_tunnel_scheme": "http",
      "tampered": false,
      "tamper_checked_at_ms": 1790000000000
    }
  ]
}
```

(`203.0.113.0/24` is a documentation range; real responses list real endpoints.)

- `anonymity`, `exit_country`, `https_*`, and `tampered` are omitted or empty
  when never measured or older than `classification_max_age` (default 24h).
- `tampered: true` means the proxy injected response headers or rewrote the echo
  body. Tampered proxies stay listed unless you pass `exclude_tampered`.
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
| `/` and `/dashboard` | Built-in homepage and operations dashboard. |
| `/dashboard/data?range=1h\|6h\|24h` | Dashboard JSON backed by Prometheus; cached 15 seconds per range. |

## Operator endpoints

`/metrics` (Prometheus), `/report` (monitor status and validation counts, never
proxy URLs) and the internal API `/internal/api/v1/proxies` (bearer token, off
unless `internal_api_token_file` is set) are for operators. Serve them on
`admin_listen_addr` and keep them off the public internet; see the
[README](../README.md#configuration) and [deployment guide](deployment.md).
