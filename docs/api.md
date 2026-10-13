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

## Operator source report

`GET /report/sources` returns bounded source quality and cost aggregates for the
current configured source catalog. It is an operator endpoint served on the
same `admin_listen_addr` listener as `/report` and `/metrics`; if
`admin_listen_addr` is empty, it shares the main listener for compatibility.
Keep that listener private or prevent public ingress from forwarding it. The
response contains 32-character lowercase hexadecimal source IDs and effective
parser schemes, not source URLs, feed contents, or proxy endpoints. Each ID is
derived from the exact trimmed feed URL and parser-scheme hint. It is `no-store`
and accepts `GET` and `HEAD`.

When source learning is disabled, the response status is `disabled`. Otherwise
it includes a bounded current-catalog snapshot. `quality_captured_at`,
`quality_age_seconds`, and `quality_stale` describe the local in-process cache
(up to one minute); `source_counts_at`, `source_counts_age_seconds`, and
`source_counts_observed` describe the latest validated-set scan/aggregate. The
quality and candidate-count reads occur separately, so the report is not an
atomic cross-section. Check the timestamps before comparing fields. If source
counts were not observed, the per-source `validated` and `routing_eligible`
fields and the unattributed totals are omitted. If a quality field has no
measurement yet or its quality record is corrupt, its `*_measured` flag is
false and its numeric fields are omitted; an omitted value is unknown, not zero.

The report distinguishes source history from current candidate attribution:
`admitted` counts actual new candidate-set insertions, not staged source rows.
Each candidate keeps one selected source origin. Duplicate feed appearances
do not create multiple source votes. `unattributed_validated` and
`unattributed_routing_eligible` count validated candidates whose source ID is
missing or no longer belongs to the configured catalog; they cannot be assigned
to a current source. The shared Redis aggregate uses `unattributed_routing` for
the routing-eligible counterpart; the report field uses the more explicit
`unattributed_routing_eligible` name.

The shared validated aggregate carries source-count maps between replicas, but
the public `/stats` schema deliberately omits source attribution. Operators see
that attribution through `/report/sources` only.

`routing_eligible` is the subset of validated candidates meeting the current
strict router evidence: success ratio at least 80%, a positive success streak,
a passing fresh HTTPS/CONNECT check, and a fresh clean tamper check, with age
bounded by `classification_max_age`. It is a point-in-time filter count, not a
claim that the proxy will remain usable.

Admission refresh fields (`admission_refreshes`, `last_admitted`,
`zero_admission_streak`, and `last_admission_at`) are updated only for a whole
successful refresh pass and sources whose pass was not truncated or affected by
the candidate cap. Failed, skipped, truncated, and capped sources do not add a
zero-admission observation. `admitted` itself counts atomic new set insertions
and may include accepted additions from a pass whose later step failed; it is
not the same as a completed admission observation.

Fetch cost fields sum HTTP client attempts, consumed HTTP response-body bytes,
and elapsed stream work. Attempts count calls to the HTTP client; bytes exclude
headers and TLS framing, and a conditional `304` has zero response-body bytes
even though the cached body is replayed to the parser. Elapsed time covers the
whole stream callback, including parsing and staging, rather than pure HTTP
latency. File feeds contribute zero HTTP attempts and network bytes.

Probe cost fields count committed probe outcomes only, after lease and probe
contract checks. Elapsed work includes the primary probe and any auxiliary
checks executed within that lease. It is not total CPU time or a complete
source-cost measure. Changing the base probe contract resets probe-cost
history for the new contract; legacy quality hashes without these fields remain
unmeasured until new observations arrive. A stored probe contract alone is not
enough: probe-cost fields are measured only when the current contract matches
and committed probe-completion counters exist. Source quality hashes expire
after 30 days, so cumulative counters restart after expiry.

Per-source Prometheus efficiency series are published only by the stats leader
when both `source_learning_enabled` and `stats_leader_enabled` are true. With
either feature disabled, these per-source series are absent. Only the unique
lease holder exports them; followers suppress their per-source series. The
JSON report remains usable independently of stats-leader publication when
source learning is enabled. The metric families include
`freeproxyapi_source_admitted_total`,
`freeproxyapi_source_admission_refreshes_total`,
`freeproxyapi_source_http_attempts_total`,
`freeproxyapi_source_response_bytes_total`,
`freeproxyapi_source_fetch_work_milliseconds_total`,
`freeproxyapi_source_probe_completions_total`,
`freeproxyapi_source_probe_successful_completions_total`,
`freeproxyapi_source_probe_work_milliseconds_total`,
`freeproxyapi_source_validated`,
`freeproxyapi_source_routing_eligible`, and
`freeproxyapi_source_zero_admission_streak`. The unattributed count and
snapshot families `freeproxyapi_source_unattributed_validated`,
`freeproxyapi_source_unattributed_routing_eligible`,
`freeproxyapi_source_efficiency_quality_snapshot_age_seconds`,
`freeproxyapi_source_efficiency_counts_snapshot_age_seconds`, and
`freeproxyapi_source_efficiency_counts_observed` have no per-source label.
Per-source series use the bounded `source_id` hash label, never a URL or
endpoint.

## Feed-audit CLI

The read-only `feed-audit` CLI rechecks feed retrieval and parsing without
probing listed endpoints or connecting to Redis. It reads an input JSON array
from a file (default `/dev/stdin`):

```json
[{"url":"https://feeds.example.net/proxies.txt","scheme":"http"}]
```

The URL must be HTTP(S), without user information or a fragment, and no longer
than 8,192 characters. The complete batch is validated before requests start;
private IP literals and `localhost` are rejected up front, and resolved
hostnames are checked against the source fetcher's public-address dial policy.
`scheme` is the default parser scheme for bare records and may be `http`,
`socks4`, or `socks5`; it defaults to `http`. There is no automatic scheme
inference. The fetcher rejects redirects. The command writes no files and does
not expose source URLs, response bodies, proxy endpoints, or raw fetch errors.

The JSON input is limited to 1 MiB and 128 source records.
Defaults are a 20-second per-source timeout, two workers, 32 MiB maximum
response body per source, and 100,000 accepted records per source. Concurrency
is restricted to 1–4, timeout to at most 90 seconds, and the byte and record
caps cannot exceed those defaults. Reaching the record cap marks a result
truncated and ineligible; an empty parsed set is also ineligible, and either
result omits the canonical-set hash. Results
are JSON Lines with source ID, input index, parser scheme, eligibility,
truncation, parsed-unique and rejected counts, counts by endpoint scheme,
fetch-cost fields, optional canonical-set hash, and a bounded error code or
HTTP status; HTTP status errors also include the numeric `http_status` field.
Timeouts, cancellation, byte limits, record limits, and other fetch/parse
errors have bounded codes. Eligibility requires a completed fetch and parse, a
nonempty canonical set, and no record cap.

The CLI's `http_attempts`, `network_bytes`, and `duration_ms` use the same
measurement definitions as the source report: response-body reader bytes only,
cached replay excluded, and duration includes parsing work.

## Operator endpoints

`/metrics` (Prometheus), `/report` (monitor status and validation counts),
`/report/sources` (source IDs and aggregate measurements), and the internal API
`/internal/api/v1/proxies` (bearer token, off unless
`internal_api_token_file` is set) are for operators. Serve them on
`admin_listen_addr` and keep them off the public internet; see the
[README](../README.md#configuration) and [deployment guide](deployment.md).
