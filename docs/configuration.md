# Configuration reference

`freeproxyapi monitor -config <file>` reads one JSON file. Every key is
optional except `sources` (at least one entry is required), and
`probe_target` when `network_validation_enabled` is `true`. Unknown keys are
ignored, so check the spelling against this page.

- **Durations** are Go durations such as `30s`, `5m`, or `24h` and must be
  positive.
- **Default** is the value used when the key is absent or `0`/empty. A range
  is checked at startup, and an out-of-range value stops the monitor with an
  error that names the key.
- A few keys can also be set by environment variable; see
  [Environment overrides](#environment-overrides). A non-empty variable wins
  over the file.

Working examples: [`examples/monitor.json`](../examples/monitor.json) (Compose,
validation off) and [`examples/monitor-gost.json`](../examples/monitor-gost.json)
(validation and GeoIP on). Replace its `probe_target` with an endpoint you
operate and have approval to use. Read the
[responsible-use guide](responsible-use.md) before enabling validation.

## Core

| Key | Default | Description |
| --- | --- | --- |
| `redis_url` | `redis://127.0.0.1:6379/0` | Redis connection. Prefer `FREEPROXYAPI_REDIS_URL` so a password stays out of the file and any ConfigMap. |
| `namespace` | `freeproxyapi:v1` | Prefix for every Redis key. See [redis-schema.md](redis-schema.md). |
| `sources` | none (required) | Proxy feeds, such as `file:///data/proxies.txt` or an `https://` URL. An optional `scheme@` prefix sets the default scheme for bare `host:port` lines in that feed, for example `socks5@https://example.com/socks5.txt`. |
| `default_proxy_scheme` | `http` | Scheme for bare `host:port` records whose feed has no `scheme@` prefix. |
| `listen_addr` | `:8080` | Public HTTP listener. |
| `admin_listen_addr` | empty | Optional second listener, for example `127.0.0.1:9090`, for `/metrics`, `/report` and `/internal/api/v1/proxies`. When set, those endpoints are absent from `listen_addr`. Must differ from `listen_addr`. |
| `public_base_url` | empty | Public origin of your deployment, such as `https://proxies.example.com`. Used for canonical, Open Graph and code-sample URLs on the built-in pages. When empty, samples show the origin the visitor reached and canonical tags are omitted. |
| `trusted_proxy_cidrs` | none | CIDRs of ingress that strips caller-supplied forwarding headers and writes trusted ones. Leave empty to identify clients by socket peer. |
| `internal_api_token_file` | empty | Bearer-token file for the internal API. The API is disabled when this is empty. See [deployment.md](deployment.md#internal-query-api). |
| `prometheus_url` | empty | Prometheus base URL (`http(s)`, no credentials, query or fragment). The dashboard needs it. |

`/metrics` exposes feed hostnames and probe settings. When `admin_listen_addr`
is empty (the compatibility default), `/metrics`, `/report` and the internal API
share the public listener, so your ingress must not forward them. The Compose
example binds the admin listener to the container's loopback; bind
`0.0.0.0:9090` and publish the port only on a network you control to scrape it.

## Feed intake

| Key | Default | Range | Description |
| --- | --- | --- | --- |
| `fetch_interval` | `15m` | | How often feeds are refreshed. |
| `source_request_timeout` | `request_timeout` | | Timeout for fetching one feed. |
| `source_max_bytes` | 1 MiB | up to 32 MiB | Largest feed body that is read. |
| `max_records_per_source` | `250000` | 1–5,000,000 | Most endpoints one feed can add per refresh; the rest are ignored. |
| `max_candidates` | `0` (off) | ≥ 0 | When positive, stops new candidates being added once the set reaches this size. |

See [Securing Redis and bounding feed intake](deployment.md#securing-redis-and-bounding-feed-intake).

## Validation

Validation is **off by default**. With `network_validation_enabled` unset or
`false` the monitor fetches feeds into Redis but starts no probe workers, so
nothing is tested and `/proxies` stays empty.

| Key | Default | Range | Description |
| --- | --- | --- | --- |
| `network_validation_enabled` | `false` | | Turns on probe workers. Requires `probe_target`. |
| `probe_target` | none | | URL fetched through each proxy. Use an endpoint you operate and may use. |
| `probe_target_mode` | `standard` | `standard`, `echo` | `echo` also classifies anonymity and exit country from the target's reply. See [deployment.md](deployment.md#anonymity-classification). |
| `probe_expected_body` | empty | | Exact response body (trailing whitespace ignored) a standard-mode probe must return to pass. Empty accepts any 2xx. Ignored in echo mode. |
| `anonymity_check_url` | empty | | Echo endpoint, or a comma-separated list tried in order for failover, used to classify anonymity. |
| `workers` | `2` | 1–768 | Concurrent probe workers per replica. |
| `global_requests_per_minute` | `30` | 1–120,000 | Request budget shared by all replicas. |
| `probe_permit_chunk` | `0` | 0–120,000 | Budget permits a replica reserves per refill. `0` selects the larger of 10 and `workers`/4. |
| `request_timeout` | `5s` | | Timeout for one probe request. |
| `probe_connect_timeout` | `3s` (at most `request_timeout`) | | Connect timeout. Must not exceed `request_timeout`. |
| `lease_ttl` | `30s` | | How long a replica holds a claimed candidate. |

## Listing and retest schedule

A proxy is listed only after `min_samples_for_listing` successful probes. The
rest of this table controls how soon candidates are probed again.

| Key | Default | Range | Description |
| --- | --- | --- | --- |
| `min_samples_for_listing` | `3` | 1–10 | Successful probes needed before a proxy is listed. |
| `sample_retest_interval` | `60s` | | Gap between probes of a candidate that is not yet listable. |
| `validated_retest_interval` | `6h` | | Re-probe interval for a healthy listed proxy. |
| `listed_failure_retest_interval` | `5m` | | Re-probe interval after a listed proxy fails, so dead proxies leave `/proxies` quickly. |
| `failed_retest_interval` | `48h` | | Re-probe interval for a failing candidate that is not listed. |
| `retest_jitter_pct` | `10` | 0–50 | Spreads validated and failed intervals by up to ±N% so candidates do not come due together. |
| `max_consecutive_failures` | `10` | 0–100 | Failures before a candidate is evicted. `0` selects the default. |
| `discard_failed_candidates` | `false` | | Discard a failing candidate instead of rescheduling it. |
| `evicted_backoff_base` | `2h` | | Block on re-adding an evicted or discarded endpoint, doubling with each repeat. |
| `evicted_backoff_max` | `168h` | ≥ `evicted_backoff_base` | Cap on that block. |
| `requeue_pending_on_start` | `false` | | One-time sweep at startup that makes pending candidates due again. |

## Accuracy checks

| Key | Default | Range | Description |
| --- | --- | --- | --- |
| `https_probe_target` | empty (off) | `https://` URL, no credentials | After roughly one in N successful probes, and only when the global budget grants one more permit, fetches this URL through the proxy's CONNECT (or SOCKS) tunnel with TLS verification and records `https_ok`. An `https://` proxy whose own TLS handshake fails is retried as a plain CONNECT proxy. |
| `https_probe_expected_body` | empty | | Exact body the HTTPS probe must return. |
| `https_probe_every` | `4` | 1–1000 | The N above. |
| `control_probe_enabled` | `true` | | Each replica fetches `probe_target` directly to detect that the probe origin itself is failing. |
| `control_probe_interval` | `60s` | at least `1s` | How often the control fetch runs. |
| `control_probe_failure_threshold` | `3` | 1–100 | After this many consecutive control failures a replica's workers stop claiming work, and failed outcomes are released uncommitted, until one control check succeeds. |
| `classification_max_age` | `24h` | | Age after which anonymity, exit and HTTPS measurements are treated as unknown in `/proxies`. `/stats` and metrics still count stored values regardless of age. |
| `geoip_db_path` | empty | | Optional MaxMind country database. |
| `geoip_asn_db_path` | empty | | Optional MaxMind ASN database. |

## Redis connections

| Key | Default | Range | Description |
| --- | --- | --- | --- |
| `redis_pool_size` | `0` (auto) | 0–256 | Worker connections per replica. Auto is one per 32 workers, clamped to 10–32. Workers and background jobs share this pool. |
| `redis_api_pool_size` | `8` | 0–64 | Separate pool for HTTP reads (`/proxies`, the internal API, `/stats`, `/report`, `/dashboard/data`, `/readyz`), so busy workers cannot make them wait. A read gives up after 2s without a free connection. `0` selects the default. |
| `stats_leader_enabled` | `true` | | One replica holding a Redis lease scans the validated set and shares the aggregate; others read it instead of scanning. |
| `stats_leader_ttl` | `90s` | above 30s | Lease length for that replica. |
| `stats_aggregate_ttl` | `5m` | at least 60s | How long the shared aggregate is kept. |

## Built-in pages

| Key | Default | Description |
| --- | --- | --- |
| `analytics_measurement_id` | empty | Google Analytics 4 ID (`G-XXXXXXXXXX`). Empty means the pages load no third-party scripts and send nothing to Google. When set, the pages show a consent banner and load Google Analytics only after the visitor accepts. A Global Privacy Control or Do Not Track signal counts as declining, and an "Analytics settings" link lets visitors change their mind. You are still responsible for any other notice or choice the law where your visitors live requires. |
| `privacy_url` | empty | `http(s)` URL of your privacy notice, linked from the consent banner. |
| `cloudflare_web_analytics` | `false` | Set to `true` only if Cloudflare Web Analytics auto-install is on for your hostname: it lets the injected beacon script through the pages' Content-Security-Policy. |

Privacy details are in [privacy.md](privacy.md).

## Environment overrides

These variables, when non-empty, override the matching file key. Integer
variables must parse as integers or the monitor refuses to start.

| Variable | Overrides |
| --- | --- |
| `FREEPROXYAPI_REDIS_URL` | `redis_url` |
| `FREEPROXYAPI_PROBE_TARGET` | `probe_target` |
| `FREEPROXYAPI_PROBE_TARGET_MODE` | `probe_target_mode` |
| `FREEPROXYAPI_PROBE_EXPECTED_BODY` | `probe_expected_body` |
| `FREEPROXYAPI_WORKERS` | `workers` |
| `FREEPROXYAPI_GLOBAL_REQUESTS_PER_MINUTE` | `global_requests_per_minute` |
| `FREEPROXYAPI_ADMIN_LISTEN_ADDR` | `admin_listen_addr` |
| `FREEPROXYAPI_PUBLIC_BASE_URL` | `public_base_url` |
| `FREEPROXYAPI_PRIVACY_URL` | `privacy_url` |
| `FREEPROXYAPI_ANALYTICS_MEASUREMENT_ID` | `analytics_measurement_id` |

The deployment flow, including which of these the Kubernetes manifests set, is
in [deployment.md](deployment.md).
