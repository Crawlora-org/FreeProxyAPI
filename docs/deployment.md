# Deployment

## Single machine

Review `examples/monitor.json` and the mounted `examples/proxies.txt` first.
The checked-in configuration keeps network validation disabled.

```sh
docker compose up --build
```

To enable validation, change `network_validation_enabled` to `true`, provide a
probe target you control, and keep the global request budget conservative.
Before enabling large-scale validation in a cloud environment, consult your
provider's acceptable-use, abuse, and DDoS policies and obtain any required
approval. High-volume validation traffic from one cloud deployment can be
mistaken for scanning or a denial-of-service event.

## Kubernetes

`k8s/base` deploys three monitor replicas sharing one Redis instance. Their
hostname topology-spread rule prefers distinct nodes while keeping replicas
schedulable on smaller clusters. The private production overlay makes this
separation strict, so it requires enough schedulable nodes. Redis uses a
five-GiB PersistentVolumeClaim;
choose a suitable storage class for your cluster before applying it. Each
replica serves `/`, `/livez`, `/readyz`,
`/report`, `/stats`, `/proxies`, and `/metrics` on port 8080 (see `listen_addr`
in the monitor config); the Deployment's probes use the health endpoints and
the Service exposes them for scraping.

```sh
kubectl create namespace freeproxyapi --dry-run=client -o yaml | kubectl apply -f -
kubectl apply -k k8s/base
```

The public manifests intentionally contain no proxy feeds and default to
inventory-only operation. Images are published by GitHub Actions to
`ghcr.io/crawlora-org/freeproxyapi`; use a specific immutable tag for a real
deployment.

### Automatic live deployment from GitHub Actions

The `Publish container` workflow deploys in batches rather than on every merge.
A scheduled run every 30 minutes checks whether `main` has moved past the
commit of the latest successful `production` deployment; if it has, the run
builds, publishes a UTC release tag, and deploys the exact image digest
produced by that build, otherwise it stops after a few seconds. To deploy
immediately, run `gh workflow run "Publish container" --ref main -f force=true`
(or Actions -> Publish container -> Run workflow). Version tags still build
and publish an image but never deploy. The job renders
`k8s/overlays/live-ci`, applies the namespaced live resources, waits for Redis,
`freeproxyapi`, and `gost-router` to roll out, verifies both application image
references, and checks `/readyz`. Deployments are serialized and stale runs
are skipped so an older build cannot roll back a newer `main` deployment.

Before enabling the job, bootstrap the `freeproxyapi` namespace and create the
CI deployer identity below. Because workload-write permissions can indirectly
reference existing Secrets, use a dedicated namespace and an admission policy
that prevents unauthorized secret mounts, privileged settings, and host access.
The CI overlay deliberately omits the Namespace object so the CI credential can
remain namespace-scoped.

#### Creating the CI deployer identity

A cluster admin does this once, from a workstation. CI never creates or changes
it, and no agent or workflow should handle the admin credential.

**Quick path.** [`scripts/ci-kubeconfig.sh`](../scripts/ci-kubeconfig.sh)
performs the steps below with an admin kubeconfig for your cluster. It shows the
target cluster and asks you to confirm it, applies the RBAC, builds the
kubeconfig, verifies that it can deploy but cannot read Secrets or change the
cluster, and then stores the result without writing it to disk:

```sh
./scripts/ci-kubeconfig.sh --admin-kubeconfig PATH --set-secret --repo OWNER/REPO
```

Get a current admin kubeconfig from your provider first, for example
`aws eks update-kubeconfig`, `gcloud container clusters get-credentials`,
`az aks get-credentials`, or your provider's console. Use `--print | pbcopy`
instead of `--set-secret` to get the base64 value for another secret store. The
script works with any Kubernetes cluster, is idempotent, and is safe to
re-run. The manual steps follow for reference.

1. Use an admin kubeconfig for the target cluster (admin credentials are often
   short-lived and must not be stored in GitHub). Create the namespace if it does not exist,
   then apply [`k8s/ci-deployer/rbac.yaml`](../k8s/ci-deployer/rbac.yaml), which
   defines a `ci-deployer` ServiceAccount, a namespaced Role, its RoleBinding,
   and a non-expiring token Secret:

   ```sh
   kubectl create namespace freeproxyapi   # skip if bootstrap.sh already did
   kubectl apply -f k8s/ci-deployer/rbac.yaml
   ```

   The Role allows `get`/`create`/`patch` on `deployments.apps`,
   `statefulsets.apps`, `services`, `configmaps`, `networkpolicies` and
   `poddisruptionbudgets`, plus `list`/`watch` on workloads for
   `rollout status`, `get`/`list`/`delete` on `pods`, and `create` on
   `pods/exec`. It cannot read Secrets or touch anything cluster-wide. Remove
   `delete` on pods if you do not want the stale-router-pod fallback.

2. Build a kubeconfig that uses the ServiceAccount token:

   ```sh
   NS=freeproxyapi
   SERVER=$(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}')
   CA=$(kubectl -n $NS get secret ci-deployer-token -o jsonpath='{.data.ca\.crt}')
   TOKEN=$(kubectl -n $NS get secret ci-deployer-token -o jsonpath='{.data.token}' | base64 --decode)
   cat > ci-kubeconfig <<EOF
   apiVersion: v1
   kind: Config
   clusters: [{name: c, cluster: {server: $SERVER, certificate-authority-data: $CA}}]
   users: [{name: ci-deployer, user: {token: $TOKEN}}]
   contexts: [{name: ci, context: {cluster: c, user: ci-deployer, namespace: $NS}}]
   current-context: ci
   EOF
   ```

3. Check the scope before storing it:

   ```sh
   KUBECONFIG=./ci-kubeconfig kubectl auth can-i patch deployments -n freeproxyapi   # yes
   KUBECONFIG=./ci-kubeconfig kubectl auth can-i get secrets -n freeproxyapi         # no
   KUBECONFIG=./ci-kubeconfig kubectl auth can-i list nodes                           # no
   ```

4. Store it as the `KUBE_CONFIG_B64` secret on the GitHub `production`
   environment, then delete the local file:

   ```sh
   base64 < ci-kubeconfig | tr -d '\n' | gh secret set KUBE_CONFIG_B64 --env production
   rm ci-kubeconfig
   ```

To rotate the credential, delete the `ci-deployer-token` Secret, re-apply
`k8s/ci-deployer/rbac.yaml`, and repeat steps 2 to 4. If the cluster is rebuilt,
repeat the whole procedure: the secret holds the cluster endpoint and CA.

Keep the `production` environment protected with the repository's normal
approval policy if deployments should require review. Never commit the
kubeconfig, client key, bearer token, registry credentials, or the private
`monitor.json` sources. The workflow fails before applying anything when the
secret is missing and verifies the rollout is using the exact image digest from
the publish job.

If the job cannot run at all (for example GitHub Actions is unavailable), the
maintainer can deploy `origin/main` with the break-glass procedure in
[manual-deploy.md](manual-deploy.md), which repeats the same build, digest
pinning, validation, rollout, and verification steps from a workstation.

## Health, metrics, and shutdown behavior

- `/livez` always answers 200 once the process is serving.
- `/readyz` pings Redis with a short timeout and answers 503 while Redis is
  unreachable, so orchestrators stop routing to a replica that cannot claim
  work.
- `/report`, `/stats`, and `/metrics` expose aggregate counts only — never endpoint URLs or
  source contents. `/stats` is safe to expose publicly and returns the validated
  total, stable total, counts by endpoint country, and stable counts by observed
  exit country. `/proxies` is the separate,
  public query surface; it is the only unauthenticated route that returns
  credential-free endpoint URLs. The metric reference lives in the README; the
  key layout behind those counts is documented in [redis-schema.md](redis-schema.md).
- On `SIGTERM`/`SIGINT` a replica stops claiming new candidates, lets in-flight
  probes finish or abort them at their own timeout, releases any lease it still
  owns back to the pending queue for other replicas, and then exits.
- When all pending candidates are scheduled in the future, validation workers
  sleep instead of polling Redis. The scheduler wakes them for due work and
  exposes `freeproxyapi_pending_due`,
  `freeproxyapi_pending_oldest_due_age_seconds`, and
  `freeproxyapi_pending_next_due_in_seconds` for capacity diagnosis.
- `freeproxyapi_source_refreshes_total{result}` is `ok`, `error`, `skipped`
  (lock held by another replica or a refresh already running here), or
  `canceled` (the refresh was interrupted by shutdown/rollout; it releases the
  cluster-wide lock immediately and does not mark `/report` as failed). A Redis
  error acquiring the lock is counted as `error` and retried with jittered
  backoff (about 15s, 30s, 60s, 120s, 240s, capped at 5m) until it succeeds,
  another replica holds the lock, or the replica shuts down.
- Each replica opens two Redis connection pools: `workers` (claims,
  completions, budget permits, source refreshes, background stats; sized by
  `redis_pool_size`) and `api` (HTTP reads; sized by `redis_api_pool_size`,
  2s pool wait). `freeproxyapi_redis_pool_*{pool}` exposes each pool's size,
  open/idle connections, pending requests, and cumulative hits, misses, waits,
  wait seconds, timeouts, and stale connections. A rising
  `rate(freeproxyapi_redis_pool_wait_seconds_total{pool="workers"}[5m])` with
  `freeproxyapi_redis_pool_connections` at `freeproxyapi_redis_pool_size`
  means workers are queueing for Redis; any
  `freeproxyapi_redis_pool_timeouts_total{pool="api"}` increase means HTTP
  reads could not get a connection.
- `freeproxyapi_source_fetch_failures_by_source_total{host,reason}` counts
  failed feed fetches by feed hostname (never path or query; `file` for local
  files; at most 512 hosts, the rest under `other`) and a fixed `reason`:
  `timeout`, `http_status`, `too_large`, `parse`, `dns`, `refused`, `tls`,
  `canceled`, or `other`.
- `freeproxyapi_source_fetch_failures_by_reason_total{reason}` provides the
  bounded, host-independent failure counter used for aggregate alerts and
  dashboard rates. It avoids treating a scrape of one replica as the complete
  source-failure total; sum it across replicas in Prometheus.
- `source_request_timeout` bounds source-feed HTTP fetches independently from
  `request_timeout`, which continues to bound proxy validation probes. If the
  source-specific setting is omitted, it inherits `request_timeout`.
- Each feed gets one bounded retry for transient timeouts/network errors;
  HTTP 429 retries honor `Retry-After` when it is present, while 404, parse,
  size, and other permanent errors are not retried. The aggregate counters
  `freeproxyapi_source_fetch_retries_total` and
  `freeproxyapi_source_fetch_recovered_total` distinguish retries from feeds
  that ultimately failed.

## Private-cluster operation (operator flow)

The production monitor config lives in Git: `k8s/overlays/live-local/`
(tracked) layers `monitor.json`, including the reviewed `sources` list, over
`../private-cluster` with a `configMapGenerator` merge, and `k8s/overlays/live-ci`
renders it for the `deploy-live` job. Change sources, knobs, or
`trusted_proxy_cidrs` by editing `k8s/overlays/live-local/monitor.json` in a
pull request; merging deploys it.

To run a separate cluster that CI does not manage against its own source list:

1. Deploy the image with `kubectl apply -k k8s/overlays/private-cluster`
   (update the immutable image tag in that overlay when selecting a release).
2. Create your own overlay (for example a copy of `k8s/overlays/live-local/`
   outside this repository, or on a private branch) whose
   `configMapGenerator` merge supplies that cluster's `monitor.json`.
3. Apply that overlay with `kubectl apply -k`. Because the generated
   ConfigMap uses a stable name, restart the Deployment after changing its
   content.

These manual steps are for a cluster that CI does not manage. The production
`freeproxyapi` namespace is deployed only by the `deploy-live` job (the
30-minute scheduled run after `main` changes, or a manual run); change
`k8s/overlays/live-local` through a pull request and
never apply it by hand there. A manual apply ships the local checkout as-is,
including uncommitted edits, and overwrites the last CI rollout (the job's
"Verify deployed images" step then fails). When the job cannot run, use
[manual-deploy.md](manual-deploy.md) instead of `kubectl apply`.

The JSON values for probe concurrency and the shared request budget can be
overridden at process start without editing the mounted file. The probe target
can be overridden the same way:

```sh
FREEPROXYAPI_WORKERS=12 \
FREEPROXYAPI_GLOBAL_REQUESTS_PER_MINUTE=600 \
FREEPROXYAPI_PROBE_TARGET='https://probe.example/healthz' \
FREEPROXYAPI_PROBE_TARGET_MODE=standard \
  ./freeproxyapi monitor -config /config/monitor.json
```

Environment values take precedence over JSON. `probe_target` must be set when
network validation is enabled. Use a probe endpoint you operate and have
approved for the resulting traffic; large-scale validation can trigger cloud
provider abuse controls.

For a bounded initial sweep, the private burst profile uses a health-gated
rolling rollout with at most one unavailable replica and no surge, then 33
monitor replicas, 384 workers per replica, and a Redis-wide budget of 60,000
probes per minute (about 3.6 million per hour). This provides 12,672 worker
slots while the Redis-wide limiter remains the hard request-rate ceiling. It
enables `discard_failed_candidates`, which removes failed candidates after the
verified
completion lease rather than retaining their failure history. On startup, the
profile also performs a Redis-guarded, one-shot requeue that moves all pending
candidates due immediately; this is what turns a backlog of future-scheduled
retests into an explicit sweep. Keep this mode off for steady-state retesting,
and promote it only after the probe target, Redis free space, AOF rewrite
status, and node CPU/network headroom pass the canary gates. The profile is
configuration only; applying it is a separate operator action.

Steady-state throughput is bounded by how much work is due, not by worker
slots: `pending` counts every candidate waiting for its next scheduled check,
and most of it is scheduled in the future. The live profile therefore retests
failed candidates every `failed_retest_interval` of 1h with
`retest_jitter_pct` 50, which spreads retests evenly instead of in waves that
drain at the budget ceiling and then leave workers idle. It runs 32 replicas
with 768 workers each (24,576 slots) under a Redis-wide budget of 90,000
probes per minute. The safety limits are 768 workers per replica (already at
the cap) and 120,000 probes per minute; Redis is single-threaded and the three production nodes
have 4.5 allocatable cores, so raise the budget only while
`freeproxyapi-redis` CPU stays below one core and monitor CPU is not throttled.

### Redis CPU headroom

Redis is one thread, and everything waits on it: the workers' claim and
complete scripts, and the `/proxies` and `/stats` reads. When that thread is
saturated, reads queue behind the workers' scripts, so it shows up as slow
public reads and `503` responses rather than as slow workers. Signs of it:

- `redis-cli --latency`, run inside the Redis pod, reports more than a
  millisecond or two. A healthy Redis answers in well under 1 ms.
- `SLOWLOG GET` is full of commands that normally take microseconds, such as
  `ZRANGEBYSCORE ... LIMIT 0 1` or `EVALSHA`. The process was descheduled or
  starved while running them.
- `INFO commandstats` shows `EVALSHA` (the worker claim and complete scripts)
  taking most of the time and `HMGET` (the `/proxies` read) a small share.
- `kubectl top pod` shows Redis near one core. It cannot use more than one.
- `INFO cpu` shows `used_cpu_sys` well above `used_cpu_user`. That points at the
  platform (the VM's clocksource or virtualization) rather than at Redis.

Two things set how much Redis can do. One is its cost per command, which is a
property of the platform (CPU generation, clocksource), not a setting here. The
other is how many commands it is asked to run, and that is the lever this
repository controls: see "How probe workers get work" below, `workers`, and the
replica count.

The Redis CPU request (300m) is its weight when its node is full, so it matters
when the node is the bottleneck. It does not help once Redis is using a whole
core, and it cannot be raised freely: a request larger than the free CPU on
every node leaves the Redis pod `Pending` after the StatefulSet rolls, which is
an outage. Check what is free per node first:

```sh
kubectl describe nodes | grep -A6 'Allocated resources'
```

Raise it only after freeing requests elsewhere: fewer monitor replicas or
`workers`, or another node. Changing the request restarts Redis when deployed.
It reloads its append-only file, which can take tens of seconds for millions of
keys, and the startup probe allows ten minutes.

### How probe workers get work

Each replica runs one claimer and `workers` probe workers. Only the claimer asks
Redis for work. An idle worker tells the claimer it is waiting; the claimer
takes that many probe permits, leases up to that many due candidates in one
Redis call (at most 64, one script run of about a millisecond), and hands each
claim straight to a waiting worker. It never claims ahead of the workers, so a
lease does not age in a queue, and it releases any claim no worker took when the
replica shuts down. The control-probe pause, the global request budget and the
lease rules are unchanged.

Before this, every idle worker polled Redis on its own. When the scheduler
signalled that work was due, all of a replica's idle workers (768 on the live
profile) woke at once and each ran a claim script for the few candidates that
were actually due. With hundreds of workers per replica that became most of
Redis's work, on a single thread. Against a real Redis 7.4 with 768 workers and 6,000 candidates that
became due over 15 seconds, the same work took the same 14.6 s with 82% fewer
`EVALSHA` calls (35,109 to 6,215), 99% fewer `ZRANGEBYSCORE` calls (29,040 to
181), and 36% fewer commands overall (161,271 to 103,466).

To see it in production, `freeproxyapi_claim_batches_total` counts claim calls
that leased something and `freeproxyapi_claim_empty_total` those that found
nothing due. Their sum is how often a replica asked Redis for work; it should
be a small fraction of `freeproxyapi_claims_total`.

Redis connections are sized explicitly rather than from go-redis's
`10 * GOMAXPROCS` default. The monitor's 500m CPU limit makes Go's
container-aware GOMAXPROCS 2, so that default gave each replica a single
20-connection pool shared by hundreds of workers and every HTTP read; worker
traffic kept it full and internal API queries hit their 10s deadline waiting
for a connection. The live profile keeps the worker pool at that proven 20
(`redis_pool_size`), because Redis is single-threaded and more connections
add event-loop latency, not throughput. It adds an 8-connection `api` pool
(`redis_api_pool_size`) for HTTP reads. With 32 replicas that is at most
32 × (20 + 8) = 896 monitor connections against Redis `maxclients` 10000.
Keep `replicas × (redis_pool_size + redis_api_pool_size)` well below
`maxclients` when scaling.

The service exposes an httpbin-compatible `/get` endpoint that returns the
caller-visible address. Set
`probe_target_mode` to `echo` and leave `anonymity_check_url` empty. Each
successful validation then uses the single probe response for both reachability
and anonymity classification:

```json
{
  "network_validation_enabled": true,
  "probe_target": "https://freeproxyapi.crawlora.net/get",
  "probe_target_mode": "echo",
  "anonymity_check_url": ""
}
```

The default `probe_target_mode` is `standard`; it validates reachability from
the proxy by accepting a successful HTTP response. Standard-mode probes append
a random `_fpa` query parameter and send `Cache-Control: no-cache` and
`Pragma: no-cache` so intermediary caches cannot answer for a dead upstream.
Set `probe_expected_body` (or `FREEPROXYAPI_PROBE_EXPECTED_BODY`) to require an
exact body, compared after trimming trailing whitespace; the live profile uses
`"success"` for `http://detectportal.firefox.com/success.txt`, which rejects
captive portals and injected pages that still answer 2xx. The setting is
ignored in echo mode. Listing accuracy is tuned with `min_samples_for_listing`
(default 3; a proxy needs that many recorded probes and an 80% rolling success
ratio before it appears in `/proxies`), `sample_retest_interval` (default 60s
between those early probes), and `listed_failure_retest_interval` (default 5m
before a listed proxy that just failed is re-probed). Meanwhile,
`anonymity_check_url` remains a separate optional request. The live profile
keeps the broad Firefox reachability probe in standard mode and points
`anonymity_check_url` at the operator-owned `/get` endpoint, so a proxy whose
network blocks our hostname stays listed with an unknown anonymity class instead
of failing validation. Do not use a shared public echo service for high-volume
validation unless its operator has approved the traffic.

## Lowering the public page size

`/proxies` serves pages of up to 1000 results, but its default is 100 and pages
above 100 are deprecated: reading a large list with one request makes the
server scan the whole validated set, while pages of 100 stop early. The maximum
is `public_max_limit` (default 1000), so lowering it is a configuration change,
not a code change. Do it only when nothing depends on large pages, or the
clients that do will silently receive 100 results instead of up to 1000.

1. Ship a `proxy-router` image that follows `has_more` (it asks for the whole
   limit first and then continues with `offset`) and move your own sidecars,
   overlays, and the `docker-compose.gost-public.yml` pin to it. Sidecars that
   others run keep their older image until they upgrade.
2. Watch large-page traffic: `sum(rate(freeproxyapi_public_large_page_requests_total[1d]))`.
   It counts public requests with a `limit` above 100. The built-in pages use at
   most 50 rows, so this is external clients, including older routers.
3. When it has stayed near zero for as long as you are willing to wait, set
   `"public_max_limit": 100` in `k8s/overlays/live-local/monitor.json` through a
   pull request. Responses then report `limit: 100`, and clients that follow
   `has_more` keep working. A client that reads only the first page gets 100.

Each page is a separate request against the 60 requests per minute per client
IP limit, so a router reading 1000 proxies in pages of 100 uses 10 of them per
refresh.

## Anonymity classification

When `anonymity_check_url` names an echo endpoint (any service that reflects
the caller's IP and headers — httpbin-style JSON works), every successful main
probe is followed by one extra request through that proxy to classify it as
`transparent`, `anonymous`, or `elite`. A comma-separated list provides
automatic failover: each check tries the endpoints in order until one answers,
and the self-check that learns the monitor's own egress IP does the same.

Every listed echo URL consumes a global-budget permit.

Use a plain `http://` echo URL. An `https://` URL makes HTTP proxies open a
CONNECT tunnel, inside which they cannot add `Via` or `X-Forwarded-For`, so
every header leak is invisible and those proxies are misclassified `elite`. It
also restricts classification to proxies that allow CONNECT to port 443; HTTPS
capability is measured separately by `https_probe_target`. The live profile
uses `http://freeproxyapi.crawlora.net/get`, which Cloudflare serves without an
HTTPS redirect — keep `/get` excluded if "Always Use HTTPS" is ever enabled.

Behind Cloudflare, `/get` never echoes `CF-Connecting-IP` or the hops the
ingress appends to `X-Forwarded-For`: when the socket peer is in
`trusted_proxy_cidrs`, trailing trusted entries and the resolved client entry
are removed and only the `X-Forwarded-For` entries the proxy itself sent
(capped at 256 bytes) are returned. A transparent proxy that forwards the
monitor's address in `X-Forwarded-For` is therefore classified `transparent`
rather than `elite`.

### Tamper detection

The same echo request also checks whether the proxy modifies traffic. Each
request carries a random `fpa_nonce` query parameter (24 hex characters), and
only a response with status 200 is compared; block pages and captive portals
that answer 403 or any other status fail the check as before and are never
recorded as tampered. A 200 response is `tampered` when:

- the body is not the echo JSON object, `args.fpa_nonce` does not echo the
  nonce exactly, or `url` does not contain the requested host, path, and nonce
  (the scheme is ignored so httpbin-style echoes behind TLS terminators work);
- or the response carries a header that no recent direct fetch of the same echo
  URL returned, or more `Server` or `Content-Type` values than the direct
  fetches did (for example an injected second `Server` line).

The header baseline is the union of header names over the last 8 direct
fetches that the egress-IP self-check already makes (once at startup and then
hourly, with a nonce of its own), so Cloudflare header variance does not cause
false positives. Until the first direct fetch succeeds only the body is
checked. An endpoint whose direct response does not echo the nonce is not
tamper-checked at all, and before any direct fetch only URLs whose path ends in
`/get` are checked. These headers are an allowlist of benign proxy and cache
headers that never count as injection: `Via`, `X-Cache`, `X-Cache-Lookup`,
`X-Cache-Hits`, `Age`, `Cache-Status`, `Accept-Ranges`, `Proxy-Connection`,
`Connection`, `Keep-Alive`, `Proxy-Agent`, `Content-Length`, and
`Transfer-Encoding`. Any other added header still counts: in a sample of 29
listed HTTP proxies, `X-PANW-*` (a Palo Alto inspection firewall) and
`Fly-Request-Id` (a proxy fronted by another edge) were flagged, while the
remaining clean proxies added nothing beyond this list.

A tampered response is still classified for anonymity when its JSON is
parseable. Results are stored as `tampered` and `tamper_checked_at_ms`, and
tampered proxies **stay listed**. `/proxies` and the internal API expose
`tampered` while fresh (within `classification_max_age`), and
`exclude_tampered=true` drops proxies whose fresh flag is true. Unchecked or
stale proxies stay included. `proxy-router` sends it by default
(`FREEPROXYAPI_EXCLUDE_TAMPERED`). Outcomes are counted in
`freeproxyapi_echo_tamper_results_total{outcome="clean"|"tampered"}`, with the
first detected reason in
`freeproxyapi_echo_tamper_reasons_total{reason="body"|"nonce"|"url"|"header"}`.

## Securing Redis and bounding feed intake

Redis holds every candidate and validated endpoint and ships with no
authentication in the checked-in Compose file and Kubernetes manifests. Treat
it as private infrastructure:

- Never publish its port. The Compose file does not map it to the host; any
  other container on the same Compose network can still reach it.
- The Kubernetes `redis-networkpolicy.yaml` only restricts traffic if your CNI
  enforces `NetworkPolicy` (kind and a default k3s flannel install do not).
- To require a password, start Redis with `--requirepass` (or an ACL user) and
  give the monitor the credentialed URL through the environment, so it stays
  out of `monitor.json` and any ConfigMap built from it:

  ```sh
  FREEPROXYAPI_REDIS_URL='redis://:CHANGE-ME@redis:6379/0'
  ```

  In Kubernetes, set that variable from a Secret with `secretKeyRef`. The URL
  is never logged.

Feeds are untrusted input, and each unique endpoint becomes a Redis hash:

- `max_records_per_source` (default 250000, maximum 5000000) stops reading a
  feed after that many accepted records and ignores the rest. The records
  already read still merge, so a runaway list is truncated rather than dropped.
  Truncations are logged and counted in `freeproxyapi_sources_truncated_total`.
- `max_candidates` (default 0 = off) stops adding new endpoints once the
  candidate set reaches it. Feeds are drained in priority order, so the
  lowest-priority feeds lose out first, and a batch can overshoot by up to
  2000. Skipped endpoints are counted in
  `freeproxyapi_candidates_capped_total`. Set it comfortably below Redis
  `maxmemory`, which the base manifest runs with `noeviction`.
- Source URLs are logged as `scheme://host` only, and fetch errors have URLs
  redacted, because feed URLs sometimes carry API keys.

## Internal query API

`internal_api_token_file` points at a mounted file whose contents are the
bearer token for `/internal/api/v1/proxies`. Without it the endpoint does not
exist. The API returns credential-free endpoint URLs filtered by endpoint
country (`country`), observed exit country (`exit_country`), ASN, anonymity,
latency ceiling, minimum success ratio, `max_age` freshness of the last
successful probe, `https`, `exclude_tampered`, and limit — keep the Service
ClusterIP/internal only; the public `/proxies` route is the public alternative.

## Unified country-aware HTTP proxy

The complete Kubernetes, multi-country, and local-process walkthrough is in
[gost-router.md](gost-router.md).

The optional `k8s/overlays/gost-router` deployment runs GOST v3 alongside a
small synchronizer. The synchronizer periodically reads the validated set from
the internal API, renders mixed HTTP/SOCKS upstreams, and asks GOST to reload
the configuration. GOST requires its own client credentials. Port 3128 is the
all-country pool; configured country pools start at port 3129.

The live overlay configures the US pool on port 3129:

```sh
curl --proxy http://PROXY_USER:PROXY_PASSWORD@gost.example.net:3129 \
  https://example.com
```

Port 3128 uses all validated upstreams. Set
`GOST_COUNTRY_FIELD=exit` to route by the observed exit country instead of the
proxy endpoint country. Add country codes to `GOST_COUNTRIES` and expose their
corresponding ports in the Service manifest.

Create the required router Secret before applying the overlay:

```sh
kubectl -n freeproxyapi create secret generic gost-router-auth \
  --from-literal=proxy-username='replace-me' \
  --from-literal=proxy-password='replace-me' \
  --from-literal=api-username='replace-me' \
  --from-literal=api-password='replace-me'
kubectl apply -k k8s/overlays/gost-router
```

The overlay expects the existing `freeproxyapi-internal-api` Secret and a
published FreeProxyAPI image containing `proxy-router`; update its image tag
before applying it. Do not expose the GOST Web API or the FreeProxyAPI
internal API publicly.

## Public stats and Cloudflare Tunnel

The live hostname serves a small public developer homepage at `/` with the
current availability summary and query examples. The `/dashboard` page
shows aggregate operational data queried from Prometheus. Configure its base
URL with `prometheus_url` in `monitor.json`; the Kubernetes base config points
at the Prometheus service installed by the monitoring overlay.

`/stats` intentionally requires no authentication and contains counts only:

```sh
curl https://freeproxyapi.crawlora.net/stats
```

It returns `status`, `total`, `stable`, `by_country`, `stable_by_country`,
`by_anonymity` (validated proxies per anonymity class, such as `elite`,
`anonymous`, `transparent`, `unknown`), `latency_bands` (validated proxies per
band: `<200ms`, `200-500ms`, `500-1000ms`, `>1000ms`, plus `unknown` when no
latency has been measured), and `checked_at`. Every count comes from the same
cached validated-pool aggregate, so the maps add up to `total`.

The public proxy query returns pages of matching results. An optional `limit`
from 1 to `public_max_limit` (default 1000) sets the page size; an omitted,
zero, negative, or invalid `limit` returns 100, and a larger one is clamped. An
optional `offset` from 0 to 100000 skips earlier matches (other values return
`400`). Each response carries `count`, `proxies`,
`limit`, `offset`, and `has_more`; request `offset + limit` while `has_more` is
true. Pages are best-effort because the validated pool changes between requests:

```sh
curl 'https://freeproxyapi.crawlora.net/proxies?exit_country=US&min_ratio_pct=80'
```

`/proxies` is limited to 60 requests per minute per client IP on each
application replica. The Cloudflare edge also applies the zone's 10-requests-per-10-
seconds per-IP/colocation rule to this path. Successful responses are cacheable
for 30 seconds, with the query string included in the cache key.

Client identity defaults to the TCP socket peer. To use `CF-Connecting-IP` or
`X-Forwarded-For`, set `trusted_proxy_cidrs` in the private monitor config to
the exact CIDRs of the ingress processes that connect directly to the monitor.
For an in-cluster `cloudflared` deployment, these are the pod or node source
CIDRs actually observed in the monitor's `RemoteAddr`; they are not
automatically Cloudflare's published edge ranges. Keep the list empty when the
monitor is directly reachable. The trusted ingress must strip and overwrite
`CF-Connecting-IP` and must replace or sanitize `X-Forwarded-For`; trusting a
proxy that forwards caller-supplied identity headers permits address spoofing.
Avoid broad cluster, loopback, or private-network defaults unless every possible
socket peer in that range is a controlled, sanitizing ingress.

`/dashboard/data` canonicalizes its range to `1h`, `6h`, or `24h` and caches
each result on each replica for 15 seconds. Concurrent requests for the same
range share one refresh, and each replica permits at most two Prometheus
refreshes at once. Successful responses carry
`Cache-Control: public, max-age=15, s-maxage=15`; errors stay `no-store`.
Failed refreshes have a short retry cooldown so an outage cannot fan out into a
new expensive query set for every public request. While Prometheus keeps
failing, the last successful payload for that range is served with
`"status": "degraded"` and `"stale": true`; with no earlier success the
response is `503`.

`current` includes the queue gauges `pending` (all queued candidates),
`pending_due` (candidates due for validation now), and
`pending_next_due_in_seconds` (seconds until the earliest one is due; `0` when
something is already due or the queue is empty), each the maximum across
replicas. The payload also carries a `health` object computed from instant
queries that are shared across ranges. Each field is a number, or `null` when
its query returns no samples:

| Field | Query |
| --- | --- |
| `https_checks_1h` | `sum(increase(freeproxyapi_https_probe_results_total[1h]))` |
| `https_pass_rate_1h` | `100 * ok / total` over the same 1h window; `null` when there were no checks |
| `tamper_checks_1h` | `sum(increase(freeproxyapi_echo_tamper_results_total[1h]))` |
| `tampered_1h` | `sum(increase(freeproxyapi_echo_tamper_results_total{outcome="tampered"}[1h]))` |
| `internal_api_failures_15m` | `sum(increase(freeproxyapi_internal_api_query_failures_total[15m]))` |
| `gost_router_available` | `max(kube_deployment_status_replicas_available{namespace="freeproxyapi",deployment="gost-router"})` |
| `gost_router_desired` | `max(kube_deployment_spec_replicas{namespace="freeproxyapi",deployment="gost-router"})` |

### Paths forwarded by the Cloudflare edge

The Cloudflare configuration in front of the live hostname is managed outside
this repository. It currently forwards only these paths to the monitor:
exactly `/`, `/icon.png`, `/favicon.png`, `/get`, `/stats`, `/proxies`, and
everything under `/dashboard` (for example `/dashboard/data`). Any other path,
including `/social-share.png`, `/report`, and `/metrics`, gets a `404` from
Cloudflare without reaching the origin.

For that reason the Open Graph image is also served at
`/dashboard/social-share.png`, and public pages should reference that URL.
`/social-share.png` still works when the monitor is reached directly. New
public root paths such as `/favicon.ico` or `/sitemap.xml` need a Cloudflare
rule that forwards them before the monitor can serve them.

For the live cluster, the dedicated `k8s/overlays/cloudflared` deployment runs
two replicas of the same Cloudflare Tunnel token and connects them to the
private `freeproxyapi` Service. Their hostname topology-spread rule strictly
separates them by node; the overlay therefore requires at least two
schedulable nodes. Create the
`cloudflared-freeproxyapi` Secret out-of-band from the tunnel token, then apply
the overlay. Keep `/internal/api/v1/proxies` and the proxy-router administration
API private. Redis remains a single StatefulSet, so this provides application
and ingress redundancy but not Redis failover.

## Alerting and dashboards

The repository includes a private monitoring overlay for Prometheus, Grafana,
and Alertmanager. It scrapes the `freeproxyapi` Service at `/metrics`, loads
the dashboard and alert rules, and persists time-series data on the `ssd`
StorageClass (the cluster requires at least 5 GiB per volume):

```sh
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -
GRAFANA_ADMIN_PASSWORD='use-a-random-password'
helm upgrade --install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring --version 70.4.2 \
  --set-string grafana.adminPassword="$GRAFANA_ADMIN_PASSWORD" \
  --values k8s/overlays/monitoring/values.yaml
kubectl apply -k k8s/overlays/monitoring
```

Keep Grafana, Prometheus, and Alertmanager as internal `ClusterIP` Services.
Use `kubectl -n monitoring port-forward svc/monitoring-grafana 3000:80` for
operator access. The dashboard is loaded automatically from
`k8s/overlays/monitoring/grafana-dashboard.json` and the alert rules from
`k8s/overlays/monitoring/prometheus-rule.yaml` (the single source of truth
for both); alert rules cover validated-count drops,
probe failures, source failures, budget denials, lease reclaims, GeoIP
availability, and unknown-country growth.

Probe outcomes are also exported by kind and failure reason, alongside the
unchanged `freeproxyapi_probe_results_total{outcome}` and target-labeled
families:

- `freeproxyapi_probe_outcomes_total{kind,outcome,reason}` counter.
- `freeproxyapi_probe_outcome_duration_seconds{kind,outcome}` histogram.

`kind` is `first` (the candidate had no recorded check when claimed) or
`retest`. `outcome` is `ok` or `failed`; `reason` is `ok` for successes and
one of a fixed set of codes for failures: `connect_timeout` (TCP connect to
the proxy hop), `handshake_timeout` (TLS or SOCKS handshake), `timeout`
(after connect), `refused`, `reset`, `eof`, `unreachable`, `dns`, `tls`,
`proxy_auth`, `connect_rejected` (CONNECT answered non-2xx), `bad_status_3xx`,
`bad_status_4xx`, `bad_status_5xx`, `bad_response`, `unexpected_body`,
`socks_handshake`, `too_large`, `blocked_address`, `invalid_proxy`, `canceled`,
and `other`. The same code is stored as the candidate's `last_error`. For
example, `sum by (reason) (rate(freeproxyapi_probe_outcomes_total{outcome="failed"}[10m]))`
breaks failures down and
`sum by (kind) (rate(freeproxyapi_probe_outcomes_total{outcome="ok"}[1h])) / sum by (kind) (rate(freeproxyapi_probe_outcomes_total[1h]))`
compares first-probe and retest success rates.
