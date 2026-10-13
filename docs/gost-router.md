# Country-aware GOST router

## Public, GOST-only middleware

If you only need a local authenticated GOST proxy and do not want to run the
FreeProxyAPI monitor or Redis, use [`docker-compose.gost-public.yml`](../docker-compose.gost-public.yml).
The synchronizer runs in explicit public-API mode and reads validated proxies
from `https://freeproxyapi.crawlora.net/proxies`.

Prepare the shared runtime file and credentials. The bootstrap file must carry
the same API credentials you export, because `proxy-router` authenticates its
GOST reload requests with them:

```sh
mkdir -p runtime
export GOST_API_USERNAME='choose-an-admin-user'
export GOST_API_PASSWORD='choose-an-admin-password'
export GOST_PROXY_USERNAME='choose-a-client-user'
export GOST_PROXY_PASSWORD='choose-a-client-password'
export GOST_COUNTRIES='DE,SG,US'

python3 - <<'PY'
import json
import os

with open('runtime/gost.json', 'w', encoding='utf-8') as handle:
    json.dump({
        'api': {
            'addr': ':18080',
            'auth': {
                'username': os.environ['GOST_API_USERNAME'],
                'password': os.environ['GOST_API_PASSWORD'],
            },
        },
    }, handle, indent=2)
    handle.write('\n')
os.chmod('runtime/gost.json', 0o600)
PY
```

Start only GOST and the synchronizer:

```sh
docker compose -f docker-compose.gost-public-sidecar.yml up -d
docker compose -f docker-compose.gost-public-sidecar.yml logs -f proxy-router
```

This launcher reuses `docker-compose.gost-public.yml`; it runs GOST and the
`proxy-router` refresh sidecar only. The original filename remains supported.

The default unified listener is `127.0.0.1:3128`. Country listeners are
assigned alphabetically from `3129` (`DE`), `3130` (`SG`), and `3131` (`US`)
for the example above. The listener still requires the client credentials.

The public API filter variables are optional and can be combined:

```sh
export FREEPROXYAPI_COUNTRY=US
export FREEPROXYAPI_EXIT_COUNTRY=CA
export FREEPROXYAPI_EXIT_ASN=AS64501
export FREEPROXYAPI_ANONYMITY=elite
export FREEPROXYAPI_ASN=AS64500
export FREEPROXYAPI_TARGET_PROFILE=owned-origin
export FREEPROXYAPI_MAX_LATENCY_MS=500
export FREEPROXYAPI_MIN_RATIO_PCT=90
export FREEPROXYAPI_GEO_MISMATCH=true
export FREEPROXYAPI_EXCLUDE_HOSTING=true
export FREEPROXYAPI_STABLE_EXIT=true
export FREEPROXYAPI_MIN_SUCCESS_STREAK=4
export FREEPROXYAPI_MIN_RELIABILITY_7D_PCT=90
export FREEPROXYAPI_MIN_RELIABILITY_7D_SAMPLES=20
export FREEPROXYAPI_EXCLUDE_TAMPERED=true
export FREEPROXYAPI_REQUIRE_TAMPER_CHECKED=true
export FREEPROXYAPI_HTTPS=true
export FREEPROXYAPI_LIMIT=250
```

`FREEPROXYAPI_COUNTRY` and `FREEPROXYAPI_EXIT_COUNTRY` filter the endpoint and
observed exit countries respectively. `FREEPROXYAPI_EXCLUDE_TAMPERED`
(flag `-exclude-tampered`, default `true`) sends `exclude_tampered=true`, so
the router skips proxies whose latest fresh echo tamper check found injected
headers or a rewritten body. Set it to `false` to route through them anyway.
For a measured clean pool, set `FREEPROXYAPI_REQUIRE_TAMPER_CHECKED=true`
(flag `-require-tamper-checked`) to also reject unchecked or expired tamper
results. Set `FREEPROXYAPI_HTTPS=true` (flag `-https`) to require a passing
HTTPS/CONNECT check. Both options default to `false`; the stricter filters can
leave the pool empty until the monitor has collected matching measurements.
The router also checks returned clean verdicts and timestamps locally, so an
older compatible API that ignores the filter cannot supply unchecked proxies.
The API controls the classification age limit. A clean echo observation does
not guarantee that other traffic is unchanged.
In strict mode, an empty pool or a failed API refresh writes and reloads an
API-only GOST configuration, withdrawing proxy listeners until a matching
pool returns. Write or reload failures are reported and retried; withdrawal
takes effect only after GOST accepts the reload.

Additional quality filters include `FREEPROXYAPI_EXIT_ASN`,
`FREEPROXYAPI_EXCLUDE_HOSTING`, `FREEPROXYAPI_STABLE_EXIT`,
`FREEPROXYAPI_MIN_SUCCESS_STREAK`, `FREEPROXYAPI_MIN_RELIABILITY_7D_PCT`, and
`FREEPROXYAPI_MIN_RELIABILITY_7D_SAMPLES`. `exclude_hosting` removes only exits
explicitly matched by the ASN-organization heuristic; an unknown value remains
eligible and does not mean residential. `stable_exit` requires at least two
actual matching exit-IP observations in seven days with no observed change in
that window. Missing samples do not count as evidence of stability. The
reliability percentage uses completed observations after a candidate's first
successful sample; missing hours add no failures and hour-bucket boundaries
can vary by less than one hour.

`FREEPROXYAPI_TARGET_PROFILE` requests a named content profile configured by
the API operator. The profile requires an operator-owned HTTP(S) endpoint and
an expected response body. A public API that does not configure that profile
will reject the query; the templates use an empty default. Profile checks are
sampled and consume the proxy monitor's global request budget. See the [API
reference](api.md) and [configuration reference](configuration.md) for the
measured fields and profile setup.

These filters select entries from FreeProxyAPI's listed pool.
`GOST_COUNTRY_FIELD` controls which
country field is used when building dedicated listeners. The Compose template
keeps the API and proxy listeners on loopback; change the host bind only after
adding the network controls and authentication appropriate for your deployment.

To use another compatible endpoint, set both `FREEPROXYAPI_PUBLIC=true` and
`FREEPROXYAPI_URL`. Public mode intentionally sends no bearer token. The
existing private mode remains available by using the Kubernetes overlay or
`docker-compose.gost.yml`.

For Kubernetes users, the equivalent GOST-only sidecar overlay is
`k8s/overlays/gost-public-sidecar`. Create the required credentials, review the
filter and country environment values in `deployment.yaml`, then apply it:

```sh
kubectl apply -f k8s/overlays/gost-public-sidecar/namespace.yaml
kubectl -n gost-public create secret generic gost-public-auth \
  --from-literal=proxy-username='choose-a-client-user' \
  --from-literal=proxy-password='choose-a-client-password' \
  --from-literal=api-username='choose-an-admin-user' \
  --from-literal=api-password='choose-an-admin-password'
kubectl apply -k k8s/overlays/gost-public-sidecar
kubectl -n gost-public rollout status deployment/gost-public-sidecar --timeout=180s
```

The `proxy-router` sidecar fetches immediately and refreshes every minute. By
default, if the public API is temporarily unavailable, it keeps the last
generated GOST configuration and retries on the next interval. With
`FREEPROXYAPI_REQUIRE_TAMPER_CHECKED=true`, it instead withdraws the proxy
listeners and retries until a measured clean pool is available.

The overlay needs no bootstrap file. `proxy-router` writes an API-only GOST
config using the `api-username` and `api-password` from the Secret, so `gost`
starts even while the public API is unreachable and opens the proxy listeners on
the first successful sync. The Pod can take up to 30 minutes to become ready
in that case (the `gost` startup probe). Its reload requests must use the same
API credentials GOST starts with, which is why the file is generated from the
Secret instead of copied from a fixed one.

The optional `k8s/overlays/gost-router` overlay runs two containers in each
gost-router Pod:

- `proxy-router` reads validated proxies from FreeProxyAPI's private API and
  writes `/runtime/gost.json`.
- `gost` runs GOST v3, reloads that file, and exposes authenticated HTTP proxy
  listeners.

Port `3128` is always the unified pool. Country listeners start at `3129` and
are assigned after country codes are normalized, deduplicated, and sorted
alphabetically. For example, `GOST_COUNTRIES=DE,SG,US` maps `DE` to `3129`,
`SG` to `3130`, and `US` to `3131`. The router uses endpoint country by
default; set `GOST_COUNTRY_FIELD=exit` to use the observed exit country.

## Start the router in Kubernetes

Deploy FreeProxyAPI and Redis first. The router also needs the existing
`freeproxyapi-internal-api` Secret and a separate Secret containing four GOST
credentials:

```sh
kubectl -n freeproxyapi create secret generic gost-router-auth \
  --from-literal=proxy-username='choose-a-client-user' \
  --from-literal=proxy-password='choose-a-client-password' \
  --from-literal=api-username='choose-an-admin-user' \
  --from-literal=api-password='choose-an-admin-password'
kubectl apply -k k8s/overlays/gost-router
kubectl -n freeproxyapi rollout status deployment/gost-router --timeout=180s
```

The checked-in overlay runs two replicas behind the `gost-router` Service,
spread across nodes when possible by a preferred pod anti-affinity, and a
`gost-router` PodDisruptionBudget lets a node drain evict only one of them at a
time. Each Pod runs its own `proxy-router` and syncs from the internal API
independently, so the two Pods can briefly serve different upstream sets
between refreshes. The overlay exposes the all-country listener on `3128` and
the default US listener on `3129`. The router synchronizer queries at most 1,000
validated proxies every minute, following the API's paging (`has_more` and
`offset`) when it serves fewer results per page than that, and keeping the last
generated configuration if any page fails. It reloads GOST on its first refresh, whenever
the rendered configuration changes, and after a failed reload; an unchanged
configuration is not reloaded. A FreeProxyAPI response larger than 8 MiB is
rejected with an explicit size error; lower `FREEPROXYAPI_LIMIT`, the most
proxies fetched in total per refresh, if that happens.

When `/runtime/gost.json` does not exist yet, `proxy-router` first writes an
API-only bootstrap config, so `gost` starts even while FreeProxyAPI is still
rolling out or its internal API is failing. The proxy listeners open on the
first successful sync; until then the Pod stays unready and a `startupProbe`
(up to 30 minutes) keeps liveness from restarting `gost`. Rolling updates use
`maxSurge: 1` and `maxUnavailable: 0`, so both existing Pods keep serving until
each replacement has completed its first sync. An existing config file, for
example after a `proxy-router` restart, is never replaced by the bootstrap.

`proxy-router` exits at startup when any of the four GOST credentials is
missing (they are required in both public and private API modes) or when a
numeric, duration, or boolean environment variable such as
`GOST_REFRESH_INTERVAL`, `GOST_MIN_RATIO_PCT`, `FREEPROXYAPI_LIMIT`, or
`FREEPROXYAPI_PUBLIC` cannot be parsed. Durations need a unit, for example
`1m` rather than `60`.

Every upstream is re-validated before it is written into the GOST config.
Credentialed URLs, non-public IP literals, `localhost` forms (including
`localhost.` and `*.localhost`), inet_aton-style numeric hosts such as
`2130706433` or `0x7f.0.0.1`, and wildcard-DNS names that embed a non-public
IPv4 address such as `127.0.0.1.nip.io` are dropped. Other hostnames are kept
because source feeds may carry them, and GOST resolves them itself when it
dials. The router does not re-resolve those names, so DNS for a validated
hostname is trusted at dial time.

## Configure multiple countries

Create a local Kustomize overlay or apply a reviewed patch that changes the
`proxy-router` container's `GOST_COUNTRIES` value. For example:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gost-router
spec:
  template:
    spec:
      containers:
        - name: proxy-router
          env:
            - name: GOST_COUNTRIES
              value: DE,SG,US
            # Use exit-country metadata instead when required:
            # - name: GOST_COUNTRY_FIELD
            #   value: exit
```

Expose the corresponding numeric listener ports on the Service. With
`DE,SG,US`, use:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: gost-router
spec:
  ports:
    - name: http-proxy
      port: 3128
      targetPort: 3128
    - name: de-http
      port: 3129
      targetPort: 3129
    - name: sg-http
      port: 3130
      targetPort: 3130
    - name: us-http
      port: 3131
      targetPort: 3131
```

Apply the patch and wait for the generated GOST config to reload:

```sh
kubectl apply -k k8s/overlays/gost-router
kubectl -n freeproxyapi rollout status deployment/gost-router --timeout=180s
kubectl -n freeproxyapi logs deployment/gost-router -c proxy-router --tail=50
```

The synchronizer log should show a successful FreeProxyAPI query and GOST
reload. If you need to inspect the rendered configuration, query GOST's
authenticated Web API from inside the Pod:

```sh
kubectl -n freeproxyapi port-forward deploy/gost-router 18080:18080
curl -u 'choose-an-admin-user:choose-an-admin-password' \
  'http://127.0.0.1:18080/config?format=json'
```

Keep the GOST Web API on its loopback address. Do not publish port `18080`,
the FreeProxyAPI internal API, or the router credentials outside the private
cluster network.

## Start the router with Docker Compose

[`docker-compose.gost.yml`](../docker-compose.gost.yml) is a standalone stack
for a single host. It runs Redis, the network-validating FreeProxyAPI monitor,
GOST, and the synchronizer. The monitor must have both MaxMind databases so it
can annotate endpoint country and ASN; obtain and use the databases under the
terms of their license, then place these files at:

```text
secrets/geoip/GeoLite2-Country.mmdb
secrets/geoip/GeoLite2-ASN.mmdb
```

The stack also expects an operator bearer token (at least 16 characters) in
`secrets/freeproxyapi-internal-api-token`. Keep the four GOST credentials in a
local `.env` file or exported environment variables; `.env` and the local
`secrets/` and `runtime/` directories are ignored by Git.

Create a bootstrap config before starting the stack. It only enables the GOST
Web API; `proxy-router` replaces it with the complete country-aware config once
the first validated proxies are available:

```sh
mkdir -p secrets/geoip runtime
printf '%s\n' 'replace-with-a-long-random-token' > secrets/freeproxyapi-internal-api-token
chmod 600 secrets/freeproxyapi-internal-api-token

export GOST_API_USERNAME='choose-an-admin-user'
export GOST_API_PASSWORD='choose-an-admin-password'
export GOST_PROXY_USERNAME='choose-a-client-user'
export GOST_PROXY_PASSWORD='choose-a-client-password'
export GOST_COUNTRIES='DE,SG,US'
export GOST_COUNTRY_FIELD='entry'

# If the values live in .env instead, load that ignored file first:
# set -a; . ./.env; set +a

python3 - <<'PY'
import json
import os

with open('runtime/gost.json', 'w', encoding='utf-8') as handle:
    json.dump({
        'api': {
            'addr': ':18080',
            'auth': {
                'username': os.environ['GOST_API_USERNAME'],
                'password': os.environ['GOST_API_PASSWORD'],
            },
        },
    }, handle, indent=2)
    handle.write('\n')
PY

docker compose -f docker-compose.gost.yml up --build -d
docker compose -f docker-compose.gost.yml ps
```

Put the source inventory in `examples/proxies.txt`, or edit the read-only
source bind mount in the Compose file to point at an operator-managed file.
The default country-to-port map is deterministic: `3128` is the unified pool,
then `3129` is `DE`, `3130` is `SG`, and `3131` is `US` because country codes
are sorted before ports are assigned. All listeners require the client
credentials above:

```sh
curl --proxy-user "$GOST_PROXY_USERNAME:$GOST_PROXY_PASSWORD" \
  --proxy http://127.0.0.1:3131 https://example.com
```

The GOST API is bound to host loopback only for inspection. Check synchronizer
logs and the rendered configuration after startup:

```sh
docker compose -f docker-compose.gost.yml logs --tail=50 proxy-router
curl -u "$GOST_API_USERNAME:$GOST_API_PASSWORD" \
  'http://127.0.0.1:18080/config?format=json'
```

If you change `GOST_COUNTRIES`, update the four GOST listener port mappings in
`docker-compose.gost.yml` to match the sorted country list. Stop the stack with
`docker compose -f docker-compose.gost.yml down`; add `-v` only when Redis data
should also be removed.

## Use a country listener

All listeners require the proxy credentials stored in `gost-router-auth`:

```sh
# Unified pool (all validated upstreams)
curl --proxy http://choose-a-client-user:choose-a-client-password@gost-router.freeproxyapi.svc.cluster.local:3128 \
  https://example.com

# US pool for GOST_COUNTRIES=DE,SG,US (port 3131)
curl --proxy http://choose-a-client-user:choose-a-client-password@gost-router.freeproxyapi.svc.cluster.local:3131 \
  https://example.com
```

Use URL-safe credentials or configure the equivalent `curl --proxy-user`
flags when a password contains reserved URL characters. A country listener
is created only when the current validated set contains at least one proxy for
that country; the all-country listener still requires at least one validated
proxy overall.

## Named sticky sessions

`GOST_SESSIONS` optionally creates one authenticated GOST listener for each
named session. These are named ports shared by every client using them, not
per-client or cookie-based sessions. Set a comma-separated list of up to 128
unique IDs; each ID is 1–32 ASCII letters, digits, underscores, or hyphens,
starting with a letter or digit. Ports use sorted ID order starting at
`GOST_SESSION_BASE_PORT` (default 10000). `GOST_SESSION_TTL` defaults to 10
minutes and `GOST_SESSION_FAILOVER` defaults to false. The Compose and base
Kubernetes templates leave `GOST_SESSIONS` empty. The live overlay separately
enables two named listeners.

The base port must be at least 1024, and the assigned range must fit below
65536 and avoid the unified/country listeners and GOST Web API port. For
several sessions, leave enough free consecutive ports.

Pins live in synchronizer memory and are lost on restart; deterministic
selection may choose the same upstream again but does not guarantee it. If a
pinned proxy disappears from the filtered pool or its latest status is failed,
its session listener is withheld for that refresh. The pin remains held through
TTL and the listener may resume if the same upstream becomes eligible again. A
known changed exit IP also invalidates that pin. Unknown exit metadata does not
establish IP continuity; use `FREEPROXYAPI_STABLE_EXIT=true` when you require
the pool to have repeated recent exit observations. With
`GOST_SESSION_FAILOVER=true`, the router may reassign an invalid pin from the
current eligible pool before TTL expiry. A refresh API error removes the
session listeners after GOST accepts
the reload; a requested quality-filter failure may withdraw the
whole proxy pool.

The live Kubernetes overlay enables `alpha` and `beta` through the separate
`gost-router-sessions` ClusterIP Service on ports 10000 and 10001. That Service
uses client-IP affinity so a client's connections reach the same router Pod;
the listener still uses the existing GOST proxy credentials. Pins are stored
in each router process rather than shared between replicas, so a Pod restart
loses its pins. Enabling failover can assign a replacement before TTL expiry
and starts a new pin lifetime. The separate Service remains cluster-internal.

The live overlay requires a measured success streak of at least one and a
fresh, explicitly clean tamper check for every router pool. This excludes
legacy unknown streaks and proxies whose latest completed check failed.
The general templates leave these additional requirements disabled.

### Expose two session ports with Compose

The checked-in Compose files publish only the normal and country listeners.
Create a local `compose.sessions.yaml` override to enable two sessions and bind
their listeners to host loopback:

```yaml
services:
  proxy-router:
    environment:
      GOST_SESSIONS: "alpha,beta"
      GOST_SESSION_TTL: "10m"
      GOST_SESSION_BASE_PORT: "10000"
      GOST_SESSION_FAILOVER: "false"
  gost:
    ports:
      - "127.0.0.1:10000:10000"
      - "127.0.0.1:10001:10001"
```

Start it after either Compose base file, for example:

```sh
docker compose -f docker-compose.gost-public.yml -f compose.sessions.yaml up -d
```

The session listeners still require the configured GOST proxy credentials. Do
not broaden the host binds unless the network and authentication controls are
appropriate for your deployment.

### Expose session ports with Kubernetes

The checked-in `gost-router` Service does not publish these ports. In a local
Kustomize patch, set the `proxy-router` environment values and add matching
ports to the `gost` container and Service. For the `alpha,beta` example, add:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gost-router
spec:
  template:
    spec:
      containers:
        - name: proxy-router
          env:
            - name: GOST_SESSIONS
              value: "alpha,beta"
            - name: GOST_SESSION_BASE_PORT
              value: "10000"
        - name: gost
          ports:
            - name: session-alpha
              containerPort: 10000
            - name: session-beta
              containerPort: 10001
---
apiVersion: v1
kind: Service
metadata:
  name: gost-router
spec:
  ports:
    - name: session-alpha
      port: 10000
      targetPort: session-alpha
    - name: session-beta
      port: 10001
      targetPort: session-beta
```

The Service stays `ClusterIP` in the example. Add external exposure only in
your own overlay and protect it with the network policy and client
authentication your deployment requires.

## Local GOST process

For a non-Kubernetes process, build the synchronizer and point it at a private
FreeProxyAPI API URL and a local GOST Web API. The same environment variables
used by the container apply:

```sh
export FREEPROXYAPI_URL='http://127.0.0.1:8080/internal/api/v1/proxies'
export FREEPROXYAPI_TOKEN_FILE="$PWD/internal-api-token"
export GOST_CONFIG_FILE="$PWD/gost.json"
export GOST_API_URL='http://127.0.0.1:18080'
export GOST_API_USERNAME='admin'
export GOST_API_PASSWORD='change-this'
export GOST_PROXY_USERNAME='client'
export GOST_PROXY_PASSWORD='change-this-too'
export GOST_COUNTRIES='DE,SG,US'
export GOST_COUNTRY_FIELD='entry'

go run ./cmd/proxy-router
gost -C "$GOST_CONFIG_FILE"
```

Passwords are read only from the environment or from files; the synchronizer
has no password flags because command-line arguments are visible to other
local users through `ps`. To keep passwords out of the environment too, set
`GOST_API_PASSWORD_FILE` / `GOST_PROXY_PASSWORD_FILE` (or the
`-gost-api-password-file` / `-proxy-password-file` flags) to a file holding
the password; one trailing newline is ignored. Setting both a password
variable and its file is a startup error. Usernames stay available as
`-gost-api-user` / `-proxy-user` flags.

The FreeProxyAPI token file must contain the operator-issued bearer token and
be readable only by the synchronizer. Keep both APIs on loopback or another
private network path.
