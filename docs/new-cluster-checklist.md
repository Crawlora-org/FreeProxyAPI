# New-cluster checklist

Bringing FreeProxyAPI up on a fresh cluster (or a fresh namespace on an
existing one). Everything here is automated by `scripts/bootstrap.sh`; this
document explains what it does and what it cannot do for you.

## Prerequisites

- A cluster you control, with:
  - a default StorageClass that provisions volumes of **at least 5 Gi**
    (several CSI drivers reject smaller PVC requests — the Redis StatefulSet
    claims 5 Gi),
  - working DNS + egress so pods can reach your inventory source URLs, the
    echo endpoint(s), and raw.githubusercontent.com (GeoIP database fetch at
    pod start).
- `kubectl` on your workstation pointed at the target cluster.
- The AMD64 container image available in GitHub Container Registry. The included
  GitHub Actions workflow publishes `ghcr.io/crawlora-org/freeproxyapi` for
  `main` and version tags.
- If the package is private: create an imagePullSecret named `ghcr-registry`
  (or edit the overlay's pullSecrets) before applying.

## Operator-supplied inputs

1. **Monitor config** — a `monitor.json` carrying the cluster's `sources` list
   plus knobs (validation enabled/disabled, anonymity endpoint list, GeoIP
   paths, budget). For the production cluster this is the tracked
   `k8s/overlays/live-local/monitor.json`, changed only through pull requests
   and deployed by CI. For another cluster, keep a separate overlay (outside
   this repository or on a private branch) whose kustomization merges its own
   file over `../private-cluster`.
2. **Inventory file** (optional, validation-disabled mode) — mounted via the
   base ConfigMap's `proxies.txt` key.

## Run it

```sh
NAMESPACE=freeproxyapi \
./scripts/bootstrap.sh
```

The script is idempotent: re-running updates config and re-applies.
It prints smoke-check results (readiness, homepage, public stats, public proxy
query, report payload, metrics reachability, and authenticated internal-API
query) and fails loudly if any of them regresses.
Cloudflare Tunnel routing is a separate optional production step described in
the deployment guide.

## After bootstrap

- Watch pod logs for `geo enriched validated`, `validated requeue`, and the
  absence of `source fetch failed` lines.
- Import `k8s/overlays/monitoring/grafana-dashboard.json` into Grafana; load
  `k8s/overlays/monitoring/prometheus-rule.yaml` into Prometheus
  (PrometheusRule CRD required, or translate the groups into your rule
  format), or apply `k8s/overlays/monitoring` as described in
  `docs/deployment.md`.
- Query the internal API from inside the cluster:

  ```sh
  TOKEN=$(cat /secure/path/internal-api-token)
  kubectl -n freeproxyapi run curl --rm -i --image=curlimages/curl -- \
    -s -H "Authorization: Bearer $TOKEN" \
    'http://freeproxyapi:8080/internal/api/v1/proxies?country=US&min_ratio_pct=80&limit=10'
  ```

## Teardown

```sh
kubectl delete namespace freeproxyapi
```

All state (Redis data included) lives in that namespace; nothing outside it is
owned by FreeProxyAPI.
