# Source expansion — 12 September 2026

The production inventory started at 70 feeds. Local configuration contained
35 pending additions. Ten more feeds were found across RelayGlass,
OpenProxyList, and the xyzs996 proxy health list.

All 45 candidate feeds downloaded and parsed locally. Ten large feeds
(over 10,000 accepted records per feed) were excluded from this rollout to
avoid adding a large unvalidated backlog. The selected 35 feeds retain their
protocol hints; RelayGlass HTTPS entries use HTTP CONNECT, so their hint is
`http`. The resulting production configuration contains 105 feeds.

The canary uses the existing production image and GeoIP databases, a separate
Redis namespace, no public Service selector, and a 60-request-per-minute
budget. It fetched 35/35 feeds, rejected ten malformed/unsafe records, and
ingested 27,923 unique candidates, including 11,050 absent from production
at the comparison snapshot. These are candidates, not validated proxies.

Canary gate: the final worker run completed 89 probes: 7 succeeded and 82
failed. There were no result conflicts, lease reclaims, source-fetch failures,
or pod restarts. All three production replicas stayed ready. A passing probe
only proves availability at that moment; it is not a long-term reliability
guarantee. The canary was stopped before production promotion.

Rollout completed at approximately 17:35 UTC on 11 September (01:35 on
12 September in Asia/Shanghai). All three updated replicas were ready and
available. Production fetched 105/105 feeds and tracked 53,486 candidates,
up from 42,038 before rollout. The final snapshot retained 276 validated
proxies with 24 probes in flight; the candidate increase is not a claim of
11,448 newly working proxies. `/readyz`, public `/stats`, and public
`/proxies?limit=1` passed; the last returned exactly one proxy.

The canary Pod and ConfigMap were deleted and its Redis namespace contained
zero keys after cleanup. The application image remained
`ghcr.io/crawlora-org/freeproxyapi:all-results-20260911172159`.

Rollback snapshots and evidence are saved locally under
`/tmp/freeproxyapi-source-expansion/`. To restore the previous source config:

```sh
kubectl --kubeconfig terraform/kubeconfig.yaml -n freeproxyapi patch configmap freeproxyapi-config --type merge --patch-file /tmp/freeproxyapi-source-expansion/rollback-patch.json
kubectl --kubeconfig terraform/kubeconfig.yaml -n freeproxyapi rollout restart deployment/freeproxyapi
```

New source references: [RelayGlass](https://github.com/relayglass/free-proxy-list),
[OpenProxyList](https://api.openproxylist.xyz/http.txt), and
[proxy health list](https://github.com/xyzs996/free-proxy-health-list).

Checks: `go test ./...`, `go vet ./...`, Kustomize rendering, and
`git diff --check` passed. Only the production `sources` field is promoted;
the existing image and other runtime settings are preserved.
