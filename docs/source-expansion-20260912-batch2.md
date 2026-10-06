# Source expansion — 12 September 2026, batch 2

Starting production snapshot: 105 feeds, 53,486 candidates, 328 validated
proxies, and three healthy replicas.

Initially evaluated 23 feed URLs. Fourteen passed bounded download and parser checks.
Three mmpx12 URLs returned 404; three MuRongPIG raw lists exceeded 89,000
accepted records each. Its three smaller checked lists were also excluded
because the repository's latest commit was 20 August 2025. The selected
GitHub repositories had commits on 11 September 2026.

Selected feeds come from [Vakhov](https://github.com/vakhov/fresh-proxy-list),
[Zaeem20](https://github.com/Zaeem20/FREE_PROXIES_LIST),
[KangProxy](https://github.com/officialputuid/KangProxy), and
[ProxySpace](https://proxyspace.pro/).

Local payload snapshot:

| Feed | Accepted records | Rejected records |
| --- | ---: | ---: |
| vakhov-http | 528 | 0 |
| Zaeem20-http | 131 | 0 |
| officialputuid-http | 896 | 0 |
| proxyspace-http | 3,472 | 2 |
| vakhov-socks4 | 168 | 0 |
| Zaeem20-socks4 | 75 | 0 |
| officialputuid-socks4 | 779 | 0 |
| proxyspace-socks4 | 3,924 | 2 |
| vakhov-socks5 | 21 | 0 |
| Zaeem20-socks5 | 129 | 0 |
| officialputuid-socks5 | 651 | 0 |
| proxyspace-socks5 | 2,310 | 2 |
| vakhov-https | 6 | 0 |
| zaeem-https | 336 | 0 |

The production canary fetched 14/14 feeds and ingested 11,212 unique
candidates, rejecting six malformed or unsafe records. Of these candidates,
79 were absent from production at comparison time. The canary prioritized
these unseen candidates without changing the 60-request-per-minute budget.
It used eight workers, the existing image and target, an isolated Redis
namespace, no public Service selector, and a 15-minute deadline.

The first canary completed 240 probes: one succeeded and 239 failed, with
zero source-fetch failures, lease reclaims, result conflicts, or restarts.
All 79 candidates absent from production were tested; none succeeded. This
batch adds source diversity, but the sample does not demonstrate additional
working proxies.

Nine further URLs were evaluated. Three
[ALIILAPRO](https://github.com/ALIILAPRO/Proxy) feeds were fresh (11 September
2026): HTTP 605 records, SOCKS4 202, SOCKS5 663, all parsed without rejected
records. The six prxchk/proxylist-to feeds were excluded because their latest
repository commits were in 2024/2023. These three selected feeds are tested
in a second isolated canary before promotion.

The second canary fetched 3/3 feeds, parsed 1,467 unique candidates, and
completed 68 probes: one succeeded and 67 failed. All 37 candidates absent
from production were tested; none succeeded. It had no source-fetch errors,
lease reclaims, result conflicts, or restarts. The first and second canary
new-candidate counts may overlap and must not be summed as distinct proxies.

Both canaries passed fetch/parser/runtime integration checks. Promotion is
for ongoing discovery and source diversity; no net-new working proxy claim
is supported by these short samples. Production's image, request budget,
probe target, and all other runtime settings are unchanged. Both canaries
were stopped before promotion.

Rollout verification completed around 17:50 UTC on 11 September (01:50
on 12 September Asia/Shanghai). All three updated replicas were ready and
available, and each mounted the 122-feed configuration. Runtime image
remained `ghcr.io/crawlora-org/freeproxyapi:all-results-20260911172159`.
Readiness passed, public `/stats` returned status `ok`, and
`/proxies?limit=1` returned exactly one proxy.

Final internal snapshot: 54,413 candidates, 372 validated, 24 probes in
flight. The public stats snapshot returned 368 validated proxies at its own
snapshot time. These totals are not attributed to the new feeds. The prior
105-feed refresh completed with 104/105 successful fetches; an existing
Databay SOCKS5 feed returned HTTP 429. The new replicas respect the existing
source-refresh lock and 20-minute schedule rather than forcing another
immediate full refresh. All 17 additions were separately fetched in the
canaries; their normal production ingestion follows that schedule.

A ConfigMap resource-version conflict was resolved by reading the latest
object and confirming the runtime configuration was semantically unchanged
before retrying the source-only patch. Canary Pod/ConfigMap resources were
removed, and both temporary Redis namespaces contained zero keys after
cleanup.

Only the production `sources` field changed, from 105 to 122 feeds.
Local source/parser/monitor tests, Kustomize rendering, and
`git diff --check` passed. Existing unrelated working-tree edits are retained.

Evidence and rollback snapshots: `/tmp/freeproxyapi-source-expansion-batch2/`.

To roll back this batch's source configuration:

```sh
kubectl --kubeconfig terraform/kubeconfig.yaml -n freeproxyapi patch configmap freeproxyapi-config --type merge --patch-file /tmp/freeproxyapi-source-expansion-batch2/rollback-patch.json
kubectl --kubeconfig terraform/kubeconfig.yaml -n freeproxyapi rollout restart deployment/freeproxyapi
```
