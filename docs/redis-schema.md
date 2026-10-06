# Redis key schema

The monitor coordinates all state through Redis under a single namespace
prefix. Every key below is relative to `namespace`, which defaults to
`freeproxyapi:v1`. No endpoint URLs appear in key names.

## Keys

| Key | Type | Purpose |
| --- | --- | --- |
| `<ns>:candidates` | SET | SHA-256 hex of every canonical endpoint URL currently tracked. |
| `<ns>:source-unique` | STRING | Cross-source unique canonical endpoint count from the latest completed source refresh. |
| `<ns>:source-refresh-stats` | HASH | `parsed` valid records and `succeeded` source count from the latest completed source refresh. |
| `<ns>:proxy:<id>` | HASH | Per-candidate record. `url` is the credential-free endpoint string; `last_checked_at_ms`, `last_ok_at_ms` (time of the latest successful probe), `last_status` (`ok`/`failed`), `last_http_status`, `last_latency_ms`, `last_error` (a short failure code from `probe.ClassifyError`, at most 32 bytes, such as `connect_timeout`, `refused`, or `bad_status_4xx`; empty after a success — raw transport errors are not stored, which keeps every field under `hash-max-listpack-value` so the hash stays a compact listpack), the `consecutive_failures` streak, and stability history — `results10` (last ten outcomes as 1/0), `ok_ratio_pct`, `latency_ewma_ms`, `jitter_ewma_ms` — describe history. Optional `country` (ISO), `asn`, `anonymity` (elite/anonymous/transparent), `exit_ip`, and `exit_country` (GeoIP country of the observed exit IP) annotations come from an operator-supplied GeoIP database plus an optional configured echo endpoint, and are never cleared by a later empty lookup. `anonymity_checked_at_ms` is set whenever any of `anonymity`/`exit_ip`/`exit_country` is written; readers treat those fields as unknown once older than `classification_max_age` or when the timestamp is absent. A sampled HTTPS-capability check writes `https_ok` (`1`/`0`) with `https_checked_at_ms`, and `https_tunnel_scheme` (`http`, `https`, or a SOCKS scheme) after a successful check; these are unchanged when no check ran. An anonymity echo check that returned 200 and was compared for tampering writes `tampered` (`1` when the proxy modified the echo body or injected response headers, `0` when clean) with `tamper_checked_at_ms`; both are unchanged when no tamper check ran, and readers treat `tampered` as unknown once older than `classification_max_age` or when the timestamp is absent. Lease fields are `lease_token`, `lease_owner`, and `lease_expires_at_ms`. |
| `<ns>:tomb:<id>` | STRING | Eviction tombstone `"<strikes>:<blocked_until_ms>"` written when a candidate is evicted after `max_consecutive_failures` or discarded by `discard_failed_candidates`. Each strike blocks re-adding for `evicted_backoff_base * 2^(strikes-1)` capped at `evicted_backoff_max`; source refreshes skip (no SADD/HSET/ZADD) endpoints that are not current candidates while blocked, and never priority-promote them. The key expires `block + max(block, 2 * max(1, max_consecutive_failures) * failed_retest_interval)` after the eviction, so strike memory survives one re-add and re-eviction cycle and then disappears on its own. |
| `<ns>:pending` | ZSET | Candidates waiting to be tested. Score is due-at epoch milliseconds. |
| `<ns>:leased` | ZSET | Candidates currently leased across all replicas. Score is lease-expiry epoch milliseconds. |
| `<ns>:validated` | SET | Candidate IDs whose `results10` history holds at least `min_samples_for_listing` outcomes (default 3) and whose rolling `ok_ratio_pct` is at least 80. Every completion, success or failure, re-evaluates that rule, so one lucky success does not list a candidate and one transient failure does not evict a stable one. Records without a ratio fall back to their latest outcome. |
| `<ns>:source-refresh-lock` | STRING | Short-TTL `SETNX` lease so exactly one replica refreshes inventory sources at a time; the owner renews it while active and losing it is normal. |
| `<ns>:source-stage:<token>` | ZSET or SET | Temporary staging keys for one source refresh, holding canonical endpoint strings. The shared refresh stage is a ZSET: score is the lowest source priority seen for the endpoint (`ZADD LT`), so it is both the cross-source dedupe index and the drain order. Each feed first stages into its own private SET (one `SADD` per record); only after the feed streamed completely is that SET `SPOP`-merged into the shared ZSET in bounded batches, so a feed that fails mid-stream contributes nothing. TTL 2 hours, re-armed on every staging batch, merge batch, and drain pass; every key is deleted when its refresh or feed finishes. |
| `<ns>:once:<name>` | STRING | Completion marker for a cluster-wide one-shot job; retained for the job's TTL so other replicas skip it. Permanent one-shot migrations (for example `pending-requeue-on-start-v2`) store it without expiry and persist an existing expiring marker. |
| `<ns>:once:<name>:lock` | STRING | Token-owned lock held while a one-shot job runs. Renewed every TTL/3 by the owner; if a renewal finds another token, the job's context is cancelled and no completion marker is written. |
| `<ns>:probe-budget:<YYYYMMDDHHMM>` | STRING counter | Global per-minute probe budget shared by every replica. Incremented and given a two-minute expiry atomically. Replicas reserve permits in chunks (`probe_permit_chunk`, default `max(10, workers/4)`) with a Lua `INCRBY` capped at `limit - used`, so the counter never exceeds the limit; permits a replica still holds when the minute ends are discarded, bounding waste to one chunk per replica per window. |
| `<ns>:stats-leader` | STRING | Random token of the replica that holds the stats-leader lease. Taken with `SET NX PX` semantics and renewed on every stats publish (TTL `stats_leader_ttl`, default 90s); released on graceful shutdown. Only the holder scans the validated set. |
| `<ns>:validated-aggregate` | STRING (JSON) | Identity-free validated-set aggregate written by the stats leader, only while its lease token matches: `computed_at_ms`, `by_country`, `stable_by_country`, `by_asn`, `by_anonymity`, `by_latency_band`, `stable`, `band_known`, `geo_mismatch`. TTL `stats_aggregate_ttl` (default 5m). Followers ignore it once older than two publish intervals (60s) and scan locally instead. |

## Atomicity

Claiming, completing, releasing, and reclaiming work run as single Lua scripts,
so two replicas can never own the same candidate:

- **Claim** removes one due candidate from `pending`, stamps its lease fields,
  and inserts it into `leased` in one step.
- **Complete** verifies the caller's lease token against the stored token and
  returns zero on mismatch, so a late worker whose lease expired cannot clobber
  a newer owner's result. Rescheduling is tiered: validated candidates return
  to `pending` after `validated_retest_interval` (default 6h), failed ones after
  `failed_retest_interval` (default 48h). Two refinements apply: a success
  that does not yet meet the listing rule is re-probed after
  `sample_retest_interval` (default 60s), and a failure that leaves the
  candidate listed is re-probed after `listed_failure_retest_interval`
  (default 5m) so a dead listed proxy is delisted quickly. A candidate that fails
  `max_consecutive_failures` times in a row (default 10) is evicted — its hash
  is deleted and it is removed from every index — instead of consuming probe
  budget forever. A success resets the streak. When the explicit
  `discard_failed_candidates` burst setting is enabled, a failed completion is
  removed from the hash and every index immediately; this is intended for a
  bounded initial sweep, not steady-state retesting. Both paths verify the
  lease token before deleting state, and the setting defaults to false.
  After either deletion the store writes the `<ns>:tomb:<id>` backoff
  tombstone in a follow-up script. Retest due times are spread by a uniform
  ±`retest_jitter_pct` (default 10) so candidates checked together do not
  come due together.
- **Reclaim** moves only candidates whose recorded expiry has passed back to
  `pending`.
- **Release** lets a shutting-down worker hand an untested claim straight back
  to `pending` instead of letting the lease TTL lapse.

## Compatibility and versioning policy

- The schema version lives in the namespace value itself (`freeproxyapi:v1`
  today). A breaking change to any key layout or record field ships under a new
  namespace (for example `freeproxyapi:v2`). Old and new replicas then operate on
  disjoint key sets instead of corrupting each other during a rollout.
- Consumers must treat hash fields they do not recognize as opaque: additive
  fields are non-breaking.
- Scores are epoch milliseconds in UTC; timestamps in records are also epoch
  milliseconds.
- Operators can flush an audit dataset by deleting everything under the current
  namespace prefix; no other Redis state is owned by this tool.
