# Responsible use

FreeProxyAPI aggregates proxy endpoints that third parties have already
published in public lists, tests whether they currently work, and serves the
results. It helps developers and researchers measure the reliability, anonymity
and integrity of publicly listed proxies.

## What it does and does not do

- It **does not scan** networks or discover hosts. It only tests endpoints that
  appear in the feeds you configure.
- It refuses private, loopback, link-local and other non-public targets, and
  re-checks the address it actually connects to.
- Validation is **off by default** and bounded by a global request budget,
  per-proxy retest intervals, and an eviction backoff for dead endpoints.
- Results are availability data at the moment of the last check, not a
  guarantee. Many listed proxies stop working within minutes, and fields such as
  `anonymity`, `exit_country` and `tampered` come from a plain-HTTP echo that
  the proxy itself can influence, so treat them as hints.

## If you operate an instance

- **Feeds.** Use lists you are permitted to fetch. Some providers restrict
  automated access or redistribution; honor their terms, keep refresh intervals
  reasonable, and drop a source if its owner asks. The repository ships only
  sample feeds.
- **Probe target.** Use an endpoint you operate, or one whose owner has
  approved the traffic. Do not point high-volume validation at a shared public
  service. Start with a conservative `global_requests_per_minute`.
- **Cloud providers.** Large-scale proxy validation can trigger abuse systems or
  be treated as scanning or DDoS-like traffic. Consult your provider first,
  follow its policies, and obtain approval where required.
- **GeoIP data.** Bring your own GeoLite2 license and keep MaxMind's attribution
  where you display derived data.
- **Exposure.** Keep the operator endpoints (`/metrics`, `/report`, the
  internal API) and any GOST router or admin interface on a private network;
  set `admin_listen_addr`. Do not run an unapproved public egress service.
- **Analytics and privacy.** The built-in pages load no third-party scripts
  unless you set `analytics_measurement_id`. If you turn it on, or publish the
  API, tell your visitors what you collect; see [privacy.md](privacy.md).

## If you use the data

Use returned endpoints only against systems you are authorized to access, and
follow applicable laws, terms of service and network policies. Do not use them
to attack systems, evade bans or rate limits, commit fraud, or scrape in
violation of a site's terms. See [ACCEPTABLE_USE.md](../ACCEPTABLE_USE.md).
Treat every returned proxy as untrusted: it can read or modify unencrypted
traffic, so never send credentials or sensitive data through one.

## Reporting

Report security issues as described in [SECURITY.md](../SECURITY.md). To ask
for an endpoint or source to be removed, or to report abuse of a hosted
instance, open an issue using the "Abuse report or removal request" template.
