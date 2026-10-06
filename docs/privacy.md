# Privacy

This page describes what the software processes and what the hosted service at
<https://freeproxyapi.crawlora.net/> does. If you run your own instance, you
are responsible for your own deployment; the notes below say what the software
does by default.

## What the software processes

- **Client IP address.** Used for per-client rate limiting of `/proxies` and to
  answer `/get`. It is kept in the server's memory only (IPv6 clients are
  grouped by /64), is not written to Redis, and is not logged by the
  application. The address comes from the socket peer, or from
  `CF-Connecting-IP` / `X-Forwarded-For` only when the peer is listed in
  `trusted_proxy_cidrs`.
- **`/get`.** Echoes the caller-visible IP and some request headers back to the
  caller. Nothing is stored.
- **Proxy data.** Endpoints published in third-party lists, their measured
  latency and success rate, and GeoIP-derived country and ASN. This is data
  about servers, not about the people who query the API.
- **No accounts.** There is no sign-up, login, cookie, or user profile.

## Analytics

The built-in homepage and dashboard load **no** third-party scripts unless the
operator sets `analytics_measurement_id`. When it is set, the pages show a
consent banner and load Google Analytics 4 **only if the visitor accepts**:

- Nothing is requested from Google, and no analytics cookies are set, before
  the visitor chooses. Declining is as easy as accepting.
- The choice is remembered in the browser's local storage on that device
  (`fpa-analytics-consent`). It is not sent anywhere.
- A browser's Global Privacy Control or Do Not Track signal is treated as
  "decline": no banner is shown and Google Analytics never loads.
- The "Analytics settings" link at the bottom left of each page reopens the
  banner. Declining after having accepted stops measurement and removes the
  Google Analytics cookies the page can see.
- When accepted, Google Analytics sends usage data to Google and sets cookies
  (`_ga`, `_ga_*`) under Google's own terms.

The hosted service at freeproxyapi.crawlora.net has analytics enabled and shows
this banner. The API endpoints (`/proxies`, `/stats`, `/get`) never run
analytics. Operators set `privacy_url` so the banner links to their own privacy
notice.

## Infrastructure

Traffic to the hosted service passes through Cloudflare, which processes
request metadata (including IP addresses) under its own terms and may keep edge
logs, and may inject its own Web Analytics beacon into pages if that is enabled on the hostname. Cloudflare describes that beacon as cookieless, and it is not covered by the consent banner. The application itself writes no per-request access log.

## Your choices

- Use the hosted API without the web pages: `/proxies`, `/stats` and `/get` run
  no analytics. On the pages, decline the banner or enable Global Privacy Control.
- Self-host: analytics are off by default, and you control retention.
- Questions or removal requests about data in the hosted service: open an issue
  with the "Abuse report or removal request" template.
