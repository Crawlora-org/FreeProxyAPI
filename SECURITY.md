# Security policy

## Reporting a vulnerability

Please **do not open a public issue** for a suspected vulnerability or for an
exposed credential.

Report it privately with GitHub's **Report a vulnerability** button on this
repository's Security tab (private vulnerability reporting). Include:

- the affected version or commit,
- steps to reproduce,
- the impact you observed,
- any suggested mitigation.

Do not include credentials or real proxy endpoints in a report.

You can expect an acknowledgement within 5 business days. We will keep you
updated, work on a fix, and credit you in the release notes if you wish.

## Supported versions

Only the latest release and the `main` branch receive security fixes.

## Scope

In scope: the monitor and its HTTP API, the proxy router, the container image,
and the Kubernetes and Compose manifests in this repository.

Out of scope: the third-party proxy lists the software reads, the behaviour of
listed proxies themselves, and denial of service through volumetric traffic.
Misconfiguration of your own deployment is not a vulnerability, but reports of
unsafe defaults are welcome.
