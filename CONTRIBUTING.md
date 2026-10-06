# Contributing

Thanks for helping. Bug reports, fixes, docs, and new tests are all welcome.

## Before you start

- For anything beyond a small fix, open an issue first so we can agree on the
  approach.
- Report vulnerabilities privately, as described in [SECURITY.md](SECURITY.md).
- Read [docs/responsible-use.md](docs/responsible-use.md). Changes that add
  defaults pointing at third-party services, raise default request budgets, or
  add proxy-discovery behavior will not be accepted.

## Development setup

You need Go 1.27.1 or newer (`go.mod` sets the toolchain; older Go versions
download it automatically). Redis is not required to run the tests.

```sh
make check        # gofmt check, go vet, go test, go test -race
make test         # go test ./...
```

CI additionally runs `staticcheck`, `govulncheck`, and renders the Kubernetes
manifests. To try the service locally, follow the Quick start in the
[README](README.md).

## Pull requests

- Keep a change focused, and include tests. Tests must be hermetic: no network,
  no real cluster, no credentials.
- Update documentation and [docs/redis-schema.md](docs/redis-schema.md) when you
  change behavior, configuration, or the stored schema.
- Run `make check` before pushing. Match the style of the surrounding code.
- Write commit messages that explain *why*. Squash-merge is used, so the PR
  title and description become the commit message.
- By contributing you agree your work is licensed under the project's
  [MIT License](LICENSE).

## Adding or changing proxy feeds

The repository ships only sample feeds. If you propose a feed for an example
config, check that its terms allow automated fetching, keep refresh intervals
polite, and prefer a list that publishes its own license. To ask for a source or
endpoint to be removed, open an issue with the "Abuse report or removal request"
template.

## Code of conduct

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
