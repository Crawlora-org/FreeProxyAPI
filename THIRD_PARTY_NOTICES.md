# Third-party notices

FreeProxyAPI is licensed under the [MIT License](LICENSE). The binaries and
container image also include the following third-party Go modules, each under
its own license. Full license texts are in the module sources
(`go mod download`, then each module's `LICENSE` file).

| Module | Version | License |
| --- | --- | --- |
| [github.com/redis/go-redis/v9](https://github.com/redis/go-redis) | v9.22.0 | BSD-2-Clause |
| [github.com/oschwald/geoip2-golang](https://github.com/oschwald/geoip2-golang) | v1.13.0 | ISC |
| [github.com/oschwald/maxminddb-golang](https://github.com/oschwald/maxminddb-golang) | v1.13.0 | ISC |
| [github.com/cespare/xxhash/v2](https://github.com/cespare/xxhash) | v2.3.0 | MIT |
| [go.uber.org/atomic](https://github.com/uber-go/atomic) | v1.11.0 | MIT |
| [golang.org/x/sys](https://pkg.go.dev/golang.org/x/sys) | v0.44.0 | BSD-3-Clause |

The Go standard library is licensed under a BSD-3-Clause license.

Test-only dependencies (`github.com/alicebob/miniredis/v2` and its
dependencies) are not part of the shipped binaries.

## GeoIP data

The software can read MaxMind GeoLite2 databases, but does not include them.
If you provide one, you must have your own MaxMind license and comply with its
terms, including attribution: *This product includes GeoLite2 Data created by
MaxMind, available from <https://www.maxmind.com>.*
