# Base images are pinned by multi-arch index digest; keep the tag for
# readability and bump tag and digest together
# (`docker buildx imagetools inspect <image:tag>`).
FROM golang:1.27.1-alpine@sha256:8a5910f31396cd4d89662f56c68b3ae31d374308270a1c3bd96672ee5ed43414 AS build

# Optional module-proxy override for restricted networks:
#   docker build --build-arg GOPROXY=https://goproxy.cn,direct .
ARG GOPROXY
ENV GOPROXY=${GOPROXY:-https://proxy.golang.org,direct}

WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/freeproxyapi ./cmd/freeproxyapi \
    && CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/proxy-router ./cmd/proxy-router

FROM alpine:3.24@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6
LABEL org.opencontainers.image.source="https://github.com/Crawlora-org/FreeProxyAPI" \
      org.opencontainers.image.licenses="MIT" \
      org.opencontainers.image.description="Collects public proxy lists, validates them, and serves the results over HTTP."
RUN addgroup -S relayaudit && adduser -S -G relayaudit relayaudit
COPY --from=build /out/freeproxyapi /usr/local/bin/freeproxyapi
COPY --from=build /out/proxy-router /usr/local/bin/proxy-router
COPY --from=build /src/LICENSE /src/THIRD_PARTY_NOTICES.md /licenses/
USER relayaudit
# Docker/Compose liveness only; Kubernetes ignores HEALTHCHECK and uses the
# probes in k8s/. One image serves both binaries, so the check is
# binary-aware: when PID 1 is `freeproxyapi monitor` it probes /livez on the
# default listen_addr (:8080) with busybox wget (also used by CI and
# scripts/bootstrap.sh, so keep it in the image); anything else, such as
# proxy-router started via an entrypoint override, has no HTTP listener and
# reports healthy. Override in Compose if listen_addr changes.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD case "$(tr '\0' ' ' </proc/1/cmdline)" in \
          *freeproxyapi\ monitor*) wget -q -T 4 -O /dev/null http://127.0.0.1:8080/livez ;; \
          *) exit 0 ;; \
        esac
ENTRYPOINT ["/usr/local/bin/freeproxyapi"]
CMD ["help"]
