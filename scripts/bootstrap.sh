#!/usr/bin/env bash
# FreeProxyAPI cluster bootstrap.
#
# Idempotent one-command setup for a fresh (or existing) cluster/namespace:
#   1. namespace
#   2. internal-API token Secret (provided, reused from the cluster, or generated)
#   3. imagePullSecret check
#   4. kustomize apply of the operator overlay
#   5. rollout wait + smoke checks
#
# Prerequisites (see docs/new-cluster-checklist.md):
#   - kubectl pointed at the target cluster
#   - k8s/overlays/live-local/ prepared (monitor.json with your sources,
#     kustomization merging it over ../private-cluster)
#
# Usage:
#   ./scripts/bootstrap.sh
#   NAMESPACE=freeproxyapi ./scripts/bootstrap.sh
#
# Rerunning without INTERNAL_API_TOKEN keeps the token already stored in the
# freeproxyapi-internal-api Secret; set INTERNAL_API_TOKEN to rotate it.

set -euo pipefail

NAMESPACE="${NAMESPACE:-freeproxyapi}"
INTERNAL_API_TOKEN="${INTERNAL_API_TOKEN:-}"
OVERLAY_DIR="${OVERLAY_DIR:-k8s/overlays/live-local}"
IMAGE_PULL_SECRET="${IMAGE_PULL_SECRET:-ghcr-registry}"
TOKEN_SECRET="freeproxyapi-internal-api"
KUBECTL_TIMEOUT=(--request-timeout=120s)

log() { printf '[bootstrap] %s\n' "$*"; }
die() { printf '[bootstrap] ERROR: %s\n' "$*" >&2; exit 1; }

tmp_files=()
cleanup() {
  if ((${#tmp_files[@]})); then
    rm -f "${tmp_files[@]}"
  fi
}
trap cleanup EXIT
make_tmp() {
  local file
  file=$(mktemp "${TMPDIR:-/tmp}/freeproxyapi-bootstrap.XXXXXX")
  tmp_files+=("$file")
  printf '%s\n' "$file"
}

# retry wraps kubectl calls that talk to the cluster: cluster APIs - especially
# over spotty uplinks - routinely drop response bodies; three attempts is enough.
retry() {
  local attempt=1 out=""
  while ((attempt <= 3)); do
    if out=$(kubectl "${KUBECTL_TIMEOUT[@]}" "$@" 2>&1); then
      printf '%s\n' "$out"
      return 0
    fi
    printf '[bootstrap]   kubectl %s failed (attempt %d/3)\n' "$1" "$attempt" >&2
    attempt=$((attempt + 1))
    if ((attempt <= 3)); then
      sleep 20
    fi
  done
  printf '%s\n' "$out" >&2
  return 1
}
k() { retry "$@"; }

[[ -d "$OVERLAY_DIR" ]] || die "overlay directory not found: $OVERLAY_DIR (see docs/deployment.md)"
command -v kubectl > /dev/null || die "kubectl not found"

log "target namespace: $NAMESPACE"

# apply_staged renders a manifest with a client-side dry-run, then applies the
# rendered file under retry, so flaky API links cannot abort the bootstrap
# mid-flight.
apply_staged() {
  local desc="$1" tmp
  shift
  tmp=$(make_tmp)
  kubectl "${KUBECTL_TIMEOUT[@]}" "$@" --dry-run=client -o yaml > "$tmp" \
    || die "render failed: $desc"
  k apply -f "$tmp" > /dev/null || die "apply failed: $desc"
}

log "step 1/5: namespace"
apply_staged "namespace" create namespace "$NAMESPACE"

log "step 2/5: internal API token secret"
if [[ -n "$INTERNAL_API_TOKEN" ]]; then
  log "  using INTERNAL_API_TOKEN from the environment"
  write_token=true
else
  existing=$(k -n "$NAMESPACE" get secret "$TOKEN_SECRET" --ignore-not-found \
    -o jsonpath='{.data.token}') || die "unable to read existing $TOKEN_SECRET secret"
  if [[ -n "$existing" ]]; then
    INTERNAL_API_TOKEN=$(printf '%s' "$existing" | base64 --decode) \
      || die "existing $TOKEN_SECRET secret has an undecodable token"
    [[ -n "$INTERNAL_API_TOKEN" ]] || die "existing $TOKEN_SECRET secret has an empty token"
    log "  reusing token from existing $TOKEN_SECRET secret"
    write_token=false
  else
    command -v openssl > /dev/null || die "openssl not available and INTERNAL_API_TOKEN unset"
    INTERNAL_API_TOKEN=$(openssl rand -hex 24)
    log "  generated a new token (store it somewhere safe if you query the API)"
    write_token=true
  fi
fi
if [[ "$write_token" == true ]]; then
  tokfile=$(make_tmp)
  printf '%s' "$INTERNAL_API_TOKEN" > "$tokfile"
  apply_staged "internal api token secret" -n "$NAMESPACE" create secret generic "$TOKEN_SECRET" \
    --from-file=token="$tokfile"
fi

log "step 3/5: imagePullSecret check ($IMAGE_PULL_SECRET)"
pull_secret=$(k get secret "$IMAGE_PULL_SECRET" -n "$NAMESPACE" --ignore-not-found -o name) \
  || die "unable to check $IMAGE_PULL_SECRET secret"
if [[ -n "$pull_secret" ]]; then
  log "  found $IMAGE_PULL_SECRET in target namespace"
else
  log "  skipped: create $IMAGE_PULL_SECRET in $NAMESPACE before applying a private image"
fi

log "step 4/5: kustomize apply ($OVERLAY_DIR)"
apply_log=$(make_tmp)
if ! k apply -k "$OVERLAY_DIR" > "$apply_log" 2>&1; then
  cat "$apply_log"
  die "apply failed"
fi
tail -5 "$apply_log"

log "step 5/5: rollout + smoke checks"
k -n "$NAMESPACE" rollout status deploy/freeproxyapi --timeout=420s

# Only Running monitor pods ship wget and serve :8080; other pods with the
# same app name (e.g. cloudflared) or pending replicas are excluded.
pods=$(k -n "$NAMESPACE" get pods \
  -l 'app.kubernetes.io/name=freeproxyapi,app.kubernetes.io/component=monitor' \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[*].metadata.name}')
POD="${pods%% *}"
[[ -n "$POD" ]] || die "no Running monitor pod found in $NAMESPACE"
log "  smoke-checking pod $POD"

smoke() {
  local path="$1" expect="$2" desc="$3" got
  got=$(k -n "$NAMESPACE" exec "$POD" -- wget -qO- -T10 "http://127.0.0.1:8080$path" 2>/dev/null | head -c 2000) || true
  case "$got" in
    *"$expect"*) log "  ok: $desc" ;;
    *) die "smoke check failed for $desc (got: ${got:-<empty>})" ;;
  esac
}
smoke /readyz '"ready":true' "readiness"
smoke / '<title>FreeProxyAPI' "homepage"
smoke /report '"status":"ok"' "report payload"
smoke /stats '"status":"ok"' "public stats"
smoke '/proxies?limit=1' '"count"' "public proxy query"
k -n "$NAMESPACE" exec "$POD" -- wget -qO- -T10 http://127.0.0.1:8080/metrics > /dev/null \
  || die "smoke check failed for /metrics"
log "  ok: /metrics reachable"

# The token is piped over stdin so it never appears in process arguments.
# shellcheck disable=SC2016 # $(cat /tmp/tok) must expand inside the pod.
TOKEN_SMOKE=$(printf '%s' "$INTERNAL_API_TOKEN" | k -n "$NAMESPACE" exec -i "$POD" -- sh -c 'cat > /tmp/tok; wget -qO- -T10 --header="Authorization: Bearer $(cat /tmp/tok)" "http://127.0.0.1:8080/internal/api/v1/proxies?limit=1"; rm -f /tmp/tok' 2>/dev/null | head -c 120) || true
case "$TOKEN_SMOKE" in
  *'"count"'*) log "  ok: internal API authenticated query" ;;
  *) log "  note: internal API did not answer a tokenized query (disabled or empty validated set yet)" ;;
esac

log "bootstrap complete."
log "next: watch 'geo enriched' / 'validated requeue' lines in pod logs; apply k8s/overlays/monitoring (dashboard + alert rules, see docs/deployment.md) when ready."
