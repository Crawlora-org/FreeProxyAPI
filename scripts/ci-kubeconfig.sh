#!/usr/bin/env bash
# Create the namespace-scoped CI deployer identity and produce KUBE_CONFIG_B64.
#
# For a cluster ADMIN to run by hand on a workstation. It uses an admin
# kubeconfig, so it must not run in CI and is not for automated agents.
#
# What it does:
#   1. (optional) refreshes the Terraform-managed admin kubeconfig,
#   2. shows the target cluster and asks you to confirm it,
#   3. applies k8s/ci-deployer/rbac.yaml (ServiceAccount, Role, RoleBinding,
#      token Secret) into the namespace,
#   4. builds a kubeconfig that uses the ServiceAccount token,
#   5. checks that it can deploy but cannot read Secrets or touch the cluster,
#   6. stores it as the KUBE_CONFIG_B64 GitHub environment secret, or prints it
#      base64-encoded for you to pipe somewhere.
#
# It is idempotent: re-running reuses the same ServiceAccount token. See
# "Creating the CI deployer identity" in docs/deployment.md.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: scripts/ci-kubeconfig.sh (--set-secret | --print) [options]

Output (exactly one):
  --set-secret            Store the result as the KUBE_CONFIG_B64 secret with gh.
  --print                 Print the base64 value to stdout. Refused on a
                          terminal; pipe it, e.g.  ... --print | pbcopy

Options:
  --admin-kubeconfig PATH Admin kubeconfig to use (default: terraform/kubeconfig.yaml).
  --refresh               Run scripts/refresh-kubeconfig.sh first to mint a fresh
                          admin kubeconfig (needs the Rackspace Spot token; see
                          terraform/README.md). Only with the default path.
  --namespace NAME        Target namespace (default: freeproxyapi).
  --repo OWNER/REPO       Repository for --set-secret (default: gh's current repo).
  --env NAME              GitHub environment for --set-secret (default: production).
  --yes                   Do not ask for confirmation of the target cluster.
  -h, --help              Show this help.
EOF
}

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
log() { printf '%s\n' "$*" >&2; }

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
admin_kubeconfig="$repo_root/terraform/kubeconfig.yaml"
namespace=freeproxyapi
refresh=0 set_secret=0 print=0 assume_yes=0
gh_repo="" gh_env=production

while [ $# -gt 0 ]; do
  case "$1" in
    --set-secret) set_secret=1 ;;
    --print) print=1 ;;
    --admin-kubeconfig) [ $# -ge 2 ] || die "--admin-kubeconfig needs a path"; admin_kubeconfig=$2; custom_path=1; shift ;;
    --refresh) refresh=1 ;;
    --namespace) [ $# -ge 2 ] || die "--namespace needs a value"; namespace=$2; shift ;;
    --repo) [ $# -ge 2 ] || die "--repo needs OWNER/REPO"; gh_repo=$2; shift ;;
    --env) [ $# -ge 2 ] || die "--env needs a name"; gh_env=$2; shift ;;
    --yes) assume_yes=1 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
  shift
done

[ $((set_secret + print)) -eq 1 ] || { usage >&2; die "choose exactly one of --set-secret or --print"; }
[ "$refresh" -eq 0 ] || [ -z "${custom_path:-}" ] || die "--refresh only works with the default admin kubeconfig"
printf '%s' "$namespace" | grep -Eq '^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$' || die "invalid namespace: $namespace"
if [ "$print" -eq 1 ] && [ -t 1 ]; then
  die "refusing to print a credential to a terminal; pipe it (e.g. --print | pbcopy) or use --set-secret"
fi

command -v kubectl >/dev/null 2>&1 || die "kubectl is required"
command -v base64 >/dev/null 2>&1 || die "base64 is required"
if [ "$set_secret" -eq 1 ]; then command -v gh >/dev/null 2>&1 || die "gh is required for --set-secret"; fi

if [ "$refresh" -eq 1 ]; then
  log "Refreshing the Terraform-managed admin kubeconfig..."
  "$repo_root/scripts/refresh-kubeconfig.sh" >&2
fi
[ -s "$admin_kubeconfig" ] || die "admin kubeconfig not found: $admin_kubeconfig (try --refresh, or pass --admin-kubeconfig)"

# Only the chosen file is used: never merge in whatever $KUBECONFIG points at.
admin() { KUBECONFIG="$admin_kubeconfig" kubectl --request-timeout=30s "$@"; }

b64d() {
  local in
  in=$(cat)
  printf '%s' "$in" | base64 --decode 2>/dev/null || printf '%s' "$in" | base64 -D
}

server=$(admin config view --minify -o jsonpath='{.clusters[0].cluster.server}')
context=$(admin config current-context)
[ -n "$server" ] || die "could not read the API server from $admin_kubeconfig"

log "Target cluster : $server"
log "Context        : $context"
log "Namespace      : $namespace"
log "Admin config   : $admin_kubeconfig"
if [ "$assume_yes" -ne 1 ]; then
  [ -t 0 ] || die "no terminal to confirm on; re-run with --yes once you have checked the target above"
  printf 'Create the CI deployer identity on this cluster? [y/N] ' >&2
  read -r answer
  case "$answer" in y|Y|yes|YES) ;; *) die "aborted" ;; esac
fi

admin get namespace "$namespace" >/dev/null 2>&1 || {
  log "Creating namespace $namespace..."
  admin create namespace "$namespace" >/dev/null
}

log "Applying k8s/ci-deployer/rbac.yaml..."
sed "s/namespace: freeproxyapi/namespace: $namespace/" "$repo_root/k8s/ci-deployer/rbac.yaml" | admin apply -f - >&2

log "Waiting for the ServiceAccount token..."
token_b64=""
for _ in $(seq 1 30); do
  token_b64=$(admin -n "$namespace" get secret ci-deployer-token -o jsonpath='{.data.token}' 2>/dev/null || true)
  [ -n "$token_b64" ] && break
  sleep 1
done
[ -n "$token_b64" ] || die "the ci-deployer-token Secret was not populated; check the controller and retry"

umask 077
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

# Prefer the CA the admin kubeconfig already trusts for this endpoint; fall back
# to the CA the cluster published in the token Secret.
ca=$(admin config view --raw --minify -o jsonpath='{.clusters[0].cluster.certificate-authority-data}' 2>/dev/null || true)
[ -n "$ca" ] || ca=$(admin -n "$namespace" get secret ci-deployer-token -o jsonpath='{.data.ca\.crt}')
[ -n "$ca" ] || die "could not determine the cluster CA"
token=$(printf '%s' "$token_b64" | b64d)

ci_kubeconfig="$tmp/ci-kubeconfig"
cat >"$ci_kubeconfig" <<EOF
apiVersion: v1
kind: Config
clusters: [{name: c, cluster: {server: $server, certificate-authority-data: $ca}}]
users: [{name: ci-deployer, user: {token: $token}}]
contexts: [{name: ci, context: {cluster: c, user: ci-deployer, namespace: $namespace}}]
current-context: ci
EOF
unset token token_b64

ci() { KUBECONFIG="$ci_kubeconfig" kubectl --request-timeout=30s "$@"; }
check() { # check EXPECTED -- kubectl auth can-i args...
  local want=$1 got
  shift 2
  got=$(ci auth can-i "$@" 2>/dev/null || true)
  [ "$got" = "$want" ] || die "scope check failed: 'auth can-i $*' answered '${got:-<nothing>}', expected '$want'"
}
log "Checking the new credential's scope..."
check yes -- patch deployments -n "$namespace"
check yes -- create pods/exec -n "$namespace"
check no -- get secrets -n "$namespace"
check no -- list nodes
check no -- create clusterrolebindings
log "Scope OK: can deploy in $namespace, cannot read Secrets or change the cluster."

value=$(base64 <"$ci_kubeconfig" | tr -d '\n')

if [ "$set_secret" -eq 1 ]; then
  args=(secret set KUBE_CONFIG_B64 --env "$gh_env")
  [ -z "$gh_repo" ] || args+=(--repo "$gh_repo")
  printf '%s' "$value" | gh "${args[@]}"
  log "Stored KUBE_CONFIG_B64 on the '$gh_env' environment. Nothing was written to disk."
else
  printf '%s\n' "$value"
fi
