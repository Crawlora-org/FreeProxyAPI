#!/usr/bin/env bash
# Break-glass production deploy for when the GitHub Actions deploy-live job
# cannot run. It mirrors that job: build origin/main for linux/amd64, publish a
# UTC timestamp tag, pin the rendered live-ci manifest to the pushed digest,
# validate it, apply it, wait for the rollouts, and verify the result.
#
# It always builds from a fresh detached worktree at origin/main, so local
# edits and unmerged branches can never ship. Without --yes it stops after the
# client-side dry run and applies nothing. See docs/manual-deploy.md for when
# this is allowed.
set -euo pipefail

IMAGE_NAME="ghcr.io/crawlora-org/freeproxyapi"
NAMESPACE="freeproxyapi"

usage() {
	cat <<'EOF'
Usage: scripts/deploy-live-manual.sh [--yes] [--with-monitoring] [--builder NAME] [--kubeconfig PATH]

  --yes              Publish the image and apply to production. Without it the
                     script builds and pushes nothing and stops after the dry run.
  --with-monitoring  Also apply k8s/overlays/monitoring (alerts and dashboard).
  --builder NAME     docker buildx builder that can build linux/amd64.
  --kubeconfig PATH  kubeconfig for the production cluster (default: $KUBECONFIG).
EOF
}

apply=0
with_monitoring=0
builder=""
kubeconfig="${KUBECONFIG:-}"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--yes) apply=1 ;;
	--with-monitoring) with_monitoring=1 ;;
	--builder)
		builder="${2:?--builder needs a value}"
		shift
		;;
	--kubeconfig)
		kubeconfig="${2:?--kubeconfig needs a value}"
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage >&2
		exit 2
		;;
	esac
	shift
done

fail() {
	echo "deploy-live-manual: $*" >&2
	exit 1
}

for tool in git go docker kubectl ruby perl; do
	command -v "$tool" >/dev/null 2>&1 || fail "$tool is required"
done
[[ -n "$kubeconfig" && -s "$kubeconfig" ]] || fail "set --kubeconfig or KUBECONFIG to the production kubeconfig"
kubeconfig="$(cd "$(dirname "$kubeconfig")" && pwd -P)/$(basename "$kubeconfig")"
export KUBECONFIG="$kubeconfig"

repo_root="$(git rev-parse --show-toplevel)"
workdir="$(mktemp -d)"
source_dir="$workdir/src"
cleanup() {
	git -C "$repo_root" worktree remove --force "$source_dir" >/dev/null 2>&1 || true
	rm -rf "$workdir"
}
trap cleanup EXIT

echo "== Checking out origin/main"
git -C "$repo_root" fetch --quiet origin main
sha="$(git -C "$repo_root" rev-parse origin/main)"
git -C "$repo_root" worktree add --quiet --detach "$source_dir" "$sha"
cd "$source_dir"
[[ -z "$(git status --porcelain --untracked-files=all)" ]] || fail "fresh worktree is not clean"
echo "commit: $(git log -1 --format='%h %s')"
echo "cluster: $(kubectl config current-context) ($(kubectl config view --minify -o jsonpath='{.clusters[0].cluster.server}'))"

echo "== Running checks"
go vet ./...
go test ./... -count=1

tag="$(date -u +'%Y%m%d%H%M%S')"
if [[ "$apply" -eq 1 ]]; then
	echo "== Building and pushing $IMAGE_NAME:$tag"
	docker buildx build ${builder:+--builder "$builder"} \
		--platform linux/amd64 \
		--provenance=true --sbom=true \
		--label "org.opencontainers.image.revision=$sha" \
		--tag "$IMAGE_NAME:$tag" --tag "$IMAGE_NAME:latest" \
		--metadata-file "$workdir/build.json" \
		--push .
	digest="$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV[0])).fetch("containerimage.digest")' "$workdir/build.json")"
else
	# A dry run validates the manifest against a placeholder digest; nothing is
	# built or pushed.
	digest="sha256:$(printf '0%.0s' $(seq 1 64))"
	echo "== Dry run: skipping build and push (placeholder digest)"
fi
[[ "$tag" =~ ^[0-9]{14}$ ]] || fail "unexpected image tag $tag"
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || fail "unexpected image digest $digest"
expected_image="$IMAGE_NAME@$digest"

echo "== Rendering and validating the live manifest"
TAG="$tag" perl -pi -e 's/^(    newTag: ")[^"]*(")$/$1$ENV{TAG}$2/' k8s/overlays/live-local/kustomization.yaml
grep -qE "^    newTag: \"$tag\"\$" k8s/overlays/live-local/kustomization.yaml || fail "live overlay tag update failed"
manifest="$workdir/freeproxyapi-live.yaml"
kubectl kustomize k8s/overlays/live-ci >"$manifest"
IMAGE="$IMAGE_NAME" DIGEST="$digest" perl -pi -e 's#(^\s*image: \Q$ENV{IMAGE}\E):\S+#$1\@$ENV{DIGEST}#' "$manifest"
EXPECTED_IMAGE="$expected_image" ruby -ryaml -e '
  documents = File.read(ARGV.fetch(0)).split(/^---\s*$/).map { |doc| YAML.safe_load(doc) }.compact
  allowed = %w[ConfigMap Service Deployment StatefulSet NetworkPolicy PodDisruptionBudget]
  abort "unexpected resource kind" unless documents.all? { |doc| allowed.include?(doc.fetch("kind")) }
  abort "resource is not namespaced" unless documents.all? { |doc| doc.dig("metadata", "namespace") == "freeproxyapi" }
  deployments = documents.select { |doc| doc["kind"] == "Deployment" }
  { "freeproxyapi" => "freeproxyapi", "gost-router" => "proxy-router" }.each do |name, container|
    deployment = deployments.find { |doc| doc.dig("metadata", "name") == name }
    abort "missing deployment #{name}" unless deployment
    image = deployment.fetch("spec").fetch("template").fetch("spec").fetch("containers").find { |item| item["name"] == container }.fetch("image")
    abort "unexpected image for #{name}: #{image}" unless image == ENV.fetch("EXPECTED_IMAGE")
  end
  puts "validated #{documents.size} objects"
' "$manifest"
kubectl apply --dry-run=client --validate=true -f "$manifest"
if [[ "$with_monitoring" -eq 1 ]]; then
	monitoring_manifest="$workdir/monitoring.yaml"
	kubectl kustomize k8s/overlays/monitoring >"$monitoring_manifest"
	kubectl apply --dry-run=client --validate=true -f "$monitoring_manifest"
fi

if [[ "$apply" -ne 1 ]]; then
	echo "== Dry run complete for $sha; rerun with --yes to deploy"
	exit 0
fi

echo "== Applying"
kubectl apply -f "$manifest"
if [[ "$with_monitoring" -eq 1 ]]; then
	kubectl apply -f "$monitoring_manifest"
fi

echo "== Waiting for rollouts"
kubectl -n "$NAMESPACE" rollout status statefulset/freeproxyapi-redis --timeout=5m
kubectl -n "$NAMESPACE" rollout status deployment/freeproxyapi --timeout=20m
kubectl -n "$NAMESPACE" rollout status deployment/gost-router --timeout=30m

echo "== Verifying"
monitor_image="$(kubectl -n "$NAMESPACE" get deployment/freeproxyapi -o jsonpath='{.spec.template.spec.containers[?(@.name=="freeproxyapi")].image}')"
router_image="$(kubectl -n "$NAMESPACE" get deployment/gost-router -o jsonpath='{.spec.template.spec.containers[?(@.name=="proxy-router")].image}')"
if [[ "$monitor_image" != "$expected_image" || "$router_image" != "$expected_image" ]]; then
	fail "expected both workloads on $expected_image; monitor=${monitor_image:-<none>} router=${router_image:-<none>}"
fi
kubectl get --raw "/api/v1/namespaces/$NAMESPACE/services/freeproxyapi:8080/proxy/readyz"
echo
echo "== Deployed $sha as $IMAGE_NAME:$tag ($digest)"
