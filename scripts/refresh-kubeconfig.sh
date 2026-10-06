#!/usr/bin/env sh
set -eu

repo_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
exec python3 "$repo_root/terraform/refresh_kubeconfig.py" \
  --terraform-dir "$repo_root/terraform" "$@"
