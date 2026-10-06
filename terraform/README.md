# Regional Kubernetes infrastructure

This directory provisions a dedicated Rackspace Spot Kubernetes cloudspace for
FreeProxyAPI. It uses the Rackspace Spot provider and kubeconfig flow and keeps
its own state, so it cannot mutate any other cluster.

The default target is the DFW region (`us-central-dfw-1`) with four fixed
`mh.vs1.medium-dfw` workers. This class is used because the reference
deployment has previously used the DFW regional pattern; Spot may still fulfill
bids without successfully joining nodes, so the readiness checks are mandatory.
Change the variables deliberately if a different region is required.

## Provision

Set `TF_VAR_rackspace_spot_token` from a secret manager or copy
`terraform.tfvars.example` to the ignored `terraform.tfvars` and fill in the
token. Then run:

```sh
terraform init
terraform plan
terraform apply
```

Apply waits for the control plane and writes `terraform/kubeconfig.yaml` using
the Rackspace Spot kubeconfig API. The generated file is ignored and should not
be committed.

## Refresh expired Kubernetes credentials

The generated kubeconfig contains a short-lived Kubernetes credential. Refresh
it from the repository root with a token supplied by a secret manager or the
ignored `.env` file:

```sh
./scripts/refresh-kubeconfig.sh
```

The wrapper reads `TF_VAR_rackspace_spot_token` from `.env` when present and
passes it to Terraform through the child process environment (Terraform does
not read `.env` itself); explicit environment variables take precedence. Keep `.env` local and
`0600`, and load it from a secret manager rather than committing it.

The command refreshes Terraform state, asks Rackspace Spot to mint a new
kubeconfig, sets its namespace to `freeproxyapi`, verifies Kubernetes
`/readyz`, and replaces the old file only after all checks pass. It retries
transient API failures three times. The token is never printed. To reissue the
credential without a Terraform state refresh, use:

```sh
./scripts/refresh-kubeconfig.sh --skip-terraform-refresh
```

To check the existing credential without minting a new one:

```sh
./scripts/refresh-kubeconfig.sh --verify-only
```

If `/readyz` reports unauthorized, the existing file is stale or its token is
invalid; update `.env` from the secret manager and rerun the refresh. A failed
refresh leaves the previous kubeconfig untouched. Never commit
`terraform.tfvars`, `terraform/terraform.tfstate`, or the generated
`terraform/kubeconfig.yaml`.

## Terraform state

State is currently **local**: `terraform/terraform.tfstate` (plus
`terraform.tfstate.backup`) lives only on the operator machine that last ran
`apply`, and is gitignored. That carries real risks:

- **Single copy.** Losing the laptop or deleting the directory loses the
  mapping to the live cloudspace; recovery means `terraform import` or manual
  cleanup of orphaned Spot resources.
- **No locking.** Two operators (or a concurrent `scripts/refresh-kubeconfig.sh`
  run) can write state at the same time and corrupt it.
- **Secrets at rest.** State holds provider outputs such as the kubeconfig in
  plain text; it must stay `0600` and never be committed or shared in chat.

No backend is configured on purpose, because choosing one requires account and
access decisions. Recommended options, in order of simplicity:

1. **S3-compatible bucket with locking** (AWS S3, Cloudflare R2, MinIO) with
   versioning and server-side encryption enabled, and access limited to
   infrastructure operators.
2. **Terraform Cloud / HCP Terraform** (`cloud {}` block) for managed
   locking, encryption, and run history.

Example backend (not active; add it to `versions.tf`, then run
`terraform init -migrate-state` once and delete the local state copies after
verifying `terraform plan` shows no changes):

```hcl
# terraform {
#   backend "s3" {
#     bucket       = "freeproxyapi-tfstate"
#     key          = "rackspace-spot/dfw/terraform.tfstate"
#     region       = "us-east-1"
#     encrypt      = true
#     use_lockfile = true # S3-native locking (Terraform >= 1.10)
#     # For R2/MinIO also set endpoints = { s3 = "https://..." },
#     # skip_credentials_validation = true, use_path_style = true.
#   }
# }
```

Supply backend credentials through environment variables (for example
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`), never in committed files.

## Verify infrastructure

```sh
KUBECONFIG="$PWD/terraform/kubeconfig.yaml" kubectl get nodes -o wide
KUBECONFIG="$PWD/terraform/kubeconfig.yaml" kubectl get storageclass
KUBECONFIG="$PWD/terraform/kubeconfig.yaml" kubectl get --raw=/readyz
```

The FreeProxyAPI bootstrap script then creates the namespace and secrets,
applies the private operator overlay, waits for the application rollout, and
performs in-cluster health and authenticated API smoke checks:

```sh
KUBECONFIG="$PWD/terraform/kubeconfig.yaml" \
./scripts/bootstrap.sh
```
