#!/usr/bin/env python3

import argparse
import json
import os
import pathlib
import re
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request


KUBECONFIG_API = "https://spot.rackspace.com/apis/auth.ngpc.rxt.io/v1/generate-kubeconfig"
DEFAULT_CLOUDSPACE = "freeproxyapi-dfw"
DEFAULT_ORGANIZATION = "tony"
RETRYABLE_HTTP_STATUS = {408, 429, 500, 502, 503, 504}


def read_tfvars(tfvars_path: pathlib.Path) -> dict[str, str]:
    """Read the simple string assignments used by terraform.tfvars."""

    values: dict[str, str] = {}
    pattern = re.compile(r'^([A-Za-z0-9_]+)\s*=\s*"([^"\\]*(?:\\.[^"\\]*)*)"\s*(?:#.*)?$')
    for raw_line in tfvars_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if match:
            # HCL string escapes: \" and \\ (and any other \x) mean the next character.
            values[match.group(1)] = re.sub(r"\\(.)", r"\1", match.group(2))
    return values


def read_env_file(env_path: pathlib.Path) -> dict[str, str]:
    """Read simple KEY=value entries without executing the dotenv file."""

    values: dict[str, str] = {}
    pattern = re.compile(r'^(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)$')
    for raw_line in env_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        match = pattern.match(line)
        if not match:
            continue
        values[match.group(1)] = parse_env_value(match.group(2))
    return values


def parse_env_value(raw: str) -> str:
    """Return a dotenv value without surrounding quotes or a trailing comment.

    A quoted value ends at its closing quote, so `KEY="a # b" # note` yields
    `a # b`. An unquoted value ends at the first ` #`, so `KEY=abc # note`
    yields `abc` while `KEY=a#b` keeps the `#`.
    """

    value = raw.strip()
    if value[:1] in {"'", '"'}:
        closing = value.find(value[0], 1)
        if closing != -1:
            return value[1:closing]
        return value
    return re.split(r"\s+#", value, maxsplit=1)[0].rstrip()


def generate_kubeconfig(
    organization_name: str,
    cloudspace_name: str,
    token: str,
    timeout: float,
    retries: int,
) -> str:
    body = json.dumps(
        {
            "organization_name": organization_name,
            "cloudspace_name": cloudspace_name,
            "refresh_token": token,
        }
    ).encode()
    request = urllib.request.Request(
        KUBECONFIG_API,
        data=body,
        headers={"Content-Type": "application/json"},
    )

    for attempt in range(retries + 1):
        delay = min(2**attempt, 8)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                payload = json.loads(response.read().decode())
            data = payload.get("data") if isinstance(payload, dict) else None
            kubeconfig = data.get("kubeconfig") if isinstance(data, dict) else None
            if not isinstance(kubeconfig, str) or not kubeconfig.strip():
                raise RuntimeError("Rackspace Spot returned an empty kubeconfig")
            return kubeconfig.lstrip("\n")
        except urllib.error.HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_STATUS or attempt == retries:
                raise RuntimeError(f"Rackspace Spot kubeconfig API returned HTTP {exc.code}") from exc
            server_delay = retry_after_seconds(exc)
            if server_delay is not None:
                delay = server_delay
        except (TimeoutError, OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            if attempt == retries:
                raise RuntimeError("could not retrieve kubeconfig from Rackspace Spot") from exc

        time.sleep(delay)

    raise RuntimeError("could not retrieve kubeconfig from Rackspace Spot")


RETRY_AFTER_STATUS = {429, 503}
MAX_RETRY_AFTER_SECONDS = 30.0


def retry_after_seconds(exc: urllib.error.HTTPError) -> float | None:
    """Return the server-requested wait for 429/503, capped, or None.

    Only the delta-seconds form of Retry-After is honored; an HTTP-date or a
    malformed value falls back to the normal backoff.
    """

    if exc.code not in RETRY_AFTER_STATUS or exc.headers is None:
        return None
    raw = (exc.headers.get("Retry-After") or "").strip()
    if not raw.isdigit():
        return None
    return min(float(raw), MAX_RETRY_AFTER_SECONDS)


def temporary_kubeconfig(target: pathlib.Path, content: str) -> pathlib.Path:
    target.parent.mkdir(parents=True, exist_ok=True)
    file_descriptor, temporary_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        dir=target.parent,
        text=True,
    )
    path = pathlib.Path(temporary_path)
    owns_file_descriptor = True
    try:
        os.fchmod(file_descriptor, 0o600)
        with os.fdopen(file_descriptor, "w") as temporary_file:
            owns_file_descriptor = False
            temporary_file.write(content)
            temporary_file.write("\n")
    except BaseException:
        if owns_file_descriptor:
            os.close(file_descriptor)
        path.unlink(missing_ok=True)
        raise
    return path


def run_kubectl(
    kubectl_cmd: str,
    kubeconfig: pathlib.Path,
    arguments: list[str],
    timeout: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            [kubectl_cmd, "--kubeconfig", str(kubeconfig), *arguments],
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"{kubectl_cmd} was not found on PATH") from exc
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError("kubectl timed out while verifying the kubeconfig") from exc


def set_namespace(kubectl_cmd: str, kubeconfig: pathlib.Path, namespace: str, timeout: float) -> None:
    result = run_kubectl(
        kubectl_cmd,
        kubeconfig,
        ["config", "set-context", "--current", "--namespace", namespace],
        timeout,
    )
    if result.returncode != 0:
        # kubectl config errors describe the file or context, never credentials.
        detail = (result.stderr or "").strip()[-300:]
        message = "kubectl could not set the kubeconfig namespace"
        raise RuntimeError(f"{message}: {detail}" if detail else message)


def verify_kubeconfig(kubectl_cmd: str, kubeconfig: pathlib.Path, timeout: float) -> None:
    result = run_kubectl(kubectl_cmd, kubeconfig, ["get", "--raw=/readyz"], timeout)
    if result.returncode != 0 or result.stdout.strip().lower() != "ok":
        raise RuntimeError("kubeconfig verification failed: Kubernetes /readyz was not reachable")


def refresh_terraform(terraform_cmd: str, terraform_dir: pathlib.Path) -> None:
    try:
        subprocess.run(
            [terraform_cmd, f"-chdir={terraform_dir}", "apply", "-refresh-only", "-auto-approve"],
            check=True,
        )
    except FileNotFoundError as exc:
        raise RuntimeError(f"{terraform_cmd} was not found on PATH") from exc
    except subprocess.CalledProcessError as exc:
        raise RuntimeError(f"Terraform refresh-only failed with exit code {exc.returncode}") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh and verify the regional Rackspace Spot kubeconfig.")
    parser.add_argument("--terraform-dir", default="terraform")
    parser.add_argument("--terraform-cmd", default="terraform")
    parser.add_argument("--kubectl-cmd", default="kubectl")
    parser.add_argument("--tfvars", default=None)
    parser.add_argument("--kubeconfig", default=None)
    parser.add_argument("--organization", default=None)
    parser.add_argument("--cloudspace-name", default=None)
    parser.add_argument("--namespace", default="freeproxyapi")
    parser.add_argument("--timeout", type=float, default=30.0, help="API and kubectl timeout in seconds (default: 30)")
    parser.add_argument("--retries", type=int, default=3, help="Additional API attempts after a transient failure (default: 3)")
    parser.add_argument(
        "--skip-terraform-refresh",
        action="store_true",
        help="Do not run terraform apply -refresh-only before minting the kubeconfig",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip the Kubernetes /readyz check (not recommended)",
    )
    parser.add_argument(
        "--verify-only",
        action="store_true",
        help="Verify the existing kubeconfig without contacting the Rackspace API",
    )
    args = parser.parse_args()
    if args.timeout <= 0:
        parser.error("--timeout must be greater than zero")
    if args.retries < 0:
        parser.error("--retries must not be negative")
    return args


def main() -> int:
    args = parse_args()
    terraform_dir = pathlib.Path(args.terraform_dir).resolve()
    tfvars_path = pathlib.Path(args.tfvars).resolve() if args.tfvars else terraform_dir / "terraform.tfvars"
    kubeconfig_path = pathlib.Path(args.kubeconfig).resolve() if args.kubeconfig else terraform_dir / "kubeconfig.yaml"

    try:
        if args.verify_only:
            if not kubeconfig_path.is_file():
                raise RuntimeError(f"kubeconfig does not exist: {kubeconfig_path}")
            verify_kubeconfig(args.kubectl_cmd, kubeconfig_path, args.timeout)
            print(f"verified {kubeconfig_path}")
            return 0

        values = read_tfvars(tfvars_path) if tfvars_path.exists() else {}
        env_path = terraform_dir.parent / ".env"
        env_values = read_env_file(env_path) if env_path.exists() else {}
        # The environment wins over .env, which wins over terraform.tfvars.
        # Report which one supplied the token (never its value) so a stale
        # TF_VAR in the shell is easy to spot.
        token, token_source = None, None
        for candidate, source in (
            (os.environ.get("TF_VAR_rackspace_spot_token"), "environment TF_VAR_rackspace_spot_token"),
            (env_values.get("TF_VAR_rackspace_spot_token"), f"{env_path.name} TF_VAR_rackspace_spot_token"),
            (values.get("rackspace_spot_token"), f"{tfvars_path.name} rackspace_spot_token"),
        ):
            if candidate:
                token, token_source = candidate, source
                break
        organization_name = (
            args.organization
            or os.environ.get("TF_VAR_rackspace_organization_name")
            or env_values.get("TF_VAR_rackspace_organization_name")
            or values.get("rackspace_organization_name")
            or DEFAULT_ORGANIZATION
        )
        cloudspace_name = (
            args.cloudspace_name
            or os.environ.get("TF_VAR_cloudspace_name")
            or env_values.get("TF_VAR_cloudspace_name")
            or values.get("cloudspace_name")
            or DEFAULT_CLOUDSPACE
        )
        if not token or token == "replace-me":
            raise RuntimeError(
                "Rackspace Spot token is missing; set TF_VAR_rackspace_spot_token or use a real terraform.tfvars value"
            )

        if not args.skip_terraform_refresh:
            refresh_terraform(args.terraform_cmd, terraform_dir)

        content = generate_kubeconfig(
            organization_name,
            cloudspace_name,
            token,
            args.timeout,
            args.retries,
        )
        candidate_path = temporary_kubeconfig(kubeconfig_path, content)
        try:
            set_namespace(args.kubectl_cmd, candidate_path, args.namespace, args.timeout)
            if not args.no_verify:
                verify_kubeconfig(args.kubectl_cmd, candidate_path, args.timeout)
            os.replace(candidate_path, kubeconfig_path)
        finally:
            candidate_path.unlink(missing_ok=True)

        status = "verified" if not args.no_verify else "not verified"
        print(f"wrote {kubeconfig_path} ({status}; token from {token_source})")
        return 0
    except (OSError, RuntimeError) as exc:
        print(f"kubeconfig refresh failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
