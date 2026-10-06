"""Tests for refresh_kubeconfig.py. Run: python3 -m unittest discover -s terraform"""

import io
import json
import pathlib
import subprocess
import sys
import tempfile
import unittest
import urllib.error
from email.message import Message
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

import refresh_kubeconfig as rk  # noqa: E402


def http_error(code: int, retry_after: str | None = None) -> urllib.error.HTTPError:
    headers = Message()
    if retry_after is not None:
        headers["Retry-After"] = retry_after
    return urllib.error.HTTPError(rk.KUBECONFIG_API, code, "error", headers, io.BytesIO(b""))


class FakeResponse:
    def __init__(self, payload: dict):
        self._body = json.dumps(payload).encode()

    def read(self) -> bytes:
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class ReadTfvarsTest(unittest.TestCase):
    def read(self, text: str) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "terraform.tfvars"
            path.write_text(text)
            return rk.read_tfvars(path)

    def test_plain_values_and_comments(self):
        values = self.read('# header\nrackspace_spot_token = "abc123" # trailing\ncloudspace_name="dfw"\n')
        self.assertEqual(values, {"rackspace_spot_token": "abc123", "cloudspace_name": "dfw"})

    def test_escaped_quotes_and_backslashes_are_unescaped(self):
        values = self.read('rackspace_spot_token = "a\\"b\\\\c"\n')
        self.assertEqual(values["rackspace_spot_token"], 'a"b\\c')

    def test_non_string_assignments_are_ignored(self):
        self.assertEqual(self.read("workers = 12\n"), {})


class ReadEnvFileTest(unittest.TestCase):
    def read(self, text: str) -> dict[str, str]:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / ".env"
            path.write_text(text)
            return rk.read_env_file(path)

    def test_inline_comment_is_stripped_from_unquoted_value(self):
        self.assertEqual(self.read("TF_VAR_rackspace_spot_token=abc # note\n")["TF_VAR_rackspace_spot_token"], "abc")

    def test_hash_inside_unquoted_value_is_kept(self):
        self.assertEqual(self.read("TOKEN=a#b\n")["TOKEN"], "a#b")

    def test_quoted_values_keep_hashes_and_drop_trailing_comment(self):
        values = self.read('A="x # y" # note\nB=\'p q\'\nexport C="z"\n')
        self.assertEqual(values, {"A": "x # y", "B": "p q", "C": "z"})

    def test_comment_and_invalid_lines_are_skipped(self):
        self.assertEqual(self.read("# comment\n\nnot a pair\n1BAD=x\n"), {})


class RetryAfterTest(unittest.TestCase):
    def test_numeric_retry_after_on_429_and_503(self):
        self.assertEqual(rk.retry_after_seconds(http_error(429, "5")), 5.0)
        self.assertEqual(rk.retry_after_seconds(http_error(503, "2")), 2.0)

    def test_retry_after_is_capped(self):
        self.assertEqual(rk.retry_after_seconds(http_error(429, "600")), rk.MAX_RETRY_AFTER_SECONDS)

    def test_other_statuses_dates_and_missing_header_fall_back(self):
        self.assertIsNone(rk.retry_after_seconds(http_error(502, "5")))
        self.assertIsNone(rk.retry_after_seconds(http_error(429, "Wed, 21 Oct 2026 07:28:00 GMT")))
        self.assertIsNone(rk.retry_after_seconds(http_error(429)))


class GenerateKubeconfigTest(unittest.TestCase):
    def test_honors_retry_after_then_succeeds(self):
        responses = [http_error(429, "7"), FakeResponse({"data": {"kubeconfig": "\napiVersion: v1\n"}})]

        def fake_urlopen(request, timeout):
            item = responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with mock.patch.object(rk.urllib.request, "urlopen", side_effect=fake_urlopen), mock.patch.object(rk.time, "sleep") as sleep:
            result = rk.generate_kubeconfig("org", "space", "token", timeout=1, retries=2)
        self.assertEqual(result, "apiVersion: v1\n")
        sleep.assert_called_once_with(7.0)

    def test_uses_backoff_without_retry_after(self):
        responses = [http_error(502), http_error(502), FakeResponse({"data": {"kubeconfig": "ok"}})]

        def fake_urlopen(request, timeout):
            item = responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item

        with mock.patch.object(rk.urllib.request, "urlopen", side_effect=fake_urlopen), mock.patch.object(rk.time, "sleep") as sleep:
            rk.generate_kubeconfig("org", "space", "token", timeout=1, retries=3)
        self.assertEqual([c.args[0] for c in sleep.call_args_list], [1, 2])

    def test_non_retryable_status_raises_without_leaking_token(self):
        with mock.patch.object(rk.urllib.request, "urlopen", side_effect=http_error(401)), mock.patch.object(rk.time, "sleep") as sleep:
            with self.assertRaises(RuntimeError) as ctx:
                rk.generate_kubeconfig("org", "space", "secret-token", timeout=1, retries=3)
        self.assertIn("HTTP 401", str(ctx.exception))
        self.assertNotIn("secret-token", str(ctx.exception))
        sleep.assert_not_called()


class SetNamespaceTest(unittest.TestCase):
    def test_error_includes_kubectl_stderr(self):
        failed = subprocess.CompletedProcess(args=[], returncode=1, stdout="", stderr="error: current-context is not set\n")
        with mock.patch.object(rk, "run_kubectl", return_value=failed):
            with self.assertRaises(RuntimeError) as ctx:
                rk.set_namespace("kubectl", pathlib.Path("kubeconfig.yaml"), "freeproxyapi", 5)
        self.assertEqual(str(ctx.exception), "kubectl could not set the kubeconfig namespace: error: current-context is not set")

    def test_success_does_not_raise(self):
        ok = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        with mock.patch.object(rk, "run_kubectl", return_value=ok):
            rk.set_namespace("kubectl", pathlib.Path("kubeconfig.yaml"), "freeproxyapi", 5)


if __name__ == "__main__":
    unittest.main()


class RefreshTerraformTest(unittest.TestCase):
    def test_environment_carries_the_resolved_token_and_keeps_the_rest(self):
        env = rk.terraform_environment("secret-token", {"PATH": "/bin", "TF_VAR_rackspace_spot_token": "stale"})
        self.assertEqual(env["TF_VAR_rackspace_spot_token"], "secret-token")
        self.assertEqual(env["PATH"], "/bin")

    def test_token_reaches_terraform_only_through_the_environment(self):
        env = rk.terraform_environment("secret-token", {"PATH": "/bin"})
        with mock.patch.object(rk.subprocess, "run") as run:
            rk.refresh_terraform("terraform", pathlib.Path("/tmp/tf"), env)
        command = run.call_args.args[0]
        self.assertNotIn("secret-token", " ".join(command))
        self.assertIn("-input=false", command)
        self.assertEqual(run.call_args.kwargs["env"]["TF_VAR_rackspace_spot_token"], "secret-token")

    def test_failure_does_not_leak_the_token(self):
        error = subprocess.CalledProcessError(1, ["terraform"])
        with mock.patch.object(rk.subprocess, "run", side_effect=error):
            with self.assertRaises(RuntimeError) as ctx:
                rk.refresh_terraform("terraform", pathlib.Path("/tmp/tf"), {"TF_VAR_rackspace_spot_token": "secret-token"})
        self.assertNotIn("secret-token", str(ctx.exception))
