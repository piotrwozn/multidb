import copy
import hashlib
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest
from unittest import mock

import install_kubernetes_tools as tools


def archive(entries):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz") as output:
        for name, data in entries:
            member = name if isinstance(name, tarfile.TarInfo) else tarfile.TarInfo(name)
            if not isinstance(name, tarfile.TarInfo):
                member.size = len(data)
            output.addfile(member, io.BytesIO(data) if data is not None else None)
    return buffer.getvalue()


def fixture_pin(data, member="tool", entries=None):
    payload = archive(entries if entries is not None else [(member, data)])
    pin = {"member": member, "allowed_members": (member, "LICENSE"),
           "download_sha256": hashlib.sha256(payload).hexdigest(), "binary_sha256": hashlib.sha256(data).hexdigest()}
    return payload, pin


class InstallerTests(unittest.TestCase):
    def test_only_known_verified_https_destinations_are_permitted(self):
        pin = tools.PINS["kubeconform"]
        tools.check_url(pin["url"], pin)
        tools.check_url("https://release-assets.githubusercontent.com/asset?signature=fixture", pin)
        for url in ("http://github.com/release", "file:///tmp/tool", "https://github.com.evil.invalid/x",
                    "https://github.com@evil.invalid/x", "https://github.com:444/x", "https://user:password@github.com/x"):
            with self.subTest(url=url), self.assertRaises(ValueError):
                tools.check_url(url, pin)
        redirect = tools.VerifiedRedirect(pin)
        request = tools.urllib.request.Request(pin["url"])
        with self.assertRaises(ValueError):
            redirect.redirect_request(request, None, 302, "redirect", {}, "http://github.com/plaintext")

    def test_checksum_is_verified_before_parsing_an_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "source", Path(directory) / "tool"
            source.write_bytes(b"not a tar")
            pin = {**tools.PINS["helm"], "download_sha256": "0" * 64}
            with mock.patch.object(tools.tarfile, "open") as parse:
                with self.assertRaisesRegex(ValueError, "archive checksum"):
                    tools.extract_binary(source, pin, output)
                parse.assert_not_called()
            self.assertFalse(output.exists())

    def test_only_the_allowlisted_regular_binary_is_copied_and_hashed(self):
        data = b"synthetic fixture bytes; never executed"
        payload, pin = fixture_pin(data, entries=[("LICENSE", b"fixture"), ("tool", data)])
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "source", Path(directory) / "tool"
            source.write_bytes(payload)
            tools.extract_binary(source, pin, output)
            self.assertEqual(output.read_bytes(), data)
            self.assertEqual(output.stat().st_mode & 0o777, 0o755)
            self.assertEqual(sorted(path.name for path in Path(directory).iterdir()), ["source", "tool"])

    def test_archive_links_duplicates_traversal_and_unlisted_members_are_rejected(self):
        link = tarfile.TarInfo("tool")
        link.type, link.linkname = tarfile.SYMTYPE, "../outside"
        cases = [[(link, None)], [("tool", b"a"), ("tool", b"b")],
                 [("../tool", b"a")], [("tool", b"a"), ("unexpected", b"b")]]
        for entries in cases:
            with self.subTest(entries=entries), tempfile.TemporaryDirectory() as directory:
                payload, pin = fixture_pin(b"a", entries=entries)
                source, output = Path(directory) / "source", Path(directory) / "tool"
                source.write_bytes(payload)
                with self.assertRaises(ValueError):
                    tools.extract_binary(source, pin, output)
                self.assertFalse(output.exists())

    def test_wrong_binary_digest_cannot_be_executed(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "helm"
            binary.write_bytes(b"wrong executable")
            with mock.patch.object(tools.subprocess, "run") as run:
                with self.assertRaisesRegex(ValueError, "checksum"):
                    tools.verify_version("helm", binary, tools.PINS["helm"])
                run.assert_not_called()

    def test_exact_reported_versions_are_required_even_with_a_matching_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            binary = Path(directory) / "tool"
            binary.write_bytes(b"version-fixture")
            for name in tools.PINS:
                pin = {**tools.PINS[name], "binary_sha256": tools.digest(binary)}
                output = '{"clientVersion":{"gitVersion":"v0.0.0"}}' if name == "kubectl" else "v0.0.0"
                with self.subTest(name=name), mock.patch.object(tools.subprocess, "run", return_value=subprocess.CompletedProcess([], 0, output, "")):
                    with self.assertRaises(ValueError):
                        tools.verify_version(name, binary, pin)

    def test_installation_and_reverification_never_use_network_or_path_fallback_after_download(self):
        # Synthetic fixture pins are scoped to this unit test. No executable is run.
        pins, payloads = copy.deepcopy(tools.PINS), {}
        for name, pin in pins.items():
            data = ("fixture-only:" + name).encode()
            if pin["member"]:
                payload = archive([(pin["member"], data)])
            else:
                payload = data
            pin["download_sha256"], pin["binary_sha256"] = hashlib.sha256(payload).hexdigest(), hashlib.sha256(data).hexdigest()
            payloads[pin["url"]] = payload
        def download(pin, destination):
            Path(destination).write_bytes(payloads[pin["url"]])
        def version(argv, **_kwargs):
            name = Path(argv[0]).name
            output = {"helm": "v4.3.1+gabcdef", "kubeconform": "v0.8.0", "kind": "kind v0.33.0 go1.test linux/amd64",
                      "kubectl": '{"clientVersion":{"gitVersion":"v1.35.8"}}'}[name]
            return subprocess.CompletedProcess(argv, 0, output, "")
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(tools, "PINS", pins), \
                mock.patch.object(tools, "download", side_effect=download), mock.patch.object(tools.subprocess, "run", side_effect=version), \
                mock.patch.object(tools.platform, "system", return_value="Linux"), mock.patch.object(tools.platform, "machine", return_value="x86_64"):
            output = Path(directory) / "new"
            report = tools.install(output, runtime=True)
            self.assertEqual(report["status"], "accepted")
            self.assertEqual({item["tool"] for item in report["tools"]}, set(pins))
            self.assertEqual(tools.verify_installed(output, runtime=True)["status"], "accepted")
            # Runtime resolves optional docker/openssl from this directory first;
            # an unpinned extra executable must never override the system tools.
            (output / "docker").write_bytes(b"unexpected executable")
            with self.assertRaisesRegex(ValueError, "unexpected"):
                tools.verify_installed(output, runtime=True)
            (output / "docker").unlink()
            with self.assertRaises(FileExistsError):
                tools.install(output, runtime=True)
            (output / "helm").write_bytes(b"changed")
            with self.assertRaisesRegex(ValueError, "checksum"):
                tools.verify_installed(output, runtime=True)

    def test_failed_install_has_failed_provenance_and_no_accepted_binary(self):
        with tempfile.TemporaryDirectory() as directory, mock.patch.object(tools, "download", side_effect=ValueError("checksum failed")), \
                mock.patch.object(tools.platform, "system", return_value="Linux"), mock.patch.object(tools.platform, "machine", return_value="x86_64"):
            output = Path(directory) / "new"
            with self.assertRaises(ValueError):
                tools.install(output)
            self.assertEqual(json.loads((output / "provenance.json").read_text())["status"], "failed")
            self.assertFalse((output / "helm").exists())


if __name__ == "__main__":
    unittest.main()
