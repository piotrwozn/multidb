import hashlib
import json
import os
import signal
import time
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import install_release_tools
import publish_qualified_image as publish


class QualifiedPublicationTests(unittest.TestCase):
    def test_registry_reference_is_exact_context_bound_and_metadata_is_oci_safe(self):
        self.assertEqual(publish.image_reference("Owner/Repository", "1.0.0+build.1", "v1.0.0+build.1"),
                         "ghcr.io/owner/repository:v1.0.0_build.1")
        self.assertEqual(publish.image_reference("owner/repository", "1.0.0-rc.1", "v1.0.0-rc.1"),
                         "ghcr.io/owner/repository:v1.0.0-rc.1")
        for repository, version, tag in (("../foreign", "1.0.0", "v1.0.0"),
                                         ("owner/repo", "1.0.0", "v1.0.1"),
                                         ("owner/repo", "1.0.0+" + "a" * 128, "v1.0.0+" + "a" * 128)):
            with self.subTest(repository=repository, tag=tag), self.assertRaises(ValueError):
                publish.image_reference(repository, version, tag)

    def test_copy_verifies_digest_before_tag_and_verifies_tag_after_promotion(self):
        raw = b'{"qualified":"raw bytes"}\n'
        expected = "sha256:" + hashlib.sha256(raw).hexdigest()
        calls = []
        def runner(command):
            calls.append(list(map(str, command)))
            return raw if command[1:3] == ["manifest", "fetch"] else b"copied"
        destination = "ghcr.io/owner/repo:v1.0.0"
        self.assertEqual(publish.copy_verified("oras", Path("/proof/image.tar"), expected, destination, runner), expected)
        self.assertEqual(calls[0], ["oras", "cp", "--from-oci-layout", "/proof/image.tar@" + expected, "ghcr.io/owner/repo@" + expected])
        self.assertEqual(calls[1], ["oras", "manifest", "fetch", "ghcr.io/owner/repo@" + expected])
        self.assertEqual(calls[2], ["oras", "tag", "ghcr.io/owner/repo@" + expected, "v1.0.0"])
        self.assertEqual(calls[3], ["oras", "manifest", "fetch", destination])

    def test_modified_remote_bytes_never_promote_a_release_tag(self):
        calls = []
        def runner(command):
            calls.append(command)
            return b"changed raw bytes"
        with self.assertRaises(ValueError):
            publish.copy_verified("oras", Path("image.tar"), "sha256:" + "1" * 64, "ghcr.io/owner/repo:v1.0.0", runner)
        self.assertFalse(any(command[1] == "tag" for command in calls))

    def test_remote_newline_is_part_of_qualified_digest(self):
        raw = b'{"qualified":true}\n'
        without_newline = "sha256:" + hashlib.sha256(raw.rstrip(b"\n")).hexdigest()
        with self.assertRaises(ValueError):
            publish.verify_remote("oras", "ghcr.io/owner/repo:v1.0.0", without_newline, lambda _command: raw)

    def test_tool_output_and_deadlines_are_bounded(self):
        with self.assertRaises(ValueError):
            publish.run([sys.executable, "-c", "print('x' * 10000)"], limit=16)
        with self.assertRaises(TimeoutError):
            publish.run([sys.executable, "-c", "import time; time.sleep(3)"], timeout=.05)
        self.assertEqual(publish.run([sys.executable, "-c", "print('ok', end='')"]), b"ok")

    def test_timeout_kills_owned_pipe_descendant_after_direct_parent_exits(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "descendant.pid"
            code = "import subprocess,sys; from pathlib import Path; child=subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)']); Path(" + repr(str(path)) + ").write_text(str(child.pid))"
            descendant = None
            try:
                with self.assertRaises(TimeoutError):
                    publish.run([sys.executable, "-c", code], timeout=.5)
                self.assertTrue(path.exists(), "the owned descendant fixture must actually start")
                descendant = int(path.read_text())
                deadline = time.monotonic() + 1
                while time.monotonic() < deadline:
                    try:
                        state = Path(f"/proc/{descendant}/stat").read_text().rsplit(")", 1)[1].split()[0]
                    except FileNotFoundError:
                        break
                    if state == "Z":
                        break
                    time.sleep(.01)
                else:
                    self.fail("owned descendant survived the command timeout")
            finally:
                if descendant is not None:
                    try:
                        os.kill(descendant, signal.SIGKILL)
                    except ProcessLookupError:
                        pass

    def test_blocked_full_qualification_never_executes_the_registry_tool(self):
        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            (repo / "Cargo.toml").write_text('[package]\nversion="1.0.0"\n')
            argv = ["--repo", str(repo), "--evidence-root", str(repo), "--manifest", str(repo / "manifest.json"),
                    "--binary", str(repo / "binary"), "--bundle", str(repo / "bundle"), "--oras", str(repo / "oras"),
                    "--output", str(repo / "receipt.json")]
            with patch.dict(os.environ, {"GITHUB_REPOSITORY": "owner/repo", "GITHUB_REF_NAME": "v1.0.0"}), \
                    patch.object(publish.readiness, "verify", return_value={"status": "blocked"}), \
                    patch.object(publish, "verify_oras") as verify_tool, patch.object(publish, "copy_verified") as copy:
                self.assertEqual(publish.main(argv), 1)
                verify_tool.assert_not_called()
                copy.assert_not_called()

    def test_modified_tool_is_refused_before_any_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "oras"
            path.write_bytes(b"not the official tool")
            with patch.object(install_release_tools.subprocess, "run") as execute:
                with self.assertRaises(ValueError):
                    install_release_tools.verify(path)
                execute.assert_not_called()

    def test_rc_full_publication_cannot_fall_through_to_a_stable_copy(self):
        with patch.dict(os.environ, {"GITHUB_REPOSITORY": "owner/repo", "GITHUB_REF_NAME": "v1.0.0-rc.1"}), \
                patch.object(publish.readiness, "verify") as verify, patch.object(publish, "copy_verified") as copy:
            self.assertEqual(publish.main([]), 1)
            verify.assert_not_called()
            copy.assert_not_called()


if __name__ == "__main__":
    unittest.main()
