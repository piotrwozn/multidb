import contextlib
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

import kubernetes_runtime as runtime


class RuntimeSafety(unittest.TestCase):
    def test_only_dedicated_names_and_private_context_are_used(self):
        for name in ("prod", "kind", "default", "multidb-qualification/../prod", "multidb-qualification-UPPER"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                runtime.validate_cluster_name(name)
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"KUBECONFIG": "/user/private-config", "DOCKER_HOST": "tcp://foreign:2375", "DOCKER_CONTEXT": "production"}):
            fixture = runtime.Fixture(Path(directory) / "new", Path(directory), "multidb-qualification")
            with patch.object(fixture, "tool", return_value="kubectl"):
                command = fixture.kubectl("get", "nodes")
            self.assertIn(str(fixture.kubeconfig), command)
            self.assertIn("kind-multidb-qualification", command)
            self.assertNotIn("/user/private-config", command)
            self.assertEqual(fixture.env["DOCKER_HOST"], "unix:///var/run/docker.sock")
            self.assertNotIn("DOCKER_CONTEXT", fixture.env)
            self.assertEqual(os.environ["DOCKER_HOST"], "tcp://foreign:2375")
            self.assertEqual(os.environ["KUBECONFIG"], "/user/private-config")

    def test_foreign_container_ids_block_cleanup_before_any_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
            fixture.private.mkdir()
            runtime.write_json(fixture.private / "owner.json", {"cluster": fixture.cluster, "uid": os.getuid(), "owned_container_ids": ["owned-node"]})
            with patch.object(fixture, "containers", return_value=["foreign-node"]), patch.object(fixture, "run") as run:
                with self.assertRaises(runtime.Blocked):
                    fixture.cleanup()
                run.assert_not_called()

    def test_existing_cluster_cannot_be_adopted_or_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory) / "fresh", Path(directory), "multidb-qualification")
            with patch.object(fixture, "tool", return_value="kind"), patch("kubernetes_runtime.shutil.disk_usage", return_value=Mock(free=10 * 1024**3)), \
                    patch("kubernetes_runtime.subprocess.check_output", return_value=b"multidb-qualification\n"), patch.object(fixture, "run") as run:
                with self.assertRaises(runtime.Blocked):
                    fixture.initialize()
                run.assert_not_called()
                self.assertFalse(fixture.directory.exists())

    def test_plan_stays_unrun_and_never_executes_tools(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()) as stream, \
                patch("kubernetes_runtime.subprocess.run") as run, patch("kubernetes_runtime.subprocess.check_output") as check:
            output = Path(directory) / "not-created"
            self.assertEqual(runtime.main(["--output", str(output), "--plan"]), 0)
            report = json.loads(stream.getvalue())
            self.assertEqual(report["status"], "unrun")
            self.assertEqual(report["physical_gpu"], "blocked")
            self.assertEqual(report["kindnet_policy_enforcement"], "blocked")
            self.assertFalse(output.exists())
            run.assert_not_called()
            check.assert_not_called()

    def test_client_server_skew_is_checked(self):
        versions = {"clientVersion": {"gitVersion": "v1.35.8"}, "serverVersion": {"gitVersion": "v1.35.8"}}
        runtime.supported_skew(versions)
        versions["clientVersion"]["gitVersion"] = "v1.37.2"
        with self.assertRaises(runtime.Blocked):
            runtime.supported_skew(versions)

    def test_readback_never_coerces_types_or_accepts_missing_rows(self):
        runtime.checked_rows([[1, "one"]], [[1, "one"]])
        for actual in ([], [[True, "one"]], [[1.0, "one"]], [[1, "wrong"]], [[1, "one"], [2, "two"]]):
            with self.subTest(actual=actual), self.assertRaises(AssertionError):
                runtime.checked_rows(actual, [[1, "one"]])

    def test_runtime_values_keep_explicit_volatile_and_real_secret_contracts(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
            fixture.private.mkdir()
            fixture.public.mkdir()
            with patch.object(fixture, "verify_runtime_owner"):
                matrix = runtime.RuntimeMatrix(fixture, Path(directory), Path(directory) / "not-built", "source-sha")
            volatile = matrix.values("in_memory", "image", "sha256:" + "1" * 64, "tls")
            self.assertFalse(volatile["persistence"]["enabled"])
            self.assertTrue(volatile["persistence"]["devEmptyDir"])
            self.assertTrue(volatile["runtime"]["allowVolatile"])
            self.assertEqual(volatile["audit"]["secretName"], "qualification-audit")
            encrypted = matrix.values("transactional", "image", "sha256:" + "1" * 64, "tls", encrypted=True)
            self.assertTrue(encrypted["persistence"]["enabled"])
            self.assertTrue(encrypted["encryption"]["enabled"])
            self.assertEqual(encrypted["image"]["pullPolicy"], "Never")

    def test_missing_final_artifact_is_blocked_with_all_profiles_unrun(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
            fixture.private.mkdir()
            fixture.public.mkdir()
            with patch.object(fixture, "verify_runtime_owner"):
                matrix = runtime.RuntimeMatrix(fixture, Path(directory), Path(directory) / "not-built", "source-sha")
            with patch.object(fixture, "verify_runtime_owner"), patch.object(matrix, "build_image") as build:
                result = matrix.execute(Path(directory), Path(directory) / "no-assets")
            self.assertEqual(result["status"], "blocked")
            self.assertFalse(result["production_deployment_qualified"])
            self.assertTrue(all(value["status"] == "unrun" for value in result["profiles"].values()))
            build.assert_not_called()
            self.assertTrue((matrix.public / "runtime.json").is_file())

    def test_containerd_import_selects_qualified_platform_and_checks_manifest(self):
        for actual_digest in ("sha256:" + "1" * 64, "sha256:" + "2" * 64):
            with self.subTest(actual_digest=actual_digest), tempfile.TemporaryDirectory() as directory:
                fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
                fixture.private.mkdir()
                fixture.public.mkdir()
                with patch.object(fixture, "verify_runtime_owner"):
                    matrix = runtime.RuntimeMatrix(fixture, Path(directory), Path(directory) / "binary", "source")
                evidence = {"reference": "qualified:tag", "manifest_digest": "sha256:" + "1" * 64}
                calls = []
                def run(name, command, **_kwargs):
                    calls.append((name, command))
                    if name == "image-imported-list":
                        return "docker.io/library/qualified:tag type " + actual_digest
                    if name == "image-pinned-list":
                        return "docker.io/library/qualified@" + evidence["manifest_digest"] + " type " + actual_digest
                    return "ok"
                with patch.object(fixture, "verify_runtime_owner", return_value={"owned_container_ids": ["owned"]}), \
                        patch.object(matrix, "run", side_effect=run):
                    if actual_digest == evidence["manifest_digest"]:
                        self.assertEqual(matrix.import_image(Path(directory) / "verified.oci.tar", evidence), "docker.io/library/qualified")
                    else:
                        with self.assertRaises(AssertionError):
                            matrix.import_image(Path(directory) / "verified.oci.tar", evidence)
                        self.assertNotIn("image-pin", [name for name, _command in calls])
                command = next(command for name, command in calls if name == "image-import")
                self.assertEqual(command[command.index("--platform") + 1], "linux/amd64")
                self.assertEqual(command[command.index("--snapshotter") + 1], "native")
                self.assertIn("--local", command)

    def test_stale_clean_exit_cannot_satisfy_the_new_sigterm(self):
        def pod(count, running, ended, code=0, uid="one"):
            return {"metadata": {"uid": uid}, "status": {"containerStatuses": [{"name": "multidb", "ready": True,
                "restartCount": count, "containerID": running, "lastState": {"terminated": {"exitCode": code, "containerID": ended}}}]}}
        before = pod(4, "containerd://current", "containerd://previous")
        self.assertFalse(runtime.observed_clean_restart(before, before))
        self.assertTrue(runtime.observed_clean_restart(before, pod(5, "containerd://new", "containerd://current")))
        for after in (pod(5, "containerd://new", "containerd://previous"), pod(5, "containerd://new", "containerd://current", 137),
                      pod(5, "containerd://new", "containerd://current", uid="different")):
            with self.assertRaises(AssertionError):
                runtime.observed_clean_restart(before, after)

    def test_runtime_constructor_refuses_unowned_output_before_mkdir(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
            with patch.object(fixture, "verify_runtime_owner", side_effect=runtime.Blocked("foreign")):
                with self.assertRaises(runtime.Blocked):
                    runtime.RuntimeMatrix(fixture, Path(directory), Path(directory) / "binary", "source")
            self.assertFalse(fixture.private.exists())
            self.assertFalse(fixture.public.exists())

    def test_partial_infrastructure_record_cannot_authorize_runtime(self):
        with tempfile.TemporaryDirectory() as directory:
            fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
            fixture.directory.chmod(0o700)
            fixture.private.mkdir(mode=0o700)
            fixture.kubeconfig.write_text("owned fixture config")
            fixture.kubeconfig.chmod(0o600)
            runtime.write_json(fixture.private / "owner.json", {"status": "failed"})
            (fixture.private / "owner.json").chmod(0o600)
            with patch.object(fixture, "verify_owner", return_value={"status": "failed"}):
                with self.assertRaises(runtime.Blocked):
                    fixture.verify_runtime_owner()

    def test_encryption_needs_actual_wrong_key_crypto_failure_and_restored_readback(self):
        for outcome in ("accepted", "ready", "unrelated"):
            with self.subTest(outcome=outcome), tempfile.TemporaryDirectory() as directory:
                fixture = runtime.Fixture(Path(directory), Path(directory), "multidb-qualification")
                fixture.private.mkdir()
                fixture.public.mkdir()
                with patch.object(fixture, "verify_runtime_owner"):
                    matrix = runtime.RuntimeMatrix(fixture, Path(directory), Path(directory) / "binary", "source")
                case = matrix.private / "case"
                case.mkdir()
                original = case / "original.json"
                runtime.write_json(original, {"encryption": {"secretName": "correct"}})
                pod = {"metadata": {"uid": "bad-key-pod", "name": "pod"}, "spec": {"volumes": [
                    {"name": "encryption", "secret": {"secretName": "q-enc-wrong-key"}}]},
                    "status": {"containerStatuses": [{"name": "multidb", "ready": outcome == "ready", "restartCount": 1,
                        "lastState": {"terminated": {"exitCode": 1, "containerID": "containerd://failed"}}}]}}
                calls = []
                def run(name, *_args, **_kwargs):
                    calls.append(name)
                    if name == "wrong-key-pods":
                        return json.dumps({"items": [pod]})
                    if name == "wrong-key-startup":
                        return "encrypted value authentication failed" if outcome == "accepted" else "unrelated startup failure"
                    return "ok"
                @contextlib.contextmanager
                def forward(*_args):
                    yield 1234
                with patch.object(fixture, "tool", return_value="fixture-tool"), \
                        patch.object(matrix, "run", side_effect=run), patch.object(matrix, "forward", side_effect=forward), \
                        patch("kubernetes_runtime.HttpApi.rows", return_value=[[1, "one"]]):
                    if outcome == "accepted":
                        result = matrix.wrong_key_reopen("q-enc", "service", original, case, [[1, "one"]])
                        self.assertEqual(result["status"], "accepted")
                    else:
                        with self.assertRaises(AssertionError):
                            matrix.wrong_key_reopen("q-enc", "service", original, case, [[1, "one"]])
                self.assertIn("correct-key-restore", calls)


if __name__ == "__main__":
    unittest.main()
