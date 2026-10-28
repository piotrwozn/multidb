import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

import competition
import domain_common


class CompletionAndProvenance(unittest.TestCase):
    def test_inspect_allowlist_excludes_environment_mounts_and_unknown_credentials(self):
        inspected = {
            "Id": "abc", "Image": "sha256:test", "Name": "test",
            "Config": {"Image": "pinned-image", "Env": ["TOKEN=secret"], "Labels": {"password": "secret"}},
            "State": {"Pid": 123, "Status": "running", "OOMKilled": False},
            "Mounts": [{"Source": "/credentials/secret"}],
            "HostConfig": {"Memory": 4, "CpusetCpus": "0,1", "Binds": ["/secret:/secret"]},
            "NetworkSettings": {"Ports": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "15432"}]}},
        }
        sanitized = competition.sanitized_inspect(inspected)
        self.assertNotIn("secret", json.dumps(sanitized))
        self.assertEqual(sanitized["State"]["Pid"], 123)
        self.assertEqual(sanitized["HostConfig"]["CpusetCpus"], "0,1")

    def test_completion_requires_clean_exit_and_unchanged_original_and_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            binary, snapshot = output / "original", output / "copy"
            binary.write_bytes(b"version1")
            snapshot.write_bytes(b"version1")
            digest = competition.sha256(binary)
            for code, forced in ((1, False), (0, True), (-15, False)):
                with self.assertRaises(RuntimeError):
                    competition.verify_completion(output, binary, snapshot, digest, code, {"forced_kill": forced})
                self.assertFalse((output / "completion.json").exists())
            binary.write_bytes(b"version2")
            with self.assertRaises(RuntimeError):
                competition.verify_completion(output, binary, snapshot, digest, 0, {})
            binary.write_bytes(b"version1")
            snapshot.write_bytes(b"version2")
            with self.assertRaises(RuntimeError):
                competition.verify_completion(output, binary, snapshot, digest, 0, {})
            snapshot.write_bytes(b"version1")
            competition.verify_completion(output, binary, snapshot, digest, 0, {"forced_kill": False})
            self.assertEqual(json.loads((output / "completion.json").read_text())["status"], "passed")


class RecorderValidation(unittest.TestCase):
    def test_failed_validation_keeps_operation_latency_and_invalidates_section(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(domain_common, "require_idle"):
            recorder = domain_common.Recorder(directory)
            try:
                def invalid(_result, _index):
                    raise AssertionError("wrong result")
                with patch.object(domain_common.time, "perf_counter_ns", side_effect=[1_000_000, 3_000_000]):
                    with self.assertRaises(RuntimeError):
                        recorder.measure("engine", "work", 0, 1, lambda _: 42, invalid)
                summary = recorder.summaries[0]
                self.assertEqual(summary["correctness_status"], "failed")
                self.assertEqual(summary["successes"], 0)
                self.assertEqual(summary["errors"], 1)
                raw = (Path(directory) / "domain-samples.csv").read_text()
                self.assertIn(",2.0,False,", raw)
            finally:
                recorder.close()


class ServiceLifecycle(unittest.TestCase):
    def test_selected_ports_must_be_free_before_initialization(self):
        with patch.object(competition.socket, "socket") as constructor:
            competition.require_unused_ports(["sql", "analytics", "graph"], analytical=True)
            ports = [call.args[0][1] for call in constructor.return_value.__enter__.return_value.bind.call_args_list]
            self.assertEqual(ports, [15432, 15433, 15434, 17474, 18080, 18081, 27018])
            constructor.return_value.__enter__.return_value.bind.side_effect = OSError("port already in use")
            with self.assertRaisesRegex(OSError, "already in use"):
                competition.require_unused_ports(["kv"])

    def test_transient_startup_and_recovery_wait_is_outside_measurements(self):
        with patch.object(competition, "competitor_probe", side_effect=[OSError("loading"), None]) as probe, \
                patch.object(competition.time, "sleep") as sleep:
            competition.wait_competitor("redis")
            self.assertEqual(probe.call_count, 2)
            sleep.assert_called_once_with(.25)
        with patch.object(competition, "competitor_probe", side_effect=RuntimeError("not ready")), \
                patch.object(competition.time, "monotonic", side_effect=[0, 2]), \
                patch.object(competition.time, "sleep") as sleep:
            with self.assertRaises(TimeoutError):
                competition.wait_competitor("neo4j", seconds=1)
            sleep.assert_not_called()

    def test_comparators_are_removed_with_logs_before_next_phase(self):
        calls = []
        sampler = unittest.mock.Mock()
        def docker(arguments, **kwargs):
            calls.append(tuple(arguments[:2]))
            if arguments[0] == "inspect":
                return SimpleNamespace(stdout=json.dumps([{"State": {"Pid": 123}}]))
            return SimpleNamespace(stdout="")
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(competition, "start_competitor", side_effect=lambda engine, *_: [engine]), \
                patch.object(competition, "docker", side_effect=docker), \
                patch.object(competition, "wait_port"), patch.object(competition, "wait_competitor"):
            services = competition.CompetitorServices(Path(directory), [0, 1], sampler)
            with services.phase(["postgres", "mongodb"]):
                self.assertEqual(set(services.containers), {"postgres", "mongodb"})
            self.assertEqual(services.containers, {})
            first_removals = len([call for call in calls if call[0] == "rm"])
            self.assertEqual(first_removals, 2)
            with services.phase(["redis"]):
                self.assertEqual(set(services.containers), {"redis"})
                self.assertEqual(len([call for call in calls if call[0] == "rm"]), 2)
            self.assertEqual(len([call for call in calls if call[0] == "rm"]), 3)
            self.assertTrue((Path(directory) / "postgres.log").exists())
            self.assertTrue((Path(directory) / "mongodb-inspect.json").exists())

    def test_failed_phase_still_removes_started_container(self):
        from types import SimpleNamespace
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(competition, "start_competitor", return_value=[]), \
                patch.object(competition, "docker", return_value=SimpleNamespace(stdout=json.dumps([{"State": {}}]))) as docker, \
                patch.object(competition, "wait_port"), patch.object(competition, "wait_competitor"):
            services = competition.CompetitorServices(Path(directory), [0, 1], unittest.mock.Mock())
            with self.assertRaisesRegex(RuntimeError, "query failure"):
                with services.phase(["qdrant"]):
                    raise RuntimeError("query failure")
            self.assertEqual(services.containers, {})
            self.assertTrue(any(call.args[0][:2] == ["rm", "-fv"] for call in docker.call_args_list))

    def test_false_verifier_result_does_not_publish_success(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(domain_common, "require_idle"):
            recorder = domain_common.Recorder(directory)
            try:
                with self.assertRaises(RuntimeError):
                    recorder.measure("engine", "work", 0, 1, lambda _: 42, lambda *_: False)
                self.assertEqual(recorder.summaries[0]["successes"], 0)
                self.assertEqual(recorder.summaries[0]["correctness_status"], "failed")
            finally:
                recorder.close()


if __name__ == "__main__":
    unittest.main()
