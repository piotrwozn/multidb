import contextlib
import copy
import io
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import external
import qualify


class DeclaredProfiles(unittest.TestCase):
    def test_profiles_are_explicit_and_invalid_slo_never_relaxes_to_green(self):
        smoke = qualify.load_profile("smoke")
        local = qualify.load_profile("local-15m")
        self.assertEqual((local["duration_seconds"], local["concurrency"], local["rows"], local["payload_bytes"]), (900, 8, 100000, 256))
        for key, invalid in (("duration_seconds", float("nan")), ("concurrency", True),
                             ("maximum_unexpected_errors", 1), ("payload_bytes", 12)):
            profile = {**smoke, key: invalid}
            with self.subTest(key=key), self.assertRaises(ValueError):
                qualify.validate_profile(profile)
        cold = qualify.load_profile("cold")
        cold["maximum_resident_fraction_after_eviction"] = 1
        with self.assertRaises(ValueError):
            qualify.validate_profile(cold)

    def test_larger_than_ram_and_disk_claims_require_observed_capacity(self):
        profile = qualify.load_profile("larger-than-ram")
        host = {"effective_cpus": 4, "effective_memory_bytes": 16 * 1024**3}
        with self.assertRaisesRegex(qualify.Blocked, "does not exceed"):
            qualify.preflight(profile, host, 1024**4)
        profile.update(rows=17 * 1024 * 1024, payload_bytes=1024, maximum_directory_bytes=200 * 1024**3)
        with self.assertRaisesRegex(qualify.Blocked, "insufficient disk"):
            qualify.preflight(profile, host, 4 * 1024**3)
        accepted = qualify.preflight(profile, host, 200 * 1024**3)
        self.assertTrue(accepted["larger_than_ram_verified"])

    def test_plan_does_not_launch_or_create_output(self):
        with tempfile.TemporaryDirectory() as directory, patch("qualify.subprocess.Popen") as launch, \
                contextlib.redirect_stdout(io.StringIO()) as output:
            destination = Path(directory) / "not-created"
            self.assertEqual(qualify.main(["--binary", sys.executable, "--revision", "source-sha",
                                          "--output", str(destination), "--plan"]), 0)
            plan = json.loads(output.getvalue())
            self.assertEqual(plan["status"], "unrun")
            self.assertFalse(plan["scope"]["production_release_approved"])
            self.assertFalse(destination.exists())
            launch.assert_not_called()

    def test_unrun_or_blocked_execution_cannot_be_published_as_accepted(self):
        for failure in (None, qualify.Blocked("no physical capacity")):
            with self.subTest(failure=failure), tempfile.TemporaryDirectory() as directory, \
                    patch("qualify.execute", side_effect=failure), patch("qualify.fcntl.flock"), \
                    contextlib.redirect_stdout(io.StringIO()):
                destination = Path(directory) / "evidence"
                self.assertEqual(qualify.main(["--binary", sys.executable, "--revision", "source-sha",
                                              "--output", str(destination)]), 2)
                report = json.loads((destination / "report.json").read_text())
                self.assertEqual(report["status"], "blocked" if failure else "unrun")
                self.assertTrue(report["evidence"])
                self.assertEqual(len(report["criteria_sha256"]), 64)


class OracleAndSlo(unittest.TestCase):
    def test_full_oracle_covers_every_source_and_acknowledged_version(self):
        profile = qualify.load_profile("smoke")
        profile["rows"] = 513
        versions = {0: 256, 255: 3, 512: 9}

        class Fixture:
            corrupt = None

            def rows(self, sql):
                if "COUNT(*)" in sql:
                    return [[profile["rows"]]]
                start, end = map(int, re.search(r"id>=(\d+) AND id<(\d+)", sql).groups())
                records = [[i, versions.get(i, 0), qualify.payload(i, versions.get(i, 0), profile["payload_bytes"])] for i in range(start, end)]
                if self.corrupt is not None and start <= self.corrupt < end:
                    records[self.corrupt - start][1] += 1
                return records

        api = Fixture()
        report = qualify.full_oracle(api, profile, versions)
        self.assertEqual(report["rows_checked"], 513)
        self.assertEqual(report["acknowledged_final_versions"], 3)
        api.corrupt = 512
        with self.assertRaisesRegex(AssertionError, "key512"):
            qualify.full_oracle(api, profile, versions)
        with self.assertRaises(AssertionError):
            qualify.verify_record([True, 0, qualify.payload(1, 0, 256)], 1, 0, 256)

    def test_denied_response_must_be_policy_403_without_data(self):
        api = Mock()
        good = {"ok": False, "error": {"code": "forbidden"}}
        api.request.return_value = 403, good
        qualify.denied(api)
        for status, response in ((200, {"ok": True, "data": []}), (401, good), (403, {**good, "data": {"payload": "secret"}})):
            api.request.return_value = status, response
            with self.subTest(status=status), self.assertRaises(AssertionError):
                qualify.denied(api)

    def test_slo_rejects_tail_errors_and_missing_operation_coverage(self):
        profile = qualify.load_profile("smoke")
        profile["minimum_successful_requests"] = 4
        with tempfile.TemporaryDirectory() as directory:
            measurements = qualify.Measurements(Path(directory))
            try:
                for index, kind in enumerate(qualify.KINDS):
                    measurements.record(0, index, kind, 1)
                self.assertEqual(measurements.result(profile)["status"], "accepted")
                measurements.record(0, 4, "point_read", 100000)
                self.assertEqual(measurements.result(profile)["status"], "failed")
                measurements.record(0, 5, "upsert", 1, AssertionError("ack lost"))
                self.assertEqual(measurements.result(profile)["unexpected_errors"], 1)
            finally:
                measurements.close()

    def test_resource_limits_are_hard_bounds(self):
        profile = qualify.load_profile("smoke")
        sample = {"rss_bytes": 1, "file_descriptors": 1, "directory_bytes": 1,
                  "free_disk_bytes": profile["minimum_free_disk_bytes"]}
        qualify.enforce_resources(sample, profile)
        for field, limit in (("rss_bytes", "maximum_rss_bytes"), ("file_descriptors", "maximum_file_descriptors"),
                             ("directory_bytes", "maximum_directory_bytes")):
            with self.subTest(field=field), self.assertRaises(AssertionError):
                qualify.enforce_resources({**sample, field: profile[limit] + 1}, profile)


class OwnedServer(unittest.TestCase):
    def test_pg_credentials_are_generated_owned_and_local_http_scope_is_explicit(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {
                "MULTIDB_PG_PASSWORD": "foreign deployment secret", "MULTIDB_PG_TLS_MODE": "required", "MULTIDB_SERVE_SPEC": "foreign-spec"}):
            root = Path(directory)
            server = qualify.Server(root / "binary", root / "db", root, qualify.load_profile("smoke"), 18180, 15533)
            environment = server.environment()
            self.assertEqual(environment["MULTIDB_PG_USER"], "qualification")
            self.assertEqual(environment["MULTIDB_RUNTIME_MODE"], "local-dev")
            self.assertEqual(environment["MULTIDB_PG_TLS_MODE"], "disabled")
            self.assertRegex(environment["MULTIDB_PG_PASSWORD"], r"^[0-9a-f]{64}$")
            self.assertNotEqual(environment["MULTIDB_PG_PASSWORD"], "foreign deployment secret")
            self.assertNotIn("MULTIDB_SERVE_SPEC", environment)
            self.assertEqual(environment["MULTIDB_PG_PASSWORD"], server.environment()["MULTIDB_PG_PASSWORD"])
            other = qualify.Server(root / "binary", root / "db", root, qualify.load_profile("smoke"), 18180, 15533)
            self.assertNotEqual(environment["MULTIDB_PG_PASSWORD"], other.environment()["MULTIDB_PG_PASSWORD"])

    def test_startup_failure_survives_nonzero_shutdown_and_records_secondary_facts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = qualify.Server(root / "binary", root / "db", root, qualify.load_profile("smoke"), 18180, 15533)
            process = Mock(returncode=1)
            process.poll.return_value = 1
            with patch("qualify.free_ports"), patch("qualify.subprocess.Popen", return_value=process):
                with self.assertRaisesRegex(RuntimeError, "exited at startup:1"):
                    server.start()
            self.assertIsNone(server.process)
            self.assertEqual(server.shutdowns[0]["status"], "failed")
            self.assertEqual(server.shutdowns[0]["returncode"], 1)
            self.assertIn("did not exit cleanly", server.shutdowns[0]["error"]["message"])

    def test_execute_preserves_primary_failure_when_final_teardown_also_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            binary = root / "original"
            binary.write_bytes(b"synthetic artifact")
            server = Mock(shutdowns=[{"status": "failed", "returncode": 1}])
            server.start.side_effect = RuntimeError("primary startup diagnostic")
            server.stop.side_effect = RuntimeError("secondary shutdown diagnostic")
            report = {"artifact": {"sha256": qualify.digest(binary)}, "sections": {}}
            args = qualify.argparse.Namespace(binary=binary, output=root, http_port=18180, pg_port=15533)
            with patch("qualify.require_idle"), patch("qualify.free_ports"), patch("qualify.Server", return_value=server):
                with self.assertRaisesRegex(RuntimeError, "primary startup diagnostic"):
                    qualify.execute(args, qualify.load_profile("smoke"), report)
            self.assertEqual(report["secondary_shutdown_failures"][0]["message"], "secondary shutdown diagnostic")
            self.assertEqual(report["shutdowns"][0]["returncode"], 1)

    def test_functional_calibration_never_claims_performance_or_production(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(qualify.main(["--binary", sys.executable, "--revision", "calibration-only",
                "--output", str(Path(directory) / "fresh"), "--plan", "--functional-calibration"]), 0)
            report = json.loads(output.getvalue())
            self.assertTrue(report["scope"]["functional_calibration"])
            self.assertFalse(report["scope"]["performance_qualified"])
            self.assertFalse(report["scope"]["production_release_approved"])
            self.assertFalse(report["scope"]["compiler_interference_guard_applied"])
        with patch("qualify.require_idle", side_effect=qualify.Blocked("active compiler")) as guard:
            qualify.require_scope_idle(True)
            guard.assert_not_called()
            with self.assertRaises(qualify.Blocked):
                qualify.require_scope_idle(False)
        with tempfile.TemporaryDirectory() as directory, self.assertRaisesRegex(ValueError, "smoke profile"):
            qualify.main(["--binary", sys.executable, "--revision", "calibration-only", "--output", str(Path(directory) / "fresh"),
                          "--plan", "--functional-calibration", "--profile", "local-15m"])


class OfflineResourceSampling(unittest.TestCase):
    def test_exiting_child_proc_permission_race_preserves_exit_status(self):
        for returncode in (0, 7):
            with self.subTest(returncode=returncode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                server = Mock(binary=root / "binary", functional_calibration=False)
                server.environment.return_value = {}
                process = Mock(pid=12345, returncode=returncode)
                process.poll.side_effect = [None, returncode]
                process.wait.return_value = returncode
                with patch("qualify.require_scope_idle"), patch("qualify.subprocess.Popen", return_value=process), \
                        patch("qualify.process_resources", side_effect=PermissionError("exiting child /proc/io")), \
                        patch("qualify.os.killpg"):
                    if returncode:
                        with self.assertRaises(subprocess.CalledProcessError) as error:
                            qualify.offline_command(server, root, "backup", ["backup", "full"])
                        self.assertEqual(error.exception.returncode, returncode)
                    else:
                        self.assertEqual(qualify.offline_command(server, root, "backup", ["backup", "full"]), root / "backup.log")
                self.assertEqual((root / "backup-resources.jsonl").read_text(), "")

    def test_live_child_proc_permission_error_remains_a_failure_and_stops_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            server = Mock(binary=root / "binary", functional_calibration=False)
            server.environment.return_value = {}
            process = Mock(pid=12345, returncode=0)
            process.poll.side_effect = [None, None]
            process.wait.return_value = 0
            with patch("qualify.require_scope_idle"), patch("qualify.subprocess.Popen", return_value=process), \
                    patch("qualify.process_resources", side_effect=PermissionError("live child access denied")), \
                    patch("qualify.os.killpg") as kill:
                with self.assertRaisesRegex(PermissionError, "live child access denied"):
                    qualify.offline_command(server, root, "backup", ["backup", "full"])
            kill.assert_called_once_with(process.pid, qualify.signal.SIGTERM)


class ExternalEvidence(unittest.TestCase):
    def example(self, directory, gate="physical_gpu"):
        binary = Path(directory) / "binary"
        binary.write_bytes(b"exact-artifact")
        raw = Path(directory) / "raw.json"
        raw.write_text('{"measured":true}\n')
        plan = external.contract(gate, binary, "source-sha")
        report = {"schema_version": 1, "gate": gate, "artifact_sha256": qualify.digest(binary),
                  "source_revision": "source-sha", "criteria_sha256": external.criteria_digest(plan), "status": "accepted",
                  "started_at_utc": "2026-10-28T00:00:00+00:00", "finished_at_utc": "2026-10-29T00:00:00+00:00",
                  "checks": {key: {"status": "accepted"} for key in external.CHECKS[gate]},
                  "environment": {"adapters": [{"type": "physical", "software": False, "vendor": "vendor", "model": "model", "driver": "driver", "api": "Vulkan", "device_id": "device"}]},
                  "operator_acceptance": {"reviewer": "fixture reviewer", "evidence_ticket": "fixture-only", "accepted_at_utc": "2026-10-29T01:00:00+00:00"},
                  "evidence": [{"path": "raw.json", "sha256": qualify.digest(raw)}]}
        return plan, report

    def test_software_gpu_missing_checks_and_missing_acceptance_never_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            plan, report = self.example(directory)
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "accepted")
            for change in ("software", "check", "acceptance", "criteria", "revision"):
                damaged = copy.deepcopy(report)
                if change == "software":
                    damaged["environment"]["adapters"][0]["software"] = True
                elif change == "check":
                    damaged["checks"]["device_loss"]["status"] = "unrun"
                elif change == "acceptance":
                    damaged.pop("operator_acceptance")
                elif change == "criteria":
                    damaged["criteria_sha256"] = "0" * 64
                else:
                    damaged["source_revision"] = "different"
                with self.subTest(change=change):
                    self.assertEqual(external.validate_evidence(plan, damaged, directory)["status"], "blocked")

    def test_raw_evidence_tampering_symlinks_and_traversal_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            plan, report = self.example(directory)
            for name in ("../escape.json", "/absolute.json", "missing.json"):
                damaged = copy.deepcopy(report)
                damaged["evidence"][0]["path"] = name
                self.assertEqual(external.validate_evidence(plan, damaged, directory)["status"], "blocked")
            Path(directory, "linked.json").symlink_to(Path(directory, "raw.json"))
            report["evidence"][0]["path"] = "linked.json"
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "blocked")
            report["evidence"][0]["path"] = "raw.json"
            Path(directory, "raw.json").write_text("changed")
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "blocked")

    def test_three_hosts_and_actual_production_duration_are_required(self):
        with tempfile.TemporaryDirectory() as directory:
            plan, report = self.example(directory, "cluster")
            report["environment"] = {"nodes": [{"host_id": "same-host", "process_id": i + 1, "certificate_identity": str(i)} for i in range(3)]}
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "blocked")
            for i, node in enumerate(report["environment"]["nodes"]):
                node["host_id"] = f"host{i}"
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "accepted")
            plan, report = self.example(directory, "production_environment")
            report["environment"] = {key: "recorded" for key in ("hardware", "storage", "filesystem", "deployment", "slo_owner")}
            report["finished_at_utc"] = "2026-10-28T00:15:00+00:00"
            self.assertEqual(external.validate_evidence(plan, report, directory)["status"], "blocked")

    def test_empty_command_stays_unrun_without_launch(self):
        with tempfile.TemporaryDirectory() as directory, patch("external.run_command") as launch:
            plan, _ = self.example(directory)
            path = Path(directory) / "plan.json"
            qualify.write_json(path, plan)
            output = Path(directory) / "output"
            self.assertEqual(external.main(["run", "--plan", str(path), "--output", str(output)]), 2)
            self.assertEqual(json.loads((output / "report.json").read_text())["status"], "unrun")
            launch.assert_not_called()

    def test_external_timeout_stops_owned_process_group(self):
        process = Mock(pid=12345)
        process.wait.side_effect = [subprocess.TimeoutExpired(["fixture"], 1), 0]
        with patch("external.subprocess.Popen", return_value=process), patch("external.os.killpg") as kill:
            with self.assertRaises(subprocess.TimeoutExpired):
                external.run_command(["fixture"], Path("/tmp"), {}, 1, io.BytesIO())
            kill.assert_called_once_with(12345, external.signal.SIGTERM)

    def test_zero_exit_without_raw_contract_report_is_failed(self):
        with tempfile.TemporaryDirectory() as directory, patch("external.run_command"):
            plan, _ = self.example(directory)
            plan["command"] = ["reviewed-fixture"]
            path = Path(directory) / "plan.json"
            qualify.write_json(path, plan)
            output = Path(directory) / "output"
            self.assertEqual(external.main(["run", "--plan", str(path), "--output", str(output)]), 2)
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["status"], "failed")


if __name__ == "__main__":
    unittest.main()
