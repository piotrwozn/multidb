"""Deterministic runner tests; no server, harness, compiler, or network needed."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch


SPEC = importlib.util.spec_from_file_location("benchmark_compare", Path(__file__).with_name("compare.py"))
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def results():
    result = []
    for work in (*runner.WORKLOADS, "load_batch"):
        for rep in ([0] if work == "load_batch" else [1, 2, 3]):
            result.append({
                "engine": "multidb", "dataset_rows": 10000, "workload": work, "repeat": rep,
                "attempts": 100, "successes": 100, "errors": 0, "correctness_status": "passed",
                "successful_requests_per_second": 100 + rep,
                "successful_records_per_second": 1000 + rep,
                "p50_ms": 10 + rep, "p95_ms": 20 + rep, "p99_ms": 30 + rep,
            })
    return result


class RunnerTests(unittest.TestCase):
    def test_cpu_selection_uses_allowed_ids_and_rejects_overlap(self):
        self.assertEqual(runner.select_cpus(available={5, 7, 9, 11}), ([5, 7], [9, 11]))
        with self.assertRaises(ValueError):
            runner.select_cpus("5,7", "7,9", {5, 7, 9})
        with self.assertRaises(ValueError):
            runner.select_cpus("0,1", "9", {5, 7, 9})

    def test_server_ignores_external_deployment_overrides(self):
        with patch.dict(os.environ, {"MULTIDB_SPEC_PATH": "/production.json",
                                     "MULTIDB_ADMIN_TOKEN_FILE": "/secret",
                                     "MULTIDB_HISTORY_RETAIN_LSNS": "1", "LANG": "C"}):
            environment = runner.server_environment(Path("/fresh/db.redb"))
        self.assertNotIn("MULTIDB_SPEC_PATH", environment)
        self.assertNotIn("MULTIDB_ADMIN_TOKEN_FILE", environment)
        self.assertNotIn("MULTIDB_HISTORY_RETAIN_LSNS", environment)
        self.assertEqual(environment["MULTIDB_DB_PATH"], "/fresh/db.redb")
        self.assertEqual(environment["LANG"], "C")

    def test_hash_verification_rejects_modified_harness_before_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            for name in runner.HARNESSES:
                (path / name).write_text("raise RuntimeError('must never execute')")
            (path / "provenance.json").write_text(json.dumps({"files": runner.HARNESSES}))
            with self.assertRaisesRegex(ValueError, "hash mismatch"):
                runner.verify_harness(path)

    def test_command_keeps_metacharacters_in_one_argument(self):
        command = runner.cpu_command([5, 7], ["/tmp/name with spaces;$(bad)", "serve"])
        self.assertEqual(command[-2:], ["/tmp/name with spaces;$(bad)", "serve"])
        self.assertEqual(command[1:3], ["-c", "5,7"])

    def test_invalid_incomplete_or_duplicate_results_are_never_compared(self):
        original = results()
        variants = [original[:-1], original + [original[0]]]
        for field, value in (("correctness_status", "failed"), ("errors", 1),
                             ("attempts", 0), ("p50_ms", float("nan"))):
            changed = copy.deepcopy(original)
            changed[0][field] = value
            variants.append(changed)
        for variant in variants:
            with self.subTest(variant=variant[-1]):
                with self.assertRaises(ValueError):
                    runner.comparison(original, variant, [10000], 3)

    def test_medians_and_ratio_preserve_variation_and_sample_counts(self):
        before = results()
        after = copy.deepcopy(before)
        for item in after:
            item["successful_requests_per_second"] *= 2
            item["p50_ms"] /= 2
        point = next(x for x in runner.comparison(before, after, [10000], 3) if x["workload"] == "point_read")
        self.assertEqual(point["throughput_ratio_after_over_before"], 2)
        self.assertEqual(point["p50_speedup_before_over_after"], 2)
        self.assertEqual(point["before"]["p50_ms"], {"median": 12, "min": 11, "max": 13})
        self.assertEqual(point["before"]["attempts"], 300)
        after[0]["attempts"] = after[0]["successes"] = 101
        with self.assertRaisesRegex(ValueError, "different sample counts"):
            runner.comparison(before, after, [10000], 3)

    def test_idle_cargo_is_allowed_but_compilers_block_timing(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            for pid, name, state in ((1, "cargo", "S"), (2, "rustc", "S"), (3, "rustc", "Z")):
                entry = path / str(pid)
                entry.mkdir()
                (entry / "comm").write_text(name)
                (entry / "stat").write_text(f"{pid} ({name}) {state} 0 0")
            self.assertEqual(runner.active_builds(path), [{"pid": 2, "name": "rustc"}])

    def test_server_cleanup_runs_even_if_client_cleanup_raises(self):
        client, server = object(), object()
        with patch.object(runner, "stop_process", side_effect=[RuntimeError("client cleanup"), {"was_running": True}]) as stop:
            with self.assertRaisesRegex(RuntimeError, "client cleanup"):
                runner.cleanup_processes(client, server)
        self.assertEqual([call.args[0] for call in stop.call_args_list], [client, server])

    def test_exit_race_during_termination_still_reaps_child(self):
        process = Mock(pid=123)
        process.poll.return_value = None
        with patch.object(runner.os, "killpg", side_effect=ProcessLookupError):
            result = runner.stop_process(process)
        process.wait.assert_called_once_with(timeout=20)
        self.assertEqual(result, {"was_running": True, "forced_kill": False})

    def test_unexpected_server_exit_is_rejected_even_after_client_finishes(self):
        process = Mock(returncode=17)
        process.poll.return_value = 17
        with self.assertRaisesRegex(RuntimeError, "exited unexpectedly"):
            runner.require_server_alive(process, "before")


if __name__ == "__main__":
    unittest.main()
