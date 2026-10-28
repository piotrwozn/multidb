import csv
import json
from pathlib import Path
import tempfile
import unittest

from verify_domains import expected_sections, verify


PLAN = {"domains": ["kv"], "rows": [100], "ops": 2, "repeats": 1}


def fixture(path):
    summaries, raw = [], []
    for (engine, work, repeat), count in expected_sections(PLAN).items():
        summaries.append(dict(engine=engine, workload=work, repeat=repeat,
                              requested=count, attempts=count, successes=count, errors=0, failures=[],
                              elapsed_seconds=2., requests_per_second=count / 2., correctness_status="passed",
                              p50_ms=1.5, p95_ms=1.95, p99_ms=1.99, min_ms=1., max_ms=2., mean_ms=1.5))
        raw.extend(dict(engine=engine, workload=work, repeat=repeat, operation=i,
                        latency_ms=i + 1, success=True, error="") for i in range(count))
    write(path, summaries, raw)
    return summaries, raw


def write(path, summaries, raw):
    (path / "domain-results.json").write_text(json.dumps(summaries))
    with (path / "domain-samples.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=("engine", "workload", "repeat", "operation", "latency_ms", "success", "error"))
        writer.writeheader()
        writer.writerows(raw)


class IndependentDomainVerifier(unittest.TestCase):
    def test_valid_complete_evidence_and_plan_independence(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            fixture(path)
            proof = verify(path, PLAN)
            self.assertTrue(proof["passed"], proof)
            self.assertEqual(proof["expected_sections"], 6)
            self.assertEqual(proof["raw_samples"], 12)
        plan = {"domains": ["analytics", "kv", "vector", "graph", "tensor"],
                "rows": [10000, 100000], "ops": 200, "repeats": 3}
        expected = expected_sections(plan)
        analytical = expected_sections({**plan, "analytical": True})
        self.assertEqual(len(analytical) - len(expected), 30)
        self.assertEqual(expected[("deeplake", "tensor_append_batch_cpu_reference_n10000", 1)], 157)
        self.assertEqual(expected[("neo4j_http", "graph.neighbors_depth2.n1000", 0)], 200)

    def test_missing_duplicate_corrupt_failed_and_nonfinite_evidence_is_rejected(self):
        for kind in ("missing_section", "duplicate_summary", "duplicate_operation", "missing_operation",
                     "percentile", "throughput", "failed_operation", "nan", "unexpected_section"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as directory:
                path = Path(directory)
                summaries, raw = fixture(path)
                if kind == "missing_section":
                    summaries.pop()
                    raw = raw[:-2]
                elif kind == "duplicate_summary":
                    summaries.append(summaries[0])
                elif kind == "duplicate_operation":
                    raw[1]["operation"] = 0
                elif kind == "missing_operation":
                    raw.pop()
                elif kind == "percentile":
                    summaries[0]["p95_ms"] = 99
                elif kind == "throughput":
                    summaries[0]["requests_per_second"] = 999
                elif kind == "failed_operation":
                    raw[0]["success"], raw[0]["error"] = False, "wrong answer"
                elif kind == "nan":
                    raw[0]["latency_ms"] = "nan"
                else:
                    summaries[0]["workload"] = "kv.unreviewed"
                write(path, summaries, raw)
                self.assertFalse(verify(path, PLAN)["passed"])


if __name__ == "__main__":
    unittest.main()
