import importlib.util
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

import domain_analytics as analytics


class ExpectedResults(unittest.TestCase):
    def test_group_math_matches_explicit_integer_grouping(self):
        for size in (100, 137, 10_000):
            groups = {}
            for index in range(size):
                groups.setdefault(index % 100, []).append(index)
            expected = [[category, len(values), sum(values)] for category, values in sorted(groups.items())]
            self.assertEqual(analytics.aggregate_expected(size), expected)

    def test_validation_rejects_numeric_coercion_and_truncated_results(self):
        analytics.require_equal([[1, 2]], [[1, 2]])
        for result in ([[1.0, 2]], [[True, 2]], [], [[1, 2], [3, 4]]):
            with self.assertRaises(AssertionError):
                analytics.require_equal(result, [[1, 2]])

    def test_range_and_filter_references_cover_nonmultiple_dataset(self):
        for name, count, commands, expected in analytics.sections(137, "items", 101):
            self.assertGreater(count, 0)
            self.assertTrue(commands)
            if name == "range100":
                self.assertTrue(all(len(rows) == 100 for rows in expected))
                self.assertTrue(all(0 <= rows[0][0] <= rows[-1][0] < 137 for rows in expected))
            elif name == "count_filter":
                self.assertEqual(expected[0], [[2]])
                self.assertEqual(expected[36], [[2]])
                self.assertEqual(expected[37], [[1]])


@unittest.skipUnless(importlib.util.find_spec("duckdb"), "DuckDB 1.4.3 is required for adapter validation")
class AdapterValidation(unittest.TestCase):
    def test_real_duckdb_operations_and_multidb_wire_adapter_without_timing(self):
        import duckdb
        if duckdb.__version__ != "1.4.3":
            self.skipTest("pinned DuckDB 1.4.3 required")

        class WireFixture:
            """A shape-faithful test double; this does not test the MultiDB engine."""
            def __init__(self):
                self.connection = duckdb.connect(":memory:")

            def json(self, method, path, body):
                if (method, path) == ("POST", "/builder/table"):
                    self.connection.execute(f"CREATE TABLE {body['name']} (id BIGINT, category BIGINT, value BIGINT, payload VARCHAR)")
                    return {}
                if (method, path) != ("POST", "/sql"):
                    raise AssertionError((method, path))
                cursor = self.connection.execute(body["sql"])
                values = [list(row) for row in cursor.fetchall()]
                if body["sql"].startswith("INSERT"):
                    return {"output": {"kind": "affected_rows", "affected_rows": values[0][0]}}
                return {"output": {"kind": "rows", "rows": values}}

        class UntimedRecorder:
            def __init__(self):
                self.sections = []

            def measure(self, engine, workload, repeat, count, operation, verify, **metadata):
                for index in range(count):
                    verify(operation(index), index)
                self.sections.append((engine, workload))

        fixture = WireFixture()
        try:
            for include_columnar in (False, True):
                columnar = WireFixture() if include_columnar else None
                try:
                    with tempfile.TemporaryDirectory() as directory:
                        recorder = UntimedRecorder()
                        args = SimpleNamespace(output=Path(directory), rows=[137], repeats=1, ops=2)
                        analytics.run(args, recorder, fixture, analytical=columnar)
                        self.assertEqual(len(recorder.sections), 15 if include_columnar else 10)
                        engines = {"duckdb_inprocess", "multidb_http"}
                        if include_columnar:
                            engines.add("multidb_analytical_http")
                        self.assertEqual({engine for engine, _ in recorder.sections}, engines)
                        self.assertTrue((Path(directory) / "duckdb-analytics.duckdb").is_file())
                        self.assertTrue((Path(directory) / "analytics-environment.json").is_file())
                finally:
                    if columnar is not None:
                        columnar.connection.close()
        finally:
            fixture.connection.close()


if __name__ == "__main__":
    unittest.main()
