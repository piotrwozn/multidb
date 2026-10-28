import copy
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import domains_vector_graph as domain


class VectorGraphValidationTests(unittest.TestCase):
    def test_graph_reference_deduplicates_depth_two_paths_and_wraparound(self):
        offsets = {1, 7, 31, 127, 2, 8, 32, 128, 14, 38, 134, 62, 158, 254}
        self.assertEqual(domain.graph_targets(1000, 0, 2), sorted(domain.node_id(value) for value in offsets))
        self.assertEqual(domain.graph_targets(1000, 999, 2), sorted(domain.node_id((999 + value) % 1000) for value in offsets))
        self.assertNotIn(domain.node_id(999), domain.graph_targets(1000, 999, 2))

    def test_graph_validation_rejects_duplicate_missing_and_extra_destinations(self):
        expected = ["a", "b"]
        domain.validate_graph_rows([["b"], ["a"]], expected)
        for rows in ([["a"], ["a"], ["b"]], [["a"]], [["a"], ["b"], ["c"]], [[1], ["b"]]):
            with self.subTest(rows=rows), self.assertRaises(AssertionError):
                domain.validate_graph_rows(rows, expected)

    def test_vector_validation_checks_distance_identity_duplicates_and_recall(self):
        vectors = [[0.], [1.], [2.]]
        result = {"hits": [{"metadata": {"index": 0}, "distance": 0.}, {"metadata": {"index": 1}, "distance": 1.}]}
        stats = lambda: {"queries": 0, "sum": 0., "mean": 0., "minimum": 1.}
        totals = stats()
        domain.validate_vector_hits(result, [0, 1], vectors, [0.], totals)
        self.assertEqual(totals["mean"], 1.)
        invalid = []
        wrong_distance = copy.deepcopy(result)
        wrong_distance["hits"][1]["distance"] = 2.
        invalid.append(wrong_distance)
        wrong_identity = copy.deepcopy(result)
        wrong_identity["hits"][1]["metadata"]["index"] = 9
        invalid.append(wrong_identity)
        duplicates = copy.deepcopy(result)
        duplicates["hits"][1] = copy.deepcopy(duplicates["hits"][0])
        invalid.append(duplicates)
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(AssertionError):
                domain.validate_vector_hits(value, [0, 1], vectors, [0.], stats())
        with self.assertRaises(AssertionError):
            domain.validate_vector_hits({"hits": [result["hits"][0]]}, [2], vectors, [0.], stats())

    def test_unsupported_filter_requires_explicit_http_400(self):
        class Fake:
            def __init__(self, failure=None):
                self.failure = failure

            def json(self, *args):
                if self.failure:
                    raise self.failure
                return {"hits": []}

        domain.require_http_400(Fake(RuntimeError("HTTP400 /search: unknown field")), "/search", {})
        for response in (Fake(), Fake(RuntimeError("HTTP500 /search: failure"))):
            with self.assertRaises(AssertionError):
                domain.require_http_400(response, "/search", {})

    def test_neo4j_transaction_errors_cannot_qualify_as_empty_success(self):
        class Failed:
            def json(self, *args):
                return {"errors": [{"code": "Neo.ClientError.Statement.SyntaxError"}], "results": []}

        with self.assertRaises(AssertionError):
            domain.neo4j_rows(Failed(), "invalid Cypher")

    def test_graph_only_run_never_contacts_vector_competitor(self):
        class FakeHttp:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                pass

        with tempfile.TemporaryDirectory() as directory:
            args = SimpleNamespace(output=directory, domains=["graph"], ops=1, repeats=1)
            with patch.object(domain, "Http", FakeHttp), patch.object(domain, "run_vectors") as vectors, patch.object(domain, "run_graphs") as graphs:
                domain.run(args, object())
                vectors.assert_not_called()
                graphs.assert_called_once()
