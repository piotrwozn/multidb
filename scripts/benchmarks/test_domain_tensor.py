"""Untimed adapter checks; a real temporary DeepLake dataset, no server or compiler."""
import importlib.util
import tempfile
import unittest
from pathlib import Path

from domain_tensor import DeepLake, check_hits, check_rows, oracle, sample


@unittest.skipUnless(importlib.util.find_spec("deeplake") and importlib.util.find_spec("numpy"),
                     "optional benchmark DeepLake/NumPy environment is not installed")
class TensorAdapterTests(unittest.TestCase):
    def test_deeplake_sources_shapes_projection_snapshot_and_exact_search(self):
        import deeplake
        import numpy as np

        with tempfile.TemporaryDirectory(prefix="tensor-adapter-") as directory:
            path = Path(directory) / "dataset"
            adapter = DeepLake(path, deeplake, np)
            appended = adapter.append([sample(i, np) for i in range(24)])
            self.assertEqual(appended["samples"], 24)
            check_rows(adapter.read(0, 24), 0, 24, np)
            revision = adapter.snapshot()
            adapter.append([sample(24, np)])
            self.assertEqual(adapter.view.version, revision)
            self.assertEqual(len(adapter.view), 24)
            self.assertEqual(len(adapter.dataset), 25)
            check_rows(adapter.read(19, 5, projected=True), 19, 5, np, projected=True)
            for query, expected in oracle(24, [0, 7, 23], np):
                check_hits(adapter.search(query, 24), expected)
            with self.assertRaisesRegex(ValueError, "fresh"):
                DeepLake(path, deeplake, np)

    def test_correctness_checks_reject_source_dtype_and_search_errors(self):
        import numpy as np

        wrong = sample(0, np)
        wrong["image"] = wrong["image"].astype(np.float32)
        with self.assertRaisesRegex(AssertionError, "dtype/shape"):
            check_rows([wrong], 0, 1, np)
        wrong = sample(1, np)
        with self.assertRaisesRegex(AssertionError, "provenance"):
            check_rows([wrong], 0, 1, np)
        with self.assertRaisesRegex(AssertionError, "oracle"):
            check_hits([{"index": 1, "distance": 0.0}], [(0, 0.0)])
        with self.assertRaisesRegex(AssertionError, "oracle"):
            check_hits([{"index": 0, "distance": float("nan")}], [(0, 0.0)])


if __name__ == "__main__":
    unittest.main()
