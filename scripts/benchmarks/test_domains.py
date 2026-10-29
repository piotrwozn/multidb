import csv
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from domain_common import Recorder, percentile


class DomainMeasurementTests(unittest.TestCase):
    def test_quantiles_and_validation_fail_closed(self):
        self.assertEqual(percentile([4, 1, 3, 2], .5), 2.5)
        with tempfile.TemporaryDirectory() as directory, patch('domain_common.require_idle'):
            recorder = Recorder(directory)
            try:
                recorder.measure('fixture', 'read', 1, 4, lambda i: i, lambda result, i: result == i)
                with self.assertRaises(RuntimeError):
                    recorder.measure('fixture', 'bad_read', 1, 4, lambda i: i, lambda *_: False)
            finally:
                recorder.close()
            summaries = json.loads((Path(directory) / 'domain-results.json').read_text())
            self.assertEqual(summaries[0]['successes'], 4)
            self.assertEqual(summaries[1]['successes'], 0)
            self.assertEqual(summaries[1]['errors'], 3)
            with (Path(directory) / 'domain-samples.csv').open() as stream:
                self.assertEqual(len(list(csv.DictReader(stream))), 7)
