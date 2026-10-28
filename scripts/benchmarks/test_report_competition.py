import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from report_competition import aggregate


def proof(path):
    # These fixtures isolate the report's proof binding. The independent raw
    # verifier's complete-plan and metric checks have their own regression tests.
    samples = path / 'domains/domain-samples.csv'
    if not samples.exists():
        samples.write_text('fixture raw evidence\n')
    hashes = {name: hashlib.sha256((samples.parent / name).read_bytes()).hexdigest()
              for name in ('domain-results.json', 'domain-samples.csv')}
    (samples.parent / 'independent-verification.json').write_text(json.dumps({
        'schema_version': 1, 'passed': True, 'source_sha256': hashes,
    }))


class ReportValidationTests(unittest.TestCase):
    def test_incomplete_failed_and_duplicate_evidence_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with self.assertRaises(ValueError):
                aggregate(path)
            (path / 'completion.json').write_text(json.dumps({'status': 'passed', 'clean_server_exit': True}))
            (path / 'domains').mkdir()
            row = dict(engine='neo4j_http', workload='graph.depth2', repeat=1,
                       successes=10, errors=0, correctness_status='passed',
                       requests_per_second=100, p50_ms=1, p95_ms=2, p99_ms=3,
                       metadata={'dataset_nodes': 1000})
            output = path / 'domains/domain-results.json'
            output.write_text(json.dumps([row]))
            with self.assertRaises(ValueError):
                aggregate(path)
            proof(path)
            summary = aggregate(path)
            self.assertEqual(summary[0]['dataset_size'], 1000)
            self.assertTrue(summary[0]['tail_sample_warning'])
            output.write_text(json.dumps([row, row]))
            proof(path)
            with self.assertRaises(ValueError):
                aggregate(path)
            output.write_text(json.dumps([{**row, 'errors': 1}]))
            proof(path)
            with self.assertRaises(ValueError):
                aggregate(path)

    def test_summary_and_raw_changes_invalidate_existing_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / 'completion.json').write_text(json.dumps({'status': 'passed', 'clean_server_exit': True}))
            (path / 'domains').mkdir()
            output = path / 'domains/domain-results.json'
            row = dict(engine='neo4j_http', workload='graph.depth2', repeat=1,
                       successes=10, errors=0, correctness_status='passed',
                       requests_per_second=100, p50_ms=1, p95_ms=2, p99_ms=3,
                       metadata={'dataset_nodes': 1000})
            output.write_text(json.dumps([row]))
            proof(path)
            output.write_text(json.dumps([{**row, 'p50_ms': .001}]))
            with self.assertRaisesRegex(ValueError, 'changed after independent'):
                aggregate(path)
            proof(path)
            (path / 'domains/domain-samples.csv').write_text('changed raw evidence\n')
            with self.assertRaisesRegex(ValueError, 'changed after independent'):
                aggregate(path)
            (path / 'failure.json').write_text('{}')
            with self.assertRaises(ValueError):
                aggregate(path)
