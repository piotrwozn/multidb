import json
import hashlib
from pathlib import Path
import tempfile
import unittest
from report_competition import aggregate, aggregate_campaigns


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


def campaign(path, domain, **overrides):
    """Small proof-bound fixtures; full raw-plan validation is tested separately."""
    path.mkdir()
    run = dict(binary_sha256='a' * 64, revision='b' * 40,
               rows=[10000, 100000], repeats=3, ops=200,
               server_cpus=[0, 1], client_cpus=[2, 3],
               host={'platform': 'fixture-host', 'cgroup': {'cpu.max': '400000 100000'}},
               domains=[domain])
    run.update(overrides)
    (path / 'run.json').write_text(json.dumps(run))
    (path / 'completion.json').write_text(json.dumps({
        'status': 'passed', 'clean_server_exit': True,
        'binary_sha256': run['binary_sha256'],
    }))
    row = dict(engine=f'{domain}_engine', repeat=0,
               successes=10, errors=0, correctness_status='passed',
               p50_ms=1, p95_ms=2, p99_ms=3)
    if domain == 'sql':
        (path / 'sql').mkdir()
        row.update(workload='point_read', dataset_rows=10000,
                   successful_requests_per_second=100)
        (path / 'sql/results.json').write_text(json.dumps([row]))
        (path / 'sql/independent-verification.json').write_text(json.dumps({'passed': True}))
    else:
        (path / 'domains').mkdir()
        row.update(workload=f'{domain}.query', requests_per_second=100,
                   metadata={'dataset_rows': 10000})
        (path / 'domains/domain-results.json').write_text(json.dumps([row]))
        proof(path)
    return path


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


class CombinedCampaignTests(unittest.TestCase):
    def test_complete_disjoint_campaigns_combine_and_single_legacy_is_compatible(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sql = campaign(root / 'sql', 'sql')
            graph = campaign(root / 'graph', 'graph')
            rows = aggregate_campaigns([sql, graph])
            self.assertEqual([row['domain'] for row in rows], ['graph', 'sql'])
            (sql / 'run.json').unlink()
            self.assertEqual(aggregate_campaigns([sql]), aggregate(sql))
            with self.assertRaisesRegex(ValueError, 'metadata is missing or invalid'):
                aggregate_campaigns([sql, graph])

    def test_artifact_source_workload_cpu_and_host_mismatches_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sql = campaign(root / 'sql', 'sql')
            graph = campaign(root / 'graph', 'graph')
            original = json.loads((graph / 'run.json').read_text())
            differences = {
                'binary_sha256': 'c' * 64, 'revision': 'd' * 40,
                'rows': [20000, 100000], 'repeats': 4, 'ops': 201,
                'server_cpus': [1, 2], 'client_cpus': [3, 4],
                'host': {'platform': 'different-host'},
            }
            for field, value in differences.items():
                with self.subTest(field=field):
                    changed = {**original, field: value}
                    (graph / 'run.json').write_text(json.dumps(changed))
                    (graph / 'completion.json').write_text(json.dumps({
                        'status': 'passed', 'clean_server_exit': True,
                        'binary_sha256': changed['binary_sha256'],
                    }))
                    with self.assertRaisesRegex(ValueError, f'configuration mismatch: {field}'):
                        aggregate_campaigns([sql, graph])

    def test_duplicate_cells_overlapping_domains_and_relabelling_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = campaign(root / 'first', 'sql')
            second = campaign(root / 'second', 'sql')
            with self.assertRaisesRegex(ValueError, 'duplicate output cell'):
                aggregate_campaigns([first, second])
            output = second / 'sql/results.json'
            row = json.loads(output.read_text())[0]
            output.write_text(json.dumps([{**row, 'workload': 'range_read'}]))
            with self.assertRaisesRegex(ValueError, 'overlapping requested domains'):
                aggregate_campaigns([first, second])
            run = json.loads((second / 'run.json').read_text())
            (second / 'run.json').write_text(json.dumps({**run, 'domains': ['graph']}))
            with self.assertRaisesRegex(ValueError, 'results do not match requested domains'):
                aggregate_campaigns([first, second])

    def test_full_suite_requires_all_six_domains(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            domains = ['sql', 'analytics', 'kv', 'vector', 'graph', 'tensor']
            paths = [campaign(root / domain, domain) for domain in domains]
            with self.assertRaisesRegex(ValueError, 'full suite requires all six domains'):
                aggregate_campaigns(paths[:-1], require_full_suite=True)
            with self.assertRaisesRegex(ValueError, 'full suite requires all six domains'):
                aggregate_campaigns(paths[:1], require_full_suite=True)
            rows = aggregate_campaigns(paths, require_full_suite=True)
            self.assertEqual({row['domain'] for row in rows}, set(domains))
            self.assertEqual(len(rows), 6)

    def test_failed_incomplete_and_changed_proofs_cannot_enter_combined_report(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sql = campaign(root / 'sql', 'sql')
            graph = campaign(root / 'graph', 'graph')
            completion = (graph / 'completion.json').read_text()
            (graph / 'completion.json').unlink()
            with self.assertRaisesRegex(ValueError, 'incomplete or failed'):
                aggregate_campaigns([sql, graph])
            (graph / 'completion.json').write_text(completion)
            (graph / 'failure.json').write_text('{}')
            with self.assertRaisesRegex(ValueError, 'incomplete or failed'):
                aggregate_campaigns([sql, graph])
            (graph / 'failure.json').unlink()
            mismatched = json.loads(completion)
            mismatched['binary_sha256'] = 'c' * 64
            (graph / 'completion.json').write_text(json.dumps(mismatched))
            with self.assertRaisesRegex(ValueError, 'binary_sha256 differs from completion'):
                aggregate_campaigns([sql, graph])
            (graph / 'completion.json').write_text(completion)
            (graph / 'domains/domain-samples.csv').write_text('changed raw evidence\n')
            with self.assertRaisesRegex(ValueError, 'changed after independent verification'):
                aggregate_campaigns([sql, graph])
