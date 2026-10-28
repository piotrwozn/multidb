#!/usr/bin/env python3
"""Summarize successful raw SQL/domain campaigns without hiding failed sections."""
from __future__ import annotations

import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path
import statistics


SUITE_DOMAINS = frozenset({'sql', 'analytics', 'kv', 'vector', 'graph', 'tensor'})
COMMON_RUN_FIELDS = (
    'binary_sha256', 'revision', 'rows', 'repeats', 'ops',
    'server_cpus', 'client_cpus', 'host',
)


def aggregate(directory):
    directory = Path(directory)
    if (directory / 'failure.json').exists() or not (directory / 'completion.json').exists():
        raise ValueError(f'campaign is incomplete or failed: {directory}')
    complete = json.loads((directory / 'completion.json').read_text())
    if complete.get('status') != 'passed' or not complete.get('clean_server_exit'):
        raise ValueError('campaign did not pass a clean shutdown')
    grouped = collections.defaultdict(list)
    sql = directory / 'sql' / 'results.json'
    if sql.exists():
        proof = json.loads((directory / 'sql' / 'independent-verification.json').read_text())
        if proof.get('passed') is not True:
            raise ValueError('independent SQL verification did not pass')
        for value in json.loads(sql.read_text()):
            if value.get('correctness_status') != 'passed' or value.get('errors'):
                raise ValueError('invalid SQL result cannot qualify for comparison')
            grouped[('sql', value['workload'], value['dataset_rows'], value['engine'])].append({
                **value, 'requests_per_second': value['successful_requests_per_second'],
            })
    domains = directory / 'domains' / 'domain-results.json'
    if domains.exists():
        try:
            proof = json.loads((domains.parent / 'independent-verification.json').read_text())
            if proof.get('schema_version') != 1 or proof.get('passed') is not True:
                raise ValueError('independent domain verification did not pass')
            for name in ('domain-results.json', 'domain-samples.csv'):
                expected_hash = proof.get('source_sha256', {}).get(name)
                actual_hash = hashlib.sha256((domains.parent / name).read_bytes()).hexdigest()
                if expected_hash != actual_hash:
                    raise ValueError(f'domain evidence changed after independent verification: {name}')
        except (OSError, KeyError, TypeError) as error:
            raise ValueError('independent domain verification evidence is missing or invalid') from error
        for value in json.loads(domains.read_text()):
            if value.get('correctness_status') != 'passed' or value.get('errors'):
                raise ValueError('invalid domain result cannot qualify for comparison')
            domain = 'tensor' if value['workload'].startswith('tensor_') else value['workload'].split('.')[0]
            metadata = value.get('metadata', {})
            size = metadata.get('dataset_rows', metadata.get('dataset_vectors', metadata.get('dataset_nodes', metadata.get('dataset_samples', 0))))
            grouped[(domain, value['workload'], size, value['engine'])].append(value)
    result = []
    for (domain, workload, size, engine), repeats in sorted(grouped.items()):
        if len({r['repeat'] for r in repeats}) != len(repeats):
            raise ValueError(f'duplicate repeat: {domain}/{workload}/{engine}')
        item = dict(domain=domain, workload=workload, dataset_size=size, engine=engine,
                    repeats=len(repeats), samples=sum(r['successes'] for r in repeats), errors=0)
        for metric in ('requests_per_second', 'p50_ms', 'p95_ms', 'p99_ms'):
            values = [r[metric] for r in repeats]
            if not values or any(not isinstance(v, (int, float)) or v <= 0 for v in values):
                raise ValueError(f'invalid metric: {metric}')
            item[metric] = statistics.median(values)
            item[metric + '_repeat_min'] = min(values)
            item[metric + '_repeat_max'] = max(values)
        item['tail_sample_warning'] = item['samples'] < 100
        result.append(item)
    if not result:
        raise ValueError('campaign contains no qualifying sections')
    return result


def aggregate_campaigns(paths, *, require_full_suite=False):
    """Combine complete, compatible campaigns with disjoint domain ownership.

    A single campaign keeps the legacy report contract unless full-suite
    coverage is explicitly requested. Multi-campaign reports additionally bind
    every section to the same artifact, source, workload sizes and host plan.
    """
    paths = [Path(path) for path in paths]
    if not paths:
        raise ValueError('at least one campaign is required')
    if len(paths) == 1 and not require_full_suite:
        return aggregate(paths[0])

    reference = None
    seen_domains = set()
    seen_cells = set()
    combined = []
    for directory in paths:
        # Preserve all clean-shutdown, correctness and raw-proof checks. A
        # successful subset of a failed campaign cannot enter a combined report.
        rows = aggregate(directory)
        try:
            run = json.loads((directory / 'run.json').read_text())
            if not isinstance(run, dict) or any(run.get(key) is None for key in COMMON_RUN_FIELDS):
                raise ValueError('required run identity fields are missing')
            domains = run.get('domains')
            if (not isinstance(domains, list) or not domains
                    or any(not isinstance(domain, str) for domain in domains)
                    or len(set(domains)) != len(domains)
                    or set(domains) - SUITE_DOMAINS):
                raise ValueError('requested domains are missing or invalid')
            complete = json.loads((directory / 'completion.json').read_text())
            if complete.get('binary_sha256') != run['binary_sha256']:
                raise ValueError('run binary_sha256 differs from completion')
        except (OSError, TypeError, json.JSONDecodeError) as error:
            raise ValueError(f'campaign run metadata is missing or invalid: {directory}') from error

        if reference is None:
            reference = run
        else:
            for field in COMMON_RUN_FIELDS:
                if run[field] != reference[field]:
                    raise ValueError(f'campaign configuration mismatch: {field}')

        requested = set(domains)
        if {row['domain'] for row in rows} != requested:
            raise ValueError(f'results do not match requested domains: {directory}')
        for row in rows:
            key = (row['domain'], row['workload'], row['dataset_size'], row['engine'])
            if key in seen_cells:
                raise ValueError(f'duplicate output cell: {key}')
            seen_cells.add(key)
        overlap = seen_domains.intersection(requested)
        if overlap:
            raise ValueError(f'overlapping requested domains: {sorted(overlap)}')
        seen_domains.update(requested)
        combined.extend(rows)

    if require_full_suite and seen_domains != SUITE_DOMAINS:
        missing = sorted(SUITE_DOMAINS - seen_domains)
        raise ValueError(f'full suite requires all six domains; missing: {missing}')
    return sorted(combined, key=lambda row: (
        row['domain'], row['workload'], row['dataset_size'], row['engine'],
    ))


def render(rows):
    lines = ['# Pinned competitor measurements', '',
             'Median across repeated runs of each section; first operations may be cold unless the adapter explicitly warms them. Throughput includes client validation. Raw samples retain every attempted request.', '',
             'These are end-to-end API/deployment comparisons. Protocols, audit, durability APIs and resource limits differ where recorded in run.json and domain metadata. No physical GPU or universal ranking is claimed.', '',
             '| Domain | Workload | Rows/vectors | Engine | Repeats | Samples | Requests/s | p50 ms | p95 ms | p99 ms | p99 repeat range ms |',
             '|---|---|---:|---|---:|---:|---:|---:|---:|---:|---|']
    for row in rows:
        lines.append(f"| {row['domain']} | {row['workload']} | {row['dataset_size']} | {row['engine']} | {row['repeats']} | {row['samples']} | {row['requests_per_second']:.2f} | {row['p50_ms']:.4f} | {row['p95_ms']:.4f} | {row['p99_ms']:.4f} | {row['p99_ms_repeat_min']:.4f}–{row['p99_ms_repeat_max']:.4f} |")
    lines += ['', 'Percentiles with fewer than100 successful samples are descriptive small-sample observations. Load sections may contain only one repetition; they must not be described as three-repeat load trials.', '']
    return '\n'.join(lines)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('campaign', type=Path, nargs='+')
    p.add_argument('--output-prefix', type=Path, required=True)
    p.add_argument('--require-full-suite', action='store_true',
                   help='require complete, disjoint coverage of all six benchmark domains')
    args = p.parse_args()
    rows = aggregate_campaigns(args.campaign, require_full_suite=args.require_full_suite)
    for suffix in ('.json', '.csv', '.md'):
        if args.output_prefix.with_suffix(suffix).exists():
            raise FileExistsError('existing reports are never overwritten')
    args.output_prefix.parent.mkdir(parents=True, exist_ok=True)
    args.output_prefix.with_suffix('.json').write_text(json.dumps(rows, indent=2, allow_nan=False) + '\n')
    args.output_prefix.with_suffix('.md').write_text(render(rows))
    with args.output_prefix.with_suffix('.csv').open('x', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    main()
