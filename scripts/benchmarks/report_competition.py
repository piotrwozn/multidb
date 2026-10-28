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
    p.add_argument('campaign', type=Path)
    p.add_argument('--output-prefix', type=Path, required=True)
    args = p.parse_args()
    rows = aggregate(args.campaign)
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
