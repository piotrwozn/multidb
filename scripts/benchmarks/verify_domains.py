#!/usr/bin/env python3
"""Independently verify domain raw CSV, summaries and the complete campaign plan.

This module does not import a Recorder or adapter. Timing consistency does not
replace their result/recall validation; all reported failures remain disqualifying.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import statistics


def quantile(values, probability):
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    low, high = math.floor(position), math.ceil(position)
    return ordered[low] * (high - position) + ordered[high] * (position - low) if low != high else ordered[low]


def expected_sections(plan):
    """The reviewed workload contract, kept independent from adapter execution."""
    domains = plan.get("domains", [])
    domains = domains.split(",") if isinstance(domains, str) else domains
    sizes = plan["rows"]
    sizes = [int(size) for size in sizes.split(",")] if isinstance(sizes, str) else sizes
    repeats, operations = plan["repeats"], plan["ops"]
    if (type(repeats) is not int or type(operations) is not int or min(repeats, operations) < 1
            or not sizes or any(type(size) is not int or size < 1 for size in sizes)
            or len(sizes) != len(set(sizes)) or len(domains) != len(set(domains))
            or set(domains) - {"sql", "analytics", "kv", "vector", "graph", "tensor"}):
        raise ValueError("invalid campaign plan")
    result = {}

    def add(engines, workload, rounds, count):
        for engine in engines:
            for repeat in rounds:
                key = engine, workload, repeat
                if key in result:
                    raise ValueError(f"duplicate expected section: {key}")
                result[key] = count

    if "analytics" in domains:
        engines = ["duckdb_inprocess", "multidb_http"]
        if plan.get("analytical", False):
            engines.append("multidb_analytical_http")
        for size in sizes:
            for work, count in (("range100", operations), ("group_count_sum", min(30, operations)),
                                ("projection_full_scan", min(30, operations)),
                                ("full_payload_scan", min(5, operations)), ("count_filter", operations)):
                add(engines, f"analytics.{work}.n{size}", range(repeats), count)
    if "kv" in domains:
        for size in sizes:
            for work, count in (("point_get", operations), ("full_record_upsert", operations),
                                ("atomic_insert_batch100", min(30, operations))):
                add(("redis_resp", "multidb_http"), f"kv.{work}.n{size}", range(1, repeats + 1), count)
    if "vector" in domains:
        for size in (1000, 10000):
            add(("multidb_http", "qdrant_http"), f"vector.ann_k10.n{size}", range(repeats), operations)
    if "graph" in domains:
        for size in (1000, 10000):
            for work in ("neighbors_depth1", "neighbors_depth2", "durable_edge_update"):
                add(("multidb_http", "neo4j_http"), f"graph.{work}.n{size}", range(repeats), operations)
    if "tensor" in domains:
        for size in sizes:
            for work, count in (("append_batch", (size + 63) // 64),
                                ("projected_read", operations), ("exact_l2_search", operations)):
                add(("multidb", "deeplake"), f"tensor_{work}_cpu_reference_n{size}",
                    range(1, repeats + 1), count)
    return result


def verify(directory, plan):
    directory = Path(directory)
    issues = []
    raw, summaries = defaultdict(list), {}
    expected = expected_sections(plan)
    try:
        results_path = directory / "domain-results.json"
        documents = (json.loads(results_path.read_text()) if expected or results_path.exists() else [])
        if not isinstance(documents, list):
            raise ValueError("domain-results.json is not an array")
        for row in documents:
            key = row["engine"], row["workload"], row["repeat"]
            if type(key[2]) is not int or key in summaries:
                issues.append(f"invalid or duplicate summary identity: {key}")
            summaries[key] = row
        with (directory / "domain-samples.csv").open(newline="") as stream:
            reader = csv.DictReader(stream)
            required = {"engine", "workload", "repeat", "operation", "latency_ms", "success", "error"}
            if not required <= set(reader.fieldnames or []):
                raise ValueError("raw CSV headers are incomplete")
            for row in reader:
                key = row["engine"], row["workload"], int(row["repeat"])
                operation, latency = int(row["operation"]), float(row["latency_ms"])
                if row["success"] not in {"True", "False"} or not math.isfinite(latency) or latency < 0:
                    raise ValueError(f"invalid raw sample: {key}/{operation}")
                successful = row["success"] == "True"
                if successful == bool(row["error"]):
                    issues.append(f"sample success/error disagree: {key}/{operation}")
                raw[key].append((operation, latency, successful))
        for label, keys in (("summaries", set(summaries)), ("raw CSV", set(raw))):
            for key in sorted(set(expected) - keys):
                issues.append(f"missing expected section in {label}: {key}")
            for key in sorted(keys - set(expected)):
                issues.append(f"unexpected section in {label}: {key}")
        for key, result in summaries.items():
            samples = raw.get(key, [])
            successful = [latency for _, latency, success in samples if success]
            errors = len(samples) - len(successful)
            count = expected.get(key)
            if ([operation for operation, _, _ in samples] != list(range(len(samples)))
                    or len(samples) != count or result.get("requested") != count):
                issues.append(f"incomplete/duplicate operation sequence or count: {key}")
            for field, actual in (("attempts", len(samples)), ("successes", len(successful)), ("errors", errors)):
                if type(result.get(field)) is not int or result[field] != actual:
                    issues.append(f"raw/summary {field} mismatch: {key}")
            if errors or result.get("failures") or result.get("correctness_status") != "passed":
                issues.append(f"invalid operation/correctness result: {key}")
            elapsed = result.get("elapsed_seconds")
            if type(elapsed) not in (int, float) or not math.isfinite(elapsed) or elapsed <= 0:
                issues.append(f"invalid section elapsed time: {key}")
                continue

            def check(field, value):
                actual = result.get(field)
                if (type(actual) not in (int, float) or not math.isfinite(actual)
                        or not math.isclose(actual, value, rel_tol=1e-9, abs_tol=1e-9)):
                    issues.append(f"raw/summary {field} mismatch: {key}")

            check("requests_per_second", len(successful) / elapsed)
            if successful:
                for field, probability in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99)):
                    check(field, quantile(successful, probability))
                check("min_ms", min(successful))
                check("max_ms", max(successful))
                check("mean_ms", statistics.mean(successful))
            else:
                issues.append(f"no successful observations: {key}")
    except (OSError, ValueError, KeyError, TypeError, OverflowError) as error:
        issues.append(f"invalid or missing evidence: {type(error).__name__}: {error}")
    proof = {"schema_version": 1, "passed": not issues, "issues": issues,
             "expected_sections": len(expected), "summaries": len(summaries),
             "raw_samples": sum(len(rows) for rows in raw.values()),
             "scope": "Raw timing/count consistency and complete reviewed workload plan; adapters validate returned data"}
    proof["source_sha256"] = {name: hashlib.sha256((directory / name).read_bytes()).hexdigest()
                              for name in ("domain-results.json", "domain-samples.csv") if (directory / name).is_file()}
    (directory / "independent-verification.json").write_text(json.dumps(proof, indent=2) + "\n")
    return proof


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("campaign", type=Path)
    args = parser.parse_args()
    plan = json.loads((args.campaign / "run.json").read_text())
    proof = verify(args.campaign / "domains", plan)
    print(json.dumps(proof, indent=2))
    return 0 if proof["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
