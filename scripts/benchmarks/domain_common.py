"""Shared real-operation measurement and authenticated loopback HTTP helpers.

Validation runs after each operation's latency timer. Section throughput includes
client validation and recording overhead. Invalid sections never qualify for a
performance comparison. Files are append-only and retain failed attempts too.
"""
from __future__ import annotations

import csv
import http.client
import json
import math
from pathlib import Path
import statistics
import time
from urllib.parse import urlsplit

from compare import require_idle


def percentile(values, probability):
    ordered = sorted(values)
    if not ordered:
        return None
    at = (len(ordered) - 1) * probability
    lo, hi = math.floor(at), math.ceil(at)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (at - lo)


class Http:
    """One persistent connection; redirects are never followed."""
    def __init__(self, base="http://127.0.0.1:18080", *, headers=None, envelope=True):
        parsed = urlsplit(base)
        if parsed.scheme != "http" or parsed.hostname not in ("127.0.0.1", "localhost"):
            raise ValueError("benchmark HTTP endpoints must be local http:// servers")
        self.connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
        self.prefix = parsed.path.rstrip("/")
        self.headers = dict(headers if headers is not None else {
            "Authorization": "Bearer benchmark-token-only", "X-MultiDB-Principal": "admin",
        })
        self.envelope = envelope

    def json(self, method, path, body=None, *, on_sent=None):
        encoded = None if body is None else json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        headers = {"Accept": "application/json", **self.headers}
        if encoded is not None:
            headers["Content-Type"] = "application/json"
        self.connection.request(method, self.prefix + path, encoded, headers)
        if on_sent is not None:
            on_sent()
        response = self.connection.getresponse()
        raw = response.read(64 * 1024 * 1024 + 1)
        if len(raw) > 64 * 1024 * 1024:
            self.connection.close()
            raise RuntimeError("benchmark response exceeds64MiB")
        if not 200 <= response.status < 300:
            raise RuntimeError(f"HTTP{response.status} {path}: {raw[:1024]!r}")
        payload = json.loads(raw)
        if self.envelope:
            if not isinstance(payload, dict) or payload.get("ok") is not True:
                raise RuntimeError(f"invalid MultiDB envelope: {payload!r}")
            return payload["data"]
        return payload

    def close(self):
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class Recorder:
    def __init__(self, output):
        self.output = Path(output)
        self.output.mkdir(parents=True, exist_ok=True)
        self.summaries = []
        self.path = self.output / "domain-samples.csv"
        self.stream = self.path.open("x", newline="", encoding="utf-8")
        self.writer = csv.DictWriter(self.stream, fieldnames=(
            "engine", "workload", "repeat", "operation", "latency_ms", "success", "error",
        ))
        self.writer.writeheader()
        with (self.output / "domain-results.json").open("x", encoding="utf-8") as summaries:
            summaries.write("[]\n")

    def measure(self, engine, workload, repeat, count, operation, verify=lambda result, i: None, **metadata):
        if count < 1:
            raise ValueError("count must be positive")
        require_idle()
        samples, failures = [], []
        started = time.perf_counter()
        for i in range(count):
            error = ""
            tick = time.perf_counter_ns()
            elapsed = None
            try:
                result = operation(i)
                elapsed = (time.perf_counter_ns() - tick) / 1e6
                checked = verify(result, i)
                if checked is False:
                    raise AssertionError("result validation returned False")
            except Exception as failure:
                if elapsed is None:
                    elapsed = (time.perf_counter_ns() - tick) / 1e6
                error = f"{type(failure).__name__}: {failure}"
                failures.append({"operation": i, "error": error})
            self.writer.writerow(dict(engine=engine, workload=workload, repeat=repeat,
                                      operation=i, latency_ms=elapsed, success=not error, error=error))
            if not error:
                samples.append(elapsed)
            if len(failures) >= 3:
                break
        wall = time.perf_counter() - started
        require_idle()
        self.stream.flush()
        summary = dict(engine=engine, workload=workload, repeat=repeat,
                       requested=count, attempts=len(samples) + len(failures), successes=len(samples),
                       errors=len(failures), failures=failures, elapsed_seconds=wall,
                       requests_per_second=len(samples) / wall,
                       p50_ms=percentile(samples, .5), p95_ms=percentile(samples, .95), p99_ms=percentile(samples, .99),
                       min_ms=min(samples) if samples else None, max_ms=max(samples) if samples else None,
                       mean_ms=statistics.mean(samples) if samples else None,
                       correctness_status="failed" if failures else "passed", metadata=metadata)
        self.summaries.append(summary)
        (self.output / "domain-results.json").write_text(json.dumps(self.summaries, indent=2, allow_nan=False) + "\n")
        print(json.dumps({k: summary[k] for k in ("engine", "workload", "repeat", "successes", "errors", "p50_ms")}), flush=True)
        if failures:
            raise RuntimeError(f"invalid benchmark section {engine}/{workload}: {failures}")
        return summary

    def close(self):
        self.stream.close()
