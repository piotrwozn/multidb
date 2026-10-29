#!/usr/bin/env python3
"""Qualify a release's strict Tensor profile on a real POSIX GPU host.

This functional qualification never accepts a CPU/software fallback. It creates
only a fresh disposable database. Device-reset/driver-loss injection and the
platform support matrix remain separate acceptance records.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import platform
import secrets
import signal
import socket
import struct
import subprocess
import time


class ApiFailure(RuntimeError):
    def __init__(self, status, payload):
        self.status, self.payload = status, payload
        super().__init__(f"HTTP {status}: {payload!r}")


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, document):
    path.write_text(json.dumps(document, indent=2, allow_nan=False) + "\n")


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def api(port, token, method, path, body=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    try:
        encoded = None if body is None else json.dumps(body, allow_nan=False).encode()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        connection.request(method, path, encoded, headers)
        response = connection.getresponse()
        raw = response.read(16 * 1024 * 1024 + 1)
        if len(raw) > 16 * 1024 * 1024:
            raise AssertionError("qualification response exceeds its byte limit")
        payload = json.loads(raw)
        if response.status != 200 or payload.get("ok") is not True:
            raise ApiFailure(response.status, payload)
        return payload["data"]
    finally:
        connection.close()


def require_gpu(result, adapters):
    execution = result.get("execution", {})
    if execution.get("device") != "gpu" or not execution.get("adapter") or not execution.get("backend"):
        raise AssertionError(f"physical-GPU execution evidence is missing: {execution!r}")
    name = execution["adapter"].casefold()
    if any(term in name for term in ("swiftshader", "llvmpipe", "lavapipe", "software", "basic render")):
        raise AssertionError(f"software GPU adapter is ineligible: {execution!r}")
    # The strict Tensor profile additionally rejects wgpu Cpu/Other adapters.
    adapters.append(execution)


def require_close(actual, expected):
    if len(actual) != len(expected) or any(not math.isfinite(value) or not math.isclose(value, reference, rel_tol=2e-4, abs_tol=2e-5)
                                           for value, reference in zip(actual, expected)):
        raise AssertionError(f"GPU/reference mismatch: {actual!r} versus {expected!r}")


def f32(value):
    return struct.unpack("f", struct.pack("f", value))[0]


def tensor(shape, dtype, values):
    return {"shape": shape, "data": {dtype: values}}


def require_tensors(actual, expected):
    if actual.keys() != expected.keys():
        raise AssertionError("named tensor columns differ")
    for name, samples in expected.items():
        if len(actual[name]) != len(samples):
            raise AssertionError(f"tensor sample count differs: {name}")
        for observed, reference in zip(actual[name], samples):
            if observed["shape"] != reference["shape"] or observed["data"].keys() != reference["data"].keys():
                raise AssertionError(f"tensor shape or dtype differs: {name}")
            dtype, values = next(iter(reference["data"].items()))
            observed_values = observed["data"][dtype]
            # JSON may print an f32 as its shortest round-tripping decimal.
            # Compare storage precision, rather than Python's f64 parse of it.
            if dtype == "f32":
                values = [f32(value) for value in values]
                observed_values = [f32(value) for value in observed_values]
            if observed_values != values:
                raise AssertionError(f"tensor values differ: {name} ({dtype})")


def fixture():
    columns = [
        {"name": "embedding", "dtype": "f32", "shape": [3]},
        {"name": "mixed_scale", "dtype": "f32", "shape": [2]},
        {"name": "ragged", "dtype": "f32", "shape": [None]},
        {"name": "image", "dtype": "u8", "shape": [None, None, 3]},
        {"name": "precise", "dtype": "f64", "shape": [2]},
        {"name": "integers", "dtype": "i64", "shape": [2]},
        {"name": "flags", "dtype": "bool", "shape": [2]},
        {"name": "caption", "dtype": "utf8", "shape": []},
    ]
    vectors = [[3., 4., 0.], [0., 0., 0.], [1., 2., 2.], [-3., -4., 0.]]
    ragged = [[1., 2., 3.], [3., 4.], [], [-1., -2.]]
    mixed = [[f32(1e20), 1.], [f32(1e20), 0.], [f32(-1e20), 4.], [0., 0.]]
    images = [([1, 1, 3], [255, 0, 0]), ([1, 2, 3], list(range(6))), ([0, 0, 3], []), ([1, 1, 3], [1, 2, 3])]
    batch = {
        "embedding": [tensor([3], "f32", values) for values in vectors],
        "mixed_scale": [tensor([2], "f32", values) for values in mixed],
        "ragged": [tensor([len(values)], "f32", values) for values in ragged],
        "image": [tensor(shape, "u8", values) for shape, values in images],
        "precise": [tensor([2], "f64", [index + .25, index + .5]) for index in range(4)],
        "integers": [tensor([2], "i64", [index, -index]) for index in range(4)],
        "flags": [tensor([2], "bool", [index % 2 == 0, index % 2 == 1]) for index in range(4)],
        "caption": [tensor([], "utf8", [f"Zażółć 🌈 {index}"]) for index in range(4)],
    }
    return columns, batch, vectors, mixed, ragged, images


def start(binary, environment, output, port, token):
    log = (output / "server.log").open("ab")
    process = subprocess.Popen([str(binary), "serve"], env=environment, stdout=log,
                               stderr=subprocess.STDOUT, cwd=output, start_new_session=True)
    deadline = time.monotonic() + 60
    try:
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"strict Tensor-profile startup failed ({process.returncode}); see server.log")
            try:
                api(port, token, "GET", "/api/status")
                return process, log
            except (OSError, ApiFailure, json.JSONDecodeError):
                time.sleep(.1)
        raise TimeoutError("strict Tensor-profile startup exceeded 60 seconds")
    except BaseException:
        try:
            if process.poll() is None:
                stop(process)
        finally:
            log.close()
        raise


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
            raise RuntimeError("GPU qualification server required forced shutdown")
    if process.returncode != 0:
        raise RuntimeError(f"GPU qualification server exited with {process.returncode}")


def qualify(args):
    if os.name != "posix":
        raise ValueError("this release-API harness requires POSIX process signals; record Windows GPU tests separately")
    binary, output = args.binary.resolve(), args.output.resolve()
    if not binary.is_file() or output.exists() or min(args.requests, args.concurrency) < 1:
        raise ValueError("an existing release binary, new output directory and positive counts are required")
    output.mkdir(parents=True, mode=0o700)
    runtime = output / "runtime"
    runtime.mkdir(mode=0o700)
    port, pg_port = free_port(), free_port()
    while pg_port == port:
        pg_port = free_port()
    token = secrets.token_urlsafe(32)
    environment = {name: value for name, value in os.environ.items() if not name.startswith("MULTIDB_")}
    environment.update(MULTIDB_RUNTIME_MODE="local-dev", MULTIDB_PROFILE="tensor",
                       MULTIDB_DB_PATH=str(output / "tensor.redb"), MULTIDB_BIND=f"127.0.0.1:{port}",
                       MULTIDB_PG_BIND=f"127.0.0.1:{pg_port}", MULTIDB_PG_TLS_MODE="disabled",
                       MULTIDB_PG_USER="gpu_qualification", MULTIDB_PG_PASSWORD=secrets.token_urlsafe(32),
                       MULTIDB_ADMIN_TOKEN=token, MULTIDB_ADMIN_PASSWORD=secrets.token_urlsafe(32),
                       XDG_RUNTIME_DIR=str(runtime))
    evidence = {"schema_version": 1, "kind": "strict_physical_gpu_release_api_qualification",
                "source_revision": args.revision, "binary_sha256": sha256(binary), "platform": platform.platform(),
                "python": platform.python_version(), "profile": "tensor", "software_fallback_allowed": False,
                "tests": [], "adapters": [], "complete_production_qualification": False,
                "separate_acceptance_required": ["driver/device-loss injection", "all supported physical adapters and OSes", "production-duration load and memory-pressure qualification"]}
    write_json(output / "run.json", {key: value for key, value in evidence.items() if key not in ("tests", "adapters")})
    process, log = None, None
    try:
        process, log = start(binary, environment, output, port, token)
        columns, batch, vectors, mixed, ragged, images = fixture()
        call = lambda method, path, body=None: api(port, token, method, path, body)
        path = "/api/data/tensor-datasets/qualification"
        call("POST", "/api/builder/tensor-dataset", {"name": "qualification", "chunk_samples": 2, "tensors": columns})
        appended = call("POST", path + "/append", {"tensors": batch})
        if (appended["start"], appended["end"]) != (0, 4):
            raise AssertionError("typed append interval differs")
        revision = appended["revision"]
        read = call("POST", path + "/read", {"start": 0, "limit": 4, "revision": revision})
        require_tensors(read["tensors"], batch)
        if read["total_samples"] != 4:
            raise AssertionError("named dtype/shape/Unicode roundtrip differs")
        evidence["tests"].append("all six storage dtypes; fixed, ragged, empty and multimodal shapes; Unicode")

        def search(column, query, metric="L2", expected_vectors=vectors):
            result = call("POST", path + "/search", {"tensor": column, "query": query, "metric": metric, "k": 4, "max_samples": 4, "revision": revision})
            require_gpu(result, evidence["adapters"])
            expected = []
            for index, vector in enumerate(expected_vectors):
                if metric == "Cosine" and not any(vector):
                    continue
                distance = math.dist(vector, query) if metric == "L2" else max(0., min(2., 1. - sum(a*b for a,b in zip(vector, query)) / (math.hypot(*vector) * math.hypot(*query))))
                expected.append((distance, index))
            expected.sort()
            if [hit["index"] for hit in result["hits"]] != [index for _, index in expected]:
                raise AssertionError("GPU exact top-k/tie order differs from CPU oracle")
            require_close([hit["distance"] for hit in result["hits"]], [distance for distance, _ in expected])
            if result["revision"] != revision or result["scanned_samples"] != 4:
                raise AssertionError("GPU search snapshot/coverage differs")
            return result

        search("embedding", [0., 0., 0.])
        cosine = search("embedding", [3., 4., 0.], "Cosine")
        if cosine["skipped_samples"] != 1:
            raise AssertionError("zero cosine row was not explicitly skipped")
        search("mixed_scale", [f32(1e20), 0.], expected_vectors=mixed)
        evidence["tests"].append("GPU exact L2/cosine, zero cosine rows, deterministic ties and mixed-scale coordinates against CPU references")

        for column, rows in (("ragged", ragged), ("image", [values for _, values in images])):
            for operation in ("sum", "l2_norm", "mean"):
                selected = rows[:2] if operation == "mean" else rows
                result = call("POST", path + "/compute", {"tensor": column, "operation": operation, "start": 0, "limit": len(selected), "revision": revision})
                require_gpu(result, evidence["adapters"])
                expected = [math.fsum(row) if operation == "sum" else (math.hypot(*row) if operation == "l2_norm" else math.fsum(row)/len(row)) for row in selected]
                require_close(result["values"], expected)
        evidence["tests"].append("GPU F32/U8 sum, mean and L2 reductions over ragged/multidimensional/empty rows")

        for route, body, status in [
            ("/compute", {"tensor": "ragged", "operation": "mean", "limit": 4}, 400),
            ("/compute", {"tensor": "integers", "operation": "sum", "limit": 4}, 422),
            ("/search", {"tensor": "precise", "query": [1., 0.], "metric": "L2"}, 422),
            ("/search", {"tensor": "embedding", "query": [1e-100, 0., 0.], "metric": "L2"}, 400),
            ("/search", {"tensor": "embedding", "query": [1e-45, 0., 0.], "metric": "L2"}, 422),
            ("/read", {"limit": 1025}, 400),
        ]:
            try:
                call("POST", path + route, {**body, "revision": revision})
            except ApiFailure as error:
                if error.status != status:
                    raise
            else:
                raise AssertionError(f"unsupported/invalid GPU request was accepted: {route}")
        evidence["tests"].append("unsupported dtype/underflow, empty mean and read limits explicitly reject without CPU fallback")

        call("POST", path + "/append", {"tensors": batch})
        pinned = call("POST", path + "/read", {"start": 0, "limit": 8, "revision": revision})
        require_tensors(pinned["tensors"], batch)
        if pinned["end"] != 4 or pinned["total_samples"] != 4:
            raise AssertionError("immutable revision changed after append")
        with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            list(pool.map(lambda _: search("embedding", [0., 0., 0.]), range(args.requests)))
        evidence["tests"].append(f"{args.requests} correct serialized GPU dispatches under {args.concurrency} concurrent API clients; immutable snapshot after append")
        stop(process)
        process = None
        log.close()
        log = None
        process, log = start(binary, environment, output, port, token)
        restored = call("POST", path + "/read", {"start": 0, "limit": 8})
        require_tensors(restored["tensors"], {name: values + values for name, values in batch.items()})
        search("embedding", [0., 0., 0.])
        evidence["tests"].append("real graceful restart preserves typed data, immutable revisions and strict GPU execution")
        stop(process)
        process = None
        log.close()
        log = None
        if sha256(binary) != evidence["binary_sha256"]:
            raise AssertionError("release binary changed during qualification")
        evidence["adapters"] = list({json.dumps(adapter, sort_keys=True): adapter for adapter in evidence["adapters"]}.values())
        evidence["status"] = "passed"
        write_json(output / "result.json", evidence)
    except BaseException as failure:
        evidence.update(status="failed", error=f"{type(failure).__name__}: {failure}")
        write_json(output / "result.json", evidence)
        raise
    finally:
        if process is not None and process.poll() is None:
            stop(process)
        if log is not None:
            log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--requests", type=int, default=200)
    parser.add_argument("--concurrency", type=int, default=4)
    qualify(parser.parse_args())
