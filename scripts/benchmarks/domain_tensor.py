"""Typed tensor benchmarks: MultiDB HTTP CPU reference versus DeepLake 4.7.4 local.

All datasets are fresh. Appends include a durable API commit on each backend;
DeepLake's commit API and redb Immediate durability are reported separately.
Search is exact CPU L2 on both sides. No GPU measurements are claimed here.
"""
from __future__ import annotations

import json
import math
from pathlib import Path

from domain_common import Http


BATCH = 64
READ_BATCH = 32
IMAGE_SHAPE = (8, 8, 3)
DIMENSION = 16


def sample(index, np):
    # Binary-exact coordinates give both f32 and f64 searches the same tie order.
    embedding = np.array([index * (j + 1) / 128 for j in range(DIMENSION)], dtype=np.float32)
    return {
        "text": f"sample-{index:012d}",
        "image": np.full(IMAGE_SHAPE, index % 251, dtype=np.uint8),
        "embedding": embedding,
    }


def check_rows(actual, start, count, np, *, projected=False):
    if len(actual) != count:
        raise AssertionError(f"expected {count} tensor rows, got {len(actual)}")
    for offset, row in enumerate(actual):
        expected = sample(start + offset, np)
        columns = {"text", "embedding"} if projected else set(expected)
        if set(row) != columns or row["text"] != expected["text"]:
            raise AssertionError("tensor projection/text provenance mismatch")
        for column in columns - {"text"}:
            value = row[column]
            if value.dtype != expected[column].dtype or value.shape != expected[column].shape:
                raise AssertionError(f"wrong tensor dtype/shape: {column}")
            if not np.array_equal(value, expected[column]):
                raise AssertionError(f"wrong source tensor value: {column}")


class MultiDb:
    def __init__(self, http, name, np):
        self.http, self.np, self.name = http, np, name
        self.path = f"/api/data/tensor-datasets/{name}"
        self.revision = None
        self.snapshot_samples = None
        self.count = 0
        http.json("POST", "/api/builder/tensor-dataset", {
            "name": name, "chunk_samples": BATCH,
            "tensors": [
                {"name": "text", "dtype": "utf8", "shape": []},
                {"name": "image", "dtype": "u8", "shape": list(IMAGE_SHAPE)},
                {"name": "embedding", "dtype": "f32", "shape": [DIMENSION]},
            ],
        })

    def append(self, rows):
        tensors = {"text": [], "image": [], "embedding": []}
        for row in rows:
            tensors["text"].append({"shape": [], "data": {"utf8": [row["text"]]}})
            for column, dtype in (("image", "u8"), ("embedding", "f32")):
                value = row[column]
                tensors[column].append({"shape": list(value.shape), "data": {dtype: value.ravel().tolist()}})
        result = self.http.json("POST", self.path + "/append", {"tensors": tensors})
        if result["start"] != self.count or result["end"] != self.count + len(rows):
            raise AssertionError("MultiDB append returned an inconsistent sample interval")
        self.count = result["end"]
        return {"samples": self.count, "revision": result["revision"]}

    def snapshot(self):
        info = self.http.json("GET", self.path)
        if info["samples"] != self.count:
            raise AssertionError("MultiDB snapshot metadata disagrees with committed appends")
        self.revision = info["revision"]
        self.snapshot_samples = info["samples"]
        return self.revision

    def read(self, start, count, *, projected=False):
        columns = ["text", "embedding"] if projected else ["text", "image", "embedding"]
        body = {"start": start, "limit": count, "columns": columns}
        if self.revision is not None:
            body["revision"] = self.revision
        result = self.http.json("POST", self.path + "/read", body)
        if result["start"] != start or result["end"] != start + count:
            raise AssertionError("MultiDB read returned an inconsistent sample interval")
        if self.revision is not None and result["revision"] != self.revision:
            raise AssertionError("MultiDB read changed the selected snapshot")
        if self.snapshot_samples is not None and result["total_samples"] != self.snapshot_samples:
            raise AssertionError("MultiDB read exposed later snapshot metadata")
        tensors = result["tensors"]
        if set(tensors) != set(columns) or any(len(values) != count for values in tensors.values()):
            raise AssertionError("MultiDB returned extra or unaligned tensor columns")
        rows = []
        for position in range(count):
            text = tensors["text"][position]
            if text["shape"] != [] or set(text["data"]) != {"utf8"} or len(text["data"]["utf8"]) != 1:
                raise AssertionError("MultiDB scalar text lost its native tensor type")
            row = {"text": text["data"]["utf8"][0]}
            for column, dtype, shape in (("image", "u8", IMAGE_SHAPE), ("embedding", "f32", (DIMENSION,))):
                if column not in columns:
                    continue
                tensor = tensors[column][position]
                if tensor["shape"] != list(shape) or set(tensor["data"]) != {dtype}:
                    raise AssertionError(f"MultiDB changed {column} dtype or shape")
                row[column] = self.np.asarray(tensor["data"][dtype], dtype="uint8" if dtype == "u8" else "float32").reshape(shape)
            rows.append(row)
        return rows

    def search(self, query, count):
        result = self.http.json("POST", self.path + "/search", {
            "tensor": "embedding", "query": query.tolist(), "metric": "L2",
            "k": min(10, count), "max_samples": count, "revision": self.revision,
        })
        if result["execution"]["device"] != "cpu" or result["scanned_samples"] != count:
            raise AssertionError("benchmark requires a complete explicit CPU reference search")
        if result["revision"] != self.revision or result["total_samples"] != count:
            raise AssertionError("MultiDB search changed the selected snapshot")
        return result["hits"]


class DeepLake:
    def __init__(self, path, deeplake, np):
        if path.exists():
            raise ValueError("DeepLake benchmark path must be fresh")
        self.np = np
        self.dataset = deeplake.create(str(path), schema={
            "text": deeplake.types.Text(),
            "image": deeplake.types.Array(deeplake.types.UInt8(), list(IMAGE_SHAPE)),
            "embedding": deeplake.types.Array(deeplake.types.Float32(), [DIMENSION]),
        })
        self.view = self.dataset

    def append(self, rows):
        self.dataset.append(rows)
        self.dataset.commit("tensor benchmark append")
        return {"samples": len(self.dataset), "revision": self.dataset.version}

    def snapshot(self):
        tag = self.dataset.tag("benchmark_snapshot")
        self.view = tag.open()
        return tag.version

    def read(self, start, count, *, projected=False):
        columns = ["text", "embedding"] if projected else ["text", "image", "embedding"]
        selection = self.view.query("SELECT " + ", ".join(columns))[start:start + count]
        # Force all native reads inside the operation timer, including lazy columns.
        values = {column: selection[column] for column in columns}
        return [{column: values[column][i] for column in columns} for i in range(count)]

    def search(self, query, count):
        literal = "ARRAY[" + ",".join(map(repr, query.tolist())) + "]"
        distance = f"L2_NORM(embedding - {literal})"
        result = self.view.query(f"SELECT text, {distance} AS distance ORDER BY {distance}, text LIMIT {min(10, count)}")
        names, distances = result["text"][:], result["distance"][:]
        return [{"index": int(name.removeprefix("sample-")), "distance": float(value)} for name, value in zip(names, distances)]


def oracle(rows, targets, np):
    vectors = np.arange(rows, dtype=np.float64)[:, None] * np.arange(1, DIMENSION + 1, dtype=np.float64)[None, :] / 128
    indices = np.arange(rows)
    answers = []
    for target in targets:
        query = sample(target, np)["embedding"]
        distances = np.linalg.norm(vectors - query.astype(np.float64), axis=1)
        ordered = np.lexsort((indices, distances))[:min(10, rows)]
        answers.append((query, [(int(i), float(distances[i])) for i in ordered]))
    return answers


def check_hits(actual, expected):
    if len(actual) != len(expected):
        raise AssertionError("exact tensor search returned the wrong top-k count")
    for hit, (index, distance) in zip(actual, expected):
        if hit["index"] != index or not math.isclose(hit["distance"], distance, rel_tol=2e-4, abs_tol=2e-5):
            raise AssertionError(f"exact search disagrees with independent NumPy oracle: {actual!r}")


def run(args, recorder):
    import deeplake
    import numpy as np

    if deeplake.__version__ != "4.7.4":
        raise ValueError("this reviewed adapter requires DeepLake 4.7.4")
    if any(not 1 <= rows <= 100_000 for rows in args.rows):
        raise ValueError("tensor benchmark sizes must be 1..100000 samples")
    directory = Path(args.output) / "tensor-datasets"
    directory.mkdir(exist_ok=False)
    metadata = {"execution_mode": "cpu_reference", "gpu_measured": False,
                "deeplake_version": deeplake.__version__, "numpy_version": np.__version__,
                "image_shape": IMAGE_SHAPE, "embedding_dimension": DIMENSION,
                "fixture": "binary-exact linearly spaced 16-dimensional vectors, deterministic RGB8 images and indexed source text",
                "append_batch_samples": BATCH, "distance_metric": "L2", "exact": True,
                "comparison_boundary": "MultiDB authenticated HTTP/JSON versus embedded DeepLake Python; durability semantics differ"}
    with (directory / "provenance.json").open("x") as output:
        json.dump(metadata, output, indent=2)
    with Http() as http:
        for rows in args.rows:
            answers = oracle(rows, [(i * 997) % rows for i in range(min(8, args.ops))], np)
            for repeat in range(1, args.repeats + 1):
                adapters = (
                    ("multidb", MultiDb(http, f"bench_tensor_n{rows}_r{repeat}", np), "redb Immediate commit; default durable audit"),
                    ("deeplake", DeepLake(directory / f"deeplake_n{rows}_r{repeat}", deeplake, np), "DeepLake commit() per append; no fsync equivalence claimed"),
                )
                for engine, adapter, durability in adapters:
                    details = {**metadata, "dataset_rows": rows, "durability": durability}
                    def append_batch(i):
                        return adapter.append([sample(j, np) for j in range(i * BATCH, min(rows, (i + 1) * BATCH))])
                    recorder.measure(engine, f"tensor_append_batch_cpu_reference_n{rows}", repeat,
                                     math.ceil(rows / BATCH), append_batch,
                                     lambda result, i: result["samples"] == min(rows, (i + 1) * BATCH), **details)
                    # Full source/type verification is outside every measurement section.
                    for start in range(0, rows, 128):
                        count = min(128, rows - start)
                        check_rows(adapter.read(start, count), start, count, np)
                    revision = adapter.snapshot()
                    adapter.append([sample(rows, np)])
                    check_rows(adapter.read(rows - 1, 1), rows - 1, 1, np)
                    if engine == "deeplake" and len(adapter.view) != rows:
                        raise AssertionError("DeepLake tag did not pin the append snapshot")
                    count = min(READ_BATCH, rows)
                    positions = [(i * 37) % (rows - count + 1) for i in range(args.ops)]
                    recorder.measure(engine, f"tensor_projected_read_cpu_reference_n{rows}", repeat, args.ops,
                                     lambda i: adapter.read(positions[i], count, projected=True),
                                     lambda result, i: check_rows(result, positions[i], count, np, projected=True),
                                     snapshot_revision=revision, projected_columns=["text", "embedding"], **details)
                    recorder.measure(engine, f"tensor_exact_l2_search_cpu_reference_n{rows}", repeat, args.ops,
                                     lambda i: adapter.search(answers[i % len(answers)][0], rows),
                                     lambda result, i: check_hits(result, answers[i % len(answers)][1]),
                                     snapshot_revision=revision, k=min(10, rows), **details)
