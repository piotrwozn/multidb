"""Authenticated HTTP vector/graph operations against pinned local competitors.

ANN answers are checked against NumPy exact L2 outside operation latency timers.
Graph answers are checked against an independently generated reachable-node set.
Fixture creation and index construction are outside query timings. Durable edge
updates are a separate workload; transport, audit and storage settings are retained
in metadata and must be considered when comparing end-to-end measurements.
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
from pathlib import Path
import time

from domain_common import Http

SEED = 20261028
DIMENSIONS = 128
K = 10
EF_SEARCH = 256
OFFSETS = (1, 7, 31, 127)
PASSWORD = "benchmark-pass-only"


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")


def exact_neighbors(vectors, queries, k=K, selected=None):
    """Float64 reference over the exact float32 coordinates ingested by both servers."""
    import numpy as np
    candidates = np.arange(len(vectors)) if selected is None else np.asarray(selected)
    data = vectors[candidates].astype(np.float64)
    answers = []
    for query in queries:
        delta = data - query.astype(np.float64)
        squared = np.einsum("ij,ij->i", delta, delta)
        ordered = np.lexsort((candidates, squared))[:k]
        answers.append([int(index) for index in candidates[ordered]])
    return answers


def validate_vector_hits(result, expected, vectors, query, recall, *, qdrant=False):
    hits = result.get("result") if qdrant else result.get("hits")
    require(isinstance(hits, list) and len(hits) == len(expected), "ANN hit count differs")
    indices = []
    for hit in hits:
        metadata = hit.get("payload") if qdrant else hit.get("metadata")
        require(isinstance(metadata, dict) and type(metadata.get("index")) is int, "ANN hit lacks fixture identity")
        index = metadata["index"]
        require(0 <= index < len(vectors), "ANN returned an unknown document")
        if qdrant:
            require(hit.get("id") == index + 1, "Qdrant id does not match its payload")
        distance = hit.get("score") if qdrant else hit.get("distance")
        require(type(distance) in (int, float) and math.isfinite(distance) and distance >= 0, "invalid L2 distance")
        reference = math.sqrt(sum((float(left) - float(right)) ** 2 for left, right in zip(vectors[index], query)))
        require(math.isclose(distance, reference, rel_tol=2e-5, abs_tol=2e-5), "ANN returned a different distance metric")
        indices.append(index)
    require(len(set(indices)) == len(indices), "ANN returned duplicate documents")
    actual_recall = len(set(indices).intersection(expected)) / len(expected)
    require(actual_recall >= .8, f"ANN recall@{len(expected)} below .8: {actual_recall}")
    recall["queries"] += 1
    recall["sum"] += actual_recall
    recall["mean"] = recall["sum"] / recall["queries"]
    recall["minimum"] = min(recall["minimum"], actual_recall)


def wait_qdrant_index(http, name, size):
    deadline = time.monotonic() + 240
    while True:
        result = http.json("GET", f"/collections/{name}").get("result", {})
        if result.get("status") == "green" and result.get("points_count") == size and result.get("indexed_vectors_count", 0) >= size:
            return result
        if time.monotonic() >= deadline:
            raise TimeoutError(f"Qdrant HNSW index not ready for {size} vectors: {result!r}")
        time.sleep(.25)


def require_http_400(http, path, body):
    try:
        http.json("POST", path, body)
    except RuntimeError as error:
        require(str(error).startswith("HTTP400 "), f"expected explicit 400, got {error}")
    else:
        raise AssertionError("unsupported search options were silently accepted")


def vector_robustness(multidb, qdrant, name, vectors, queries):
    # A server-side filter must never be silently ignored. MultiDB's HTTP endpoint
    # currently rejects this unsupported surface; do not substitute client filtering.
    require_http_400(multidb, f"/data/vectors/{name}/search", {
        "vector": queries[0].tolist(), "k": K, "filter": {"tag": "keep"},
    })
    selected = list(range(0, len(vectors), 5))
    expected = exact_neighbors(vectors, queries[:1], selected=selected)[0]
    filtered = qdrant.json("POST", f"/collections/{name}/points/search", {
        "vector": queries[0].tolist(), "limit": K, "with_payload": True,
        "params": {"hnsw_ef": EF_SEARCH},
        "filter": {"must": [{"key": "tag", "match": {"value": "keep"}}]},
    })
    require(all(hit.get("payload", {}).get("tag") == "keep" for hit in filtered.get("result", [])), "Qdrant filter leaked an excluded payload")
    stats = {"queries": 0, "sum": 0., "mean": 0., "minimum": 1.}
    validate_vector_hits(filtered, expected, vectors, queries[0], stats, qdrant=True)
    return {"timed": False, "multidb_filtered_http": "explicit400_unsupported", "qdrant_filtered_recall_at_10": stats["mean"]}


def extreme_cosine_robustness(multidb, qdrant, suffix):
    name = f"domain_extreme_{suffix}"
    multidb.json("POST", "/builder/vector", {"name": name, "dim": 2, "metric": "Cosine"})
    qdrant.json("PUT", f"/collections/{name}", {"vectors": {"size": 2, "distance": "Cosine"}})
    magnitudes = (3.4028234663852886e38, 1.1754943508222875e-38, 1.401298464324817e-45)
    points = []
    for index, magnitude in enumerate(magnitudes):
        multidb.json("POST", f"/data/vectors/{name}/vectors", {"metadata": {"direction": "along"}, "vector": [magnitude, 0.]})
        multidb.json("POST", f"/data/vectors/{name}/vectors", {"metadata": {"direction": "opposite"}, "vector": [-magnitude, 0.]})
        points.extend([
            {"id": index * 2 + 1, "payload": {"direction": "along"}, "vector": [magnitude, 0.]},
            {"id": index * 2 + 2, "payload": {"direction": "opposite"}, "vector": [-magnitude, 0.]},
        ])
    qdrant_status = "passed"
    try:
        qdrant.json("PUT", f"/collections/{name}/points?wait=true", {"points": points})
    except Exception as failure:
        qdrant_status = f"rejected: {type(failure).__name__}: {failure}"
    for magnitude in magnitudes:
        result = multidb.json("POST", f"/data/vectors/{name}/search", {"vector": [magnitude, 0.], "k": 6})
        hits = result.get("hits", [])
        require(len(hits) == 6, "MultiDB lost finite extreme cosine vectors")
        require([hit.get("distance") for hit in hits] == [0., 0., 0., 2., 2., 2.], "MultiDB extreme cosine distances differ")
        if qdrant_status == "passed":
            try:
                result = qdrant.json("POST", f"/collections/{name}/points/search", {"vector": [magnitude, 0.], "limit": 6, "with_payload": True, "params": {"exact": True}})
                hits = result.get("result", [])
                require(len(hits) == 6 and all(math.isfinite(hit["score"]) for hit in hits), "nonfinite/missing scores")
                require(all(math.isclose(hit["score"], 1. if hit["payload"]["direction"] == "along" else -1., abs_tol=2e-5) for hit in hits), "extreme cosine similarity differs")
            except Exception as failure:
                qdrant_status = f"failed: {type(failure).__name__}: {failure}"
    return {"timed": False, "multidb_extreme_cosine": "passed", "qdrant_extreme_cosine": qdrant_status}


def extreme_l2_robustness(multidb, qdrant, suffix):
    name = f"domain_extreme_l2_{suffix}"
    multidb.json("POST", "/builder/vector", {"name": name, "dim": 2, "metric": "L2"})
    qdrant.json("PUT", f"/collections/{name}", {"vectors": {"size": 2, "distance": "Euclid"}})
    vectors = [[1e20, 0.], [3e20, 4e20]]
    for index, vector in enumerate(vectors):
        multidb.json("POST", f"/data/vectors/{name}/vectors", {"metadata": {"index": index}, "vector": vector})
    result = multidb.json("POST", f"/data/vectors/{name}/search", {"vector": [0., 0.], "k": 2})
    require(len(result.get("hits", [])) == 2, "MultiDB lost large finite L2 vectors")
    require(all(type(hit.get("distance")) in (int, float) and math.isfinite(hit["distance"]) and math.isclose(hit["distance"], expected, rel_tol=2e-5)
                for hit, expected in zip(result["hits"], [1e20, 5e20])), "MultiDB overflowed a representable L2 distance")
    qdrant_status = "passed"
    try:
        qdrant.json("PUT", f"/collections/{name}/points?wait=true", {"points": [{"id": index + 1, "vector": vector} for index, vector in enumerate(vectors)]})
        response = qdrant.json("POST", f"/collections/{name}/points/search", {"vector": [0., 0.], "limit": 2, "params": {"exact": True}})
        hits = response.get("result", [])
        require(len(hits) == 2 and all(type(hit.get("score")) in (int, float) and math.isfinite(hit["score"]) and math.isclose(hit["score"], expected, rel_tol=2e-5)
                                     for hit, expected in zip(hits, [1e20, 5e20])), "representable L2 distance overflowed")
    except Exception as failure:
        qdrant_status = f"failed: {type(failure).__name__}: {failure}"
    maximum = 3.4028234663852886e38
    multidb.json("POST", f"/data/vectors/{name}/vectors", {"metadata": {"index": 2}, "vector": [maximum, maximum]})
    require_http_400(multidb, f"/data/vectors/{name}/search", {"vector": [0., 0.], "k": 3})
    return {"timed": False, "multidb_large_representable_l2": "passed", "multidb_unrepresentable_l2": "explicit400", "qdrant_large_representable_l2": qdrant_status}


def run_vectors(args, recorder, multidb, suffix, report):
    import numpy as np
    sizes = [1000, 10000]
    with Http("http://127.0.0.1:16333", headers={"api-key": PASSWORD}, envelope=False) as qdrant:
        version = qdrant.json("GET", "/").get("version")
        require(version == "1.16.2", f"Qdrant 1.16.2 required, got {version!r}")
        report["vector_sizes"] = sizes
        report["numpy_version"] = np.__version__
        report["extreme_cosine"] = extreme_cosine_robustness(multidb, qdrant, suffix)
        report["extreme_l2"] = extreme_l2_robustness(multidb, qdrant, suffix)
        for size in sizes:
            rng = np.random.default_rng(SEED)
            vectors = rng.normal(0., .25, (size, DIMENSIONS)).astype(np.float32)
            queries = rng.normal(0., .25, (args.ops, DIMENSIONS)).astype(np.float32)
            expected = exact_neighbors(vectors, queries)
            name = f"domain_vector_{suffix}_{size}"
            multidb.json("POST", "/builder/vector", {"name": name, "dim": DIMENSIONS, "metric": "L2", "hnsw": {"m": 16, "ef_construction": 128, "ef_search": EF_SEARCH}})
            qdrant.json("PUT", f"/collections/{name}", {
                "vectors": {"size": DIMENSIONS, "distance": "Euclid", "on_disk": False},
                "hnsw_config": {"m": 16, "ef_construct": 128, "full_scan_threshold": 10},
                "optimizers_config": {"default_segment_number": 1, "indexing_threshold": 1},
            })
            for index, vector in enumerate(vectors):
                multidb.json("POST", f"/data/vectors/{name}/vectors", {"metadata": {"index": index, "tag": "keep" if index % 5 == 0 else "omit"}, "vector": vector.tolist()})
            # Native fixture ingestion can exceed Qdrant's HTTP keepalive
            # timeout. Start a fresh untimed connection; measured operations
            # never retry a disconnected request.
            qdrant.close()
            for start in range(0, size, 128):
                points = [{"id": index + 1, "payload": {"index": index, "tag": "keep" if index % 5 == 0 else "omit"}, "vector": vectors[index].tolist()} for index in range(start, min(size, start + 128))]
                qdrant.json("PUT", f"/collections/{name}/points?wait=true", {"points": points})
            info = wait_qdrant_index(qdrant, name, size)
            report[f"vector_robustness_n{size}"] = vector_robustness(multidb, qdrant, name, vectors, queries)
            metadata = dict(dataset_vectors=size, dimensions=DIMENSIONS, seed=SEED, metric="L2 Euclidean", k=K,
                            m=16, ef_construction=128, ef_search=EF_SEARCH, fixture_load_timed=False,
                            qdrant_version=version, qdrant_indexed_vectors=info["indexed_vectors_count"],
                            qdrant_full_scan_threshold_kb=10,
                            protocol="authenticated loopback HTTP JSON", validation="NumPy float64 exact reference; per-query recall>=.8 and mean>=.95; exact returned distances",
                            caveat="different server audit, WAL and protocol implementations; end-to-end warm ANN comparison")
            for repeat in range(args.repeats):
                engines = ("multidb_http", "qdrant_http") if repeat % 2 == 0 else ("qdrant_http", "multidb_http")
                for engine in engines:
                    # Warm the same query outside the measured section on each server.
                    def operation(index, selected=engine):
                        query = queries[index].tolist()
                        if selected == "multidb_http":
                            return multidb.json("POST", f"/data/vectors/{name}/search", {"vector": query, "k": K})
                        return qdrant.json("POST", f"/collections/{name}/points/search", {"vector": query, "limit": K, "with_payload": True, "params": {"hnsw_ef": EF_SEARCH, "exact": False}})
                    operation(0)
                    stats = {"queries": 0, "sum": 0., "mean": 0., "minimum": 1.}
                    def verify(result, index, selected=engine):
                        validate_vector_hits(result, expected[index], vectors, queries[index], stats, qdrant=selected == "qdrant_http")
                        # The last timed attempt must fail validation too, rather than
                        # leaving an apparently valid section in the raw results file.
                        if index == args.ops - 1:
                            require(stats["mean"] >= .95, f"{selected} mean recall@10 below .95: {stats!r}")
                    recorder.measure(engine, f"vector.ann_k10.n{size}", repeat, args.ops, operation,
                                     verify,
                                     recall_at_10=stats, engine_order=list(engines), **metadata)
                    require(stats["mean"] >= .95, f"{engine} mean recall@10 below .95: {stats!r}")


def node_id(index):
    return f"n{index:06}"


def graph_targets(size, start, depth):
    visited = {start}
    frontier = [start]
    for _ in range(depth):
        following = []
        for source in frontier:
            for offset in OFFSETS:
                target = (source + offset) % size
                if target not in visited:
                    visited.add(target)
                    following.append(target)
        frontier = following
    visited.remove(start)
    return sorted(node_id(index) for index in visited)


def neo4j_rows(http, statement, parameters=None):
    response = http.json("POST", "/db/neo4j/tx/commit", {"statements": [{"statement": statement, "parameters": parameters or {}}]})
    require(response.get("errors") == [], f"Neo4j rejected operation: {response.get('errors')!r}")
    results = response.get("results")
    require(isinstance(results, list) and len(results) == 1, "invalid Neo4j transaction response")
    return [item["row"] for item in results[0].get("data", [])]


def multidb_rows(http, sql):
    response = http.json("POST", "/sql", {"sql": sql})
    output = response.get("output", {})
    require(output.get("kind") == "rows" and isinstance(output.get("rows"), list), "invalid MultiDB SQL result")
    return output["rows"]


def validate_graph_rows(rows, expected):
    require(isinstance(rows, list) and all(isinstance(row, list) and len(row) == 1 and isinstance(row[0], str) for row in rows), "invalid graph node projection")
    actual = [row[0] for row in rows]
    require(len(actual) == len(set(actual)), "graph traversal returned duplicate destinations")
    require(sorted(actual) == expected, f"graph reachable nodes differ: {actual!r} != {expected!r}")


def run_graphs(args, recorder, multidb, suffix, report):
    sizes = [1000, 10000]
    authorization = base64.b64encode(f"neo4j:{PASSWORD}".encode()).decode()
    with Http("http://127.0.0.1:17474", headers={"Authorization": f"Basic {authorization}"}, envelope=False) as neo4j:
        version = neo4j_rows(neo4j, "CALL dbms.components() YIELD versions RETURN versions[0]")[0][0]
        require(version == "5.26.18", f"Neo4j 5.26.18 required, got {version!r}")
        report["graph_sizes"] = sizes
        for size in sizes:
            name = f"domain_graph_{suffix}_{size}"
            label = f"DomainGraph{suffix}N{size}"
            multidb.json("POST", "/builder/graph", {"name": name, "graph_id": 1000000 + size})
            neo4j_rows(neo4j, f"CREATE CONSTRAINT {label}Unique IF NOT EXISTS FOR (n:{label}) REQUIRE n.id IS UNIQUE")
            for start in range(0, size, 512):
                neo4j_rows(neo4j, f"UNWIND $ids AS id CREATE (n:{label} {{id:id}})", {"ids": [node_id(index) for index in range(start, min(size, start + 512))]})
            edges = [{"source": node_id(index), "target": node_id((index + offset) % size), "label": "related", "props": {"fixture": True}} for index in range(size) for offset in OFFSETS]
            for edge in edges:
                result = multidb.json("POST", f"/data/graphs/{name}/edges", edge)
                require(result.get("written") is True, "MultiDB edge write was not acknowledged")
            # Reconnect outside timing after the other backend's long load.
            neo4j.close()
            for start in range(0, len(edges), 512):
                neo4j_rows(neo4j, f"UNWIND $edges AS e MATCH (s:{label} {{id:e.source}}),(t:{label} {{id:e.target}}) CREATE (s)-[:RELATED {{fixture:true}}]->(t)", {"edges": edges[start:start + 512]})
            require(neo4j_rows(neo4j, f"MATCH (s:{label})-[:RELATED]->(t:{label}) RETURN count(*)") == [[len(edges)]], "Neo4j fixture edge count differs")
            metadata = dict(dataset_nodes=size, dataset_edges=len(edges), outgoing_degree=4, offsets=list(OFFSETS),
                            neo4j_version=version, fixture_load_timed=False, protocol="authenticated loopback HTTP JSON",
                            validation="exact complete reachable-node set with source excluded and no duplicates",
                            caveat="MultiDB SQL graph function versus Neo4j transactional Cypher; audit, WAL and planning differ")
            starts = [(index * 97) % size for index in range(args.ops)]
            for depth in (1, 2):
                expected = [graph_targets(size, start, depth) for start in starts]
                for repeat in range(args.repeats):
                    engines = ("multidb_http", "neo4j_http") if repeat % 2 == 0 else ("neo4j_http", "multidb_http")
                    for engine in engines:
                        def operation(index, selected=engine):
                            start = node_id(starts[index])
                            if selected == "multidb_http":
                                return multidb_rows(multidb, f"SELECT * FROM graph_neighbors('{name}', '{start}', 'related', {depth})")
                            return neo4j_rows(neo4j, f"MATCH (s:{label} {{id:$start}})-[:RELATED*1..{depth}]->(t:{label}) WHERE t.id <> $start RETURN DISTINCT t.id", {"start": start})
                        operation(0)
                        recorder.measure(engine, f"graph.neighbors_depth{depth}.n{size}", repeat, args.ops, operation,
                                         lambda rows, index: validate_graph_rows(rows, expected[index]), depth=depth, engine_order=list(engines), **metadata)
            for repeat in range(args.repeats):
                engines = ("multidb_http", "neo4j_http") if repeat % 2 == 0 else ("neo4j_http", "multidb_http")
                for engine in engines:
                    def write(index, selected=engine):
                        edge = dict(edges[(index * 17) % len(edges)])
                        sequence = repeat * args.ops + index
                        if selected == "multidb_http":
                            edge["props"] = {"write_seq": sequence}
                            return multidb.json("POST", f"/data/graphs/{name}/edges", edge)
                        rows = neo4j_rows(neo4j, f"MATCH (s:{label} {{id:$source}})-[r:RELATED]->(t:{label} {{id:$target}}) SET r.write_seq=$sequence RETURN count(r)", {**edge, "sequence": sequence})
                        require(rows == [[1]], "Neo4j edge update missed its durable fixture relationship")
                        return {"written": True}
                    recorder.measure(engine, f"graph.durable_edge_update.n{size}", repeat, args.ops, write,
                                     lambda result, _: result.get("written") is True,
                                     write_semantics="one durable transaction per existing edge update; separate from query timings", engine_order=list(engines),
                                     **{**metadata, "validation": "successful write acknowledgement; Neo4j also requires exactly one affected relationship; MultiDB public HTTP has no edge-property readback, so stored write_seq values are not independently verified"})


def run(args, recorder):
    if args.ops < 1 or args.repeats < 1:
        raise ValueError("vector/graph operations and repeats must be positive")
    output = Path(args.output)
    suffix = hashlib.sha256(str(output.resolve()).encode()).hexdigest()[:8]
    domains = getattr(args, "domains", ["vector", "graph"])
    if isinstance(domains, str):
        domains = domains.split(",")
    report = {"seed": SEED, "fixture_load_timed": False, "domain_sizes_independent_of_sql_rows": True,
              "sizes": [1000, 10000], "robustness_cases_timed": False}
    report_path = output / "vector-graph-correctness.json"
    if report_path.exists():
        previous = json.loads(report_path.read_text())
        require(isinstance(previous, dict), "invalid prior vector/graph correctness report")
        report.update(previous)
    with Http() as multidb:
        if "vector" in domains:
            run_vectors(args, recorder, multidb, suffix, report)
        if "graph" in domains:
            run_graphs(args, recorder, multidb, suffix, report)
    write_json(report_path, report)
