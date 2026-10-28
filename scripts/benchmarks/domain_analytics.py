"""DuckDB 1.4.3 and MultiDB SQL analytical operations over identical data.

DuckDB runs in process against a durable local file with two threads; MultiDB
runs through authenticated loopback HTTP. Latencies therefore include different
transport and serialization costs and are not isolated engine comparisons.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from domain_common import Http


def payload(index):
    return f"{index:016x}" * 16


def row(index):
    return [index, index % 100, index, payload(index)]


def aggregate_expected(size):
    result = []
    for category in range(min(100, size)):
        count = (size - 1 - category) // 100 + 1
        result.append([category, count, count * category + 100 * count * (count - 1) // 2])
    return result


def require_equal(actual, expected):
    # Do not coerce floats/strings to integers: exact integer answers are required.
    if actual != expected or any(
        type(value) is not type(reference)
        for values, references in zip(actual, expected)
        for value, reference in zip(values, references)
    ):
        raise AssertionError(f"analytical result differs: actual={actual[:3]!r}, expected={expected[:3]!r}")


def multidb_rows(http, sql):
    response = http.json("POST", "/sql", {"sql": sql})
    output = response.get("output")
    if not isinstance(output, dict) or not isinstance(output.get("rows"), list):
        raise AssertionError(f"invalid SQL row response: {response!r}")
    return output["rows"]


def prepare_multidb(http, name, size, *, indexes=True):
    http.json("POST", "/builder/table", {
        "name": name,
        "schema": {"columns": [
            {"name": column, "ty": "Str" if column == "payload" else "Int", "nullable": False}
            for column in ("id", "category", "value", "payload")
        ], "primary_key": 0},
        "indexes": [{"id": 1, "column": 0}, {"id": 2, "column": 1}] if indexes else [],
    })
    for start in range(0, size, 512):
        values = ",".join(
            f"({index},{index % 100},{index},'{payload(index)}')"
            for index in range(start, min(size, start + 512))
        )
        response = http.json("POST", "/sql", {
            "sql": f"INSERT INTO {name} (id,category,value,payload) VALUES {values}",
        })
        output = response.get("output")
        expected = min(512, size - start)
        if not isinstance(output, dict) or output.get("affected_rows") != expected:
            raise AssertionError(f"load affected-row count differs: {response!r}, expected {expected}")
    require_equal(multidb_rows(http, f"SELECT COUNT(*) FROM {name}"), [[size]])


def prepare_duckdb(connection, name, size):
    connection.execute(f"CREATE TABLE {name} (id BIGINT PRIMARY KEY, category BIGINT NOT NULL, value BIGINT NOT NULL, payload VARCHAR NOT NULL)")
    connection.execute("BEGIN TRANSACTION")
    try:
        # The same generated payload and integer schema, with construction/load outside timing.
        connection.executemany(f"INSERT INTO {name} VALUES (?,?,?,?)", [row(index) for index in range(size)])
        connection.execute("COMMIT")
    except BaseException:
        connection.execute("ROLLBACK")
        raise
    connection.execute(f"CREATE INDEX {name}_category ON {name}(category)")
    connection.execute("CHECKPOINT")
    require_equal([list(values) for values in connection.execute(f"SELECT COUNT(*) FROM {name}").fetchall()], [[size]])


def sections(size, name, operations):
    """Precompute SQL and reference answers before any operation latency timer."""
    ranges = [((index * 97) % (size - 99)) for index in range(operations)]
    yield (
        "range100", operations,
        [f"SELECT id,category,value,payload FROM {name} WHERE id>={start} AND id<{start + 100} ORDER BY id" for start in ranges],
        [[row(index) for index in range(start, start + 100)] for start in ranges],
    )
    yield (
        "group_count_sum", min(30, operations),
        [f"SELECT category,COUNT(*),SUM(value) FROM {name} GROUP BY category ORDER BY category"],
        [aggregate_expected(size)],
    )
    yield (
        "projection_full_scan", min(30, operations),
        [f"SELECT id,value FROM {name} ORDER BY id"],
        [[[index, index] for index in range(size)]],
    )
    yield (
        "full_payload_scan", min(5, operations),
        [f"SELECT id,category,value,payload FROM {name} ORDER BY id"],
        [[row(index) for index in range(size)]],
    )
    categories = [index % 100 for index in range(operations)]
    yield (
        "count_filter", operations,
        [f"SELECT COUNT(*) FROM {name} WHERE category={category}" for category in categories],
        [[[(size - 1 - category) // 100 + 1]] for category in categories],
    )


def run(args, recorder, multidb=None, analytical=None):
    import duckdb
    if duckdb.__version__ != "1.4.3":
        raise RuntimeError(f"DuckDB 1.4.3 required, found {duckdb.__version__}")
    sizes = [int(size) for size in (args.rows.split(",") if isinstance(args.rows, str) else args.rows)]
    if not sizes or min(sizes) < 100 or args.ops < 1:
        raise ValueError("analytical benchmark requires rows >=100 and positive ops")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    database_path = output / "duckdb-analytics.duckdb"
    if database_path.exists():
        raise FileExistsError(f"refusing to reuse benchmark DuckDB database: {database_path}")
    http = multidb if multidb is not None else Http()
    owned_http = multidb is None
    owned_analytical = analytical is None and getattr(args, "analytical", False)
    columnar = analytical if analytical is not None else (Http("http://127.0.0.1:18081") if owned_analytical else None)
    connection = duckdb.connect(str(database_path))
    try:
        connection.execute("SET threads=2")
        environment = {
            "duckdb_version": duckdb.__version__, "duckdb_threads": 2,
            "duckdb_storage": "durable local file", "duckdb_protocol": "in-process Python API",
            "multidb_protocol": "authenticated loopback HTTP /sql (JSON)",
            "multidb_layout": "Transactional row layout and optional Analytical columnar layout",
            "analytical_comparator_enabled": columnar is not None,
            "profile_limits": "native profile defaults differ: Transactional and Analytical are not normalized to the same memory limit",
            "comparison_limit": "transport, serialization and client APIs differ; not isolated engine performance",
            "validation": "exact integers and full ordered results after latency timer",
            "throughput": "section wall time includes validation overhead", "load_timed": False,
        }
        (output / "analytics-environment.json").write_text(json.dumps(environment, indent=2) + "\n")
        suffix = hashlib.sha256(str(output.resolve()).encode()).hexdigest()[:8]
        for size in sizes:
            name = f"domain_analytics_{suffix}_{size}"
            prepare_multidb(http, name, size)
            if columnar is not None:
                prepare_multidb(columnar, name, size, indexes=False)
            prepare_duckdb(connection, name, size)
            for workload, count, commands, expected in sections(size, name, args.ops):
                for repeat in range(args.repeats):
                    engines = ("duckdb_inprocess", "multidb_http") + (("multidb_analytical_http",) if columnar is not None else ())
                    for engine in engines:
                        def operation(index, selected=engine):
                            command = commands[index % len(commands)]
                            if selected == "duckdb_inprocess":
                                return [list(values) for values in connection.execute(command).fetchall()]
                            return multidb_rows(columnar if selected == "multidb_analytical_http" else http, command)
                        recorder.measure(
                            engine, f"analytics.{workload}.n{size}", repeat, count, operation,
                            lambda result, index: require_equal(result, expected[index % len(expected)]),
                            dataset_rows=size,
                            measured_layout="Analytical columnar" if engine == "multidb_analytical_http" else ("Transactional row" if engine == "multidb_http" else "DuckDB native"),
                            **environment,
                        )
    finally:
        connection.close()
        if owned_http:
            http.close()
        if owned_analytical:
            columnar.close()
