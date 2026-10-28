"""Redis 8.2.3 durable full-record KV versus MultiDB typed primary-key rows.

Redis uses authenticated RESP, AOF and appendfsync=always. MultiDB uses
authenticated loopback HTTP SQL with the configured durable row profile. Both
store the same logical four-column records; transport and encoding costs differ.
Redis MULTI/EXEC prevents interleaving but does not roll back runtime command
errors; such errors invalidate the complete measured section here.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

from domain_common import Http


def row(identifier, value=None):
    return [identifier, identifier % 100, identifier if value is None else value,
            f"{identifier:016x}" * 16]


def encode(record):
    return json.dumps(record, separators=(",", ":"), allow_nan=False)


def assert_record(actual, expected):
    if (actual != expected or not isinstance(actual, list) or len(actual) != 4
            or any(type(value) is not type(reference) for value, reference in zip(actual, expected))):
        raise AssertionError(f"KV record differs: actual={actual!r}, expected={expected!r}")


def sql_rows(http, command):
    response = http.json("POST", "/sql", {"sql": command})
    output = response.get("output", {})
    if not isinstance(output.get("rows"), list):
        raise AssertionError(f"invalid SQL rows: {response!r}")
    return output["rows"]


def affected(response, expected):
    output = response.get("output", {})
    if type(output.get("affected_rows")) is not int or output["affected_rows"] != expected:
        raise AssertionError(f"affected-row count differs: {response!r}; expected {expected}")


def insert_sql(name, records, upsert=False):
    values = ",".join(f"({i},{category},{value},'{payload}')" for i, category, value, payload in records)
    command = f"INSERT INTO {name} (id,category,value,payload) VALUES {values}"
    if upsert:
        command += " ON CONFLICT (id) DO UPDATE SET category=EXCLUDED.category,value=EXCLUDED.value,payload=EXCLUDED.payload"
    return command


def get_multidb(http, name, identifier):
    result = sql_rows(http, f"SELECT id,category,value,payload FROM {name} WHERE id={identifier}")
    if len(result) != 1:
        raise AssertionError(f"expected one KV row, got {len(result)}")
    return result[0]


def get_redis(client, prefix, identifier):
    value = client.get(f"{prefix}:{identifier}")
    if value is None:
        raise AssertionError(f"Redis key missing: {prefix}:{identifier}")
    return json.loads(value)


def redis_batch(client, pairs):
    # transaction=True sends MULTI/EXEC: the complete 100-record batch is atomic.
    with client.pipeline(transaction=True) as pipeline:
        for key, value in pairs:
            pipeline.set(key, value)
        return pipeline.execute()


def require_redis_batch(result, count):
    if not isinstance(result, list) or len(result) != count or any(value is not True for value in result):
        raise AssertionError(f"Redis MULTI/EXEC write failed: {result!r}")


def verify_redis_configuration(client):
    info = client.info("server")
    version = info.get("redis_version")
    if version != "8.2.3":
        raise RuntimeError(f"Redis 8.2.3 required, found {version}")
    settings = client.config_get("appendonly", "appendfsync")
    if settings.get("appendonly") != "yes" or settings.get("appendfsync") != "always":
        raise RuntimeError("Redis benchmark requires appendonly=yes and appendfsync=always")
    return {"redis_version": version, "redis_run_id": info.get("run_id"),
            "redis_appendonly": "yes", "redis_appendfsync": "always"}


def clients(multidb, redis_client):
    own_http, own_redis = multidb is None, redis_client is None
    http = multidb if multidb is not None else Http()
    if redis_client is None:
        import redis
        if redis.__version__ != "6.4.0":
            raise RuntimeError(f"redis-py 6.4.0 required, found {redis.__version__}")
        redis_client = redis.Redis(host="127.0.0.1", port=16379, password="benchmark-pass-only",
                                   decode_responses=True, socket_timeout=30, socket_connect_timeout=5,
                                   retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0), retry_on_error=[])
    return http, redis_client, own_http, own_redis


def run(args, recorder, multidb=None, redis_client=None):
    sizes = [int(n) for n in (args.rows.split(",") if isinstance(args.rows, str) else args.rows)]
    if not sizes or min(sizes) < 100 or len(set(sizes)) != len(sizes) or min(args.ops, args.repeats) < 1:
        raise ValueError("KV requires unique sizes >=100 and positive ops/repeats")
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    persistence_path = output / "kv-persistence.json"
    if persistence_path.exists():
        raise FileExistsError("refusing to reuse an existing KV fixture")
    http, redis_client, own_http, own_redis = clients(multidb, redis_client)
    try:
        environment = {
            **verify_redis_configuration(redis_client), "redis_protocol": "authenticated loopback RESP",
            "redis_py_version": "6.4.0" if own_redis else "provided client; version not assumed",
            "multidb_protocol": "authenticated loopback HTTP SQL/JSON", "multidb_indexes": "primary key only",
            "records_per_batch": 100, "batch_atomicity": "Redis MULTI/EXEC and SQL multi-values INSERT",
            "redis_transaction_errors": "no runtime-error rollback; errors invalidate the section",
            "comparison_limit": "logical records equal; transports, parsers and encodings differ",
            "validation": "exact records after latency timer; throughput includes validation",
            "load_timed": False, "restart_persistence": "not exercised unless verify_persistence is called after real restart",
        }
        (output / "kv-environment.json").write_text(json.dumps(environment, indent=2) + "\n")
        suffix = hashlib.sha256(str(output.resolve()).encode()).hexdigest()[:10]
        probes = []
        batch_count = min(30, args.ops)
        for size in sizes:
            name = f"domain_kv_{suffix}_{size}"
            if next(iter(redis_client.scan_iter(match=f"{name}:*", count=1000)), None) is not None:
                raise FileExistsError(f"Redis fixture namespace already exists: {name}")
            http.json("POST", "/builder/table", {
                "name": name, "schema": {"columns": [
                    {"name": column, "ty": "Str" if column == "payload" else "Int", "nullable": False}
                    for column in ("id", "category", "value", "payload")], "primary_key": 0},
                "indexes": [],
            })
            for start in range(0, size, 100):
                records = [row(i) for i in range(start, min(size, start + 100))]
                affected(http.json("POST", "/sql", {"sql": insert_sql(name, records)}), len(records))
                require_redis_batch(redis_batch(redis_client, [(f"{name}:{r[0]}", encode(r)) for r in records]), len(records))
            expected_values = {}
            for identifier in (0, 42, size // 2, size - 1):
                for actual in (get_multidb(http, name, identifier), get_redis(redis_client, name, identifier)):
                    assert_record(actual, row(identifier))
            for repeat in range(1, args.repeats + 1):
                engines = ("redis_resp", "multidb_http") if repeat % 2 else ("multidb_http", "redis_resp")
                ids = [(i * 97 + 42) % size for i in range(args.ops)]
                commands = [f"SELECT id,category,value,payload FROM {name} WHERE id={i}" for i in ids]
                for engine in engines:
                    def get(index, engine=engine):
                        if engine == "redis_resp":
                            return get_redis(redis_client, name, ids[index])
                        values = sql_rows(http, commands[index])
                        if len(values) != 1:
                            raise AssertionError("SQL point lookup did not return exactly one record")
                        return values[0]
                    recorder.measure(engine, f"kv.point_get.n{size}", repeat, args.ops, get,
                                     lambda result, i: assert_record(result, row(ids[i], expected_values.get(ids[i], ids[i]))),
                                     dataset_rows=size, **environment)
                updates = [row(i % size, 2_000_000_000 + repeat * 1_000_000 + i) for i in range(args.ops)]
                update_commands = [insert_sql(name, [r], upsert=True) for r in updates]
                encoded_updates = [encode(r) for r in updates]
                for engine in engines:
                    def upsert(index, engine=engine):
                        record = updates[index]
                        return (redis_client.set(f"{name}:{record[0]}", encoded_updates[index]) if engine == "redis_resp"
                                else http.json("POST", "/sql", {"sql": update_commands[index]}))
                    def check_update(result, index, engine=engine):
                        if engine == "redis_resp":
                            if result is not True:
                                raise AssertionError(f"Redis SET failed: {result!r}")
                            actual = get_redis(redis_client, name, updates[index][0])
                        else:
                            affected(result, 1)
                            actual = get_multidb(http, name, updates[index][0])
                        assert_record(actual, updates[index])
                    recorder.measure(engine, f"kv.full_record_upsert.n{size}", repeat, args.ops, upsert,
                                     check_update, dataset_rows=size, **environment)
                expected_values.update({record[0]: record[2] for record in updates})
                batches = [[row(size + (repeat - 1) * batch_count * 100 + batch * 100 + i)
                            for i in range(100)] for batch in range(batch_count)]
                batch_commands = [insert_sql(name, records) for records in batches]
                redis_pairs = [[(f"{name}:{record[0]}", encode(record)) for record in records] for records in batches]
                for engine in engines:
                    def batch(index, engine=engine):
                        return (redis_batch(redis_client, redis_pairs[index]) if engine == "redis_resp"
                                else http.json("POST", "/sql", {"sql": batch_commands[index]}))
                    def check_batch(result, index, engine=engine):
                        records = batches[index]
                        if engine == "redis_resp":
                            require_redis_batch(result, 100)
                            actual = [json.loads(value) for value in redis_client.mget([f"{name}:{record[0]}" for record in records])]
                        else:
                            affected(result, 100)
                            start = records[0][0]
                            actual = sql_rows(http, f"SELECT id,category,value,payload FROM {name} WHERE id>={start} AND id<{start + 100} ORDER BY id")
                        if len(actual) != 100:
                            raise AssertionError("atomic batch row count differs")
                        for value, reference in zip(actual, records):
                            assert_record(value, reference)
                    recorder.measure(engine, f"kv.atomic_insert_batch100.n{size}", repeat, batch_count,
                                     batch, check_batch, dataset_rows=size, **environment)
                count = size + repeat * batch_count * 100
                if sql_rows(http, f"SELECT COUNT(*) FROM {name}") != [[count]]:
                    raise AssertionError("MultiDB total KV count differs")
                if sum(1 for _ in redis_client.scan_iter(match=f"{name}:*", count=1000)) != count:
                    raise AssertionError("Redis total KV count differs")
            sample_ids = sorted({0, 42, size - 1, size + args.repeats * batch_count * 100 - 1})
            probes.append({"name": name, "count": count,
                           "records": [row(i, expected_values.get(i, i)) for i in sample_ids]})
        persistence_path.write_text(json.dumps({"restart_tested": False, "redis_run_id": environment["redis_run_id"],
                                                "fixtures": probes}, indent=2) + "\n")
    finally:
        if own_http:
            http.close()
        if own_redis:
            redis_client.close()


def verify_persistence(args, *, multidb_restart, multidb=None, redis_client=None):
    """Call only after the orchestrator has really restarted both persistent servers."""
    path = Path(args.output) / "kv-persistence.json"
    probes = json.loads(path.read_text())
    before_pid, after_pid = multidb_restart
    if any(type(pid) is not int or pid <= 0 for pid in (before_pid, after_pid)) or before_pid == after_pid:
        raise ValueError("real MultiDB restart requires distinct positive before/after server PIDs")
    http, redis_client, own_http, own_redis = clients(multidb, redis_client)
    try:
        environment = verify_redis_configuration(redis_client)
        if not environment["redis_run_id"] or environment["redis_run_id"] == probes["redis_run_id"]:
            raise RuntimeError("Redis has not restarted; refusing a persistence claim")
        for fixture in probes["fixtures"]:
            name = fixture["name"]
            if sql_rows(http, f"SELECT COUNT(*) FROM {name}") != [[fixture["count"]]]:
                raise AssertionError("MultiDB KV count differs after restart")
            if sum(1 for _ in redis_client.scan_iter(match=f"{name}:*", count=1000)) != fixture["count"]:
                raise AssertionError("Redis KV count differs after restart")
            for expected in fixture["records"]:
                assert_record(get_multidb(http, name, expected[0]), expected)
                assert_record(get_redis(redis_client, name, expected[0]), expected)
    finally:
        if own_http:
            http.close()
        if own_redis:
            redis_client.close()
    probes["restart_tested"] = True
    probes["redis_restarted_run_id"] = environment["redis_run_id"]
    probes["multidb_restart_pids"] = [before_pid, after_pid]
    path.write_text(json.dumps(probes, indent=2) + "\n")
