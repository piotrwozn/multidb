#!/usr/bin/env python3
"""Run seven pinned competitors against an immutable MultiDB release binary.

Linux, local disposable Docker instances only. No deployment credentials are
used. The output path must be new; all raw timings and invalid attempts survive.
Run after building/testing, with no other workloads on the host.
"""
from __future__ import annotations

import argparse
import base64
import contextlib
import copy
import datetime
import fcntl
import importlib
import importlib.metadata
import json
import os
from pathlib import Path
import signal
import shutil
import socket
import subprocess
import sys
import time
import uuid

from compare import (cleanup_processes, cpu_command, host_metadata, require_idle,
                     select_cpus, server_environment, sha256, verify_harness, write_json)
from domain_common import Http, Recorder
from resources import ResourceSampler

IMAGES = {
    "postgres": "postgres@sha256:1090bc3a8ccfb0b55f78a494d76f8d603434f7e4553543d6e807bc7bd6bbd17f",
    "mongodb": "mongo@sha256:9814652e33f0cf8b9fddea8b46dfc9d8e19b130dcfdd7b510ca58bb0d40c8b71",
    "redis": "redis@sha256:08ad0b1d280850169a790dba1393ff7a90aef951fc19632cf4d3ce4f78e679ba",
    "qdrant": "qdrant/qdrant@sha256:dab6de32f7b2cc599985a7c764db3e8b062f70508fb85ca074aa856f829bf335",
    "neo4j": "neo4j@sha256:ef6f1cb1a5594cbfddb82876a9dc67171941eabca3a38100f2bef0b3fc6c05e9",
}
PASSWORD = "benchmark-pass-only"


def sanitized_inspect(document):
    """An allowlist; image/container environment and mounts never enter artifacts."""
    result = {}
    for key in ("Id", "Image", "Name"):
        result[key] = document.get(key)
    result["Config"] = {"Image": document.get("Config", {}).get("Image")}
    result["State"] = {key: document.get("State", {}).get(key) for key in ("Status", "Pid", "OOMKilled")}
    result["HostConfig"] = {key: document.get("HostConfig", {}).get(key)
                            for key in ("CpusetCpus", "Memory", "NanoCpus", "ShmSize")}
    result["ports"] = document.get("NetworkSettings", {}).get("Ports", {})
    return result


def verify_completion(output, binary, snapshot, expected_hash, server_returncode, shutdown):
    if server_returncode != 0 or shutdown.get("forced_kill"):
        raise RuntimeError(f"server did not shut down cleanly: {server_returncode}, {shutdown}")
    if sha256(binary) != expected_hash or sha256(snapshot) != expected_hash:
        raise RuntimeError("benchmark binary changed during the run")
    write_json(output / "completion.json", {"status": "passed", "clean_server_exit": True,
                                           "binary_sha256": expected_hash})


def wait_sql(port=15433):
    """Verify the authenticated pgwire SQL service, not only the HTTP listener."""
    import psycopg
    wait_port(port)
    with psycopg.connect(host="127.0.0.1", port=port, dbname="multidb", user="benchmark",
                          password=PASSWORD, sslmode="disable", connect_timeout=5,
                          autocommit=True, prepare_threshold=None) as connection:
        rows = connection.execute("SELECT 1", prepare=False).fetchall()
        if rows != [(1,)]:
            raise RuntimeError(f"SQL readiness returned unexpected result: {rows!r}")


SERVICE_PORTS = {"postgres": 15432, "mongodb": 27018, "redis": 16379, "qdrant": 16333, "neo4j": 17474}


def require_unused_ports(domains, analytical=False):
    ports = {18080, 15433}
    for domain, engines in (("sql", ("postgres", "mongodb")), ("kv", ("redis",)),
                            ("vector", ("qdrant",)), ("graph", ("neo4j",))):
        if domain in domains:
            ports.update(SERVICE_PORTS[engine] for engine in engines)
    if analytical and "analytics" in domains:
        ports.update((18081, 15434))
    # Check before launching or issuing any initialization request. In
    # particular, an old listener must not be mistaken for our new process.
    with contextlib.ExitStack() as stack:
        for port in sorted(ports):
            probe = stack.enter_context(socket.socket())
            # A previous, cleanly closed campaign can leave TCP TIME_WAIT
            # connections. Match the server's restart semantics while still
            # refusing a socket held by an active listener.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                raise RuntimeError(f"benchmark port {port} is unavailable") from error


def competitor_probe(engine):
    """Authenticated, untimed readiness; TCP listen can precede data recovery."""
    if engine == "postgres":
        import psycopg
        with psycopg.connect(host="127.0.0.1", port=SERVICE_PORTS[engine], dbname="bench", user="bench",
                              password=PASSWORD, sslmode="disable", connect_timeout=2,
                              autocommit=True) as connection:
            if connection.execute("SELECT 1").fetchall() != [(1,)]:
                raise RuntimeError("unexpected PostgreSQL readiness result")
    elif engine == "mongodb":
        import pymongo
        with pymongo.MongoClient(host="127.0.0.1", port=SERVICE_PORTS[engine], username="benchmark",
                                 password=PASSWORD, serverSelectionTimeoutMS=2000, connectTimeoutMS=2000,
                                 socketTimeoutMS=2000) as client:
            client.admin.command("ping")
    elif engine == "redis":
        import redis
        with redis.Redis(host="127.0.0.1", port=SERVICE_PORTS[engine], password=PASSWORD,
                         socket_connect_timeout=2, socket_timeout=2,
                         retry=redis.retry.Retry(redis.backoff.NoBackoff(), 0), retry_on_error=[]) as client:
            if client.ping() is not True:
                raise RuntimeError("unexpected Redis readiness result")
    elif engine == "qdrant":
        with Http("http://127.0.0.1:16333", headers={"api-key": PASSWORD}, envelope=False) as http:
            http.connection.timeout = 2
            http.json("GET", "/collections")
    elif engine == "neo4j":
        authorization = base64.b64encode(f"neo4j:{PASSWORD}".encode()).decode()
        with Http("http://127.0.0.1:17474", headers={"Authorization": f"Basic {authorization}"}, envelope=False) as http:
            http.connection.timeout = 2
            result = http.json("POST", "/db/neo4j/tx/commit", {"statements": [{"statement": "RETURN 1"}]})
            entries = result.get("results")
            if (result.get("errors") != [] or not isinstance(entries, list) or len(entries) != 1
                    or [entry.get("row") for entry in entries[0].get("data", [])] != [[1]]):
                raise RuntimeError(f"Neo4j database is not ready: {result!r}")
    else:
        raise ValueError(engine)


def wait_competitor(engine, seconds=120):
    deadline = time.monotonic() + seconds
    while True:
        try:
            competitor_probe(engine)
            return
        except Exception as failure:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"{engine} did not become authenticated and ready") from failure
            time.sleep(.25)


class CompetitorServices:
    """Start only a phase's comparators; retain diagnostics before removing them."""
    def __init__(self, output, cpus, sampler):
        self.output, self.cpus, self.sampler = output, cpus, sampler
        self.containers = {}

    @contextlib.contextmanager
    def phase(self, engines):
        try:
            for engine in engines:
                name = f"multidb-audit-{engine}-{uuid.uuid4().hex[:10]}"
                self.containers[engine] = name
                command = start_competitor(engine, name, self.cpus)
                write_json(self.output / f"{engine}-command.json", command)
                inspected = json.loads(docker(["inspect", name], capture_output=True, text=True).stdout)[0]
                safe = sanitized_inspect(inspected)
                write_json(self.output / f"{engine}-inspect.json", safe)
                if safe["State"].get("Pid"):
                    self.sampler.add_container(engine, name, safe["State"]["Pid"])
                wait_port(SERVICE_PORTS[engine])
                wait_competitor(engine)
            yield
        finally:
            self.close()

    def close(self):
        errors = []
        for engine, name in list(self.containers.items()):
            try:
                with (self.output / f"{engine}.log").open("wb") as stream:
                    docker(["logs", name], stdout=stream, stderr=subprocess.STDOUT, timeout=30)
            except Exception as failure:
                errors.append(str(failure))
            try:
                docker(["rm", "-fv", name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
                self.containers.pop(engine, None)
                self.sampler.remove_target(engine)
            except Exception as failure:
                errors.append(str(failure))
        if errors:
            raise RuntimeError(f"comparator cleanup failed: {errors}")


@contextlib.contextmanager
def analytical_server(args, snapshot, cpus, sampler):
    environment = server_environment(args.output / "multidb-analytical.redb")
    environment.update(MULTIDB_PROFILE="analytical", MULTIDB_BIND="127.0.0.1:18081", MULTIDB_PG_BIND="127.0.0.1:15434")
    server = None
    with (args.output / "multidb-analytical.log").open("wb") as stream:
        try:
            server = subprocess.Popen(cpu_command(cpus, [snapshot, "serve"]), env=environment,
                                      stdout=stream, stderr=subprocess.STDOUT, cwd=args.output, start_new_session=True)
            sampler.add_target("multidb_analytical", server.pid)
            wait_port(18081)
            with Http("http://127.0.0.1:18081") as http:
                security = http.json("GET", "/security")
                principals = [entry for entry in security["principals"] if entry["user"] != "benchmark"]
                principals.append({"user": "benchmark", "principal": "benchmark", "roles": ["admin"]})
                http.json("POST", "/security", {"roles": security["roles"], "principals": principals})
                wait_sql(15434)
                yield http
        finally:
            if server:
                shutdown = cleanup_processes(None, server)
                write_json(args.output / "analytical-server-exit.json", {"returncode": server.returncode, "shutdown": shutdown})
                sampler.remove_target("multidb_analytical")
                if server.returncode != 0 or shutdown.get("forced_kill"):
                    raise RuntimeError("Analytical server did not shut down cleanly")


def docker(arguments, **kwargs):
    env = {k: v for k, v in os.environ.items() if k not in (
        "DOCKER_HOST", "DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH")}
    return subprocess.run(["docker", "--host=unix:///var/run/docker.sock", *arguments],
                          env=env, check=True, **kwargs)


def start_competitor(engine, name, cpus):
    arguments = ["run", "-d", "--name", name, "--cpus=2", "--memory=4g", "--cpuset-cpus", ",".join(map(str, cpus))]
    if engine == "postgres":
        arguments += ["--shm-size=256m", "-p", "127.0.0.1:15432:5432", "-e", "POSTGRES_USER=bench",
                      "-e", f"POSTGRES_PASSWORD={PASSWORD}", "-e", "POSTGRES_DB=bench", IMAGES[engine]]
    elif engine == "mongodb":
        arguments += ["-p", "127.0.0.1:27018:27017", "-e", "MONGO_INITDB_ROOT_USERNAME=benchmark",
                      "-e", f"MONGO_INITDB_ROOT_PASSWORD={PASSWORD}", IMAGES[engine], "--bind_ip_all",
                      "--wiredTigerCacheSizeGB", "1"]
    elif engine == "redis":
        arguments += ["-p", "127.0.0.1:16379:6379", IMAGES[engine], "redis-server", "--requirepass", PASSWORD,
                      "--appendonly", "yes", "--appendfsync", "always", "--save", ""]
    elif engine == "qdrant":
        arguments += ["-p", "127.0.0.1:16333:6333", "-e", f"QDRANT__SERVICE__API_KEY={PASSWORD}",
                      "-e", "QDRANT__STORAGE__PERFORMANCE__MAX_SEARCH_THREADS=2", IMAGES[engine]]
    elif engine == "neo4j":
        arguments += ["-p", "127.0.0.1:17474:7474", "-e", f"NEO4J_AUTH=neo4j/{PASSWORD}",
                      "-e", "NEO4J_server_memory_heap_initial__size=256m",
                      "-e", "NEO4J_server_memory_heap_max__size=512m",
                      "-e", "NEO4J_server_memory_pagecache_size=512m", IMAGES[engine]]
    else:
        raise ValueError(engine)
    docker(arguments, capture_output=True, text=True, timeout=120)
    return arguments


def wait_port(port, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                return
        except OSError:
            time.sleep(.25)
    raise TimeoutError(f"server did not listen on {port}")


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--binary", type=Path, required=True)
    p.add_argument("--revision", required=True)
    p.add_argument("--harness", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--rows", default="10000,100000")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--ops", type=int, default=200)
    p.add_argument("--server-cpus", default="0,1")
    p.add_argument("--client-cpus", default="2,3")
    p.add_argument("--domains", default="sql,analytics,kv,vector,graph,tensor")
    p.add_argument("--verify-restarts", action=argparse.BooleanOptionalAction, default=True,
                   help="verify KV data after real native/Redis restart outside timing")
    p.add_argument("--plan", action="store_true")
    p.add_argument("--analytical", action="store_true", help="also compare DuckDB with a second Analytical/columnar MultiDB server")
    return p


def main():
    args = parser().parse_args()
    args.binary = args.binary.resolve()
    args.harness = args.harness.resolve()
    args.output = args.output.resolve()
    args.rows = [int(n) for n in args.rows.split(",")]
    args.domains = args.domains.split(",")
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise ValueError("--binary must be an existing release executable")
    if args.output.exists() or min(args.rows + [args.repeats, args.ops]) <= 0:
        raise ValueError("output must be new; counts must be positive")
    if len(set(args.rows)) != len(args.rows) or len(set(args.domains)) != len(args.domains):
        raise ValueError("rows and domains must not contain duplicates")
    if ("analytics" in args.domains or "sql" in args.domains) and min(args.rows) < 100:
        raise ValueError("SQL and analytics require at least 100 rows")
    if set(args.domains) - {"sql", "analytics", "kv", "vector", "graph", "tensor"}:
        raise ValueError("unknown benchmark domain")
    server_cpus, client_cpus = select_cpus(args.server_cpus, args.client_cpus)
    provenance = verify_harness(args.harness)
    plan = dict(host=host_metadata(), binary=str(args.binary), binary_sha256=sha256(args.binary),
                revision=args.revision, rows=args.rows, repeats=args.repeats, ops=args.ops, domains=args.domains,
                runner_files_sha256={path.name: sha256(path) for path in sorted(Path(__file__).parent.glob("*.py"))},
                analytical=args.analytical,
                server_cpus=server_cpus, client_cpus=client_cpus, docker_images=IMAGES,
                comparator_versions={"duckdb": "1.4.3", "deeplake": "4.7.4", "redis_client": "6.4.0"},
                harness=provenance, methodology="Repeated end-to-end operations; first operations can be cold, no implicit warmup. Protocol/API differences disclosed per domain. No hardware GPU.",
                resource_limits={"competitor_containers": "2 CPU quota, 4 GiB RAM, pinned server affinity", "multidb": "server affinity only; native host/cgroup memory limit", "inprocess_engines": "client affinity, share runner process"},
                resource_sampling="native/embedded process diagnostics; container aggregate cgroup-v2 charged memory and CPU where available; missing scopes explicit, RAM ranking not comparable",
                cpu_governors={str(cpu): (Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").read_text().strip()
                                         if Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").exists() else "unavailable")
                               for cpu in server_cpus + client_cpus})
    if args.plan:
        print(json.dumps(plan, indent=2))
        return
    require_idle()
    lock_fd = os.open(f"/tmp/multidb-benchmark-{os.getuid()}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(lock_fd, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        require_unused_ports(args.domains, args.analytical)
        args.output.mkdir(parents=True)
        write_json(args.output / "run.json", {**plan, "started_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        (args.output / "python-packages.txt").write_text("\n".join(sorted(
            f"{distribution.metadata['Name']}=={distribution.version}"
            for distribution in importlib.metadata.distributions()
        )) + "\n")
        snapshot = args.output / "multidb"
        shutil.copy2(args.binary, snapshot)
        if sha256(snapshot) != plan["binary_sha256"]:
            raise RuntimeError("binary changed while snapshotting")
        services = None
        server = None
        server_log = (args.output / "multidb.log").open("wb")
        recorder = None
        sampler = None
        client = None
        targets = {"benchmark_client": os.getpid()}
        def interrupted(signum, _frame):
            raise KeyboardInterrupt(f"benchmark interrupted by signal {signum}")
        previous_term = signal.signal(signal.SIGTERM, interrupted)
        try:
            server = subprocess.Popen(cpu_command(server_cpus, [snapshot, "serve"]),
                                      env=server_environment(args.output / "multidb.redb"), stdout=server_log,
                                      stderr=subprocess.STDOUT, cwd=args.output, start_new_session=True)
            targets["multidb"] = server.pid
            wait_port(18080)
            with (args.output / "initialization.log").open("w") as stream:
                subprocess.run([sys.executable, str(args.harness / "initialize-multidb.py")], check=True, timeout=90,
                               stdout=stream, stderr=subprocess.STDOUT)
            wait_sql()
            os.sched_setaffinity(0, set(client_cpus))
            sampler = ResourceSampler(args.output, targets)
            sampler.start()
            services = CompetitorServices(args.output, server_cpus, sampler)
            if "sql" in args.domains:
                with services.phase(["postgres", "mongodb"]):
                    sql_output = args.output / "sql"
                    command = [sys.executable, str(args.harness / "benchmark.py"), "--engines", "postgres,mongodb,multidb",
                               "--rows", ",".join(map(str, args.rows)), "--repeats", str(args.repeats),
                               "--read-ops", "1000", "--write-ops", "300", "--batch-ops", "30",
                               "--aggregate-ops", "30", "--mixed-ops", "500", "--seed", "20261028", "--output", str(sql_output)]
                    require_idle()
                    with (args.output / "sql.log").open("wb") as stream:
                        client = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
                        sampler.add_target("sql_benchmark_client", client.pid)
                        returncode = client.wait(timeout=7200)
                        if returncode != 0:
                            raise subprocess.CalledProcessError(returncode, command)
                    require_idle()
                    subprocess.run([sys.executable, str(args.harness / "verify-results.py"), str(sql_output)], check=True, timeout=60)
                    sampler.remove_target("sql_benchmark_client")
            recorder = Recorder(args.output / "domains")
            for domain, module, engines in (
                ("analytics", "domain_analytics", []),
                ("kv", "domain_kv", ["redis"]),
                ("vector", "domains_vector_graph", ["qdrant"]),
                ("graph", "domains_vector_graph", ["neo4j"]),
                ("tensor", "domain_tensor", []),
            ):
                if domain not in args.domains:
                    continue
                require_idle()
                phase_args = copy.copy(args)
                if domain in ("vector", "graph"):
                    phase_args.domains = [domain]
                with services.phase(engines):
                    adapter = importlib.import_module(module)
                    if domain == "analytics" and args.analytical:
                        with analytical_server(args, snapshot, server_cpus, sampler) as analytical:
                            adapter.run(phase_args, recorder, analytical=analytical)
                    else:
                        adapter.run(phase_args, recorder)
                    if domain == "kv" and args.verify_restarts:
                        # Run while this phase still owns its Redis container. No
                        # measurement spans process shutdown, restart or validation.
                        old_pid = server.pid
                        restart_shutdown = cleanup_processes(None, server)
                        if server.returncode != 0 or restart_shutdown.get("forced_kill"):
                            raise RuntimeError("native KV source did not stop cleanly before restart")
                        server_log.close()
                        server_log = (args.output / "multidb.log").open("ab")
                        server = subprocess.Popen(cpu_command(server_cpus, [snapshot, "serve"]),
                            env=server_environment(args.output / "multidb.redb"), stdout=server_log,
                            stderr=subprocess.STDOUT, cwd=args.output, start_new_session=True)
                        wait_port(18080)
                        wait_sql()
                        sampler.add_target("multidb", server.pid)
                        redis_name = services.containers["redis"]
                        docker(["restart", redis_name], stdout=subprocess.DEVNULL, timeout=90)
                        wait_port(SERVICE_PORTS["redis"])
                        wait_competitor("redis")
                        inspected = json.loads(docker(["inspect", redis_name], capture_output=True, text=True).stdout)[0]
                        safe = sanitized_inspect(inspected)
                        write_json(args.output / "redis-restarted-inspect.json", safe)
                        sampler.add_container("redis", redis_name, safe["State"]["Pid"])
                        adapter.verify_persistence(phase_args, multidb_restart=(old_pid, server.pid))
                        write_json(args.output / "kv-restart.json", {"passed": True,
                            "native_before_pid": old_pid, "native_after_pid": server.pid,
                            "native_shutdown": restart_shutdown, "redis_container": redis_name,
                            "timed": False})
            import verify_domains
            proof = verify_domains.verify(args.output / "domains", vars(args))
            if not proof["passed"]:
                raise RuntimeError(f"domain result validation failed: {proof}")
            if server.poll() is not None:
                raise RuntimeError("MultiDB exited during measurements")
            require_idle()
        except BaseException as failure:
            write_json(args.output / "failure.json", {"type": type(failure).__name__, "message": str(failure)})
            raise
        finally:
            active_failure = sys.exc_info()[0] is not None
            cleanup_errors = []
            for close in [recorder.close if recorder else None, services.close if services else None, sampler.close if sampler else None]:
                if close:
                    try:
                        close()
                    except Exception as failure:
                        cleanup_errors.append(str(failure))
            shutdown = {}
            if server:
                try:
                    shutdown = cleanup_processes(client, server)
                    write_json(args.output / "server-exit.json", {"returncode": server.returncode, "shutdown": shutdown})
                except Exception as failure:
                    cleanup_errors.append(str(failure))
            server_log.close()
            if cleanup_errors:
                write_json(args.output / "cleanup-failure.json", {"errors": cleanup_errors})
                if not active_failure:
                    write_json(args.output / "failure.json", {"type": "CleanupError", "message": str(cleanup_errors)})
                    raise RuntimeError(f"benchmark cleanup failed: {cleanup_errors}")
            signal.signal(signal.SIGTERM, previous_term)
        try:
            verify_completion(args.output, args.binary, snapshot, plan["binary_sha256"], server.returncode, shutdown)
        except BaseException as failure:
            write_json(args.output / "failure.json", {"type": type(failure).__name__, "message": str(failure)})
            raise


if __name__ == "__main__":
    main()
