#!/usr/bin/env python3
"""Run a declared local qualification scenario against a fresh owned database.

The acceptance applies only to the recorded scenario/artifact. It never grants
production, physical GPU, distributed or power-loss certification. Standard
library only; no build, download, Docker launch or shell evaluation is performed.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import contextlib
import csv
import ctypes
import datetime
import fcntl
import hashlib
import http.client
import json
import math
import mmap
import os
from pathlib import Path
import shutil
import signal
import socket
import statistics
import struct
import subprocess
import sys
import threading
import time

ROOT = Path(__file__).resolve().parent
TABLE = "qualification_rows"
STATUSES = {"accepted", "failed", "blocked", "unrun"}
TOKEN = "qualification-local-disposable-token"
PASSWORD = "qualification-local-disposable-password"
KINDS = ("point_read", "upsert", "large_result", "denied_policy")


class Blocked(RuntimeError):
    pass


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf8") as stream:
        json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
        stream.write("\n")
    temporary.replace(path)


def positive(value):
    return type(value) in (int, float) and math.isfinite(value) and value > 0


def validate_profile(profile):
    integer_fields = ("concurrency", "rows", "payload_bytes", "slow_clients", "large_result_rows",
                      "history_retain_lsns", "minimum_successful_requests", "maximum_rss_bytes",
                      "maximum_file_descriptors", "maximum_directory_bytes", "minimum_free_disk_bytes",
                      "minimum_memory_bytes")
    for name in integer_fields:
        if type(profile.get(name)) is not int or profile[name] < 1:
            raise ValueError(f"profile requires a positive integer {name}")
    for name in ("duration_seconds", "requests_per_second_per_worker", "request_timeout_seconds", "minimum_effective_cpus"):
        if not positive(profile.get(name)):
            raise ValueError(f"profile requires positive finite {name}")
    if profile["rows"] < max(profile["concurrency"], profile["large_result_rows"]) or profile["payload_bytes"] < 64:
        raise ValueError("rows must cover workers/results; payload must be at least64 bytes")
    if profile.get("maximum_unexpected_errors") != 0 or type(profile.get("maximum_unexpected_errors")) is not int:
        raise ValueError("qualification requires zero unexpected errors")
    for name in ("cold_start", "larger_than_ram"):
        if type(profile.get(name)) is not bool:
            raise ValueError(f"{name} must be boolean")
    io_fraction = profile.get("minimum_read_io_fraction_of_ram")
    if (type(io_fraction) not in (int, float) or not math.isfinite(io_fraction) or io_fraction < 0
            or (profile["larger_than_ram"] and io_fraction < 1)):
        raise ValueError("larger-than-RAM requires observed backing-read I/O at least equal to effective RAM")
    if profile["cold_start"]:
        fraction = profile.get("maximum_resident_fraction_after_eviction")
        if type(fraction) not in (int, float) or not math.isfinite(fraction) or not 0 <= fraction <= .05:
            raise ValueError("cold profile requires a finite resident-fraction ceiling <=0.05")
    if set(profile.get("latency_ms", {})) != set(KINDS):
        raise ValueError("declare latency limits for every measured operation")
    for name, limits in profile["latency_ms"].items():
        if set(limits) != {"p95", "p99"} or not all(positive(v) for v in limits.values()) or limits["p95"] > limits["p99"]:
            raise ValueError(f"invalid latency limits for {name}")
    if profile["duration_seconds"] * profile["requests_per_second_per_worker"] * profile["concurrency"] > 2_000_000:
        raise ValueError("split runs exceeding two million planned samples into separate campaigns")
    return profile


def load_profile(name, override=None):
    catalog = json.loads((ROOT / "profiles.json").read_text())["profiles"]
    def resolve(key):
        value = dict(catalog[key])
        parent = value.pop("extends", None)
        return {**(resolve(parent) if parent else {}), **value}
    value = resolve(name)
    if override:
        patch = json.loads(Path(override).read_text())
        unknown = set(patch) - set(value)
        if unknown:
            raise ValueError(f"unknown profile overrides: {sorted(unknown)}")
        value.update(patch)
    return validate_profile(value)


def quantile(values, probability):
    ordered = sorted(values)
    at = (len(ordered) - 1) * probability
    lo, hi = math.floor(at), math.ceil(at)
    return ordered[lo] + (ordered[hi] - ordered[lo]) * (at - lo)


def host_resources():
    memory = os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")
    cpus = len(os.sched_getaffinity(0))
    cgroup = {}
    for name in ("memory.max", "cpu.max", "cpuset.cpus.effective"):
        path = Path("/sys/fs/cgroup") / name
        if path.exists():
            cgroup[name] = path.read_text().strip()
    if cgroup.get("memory.max", "max") != "max":
        memory = min(memory, int(cgroup["memory.max"]))
    quota = cgroup.get("cpu.max", "max 100000").split()
    if quota[0] != "max":
        cpus = min(cpus, int(quota[0]) / int(quota[1]))
    return {"effective_memory_bytes": memory, "effective_cpus": cpus,
            "allowed_cpus": sorted(os.sched_getaffinity(0)), "cgroup": cgroup,
            "platform": {name: getattr(os.uname(), name) for name in ("sysname", "nodename", "release", "version", "machine")}}


def require_idle():
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            name = (entry / "comm").read_text().strip()
            state = (entry / "stat").read_text().rsplit(")", 1)[1].split()[0]
            if name in {"rustc", "rustdoc", "cc", "ld", "ld.lld", "rust-lld"} and state != "Z":
                raise Blocked(f"active compiler/linker would contaminate qualification: pid{entry.name} {name}")
        except (FileNotFoundError, ProcessLookupError):
            continue


def require_scope_idle(functional_calibration=False):
    if not functional_calibration:
        require_idle()


def free_ports(ports):
    with contextlib.ExitStack() as stack:
        for port in ports:
            probe = stack.enter_context(socket.socket())
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind(("127.0.0.1", port))
            except OSError as error:
                raise Blocked(f"qualification port{port} is occupied") from error


def preflight(profile, host, free_bytes):
    if host["effective_cpus"] < profile["minimum_effective_cpus"] or host["effective_memory_bytes"] < profile["minimum_memory_bytes"]:
        raise Blocked("host does not meet the declared CPU/memory profile")
    logical_bytes = profile["rows"] * profile["payload_bytes"]
    if profile["larger_than_ram"] and logical_bytes <= host["effective_memory_bytes"]:
        raise Blocked("logical fixture payload does not exceed observed effective host/cgroup RAM; increase rows/payload")
    # Conservative screening only; runtime sampling enforces the actual ceiling.
    estimate = logical_bytes * 8 + 128 * 1024 * 1024
    if estimate > profile["maximum_directory_bytes"]:
        raise Blocked("estimated fixture/backup space exceeds the declared directory ceiling")
    if free_bytes < estimate + profile["minimum_free_disk_bytes"]:
        raise Blocked("insufficient disk for this declared fixture, backup/restore and free-space floor")
    return {"logical_payload_bytes": logical_bytes, "conservative_disk_estimate_bytes": estimate,
            "larger_than_ram_verified": logical_bytes > host["effective_memory_bytes"]}


class Api:
    def __init__(self, port, timeout=5, principal="admin"):
        self.port, self.timeout, self.principal = port, timeout, principal
        self.connection = None

    def close(self):
        if self.connection:
            self.connection.close()
        self.connection = None

    def request(self, method, path, body=None):
        if self.connection is None:
            self.connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=self.timeout)
        raw = None if body is None else json.dumps(body, separators=(",", ":"), allow_nan=False).encode()
        self.connection.request(method, path, raw, {"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}",
                                                   "X-MultiDB-Principal": self.principal})
        response = self.connection.getresponse()
        data = response.read(64 * 1024 * 1024 + 1)
        if len(data) > 64 * 1024 * 1024:
            raise AssertionError("response exceeded64MiB")
        return response.status, json.loads(data)

    def json(self, method, path, body=None):
        status, response = self.request(method, path, body)
        if not 200 <= status < 300 or response.get("ok") is not True:
            raise AssertionError(f"unexpected HTTP{status} {path}: {str(response)[:512]}")
        return response["data"]

    def rows(self, sql):
        output = self.json("POST", "/sql", {"sql": sql})["output"]
        if output.get("kind") != "rows" or not isinstance(output.get("rows"), list):
            raise AssertionError("SQL did not return row output")
        return output["rows"]

    def write(self, sql, count):
        output = self.json("POST", "/sql", {"sql": sql})["output"]
        if type(output.get("affected_rows")) is not int or output["affected_rows"] != count:
            raise AssertionError("write acknowledgement has incorrect affected_rows")


def payload(identifier, version, size):
    prefix = f"id={identifier};version={version};"
    if len(prefix) > size:
        raise ValueError("payload size cannot represent this fixture")
    return prefix + "x" * (size - len(prefix))


def verify_record(record, identifier, version, size):
    expected = [identifier, version, payload(identifier, version, size)]
    if record != expected or any(type(value) is not type(reference) for value, reference in zip(record, expected)):
        raise AssertionError(f"readback differs for key{identifier}, expected version{version}")


def insert(records):
    values = ",".join(f"({identifier},{version},'{data}')" for identifier, version, data in records)
    return f"INSERT INTO {TABLE} (id,version,payload) VALUES {values}"


def upsert(identifier, version, size):
    return insert([[identifier, version, payload(identifier, version, size)]]) + " ON CONFLICT (id) DO UPDATE SET version=EXCLUDED.version,payload=EXCLUDED.payload"


class Server:
    def __init__(self, binary, database, output, profile, port, pg_port, functional_calibration=False):
        self.binary, self.database, self.output, self.profile = binary, database, output, profile
        self.port, self.pg_port, self.process, self.log = port, pg_port, None, None
        self.pg_password = os.urandom(32).hex()
        self.shutdowns = []
        self.functional_calibration = functional_calibration

    def environment(self):
        env = {key: value for key, value in os.environ.items() if not key.startswith("MULTIDB_")}
        env.update(MULTIDB_RUNTIME_MODE="local-dev", MULTIDB_PG_TLS_MODE="disabled", MULTIDB_PROFILE="transactional",
                   MULTIDB_DB_PATH=str(self.database), MULTIDB_BIND=f"127.0.0.1:{self.port}",
                   MULTIDB_PG_BIND=f"127.0.0.1:{self.pg_port}", MULTIDB_ADMIN_TOKEN=TOKEN,
                   MULTIDB_ADMIN_PASSWORD=PASSWORD, MULTIDB_PG_USER="qualification",
                   MULTIDB_PG_PASSWORD=self.pg_password,
                   MULTIDB_HISTORY_RETAIN_LSNS=str(self.profile["history_retain_lsns"]))
        return env

    def start(self):
        free_ports((self.port, self.pg_port))
        self.log = (self.output / "server.log").open("ab")
        self.process = subprocess.Popen([str(self.binary), "serve"], cwd=self.output, env=self.environment(),
                                        stdout=self.log, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 60
        try:
            while time.monotonic() < deadline:
                if self.process.poll() is not None:
                    raise RuntimeError(f"owned server exited at startup:{self.process.returncode}")
                api = Api(self.port, timeout=1)
                try:
                    if api.rows("SELECT 1") == [[1]]:
                        return
                except (OSError, AssertionError, ValueError, http.client.HTTPException):
                    time.sleep(.1)
                finally:
                    api.close()
            raise TimeoutError("owned server did not become authenticated and ready")
        except BaseException as error:
            self.stop(primary_error=error)
            raise

    def stop(self, primary_error=None):
        if not self.process:
            return
        forced = False
        outcome = {"status": "unrun"}
        try:
            if self.process.poll() is None:
                os.killpg(self.process.pid, signal.SIGTERM)
                try:
                    self.process.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    forced = True
                    os.killpg(self.process.pid, signal.SIGKILL)
                    self.process.wait(timeout=10)
            if forced or self.process.returncode != 0:
                raise RuntimeError(f"owned server did not exit cleanly:{self.process.returncode}, forced={forced}")
            outcome["status"] = "accepted"
        except Exception as error:
            outcome.update(status="failed", error={"type": type(error).__name__, "message": str(error)})
            if primary_error is None:
                raise
            primary_error.add_note("secondary server shutdown failure:" + str(error))
        finally:
            outcome.update(returncode=self.process.returncode, forced_kill=forced)
            self.shutdowns.append(outcome)
            if self.log:
                self.log.close()
                self.log = None
            self.process = None


def disk_bytes(directory):
    total = 0
    for path in Path(directory).rglob("*"):
        if path.is_symlink():
            raise AssertionError("unexpected symlink in owned evidence directory")
        if path.is_file():
            total += path.stat().st_blocks * 512
    return total


def resource_sample(server, output):
    if not server.process or server.process.poll() is not None:
        raise AssertionError("server exited during resource sampling")
    return process_resources(server.process.pid, output)


def process_resources(pid, output):
    status = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/status").read_text().splitlines() if ":" in line)
    io = dict(line.split(":", 1) for line in Path(f"/proc/{pid}/io").read_text().splitlines())
    return {"time_monotonic": time.monotonic(), "rss_bytes": int(status["VmRSS"].split()[0]) * 1024,
            "rss_hwm_bytes": int(status["VmHWM"].split()[0]) * 1024,
            "backing_read_bytes": int(io["read_bytes"]), "backing_write_bytes": int(io["write_bytes"]),
            "file_descriptors": len(list(Path(f"/proc/{pid}/fd").iterdir())), "directory_bytes": disk_bytes(output),
            "free_disk_bytes": shutil.disk_usage(output).free, "scope": "owned server process RSS/fd; allocated owned-directory disk; host free disk"}


def enforce_resources(sample, profile):
    for field, ceiling in (("rss_bytes", "maximum_rss_bytes"), ("file_descriptors", "maximum_file_descriptors"),
                           ("directory_bytes", "maximum_directory_bytes")):
        if sample[field] > profile[ceiling]:
            raise AssertionError(f"resource ceiling exceeded:{field}={sample[field]} >{profile[ceiling]}")
    if sample["free_disk_bytes"] < profile["minimum_free_disk_bytes"]:
        raise AssertionError("free-disk floor crossed")
    if sample.get("rss_hwm_bytes", sample["rss_bytes"]) > profile["maximum_rss_bytes"]:
        raise AssertionError("process RSS high-water mark exceeded predeclared ceiling")


class Measurements:
    def __init__(self, output):
        self.lock = threading.Lock()
        self.latencies = collections.defaultdict(list)
        self.errors = []
        self.stream = (output / "requests.csv").open("x", newline="")
        self.writer = csv.writer(self.stream)
        self.writer.writerow(("worker", "sequence", "operation", "elapsed_ms", "accepted", "error"))

    def record(self, worker, sequence, operation, elapsed, failure=None):
        with self.lock:
            if failure:
                self.errors.append({"worker": worker, "sequence": sequence, "operation": operation, "error": str(failure)[:512]})
            else:
                self.latencies[operation].append(elapsed)
            self.writer.writerow((worker, sequence, operation, elapsed, not failure, "" if not failure else str(failure)[:512]))

    def result(self, profile):
        metrics = {}
        issues = []
        for kind in KINDS:
            values = self.latencies[kind]
            if not values:
                issues.append(f"no successful samples:{kind}")
                continue
            metrics[kind] = {"samples": len(values), "p50_ms": quantile(values, .5), "p95_ms": quantile(values, .95),
                             "p99_ms": quantile(values, .99), "maximum_ms": max(values), "mean_ms": statistics.mean(values),
                             "p99_small_sample": len(values) < 1000}
            for quantile_name in ("p95", "p99"):
                if metrics[kind][quantile_name + "_ms"] > profile["latency_ms"][kind][quantile_name]:
                    issues.append(f"{kind} {quantile_name} exceeded predeclared limit")
        successes = sum(len(values) for values in self.latencies.values())
        if successes < profile["minimum_successful_requests"]:
            issues.append("minimum successful-request count not achieved")
        if self.errors:
            issues.append("unexpected request/oracle errors")
        return {"status": "failed" if issues else "accepted", "metrics": metrics, "successful_requests": successes,
                "unexpected_errors": len(self.errors), "errors": self.errors[:100], "issues": issues}

    def close(self):
        self.stream.close()


def full_oracle(api, profile, versions):
    observed = hashlib.sha256()
    count = 0
    for start in range(0, profile["rows"], 256):
        end = min(profile["rows"], start + 256)
        records = api.rows(f"SELECT id,version,payload FROM {TABLE} WHERE id>={start} AND id<{end} ORDER BY id")
        if len(records) != end - start:
            raise AssertionError(f"full readback lost/duplicated rows at{start}")
        for identifier, record in zip(range(start, end), records):
            verify_record(record, identifier, versions.get(identifier, 0), profile["payload_bytes"])
            observed.update(json.dumps(record, separators=(",", ":")).encode() + b"\n")
        count += len(records)
    if api.rows(f"SELECT COUNT(*) FROM {TABLE}") != [[profile["rows"]]]:
        raise AssertionError("full readback has unknown extra rows")
    return {"status": "accepted", "rows_checked": count, "logical_sha256": observed.hexdigest(),
            "acknowledged_final_versions": len(versions), "oracle": "all source rows, exact types/payloads and last acknowledged per-worker-owned key version"}


def denied(api):
    status, response = api.request("POST", "/sql", {"sql": f"SELECT payload FROM {TABLE} WHERE id=0"})
    if status != 403 or response.get("ok") is not False or response.get("error", {}).get("code") != "forbidden" or "data" in response:
        raise AssertionError(f"denied principal escaped policy boundary:{status} {str(response)[:256]}")


def worker(index, api_port, profile, stop, measurements, versions, deadline, counters):
    api = Api(api_port, profile["request_timeout_seconds"])
    restricted = Api(api_port, profile["request_timeout_seconds"], "qualification_denied")
    width = (profile["rows"] - 1 - index) // profile["concurrency"] + 1
    sequence = 0
    period = 1 / profile["requests_per_second_per_worker"]
    next_request = time.monotonic()
    try:
        while not stop.is_set() and time.monotonic() < deadline:
            identifier = index + profile["concurrency"] * ((sequence * 97) % width)
            kind = "denied_policy" if sequence % 17 == 0 else ("upsert" if sequence % 10 in (6, 7, 8) else ("large_result" if sequence % 10 == 9 else "point_read"))
            tick = time.perf_counter_ns()
            failure = None
            try:
                if kind == "denied_policy":
                    denied(restricted)
                elif kind == "upsert":
                    version = sequence + 1
                    api.write(upsert(identifier, version, profile["payload_bytes"]), 1)
                    versions[identifier] = version  # This worker alone owns this key partition.
                elif kind == "large_result":
                    count = min(profile["large_result_rows"], profile["rows"])
                    first = (sequence * 997 + index * 131) % (profile["rows"] - count + 1)
                    records = api.rows(f"SELECT id,version,payload FROM {TABLE} WHERE id>={first} AND id<{first + count} ORDER BY id")
                    if len(records) != count:
                        raise AssertionError("large result lost rows")
                    for expected, record in enumerate(records, first):
                        if not isinstance(record, list) or len(record) != 3 or type(record[1]) is not int:
                            raise AssertionError("large result changed types")
                        verify_record(record, expected, record[1], profile["payload_bytes"])
                else:
                    records = api.rows(f"SELECT id,version,payload FROM {TABLE} WHERE id={identifier}")
                    if len(records) != 1:
                        raise AssertionError("point lookup lost or duplicated row")
                    verify_record(records[0], identifier, versions.get(identifier, 0), profile["payload_bytes"])
            except Exception as error:
                failure = error
                stop.set()
            measurements.record(index, sequence, kind, (time.perf_counter_ns() - tick) / 1e6, failure)
            sequence += 1
            if sequence % 50 == 0:
                api.close()
                restricted.close()
                counters[index] += 1
            next_request += period
            stop.wait(max(0, next_request - time.monotonic()))
    finally:
        api.close()
        restricted.close()


def disruptive_clients(port, pg_port, stop, counts, failures):
    sequence = 0
    while not stop.is_set():
        try:
            # Read-only request is fully transmitted and the peer disconnects
            # before consuming a response. Never create an uncertain write.
            body = json.dumps({"sql": f"SELECT id,payload FROM {TABLE} LIMIT 128"}).encode()
            request = (f"POST /sql HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\n"
                       f"X-MultiDB-Principal: admin\r\nContent-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n").encode() + body
            with socket.create_connection(("127.0.0.1", port), timeout=2) as connection:
                connection.sendall(request)
            # An incomplete pgwire startup packet exercises connection cleanup;
            # authenticated pgwire workloads are a separate qualification scope.
            with socket.create_connection(("127.0.0.1", pg_port), timeout=2) as connection:
                connection.sendall(struct.pack("!II", 64, 196608) + b"user\0qualification")
            sequence += 1
            counts["read_only_disconnects"] = sequence
            counts["partial_pgwire_startups"] = sequence
        except OSError as error:
            failures.append(f"disconnect client could not connect:{error}")
            stop.set()
        stop.wait(.5)


def slow_client(index, port, pg_port, stop, counts, failures):
    while not stop.is_set():
        try:
            with socket.create_connection(("127.0.0.1", port if index % 2 == 0 else pg_port), timeout=2) as connection:
                packet = b"GET /health HTTP/1.1\r\nHost: localhost\r\nX-Slow: " if index % 2 == 0 else struct.pack("!II", 64, 196608) + b"user\0qualification"
                connection.sendall(packet)
                for _ in range(10):
                    if stop.wait(.2):
                        break
                    try:
                        connection.sendall(b"x")
                    except (BrokenPipeError, ConnectionResetError):
                        break  # A bounded server refusal is an allowed defense.
                counts[index] += 1
        except OSError as error:
            failures.append(f"slow client could not connect:{error}")
            stop.set()


def cold_evict(path, ceiling):
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise Blocked("cold qualification needs Linux fadvise+mincore; no shared/global cache drop is performed")
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
        os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
        size = path.stat().st_size
        with mmap.mmap(stream.fileno(), 0, access=mmap.ACCESS_COPY) as mapped:
            anchor = ctypes.c_char.from_buffer(mapped)
            pages = (size + mmap.PAGESIZE - 1) // mmap.PAGESIZE
            vector = (ctypes.c_ubyte * pages)()
            libc = ctypes.CDLL(None, use_errno=True)
            if libc.mincore(ctypes.c_void_p(ctypes.addressof(anchor)), ctypes.c_size_t(size), vector) != 0:
                del anchor
                raise Blocked(f"mincore could not verify cold pages:errno{ctypes.get_errno()}")
            resident = sum(value & 1 for value in vector)
            del anchor
    fraction = resident / pages
    if fraction > ceiling:
        raise Blocked(f"fixture pages remain resident:{fraction:.3f} >{ceiling}; cannot claim cold start")
    return {"status": "accepted", "file": path.name, "pages": pages, "resident_pages": resident,
            "resident_fraction": fraction, "method": "fsync + fadvise(DONTNEED), independently checked with mincore before server restart",
            "scope": "owned fixture page cache only; controller/device caches not certified cold"}


def offline_command(server, output, name, arguments):
    require_scope_idle(server.functional_calibration)
    command = [str(server.binary), *map(str, arguments)]
    with (output / (name + ".log")).open("xb") as stream, (output / (name + "-resources.jsonl")).open("x") as resources:
        process = subprocess.Popen(command, env=server.environment(), cwd=output,
                                   stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        deadline = time.monotonic() + 300
        try:
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"offline operation exceeded300s:{name}")
                try:
                    sample = process_resources(process.pid, output)
                except (FileNotFoundError, ProcessLookupError, KeyError):
                    if process.poll() is None:
                        raise
                    break
                enforce_resources(sample, server.profile)
                resources.write(json.dumps(sample) + "\n")
                resources.flush()
                time.sleep(.2)
            if process.wait(timeout=5):
                raise subprocess.CalledProcessError(process.returncode, command)
        except BaseException:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=10)
            raise
    return output / (name + ".log")


def artifact_manifest(output):
    return [{"path": str(path.relative_to(output)), "bytes": path.stat().st_size, "sha256": digest(path)}
            for path in sorted(output.rglob("*")) if path.is_file() and path.name not in {"report.json", "report.json.tmp"}]


def execute(args, profile, report):
    output, binary = args.output, args.binary
    functional_calibration = getattr(args, "functional_calibration", False)
    require_scope_idle(functional_calibration)
    free_ports((args.http_port, args.pg_port))
    report["host"] = host_resources()
    report["active_section"] = "preflight"
    report["sections"]["preflight"] = {"status": "accepted", **preflight(profile, report["host"], shutil.disk_usage(output).free)}
    snapshot = output / "multidb"
    shutil.copy2(binary, snapshot)
    if digest(snapshot) != report["artifact"]["sha256"]:
        raise AssertionError("binary changed while snapshotting")
    server = Server(snapshot, output / "source.redb", output, profile, args.http_port, args.pg_port, functional_calibration)
    versions, measurements, api = {}, None, None
    stop = threading.Event()
    threads = []
    resource_failures, interference_failures, samples = [], [], []
    try:
        report["active_section"] = "fixture"
        server.start()
        api = Api(args.http_port, profile["request_timeout_seconds"])
        security = api.json("GET", "/security")
        principals = [p for p in security["principals"] if p["user"] != "qualification_denied"]
        principals.append({"user": "qualification_denied", "principal": "qualification_denied", "roles": []})
        api.json("POST", "/security", {"roles": security["roles"], "principals": principals})
        api.json("POST", "/builder/table", {"name": TABLE, "schema": {"primary_key": 0, "columns": [
            {"name": name, "ty": "Str" if name == "payload" else "Int", "nullable": False}
            for name in ("id", "version", "payload")]}, "indexes": []})
        # This retained point must be readable before any online pruning.
        api.rows(f"SELECT id,version,payload FROM {TABLE} AS OF LSN 0")
        for start in range(0, profile["rows"], 128):
            enforce_resources(resource_sample(server, output), profile)
            end = min(profile["rows"], start + 128)
            api.write(insert([[i, 0, payload(i, 0, profile["payload_bytes"])] for i in range(start, end)]), end - start)
        for version in range(1, 257):
            api.write(upsert(0, version, profile["payload_bytes"]), 1)
            versions[0] = version
        report["sections"]["fixture"] = full_oracle(api, profile, versions)
        if profile["cold_start"]:
            report["active_section"] = "cold_start"
            api.close()
            server.stop()
            report["sections"]["cold_start"] = cold_evict(server.database, profile["maximum_resident_fraction_after_eviction"])
            server.start()
            api = Api(args.http_port, profile["request_timeout_seconds"])
        report["active_section"] = "sustained_http"
        measurements = Measurements(output)
        reconnects, slow_counts, disruption_counts = [0] * profile["concurrency"], [0] * profile["slow_clients"], {}
        def sample_resources():
            try:
                with (output / "resources.jsonl").open("x") as stream:
                    while not stop.is_set():
                        require_scope_idle(functional_calibration)
                        sample = resource_sample(server, output)
                        enforce_resources(sample, profile)
                        stream.write(json.dumps(sample) + "\n")
                        stream.flush()
                        samples.append(sample)
                        stop.wait(1)
            except Exception as error:
                resource_failures.append(str(error))
                stop.set()
        threads.append(threading.Thread(target=sample_resources, daemon=True))
        threads.append(threading.Thread(target=disruptive_clients, args=(args.http_port, args.pg_port, stop, disruption_counts, interference_failures), daemon=True))
        for index in range(profile["slow_clients"]):
            threads.append(threading.Thread(target=slow_client, args=(index, args.http_port, args.pg_port, stop, slow_counts, interference_failures), daemon=True))
        for thread in threads:
            thread.start()
        started = time.monotonic()
        deadline = started + profile["duration_seconds"]
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=profile["concurrency"]) as pool:
                futures = [pool.submit(worker, index, args.http_port, profile, stop, measurements, versions, deadline, reconnects)
                           for index in range(profile["concurrency"])]
                for future in futures:
                    future.result()
        finally:
            stop.set()
            for thread in threads:
                thread.join(timeout=profile["request_timeout_seconds"] + 3)
            if any(thread.is_alive() for thread in threads):
                raise AssertionError("qualification client/sampler thread did not stop")
        elapsed = time.monotonic() - started
        report["sections"]["sustained_http"] = {**measurements.result(profile), "elapsed_seconds": elapsed,
                                                 "concurrency": profile["concurrency"], "duration_slo_seconds": profile["duration_seconds"]}
        if elapsed < profile["duration_seconds"]:
            report["sections"]["sustained_http"]["status"] = "failed"
            report["sections"]["sustained_http"]["issues"].append("run ended before predeclared duration")
        report["sections"]["resources"] = {"status": "failed" if resource_failures or not samples else "accepted", "failures": resource_failures,
                                                "samples": len(samples), "scope": "native server RSS and fd; allocated owned-directory disk; no process-tree/GPU/host-RAM ranking",
                                                "peaks": {key: max((s[key] for s in samples), default=0) for key in ("rss_bytes", "rss_hwm_bytes", "file_descriptors", "directory_bytes")}}
        if profile["larger_than_ram"]:
            actual_read_bytes = max(0, samples[-1]["backing_read_bytes"] - samples[0]["backing_read_bytes"]) if samples else 0
            required_read_bytes = math.ceil(profile["minimum_read_io_fraction_of_ram"] * report["host"]["effective_memory_bytes"])
            report["sections"]["larger_than_ram"] = {"status": "accepted" if actual_read_bytes >= required_read_bytes else "blocked",
                "logical_payload_bytes": profile["rows"] * profile["payload_bytes"], "backing_reads_during_load_bytes": actual_read_bytes,
                "minimum_backing_read_bytes": required_read_bytes,
                "reason": "physical backing-read pressure must be observed, not inferred from a large mostly-idle fixture"}
        report["sections"]["reconnect"] = {"status": "accepted" if all(reconnects) else "failed", "per_worker": reconnects}
        report["sections"]["slow_and_disconnected_clients"] = {"status": "accepted" if all(slow_counts) and disruption_counts.get("read_only_disconnects", 0) and not interference_failures else "failed",
            "slow_connection_cycles": slow_counts, **disruption_counts, "failures": interference_failures,
            "scope": "slow HTTP headers/partial pgwire startup; complete read-only HTTP then disconnect; authenticated pgwire load is separate"}
        report["active_section"] = "readback_after_load"
        report["sections"]["readback_after_load"] = full_oracle(api, profile, versions)
        report["active_section"] = "online_gc"
        status, response = api.request("POST", "/sql", {"sql": f"SELECT id FROM {TABLE} WHERE id=0 AS OF LSN 0"})
        if status != 500 or response.get("ok") is not False or "retention expired" not in response.get("error", {}).get("message", "").lower():
            raise AssertionError("online GC was not independently observed via expired pre-GC AS OF point")
        report["sections"]["online_gc"] = {"status": "accepted", "history_retain_lsns": profile["history_retain_lsns"],
            "proof": "AS OF LSN0 readable before writes; expires during concurrent workload before offline maintenance"}
        api.close()
        server.stop()
        report["active_section"] = "offline_gc"
        gc_path = offline_command(server, output, "offline-gc", ["maintenance", "gc", "--db", server.database,
            "--profile", "auto", "--history-retain-lsns", str(profile["history_retain_lsns"]), "--batch-size", "64"])
        gc = json.loads(gc_path.read_text())
        if gc.get("dry_run") is not False or gc.get("complete") is not True:
            raise AssertionError("offline GC did not finish")
        report["sections"]["offline_gc"] = {"status": "accepted", "report": gc}
        report["active_section"] = "offline_backup_restore"
        offline_command(server, output, "full-backup", ["backup", "full", "--db", server.database, "--profile", "auto", "--dest", output / "backups"])
        backups = [path for path in (output / "backups").iterdir() if path.is_dir()]
        if len(backups) != 1:
            raise AssertionError("expected one new full backup")
        offline_command(server, output, "verify-backup", ["verify", "--backup", backups[0]])
        restored = output / "restored.redb"
        offline_command(server, output, "restore", ["restore", "--backup", backups[0], "--dest-db", restored, "--profile", "transactional"])
        server.database = restored
        server.start()
        api = Api(args.http_port, profile["request_timeout_seconds"])
        restored_oracle = full_oracle(api, profile, versions)
        if restored_oracle["logical_sha256"] != report["sections"]["readback_after_load"]["logical_sha256"]:
            raise AssertionError("restore differs from final acknowledged source")
        report["sections"]["offline_backup_restore"] = {**restored_oracle, "full_verified": True,
            "scope": "offline full backup and plaintext fresh-target restore; encryption/PITR use separate recovery evidence"}
        enforce_resources(resource_sample(server, output), profile)
    finally:
        primary_error = sys.exc_info()[1]
        stop.set()
        cleanup_failures = []
        for name, close in (("api", api.close if api else None),
                            ("measurements", measurements.close if measurements else None),
                            ("server", server.stop)):
            if close:
                try:
                    close()
                except Exception as error:
                    cleanup_failures.append((name, error))
        report["shutdowns"] = server.shutdowns
        if cleanup_failures:
            report["secondary_shutdown_failures"] = [{"component": name, "type": type(error).__name__, "message": str(error)}
                                                      for name, error in cleanup_failures]
            if primary_error is None:
                raise cleanup_failures[0][1]
    if digest(binary) != report["artifact"]["sha256"] or digest(snapshot) != report["artifact"]["sha256"]:
        raise AssertionError("qualification artifact changed during execution")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--profile", choices=("smoke", "local-15m", "cold", "larger-than-ram"), default="smoke")
    parser.add_argument("--profile-overrides", type=Path)
    parser.add_argument("--http-port", type=int, default=18180)
    parser.add_argument("--pg-port", type=int, default=15533)
    parser.add_argument("--plan", action="store_true", help="validate and print plan; never launch a server")
    parser.add_argument("--functional-calibration", action="store_true", help="smoke tool exercise only; latency is not qualification evidence")
    args = parser.parse_args(argv)
    args.binary, args.output = args.binary.resolve(), args.output.resolve()
    if not args.binary.is_file() or not os.access(args.binary, os.X_OK):
        raise ValueError("binary must be an existing executable")
    if not args.revision.strip() or args.output.exists() or args.http_port == args.pg_port or any(not 1024 <= p <= 65535 for p in (args.http_port, args.pg_port)):
        raise ValueError("fresh output, explicit revision and distinct unprivileged ports required")
    profile = load_profile(args.profile, args.profile_overrides)
    if args.functional_calibration and args.profile != "smoke":
        raise ValueError("functional calibration is restricted to the smoke profile")
    source_hashes = {str(p.relative_to(ROOT)): digest(p) for p in sorted(ROOT.rglob("*")) if p.is_file() and "__pycache__" not in p.parts}
    report = {"schema_version": 1, "status": "unrun", "profile_name": args.profile, "slo": profile,
              "criteria_sha256": hashlib.sha256(json.dumps(profile, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest(),
              "started_at_utc": utc_now(), "artifact": {"path": str(args.binary), "sha256": digest(args.binary), "revision": args.revision},
              "runner_sha256": source_hashes, "scope": {"local_owned_database": True, "production_release_approved": False,
                  "functional_calibration": args.functional_calibration, "performance_qualified": False,
                  "compiler_interference_guard_applied": not args.functional_calibration,
                  "physical_gpu_measured": False, "power_loss_certified": False, "authenticated_pgwire_load": False},
              "sections": {name: {"status": "unrun"} for name in ("preflight", "fixture", "sustained_http", "resources", "reconnect",
                  "slow_and_disconnected_clients", "readback_after_load", "online_gc", "offline_gc", "offline_backup_restore")}}
    if profile["cold_start"]:
        report["sections"]["cold_start"] = {"status": "unrun"}
    if profile["larger_than_ram"]:
        report["sections"]["larger_than_ram"] = {"status": "unrun"}
    report["sections"]["concurrent_backup"] = {"status": "blocked", "required_in_local_scope": False,
        "reason": "public backup CLI requires exclusive offline database access; runner never opens a second live writer or copies a live redb file"}
    report["external_gates"] = {name: {"status": "unrun", "reason": "requires separately executed/reviewed evidence via external.py"}
                                for name in ("physical_gpu", "cluster", "production_environment", "encrypted_pitr")}
    if args.plan:
        report["host"] = host_resources()
        print(json.dumps(report, indent=2, allow_nan=False))
        return 0
    os.umask(0o077)
    args.output.mkdir(parents=True, mode=0o700)
    write_json(args.output / "plan.json", report)
    # Shared with the benchmark runner so two owned campaigns cannot compete
    # for the same machine while both believe it is otherwise idle.
    lock = None
    try:
        lock_fd = os.open(f"/tmp/multidb-benchmark-{os.getuid()}.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        lock = os.fdopen(lock_fd, "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Blocked("another benchmark/qualification campaign owns this host") from error
        execute(args, profile, report)
        required = [v for v in report["sections"].values() if v.get("required_in_local_scope", True)]
        report["status"] = "failed" if any(v["status"] == "failed" for v in required) else ("blocked" if any(v["status"] == "blocked" for v in required) else ("unrun" if any(v["status"] == "unrun" for v in required) else "accepted"))
        report["scope"]["performance_qualified"] = report["status"] == "accepted" and not args.functional_calibration
    except Exception as error:
        report["status"] = "blocked" if isinstance(error, Blocked) else "failed"
        current = report.get("active_section", "preflight")
        report["failure"] = {"type": type(error).__name__, "message": str(error), "stage": current}
        report["sections"][current] = {"status": report["status"], "reason": str(error)}
    finally:
        report.pop("active_section", None)
        report["finished_at_utc"] = utc_now()
        report["evidence"] = artifact_manifest(args.output)
        write_json(args.output / "report.json", report)
        if lock:
            lock.close()
    print(json.dumps({"status": report["status"], "report": str(args.output / "report.json")}, allow_nan=False))
    return 0 if report["status"] == "accepted" else 2


if __name__ == "__main__":
    raise SystemExit(main())
