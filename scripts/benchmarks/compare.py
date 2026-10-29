#!/usr/bin/env python3
"""Run reviewed MultiDB loopback benchmarks against two immutable native binaries.

This runner never builds, downloads, reuses a database, or invokes a shell.
The external harness must first be reviewed and copied with provenance.json.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time


HARNESSES = {
    "benchmark.py": "1288ed7c8dda92e3f8a9af8556401d3eee14b03560946e74ca860585c699b4f4",
    "initialize-multidb.py": "309135a4ad97e66422e31fcd5e6492a26b5a75ead218b824f01a44793e784609",
    "verify-results.py": "197827e4922b7f46f98ea864ef104557114dd714d6ecc646e8b54e258101d832",
}
WORKLOADS = (
    "point_read", "category_read", "range_read", "aggregate", "insert_single",
    "insert_batch", "upsert", "mixed_c1", "mixed_c8",
)
METRICS = (
    "successful_requests_per_second", "successful_records_per_second",
    "p50_ms", "p95_ms", "p99_ms",
)
PORTS = (15433, 18080)


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def verify_harness(directory):
    provenance = json.loads((directory / "provenance.json").read_text())
    for name, expected in HARNESSES.items():
        if provenance["files"].get(name) != expected or sha256(directory / name) != expected:
            raise ValueError(f"reviewed harness hash mismatch: {name}")
    return provenance


def select_cpus(server=None, client=None, available=None):
    available = sorted(os.sched_getaffinity(0) if available is None else available)
    if server is None and client is None:
        if len(available) < 3:
            raise ValueError("need at least three allowed CPUs for disjoint server/client affinity")
        return available[:2], available[2:]
    if server is None or client is None:
        raise ValueError("set both --server-cpus and --client-cpus")
    server, client = [int(x) for x in server.split(",")], [int(x) for x in client.split(",")]
    if not server or not client or set(server) & set(client):
        raise ValueError("server and client CPU sets must be nonempty and disjoint")
    if not set(server + client) <= set(available):
        raise ValueError(f"requested CPU outside allowed affinity {available}")
    return server, client


def cpu_command(cpus, command):
    return [shutil.which("taskset") or "taskset", "-c", ",".join(map(str, cpus)), *map(str, command)]


def server_environment(database):
    # Never inherit another deployment's credentials, spec, retention, or governor overrides.
    environment = {k: v for k, v in os.environ.items() if not k.startswith("MULTIDB_")}
    environment.update({
        "MULTIDB_RUNTIME_MODE": "local-dev", "MULTIDB_PG_TLS_MODE": "disabled",
        "MULTIDB_ADMIN_PASSWORD": "benchmark-pass-only",
        "MULTIDB_ADMIN_TOKEN": "benchmark-token-only",
        "MULTIDB_PG_USER": "benchmark", "MULTIDB_PG_PASSWORD": "benchmark-pass-only",
        "MULTIDB_PROFILE": "transactional", "MULTIDB_DB_PATH": str(database),
        "MULTIDB_BIND": "127.0.0.1:18080", "MULTIDB_PG_BIND": "127.0.0.1:15433",
    })
    return environment


def active_builds(proc=Path("/proc")):
    found = []
    for entry in proc.iterdir():
        if not entry.name.isdigit():
            continue
        try:
            name = (entry / "comm").read_text().strip()
            state = (entry / "stat").read_text().rsplit(")", 1)[1].split()[0]
            # A sleeping cargo shell alone is harmless. Compilers/linkers are not.
            if name in {"rustc", "rustdoc", "cc", "ld", "ld.lld", "rust-lld"} and state != "Z":
                found.append({"pid": int(entry.name), "name": name})
            elif name in {"cargo", "cargo-clippy"} and state in {"R", "D"}:
                found.append({"pid": int(entry.name), "name": name})
        except (OSError, IndexError):
            continue
    return found


def require_idle():
    builds = active_builds()
    if builds:
        raise RuntimeError(f"active compilation would contaminate timing: {builds}")


def require_free_ports():
    with contextlib.ExitStack() as stack:
        for port in PORTS:
            probe = stack.enter_context(socket.socket())
            # Clean shutdown can leave TIME_WAIT sockets, which are safe for a
            # restarting server; an active listener still makes bind fail.
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))


def host_metadata():
    cgroup = {}
    for name in ("cpu.max", "cpuset.cpus.effective", "memory.max"):
        path = Path("/sys/fs/cgroup") / name
        if path.exists():
            cgroup[name] = path.read_text().strip()
    return {
        "platform": platform.platform(), "python": sys.version,
        "allowed_cpus": sorted(os.sched_getaffinity(0)), "cgroup": cgroup,
        "memory_limit_note": "Host/cgroup budget only; no per-server Docker memory limit.",
    }


def resource_sample(pid):
    sample = {"time_monotonic": time.monotonic()}
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:", "Threads:")):
                key, value = line.split(":", 1)
                sample[key] = value.strip()
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            key, value = line.split(":", 1)
            sample[key] = int(value)
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        sample["cpu_seconds"] = (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK")
    except OSError:
        sample["unavailable"] = True
    return sample


def stop_process(process):
    outcome = {"was_running": process.poll() is None, "forced_kill": False}
    if outcome["was_running"]:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass  # The child may finish between poll() and killpg().
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            outcome["forced_kill"] = True
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=10)
    return outcome


def cleanup_processes(client, server):
    """Always stop the server, including when client cleanup fails."""
    try:
        if client is not None:
            stop_process(client)
    finally:
        server_shutdown = stop_process(server)
    return server_shutdown


def require_server_alive(server, label):
    if server.poll() is not None:
        raise RuntimeError(f"{label} server exited unexpectedly ({server.returncode})")


def benchmark_command(args, directory, client_cpus):
    return cpu_command(client_cpus, [
        args.python, args.harness / "benchmark.py", "--engines", "multidb",
        "--rows", args.rows, "--repeats", args.repeats,
        "--read-ops", args.read_ops, "--write-ops", args.write_ops,
        "--batch-ops", args.batch_ops, "--aggregate-ops", args.aggregate_ops,
        "--mixed-ops", args.mixed_ops, "--batch-size", 100, "--seed", 20261028,
        "--timeout", args.timeout, "--workloads", ",".join(WORKLOADS),
        # Fresh databases allow identical SQL identifiers in both phases.
        "--run-id", "localbench", "--output", directory / "raw",
    ])


def run_case(args, label, binary, revision, client_cpus, server_cpus):
    require_idle()
    require_free_ports()
    directory = args.output / label
    directory.mkdir()  # Exclusive: database and raw output must both be fresh.
    shutil.copyfile(Path(__file__), directory / "runner.py")
    executable = directory / "multidb"
    shutil.copy2(binary, executable)
    executable.chmod(executable.stat().st_mode | 0o100)
    command = benchmark_command(args, directory, client_cpus)
    write_json(directory / "provenance.json", {
        "label": label, "revision": revision, "original_binary": str(binary),
        "binary_sha256": sha256(executable), "client_command": command,
        "runner_sha256": sha256(directory / "runner.py"),
        "server_cpus": server_cpus, "client_cpus": client_cpus,
        "profile": "transactional", "runtime_mode": "local-dev",
        "auth": "SCRAM benchmark principal mapped to admin; same disposable harness credentials",
        "audit": "profile default; no bypass or override", "tls": "loopback plaintext",
    })
    started = time.monotonic()
    with (directory / "server.log").open("wb") as server_log:
        server = subprocess.Popen(
            cpu_command(server_cpus, [executable, "serve"]), cwd=directory,
            env=server_environment(directory / "multidb.redb"), stdout=server_log,
            stderr=subprocess.STDOUT, start_new_session=True,
        )
        client = None
        samples = []
        try:
            # Reviewed initializer waits for health and grants the benchmark principal.
            with (directory / "initialize.log").open("wb") as log:
                subprocess.run(cpu_command(client_cpus, [args.python, args.harness / "initialize-multidb.py"]),
                               check=True, stdout=log, stderr=subprocess.STDOUT, timeout=90)
            require_idle()
            with (directory / "benchmark.log").open("wb") as log, (directory / "resources.jsonl").open("w") as metrics:
                client = subprocess.Popen(command, cwd=directory, stdout=log,
                                          stderr=subprocess.STDOUT, start_new_session=True)
                while client.poll() is None:
                    require_idle()
                    if server.poll() is not None:
                        raise RuntimeError(f"{label} server exited during benchmark")
                    if time.monotonic() - started > args.max_seconds:
                        raise TimeoutError(f"{label} exceeded --max-seconds; results are incomplete")
                    sample = resource_sample(server.pid)
                    samples.append(sample)
                    metrics.write(json.dumps(sample) + "\n")
                    metrics.flush()
                    time.sleep(1)
                if client.returncode:
                    raise RuntimeError(f"{label} harness failed ({client.returncode}); see benchmark.log")
            require_server_alive(server, label)
            with (directory / "verification.log").open("wb") as log:
                subprocess.run([str(args.python), str(args.harness / "verify-results.py"), str(directory / "raw")],
                               check=True, stdout=log, stderr=subprocess.STDOUT, timeout=60)
            require_server_alive(server, label)
        finally:
            server_shutdown = None
            try:
                server_shutdown = cleanup_processes(client, server)
            finally:
                write_json(directory / "server-exit.json", {
                    "returncode": server.returncode, "case_elapsed_seconds": time.monotonic() - started,
                    "database_bytes": sum(p.stat().st_size for p in directory.glob("multidb.redb*")),
                    "sample_count": len(samples), "shutdown": server_shutdown,
                })
        if not server_shutdown["was_running"] or server_shutdown["forced_kill"] or server.returncode != 0:
            raise RuntimeError(f"{label} server did not complete a clean, requested shutdown")
    return json.loads((directory / "raw" / "results.json").read_text())


def aggregate(results, rows, repeats):
    grouped = {}
    for result in results:
        key = (result["dataset_rows"], result["workload"])
        if (result["engine"] != "multidb" or result.get("correctness_status") != "passed"
                or result["errors"] or result["attempts"] <= 0
                or result["successes"] != result["attempts"]
                or any(not math.isfinite(result[m]) or result[m] <= 0 for m in METRICS)):
            raise ValueError(f"invalid result cannot be compared: {key}")
        grouped.setdefault(key, []).append(result)
    expected = {(n, work) for n in rows for work in (*WORKLOADS, "load_batch")}
    if set(grouped) != expected:
        raise ValueError("missing or unexpected workload/dataset in comparison")
    summaries = {}
    for key, values in grouped.items():
        expected_repeats = {0} if key[1] == "load_batch" else set(range(1, repeats + 1))
        if len(values) != len(expected_repeats) or {v["repeat"] for v in values} != expected_repeats:
            raise ValueError(f"missing or duplicate repeat: {key}")
        summaries[key] = {
            metric: {"median": statistics.median(v[metric] for v in values),
                     "min": min(v[metric] for v in values), "max": max(v[metric] for v in values)}
            for metric in METRICS
        }
        summaries[key]["attempts"] = sum(v["attempts"] for v in values)
    return summaries


def comparison(before, after, rows, repeats):
    before, after = aggregate(before, rows, repeats), aggregate(after, rows, repeats)
    report = []
    for key in sorted(before):
        if before[key]["attempts"] != after[key]["attempts"]:
            raise ValueError(f"different sample counts between before and after: {key}")
        item = {"rows": key[0], "workload": key[1], "before": before[key], "after": after[key]}
        item["throughput_ratio_after_over_before"] = (
            after[key][METRICS[0]]["median"] / before[key][METRICS[0]]["median"])
        item["p50_speedup_before_over_after"] = (
            before[key]["p50_ms"]["median"] / after[key]["p50_ms"]["median"])
        report.append(item)
    return report


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--before", type=Path, required=True)
    p.add_argument("--after", type=Path, required=True)
    p.add_argument("--before-revision", required=True)
    p.add_argument("--after-revision", required=True)
    p.add_argument("--harness", type=Path, required=True)
    p.add_argument("--python", type=Path, default=Path(sys.executable))
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--server-cpus")
    p.add_argument("--client-cpus")
    p.add_argument("--rows", default="10000,100000")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--read-ops", type=int, default=1000)
    p.add_argument("--write-ops", type=int, default=300)
    p.add_argument("--batch-ops", type=int, default=30)
    p.add_argument("--aggregate-ops", type=int, default=30)
    p.add_argument("--mixed-ops", type=int, default=500)
    p.add_argument("--timeout", type=float, default=15)
    p.add_argument("--max-seconds", type=int, default=7200)
    p.add_argument("--plan", action="store_true", help="validate and print settings; start nothing")
    return p


def main():
    args = parser().parse_args()
    # Keep venv/bin/python's symlink path: resolve() would lose virtualenv selection.
    args.python = args.python.absolute()
    for key in ("before", "after", "harness", "output"):
        setattr(args, key, getattr(args, key).resolve())
    for path in (args.before, args.after, args.python):
        if not path.is_file() or not os.access(path, os.X_OK):
            raise ValueError(f"missing executable: {path}")
    if args.output.exists():
        raise ValueError("--output must be a new path; existing results are never overwritten")
    rows = [int(n) for n in args.rows.split(",")]
    if len(rows) != len(set(rows)) or not rows or min(rows) < 100:
        raise ValueError("--rows must be unique integers >=100")
    if min(args.repeats, args.read_ops, args.write_ops, args.batch_ops, args.aggregate_ops,
           args.mixed_ops, args.timeout, args.max_seconds) <= 0:
        raise ValueError("operation counts and timeouts must be positive")
    provenance = verify_harness(args.harness)
    server_cpus, client_cpus = select_cpus(args.server_cpus, args.client_cpus)
    plan = {"host": host_metadata(), "server_cpus": server_cpus, "client_cpus": client_cpus,
            "before": str(args.before), "after": str(args.after), "output": str(args.output),
            "rows": rows, "repeats": args.repeats, "workloads": WORKLOADS,
            "harness_provenance": provenance, "builds_detected": active_builds()}
    if args.plan:
        print(json.dumps(plan, indent=2))
        return 0
    require_idle()
    require_free_ports()
    lock_path = Path("/tmp") / f"multidb-benchmark-{os.getuid()}.lock"
    lock_fd = os.open(lock_path, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(lock_fd, "w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        args.output.mkdir(parents=True, exist_ok=False)
        write_json(args.output / "run.json", {**plan, "started_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat()})
        with (args.output / "python-packages.json").open("w") as log:
            subprocess.run([str(args.python), "-c", "import importlib.metadata,json; print(json.dumps({p.metadata['Name']:p.version for p in importlib.metadata.distributions()},sort_keys=True,indent=2))"],
                           check=True, stdout=log, timeout=30)
        try:
            before = run_case(args, "before", args.before, args.before_revision, client_cpus, server_cpus)
            after = run_case(args, "after", args.after, args.after_revision, client_cpus, server_cpus)
            report = comparison(before, after, rows, args.repeats)
            write_json(args.output / "comparison.json", report)
            lines = ["# Local native before/after benchmark", "", "Median across repetitions; load has one sample per dataset. Ratios are after/before throughput.", "",
                     "| Rows | Workload | Before req/s | After req/s | Ratio | Before p50 ms | After p50 ms |", "|---:|---|---:|---:|---:|---:|---:|"]
            for item in report:
                b, a = item["before"], item["after"]
                lines.append(f"| {item['rows']} | {item['workload']} | {b[METRICS[0]]['median']:.2f} | {a[METRICS[0]]['median']:.2f} | {item['throughput_ratio_after_over_before']:.3f} | {b['p50_ms']['median']:.3f} | {a['p50_ms']['median']:.3f} |")
            (args.output / "comparison.md").write_text("\n".join(lines) + "\n")
        except BaseException as error:
            write_json(args.output / "FAILED.json", {"error": str(error), "valid_comparison": False})
            raise
    print(args.output / "comparison.md")
    return 0


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt("benchmark interrupted; incomplete results are invalid")

    signal.signal(signal.SIGTERM, interrupted)
    raise SystemExit(main())
