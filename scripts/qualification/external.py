#!/usr/bin/env python3
"""Prepare/run external qualification hooks and validate their evidence contract.

Hook commands are trusted operator-supplied argv arrays, never inferred from an
uploaded report and never run by the local soak. Empty commands remain unrun.
An accepted report needs explicit operator review as well as complete evidence;
hashes bind bytes and do not make the external author trustworthy.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import os
from pathlib import Path
import signal
import subprocess
import time

from qualify import digest, utc_now, write_json

CHECKS = {
    "physical_gpu": ["numerical_reference", "device_loss", "deadlines", "restart", "memory_pressure", "docker_device_passthrough"],
    "cluster": ["failover", "network_partition", "prepared_recovery", "durable_decisions", "transaction_id_uniqueness", "certificate_node_identity"],
    "production_environment": ["sustained_load", "storage_failure", "disk_full", "restore_rpo_rto", "upgrade_rollback", "monitoring_alerts", "independent_security_review"],
}


def contract(gate, binary, revision):
    return {"schema_version": 1, "gate": gate, "artifact": {"sha256": digest(binary), "revision": revision, "path": str(binary)},
            "command": [], "timeout_seconds": 172800 if gate == "production_environment" else 7200,
            "criteria": {"required_checks": CHECKS[gate], "minimum_duration_seconds": 86400 if gate == "production_environment" else 1,
                         "operator_acceptance_required": True},
            "scope": "External hardware/deployment evidence; replace empty command with a reviewed argv, execute on the intended environment and attach operator acceptance"}


def criteria_digest(plan):
    import hashlib
    return hashlib.sha256(json.dumps(plan["criteria"], sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def validate_plan(plan):
    if plan.get("schema_version") != 1 or plan.get("gate") not in CHECKS:
        raise ValueError("unknown external evidence plan")
    if (not isinstance(plan.get("command"), list) or any(not isinstance(v, str) or not v or "\0" in v for v in plan["command"])
            or type(plan.get("timeout_seconds")) is not int or plan["timeout_seconds"] < 1):
        raise ValueError("external command must be an argv array and timeout a positive integer")
    criteria = plan.get("criteria", {})
    if (set(criteria.get("required_checks", [])) != set(CHECKS[plan["gate"]])
            or criteria.get("operator_acceptance_required") is not True
            or type(criteria.get("minimum_duration_seconds")) is not int
            or criteria["minimum_duration_seconds"] < (86400 if plan["gate"] == "production_environment" else 1)):
        raise ValueError("external mandatory acceptance criteria cannot be removed")
    artifact = plan.get("artifact", {})
    if (not isinstance(artifact.get("sha256"), str) or len(artifact["sha256"]) != 64
            or any(v not in "0123456789abcdef" for v in artifact["sha256"]) or not artifact.get("revision")):
        raise ValueError("external plan needs exact artifact hash and source revision")


def validate_evidence(plan, report, directory):
    validate_plan(plan)
    if not isinstance(report, dict):
        return {"status": "blocked", "issues": ["external evidence is not a JSON object"]}
    issues = []
    if (report.get("schema_version") != 1 or report.get("gate") != plan["gate"]
            or report.get("artifact_sha256") != plan["artifact"]["sha256"]
            or report.get("source_revision") != plan["artifact"]["revision"]
            or report.get("criteria_sha256") != criteria_digest(plan)):
        issues.append("evidence schema/gate/artifact/revision/criteria does not match the predeclared plan")
    if report.get("status") == "failed":
        return {"status": "failed", "issues": issues + ["external checks explicitly failed"]}
    if report.get("status") != "accepted":
        issues.append("external checks are not accepted")
    checks = report.get("checks", {})
    if not isinstance(checks, dict):
        checks = {}
    for name in CHECKS[plan["gate"]]:
        if not isinstance(checks.get(name), dict) or checks[name].get("status") != "accepted":
            issues.append(f"required check missing/not accepted:{name}")
    try:
        start = datetime.datetime.fromisoformat(report["started_at_utc"])
        end = datetime.datetime.fromisoformat(report["finished_at_utc"])
        if not start.tzinfo or not end.tzinfo or (end - start).total_seconds() < plan["criteria"]["minimum_duration_seconds"]:
            raise ValueError("duration below predeclared minimum")
    except (KeyError, TypeError, ValueError) as error:
        issues.append(f"invalid external duration:{error}")
    environment = report.get("environment", {})
    if not isinstance(environment, dict):
        environment = {}
    if plan["gate"] == "physical_gpu":
        adapters = environment.get("adapters", [])
        if not isinstance(adapters, list) or not adapters or any(not isinstance(a, dict) or a.get("type") != "physical" or a.get("software") is not False
                               or any(not a.get(k) for k in ("vendor", "model", "driver", "api", "device_id")) for a in adapters):
            issues.append("physical GPU vendor/model/driver/API/device proof is missing; software adapters do not qualify")
    elif plan["gate"] == "cluster":
        nodes = environment.get("nodes", [])
        if (not isinstance(nodes, list) or len(nodes) < 3 or any(not isinstance(n, dict) or any(not n.get(k) for k in ("host_id", "process_id", "certificate_identity")) for n in nodes)
                or len({n.get("host_id") for n in nodes if isinstance(n, dict)}) < 3):
            issues.append("cluster evidence needs at least three distinct hosts/processes with certificate identities")
    else:
        if any(not environment.get(key) for key in ("hardware", "storage", "filesystem", "deployment", "slo_owner")):
            issues.append("intended production hardware/storage/filesystem/deployment/SLO owner missing")
    acceptance = report.get("operator_acceptance", {})
    if not isinstance(acceptance, dict):
        acceptance = {}
    if any(not isinstance(acceptance.get(key), str) or not acceptance[key].strip() for key in ("reviewer", "evidence_ticket", "accepted_at_utc")):
        issues.append("explicit operator review/acceptance is missing; a command exiting zero cannot grant production readiness")
    else:
        try:
            accepted_at = datetime.datetime.fromisoformat(acceptance["accepted_at_utc"])
            finished = datetime.datetime.fromisoformat(report["finished_at_utc"])
            if not accepted_at.tzinfo or not finished.tzinfo or accepted_at < finished:
                raise ValueError("operator acceptance must follow the measured run")
        except (KeyError, TypeError, ValueError) as error:
            issues.append(f"invalid operator acceptance time:{error}")
    sources = report.get("evidence", [])
    if not isinstance(sources, list) or not sources:
        issues.append("no raw evidence files supplied")
        sources = []
    seen = set()
    root = Path(directory).resolve()
    for source in sources:
        try:
            relative = Path(source["path"])
            if relative.is_absolute() or ".." in relative.parts or str(relative) in seen:
                raise ValueError("unsafe/duplicate raw evidence path")
            seen.add(str(relative))
            candidate = root / relative
            if any(p.is_symlink() for p in [candidate, *candidate.parents] if p != root and p.is_relative_to(root)):
                raise ValueError("raw evidence symlink is not accepted")
            if not candidate.is_file() or not candidate.resolve().is_relative_to(root) or digest(candidate) != source["sha256"]:
                raise ValueError("raw evidence missing, escaping directory or hash mismatch")
        except (KeyError, TypeError, ValueError, OSError) as error:
            issues.append(f"invalid raw evidence:{error}")
    return {"status": "blocked" if issues else "accepted", "issues": issues,
            "scope": "structural/hash validation of explicitly reviewed external evidence; external claims still rely on the recorded operator"}


def run_command(command, directory, env, timeout, stream):
    process = subprocess.Popen(command, cwd=directory, env=env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
    try:
        code = process.wait(timeout=timeout)
        if code:
            raise subprocess.CalledProcessError(code, command)
    except BaseException:
        # A timed-out harness may have spawned workers. Stop the owned process
        # group rather than leaving a hidden GPU/load workload behind.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            with contextlib.suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        raise


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="mode", required=True)
    template = commands.add_parser("template")
    template.add_argument("--gate", choices=CHECKS, required=True)
    template.add_argument("--binary", type=Path, required=True)
    template.add_argument("--revision", required=True)
    template.add_argument("--output", type=Path, required=True)
    run = commands.add_parser("run")
    run.add_argument("--plan", type=Path, required=True)
    run.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    if args.output.exists():
        raise FileExistsError("external qualification output must be fresh")
    os.umask(0o077)
    if args.mode == "template":
        write_json(args.output, contract(args.gate, args.binary.resolve(), args.revision))
        return 0
    plan = json.loads(args.plan.read_text())
    validate_plan(plan)
    args.output.mkdir(parents=True, mode=0o700)
    write_json(args.output / "plan.json", plan)
    result = {"schema_version": 1, "gate": plan["gate"], "artifact": plan["artifact"], "criteria_sha256": criteria_digest(plan),
              "started_at_utc": utc_now(), "status": "unrun", "issues": []}
    artifact = Path(plan["artifact"].get("path", ""))
    if not artifact.is_file() or digest(artifact) != plan["artifact"]["sha256"]:
        result.update(status="blocked", issues=["exact artifact is absent/changed on this execution host"])
    elif not plan["command"]:
        result["issues"].append("no reviewed external command configured; nothing executed")
    else:
        env = {key: value for key, value in os.environ.items() if not key.startswith("MULTIDB_QUALIFICATION_")}
        env.update(MULTIDB_QUALIFICATION_REPORT=str((args.output / "external-evidence.json").resolve()),
                   MULTIDB_QUALIFICATION_PLAN=str((args.output / "plan.json").resolve()),
                   MULTIDB_QUALIFICATION_CRITERIA_SHA256=criteria_digest(plan))
        try:
            measured_start = time.monotonic()
            with (args.output / "command.log").open("xb") as stream:
                run_command(plan["command"], args.output, env, plan["timeout_seconds"], stream)
            result["observed_execution_seconds"] = time.monotonic() - measured_start
            evidence = json.loads((args.output / "external-evidence.json").read_text())
            result.update(validate_evidence(plan, evidence, args.output))
            result["raw_report_sha256"] = digest(args.output / "external-evidence.json")
            if result["observed_execution_seconds"] < plan["criteria"]["minimum_duration_seconds"]:
                result.update(status="blocked", issues=result["issues"] + ["observed hook execution is shorter than the predeclared minimum; timestamps alone cannot qualify a soak"])
            if digest(artifact) != plan["artifact"]["sha256"]:
                result.update(status="failed", issues=["artifact changed while external checks ran"])
        except (subprocess.SubprocessError, OSError, ValueError) as error:
            result.update(status="failed", issues=[f"external hook failed:{type(error).__name__}:{error}"])
    result["finished_at_utc"] = utc_now()
    write_json(args.output / "report.json", result)
    return 0 if result["status"] == "accepted" else 2


if __name__ == "__main__":
    raise SystemExit(main())
