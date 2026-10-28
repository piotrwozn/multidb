#!/usr/bin/env python3
"""Fail closed before publishing stable >=1.0.0 artifacts.

Only local, reviewed evidence roots are read. This validates declared acceptance
and byte/source bindings; it does not independently certify an external reviewer,
hardware claim or production environment.
"""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path, PurePosixPath
import posixpath
import re
import subprocess
import sys
import tomllib

SCHEMA = 1
FEATURES = ("physical_gpu", "graphrag", "cluster")
TARGETS = ("linux-x86_64", "windows-x86_64")
REQUIRED_CI_JOBS = ("check", "msrv", "supply-chain", "fuzz", "secure-default", "kubernetes-tooling", "kubernetes-runtime")
CPU_PROFILES = ("transactional", "analytical", "document", "vector", "time_series", "high_durability", "balanced", "in_memory")
KUBERNETES_POSITIVE = (
    "production", "local-dev", "production-large", "explicit-network-storage-tls-spec",
    "cpu-transactional", "cpu-analytical", "cpu-document", "cpu-vector", "cpu-timeseries", "cpu-highdurability", "cpu-balanced",
    "product-game_local_balanced", "product-desktop_app_embedded", "product-ai_agent_memory", "product-secure_app",
    "product-production_cp", "product-analytics_columnar", "cpu-in-memory", "encrypted-spec", "tensor-gpu-nvidia",
    "tensor-gpu-amd", "tensor-gpu-intel", "tensor-gpu-intel-xe", "product-tensor-dataset", "alias-deeplake-like",
)
KUBERNETES_NEGATIVE = (
    "default-missing-digest", "replicas-two", "production-ephemeral", "production-no-network-policy", "bind-port-mismatch",
    "ingress-missing-tls", "ingress-missing-trust", "ingress-missing-proxy", "pg-secret-empty-key", "pg-tls-missing-secret",
    "queue-out-of-range", "concurrency-out-of-range", "unsupported-setting", "unsupported-replication", "empty-network-peer",
    "monitor-without-crd", "memory-without-volatile-ack", "durable-with-volatile-ack", "tensor-without-device",
    "gpu-without-resource", "gpu-mismatched-resource-count", "unsupported-gpu-vendor", "encryption-empty-secret",
    "studio-overlaps-secret", "production-audit-disabled", "audit-anchor-database-collision", "audit-empty-secret", "audit-empty-secret-key",
)
STATIC_TOOL_SHA256 = {"helm": "b66cb19f2b31f9d427632ea6c4b4a1ff89b74ecaeaee323cd166da41a7af53bd",
                      "kubeconform": "457722b36e98d7bdbc91525e594951ef5d7ea3a29da2d13b9b28a2fb8a5097b7"}
CHECKS = {
    "core_ci": ("required_ci", "default_secure_features", "all_features", "no_default_features", "msrv", "sdks_studio", "container", "fuzz", "dependency_policy_sbom"),
    "soak": ("declared_slo", "sustained_concurrent_load", "slow_disconnect_reconnect", "denied_policy", "acknowledged_full_readback", "backup_gc", "resource_ceilings", "cold", "larger_than_ram"),
    "recovery": ("full_incremental", "pitr_lsn_time", "encrypted_source_target", "key_rotation_recovery", "damaged_chain_rejected", "process_crash", "disk_full_short_write", "upgrade_rollback", "rpo_rto"),
    "operations": ("deployment_runbook", "readiness_shutdown", "disk_alerts", "backup_schedule_restore_drill", "key_custody", "incident_rollback", "supported_platforms_versions"),
    "kubernetes": ("static_all_profiles", "unsafe_values_rejected", "cpu_all_profiles", "pvc_and_volatile_restarts", "denied_policy_tls", "graceful_sigterm", "encrypted_wrong_key_reopen", "exact_linux_image"),
    "security": ("independent_review", "authn_authz_tenants", "wire_foreign_trust", "key_lifecycle", "audit_durability", "exposure_response_policy"),
    "physical_gpu": ("physical_adapter", "numerical_reference", "device_loss", "deadlines_restart", "memory_pressure", "docker_passthrough"),
    "graphrag": ("exact_reference_recall", "policy_citation_integrity", "concurrent_mutation", "cold_index", "bounded_memory_deadlines"),
    "cluster": ("separate_hosts", "partition_failover", "prepared_recovery", "durable_decisions_unique_ids", "certificate_node_identity", "ap_shard_bounds", "migration_convergence"),
}
EXCLUDED_TREES = ("qualification/evidence/", "qualification/releases/", "docs/release-evidence/", "docs/performance/")
HEX = re.compile(r"[0-9a-f]{64}\Z")
REV = re.compile(r"[0-9a-f]{40}\Z")


def strict_json(path):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError(f"duplicate JSON key:{key}")
            result[key] = value
        return result
    def constant(value):
        raise ValueError(f"nonfinite JSON number:{value}")
    def finite_float(value):
        number = float(value)
        if not math.isfinite(number):
            raise ValueError(f"nonfinite JSON number:{value}")
        return number
    with Path(path).open("rb") as stream:
        raw = stream.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("normalized manifest/report exceeds8MiB; keep bulk raw proofs separate")
    return json.loads(raw.decode("utf8"), object_pairs_hook=pairs, parse_constant=constant, parse_float=finite_float)


def sha(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def version_policy(version):
    number = r"(?:0|[1-9][0-9]*)"
    matched = re.fullmatch(rf"({number})\.({number})\.({number})(?:-([0-9A-Za-z.-]+))?(?:\+([0-9A-Za-z.-]+))?", version)
    if not matched:
        raise ValueError("invalid semantic version")
    prerelease, build = matched.group(4), matched.group(5)
    for field in (prerelease, build):
        if field is not None and any(not part for part in field.split(".")):
            raise ValueError("empty semantic-version identifier")
    if prerelease and any(part.isdigit() and len(part) > 1 and part[0] == "0" for part in prerelease.split(".")):
        raise ValueError("numeric prerelease identifiers cannot have leading zeroes")
    core = tuple(map(int, matched.group(1, 2, 3)))
    preview = bool(prerelease) or core[0] == 0
    return {"version": version, "qualification_required": not prerelease and core >= (1, 0, 0),
            "prerelease": preview, "make_latest": "false" if preview else "true"}


def git(repo, *args):
    return subprocess.check_output(["git", "-C", str(repo), *args], stderr=subprocess.PIPE)


def excluded(path):
    return (path.startswith(EXCLUDED_TREES) or path == "README.md" or (path.startswith("docs/") and path.endswith(".md")))


def source_fingerprint(repo, revision):
    commit = git(repo, "rev-parse", "--verify", revision + "^{commit}").decode().strip()
    entries = {}
    for record in git(repo, "ls-tree", "-rz", "--full-tree", commit).split(b"\0"):
        if not record:
            continue
        metadata, name = record.split(b"\t", 1)
        mode, kind, object_id = metadata.decode().split()
        path = name.decode("utf8")
        if kind != "blob":
            raise ValueError(f"unbound submodule/non-blob source input:{path}")
        entries[path] = (mode, object_id)
    # Markdown used by include_str!/include_bytes! remains a build input even
    # though ordinary narrative/evidence commits can change without rebuilding.
    included = set()
    for path, (_, object_id) in entries.items():
        if not path.endswith(".rs"):
            continue
        source = git(repo, "cat-file", "blob", object_id).decode("utf8")
        for macro in re.finditer(r"include_(?:str|bytes)!\s*\(\s*([^)]*)\)", source):
            literal = re.fullmatch(r'"([^"\\]+)"\s*,?\s*', macro.group(1), re.S)
            if not literal:
                raise ValueError(f"nonliteral include needs explicit fingerprint support:{path}")
            relative = posixpath.normpath(posixpath.join(posixpath.dirname(path), literal.group(1)))
            if relative.startswith("../") or relative not in entries:
                raise ValueError(f"include input is not bound to this source tree:{relative}")
            included.add(relative)
    selected = {path: entry for path, entry in entries.items() if not excluded(path) or path in included}
    value = hashlib.sha256()
    for path, (mode, object_id) in sorted(selected.items()):
        if mode == "120000":
            raise ValueError(f"source symlink target is not independently bound:{path}")
        # Git blob IDs bind exact bytes; also hash them with SHA256 so this
        # fingerprint does not inherit Git's SHA1 collision assumptions.
        content = git(repo, "cat-file", "blob", object_id)
        value.update(path.encode() + b"\0" + mode.encode() + b"\0" + hashlib.sha256(content).digest())
    return {"runtime_source_revision": commit, "runtime_fingerprint": value.hexdigest(), "bound_inputs": len(selected), "included_inputs": sorted(included)}


def dirty_source(repo, included=()):
    changed = set()
    for args in (("diff", "--name-only", "-z"), ("diff", "--cached", "--name-only", "-z"),
                 ("ls-files", "--others", "--exclude-standard", "-z")):
        changed.update(name.decode() for name in git(repo, *args).split(b"\0") if name)
    # Included Markdown is conservatively considered dirty here too. Ordinary
    # uncommitted narrative edits must not hide a changed compile-time include.
    return sorted(path for path in changed if not excluded(path) or path in included)


def local_file(root, reference):
    if not isinstance(reference, dict) or set(reference) != {"path", "sha256"} or not isinstance(reference.get("path"), str) or not HEX.fullmatch(str(reference.get("sha256", ""))):
        raise ValueError("file reference must contain exactly relative path and SHA256")
    relative = PurePosixPath(reference["path"])
    if (reference["path"] != relative.as_posix() or relative.is_absolute() or not relative.parts
            or ".." in relative.parts or "\\" in reference["path"] or ":" in reference["path"]):
        raise ValueError("unsafe evidence path")
    candidate = root.joinpath(*relative.parts)
    if any(path.is_symlink() for path in [candidate, *candidate.parents] if path != root and path.is_relative_to(root)):
        raise ValueError("symlink evidence is not accepted")
    if not candidate.is_file() or not candidate.resolve().is_relative_to(root) or sha(candidate) != reference["sha256"]:
        raise ValueError(f"evidence missing/escaping/hash mismatch:{reference['path']}")
    return candidate


def requirements(gate):
    result = {"schema_version": SCHEMA, "gate": gate, "required_checks": list(CHECKS[gate]),
            "minimum_soak_seconds": 86400 if gate == "soak" else None,
            "independent_review_required": gate == "security", "operator_acceptance_required": True}
    if gate == "core_ci":
        result["required_jobs"] = list(REQUIRED_CI_JOBS)
    if gate == "kubernetes":
        result.update(runtime_target="linux-x86_64", cpu_profiles=list(CPU_PROFILES),
                      static_positive=list(KUBERNETES_POSITIVE), static_negative=list(KUBERNETES_NEGATIVE),
                      static_tools_sha256=STATIC_TOOL_SHA256,
                      scope="Linux deployment proof for the exact Linux release artifact; Windows artifact is not executed by Kind; physical GPU/HA remain separate gates")
    return result


def criteria_hash(gate):
    return hashlib.sha256(json.dumps(requirements(gate), sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def kubernetes_proof(repo, root, facts, proofs, binding, linux_artifact_sha256):
    """Validate actual runner reports as raw proofs, including their raw files.

    Kind executes the Linux artifact. A Windows release must bind the same Linux
    deployment proof explicitly; this never claims that Windows ran inside Kind.
    """
    if (set(facts) != {"runtime_target", "runtime_artifact_sha256", "runtime_scope", "static_report", "runtime_report", "image_report"}
            or facts.get("runtime_target") != "linux-x86_64" or facts.get("runtime_artifact_sha256") != linux_artifact_sha256
            or facts.get("runtime_scope") != "linux_deployment_only"):
        raise ValueError("kubernetes:explicit exact Linux deployment artifact/scope required")

    def proof(name, expected=None):
        if not isinstance(name, str) or name not in proofs or (expected is not None and proofs[name] != expected):
            raise ValueError("kubernetes:referenced raw proof missing or conflicting")
        return local_file(root, {"path": name, "sha256": proofs[name]})

    def adjacent(parent, name, expected):
        if (not HEX.fullmatch(str(expected)) or not isinstance(name, str) or PurePosixPath(name).is_absolute()
                or ".." in PurePosixPath(name).parts):
            raise ValueError("kubernetes:unsafe nested proof path")
        return proof((PurePosixPath(parent).parent / name).as_posix(), expected)

    static_name, runtime_name, image_name = (facts.get(key) for key in ("static_report", "runtime_report", "image_report"))
    static, runtime, image = (strict_json(proof(name)) for name in (static_name, runtime_name, image_name))
    if any(not isinstance(report, dict) for report in (static, runtime, image)):
        raise ValueError("kubernetes:raw reports must be JSON objects")
    chart = repo / "ops/helm/multidb"
    chart_files = {path.relative_to(chart).as_posix(): sha(path) for path in sorted(chart.rglob("*")) if path.is_file()}
    if (not chart_files or static.get("schema_version") != 1 or static.get("status") != "accepted"
            or static.get("production_deployment_qualified") is not False
            or static.get("kind") != "kubernetes_static_chart_qualification" or static.get("chart", {}).get("files") != chart_files
            or static.get("validator_sha256") != sha(repo / "scripts/qualification/kubernetes.py")
            or static.get("requirements_sha256") != sha(repo / "scripts/qualification/requirements-kubernetes.txt")
            or {name: data.get("sha256") for name, data in static.get("tools", {}).items()} != STATIC_TOOL_SHA256
            or static.get("sections", {}).get("static_validation", {}).get("status") != "accepted"):
        raise ValueError("kubernetes:accepted current-source static report required")
    scenarios = static.get("scenarios")
    if (not isinstance(scenarios, list) or len(scenarios) != len(KUBERNETES_POSITIVE) + len(KUBERNETES_NEGATIVE)
            or len({scenario.get("name") for scenario in scenarios}) != len(scenarios)):
        raise ValueError("kubernetes:complete unique static scenario matrix required")
    expected_scenarios = {**dict.fromkeys(KUBERNETES_POSITIVE, "accept"), **dict.fromkeys(KUBERNETES_NEGATIVE, "reject")}
    if {scenario.get("name"): scenario.get("expected") for scenario in scenarios} != expected_scenarios:
        raise ValueError("kubernetes:missing/unknown static profile or unsafe-values scenario")
    for scenario in scenarios:
        name = scenario["name"]
        if scenario.get("status") != "accepted":
            raise ValueError("kubernetes:unaccepted static scenario")
        adjacent(static_name, name + ".values.json", scenario.get("override_sha256"))
        if scenario["expected"] == "reject":
            if type(scenario.get("helm_template_exit")) is not int or scenario["helm_template_exit"] == 0:
                raise ValueError("kubernetes:unsafe values were not rejected by actual Helm render")
        else:
            summary = scenario.get("schema_summary", {})
            if (scenario.get("helm_lint_exit") != 0 or scenario.get("helm_template_exit") != 0 or scenario.get("kubeconform_exit") != 0
                    or any(type(summary.get(key)) is not int or summary[key] != 0 for key in ("errors", "invalid", "skipped"))
                    or type(summary.get("valid")) is not int or summary["valid"] < 1):
                raise ValueError("kubernetes:strict rendered API schema validation missing")
            adjacent(static_name, name + ".rendered.yaml", scenario.get("rendered_sha256"))
    commands = strict_json(adjacent(static_name, "commands.json", static.get("commands_sha256")))
    if not isinstance(commands, list) or len(commands) < 2 + 3 * len(KUBERNETES_POSITIVE) + 2 * len(KUBERNETES_NEGATIVE):
        raise ValueError("kubernetes:actual Helm/schema command trace missing")
    for command in commands:
        for stream in ("stdout", "stderr"):
            adjacent(static_name, command.get(stream), command.get(stream + "_sha256"))
    if not isinstance(static.get("schema_cache"), dict) or not static["schema_cache"]:
        raise ValueError("kubernetes:strict API schema raw proofs missing")
    for name, expected in static["schema_cache"].items():
        adjacent(static_name, name, expected)
    if (runtime.get("schema_version") != 1 or runtime.get("status") != "accepted"
            or runtime.get("production_deployment_qualified") is not False
            or runtime.get("runtime_source_revision") != binding["runtime_source_revision"]
            or runtime.get("binary_sha256") != linux_artifact_sha256 or runtime.get("chart_sha256") != chart_files
            or runtime.get("runner_sha256") != sha(repo / "scripts/qualification/kubernetes_runtime.py")
            or set(runtime.get("profiles", {})) != set(CPU_PROFILES)):
        raise ValueError("kubernetes:current source/exact Linux artifact/all CPU runtime reports required")
    for profile, result in {**runtime["profiles"], "encrypted": runtime.get("encrypted_reopen", {})}.items():
        if (result.get("status") != "accepted" or result.get("binary_sha256") != linux_artifact_sha256
                or result.get("profile") != ("transactional" if profile == "encrypted" else profile)
                or type(result.get("acknowledged_batch_rows")) is not int or result["acknowledged_batch_rows"] < 32
                or type(result.get("process_sigterm_exit_code")) is not int or result["process_sigterm_exit_code"] != 0
                or not result.get("old_pod_uid") or not result.get("new_pod_uid") or result["old_pod_uid"] == result["new_pod_uid"]
                or not HEX.fullmatch(str(result.get("pgwire_verified_tls_certificate_sha256", "")))
                or result.get("denied_policy") != "accepted"
                or result.get("restart_result") != ("expected_loss_verified" if profile == "in_memory" else "complete_readback_verified")):
            raise ValueError("kubernetes:actual TLS/policy/graceful/replacement/readback proof missing")
        if profile == "encrypted":
            wrong = result.get("wrong_key_reopen", {})
            if (result.get("encrypted_key_configured") is not True or wrong.get("status") != "accepted"
                    or not wrong.get("pod_uid") or type(wrong.get("exit_code")) is not int or wrong["exit_code"] == 0):
                raise ValueError("kubernetes:actual wrong-key startup rejection and correct-key readback required")
    command_proofs = runtime.get("command_proofs")
    if not isinstance(command_proofs, dict) or not command_proofs:
        raise ValueError("kubernetes:runtime command raw proofs missing")
    for name, expected in command_proofs.items():
        if not HEX.fullmatch(str(expected)) or not isinstance(name, str) or PurePosixPath(name).name != name:
            raise ValueError("kubernetes:runtime command filename must be a basename")
        proof((PurePosixPath(runtime_name).parent.parent / name).as_posix(), expected)
    if (image.get("binary_sha256") != linux_artifact_sha256 or image.get("runtime_source_revision") != binding["runtime_source_revision"]
            or image.get("dockerfile_sha256") != sha(repo / "Dockerfile.prebuilt")
            or image.get("converter_sha256") != sha(repo / "scripts/qualification/oci_archive.py")
            or image.get("manifest_digest") == image.get("config_digest")
            or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", str(image.get(key, ""))) for key in ("manifest_digest", "config_digest"))
            or not isinstance(image.get("layer_digests"), list) or not image["layer_digests"]
            or any(not re.fullmatch(r"sha256:[0-9a-f]{64}", str(value)) for value in image["layer_digests"])):
        raise ValueError("kubernetes:actual OCI manifest/config/layer and release artifact binding required")
    archive = adjacent(image_name, "runtime-image.oci.tar", image.get("archive_sha256"))
    # Load the checker-bundled, source-bound helper, never code from the evidence
    # root. It streams blob hashes and validates index/manifest descriptors.
    helper = Path(__file__).with_name("qualification") / "oci_archive.py"
    spec = importlib.util.spec_from_file_location("readiness_oci_archive", helper)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.verify_oci_archive(archive, image)


def gate_report(root, gate, entry, binding, repo=None, linux_artifact_sha256=None):
    if not isinstance(entry, dict) or set(entry) != {"status", "report", "raw_proofs"} or entry["status"] != "accepted":
        raise ValueError(f"{gate}:missing/unrun/blocked/failed or malformed gate")
    path = local_file(root, entry["report"])
    raw = entry["raw_proofs"]
    if not isinstance(raw, list) or not raw:
        raise ValueError(f"{gate}:raw proof files are required")
    proofs = {}
    for reference in raw:
        local_file(root, reference)
        name = reference["path"]
        if name in proofs or name == entry["report"]["path"]:
            raise ValueError(f"{gate}:duplicate/self-referential proof")
        proofs[name] = reference["sha256"]
    report = strict_json(path)
    expected_keys = {"schema_version", "gate", "status", "binding", "criteria_sha256", "checks", "raw_proofs", "review", "facts"}
    if not isinstance(report, dict) or set(report) != expected_keys:
        raise ValueError(f"{gate}:unknown/missing report fields")
    if report["schema_version"] != SCHEMA or report["gate"] != gate or report["status"] != "accepted" or report["binding"] != binding or report["criteria_sha256"] != criteria_hash(gate):
        raise ValueError(f"{gate}:report status/artifact/source/criteria binding mismatch")
    if report["raw_proofs"] != proofs:
        raise ValueError(f"{gate}:report and manifest raw proof sets disagree")
    checks = report["checks"]
    if not isinstance(checks, dict) or set(checks) != set(CHECKS[gate]):
        raise ValueError(f"{gate}:unknown/missing required checks")
    for name, check in checks.items():
        if (not isinstance(check, dict) or set(check) != {"status", "proofs"} or check["status"] != "accepted"
                or not isinstance(check["proofs"], list) or not check["proofs"] or len(set(check["proofs"])) != len(check["proofs"])
                or any(p not in proofs for p in check["proofs"])):
            raise ValueError(f"{gate}/{name}:check is unaccepted or lacks bound raw proof")
    review = report["review"]
    if not isinstance(review, dict) or any(not isinstance(review.get(key), str) or not review[key].strip() for key in ("reviewer", "acceptance_record", "accepted_at_utc")):
        raise ValueError(f"{gate}:explicit operator acceptance missing")
    try:
        accepted_at = datetime.datetime.fromisoformat(review["accepted_at_utc"])
        if not accepted_at.tzinfo or accepted_at > datetime.datetime.now(datetime.timezone.utc):
            raise ValueError("acceptance timestamp needs timezone and cannot be in the future")
    except ValueError as error:
        raise ValueError(f"{gate}:invalid acceptance timestamp") from error
    facts = report["facts"]
    if not isinstance(facts, dict):
        raise ValueError(f"{gate}:facts missing")
    if gate == "kubernetes":
        if repo is None or not HEX.fullmatch(str(linux_artifact_sha256)):
            raise ValueError("kubernetes:exact Linux artifact binding unavailable")
        kubernetes_proof(repo, root, facts, proofs, binding, linux_artifact_sha256)
    if gate == "core_ci":
        if (facts.get("ci_head_sha") != binding["runtime_source_revision"] or facts.get("required_ci_conclusion") != "success"
                or facts.get("jobs") != {key: "success" for key in REQUIRED_CI_JOBS}
                or facts.get("default_secure_features") is not True or not isinstance(facts.get("features"), list)
                or "insecure-transport" in facts["features"]):
            raise ValueError("core_ci:complete required successful jobs and secure source/artifact build facts required")
    if gate == "soak" and (type(facts.get("observed_duration_seconds")) not in (int, float) or facts["observed_duration_seconds"] < 86400
                           or facts.get("unexpected_errors") != 0 or type(facts.get("unexpected_errors")) is not int):
        raise ValueError("soak:24-hour intended-environment run with zero unexpected errors required")
    if gate == "soak" and (any(facts.get(key) is not True for key in ("slo_declared_before_run", "cold_verified", "larger_than_ram_verified"))
                           or not facts.get("intended_environment")):
        raise ValueError("soak:predeclared SLO, cold/large pressure and intended-environment evidence required")
    if gate == "recovery" and (any(facts.get(key) is not True for key in ("source_encrypted", "target_encrypted", "rpo_rto_accepted"))
                               or any(type(facts.get(key)) is not int or facts[key] != 0 for key in ("acknowledged_writes_lost", "partial_atomic_batches"))
                               or not facts.get("tested_previous_version")):
        raise ValueError("recovery:encrypted recovery, zero acknowledged/atomic loss and actual previous-version upgrade proof required")
    if gate == "operations" and any(facts.get(key) is not True for key in ("runbook_exercised", "alerts_delivered")):
        raise ValueError("operations:executed runbook and delivered alert drill required")
    if gate == "security" and (review.get("independent") is not True or not review.get("organization")):
        raise ValueError("security:independent reviewer/organization required")
    if gate == "security" and (type(facts.get("unresolved_blockers")) is not int or facts["unresolved_blockers"] != 0):
        raise ValueError("security:confirmed blockers must be resolved")
    if gate == "physical_gpu" and (facts.get("physical_adapter") is not True or facts.get("software_adapter") is not False
                                   or any(not facts.get(k) for k in ("vendor", "model", "driver", "api", "device_id"))):
        raise ValueError("physical_gpu:real hardware proof missing")
    if gate == "graphrag" and (type(facts.get("mean_recall")) not in (int, float) or not .95 <= facts["mean_recall"] <= 1
                               or type(facts.get("minimum_recall")) not in (int, float) or not .8 <= facts["minimum_recall"] <= 1
                               or type(facts.get("policy_citation_violations")) is not int or facts["policy_citation_violations"] != 0
                               or any(facts.get(key) is not True for key in ("cold_index_verified", "bounded_memory_deadlines"))):
        raise ValueError("graphrag:qualified recall, citation/policy integrity and cold/bounded index proof required")
    if gate == "cluster" and (not isinstance(facts.get("host_ids"), list) or any(not isinstance(h, str) or not h.strip() for h in facts["host_ids"])
                             or len(set(facts["host_ids"])) < 3 or facts.get("certificate_identity_bound") is not True):
        raise ValueError("cluster:three independent hosts required")
    return {"status": "accepted", "report_sha256": entry["report"]["sha256"], "raw_files_checked": len(proofs)}


def verify(repo, root, manifest_path, target, binary, version):
    result = {"schema_version": 1, "status": "blocked", "version": version, "target": target, "issues": [], "gates": {}}
    try:
        manifest_path = manifest_path.absolute()
        if not manifest_path.is_relative_to(root) or not manifest_path.is_file():
            raise ValueError("manifest must exist under the reviewed local evidence root")
        relative_manifest = manifest_path.relative_to(root).as_posix()
        local_file(root, {"path": relative_manifest, "sha256": sha(manifest_path)})
        document = strict_json(manifest_path)
        if not isinstance(document, dict) or set(document) != {"schema_version", "target_version", "source", "requested_features", "artifacts"}:
            raise ValueError("unknown/missing manifest fields")
        if document["schema_version"] != SCHEMA or document["target_version"] != version:
            raise ValueError("manifest schema/version mismatch")
        if not isinstance(document["requested_features"], list) or sorted(document["requested_features"]) != sorted(FEATURES):
            raise ValueError("all requested GPU/GraphRAG/cluster features require qualification; no waiver/unknown feature accepted")
        source = document["source"]
        if not isinstance(source, dict) or set(source) != {"runtime_source_revision", "runtime_fingerprint"} or not REV.fullmatch(str(source["runtime_source_revision"])) or not HEX.fullmatch(str(source["runtime_fingerprint"])):
            raise ValueError("immutable source revision/fingerprint required")
        historical = source_fingerprint(repo, source["runtime_source_revision"])
        current = source_fingerprint(repo, "HEAD")
        if current["runtime_fingerprint"] != source["runtime_fingerprint"] or historical["runtime_fingerprint"] != source["runtime_fingerprint"]:
            raise ValueError("runtime/build/release inputs differ from the qualified source tree")
        dirty = dirty_source(repo, current["included_inputs"])
        if dirty:
            raise ValueError(f"uncommitted runtime/build inputs:{dirty}")
        embedded = tomllib.loads((repo / "Cargo.toml").read_text())["package"]["version"]
        if embedded != version:
            raise ValueError("Cargo version disagrees with qualification target")
        artifacts = document["artifacts"]
        if not isinstance(artifacts, dict) or set(artifacts) != set(TARGETS):
            raise ValueError("exact Linux and Windows artifacts are both required")
        for platform_name, artifact in artifacts.items():
            if not isinstance(artifact, dict) or set(artifact) != {"sha256", "gates"} or not HEX.fullmatch(str(artifact["sha256"])) or not isinstance(artifact["gates"], dict) or set(artifact["gates"]) != set(CHECKS):
                raise ValueError(f"{platform_name}:exact artifact and complete known gates required")
        selected = artifacts[target]
        if not binary or not binary.is_file() or sha(binary) != selected["sha256"]:
            raise ValueError("actual final local release binary is absent or its hash differs from qualified artifact")
        result["artifact_sha256"] = selected["sha256"]
        result["source"] = source
        binding = {**source, "artifact_sha256": selected["sha256"], "target": target, "version": version}
        for gate, entry in selected["gates"].items():
            try:
                result["gates"][gate] = gate_report(root, gate, entry, binding, repo, artifacts["linux-x86_64"]["sha256"])
            except (ValueError, OSError, TypeError, KeyError, AttributeError) as error:
                result["gates"][gate] = {"status": "blocked", "reason": str(error)}
                result["issues"].append(str(error))
        if not result["issues"]:
            result["status"] = "accepted"
        result["manifest_sha256"] = sha(manifest_path)
    except (ValueError, OSError, TypeError, KeyError, subprocess.SubprocessError) as error:
        result["issues"].append(str(error))
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--fingerprint", action="store_true")
    parser.add_argument("--source-revision", default="HEAD")
    parser.add_argument("--policy", action="store_true")
    parser.add_argument("--requirements", choices=CHECKS)
    parser.add_argument("--version")
    parser.add_argument("--github-output", type=Path)
    parser.add_argument("--evidence-root", type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--target", choices=TARGETS)
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args(argv)
    try:
        repo = args.repo.resolve()
        version = args.version or tomllib.loads((repo / "Cargo.toml").read_text())["package"]["version"]
        policy = version_policy(version)
        if args.fingerprint:
            value = source_fingerprint(repo, args.source_revision)
        elif args.policy:
            value = policy
            if args.github_output:
                with args.github_output.open("a") as stream:
                    for key in ("qualification_required", "prerelease", "make_latest"):
                        stream.write(f"{key}={str(value[key]).lower()}\n")
        elif args.requirements:
            value = {**requirements(args.requirements), "criteria_sha256": criteria_hash(args.requirements)}
        else:
            if not args.evidence_root or not args.manifest or not args.target or not args.binary:
                raise ValueError("verification requires local --evidence-root, --manifest, --target and actual --binary")
            value = verify(repo, args.evidence_root.resolve(), args.manifest, args.target, args.binary.resolve(), version)
        print(json.dumps(value, indent=2, allow_nan=False))
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(value, indent=2, allow_nan=False) + "\n")
        return 0 if "status" not in value or value["status"] == "accepted" else 1
    except (ValueError, KeyError, TypeError, OSError, subprocess.SubprocessError) as error:
        print(json.dumps({"schema_version": 1, "status": "blocked", "issues": [str(error)]}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
