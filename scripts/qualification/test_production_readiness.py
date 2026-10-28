import copy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import tarfile
import unittest

import oci_archive

SCRIPT = Path(__file__).resolve().parents[1] / "check-production-readiness.py"
SPEC = importlib.util.spec_from_file_location("production_readiness", SCRIPT)
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def command(repo, *arguments):
    return subprocess.check_output(["git", "-C", str(repo), *arguments], stderr=subprocess.DEVNULL).decode().strip()


def write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2) + "\n")


class ReadinessTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.base = Path(self.directory.name)
        self.repo = self.base / "repo"
        self.repo.mkdir()
        command(self.repo, "init", "-q")
        command(self.repo, "config", "user.name", "Fixture")
        command(self.repo, "config", "user.email", "fixture@example.invalid")
        (self.repo / "Cargo.toml").write_text('[package]\nname="fixture"\nversion="1.0.0"\n')
        (self.repo / "Cargo.lock").write_text("fixture-lock")
        (self.repo / "src").mkdir()
        (self.repo / "src/lib.rs").write_text('const DOC: &str = include_str!("../docs/embedded.md");\n')
        (self.repo / "docs").mkdir()
        (self.repo / "docs/embedded.md").write_text("compiled input")
        (self.repo / "docs/narrative.md").write_text("narrative")
        (self.repo / ".github/workflows").mkdir(parents=True)
        (self.repo / ".github/workflows/release.yml").write_text("secure-default-build")
        (self.repo / ".github/workflows/ci.yml").write_text("required-default-and-kubernetes-jobs")
        (self.repo / "ops/helm/multidb").mkdir(parents=True)
        (self.repo / "ops/helm/multidb/values.yaml").write_text("SYNTHETIC CHART FIXTURE")
        (self.repo / "scripts/qualification").mkdir(parents=True)
        for name in ("kubernetes.py", "kubernetes_runtime.py", "requirements-kubernetes.txt", "oci_archive.py"):
            (self.repo / "scripts/qualification" / name).write_text("SYNTHETIC SOURCE FIXTURE " + name)
        (self.repo / "Dockerfile.prebuilt").write_text("SYNTHETIC DOCKERFILE FIXTURE")
        self.commit()
        info = gate.source_fingerprint(self.repo, "HEAD")
        self.source = {key: info[key] for key in ("runtime_source_revision", "runtime_fingerprint")}
        self.binaries = {}
        artifacts = {}
        for target in gate.TARGETS:
            binary = self.base / target
            binary.write_bytes(target.encode())
            self.binaries[target] = binary
            binding = {**self.source, "artifact_sha256": gate.sha(binary), "target": target, "version": "1.0.0"}
            entries = {}
            for name, checks in gate.CHECKS.items():
                raw_path = f"qualification/evidence/{target}-{name}.log"
                raw = self.repo / raw_path
                raw.parent.mkdir(parents=True, exist_ok=True)
                raw.write_text("TEST FIXTURE ONLY: external claims are synthetic\n")
                facts = self.facts(name)
                raw_proofs = {raw_path: gate.sha(raw)}
                if name == "kubernetes":
                    facts, kubernetes_proofs = self.kubernetes_fixture(target)
                    raw_proofs.update(kubernetes_proofs)
                report = {"schema_version": 1, "gate": name, "status": "accepted", "binding": binding,
                          "criteria_sha256": gate.criteria_hash(name), "facts": facts,
                          "raw_proofs": raw_proofs,
                          "checks": {check: {"status": "accepted", "proofs": list(raw_proofs)} for check in checks},
                          "review": {"reviewer": "synthetic fixture", "acceptance_record": "unit fixture only", "accepted_at_utc": "2001-01-01T00:00:00+00:00",
                                     "independent": True, "organization": "fixture"}}
                path = f"qualification/evidence/{target}-{name}.json"
                write(self.repo / path, report)
                entries[name] = {"status": "accepted", "report": {"path": path, "sha256": gate.sha(self.repo / path)},
                                 "raw_proofs": [{"path": name, "sha256": digest} for name, digest in raw_proofs.items()]}
            artifacts[target] = {"sha256": gate.sha(binary), "gates": entries}
        self.manifest = {"schema_version": 1, "target_version": "1.0.0", "source": self.source,
                         "requested_features": list(gate.FEATURES), "artifacts": artifacts}
        self.path = self.repo / "qualification/releases/1.0.0/manifest.json"
        write(self.path, self.manifest)

    def tearDown(self):
        self.directory.cleanup()

    def commit(self):
        command(self.repo, "add", ".")
        command(self.repo, "commit", "-qm", "fixture")

    def facts(self, name):
        return {
            "core_ci": {"ci_head_sha": self.source["runtime_source_revision"], "required_ci_conclusion": "success",
                        "jobs": {key: "success" for key in gate.REQUIRED_CI_JOBS}, "default_secure_features": True, "features": ["tensor-gpu"]},
            "soak": {"observed_duration_seconds": 86400, "unexpected_errors": 0, "slo_declared_before_run": True, "cold_verified": True, "larger_than_ram_verified": True, "intended_environment": "synthetic"},
            "recovery": {"source_encrypted": True, "target_encrypted": True, "rpo_rto_accepted": True, "acknowledged_writes_lost": 0, "partial_atomic_batches": 0, "tested_previous_version": "0.1.0"},
            "operations": {"runbook_exercised": True, "alerts_delivered": True},
            "kubernetes": {},
            "security": {"unresolved_blockers": 0},
            "physical_gpu": {"physical_adapter": True, "software_adapter": False, "vendor": "fixture", "model": "fixture", "driver": "fixture", "api": "fixture", "device_id": "fixture"},
            "graphrag": {"mean_recall": .96, "minimum_recall": .8, "policy_citation_violations": 0, "cold_index_verified": True, "bounded_memory_deadlines": True},
            "cluster": {"host_ids": ["one", "two", "three"], "certificate_identity_bound": True},
        }[name]

    def kubernetes_fixture(self, target):
        """Tiny synthetic data exercising the contract; never a measured report."""
        base = self.repo / "qualification/evidence" / target
        static_dir, runtime_dir = base / "static", base / "runtime"
        static_dir.mkdir(parents=True)
        runtime_dir.mkdir(parents=True)
        chart_files = {"values.yaml": gate.sha(self.repo / "ops/helm/multidb/values.yaml")}
        static = {"schema_version": 1, "status": "accepted", "production_deployment_qualified": False, "kind": "kubernetes_static_chart_qualification",
                  "chart": {"files": chart_files}, "validator_sha256": gate.sha(self.repo / "scripts/qualification/kubernetes.py"),
                  "requirements_sha256": gate.sha(self.repo / "scripts/qualification/requirements-kubernetes.txt"),
                  "tools": {name: {"sha256": value} for name, value in gate.STATIC_TOOL_SHA256.items()},
                  "sections": {"static_validation": {"status": "accepted"}}, "scenarios": [], "schema_cache": {}}
        for expected, names in (("accept", gate.KUBERNETES_POSITIVE), ("reject", gate.KUBERNETES_NEGATIVE)):
            for name in names:
                values = static_dir / (name + ".values.json")
                write(values, {"fixture": True})
                case = {"name": name, "expected": expected, "status": "accepted", "override_sha256": gate.sha(values),
                        "helm_lint_exit": 0, "helm_template_exit": 0 if expected == "accept" else 1}
                if expected == "accept":
                    rendered = static_dir / (name + ".rendered.yaml")
                    rendered.write_text("SYNTHETIC RENDER FIXTURE")
                    case.update(rendered_sha256=gate.sha(rendered), kubeconform_exit=0,
                                schema_summary={"errors": 0, "invalid": 0, "skipped": 0, "valid": 1})
                static["scenarios"].append(case)
        trace = static_dir / "trace.txt"
        trace.write_text("SYNTHETIC COMMAND TRACE")
        schema = static_dir / "schema.json"
        write(schema, {"fixture": True})
        static["schema_cache"] = {schema.name: gate.sha(schema)}
        count = 2 + 3 * len(gate.KUBERNETES_POSITIVE) + 2 * len(gate.KUBERNETES_NEGATIVE)
        write(static_dir / "commands.json", [{"stdout": "trace.txt", "stderr": "trace.txt", "stdout_sha256": gate.sha(trace),
                                             "stderr_sha256": gate.sha(trace)} for _ in range(count)])
        static["commands_sha256"] = gate.sha(static_dir / "commands.json")
        write(static_dir / "report.json", static)
        linux_hash = gate.sha(self.base / "linux-x86_64")
        case = {"status": "accepted", "binary_sha256": linux_hash, "acknowledged_batch_rows": 32, "process_sigterm_exit_code": 0,
                "old_pod_uid": "synthetic-old", "new_pod_uid": "synthetic-new", "pgwire_verified_tls_certificate_sha256": "1" * 64,
                "denied_policy": "accepted", "restart_result": "complete_readback_verified"}
        profiles = {name: {**case, "profile": name} for name in gate.CPU_PROFILES}
        profiles["in_memory"]["restart_result"] = "expected_loss_verified"
        encrypted = {**case, "profile": "transactional", "encrypted_key_configured": True,
                     "wrong_key_reopen": {"status": "accepted", "pod_uid": "synthetic-wrong", "exit_code": 1}}
        command_log = base / "runtime-command.log"
        command_log.write_text("SYNTHETIC RUNTIME COMMAND")
        runtime = {"schema_version": 1, "status": "accepted", "production_deployment_qualified": False,
                   "runtime_source_revision": self.source["runtime_source_revision"],
                   "binary_sha256": linux_hash, "chart_sha256": chart_files, "profiles": profiles, "encrypted_reopen": encrypted,
                   "runner_sha256": gate.sha(self.repo / "scripts/qualification/kubernetes_runtime.py"),
                   "command_proofs": {command_log.name: gate.sha(command_log)}}
        write(runtime_dir / "runtime.json", runtime)
        def tar_bytes(entries):
            buffer = io.BytesIO()
            with tarfile.open(fileobj=buffer, mode="w:", format=tarfile.USTAR_FORMAT) as archive:
                for name, data in entries:
                    member = tarfile.TarInfo(name)
                    member.size = len(data)
                    archive.addfile(member, io.BytesIO(data))
            return buffer.getvalue()
        layer = tar_bytes([("fixture", b"SYNTHETIC UNIT FIXTURE, NOT AN EXECUTABLE IMAGE")])
        config = json.dumps({"architecture": "amd64", "os": "linux", "rootfs": {"type": "layers",
                             "diff_ids": ["sha256:" + hashlib.sha256(layer).hexdigest()]}}).encode()
        config_name = hashlib.sha256(config).hexdigest() + ".json"
        reference = "multidb-qualification:unit-fixture-only"
        docker_save = runtime_dir / "synthetic.docker.tar"
        docker_save.write_bytes(tar_bytes([(config_name, config), ("layer.tar", layer), ("manifest.json", json.dumps([
            {"Config": config_name, "Layers": ["layer.tar"], "RepoTags": [reference]}]).encode())]))
        image = oci_archive.convert(docker_save, runtime_dir / "runtime-image.oci.tar", reference)
        docker_save.unlink()
        image.update({"binary_sha256": linux_hash, "runtime_source_revision": self.source["runtime_source_revision"],
              "dockerfile_sha256": gate.sha(self.repo / "Dockerfile.prebuilt"),
              "converter_sha256": gate.sha(self.repo / "scripts/qualification/oci_archive.py")})
        write(runtime_dir / "image.json", image)
        facts = {"runtime_target": "linux-x86_64", "runtime_artifact_sha256": linux_hash, "runtime_scope": "linux_deployment_only",
                 "static_report": (static_dir / "report.json").relative_to(self.repo).as_posix(),
                 "runtime_report": (runtime_dir / "runtime.json").relative_to(self.repo).as_posix(),
                 "image_report": (runtime_dir / "image.json").relative_to(self.repo).as_posix()}
        proofs = {path.relative_to(self.repo).as_posix(): gate.sha(path) for path in base.rglob("*") if path.is_file()}
        return facts, proofs

    def verify(self):
        return gate.verify(self.repo, self.repo, self.path, "linux-x86_64", self.binaries["linux-x86_64"], "1.0.0")

    def replace_report(self, name, mutate):
        entry = self.manifest["artifacts"]["linux-x86_64"]["gates"][name]
        path = self.repo / entry["report"]["path"]
        report = gate.strict_json(path)
        mutate(report)
        write(path, report)
        entry["report"]["sha256"] = gate.sha(path)
        write(self.path, self.manifest)

    def replace_kubernetes_raw(self, key, mutate, target="linux-x86_64"):
        entry = self.manifest["artifacts"][target]["gates"]["kubernetes"]
        normalized_path = self.repo / entry["report"]["path"]
        normalized = gate.strict_json(normalized_path)
        name = normalized["facts"][key]
        path = self.repo / name
        value = gate.strict_json(path)
        mutate(value)
        write(path, value)
        normalized["raw_proofs"][name] = gate.sha(path)
        for reference in entry["raw_proofs"]:
            if reference["path"] == name:
                reference["sha256"] = gate.sha(path)
        write(normalized_path, normalized)
        entry["report"]["sha256"] = gate.sha(normalized_path)
        write(self.path, self.manifest)

    def test_complete_exact_artifact_fixture_is_accepted(self):
        self.assertEqual(self.verify()["status"], "accepted")
        self.binaries["linux-x86_64"].write_bytes(b"different-build")
        self.assertEqual(self.verify()["status"], "blocked")

    def test_secure_default_and_kubernetes_jobs_cannot_be_omitted_or_skipped(self):
        required = ("secure-default", "kubernetes-tooling", "kubernetes-runtime")
        self.assertTrue(set(required) <= set(gate.requirements("core_ci")["required_jobs"]))
        for job in required:
            for status in (None, "skipped", "failure"):
                with self.subTest(job=job, status=status):
                    def mutate(report):
                        report["facts"]["jobs"] = {name: "success" for name in gate.REQUIRED_CI_JOBS}
                        if status is None:
                            report["facts"]["jobs"].pop(job)
                        else:
                            report["facts"]["jobs"][job] = status
                    self.replace_report("core_ci", mutate)
                    self.assertEqual(self.verify()["status"], "blocked")

    def test_successful_ci_jobs_do_not_replace_kubernetes_proofs(self):
        self.replace_report("kubernetes", lambda report: report.update(facts={}))
        result = self.verify()
        self.assertEqual(result["gates"]["core_ci"]["status"], "accepted")
        self.assertEqual(result["gates"]["kubernetes"]["status"], "blocked")

    def test_static_matrix_requires_all_profiles_rejections_and_current_validator(self):
        entry = self.manifest["artifacts"]["linux-x86_64"]["gates"]["kubernetes"]
        original = gate.strict_json(self.repo / gate.strict_json(self.repo / entry["report"]["path"])["facts"]["static_report"])
        mutations = (
            lambda report: report["scenarios"].pop(),
            lambda report: report["scenarios"].__setitem__(0, report["scenarios"][1]),
            lambda report: report["scenarios"][-1].update(helm_template_exit=0),
            lambda report: report["scenarios"][0]["schema_summary"].update(skipped=1),
            lambda report: report.update(validator_sha256="0" * 64),
            lambda report: report["chart"]["files"].update({"values.yaml": "0" * 64}),
            lambda report: report["tools"]["helm"].update(sha256="0" * 64),
        )
        for index, mutate in enumerate(mutations):
            self.replace_kubernetes_raw("static_report", lambda report: (report.clear(), report.update(copy.deepcopy(original))))
            self.replace_kubernetes_raw("static_report", mutate)
            with self.subTest(case=index):
                self.assertEqual(self.verify()["gates"]["kubernetes"]["status"], "blocked")

    def test_cpu_runtime_requires_exact_artifact_every_profile_and_wrong_key_proof(self):
        entry = self.manifest["artifacts"]["linux-x86_64"]["gates"]["kubernetes"]
        original = gate.strict_json(self.repo / gate.strict_json(self.repo / entry["report"]["path"])["facts"]["runtime_report"])
        mutations = (
            lambda report: report["profiles"].pop("document"),
            lambda report: report.update(binary_sha256="0" * 64),
            lambda report: report["profiles"]["vector"].update(status="unrun"),
            lambda report: report["profiles"]["in_memory"].update(restart_result="complete_readback_verified"),
            lambda report: report["profiles"]["balanced"].update(process_sigterm_exit_code=137),
            lambda report: report["profiles"]["analytical"].update(profile="transactional"),
            lambda report: report["encrypted_reopen"].update(wrong_key_reopen={"status": "unrun"}),
            lambda report: report["encrypted_reopen"].update(wrong_key_reopen={"status": "accepted"}),
            lambda report: report.update(command_proofs={}),
            lambda report: report.update(runtime_source_revision="0" * 40),
        )
        for index, mutate in enumerate(mutations):
            self.replace_kubernetes_raw("runtime_report", lambda report: (report.clear(), report.update(copy.deepcopy(original))))
            self.replace_kubernetes_raw("runtime_report", mutate)
            with self.subTest(case=index):
                self.assertEqual(self.verify()["gates"]["kubernetes"]["status"], "blocked")

    def test_oci_config_id_cannot_substitute_for_manifest_and_raw_hashes_are_bound(self):
        self.replace_kubernetes_raw("image_report", lambda report: report.update(manifest_digest=report["config_digest"]))
        self.assertEqual(self.verify()["gates"]["kubernetes"]["status"], "blocked")

    def test_oci_digest_claim_must_match_the_actual_archive_descriptors(self):
        self.replace_kubernetes_raw("image_report", lambda report: report.update(manifest_digest="sha256:" + "0" * 64))
        self.assertEqual(self.verify()["gates"]["kubernetes"]["status"], "blocked")

    def test_kubernetes_oci_archive_tampering_is_rejected(self):
        entry = self.manifest["artifacts"]["linux-x86_64"]["gates"]["kubernetes"]
        image = gate.strict_json(self.repo / entry["report"]["path"])["facts"]["image_report"]
        (self.repo / image).with_name("runtime-image.oci.tar").write_bytes(b"changed OCI")
        result = self.verify()["gates"]["kubernetes"]
        self.assertEqual(result["status"], "blocked")
        self.assertIn("hash mismatch", result["reason"])

    def test_windows_gate_explicitly_binds_linux_deployment_artifact(self):
        result = gate.verify(self.repo, self.repo, self.path, "windows-x86_64", self.binaries["windows-x86_64"], "1.0.0")
        self.assertEqual(result["status"], "accepted")
        entry = self.manifest["artifacts"]["windows-x86_64"]["gates"]["kubernetes"]
        path = self.repo / entry["report"]["path"]
        report = gate.strict_json(path)
        report["facts"]["runtime_artifact_sha256"] = gate.sha(self.binaries["windows-x86_64"])
        write(path, report)
        entry["report"]["sha256"] = gate.sha(path)
        write(self.path, self.manifest)
        result = gate.verify(self.repo, self.repo, self.path, "windows-x86_64", self.binaries["windows-x86_64"], "1.0.0")
        self.assertEqual(result["gates"]["kubernetes"]["status"], "blocked")

    def test_evidence_only_commit_does_not_require_self_commit_hash(self):
        self.commit()
        self.assertNotEqual(command(self.repo, "rev-parse", "HEAD"), self.source["runtime_source_revision"])
        self.assertEqual(self.verify()["status"], "accepted")

    def test_version_workflow_lock_and_embedded_docs_are_source_inputs(self):
        baseline = self.source["runtime_fingerprint"]
        for name in ("Cargo.toml", "Cargo.lock", ".github/workflows/release.yml", ".github/workflows/ci.yml", "docs/embedded.md"):
            with self.subTest(name=name):
                path = self.repo / name
                original = path.read_text()
                path.write_text(original + "\nchanged")
                self.assertEqual(self.verify()["status"], "blocked")
                self.commit()
                self.assertNotEqual(gate.source_fingerprint(self.repo, "HEAD")["runtime_fingerprint"], baseline)
                path.write_text(original)
                self.commit()
                self.assertEqual(gate.source_fingerprint(self.repo, "HEAD")["runtime_fingerprint"], baseline)
        (self.repo / "docs/narrative.md").write_text("new narrative")
        self.commit()
        self.assertEqual(self.verify()["status"], "accepted")

    def test_missing_unknown_unrun_and_waived_features_are_blocked(self):
        original = copy.deepcopy(self.manifest)
        for change in ("missing", "unknown", "unrun", "feature"):
            self.manifest = copy.deepcopy(original)
            gates = self.manifest["artifacts"]["linux-x86_64"]["gates"]
            if change == "missing":
                gates.pop("security")
            elif change == "unknown":
                gates["waiver"] = gates["security"]
            elif change == "unrun":
                gates["security"]["status"] = "unrun"
            else:
                self.manifest["requested_features"].remove("physical_gpu")
            write(self.path, self.manifest)
            with self.subTest(change=change):
                self.assertEqual(self.verify()["status"], "blocked")

    def test_nonfinite_and_duplicate_json_are_rejected(self):
        for text in ('{"gate":1,"gate":2}', '{"x":NaN}', '{"x":1e9999}', '{"x":Infinity}'):
            path = self.base / "bad.json"
            path.write_text(text)
            with self.subTest(text=text), self.assertRaises(ValueError):
                gate.strict_json(path)

    def test_report_binding_and_raw_tamper_are_blocked(self):
        self.replace_report("soak", lambda r: r["binding"].update(artifact_sha256="0" * 64))
        self.assertEqual(self.verify()["status"], "blocked")
        self.replace_report("soak", lambda r: r["binding"].update(artifact_sha256=gate.sha(self.binaries["linux-x86_64"])))
        entry = self.manifest["artifacts"]["linux-x86_64"]["gates"]["soak"]
        (self.repo / entry["raw_proofs"][0]["path"]).write_text("tampered")
        self.assertEqual(self.verify()["status"], "blocked")

    def test_15_minute_soak_software_gpu_and_dependent_review_do_not_pass(self):
        for name, mutate in (("soak", lambda r: r["facts"].update(observed_duration_seconds=900)),
                             ("physical_gpu", lambda r: r["facts"].update(software_adapter=True)),
                             ("security", lambda r: r["review"].update(independent=False))):
            self.replace_report(name, mutate)
            with self.subTest(name=name):
                self.assertEqual(self.verify()["gates"][name]["status"], "blocked")

    def test_traversal_and_symlinks_are_rejected(self):
        reference = {"path": "../outside", "sha256": "0" * 64}
        with self.assertRaises(ValueError):
            gate.local_file(self.repo, reference)
        link = self.repo / "qualification/evidence/linked"
        link.symlink_to(self.binaries["linux-x86_64"])
        with self.assertRaises(ValueError):
            gate.local_file(self.repo, {"path": link.relative_to(self.repo).as_posix(), "sha256": gate.sha(link)})
        manifest_link = self.repo / "qualification/releases/linked.json"
        manifest_link.symlink_to(self.path)
        self.assertEqual(gate.verify(self.repo, self.repo, manifest_link, "linux-x86_64", self.binaries["linux-x86_64"], "1.0.0")["status"], "blocked")

    def test_rc_and_zero_major_release_are_never_latest(self):
        for version in ("1.0.0-rc.1", "2.0.0-beta.2+build", "0.1.0"):
            policy = gate.version_policy(version)
            self.assertTrue(policy["prerelease"])
            self.assertEqual(policy["make_latest"], "false")
            self.assertFalse(policy["qualification_required"])
        self.assertTrue(gate.version_policy("1.0.0")["qualification_required"])
        stable_metadata = gate.version_policy("1.0.0+build-with-hyphen")
        self.assertTrue(stable_metadata["qualification_required"])
        self.assertFalse(stable_metadata["prerelease"])
        for version in ("01.0.0", "1.0.0-01", "1.0.0-rc..1"):
            with self.assertRaises(ValueError):
                gate.version_policy(version)

    def test_registry_publication_waits_for_both_platform_gates(self):
        workflow = (SCRIPT.parents[1] / ".github/workflows/release.yml").read_text()
        builders, publisher = workflow.split("\n  publish:", 1)
        self.assertNotIn("--push target/docker-smoke-context", builders)
        self.assertIn("- windows-release", publisher)
        self.assertLess(publisher.index("Recheck downloaded production artifacts"), publisher.index("--push target/docker-smoke-context"))
        self.assertIn("cmp target/docker-smoke-context/multidb target/release/multidb-linux-x86_64", publisher)
        self.assertIn("prerelease: ${{ needs.release-policy.outputs.prerelease == 'true' }}", publisher)
        self.assertIn("make_latest: ${{ needs.release-policy.outputs.make_latest }}", publisher)


if __name__ == "__main__":
    unittest.main()
