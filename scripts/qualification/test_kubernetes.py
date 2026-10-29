#!/usr/bin/env python3
"""Mutation regressions for parsed Kubernetes qualification evidence.

The embedded baseline is actual Helm output captured before the mutation tests.
Live chart lint/render/schema validation remains a separate executable run.
"""
import copy
from decimal import Decimal
import importlib.util
import json
import os
from pathlib import Path
import stat
import subprocess
import tempfile
import types
import unittest

MODULE_PATH = Path(__file__).with_name("kubernetes.py")
SPEC = importlib.util.spec_from_file_location("multidb_kubernetes_qualification", MODULE_PATH)
kube = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(kube)


class StrictYamlTests(unittest.TestCase):
    def test_nested_and_flow_duplicate_keys_fail(self):
        for source in ("spec:\n  replicas: 1\n  replicas: 2\n", "spec: {replicas: 1, replicas: 2}"):
            with self.subTest(source=source), self.assertRaisesRegex(ValueError, "duplicate"):
                kube.strict_yaml(source)

    def test_aliases_unsafe_tags_and_non_string_keys_fail(self):
        for source in ("a: &same {safe: true}\nb: *same", "a: !!python/object/apply:os.system [id]", "1: value"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                kube.strict_yaml(source)

    def test_quantities_compare_storage_bytes_and_fractional_cpu(self):
        self.assertEqual(kube.quantity("1000m"), kube.quantity("1"))
        self.assertEqual(kube.quantity("1Gi"), Decimal(1073741824))
        self.assertEqual(kube.quantity("1e3"), kube.quantity("1k"))
        self.assertEqual(kube.quantity("1.5Gi"), kube.quantity("1536Mi"))
        for bad in ("1bogus", "Gi", "NaN", "2KiB"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                kube.quantity(bad)


class PrivateAnchorInitializationTests(unittest.TestCase):
    def test_actual_init_preserves_private_anchor_and_refuses_symlinks_or_legacy_evidence(self):
        for case in ("fresh", "existing", "directory-symlink", "legacy-file", "legacy-dangling-symlink"):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                data = root / "data"
                data.mkdir(mode=0o777)
                data.chmod(0o777)
                private = data / "audit-private"
                legacy = data / "audit.anchor"
                target = root / "untouched-target"
                if case == "existing":
                    private.mkdir(mode=0o700)
                    (private / "audit.anchor").write_bytes(b"independent-existing-anchor")
                elif case == "directory-symlink":
                    target.mkdir(mode=0o777)
                    target.chmod(0o777)
                    private.symlink_to(target, target_is_directory=True)
                elif case == "legacy-file":
                    legacy.write_bytes(b"independent-legacy-anchor")
                elif case == "legacy-dangling-symlink":
                    legacy.symlink_to(root / "missing-legacy-evidence")
                command = kube.PRIVATE_INIT_COMMAND.replace("/runtime/private", str(root / "runtime-private"))
                command = command.replace("/var/lib/multidb/audit.anchor", str(legacy))
                result = subprocess.run(["/bin/sh", "-ec", command], env={**os.environ, "AUDIT_ANCHOR_DIR": str(private)},
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=10)
                self.assertEqual(result.returncode == 0, case in ("fresh", "existing"))
                self.assertEqual(stat.S_IMODE(data.stat().st_mode), 0o777)
                if case in ("fresh", "existing"):
                    self.assertEqual(stat.S_IMODE(private.stat().st_mode), 0o700)
                if case == "existing":
                    self.assertEqual((private / "audit.anchor").read_bytes(), b"independent-existing-anchor")
                elif case == "directory-symlink":
                    self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o777)
                elif case == "legacy-file":
                    self.assertEqual(legacy.read_bytes(), b"independent-legacy-anchor")
                    self.assertFalse(private.exists())
                elif case == "legacy-dangling-symlink":
                    self.assertTrue(legacy.is_symlink())
                    self.assertFalse(private.exists())


class SemanticMutationTests(unittest.TestCase):
    def check_mutation_rejected(self, kind, mutate):
        objects = copy.deepcopy(BASE_OBJECTS)
        target = next(item for item in objects if item["kind"] == kind)
        mutate(target)
        with self.assertRaises(ValueError):
            kube.validate_objects(objects, BASE_VALUES)

    def test_baseline_has_no_deployment_or_image_verification_claim(self):
        result = kube.validate_objects(BASE_OBJECTS, BASE_VALUES)
        self.assertEqual(result["object_count"], 7)
        self.assertFalse(result["production_image_verified"])

    def test_schema_looking_text_is_not_an_api_object(self):
        with self.assertRaises(ValueError):
            kube.validate_objects(["Deployment replicas: 1 readOnlyRootFilesystem: true"], BASE_VALUES)

    def test_duplicate_objects_and_unknown_kinds_fail(self):
        with self.assertRaises(ValueError):
            kube.validate_objects(BASE_OBJECTS + [BASE_OBJECTS[0]], BASE_VALUES)
        with self.assertRaises(ValueError):
            kube.validate_objects(BASE_OBJECTS + [{"apiVersion": "v1", "kind": "Secret", "metadata": {"name": "unexpected"}}], BASE_VALUES)

    def test_single_writer_and_selector_mutations_fail(self):
        for kind, mutate in (
            ("Deployment", lambda obj: obj["spec"].update(replicas=2)),
            ("Deployment", lambda obj: obj["spec"].update(strategy={"type": "RollingUpdate"})),
            ("Deployment", lambda obj: obj["spec"]["selector"].update(matchLabels={})),
            ("Service", lambda obj: obj["spec"].update(selector={})),
            ("NetworkPolicy", lambda obj: obj["spec"].update(podSelector={})),
            ("NetworkPolicy", lambda obj: obj["spec"].update(egress=[{}])),
        ):
            with self.subTest(kind=kind):
                self.check_mutation_rejected(kind, mutate)

    def test_exact_pvc_and_secret_references_are_checked(self):
        self.check_mutation_rejected("Deployment", lambda obj: obj["spec"]["template"]["spec"]["volumes"][0]["persistentVolumeClaim"].update(claimName="wrong-volume"))
        def bad_secret(obj):
            auth = next(item for item in obj["spec"]["template"]["spec"]["volumes"] if item["name"] == "auth")
            auth["projected"]["sources"][0]["secret"]["items"][0]["key"] = "wrong-key"
        self.check_mutation_rejected("Deployment", bad_secret)
        self.check_mutation_rejected("PersistentVolumeClaim", lambda obj: obj["metadata"]["annotations"].update({"helm.sh/resource-policy": "delete"}))

    def test_readiness_and_resource_quantities_are_checked(self):
        self.check_mutation_rejected("Deployment", lambda obj: obj["spec"]["template"]["spec"]["containers"][0]["readinessProbe"]["httpGet"].update(path="/health"))
        self.check_mutation_rejected("Deployment", lambda obj: obj["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"].update(memory="500Mi"))
        equivalent = copy.deepcopy(BASE_OBJECTS)
        deployment = next(obj for obj in equivalent if obj["kind"] == "Deployment")
        deployment["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] = "4000m"
        kube.validate_objects(equivalent, BASE_VALUES)

    def test_privileged_and_service_account_mutations_fail(self):
        self.check_mutation_rejected("Deployment", lambda obj: obj["spec"]["template"]["spec"]["initContainers"][0]["securityContext"].update(privileged=True))
        self.check_mutation_rejected("ServiceAccount", lambda obj: obj.update(automountServiceAccountToken=True))

    def test_audit_key_permissions_and_durable_anchor_are_exact(self):
        def exposed_key(obj):
            volume = next(item for item in obj["spec"]["template"]["spec"]["volumes"] if item["name"] == "audit-key")
            volume["secret"]["defaultMode"] = 438
        def wrong_anchor(obj):
            env = obj["spec"]["template"]["spec"]["containers"][0]["env"]
            next(item for item in env if item["name"] == "MULTIDB_AUDIT_ANCHOR_PATH")["value"] = "/tmp/lost.anchor"
        self.check_mutation_rejected("Deployment", exposed_key)
        self.check_mutation_rejected("Deployment", wrong_anchor)

    def test_private_anchor_init_cannot_omit_mount_permissions_or_parent(self):
        def initializer(obj):
            return obj["spec"]["template"]["spec"]["initContainers"][0]
        self.check_mutation_rejected("Deployment", lambda obj: initializer(obj)["volumeMounts"].pop())
        self.check_mutation_rejected("Deployment", lambda obj: initializer(obj).update(command=["/bin/sh", "-ec", "mkdir -p /runtime/private"]))
        self.check_mutation_rejected("Deployment", lambda obj: initializer(obj)["env"][0].update(value="/var/lib/multidb"))


class EvidenceTests(unittest.TestCase):
    def test_schema_results_must_cover_every_exact_object_without_skips(self):
        report = {"summary": {"valid": len(BASE_OBJECTS), "invalid": 0, "errors": 0, "skipped": 0},
                  "resources": [{"version": obj["apiVersion"], "kind": obj["kind"], "name": obj["metadata"]["name"], "status": "statusValid"} for obj in BASE_OBJECTS]}
        kube.validate_schema_report(report, BASE_OBJECTS)
        for mutate in (
            lambda value: value["resources"].pop(),
            lambda value: value["resources"][0].update(status="statusSkipped"),
            lambda value: value["resources"][0].update(name="another-object"),
            lambda value: value["summary"].update(skipped=1),
            lambda value: value["resources"].append(value["resources"][0]),
        ):
            invalid = copy.deepcopy(report)
            mutate(invalid)
            with self.assertRaises(ValueError):
                kube.validate_schema_report(invalid, BASE_OBJECTS)

    def chart(self, directory):
        directory.mkdir()
        for filename, contents in (("Chart.yaml", "name: multidb\n"), ("values.schema.json", "{}"), ("values.yaml", "{}")):
            (directory / filename).write_text(contents)
        return directory

    def args(self, chart, output, plan=False):
        return types.SimpleNamespace(chart=chart, output=output, plan=plan, helm="/absent-helm-qualification", kubeconform="/absent-kubeconform-qualification", kubernetes_version="1.35.0")

    def test_plan_never_accepts_or_executes_commands(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "evidence"
            self.assertEqual(kube.run(self.args(self.chart(root / "chart"), output, plan=True)), 2)
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["status"], "blocked")
            self.assertEqual(report["sections"]["real_cluster"]["status"], "blocked")
            self.assertFalse(report["production_deployment_qualified"])
            self.assertFalse((output / "commands.json").exists())

    def test_missing_tool_is_blocked_with_nonzero_exit(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "evidence"
            self.assertEqual(kube.run(self.args(self.chart(root / "chart"), output)), 2)
            report = json.loads((output / "report.json").read_text())
            self.assertEqual(report["status"], "blocked")
            self.assertIn("required tool", report["sections"]["static_validation"]["reason"])

    def test_fresh_output_and_chart_hash_are_enforced(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            chart = self.chart(root / "chart")
            before = kube.chart_inventory(chart)
            (chart / "values.yaml").write_text("replicaCount: 2\n")
            self.assertNotEqual(before, kube.chart_inventory(chart))
            with self.assertRaises(FileExistsError):
                kube.run(self.args(chart, root))

BASE_OBJECTS = [{'apiVersion': 'networking.k8s.io/v1',
  'kind': 'NetworkPolicy',
  'metadata': {'name': 'qualification-multidb'},
  'spec': {'podSelector': {'matchLabels': {'app.kubernetes.io/name': 'multidb',
                                           'app.kubernetes.io/instance': 'qualification'}},
           'policyTypes': ['Ingress', 'Egress'],
           'ingress': [],
           'egress': []}},
 {'apiVersion': 'policy/v1',
  'kind': 'PodDisruptionBudget',
  'metadata': {'name': 'qualification-multidb'},
  'spec': {'minAvailable': 1,
           'unhealthyPodEvictionPolicy': 'AlwaysAllow',
           'selector': {'matchLabels': {'app.kubernetes.io/name': 'multidb',
                                        'app.kubernetes.io/instance': 'qualification'}}}},
 {'apiVersion': 'v1',
  'kind': 'ServiceAccount',
  'metadata': {'name': 'qualification-multidb'},
  'automountServiceAccountToken': False},
 {'apiVersion': 'v1',
  'kind': 'ConfigMap',
  'metadata': {'name': 'qualification-multidb-config',
               'labels': {'app.kubernetes.io/name': 'multidb', 'app.kubernetes.io/instance': 'qualification'}},
  'data': {'MULTIDB_RUNTIME_MODE': 'production',
           'MULTIDB_DB_PATH': '/var/lib/multidb/multidb.redb',
           'MULTIDB_BIND': '0.0.0.0:8080',
           'MULTIDB_PG_BIND': '0.0.0.0:5432',
           'MULTIDB_STUDIO_DIR': '/usr/share/multidb/studio',
           'MULTIDB_PG_USER': 'multidb',
           'MULTIDB_MAX_CONCURRENT_QUERIES': '2',
           'MULTIDB_QUERY_QUEUE_TIMEOUT_MS': '5000',
           'MULTIDB_PROFILE': 'transactional',
           'MULTIDB_AUDIT_JSONL_PATH': '/var/lib/multidb/audit/audit.jsonl',
           'MULTIDB_BACKUP_TARGET': 's3://multidb-local/backups',
           'MULTIDB_MINIO_ENDPOINT': 'http://minio:9000',
           'MULTIDB_VAULT_ADDR': 'http://vault:8200',
           'MULTIDB_VAULT_KEK_PATH': 'secret/data/multidb/kek'}},
 {'apiVersion': 'v1',
  'kind': 'PersistentVolumeClaim',
  'metadata': {'name': 'qualification-multidb-data',
               'annotations': {'helm.sh/resource-policy': 'keep'},
               'labels': {'app.kubernetes.io/name': 'multidb', 'app.kubernetes.io/instance': 'qualification'}},
  'spec': {'accessModes': ['ReadWriteOnce'], 'resources': {'requests': {'storage': '10Gi'}}}},
 {'apiVersion': 'v1',
  'kind': 'Service',
  'metadata': {'name': 'qualification-multidb',
               'labels': {'app.kubernetes.io/name': 'multidb', 'app.kubernetes.io/instance': 'qualification'}},
  'spec': {'type': 'ClusterIP',
           'selector': {'app.kubernetes.io/name': 'multidb', 'app.kubernetes.io/instance': 'qualification'},
           'ports': [{'name': 'pgwire', 'port': 5432, 'targetPort': 'pgwire'},
                     {'name': 'admin', 'port': 8080, 'targetPort': 'admin'}]}},
 {'apiVersion': 'apps/v1',
  'kind': 'Deployment',
  'metadata': {'name': 'qualification-multidb',
               'labels': {'app.kubernetes.io/name': 'multidb', 'app.kubernetes.io/instance': 'qualification'}},
  'spec': {'replicas': 1,
           'revisionHistoryLimit': 3,
           'selector': {'matchLabels': {'app.kubernetes.io/name': 'multidb',
                                        'app.kubernetes.io/instance': 'qualification'}},
           'strategy': {'type': 'Recreate'},
           'template': {'metadata': {'labels': {'app.kubernetes.io/name': 'multidb',
                                                'app.kubernetes.io/instance': 'qualification'},
                                     'annotations': {'multidb.io/upgrade-policy': 'stop-single-writer-before-replacement',
                                                     'multidb.io/downgrade-policy': 'reject-unless-explicit-rollback',
                                                     'multidb.io/storage-durability': 'persistent-volume',
                                                     'checksum/config': '1d6e781a19a9fa910710cd820be6bc1a73c09d7ea188433d19336645a1e4f123'}},
                        'spec': {'serviceAccountName': 'qualification-multidb',
                                 'automountServiceAccountToken': False,
                                 'terminationGracePeriodSeconds': 60,
                                 'securityContext': {'runAsNonRoot': True,
                                                     'runAsUser': 10001,
                                                     'runAsGroup': 10001,
                                                     'fsGroup': 10001,
                                                     'fsGroupChangePolicy': 'OnRootMismatch',
                                                     'seccompProfile': {'type': 'RuntimeDefault'}},
                                 'initContainers': [{'name': 'init-runtime',
                                                     'image': 'ghcr.io/piotrwozn/multidb@sha256:1111111111111111111111111111111111111111111111111111111111111111',
                                                     'imagePullPolicy': 'IfNotPresent',
                                                     'command': ['/bin/sh',
                                                                 '-ec',
                                                                 'umask 077; mkdir -p /runtime/private; chmod 0700 '
                                                                 '/runtime/private; if [ -n "${AUDIT_ANCHOR_DIR:-}" ]; '
                                                                 'then if [ -e /var/lib/multidb/audit.anchor ] || [ -L '
                                                                 '/var/lib/multidb/audit.anchor ]; then echo "preserve '
                                                                 'and migrate the existing flat audit.anchor before '
                                                                 'starting this chart" >&2; exit 1; fi; test ! -L '
                                                                 '"$AUDIT_ANCHOR_DIR"; mkdir -p "$AUDIT_ANCHOR_DIR"; '
                                                                 'test ! -L "$AUDIT_ANCHOR_DIR"; chmod 0700 '
                                                                 '"$AUDIT_ANCHOR_DIR"; fi'],
                                                     'env': [{'name': 'AUDIT_ANCHOR_DIR',
                                                              'value': '/var/lib/multidb/audit-private'}],
                                                     'securityContext': {'allowPrivilegeEscalation': False,
                                                                         'readOnlyRootFilesystem': True,
                                                                         'capabilities': {'drop': ['ALL']}},
                                                     'resources': {'requests': {'cpu': '10m', 'memory': '16Mi'},
                                                                   'limits': {'cpu': '100m', 'memory': '32Mi'}},
                                                     'volumeMounts': [{'name': 'runtime', 'mountPath': '/runtime'},
                                                                      {'name': 'data',
                                                                       'mountPath': '/var/lib/multidb'}]}],
                                 'containers': [{'name': 'multidb',
                                                 'image': 'ghcr.io/piotrwozn/multidb@sha256:1111111111111111111111111111111111111111111111111111111111111111',
                                                 'imagePullPolicy': 'IfNotPresent',
                                                 'securityContext': {'allowPrivilegeEscalation': False,
                                                                     'readOnlyRootFilesystem': True,
                                                                     'capabilities': {'drop': ['ALL']}},
                                                 'envFrom': [{'configMapRef': {'name': 'qualification-multidb-config'}}],
                                                 'env': [{'name': 'MULTIDB_ADMIN_PASSWORD_FILE',
                                                          'value': '/etc/multidb/auth/admin-password'},
                                                         {'name': 'MULTIDB_ADMIN_TOKEN_FILE',
                                                          'value': '/etc/multidb/auth/admin-token'},
                                                         {'name': 'MULTIDB_PG_PASSWORD_FILE',
                                                          'value': '/etc/multidb/auth/pg-password'},
                                                         {'name': 'MULTIDB_AUDIT_KEY_PATH',
                                                          'value': '/etc/multidb/audit-key/key.bin'},
                                                         {'name': 'MULTIDB_AUDIT_ANCHOR_PATH',
                                                          'value': '/var/lib/multidb/audit-private/audit.anchor'},
                                                         {'name': 'MULTIDB_PG_TLS_CERT',
                                                          'value': '/etc/multidb/pg-tls/tls.crt'},
                                                         {'name': 'MULTIDB_PG_TLS_KEY',
                                                          'value': '/etc/multidb/pg-tls/tls.key'},
                                                         {'name': 'XDG_RUNTIME_DIR',
                                                          'value': '/tmp/multidb-runtime/private'}],
                                                 'ports': [{'name': 'pgwire', 'containerPort': 5432},
                                                           {'name': 'admin', 'containerPort': 8080}],
                                                 'startupProbe': {'httpGet': {'path': '/health', 'port': 'admin'},
                                                                  'periodSeconds': 5,
                                                                  'failureThreshold': 60,
                                                                  'timeoutSeconds': 2},
                                                 'readinessProbe': {'httpGet': {'path': '/ready', 'port': 'admin'},
                                                                    'initialDelaySeconds': 5,
                                                                    'periodSeconds': 10,
                                                                    'failureThreshold': 3,
                                                                    'timeoutSeconds': 2},
                                                 'livenessProbe': {'httpGet': {'path': '/health', 'port': 'admin'},
                                                                   'periodSeconds': 10,
                                                                   'failureThreshold': 6,
                                                                   'timeoutSeconds': 2},
                                                 'lifecycle': {'preStop': {'exec': {'command': ['/bin/sh',
                                                                                                '-c',
                                                                                                'sleep 5']}}},
                                                 'volumeMounts': [{'name': 'data', 'mountPath': '/var/lib/multidb'},
                                                                  {'name': 'tmp', 'mountPath': '/tmp'},
                                                                  {'name': 'runtime',
                                                                   'mountPath': '/tmp/multidb-runtime'},
                                                                  {'name': 'auth',
                                                                   'mountPath': '/etc/multidb/auth',
                                                                   'readOnly': True},
                                                                  {'name': 'audit-key',
                                                                   'mountPath': '/etc/multidb/audit-key',
                                                                   'readOnly': True},
                                                                  {'name': 'pg-tls',
                                                                   'mountPath': '/etc/multidb/pg-tls',
                                                                   'readOnly': True}],
                                                 'resources': {'limits': {'cpu': '4', 'memory': '4Gi'},
                                                               'requests': {'cpu': '1', 'memory': '1Gi'}}}],
                                 'volumes': [{'name': 'data',
                                              'persistentVolumeClaim': {'claimName': 'qualification-multidb-data'}},
                                             {'name': 'tmp', 'emptyDir': {'sizeLimit': '256Mi'}},
                                             {'name': 'runtime', 'emptyDir': {'medium': 'Memory', 'sizeLimit': '16Mi'}},
                                             {'name': 'auth',
                                              'projected': {'defaultMode': 288,
                                                            'sources': [{'secret': {'name': 'multidb-admin',
                                                                                    'items': [{'key': 'admin-password',
                                                                                               'path': 'admin-password'}]}},
                                                                        {'secret': {'name': 'multidb-admin',
                                                                                    'items': [{'key': 'admin-token',
                                                                                               'path': 'admin-token'}]}},
                                                                        {'secret': {'name': 'multidb-pg',
                                                                                    'items': [{'key': 'password',
                                                                                               'path': 'pg-password'}]}}]}},
                                             {'name': 'audit-key',
                                              'secret': {'secretName': 'multidb-audit',
                                                         'defaultMode': 288,
                                                         'items': [{'key': 'audit-key', 'path': 'key.bin'}]}},
                                             {'name': 'pg-tls',
                                              'secret': {'secretName': 'multidb-pg-tls',
                                                         'defaultMode': 288,
                                                         'items': [{'key': 'tls.crt', 'path': 'tls.crt'},
                                                                   {'key': 'tls.key', 'path': 'tls.key'}]}}]}}}}]

BASE_VALUES = {'replicaCount': 1,
 'nameOverride': '',
 'fullnameOverride': '',
 'image': {'repository': 'ghcr.io/piotrwozn/multidb',
           'tag': '1.0.0-rc.1',
           'digest': 'sha256:1111111111111111111111111111111111111111111111111111111111111111',
           'pullPolicy': 'IfNotPresent'},
 'service': {'type': 'ClusterIP', 'pgwirePort': 5432, 'adminPort': 8080},
 'runtime': {'mode': 'production',
             'dbPath': '/var/lib/multidb/multidb.redb',
             'studioDir': '/usr/share/multidb/studio',
             'adminBind': '0.0.0.0:8080',
             'pgBind': '0.0.0.0:5432',
             'pgUser': 'multidb',
             'terminationGracePeriodSeconds': 60,
             'preStopDelaySeconds': 5,
             'allowVolatile': False},
 'runtimeClassName': '',
 'supplementalGroups': [],
 'gpu': {'enabled': False, 'vendor': '', 'resourceName': ''},
 'encryption': {'enabled': False,
                'secretName': 'multidb-encryption',
                'key': 'key',
                'mountPath': '/etc/multidb/encryption'},
 'audit': {'enabled': True,
           'secretName': 'multidb-audit',
           'key': 'audit-key',
           'mountPath': '/etc/multidb/audit-key',
           'anchorPath': '/var/lib/multidb/audit-private/audit.anchor'},
 'secrets': {'adminPassword': {'name': 'multidb-admin', 'key': 'admin-password'},
             'adminToken': {'name': 'multidb-admin', 'key': 'admin-token'},
             'pgPassword': {'name': 'multidb-pg', 'key': 'password'},
             'pgTls': {'name': 'multidb-pg-tls',
                       'certKey': 'tls.crt',
                       'privateKey': 'tls.key',
                       'mountPath': '/etc/multidb/pg-tls'}},
 'persistence': {'enabled': True,
                 'devEmptyDir': False,
                 'existingClaim': '',
                 'storageClassName': '',
                 'accessModes': ['ReadWriteOnce'],
                 'size': '10Gi',
                 'retainOnDelete': True},
 'config': {'profile': 'transactional',
            'replication': '',
            'auditJsonlPath': '/var/lib/multidb/audit/audit.jsonl',
            'backupTarget': 's3://multidb-local/backups',
            'minioEndpoint': 'http://minio:9000',
            'vaultAddress': 'http://vault:8200',
            'vaultKekPath': 'secret/data/multidb/kek',
            'maxConcurrentQueries': 2,
            'queryQueueTimeoutMs': 5000,
            'historyRetainLsns': ''},
 'spec': {'enabled': False,
          'configMap': 'multidb-spec',
          'key': 'multidb.yaml',
          'mountPath': '/etc/multidb/spec',
          'apply': 'verify'},
 'resources': {'requests': {'cpu': '1', 'memory': '1Gi'}, 'limits': {'cpu': '4', 'memory': '4Gi'}},
 'probes': {'readinessPath': '/ready',
            'livenessPath': '/health',
            'initialDelaySeconds': 5,
            'periodSeconds': 10,
            'timeoutSeconds': 2,
            'startupPeriodSeconds': 5,
            'startupFailureThreshold': 60,
            'readinessFailureThreshold': 3,
            'livenessFailureThreshold': 6},
 'pdb': {'enabled': True},
 'networkPolicy': {'enabled': True, 'clientPeers': [], 'proxyPeers': [], 'metricsPeers': [], 'egress': []},
 'ingress': {'enabled': False,
             'className': '',
             'annotations': {},
             'hosts': [],
             'tls': [],
             'principalHeaderTrustAcknowledged': False},
 'monitoring': {'enabled': False,
                'serviceMonitor': {'enabled': False, 'interval': '30s', 'scrapeTimeout': '10s', 'labels': {}}},
 'podAnnotations': {},
 'nodeSelector': {},
 'affinity': {},
 'tolerations': [],
 'topologySpreadConstraints': [],
 'imagePullSecrets': []}
