#!/usr/bin/env python3
"""Validate Helm output and Kubernetes schemas; never qualify a live deployment.

The deterministic test image digest is used only to exercise manifest rendering.
It identifies no verified release image. A live cluster/CNI/storage and hardware
qualification is a separate, unrun gate in every report from this program.
"""
from __future__ import annotations

import argparse
import copy
import datetime
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time

try:
    import yaml
except ImportError:
    yaml = None

PY_YAML_VERSION = "6.0.3"
TEST_DIGEST = "sha256:" + "1" * 64
CPU_PROFILES = ("Transactional", "Analytical", "Document", "Vector", "TimeSeries", "HighDurability", "Balanced")
CPU_PRODUCT_PROFILES = ("game_local_balanced", "desktop_app_embedded", "ai_agent_memory", "secure_app", "production_cp", "analytics_columnar")
GPU_RESOURCES = {"nvidia": "nvidia.com/gpu", "amd": "amd.com/gpu", "intel": "gpu.intel.com/i915"}
RELEASE = "qualification"
NAMESPACE = "multidb-qualification"
API_VERSIONS = {"Deployment": "apps/v1", "Service": "v1", "ConfigMap": "v1",
                "PersistentVolumeClaim": "v1", "ServiceAccount": "v1",
                "NetworkPolicy": "networking.k8s.io/v1", "Ingress": "networking.k8s.io/v1",
                "PodDisruptionBudget": "policy/v1"}
PRIVATE_INIT_COMMAND = ('umask 077; mkdir -p /runtime/private; chmod 0700 /runtime/private; '
                        'if [ -n "${AUDIT_ANCHOR_DIR:-}" ]; then '
                        'if [ -e /var/lib/multidb/audit.anchor ] || [ -L /var/lib/multidb/audit.anchor ]; then '
                        'echo "preserve and migrate the existing flat audit.anchor before starting this chart" >&2; exit 1; fi; '
                        'test ! -L "$AUDIT_ANCHOR_DIR"; mkdir -p "$AUDIT_ANCHOR_DIR"; '
                        'test ! -L "$AUDIT_ANCHOR_DIR"; chmod 0700 "$AUDIT_ANCHOR_DIR"; fi')


def utc_now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def digest(path):
    value = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            value.update(block)
    return value.hexdigest()


def write_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def strict_yaml(source):
    if yaml is None or yaml.__version__ != PY_YAML_VERSION:
        raise RuntimeError(f"install requirements-kubernetes.txt (PyYAML=={PY_YAML_VERSION})")

    class StrictLoader(yaml.SafeLoader):
        def compose_node(self, parent, index):
            if self.check_event(yaml.AliasEvent):
                raise ValueError("YAML aliases are not accepted in qualification artifacts")
            return super().compose_node(parent, index)

        def construct_mapping(self, node, deep=False):
            result = {}
            for key_node, value_node in node.value:
                key = self.construct_object(key_node, deep=deep)
                if not isinstance(key, str):
                    raise ValueError("YAML mappings require string keys")
                if key in result:
                    raise ValueError(f"duplicate YAML key: {key}")
                result[key] = self.construct_object(value_node, deep=deep)
            return result

    try:
        documents = list(yaml.load_all(source, Loader=StrictLoader))
    except yaml.YAMLError as error:
        raise ValueError(f"invalid strict YAML: {error}") from error
    return [document for document in documents if document is not None]


def merge_values(base, override):
    result = copy.deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = merge_values(result[key], value)
        else:
            result[key] = copy.deepcopy(value)
    return result


def quantity(value):
    """Compare Kubernetes quantities numerically, including fractional CPU and SI."""
    match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+))([eE][+-]?\d+|[numkKMGTPE]i?)?", str(value))
    if not match:
        raise ValueError(f"invalid resource quantity: {value!r}")
    number, suffix = Decimal(match[1]), match[2] or ""
    if suffix.startswith(("e", "E")) and len(suffix) > 1 and suffix[1] in "+-0123456789":
        return number * Decimal(10) ** int(suffix[1:])
    decimal = {"": 0, "n": -9, "u": -6, "m": -3, "k": 3, "K": 3, "M": 6,
               "G": 9, "T": 12, "P": 15, "E": 18}
    if suffix.endswith("i"):
        return number * Decimal(1024) ** ("KMGTPE".index(suffix[0]) + 1)
    return number * Decimal(10) ** decimal[suffix]


def require(condition, message):
    if not condition:
        raise ValueError(message)


def keyed(items, field="name"):
    require(isinstance(items, list), f"expected object list keyed by {field}")
    result = {}
    for item in items:
        require(isinstance(item, dict) and field in item and item[field] not in result,
                f"missing or duplicate {field} in object list")
        result[item[field]] = item
    return result


def validate_objects(documents, values):
    """Independent assertions over parsed API objects, not rendered text matches."""
    objects = {}
    identities = set()
    for document in documents:
        require(isinstance(document, dict), "rendered document is not an API object")
        kind, version = document.get("kind"), document.get("apiVersion")
        require(kind in API_VERSIONS and version == API_VERSIONS[kind], "unexpected kind or API version")
        metadata = document.get("metadata", {})
        identity = (version, kind, metadata.get("namespace", NAMESPACE), metadata.get("name"))
        require(identity not in identities and identity[-1], "duplicate or unnamed API object")
        identities.add(identity)
        require(kind not in objects, f"unexpected extra {kind} object")
        objects[kind] = document
    expected = {"Deployment", "Service", "ServiceAccount", "ConfigMap"}
    persistent = values["persistence"]
    production = values["runtime"]["mode"] == "production"
    volatile = values["runtime"].get("allowVolatile", False)
    gpu = values.get("gpu", {"enabled": False, "vendor": ""})
    encryption = values.get("encryption", {"enabled": False})
    audit = values["audit"]
    require(not production or audit["enabled"], "production requires a private audit key")
    if persistent["enabled"] and not persistent["devEmptyDir"] and not persistent["existingClaim"]:
        expected.add("PersistentVolumeClaim")
    if values["networkPolicy"]["enabled"]:
        expected.add("NetworkPolicy")
    if values["pdb"]["enabled"]:
        expected.add("PodDisruptionBudget")
    if values["ingress"]["enabled"]:
        expected.add("Ingress")
    require(set(objects) == expected, f"rendered kind selection differs: {set(objects)} versus {expected}")

    deployment = objects["Deployment"]
    name = deployment["metadata"]["name"]
    labels = {"app.kubernetes.io/name": values.get("nameOverride") or "multidb",
              "app.kubernetes.io/instance": RELEASE}
    spec = deployment["spec"]
    require(spec["replicas"] == 1 and spec["strategy"] == {"type": "Recreate"}, "single-writer Recreate contract changed")
    require(spec["selector"] == {"matchLabels": labels} and spec["template"]["metadata"]["labels"] == labels,
            "deployment/pod selectors differ")
    pod = spec["template"]["spec"]
    annotations = spec["template"]["metadata"].get("annotations", {})
    durability = "volatile-data-lost-on-restart" if volatile else ("ephemeral-emptydir-data-lost-on-pod-replacement" if persistent["devEmptyDir"] else "persistent-volume")
    require(annotations.get("multidb.io/storage-durability") == durability,
            "pod storage-durability declaration differs")
    require(annotations.get("multidb.io/upgrade-policy") == "stop-single-writer-before-replacement"
            and annotations.get("multidb.io/downgrade-policy") == "reject-unless-explicit-rollback", "upgrade/downgrade policy differs")
    require(pod.get("automountServiceAccountToken") is False and pod.get("serviceAccountName") == name,
            "pod token or dedicated service account changed")
    require(not any(pod.get(key) for key in ("hostNetwork", "hostPID", "hostIPC", "nodeName")), "host isolation changed")
    security_context = {"runAsNonRoot": True, "runAsUser": 10001, "runAsGroup": 10001,
                        "fsGroup": 10001, "fsGroupChangePolicy": "OnRootMismatch", "seccompProfile": {"type": "RuntimeDefault"}}
    if values.get("supplementalGroups"):
        security_context["supplementalGroups"] = values["supplementalGroups"]
    require(pod["securityContext"] == security_context,
            "non-root/fsGroup/seccomp contract changed")
    require(pod.get("runtimeClassName", "") == values.get("runtimeClassName", ""), "GPU runtime class selection differs")
    require(objects["ServiceAccount"]["metadata"]["name"] == name
            and objects["ServiceAccount"].get("automountServiceAccountToken") is False, "service-account token contract changed")
    containers, init = keyed(pod["containers"]), keyed(pod["initContainers"])
    require(set(containers) == {"multidb"} and set(init) == {"init-runtime"}, "unexpected application/init container")
    container, initializer = containers["multidb"], init["init-runtime"]
    require("command" not in container and "args" not in container, "application entrypoint was replaced")
    image = values["image"]
    reference = image["repository"] + ("@" + image["digest"] if image["digest"] else ":" + image["tag"])
    require(not production or re.fullmatch(r"sha256:[a-f0-9]{64}", image["digest"]), "production image is not digest-pinned")
    hardening = {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True, "capabilities": {"drop": ["ALL"]}}
    for item in (container, initializer):
        require(item["image"] == reference and item["imagePullPolicy"] == image["pullPolicy"], "image reference/policy differs")
        require(item["securityContext"] == hardening, "container hardening changed")
    require(initializer["command"] == ["/bin/sh", "-ec", PRIVATE_INIT_COMMAND],
            "private runtime directory initialization changed")
    private_anchor = audit["enabled"] and not volatile
    init_mounts = [{"name": "runtime", "mountPath": "/runtime"}]
    if private_anchor:
        init_mounts.append({"name": "data", "mountPath": "/var/lib/multidb"})
    for path in (values["secrets"]["pgTls"]["mountPath"], values["spec"]["mountPath"], encryption["mountPath"], audit["mountPath"]):
        require("//" not in path and not any(part in (".", "..") for part in path.split("/")), "credential/spec/key mount path is not canonical")
        mount = Path(path)
        data = Path("/var/lib/multidb")
        require(not (mount.is_relative_to(data) or data.is_relative_to(mount)), "credential/spec/key mount overlaps the writable data volume")
    require(initializer["volumeMounts"] == init_mounts, "init private runtime/audit volume mount differs")
    init_env = [{"name": "AUDIT_ANCHOR_DIR", "value": str(Path(audit["anchorPath"]).parent)}] if private_anchor else []
    require(initializer.get("env", []) == init_env, "private persistent audit directory initialization differs")
    require(container["envFrom"] == [{"configMapRef": {"name": name + "-config"}}], "configmap source differs")
    secrets, runtime, probes = values["secrets"], values["runtime"], values["probes"]
    env = {"MULTIDB_ADMIN_PASSWORD_FILE": "/etc/multidb/auth/admin-password",
           "MULTIDB_PG_PASSWORD_FILE": "/etc/multidb/auth/pg-password", "XDG_RUNTIME_DIR": "/tmp/multidb-runtime/private"}
    if secrets["adminToken"]["name"]:
        env["MULTIDB_ADMIN_TOKEN_FILE"] = "/etc/multidb/auth/admin-token"
    if encryption["enabled"]:
        env["MULTIDB_ENCRYPTION_KEY_PATH"] = encryption["mountPath"] + "/key.bin"
    if audit["enabled"]:
        env["MULTIDB_AUDIT_KEY_PATH"] = audit["mountPath"] + "/key.bin"
        if not volatile:
            require(re.fullmatch(r"/var/lib/multidb/[A-Za-z0-9._-]+/[A-Za-z0-9._-]+", audit["anchorPath"])
                    and not any(part in (".", "..") for part in Path(audit["anchorPath"]).parts)
                    and not Path(audit["anchorPath"]).is_relative_to(Path(runtime["dbPath"]))
                    and not Path(runtime["dbPath"]).is_relative_to(Path(audit["anchorPath"])), "audit anchor must be a separate file on the data volume")
            env["MULTIDB_AUDIT_ANCHOR_PATH"] = audit["anchorPath"]
    if gpu["enabled"] and gpu["vendor"] == "nvidia":
        env["NVIDIA_DRIVER_CAPABILITIES"] = "compute,utility,graphics"
    if production:
        env.update(MULTIDB_PG_TLS_CERT=secrets["pgTls"]["mountPath"] + "/tls.crt",
                   MULTIDB_PG_TLS_KEY=secrets["pgTls"]["mountPath"] + "/tls.key")
    else:
        env["MULTIDB_PG_TLS_MODE"] = "disabled"
    require(container["env"] == [{"name": key, "value": value} for key, value in env.items()]
            or keyed(container["env"]) == {key: {"name": key, "value": value} for key, value in env.items()},
            "secret-file/TLS/runtime environment differs")
    require(container["ports"] == [{"name": "pgwire", "containerPort": values["service"]["pgwirePort"]},
            {"name": "admin", "containerPort": values["service"]["adminPort"]}], "container port selection differs")
    expected_probes = {
        "startupProbe": {"httpGet": {"path": "/health", "port": "admin"}, "periodSeconds": probes["startupPeriodSeconds"],
                         "failureThreshold": probes["startupFailureThreshold"], "timeoutSeconds": probes["timeoutSeconds"]},
        "readinessProbe": {"httpGet": {"path": "/ready", "port": "admin"}, "initialDelaySeconds": probes["initialDelaySeconds"],
                           "periodSeconds": probes["periodSeconds"], "failureThreshold": probes["readinessFailureThreshold"],
                           "timeoutSeconds": probes["timeoutSeconds"]},
        "livenessProbe": {"httpGet": {"path": "/health", "port": "admin"}, "periodSeconds": probes["periodSeconds"],
                          "failureThreshold": probes["livenessFailureThreshold"], "timeoutSeconds": probes["timeoutSeconds"]},
    }
    for probe, expected_probe in expected_probes.items():
        require(container[probe] == expected_probe, f"{probe} semantics differ")
    require(pod["terminationGracePeriodSeconds"] == runtime["terminationGracePeriodSeconds"]
            and runtime["terminationGracePeriodSeconds"] > runtime["preStopDelaySeconds"] + 20,
            "graceful termination budget differs")
    require(container["lifecycle"] == {"preStop": {"exec": {"command": ["/bin/sh", "-c", f"sleep {runtime['preStopDelaySeconds']}"]}}},
            "preStop drain delay differs")
    actual_resources = container["resources"]
    require(set(actual_resources) == {"requests", "limits"}, "resource request/limit selection differs")
    for category in ("requests", "limits"):
        require(set(actual_resources[category]) == set(values["resources"][category]), "resource names differ")
        for resource, expected_value in values["resources"][category].items():
            require(quantity(actual_resources[category][resource]) == quantity(expected_value) > 0,
                    f"{category}.{resource} quantity differs")
    for resource, requested in actual_resources["requests"].items():
        require(resource in actual_resources["limits"] and quantity(requested) <= quantity(actual_resources["limits"][resource]),
                f"resource request exceeds limit: {resource}")
    profile = config_profile = values["config"]["profile"].lower().replace("-", "_")
    tensor_profile = profile in ("tensor", "tensor_dataset", "deeplake_like")
    if not values["spec"]["enabled"]:
        require(not tensor_profile or gpu["enabled"], "Tensor profile must require explicit physical GPU resources")
    if gpu["enabled"]:
        require(gpu["vendor"] in GPU_RESOURCES, "unknown physical GPU vendor")
        resource = gpu.get("resourceName") or GPU_RESOURCES.get(gpu["vendor"])
        require(resource and quantity(actual_resources["limits"].get(resource, 0)) >= 1
                and quantity(actual_resources["requests"].get(resource, 0)) == quantity(actual_resources["limits"].get(resource, 0)),
                "physical GPU vendor request/limit selection differs")
        require(not any(key in env for key in ("MULTIDB_ALLOW_CPU_FALLBACK", "MULTIDB_GPU_CPU_FALLBACK")), "GPU deployment declares a CPU fallback")

    volumes = keyed(pod["volumes"])
    mounts = keyed(container["volumeMounts"])
    expected_volumes = {"data", "tmp", "runtime", "auth"} | ({"pg-tls"} if production else set()) | ({"spec"} if values["spec"]["enabled"] else set())
    if encryption["enabled"]:
        expected_volumes.add("encryption")
    if audit["enabled"]:
        expected_volumes.add("audit-key")
    require(set(volumes) == expected_volumes and set(mounts) == expected_volumes, "volume/mount selection differs")
    require(mounts["data"] == {"name": "data", "mountPath": "/var/lib/multidb"}, "data mount differs")
    require(volumes["tmp"] == {"name": "tmp", "emptyDir": {"sizeLimit": "256Mi"}}
            and volumes["runtime"] == {"name": "runtime", "emptyDir": {"medium": "Memory", "sizeLimit": "16Mi"}}, "scratch volumes differ")
    require(mounts["tmp"] == {"name": "tmp", "mountPath": "/tmp"}
            and mounts["runtime"] == {"name": "runtime", "mountPath": "/tmp/multidb-runtime"}, "runtime scratch mounts differ")
    if persistent["devEmptyDir"]:
        memory_profile = config_profile in ("inmemory", "in_memory", "memory")
        require((not production or (volatile and memory_profile)) and volumes["data"] == {"name": "data", "emptyDir": {}}, "ephemeral production data requires explicit volatile InMemory")
        require(not volatile or (memory_profile and not persistent["enabled"] and not values["spec"]["enabled"] and not encryption["enabled"]),
                "volatile mode claims unsupported durability/encryption/spec semantics")
    else:
        claim = persistent["existingClaim"] or name + "-data"
        require(volumes["data"] == {"name": "data", "persistentVolumeClaim": {"claimName": claim}}, "exact data PVC selection differs")
        if "PersistentVolumeClaim" in objects:
            pvc = objects["PersistentVolumeClaim"]
            require(pvc["metadata"]["name"] == claim and pvc["spec"]["accessModes"] == persistent["accessModes"], "PVC identity/access modes differ")
            require(quantity(pvc["spec"]["resources"]["requests"]["storage"]) == quantity(persistent["size"]), "PVC capacity differs")
            require(pvc["spec"].get("storageClassName", "") == persistent["storageClassName"], "PVC storage class differs")
            require(pvc["metadata"].get("annotations", {}).get("helm.sh/resource-policy") == ("keep" if persistent["retainOnDelete"] else None), "PVC deletion retention differs")
    auth_sources = []
    for source, filename in (("adminPassword", "admin-password"), ("adminToken", "admin-token"), ("pgPassword", "pg-password")):
        if secrets[source]["name"]:
            auth_sources.append({"secret": {"name": secrets[source]["name"], "items": [{"key": secrets[source]["key"], "path": filename}]}})
    require(volumes["auth"] == {"name": "auth", "projected": {"defaultMode": 288, "sources": auth_sources}}, "exact auth secret projection differs")
    require(mounts["auth"] == {"name": "auth", "mountPath": "/etc/multidb/auth", "readOnly": True}, "auth mount must be read-only")
    if production:
        require(volumes["pg-tls"] == {"name": "pg-tls", "secret": {"secretName": secrets["pgTls"]["name"], "defaultMode": 288,
                "items": [{"key": secrets["pgTls"]["certKey"], "path": "tls.crt"}, {"key": secrets["pgTls"]["privateKey"], "path": "tls.key"}]}}, "exact TLS secret projection differs")
        require(mounts["pg-tls"] == {"name": "pg-tls", "mountPath": secrets["pgTls"]["mountPath"], "readOnly": True}, "TLS mount differs")
    if encryption["enabled"]:
        require(volumes["encryption"] == {"name": "encryption", "secret": {"secretName": encryption["secretName"], "defaultMode": 288,
                "items": [{"key": encryption["key"], "path": "key.bin"}]}}, "encryption key secret/permissions differ")
        require(mounts["encryption"] == {"name": "encryption", "mountPath": encryption["mountPath"], "readOnly": True}, "encryption key mount differs")
    if audit["enabled"]:
        require(volumes["audit-key"] == {"name": "audit-key", "secret": {"secretName": audit["secretName"], "defaultMode": 288,
                "items": [{"key": audit["key"], "path": "key.bin"}]}}, "audit key secret/permissions differ")
        require(mounts["audit-key"] == {"name": "audit-key", "mountPath": audit["mountPath"], "readOnly": True}, "audit key mount differs")
    if values["spec"]["enabled"]:
        selected = values["spec"]
        require(volumes["spec"] == {"name": "spec", "configMap": {"name": selected["configMap"], "defaultMode": 288,
                "items": [{"key": selected["key"], "path": selected["key"]}]}}, "spec configmap/key selection differs")
        require(mounts["spec"] == {"name": "spec", "mountPath": selected["mountPath"], "readOnly": True}, "spec mount differs")

    config = values["config"]
    expected_config = {"MULTIDB_RUNTIME_MODE": runtime["mode"], "MULTIDB_DB_PATH": runtime["dbPath"],
        "MULTIDB_BIND": runtime["adminBind"], "MULTIDB_PG_BIND": runtime["pgBind"], "MULTIDB_STUDIO_DIR": runtime["studioDir"],
        "MULTIDB_PG_USER": runtime["pgUser"], "MULTIDB_MAX_CONCURRENT_QUERIES": str(config["maxConcurrentQueries"]),
        "MULTIDB_QUERY_QUEUE_TIMEOUT_MS": str(config["queryQueueTimeoutMs"]), "MULTIDB_AUDIT_JSONL_PATH": config["auditJsonlPath"],
        "MULTIDB_BACKUP_TARGET": config["backupTarget"], "MULTIDB_MINIO_ENDPOINT": config["minioEndpoint"],
        "MULTIDB_VAULT_ADDR": config["vaultAddress"], "MULTIDB_VAULT_KEK_PATH": config["vaultKekPath"]}
    if config["historyRetainLsns"]:
        expected_config["MULTIDB_HISTORY_RETAIN_LSNS"] = str(config["historyRetainLsns"])
    if values["spec"]["enabled"]:
        expected_config.update(MULTIDB_PROFILE="", MULTIDB_REPLICATION="", MULTIDB_SPEC_APPLY=values["spec"]["apply"],
                               MULTIDB_SPEC_PATH=values["spec"]["mountPath"] + "/" + values["spec"]["key"])
    else:
        expected_config["MULTIDB_PROFILE"] = config["profile"]
    require(objects["ConfigMap"]["metadata"]["name"] == name + "-config" and objects["ConfigMap"]["data"] == expected_config,
            "runtime config semantics differ")
    service = objects["Service"]["spec"]
    require(service["type"] == values["service"]["type"] and service["selector"] == labels, "service selector/type differs")
    require(service["ports"] == [{"name": port, "port": values["service"][key], "targetPort": port}
            for port, key in (("pgwire", "pgwirePort"), ("admin", "adminPort"))], "service port targets differ")
    if "NetworkPolicy" in objects:
        policy, network = objects["NetworkPolicy"]["spec"], values["networkPolicy"]
        expected_ingress = []
        for peers, ports in (("clientPeers", ["pgwire", "admin"]), ("proxyPeers", ["admin"]), ("metricsPeers", ["admin"])):
            if network[peers]:
                expected_ingress.append({"from": network[peers], "ports": [{"protocol": "TCP", "port": port} for port in ports]})
        require(policy == {"podSelector": {"matchLabels": labels}, "policyTypes": ["Ingress", "Egress"],
                "ingress": expected_ingress, "egress": network["egress"]}, "network policy selectors/ports/explicit DNS egress differ")
    if "PodDisruptionBudget" in objects:
        require(objects["PodDisruptionBudget"]["spec"] == {"minAvailable": 1, "unhealthyPodEvictionPolicy": "AlwaysAllow",
                "selector": {"matchLabels": labels}}, "PDB selection/availability differs")
    if "Ingress" in objects:
        ingress = values["ingress"]
        require(ingress["principalHeaderTrustAcknowledged"] and values["networkPolicy"]["proxyPeers"], "ingress trust/proxy acknowledgment missing")
        expected_ingress = {"tls": ingress["tls"], "rules": [{"host": host, "http": {"paths": [{"path": "/", "pathType": "Prefix",
            "backend": {"service": {"name": name, "port": {"name": "admin"}}}}]}} for host in ingress["hosts"]]}
        if ingress["className"]:
            expected_ingress["ingressClassName"] = ingress["className"]
        require(objects["Ingress"]["spec"] == expected_ingress, "TLS ingress host/service selection differs")
    for hook, empty in (("nodeSelector", {}), ("affinity", {}), ("tolerations", []), ("topologySpreadConstraints", []), ("imagePullSecrets", [])):
        require(pod.get(hook, empty) == values[hook], f"scheduling hook differs: {hook}")
    return {"object_count": len(objects), "kinds": sorted(objects), "image_reference": reference,
            "replicas": 1, "production_image_verified": False, "runtime_mode": runtime["mode"],
            "configured_profile": values["config"]["profile"], "volatile_data_survives_restart": False if volatile else "requires live cluster evidence",
            "gpu_vendor": gpu["vendor"] if gpu["enabled"] else None, "physical_gpu_execution_verified": False,
            "encryption_key_mounted": encryption["enabled"], "audit_key_mounted": audit["enabled"],
            "audit_anchor_on_data_volume": audit["enabled"] and not volatile, "spec_contents_verified": False}


def scenarios():
    pin = {"image": {"digest": TEST_DIGEST}}
    peers = {"networkPolicy": {
        "clientPeers": [{"namespaceSelector": {"matchLabels": {"purpose": "application"}}, "podSelector": {"matchLabels": {"role": "api"}}}],
        "proxyPeers": [{"namespaceSelector": {"matchLabels": {"purpose": "ingress"}}, "podSelector": {"matchLabels": {"role": "proxy"}}}],
        "metricsPeers": [{"namespaceSelector": {"matchLabels": {"purpose": "observability"}}, "podSelector": {"matchLabels": {"role": "prometheus"}}}],
        "egress": [{"to": [{"namespaceSelector": {"matchLabels": {"kubernetes.io/metadata.name": "kube-system"}},
                            "podSelector": {"matchLabels": {"k8s-app": "kube-dns"}}}],
                    "ports": [{"protocol": "UDP", "port": 53}, {"protocol": "TCP", "port": 53}]}]}}
    customization = merge_values(pin, peers)
    customization = merge_values(customization, {
        "persistence": {"existingClaim": "retained-database"},
        "secrets": {"adminPassword": {"name": "custom-admin", "key": "login"}, "adminToken": {"name": "custom-token", "key": "bearer"},
                    "pgPassword": {"name": "custom-pg", "key": "pg-login"}, "pgTls": {"name": "custom-tls", "certKey": "public.pem", "privateKey": "private.pem"}},
        "spec": {"enabled": True, "configMap": "qualification-spec", "key": "database.yaml", "apply": "create"},
        "audit": {"secretName": "custom-audit", "key": "audit-authentication", "mountPath": "/etc/multidb/custom-audit", "anchorPath": "/var/lib/multidb/custom-private/custom-audit.anchor"},
        "ingress": {"enabled": True, "className": "nginx", "hosts": ["db.example.test"], "tls": [{"secretName": "http-tls", "hosts": ["db.example.test"]}],
                    "principalHeaderTrustAcknowledged": True},
        "probes": {"startupPeriodSeconds": 3, "startupFailureThreshold": 80, "readinessFailureThreshold": 4,
                   "livenessFailureThreshold": 8, "periodSeconds": 7, "timeoutSeconds": 3, "initialDelaySeconds": 9}})
    positive = [("production", [], pin), ("local-dev", ["values-local.yaml"], {}),
                ("production-large", ["values-production-large.yaml"], pin), ("explicit-network-storage-tls-spec", [], customization)]
    positive += [("cpu-" + profile.lower(), [], merge_values(pin, {"config": {"profile": profile}})) for profile in CPU_PROFILES]
    positive += [("product-" + profile, [], merge_values(pin, {"config": {"profile": profile}})) for profile in CPU_PRODUCT_PROFILES]
    positive += [("cpu-in-memory", ["values-in-memory.yaml"], pin), ("encrypted-spec", ["values-encrypted-spec.yaml"], pin)]
    positive += [("tensor-gpu-" + vendor, ["values-gpu-" + vendor + ".yaml"],
                  merge_values(pin, {"runtimeClassName": "qualified-" + vendor, "supplementalGroups": [44, 109]})) for vendor in GPU_RESOURCES]
    positive += [("tensor-gpu-intel-xe", [], merge_values(pin, {
        "config": {"profile": "tensor"}, "gpu": {"enabled": True, "vendor": "intel", "resourceName": "gpu.intel.com/xe"},
        "resources": {"requests": {"gpu.intel.com/xe": 1}, "limits": {"gpu.intel.com/xe": 1}},
        "supplementalGroups": [109], "nodeSelector": {"kubernetes.io/os": "linux", "multidb.io/gpu-vendor": "intel"}})),
        ("product-tensor-dataset", ["values-gpu-nvidia.yaml"], merge_values(pin, {"config": {"profile": "tensor_dataset"}})),
        ("alias-deeplake-like", ["values-gpu-amd.yaml"], merge_values(pin, {"config": {"profile": "deeplake_like"}}))]
    negative = {
        "default-missing-digest": {}, "replicas-two": {"replicaCount": 2},
        "production-ephemeral": {"persistence": {"enabled": False, "devEmptyDir": True}},
        "production-no-network-policy": {"networkPolicy": {"enabled": False}},
        "bind-port-mismatch": {"runtime": {"adminBind": "0.0.0.0:8081"}},
        "ingress-missing-tls": {"ingress": {"enabled": True}},
        "ingress-missing-trust": merge_values(peers, {"ingress": {"enabled": True, "hosts": ["db.example.test"], "tls": [{"secretName": "http-tls", "hosts": ["db.example.test"]}]}}),
        "ingress-missing-proxy": {"ingress": {"enabled": True, "principalHeaderTrustAcknowledged": True, "hosts": ["db.example.test"], "tls": [{"secretName": "http-tls", "hosts": ["db.example.test"]}]}},
        "pg-secret-empty-key": {"secrets": {"pgPassword": {"key": ""}}},
        "pg-tls-missing-secret": {"secrets": {"pgTls": {"name": ""}}},
        "queue-out-of-range": {"config": {"queryQueueTimeoutMs": 60_001}},
        "concurrency-out-of-range": {"config": {"maxConcurrentQueries": 4097}},
        "unsupported-setting": {"automaticHa": True},
        "unsupported-replication": {"config": {"replication": "raft"}},
        "empty-network-peer": {"networkPolicy": {"clientPeers": [{}]}},
        "monitor-without-crd": merge_values(peers, {"monitoring": {"enabled": True, "serviceMonitor": {"enabled": True}}}),
        "memory-without-volatile-ack": {"config": {"profile": "InMemory"}},
        "durable-with-volatile-ack": {"runtime": {"allowVolatile": True}},
        "tensor-without-device": {"config": {"profile": "Tensor"}},
        "gpu-without-resource": {"config": {"profile": "Tensor"}, "gpu": {"enabled": True, "vendor": "nvidia"}},
        "gpu-mismatched-resource-count": {"config": {"profile": "Tensor"}, "gpu": {"enabled": True, "vendor": "nvidia"},
            "resources": {"requests": {"nvidia.com/gpu": 1}, "limits": {"nvidia.com/gpu": 2}}},
        "unsupported-gpu-vendor": {"gpu": {"enabled": True, "vendor": "software"}},
        "encryption-empty-secret": {"encryption": {"enabled": True, "secretName": ""}},
        "studio-overlaps-secret": {"runtime": {"studioDir": "/etc/multidb/auth"}},
        "production-audit-disabled": {"audit": {"enabled": False}},
        "audit-anchor-database-collision": {"audit": {"anchorPath": "/var/lib/multidb/audit-private/audit.anchor"}, "runtime": {"dbPath": "/var/lib/multidb/audit-private/audit.anchor"}},
        "audit-anchor-world-writable-pvc-root": {"audit": {"anchorPath": "/var/lib/multidb/audit.anchor"}},
        "audit-secret-masks-private-anchor": {"audit": {"mountPath": "/var/lib/multidb/audit-private"}},
        "audit-parent-is-database-file": {"runtime": {"dbPath": "/var/lib/multidb/audit-private"}},
        "audit-secret-noncanonical-data-overlap": {"audit": {"mountPath": "/var//lib/multidb/audit-private"}},
        "audit-empty-secret": {"audit": {"secretName": ""}},
        "audit-empty-secret-key": {"audit": {"key": ""}},
    }
    negative = [(name, [], override if name == "default-missing-digest" else merge_values(pin, override)) for name, override in negative.items()]
    return positive, negative


def chart_inventory(chart):
    files = {}
    for path in sorted(chart.rglob("*")):
        require(not path.is_symlink(), "chart symlinks are not accepted")
        if path.is_file():
            files[str(path.relative_to(chart))] = digest(path)
    require("Chart.yaml" in files and "values.schema.json" in files and "values.yaml" in files, "chart/schema/defaults are required")
    return {"files": files, "sha256": hashlib.sha256(json.dumps(files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()}


def validate_schema_report(report, documents):
    summary = report.get("summary", {})
    require(summary.get("valid") == len(documents) and all(summary.get(key) == 0 for key in ("invalid", "errors", "skipped")),
            "schema validator did not validate every rendered object")
    resources = report.get("resources", [])
    expected = {(doc["apiVersion"], doc["kind"], doc["metadata"]["name"]) for doc in documents}
    actual = set()
    for resource in resources:
        require(resource.get("status") == "statusValid", "schema resource has an invalid/error/skipped status")
        identity = (resource.get("version"), resource.get("kind"), resource.get("name"))
        require(identity not in actual, "schema report contains duplicate object results")
        actual.add(identity)
    require(len(resources) == len(documents) and actual == expected, "schema results do not cover the exact rendered API objects")
    return summary


def resolve_tool(value):
    resolved = shutil.which(str(value))
    if resolved is None:
        raise FileNotFoundError(f"required tool not executable: {value}")
    return Path(resolved).resolve()


class CommandRunner:
    def __init__(self, output):
        self.output, self.records = output, []
        self.environment = {key: value for key, value in os.environ.items() if not key.startswith("HELM_")}
        for name in ("CACHE", "CONFIG", "DATA"):
            self.environment[f"HELM_{name}_HOME"] = str(output / ("helm-" + name.lower()))

    def run(self, argv):
        number = len(self.records)
        stdout, stderr = self.output / f"command-{number:03}.stdout", self.output / f"command-{number:03}.stderr"
        record = {"argv": [str(part) for part in argv], "started_at_utc": utc_now(), "stdout": stdout.name, "stderr": stderr.name}
        began = time.monotonic()
        with stdout.open("xb") as out, stderr.open("xb") as err:
            try:
                result = subprocess.run(record["argv"], cwd=self.output, env=self.environment, stdout=out, stderr=err, timeout=55, check=False)
                record["exit_code"] = result.returncode
            except subprocess.TimeoutExpired:
                record.update(exit_code=None, timeout=True)
        record.update(duration_seconds=time.monotonic() - began, stdout_sha256=digest(stdout), stderr_sha256=digest(stderr))
        self.records.append(record)
        write_json(self.output / "commands.json", self.records)
        return record, stdout.read_bytes(), stderr.read_bytes()


def run(args):
    output, chart = args.output.resolve(), args.chart.resolve()
    if output.exists():
        raise FileExistsError("qualification output directory must be fresh")
    os.umask(0o077)
    output.mkdir(parents=True, mode=0o700)
    report = {"schema_version": 1, "kind": "kubernetes_static_chart_qualification", "started_at_utc": utc_now(),
              "status": "blocked", "production_deployment_qualified": False,
              "render_test_digest": TEST_DIGEST, "test_digest_scope": "synthetic fixture only; no deployed or verified image claimed",
              "kubernetes_version": args.kubernetes_version, "sections": {}, "tools": {}, "scenarios": []}
    runner = CommandRunner(output)
    try:
        require(re.fullmatch(r"1\.\d+\.\d+", args.kubernetes_version) is not None
                and tuple(map(int, args.kubernetes_version.split("."))) >= (1, 29, 0), "an explicit Kubernetes version >=1.29.0 is required")
        report["chart"] = chart_inventory(chart)
        report["validator_sha256"] = digest(Path(__file__))
        report["requirements_sha256"] = digest(Path(__file__).with_name("requirements-kubernetes.txt"))
        if args.plan:
            positive, negative = scenarios()
            report["planned_scenarios"] = {"positive": [item[0] for item in positive], "must_reject": [item[0] for item in negative]}
            report["sections"]["static_validation"] = {"status": "blocked", "reason": "plan only; no Helm or Kubernetes schema checks executed"}
            return finish(report, output)
        if yaml is None or yaml.__version__ != PY_YAML_VERSION:
            raise FileNotFoundError(f"PyYAML=={PY_YAML_VERSION} is required; install requirements-kubernetes.txt")
        tools = {"helm": resolve_tool(args.helm), "kubeconform": resolve_tool(args.kubeconform)}
        for name, binary in tools.items():
            report["tools"][name] = {"path": str(binary), "sha256": digest(binary)}
            command = [binary, "version", "--short"] if name == "helm" else [binary, "-v"]
            record, stdout, stderr = runner.run(command)
            require(record["exit_code"] == 0, f"{name} version command failed")
            report["tools"][name]["version"] = stdout.decode(errors="replace").strip()
        report["pyyaml"] = {"version": yaml.__version__, "module_sha256": digest(yaml.__file__)}
        base = strict_yaml((chart / "values.yaml").read_text())
        require(len(base) == 1 and isinstance(base[0], dict), "chart default values are not one mapping")
        positive, negative = scenarios()
        for expected_success, cases in ((True, positive), (False, negative)):
            for name, files, override in cases:
                section = {"name": name, "expected": "accept" if expected_success else "reject", "status": "failed"}
                report["scenarios"].append(section)
                override_file = output / (name + ".values.json")
                write_json(override_file, override)
                values = copy.deepcopy(base[0])
                value_args = []
                for filename in files:
                    path = chart / filename
                    parsed = strict_yaml(path.read_text())
                    require(len(parsed) == 1 and isinstance(parsed[0], dict), "scenario values are not one mapping")
                    values = merge_values(values, parsed[0])
                    value_args += ["-f", path]
                values = merge_values(values, override)
                value_args += ["-f", override_file]
                section["override_sha256"] = digest(override_file)
                lint, _, _ = runner.run([tools["helm"], "lint", "--strict", chart, "--kube-version", args.kubernetes_version, *value_args])
                template, rendered, _ = runner.run([tools["helm"], "template", RELEASE, chart, "--namespace", NAMESPACE,
                    "--kube-version", args.kubernetes_version, *value_args])
                section.update(helm_lint_exit=lint["exit_code"], helm_template_exit=template["exit_code"])
                if not expected_success:
                    require(lint["exit_code"] is not None and template["exit_code"] not in (None, 0), f"unsafe scenario was accepted: {name}")
                    section["rejection_basis"] = "actual helm template returned nonzero; lint result is recorded independently"
                    section["status"] = "accepted"
                    continue
                require(lint["exit_code"] == 0 and template["exit_code"] == 0, f"Helm positive scenario failed: {name}")
                manifest = output / (name + ".rendered.yaml")
                manifest.write_bytes(rendered)
                section["rendered_sha256"] = digest(manifest)
                documents = strict_yaml(rendered.decode())
                section["semantics"] = validate_objects(documents, values)
                schema_cache = output / "schema-cache"
                schema_cache.mkdir(exist_ok=True)
                schema, raw, _ = runner.run([tools["kubeconform"], "-strict", "-summary", "-verbose", "-output", "json", "-n", "2", "-cache", schema_cache,
                    "-kubernetes-version", args.kubernetes_version, manifest])
                section["kubeconform_exit"] = schema["exit_code"]
                require(schema["exit_code"] == 0, f"strict Kubernetes schema validation failed: {name}")
                section["schema_summary"] = validate_schema_report(json.loads(raw), documents)
                section["status"] = "accepted"
        require(report["chart"] == chart_inventory(chart), "chart changed during validation")
        require(report["validator_sha256"] == digest(Path(__file__))
                and report["requirements_sha256"] == digest(Path(__file__).with_name("requirements-kubernetes.txt")), "validator/dependency specification changed during validation")
        for tool in report["tools"].values():
            require(digest(tool["path"]) == tool["sha256"], "tool changed during validation")
        report["schema_cache"] = {str(path.relative_to(output)): digest(path) for path in sorted((output / "schema-cache").rglob("*")) if path.is_file()}
        report["sections"]["static_validation"] = {"status": "accepted", "scope": "actual Helm strict lint/render, parsed API semantics and strict Kubernetes schemas"}
        report["status"] = "accepted"
    except FileNotFoundError as error:
        report["sections"]["static_validation"] = {"status": "blocked", "reason": str(error)}
    except (ValueError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        report["status"] = "failed"
        report["sections"]["static_validation"] = {"status": "failed", "reason": f"{type(error).__name__}: {error}"}
    return finish(report, output)


def finish(report, output):
    report["sections"]["real_cluster"] = {"status": "blocked", "reason": "no live cluster runner executed; CNI isolation, CSI persistence, probes, restart, restore and upgrades require separate real-cluster evidence"}
    report["sections"]["ha"] = {"status": "blocked", "reason": "single-writer deployment; replicas=1 and a PDB do not qualify HA"}
    report["sections"]["physical_gpu"] = {"status": "blocked", "reason": "NVIDIA/AMD/Intel manifest rendering does not prove a physical adapter, device plugin, driver, strict GPU execution or device-loss recovery"}
    report["sections"]["profile_runtime_matrix"] = {"status": "blocked", "reason": "CPU/GPU profile variants were rendered only; execute each intended profile on a real cluster with the exact image and declared secrets/spec"}
    report["finished_at_utc"] = utc_now()
    if (output / "commands.json").exists():
        report["commands_sha256"] = digest(output / "commands.json")
    write_json(output / "report.json", report)
    print(json.dumps({"status": report["status"], "report": str(output / "report.json"), "production_deployment_qualified": False}))
    return 0 if report["status"] == "accepted" else 2


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chart", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--helm", default="helm")
    parser.add_argument("--kubeconform", default="kubeconform")
    parser.add_argument("--kubernetes-version", required=True)
    parser.add_argument("--plan", action="store_true")
    return run(parser.parse_args(argv))


if __name__ == "__main__":
    sys.exit(main())
