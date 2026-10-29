#!/usr/bin/env python3
"""Own a disposable single-node Kind fixture; qualify CPU runtime separately.

Never selects a user context, attaches to a preexisting cluster or claims Kind
kindnet/GPU/HA certification. Private kubeconfig and fixture keys are not public
evidence. No build, image import or application deployment occurs in plan mode.
"""
from __future__ import annotations

import argparse
import contextlib
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import ssl
import struct
import subprocess
import threading
import time
import uuid

from qualify import Blocked, digest, utc_now, write_json

NODE_IMAGE = "kindest/node:v1.35.8@sha256:07b2536e30b803ed61d1677a79df6115f798ce64c80f9e22f6ed45afd09323c0"
CPU_PROFILES = ("transactional", "analytical", "document", "vector", "time_series", "high_durability", "balanced", "in_memory")


def supported_skew(versions):
    def parts(value):
        import re
        match = re.fullmatch(r"v?(\d+)\.(\d+)\..*", value)
        if not match:
            raise Blocked("cannot establish kubectl/server version")
        return tuple(map(int, match.groups()))
    client, server = parts(versions["clientVersion"]["gitVersion"]), parts(versions["serverVersion"]["gitVersion"])
    if client[0] != server[0] or abs(client[1] - server[1]) > 1:
        raise Blocked("kubectl/server pair is outside supported one-minor skew")


def checked_rows(actual, expected):
    if actual != expected or any(not isinstance(row, list) or len(row) != 2 or type(row[0]) is not int or type(row[1]) is not str for row in actual):
        raise AssertionError("complete acknowledged fixture readback differs")


def main_status(pod):
    return next((s for s in pod.get("status", {}).get("containerStatuses", []) if s.get("name") == "multidb"), {})


def observed_clean_restart(before, after):
    previous, current = main_status(before), main_status(after)
    if before["metadata"]["uid"] != after["metadata"]["uid"]:
        raise AssertionError("Pod was replaced during the process-only SIGTERM check")
    old_id = previous.get("containerID")
    if not old_id or not previous.get("ready"):
        raise AssertionError("before-signal container identity/readiness is missing")
    if current.get("restartCount", 0) <= previous.get("restartCount", 0) or current.get("ready") is not True:
        return False
    ended = current.get("lastState", {}).get("terminated", {})
    if ended.get("containerID") != old_id or ended.get("exitCode") != 0 or not current.get("containerID") or current["containerID"] == old_id:
        raise AssertionError("observed restart is not the signaled container's clean termination")
    return True


def local_environment(kubeconfig):
    env = dict(os.environ)
    for key in ("DOCKER_CONTEXT", "DOCKER_TLS", "DOCKER_TLS_VERIFY", "DOCKER_CERT_PATH", "KUBECONFIG", "HELM_KUBECONTEXT"):
        env.pop(key, None)
    env.update(DOCKER_HOST="unix:///var/run/docker.sock", KIND_EXPERIMENTAL_PROVIDER="docker",
               KIND_EXPERIMENTAL_CONTAINERD_SNAPSHOTTER="native", KUBECONFIG=str(kubeconfig))
    additions = ("127.0.0.1", "localhost", ".cluster.local", "10.96.0.0/12", "10.244.0.0/16")
    env["NO_PROXY"] = ",".join(filter(None, [env.get("NO_PROXY", ""), *additions]))
    env["no_proxy"] = env["NO_PROXY"]
    return env


def validate_cluster_name(name):
    import re
    if not re.fullmatch(r"multidb-qualification(?:-[a-z0-9-]{1,24})?", name):
        raise ValueError("cluster name must be a dedicated multidb-qualification name")


class Fixture:
    def __init__(self, directory, tools, cluster, nested_kmsg=False):
        self.directory, self.tools, self.cluster = Path(directory), Path(tools), cluster
        validate_cluster_name(cluster)
        self.private = self.directory / "private"
        self.public = self.directory / "evidence"
        self.kubeconfig = self.private / "kubeconfig"
        self.env = local_environment(self.kubeconfig)
        self.nested_kmsg = nested_kmsg

    def tool(self, name):
        path = self.tools / name
        if not path.is_file():
            found = shutil.which(name)
            if not found:
                raise Blocked(f"required tool is absent:{name}")
            return found
        return str(path)

    def run(self, name, command, timeout=180):
        self.public.mkdir(parents=True, exist_ok=True, mode=0o700)
        log = self.public / (name + ".log")
        if log.exists():
            raise FileExistsError(f"command evidence already exists:{name}")
        with log.open("xb") as stream:
            result = subprocess.run(list(map(str, command)), env=self.env, stdout=stream,
                                    stderr=subprocess.STDOUT, timeout=timeout)
        if result.returncode:
            raise RuntimeError(f"{name} exited{result.returncode}; inspect{log}")
        return log.read_text()

    def docker(self, *arguments):
        return [self.tool("docker"), "--host=unix:///var/run/docker.sock", *map(str, arguments)]

    def kubectl(self, *arguments):
        return [self.tool("kubectl"), "--kubeconfig", str(self.kubeconfig), "--context", "kind-" + self.cluster, *map(str, arguments)]

    def containers(self):
        ids = subprocess.check_output(self.docker("ps", "-aq", "--filter", "label=io.x-k8s.kind.cluster=" + self.cluster), env=self.env).decode().split()
        return sorted(ids)

    def initialize(self):
        if self.directory.exists():
            raise FileExistsError("Kind fixture output must be fresh")
        if shutil.disk_usage(self.directory.parent).free < 5 * 1024**3:
            raise Blocked("at least5GiB free disk required before creating the owned Kind node")
        clusters = subprocess.check_output([self.tool("kind"), "get", "clusters"], env=self.env, stderr=subprocess.DEVNULL).decode().split()
        if self.cluster in clusters or self.containers():
            raise Blocked("cluster already exists; refusing to attach or replace it")
        os.umask(0o077)
        self.private.mkdir(parents=True, mode=0o700)
        self.public.mkdir(mode=0o700)
        state = {"schema_version": 1, "cluster": self.cluster, "run_id": uuid.uuid4().hex, "uid": os.getuid(),
                 "created_at_utc": utc_now(), "node_image": NODE_IMAGE, "status": "unrun", "owned_container_ids": [],
                 "kubeconfig": str(self.kubeconfig), "scope": "one control-plane local Kind; no HA, physical GPU or kindnet enforcement claim"}
        state["nested_actual_kmsg_device_requested"] = self.nested_kmsg
        state["containerd_snapshotter"] = "native"
        write_json(self.private / "owner.json", state)
        configuration = self.private / "kind.yaml"
        configuration.write_text("kind: Cluster\napiVersion: kind.x-k8s.io/v1alpha4\nnetworking:\n  apiServerAddress: 127.0.0.1\nnodes:\n- role: control-plane\n  labels:\n    multidb.io/qualification-run: " + state["run_id"] + "\n")
        try:
            image = json.loads(subprocess.check_output(self.docker("image", "inspect", NODE_IMAGE), env=self.env))[0]
            state["node_image_id"] = image["Id"]
            state["node_image_bytes"] = image["Size"]
            if shutil.disk_usage(self.directory).free < image["Size"] * 2 + 3 * 1024**3:
                raise Blocked("insufficient reserve for VFS node copy, inner images and runtime application")
            monitor_stop, monitor_errors = threading.Event(), []
            def prepare_real_kernel_device():
                try:
                    deadline = time.monotonic() + 120
                    while not monitor_stop.is_set() and time.monotonic() < deadline:
                        nodes = self.containers()
                        if nodes:
                            node = nodes[0]
                            ready = subprocess.run(self.docker("exec", node, "test", "-S", "/run/systemd/private"), env=self.env,
                                                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=10)
                            if ready.returncode == 0:
                                # Actual char device, inside our node only. A null/
                                # console symlink would conceal missing kernel data.
                                script = "if [ ! -e /dev/kmsg ]; then mknod /dev/kmsg c 1 11; fi; test ! -L /dev/kmsg; test -c /dev/kmsg; head -c 0 /dev/kmsg >/dev/null; stat -c '%t:%T:%F' /dev/kmsg"
                                self.run("actual-kmsg-device", self.docker("exec", "--privileged", node, "sh", "-ec", script), timeout=15)
                                return
                        monitor_stop.wait(.2)
                    if not monitor_stop.is_set():
                        raise TimeoutError("owned node systemd did not become available for actual kernel device check")
                except Exception as error:
                    monitor_errors.append(str(error))
            monitor = threading.Thread(target=prepare_real_kernel_device, daemon=True) if self.nested_kmsg else None
            if monitor:
                monitor.start()
            try:
                self.run("kind-create", [self.tool("kind"), "create", "cluster", "--name", self.cluster, "--image", NODE_IMAGE,
                         "--config", configuration, "--kubeconfig", self.kubeconfig, "--wait", "180s", "--retain"], timeout=300)
            finally:
                monitor_stop.set()
                if monitor:
                    monitor.join(timeout=20)
            if monitor_errors:
                raise Blocked(f"actual nested kernel device unavailable:{monitor_errors}")
            os.chmod(self.kubeconfig, 0o600)
            state["owned_container_ids"] = self.containers()
            nodes = json.loads(self.run("nodes", self.kubectl("get", "nodes", "-o", "json")))
            versions = json.loads(self.run("matched-client-server-version", self.kubectl("version", "-o", "json")))
            supported_skew(versions)
            if len(nodes.get("items", [])) != 1:
                raise AssertionError("owned cluster must contain exactly one node")
            node = nodes["items"][0]
            if node["metadata"]["labels"].get("multidb.io/qualification-run") != state["run_id"]:
                raise AssertionError("node ownership label differs")
            if not any(c.get("type") == "Ready" and c.get("status") == "True" for c in node["status"]["conditions"]):
                raise AssertionError("owned Kind node is not ready")
            state.update(status="accepted", kubernetes_version=node["status"]["nodeInfo"]["kubeletVersion"],
                         completed_at_utc=utc_now(), kubeconfig_sha256=digest(self.kubeconfig))
            state["kubectl_version"] = versions["clientVersion"]["gitVersion"]
        except Exception as error:
            state.update(status="blocked" if isinstance(error, Blocked) else "failed", reason=f"{type(error).__name__}:{error}",
                         owned_container_ids=self.containers())
            raise
        finally:
            write_json(self.private / "owner.json", state)
            public_state = {key: value for key, value in state.items() if key not in {"kubeconfig", "kubeconfig_sha256", "uid"}}
            public_state["unqualified_features"] = {"physical_gpu": "blocked:no physical adapter/device plugin", "ha": "blocked:one node and no live CLI cluster integration",
                                                     "network_policy_enforcement": "blocked:Kind kindnet does not enforce NetworkPolicy"}
            write_json(self.public / "infrastructure.json", public_state)
        return state

    def verify_owner(self):
        state = json.loads((self.private / "owner.json").read_text())
        if state.get("cluster") != self.cluster or state.get("uid") != os.getuid() or sorted(state.get("owned_container_ids", [])) != self.containers() or not state.get("owned_container_ids"):
            raise Blocked("private ownership record does not match the actual node containers")
        if state.get("kubeconfig_sha256") and digest(self.kubeconfig) != state["kubeconfig_sha256"]:
            raise Blocked("private kubeconfig changed")
        return state

    def verify_runtime_owner(self):
        state = self.verify_owner()
        for path in (self.directory, self.private, self.private / "owner.json", self.kubeconfig):
            info = path.stat()
            if path.is_symlink() or info.st_uid != os.getuid() or info.st_mode & 0o077:
                raise Blocked("runtime state/kubeconfig must be privately owned and not a symlink")
        if state.get("status") != "accepted" or not state.get("kubeconfig_sha256") or digest(self.kubeconfig) != state["kubeconfig_sha256"]:
            raise Blocked("runtime requires accepted infrastructure and an unchanged private kubeconfig")
        return state

    def cleanup(self):
        state = self.verify_owner()
        self.run("owned-node-delete", self.docker("rm", "--force", "--volumes", *state["owned_container_ids"]))
        if self.containers():
            raise RuntimeError("owned node containers remain after Kind delete")
        state["deleted_at_utc"] = utc_now()
        write_json(self.private / "owner.json", state)
        return {"status": "accepted", "deleted_cluster": self.cluster, "scope": "only private-owner-matched cluster; no image/volume prune"}


class HttpApi:
    def __init__(self, port, token, principal="admin"):
        self.port, self.token, self.principal = port, token, principal

    def request(self, method, path, body=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            raw = None if body is None else json.dumps(body, separators=(",", ":")).encode()
            connection.request(method, path, raw, {"Authorization": "Bearer " + self.token, "X-MultiDB-Principal": self.principal, "Content-Type": "application/json"})
            response = connection.getresponse()
            raw = response.read(2 * 1024 * 1024 + 1)
            if len(raw) > 2 * 1024 * 1024:
                raise AssertionError("small runtime fixture returned an oversized response")
            return response.status, json.loads(raw)
        finally:
            connection.close()

    def json(self, method, path, body=None):
        status, result = self.request(method, path, body)
        if status // 100 != 2 or result.get("ok") is not True:
            raise AssertionError(f"unexpected runtime API status{status}:{str(result)[:256]}")
        return result["data"]

    def rows(self, sql):
        return self.json("POST", "/sql", {"sql": sql})["output"]["rows"]


class RuntimeMatrix:
    def __init__(self, fixture, chart, binary, revision):
        fixture.verify_runtime_owner()
        self.fixture, self.chart, self.binary, self.revision = fixture, Path(chart).resolve(), Path(binary).resolve(), revision
        self.run_id = uuid.uuid4().hex[:10]
        self.private = fixture.private / ("runtime-" + self.run_id)
        self.public = fixture.public / ("runtime-" + self.run_id)
        self.private.mkdir(mode=0o700)
        self.public.mkdir(mode=0o700)
        self.serial = 0
        self.command_proofs = {}
        self.namespace = "multidb-qualification-" + self.run_id
        self.token = os.urandom(32).hex()

    def run(self, name, command, timeout=180):
        self.fixture.verify_runtime_owner()
        if shutil.disk_usage(self.private).free < 1024**3:
            raise Blocked("runtime disk reserve crossed1GiB; no further deployment/build mutation")
        self.serial += 1
        label = f"{self.serial:03}-{name}"
        text = self.fixture.run("runtime-" + self.run_id + "-" + label, command, timeout)
        log = self.fixture.public / ("runtime-" + self.run_id + "-" + label + ".log")
        self.command_proofs[log.name] = digest(log)
        return text

    def kubectl(self, *arguments):
        return self.fixture.kubectl("-n", self.namespace, *arguments)

    def helm(self, *arguments):
        return [self.fixture.tool("helm"), "--kubeconfig", str(self.fixture.kubeconfig), "--kube-context", "kind-" + self.fixture.cluster,
                "--namespace", self.namespace, *map(str, arguments)]

    def secret(self, name, files):
        self.run("secret-" + name, self.kubectl("create", "secret", "generic", name, *[f"--from-file={key}={path}" for key, path in files.items()]))

    def prepare_namespace(self):
        self.fixture.verify_runtime_owner()
        existing = subprocess.run(self.fixture.kubectl("get", "namespace", self.namespace), env=self.fixture.env,
                                  stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30)
        if existing.returncode == 0:
            raise Blocked("runtime namespace already exists; do not attach to a previous/user deployment")
        self.run("namespace", self.fixture.kubectl("create", "namespace", self.namespace))
        self.run("restricted-namespace", self.fixture.kubectl("label", "namespace", self.namespace,
            "pod-security.kubernetes.io/enforce=restricted", "pod-security.kubernetes.io/enforce-version=v1.35",
            "pod-security.kubernetes.io/audit=restricted", "pod-security.kubernetes.io/warn=restricted", "multidb.io/qualification-run=" + self.run_id))
        files = {}
        for name, content in (("admin-password", os.urandom(24).hex().encode()), ("admin-token", self.token.encode()),
                              ("pg-password", os.urandom(24).hex().encode()), ("audit-key", os.urandom(32)), ("encryption-key", os.urandom(32))):
            path = self.private / name
            path.write_bytes(content)
            path.chmod(0o600)
            files[name] = path
        self.secret("qualification-admin", {key: files[key] for key in ("admin-password", "admin-token")})
        self.secret("qualification-pg", {"password": files["pg-password"]})
        self.secret("qualification-audit", {"audit-key": files["audit-key"]})
        self.secret("qualification-encryption", {"key": files["encryption-key"]})

    def build_image(self, repository_root, studio_dir):
        from oci_archive import convert
        from qualify import require_idle
        require_idle()
        if not self.binary.is_file() or not Path(studio_dir).is_dir() or not (Path(studio_dir) / "index.html").is_file():
            raise Blocked("actual release binary and built Studio assets are required; placeholders are not used")
        if shutil.disk_usage(self.private).free < 6 * 1024**3:
            raise Blocked("insufficient reserve for prebuilt image/export/OCI conversion")
        context = self.private / "context"
        context.mkdir()
        shutil.copy2(self.binary, context / "multidb")
        shutil.copytree(studio_dir, context / "studio")
        binary_hash = digest(self.binary)
        reference = "multidb-qualification:" + binary_hash[:12] + "-" + self.run_id
        self.run("image-build", self.fixture.docker("build", "--file", Path(repository_root) / "Dockerfile.prebuilt", "--tag", reference, context), timeout=900)
        docker_archive = self.private / "docker-image.tar"
        self.run("image-save", self.fixture.docker("save", "--output", docker_archive, reference), timeout=300)
        archive = self.public / "runtime-image.oci.tar"
        evidence = convert(docker_archive, archive, reference)
        evidence.update(binary_sha256=binary_hash, runtime_source_revision=self.revision,
                        dockerfile_sha256=digest(Path(repository_root) / "Dockerfile.prebuilt"),
                        converter_sha256=digest(Path(__file__).with_name("oci_archive.py")))
        write_json(self.public / "image.json", evidence)
        docker_archive.unlink()  # Owned redundant intermediate; OCI proof survives.
        if digest(self.binary) != binary_hash or digest(context / "multidb") != binary_hash:
            raise AssertionError("release binary changed during image construction")
        return archive, evidence

    def import_image(self, archive, evidence):
        node = self.fixture.verify_runtime_owner()["owned_container_ids"]
        if len(node) != 1:
            raise Blocked("runtime image import requires the owned single node")
        remote = "/kind/multidb-qualification-" + self.run_id + ".oci.tar"
        self.run("image-copy", self.fixture.docker("cp", str(archive), node[0] + ":" + remote), timeout=300)
        # The converter only accepts linux/amd64. Use ctr's local importer:
        # containerd 2's transfer service cannot initialize this native unpacker,
        # even with an explicit platform. Digest verification still follows.
        self.run("image-import", self.fixture.docker("exec", node[0], "ctr", "--namespace=k8s.io", "images", "import", "--local", "--snapshotter", "native", "--platform", "linux/amd64", "--digests", remote), timeout=300)
        self.run("image-copy-cleanup", self.fixture.docker("exec", node[0], "rm", remote))
        repository = evidence["reference"].rsplit(":", 1)[0]
        if "/" not in repository:
            repository = "docker.io/library/" + repository
        pinned = repository + "@" + evidence["manifest_digest"]
        listing = self.run("image-imported-list", self.fixture.docker("exec", node[0], "ctr", "--namespace=k8s.io", "images", "list"))
        possible = (evidence["reference"], repository + ":" + evidence["reference"].rsplit(":", 1)[1])
        imported = next((name for name in possible if any(line.split() and line.split()[0] == name and evidence["manifest_digest"] in line for line in listing.splitlines())), None)
        if imported is None:
            raise AssertionError("imported reference does not point to the actual expected OCI manifest")
        self.run("image-pin", self.fixture.docker("exec", node[0], "ctr", "--namespace=k8s.io", "images", "tag", imported, pinned))
        listing = self.run("image-pinned-list", self.fixture.docker("exec", node[0], "ctr", "--namespace=k8s.io", "images", "list"))
        matching = [line for line in listing.splitlines() if line.split() and line.split()[0] == pinned]
        if len(matching) != 1 or evidence["manifest_digest"] not in matching[0]:
            raise AssertionError("containerd did not retain the exact OCI manifest digest")
        return repository

    @contextlib.contextmanager
    def forward(self, service, port):
        self.fixture.verify_runtime_owner()
        log = self.private / f"forward-{self.serial}-{port}.log"
        stream = log.open("xb")
        process = subprocess.Popen(self.kubectl("port-forward", "--address", "127.0.0.1", "service/" + service, f":{port}"),
                                   env=self.fixture.env, stdout=stream, stderr=subprocess.STDOUT, start_new_session=True)
        try:
            import re
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                if process.poll() is not None:
                    raise RuntimeError("owned port-forward exited")
                match = re.search(r"Forwarding from 127.0.0.1:(\d+)", log.read_text())
                if match:
                    yield int(match.group(1))
                    return
                time.sleep(.1)
            raise TimeoutError("owned localhost port-forward did not become ready")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
            stream.close()

    def values(self, profile, repository, image_digest, tls_name, encrypted=False):
        memory = profile == "in_memory"
        return {"image": {"repository": repository, "digest": image_digest, "pullPolicy": "Never"},
                "config": {"profile": profile, "maxConcurrentQueries": 2},
                "runtime": {"allowVolatile": memory}, "pdb": {"enabled": False},
                "persistence": {"enabled": not memory, "devEmptyDir": memory, "size": "1Gi"},
                "resources": {"requests": {"cpu": "100m", "memory": "256Mi"}, "limits": {"cpu": "1", "memory": "2Gi"}},
                "secrets": {"adminPassword": {"name": "qualification-admin"}, "adminToken": {"name": "qualification-admin"},
                            "pgPassword": {"name": "qualification-pg"}, "pgTls": {"name": tls_name}},
                "audit": {"secretName": "qualification-audit"},
                "encryption": {"enabled": encrypted, "secretName": "qualification-encryption"}}

    def pod(self, release):
        result = json.loads(self.run("pod", self.kubectl("get", "pods", "-l", "app.kubernetes.io/instance=" + release, "-o", "json")))
        items = [item for item in result.get("items", []) if not item["metadata"].get("deletionTimestamp")]
        if len(items) != 1:
            raise AssertionError("runtime release must have exactly one live Pod")
        return items[0]

    def verify_pod_binary(self, pod, expected_hash):
        actual = self.run("binary-sha", self.kubectl("exec", pod["metadata"]["name"], "--", "sha256sum", "/usr/local/bin/multidb")).split()[0]
        if actual != expected_hash:
            raise AssertionError("actual Pod executable differs from the qualified artifact")
        return actual

    def tls(self, service, certificate):
        context = ssl.create_default_context(cafile=str(certificate))
        with self.forward(service, 5432) as port, socket.create_connection(("127.0.0.1", port), timeout=10) as connection:
            connection.sendall(struct.pack("!II", 8, 80877103))
            if connection.recv(1) != b"S":
                raise AssertionError("pgwire did not accept its TLS negotiation")
            with context.wrap_socket(connection, server_hostname=service) as secured:
                return hashlib.sha256(secured.getpeercert(binary_form=True)).hexdigest()

    def fixture_data(self, api, expected):
        api.json("POST", "/builder/table", {"name": "qualification_probe", "schema": {"primary_key": 0,
                 "columns": [{"name": "id", "ty": "Int", "nullable": False}, {"name": "payload", "ty": "Str", "nullable": False}]}, "indexes": []})
        sql = "INSERT INTO qualification_probe VALUES " + ",".join(f"({i},'{value}')" for i, value in expected)
        output = api.json("POST", "/sql", {"sql": sql})["output"]
        if type(output.get("affected_rows")) is not int or output["affected_rows"] != len(expected):
            raise AssertionError("fixture write did not acknowledge the complete batch")
        checked_rows(api.rows("SELECT id,payload FROM qualification_probe ORDER BY id"), expected)

    def denial(self, api):
        security = api.json("GET", "/security")
        principals = [p for p in security["principals"] if p["user"] != "qualification_denied"]
        principals.append({"user": "qualification_denied", "principal": "qualification_denied", "roles": []})
        api.json("POST", "/security", {"roles": security["roles"], "principals": principals})
        denied = HttpApi(api.port, self.token, "qualification_denied")
        status, response = denied.request("POST", "/sql", {"sql": "SELECT payload FROM qualification_probe"})
        if status != 403 or response.get("ok") is not False or response.get("error", {}).get("code") != "forbidden" or "data" in response:
            raise AssertionError("no-role SQL request escaped policy")

    def wrong_key_reopen(self, release, service, original_values, case_private, expected):
        wrong_key = case_private / "deliberately-wrong-encryption-key"
        wrong_key.write_bytes(os.urandom(32))
        wrong_key.chmod(0o600)
        wrong_secret = release + "-wrong-key"
        self.secret(wrong_secret, {"key": wrong_key})
        damaged = json.loads(original_values.read_text())
        damaged["encryption"]["secretName"] = wrong_secret
        wrong_values = case_private / "wrong-key-values.json"
        write_json(wrong_values, damaged)
        result = None
        try:
            self.run("wrong-key-upgrade", self.helm("upgrade", release, self.chart, "--values", wrong_values))
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                pods = json.loads(self.run("wrong-key-pods", self.kubectl("get", "pods", "-l", "app.kubernetes.io/instance=" + release, "-o", "json")))
                candidates = [p for p in pods.get("items", []) if not p["metadata"].get("deletionTimestamp")
                              and any(v.get("name") == "encryption" and v.get("secret", {}).get("secretName") == wrong_secret for v in p["spec"]["volumes"])]
                if len(candidates) > 1:
                    raise AssertionError("wrong-key replacement overlapped multiple live Pods")
                if candidates:
                    pod = candidates[0]
                    state = main_status(pod)
                    if state.get("ready"):
                        raise AssertionError("wrong encryption key became Ready; configured mounts alone do not prove encryption")
                    ended = state.get("lastState", {}).get("terminated") or state.get("state", {}).get("terminated")
                    if ended and type(ended.get("exitCode")) is int and ended["exitCode"] != 0:
                        args = ["logs", pod["metadata"]["name"], "--container=multidb"]
                        if state.get("restartCount", 0) > 0:
                            args.append("--previous")
                        logs = self.run("wrong-key-startup", self.kubectl(*args))
                        if not any(marker in logs.lower() for marker in ("encrypted value authentication failed", "wrapped dek authentication failed")):
                            raise AssertionError("wrong-key Pod failed for an unrelated reason; no encryption proof")
                        result = {"status": "accepted", "pod_uid": pod["metadata"]["uid"], "exit_code": ended["exitCode"],
                                  "proof": "actual changed key mounted; startup rejected with encrypted authentication failure and never Ready"}
                        break
                time.sleep(1)
            if result is None:
                raise AssertionError("actual wrong-key encrypted startup failure was not observed")
        finally:
            self.run("correct-key-restore", self.helm("upgrade", release, self.chart, "--values", original_values,
                                                     "--wait", "--timeout", "180s"), timeout=210)
        with self.forward(service, 8080) as port:
            checked_rows(HttpApi(port, self.token).rows("SELECT id,payload FROM qualification_probe ORDER BY id"), expected)
        return result

    def case(self, profile, repository, evidence, encrypted=False):
        name = profile.replace("_", "-") + ("-encrypted" if encrypted else "")
        release = "q-" + name
        service = release + "-multidb"
        case_private = self.private / name
        case_private.mkdir()
        certificate, key = case_private / "tls.crt", case_private / "tls.key"
        self.run("certificate", ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key, "-out", certificate,
                                 "-days", "2", "-subj", "/CN=" + service, "-addext", "subjectAltName=DNS:" + service + ",DNS:" + service + "." + self.namespace + ".svc"])
        key.chmod(0o600)
        tls_name = release + "-tls"
        self.run("tls-secret", self.kubectl("create", "secret", "tls", tls_name, "--cert", certificate, "--key", key))
        values_file = case_private / "values.json"
        write_json(values_file, self.values(profile, repository, evidence["manifest_digest"], tls_name, encrypted))
        self.run("install-" + name, self.helm("upgrade", "--install", release, self.chart, "--values", values_file, "--wait", "--timeout", "180s"), timeout=210)
        first_pod = self.pod(release)
        pod_name = first_pod["metadata"]["name"]
        binary = self.verify_pod_binary(first_pod, evidence["binary_sha256"])
        tls_hash = self.tls(service, certificate)
        expected = [[i, "qualification-" + self.run_id + "-" + str(i)] for i in range(32)]
        with self.forward(service, 8080) as port:
            api = HttpApi(port, self.token)
            self.fixture_data(api, expected)
            self.denial(api)
        before_signal = self.pod(release)
        before_uid = before_signal["metadata"]["uid"]
        self.run("sigterm", self.kubectl("exec", before_signal["metadata"]["name"], "--", "/bin/sh", "-c", "kill -TERM 1"))
        exit_code = None
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            current = self.pod(release)
            if observed_clean_restart(before_signal, current):
                exit_code = 0
                break
            time.sleep(1)
        if exit_code != 0:
            raise AssertionError(f"graceful SIGTERM clean exit/restart was not observed:{exit_code}")
        self.verify_pod_binary(current, evidence["binary_sha256"])
        with self.forward(service, 8080) as port:
            api = HttpApi(port, self.token)
            if profile == "in_memory":
                if any(o["name"] == "qualification_probe" for o in api.json("GET", "/catalog")["objects"]):
                    raise AssertionError("InMemory unexpectedly retained its process-local fixture")
                self.fixture_data(api, expected)
            else:
                checked_rows(api.rows("SELECT id,payload FROM qualification_probe ORDER BY id"), expected)
        self.run("pod-replacement", self.kubectl("rollout", "restart", "deployment/" + service))
        self.run("replacement-ready", self.kubectl("rollout", "status", "deployment/" + service, "--timeout=180s"), timeout=190)
        new_pod = self.pod(release)
        if new_pod["metadata"]["uid"] == before_uid:
            raise AssertionError("Pod replacement was not actually observed")
        self.verify_pod_binary(new_pod, evidence["binary_sha256"])
        with self.forward(service, 8080) as port:
            api = HttpApi(port, self.token)
            if profile == "in_memory":
                if any(o["name"] == "qualification_probe" for o in api.json("GET", "/catalog")["objects"]):
                    raise AssertionError("InMemory unexpectedly retained its replaced-Pod fixture")
            else:
                checked_rows(api.rows("SELECT id,payload FROM qualification_probe ORDER BY id"), expected)
        wrong_key = self.wrong_key_reopen(release, service, values_file, case_private, expected) if encrypted else None
        if encrypted:
            self.verify_pod_binary(self.pod(release), evidence["binary_sha256"])
        result = {"status": "accepted", "profile": profile, "encrypted_key_configured": encrypted,
                  "binary_sha256": binary, "pgwire_verified_tls_certificate_sha256": tls_hash,
                  "acknowledged_batch_rows": len(expected), "process_sigterm_exit_code": exit_code,
                  "old_pod_uid": before_uid, "new_pod_uid": new_pod["metadata"]["uid"],
                  "restart_result": "expected_loss_verified" if profile == "in_memory" else "complete_readback_verified",
                  "wrong_key_reopen": wrong_key,
                  "denied_policy": "accepted", "scope": "basic actual runtime/TLS/policy/graceful/PVC or explicit volatile semantics; not throughput/HA/hardware certification"}
        self.run("uninstall-" + name, self.helm("uninstall", release, "--wait", "--timeout", "120s"), timeout=150)
        return result

    def execute(self, repository_root, studio_dir):
        self.fixture.verify_runtime_owner()
        report = {"schema_version": 1, "status": "unrun", "started_at_utc": utc_now(), "runtime_source_revision": self.revision,
                  "binary_sha256": digest(self.binary) if self.binary.is_file() else None, "production_deployment_qualified": False,
                  "scope": "single-node Kind CPU functional runtime; no latency SLO measured",
                  "profiles": {p: {"status": "unrun"} for p in CPU_PROFILES}, "encrypted_reopen": {"status": "unrun"},
                  "blocked": {"physical_gpu": "no actual adapter/device plugin", "ha": "single node/no live CLI cluster",
                              "network_policy_enforcement": "default Kind kindnet does not enforce NetworkPolicy", "large_capacity_slo": "insufficient local hardware/not measured"}}
        try:
            if not self.binary.is_file():
                raise Blocked("final immutable release executable is not yet available")
            versions = json.loads(self.run("versions", self.fixture.kubectl("version", "-o", "json")))
            supported_skew(versions)
            archive, evidence = self.build_image(repository_root, studio_dir)
            repository = self.import_image(archive, evidence)
            self.prepare_namespace()
            for profile in CPU_PROFILES:
                try:
                    report["profiles"][profile] = self.case(profile, repository, evidence)
                except Exception as error:
                    report["profiles"][profile] = {"status": "failed", "reason": str(error)}
                    raise
                write_json(self.public / "runtime.json", report)
            try:
                report["encrypted_reopen"] = self.case("transactional", repository, evidence, encrypted=True)
            except Exception as error:
                report["encrypted_reopen"] = {"status": "failed", "reason": str(error)}
                raise
            report["status"] = "accepted"
        except Exception as error:
            report.update(status="blocked" if isinstance(error, Blocked) else "failed", reason=f"{type(error).__name__}:{error}")
        finally:
            report["finished_at_utc"] = utc_now()
            report["private_material_excluded_from_public_evidence"] = True
            report["namespace"] = self.namespace
            report["runner_sha256"] = digest(Path(__file__))
            report["command_proofs"] = self.command_proofs
            report["chart_sha256"] = {str(path.relative_to(self.chart)): digest(path) for path in sorted(self.chart.rglob("*")) if path.is_file()}
            write_json(self.public / "runtime.json", report)
        return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--tools", type=Path, default=Path("/workspace/scratch/kubernetes-tools"))
    parser.add_argument("--cluster", default="multidb-qualification")
    parser.add_argument("--prepare-cluster", action="store_true")
    parser.add_argument("--cleanup", action="store_true")
    parser.add_argument("--plan", action="store_true")
    parser.add_argument("--nested-kmsg-device", action="store_true", help="create actual /dev/kmsg char1:11 inside the new owned node if absent; never a null/console substitute")
    parser.add_argument("--runtime", action="store_true", help="build/import an actual prebuilt image and execute CPU cases on the private owned cluster")
    parser.add_argument("--binary", type=Path)
    parser.add_argument("--revision")
    parser.add_argument("--repository", type=Path, default=Path(__file__).resolve().parents[2])
    parser.add_argument("--studio-dir", type=Path)
    args = parser.parse_args(argv)
    fixture = Fixture(args.output.resolve(), args.tools.resolve(), args.cluster, args.nested_kmsg_device)
    if args.plan:
        print(json.dumps({"schema_version": 1, "status": "unrun", "cluster": args.cluster, "node_image": NODE_IMAGE,
                          "cpu_profiles": list(CPU_PROFILES), "physical_gpu": "blocked", "kindnet_policy_enforcement": "blocked",
                          "required_runtime_artifact": "immutable binary plus real locally imported OCI manifest digest; no image-ID-as-manifest substitute"}, indent=2))
        return 0
    if sum((args.prepare_cluster, args.cleanup, args.runtime)) != 1:
        raise ValueError("choose exactly one action, or --plan")
    try:
        if args.runtime:
            if not args.binary or not args.revision:
                raise ValueError("actual --binary and source --revision required for runtime")
            matrix = RuntimeMatrix(fixture, args.repository / "ops/helm/multidb", args.binary, args.revision)
            result = matrix.execute(args.repository, args.studio_dir or args.repository / "studio/dist")
        else:
            result = fixture.initialize() if args.prepare_cluster else fixture.cleanup()
        print(json.dumps({"status": result["status"], "cluster": args.cluster, "public_evidence": str(fixture.public)}, indent=2))
        return 0 if result["status"] == "accepted" else 2
    except Exception as error:
        print(json.dumps({"status": "blocked" if isinstance(error, Blocked) else "failed", "reason": str(error)}, indent=2))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
