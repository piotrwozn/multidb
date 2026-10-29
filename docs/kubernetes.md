# Kubernetes deployment contract

The chart deploys **one MultiDB process for any existing storage profile**:
durable CPU profiles, explicitly volatile InMemory, or GPU-backed Tensor.
It provides deployment controls for application teams; it does not turn flat
`multidb serve` into database HA, a live Raft/AP cluster, read replicas or
horizontal write scaling. `replicaCount` must be 1. Multiple application pods
can use bounded connection pools against this instance, but an application with
Allegro-scale availability/throughput requirements needs its actual workload,
failure and recovery qualification before adopting this database.

The chart fails for unsupported multiwriter configuration instead of presenting
three independent processes as a cluster. The flat server ignores the legacy
`MULTIDB_REPLICATION` hint; spec mode checks its declared replication and rejects
unsupported live integrations. A multi-node production promise remains blocked
until the actual server, consensus, identity and recovery paths are integrated
and tested. Node affinity, topology constraints, a PDB and Kubernetes itself do
not supply that missing database replication.

## Required platform and release inputs

The chart targets Linux and Kubernetes 1.29 or newer. Qualify the actual cluster,
CSI driver, storage class, filesystem and image; the version range is an API
compatibility constraint, not evidence that every platform has been tested.

- Supply a real, verified `image.digest: sha256:<64 lowercase hex characters>`.
  Production values deliberately have no invented digest and therefore cannot
  install unchanged. Check the signed image digest against the release evidence.
- Provide existing credential and pgwire TLS Secrets. The chart does not generate
  passwords or private keys and never writes their values into a ConfigMap.
- For durable profiles, use storage with sufficient room for history, audit and
  recovery staging. Confirm volume ownership for UID/GID 10001 and CSI fsGroup
  behavior. InMemory instead requires an explicit volatile declaration and
  intentionally has no PVC or data-survival guarantee.
- Use a CNI that actually enforces NetworkPolicy. Empty peer/egress lists deny
  application traffic by default. A default Kind kindnet cluster does **not**
  prove enforcement; accepting a NetworkPolicy object is not a network test.
- Establish the operator runbooks, backup downtime, RPO/RTO, SLO owner and
  supported SQL/API subset. See [qualification](operations/qualification.md) and
  [deployment/incidents](operations/deployment-and-incidents.md).

For example, create Secrets from approved private files, without putting values
in shell history or Git:

```sh
kubectl -n database create secret generic multidb-admin \
  --from-file=admin-password=/private/admin-password \
  --from-file=admin-token=/private/admin-token
kubectl -n database create secret generic multidb-pg \
  --from-file=password=/private/pg-password
kubectl -n database create secret tls multidb-pg-tls \
  --cert=/private/pg-server.crt --key=/private/pg-server.key
kubectl -n database create secret generic multidb-audit \
  --from-file=audit-key=/private/audit-signing.key
```

The certificate SAN must match the service name that pgwire clients verify.
Production always mounts pgwire TLS and never sets the local plaintext bypass.
Production also requires a private existing audit key (32 raw bytes or64 hex
characters), projected read-only as0440 with the trusted fsGroup. The durable
anchor defaults to `/var/lib/multidb/audit-private/audit.anchor`, separate from the database
and key, on the same PVC. The non-root init container creates its private parent
with mode0700, including on StorageClasses whose PVC root is world-writable;
it refuses a symlink parent and never changes permissions on the PVC root.
Custom anchors must also use one private child directory below `/var/lib/multidb`.
When upgrading an existing deployment with a flat anchor at the PVC root, stop
the writer and preserve that exact anchor in the new private directory before
changing the configured path; do not discard it or regenerate its history.
The init container refuses to start while the legacy flat default `audit.anchor`
still exists (including a dangling symlink), so its independent antirollback
anchor cannot be silently abandoned by this default-path change.
Retain the correct key and
anchor as operational security evidence, and never replace a bad/wrong-key
prefix to force startup. InMemory receives the real key but omits a persistent
anchor because its audit history is explicitly volatile. Local-development
values alone may disable the production key and retain the development signer.
Set `secrets.adminToken.name: ""` to omit the legacy bearer token when it is not
needed; password sessions remain available. Configure least-privilege principal
roles using the documented API and verify both an authorized and denied request.
A successful TLS/SCRAM connection alone is not proof of SQL authorization.

## Production values and network boundaries

Put real site values in a private reviewed file. The following is a shape
example; its digest placeholder is deliberately not installable:

```yaml
image:
  digest: sha256:<verified-release-digest>
persistence:
  storageClassName: approved-durable-class
networkPolicy:
  clientPeers:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: applications
      podSelector:
        matchLabels:
          app.kubernetes.io/part-of: approved-application
  proxyPeers:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ingress-system
      podSelector:
        matchLabels:
          app.kubernetes.io/name: approved-gateway
```

Each peer object combines its namespace and pod selectors with AND. Separate
peer entries are OR. Client peers can reach pgwire and the HTTP API; proxy and
metrics peers can reach HTTP. NetworkPolicy is a network/port control and cannot
restrict an admitted HTTP peer to one URL or strip identity headers. Keep the
ClusterIP service private, enforce application authorization and protect tokens.
Add only the required DNS/cloud destinations to `networkPolicy.egress`; the
default allows no pod-initiated egress. Retained MinIO/Vault/backup target values
are operator hints, not working background backup or Vault integrations in flat
serve. The chart does not create a backup schedule by setting those values.

```sh
helm lint ops/helm/multidb -f /private/site-values.yaml
helm template database ops/helm/multidb -n database \
  -f /private/site-values.yaml > /private/rendered.yaml
# Validate/review the rendered resources before applying them.
helm upgrade --install database ops/helm/multidb -n database \
  -f /private/site-values.yaml --wait --timeout 15m
```

Schema and template validation reject extra replicas, invalid/missing production
digests, unacknowledged volatile/emptyDir storage, disabled production NetworkPolicy, mismatched bind
ports, missing persistence, and unsafe Ingress prerequisites. The schema also
rejects unknown values, empty peers that would admit everything, invalid TLS
secret references, and unsupported runtime governor bounds. Do not bypass schema
validation or use a disposable test digest as a release image.

## Exposure, probes and credentials

Ingress is disabled by default. Enabling it requires declared TLS hosts, a trusted
`networkPolicy.proxyPeers` selection and explicit
`ingress.principalHeaderTrustAcknowledged: true`. Every HTTP host needs a matching
TLS host entry. Use an authenticated TLS-only gateway/controller and verify its
actual redirect/rejection behavior for HTTP. Controller-specific annotations are
operator-owned; a generic Ingress TLS stanza cannot guarantee every controller's
HTTP policy.

The gateway must strip untrusted `X-MultiDB-Principal`, or replace it only after
authenticating and mapping an approved identity. Prefer credential-bound sessions
for interactive administration. An acknowledgment value does not install header
sanitization, client authentication or a service mesh. Test that a client cannot
spoof a more privileged principal through the deployed gateway. HTTP inside the
pod network is not native server TLS; use an approved mesh/proxy if in-cluster
transport encryption is required.

Secrets are projected read-only as files with mode 0440 and fsGroup 10001; the
server receives `*_FILE` paths instead of plaintext environment variables. A
dedicated ServiceAccount has token automount disabled and no chart-created API
RBAC. Pod/container contexts run as nonroot UID/GID 10001, drop all capabilities,
use RuntimeDefault seccomp, forbid privilege escalation and make the root
filesystem read-only. Writable paths are the data volume and bounded temporary
volumes. A nonroot init container creates a private 0700 XDG runtime directory.

Startup probing permits the configured recovery window before liveness starts;
readiness uses `/ready`, liveness `/health`. Adjust the startup budget using
measured recovery time rather than causing repeated restarts of a large volume.
The preStop delay permits endpoint/gateway propagation before SIGTERM. The grace
period must exceed that delay plus the drain budget; forced deletion bypasses
graceful handling and is not a safe rolling-upgrade tool.

Optional `monitoring.serviceMonitor.enabled` requires the Prometheus Operator
ServiceMonitor CRD, `monitoring.enabled`, explicit metrics peers and the existing
bearer-token Secret. This token is a privileged automation credential: protect
the Prometheus operator/namespace and Secret access, or place a separately
authenticated metrics proxy in front of the endpoint. ServiceMonitor configuration
alone does not deliver alerts or establish least-privilege monitoring credentials.

## Capacity for large applications

[values-production-large.yaml](../ops/helm/multidb/values-production-large.yaml)
is an explicit capacity starting point: requests of eight CPUs/16 GiB RAM,
limits of sixteen CPUs/32 GiB RAM, 4/16 GiB ephemeral storage and a 500 GiB PVC.
It prefers labeled database nodes and exposes Linux/zone scheduling hooks. Its
RWOP access mode needs a supporting CSI driver. These are planning values,
**not a tested throughput, availability or tail-latency SLO**.

The default chart limits HTTP/pgwire query concurrency to two with a 4 GiB pod
memory limit. The large profile permits sixteen queries. The Transactional
profile can budget 512 MiB per query; add index/native/cache/mapped-page overhead
when sizing. Tune `config.maxConcurrentQueries` and `config.queryQueueTimeoutMs`
within the actual server's 4096/60000 ms bounds. Other query budgets belong in a
supported DatabaseSpec/profile. No invented memory/GPU/replication environment
knobs are emitted. Pool client connections, preserve backpressure, measure busy
responses/timeouts, and qualify the intended mix at its actual data size.

`nodeSelector`, `affinity`, `tolerations`, `topologySpreadConstraints` and
`imagePullSecrets` are deployment hooks. A single writer placed in one zone has
that zone's availability risk; scheduling preferences do not replicate its data.
GPU devices, drivers, passthrough and real accelerator SLOs require separate
hardware qualification; CPU-only manifest tests cannot certify them.

## All profiles, encrypted specs and GPU deployment

The [profile examples](../ops/helm/multidb/examples/README.md) list every built-in
storage profile and the actual parser's product slugs/aliases. Select durable
CPU profiles through `config.profile`: `transactional`, `analytical`, `document`,
`vector`, `time_series`, `high_durability` or `balanced`. Their names select
existing engine behavior; they do not all provide identical model operations,
SQL coverage or performance. Use the actual profile/model support matrix and
qualify the APIs the application uses.

For InMemory, combine the real site values with
`values-in-memory.yaml`. It sets `config.profile: in_memory`,
`runtime.allowVolatile: true`, `persistence.enabled: false` and
`persistence.devEmptyDir: true`. The pod is explicitly annotated as volatile:
database state is lost on process/pod restart. Durable guarantees, encrypted
at-rest storage and a PVC cannot be implied by this selection. The same image,
TLS, authorization and network/security controls still apply in production mode.
Setting `allowVolatile` for a durable profile or retaining a PVC under that flag
is rejected. Volatile DatabaseSpec provisioning is currently refused by `serve`;
the chart supports InMemory through its working flat-profile path.

Custom/product-profile knobs, object provisioning and encryption guarantees use
the existing `spec` ConfigMap workflow. `values-encrypted-spec.yaml` adds a
read-only 0440 key Secret, and the server receives the actual supported
`MULTIDB_ENCRYPTION_KEY_PATH` selector. The key contains 32 raw bytes or a
64-character hexadecimal value. Keep the correct key available on restart and
in backup/recovery custody. A Secret edit does not automatically re-encrypt data;
this CLI selector uses file-key mode, while envelope rotation remains a Rust API
operation. The chart does not invent automatic Vault/envelope rotation.

Tensor deployments use `values-gpu-nvidia.yaml`, `values-gpu-amd.yaml` or
`values-gpu-intel.yaml`, plus real site values. These set the Tensor profile,
vendor, extended resource requests/limits and vendor-node scheduling labels.
`runtimeClassName` can select an installed runtime; `supplementalGroups` supplies
actual render/video device GIDs while keeping UID10001, no root group, no extra
capabilities and no privileged/hostPath device bypass. Defaults request
`nvidia.com/gpu`, `amd.com/gpu` or `gpu.intel.com/i915`; a qualified plugin resource
such as Intel `gpu.intel.com/xe` can be selected through `gpu.resourceName` with
matching requests and limits. NVIDIA receives
`NVIDIA_DRIVER_CAPABILITIES=compute,utility,graphics` for Vulkan graphics access.

The host needs a real supported adapter, vendor device plugin, driver/Vulkan ICD,
container runtime or CDI setup and device permissions for the nonroot user. An
AMD compute plugin alone is not proof of usable Vulkan render-node access. The
chart never creates that hardware/driver stack or treats a node label as proof.
Tensor refuses to start without its supported GPU; it does not silently switch
the Tensor profile to CPU. GPU execution, numerical/device-loss/deadline tests
and performance remain blocked on a CPU-only cluster. See
[GPU container requirements](tensor-datasets-docker.md).

## Upgrade, storage migration and drains

The workload remains a Deployment to preserve the existing release API and
`persistence.existingClaim` path. Its strategy is **Recreate**, without surge:
normal controller upgrades terminate the old writer before creating the new one.
This intentionally trades update availability for single-writer storage safety.
There is no claim of zero-downtime database upgrade.

PVC names and the existingClaim value must remain stable across upgrades. The
generated PVC has `helm.sh/resource-policy: keep` by default, so uninstalling or
changing chart resources does not silently delete database storage. This also
means an operator must account for retained volumes. Never delete/recreate an
existing PVC to change access mode or storage class. For migration, verify an
offline backup, restore to a fresh reviewed target/claim, check full logical data
and schema, then switch only after operator RPO/RTO acceptance. Do not mount the
old claim in two active releases. Existing volumes must already have reviewed
ownership/permissions; do not recursively chown an unknown live data volume.

The default PDB has `minAvailable: 1` and permits eviction of unhealthy pods.
It protects a healthy single writer from accidental voluntary drain, and therefore
intentionally blocks a healthy node drain. It is not HA and does not govern
Deployment-controller replacement or force deletion. For approved maintenance:

1. Drain application traffic and prepare/verify the recovery point.
2. Explicitly suspend the PDB (`pdb.enabled: false` in approved maintenance
   values), stop the writer cleanly and confirm it no longer owns the database.
3. Perform the controlled upgrade/drain/storage work. For a failed or partitioned
   node, prove fencing/storage detach before replacing the writer; force-deleting
   an API object does not prove the old process stopped.
4. Wait for the replacement's readiness, verify acknowledged data, schema,
   authorization and TLS, then restore the PDB and application traffic.

Helm rollback changes manifests/images; it does not roll back the on-disk format.
Use the previous-version compatibility drill or a fresh verified restore. Preserve
failed-candidate evidence and account for newer acknowledged writes. The
[incident runbook](operations/deployment-and-incidents.md) covers this decision.

## Validation evidence

`scripts/ops-smoke.ps1` and `scripts/upgrade-smoke.ps1` require Helm and execute
real lint/template/schema/rendered-object checks through the Kubernetes
qualification tool. Missing tools are failures, not static-only success. Rendered
validation checks the safe default, existingClaim, spec, local-development and
large-capacity variants, every CPU profile, explicit InMemory, encrypted specs
and vendor-GPU renderings plus negative values. It does not prove CSI durability,
actual drain/restart behavior, CNI enforcement, production HA or an enterprise SLO.

Run the actual intended-cluster storage, TLS/header-spoofing, network-denial,
restore/upgrade/fencing, monitoring/alert and workload drills, then bind their
logs/configuration/artifact hashes into the release qualification evidence.
Unsupported multi-node/physical-GPU scenarios remain blocked until those actual
tests and integrations exist.

### Disposable local Kind runtime lab

[`kubernetes_runtime.py`](../scripts/qualification/kubernetes_runtime.py) owns a
new single-control-plane Kind cluster and private kubeconfig. It refuses an
existing cluster or mismatched node-container ownership and never changes a user
current-context. Pin and verify the node image and tools before use; kubectl and
the server must stay within the supported one-minor skew. The included fixture
pins Kubernetes1.35.8. A plan launches nothing:

```sh
python3 scripts/qualification/kubernetes_runtime.py \
  --output /private/new/kind-fixture --plan
```

Infrastructure preparation requires the pinned node image already present in
the local Docker daemon and enough measured disk reserve for node/image copies.
In a nested Docker environment, the lab uses Kind's native containerd snapshotter
when overlay-on-overlay mounts are unavailable. Its explicit
`--nested-kmsg-device` option creates only an actual missing `/dev/kmsg` character
device1:11 inside the owned node; it never substitutes `/dev/null` or console to
hide absent kernel data. Such infrastructure adaptations are recorded and do
not qualify production storage performance or failure behavior.

```sh
python3 scripts/qualification/kubernetes_runtime.py \
  --output /private/new/kind-fixture --tools /private/verified/tools \
  --prepare-cluster --nested-kmsg-device
```

After the final release executable and built Studio assets exist, stop other
builds/load generators and run the functional matrix against that owned fixture:

```sh
python3 scripts/qualification/kubernetes_runtime.py \
  --output /private/new/kind-fixture --tools /private/verified/tools \
  --runtime --binary /immutable/final/multidb --revision <source-commit> \
  --studio-dir /verified/build/studio
```

This builds the actual prebuilt image locally without pushing it, converts the
Docker archive into a checked OCI archive, records the real OCI **manifest**
digest rather than substituting a Docker config/image ID, imports it into Kind,
and pins the deployment to that digest. Each Pod's actual executable hash must
match the supplied release binary. The application namespace uses restricted
Pod Security admission and private generated credential/audit/encryption files.

Each CPU storage profile gets a separate result: authenticated typed writes and
complete readback, denied policy, verified pgwire TLS, observed clean SIGTERM
exit/container restart, and real Pod replacement followed by PVC readback.
InMemory must instead lose its fixture after each process/Pod restart. An
additional encrypted-key/reopen case uses the real mounted selector. These are
functional checks, not throughput/tail-latency or power-loss qualification.
Physical GPU, single-node HA, Kind kindnet enforcement and oversized capacity
remain explicitly blocked; no absent scenario is silently accepted.

Archive only the fixture's `evidence/` subtree. `private/` contains its privileged
kubeconfig and disposable keys/passwords and must not enter Git or a public
evidence bundle. Cleanup rechecks private ownership before deleting only that
cluster; it performs no Docker image/volume prune:

```sh
python3 scripts/qualification/kubernetes_runtime.py \
  --output /private/new/kind-fixture --tools /private/verified/tools --cleanup
```
