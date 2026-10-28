# Profile selection examples

Combine exactly one execution/storage selection with reviewed production image,
secret, network and capacity values. All commands use the same single-writer
chart. These are configuration examples, not measured SLOs or hardware evidence.

| Storage profile | `--set config.profile=...` | State/execution |
| --- | --- | --- |
| Transactional | `transactional` | Durable row storage, CPU query execution |
| Analytical | `analytical` | Durable columnar storage, CPU analytics |
| Document | `document` | Durable document-oriented preset, CPU |
| Vector | `vector` | Durable vector-oriented preset, CPU ANN/model operations |
| TimeSeries | `time_series` | Durable time-series-oriented preset, CPU |
| HighDurability | `high_durability` | Durable two-phase local transaction preset, CPU; not live cluster HA |
| Balanced | `balanced` | Durable balanced preset, CPU |
| InMemory | Use `-f values-in-memory.yaml` | Explicitly volatile RAM; no PVC/data-survival promise |
| Tensor | Use `-f values-gpu-<vendor>.yaml` | Durable tensors; actual supported GPU required, no Tensor-profile CPU fallback |

The storage spellings and aliases are those parsed by `src/bin/multidb.rs`.
Product slugs also accepted by that parser are `game_local_balanced`,
`desktop_app_embedded`, `ai_agent_memory`, `secure_app`, `production_cp`,
`analytics_columnar` and `tensor_dataset`; snake/kebab aliases include
`in_memory`, `high_durability`, `time_series` and `deeplake_like`. The flat CLI
uses their storage-profile selection. Use a DatabaseSpec for custom knobs,
guarantees, objects, policy and explicit product-profile binding. A slug such as
`production_cp` does not activate a live cluster in flat serve. `auto` requires
an existing initialized durable database and never creates an empty one.

Every production example also needs the `multidb-audit` Secret (a private real
32-byte or64-hex signing key). Durable instances keep a separate anchor file on
the data PVC. InMemory uses the key but omits a persistent anchor, matching its
explicitly volatile audit history. Local-development may retain its development
signer; that is not production key custody.

For example, a Document instance with a real site-values file:

```sh
helm template documents ops/helm/multidb -f /private/site-values.yaml \
  --set config.profile=document
```

For spec-defined durable encrypted storage, use the existing reviewed secure-SaaS
spec as the starting point and create its ConfigMap. The key file must contain
32 raw bytes or a 64-character hexadecimal key. Preserve it through the approved
custody/recovery procedure:

```sh
kubectl -n database create configmap multidb-spec \
  --from-file=multidb.yaml=examples/secure-saas/multidb.yaml
kubectl -n database create secret generic multidb-encryption \
  --from-file=key=/private/database-encryption.key
helm template encrypted ops/helm/multidb -f /private/site-values.yaml \
  -f ops/helm/multidb/values-encrypted-spec.yaml
```

`spec.apply: create` initializes an absent database; use `verify` after creating
it, or the documented additive workflow for approved additive changes. The chart
keeps `runtime.dbPath` explicit. The file-key selector does not perform automatic
envelope/key rotation and does not re-encrypt old data when a Secret changes.
Volatile spec provisioning is currently rejected by the actual server; the
supported volatile deployment uses the explicit flat InMemory example.

GPU vendor values request one resource advertised by the appropriate device
plugin: `nvidia.com/gpu`, `amd.com/gpu` or `gpu.intel.com/i915`. Intel platforms
advertising `gpu.intel.com/xe`, or another qualified vendor resource, can set
`gpu.resourceName` and matching requests/limits explicitly. `runtimeClassName`
selects an installed RuntimeClass when needed. `supplementalGroups` must contain
the actual node render/video device GIDs, never a guessed universal GID or root.
The chart never adds privileged mode, hostPath device mounts or capabilities.
NVIDIA sets `NVIDIA_DRIVER_CAPABILITIES=compute,utility,graphics` for Vulkan.

Install and qualify the real host driver, device plugin, container runtime/CDI,
Vulkan ICD and device permissions first. Vendor node labels are scheduling
requirements that operators must supply; labels are not physical-device proof.
On a CPU-only node the Tensor profile must fail, and Kind cannot qualify GPU
execution. See [the Kubernetes contract](../../../../docs/kubernetes.md) and
[GPU container requirements](../../../../docs/tensor-datasets-docker.md).
