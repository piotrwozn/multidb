# Phase 43 Extension Manifest And Marketplace Contract

Phase 43 makes extensions explicit product contracts. MultiDB now exposes a
first-class extension manifest catalog that describes provided runtime
capabilities, registry entries, compatibility, limitations, migrations and
Studio UI panels. The contract is marketplace-ready metadata, not a public
package marketplace or installer.

## Implemented Contracts

- `ExtensionManifest` is a public typed contract with:
  - `name`, `version`, `compatible_multidb` and support `status`,
  - `provides` for types, indexes, operators and storage strategies,
  - typed registries with required core capabilities,
  - `config_schema`, `limitations`, `migrations` and `ui_panels`,
  - `core_boundary` ownership for WAL, transactions, recovery, security and
    RBAC.
- `ExtensionManifestValidator` rejects manifests that leave required fields
  empty, duplicate registry/provide ids, register entries not declared in
  `provides`, reference undeclared capabilities, or claim ownership of core
  WAL/transaction/recovery/security/RBAC guarantees.
- Built-in manifests cover the existing extension capability catalog:
  `audit`, `cdc`, `columnar_layout`, `document_index`, `full_text`,
  `graph_index`, `time_series` and `vector_hnsw`.
- `compile_extension_catalog` deterministically projects manifests into
  manifest names, capabilities, registry ids and UI panel ids.
- `GuaranteeValidator` keeps unknown extension refs allowed but downgrades
  support to `Custom`, applies built-in manifest support status, and reports
  implicit built-in extensions required by collection roles or indexes.
- `/extensions` returns the previous summary fields plus each full manifest.
- Studio renders extension capabilities, registry entries, limitations,
  migrations and UI panels from `/extensions` manifests.

## Boundaries

- Phase 43 does not install external packages, execute marketplace purchases,
  load unsandboxed native plugins, or let extensions mutate WAL, recovery,
  transaction, security or RBAC state directly.
- `DatabaseSpec.extensions` remains a lightweight manifest reference. Full
  manifests are catalog metadata exposed by the control plane and validated by
  the manifest validator.
- Config apply remains the Phase 41 confirm/audit-only contract; extension
  installation and removal are still unsupported physical runtime changes in
  migration dry-runs.

## Acceptance Tests

- Built-in extension manifests validate and cover every extension capability.
- Manifest validation rejects core-boundary ownership and registry entries not
  declared in `provides`.
- Extension catalog compilation is deterministic.
- Guarantee validation reports implicit extension requirements and custom
  unknown extensions.
- Admin `/extensions` returns full manifest data.
- Studio renders manifest registry and UI panel data.
- Phase 43 is marked `Complete`; phase 44 owns Runtime Advisor V2, phase 45
  owns CP Cluster GA, phase 46 owns Performance Truth, phase 47 owns templates,
  and phase 48 owns public preview packaging.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --lib admin -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\studio-check.ps1
```
