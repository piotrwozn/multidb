# Phase 14 Operations GA

Phase 14 is complete for the checked-in GA support matrix.

Operator artifacts:

- `ops/kind/multidb-kind.yaml` defines a three-node local kind cluster with
  admin and pgwire port mappings.
- `ops/helm/multidb` provides a Helm chart with ConfigMap-driven local
  profile, CP replication, audit JSONL path, Vault address, and MinIO backup
  target.
- The Deployment template includes liveness and readiness probes, rolling
  update settings, and explicit downgrade rejection/rollback annotations.
- `ops/vault/dev-policy.hcl` grants local Vault dev access for KEK storage and
  transit encrypt/decrypt/rewrap paths.
- `ops/minio/backup-target.env.example` documents the local MinIO backup/PITR
  target without cloud credentials.

Verification:

- `scripts/ops-smoke.ps1` checks all operator artifacts and required
  readiness/liveness, Vault, MinIO, and audit JSONL tokens.
- `scripts/upgrade-smoke.ps1` checks rolling upgrade and downgrade rejection
  annotations and runs `helm lint` when Helm is installed.
- `scripts/check.ps1` invokes both smoke scripts after Rust and supply-chain
  checks.

Support boundaries:

- The chart is a local/kind GA artifact, not a managed cloud operator.
- `/config/apply` remains confirm/audit-only and never performs physical data
  migration.
- Cloud credentials, vendor SIEM push clients, and managed KMS/HSM adapters are
  outside this repository's default GA matrix.
