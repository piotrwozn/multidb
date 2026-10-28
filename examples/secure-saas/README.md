# Secure SaaS Template

Security-focused transactional application state.

## Why This Profile

Uses the stable secure_app profile with strong CP intent, backup, PITR, encryption and audit enabled while keeping enterprise SLA concerns explicit.

## Generated Contract

- template: secure-saas
- spec_version: 2
- profile: secure_app
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (strong_cp)
- replication: Cp

## Collections

- tenants: role=document_entity indexes=document - Tenant metadata and account state.
  - physical: `{"document_indexes":[{"path":"tenant_id"}]}`
- app_state: role=key_value indexes=primary - Transactional application control records.
- audit_log: role=audit indexes=primary - Security audit trail owned by the core audit path.
- outbox: role=event_log indexes=primary - Durable integration events for external delivery.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Provision

This spec is version 2: it carries the physical parameters provisioning needs to build
every collection (see `docs/provisioning.md`).

It requests encryption at rest, which this build provisions through the Rust API
only: `config plan --spec` on the CLI is blocked with `GUARANTEE_NOT_ENFORCEABLE`,
and `init --apply`, `config apply --spec`, `multidb serve` spec mode and the Control
Plane refuse it. Create the database with `provision::provision_new_database` on the
`rust_api` surface, with `ProvisionEnvironment.encryption` set to
`EncryptionConfig::local_envelope(<keyring>, <kek>)`. The dry run shows the reason:

```powershell
multidb config plan --spec .\multidb.yaml --db .\secure_saas.redb
```

Guarantee statuses a CLI plan reports for this spec:

- `guarantees.audit.enabled`: `enforced` with `MULTIDB_AUDIT_KEY_PATH`; without it,
  local CLI usage reports `enforced_with_development_key` (`AUDIT_DEVELOPMENT_KEY`).
  Production `multidb serve` requires an existing private audit key and, for durable
  profiles, `MULTIDB_AUDIT_ANCHOR_PATH`. The Control Plane inherits these runtime
  settings. Existing audit history requires its original signing key; changing the
  path does not re-sign it. Follow the repository's `docs/docker.md` audit setup.
- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `operator_managed`. This
  build has no backup scheduler, so the plan prints the backup and restore runbook
  (`GUARANTEE_OPERATOR_MANAGED`).
- `guarantees.encryption.at_rest`: `not_enforceable` on the CLI, `enforced` on the Rust API with a `LocalEnvelope` key.

## Known Limits

- The template does not configure Kubernetes automation, multi-region placement or enterprise SLA.
- Encryption at rest is provisioned through the Rust API only: init --apply, config apply --spec, multidb serve spec mode and the Control Plane refuse this spec with GUARANTEE_NOT_ENFORCEABLE.
- Provisioning creates and adopts objects but never drops, alters or re-encrypts them; config apply --plan stays confirm/audit-only.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
