# Secure SaaS Template

Security-focused transactional application state.

## Why This Profile

Uses the stable secure_app profile with strong CP intent, backup, PITR, encryption and audit enabled while keeping enterprise SLA concerns explicit.

## Generated Contract

- template: secure-saas
- profile: secure_app
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (strong_cp)
- replication: Cp

## Collections

- tenants: role=document_entity indexes=document - Tenant metadata and account state.
- app_state: role=key_value indexes=primary - Transactional application control records.
- audit_log: role=audit indexes=primary - Security audit trail owned by the core audit path.
- outbox: role=event_log indexes=primary - Durable integration events for external delivery.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Known Limits

- The template does not configure Kubernetes automation, multi-region placement or enterprise SLA.
- Physical config apply remains confirm/audit-only in v1.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
