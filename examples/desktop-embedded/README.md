# Desktop Embedded Template

Durable embedded storage for desktop applications.

## Why This Profile

Uses the certified embedded desktop profile to keep local durability, document data and audit visibility in one validated spec.

## Generated Contract

- template: desktop-embedded
- profile: desktop_app_embedded
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- documents: role=document_entity indexes=document - Durable user documents and settings payloads.
- settings: role=key_value indexes=primary - Application preferences addressed by stable keys.
- audit_log: role=audit indexes=primary - Tamper-evident local operator and sync audit records.
- usage_metrics: role=time_series indexes=time_series - Local time-series telemetry with bounded retention.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Known Limits

- The template is local-first and does not configure remote cluster membership.
- Application migrations still go through config plan and operator review.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
