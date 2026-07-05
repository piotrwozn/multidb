# Analytics Template

Columnar analytics starter for events, metrics and aggregate scans.

## Why This Profile

Uses the stable analytics profile to make event logs, time-series and columnar aggregate intent explicit in DatabaseSpec.

## Generated Contract

- template: analytics
- profile: analytics_columnar
- validation_status: Experimental
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- events: role=event_log indexes=primary - Append-oriented raw facts for replay and CDC.
- metrics: role=time_series indexes=time_series - Timestamped measurements for rollups.
- aggregates: role=analytics indexes=columnar - Columnar aggregates for scans and reporting.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Known Limits

- columnar_layout is currently Experimental in the extension catalog.
- This starter is optimized for read-heavy analytical paths, not OLTP writes.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
