# AI Memory Template

Vector memory plus document facts and replayable context.

## Why This Profile

Uses the stable AI memory profile because vector collections, document facts, event context and cache data are all part of its support catalog.

## Generated Contract

- template: ai-memory
- profile: ai_agent_memory
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- memories: role=vector_memory indexes=vector - Embedding memory for similarity lookup.
- facts: role=document_entity indexes=document - Structured facts and long-term notes.
- conversation_events: role=event_log indexes=primary - Replayable interaction history.
- scratch_cache: role=cache indexes=primary - Regenerable short-lived context.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Known Limits

- Embedding generation is intentionally outside the database.
- Vector indexing can lag writes; inspect explain config before treating results as strongly fresh.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
