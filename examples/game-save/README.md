# Game Save Template

Local-first save data for games and lightweight embedded state.

## Why This Profile

Uses the certified local snapshot profile so game state remains simple, fast and explicit about not claiming cross-node quorum semantics.

## Generated Contract

- template: game-save
- profile: game_local_balanced
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- saves: role=document_entity indexes=document - Versioned save slots and game-world snapshots.
- player_state: role=key_value indexes=primary - Fast direct lookup for the current player profile.
- session_events: role=event_log indexes=primary - Replayable local events for debugging and sync bridges.
- asset_cache: role=cache indexes=primary - Regenerable cache records safe to rebuild.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Known Limits

- No cross-device sync is configured by default.
- Cache data must be safe to rebuild.
- Use the generated event log as an integration boundary rather than as a remote replication claim.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
