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

## Walkthrough From A Fresh Checkout

This example demonstrates a local-first game save profile with document saves,
key-value player state, replayable session events and a rebuildable asset cache.
It does not require production credentials or an external database.

From a fresh checkout, run:

```powershell
Push-Location examples\game-save
.\smoke.ps1
Pop-Location
```

The smoke script builds the `multidb` CLI if `target\debug\multidb` is not
already present, validates `multidb.yaml`, runs `config explain --json`, and
parses `seed.json`. A successful run ends with:

```text
template smoke ok: multidb.yaml
```

To inspect the template manually after the smoke passes:

```powershell
.\target\debug\multidb config validate --spec .\examples\game-save\multidb.yaml
.\target\debug\multidb config explain --spec .\examples\game-save\multidb.yaml
Get-Content .\examples\game-save\seed.json -Raw | ConvertFrom-Json
```

Read these files next:

- `multidb.yaml` - the generated database contract for the template.
- `seed.json` - example records for save slots, player state and session events.
- `smoke.ps1` - the copy-pasteable validation path used by the repository.

Common local failures:

- `cargo` is not found: install the Rust toolchain, then rerun `.\smoke.ps1`.
- PowerShell cannot run the script: start PowerShell from the repository root and
  run `Set-ExecutionPolicy -Scope Process Bypass` for the current shell only.
- The binary is stale after pulling new changes: remove `target\debug\multidb`
  and rerun `.\smoke.ps1` so Cargo rebuilds it.

## Known Limits

- No cross-device sync is configured by default.
- Cache data must be safe to rebuild.
- Use the generated event log as an integration boundary rather than as a remote replication claim.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
