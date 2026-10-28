# Game Save Template

Local-first save data for games and lightweight embedded state.

## Why This Profile

Uses the certified local snapshot profile so game state remains simple, fast and explicit about not claiming cross-node quorum semantics.

## Generated Contract

- template: game-save
- spec_version: 2
- profile: game_local_balanced
- validation_status: Stable
- validation_valid: true
- consistency_domain: primary (local_snapshot)
- replication: Cp

## Collections

- saves: role=document_entity indexes=document - Versioned save slots and game-world snapshots.
  - physical: `{"document_indexes":[{"path":"slot"}]}`
- player_state: role=key_value indexes=primary - Fast direct lookup for the current player profile.
- session_events: role=event_log indexes=primary - Replayable local events for debugging and sync bridges.
- asset_cache: role=cache indexes=primary - Regenerable cache records safe to rebuild.

## Validate And Explain

```powershell
.\smoke.ps1
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml
```

## Provision

This spec is version 2: it carries the physical parameters provisioning needs to build
every collection (see `docs/provisioning.md`).

`multidb init --guided --template game-save --name <name> --out <dir> --apply` writes
these files and creates the database in one step. From this directory:

```powershell
multidb config plan --spec .\multidb.yaml --db .\arcade_save.redb
multidb config apply --spec .\multidb.yaml --db .\arcade_save.redb --auto-approve
multidb config drift --spec .\multidb.yaml --db .\arcade_save.redb
```

`config plan` is a dry run. `config apply --auto-approve` applies a plan that only
creates, adopts or records, and a second apply is a noop. `config drift` exits 3
when the database no longer matches the spec.

Guarantee statuses a CLI plan reports for this spec:

- `guarantees.audit.enabled`: `enforced` with `MULTIDB_AUDIT_KEY_PATH`; without it,
  local CLI usage reports `enforced_with_development_key` (`AUDIT_DEVELOPMENT_KEY`).
  Production `multidb serve` requires an existing private audit key and, for durable
  profiles, `MULTIDB_AUDIT_ANCHOR_PATH`. The Control Plane inherits these runtime
  settings. Existing audit history requires its original signing key; changing the
  path does not re-sign it. Follow the repository's `docs/docker.md` audit setup.
- `guarantees.backup.enabled` and `guarantees.backup.pitr`: `not_requested`.
- `guarantees.encryption.at_rest`: `not_requested`.

## Walkthrough From A Fresh Checkout

This example demonstrates a local-first game save profile with document saves,
key-value player state, replayable session events and a rebuildable asset cache.
It does not require production credentials or an external database. The spec is
version 2: `saves` carries the document index path (`physical.document_indexes`)
provisioning needs to build the collection, so the database can be created from
the spec.

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
.\target\debug\multidb config plan --spec .\examples\game-save\multidb.yaml --db .\target\arcade_save.redb
Get-Content .\examples\game-save\seed.json -Raw | ConvertFrom-Json
```

`config plan` is a dry run: it prints the steps (create the database, then the
four collections), reports `outcome=ready auto_approvable=true` and creates no
file.

To build the database, generate the template with `--apply`:

```powershell
.\target\debug\multidb init --guided --template game-save --name "Arcade Save" --out .\target\game-save-demo --apply
.\target\debug\multidb config drift --spec .\target\game-save-demo\multidb.yaml --db .\target\game-save-demo\arcade_save.redb
.\target\debug\multidb config status --db .\target\game-save-demo\arcade_save.redb
```

`init --apply` writes the four files, then creates the database. game-save
leaves the database path to the app (`storage_path: null`), so `init --apply`
uses `<out dir>/<sanitized name>.redb` and prints `database path=...` unless
`--db` names another path. `config drift` exits 0 while the database matches
the spec, and `config status` prints the provision record.

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
- `init --apply` exits 2 with `already_initialized`: a database already exists at
  the chosen path. Pick another `--out` or `--db`, or converge the existing
  database with `config plan --spec` and `config apply --spec --confirm`.

## Known Limits

- No cross-device sync is configured by default.
- Cache data must be safe to rebuild.
- Use the generated event log as an integration boundary rather than as a remote replication claim.
- The app chooses the database path; init --apply creates <out dir>/<name>.redb unless --db is given.

Other languages should connect through MultiDB's PostgreSQL wire compatibility; the Rust API remains the first-class SDK surface for this starter.
