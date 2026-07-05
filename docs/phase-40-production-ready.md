# Phase 40 CLI Product Layer

Phase 40 turns the configuration contracts from phases 36-39 into a usable CLI
product layer. The CLI can create a conservative starter spec, show the public
catalog, validate and explain JSON/YAML specs, and write migration dry-run plans
for automation.

## Implemented Contracts

- `multidb init --guided --profile <profile> --name <name>` writes a
  `DatabaseSpec` file without opening or mutating a database.
- Guided init resolves built-in profile aliases, writes YAML by default, writes
  JSON with `--format json`, and refuses to overwrite an existing file unless
  `--force` is passed.
- `multidb profile list`, `multidb role list`, and `multidb domain list` expose
  the phase 37 catalog in text and JSON.
- `multidb config validate`, `multidb config explain`, and
  `multidb config plan` accept `.json`, `.yaml`, and `.yml` specs.
- `multidb explain config` is an alias for `multidb config explain`.
- `--json` is a shorthand for `--output json` on product-layer commands.
- `multidb config plan --out <plan.json>` writes a JSON `MigrationPlan`
  artifact. `config apply` continues to accept JSON plan files only.

## Boundaries

The CLI remains a thin product layer over `config_spec`. It does not duplicate
guarantee validation, policy compilation, explain logic, or migration planning.

Guided init is flag-driven rather than an interactive TUI. Phase 41 now owns
the authenticated Control Plane HTTP APIs. Phase 42 adds Studio, while SDK
templates, marketplace behavior, and physical online data migration remain
later phase work.

## Acceptance Tests

The phase is covered by focused binary tests and roadmap tests:

- YAML validate, explain, and plan inputs are accepted.
- Guided init writes a valid YAML or JSON spec and protects existing files.
- Catalog list commands produce stable text and parseable JSON.
- `--json` matches `--output json`.
- `explain config` works as an alias.
- `config plan --out` writes a JSON plan artifact.
- Unknown guided profiles return an actionable `profile list` suggestion.
- Phase 40 is marked `Complete`; phase 41 adds the HTTP control plane, phase 42
  adds Studio, phase 43 adds extension manifests, phase 44 adds Runtime Advisor
  V2, phase 45 adds CP Cluster GA, phase 46 adds Performance Truth, phase 47
  adds templates, and phase 48 adds public preview packaging.

Run:

```powershell
cargo test --bin multidb config_ -- --nocapture
cargo test --bin multidb init_ -- --nocapture
cargo test --bin multidb catalog_ -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
