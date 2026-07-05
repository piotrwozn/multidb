# Phase 47 SDKs, Examples And Templates

Phase 47 turns the product configuration layer into usable starter paths. It
adds a checked-in template catalog, generated examples, smoke tests and SDK
guidance without creating a second configuration surface outside
`DatabaseSpec`.

## Implemented Contracts

- `templates` exposes `TemplateSpec`, `TemplateCollection`, `TemplateFile`,
  `TemplateMaterialization`, `built_in_templates`, `built_in_template` and
  `materialize_template`.
- Built-in templates cover:
  - `game-save` using `game_local_balanced`,
  - `desktop-embedded` using `desktop_app_embedded`,
  - `ai-memory` using `ai_agent_memory`,
  - `secure-saas` using `secure_app`,
  - `analytics` using `analytics_columnar`.
- Each template produces a `DatabaseSpec`, README, seed JSON and smoke script.
- Template specs use public profiles, collection roles, consistency domains,
  explicit extension refs, `GuaranteeValidator`, `PolicyCompiler` and
  `ConfigExplainer`.
- The CLI exposes:
  - `multidb template list`,
  - `multidb template explain <template>`,
  - `multidb init --guided --template <template> --name <name>`.
- `multidb init --guided --profile ...` remains unchanged and still writes a
  single spec file. Passing both `--profile` and `--template` is rejected.
- Checked-in examples live under `examples/` and match the same generator used
  by CLI template init.
- `scripts/templates-smoke.ps1` validates and explains every checked-in example,
  parses every seed file, then generates fresh copies under `target/` and
  repeats the same checks.

## Boundaries

Phase 47 is a product adoption layer, not a new runtime SDK or ORM. Rust
remains the first-class in-process API. Other languages should use the existing
PostgreSQL wire compatibility path documented by the ecosystem phases.

Template smoke tests validate configuration, explainability, file generation
and seed parseability. They do not create physical databases from
`DatabaseSpec`, because the current config apply contract remains
confirm/audit-only and does not run physical migrations.

The `analytics` template can validate with `Experimental` support status
because `columnar_layout` is still experimental in the extension catalog. The
template keeps that limitation visible in README and explain output.

Phase 45 closes the CP Cluster GA smoke contract. Phase 48 closes the public
preview packaging path with install, support matrix, known limits and preview
smoke.

## Acceptance Tests

Run:

```powershell
cargo test --lib templates -- --nocapture
cargo test --bin multidb template_ -- --nocapture
cargo test --bin multidb init_guided_template -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\templates-smoke.ps1
.\scripts\check.ps1
```
