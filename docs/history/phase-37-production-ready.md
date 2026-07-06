# Phase 37 Profiles, Roles And Consistency Domains

Phase 37 turns the `DatabaseSpec` v1 strings and enums from phase 36 into a
public product catalog. The catalog names the built-in use-case profiles,
collection roles, consistency domains and support statuses that validators, CLI
commands, control-plane APIs and Studio views can share.

## Implemented Contracts

- Public support statuses are available as `SupportStatus`:
  `Certified`, `Stable`, `Experimental`, `Custom`, and `Invalid`.
- Built-in profile metadata is available through `built_in_profiles()` and
  `built_in_profile(...)`.
  - Product slugs: `game_local_balanced`, `desktop_app_embedded`,
    `ai_agent_memory`, `secure_app`, `production_cp`, and
    `analytics_columnar`.
  - Compatibility aliases preserve phase 36 imports from technical
    `DbConfig` slugs such as `balanced`, `vector`, `transactional`,
    `high_durability`, `analytical`, and `time_series`.
- Collection role metadata is available through
  `collection_role_definitions()` and `collection_role_definition(...)`.
  The catalog covers `document_entity`, `key_value`, `event_log`,
  `vector_memory`, `cache`, `audit`, `graph`, `analytics`, and `time_series`.
- Consistency domain metadata is available through
  `consistency_domain_definitions()` and `consistency_domain_definition(...)`.
  The catalog covers `local_snapshot`, `strong_cp`, and `eventual_ap`.
- `DatabaseSpec::catalog_support_status()` derives a catalog status without
  mutating storage or changing the JSON schema:
  - structurally invalid specs are `Invalid`,
  - unknown profiles and profile/role combinations outside the built-in matrix
    are `Custom`,
  - otherwise the result is the weakest status across the selected profile,
    roles and domains.

## Boundaries

Phase 37 does not parse YAML, explain configuration decisions, or plan
migrations. Phase 38 now rejects dangerous guarantee combinations and compiles a
runtime policy; phases 39-40 continue with explain, dry-run and guided CLI
workflows.

The catalog is intentionally descriptive. `production_cp` is now `Stable`
because Phase 45 closes the local/process CP OpenRaft Cluster GA smoke gate;
Kubernetes automation and enterprise SLA remain separate concerns.

## Acceptance Tests

The phase is covered by focused unit tests in `config_spec` and `roadmap`:

- catalog completeness for six profiles, nine roles and three domains,
- unique profile slugs and aliases,
- legacy technical profile aliases resolving into the product catalog,
- `Certified`, `Stable`, `Custom`, `Invalid`, and `Experimental` status derivation,
- phase 37 marked `Complete`; later phase statuses are tracked individually by
  `src/roadmap.rs`.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
