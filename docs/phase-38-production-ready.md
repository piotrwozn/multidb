# Phase 38 Guarantee Validator And Policy Compiler

Phase 38 makes `DatabaseSpec` enforceable. It adds a product-level guarantee
contract, a validator that blocks impossible promises, a deterministic compiler
that produces a runtime policy plan, and a first CLI entrypoint for validation.

## Implemented Contracts

- `DatabaseSpec` v1 now includes `guarantees`, collection `indexes`, and
  extension `stability`.
- `GuaranteeValidator::validate` returns a `ValidationReport` with:
  `valid`, support `status`, stable issue codes, severity, field path,
  operator-facing message, repair suggestion, and certification impact.
- Hard validation errors block:
  - strong CP with local write acknowledgement,
  - AP/eventual consistency without conflict resolution,
  - `production_cp` without backup,
  - sensitive data without encryption at rest,
  - vector collections without a vector index,
  - graph collections without a graph index,
  - audit collections while audit is disabled,
  - strict cross-domain transactions spanning CP and AP domains.
- Warnings keep the report valid but lower support confidence for custom
  profiles, roles outside a profile certification matrix, and experimental
  extensions.
- `PolicyCompiler::compile` validates first and returns a deterministic
  `CompiledPolicy` only for valid specs.
- `multidb config validate --spec <json> [--output text|json]` validates JSON
  specs. It exits `0` for valid reports and `2` when guarantee validation
  rejects the spec.

## Boundaries

Phase 38 intentionally accepts JSON only. YAML parsing, guided config creation,
explain reports, migration dry-run, and apply workflows remain phase 39-40 work.

The compiler is a pure planning contract. It does not open storage, mutate a
database, create indexes, install extensions, or tune a running system.

## Acceptance Tests

The phase is covered by focused unit tests in `config_spec`, CLI tests in the
`multidb` binary, and roadmap tests:

- full matrix coverage for hard guarantee conflicts,
- every hard error carries a field path and repair suggestion,
- contradictory specs cannot report `Certified`,
- policy compilation is deterministic and rejects invalid specs,
- JSON schema snapshot covers guarantees, collection indexes and extension
  stability,
- CLI validation covers valid JSON, invalid JSON with exit code `2`, JSON
  output, and YAML rejection,
- phase 38 is `Complete`; later phase statuses are tracked individually by
  `src/roadmap.rs`.

Run:

```powershell
cargo test --lib config_spec -- --nocapture
cargo test --bin multidb config_validate -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\check.ps1
```
