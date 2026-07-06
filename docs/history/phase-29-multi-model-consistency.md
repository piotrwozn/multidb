# Phase 29 Multi-Model Consistency And Safe Extensibility

Phase 29 hardens the cross-model catalog and extension boundaries above the
core relational/document/vector APIs. It is `Complete` in `src/roadmap.rs`;
external conformance matrices for every model remain future ecosystem evidence,
not an open status marker for this phase.

Implemented in this phase:

- Catalog validation rejects duplicate ids and invalid model object names.
- Phase 19 helper parsing uses SQL AST boundaries instead of substring matching
  for graph/time-series/geo helper calls.
- Vector handles share fresh index state after writes.
- Time-series and graph writes trigger registered hooks.
- Changefeed reads through `changefeed_as` apply masking policy for the caller.
- WASM sandbox limits continue to guard extension execution.

Verification refreshed on 2026-07-04:

- `cargo test --lib phase29 -- --nocapture` covers catalog validation, parser
  boundaries, vector state freshness, hook firing and masked changefeeds.

Production notes:

- Full Cypher/GQL, OGC polygon coverage and external model-specific conformance
  matrices remain outside this in-crate phase contract.
