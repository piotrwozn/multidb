# Phase 25 Canonical Value And Key Format

Phase 25 is `Complete` in `src/roadmap.rs`. It defines the durable value/key
compatibility contract used by the model, query, vector and replication layers.
Later GA gates may add evidence around packaging and operations, but this phase
document is no longer a `ProductionGap` marker.

Implemented in this phase:

- Canonical `Value` encoding rejects non-finite floats, over-deep values,
  oversized values and unknown codec versions.
- Object keys are canonicalized into deterministic order and negative zero is
  normalized.
- Legacy JSON values remain readable as a migration source, while new writes use
  the explicit binary codec version.
- Key encoding helpers provide length-prefixed prefix ranges and open-ended
  successor bounds for document, vector, relational and shard routing keys.
- Fuzz scaffolding covers value/key parsing surfaces outside the normal fast
  test gate.

Verification refreshed on 2026-07-04:

- `cargo test --lib codec -- --nocapture` covers canonical value behavior.
- `cargo test --lib keyenc -- --nocapture` covers prefix/successor key ranges.

Production notes:

- Format changes must update the Phase 24 format registry and add migration or
  restore guidance before new writes use a new durable version.
