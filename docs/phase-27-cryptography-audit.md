# Phase 27 Cryptography And Audit

Phase 27 is complete for the checked-in GA support matrix. It ships local
file-backed and Vault-dev KEK providers, envelope DEK rotation, crypto-shred,
and deterministic JSONL audit handoff. Vendor-specific HSM/KMS/SIEM delivery
clients are outside the GA matrix and must be provided by an operator adapter.

Implemented in this phase:

- `KeyProvider`/`KekProvider` abstractions cover static, file-backed, configured
  Vault-dev, and envelope key providers.
- `EnvelopeKeyProvider` supports DEK rotation, KEK rewrap, historical key reads
  by key id, and crypto-shred by destroying wrapped DEK material.
- `VaultKekProvider` reads a 32-byte KEK from a local Vault dev server over KV
  v2 (`secret/data/...`) using `VAULT_ADDR`, `VAULT_TOKEN`, and
  `MULTIDB_VAULT_KEK_PATH` style configuration.
- `KeyRotationPlan` and `CryptoShredReport` expose stable operator-facing
  rotation and shred summaries.
- Encrypted storage writes include key-version metadata, random v3 nonces and
  authenticated payloads; legacy v1 ciphertext remains readable.
- Audit events are sanitized, bounded, and can be chained with integrity hashes;
  memory and JSONL file sinks are available.

Verification refreshed on 2026-07-04:

- `cargo test --lib crypto_shred -- --nocapture` passes: engine-level rotation
  plan and crypto-shred behavior.
- `cargo test --lib vault_kek_provider -- --nocapture` passes: Vault KV v2
  request framing, token header, and KEK parsing through a local TCP loopback.
- `cargo test --lib envelope_key_provider -- --nocapture` passes: envelope DEK
  rotation and shred behavior.
- `cargo test --lib audit -- --nocapture` covers audit integrity, RBAC audit
  access and JSONL file sink behavior.

Production notes:

- The built-in Vault provider intentionally supports local dev HTTP Vault only.
  HTTPS, cloud-managed KMS, HSMs, and vendor SIEM push clients are rejected or
  left to operator adapters outside the checked-in GA matrix.
- The JSONL audit sink is the stable SIEM handoff format.
