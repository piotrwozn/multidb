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

Verification refreshed on 2026-10-16:

- `cargo test --lib crypto_shred -- --nocapture` passes: engine-level rotation
  plan, crypto-shred refusal for current and in-use keys, and the
  rotate, re-encrypt, then shred flow.
- `cargo test --lib reencrypt -- --nocapture` passes: batched re-encryption of
  every table (including metadata, MVCC and commit-log tables), reopen after
  shred, and the compressed envelope engine path.
- `cargo test --lib vault_kek_provider -- --nocapture` passes: Vault KV v2
  request framing, token header, and KEK parsing through a local TCP loopback.
- `cargo test --lib envelope_key_provider -- --nocapture` passes: envelope DEK
  rotation and shred behavior, current/last key refusal, atomic keyring
  persistence, in-process KEK rotation, destroyed-key cache eviction, and the
  missing-keyring error.
- `cargo test --lib audit -- --nocapture` covers audit integrity, RBAC audit
  access and JSONL file sink behavior.

Production notes:

- The built-in Vault provider intentionally supports local dev HTTP Vault only.
  HTTPS, cloud-managed KMS, HSMs, and vendor SIEM push clients are rejected or
  left to operator adapters outside the checked-in GA matrix.
- The JSONL audit sink is the stable SIEM handoff format.

Key lifecycle rules:

- Rotate, then re-encrypt, then destroy: `rotate_dek` only changes the key used
  for new writes. Run `reencrypt_existing_values` until it reports zero
  re-encrypted values before destroying an old DEK.
- `destroy_dek`/`crypto_shred` refuse the current key, the last live key, and
  any key that still protects stored values. Crypto-shred therefore protects
  residual copies (old backups, freed redb pages, replicas); it does not erase
  live data in place. Backups taken under a destroyed key become unreadable.
- Rotate the KEK through the same provider instance the database uses, or while
  the database is closed; other provider instances keep the old KEK.
- Opening a database that already contains data with a missing envelope keyring
  is a hard error. A keyring is only created for an empty database.
