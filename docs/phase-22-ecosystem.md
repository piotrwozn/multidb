# Phase 22 Ecosystem Compatibility

Phase 22 makes multidb easier to use from existing PostgreSQL tooling without
claiming full PostgreSQL compatibility.

## Supported Core

- PostgreSQL wire protocol remains the primary external protocol.
- Minimal `pg_catalog` and `information_schema` are exposed for client
  introspection.
- `version()` reports `14.0-multidb`.
- SQLSTATE mapping is stable for authorization, conflicts, missing objects,
  duplicate primary keys, invalid input, unsupported features, and storage
  corruption.
- CSV and JSONL import/export are available from the CLI and library.
- Mongo BSON import maps documents into multidb `Value::Object`.

## Known Limits

- Mongo wire protocol is deferred.
- Full PostgreSQL catalogs, triggers, rules, sequences, and procedural
  languages are not part of phase 22.
- Mongo `Decimal128` is imported as text with a warning to preserve precision.
- Parquet export is available through the library stack, but CLI parquet export
  is reserved for a later hardening pass.

## Smoke Example

```powershell
multidb admin status --db .\app.redb --profile transactional
multidb export jsonl --db .\app.redb --profile transactional --table users --file users.jsonl
multidb import jsonl --db .\app.redb --profile transactional --table users --file users.jsonl
```
