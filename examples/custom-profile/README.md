# Custom Profile Example

A version 2 `DatabaseSpec` with a `custom_profile`: `orders_durable` extends
the `high_durability` variant of `secure_app` (two-phase commit) and overrides
compression, cache, query and history knobs, raises the audit and backup
floors and adds the `time_series` role.

The example is for validation and explain: its `orders` collection has a
`document` index without `physical.document_indexes`, so a provisioning plan
for it (`multidb config plan --spec .\multidb.yaml --db .\orders.redb`) is
blocked with `PHYSICAL_PARAMETER_REQUIRED`. `examples/provisioning` is a spec
that provisioning builds end to end.

## Validate And Explain

```powershell
multidb config validate --spec .\multidb.yaml
multidb config explain --spec .\multidb.yaml --json
```

Validation reports `valid=true status=Custom` with Advice only:
`CUSTOM_PROFILE_DEFINED` lists the overridden knobs and
`PROFILE_FORMAT_REQUIRES_NEW_BINARY` notes that zstd compression means value
framing (`schema_version` 2). Explain reports one `$.profile.knobs.<path>`
decision per knob with its value, source (`preset:secure_app`,
`variant:high_durability` or `override`) and what applies it.

See `docs/profiles.md` for the knob reference and the validation codes.
