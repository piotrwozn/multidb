# Backup and restore safety

Full and incremental backup build their complete payload and manifest inside
an owner-only `.partial-<uuid>` directory. Each file and the containing
directory are synced before the directory is renamed to the final backup id.
Publication uses a stable `.publish.lock` file to serialize cooperating writers
and refuses an occupied final path. Keep the backup root protected from
unrelated filesystem changes; the lock is not a sandbox against another process
that ignores it. An ordinary failure removes staging. An abrupt process exit
can leave a `.partial-<uuid>` directory, which `list_backups` ignores. Inspect
and remove such an orphan only after confirming its writer is stopped.
Every other backup directory must have a valid manifest: deleting a manifest
from a published backup is reported as an error. This intentionally stops
silently treating lost published metadata as an unfinished backup attempt.
A directory-sync error after rename can leave a complete final backup despite
an error result, with uncertain durability of the new directory entry.
Backup/restore directory creation also syncs ancestor entries, so newly created
parent directories are covered. Retries sync existing ancestors as well because
a failed earlier attempt may have left them visible without a successful sync.

Restore writes a private staging database beside the destination, replays the
validated chain in one storage transaction, closes and syncs the database, then
publishes it without replacing another path. Failures during validation or
replay remove staging and leave no target database. If another process creates
the destination meanwhile, restore preserves that file and fails. A directory
sync failure after publication is an error with an uncertain durability outcome;
the published file already contains the complete restore.

The target must be **absent**. Existing databases, empty files, directories and
symlinks, including dangling symlinks, are rejected. This intentionally tightens
the previous empty-or-absent contract to prevent overwrite races. For tools that
reserve a destination using an empty temporary file, choose a fresh filename
inside a private temporary directory instead. Restore into an in-memory profile
is rejected because the API returns a report rather than a live database handle.

On Unix, new database files, backup payloads, manifests and keyrings are created
with owner-only file permissions, and backup/staging/verification directories
with owner-only directory permissions. Existing database file permissions are
left to the operator. Backup verification may temporarily contain decrypted
values even for an encrypted backup; its drill now stays inside a private
directory removed on success or error. Restore creates a plaintext database by
default. Optional target encryption also encrypts staging from its first value
write. Source backup encryption and target database encryption use separate
settings and can use different keys. A configured, authenticated encrypted
target meets `floors.encryption_at_rest`; other profile-floor rules and the
explicit unmet-floor opt-in remain in force.

Full backup persists a stable source timeline in internal maintenance metadata.
Incremental backup refuses a parent belonging to another source timeline.
Legacy backup chains can still be restored, but taking further incrementals
from a source without persisted timeline metadata requires a new full backup.
The manifest's optional `end_commit_hash` also anchors the parent's last commit,
so an incremental refuses a diverged restored branch even when it shares the
timeline. If GC removed that anchor, start a new full backup.
Keep a new full backup after history/commit-log truncation as before.
Restore validates any recorded full/incremental end-commit anchors against their
payloads, including a no-op incremental's parent. Historical manifests without
the optional hash retain checksum, chain and LSN validation. The hashes detect
inconsistency; they do not authenticate an untrusted manifest.

Time recovery selects a prefix of the commit log and stops at the first commit
whose recorded wall-clock time exceeds the requested cutoff. If a system clock
moves backwards, a later timestamp cannot cause restore to include that future
commit. LSN recovery gives the exact commit boundary without relying on wall
clock ordering. AP replication supports full snapshot recovery; it has no
incremental/PITR history.

Manifest payload paths must be relative regular files inside the backup
directory; path traversal, symlink payloads and symlink parent-backup directories
are refused. Backup files and their directory tree must remain immutable while
restore or verification reads them. Checksums detect accidental corruption;
plaintext manifests/checksums are not an authenticated provenance signature.

Restore and verification default to a 1 GiB limit for each stored and
decompressed payload. The decoder reads at most the limit plus one byte before
refusing oversized input, and its zstd history window is capped at 128 MiB.
Manifests are limited to 1 MiB. Large trusted backups can explicitly increase
the payload budget with
`RestoreConfig::default().with_max_payload_bytes(bytes)`; zero is invalid.
The `restore` and `verify` CLI commands accept `--max-payload-bytes <bytes>`.
Full payloads are still buffered in memory, so choose the override with enough
RAM for decoded JSON, records and one incremental payload, rather than assuming
streaming restore. The 1 GiB default is an intentional compatibility limit.

Opening an encrypted restore never creates a replacement keyring when its
keyring is missing. New envelope keyring creation uses exclusive publication.
Backup encryption inherited from a running encrypted database likewise opens
and authenticates its existing keyring; cached source keys cannot cause backup
to silently recreate a lost keyring. An explicitly configured alternate
`BackupConfig.encryption` retains its separate key-provisioning workflow.
Independent providers cannot overwrite a keyring changed by another provider:
key maintenance uses a stable sidecar file lock and checks the expected state.
Reopen a stale provider before retrying key maintenance. The sidecar `.lock`
file is not a key; retain the actual keyring and KEK. Coordinate all providers
before DEK destruction because providers that already hold a key can retain it
in memory, and other databases/backups may still require it.
Retain the audit signing key separately as well: logical backups preserve signed
audit history but do not contain signing secrets or process-local key paths.
Payload/chain verification does not substitute for audit-MAC verification with
that key. Production `serve` checks the restored history before admitting writes.
For an authorized PITR, preserve the original database and latest anchor as
evidence, restore to a fresh database path and choose a separate new anchor
path for the recovered history. Do not overwrite the live/latest anchor with an
older prefix or attempt to re-sign history with a different audit key.
If keyring maintenance encounters a persistence error, that provider and all its
clones refuse further key operations until reopened. A failed directory sync
can follow a visible rename: retain both KEKs after a failed KEK rotation and
determine which authenticates the on-disk keyring before proceeding. Do not
interpret an error as proof that publication never happened.

## Encrypted restore targets

Rust callers keep backup decryption in `RestoreConfig.encryption` and configure
target encryption with `with_target_encryption`. File-key mode requires an
existing 32-byte raw key or the existing supported hexadecimal key format.
Envelope mode requires both its existing keyring and the matching KEK. Missing,
malformed or unauthenticated material fails before the staging database exists.
The target key provider is opened once and passed directly to encrypted storage;
restore never silently provisions a replacement keyring.

To create a fresh envelope target, first provision a private KEK with a
cryptographically secure generator. Then explicitly opt in with
`with_target_keyring_creation(true)` or `--create-target-keyring`. Creation is
exclusive: an existing file, directory or symlink at the keyring path is never
replaced. Database and new keyring paths must be separate and non-nested.
Validate-only failures create no keyring; an error after explicit provisioning
can leave the complete keyring behind. Preserve it, inspect the failure and
retry without the creation option. Never delete a keyring merely because a
restore command returned an error.

```sh
umask 077
openssl rand -out /private/restore-kek.bin 32
multidb restore --backup /backups/INCREMENTAL_ID \
  --dest-db /private/recovered.redb --profile balanced \
  --source-keyring-file /keys/backup-keyring.json --source-kek-file /keys/backup-kek.bin \
  --target-keyring-file /private/recovered-keyring.json --target-kek-file /private/restore-kek.bin \
  --create-target-keyring
```

Omit `--create-target-keyring` to reuse an existing keyring. Use
`--source-key-file` or `--target-key-file` for legacy file keys; each is mutually
exclusive with its corresponding keyring/KEK pair. Omit source flags for
plaintext backups, and target flags for a plaintext database. The `verify`
command accepts the same encryption settings for its temporary recovery drill.
Supplying new-keyring creation to verification deliberately creates that
persistent keyring even though the restored drill database is removed.
The `MULTIDB_ENCRYPTION_KEY_PATH` setting used by normal database commands does
not implicitly select either restore key: choose source and target explicitly.

Keep envelope keyrings and KEKs in separately protected, recoverable storage.
Losing either loses access; backup payloads do not embed recovery secrets.
Retain every DEK required by retained full and incremental payloads. A DEK
rotation changes new writes; it does not re-encrypt an old backup. Re-encryption
of live database values alone is insufficient justification for destroying a
DEK while retained backups still depend on it. KEK rotation rewraps live DEKs;
test a complete chain with the new KEK before retiring the old KEK and its
corresponding keyring copies. Coordinate key maintenance with all databases
and providers sharing that keyring, preferably while they are closed.

## Reproducible local recovery drill

```sh
cargo run --locked --example backup_recovery_drill -- /private/new-recovery-drill
```

The directory must not exist. The example creates private random keys and real
redb databases, takes an encrypted full backup and two incrementals spanning a
DEK rotation, rewraps under a new KEK, verifies the full chain, restores an
encrypted latest database and LSN/time recovery points, checks exact balances,
verifies the preserved private-key audit signatures on all recovered targets,
and proves missing-keyring, old-KEK, wrong audit-key and damaged-payload rejection. It deliberately
damages the final incremental only after successful recoveries and leaves the
artifacts for inspection. `recovery-evidence.json` contains results and LSNs,
without secrets; the program prints its BLAKE3 digest. Keep the private output
directory protected and remove it when the exercise is complete.

Regression tests also inject interrupted/short/zero writes, ENOSPC and EIO
results, file-sync and publication failures, and post-publication directory-sync
failure. Linux tests exercise a real kernel ENOSPC result using `/dev/full`.
They verify that partial temporary files are cleaned up and that failures leave
either the previous complete file, no restore target, or a complete published
target with an explicitly uncertain durability outcome. These are deterministic
local fault-boundary tests, not certification of full-filesystem exhaustion,
hardware cache behavior, sudden power loss or storage-controller durability.

The release smoke scripts do not reserve an empty restore destination. Existing
profile-floor and cloud restore tests cover fresh paths. Additional regression
tests cover occupied/dangling-link targets, concurrent destination creation,
failed replay cleanup, private permissions, foreign incremental parents,
missing restore keyrings and concurrent keyring creation.
