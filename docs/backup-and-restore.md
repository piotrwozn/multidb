# Backup and restore safety

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
directory removed on success or error. Restore still creates a plaintext
database: encryption of backup payloads does not encrypt the restored target.
The profile-floor rules and explicit unmet-floor opt-in remain in force.

Full backup persists a stable source timeline in internal maintenance metadata.
Incremental backup refuses a parent belonging to another source timeline.
Legacy backup chains can still be restored, but taking further incrementals
from a source without persisted timeline metadata requires a new full backup.
The manifest's optional `end_commit_hash` also anchors the parent's last commit,
so an incremental refuses a diverged restored branch even when it shares the
timeline. If GC removed that anchor, start a new full backup.
Keep a new full backup after history/commit-log truncation as before.

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
Independent providers cannot overwrite a keyring changed by another provider:
key maintenance uses a stable sidecar file lock and checks the expected state.
Reopen a stale provider before retrying key maintenance. The sidecar `.lock`
file is not a key; retain the actual keyring and KEK. Coordinate all providers
before DEK destruction because providers that already hold a key can retain it
in memory, and other databases/backups may still require it.

The release smoke scripts do not reserve an empty restore destination. Existing
profile-floor and cloud restore tests cover fresh paths. Additional regression
tests cover occupied/dangling-link targets, concurrent destination creation,
failed replay cleanup, private permissions, foreign incremental parents,
missing restore keyrings and concurrent keyring creation.
