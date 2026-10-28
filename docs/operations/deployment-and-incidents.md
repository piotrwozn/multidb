# Deployment, recovery and incident runbook

This runbook is a qualification procedure. Record its execution on the intended
platform before including that deployment in a production support contract.
Use the exact release commit, binary/image hashes, signatures and supported
configuration recorded by the release owner. A local development qualification
does not establish Internet exposure, key custody or storage support.

## Before traffic

1. Reserve a dedicated service account and owned database volume. Verify the
   actual UID/GID can write the volume, its directory is private, and backups
   and key material have separate access controls. Do not recursively change
   permissions on an unreviewed existing data volume.
2. Configure production mode, HTTP reverse-proxy boundaries, pgwire TLS, the
   intended database profile and secret-file mounts using the documented
   [container configuration, authentication and TLS](../docker.md#runtime-contract).
   Local plaintext bindings and the public disposable qualification credentials
   are only test settings. Keep metrics and operator endpoints private.
3. Run `multidb profile show --db <path> --json` during the approved offline
   window and retain the resolved profile/floors. Confirm the intended config
   through the documented config validation/plan workflow before applying it.
   Freeze maximum tested data size, concurrency, supported SQL/API subset,
   retention, backup downtime, RPO/RTO and the compatibility window.
4. Store backups on independent storage. For encrypted storage/backups, keep
   the correct source and target keys/keyrings under a documented custodian and
   test restoration with copies of the custody material. A healthy encrypted
   database is not evidence that a lost key can be recovered. Never place keys
   in Git, command-line arguments, CI artifacts, screenshots or support logs.
5. Start the service with process supervision and verify `/health` and `/ready`
   on the actual bound endpoint. Readiness must gate client traffic. Exercise a
   real authorized read/write, a denied principal, and a pgwire TLS connection;
   an open port or HTTP health response alone does not prove database access.
6. Execute the intended-environment qualification contract and have the SLO
   owner accept its evidence. Keep unsupported GPU/distributed/foreign-source
   surfaces explicitly preview or disabled in the promised release scope.

## Monitoring and capacity

Record the approved workload-specific latency and error SLOs before deployment.
Collect request p95/p99 and error/denial/busy/timeout rates, native process RSS
and high-water mark, open descriptors, available disk, allocated database and
audit/log growth, backup age, restore-drill age, maintenance failures and process
restarts. Authenticate or restrict telemetry as required by the deployment.

Set warning and critical thresholds against measured headroom, not against a
sample run's fastest result. Include room for backup staging, a fresh restore,
history/commit-log growth and key rotation. A reasonable initial operational
policy is a warning below 20% free volume capacity and urgent intervention below
10%, supplemented by an absolute reserve large enough for the tested recovery
workflow. The local runner's 512 MiB floor is a disposable-fixture guard, not a
production disk-capacity recommendation. Test that alerts reach an actual
responder; a dashboard alone is not an alerting drill.

Online MVCC GC requires explicit `MULTIDB_HISTORY_RETAIN_LSNS` retention.
It preserves active snapshots/temporal retentions and does not truncate the
commit log. Review periodic maintenance warnings and measured growth. Offline
commit-log truncation can invalidate old incremental parents; take a new full
backup after truncation. Never treat retention settings as a backup policy.

## Planned shutdown and backup window

1. Drain new client traffic and record in-flight write outcomes. Stop load
   producers, then send SIGTERM through the supervisor to the owned server.
   Wait for a clean exit. A timeout or forced kill is an incident, even when a
   later reopen succeeds; preserve logs and investigate the shutdown path.
2. While the database is exclusively offline, run the supported full or
   incremental backup command with the correct profile and source keys. See
   [backup/restore safety](../backup-and-restore.md). Verify the backup and retain
   its manifest/payload hashes plus timeline/parent/LSN metadata.
3. Restore into an **absent** target path and compare full logical contents and
   schema. Select the documented encrypted-target options if target encryption
   is required. Encryption of the backup payload does not itself encrypt the
   restored database. Never replace the live file with an unverified restore.
4. Measure start-to-ready recovery time and the last recovered acknowledged
   write/LSN against accepted RPO/RTO. Retain evidence for latest recovery and
   the intended LSN/time PITR cases. Reopen, verify policy and health, then route
   traffic back. Record the actual maintenance downtime.

The local qualification runner performs this offline full/verify/restore cycle.
The separate recovery drill covers encrypted full/incremental/PITR and key
rotation. It deliberately damages its disposable final incremental after the
successful restores to prove rejection; that resulting test chain must never be
mistaken for a healthy operational backup set.

## Incident handling

For unexpected exits, corruption, disk-full, lost keys, authorization leakage or
audit verification failures, restrict traffic and preserve evidence first.
Record the artifact/config hashes, time, affected operation and acknowledged or
uncertain write outcomes. Copy logs through the approved secret-redaction path;
do not dump environment variables or credentials. Preserve the database, audit
anchor, backups, keyring and storage diagnostics under controlled access. Avoid
in-place repair or deletion until a reviewed recovery plan exists.

For capacity exhaustion, stop new writes before the reserve is consumed. Expand
approved storage or recover to a tested larger target. Deleting arbitrary
database sidecars, audit anchors, keyrings or backup parents can turn a capacity
incident into unrecoverable data loss. If GC or retention is the remedy, use an
approved exclusive maintenance window and retain a verified recovery point.

For suspected policy or credential compromise, remove exposure, rotate affected
credentials through the documented mechanism and verify old sessions are
revoked. Test a denied identity and tenant boundary before reopening traffic.
Independent security review and vulnerability response ownership remain required
release gates; a passing automated probe does not replace them.

## Upgrade and rollback

Before upgrading, run the declared previous-version-to-candidate migration drill
on a fresh copy, including encrypted keys, schema, policies, data and backup/PITR
compatibility. Record the allowed compatibility window and recovery duration.
Create and verify a pre-upgrade recovery point before switching the artifact.

If acceptance fails, drain and stop the candidate. Use the reviewed rollback
procedure: either a proven compatible old artifact with its supported data
format or restoration of the pre-upgrade backup into a fresh target. Do not open
an upgraded live file with an older binary merely because its version number is
smaller. Account for post-upgrade acknowledged writes in the accepted RPO and
operator decision. Verify full logical contents, authorization and readiness
before restoring traffic, and retain the failed candidate's evidence.

Record incident owners, escalation routes, supported versions, vulnerability
response expectations and the date of the last successful restore/upgrade/alert
drills with the external production-environment acceptance evidence.
