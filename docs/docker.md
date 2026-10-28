# MultiDB Docker Runtime

The Docker runtime image contains the
`multidb` server binary and the built MultiDB Studio app.

Release tags publish a signed GHCR image as
`ghcr.io/<owner>/<repo>:vX.Y.Z`. Pin the exact tag and verify the digest from
the GitHub Release; `latest` is intentionally not published.

## Local Quickstart

```powershell
docker compose up --build
```

Then open Studio at `http://127.0.0.1:8080/` and sign in as `admin` with
the compose admin password:

```text
local-dev-admin-password
```

The compose file is explicitly local development:

- `MULTIDB_RUNTIME_MODE=local-dev`
- `MULTIDB_PG_TLS_MODE=disabled`
- fixed local admin and PostgreSQL passwords

Do not copy those values into production.

## Runtime Contract

Ports:

| Port | Purpose |
| --- | --- |
| `8080` | Control Plane API and Studio |
| `5432` | PostgreSQL wire protocol |

Volume:

| Path | Purpose |
| --- | --- |
| `/var/lib/multidb` | redb database files and durable runtime data |

Environment:

| Name | Required | Default | Notes |
| --- | --- | --- | --- |
| `MULTIDB_RUNTIME_MODE` | no | `production` | Use `local-dev` only for local plaintext PG smoke. |
| `MULTIDB_DB_PATH` | no | `/var/lib/multidb/multidb.redb` | Must live on the mounted volume. |
| `MULTIDB_PROFILE` | no | `transactional` | Storage profile name, product slug, alias, or `auto` (probe the existing database; required for manifest databases built with a non-default profile). `auto` fails when the database does not exist yet, so the first start of a fresh volume needs an explicit profile. After an unclean shutdown, redb crash recovery runs before `auto` reads the database, as with a named profile. With encryption, the configured key is also used to inspect metadata. |
| `MULTIDB_BIND` | no | `0.0.0.0:8080` | HTTP API and Studio bind. |
| `MULTIDB_PG_BIND` | no | `0.0.0.0:5432` | PostgreSQL wire bind. |
| `MULTIDB_STUDIO_DIR` | no | `/usr/share/multidb/studio` | Built Studio asset directory. |
| `MULTIDB_ADMIN_PASSWORD` / `_FILE` | yes* | none | Bootstrap password for the durable `admin` credential. `_FILE` wins over the env var. |
| `MULTIDB_ADMIN_PASSWORD_RESET` | no | unset | Set to `1` to overwrite an existing stored admin credential from env/file. |
| `MULTIDB_ADMIN_SESSION_TTL_SECONDS` | no | `28800` | Browser session TTL, clamped to 60 seconds through 24 hours. |
| `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` | no | `5` | Failed `admin` password checks per client IP (IPv6 /64) before that client is locked out; the server-wide soft ceiling is 20x this per window. Clamped to at least 1. |
| `MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS` | no | `300` | Failure window for the per-client lockout and the soft ceiling; clamped to at least 1. |
| `MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS` | no | `300` | Neutral `401` per-client lockout duration; clamped to at least 1. |
| `MULTIDB_ADMIN_TOKEN` / `_FILE` | no | none | Legacy Bearer token for automation and compatibility. |
| `MULTIDB_PG_USER` | no | `multidb` | PostgreSQL SCRAM user. |
| `MULTIDB_PG_PASSWORD` / `_FILE` | yes | none | PostgreSQL SCRAM password. |
| `MULTIDB_PG_TLS_CERT` | production | none | PEM certificate path. |
| `MULTIDB_PG_TLS_KEY` | production | none | PEM private key path. |
| `MULTIDB_PG_TLS_MODE=disabled` | local only | unset | Accepted only with `MULTIDB_RUNTIME_MODE=local-dev`. |
| `MULTIDB_SPEC_PATH` | no | unset | Spec mode (preview): the `DatabaseSpec` the database is built from and checked against at every start; see [Spec mode](#spec-mode). |
| `MULTIDB_SPEC_APPLY` | no | `verify` | Spec mode only: `verify`, `create` or `additive`. Set without `MULTIDB_SPEC_PATH`, it is a usage error. |
| `MULTIDB_REPLICATION` | no | unset | Read in spec mode only (`cp` or `ap`), where it must agree with the spec; ignored otherwise. |
| `MULTIDB_MAX_CONCURRENT_QUERIES` | no | 4 x available CPUs | Statements the query governor runs at once, `1` to `4096`. PostgreSQL wire admits at most this many statements, and never more than 32, at a time. |
| `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` | no | `100` | How long a statement waits for a free governor slot before it fails as busy, `1` to `60000`. |
| `MULTIDB_HISTORY_RETAIN_LSNS` | no | unset | Opt in to online MVCC history GC, retaining this positive number of recent LSNs (plus active snapshots and temporal retention). One bounded batch runs every 10 seconds. Never truncates the commit log. |
| `MULTIDB_ENCRYPTION_KEY_PATH` | no | unset | Path to an existing 32-byte encryption key file (raw bytes or 64 hexadecimal characters). Used by `serve` and CLI database/provisioning commands; mount it read-only. |

`MULTIDB_MAX_CONCURRENT_QUERIES` and `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` must be
whole numbers in their range; any other non-empty value stops `multidb serve`
at startup instead of falling back to the default. An empty value counts as
unset and uses the default. For a database created with a custom profile the
default is its profile manifest's `query.max_concurrent_queries` and
`query.queue_timeout_ms` (see [Profiles](profiles.md)); a set value applies to
that `serve` process only and is never written to the database.

`MULTIDB_ADMIN_PASSWORD` is required only when the database does not already
contain the durable `admin` credential and no legacy admin token is configured.

RBAC roles, PG-user principal mappings and the audit on/off flag saved through
`POST /security` / Studio Security (or the `set_audit_enabled_as` library API)
are stored in the database file and reloaded on every start. Admin password
bootstrap and login only restore the built-in `admin` role, its standard grants
and the `admin` mapping when something is missing; they never replace other
stored roles, mappings or the audit flag.

Production mode fails closed unless a stored admin credential, admin password,
or legacy admin token is available. It also requires PG password and PG TLS
certificate/key paths.

## Spec Mode

Status: **Preview**. With `MULTIDB_SPEC_PATH`, `serve` builds its database
from a spec instead of a profile name, and checks it against the spec at every
start (see [provisioning.md](provisioning.md#serve)). Without
`MULTIDB_SPEC_PATH`, `serve` behaves exactly as before.

1. The spec is read through the bounded spec reader, validated and compiled;
   a spec that cannot be read, decoded or validated is refused.
2. The database path is an explicit `MULTIDB_DB_PATH`, else the spec
   `deployment.storage_path` resolved against the directory of the spec file,
   else the default `/var/lib/multidb/multidb.redb`. A variable counts as
   explicit when it is set and not blank. The image sets
   `MULTIDB_DB_PATH=/var/lib/multidb/multidb.redb`, and so do
   `docker-compose.yml` and the Helm chart, so in a container the variable is
   explicit and the spec `storage_path` is ignored (`serve` prints a note
   saying so) unless `MULTIDB_DB_PATH` is set to an empty value. Missing
   parent directories are created, as without a spec.
3. Before anything is opened, `serve` refuses an explicit `MULTIDB_PROFILE`
   that selects another storage profile than the spec profile (`auto` never
   conflicts), an explicit `MULTIDB_REPLICATION` other than the spec
   `defaults.replication`, `guarantees.encryption.at_rest: true` without
   `MULTIDB_ENCRYPTION_KEY_PATH`, and a volatile profile. The image
   sets `MULTIDB_PROFILE=transactional`, so unsetting the variable falls back
   to that default: in spec mode set it to an empty value
   (`MULTIDB_PROFILE=""`, which is not explicit) or make it agree, since the
   spec decides the profile.
4. The database is planned against the spec, and `MULTIDB_SPEC_APPLY`
   decides:

   | database | `verify` (default) | `create` | `additive` |
   | --- | --- | --- | --- |
   | missing, plan auto-approvable | refuse | create it | create it |
   | matches the spec (drift, if any, is information only) | start | start | start |
   | differs by auto-approvable additive steps | refuse | refuse | apply them |
   | an interrupted apply whose remaining steps are auto-approvable | refuse | refuse | apply them |
   | anything else: retune, weakening, destructive, format upgrade, RBAC changes on an existing database, missing flags, blocked | refuse | refuse | refuse |

   Every refusal happens before a port is bound: `serve` exits with the reason,
   the plan and the exact `multidb config apply --spec <spec> --db <path>
   [--allow-... flags] --confirm prov_...` command that confirms it (run it with
   `serve` stopped, since the database is locked while it runs), or the
   `multidb config plan` command that reviews a blocked plan. Changes are
   applied as the local operator `local:serve` and audited.

`verify` is the safe default for production: a fresh volume needs
`MULTIDB_SPEC_APPLY=create` for its first start (or `multidb init --apply` /
`config apply --spec` before it), and changes to the spec are applied with
`additive` or a reviewed `config apply`. A running server can also plan and
apply a reviewed spec itself through the preview Control Plane endpoints
`POST /provision/plan` and `POST /provision/apply` (see
[provisioning.md](provisioning.md#control-plane)); keep the mounted spec in
step with what you apply, or the next `verify` start refuses. After
provisioning, `MULTIDB_MAX_CONCURRENT_QUERIES` and
`MULTIDB_QUERY_QUEUE_TIMEOUT_MS` set the query governor of the running server
(never the database), and provisioning plans do not report a knob they set as
drift; with only one of them set, a retune of the other knob is a session
pending a restart.
Then the admin password bootstrap runs as before; its `admin` and `root`
names are reserved, so provisioning never touches them.

Roles and principals that provisioning applied are stored in the database
file like any other RBAC state, so they survive a restart with or without spec
mode (see the RBAC note above).

Known limit in this build: audit is chained with the development key
(`enforced_with_development_key`); `serve` has no audit key setting.

## Encryption At Rest

Set `MULTIDB_ENCRYPTION_KEY_PATH` to a mounted key file to create or open
an encrypted database. The file must already exist and contain 32 raw bytes
or a 64-character hexadecimal key. The variable carries only its path; key
bytes are never accepted as an environment variable or written into a spec.
The same selector works with `config plan`, `config apply`, `config drift`,
`config status`, `init --apply`, `profile show --db`, backup source reads,
and the other CLI database commands. `--profile auto` decrypts the existing
metadata with that key.

For example, with an existing key file mounted at
`/run/secrets/multidb-encryption.key`:

```text
MULTIDB_ENCRYPTION_KEY_PATH=/run/secrets/multidb-encryption.key
MULTIDB_SPEC_PATH=/etc/multidb/multidb.yaml
MULTIDB_SPEC_APPLY=create
```

A spec that asks for encryption at rest, including `secure-saas`, can be
provisioned with this key configured. Without it the plan or startup is
refused. Keep the same key available across restarts: changing the contents
of this file does not re-encrypt existing data. This selector uses the
file-key storage mode; envelope rotation remains a Rust API operation.

## Background Maintenance And Shutdown

Both `multidb serve` and `multidb admin serve` sweep time-series retention
without waiting for another series write. Each second they scan at most
128 chunk or latest-point records, resuming the scan on the next tick.
Expired chunks and their latest-point entries are removed conditionally,
so a concurrent update is preserved for a later sweep. Retention still
uses server time and whole chunks; cleanup of a large collection takes
multiple batches.

Workload-profiler samples flush every 10 seconds even when no new statement
arrives. `serve` also runs one bounded online MVCC GC batch every 10 seconds
when `MULTIDB_HISTORY_RETAIN_LSNS` is explicitly set to a positive integer.
An unset value leaves history untouched. This task does not shrink the redb
file or truncate the commit log, so it preserves incremental-backup chains.
Commit-log truncation remains an offline `maintenance gc` operation and
requires a fresh full backup afterwards.

On SIGTERM (including `docker stop`) or Ctrl+C, both servers stop accepting
requests, stop maintenance, wait for active storage work, flush buffered
profiler samples and close the database. PostgreSQL sockets are closed;
already-running statements finish before the final flush. The process may
therefore wait for a long statement to finish. SIGKILL or a container stop
timeout bypasses this shutdown path and can lose unflushed advisory samples.

## Helm Chart Variables

The chart's configmap also sets `MULTIDB_AUDIT_JSONL_PATH`,
`MULTIDB_BACKUP_TARGET`, `MULTIDB_MINIO_ENDPOINT`, `MULTIDB_VAULT_ADDR` and
`MULTIDB_VAULT_KEK_PATH` for operator tooling; `multidb serve` does not read
them in this build (`MULTIDB_VAULT_KEK_PATH` is read only by the Rust API's
Vault key provider). `MULTIDB_REPLICATION` is read only in spec mode. With
`spec.enabled: true` the chart mounts the ConfigMap `spec.configMap` read-only
at `spec.mountPath`, sets `MULTIDB_SPEC_PATH` and `MULTIDB_SPEC_APPLY`
(`spec.apply`, `verify` by default) and sets `MULTIDB_PROFILE` and
`MULTIDB_REPLICATION` to empty values, which override the image's
`MULTIDB_PROFILE=transactional` and are not explicit, so the spec decides
them; `MULTIDB_DB_PATH` (`runtime.dbPath`) stays explicit, so it wins over the
spec `storage_path`.

## API And Studio

The image serves the same Control Plane API twice:

- existing unprefixed endpoints such as `/health`, `/ready`, `/status`,
  `/config`, and `/studio`,
- same-origin Studio endpoints under `/api/*`.

Studio is served from `/` and calls `/api` by default. `/studio` remains the
capability manifest endpoint, not the HTML app. Browser sessions are
stored only in server memory and React state; restarting the server signs
Studio users out without changing the durable admin password hash.

Control Plane login limits:

- Failed `admin` password checks are counted per client IP address (IPv6
  clients per /64). After `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` failures in the
  window, that client gets a neutral `401` for the lockout duration, even with
  the correct password. A successful login clears the client's failures.
- Across all clients, at most 20x `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` failed
  checks per window are allowed before the soft ceiling applies. Past it, only
  clients without a failure in the current window may try. Clients with a
  clean window are never refused.
- Logins for any other username are rejected without a password check and
  never lock anyone out.
- The client address is the TCP peer. `X-Forwarded-For` is not trusted, so
  behind a reverse proxy, an ingress, or a Kubernetes Service that rewrites
  source addresses, all clients share one address and one lockout. Use a
  strong admin password.
- At most 2 password checks run at once. Additional login or change-password
  requests get `503` with error code `auth_busy` and `Retry-After: 1`. These
  requests are not counted as failures.
- The username is limited to 256 bytes and passwords to 1024 bytes. Longer
  values get `400` with error code `invalid_value`. The same password limit
  applies to change-password and to `MULTIDB_ADMIN_PASSWORD`; a longer
  bootstrap password stops startup when it would be stored (no stored
  credential yet, or `MULTIDB_ADMIN_PASSWORD_RESET`).
- Audit: every failed `admin` password check is recorded as `login`/`Failed`
  with the client address. Refused attempts are recorded as
  `login_rate_limited`/`Denied` once per client per lockout. Reaching the soft
  ceiling records one `login_rate_limited` event with `scope: global`. Failed
  logins for other usernames are recorded up to 20x
  `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` per window, followed by one
  `login_audit_coalesced` event.

## Concurrency

- One database is shared between PostgreSQL wire and the Control Plane
  through a reader-writer lock. Statements and Control Plane data, catalog,
  audit and advice reads run concurrently.
- `/builder/*` and `/security` updates and `POST /provision/apply` take an
  exclusive lock. They wait for running statements, and new requests wait
  behind them. Provisioning plans, drift, `GET /provision/state` and
  `GET /config/live` only read and run under the shared lock. Admin login and
  password change take it only when the bootstrap `admin` role or principal
  is missing or was changed, for example by a `/security` update.
- Admin login and password change read the stored credential under the
  shared lock and release it before the Argon2id password check, which runs on
  the blocking pool, so a check in progress never delays `/builder/*` or
  `/security` updates. Password checks have their own concurrency limit,
  separate from the query governor; see the login limits above.
- By default at most 4 x available CPUs statements execute at once; a
  container CPU limit lowers that number. `MULTIDB_MAX_CONCURRENT_QUERIES`
  overrides it. PostgreSQL wire admits up to that many statements, and never
  more than 32, and queues the rest. A statement that still finds no free
  slot, for example because Control Plane `/sql` requests hold them, waits
  `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` (100 ms by default) and then fails with
  SQLSTATE 53200. Nothing ran, so it can be retried as is. The Control Plane
  returns such failures as HTTP 503 with error code `busy` and
  `Retry-After: 1`.
- A plain `INSERT` that races another insert of the same primary key fails
  with SQLSTATE 23505, or with P0001 if a before trigger rejects the insert
  itself. `INSERT ... ON CONFLICT` and inserts into columnar
  tables (the `analytical` profile) retry internally when another statement
  commits first, and fail with SQLSTATE 40001 if 64 retries also conflict;
  retry those. A plain columnar insert appends to the last segment while checking primary-key
  uniqueness against existing rows. Concurrent appenders can still conflict on
  the tail; updates can rewrite the table. Control Plane single-row,
  single-document and time-series point writes also retry conflicts
  internally. The Control Plane returns a conflict that outlasts the retries
  as HTTP 409 with error code `conflict`. Retry single-statement requests; a
  multi-statement `/sql` body runs its statements one after another without a
  transaction, so statements before the conflicting one have already
  committed.
- WASM triggers see the event and old row that the write's commit checks, so a
  race ends in the conflict handling above, never in a trigger rejection for an
  event the statement does not have. Stored procedure (`CALL`) writes check
  nothing at commit: they are last-writer-wins, and under concurrent writes
  the event and old row their triggers see are best effort.
- A statement keeps running after its client disconnects, over PostgreSQL wire
  and Control Plane `/sql` alike, until it finishes. Exclusive requests queued
  behind it wait until then. The 30 s query timeout applies only to `SELECT`s
  that DataFusion executes, and cannot interrupt a storage scan in progress.
  Single-table `SELECT`s with an equality filter and writes have no time
  limit.
- `/catalog` exact counts scan row-layout keys and document collections;
  columnar table counts use segment metadata when available.

## Smoke Test

```powershell
.\scripts\docker-smoke.ps1
```

The smoke builds the image, starts a local-dev container, waits for `/ready`,
checks `/health`, verifies password login, session Bearer auth and logout,
checks the legacy admin token, checks Studio HTML, opens the PostgreSQL TCP
port, restarts with the same volume, and confirms the database file persisted.

Release candidates also run SDK package and example smokes against the Docker
runtime before publishing the signed release image.

## Helm Parity

`ops/helm/multidb` uses the same port, volume, and environment contract as the
Docker image. The chart defaults to one replica with PVC-backed
`/var/lib/multidb`; `emptyDir` is available only through the explicit
`persistence.devEmptyDir` development setting. Admin password, legacy admin
token, PG password, and PG TLS material are referenced through Kubernetes
Secrets. Spec mode is off by default (`spec.enabled: false`); see
[Helm Chart Variables](#helm-chart-variables).
