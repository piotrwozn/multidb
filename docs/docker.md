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
| `MULTIDB_PROFILE` | no | `transactional` | Any existing CLI profile name. |
| `MULTIDB_BIND` | no | `0.0.0.0:8080` | HTTP API and Studio bind. |
| `MULTIDB_PG_BIND` | no | `0.0.0.0:5432` | PostgreSQL wire bind. |
| `MULTIDB_STUDIO_DIR` | no | `/usr/share/multidb/studio` | Built Studio asset directory. |
| `MULTIDB_ADMIN_PASSWORD` / `_FILE` | yes* | none | Bootstrap password for the durable `admin` credential. `_FILE` wins over the env var. |
| `MULTIDB_ADMIN_PASSWORD_RESET` | no | unset | Set to `1` to overwrite an existing stored admin credential from env/file. |
| `MULTIDB_ADMIN_SESSION_TTL_SECONDS` | no | `28800` | Browser session TTL, clamped to 60 seconds through 24 hours. |
| `MULTIDB_ADMIN_LOGIN_MAX_FAILURES` | no | `5` | Failed login attempts before lockout; clamped to at least 1. |
| `MULTIDB_ADMIN_LOGIN_WINDOW_SECONDS` | no | `300` | Rolling failure window for login lockout; clamped to at least 1. |
| `MULTIDB_ADMIN_LOGIN_LOCKOUT_SECONDS` | no | `300` | Neutral `401` lockout duration; clamped to at least 1. |
| `MULTIDB_ADMIN_TOKEN` / `_FILE` | no | none | Legacy Bearer token for automation and compatibility. |
| `MULTIDB_PG_USER` | no | `multidb` | PostgreSQL SCRAM user. |
| `MULTIDB_PG_PASSWORD` / `_FILE` | yes | none | PostgreSQL SCRAM password. |
| `MULTIDB_PG_TLS_CERT` | production | none | PEM certificate path. |
| `MULTIDB_PG_TLS_KEY` | production | none | PEM private key path. |
| `MULTIDB_PG_TLS_MODE=disabled` | local only | unset | Accepted only with `MULTIDB_RUNTIME_MODE=local-dev`. |
| `MULTIDB_MAX_CONCURRENT_QUERIES` | no | 4 x available CPUs | Statements the query governor runs at once, `1` to `4096`. PostgreSQL wire admits at most this many statements, and never more than 32, at a time. |
| `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` | no | `100` | How long a statement waits for a free governor slot before it fails as busy, `1` to `60000`. |

`MULTIDB_MAX_CONCURRENT_QUERIES` and `MULTIDB_QUERY_QUEUE_TIMEOUT_MS` must be
whole numbers in their range; any other non-empty value stops `multidb serve`
at startup instead of falling back to the default. An empty value counts as
unset and uses the default.

`MULTIDB_ADMIN_PASSWORD` is required only when the database does not already
contain the durable `admin` credential and no legacy admin token is configured.

Production mode fails closed unless a stored admin credential, admin password,
or legacy admin token is available. It also requires PG password and PG TLS
certificate/key paths.

## API And Studio

The image serves the same Control Plane API twice:

- existing unprefixed endpoints such as `/health`, `/ready`, `/status`,
  `/config`, and `/studio`,
- same-origin Studio endpoints under `/api/*`.

Studio is served from `/` and calls `/api` by default. `/studio` remains the
capability manifest endpoint, not the HTML app. Browser sessions are
stored only in server memory and React state; restarting the server signs
Studio users out without changing the durable admin password hash.

## Concurrency

- One database is shared between PostgreSQL wire and the Control Plane
  through a reader-writer lock. Statements and Control Plane data, catalog,
  audit and advice reads run concurrently.
- `/builder/*` and `/security` updates take an exclusive lock. They wait for
  running statements, and new requests wait behind them. Admin login and
  password change take it only when the bootstrap `admin` role or principal
  is missing or was changed, for example by a `/security` update.
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
  with SQLSTATE 23505. `INSERT ... ON CONFLICT` and inserts into columnar
  tables (the `analytical` profile) retry internally when another statement
  commits first, and fail with SQLSTATE 40001 if 64 retries also conflict;
  retry those. A columnar insert rewrites the whole table, so inserts into one
  columnar table effectively run one at a time. Control Plane single-row,
  single-document and time-series point writes also retry conflicts
  internally. The Control Plane returns a conflict that outlasts the retries
  as HTTP 409 with error code `conflict`. Retry single-statement requests; a
  multi-statement `/sql` body runs its statements one after another without a
  transaction, so statements before the conflicting one have already
  committed.
- A statement keeps running after its client disconnects, over PostgreSQL wire
  and Control Plane `/sql` alike, until it finishes. Exclusive requests queued
  behind it wait until then. The 30 s query timeout applies only to `SELECT`s
  that DataFusion executes, and cannot interrupt a storage scan in progress.
  Single-table `SELECT`s with an equality filter and writes have no time
  limit.
- `/catalog` row counts still scan every table and collection.

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
Secrets.
