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
| `MULTIDB_PROFILE` | no | `transactional` | Storage profile name, product slug, alias, or `auto` (probe the existing database; required for manifest databases built with a non-default profile). `auto` fails when the database does not exist yet, so the first start of a fresh volume needs an explicit profile. A database left by a stopped or killed container gets redb's crash recovery before `auto` reads it, as with a named profile. |
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
   `defaults.replication`, `guarantees.encryption.at_rest: true` (serve cannot
   open encrypted databases in this build) and a volatile profile. The image
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
provisioning, the admin password bootstrap runs as before; its `admin` and
`root` names are reserved, so provisioning never touches them.

Known limits in this build:

- Audit is chained with the development key (`enforced_with_development_key`);
  `serve` has no audit key setting.
- Without spec mode, a restart re-bootstraps RBAC: plain `serve` opens the
  database without loading its persisted roles, and the admin password
  bootstrap then persists the in-memory state, so roles and principals that
  provisioning applied are replaced by the bootstrap administrator. Spec mode
  loads the persisted RBAC state at every start, so keep a provisioned
  database in spec mode.

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
