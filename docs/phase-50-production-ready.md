# Phase 50 Admin Login And Sessions

Phase 50 replaces manual Studio token entry with an admin username/password
login and short Bearer sessions. Legacy admin tokens remain supported for API
automation, CI and headless tests.

## Implemented Contracts

- `POST /auth/login` accepts `{ "username": "admin", "password": "..." }`
  without a Bearer token and returns `token`, `expires_at`,
  `expires_at_millis`, `principal: "admin"` and `roles: ["admin"]`.
- `POST /auth/logout` requires a session Bearer token and revokes that session.
- `POST /auth/change-password` requires a session Bearer token, verifies the
  current password, and stores the new PHC hash durably in the internal DB
  keyspace.
- `/auth/me` works for both session Bearers and legacy admin tokens. Session
  Bearers ignore `x-multidb-principal`; legacy tokens keep the previous
  principal-header behavior for compatibility.
- Password hashes use Argon2id PHC with 19 MiB memory, 2 iterations and
  parallelism 1, executed on blocking worker threads.
- Session tokens are random 32-byte values returned with a MultiDB session
  prefix; only token hashes are stored in memory.
- The default session TTL is 8 hours. `MULTIDB_ADMIN_SESSION_TTL_SECONDS` is
  clamped between 60 seconds and 24 hours.
- `MULTIDB_ADMIN_PASSWORD_FILE` has priority over `MULTIDB_ADMIN_PASSWORD`.
  Existing stored credentials are preserved unless
  `MULTIDB_ADMIN_PASSWORD_RESET=1` is set.
- Bootstrap ensures a durable `admin` principal and `admin` role with system
  admin and database admin grants without deleting unrelated RBAC state.
- Auth audit records cover login success/failure, logout, password change and
  bootstrap without writing plaintext secrets.

## Runtime Notes

`multidb serve` and `multidb admin serve` fail closed when no stored admin
credential, admin password or legacy admin token is available. Sessions are
in-memory by design, so restarting the server signs Studio users out without
changing the durable password hash.

Docker Compose and Helm now configure an admin password as the primary Studio
path and may also configure a legacy token for automation compatibility.

## Acceptance Tests

Run:

```powershell
cargo test --lib admin -- --nocapture
cargo test --lib db -- --nocapture
cargo test --bin multidb serve_config -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\studio-check.ps1
.\scripts\docker-smoke.ps1
.\scripts\ops-smoke.ps1
```

Phase 50 originally did not add brute-force rate limiting. Phase 53 adds the
in-memory admin login limiter, neutral lockout response and denied audit event
while preserving the Phase 50 login/session contract.
