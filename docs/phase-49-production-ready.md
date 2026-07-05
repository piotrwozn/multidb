# Phase 49 Docker Runtime

Phase 49 makes the public preview runnable as a production-shaped Docker
runtime. It does not publish a registry image or claim Kubernetes HA
automation; those remain later release hardening work.

## Implemented Contracts

- `multidb serve` starts the Control Plane API, Studio static assets and
  PostgreSQL wire server from one shared in-process database handle.
- The HTTP runtime binds to `MULTIDB_BIND` and defaults to `0.0.0.0:8080`.
- The PostgreSQL wire runtime binds to `MULTIDB_PG_BIND` and defaults to
  `0.0.0.0:5432`.
- Runtime data defaults to `/var/lib/multidb/multidb.redb`, with the container
  declaring `/var/lib/multidb` as its durable volume.
- Admin auth accepts the Phase 50 durable admin password flow and keeps
  `MULTIDB_ADMIN_TOKEN` or `MULTIDB_ADMIN_TOKEN_FILE` for compatibility.
- PostgreSQL SCRAM auth requires `MULTIDB_PG_PASSWORD` or
  `MULTIDB_PG_PASSWORD_FILE`.
- Production mode requires `MULTIDB_PG_TLS_CERT` and `MULTIDB_PG_TLS_KEY`.
  Plaintext PG is allowed only when `MULTIDB_RUNTIME_MODE=local-dev`.
- The admin router keeps existing unprefixed endpoints and also mounts the same
  API under `/api/*` for same-origin Studio deployments.
- The Docker image builds Rust and Studio in separate stages and runs as a
  non-root user in the final stage.
- `docker-compose.yml` provides a one-command local-dev quickstart.
- The Helm chart uses the same ports, binds, volume path and env names, with
  one replica and PVC persistence by default, and credentials/TLS referenced
  from Secrets.

## Boundaries

Phase 49 originally shipped with Bearer token admin auth. Phase 50 layers
durable admin password login and short in-memory browser sessions on the same
Docker runtime while preserving legacy token automation.

Phase 49 by itself does not push to a public registry, sign container images,
automate multi-node Kubernetes, or add multi-region placement. Phase 53 adds
the release publishing and image signing gates; Kubernetes automation and
multi-region placement remain outside scope.

## Acceptance Tests

Run:

```powershell
cargo test --lib admin -- --nocapture
cargo test --lib network -- --nocapture
cargo test --bin multidb serve -- --nocapture
cargo test --lib roadmap -- --nocapture
.\scripts\studio-check.ps1
.\scripts\docker-smoke.ps1
.\scripts\ops-smoke.ps1
.\scripts\upgrade-smoke.ps1
```

CI runs the Docker smoke on Ubuntu. Phase 53 release tags build, smoke, publish
and sign the GHCR image without publishing a `latest` tag.
