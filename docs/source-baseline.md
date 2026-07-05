# Source Baseline Before Docker And Spring

This repository should have a clear source baseline before adding Docker images,
Spring compatibility smoke apps, or deployment packaging.

## Source Roots

These paths are source or intentionally versioned project evidence:

- `src/`, `benches/`, `examples/`, `fuzz/`, `studio/src/`, `studio/tests/`
- `scripts/`, `docs/`, `ops/`, `baselines/`
- `Cargo.toml`, `Cargo.lock`, `deny.toml`, `README.md`
- `studio/package.json`, `studio/package-lock.json`, `studio/*.config.*`,
  `studio/index.html`, `studio/tsconfig*.json`

## Generated Or Local-Only Roots

These paths are generated, cache, or local runtime state and should stay ignored:

- `target/`, `fuzz/target/`
- `studio/node_modules/`, `studio/dist/`, `studio/test-results/`,
  `studio/playwright-report/`, `studio/tsconfig.tsbuildinfo`
- `.swarm/`, `.claude-flow/`, `.idea/`
- `plan/` local planning history
- local database/log artifacts such as `ruvector.db`, `agentdb.rvf*`, `*.pdb`,
  and `*.log`

## Current Rule

Do not start Dockerfile, Spring JPA smoke apps, or release packaging while most
project files are untracked. First classify new files into one of the buckets
above, then either track source/evidence files or add generated outputs to
`.gitignore`.

This is a workflow guard, not a requirement to erase local work. Unknown
untracked files should be reviewed with `git status --short --ignored` before
being removed or committed.
