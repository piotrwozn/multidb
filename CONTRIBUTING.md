# Contributing

MultiDB is currently optimized for careful, evidence-backed changes. Keep pull
requests small enough to review and include the command output that proves the
change.

## Local Checks

Run the repository gate before opening a pull request:

```powershell
.\scripts\check.ps1
```

For Studio changes, also run:

```powershell
.\scripts\studio-check.ps1
Push-Location studio; npm run e2e; Pop-Location
```

For release-sensitive changes, run:

```powershell
.\scripts\release-smoke.ps1
```

If Go is not installed locally, state that explicitly and run the rest of the
gate with `-SkipGo`; GitHub Actions runs the Go SDK checks.

## Pull Request Expectations

- Keep generated artifacts out of the commit.
- Do not commit `plan/`; it is local planning history, not public source.
- Update docs and tests with behavior changes.
- Preserve fail-closed behavior for auth, config validation, migration planning
  and storage corruption paths.
- Keep `docs/support-matrix.md` aligned with user-visible support claims.

## Review And Merge Policy

All pull requests require review from the repository owner before they can be
merged into `main`. The repository uses CODEOWNERS so that changes across the
tree request review from `@piotrwozn`.

## Development Notes

The project is licensed under the MIT License. By contributing, you agree that
your contribution is available under the MIT License.
