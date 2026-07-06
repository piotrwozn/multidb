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
.\scripts\phase53-ga-smoke.ps1
```

If Go is not installed locally, state that explicitly and run the rest of the
gate with `-SkipGo`; GitHub Actions runs the Go SDK checks.

## Pull Request Expectations

- Keep generated artifacts out of the commit.
- Do not commit `plan/`; it is local planning history, not public source.
- Update docs and tests with behavior changes.
- Preserve fail-closed behavior for auth, config validation, migration planning
  and storage corruption paths.
- Treat `src/roadmap.rs` as the checked-in readiness source of truth.

## Development Notes

The project is licensed under the MIT License. By contributing, you agree that
your contribution is available under the MIT License.
