# Release Notes

Use `docs/release-checklist.md` as the operational checklist for tagged
releases.

## First Public Preview

Recommended first tag:

```powershell
git tag v0.1.0-preview.1
git push origin v0.1.0-preview.1
```

Before tagging:

```powershell
.\scripts\release-smoke.ps1
```

Do not publish a stable `v1.0.0` tag until the project has external soak time,
green CI on the public repository, and a reviewed production-support statement.
