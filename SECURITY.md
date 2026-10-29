# Security Policy

MultiDB `1.0.0-rc.1` is a release candidate undergoing production qualification.
It has not received stable production acceptance. Please do not report
security issues through public GitHub issues.

## Supported Versions

| Version | Security support |
| --- | --- |
| `main` and `1.0.0-rc.1` | Best-effort triage and fixes during release-candidate qualification |
| Earlier preview tags | Upgrade to the current candidate; no separate maintenance promise |

Use currently supported, patched operating systems, language runtimes and GPU
drivers. SDK language-version minima describe source compatibility, not security
support for end-of-life interpreters or compilers. CI currently tests Node 24
and Go 1.27.3 while retaining the Go SDK's older language compatibility minimum.
Dependency advisory checks cover the root, fuzz and Rust SDK lockfiles and the
locked Studio/TypeScript SDK dependency graphs. A clean advisory result does not
replace independent review or container operating-system vulnerability scanning.

## Reporting A Vulnerability

Send a private report through GitHub Security Advisories after the repository is
published. Include:

- affected version or commit,
- impact and affected component,
- reproduction steps or proof of concept,
- whether the issue is already public.

Expected initial response time is best effort. Do not use MultiDB for critical
production data until a tagged release, green CI, and an explicit deployment
risk review are in place.
