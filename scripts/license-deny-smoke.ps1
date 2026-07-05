$ErrorActionPreference = "Stop"

$denyConfig = Join-Path $PSScriptRoot "..\deny.toml"
if (!(Test-Path $denyConfig)) {
    throw "missing deny.toml"
}

$raw = Get-Content -Raw $denyConfig
$forbidden = @(
    "AGPL-1.0-only",
    "AGPL-1.0-or-later",
    "AGPL-3.0-only",
    "AGPL-3.0-or-later",
    "SSPL-1.0"
)

foreach ($license in $forbidden) {
    if ($raw -match [regex]::Escape("`"$license`"")) {
        throw "forbidden copyleft license is allowed by deny.toml: $license"
    }
}

if ($raw -notmatch 'unknown-registry\s*=\s*"deny"') {
    throw "deny.toml must reject unknown registries"
}

if ($raw -notmatch 'unknown-git\s*=\s*"deny"') {
    throw "deny.toml must reject unknown git sources"
}

Write-Host "License deny smoke passed"
