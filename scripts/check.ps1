$ErrorActionPreference = "Stop"

if (-not $env:RUST_MIN_STACK) {
    $env:RUST_MIN_STACK = "33554432"
}

function Invoke-Cargo {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    & cargo @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "cargo $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Invoke-Cargo -Arguments @("fmt", "--check")
Invoke-Cargo -Arguments @("clippy", "--all-targets", "--all-features", "--", "-D", "warnings")
Invoke-Cargo -Arguments @("test", "--all-features")
& (Join-Path $PSScriptRoot "cluster-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "cluster-smoke.ps1 failed with exit code $LASTEXITCODE"
}
& (Join-Path $PSScriptRoot "templates-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "templates-smoke.ps1 failed with exit code $LASTEXITCODE"
}
& (Join-Path $PSScriptRoot "preview-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "preview-smoke.ps1 failed with exit code $LASTEXITCODE"
}
& (Join-Path $PSScriptRoot "perf_gate.ps1") -SelfTest
if ($LASTEXITCODE -ne 0) {
    throw "perf_gate.ps1 self-test failed with exit code $LASTEXITCODE"
}
Invoke-Cargo -Arguments @("deny", "check")
& (Join-Path $PSScriptRoot "license-deny-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "license-deny-smoke.ps1 failed with exit code $LASTEXITCODE"
}
& (Join-Path $PSScriptRoot "ops-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "ops-smoke.ps1 failed with exit code $LASTEXITCODE"
}
& (Join-Path $PSScriptRoot "upgrade-smoke.ps1")
if ($LASTEXITCODE -ne 0) {
    throw "upgrade-smoke.ps1 failed with exit code $LASTEXITCODE"
}
