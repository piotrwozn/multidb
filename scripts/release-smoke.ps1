param(
    [switch] $SkipDocker,
    [switch] $SkipGo
)

$ErrorActionPreference = "Stop"

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    Write-Host "==> $Name"
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Name failed with exit code $LASTEXITCODE"
    }
}

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
Push-Location $Root
try {
    Invoke-Checked "repository check" {
        & (Join-Path $PSScriptRoot "check.ps1")
    }

    Invoke-Checked "Studio check" {
        & (Join-Path $PSScriptRoot "studio-check.ps1")
    }

    Push-Location "studio"
    try {
        Invoke-Checked "Studio Playwright e2e" {
            npm run e2e
        }
    } finally {
        Pop-Location
    }

    if (-not $SkipDocker) {
        Invoke-Checked "Docker runtime smoke" {
            & (Join-Path $PSScriptRoot "docker-smoke.ps1")
        }
    }

    if ($SkipGo) {
        Invoke-Checked "SDK package smoke" {
            & (Join-Path $PSScriptRoot "sdk-smoke.ps1")
        }
        Invoke-Checked "SDK examples smoke" {
            & (Join-Path $PSScriptRoot "sdk-examples-smoke.ps1")
        }
    } else {
        Invoke-Checked "SDK package smoke" {
            & (Join-Path $PSScriptRoot "sdk-smoke.ps1") -RequireGo
        }
        Invoke-Checked "SDK examples smoke" {
            & (Join-Path $PSScriptRoot "sdk-examples-smoke.ps1") -RequireGo
        }
    }

    Invoke-Checked "Release performance gate" {
        & (Join-Path $PSScriptRoot "perf.ps1") -Profile release-baseline -Rows 1000 -Output target/perf/release-candidate.json
    }
    Invoke-Checked "Release performance comparison" {
        & (Join-Path $PSScriptRoot "perf_gate.ps1") -Baseline baselines/perf/release-baseline.json -Candidate target/perf/release-candidate.json -SummaryOutput target/perf/release-gate-summary.json
    }

    Invoke-Checked "cargo deny supply-chain gate" {
        cargo deny check
    }
} finally {
    Pop-Location
}

Write-Host "release smoke ok"
