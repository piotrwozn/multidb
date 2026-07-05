$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\..")
$IsWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform([System.Runtime.InteropServices.OSPlatform]::Windows)
$ExeName = if ($IsWindowsHost) { "multidb.exe" } else { "multidb" }
$Bin = Join-Path (Join-Path $RepoRoot "target") (Join-Path "debug" $ExeName)

if (-not (Test-Path $Bin)) {
    Push-Location $RepoRoot
    cargo build --bin multidb
    Pop-Location
}

$Spec = Join-Path $PSScriptRoot "multidb.yaml"
& $Bin config validate --spec $Spec
if ($LASTEXITCODE -ne 0) { throw "template config validation failed" }
& $Bin config explain --spec $Spec --json | ConvertFrom-Json | Out-Null
if ($LASTEXITCODE -ne 0) { throw "template config explain failed" }
Get-Content (Join-Path $PSScriptRoot "seed.json") -Raw | ConvertFrom-Json | Out-Null
"template smoke ok: multidb.yaml"
