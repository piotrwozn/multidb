param(
    [string] $Bin,
    [string] $Output,
    [string] $WorkDir,
    [switch] $SkipBuild
)

$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    $Result = & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
    $Result
}

function Invoke-JsonCommand {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments,
        [Parameter(Mandatory = $true)]
        [string] $Binary
    )

    $Raw = Invoke-Checked -Label $Label -Command {
        & $Binary @Arguments
    }
    $Text = $Raw -join "`n"
    if ([string]::IsNullOrWhiteSpace($Text)) {
        throw "$Label returned empty JSON"
    }
    $Text | ConvertFrom-Json
}

function Resolve-MultiDbBinary {
    if (-not [string]::IsNullOrWhiteSpace($Bin)) {
        if (-not (Test-Path $Bin)) {
            throw "missing multidb binary at $Bin"
        }
        return (Resolve-Path $Bin).Path
    }

    $Command = Get-Command "multidb" -ErrorAction SilentlyContinue
    if ($Command) {
        return $Command.Source
    }

    if ($SkipBuild) {
        throw "multidb was not found on PATH and -SkipBuild was passed"
    }

    Push-Location $RepoRoot
    try {
        Invoke-Checked -Label "cargo build --bin multidb" -Command {
            cargo build --bin multidb
        } | Out-Null
    }
    finally {
        Pop-Location
    }

    $IsWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $ExeName = if ($IsWindowsHost) { "multidb.exe" } else { "multidb" }
    $Resolved = Join-Path (Join-Path $RepoRoot "target") (Join-Path "debug" $ExeName)
    if (-not (Test-Path $Resolved)) {
        throw "missing multidb binary at $Resolved"
    }
    (Resolve-Path $Resolved).Path
}

function Resolve-TargetPath {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Path
    )

    [System.IO.Path]::GetFullPath($Path)
}

if ([string]::IsNullOrWhiteSpace($Output)) {
    $Output = Join-Path (Join-Path (Join-Path $RepoRoot "target") "preview-smoke") "summary.json"
}
if ([string]::IsNullOrWhiteSpace($WorkDir)) {
    $WorkDir = Join-Path (Join-Path (Join-Path $RepoRoot "target") "preview-smoke") "workspace"
}

$TargetRoot = Resolve-TargetPath -Path (Join-Path $RepoRoot "target")
if (-not $TargetRoot.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
    $TargetRoot = "$TargetRoot$([System.IO.Path]::DirectorySeparatorChar)"
}
$ResolvedWorkDir = Resolve-TargetPath -Path $WorkDir
if (-not $ResolvedWorkDir.StartsWith($TargetRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "refusing to clear preview smoke path outside target: $ResolvedWorkDir"
}

if (Test-Path $ResolvedWorkDir) {
    Remove-Item -LiteralPath $ResolvedWorkDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $ResolvedWorkDir | Out-Null

$ResolvedOutput = Resolve-TargetPath -Path $Output
$OutputParent = Split-Path -Parent $ResolvedOutput
if (-not [string]::IsNullOrWhiteSpace($OutputParent)) {
    New-Item -ItemType Directory -Force -Path $OutputParent | Out-Null
}

$Binary = Resolve-MultiDbBinary

$TemplateList = Invoke-JsonCommand `
    -Label "multidb template list --json" `
    -Arguments @("template", "list", "--json") `
    -Binary $Binary
$Templates = @($TemplateList)
if (-not ($Templates | Where-Object { $_.slug -eq "ai-memory" })) {
    throw "preview template ai-memory is missing from template list"
}

$TemplateExplain = Invoke-JsonCommand `
    -Label "multidb template explain ai-memory --json" `
    -Arguments @("template", "explain", "ai-memory", "--name", "Preview Smoke", "--json") `
    -Binary $Binary
if ($TemplateExplain.template -ne "ai-memory") {
    throw "template explain returned unexpected template $($TemplateExplain.template)"
}
if (-not $TemplateExplain.explain.validation.valid) {
    throw "template explain returned invalid validation report"
}

$InitSummary = Invoke-JsonCommand `
    -Label "multidb init --guided --template ai-memory" `
    -Arguments @(
        "init",
        "--guided",
        "--template",
        "ai-memory",
        "--name",
        "Preview Smoke",
        "--out",
        $ResolvedWorkDir,
        "--force",
        "--json"
    ) `
    -Binary $Binary
if (-not $InitSummary.valid) {
    throw "guided template init returned invalid summary"
}

$SpecPath = Join-Path $ResolvedWorkDir "multidb.yaml"
$SeedPath = Join-Path $ResolvedWorkDir "seed.json"
foreach ($Artifact in @($SpecPath, $SeedPath, (Join-Path $ResolvedWorkDir "README.md"), (Join-Path $ResolvedWorkDir "smoke.ps1"))) {
    if (-not (Test-Path $Artifact)) {
        throw "missing preview artifact $Artifact"
    }
}
Get-Content $SeedPath -Raw | ConvertFrom-Json | Out-Null

$Validation = Invoke-JsonCommand `
    -Label "multidb config validate --json" `
    -Arguments @("config", "validate", "--spec", $SpecPath, "--json") `
    -Binary $Binary
if (-not $Validation.valid) {
    throw "generated preview spec did not validate"
}

$Explain = Invoke-JsonCommand `
    -Label "multidb config explain --json" `
    -Arguments @("config", "explain", "--spec", $SpecPath, "--json") `
    -Binary $Binary
if (-not $Explain.validation.valid) {
    throw "generated preview spec explain was invalid"
}

$Summary = [ordered]@{
    schema_version = 1
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    binary = $Binary
    template = "ai-memory"
    template_count = $Templates.Count
    workspace = $ResolvedWorkDir
    artifacts = @("multidb.yaml", "README.md", "seed.json", "smoke.ps1")
    init = $InitSummary
    validation = [ordered]@{
        valid = $Validation.valid
        status = [string] $Validation.status
    }
    explain = [ordered]@{
        valid = $Explain.validation.valid
        status = [string] $Explain.validation.status
        decisions = @($Explain.decisions).Count
    }
}

$Summary | ConvertTo-Json -Depth 20 | Set-Content -Path $ResolvedOutput -Encoding utf8
"preview smoke ok: $ResolvedOutput"
