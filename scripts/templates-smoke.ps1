$ErrorActionPreference = "Stop"

$RepoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$TemplateNames = @(
    "game-save",
    "desktop-embedded",
    "ai-memory",
    "secure-saas",
    "analytics"
)

function Invoke-Checked {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [scriptblock] $Command
    )

    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "$Label failed with exit code $LASTEXITCODE"
    }
}

function Resolve-MultiDbBinary {
    Push-Location $RepoRoot
    try {
        Invoke-Checked -Label "cargo build --bin multidb" -Command {
            cargo build --bin multidb
        }
    }
    finally {
        Pop-Location
    }

    $IsWindowsHost = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
        [System.Runtime.InteropServices.OSPlatform]::Windows
    )
    $ExeName = if ($IsWindowsHost) { "multidb.exe" } else { "multidb" }
    $Bin = Join-Path (Join-Path $RepoRoot "target") (Join-Path "debug" $ExeName)
    if (-not (Test-Path $Bin)) {
        throw "missing multidb binary at $Bin"
    }
    $Bin
}

function Test-TemplateDirectory {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Bin,
        [Parameter(Mandatory = $true)]
        [string] $TemplateDir
    )

    $Spec = Join-Path $TemplateDir "multidb.yaml"
    $Seed = Join-Path $TemplateDir "seed.json"
    $Readme = Join-Path $TemplateDir "README.md"
    $Smoke = Join-Path $TemplateDir "smoke.ps1"

    foreach ($Path in @($Spec, $Seed, $Readme, $Smoke)) {
        if (-not (Test-Path $Path)) {
            throw "missing template artifact $Path"
        }
    }

    Invoke-Checked -Label "template validate $TemplateDir" -Command {
        & $Bin config validate --spec $Spec
    }

    $ExplainJson = & $Bin config explain --spec $Spec --json
    if ($LASTEXITCODE -ne 0) {
        throw "template explain $TemplateDir failed with exit code $LASTEXITCODE"
    }
    $ExplainJson | ConvertFrom-Json | Out-Null
    Get-Content $Seed -Raw | ConvertFrom-Json | Out-Null
}

$Bin = Resolve-MultiDbBinary
$ExamplesRoot = Join-Path $RepoRoot "examples"

foreach ($Template in $TemplateNames) {
    Test-TemplateDirectory -Bin $Bin -TemplateDir (Join-Path $ExamplesRoot $Template)
}

$TargetRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "target"))
if (-not $TargetRoot.EndsWith([System.IO.Path]::DirectorySeparatorChar)) {
    $TargetRoot = "$TargetRoot$([System.IO.Path]::DirectorySeparatorChar)"
}
$SmokeRoot = [System.IO.Path]::GetFullPath((Join-Path $RepoRoot "target\template-smoke"))
if (-not $SmokeRoot.StartsWith($TargetRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
    throw "refusing to clear template smoke path outside target: $SmokeRoot"
}
if (Test-Path $SmokeRoot) {
    Remove-Item -LiteralPath $SmokeRoot -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $SmokeRoot | Out-Null

foreach ($Template in $TemplateNames) {
    $OutDir = Join-Path $SmokeRoot $Template
    $SummaryJson = & $Bin init --guided --template $Template --name "Smoke $Template" --out $OutDir --force --json
    if ($LASTEXITCODE -ne 0) {
        throw "template init $Template failed with exit code $LASTEXITCODE"
    }
    $SummaryJson | ConvertFrom-Json | Out-Null
    Test-TemplateDirectory -Bin $Bin -TemplateDir $OutDir
}

"templates smoke ok"
