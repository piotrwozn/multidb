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

function Test-SpecFile {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Bin,
        [Parameter(Mandatory = $true)]
        [string] $Spec
    )

    if (-not (Test-Path $Spec)) {
        throw "missing spec file $Spec"
    }

    Invoke-Checked -Label "spec validate $Spec" -Command {
        & $Bin config validate --spec $Spec
    }

    $ExplainJson = & $Bin config explain --spec $Spec --json
    if ($LASTEXITCODE -ne 0) {
        throw "spec explain $Spec failed with exit code $LASTEXITCODE"
    }
    $ExplainJson | ConvertFrom-Json | Out-Null
}

function Invoke-Json {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Label,
        [Parameter(Mandatory = $true)]
        [int] $ExpectedExitCode,
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    $Raw = & $Bin @Arguments
    if ($LASTEXITCODE -ne $ExpectedExitCode) {
        throw "$Label exited with $LASTEXITCODE, expected ${ExpectedExitCode}: $($Raw -join "`n")"
    }
    ($Raw -join "`n") | ConvertFrom-Json
}

$Bin = Resolve-MultiDbBinary
$ExamplesRoot = Join-Path $RepoRoot "examples"

foreach ($Template in $TemplateNames) {
    Test-TemplateDirectory -Bin $Bin -TemplateDir (Join-Path $ExamplesRoot $Template)
}

Test-SpecFile -Bin $Bin -Spec (Join-Path (Join-Path $ExamplesRoot "custom-profile") "multidb.yaml")
$ProvisioningSpec = Join-Path (Join-Path $ExamplesRoot "provisioning") "multidb.yaml"
Test-SpecFile -Bin $Bin -Spec $ProvisioningSpec

# The frozen version 1 template specs keep validating and explaining.
$FrozenV1Root = Join-Path (Join-Path (Join-Path $RepoRoot "tests") "fixtures") "spec-v1"
$FrozenV1 = Get-ChildItem -Path $FrozenV1Root -Filter "*.yaml" |
    Where-Object { -not $_.BaseName.EndsWith(".reserialized") } |
    Sort-Object Name
if (@($FrozenV1).Count -lt 5) {
    throw "expected the five frozen version 1 template specs, found $(@($FrozenV1).Count)"
}
foreach ($Spec in $FrozenV1) {
    Test-SpecFile -Bin $Bin -Spec $Spec.FullName
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

# A dry run of the provisioning example creates no file.
$ProvisioningDb = Join-Path (Join-Path $SmokeRoot "provisioning") "orders.redb"
$Plan = Invoke-Json -Label "config plan examples/provisioning" -ExpectedExitCode 0 -Arguments @(
    "config", "plan", "--spec", $ProvisioningSpec, "--db", $ProvisioningDb, "--json"
)
if ($Plan.outcome -ne "ready") {
    throw "examples/provisioning plan is $($Plan.outcome), expected ready"
}
if (Test-Path $ProvisioningDb) {
    throw "config plan created $ProvisioningDb"
}

# init --apply builds every template that the CLI can enforce; secure-saas asks for encryption
# at rest, which only the Rust API provisions, so it is blocked (exit 2) with its files written.
foreach ($Template in $TemplateNames) {
    $OutDir = Join-Path $SmokeRoot "apply-$Template"
    $Blocked = $Template -eq "secure-saas"
    $Summary = Invoke-Json -Label "init --apply $Template" -ExpectedExitCode $(if ($Blocked) { 2 } else { 0 }) -Arguments @(
        "init", "--guided", "--template", $Template, "--name", "Smoke $Template",
        "--out", $OutDir, "--apply", "--json"
    )
    Test-TemplateDirectory -Bin $Bin -TemplateDir $OutDir
    $Spec = Join-Path $OutDir "multidb.yaml"
    $Db = [string] $Summary.database
    if ($Blocked) {
        if ($Summary.provision.status -ne "blocked") {
            throw "init --apply $Template reported $($Summary.provision.status), expected blocked"
        }
        $Errors = @($Summary.provision.plan.issues | Where-Object { $_.severity -eq "error" } | ForEach-Object { $_.code })
        if (($Errors.Count -ne 1) -or ($Errors[0] -ne "GUARANTEE_NOT_ENFORCEABLE")) {
            throw "init --apply $Template is blocked by $($Errors -join ','), expected GUARANTEE_NOT_ENFORCEABLE"
        }
        if (Test-Path $Db) {
            throw "a blocked init --apply created $Db"
        }
        continue
    }
    if ($Summary.provision.status -ne "applied") {
        throw "init --apply $Template reported $($Summary.provision.status), expected applied"
    }
    if (-not (Test-Path $Db)) {
        throw "init --apply $Template did not create $Db"
    }
    $Drift = Invoke-Json -Label "config drift $Template" -ExpectedExitCode 0 -Arguments @(
        "config", "drift", "--spec", $Spec, "--db", $Db, "--json"
    )
    if (-not $Drift.in_sync) {
        throw "config drift $Template is not in sync"
    }
    $Again = Invoke-Json -Label "config apply $Template" -ExpectedExitCode 0 -Arguments @(
        "config", "apply", "--spec", $Spec, "--db", $Db, "--auto-approve", "--json"
    )
    if ($Again.status -ne "noop") {
        throw "re-applying $Template reported $($Again.status), expected noop"
    }
}

"templates smoke ok"
