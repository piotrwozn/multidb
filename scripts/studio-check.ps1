$ErrorActionPreference = "Stop"

$RootDir = Join-Path $PSScriptRoot ".."
$StudioDir = Join-Path $RootDir "studio"
$TypescriptSdkDir = Join-Path $RootDir "sdk\typescript"

function Invoke-Npm {
    param(
        [Parameter(Mandatory = $true)]
        [string[]] $Arguments
    )

    & npm @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "npm $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

Push-Location $TypescriptSdkDir
try {
    Invoke-Npm -Arguments @("ci")
    Invoke-Npm -Arguments @("run", "build")
}
finally {
    Pop-Location
}

Push-Location $StudioDir
try {
    Invoke-Npm -Arguments @("ci")
    Invoke-Npm -Arguments @("run", "lint")
    Invoke-Npm -Arguments @("run", "typecheck")
    Invoke-Npm -Arguments @("test")
    Invoke-Npm -Arguments @("run", "build")
}
finally {
    Pop-Location
}
