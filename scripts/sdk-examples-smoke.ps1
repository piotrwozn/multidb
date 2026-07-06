param(
    [string] $ImageTag = $env:MULTIDB_SDK_EXAMPLES_IMAGE,
    [switch] $RequireGo
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

function Wait-ForControlPlane {
    param([string] $Url)

    $deadline = (Get-Date).AddSeconds(120)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri "$Url/health" -TimeoutSec 3
            if ($response.StatusCode -eq 200) {
                return
            }
        } catch {
            Start-Sleep -Seconds 2
        }
    }
    throw "Control Plane did not become healthy at $Url"
}

Push-Location (Join-Path $PSScriptRoot "..")
try {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw "docker is required for SDK examples smoke"
    }

    $project = "multidb-sdk-smoke-$PID"
    $baseUrl = "http://127.0.0.1:8080"
    $oldImage = $env:MULTIDB_IMAGE
    $env:MULTIDB_CONTROL_PLANE_URL = "$baseUrl/api"
    $env:MULTIDB_ADMIN_PASSWORD = "local-dev-admin-password"
    if (-not [string]::IsNullOrWhiteSpace($ImageTag)) {
        $env:MULTIDB_IMAGE = $ImageTag
    }

    try {
        Invoke-Checked "Docker Compose up for SDK examples" {
            $ComposeArgs = @("compose", "-p", $project, "up", "-d")
            if ([string]::IsNullOrWhiteSpace($ImageTag)) {
                $ComposeArgs = @("compose", "-p", $project, "up", "--build", "-d")
            } else {
                $ComposeArgs = @("compose", "-p", $project, "up", "--no-build", "-d")
            }
            docker @ComposeArgs
        }
        Wait-ForControlPlane -Url $baseUrl

        Push-Location "examples/sdk/typescript"
        try {
            Invoke-Checked "TypeScript SDK example install" { npm install }
            Invoke-Checked "TypeScript SDK example" { npm start }
        } finally {
            Pop-Location
        }

        $oldPythonPath = $env:PYTHONPATH
        $env:PYTHONPATH = (Resolve-Path "sdk/python/src").Path
        Invoke-Checked "Python SDK example" {
            python examples/sdk/python/example.py
        }
        $env:PYTHONPATH = $oldPythonPath

        if (Get-Command go -ErrorAction SilentlyContinue) {
            Push-Location "examples/sdk/go"
            try {
                Invoke-Checked "Go SDK example" { go run . }
            } finally {
                Pop-Location
            }
        } elseif ($RequireGo) {
            throw "go is required for SDK examples smoke in this environment"
        } else {
            Write-Host "==> Go SDK example skipped; go toolchain not found"
        }

        Invoke-Checked "Rust SDK example" {
            cargo run --manifest-path examples/sdk/rust/Cargo.toml
        }
    } finally {
        docker compose -p $project down --volumes --remove-orphans
        if ($null -eq $oldImage) {
            Remove-Item Env:\MULTIDB_IMAGE -ErrorAction SilentlyContinue
        } else {
            $env:MULTIDB_IMAGE = $oldImage
        }
    }
} finally {
    Pop-Location
}
