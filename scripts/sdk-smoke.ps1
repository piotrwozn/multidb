param(
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

Push-Location (Join-Path $PSScriptRoot "..")
try {
    Invoke-Checked "OpenAPI contract tests" {
        cargo test --lib phase52 -- --nocapture
    }

    Push-Location "sdk/typescript"
    try {
        Invoke-Checked "TypeScript SDK npm ci" { npm ci }
        Invoke-Checked "TypeScript SDK build" { npm run build }
        Invoke-Checked "TypeScript SDK tests" { npm test }
        Invoke-Checked "TypeScript SDK pack dry-run" { npm pack --dry-run }
    } finally {
        Pop-Location
    }

    Push-Location "sdk/python"
    try {
        $oldPythonPath = $env:PYTHONPATH
        $env:PYTHONPATH = (Join-Path (Get-Location) "src")
        Invoke-Checked "Python SDK unit tests" {
            python -m unittest discover -s tests
        }
        Invoke-Checked "Python SDK wheel build" {
            python -m pip wheel . -w dist --no-deps
        }
        $env:PYTHONPATH = $oldPythonPath
    } finally {
        Pop-Location
    }

    Invoke-Checked "Rust SDK tests" {
        cargo test --manifest-path sdk/rust/Cargo.toml
    }
    Invoke-Checked "Rust SDK package dry-run" {
        cargo package --manifest-path sdk/rust/Cargo.toml --allow-dirty
    }

    if (Get-Command go -ErrorAction SilentlyContinue) {
        Push-Location "sdk/go"
        try {
            Invoke-Checked "Go SDK tests" { go test ./... }
        } finally {
            Pop-Location
        }
    } elseif ($RequireGo) {
        throw "go is required for SDK smoke in this environment"
    } else {
        Write-Host "==> Go SDK tests skipped; go toolchain not found"
    }
} finally {
    Pop-Location
}
