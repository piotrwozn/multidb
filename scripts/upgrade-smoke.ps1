$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Chart = Join-Path $Root "ops/helm/multidb"
$Deployment = Get-Content (Join-Path $Chart "templates/deployment.yaml") -Raw

if (-not $Deployment.Contains("multidb.io/upgrade-policy: rolling-forward")) {
    throw "rolling upgrade annotation missing"
}

if (-not $Deployment.Contains("multidb.io/downgrade-policy: reject-unless-explicit-rollback")) {
    throw "downgrade rejection annotation missing"
}

if (Get-Command helm -ErrorAction SilentlyContinue) {
    & helm lint $Chart
    if ($LASTEXITCODE -ne 0) {
        throw "helm lint failed with exit code $LASTEXITCODE"
    }
} else {
    Write-Host "helm not installed; static upgrade/downgrade smoke only"
}

Write-Host "upgrade/downgrade smoke passed"
