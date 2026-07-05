$ErrorActionPreference = "Stop"

$Root = Resolve-Path (Join-Path $PSScriptRoot "..")
$Required = @(
    "ops/kind/multidb-kind.yaml",
    "ops/helm/multidb/Chart.yaml",
    "ops/helm/multidb/values.yaml",
    "ops/helm/multidb/templates/deployment.yaml",
    "ops/helm/multidb/templates/service.yaml",
    "ops/helm/multidb/templates/pvc.yaml",
    "ops/helm/multidb/templates/configmap.yaml",
    "ops/vault/dev-policy.hcl",
    "ops/minio/backup-target.env.example"
)

foreach ($Relative in $Required) {
    $Path = Join-Path $Root $Relative
    if (-not (Test-Path $Path)) {
        throw "missing operator artifact: $Relative"
    }
}

$Deployment = Get-Content (Join-Path $Root "ops/helm/multidb/templates/deployment.yaml") -Raw
foreach ($Token in @("readinessProbe:", "livenessProbe:", "RollingUpdate", "downgrade-policy", "secretKeyRef:", "persistentVolumeClaim:", "MULTIDB_ADMIN_PASSWORD", "MULTIDB_ADMIN_TOKEN", "MULTIDB_PG_PASSWORD", "MULTIDB_PG_TLS_CERT")) {
    if (-not $Deployment.Contains($Token)) {
        throw "deployment template missing required token: $Token"
    }
}

$Values = Get-Content (Join-Path $Root "ops/helm/multidb/values.yaml") -Raw
foreach ($Token in @("vaultAddress", "minioEndpoint", "auditJsonlPath", "backupTarget", "runtime:", "persistence:", "adminPassword:", "adminToken:", "pgPassword:", "pgTls:")) {
    if (-not $Values.Contains($Token)) {
        throw "values.yaml missing required token: $Token"
    }
}

$ConfigMap = Get-Content (Join-Path $Root "ops/helm/multidb/templates/configmap.yaml") -Raw
foreach ($Token in @("MULTIDB_RUNTIME_MODE", "MULTIDB_DB_PATH", "MULTIDB_BIND", "MULTIDB_PG_BIND", "MULTIDB_STUDIO_DIR")) {
    if (-not $ConfigMap.Contains($Token)) {
        throw "configmap template missing required token: $Token"
    }
}

$Pvc = Get-Content (Join-Path $Root "ops/helm/multidb/templates/pvc.yaml") -Raw
foreach ($Token in @("PersistentVolumeClaim", "storageClassName", "accessModes")) {
    if (-not $Pvc.Contains($Token)) {
        throw "pvc template missing required token: $Token"
    }
}

$Vault = Get-Content (Join-Path $Root "ops/vault/dev-policy.hcl") -Raw
foreach ($Token in @("transit/encrypt", "transit/decrypt", "transit/rewrap", "secret/data/multidb/kek")) {
    if (-not $Vault.Contains($Token)) {
        throw "vault dev policy missing required token: $Token"
    }
}

Write-Host "operator artifact smoke passed"
