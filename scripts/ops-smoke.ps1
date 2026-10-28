param(
    [string] $Output = "",
    [string] $Helm = "helm",
    [string] $Kubeconform = "kubeconform",
    [string] $KubernetesVersion = "1.31.0"
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path

foreach ($Relative in @(
    "ops/kind/multidb-kind.yaml", "ops/helm/multidb/Chart.yaml",
    "ops/helm/multidb/values.yaml", "ops/helm/multidb/values.schema.json",
    "ops/helm/multidb/values-production-large.yaml", "ops/helm/multidb/values-local.yaml",
    "ops/vault/dev-policy.hcl", "ops/minio/backup-target.env.example"
)) {
    if (-not (Test-Path -LiteralPath (Join-Path $Root $Relative))) {
        throw "missing operator artifact: $Relative"
    }
}

if (-not (Get-Command $Helm -ErrorAction SilentlyContinue)) {
    throw "Helm is required: static template text does not validate a deployment"
}
if (-not (Get-Command $Kubeconform -ErrorAction SilentlyContinue)) {
    throw "kubeconform is required for rendered Kubernetes schema validation"
}
$Python = Get-Command python3 -ErrorAction SilentlyContinue
if ($null -eq $Python) { $Python = Get-Command python -ErrorAction SilentlyContinue }
if ($null -eq $Python) { throw "Python3 and qualification requirements are required" }

if (-not $Output) {
    $RunSuffix = [Guid]::NewGuid().ToString("N")
    $Output = Join-Path $Root "target/qualification/kubernetes-ops-$RunSuffix"
}
$Validator = Join-Path $Root "scripts/qualification/kubernetes.py"
if (-not (Test-Path -LiteralPath $Validator)) { throw "missing Kubernetes qualification runner" }
& $Python.Source $Validator --chart (Join-Path $Root "ops/helm/multidb") --output $Output --helm $Helm --kubeconform $Kubeconform --kubernetes-version $KubernetesVersion
if ($LASTEXITCODE -ne 0) { throw "rendered operator qualification failed; inspect $Output" }
Write-Host "rendered operator/schema qualification accepted; live storage/CNI/HA acceptance is separate"
