param(
    [string] $Output = "",
    [string] $Helm = "helm",
    [string] $Kubeconform = "kubeconform",
    [string] $KubernetesVersion = "1.31.0"
)

$ErrorActionPreference = "Stop"
$Run = Join-Path $PSScriptRoot "ops-smoke.ps1"
# Exercise the real default/existingClaim/schema/negative rendering contract.
# Recreate and a retained existing claim protect controller replacement; this
# command does not assert on-disk downgrade or live node-failure qualification.
& $Run -Output $Output -Helm $Helm -Kubeconform $Kubeconform -KubernetesVersion $KubernetesVersion
if ($LASTEXITCODE -ne 0) { throw "rendered upgrade/storage contract failed" }
Write-Host "rendered single-writer upgrade/storage contract accepted; runtime upgrade/restore drill remains required"
