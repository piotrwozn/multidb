param(
    [string] $Baseline,
    [string] $Candidate,
    [int] $ThresholdPercent = -1,
    [switch] $AllowProfileMismatch,
    [string] $SummaryOutput,
    [switch] $SelfTest
)

$ErrorActionPreference = "Stop"

function Convert-ToArray($Value) {
    if ($null -eq $Value) {
        return @()
    }
    if ($Value -is [System.Array]) {
        return $Value
    }
    return @($Value)
}

function New-Thresholds {
    param(
        [int] $Throughput,
        [int] $Latency
    )

    return [pscustomobject]@{
        throughput_regression_percent = $Throughput
        latency_regression_percent = $Latency
    }
}

function Read-Report($Path) {
    if (!(Test-Path $Path)) {
        throw "missing performance report: $Path"
    }

    $document = Get-Content -Raw $Path | ConvertFrom-Json
    if ($null -eq $document) {
        return [pscustomobject]@{
            path = $Path
            schema_version = 0
            profile = $null
            thresholds = (New-Thresholds -Throughput 10 -Latency 10)
            benchmarks = @()
        }
    }

    $properties = @()
    if ($document -isnot [System.Array]) {
        $properties = @($document.PSObject.Properties.Name)
    }

    if ($properties -contains "benchmarks") {
        $thresholds = $document.thresholds
        if ($null -eq $thresholds) {
            $thresholds = New-Thresholds -Throughput 10 -Latency 10
        }
        return [pscustomobject]@{
            path = $Path
            schema_version = $document.schema_version
            profile = $document.profile
            thresholds = $thresholds
            benchmarks = Convert-ToArray $document.benchmarks
        }
    }

    return [pscustomobject]@{
        path = $Path
        schema_version = 0
        profile = $null
        thresholds = (New-Thresholds -Throughput 10 -Latency 10)
        benchmarks = Convert-ToArray $document
    }
}

function Get-EffectiveThresholds($BaselineReport) {
    if ($ThresholdPercent -ge 0) {
        return New-Thresholds -Throughput $ThresholdPercent -Latency $ThresholdPercent
    }

    if ($null -ne $BaselineReport.thresholds) {
        return $BaselineReport.thresholds
    }

    return New-Thresholds -Throughput 10 -Latency 10
}

function Test-ThroughputRegression {
    param($BaselineValue, $CandidateValue, [int] $Threshold)

    if ([double]$BaselineValue -le 0) {
        return $false
    }
    $allowed = [double]$BaselineValue * (1.0 - ($Threshold / 100.0))
    return [double]$CandidateValue -lt $allowed
}

function Test-LatencyRegression {
    param($BaselineValue, $CandidateValue, [int] $Threshold)

    if ([double]$BaselineValue -le 0) {
        return $false
    }
    $allowed = [double]$BaselineValue * (1.0 + ($Threshold / 100.0))
    return [double]$CandidateValue -gt $allowed
}

function Compare-Reports($BaselineReport, $CandidateReport) {
    if (!$AllowProfileMismatch -and $BaselineReport.profile -and $CandidateReport.profile -and $BaselineReport.profile -ne $CandidateReport.profile) {
        throw "performance profile mismatch: baseline=$($BaselineReport.profile), candidate=$($CandidateReport.profile)"
    }

    $thresholds = Get-EffectiveThresholds $BaselineReport
    $candidateByName = @{}
    foreach ($item in $CandidateReport.benchmarks) {
        $candidateByName[$item.name] = $item
    }

    $failures = @()
    $comparisons = @()

    foreach ($base in $BaselineReport.benchmarks) {
        if (!$candidateByName.ContainsKey($base.name)) {
            $failures.Add("missing benchmark $($base.name)")
            continue
        }

        $current = $candidateByName[$base.name]
        $benchmarkFailures = @()

        if (Test-ThroughputRegression $base.throughput_ops_per_sec $current.throughput_ops_per_sec $thresholds.throughput_regression_percent) {
            $message = "$($base.name) throughput regressed"
            $failures += $message
            $benchmarkFailures += $message
        }

        foreach ($field in @("p50_ms", "p95_ms", "p99_ms")) {
            if (Test-LatencyRegression $base.$field $current.$field $thresholds.latency_regression_percent) {
                $message = "$($base.name) $field regressed"
                $failures += $message
                $benchmarkFailures += $message
            }
        }

        $comparisons += [pscustomobject]@{
            name = $base.name
            baseline_throughput_ops_per_sec = [double]$base.throughput_ops_per_sec
            candidate_throughput_ops_per_sec = [double]$current.throughput_ops_per_sec
            baseline_p95_ms = [double]$base.p95_ms
            candidate_p95_ms = [double]$current.p95_ms
            status = if ($benchmarkFailures.Count -gt 0) { "regressed" } else { "passed" }
            failures = $benchmarkFailures
        }
    }

    return [pscustomobject]@{
        thresholds = $thresholds
        failures = $failures
        comparisons = $comparisons
    }
}

function Write-Summary($BaselineReport, $CandidateReport, $Result) {
    if ([string]::IsNullOrWhiteSpace($SummaryOutput)) {
        return
    }

    New-Item -ItemType Directory -Force -Path (Split-Path -Parent $SummaryOutput) | Out-Null
    $summary = [ordered]@{
        schema_version = 1
        generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
        baseline = $BaselineReport.path
        candidate = $CandidateReport.path
        profile = if ($BaselineReport.profile) { $BaselineReport.profile } else { $CandidateReport.profile }
        thresholds = $Result.thresholds
        status = if ($Result.failures.Count -eq 0) { "passed" } else { "failed" }
        failures = $Result.failures
        benchmarks = $Result.comparisons
    }
    $summary | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $SummaryOutput
    Write-Host "Wrote performance gate summary to $SummaryOutput"
}

if ($SelfTest) {
    $baselineItems = @(
        [pscustomobject]@{
            name = "synthetic_regression"
            throughput_ops_per_sec = 100.0
            p50_ms = 10.0
            p95_ms = 10.0
            p99_ms = 10.0
        }
    )
    $candidateItems = @(
        [pscustomobject]@{
            name = "synthetic_regression"
            throughput_ops_per_sec = 79.0
            p50_ms = 13.0
            p95_ms = 13.0
            p99_ms = 13.0
        }
    )
    $baselineReport = [pscustomobject]@{
        path = "self-test-baseline"
        schema_version = 1
        profile = "ci-gate"
        thresholds = (New-Thresholds -Throughput 20 -Latency 20)
        benchmarks = $baselineItems
    }
    $candidateReport = [pscustomobject]@{
        path = "self-test-candidate"
        schema_version = 1
        profile = "ci-gate"
        thresholds = (New-Thresholds -Throughput 20 -Latency 20)
        benchmarks = $candidateItems
    }
    $result = Compare-Reports $baselineReport $candidateReport
    if ($result.failures.Count -lt 4) {
        throw "performance gate self-test failed to catch synthetic 20% regression"
    }
    Write-Host "Performance gate self-test passed"
    exit 0
}

if ([string]::IsNullOrWhiteSpace($Baseline) -or [string]::IsNullOrWhiteSpace($Candidate)) {
    throw "usage: perf_gate.ps1 -Baseline <baseline.json> -Candidate <candidate.json> [-ThresholdPercent <n>] [-AllowProfileMismatch] [-SummaryOutput <summary.json>] or -SelfTest"
}

$baselineReport = Read-Report $Baseline
$candidateReport = Read-Report $Candidate
$result = Compare-Reports $baselineReport $candidateReport
Write-Summary $baselineReport $candidateReport $result

if ($result.failures.Count -gt 0) {
    $result.failures | ForEach-Object { Write-Error $_ }
    throw "performance gate failed"
}

Write-Host "Performance gate passed"
