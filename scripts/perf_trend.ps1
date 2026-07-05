param(
    [Parameter(Mandatory = $true)]
    [string[]] $Reports,
    [string] $Output = "target/perf/trend.json",
    [int] $FlatPercent = 10
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

function Read-Report($Path) {
    if (!(Test-Path $Path)) {
        throw "missing performance report: $Path"
    }

    $document = Get-Content -Raw $Path | ConvertFrom-Json
    $properties = @()
    if ($document -isnot [System.Array]) {
        $properties = @($document.PSObject.Properties.Name)
    }

    if ($properties -contains "benchmarks") {
        return [pscustomobject]@{
            path = $Path
            profile = $document.profile
            generated_at_utc = $document.generated_at_utc
            benchmarks = Convert-ToArray $document.benchmarks
        }
    }

    return [pscustomobject]@{
        path = $Path
        profile = $null
        generated_at_utc = $null
        benchmarks = Convert-ToArray $document
    }
}

function Get-PercentDelta($First, $Last) {
    if ([double]$First -le 0) {
        return $null
    }
    return [Math]::Round((([double]$Last - [double]$First) / [double]$First) * 100.0, 3)
}

function Get-TrendStatus($First, $Last, [int] $Threshold) {
    $throughputDelta = Get-PercentDelta $First.throughput_ops_per_sec $Last.throughput_ops_per_sec
    $p95Delta = Get-PercentDelta $First.p95_ms $Last.p95_ms

    if (($null -ne $throughputDelta -and $throughputDelta -lt -$Threshold) -or ($null -ne $p95Delta -and $p95Delta -gt $Threshold)) {
        return "regressed"
    }
    if (($null -ne $throughputDelta -and $throughputDelta -gt $Threshold) -or ($null -ne $p95Delta -and $p95Delta -lt -$Threshold)) {
        return "improved"
    }
    return "flat"
}

$loadedReports = @($Reports | ForEach-Object { Read-Report $_ })
$byBenchmark = @{}

foreach ($report in $loadedReports) {
    foreach ($benchmark in $report.benchmarks) {
        if (!$byBenchmark.ContainsKey($benchmark.name)) {
            $byBenchmark[$benchmark.name] = @()
        }
        $byBenchmark[$benchmark.name] += [pscustomobject]@{
            report = $report
            benchmark = $benchmark
        }
    }
}

$trends = @()
foreach ($name in ($byBenchmark.Keys | Sort-Object)) {
    $points = @($byBenchmark[$name])
    if ($points.Count -lt 2) {
        continue
    }

    $first = $points[0].benchmark
    $last = $points[$points.Count - 1].benchmark
    $trends += [pscustomobject]@{
        name = $name
        first_report = $points[0].report.path
        last_report = $points[$points.Count - 1].report.path
        first_throughput_ops_per_sec = [double]$first.throughput_ops_per_sec
        last_throughput_ops_per_sec = [double]$last.throughput_ops_per_sec
        throughput_delta_percent = Get-PercentDelta $first.throughput_ops_per_sec $last.throughput_ops_per_sec
        first_p95_ms = [double]$first.p95_ms
        last_p95_ms = [double]$last.p95_ms
        p95_delta_percent = Get-PercentDelta $first.p95_ms $last.p95_ms
        status = Get-TrendStatus $first $last $FlatPercent
    }
}

New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Output) | Out-Null
$summary = [ordered]@{
    schema_version = 1
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    flat_percent = $FlatPercent
    reports = @($loadedReports | ForEach-Object {
        [ordered]@{
            path = $_.path
            profile = $_.profile
            generated_at_utc = $_.generated_at_utc
        }
    })
    benchmarks = $trends
}

$summary | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $Output
Write-Host "Wrote performance trend report to $Output"
