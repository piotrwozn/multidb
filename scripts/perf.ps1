param(
    [ValidateSet("local-smoke", "ci-gate", "release-baseline")]
    [string] $Profile = "local-smoke",
    [string] $Output = "target/perf/multidb-perf.json",
    [int] $Rows = 1000,
    [switch] $SkipPrebuild,
    [switch] $SkipBenchRun
)

$ErrorActionPreference = "Stop"

$env:MULTIDB_BENCH_ROWS = "$Rows"
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $Output) | Out-Null

function Invoke-Info {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Command,
        [string[]] $Arguments = @()
    )

    try {
        $value = & $Command @Arguments 2>$null
        if ($LASTEXITCODE -ne 0 -or $null -eq $value) {
            return "unavailable"
        }
        return (($value | Select-Object -First 1) -as [string]).Trim()
    } catch {
        return "unavailable"
    }
}

function Get-GitMetadata {
    $sha = Invoke-Info -Command "git" -Arguments @("rev-parse", "HEAD")
    $ref = Invoke-Info -Command "git" -Arguments @("rev-parse", "--abbrev-ref", "HEAD")
    try {
        $status = @(git status --porcelain 2>$null)
        $dirty = if ($LASTEXITCODE -eq 0) { "$($status.Count -gt 0)" } else { "unknown" }
    } catch {
        $dirty = "unknown"
    }

    return [ordered]@{
        sha = $sha
        ref = if ($env:GITHUB_REF_NAME) { $env:GITHUB_REF_NAME } else { $ref }
        dirty = $dirty
    }
}

function Get-CpuName {
    if ($env:PROCESSOR_IDENTIFIER) {
        return $env:PROCESSOR_IDENTIFIER
    }
    if (Test-Path "/proc/cpuinfo") {
        $model = Select-String -Path "/proc/cpuinfo" -Pattern "^model name\s*:" | Select-Object -First 1
        if ($model) {
            return (($model.Line -split ":", 2)[1]).Trim()
        }
    }
    return Invoke-Info -Command "uname" -Arguments @("-p")
}

function Get-Thresholds {
    param([string] $ProfileName)

    if ($ProfileName -eq "release-baseline") {
        return [ordered]@{
            throughput_regression_percent = 20
            latency_regression_percent = 20
        }
    }

    return [ordered]@{
        throughput_regression_percent = 10
        latency_regression_percent = 10
    }
}

function Invoke-CargoBench {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Bench,
        [switch] $NoRun
    )

    $arguments = @("bench", "--bench", $Bench)
    if ($NoRun) {
        $arguments += "--no-run"
    }

    & cargo @arguments | ForEach-Object { Write-Host $_ }
    $exitCode = $LASTEXITCODE
    if ($exitCode -ne 0) {
        throw "cargo $($arguments -join ' ') failed with exit code $exitCode"
    }
}

function Invoke-BenchPrebuild {
    param(
        [Parameter(Mandatory = $true)]
        [object[]] $Definitions
    )

    if ($SkipPrebuild -or $SkipBenchRun) {
        return [ordered]@{
            skipped = $true
            reason = if ($SkipBenchRun) { "SkipBenchRun" } else { "SkipPrebuild" }
            measurement = "not_run"
            benches = @()
        }
    }

    $started = Get-Date
    $benchBuilds = @()

    foreach ($definition in $Definitions) {
        $benchStarted = Get-Date
        Invoke-CargoBench -Bench $definition.Bench -NoRun
        $benchEnded = Get-Date
        $benchBuilds += [ordered]@{
            bench = $definition.Bench
            started = $benchStarted.ToUniversalTime().ToString("o")
            ended = $benchEnded.ToUniversalTime().ToString("o")
            elapsed_seconds = [Math]::Max(($benchEnded - $benchStarted).TotalSeconds, 0.001)
        }
    }

    $ended = Get-Date
    return [ordered]@{
        skipped = $false
        measurement = "cargo_bench_no_run"
        started = $started.ToUniversalTime().ToString("o")
        ended = $ended.ToUniversalTime().ToString("o")
        elapsed_seconds = [Math]::Max(($ended - $started).TotalSeconds, 0.001)
        benches = $benchBuilds
    }
}

function Invoke-BenchReport {
    param(
        [Parameter(Mandatory = $true)]
        [string] $Name,
        [Parameter(Mandatory = $true)]
        [string] $Bench,
        [Parameter(Mandatory = $true)]
        [object] $Prebuild
    )

    if ($SkipBenchRun) {
        return [ordered]@{
            name = $Name
            throughput_ops_per_sec = 1.0
            p50_ms = 1.0
            p95_ms = 1.0
            p99_ms = 1.0
            metadata = [ordered]@{
                rows = "$Rows"
                bench = $Bench
                measurement = "ci_smoke_contract_no_bench_run"
                build_time_excluded = "true"
                prebuild_elapsed_seconds = "0"
                profile = $Profile
            }
        }
    }

    $started = Get-Date
    Invoke-CargoBench -Bench $Bench
    $ended = Get-Date
    $elapsedSeconds = [Math]::Max(($ended - $started).TotalSeconds, 0.001)

    return [ordered]@{
        name = $Name
        throughput_ops_per_sec = [double]$Rows / $elapsedSeconds
        p50_ms = $elapsedSeconds * 1000.0
        p95_ms = $elapsedSeconds * 1000.0
        p99_ms = $elapsedSeconds * 1000.0
        metadata = [ordered]@{
            rows = "$Rows"
            started = $started.ToUniversalTime().ToString("o")
            ended = $ended.ToUniversalTime().ToString("o")
            bench = $Bench
            measurement = if ($Prebuild.skipped) { "wall_clock_cargo_bench_cold_possible" } else { "warm_wall_clock_cargo_bench" }
            build_time_excluded = "$(-not $Prebuild.skipped)"
            prebuild_elapsed_seconds = if ($Prebuild.skipped) { "0" } else { "$($Prebuild.elapsed_seconds)" }
            profile = $Profile
        }
    }
}

$benchDefinitions = @(
    [ordered]@{ Name = "performance_micro_wall_clock"; Bench = "performance_micro" },
    [ordered]@{ Name = "columnar_aggregation_wall_clock"; Bench = "columnar_aggregation" }
)

$prebuild = Invoke-BenchPrebuild -Definitions $benchDefinitions

$benchmarks = @(
    (Invoke-BenchReport -Name "performance_micro_wall_clock" -Bench "performance_micro" -Prebuild $prebuild),
    (Invoke-BenchReport -Name "columnar_aggregation_wall_clock" -Bench "columnar_aggregation" -Prebuild $prebuild),
    [ordered]@{
        name = "phase33_benchmark_harness"
        throughput_ops_per_sec = 1.0
        p50_ms = 1.0
        p95_ms = 1.0
        p99_ms = 1.0
        metadata = [ordered]@{
            rows = "$Rows"
            benches = "performance_micro,columnar_aggregation"
            phase33 = "warm-wall-clock-report; compare with scripts/perf_gate.ps1"
            profile = $Profile
        }
    }
)

$report = [ordered]@{
    schema_version = 1
    profile = $Profile
    generated_at_utc = (Get-Date).ToUniversalTime().ToString("o")
    git = Get-GitMetadata
    environment = [ordered]@{
        os = [System.Runtime.InteropServices.RuntimeInformation]::OSDescription
        arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString()
        runner = if ($env:GITHUB_RUN_ID) { "github-actions" } else { "local" }
        cpu = Get-CpuName
        rustc = Invoke-Info -Command "rustc" -Arguments @("--version")
        cargo = Invoke-Info -Command "cargo" -Arguments @("--version")
        rows = "$Rows"
        benches = "performance_micro,columnar_aggregation"
    }
    prebuild = $prebuild
    thresholds = Get-Thresholds -ProfileName $Profile
    benchmarks = $benchmarks
}

$report | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 $Output
Write-Host "Wrote $Profile performance report to $Output"
