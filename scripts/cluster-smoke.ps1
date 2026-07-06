$ErrorActionPreference = "Stop"

if (-not $env:RUST_MIN_STACK) {
    $env:RUST_MIN_STACK = "33554432"
}

$tests = @(
    "phase45_cluster_ga_transfers_leader_and_preserves_writes",
    "phase45_cluster_ga_rejects_minority_write_after_quorum_loss",
    "phase45_cluster_ga_persists_membership_metadata_and_read_index"
)
$timeoutSeconds = 180

function Stop-ProcessTree {
    param(
        [Parameter(Mandatory = $true)]
        [int] $ProcessId
    )

    Get-CimInstance Win32_Process |
        Where-Object { $_.ParentProcessId -eq $ProcessId } |
        ForEach-Object { Stop-ProcessTree -ProcessId $_.ProcessId }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

foreach ($test in $tests) {
    $arguments = @("test", "--lib", "--all-features", $test, "--", "--ignored", "--nocapture")
    $process = [System.Diagnostics.Process]::new()
    $process.StartInfo.FileName = "cargo"
    $process.StartInfo.Arguments = $arguments -join " "
    $process.StartInfo.UseShellExecute = $false
    [void] $process.Start()
    if (-not $process.WaitForExit($timeoutSeconds * 1000)) {
        Stop-ProcessTree -ProcessId $process.Id
        throw "cluster smoke timed out at $test after $timeoutSeconds seconds"
    }
    if ($process.ExitCode -ne 0) {
        throw "cluster smoke failed at $test with exit code $($process.ExitCode)"
    }
}

Write-Host "cluster smoke passed"
