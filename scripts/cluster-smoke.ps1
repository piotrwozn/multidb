$ErrorActionPreference = "Stop"

if (-not $env:RUST_MIN_STACK) {
    $env:RUST_MIN_STACK = "33554432"
}

$tests = @(
    "internal_transport::process_smoke::mtls_three_process_failover_and_quorum_smoke",
    "repl::cp::tests::phase45_cluster_ga_transfers_leader_and_preserves_writes",
    "repl::cp::tests::phase45_cluster_ga_rejects_minority_write_after_quorum_loss",
    "repl::cp::tests::phase45_cluster_ga_persists_membership_metadata_and_read_index"
)
$timeoutSeconds = 180

function Stop-ProcessTree {
    param(
        [Parameter(Mandatory = $true)]
        [int] $ProcessId
    )

    $target = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
    if ($null -eq $target) {
        return
    }
    # PowerShell 7 / modern .NET supports tree termination on Windows and Unix.
    # Keep the CIM fallback for Windows PowerShell on .NET Framework.
    $killTree = $target.GetType().GetMethod("Kill", [type[]] @([bool]))
    if ($null -ne $killTree) {
        try {
            $target.Kill($true)
        }
        catch [System.InvalidOperationException] {
            # The process can exit between Get-Process and Kill.
        }
        return
    }
    Get-CimInstance Win32_Process |
        Where-Object { $_.ParentProcessId -eq $ProcessId } |
        ForEach-Object { Stop-ProcessTree -ProcessId $_.ProcessId }
    Stop-Process -Id $ProcessId -Force -ErrorAction SilentlyContinue
}

$discovered = & cargo test --locked --lib --all-features -- --list
if ($LASTEXITCODE -ne 0) {
    throw "cluster smoke test discovery failed with exit code $LASTEXITCODE"
}
foreach ($test in $tests) {
    if (-not ($discovered -ccontains "${test}: test")) {
        throw "cluster smoke required test was not discovered: $test"
    }
    $arguments = @("test", "--locked", "--lib", "--all-features", $test, "--", "--exact", "--ignored", "--nocapture")
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
