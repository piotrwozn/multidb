$ErrorActionPreference = "Stop"

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Host "cargo is not installed; skipping Miri smoke"
    exit 0
}

cargo miri --version
if ($LASTEXITCODE -ne 0) {
    Write-Host "cargo-miri is not available; skipping Miri smoke"
    exit 0
}

$tests = @(
    "verification::phase33_tests::phase33_linearizability_checker_catches_split_brain_history",
    "verification::phase33_tests::phase33_row_oracles_detect_mismatches"
)

foreach ($test in $tests) {
    cargo miri test --lib $test
    if ($LASTEXITCODE -ne 0) {
        throw "cargo miri test --lib $test failed"
    }
}
