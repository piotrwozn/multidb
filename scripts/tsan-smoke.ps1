$ErrorActionPreference = "Stop"

if ($IsWindows) {
    Write-Host "ThreadSanitizer smoke is Linux-only; skipping on Windows"
    exit 0
}

rustc +nightly --version
if ($LASTEXITCODE -ne 0) {
    Write-Host "nightly Rust is not installed; skipping ThreadSanitizer smoke"
    exit 0
}

$oldRustFlags = $env:RUSTFLAGS
$env:RUSTFLAGS = "-Zsanitizer=thread"
try {
    cargo +nightly test --lib repl::tests::group_commit_batches_concurrent_proposals --target x86_64-unknown-linux-gnu
    if ($LASTEXITCODE -ne 0) {
        throw "ThreadSanitizer smoke failed"
    }
}
finally {
    $env:RUSTFLAGS = $oldRustFlags
}
