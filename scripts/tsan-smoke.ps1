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

# -Zsanitizer changes the ABI, so nightly refuses to link instrumented crates
# against the prebuilt std. -Zbuild-std rebuilds std with the sanitizer, which
# needs the rust-src component.
$installed = rustup component list --toolchain nightly --installed
if (-not ($installed -match "^rust-src")) {
    throw "ThreadSanitizer smoke needs the nightly rust-src component: rustup component add rust-src --toolchain nightly"
}

$oldRustFlags = $env:RUSTFLAGS
$env:RUSTFLAGS = "-Zsanitizer=thread"
try {
    cargo +nightly test -Zbuild-std --lib repl::tests::group_commit_batches_concurrent_proposals --target x86_64-unknown-linux-gnu
    if ($LASTEXITCODE -ne 0) {
        throw "ThreadSanitizer smoke failed"
    }
}
finally {
    $env:RUSTFLAGS = $oldRustFlags
}
