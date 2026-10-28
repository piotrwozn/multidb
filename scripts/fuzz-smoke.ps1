param([switch] $Required)

$ErrorActionPreference = "Stop"

function Skip-Or-Fail([string] $Reason) {
    if ($Required) { throw $Reason }
    Write-Host $Reason
    exit 0
}

if (-not (Get-Command cargo-fuzz -ErrorAction SilentlyContinue)) {
    Skip-Or-Fail "cargo-fuzz is not installed; install with: cargo install cargo-fuzz"
}

$cargoFuzzArgs = @("fuzz", "run")
$rustcVersion = rustc --version
if ($rustcVersion -notmatch "nightly") {
    cargo +nightly --version *> $null
    if ($LASTEXITCODE -ne 0) {
        Skip-Or-Fail "nightly toolchain is not available; skipping cargo-fuzz smoke on stable rustc"
    }
    $cargoFuzzArgs = @("+nightly", "fuzz", "run")
}

$isWindows = [System.Runtime.InteropServices.RuntimeInformation]::IsOSPlatform(
    [System.Runtime.InteropServices.OSPlatform]::Windows
)
if ($isWindows) {
    $asanRuntime = Get-ChildItem `
        -Path "C:\Program Files\Microsoft Visual Studio", "C:\Program Files (x86)\Microsoft Visual Studio" `
        -Filter "clang_rt.asan_dynamic-x86_64.dll" `
        -Recurse `
        -ErrorAction SilentlyContinue |
        Select-Object -First 1

    if ($null -eq $asanRuntime) {
        Skip-Or-Fail "Windows ASan runtime is not available; skipping cargo-fuzz smoke"
    }

    $env:PATH = "$($asanRuntime.DirectoryName);$env:PATH"
}

$targets = @(
    "sql_parser",
    "value_decode",
    "backup_manifest",
    "commit_log_record",
    "bson_document",
    "compressed_value",
    "pg_copy_text",
    "keyenc_successor",
    "internal_request_frame"
)

foreach ($target in $targets) {
    & cargo @cargoFuzzArgs $target -- -runs=256 -max_total_time=10
    if ($LASTEXITCODE -ne 0) {
        throw "cargo fuzz target $target failed"
    }
}
