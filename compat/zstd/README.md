# zstd compatibility facade

Arrow IPC 58 and Parquet 58 require `zstd ^0.13`, including their latest 58.x
patch releases. That line depends on the vulnerable `zstd-safe` 7.x stack.
Updating only MultiDB's direct dependency would retain the old implementation.

This local `0.13.4` package is a compatibility facade. It re-exports the actual
`zstd` 0.14.3 implementation and forwards the 0.13 feature names. It contains no
codec implementation, copied 0.13 code, or advisory suppression. Both direct
MultiDB calls and the Arrow/Parquet calls execute upstream `zstd` 0.14.3,
`zstd-safe` 8.2 or newer, and the corresponding patched `zstd-sys` library.

The local version is deliberately distinct from the published 0.13.3 package.
Advisory tools must still check the registry implementation and its dependencies;
the facade's tiny first-party source is reviewed with MultiDB. This is not an
independent audit of upstream dependencies. Remove the patch when all transitive
callers accept the patched upstream version directly.

Existing compression, Parquet, Arrow IPC, backup and restore tests exercise the
forwarded APIs; `cargo fmt --all` also covers local path dependencies.
