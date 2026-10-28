# RC1 dependency policy and review debt

This record covers dependency-policy checks for `1.0.0-rc.1`. It does not grant independent security acceptance or production readiness. The release owner explicitly accepted the version-pinned bootstrap exemptions below as RC review debt. `supply-chain/audits.toml` remains empty: no audit was fabricated or imported to hide these exemptions.

## Executed checks

The checks ran on 2026-10-28 against the working-tree manifest and lockfile hashes below. The final runtime source commit and release artifact are not bound by this record yet; bind and rerun the required CI gates on that exact commit before publishing release claims. No `cargo build`, `check` or `test` was run for this dependency verification.

| Check | Actual result |
| --- | --- |
| cargo-audit 0.22.2 | `cargo audit --no-fetch --deny warnings --json` exited 0: 649 dependencies, zero reported vulnerabilities or warnings under the existing scoped advisory policy. |
| cargo-deny 0.19.9 | `cargo deny --all-features --frozen check --disable-fetch --show-stats` exited 0: advisories, bans, licenses and sources all passed with zero errors/warnings. |
| cargo-vet 0.10.3 | `cargo vet --locked --frozen --no-minimize-exemptions --no-registry-suggestions --output-format json` exited 0 after the explicit RC bootstrap updates: 647 exemptions, zero fully/partially audited packages and zero independent audit entries. |
| Root and fuzz manifests | Full `cargo metadata --locked --offline --format-version 1` succeeded for both; the root run also used `--all-features`. Input hashes remained unchanged during the run. This is dependency resolution, not compilation or fuzz execution. |
| CycloneDX SBOM | Blocked locally: no SBOM generator was installed. Installation/compilation was deliberately deferred to qualified CI. A lockfile inventory is not substituted for a binary/image SBOM. The final source-bound CI artifact is still required. |

Both advisory databases used the clean RustSec snapshot `40f5be6fb12ed93fc591bf4ab23b7d793b3f2239`, committed at `2026-10-28T09:50:03+01:00`, containing 1,381 advisories. The tools did not refresh the database in this run. Refresh advisories in release CI; the result is limited to this recorded snapshot. Existing ignored informational/unmaintained advisories remain documented in `.cargo/audit.toml` and `deny.toml`; this change adds no advisory ignore.

## Reachable dependency changes

The resolved graph confirms these are normal/build-reachable dependencies, not merely names left in Cargo.lock. Graph reachability does not claim every module executes for every configured database profile.

- `parquet` 58.3.0 now enables `experimental` for the allocation-aware Parquet preflight APIs. Upstream also enables `variant_experimental`, making `parquet-variant`, `parquet-variant-compute` and `parquet-variant-json` 58.3.0 reachable. These are the three new package exemptions.
- `tokio-postgres-rustls` 0.14.0 moved from dev-dependencies to regular dependencies for the foreign PostgreSQL TLS connector in `src/federation/remote_transport.rs`. Its normal graph reaches `x509-cert` 0.2.5, `tls_codec` 0.4.2 and `tls_codec_derive` 0.4.2. These four existing exemptions were promoted from `safe-to-run` to the graph-required `safe-to-deploy`. The derive crate is a build-time proc macro; this wording does not claim it executes inside the server.
- `socket2` 0.6.4 (`all`) is now a direct dependency for transport socket behavior. `brotli` 8.0.4 and `flate2` 1.1.9 are direct dependencies of the bounded compression preflight in `src/federation/parquet_preflight.rs`. They and `brotli-decompressor` 5.0.3 already had matching bootstrap exemptions; their entries were not widened. The resolved flate2 features include both `rust_backend`/`miniz_oxide` and `zlib-rs` through feature unification.

The seven changed exemption entries have exact versions and explicit notes identifying their RC bootstrap status. This is a policy-baseline update, not a claim of independent safe-to-deploy review. The initial vet run correctly failed for these seven missing criteria. The subsequent pass establishes policy completeness only. Required criteria were not lowered: the seven versions are explicitly exempted rather than claimed reviewed. No package versions were silently ignored and no `audits` entries were added.

## Hashed inventory

Raw command logs, full metadata, the reachable inventory and `report.json` are retained in the execution workspace at `/workspace/scratch/rc-supply-chain-final1`. Preserve these raw files in the final RC evidence archive; paths alone are not published release evidence. The inventory includes exact versions, crates.io checksums, resolved features and dependency paths.

| Input or proof | SHA-256 |
| --- | --- |
| `Cargo.toml` | `fd16a1c0342b7c474957ae5da8f0a0cbf622e7e587351f055ee233dd2627fc66` |
| `Cargo.lock` | `a492843f7611ff5178e5815468978f70e9ad6a30b1b45ae582611bc03e6dfa32` |
| `fuzz/Cargo.toml` | `6bb74fdcab6d5ac4b577cb52be6dbee0dd40e0505222361b4bb082746a6d4c05` |
| `fuzz/Cargo.lock` | `7c449177b6b652d96cc9e2750b67e282037cca3605b218e5adefe15fdea8f231` |
| `supply-chain/config.toml` | `dca2e2c8ff79b79b4a44f97298d6516e3daf0d321f7bb2cfeb0dc99728a80d97` |
| `supply-chain/audits.toml` | `628720e4cf05595b314bd06d3bb73e6434e3d3be10f1a394dfbbfe6ea248403c` |
| `.cargo/audit.toml` | `37f8d659e4ce95dbaa01f9598b8f3c51331011e9a21a36a579bbde30d26128ae` |
| `deny.toml` | `1727155026e909c0c2c44ef953286c3e38ade93f00c28770629861356cf793c9` |
| Evidence `reachable-inventory.json` | `85aff9f4a675cd6595389d574f2f32254bf714c9eab792351ed8e2939674ae47` |
| Evidence `metadata.stdout` | `437ef10cf6fb064fc0403447b823e689af58215ecb721fd0b07de8afd45e077c` |
| Evidence `audit.stdout` | `d31cb22022cd7bd4532ae6e2597be639d8da3ae80b4dfbda782b02224715da20` |
| Evidence `deny.stdout` | `de0c120c13eb385ed8a47a295d9fffa6d28ddeeaecf5ab36467886a08daaebd7` |
| Evidence `vet.stdout` | `c0bd596fd6e9a859ee3a3f58fcee4e07a2083eeb404081c7d5046dbe656c7313` |
| Evidence `fuzz-metadata.stdout` | `5d48bab31ca4729362e9626dd4733e626c9d982aff16bcbd4b48140d3e2f753e` |
| Evidence `report.json` | `13dba6e2d53e4a2a18228dc51812bb3b98b7f3629d4a3771a6a6ee7d27f33df1` |

| Reachable package | Version | crates.io package checksum |
| --- | --- | --- |
| `brotli` | `8.0.4` | `5cc91aac060a7a1e25823bdccbfb6af1875b88f17c6daac97894eed8207166b3` |
| `brotli-decompressor` | `5.0.3` | `3a32acac15fe1967bc3986b2a6347dffc965602354ea6f450ad07e8bfd253583` |
| `flate2` | `1.1.9` | `843fba2746e448b37e26a819579957415c8cef339bf08564fe8b7ddbd959573c` |
| `parquet` | `58.3.0` | `5dafa7d01085b62a47dd0c1829550a0a36710ea9c4fe358a05a85477cec8a908` |
| `parquet-variant` | `58.3.0` | `74c8db065291f088a2aad8ab831853eae1871c0d311c8d0b83bbc3b7e735d0fc` |
| `parquet-variant-compute` | `58.3.0` | `a530e8d5b5e14efcb39c9a6ec55432ad11f6afb7dc4455a79be0dc615fe3cc31` |
| `parquet-variant-json` | `58.3.0` | `00ed89908289f67caa2ca078f9ff9aacd6229a313ec92b12bf4f48f613dc2b97` |
| `socket2` | `0.6.4` | `52d1cfed4120b4d927bf7c0f86d2087a4a7d6027c906d9f9d525a80573b9be51` |
| `tls_codec` | `0.4.2` | `0de2e01245e2bb89d6f05801c564fa27624dbd7b1846859876c7dad82e90bf6b` |
| `tls_codec_derive` | `0.4.2` | `2d2e76690929402faae40aebdda620a2c0e25dd6d3b9afe48867dfd95991f4bd` |
| `tokio-postgres-rustls` | `0.14.0` | `4c2ad44aa0ae96db89c4742212ed41645b2f597311ff6e1945542a4d9fadc2fb` |
| `x509-cert` | `0.2.5` | `1301e935010a701ae5f8655edc0ad17c44bad3ac5ce8c39185f75453b720ae94` |

## Remaining release acceptance

The independent production security-review gate remains blocked. Before 1.0.0 acceptance, a responsible independent reviewer must address the bootstrap dependency baseline, including these new production-reachable parsers/TLS dependencies, and record actual review evidence or explicitly approved residual risks. A cargo-vet exemption is not that evidence.

The RC report must bind this policy result, a newly generated CycloneDX SBOM, provenance/signatures and the required CI job results to the frozen runtime source revision and actual release artifact hashes. It must also retain the separate physical-GPU, multi-host cluster, sustained-load and operational acceptance statuses from the production-readiness contract; this dependency pass changes none of them.
