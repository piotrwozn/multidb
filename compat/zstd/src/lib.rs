//! Compatibility facade for Arrow 58 and Parquet 58's `zstd ^0.13` dependency.
//!
//! Every API and implementation comes from the patched upstream 0.14 release.
//! This crate contains no codec implementation or copied older zstd source.

#![forbid(unsafe_code)]

pub use zstd_upstream::*;
