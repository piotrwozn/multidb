#![forbid(unsafe_code)]
#![warn(clippy::all, clippy::pedantic)]
#![deny(clippy::expect_used, clippy::unwrap_used)]
#![allow(clippy::module_name_repetitions)]

pub mod admin;
pub mod autoscale;
pub mod backup;
pub mod cdc;
pub mod cloud;
pub mod compat;
pub mod config_spec;
pub mod continuous;
pub mod db;
pub mod extension;
pub mod federation;
mod fileio;
pub mod geo;
pub mod graph;
pub mod hardening;
pub mod internal_transport;
pub mod keyenc;
pub mod migration;
pub mod model;
pub mod network;
pub mod observability;
pub mod performance;
pub mod phase30;
pub mod query;
pub mod repl;
pub mod roadmap;
pub mod runtime_advisor;
pub mod security;
pub mod storage;
pub mod templates;
pub mod temporal;
pub mod text;
pub mod timeseries;
pub mod tuning;
pub mod txn;
pub mod vector;
pub mod verification;
