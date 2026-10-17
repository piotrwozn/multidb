//! Spec-driven provisioning (preview): building and converging a database from a
//! [`DatabaseSpec`](crate::config_spec::DatabaseSpec).
//!
//! [`physical::project`] maps every collection of a version 2 spec, plus the full-text, geo and
//! history objects it derives, onto the runtime objects the `Database::create_*` APIs build.
//! Projection is pure: it validates and describes, and never touches a database.
//!
//! See `docs/provisioning.md`.

pub mod physical;
