//! Engram storage crate: the `MemoryStore` interface and SurrealDB
//! adapters.
//!
//! This crate is Phase 1, issue #2: the storage layer that ADR 0001
//! and `docs/design/schema-migrations.md` commit to. The
//! `MemoryStore` trait is the only surface the Memory Core talks
//! to; two adapters — [`store::EmbeddedStore`] (in-process) and
//! [`store::ServiceStore`] (over HTTP) — sit behind it.
//!
//! The migration runner lives in [`store::shared::run_migrations`]
//! and is identical across both adapters (parameterised on the
//! SurrealDB connection type via the `Connection` trait). It
//! reads the manifest, applies pending `.surql` files, records
//! them in `engram_schema`, and validates checksums of
//! already-applied migrations.
//!
//! Public re-exports are deliberately narrow: callers should not
//! need to reach into the `store::shared` module or the manifest
//! module. If you find yourself doing that, file an issue.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod error;
pub mod manifest;
pub mod record;
pub mod store;

pub use error::{
    AppliedMigration, Error, MigrationDirection, MigrationError, MigrationResult,
    MigrationWarning,
};
pub use manifest::{Manifest, ManifestMigration};
pub use record::{
    Concept, Entity, Episode, GraphResult, Preference, PreferenceDirection, Procedure, SignalTier,
    Task, TaskStatus,
};
pub use store::{
    config_from_repo_layout, open, EmbeddedStore, MemoryStore, MemoryStoreConfig, ServiceStore,
    StoreKind,
};
