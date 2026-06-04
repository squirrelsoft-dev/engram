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
//!
//! ## Bind-vs-literal query strategy
//!
//! Each adapter issues SurrealQL in a deliberately mixed style:
//!
//! - **Server-generated ids and enum-constrained fields are
//!   interpolated as literals** (`CREATE {id} SET kind = 'foo'`)
//!   rather than bound as parameters. SurrealDB 3.1.x has a
//!   parser quirk where the `INSIDE [...]` assertion in a
//!   schema-bound `DEFINE FIELD` mis-parses a bound string
//!   parameter (the error is "Expected object, got string",
//!   which is misleading on a typed value). The handoff-era
//!   `CONTENT $content SET <enum> = '<literal>'` form is
//!   *also* invalid SurrealQL (the `SET` clause can't follow
//!   `CONTENT`), so we now bind the whole record as a
//!   typed `Object` and let the engine validate the inner
//!   values. Record ids and the `RELATE` `in`/`out`
//!   positions are interpolated as record literals because
//!   the bind path doesn't coerce strings to records in those
//!   positions.
//!
//! - **Caller-supplied scalar data is bound** as query
//!   parameters (`$a`, `$l`, etc.). The bindings are the
//!   only way to send typed `DateTime`, `f32` arrays, and
//!   `Vec<RecordId>` through the query pipeline; the bind
//!   path's value-coercion handles these correctly.
//!
//! The two adapters' SQL strings and bind lists are kept in
//! lock-step; if you change one, change the other. The
//! integration parity test in `spikes/schema-migrations/`
//! is the catch-all for a regression here.

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
    config_from_repo_layout, open, EmbeddedStore, GraphFilters, MemoryStore, MemoryStoreConfig,
    ServiceStore, StoreKind,
};
pub use store::format_record_id_key;
pub use surrealdb::types::RecordId;
