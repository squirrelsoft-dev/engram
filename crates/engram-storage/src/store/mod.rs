//! The `MemoryStore` interface and its two adapter implementations.
//!
//! This module is the public boundary that ADR 0001 commits to. The
//! README's §3.3 method list and `docs/design/schema-migrations.md`
//! §5.2's `applyMigrations(dir) → MigrationResult` extension define
//! the surface. The trait lives in [`MemoryStore`]; the two
//! production adapters are [`EmbeddedStore`] and [`ServiceStore`].

mod config;
mod embedded;
mod service;
mod shared;
mod store;

pub use config::{MemoryStoreConfig, StoreKind};
pub use embedded::EmbeddedStore;
pub use service::ServiceStore;
pub use shared::with_ns_db;
pub use store::MemoryStore;

use crate::error::Error;
use std::path::Path;

/// Open the `MemoryStore` selected by the configuration.
///
/// The factory is the single point of policy for embedded-vs-service
/// selection (ADR 0001's `ENGRAM_EMBEDDED_PATH` / `ENGRAM_SURREAL_URL`
/// variables). Higher layers call this and never see the
/// `Surreal<Db>` / `Surreal<Http>` distinction.
pub async fn open(config: &MemoryStoreConfig) -> Result<Box<dyn MemoryStore>, Error> {
    match config.kind() {
        StoreKind::Embedded { path } => {
            let store = EmbeddedStore::connect(config, path.as_deref()).await?;
            Ok(Box::new(store))
        }
        StoreKind::Service { url, user, pass } => {
            let store = ServiceStore::connect(config, url, user, pass).await?;
            Ok(Box::new(store))
        }
    }
}

/// Convenience: build a [`MemoryStoreConfig`] from the
/// repository's default `schema/manifest.toml` and migrations
/// directory. Used by the `engram` binary and by tests; not the
/// canonical public API.
pub fn config_from_repo_layout(
    manifest_path: &Path,
    ns: impl Into<String>,
    db: impl Into<String>,
) -> Result<MemoryStoreConfig, Error> {
    MemoryStoreConfig::from_manifest(manifest_path, ns, db)
}
