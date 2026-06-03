//! The `MemoryStore` trait — the only boundary the Memory Core uses
//! to talk to storage.
//!
//! The method list comes from two sources, merged:
//!
//! 1. README §3.3's surface (writeEpisode, queryEpisodic, ...).
//!    These are the read/write methods needed for Phase 2's
//!    operations to compile against the storage adapter.
//! 2. `docs/design/schema-migrations.md` §5.2's `applyMigrations`
//!    extension, which is what Phase 1 actually wires up.
//!
//! The trait is async because both SurrealDB adapters are async and
//! there is no useful synchronous story. It is `Send + Sync` so the
//! store can be shared across `tokio` tasks (the consolidation
//! engine and the ingestion pipeline will both hold a reference).

use std::path::Path;

use async_trait::async_trait;

use crate::error::{Error, MigrationResult};
use crate::record::{
    Concept, Entity, Episode, GraphResult, Procedure, Task,
};

/// A common supertrait for both adapter shapes. The async-trait
/// macro gives us dyn-safety so the embedded and service stores can
/// both be returned as `Box<dyn MemoryStore>` from
/// [`crate::store::open`].
#[async_trait]
pub trait MemoryStore: Send + Sync {
    /// Apply any pending migrations from the directory that
    /// contains the `.surql` files referenced by the configured
    /// manifest. Per `docs/design/schema-migrations.md` §5.1-5.6:
    ///
    /// - Reads the manifest to determine the target schema version.
    /// - Lists `migrations/*.sql`, sorted by filename, and applies
    ///   any that have no record in `engram_schema` yet.
    /// - For each already-applied migration, recomputes the file's
    ///   checksum and fails fast on mismatch (§4.3).
    /// - Records a row in `engram_schema` for every newly-applied
    ///   migration, with the file's SHA-256, the Engram version, and
    ///   the direction.
    /// - If any statement in a single file fails, the file's
    ///   transaction is rolled back and the database is unchanged
    ///   (§5.3).
    async fn apply_migrations(&self) -> Result<MigrationResult, Error>;

    /// Apply the migrations in a specific directory, ignoring the
    /// configured manifest's `applied_migrations` list. Used by the
    /// `engram-migrate` subcommand and by tests; production callers
    /// should use [`apply_migrations`](Self::apply_migrations).
    async fn apply_migrations_from(&self, dir: &Path) -> Result<MigrationResult, Error>;

    /// The schema version the database is at after the most recent
    /// `apply_migrations` call. `0` means no migrations have been
    /// applied yet.
    async fn schema_version(&self) -> Result<u32, Error>;

    /// Drop all data (records) while preserving the schema. Useful
    /// for resetting between tests. Does **not** drop the
    /// `engram_schema` ledger.
    async fn clear_data(&self) -> Result<(), Error>;

    // --- Record-level operations (README §3.3) -------------------------

    async fn write_episode(&self, episode: &Episode) -> Result<Episode, Error>;
    async fn query_episodic(
        &self,
        agent_id: &str,
        limit: u32,
    ) -> Result<Vec<Episode>, Error>;
    async fn query_semantic(
        &self,
        agent_id: &str,
        embedding: &[f32],
        k: u32,
    ) -> Result<Vec<Concept>, Error>;
    async fn upsert_entity(&self, entity: &Entity) -> Result<Entity, Error>;
    async fn resolve_entity(
        &self,
        agent_id: &str,
        candidates: &[Entity],
    ) -> Result<Vec<Entity>, Error>;
    async fn upsert_concept(&self, concept: &Concept) -> Result<Concept, Error>;
    async fn relate_nodes(
        &self,
        from: &str,
        relation: &str,
        to: &str,
        weight: Option<f32>,
    ) -> Result<(), Error>;
    async fn traverse_graph(
        &self,
        start: &str,
        depth: u32,
    ) -> Result<Vec<GraphResult>, Error>;
    async fn write_task(&self, task: &Task) -> Result<Task, Error>;
    async fn query_pending(
        &self,
        agent_id: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Task>, Error>;
    async fn write_procedure(&self, procedure: &Procedure) -> Result<Procedure, Error>;
    async fn query_procedures(
        &self,
        agent_id: &str,
        limit: u32,
    ) -> Result<Vec<Procedure>, Error>;

    /// Storage health-check used by the binary's `engram doctor`
    /// and by the test harness. Returns `Ok(())` when a trivial
    /// round-trip query succeeds.
    async fn ping(&self) -> Result<(), Error>;
}
