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
    Concept, Entity, Episode, GraphResult, Preference, Procedure, Task,
};

/// Filters for [`MemoryStore::traverse_graph`].
///
/// The Phase 1 surface accepts a list of relation types and an
/// agent-scoping string. Future revisions may add edge weight
/// bounds and node-type predicates; the struct is non-exhaustive
/// so additional fields can be added without breaking the public
/// API.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct GraphFilters {
    /// Restrict the walk to a specific agent. Cross-agent
    /// traversals are not supported; an empty string disables
    /// the agent filter (only the start node's own agent's
    /// graph is reachable, so this is mostly for tests that
    /// need a global view).
    pub agent_id: String,

    /// Restrict the walk to a subset of relation tables (e.g.
    /// `episode_relates_to_concept`). An empty list means
    /// "walk all known relation types", which is the
    /// documented Phase 1 default.
    pub relations: Vec<String>,

    /// Optional cap on the number of edges returned. The
    /// underlying engine still walks the full depth; this is
    /// a post-filter.
    pub max_edges: Option<u32>,
}

impl GraphFilters {
    /// A filter that lets the walk proceed without constraints.
    pub fn any() -> Self {
        Self::default()
    }

    /// A filter that constrains the walk to a specific agent.
    pub fn for_agent(agent_id: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            ..Self::default()
        }
    }

    /// A filter that constrains the walk to a specific set of
    /// relation types.
    pub fn relations(rs: &[&str]) -> Self {
        Self {
            relations: rs.iter().map(|s| s.to_string()).collect(),
            ..Self::default()
        }
    }

    /// Set the maximum number of edges the walk should return.
    /// Returns the modified filter so calls can be chained.
    pub fn with_max_edges(mut self, max_edges: u32) -> Self {
        self.max_edges = Some(max_edges);
        self
    }
}

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

    /// Read a single episode as it existed at a past transaction
    /// time. The bi-temporal contract is documented in
    /// `docs/design/schema-migrations.md` §5.5: the engine
    /// records a new version on every update, and the `VERSION
    /// d'...'` clause is the only read path that returns a
    /// historical snapshot.
    ///
    /// `as_of` is the transaction-time wall clock, in UTC. The
    /// underlying engine (SurrealDB's MVCC layer) is responsible
    /// for resolving the version; the adapter just shapes the
    /// query.
    ///
    /// Returns `Ok(None)` if the record did not exist at that
    /// time (e.g. the caller's timestamp predates the `CREATE`).
    async fn read_episode_at(
        &self,
        episode_id: &str,
        as_of: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Episode>, Error>;

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
    async fn write_preference(&self, preference: &Preference) -> Result<Preference, Error>;
    async fn query_preferences(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        category: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Preference>, Error>;
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
        filters: &GraphFilters,
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
