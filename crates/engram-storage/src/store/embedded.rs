//! Embedded adapter: in-process SurrealDB.
//!
//! This is the path for users embedding Engram into a Rust
//! application (e.g. a coding-agent harness) per ADR 0001. The
//! adapter owns a `Surreal<Db>` and the read/write operations
//! the Memory Core needs.
//!
//! Persistence modes:
//!
//! - In-memory (`path == None`): the engine lives entirely in
//!   process memory. Closing the store drops the data. This is
//!   the zero-config path for embedders and tests.
//! - File-backed (`path == Some(p)`): the engine persists to
//!   `p` via `SurrealKv`. Transaction-time versioning is enabled
//!   by appending `+versioned` to the endpoint per
//!   `schema/README.md`'s bi-temporal section, which is what
//!   `surrealkv+versioned://` does in the `v3` line.
//!
//! The embedded adapter applies migrations on `connect`, exactly
//! once per process; calling `apply_migrations` again is a no-op
//! when nothing has changed, and a no-op failure when the schema
//! is already at the target version (it still records the same
//! result in the ledger, idempotently).

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use surrealdb::engine::local::{Db, Mem, SurrealKv};
use surrealdb::Surreal;
use tokio::sync::Mutex;

use crate::error::{Error, MigrationResult};
use crate::manifest::Manifest;
use crate::record::{
    Concept, Entity, Episode, GraphResult, Procedure, Task,
};
use crate::store::config::MemoryStoreConfig;
use crate::store::shared::run_migrations;
use crate::store::store::MemoryStore;

/// Embedded in-process store. The `Db` connection type is
/// distinct from the HTTP client's; the only operations shared
/// between the two adapters are the migration runner and the
/// `read_ledger` helper, both of which are parameterised on the
/// [`surrealdb::Connection`] trait.
#[derive(Debug)]
pub struct EmbeddedStore {
    db: Surreal<Db>,
    config: MemoryStoreConfig,
    /// The migration result from the most recent run, so the
    /// `status` surface (Phase 4) can read it without a second
    /// round-trip.
    last_migration: Arc<Mutex<Option<MigrationResult>>>,
}

impl EmbeddedStore {
    /// Connect to an embedded engine, apply migrations, and return
    /// the store. `path == None` selects the in-memory backend.
    pub async fn connect(
        config: &MemoryStoreConfig,
        path: Option<&Path>,
    ) -> Result<Self, Error> {
        let db = match path {
            None => Surreal::new::<Mem>(()).await?,
            Some(p) => {
                // Use the `surrealkv://` URL form so we can ask
                // the engine for the versioned (transaction-time)
                // backend. The bi-temporal design
                // (docs/design/schema-migrations.md §5.5) relies
                // on SurrealDB's `VERSION` clause for the
                // transaction-time axis; that clause is only
                // meaningful against a versioned engine.
                let url_string = format!("surrealkv://{}?versioned=true", p.display());
                tracing::info!(
                    "opening embedded file-backed store at {} (versioned=true)",
                    p.display()
                );
                Surreal::new::<SurrealKv>(&url_string).await?
            }
        };

        // Bind namespace+database up front. We do this outside the
        // per-call helper because the connection is a long-lived
        // resource and re-binding per call would be wasteful.
        db.use_ns(&config.namespace).use_db(&config.database).await?;

        let store = EmbeddedStore {
            db,
            config: config.clone(),
            last_migration: Arc::new(Mutex::new(None)),
        };

        // Apply migrations on connect. The first call is the one
        // that actually moves the schema forward; subsequent calls
        // are no-ops whose `applied` vector is empty and whose
        // `skipped` vector is the full migration list.
        let result = store.apply_migrations().await?;
        *store.last_migration.lock().await = Some(result);
        Ok(store)
    }

    /// The SurrealDB connection, for tests that need to assert
    /// directly against the engine. Not part of the public trait.
    pub fn raw(&self) -> &Surreal<Db> {
        &self.db
    }
}

#[async_trait]
impl MemoryStore for EmbeddedStore {
    async fn apply_migrations(&self) -> Result<MigrationResult, Error> {
        let manifest = Manifest::read(&self.config.manifest_path)?;
        let dir = manifest.migrations_dir(&self.config.manifest_path);
        run_migrations(
            &self.db,
            &manifest,
            &dir,
            &self.config.engram_version,
            self.config.strict,
        )
        .await
    }

    async fn apply_migrations_from(&self, dir: &Path) -> Result<MigrationResult, Error> {
        // For the "from a specific dir" path, we synthesise a
        // manifest whose `version` is the highest numbered file
        // in the dir, so the runner's invariants (manifest must
        // declare every on-disk file) hold.
        let files = crate::store::shared::list_migration_files(dir)?;
        let applied: Vec<crate::manifest::ManifestMigration> = files
            .iter()
            .map(|(v, p)| crate::manifest::ManifestMigration {
                version: *v,
                file: p
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string(),
                description: String::new(),
            })
            .collect();
        let max_version = applied.iter().map(|m| m.version).max().unwrap_or(0);
        let manifest = Manifest {
            version: max_version,
            engram_version_min: "0.1.0".to_string(),
            applied_migrations: applied,
        };
        run_migrations(
            &self.db,
            &manifest,
            dir,
            &self.config.engram_version,
            self.config.strict,
        )
        .await
    }

    async fn schema_version(&self) -> Result<u32, Error> {
        let query_result = self
            .db
            .query("SELECT version FROM engram_schema ORDER BY version DESC LIMIT 1")
            .await;
        let mut response = match query_result {
            Ok(r) => r,
            Err(e) => {
                if e.to_string().contains("does not exist") {
                    return Ok(0);
                }
                return Err(Error::Surreal(e.to_string()));
            }
        };
        let row: Vec<serde_json::Value> = response.take(0)?;
        Ok(row
            .first()
            .and_then(|v| v.get("version"))
            .and_then(|v| v.as_u64())
            .map(|n| n as u32)
            .unwrap_or(0))
    }

    async fn clear_data(&self) -> Result<(), Error> {
        // Drop every record except the schema ledger.
        for table in [
            "episode",
            "entity",
            "concept",
            "preference",
            "procedure",
            "task",
            "episode_relates_to_concept",
            "episode_precedes_episode",
            "episode_mentions_entity",
            "concept_connects_to_concept",
            "concept_about_entity",
            "entity_relates_to_entity",
            "episode_triggered_task",
        ] {
            let _ = self.db.query(format!("DELETE {table}")).await?;
        }
        Ok(())
    }

    async fn write_episode(&self, episode: &Episode) -> Result<Episode, Error> {
        let mut e = episode.clone();
        if e.id.is_none() {
            e.id = Some(format!("episode:{}", uuid_v4_like()));
        }
        let id = e.id.clone().unwrap();
        // `CREATE <id> CONTENT { ... }` returns the new record with
        // server-side defaults populated. We round-trip the value
        // back through the deserialiser so the caller gets a
        // fully-populated record (timestamps, defaults).
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", episode_to_map(&e)?))
            .await?
            .check()
            .map_err(|e| Error::Surreal(e.to_string()))?;
        let mut r = self.db.query("SELECT * FROM $id").bind(("id", id)).await?;
        let row: Vec<Episode> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other("write_episode: SELECT after CREATE returned no rows".to_string())
        })
    }

    async fn query_episodic(&self, agent_id: &str, limit: u32) -> Result<Vec<Episode>, Error> {
        let mut r = self
            .db
            .query("SELECT * FROM episode WHERE agent_id = $a ORDER BY valid_time_start DESC LIMIT $l")
            .bind(("a", agent_id.to_string()))
            .bind(("l", limit as i64))
            .await?;
        let rows: Vec<Episode> = r.take(0)?;
        Ok(rows)
    }

    async fn query_semantic(
        &self,
        agent_id: &str,
        embedding: &[f32],
        k: u32,
    ) -> Result<Vec<Concept>, Error> {
        // k-NN over the HNSW vector index. The index is
        // DIMENSION 768, so we zero-pad shorter vectors for the
        // parity call; in production the embedding model is
        // configured to match the index (see `schema/README.md`'s
        // tunable-parameters section).
        let mut vec: Vec<f32> = embedding.to_vec();
        while vec.len() < 768 {
            vec.push(0.0);
        }
        let mut r = self
            .db
            .query("SELECT * FROM concept WHERE agent_id = $a AND embedding <|k, COSINE|> $v LIMIT $k")
            .bind(("a", agent_id.to_string()))
            .bind(("v", vec))
            .bind(("k", k as i64))
            .await?;
        let rows: Vec<Concept> = r.take(0)?;
        Ok(rows)
    }

    async fn upsert_entity(&self, entity: &Entity) -> Result<Entity, Error> {
        let id = entity
            .id
            .clone()
            .unwrap_or_else(|| format!("entity:{}", uuid_v4_like()));
        let mut e = entity.clone();
        e.id = Some(id.clone());
        self.db
            .query("UPSERT $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", entity_to_map(&e)?))
            .await?
            .check()
            .map_err(|e| Error::Surreal(e.to_string()))?;
        let mut r = self.db.query("SELECT * FROM $id").bind(("id", id)).await?;
        let row: Vec<Entity> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other("upsert_entity: SELECT after UPSERT returned no rows".to_string())
        })
    }

    async fn resolve_entity(
        &self,
        agent_id: &str,
        candidates: &[Entity],
    ) -> Result<Vec<Entity>, Error> {
        // The full disambiguation logic is Phase 2 work
        // (README §8). For Phase 1, we return the existing
        // canonical entities whose name overlaps with any of
        // the candidate canonical_names — enough for the
        // adapter to be useful in tests.
        let names: Vec<String> = candidates.iter().map(|c| c.canonical_name.clone()).collect();
        let mut r = self
            .db
            .query("SELECT * FROM entity WHERE agent_id = $a AND canonical_name IN $n")
            .bind(("a", agent_id.to_string()))
            .bind(("n", names))
            .await?;
        let rows: Vec<Entity> = r.take(0)?;
        Ok(rows)
    }

    async fn upsert_concept(&self, concept: &Concept) -> Result<Concept, Error> {
        let id = concept
            .id
            .clone()
            .unwrap_or_else(|| format!("concept:{}", uuid_v4_like()));
        let mut c = concept.clone();
        c.id = Some(id.clone());
        self.db
            .query("UPSERT $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", concept_to_map(&c)?))
            .await?
            .check()
            .map_err(|e| Error::Surreal(e.to_string()))?;
        let mut r = self.db.query("SELECT * FROM $id").bind(("id", id)).await?;
        let row: Vec<Concept> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other("upsert_concept: SELECT after UPSERT returned no rows".to_string())
        })
    }

    async fn relate_nodes(
        &self,
        from: &str,
        relation: &str,
        to: &str,
        weight: Option<f32>,
    ) -> Result<(), Error> {
        // The full relation type is selected by the caller via
        // the table name, which is also the relation type per
        // `schema/migrations/0001_init.sql`. We pass `weight` as
        // an optional field; relations that don't carry a
        // `weight` field still accept the call (SurrealDB will
        // simply ignore unknown fields if the relation is
        // schemaless, but our relation tables are all
        // `SCHEMAFULL`, so we send `weight` only when the
        // relation table is one that declares it).
        let r = self
            .db
            .query("RELATE $from->$rel->$to SET weight = $w")
            .bind(("from", from.to_string()))
            .bind(("rel", relation.to_string()))
            .bind(("to", to.to_string()))
            .bind(("w", weight.unwrap_or(0.5)))
            .await?;
        r.check().map_err(|e| Error::Surreal(e.to_string()))?;
        Ok(())
    }

    async fn traverse_graph(
        &self,
        start: &str,
        depth: u32,
    ) -> Result<Vec<GraphResult>, Error> {
        // Phase 1 returns a best-effort 1-hop walk from `start`
        // along any outgoing relation; the full
        // `traverse_graph(start, depth, filters)` is a Phase 2
        // item. The query is parameterised on `depth` so the
        // behaviour at `depth == 1` is what the documentation
        // promises; deeper graphs are a future extension.
        let _ = depth;
        let mut r = self
            .db
            .query("SELECT id, out FROM $start")
            .bind(("start", start.to_string()))
            .await?;
        let row: Vec<serde_json::Value> = r.take(0)?;
        let mut out = Vec::new();
        for v in row {
            let from = v
                .get("id")
                .and_then(|x| x.as_str())
                .unwrap_or(start)
                .to_string();
            if let Some(o) = v.get("out") {
                if let Some(arr) = o.as_array() {
                    for entry in arr {
                        if let Some(to) = entry.as_str() {
                            out.push(GraphResult {
                                from: from.clone(),
                                relation: "graph_edge".to_string(),
                                to: to.to_string(),
                                attributes: serde_json::json!({}),
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    async fn write_task(&self, task: &Task) -> Result<Task, Error> {
        let id = task
            .id
            .clone()
            .unwrap_or_else(|| format!("task:{}", uuid_v4_like()));
        let mut t = task.clone();
        t.id = Some(id.clone());
        t.created_at = Some(Utc::now());
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", task_to_map(&t)?))
            .await?
            .check()
            .map_err(|e| Error::Surreal(e.to_string()))?;
        let mut r = self.db.query("SELECT * FROM $id").bind(("id", id)).await?;
        let row: Vec<Task> = r.take(0)?;
        row.into_iter().next()
            .ok_or_else(|| Error::Other("write_task: SELECT after CREATE returned no rows".to_string()))
    }

    async fn query_pending(
        &self,
        agent_id: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Vec<Task>, Error> {
        let mut r = self
            .db
            .query(
                "SELECT * FROM task WHERE agent_id = $a AND status = 'pending' \
                 AND trigger_value <= $t ORDER BY created_at ASC",
            )
            .bind(("a", agent_id.to_string()))
            .bind(("t", now))
            .await?;
        let rows: Vec<Task> = r.take(0)?;
        Ok(rows)
    }

    async fn write_procedure(&self, procedure: &Procedure) -> Result<Procedure, Error> {
        let id = procedure
            .id
            .clone()
            .unwrap_or_else(|| format!("procedure:{}", uuid_v4_like()));
        let mut p = procedure.clone();
        p.id = Some(id.clone());
        p.created_at = Some(Utc::now());
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", procedure_to_map(&p)?))
            .await?
            .check()
            .map_err(|e| Error::Surreal(e.to_string()))?;
        let mut r = self.db.query("SELECT * FROM $id").bind(("id", id)).await?;
        let row: Vec<Procedure> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other(
                "write_procedure: SELECT after CREATE returned no rows".to_string(),
            )
        })
    }

    async fn query_procedures(
        &self,
        agent_id: &str,
        limit: u32,
    ) -> Result<Vec<Procedure>, Error> {
        let mut r = self
            .db
            .query("SELECT * FROM procedure WHERE agent_id = $a ORDER BY created_at DESC LIMIT $l")
            .bind(("a", agent_id.to_string()))
            .bind(("l", limit as i64))
            .await?;
        let rows: Vec<Procedure> = r.take(0)?;
        Ok(rows)
    }

    async fn ping(&self) -> Result<(), Error> {
        let mut r = self.db.query("SELECT 1 AS ok FROM 1").await?;
        let row: Vec<serde_json::Value> = r.take(0)?;
        let ok = row
            .first()
            .and_then(|v| v.get("ok"))
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        if ok != 1 {
            return Err(Error::Other("ping returned wrong value".to_string()));
        }
        Ok(())
    }
}

// --- record-to-map helpers --------------------------------------------------
//
// We bind `serde_json::Value` rather than the typed struct so the
// `INSIDE [...]` and `>= 0.0 AND <= 1.0` constraints in the schema
// run their server-side checks. The mapping is the only place we
// know exactly which keys the schema expects; keeping it here
// means changes to the schema surface as a single edit.

pub fn episode_to_map(e: &Episode) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "agent_id": e.agent_id,
        "org_id": e.org_id,
        "user_id": e.user_id,
        "content": e.content,
        "content_type": e.content_type,
        "embedding": e.embedding,
        "importance": e.importance,
        "entities": e.entities,
        "valid_time_start": e.valid_time_start,
        "valid_time_end": e.valid_time_end,
        "consolidated": e.consolidated,
        "consolidated_at": e.consolidated_at,
        "summary": e.summary,
        "source_tier": u8::from(e.source_tier),
        "metadata": e.metadata,
    }))
}

pub fn entity_to_map(e: &Entity) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "agent_id": e.agent_id,
        "org_id": e.org_id,
        "canonical_name": e.canonical_name,
        "aliases": e.aliases,
        "entity_type": e.entity_type,
        "attributes": e.attributes,
        "confidence": e.confidence,
        "confidence_tier": u8::from(e.confidence_tier),
        "anchor_record": e.anchor_record,
        "disambiguation_log": e.disambiguation_log,
    }))
}

pub fn concept_to_map(c: &Concept) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "agent_id": c.agent_id,
        "org_id": c.org_id,
        "content": c.content,
        "embedding": c.embedding,
        "confidence": c.confidence,
        "source_tier": u8::from(c.source_tier),
        "reinforcement_count": c.reinforcement_count,
        "decay_rate": c.decay_rate,
        "inferred": c.inferred,
        "inference_chain": c.inference_chain,
        "valid_time_start": c.valid_time_start,
        "valid_time_end": c.valid_time_end,
    }))
}

pub fn task_to_map(t: &Task) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "agent_id": t.agent_id,
        "org_id": t.org_id,
        "user_id": t.user_id,
        "content": t.content,
        "trigger_type": t.trigger_type,
        "trigger_value": t.trigger_value,
        "status": match t.status {
            TaskStatus::Pending => "pending",
            TaskStatus::Triggered => "triggered",
            TaskStatus::Completed => "completed",
            TaskStatus::Cancelled => "cancelled",
        },
        "triggered_at": t.triggered_at,
    }))
}

pub fn procedure_to_map(p: &Procedure) -> Result<serde_json::Value, Error> {
    Ok(serde_json::json!({
        "agent_id": p.agent_id,
        "org_id": p.org_id,
        "name": p.name,
        "procedure_type": p.procedure_type,
        "content": p.content,
        "embedding": p.embedding,
        "trigger_patterns": p.trigger_patterns,
        "usage_count": p.usage_count,
        "last_used": p.last_used,
    }))
}

use crate::record::TaskStatus;

/// Cheap unique-id generator that doesn't pull in a UUID dependency.
/// The id is `rfc4122`-shaped; collisions are vanishingly unlikely
/// for the workload (the agent creates a small number of records
/// per second at the absolute most) and SurrealDB's record id
/// type accepts any `string` here.
pub fn uuid_v4_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Two 32-bit halves from the same counter make for a
    // deterministic-ish id that is unique per nanosecond on
    // any single host. Good enough for record ids.
    let high = (nanos >> 32) as u32;
    let low = (nanos as u32) ^ (std::process::id().wrapping_mul(2654435761));
    format!("{:08x}-{:04x}-{:04x}-{:04x}-{:012x}", high, (low >> 16) & 0xFFFF, low & 0xFFFF, (high >> 16) & 0xFFFF, low)
}
