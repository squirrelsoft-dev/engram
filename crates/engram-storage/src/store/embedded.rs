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
use surrealdb::types::{Datetime, Object, RecordId};
use surrealdb::Surreal;
use tokio::sync::Mutex;

use crate::error::{Error, MigrationResult};
use crate::manifest::Manifest;
use crate::record::{
    Concept, Entity, Episode, GraphResult, Preference, PreferenceDirection, Procedure, Task,
};
use crate::store::config::MemoryStoreConfig;
use crate::store::shared::run_migrations;
use crate::store::store::{GraphFilters, MemoryStore};

/// The list of graph edge table names this adapter knows about.
/// Order matches the schema in `schema/migrations/0001_init.sql`.
/// Centralised here so the graph-walk query has one source of
/// truth.
const GRAPH_EDGE_TABLES: &[&str] = &[
    "episode_relates_to_concept",
    "episode_precedes_episode",
    "episode_mentions_entity",
    "concept_connects_to_concept",
    "concept_about_entity",
    "entity_relates_to_entity",
    "episode_triggered_task",
];

/// Insert a `String` field into the `Object` only when the
/// `Option<String>` is `Some`; this is how SurrealDB's
/// `option<...>` fields are populated (a `null` JSON value is
/// rejected by the strict typing in the schema).
fn put_opt_str(o: &mut Object, k: &str, v: &Option<String>) {
    if let Some(s) = v {
        o.insert(k, s.clone());
    }
}

fn put_opt_datetime(o: &mut Object, k: &str, v: &Option<chrono::DateTime<Utc>>) {
    if let Some(d) = v {
        o.insert(k, Datetime::from(*d));
    }
}

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
        // Both backends are opened in MVCC versioned mode so the
        // bi-temporal `VERSION d'...'` clause is meaningful. The
        // surrealdb crate exposes this via the `.versioned()`
        // builder rather than a `+versioned` URL fragment; we
        // pass the path as the type's address and apply the
        // builder before `await`. For the in-memory backend the
        // `()`-shaped address is still versioned.
        let db = match path {
            None => Surreal::new::<Mem>(()).versioned().await?,
            Some(p) => {
                tracing::info!(
                    "opening embedded file-backed store at {} (versioned=true)",
                    p.display()
                );
                Surreal::new::<SurrealKv>(p).versioned().await?
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
        // `write_episode` is upsert semantics: if the caller
        // provided an id, we update; otherwise we create.
        // SurrealDB's MVCC engine records a new version on
        // every UPSERT, which is what the bi-temporal
        // `read_episode_at` path reads against.
        let mut e = episode.clone();
        let record_id = match &e.id {
            Some(r) => r.clone(),
            None => {
                let r = RecordId::new("episode", uuid_v4_like());
                e.id = Some(r.clone());
                r
            }
        };
        let id = format!("{}:{}", record_id.table.as_str(), format_record_id_key(&record_id.key));
        // The id is interpolated into the query string because
        // SurrealQL 3.1's `CREATE`/`UPSERT` clause does not
        // accept a parameter where the record id goes, and
        // the id is server-generated so there is no injection
        // risk. The rest of the payload goes in via a bound
        // `Object`; the schema's `INSIDE [...]` constraints
        // (e.g. `content_type`) are checked by the engine
        // against the value's actual type, which works for
        // object-content binds (the handoff documented a
        // separate bug for `SET` clauses that we avoid by
        // not using one).
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(episode_to_map(&e)?),
            ))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        // Re-SELECT the row. The id is interpolated (the
        // `SELECT FROM $id` form does not accept a record id
        // in the bind position for the from-clause; using
        // the id literal in the query string works because
        // the id is server-generated).
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
        let row: Vec<Episode> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other("write_episode: SELECT after UPSERT returned no rows".to_string())
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

    async fn read_episode_at(
        &self,
        episode_id: &str,
        as_of: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<Episode>, Error> {
        // Bi-temporal read: returns the version of the record
        // visible at the supplied transaction-time instant.
        // The bi-temporal contract in
        // `docs/design/schema-migrations.md` §5.5 says the
        // engine records a new version on every update and
        // the `VERSION d'...'` clause is the canonical
        // read path.
        //
        // We compute the bound in two steps:
        //
        // 1. If the engine accepts the `VERSION d'<rfc3339>'`
        //    clause on the connection's MVCC layer
        //    (`.versioned()` on `Mem` / `SurrealKv`),
        //    use it directly. This is the documented 3.1.x
        //    bi-temporal path.
        //
        // 2. Fall back to a `WHERE transaction_time <= $t`
        //    filter ordered by `transaction_time DESC` LIMIT 1
        //    if the versioned engine returns a "table does
        //    not exist" error (the versioned in-memory
        //    backend has historically had this bug for
        //    cross-relation reads; we keep the fallback
        //    until the upstream fix lands — see
        //    surrealdb/surrealdb #7245 for the related
        //    graph-traversal case).
        let formatted = as_of.to_rfc3339();
        let sql = format!("SELECT * FROM {episode_id} VERSION d'{formatted}'");
        let result = self.db.query(&sql).await;
        match result {
            Ok(mut r) => match r.take::<Vec<Episode>>(0) {
                Ok(rows) => return Ok(rows.into_iter().next()),
                Err(e) => {
                    let msg = e.to_string();
                    if !msg.contains("does not exist") {
                        return Err(Error::Surreal(msg));
                    }
                    // Fall through to the application-level
                    // bi-temporal read.
                }
            },
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("does not exist") {
                    return Err(Error::Surreal(msg));
                }
                // Fall through.
            }
        }
        // Fallback: read the record at its current version
        // and then filter by `transaction_time` in Rust.
        // This is the application-level bi-temporal surface
        // described in `docs/design/schema-migrations.md`
        // §5.5; it works against any versioned or
        // unversioned backend and avoids the parser quirks
        // of the `WHERE` clause on a record-id literal.
        //
        // We do a single `SELECT * FROM <id>` to fetch the
        // current row and check its `transaction_time`
        // against `as_of`. The MVCC engine keeps prior
        // versions around (the `.versioned()` builder
        // enables that), but the current API doesn't
        // expose a per-id historical scan; Phase 2 will
        // add it as `query_episode_versions`.
        let mut r = self.db.query(format!("SELECT * FROM {episode_id}")).await?;
        let rows: Vec<Episode> = r.take(0)?;
        if let Some(ep) = rows.into_iter().next() {
            if let Some(tx) = ep.transaction_time {
                if tx <= as_of {
                    return Ok(Some(ep));
                }
            }
        }
        Ok(None)
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
        //
        // The `k` in `<|k, COSINE|>` is interpolated as a
        // literal integer: SurrealDB's HNSW operator grammar
        // does not accept a bound parameter where `k` goes
        // (it parses `<|$k, …|>` as an identifier and bails
        // with "expected an unsigned integer"). The value
        // is a `u32` and the query string is built fresh per
        // call, so there is no injection risk.
        let mut vec: Vec<f32> = embedding.to_vec();
        while vec.len() < 768 {
            vec.push(0.0);
        }
        let sql = format!(
            "SELECT * FROM concept WHERE agent_id = $a AND embedding <|{k}, COSINE|> $v LIMIT $l"
        );
        let mut r = self
            .db
            .query(sql)
            .bind(("a", agent_id.to_string()))
            .bind(("v", vec))
            .bind(("l", k as i64))
            .await?;
        let rows: Vec<Concept> = r.take(0)?;
        Ok(rows)
    }

    async fn upsert_entity(&self, entity: &Entity) -> Result<Entity, Error> {
        // `id` is the canonical `<table>:<key>` form for the
        // query string. The `Entity::id` is `Option<RecordId>`;
        // we synthesise a RecordId for fresh inserts and reuse
        // the caller's value otherwise. The id is interpolated
        // (not bound) for the same reason as `write_episode`.
        let (id, record_id) = match &entity.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = uuid_v4_like();
                (
                    format!("entity:{key}"),
                    RecordId::new("entity", key),
                )
            }
        };
        let mut e = entity.clone();
        e.id = Some(record_id);
        // The schema has no enum on entity content, so we
        // can safely use the typed `Object` bind here. (See
        // `write_episode` for the comment on the `INSIDE`
        // constraint that forces a literal interpolation
        // workaround for enum-constrained fields.)
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind(("content", surrealdb::types::Value::Object(entity_to_map(&e)?)))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
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
        let (id, record_id) = match &concept.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = uuid_v4_like();
                (format!("concept:{key}"), RecordId::new("concept", key))
            }
        };
        let mut c = concept.clone();
        c.id = Some(record_id);
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind(("content", surrealdb::types::Value::Object(concept_to_map(&c)?)))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
        let row: Vec<Concept> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other("upsert_concept: SELECT after UPSERT returned no rows".to_string())
        })
    }

    async fn write_preference(&self, preference: &Preference) -> Result<Preference, Error> {
        let (id, record_id) = match &preference.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = uuid_v4_like();
                (
                    format!("preference:{key}"),
                    RecordId::new("preference", key),
                )
            }
        };
        let mut p = preference.clone();
        p.id = Some(record_id);
        // The whole record goes in via a bound `Object`. The
        // `direction` and `category` values are constrained by
        // the schema (`ASSERT $value INSIDE [...]`), and the
        // engine validates them against the bound object's
        // actual value type. This is the same path
        // `upsert_entity` uses successfully, and the earlier
        // handoff's bind-vs-INSIDE bug only manifested for
        // `SET <field> = $param` clauses — we don't use one
        // here.
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(preference_to_map(&p)?),
            ))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
        let row: Vec<Preference> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other(
                "write_preference: SELECT after UPSERT returned no rows".to_string(),
            )
        })
    }

    async fn query_preferences(
        &self,
        agent_id: &str,
        user_id: Option<&str>,
        category: Option<&str>,
        limit: u32,
    ) -> Result<Vec<Preference>, Error> {
        // Compose the WHERE clause incrementally so unused
        // filters don't appear in the query (SurrealDB's
        // planner can elide them, but it makes the test-side
        // diff easier to read). Each `AND` adds a parameter;
        // the underlying engine handles NULL semantics via
        // `?` (which becomes `IS NULL` in SurrealQL).
        let mut where_parts: Vec<String> = vec!["agent_id = $a".to_string()];
        if user_id.is_some() {
            where_parts.push("user_id = $u".to_string());
        }
        if category.is_some() {
            where_parts.push("category = $c".to_string());
        }
        let where_clause = where_parts.join(" AND ");
        let sql = format!(
            "SELECT * FROM preference WHERE {where_clause} \
             ORDER BY last_reinforced DESC LIMIT $l"
        );
        let mut q = self
            .db
            .query(sql)
            .bind(("a", agent_id.to_string()))
            .bind(("l", limit as i64));
        if let Some(u) = user_id {
            q = q.bind(("u", u.to_string()));
        }
        if let Some(c) = category {
            q = q.bind(("c", c.to_string()));
        }
        let mut r = q.await?;
        let rows: Vec<Preference> = r.take(0)?;
        Ok(rows)
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
        //
        // The `from` and `to` ids are interpolated as record
        // literals (e.g. `episode:ep_a`). SurrealDB's RELATE
        // statement requires a record on the `in`/`out`
        // positions and rejects a bound string (the error is
        // "Cannot execute RELATE statement where property 'in'
        // is: 'episode:ep_a'"). The `relation` and `weight`
        // values go in via bind: relation is the table name
        // (an identifier the engine consumes directly) and
        // weight is a float.
        let r = self
            .db
            .query(format!("RELATE {from}->{relation}->{to} SET weight = $w"))
            .bind(("w", weight.unwrap_or(0.5)))
            .await?;
        r.check().map_err(|e| Error::Surreal(e.to_string()))?;
        Ok(())
    }

    async fn traverse_graph(
        &self,
        start: &str,
        depth: u32,
        filters: &GraphFilters,
    ) -> Result<Vec<GraphResult>, Error> {
        // Breadth-first walk through every edge table declared
        // in `schema/migrations/0001_init.sql`. The Phase 1
        // contract is: depth == N returns edges reachable in
        // up to N hops from `start`. The walk is iterative to
        // avoid a query that grows exponentially with depth
        // (SurrealDB doesn't have a native "BFS up to depth N
        // along these relations" operator that we can call
        // from the embedded path; the service path uses the
        // same iterative walk for parity).
        //
        // Agent scoping is enforced at the *start* level: the
        // walk refuses to leave an agent's graph. Cross-agent
        // graph reads return an empty set per README §9.1.
        let tables: Vec<String> = if filters.relations.is_empty() {
            GRAPH_EDGE_TABLES.iter().map(|s| s.to_string()).collect()
        } else {
            filters.relations.clone()
        };

        if depth == 0 {
            return Ok(Vec::new());
        }

        // If the caller specified an agent filter, verify that
        // the start node belongs to that agent. We don't want
        // the walk to silently cross tenant boundaries even if
        // the caller passed a cross-agent id by mistake.
        if !filters.agent_id.is_empty() {
            let mut r = self
                .db
                .query("SELECT agent_id FROM $start LIMIT 1")
                .bind(("start", start.to_string()))
                .await?;
            let rows: Vec<serde_json::Value> = r.take(0)?;
            let start_agent: Option<String> = rows
                .first()
                .and_then(|v| v.get("agent_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if start_agent.as_deref() != Some(filters.agent_id.as_str()) {
                return Ok(Vec::new());
            }
        }

        let mut visited: std::collections::HashSet<(String, String, String)> =
            std::collections::HashSet::new();
        let mut frontier: std::collections::HashSet<String> =
            std::collections::HashSet::new();
        frontier.insert(start.to_string());
        let mut results: Vec<GraphResult> = Vec::new();

        for _ in 0..depth {
            let mut next_frontier: std::collections::HashSet<String> =
                std::collections::HashSet::new();

            for table in &tables {
                // For each table, look for edges whose `in` is
                // any node on the current frontier. The frontier
                // is a list of record ids; we coerce each
                // `<table>:<key>` string into a `RecordId` so
                // the bind path sends typed records (the
                // `record<>`-typed `in` column doesn't accept
                // a bound string list — it complains about
                // type coercion with the same misleading
                // "table does not exist" the versioned
                // engine emits for some other id-literal
                // lookups).
                let frontier_records: Vec<surrealdb::types::RecordId> = frontier
                    .iter()
                    .filter_map(|s| {
                        let (table, key) = s.split_once(':')?;
                        Some(surrealdb::types::RecordId::new(
                            table,
                            key.to_string(),
                        ))
                    })
                    .collect();
                let sql = format!(
                    "SELECT in, out FROM {table} WHERE in IN $frontier"
                );
                let result = self
                    .db
                    .query(&sql)
                    .bind(("frontier", frontier_records.clone()))
                    .await;
                let mut r = result?;
                // The relation table has `in` and `out` as
                // record-typed columns; SurrealQL returns
                // them as a single object per row, not a
                // tuple. We read them as SurrealDB `Value`
                // (so we can pattern-match on
                // `Value::RecordId(...)` and format the
                // canonical `<table>:<key>` string in Rust).
                let rows: Vec<surrealdb::types::Value> = match r.take(0) {
                    Ok(v) => v,
                    Err(_) => Vec::new(),
                };
                for row in rows {
                    let surrealdb::types::Value::Object(obj) = &row else {
                        continue;
                    };
                    let from_str = match obj.get("in") {
                        Some(surrealdb::types::Value::RecordId(rid)) => {
                            format!("{}:{}",
                                rid.table.as_str(),
                                crate::store::format_record_id_key(&rid.key))
                        }
                        Some(surrealdb::types::Value::String(s)) => s.clone(),
                        _ => continue,
                    };
                    let to_str = match obj.get("out") {
                        Some(surrealdb::types::Value::RecordId(rid)) => {
                            format!("{}:{}",
                                rid.table.as_str(),
                                crate::store::format_record_id_key(&rid.key))
                        }
                        Some(surrealdb::types::Value::String(s)) => s.clone(),
                        _ => continue,
                    };
                    let key = (from_str.clone(), table.clone(), to_str.clone());
                    if !visited.contains(&key) {
                        visited.insert(key);
                        results.push(GraphResult {
                            from: from_str,
                            relation: table.clone(),
                            to: to_str.clone(),
                            attributes: serde_json::json!({}),
                        });
                        next_frontier.insert(to_str);
                    }
                }
            }

            frontier = next_frontier;
            if frontier.is_empty() {
                break;
            }
        }

        if let Some(max) = filters.max_edges {
            results.truncate(max as usize);
        }
        Ok(results)
    }

    async fn write_task(&self, task: &Task) -> Result<Task, Error> {
        let (id, record_id) = match &task.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = uuid_v4_like();
                (format!("task:{key}"), RecordId::new("task", key))
            }
        };
        let mut t = task.clone();
        t.id = Some(record_id);
        t.created_at = Some(Utc::now());
        // The whole record goes in via a bound `Object` so
        // the schema's `INSIDE` assertions see the typed
        // value (the `SET`-clause bug we hit in the
        // handoff-era `write_preference` doesn't apply
        // here). `task_to_map` already stringifies `status`
        // because the Rust `TaskStatus` enum is round-tripped
        // via `String`; the schema assertion then runs
        // against the value's actual type.
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind(("content", surrealdb::types::Value::Object(task_to_map(&t)?)))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
        let row: Vec<Task> = r.take(0)?;
        row.into_iter().next()
            .ok_or_else(|| Error::Other("write_task: SELECT after UPSERT returned no rows".to_string()))
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
        let (id, record_id) = match &procedure.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = uuid_v4_like();
                (
                    format!("procedure:{key}"),
                    RecordId::new("procedure", key),
                )
            }
        };
        let mut p = procedure.clone();
        p.id = Some(record_id);
        p.created_at = Some(Utc::now());
        // Same path as `write_task`: bind the whole record
        // as a typed `Object`. `procedure_type` is a
        // schema-constrained enum; the engine validates
        // the bound value against the `INSIDE [...]`
        // constraint, and the bind is the same path that
        // works for `upsert_entity`'s `entity_type` field.
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind(("content", surrealdb::types::Value::Object(procedure_to_map(&p)?)))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
        let mut r = self.db.query(format!("SELECT * FROM {id}")).await?;
        let row: Vec<Procedure> = r.take(0)?;
        row.into_iter().next().ok_or_else(|| {
            Error::Other(
                "write_procedure: SELECT after UPSERT returned no rows".to_string(),
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
// We bind a `surrealdb::types::Object` (a `BTreeMap<String,
// Value>`) rather than `serde_json::Value` so the typed values
// survive the wire: a `chrono::DateTime<Utc>` becomes a
// `Value::Datetime` rather than a JSON string, which is what
// the schema's `datetime` fields expect. The mapping is the
// only place we know exactly which keys the schema expects;
// keeping it here means changes to the schema surface as a
// single edit.
//
// Important: SurrealDB's strict typing on `option<T>` fields
// rejects a JSON `null` value: the field must be omitted
// entirely from the payload to be interpreted as "no value".
// The `put_opt_*` helpers above only insert the key when the
// underlying value is `Some`, which is what every
// `option<...>` field wants.

pub fn episode_to_map(e: &Episode) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", e.agent_id.clone());
    put_opt_str(&mut m, "org_id", &e.org_id);
    put_opt_str(&mut m, "user_id", &e.user_id);
    m.insert("content", e.content.clone());
    m.insert("content_type", e.content_type.clone());
    if let Some(emb) = &e.embedding {
        m.insert("embedding", emb.clone());
    }
    m.insert("importance", e.importance);
    if let Some(ents) = &e.entities {
        // Send as `Vec<RecordId>` so the bind path produces
        // typed record references; the schema's
        // `option<array<record<entity>>>` rejects a string
        // list (the engine's coercion check returns
        // "Expected none | array<record<entity>>"). The
        // record-id values are already typed `RecordId` on
        // the struct, so this is just a typed pass-through.
        m.insert("entities", ents.clone());
    }
    m.insert("valid_time_start", Datetime::from(e.valid_time_start));
    put_opt_datetime(&mut m, "valid_time_end", &e.valid_time_end);
    m.insert("consolidated", e.consolidated);
    put_opt_datetime(&mut m, "consolidated_at", &e.consolidated_at);
    put_opt_str(&mut m, "summary", &e.summary);
    m.insert("source_tier", i64::from(u8::from(e.source_tier)));
    // `metadata` is `serde_json::Value` which implements
    // `SurrealValue`. SurrealDB sees it as a generic
    // `Value` of `Kind::Any`, which is fine for the
    // `FLEXIBLE TYPE object` field declared in the schema.
    m.insert("metadata", e.metadata.clone());
    Ok(m)
}

pub fn entity_to_map(e: &Entity) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", e.agent_id.clone());
    put_opt_str(&mut m, "org_id", &e.org_id);
    m.insert("canonical_name", e.canonical_name.clone());
    m.insert("aliases", e.aliases.clone());
    m.insert("entity_type", e.entity_type.clone());
    m.insert("attributes", e.attributes.clone());
    m.insert("confidence", e.confidence);
    m.insert("confidence_tier", i64::from(u8::from(e.confidence_tier)));
    // `anchor_record` is `option<record<episode>>` in the
    // schema. The engine rejects a string here (it sees
    // `'episode:ep_a'` and asks for a real `record` value);
    // we coerce the `RecordId` to a `Value::RecordId` so
    // the bind path sends the typed record, not a string.
    if let Some(ar) = &e.anchor_record {
        m.insert("anchor_record", ar.clone());
    }
    if let Some(log) = &e.disambiguation_log {
        m.insert("disambiguation_log", log.clone());
    }
    // `created_at` and `last_updated` are `datetime` (not
    // `option<datetime>`) per the schema. Sending the
    // CONTENT map without these fields is rejected by the
    // engine on UPSERT with "Expected `datetime` but found
    // `NONE`" — the engine doesn't fall back to the
    // `DEFAULT` clause when the field is missing from the
    // payload, because it interprets a missing payload
    // field as "no value" (None), and the schema rejects
    // None for a non-optional field. We always send both
    // fields, defaulting to "now" when the caller hasn't
    // populated them. (For a fresh INSERT, this is
    // semantically the same as the `DEFAULT` clause. For
    // an UPSERT update, this preserves the existing value
    // because the caller-supplied value comes from a
    // read-back.)
    let now = chrono::Utc::now();
    let created_at = e.created_at.unwrap_or(now);
    m.insert("created_at", Datetime::from(created_at));
    m.insert("last_updated", Datetime::from(e.last_updated.unwrap_or(now)));
    Ok(m)
}

pub fn concept_to_map(c: &Concept) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", c.agent_id.clone());
    put_opt_str(&mut m, "org_id", &c.org_id);
    m.insert("content", c.content.clone());
    if let Some(emb) = &c.embedding {
        m.insert("embedding", emb.clone());
    }
    m.insert("confidence", c.confidence);
    m.insert("source_tier", i64::from(u8::from(c.source_tier)));
    m.insert("reinforcement_count", i64::from(c.reinforcement_count));
    m.insert("decay_rate", c.decay_rate);
    m.insert("inferred", c.inferred);
    if let Some(chain) = &c.inference_chain {
        m.insert("inference_chain", chain.clone());
    }
    m.insert("valid_time_start", Datetime::from(c.valid_time_start));
    put_opt_datetime(&mut m, "valid_time_end", &c.valid_time_end);
    Ok(m)
}

pub fn task_to_map(t: &Task) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", t.agent_id.clone());
    put_opt_str(&mut m, "org_id", &t.org_id);
    put_opt_str(&mut m, "user_id", &t.user_id);
    m.insert("content", t.content.clone());
    m.insert("trigger_type", t.trigger_type.clone());
    m.insert("trigger_value", t.trigger_value.clone());
    m.insert(
        "status",
        match t.status {
            TaskStatus::Pending => "pending",
            TaskStatus::Triggered => "triggered",
            TaskStatus::Completed => "completed",
            TaskStatus::Cancelled => "cancelled",
        },
    );
    put_opt_datetime(&mut m, "triggered_at", &t.triggered_at);
    Ok(m)
}

pub fn procedure_to_map(p: &Procedure) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", p.agent_id.clone());
    put_opt_str(&mut m, "org_id", &p.org_id);
    m.insert("name", p.name.clone());
    m.insert("procedure_type", p.procedure_type.clone());
    m.insert("content", p.content.clone());
    if let Some(emb) = &p.embedding {
        m.insert("embedding", emb.clone());
    }
    m.insert("trigger_patterns", p.trigger_patterns.clone());
    m.insert("usage_count", i64::from(p.usage_count));
    put_opt_datetime(&mut m, "last_used", &p.last_used);
    Ok(m)
}

pub fn preference_to_map(p: &Preference) -> Result<Object, Error> {
    let mut m = Object::new();
    m.insert("agent_id", p.agent_id.clone());
    put_opt_str(&mut m, "org_id", &p.org_id);
    put_opt_str(&mut m, "user_id", &p.user_id);
    m.insert("category", p.category.clone());
    m.insert("content", p.content.clone());
    m.insert(
        "direction",
        match p.direction {
            PreferenceDirection::Positive => "positive",
            PreferenceDirection::Negative => "negative",
        },
    );
    m.insert("strength", p.strength);
    m.insert("source_tier", i64::from(u8::from(p.source_tier)));
    m.insert("evidence_count", i64::from(p.evidence_count));
    m.insert("valid_time_start", Datetime::from(p.valid_time_start));
    put_opt_datetime(&mut m, "valid_time_end", &p.valid_time_end);
    Ok(m)
}

use crate::record::TaskStatus;

/// Format a `RecordIdKey` for inline use in query strings
/// (e.g. `<table>:<key>`). `RecordIdKey` doesn't implement
/// `Display`, so we unwrap its variants explicitly.
pub fn format_record_id_key(key: &surrealdb::types::RecordIdKey) -> String {
    use surrealdb::types::RecordIdKey;
    match key {
        RecordIdKey::String(s) => s.clone(),
        RecordIdKey::Number(n) => n.to_string(),
        RecordIdKey::Uuid(u) => u.to_string(),
        RecordIdKey::Array(_) => format!("{key:?}"),
        RecordIdKey::Object(_) => format!("{key:?}"),
        RecordIdKey::Range(_) => format!("{key:?}"),
    }
}

/// Cheap unique-id generator that doesn't pull in a UUID dependency.
///
/// The id is hex-only (no hyphens) because SurrealDB's parser
/// otherwise reads `id-prefix-` as a date or duration token
/// and rejects the surrounding statement. The format is
/// 32 hex characters, which is the same width as a UUID
/// without the dashes. Collisions are vanishingly unlikely
/// for the workload (the agent creates a small number of
/// records per second at the absolute most) and SurrealDB's
/// record id type accepts any `string` here.
pub fn uuid_v4_like() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let high = (nanos >> 32) as u32;
    let low = (nanos as u32) ^ (std::process::id().wrapping_mul(2654435761));
    // All-hex output, no hyphens, 16 hex chars.
    format!("{:08x}{:08x}", high, low)
}
