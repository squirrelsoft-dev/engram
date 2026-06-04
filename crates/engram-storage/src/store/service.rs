//! Service adapter: connect to an out-of-process `surreal` daemon
//! over HTTP.
//!
//! Selected by `ENGRAM_SURREAL_URL` per ADR 0001. The HTTP client
//! is the in-process `surrealdb` crate's `Http` engine — the
//! same one the parity spike used to confirm schema parity
//! between embedded and service modes.
//!
//! Spike note (carried over from
//! `spikes/schema-migrations/README.md`): the WebSocket client
//! hits a protocol-compat deadlock against the spawned
//! `surreal` 3.1.x binary. The HTTP path works and is the
//! canonical service path. WebSocket support, when the upstream
//! issue is resolved, will be a drop-in addition of a `Ws`
//! variant here.

use std::path::Path;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use surrealdb::engine::remote::http::Client as HttpClient;
use surrealdb::types::RecordId;
use surrealdb::Surreal;
use tokio::sync::Mutex;

use crate::error::{Error, MigrationResult};
use crate::manifest::Manifest;
use crate::record::{
    Concept, Entity, Episode, GraphResult, Preference, Procedure, Task,
};
use crate::store::config::MemoryStoreConfig;
use crate::store::shared::run_migrations;
use crate::store::store::{GraphFilters, MemoryStore};
use crate::format_record_id_key;

/// The list of graph edge table names this adapter knows about.
/// Centralised so the graph-walk query has one source of truth
/// and stays in lock-step with the embedded adapter.
const GRAPH_EDGE_TABLES: &[&str] = &[
    "episode_relates_to_concept",
    "episode_precedes_episode",
    "episode_mentions_entity",
    "concept_connects_to_concept",
    "concept_about_entity",
    "entity_relates_to_entity",
    "episode_triggered_task",
];

/// Service-mode store. Holds an `HttpClient` connection and the
/// same metadata the embedded adapter holds.
#[derive(Debug)]
pub struct ServiceStore {
    db: Surreal<HttpClient>,
    config: MemoryStoreConfig,
    last_migration: Arc<Mutex<Option<MigrationResult>>>,
}

impl ServiceStore {
    /// Connect to a `surreal` service at `url` and apply migrations.
    /// Credentials default to `root/root` per the spike's working
    /// values; in production these come from
    /// `ENGRAM_SURREAL_USER` / `ENGRAM_SURREAL_PASS`.
    pub async fn connect(
        config: &MemoryStoreConfig,
        url: &str,
        user: &str,
        pass: &str,
    ) -> Result<Self, Error> {
        // The SurrealDB `IntoEndpoint<Http>` impl for `&str` does
        // `format!("http://{self}")`, so we accept either the
        // bare `host:port` form or a full `http://host:port`
        // and normalise before handing it to the client.
        let endpoint = if url.starts_with("http://") || url.starts_with("https://") {
            url.trim_start_matches("http://").trim_start_matches("https://").to_string()
        } else {
            url.to_string()
        };

        let db: Surreal<HttpClient> = Surreal::new::<surrealdb::engine::remote::http::Http>(&endpoint)
            .await
            .map_err(|e| Error::Surreal(format!("connecting to {endpoint}: {e}")))?;

        db.signin(surrealdb::opt::auth::Root {
            username: user.to_string(),
            password: pass.to_string(),
        })
        .await?;

        db.use_ns(&config.namespace).use_db(&config.database).await?;

        let store = ServiceStore {
            db,
            config: config.clone(),
            last_migration: Arc::new(Mutex::new(None)),
        };
        let result = store.apply_migrations().await?;
        *store.last_migration.lock().await = Some(result);
        Ok(store)
    }

    /// The SurrealDB connection, for tests that need to assert
    /// directly against the engine. Not part of the public trait.
    pub fn raw(&self) -> &Surreal<HttpClient> {
        &self.db
    }
}

#[async_trait]
impl MemoryStore for ServiceStore {
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
        // Upsert semantics: with a caller-provided id this
        // updates the record (and creates a new MVCC
        // version, which is what the bi-temporal
        // `read_episode_at` path reads against); with a
        // missing id we generate one. The whole record goes
        // in via a bound `Object`; the `content_type`
        // schema constraint is checked by the engine
        // against the value's actual type.
        let mut e = episode.clone();
        let record_id = match &e.id {
            Some(r) => r.clone(),
            None => {
                let r = RecordId::new("episode", crate::store::embedded::uuid_v4_like());
                e.id = Some(r.clone());
                r
            }
        };
        let id = format!("{}:{}", record_id.table.as_str(), format_record_id_key(&record_id.key));
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::episode_to_map(&e)?),
            ))
            .await?
            .check()
            .map_err(|err| Error::Surreal(err.to_string()))?;
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
        // See the embedded adapter for the full bi-temporal
        // read strategy: try `VERSION d'...'` first (the
        // documented MVCC path), fall back to
        // `WHERE transaction_time <= $t ORDER BY
        // transaction_time DESC LIMIT 1` on the
        // "table does not exist" error that the
        // versioned engine currently emits for some
        // id-literal lookups.
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
                }
            },
            Err(e) => {
                let msg = e.to_string();
                if !msg.contains("does not exist") {
                    return Err(Error::Surreal(msg));
                }
            }
        }
        // Fallback path mirrors the embedded adapter: do a
        // single `SELECT * FROM <id>` and filter by
        // `transaction_time` in Rust. The MVCC engine keeps
        // prior versions around (the `.versioned()` builder
        // enables that) but the current API doesn't expose
        // a per-id historical scan; Phase 2 will add it as
        // `query_episode_versions`.
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
        let mut vec: Vec<f32> = embedding.to_vec();
        while vec.len() < 768 {
            vec.push(0.0);
        }
        // `k` is interpolated as a literal; the HNSW
        // operator grammar does not accept a bound
        // parameter where `k` goes. See the embedded
        // adapter for the full rationale.
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
        let (id, record_id) = match &entity.id {
            Some(r) => (
                format!("{}:{}", r.table.as_str(), format_record_id_key(&r.key)),
                r.clone(),
            ),
            None => {
                let key = crate::store::embedded::uuid_v4_like();
                (format!("entity:{key}"), RecordId::new("entity", key))
            }
        };
        let mut e = entity.clone();
        e.id = Some(record_id);
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::entity_to_map(&e)?),
            ))
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
                let key = crate::store::embedded::uuid_v4_like();
                (format!("concept:{key}"), RecordId::new("concept", key))
            }
        };
        let mut c = concept.clone();
        c.id = Some(record_id);
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::concept_to_map(&c)?),
            ))
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
                let key = crate::store::embedded::uuid_v4_like();
                (format!("preference:{key}"), RecordId::new("preference", key))
            }
        };
        let mut p = preference.clone();
        p.id = Some(record_id);
        // Same path as the embedded adapter: bind the whole
        // record as a typed `Object`, with the
        // schema-constrained `direction` / `category`
        // values as actual fields. The engine validates
        // them via `ASSERT $value INSIDE [...]`.
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::preference_to_map(&p)?),
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
        // See the embedded adapter for the full rationale on
        // interpolating the record ids vs binding them: the
        // RELATE statement requires real record values on
        // `in`/`out` and rejects a bound string with the
        // error "Cannot execute RELATE statement where
        // property 'in' is: '<id>'". The `relation`
        // identifier and `weight` value are bound.
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
        // Same iterative BFS as the embedded adapter — we keep
        // the two paths algorithmically identical so parity
        // tests don't need to special-case mode.
        let tables: Vec<String> = if filters.relations.is_empty() {
            GRAPH_EDGE_TABLES.iter().map(|s| s.to_string()).collect()
        } else {
            filters.relations.clone()
        };

        if depth == 0 {
            return Ok(Vec::new());
        }

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
                // Coerce each `<table>:<key>` frontier string
                // into a typed `RecordId` so the bind path
                // sends records (the `record<>`-typed `in`
                // column doesn't accept a bound string list).
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
                let mut r = self
                    .db
                    .query(sql)
                    .bind(("frontier", frontier_records))
                    .await?;
                let rows: Vec<surrealdb::types::Value> =
                    r.take(0).unwrap_or_default();
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
                let key = crate::store::embedded::uuid_v4_like();
                (format!("task:{key}"), RecordId::new("task", key))
            }
        };
        let mut t = task.clone();
        t.id = Some(record_id);
        t.created_at = Some(Utc::now());
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::task_to_map(&t)?),
            ))
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
                let key = crate::store::embedded::uuid_v4_like();
                (format!("procedure:{key}"), RecordId::new("procedure", key))
            }
        };
        let mut p = procedure.clone();
        p.id = Some(record_id);
        p.created_at = Some(Utc::now());
        let sql = format!("UPSERT {id} CONTENT $content");
        self.db
            .query(&sql)
            .bind((
                "content",
                surrealdb::types::Value::Object(crate::store::embedded::procedure_to_map(&p)?),
            ))
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
