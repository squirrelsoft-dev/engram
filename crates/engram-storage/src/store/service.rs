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
        let mut e = episode.clone();
        if e.id.is_none() {
            e.id = Some(format!("episode:{}", crate::store::embedded::uuid_v4_like()));
        }
        let id = e.id.clone().unwrap();
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", crate::store::embedded::episode_to_map(&e)?))
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
            .unwrap_or_else(|| format!("entity:{}", crate::store::embedded::uuid_v4_like()));
        let mut e = entity.clone();
        e.id = Some(id.clone());
        self.db
            .query("UPSERT $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", crate::store::embedded::entity_to_map(&e)?))
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
            .unwrap_or_else(|| format!("concept:{}", crate::store::embedded::uuid_v4_like()));
        let mut c = concept.clone();
        c.id = Some(id.clone());
        self.db
            .query("UPSERT $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", crate::store::embedded::concept_to_map(&c)?))
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
            .unwrap_or_else(|| format!("task:{}", crate::store::embedded::uuid_v4_like()));
        let mut t = task.clone();
        t.id = Some(id.clone());
        t.created_at = Some(Utc::now());
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", crate::store::embedded::task_to_map(&t)?))
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
            .unwrap_or_else(|| format!("procedure:{}", crate::store::embedded::uuid_v4_like()));
        let mut p = procedure.clone();
        p.id = Some(id.clone());
        p.created_at = Some(Utc::now());
        self.db
            .query("CREATE $id CONTENT $content")
            .bind(("id", id.clone()))
            .bind(("content", crate::store::embedded::procedure_to_map(&p)?))
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
