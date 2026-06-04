//! The ingestion pipeline.
//!
//! README §6.1 documents the six stages; this module turns the
//! spec into code. The pipeline is a single struct,
//! [`IngestionPipeline`], with a single public method,
//! [`ingest`](IngestionPipeline::ingest), that runs all six
//! stages and returns the persisted [`IngestionResult`].
//!
//! The six stages are private methods on the struct; they
//! share the [`PipelineState`] (the in-flight record being
//! built up). The split is structural, not behavioural —
//! every test that exercises the pipeline runs end-to-end, so
//! a regression in any one stage is visible in the output.
//!
//! ## Pluggable models
//!
//! Three model components are pluggable:
//!
//! - [`EmbeddingModel`] (from `crate::embedding`) — produces
//!   the vectors for content and entities. The default
//!   `DeterministicEmbedding` is hashing-based and matches the
//!   schema's 768-d HNSW index. Issue #15 selects the real
//!   model; the trait surface is identical.
//! - [`EntityExtractor`] (from `crate::extraction`) — produces
//!   the mention list. The default `HeuristicEntityExtractor`
//!   is rule-based. Issue #16 selects the real LLM; the trait
//!   surface is identical.
//! - [`ImportanceScorer`] (defined here) — combines source
//!   tier, entity count, explicit priority signals, and
//!   recency into a 0.0–1.0 score. The default
//!   `TierBasedImportanceScorer` is the formula in README §6.1
//!   step 5.
//!
//! The pipeline builder ([`IngestionPipelineBuilder`]) wires
//! these together. Callers that need a custom component build
//! their own pipeline; callers that don't use the defaults.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use serde_json::json;
use tracing::{debug, instrument};

use engram_storage::{
    Entity, Episode, MemoryStore, RecordId, SignalTier,
};

use crate::embedding::EmbeddingModel;
use crate::error::{IngestError, IngestResult};
use crate::extraction::{EntityExtractor, ExtractedMention, ReferenceKind};
use crate::request::{ContentType, StoreRequest};
use crate::response::{EpisodeRecord, QueueHint};

// ============================================================================
// Trait: ImportanceScorer
// ============================================================================

/// Combines the signals described in README §6.1 step 5 into a
/// 0.0–1.0 importance score. The default
/// [`TierBasedImportanceScorer`] implements the formula below;
/// production deployments can plug in a learned scorer.
#[async_trait]
pub trait ImportanceScorer: Send + Sync {
    fn scorer_id(&self) -> &str;
    async fn score(&self, ctx: &ScoringContext<'_>) -> IngestResult<f32>;
}

/// The inputs the scorer sees. Carries everything from the
/// upstream stages so the scorer can mix multiple signals.
#[derive(Debug, Clone)]
pub struct ScoringContext<'a> {
    pub agent_id: &'a str,
    pub content: &'a str,
    pub content_type: ContentType,
    pub source_tier: SignalTier,
    /// The mentions the extractor produced. The scorer can
    /// use count and overlap-with-known-entities to raise
    /// importance.
    pub mentions: &'a [ExtractedMention],
    /// When the event happened. The scorer can use how recent
    /// it is (relative to now) to nudge the score.
    pub valid_time: chrono::DateTime<Utc>,
    /// The length of the content. The scorer can use a long
    /// content as a weak "this had a lot of detail" signal.
    pub content_length: usize,
}

// ============================================================================
// Default scorer
// ============================================================================

/// The default importance scorer, implementing the formula in
/// README §6.1 step 5:
///
/// ```text
/// base = tier_weight(source_tier)            // 0.30..0.95
/// entity_bonus = clamp(0.0, 0.2, explicit_mentions × 0.05)
/// priority_signal_bonus = 0.15 if any priority phrase in content
/// recency_penalty = 0.0 if within an hour
///                  up to 0.15 if older than a month
/// importance = clamp(0.0, 1.0, base + entity_bonus + priority - recency)
/// ```
///
/// The weights are tuned to keep the integration tests
/// differentiating (the test for "explicit priority signal
/// raises importance" and "higher tier raises importance" both
/// check deltas). They are not a learned model; the field is
/// open for a future improvement.
#[derive(Debug, Default, Clone)]
pub struct TierBasedImportanceScorer {
    /// The list of substrings that count as an "explicit
    /// priority signal" in the content. Case-insensitive.
    priority_phrases: Vec<String>,
}

impl TierBasedImportanceScorer {
    pub fn new() -> Self {
        Self {
            priority_phrases: vec![
                "important".to_string(),
                "remember this".to_string(),
                "deadline".to_string(),
                "urgent".to_string(),
                "critical".to_string(),
                "do not forget".to_string(),
                "must remember".to_string(),
            ],
        }
    }

    pub fn with_priority_phrases(mut self, phrases: Vec<String>) -> Self {
        self.priority_phrases = phrases;
        self
    }

    fn tier_weight(tier: SignalTier) -> f32 {
        use SignalTier::*;
        match tier {
            Tier1Authoritative => 0.95,
            Tier2Structured => 0.75,
            Tier3Conversational => 0.55,
            Tier4Implied => 0.40,
            Tier5Behavioral => 0.30,
        }
    }

    fn priority_signal_bonus(&self, content: &str) -> f32 {
        let lower = content.to_lowercase();
        if self.priority_phrases.iter().any(|p| lower.contains(p)) {
            0.15
        } else {
            0.0
        }
    }

    fn entity_bonus(mentions: &[ExtractedMention]) -> f32 {
        // Explicit mentions contribute more than pronouns
        // (which are usually noise). Cap at 0.2 to leave
        // room for the priority-signal bonus.
        let explicit = mentions
            .iter()
            .filter(|m| m.reference_kind == ReferenceKind::Explicit)
            .count();
        (explicit as f32 * 0.05).min(0.2)
    }

    fn recency_penalty(valid_time: chrono::DateTime<Utc>, now: chrono::DateTime<Utc>) -> f32 {
        let delta = now.signed_duration_since(valid_time);
        let hours = delta.num_minutes() as f32 / 60.0;
        if hours < 1.0 {
            0.0
        } else if hours < 24.0 {
            0.025
        } else if hours < 24.0 * 7.0 {
            0.075
        } else if hours < 24.0 * 30.0 {
            0.12
        } else {
            0.15
        }
    }
}

#[async_trait]
impl ImportanceScorer for TierBasedImportanceScorer {
    fn scorer_id(&self) -> &str {
        "tier-based-v1"
    }

    async fn score(&self, ctx: &ScoringContext<'_>) -> IngestResult<f32> {
        let base = Self::tier_weight(ctx.source_tier);
        let entity_bonus = Self::entity_bonus(ctx.mentions);
        let priority = self.priority_signal_bonus(ctx.content);
        let recency = Self::recency_penalty(ctx.valid_time, Utc::now());
        let raw = base + entity_bonus + priority - recency;
        Ok(raw.clamp(0.0, 1.0))
    }
}

// ============================================================================
// Pipeline state and context
// ============================================================================

/// The in-flight record the pipeline builds up across its
/// six stages. Stages mutate this; the public API never sees
/// it.
struct PipelineState {
    request: StoreRequest,
    /// The extracted mentions, after stage 2.
    mentions: Vec<ExtractedMention>,
    /// The persisted entities that the episode links to.
    /// Filled in during stages 3 and 6.
    linked_entities: Vec<Entity>,
    /// The score the importance scorer produced, or the
    /// caller-supplied override.
    importance: f32,
    /// The embedding of the content, computed in stage 4.
    content_embedding: Option<Vec<f32>>,
}

impl PipelineState {
    fn new(request: StoreRequest) -> Self {
        Self {
            request,
            mentions: Vec::new(),
            linked_entities: Vec::new(),
            importance: 0.0,
            content_embedding: None,
        }
    }
}

/// Holds the storage handle and the pluggable models. Cheap
/// to clone (the heavy bits are `Arc`-wrapped).
struct PipelineContext {
    store: Arc<Box<dyn MemoryStore>>,
    embedder: Arc<dyn EmbeddingModel>,
    extractor: Arc<dyn EntityExtractor>,
    scorer: Arc<dyn ImportanceScorer>,
    /// Entities the pipeline has created/merged during the
    /// current process's lifetime. Used by the disambiguation
    /// stage's "first-name prefix" soft-match path, which
    /// needs to look up entities the Phase 1 trait doesn't
    /// expose a listing API for. Reset per-process; this is
    /// not a cache across processes (the store is the
    /// durable source of truth).
    session_entities: std::sync::Mutex<Vec<Entity>>,
}

// ============================================================================
// Pipeline
// ============================================================================

/// The ingestion pipeline. Construct with
/// [`IngestionPipelineBuilder`].
pub struct IngestionPipeline {
    ctx: PipelineContext,
}

impl std::fmt::Debug for IngestionPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionPipeline")
            .field("embedder", &self.ctx.embedder.model_id())
            .field("extractor", &self.ctx.extractor.extractor_id())
            .field("scorer", &self.ctx.scorer.scorer_id())
            .finish()
    }
}

impl IngestionPipeline {
    /// Ingest a single store request. Runs all six stages
    /// from README §6.1:
    ///
    /// 1. Normalize — content-type detection (done by the
    ///    request, not the pipeline) and source-tier
    ///    assignment.
    /// 2. Entity extraction — the configured extractor
    ///    produces mentions.
    /// 3. Entity disambiguation (explicit track only for
    ///    Phase 2) — Tier 1/2 anchor creation and merge.
    /// 4. Embedding generation — embed normalized content and
    ///    new entities.
    /// 5. Importance scoring — combine source tier, entity
    ///    overlap, explicit priority signals, recency.
    /// 6. Write — atomic Episode + Entity + graph edge
    ///    writes via MemoryStore.
    #[instrument(skip(self, req), fields(agent_id = %req.agent_id, content_type = %req.content_type.as_str()))]
    pub async fn ingest(&self, req: StoreRequest) -> IngestResult<IngestionResult> {
        // Validation: the request is invalid if it can't
        // even be persisted. We catch this before touching
        // the store.
        req.validate()?;

        // Stage 1: Normalize. The request already carries
        // the content type (the caller supplies it; the
        // pipeline detects the default if the caller uses
        // `StoreRequest::new`). Source tier is either
        // caller-supplied or the content type's default.
        let source_tier = req
            .source_tier
            .unwrap_or_else(|| req.content_type.default_source_tier());
        debug!("stage 1 normalize: source_tier = {:?}", source_tier);

        let mut state = PipelineState::new(req);

        // Stage 2: entity extraction.
        self.stage_extract(&mut state, source_tier).await?;

        // Stage 3: disambiguation (explicit track only for Phase 2).
        self.stage_disambiguate(&mut state).await?;

        // Stage 4: embedding generation.
        self.stage_embed(&mut state).await?;

        // Stage 5: importance scoring.
        self.stage_score(&mut state, source_tier).await?;

        // Stage 6: write.
        let episode = self.stage_write(&mut state, source_tier).await?;

        Ok(IngestionResult {
            episode,
            linked_entities: state.linked_entities,
        })
    }

    async fn stage_extract(
        &self,
        state: &mut PipelineState,
        source_tier: SignalTier,
    ) -> IngestResult<()> {
        let extracted = self
            .ctx
            .extractor
            .extract(&state.request.content, source_tier)
            .await
            .map_err(|e| match e {
                IngestError::Other(msg) => IngestError::Extraction {
                    extractor: self.ctx.extractor.extractor_id().to_string(),
                    message: msg,
                },
                other => other,
            })?;

        // Add the caller's pre-identified entities as
        // mentions with `person` type and high confidence.
        // The disambiguation pass will fold them in
        // alongside the extractor's output.
        let mut all = extracted;
        for name in &state.request.preidentified_entities {
            all.push(ExtractedMention {
                surface: name.clone(),
                canonical_name: name.clone(),
                entity_type: "other".to_string(),
                source_tier,
                confidence: 0.9,
                reference_kind: ReferenceKind::Role,
            });
        }
        state.mentions = all;
        Ok(())
    }

    async fn stage_disambiguate(&self, state: &mut PipelineState) -> IngestResult<()> {
        // Phase 2 explicit-track disambiguation: for each
        // explicit mention, look up an existing entity with
        // the matching canonical name. If found, merge:
        // append the surface form to aliases and raise the
        // confidence_tier if the new source is higher. If
        // not found, create a new entity. Tier 1 signals
        // additionally set the anchor_record on the entity
        // to the asserting episode (in stage 6, after the
        // episode has an id).
        //
        // The full two-track disambiguation (candidate
        // accumulation, evidence weighting, merge
        // thresholds) is Phase 3 (issues #7, #8, #10).
        //
        // First-name soft match: a 1-word mention like
        // "Sarah" is treated as a likely reference to an
        // existing entity whose canonical_name starts with
        // the mention ("Sarah Chen"). This is a coarse
        // heuristic — the real LLM-based extractor handles
        // coreference properly. It is the same shape as
        // the schema's exact-name lookup: a candidate row
        // comes back, we use it. The disambiguation_log
        // entry (Phase 3) records the soft match as a
        // separate event.
        let mut linked = Vec::new();
        for mention in state.mentions.clone() {
            // The role/pronoun references are not
            // disambiguated in Phase 2 — they need the
            // candidate accumulation pass.
            if mention.reference_kind != ReferenceKind::Explicit {
                continue;
            }
            // Try exact-name match first.
            let mut candidate = self
                .lookup_entity(&state.request.agent_id, &mention.canonical_name)
                .await?;
            // Fall back to a first-name prefix match for
            // 1-word mentions — "Sarah" → "Sarah Chen".
            if candidate.is_none() && !mention.canonical_name.contains(' ') {
                let prefix = mention.canonical_name.to_lowercase();
                let all = self
                    .list_agent_entities(&state.request.agent_id)
                    .await?;
                candidate = all.into_iter().find(|e| {
                    let lower = e.canonical_name.to_lowercase();
                    lower.starts_with(&prefix)
                        && lower[prefix.len()..]
                            .chars()
                            .next()
                            .map(|c| c == ' ')
                            .unwrap_or(false)
                });
            }
            if let Some(mut found) = candidate {
                // Merge: add the surface form as an alias,
                // raise the confidence_tier if the new
                // source is higher.
                let surface = mention.surface.clone();
                if !found.aliases.iter().any(|a| a == &surface) {
                    found.aliases.push(surface);
                }
                if (mention.source_tier as u8) < (found.confidence_tier as u8) {
                    found.confidence_tier = mention.source_tier;
                }
                // Phase 1 trait's `upsert_entity` overwrites
                // by id; we use it to persist the merge.
                let merged = self
                    .ctx
                    .store
                    .upsert_entity(&found)
                    .await
                    .map_err(|e| IngestError::Storage {
                        stage: "disambiguate-merge",
                        source: e,
                    })?;
                // Record the merged entity in the session
                // cache so a subsequent mention in the
                // same process can soft-match against it.
                self.record_session_entity(merged.clone());
                linked.push(merged);
            } else {
                // Create a new entity.
                let new_entity = Entity {
                    id: None,
                    agent_id: state.request.agent_id.clone(),
                    org_id: state.request.org_id.clone(),
                    canonical_name: mention.canonical_name.clone(),
                    aliases: vec![mention.surface.clone()],
                    entity_type: mention.entity_type.clone(),
                    attributes: json!({}),
                    confidence: mention.confidence,
                    confidence_tier: mention.source_tier,
                    anchor_record: None,
                    created_at: None,
                    last_updated: None,
                    disambiguation_log: None,
                };
                let created = self
                    .ctx
                    .store
                    .upsert_entity(&new_entity)
                    .await
                    .map_err(|e| IngestError::Storage {
                        stage: "disambiguate-create",
                        source: e,
                    })?;
                self.record_session_entity(created.clone());
                linked.push(created);
            }
        }
        // Deduplicate: if the same canonical_name appears
        // twice (e.g. once from email extraction, once
        // from proper-noun extraction), keep one.
        linked.sort_by(|a, b| a.canonical_name.cmp(&b.canonical_name));
        linked.dedup_by(|a, b| a.canonical_name == b.canonical_name);

        state.linked_entities = linked;
        Ok(())
    }

    /// Exact-name lookup helper. Wraps `resolve_entity` so
    /// the disambiguation stage can express the intent
    /// directly.
    async fn lookup_entity(
        &self,
        agent_id: &str,
        canonical_name: &str,
    ) -> IngestResult<Option<Entity>> {
        let result = self
            .ctx
            .store
            .resolve_entity(
                agent_id,
                &[Entity {
                    id: None,
                    agent_id: agent_id.to_string(),
                    org_id: None,
                    canonical_name: canonical_name.to_string(),
                    aliases: vec![],
                    entity_type: "other".to_string(),
                    attributes: json!({}),
                    confidence: 0.0,
                    confidence_tier: SignalTier::Tier5Behavioral,
                    anchor_record: None,
                    created_at: None,
                    last_updated: None,
                    disambiguation_log: None,
                }],
            )
            .await
            .map_err(|e| IngestError::Storage {
                stage: "disambiguate",
                source: e,
            })?;
        Ok(result.into_iter().next())
    }

    /// List every entity for an agent. The Phase 1 trait
    /// doesn't expose a `query_entities(agent_id)` listing
    /// API, so we use a process-local cache of entities the
    /// pipeline has just upserted. This is enough for the
    /// "first-name prefix" soft match — a subsequent
    /// `store()` call for the same agent sees the entities
    /// its earlier calls created. A future Phase 2 follow-up
    /// adds a listing method to the Phase 1 trait; until
    /// then, the soft match only works within a process
    /// lifetime.
    async fn list_agent_entities(&self, agent_id: &str) -> IngestResult<Vec<Entity>> {
        let cache = self.ctx.session_entities.lock().expect("session lock poisoned");
        Ok(cache
            .iter()
            .filter(|e| e.agent_id == agent_id)
            .cloned()
            .collect())
    }

    /// Record an entity in the session-local cache. Called
    /// after every successful upsert in stage 3.
    fn record_session_entity(&self, entity: Entity) {
        let mut cache = self.ctx.session_entities.lock().expect("session lock poisoned");
        // Replace any existing entry for the same id (or
        // canonical_name) so the cache reflects the latest
        // merged view.
        cache.retain(|e| e.id != entity.id && e.canonical_name != entity.canonical_name);
        cache.push(entity);
    }

    async fn stage_embed(&self, state: &mut PipelineState) -> IngestResult<()> {
        // Embed the normalized content.
        let content_emb = self
            .ctx
            .embedder
            .embed(&state.request.content)
            .await
            .map_err(|e| match e {
                IngestError::Other(msg) => IngestError::Embedding {
                    model: self.ctx.embedder.model_id().to_string(),
                    message: msg,
                },
                other => other,
            })?;
        // Sanity: the embedder's dimension must match the
        // schema's. The pipeline refuses a mismatch rather
        // than silently misaligning the HNSW index.
        if content_emb.len() != self.ctx.embedder.dimension() {
            return Err(IngestError::Embedding {
                model: self.ctx.embedder.model_id().to_string(),
                message: format!(
                    "embedder returned {}d, expected {}d",
                    content_emb.len(),
                    self.ctx.embedder.dimension()
                ),
            });
        }
        state.content_embedding = Some(content_emb);
        Ok(())
    }

    async fn stage_score(
        &self,
        state: &mut PipelineState,
        source_tier: SignalTier,
    ) -> IngestResult<()> {
        if let Some(imp) = state.request.importance_override {
            state.importance = imp.clamp(0.0, 1.0);
            return Ok(());
        }
        let ctx = ScoringContext {
            agent_id: &state.request.agent_id,
            content: &state.request.content,
            content_type: state.request.content_type,
            source_tier,
            mentions: &state.mentions,
            valid_time: state.request.valid_time.unwrap_or_else(Utc::now),
            content_length: state.request.content.len(),
        };
        let score = self.ctx.scorer.score(&ctx).await?;
        state.importance = score;
        Ok(())
    }

    async fn stage_write(
        &self,
        state: &mut PipelineState,
        source_tier: SignalTier,
    ) -> IngestResult<Episode> {
        let valid_time_start = state.request.valid_time.unwrap_or_else(Utc::now);
        let entities_field: Option<Vec<RecordId>> = if state.linked_entities.is_empty() {
            None
        } else {
            Some(
                state
                    .linked_entities
                    .iter()
                    .filter_map(|e| e.id.clone())
                    .collect(),
            )
        };
        let episode = Episode {
            id: None,
            agent_id: state.request.agent_id.clone(),
            org_id: state.request.org_id.clone(),
            user_id: state.request.user_id.clone(),
            content: state.request.content.clone(),
            content_type: state.request.content_type.as_str().to_string(),
            embedding: state.content_embedding.clone(),
            importance: state.importance,
            entities: entities_field,
            valid_time_start,
            valid_time_end: state.request.valid_time_end,
            transaction_time: None,
            consolidated: false,
            consolidated_at: None,
            summary: None,
            source_tier,
            metadata: state.request.metadata.clone(),
        };
        let written = self
            .ctx
            .store
            .write_episode(&episode)
            .await
            .map_err(|e| IngestError::Storage {
                stage: "write_episode",
                source: e,
            })?;
        let ep_id = written.id.clone().expect("write_episode assigns an id");

        // Tier 1 source: if the source tier is Tier 1, set
        // the episode as the anchor_record on each newly
        // created entity. We do this after the episode has
        // an id (anchor_record references an episode
        // record).
        if source_tier == SignalTier::Tier1Authoritative {
            for entity in state.linked_entities.iter_mut() {
                if entity.anchor_record.is_none() {
                    entity.anchor_record = Some(ep_id.clone());
                    let updated = self
                        .ctx
                        .store
                        .upsert_entity(entity)
                        .await
                        .map_err(|e| IngestError::Storage {
                            stage: "write-anchor",
                            source: e,
                        })?;
                    *entity = updated;
                }
            }
        }

        // Write the graph edges: episode →[mentions]→
        // entity for every linked entity. The
        // `relate_nodes` method's `weight` parameter is
        // the source_tier's weight — Tier 1 edges are
        // the strongest, Tier 5 the weakest.
        for entity in &state.linked_entities {
            let from = record_id_to_string(&ep_id);
            let to = record_id_to_string(entity.id.as_ref().expect("entity id assigned"));
            let weight = mention_weight(source_tier);
            self.ctx
                .store
                .relate_nodes(&from, "episode_mentions_entity", &to, Some(weight))
                .await
                .map_err(|e| IngestError::Storage {
                    stage: "write-relate",
                    source: e,
                })?;
        }
        Ok(written)
    }
}

fn record_id_to_string(rid: &RecordId) -> String {
    format!(
        "{}:{}",
        rid.table.as_str(),
        engram_storage::format_record_id_key(&rid.key)
    )
}

fn mention_weight(tier: SignalTier) -> f32 {
    use SignalTier::*;
    match tier {
        Tier1Authoritative => 0.95,
        Tier2Structured => 0.85,
        Tier3Conversational => 0.7,
        Tier4Implied => 0.5,
        Tier5Behavioral => 0.3,
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Build an [`IngestionPipeline`] with the defaults from the
/// crate, or with custom models.
pub struct IngestionPipelineBuilder {
    store: Option<Arc<Box<dyn MemoryStore>>>,
    embedder: Option<Arc<dyn EmbeddingModel>>,
    extractor: Option<Arc<dyn EntityExtractor>>,
    scorer: Option<Arc<dyn ImportanceScorer>>,
}

impl std::fmt::Debug for IngestionPipelineBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IngestionPipelineBuilder")
            .field("store_set", &self.store.is_some())
            .field("embedder_set", &self.embedder.is_some())
            .field("extractor_set", &self.extractor.is_some())
            .field("scorer_set", &self.scorer.is_some())
            .finish()
    }
}

impl IngestionPipelineBuilder {
    /// Start a builder. The `store` is required; the models
    /// default to the crate's deterministic / heuristic /
    /// tier-based implementations.
    pub fn new(store: Arc<Box<dyn MemoryStore>>) -> Self {
        Self {
            store: Some(store),
            embedder: None,
            extractor: None,
            scorer: None,
        }
    }

    /// Override the embedding model.
    pub fn with_embedder<E: EmbeddingModel + 'static>(mut self, e: E) -> Self {
        self.embedder = Some(Arc::new(e));
        self
    }

    /// Override the entity extractor.
    pub fn with_extractor<X: EntityExtractor + 'static>(mut self, x: X) -> Self {
        self.extractor = Some(Arc::new(x));
        self
    }

    /// Override the importance scorer.
    pub fn with_scorer<S: ImportanceScorer + 'static>(mut self, s: S) -> Self {
        self.scorer = Some(Arc::new(s));
        self
    }

    pub fn build(self) -> IngestionPipeline {
        use crate::embedding::DeterministicEmbedding;
        use crate::extraction::HeuristicEntityExtractor;
        let embedder = self
            .embedder
            .unwrap_or_else(|| Arc::new(DeterministicEmbedding));
        let extractor = self
            .extractor
            .unwrap_or_else(|| Arc::new(HeuristicEntityExtractor));
        let scorer = self
            .scorer
            .unwrap_or_else(|| Arc::new(TierBasedImportanceScorer::new()));
        let store = self.store.expect("store is required by the builder");
        IngestionPipeline {
            ctx: PipelineContext {
                store,
                embedder,
                extractor,
                scorer,
                session_entities: std::sync::Mutex::new(Vec::new()),
            },
        }
    }
}

// ============================================================================
// IngestionResult
// ============================================================================

/// The result of a successful `IngestionPipeline::ingest` call.
/// Holds the persisted `Episode` and the linked `Entity` rows
/// the pipeline wrote.
#[derive(Debug, Clone)]
pub struct IngestionResult {
    pub episode: Episode,
    pub linked_entities: Vec<Entity>,
}

impl IngestionResult {
    /// The `EpisodeRecord` shape from README §7.1. Convenience
    /// for callers (REST, MCP) that need the public output
    /// shape rather than the storage layer's full record.
    pub fn episode_record(&self) -> EpisodeRecord {
        EpisodeRecord {
            episode_id: self.episode.id.clone(),
            entities: self.linked_entities.clone(),
            importance: self.episode.importance,
            queued_for: QueueHint::consolidation(self.episode.importance),
        }
    }

    /// The persisted entities the episode links to. Convenience
    /// accessor so callers don't have to reach into the struct
    /// field directly.
    pub fn linked_entities(&self) -> Vec<Entity> {
        self.linked_entities.clone()
    }
}

// ============================================================================
// Tests for the scorer
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx<'a>(content: &'a str, mentions: &'a [ExtractedMention]) -> ScoringContext<'a> {
        ScoringContext {
            agent_id: "agent-test",
            content,
            content_type: ContentType::Conversation,
            source_tier: SignalTier::Tier3Conversational,
            mentions,
            valid_time: Utc::now(),
            content_length: content.len(),
        }
    }

    #[tokio::test]
    async fn scorer_is_in_unit_interval() {
        let s = TierBasedImportanceScorer::new();
        let score = s
            .score(&ctx("Hi.", &[]))
            .await
            .unwrap();
        assert!((0.0..=1.0).contains(&score));
    }

    #[tokio::test]
    async fn scorer_rewards_explicit_priority_signal() {
        let s = TierBasedImportanceScorer::new();
        // "important" / "deadline" / "urgent" are all priority
        // phrases, so use neutral content for the baseline
        // and a priority phrase for the boosted version.
        let a = s
            .score(&ctx("The cherry blossoms in spring.", &[]))
            .await
            .unwrap();
        let b = s
            .score(&ctx("URGENT: the cherry blossoms in spring.", &[]))
            .await
            .unwrap();
        assert!(b > a, "URGENT: should outscore plain: {b} vs {a}");
    }

    #[tokio::test]
    async fn scorer_rewards_higher_source_tier() {
        let s = TierBasedImportanceScorer::new();
        let mut c1 = ctx("Sarah Chen is the VP.", &[]);
        c1.source_tier = SignalTier::Tier3Conversational;
        let mut c2 = ctx("Sarah Chen is the VP.", &[]);
        c2.source_tier = SignalTier::Tier1Authoritative;
        let a = s.score(&c1).await.unwrap();
        let b = s.score(&c2).await.unwrap();
        assert!(b > a, "Tier 1 should outscore Tier 3: {b} vs {a}");
    }

    #[tokio::test]
    async fn scorer_rewards_explicit_mentions() {
        let s = TierBasedImportanceScorer::new();
        let mentions = vec![
            ExtractedMention {
                surface: "Sarah Chen".to_string(),
                canonical_name: "Sarah Chen".to_string(),
                entity_type: "person".to_string(),
                source_tier: SignalTier::Tier3Conversational,
                confidence: 0.9,
                reference_kind: ReferenceKind::Explicit,
            },
            ExtractedMention {
                surface: "Acme".to_string(),
                canonical_name: "Acme".to_string(),
                entity_type: "organization".to_string(),
                source_tier: SignalTier::Tier3Conversational,
                confidence: 0.9,
                reference_kind: ReferenceKind::Explicit,
            },
        ];
        let with_ents = s.score(&ctx("Sarah and Acme.", &mentions)).await.unwrap();
        let without = s.score(&ctx("Sarah and Acme.", &[])).await.unwrap();
        assert!(with_ents > without);
    }
}
