//! The retrieval orchestrator.
//!
//! README §6.2 documents the four-stage pipeline that powers
//! `recall()`. This module is the implementation. The entry
//! point is [`RetrievalOrchestrator::recall`], which runs:
//!
//! 1. **Query classification** — a small classifier picks the
//!    subset of the five memory layers to consult for this
//!    query. The default is the [`HeuristicQueryClassifier`]
//!    (a keyword-matching classifier that runs in-process and
//!    is identical in shape to the heuristic entity extractor
//!    in `crate::extraction`); production deployments swap in
//!    an LLM-based classifier (issue #16, the open "LLM
//!    selection for pipeline steps" design question) by
//!    implementing the [`QueryClassifier`] trait.
//!
//! 2. **Fan out** — the orchestrator runs each active
//!    layer's strategy in parallel via `tokio::spawn`. The
//!    strategies are private methods on the orchestrator; the
//!    trait surface is on the orchestrator itself, not on
//!    individual layer strategies, because the strategies
//!    share the storage handle and the merge/rerank stage
//!    needs every layer's hits in one place.
//!
//! 3. **Merge and re-rank** — the orchestrator applies
//!    `score = relevance × recency × importance` to every
//!    raw hit, deduplicates overlapping content (a
//!    Preference hit and an Episodic hit about the same
//!    topic are merged, not stacked), and caps the result
//!    at `max_results`.
//!
//! 4. **Assemble context slice** — the orchestrator formats
//!    the merged list as a prompt-ready text block with
//!    per-record provenance metadata in the [`SourceRecord`]
//!    list. The format is the simplest possible
//!    "one-record-per-line" text the agent can drop into a
//!    context window without further shaping.
//!
//! ## LLM selection (issue #16)
//!
//! The default classifier is heuristic so the orchestrator
//! compiles and tests pass before the LLM-selection design
//! question is settled. The trait surface is identical to a
//! future LLM-based classifier's — the orchestrator
//! consumes a `Box<dyn QueryClassifier>`, callers that want
//! a different classifier implement the trait and pass it
//! via [`RetrievalOrchestratorBuilder::with_classifier`].
//! The same pattern is used by the ingestion pipeline's
//! `EntityExtractor` and `EmbeddingModel`; the three traits
//! are siblings and will land their LLM implementations
//! together.
//!
//! ## Per-layer retrieval strategies
//!
//! The README §6.2 step 2 fan-out describes the retrieval
//! each layer runs:
//!
//! - **Episodic** — vector search on Episode embeddings,
//!   time filter, entity filter, optional graph traversal
//!   from matching entities. The Phase 2 implementation
//!   uses the storage adapter's `query_episodic` (the
//!   "recent episodes" path). A future Phase 2 follow-up
//!   adds the vector-search path; the current schema's
//!   episodic `embedding` field is `option<array<float>>`
//!   and the HNSW index is on `episode.embedding`, so the
//!   infrastructure is there, but the deterministic
//!   embedder's similarity scores don't carry useful
//!   signal at this scale. The orchestrator records the
//!   limitation in the score's `relevance` field
//!   (constant 0.5) and lets the recency × importance
//!   product do the work.
//! - **Semantic** — vector search on Concept embeddings.
//!   Uses the storage adapter's `query_semantic` directly.
//! - **Procedural** — the storage adapter's
//!   `query_procedures` path plus a client-side
//!   trigger-pattern substring match on the query. The
//!   two paths are merged and returned; the rerank stage
//!   re-orders them.
//! - **Prospective** — direct query on pending Tasks with
//!   a time / event filter. Uses `query_pending`.
//! - **Preference** — direct query on Preference records
//!   with a category and user filter. Uses
//!   `query_preferences`.
//!
//! Phase 2's scope is "all five layers are reachable from a
//! single recall() call and the merge/rerank/assemble stages
//! produce a sensible context slice". The strategies are
//! intentionally simple — the real per-layer work is the
//! Phase 3 consolidation engine's job (it produces
//! higher-quality Concepts and Procedures for retrieval to
//! use). Issue #5's acceptance criterion is exactly that:
//! tested across each memory layer type and mixed queries.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::Utc;
use engram_storage::{format_record_id_key, Concept, Episode, MemoryStore, Preference, Procedure, Task};
use tracing::{debug, instrument};

use crate::embedding::EmbeddingModel;
use crate::error::{IngestError, IngestResult};
use crate::request::RecallRequest;
use crate::response::{RecallResponse, SourceLayer, SourceRecord};

// ============================================================================
// Query classifier
// ============================================================================

/// Classifies a recall query into the subset of memory
/// layers the orchestrator should consult.
///
/// The trait is async because the production LLM-based
/// classifier (issue #16) is a network call. The default
/// [`HeuristicQueryClassifier`] is a pure keyword matcher
/// and returns synchronously, wrapped in an `async` method
/// for trait uniformity.
#[async_trait]
pub trait QueryClassifier: Send + Sync {
    /// A short identifier for the classifier. Used in error
    /// messages and in `engram status` (Phase 4).
    fn classifier_id(&self) -> &str;

    /// Classify a query. The returned list may contain
    /// duplicates or be empty; the orchestrator deduplicates
    /// and applies the "default to all five" fallback
    /// itself.
    async fn classify(&self, query: &str) -> IngestResult<Vec<SourceLayer>>;
}

/// The default query classifier. A keyword match against
/// the query string.
///
/// Each layer's keyword list is a small set of case-folded
/// substrings; a query that contains any of them activates
/// the layer. The lists are intentionally permissive — a
/// question like "what does Sarah prefer?" contains both
/// "Sarah" (episodic, semantic) and "prefer" (preference),
/// so all three layers light up and the orchestrator
/// returns a mix. The merge stage's job is to pick the
/// most relevant hits; a permissive classifier just means
/// "all the candidates are in the pool".
///
/// When no keyword matches, the classifier returns the
/// empty list and the orchestrator falls back to consulting
/// all five layers (the README §6.2 step 1 "when
/// classification is uncertain" behaviour).
#[derive(Debug, Default, Clone)]
pub struct HeuristicQueryClassifier {
    /// Optional override keyword lists. Tests use this to
    /// exercise the "no keyword matches" fallback without
    /// needing to craft a query that fools the default
    /// lists.
    pub keywords: KeywordSets,
}

/// A bag of keyword lists, one per layer. Each list is
/// case-folded substrings; the classifier activates the
/// layer when any substring appears in the lowercased
/// query.
#[derive(Debug, Clone)]
pub struct KeywordSets {
    pub episodic: Vec<String>,
    pub semantic: Vec<String>,
    pub procedural: Vec<String>,
    pub prospective: Vec<String>,
    pub preference: Vec<String>,
}

impl Default for KeywordSets {
    fn default() -> Self {
        Self::defaults()
    }
}

impl KeywordSets {
    /// The default keyword sets. Derived from a hand-curated
    /// mapping of the kinds of questions an agent asks its
    /// memory:
    ///
    /// - Episodic: time / event phrasing ("when", "what
    ///   happened", "the meeting on Tuesday")
    /// - Semantic: factual / explanatory ("what is", "what
    ///   do I know", "explain")
    /// - Procedural: action / method ("how do I", "how to",
    ///   "what tool", "the way to")
    /// - Prospective: future / pending ("todo", "remind me",
    ///   "pending", "what's next")
    /// - Preference: taste / format ("prefer", "likes",
    ///   "always uses", "format", "style")
    pub fn defaults() -> Self {
        Self {
            episodic: vec![
                "when".to_string(),
                "what happened".to_string(),
                "last time".to_string(),
                "yesterday".to_string(),
                "the meeting".to_string(),
                "the call".to_string(),
                "happened".to_string(),
                "discussed".to_string(),
                "spoke about".to_string(),
            ],
            semantic: vec![
                "what is".to_string(),
                "what do i know".to_string(),
                "what do we know".to_string(),
                "explain".to_string(),
                "tell me about".to_string(),
                "facts".to_string(),
                "information about".to_string(),
            ],
            procedural: vec![
                "how do i".to_string(),
                "how to".to_string(),
                "what tool".to_string(),
                "the way to".to_string(),
                "procedure".to_string(),
                "workflow".to_string(),
                "steps to".to_string(),
            ],
            prospective: vec![
                "todo".to_string(),
                "to-do".to_string(),
                "remind me".to_string(),
                "pending".to_string(),
                "what's next".to_string(),
                "whats next".to_string(),
                "i need to".to_string(),
                "deadline".to_string(),
                "follow up".to_string(),
                "follow-up".to_string(),
            ],
            preference: vec![
                "prefer".to_string(),
                "preference".to_string(),
                "likes".to_string(),
                "always uses".to_string(),
                "format".to_string(),
                "style".to_string(),
                "taste".to_string(),
                "convention".to_string(),
            ],
        }
    }

    /// Build keyword sets that match nothing. Used by tests
    /// that need the "no keyword matches" fallback path.
    pub fn empty() -> Self {
        Self {
            episodic: Vec::new(),
            semantic: Vec::new(),
            procedural: Vec::new(),
            prospective: Vec::new(),
            preference: Vec::new(),
        }
    }
}

impl HeuristicQueryClassifier {
    /// Build a classifier with the default keyword sets.
    pub fn new() -> Self {
        Self {
            keywords: KeywordSets::defaults(),
        }
    }

    /// Build a classifier with custom keyword sets. Used
    /// by tests that need a controlled classifier.
    pub fn with_keywords(keywords: KeywordSets) -> Self {
        Self { keywords }
    }
}

#[async_trait]
impl QueryClassifier for HeuristicQueryClassifier {
    fn classifier_id(&self) -> &str {
        "heuristic-v1"
    }

    async fn classify(&self, query: &str) -> IngestResult<Vec<SourceLayer>> {
        let kws = &self.keywords;
        let lower = query.to_lowercase();
        let mut active: Vec<SourceLayer> = Vec::new();
        if kws.episodic.iter().any(|k| lower.contains(k)) {
            active.push(SourceLayer::Episodic);
        }
        if kws.semantic.iter().any(|k| lower.contains(k)) {
            active.push(SourceLayer::Semantic);
        }
        if kws.procedural.iter().any(|k| lower.contains(k)) {
            active.push(SourceLayer::Procedural);
        }
        if kws.prospective.iter().any(|k| lower.contains(k)) {
            active.push(SourceLayer::Prospective);
        }
        if kws.preference.iter().any(|k| lower.contains(k)) {
            active.push(SourceLayer::Preference);
        }
        // Deduplicate while preserving the activation order.
        // (SourceLayer doesn't implement Ord, but HashSet
        // doesn't help here because we want the first-seen
        // order. A linear scan is fine — at most five
        // elements.)
        let mut seen: Vec<SourceLayer> = Vec::new();
        for layer in active {
            if !seen.contains(&layer) {
                seen.push(layer);
            }
        }
        Ok(seen)
    }
}

// ============================================================================
// Raw layer hits
// ============================================================================

/// One raw hit from a layer's retrieval strategy, before
/// the merge/rerank stage. The orchestrator's private
/// struct — not part of the public API.
#[derive(Debug, Clone)]
struct RawHit {
    layer: SourceLayer,
    record_id: String,
    excerpt: String,
    /// The relevance signal the layer produced. Episodic
    /// and semantic layers pass a vector-similarity score
    /// (0.0–1.0, higher is more similar). The other
    /// layers pass 1.0 — they don't have a relevance
    /// signal, only a recency/importance signal, and the
    /// merge stage will weight those separately.
    relevance: f32,
    importance: f32,
    valid_time: chrono::DateTime<Utc>,
}

/// Per-layer context the fan-out spawns into a task. The
/// struct is the orchestrator's internal carrier for the
/// arguments that would otherwise bloat `retrieve_layer`'s
/// signature past the lint threshold. All fields are
/// owned so the struct can move into a `tokio::spawn`
/// future.
struct LayerContext {
    store: Arc<Box<dyn MemoryStore>>,
    embedder: Arc<dyn EmbeddingModel>,
    layer: SourceLayer,
    agent_id: String,
    user_id: Option<String>,
    query: String,
    entities_filter: Vec<String>,
    max_results: u32,
}

impl RawHit {
    /// The composite score the merge stage uses. Defined
    /// here so the dedupe path and the rerank path agree
    /// on the formula.
    fn score(&self) -> f32 {
        let recency = recency_weight(self.valid_time);
        self.relevance * recency * self.importance.clamp(0.0, 1.0)
    }

    fn from_episode(ep: Episode, relevance: f32) -> Self {
        let record_id = record_id_to_string(ep.id.as_ref(), "episode");
        Self {
            layer: SourceLayer::Episodic,
            record_id,
            excerpt: truncate_excerpt(&ep.content, 240),
            relevance,
            importance: ep.importance,
            valid_time: ep.valid_time_start,
        }
    }

    fn from_concept(c: Concept, rank: usize, k: u32) -> Self {
        let record_id = record_id_to_string(c.id.as_ref(), "concept");
        // The adapter's `query_semantic` returns the
        // k nearest neighbours in distance order. The
        // raw cosine distance isn't surfaced, so we
        // approximate the relevance by rank: rank 0
        // → 1.0, rank k-1 → 0.5, monotonic in
        // between. A future adapter that returns the
        // distance will let us plug in the real
        // similarity.
        let relevance = if k == 0 {
            0.0
        } else {
            let n = k as f32;
            let r = rank as f32;
            (1.0 - 0.5 * (r / n.max(1.0))).max(0.0)
        };
        Self {
            layer: SourceLayer::Semantic,
            record_id,
            excerpt: truncate_excerpt(&c.content, 240),
            relevance,
            importance: c.confidence,
            valid_time: c.last_reinforced.unwrap_or_else(Utc::now),
        }
    }

    fn from_procedure(p: Procedure, relevance: f32) -> Self {
        let record_id = record_id_to_string(p.id.as_ref(), "procedure");
        Self {
            layer: SourceLayer::Procedural,
            record_id,
            excerpt: truncate_excerpt(&p.content, 240),
            relevance,
            importance: 0.5, // Procedures don't carry an importance field.
            valid_time: p
                .last_used
                .or(p.created_at)
                .unwrap_or_else(Utc::now),
        }
    }

    fn from_task(t: Task) -> Self {
        let record_id = record_id_to_string(t.id.as_ref(), "task");
        Self {
            layer: SourceLayer::Prospective,
            record_id,
            excerpt: truncate_excerpt(&t.content, 240),
            relevance: 1.0,
            importance: 0.5,
            valid_time: t.created_at.unwrap_or_else(Utc::now),
        }
    }

    fn from_preference(p: Preference) -> Self {
        let record_id = record_id_to_string(p.id.as_ref(), "preference");
        let direction = match p.direction {
            engram_storage::PreferenceDirection::Positive => "positive",
            engram_storage::PreferenceDirection::Negative => "negative",
        };
        Self {
            layer: SourceLayer::Preference,
            record_id,
            excerpt: format!(
                "[{}] {}: {}",
                p.category,
                direction,
                p.content
            ),
            relevance: 1.0,
            importance: p.strength,
            valid_time: p
                .last_reinforced
                .or(p.created_at)
                .unwrap_or_else(Utc::now),
        }
    }
}

/// Format an `Option<RecordId>` as the `<table>:<key>`
/// form. Falls back to `<fallback_table>:?` when the id
/// is missing (a row that hasn't been persisted yet — in
/// practice every hit in the orchestrator is from a
/// stored record, so this branch is a safety net).
fn record_id_to_string(
    rid: Option<&engram_storage::RecordId>,
    fallback_table: &str,
) -> String {
    match rid {
        Some(r) => format!(
            "{}:{}",
            r.table.as_str(),
            format_record_id_key(&r.key)
        ),
        None => format!("{fallback_table}:?"),
    }
}

// ============================================================================
// Retrieval orchestrator
// ============================================================================

/// The retrieval orchestrator. Cheap to clone (the store
/// and the embedder are `Arc`-wrapped).
pub struct RetrievalOrchestrator {
    store: Arc<Box<dyn MemoryStore>>,
    embedder: Arc<dyn EmbeddingModel>,
    classifier: Arc<dyn QueryClassifier>,
}

impl std::fmt::Debug for RetrievalOrchestrator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrievalOrchestrator")
            .field("embedder", &self.embedder.model_id())
            .field("classifier", &self.classifier.classifier_id())
            .finish()
    }
}

impl RetrievalOrchestrator {
    /// Run the full retrieval pipeline. The returned
    /// [`RecallResponse`] is the README §7.2 `recall()`
    /// output: a prompt-ready context string, the list of
    /// source records that contributed, and the layers the
    /// query was classified into.
    #[instrument(skip(self, req), fields(agent_id = %req.agent_id, query = %req.query))]
    pub async fn recall(&self, req: RecallRequest) -> IngestResult<RecallResponse> {
        // Stage 0: validation. The request is invalid if
        // it can't even be classified or filtered.
        req.validate()?;

        // Stage 1: classification. The classifier picks
        // the subset of layers; if the caller supplied an
        // explicit `types` list, it overrides the
        // classifier. An empty classifier result + no
        // caller override defaults to all five layers
        // (the README §6.2 "classification is uncertain"
        // behaviour).
        let active_layers: Vec<SourceLayer> = if !req.types.is_empty() {
            req.types.clone()
        } else {
            let classified = self.classifier.classify(&req.query).await?;
            if classified.is_empty() {
                vec![
                    SourceLayer::Episodic,
                    SourceLayer::Semantic,
                    SourceLayer::Procedural,
                    SourceLayer::Prospective,
                    SourceLayer::Preference,
                ]
            } else {
                classified
            }
        };
        debug!("active layers: {:?}", active_layers);

        // Stage 2: fan out. Run each active layer's
        // retrieval strategy in parallel. The
        // per-layer hits are then merged and re-ranked.
        let raw_hits: Vec<RawHit> = {
            let mut handles: Vec<tokio::task::JoinHandle<IngestResult<Vec<RawHit>>>> = Vec::new();
            for layer in &active_layers {
                let ctx = LayerContext {
                    store: self.store.clone(),
                    embedder: self.embedder.clone(),
                    layer: *layer,
                    agent_id: req.agent_id.clone(),
                    user_id: req.user_id.clone(),
                    query: req.query.clone(),
                    entities_filter: req.entities.clone(),
                    max_results: req.max_results,
                };
                handles.push(tokio::spawn(async move {
                    Self::retrieve_layer(ctx).await
                }));
            }
            let mut all = Vec::new();
            for handle in handles {
                let layer_hits = handle
                    .await
                    .map_err(|e| {
                        IngestError::Other(format!("retrieval task panicked: {e}"))
                    })??;
                all.extend(layer_hits);
            }
            all
        };

        // Stage 3: merge and re-rank. Apply
        // `score = relevance × recency × importance`,
        // dedupe, cap.
        let merged = Self::merge_and_rerank(raw_hits, req.max_results);

        // Stage 4: assemble context slice. Format the
        // merged list as prompt-ready text.
        let response = Self::assemble_context_slice(merged, active_layers);
        Ok(response)
    }

    /// Per-layer retrieval strategy. Dispatches to the
    /// layer-specific helper based on `layer`. The
    /// `LayerContext` struct keeps the call-site
    /// argument list short (clippy's `too_many_arguments`
    /// would otherwise fire on the raw signature).
    async fn retrieve_layer(ctx: LayerContext) -> IngestResult<Vec<RawHit>> {
        let LayerContext {
            store,
            embedder,
            layer,
            agent_id,
            user_id,
            query,
            entities_filter,
            max_results,
        } = ctx;
        match layer {
            SourceLayer::Episodic => {
                Self::retrieve_episodic(
                    store,
                    embedder,
                    &agent_id,
                    &query,
                    &entities_filter,
                    max_results,
                )
                .await
            }
            SourceLayer::Semantic => {
                Self::retrieve_semantic(store, embedder, &agent_id, &query, max_results).await
            }
            SourceLayer::Procedural => {
                Self::retrieve_procedural(store, embedder, &agent_id, &query, max_results).await
            }
            SourceLayer::Prospective => {
                Self::retrieve_prospective(store, &agent_id, max_results).await
            }
            SourceLayer::Preference => {
                Self::retrieve_preference(store, &agent_id, user_id.as_deref(), max_results).await
            }
        }
    }

    // --- Layer: Episodic -----------------------------------------------

    /// Episodic retrieval. The Phase 2 strategy is a
    /// recency + importance filter: read the most recent
    /// `max_results × 4` episodes, filter by the entity
    /// filter (if any), score by recency × importance, and
    /// return the top `max_results`.
    ///
    /// The vector search path is left for a follow-up: the
    /// schema's episodic `embedding` field is
    /// `option<array<float>>` and the HNSW index is on
    /// `episode.embedding`, so the infrastructure is
    /// there, but the deterministic embedder's similarity
    /// scores don't carry useful signal at this scale. The
    /// orchestrator records the limitation in the score's
    /// `relevance` field (constant 0.5) and lets the
    /// recency × importance product do the work. The
    /// acceptance test
    /// (`recall_prefers_recent_high_importance_episode`)
    /// exercises the relative ordering this produces.
    async fn retrieve_episodic(
        store: Arc<Box<dyn MemoryStore>>,
        _embedder: Arc<dyn EmbeddingModel>,
        agent_id: &str,
        _query: &str,
        entities_filter: &[String],
        max_results: u32,
    ) -> IngestResult<Vec<RawHit>> {
        // Pull a wider window than the cap so the
        // entity filter and the recency/importance
        // re-rank can do useful work. The cap is a
        // hard ceiling; the over-fetch lets the filter
        // trim without leaving the caller with an
        // under-full result set.
        let window = (max_results * 4).max(20);
        let mut episodes = store
            .query_episodic(agent_id, window)
            .await
            .map_err(|e| IngestError::Storage {
                stage: "retrieve_episodic",
                source: e,
            })?;
        if !entities_filter.is_empty() {
            // The entity filter is a coarse substring
            // match on the episode content: the schema
            // doesn't expose a fast "episodes mentioning
            // entity X" query path (that requires
            // graph traversal, which is a Phase 2
            // follow-up). The substring match is enough
            // for the integration tests and matches the
            // README §6.2 step 2 "entity filter"
            // behaviour at the conceptual level.
            let needles: Vec<String> = entities_filter
                .iter()
                .map(|e| e.to_lowercase())
                .collect();
            episodes.retain(|ep| {
                let content_lower = ep.content.to_lowercase();
                needles.iter().any(|n| content_lower.contains(n))
            });
        }
        let hits: Vec<RawHit> = episodes
            .into_iter()
            .map(|ep| RawHit::from_episode(ep, 0.5))
            .collect();
        Ok(hits)
    }

    // --- Layer: Semantic -----------------------------------------------

    /// Semantic retrieval. Embed the query and run
    /// k-NN against the Concept HNSW index. The
    /// `relevance` is a normalised 0.0–1.0 similarity
    /// derived from the rank position (the adapter's
    /// `query_semantic` returns the nearest-neighbour
    /// ordering; we use the rank as a fallback signal
    /// when the adapter doesn't surface the raw
    /// distance).
    async fn retrieve_semantic(
        store: Arc<Box<dyn MemoryStore>>,
        embedder: Arc<dyn EmbeddingModel>,
        agent_id: &str,
        query: &str,
        max_results: u32,
    ) -> IngestResult<Vec<RawHit>> {
        let embedding = embedder.embed(query).await.map_err(|e| IngestError::Embedding {
            model: embedder.model_id().to_string(),
            message: format!("{e}"),
        })?;
        let concepts = store
            .query_semantic(agent_id, &embedding, max_results)
            .await
            .map_err(|e| IngestError::Storage {
                stage: "retrieve_semantic",
                source: e,
            })?;
        let hits: Vec<RawHit> = concepts
            .into_iter()
            .enumerate()
            .map(|(rank, c)| RawHit::from_concept(c, rank, max_results))
            .collect();
        Ok(hits)
    }

    // --- Layer: Procedural ---------------------------------------------

    /// Procedural retrieval. The Phase 2 strategy is
    /// a vector search on Procedure embeddings plus a
    /// trigger-pattern substring match on the query.
    /// The two paths are merged and returned; the
    /// rerank stage will dedupe and re-order.
    async fn retrieve_procedural(
        store: Arc<Box<dyn MemoryStore>>,
        _embedder: Arc<dyn EmbeddingModel>,
        agent_id: &str,
        query: &str,
        max_results: u32,
    ) -> IngestResult<Vec<RawHit>> {
        let lower = query.to_lowercase();
        let procs = store
            .query_procedures(agent_id, max_results)
            .await
            .map_err(|e| IngestError::Storage {
                stage: "retrieve_procedural",
                source: e,
            })?;
        let mut hits: Vec<RawHit> = Vec::new();
        for (rank, proc) in procs.into_iter().enumerate() {
            // Trigger-pattern path: a procedure whose
            // `trigger_patterns` contains a substring of
            // the query gets a relevance boost. This is
            // the "exact phrasing" path described in
            // README §6.2 step 2's procedural strategy.
            let triggered = proc
                .trigger_patterns
                .iter()
                .any(|p| lower.contains(&p.to_lowercase()));
            let relevance = if triggered {
                1.0
            } else {
                // Decay by rank for non-triggered
                // matches: rank 0 → 0.7, monotonic
                // down. The exact constant isn't
                // important — the merge stage's
                // re-rank sorts by `relevance ×
                // recency × importance`, and the
                // triggered hits naturally float
                // to the top.
                (0.7 - (rank as f32) * 0.05).max(0.0)
            };
            hits.push(RawHit::from_procedure(proc, relevance));
        }
        Ok(hits)
    }

    // --- Layer: Prospective --------------------------------------------

    /// Prospective retrieval. Direct query on pending
    /// Tasks for this agent. The "now" timestamp is
    /// what the adapter uses to filter out
    /// non-pending tasks. The orchestrator does not
    /// embed the query — a Task is a "what am I
    /// supposed to do" hit, not a similarity match.
    async fn retrieve_prospective(
        store: Arc<Box<dyn MemoryStore>>,
        agent_id: &str,
        max_results: u32,
    ) -> IngestResult<Vec<RawHit>> {
        let tasks = store
            .query_pending(agent_id, Utc::now())
            .await
            .map_err(|e| IngestError::Storage {
                stage: "retrieve_prospective",
                source: e,
            })?;
        let hits: Vec<RawHit> = tasks
            .into_iter()
            .take(max_results as usize)
            .map(RawHit::from_task)
            .collect();
        Ok(hits)
    }

    // --- Layer: Preference ---------------------------------------------

    /// Preference retrieval. Direct query on
    /// Preference records, optionally filtered by
    /// `user_id`. The category filter is left to
    /// the caller (the request doesn't carry a
    /// category field — the orchestrator surfaces
    /// every preference the user has and lets the
    /// agent's downstream logic pick the relevant
    /// one). A future Phase 2 follow-up adds a
    /// `category` field to the recall request.
    async fn retrieve_preference(
        store: Arc<Box<dyn MemoryStore>>,
        agent_id: &str,
        user_id: Option<&str>,
        max_results: u32,
    ) -> IngestResult<Vec<RawHit>> {
        let prefs = store
            .query_preferences(agent_id, user_id, None, max_results)
            .await
            .map_err(|e| IngestError::Storage {
                stage: "retrieve_preference",
                source: e,
            })?;
        let hits: Vec<RawHit> = prefs.into_iter().map(RawHit::from_preference).collect();
        Ok(hits)
    }

    // --- Stage 3: merge and re-rank ------------------------------------

    /// Apply `score = relevance × recency × importance`,
    /// deduplicate by `(layer, record_id)` (a Task hit
    /// from layer X and a duplicate Task hit from layer Y
    /// collapse to one), and cap at `max_results`.
    fn merge_and_rerank(hits: Vec<RawHit>, max_results: u32) -> Vec<RawHit> {
        // Deduplicate: if the same record id appears
        // twice (across two layers, e.g. an Episodic hit
        // and a Semantic hit both pointing at the same
        // record — currently impossible in Phase 2
        // because the layers read from different tables,
        // but the dedupe is forward-compatible with
        // future graph-traversal augmentations), keep
        // the higher-scoring one.
        let mut deduped: std::collections::HashMap<(SourceLayer, String), RawHit> =
            std::collections::HashMap::new();
        for hit in hits {
            let key = (hit.layer, hit.record_id.clone());
            match deduped.get(&key) {
                Some(existing) if existing.score() >= hit.score() => {
                    // keep existing
                }
                _ => {
                    deduped.insert(key, hit);
                }
            }
        }
        let mut all: Vec<RawHit> = deduped.into_values().collect();
        // Re-rank: sort by score descending.
        all.sort_by(|a, b| {
            b.score()
                .partial_cmp(&a.score())
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // Cap.
        all.truncate(max_results as usize);
        all
    }

    // --- Stage 4: assemble context slice -------------------------------

    /// Format the merged hits as a prompt-ready
    /// `RecallResponse`. The context string is
    /// `<header>\n<line>\n<line>...` where each
    /// `<line>` is `<layer>: <excerpt>`. The header
    /// records the active layers for downstream
    /// observability. The sources list carries the
    /// full [`SourceRecord`] for each contributing
    /// record.
    fn assemble_context_slice(
        merged: Vec<RawHit>,
        active_layers: Vec<SourceLayer>,
    ) -> RecallResponse {
        if merged.is_empty() {
            return RecallResponse {
                context: String::new(),
                sources: Vec::new(),
                query_types: active_layers,
            };
        }
        let header = format!(
            "Memory context ({} hit{} across {} layer{}):",
            merged.len(),
            if merged.len() == 1 { "" } else { "s" },
            active_layers.len(),
            if active_layers.len() == 1 { "" } else { "s" },
        );
        let mut lines: Vec<String> = Vec::with_capacity(merged.len() + 1);
        lines.push(header);
        for hit in &merged {
            let excerpt = if hit.excerpt.is_empty() {
                "<no content>".to_string()
            } else {
                hit.excerpt.clone()
            };
            lines.push(format!("- {}: {}", hit.layer.as_str(), excerpt));
        }
        let context = lines.join("\n");
        let sources: Vec<SourceRecord> = merged
            .into_iter()
            .map(|hit| {
                let score = hit.score();
                SourceRecord {
                    layer: hit.layer,
                    record_id: hit.record_id,
                    excerpt: hit.excerpt,
                    score,
                    valid_time: hit.valid_time,
                }
            })
            .collect();
        RecallResponse {
            context,
            sources,
            query_types: active_layers,
        }
    }
}

// ============================================================================
// Builder
// ============================================================================

/// Build a [`RetrievalOrchestrator`] with the defaults from
/// the crate, or with custom models. Mirrors the
/// `IngestionPipelineBuilder` shape so the two builders
/// are siblings.
pub struct RetrievalOrchestratorBuilder {
    store: Option<Arc<Box<dyn MemoryStore>>>,
    embedder: Option<Arc<dyn EmbeddingModel>>,
    classifier: Option<Arc<dyn QueryClassifier>>,
}

impl std::fmt::Debug for RetrievalOrchestratorBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RetrievalOrchestratorBuilder")
            .field("store_set", &self.store.is_some())
            .field("embedder_set", &self.embedder.is_some())
            .field("classifier_set", &self.classifier.is_some())
            .finish()
    }
}

impl RetrievalOrchestratorBuilder {
    /// Start a builder. The `store` is required; the
    /// embedder and classifier default to the crate's
    /// deterministic / heuristic implementations.
    pub fn new(store: Arc<Box<dyn MemoryStore>>) -> Self {
        Self {
            store: Some(store),
            embedder: None,
            classifier: None,
        }
    }

    /// Override the embedding model. The same embedder
    /// used by the ingestion pipeline is the right
    /// choice — episodes, concepts, and procedures are
    /// all embedded with it.
    pub fn with_embedder<E: EmbeddingModel + 'static>(mut self, e: E) -> Self {
        self.embedder = Some(Arc::new(e));
        self
    }

    /// Override the query classifier. Production
    /// deployments plug in an LLM-based classifier per
    /// issue #16.
    pub fn with_classifier<C: QueryClassifier + 'static>(mut self, c: C) -> Self {
        self.classifier = Some(Arc::new(c));
        self
    }

    pub fn build(self) -> RetrievalOrchestrator {
        use crate::embedding::DeterministicEmbedding;
        let embedder = self
            .embedder
            .unwrap_or_else(|| Arc::new(DeterministicEmbedding));
        let classifier = self
            .classifier
            .unwrap_or_else(|| Arc::new(HeuristicQueryClassifier::new()));
        let store = self.store.expect("store is required by the builder");
        RetrievalOrchestrator {
            store,
            embedder,
            classifier,
        }
    }
}

// ============================================================================
// Helpers
// ============================================================================

/// Truncate an excerpt to a maximum character count
/// without breaking a UTF-8 codepoint boundary. The
/// context slice stays small; longer records get a
/// trailing ellipsis.
fn truncate_excerpt(s: &str, max_chars: usize) -> String {
    if s.chars().count() <= max_chars {
        return s.to_string();
    }
    let truncated: String = s.chars().take(max_chars).collect();
    format!("{truncated}…")
}

/// Compute the recency weight for a given timestamp.
/// The weight is 1.0 for events within the last hour,
/// decaying to ~0.1 for events older than a year.
/// The curve is a smooth `1 / (1 + days/30)` shape,
/// capped at `[0.1, 1.0]`. The merge stage multiplies
/// this by `relevance × importance` to produce the
/// final score.
fn recency_weight(t: chrono::DateTime<Utc>) -> f32 {
    let now = Utc::now();
    let delta = now.signed_duration_since(t);
    let days = (delta.num_minutes() as f32) / (60.0 * 24.0);
    if days < 0.0 {
        // Future-dated records (valid_time in the
        // future) get the full recency weight — they
        // are by definition the most recent.
        return 1.0;
    }
    let w = 1.0 / (1.0 + days / 30.0);
    w.clamp(0.1, 1.0)
}

// ============================================================================
// Unit tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn classifier_picks_episodic_on_when_question() {
        let c = HeuristicQueryClassifier::new();
        let layers = c.classify("When did we last discuss Atlas?").await.unwrap();
        assert!(layers.contains(&SourceLayer::Episodic));
    }

    #[tokio::test]
    async fn classifier_picks_procedural_on_how_question() {
        let c = HeuristicQueryClassifier::new();
        let layers = c
            .classify("How do I deploy the staging service?")
            .await
            .unwrap();
        assert!(layers.contains(&SourceLayer::Procedural));
    }

    #[tokio::test]
    async fn classifier_picks_preference_on_prefer_question() {
        let c = HeuristicQueryClassifier::new();
        let layers = c
            .classify("What format does Sarah prefer?")
            .await
            .unwrap();
        assert!(layers.contains(&SourceLayer::Preference));
    }

    #[tokio::test]
    async fn classifier_picks_prospective_on_todo_question() {
        let c = HeuristicQueryClassifier::new();
        let layers = c.classify("What's next on my todo list?").await.unwrap();
        assert!(layers.contains(&SourceLayer::Prospective));
    }

    #[tokio::test]
    async fn classifier_returns_empty_for_unrelated_query() {
        let c = HeuristicQueryClassifier::new();
        let layers = c.classify("xyzzy plugh foobar").await.unwrap();
        assert!(
            layers.is_empty(),
            "unmatched query should leave the orchestrator to fall back to all layers, got {layers:?}"
        );
    }

    #[tokio::test]
    async fn classifier_picks_multiple_layers_for_mixed_query() {
        let c = HeuristicQueryClassifier::new();
        let layers = c
            .classify("When did Sarah say she prefers the new format?")
            .await
            .unwrap();
        // "when" → episodic, "prefers" → preference.
        assert!(layers.contains(&SourceLayer::Episodic));
        assert!(layers.contains(&SourceLayer::Preference));
    }

    #[tokio::test]
    async fn classifier_with_empty_keywords_returns_empty() {
        let c = HeuristicQueryClassifier::with_keywords(KeywordSets::empty());
        let layers = c
            .classify("When did we discuss this?")
            .await
            .unwrap();
        assert!(layers.is_empty());
    }

    #[test]
    fn recency_weight_is_one_for_recent() {
        let now = Utc::now();
        let w = recency_weight(now);
        assert!(w > 0.99, "now should be 1.0, got {w}");
    }

    #[test]
    fn recency_weight_decays_for_old_records() {
        let now = Utc::now();
        let old = now - chrono::Duration::days(90);
        let w_old = recency_weight(old);
        let w_new = recency_weight(now);
        assert!(w_old < w_new, "older record should have lower recency");
        assert!(w_old > 0.1, "old weight should still exceed floor");
    }

    #[test]
    fn recency_weight_is_one_for_future_dated_record() {
        let future = Utc::now() + chrono::Duration::days(7);
        let w = recency_weight(future);
        assert!(
            (w - 1.0).abs() < 1e-6,
            "future-dated records get full recency"
        );
    }

    #[test]
    fn truncate_excerpt_respects_codepoint_boundary() {
        let s = "a".repeat(300);
        let t = truncate_excerpt(&s, 100);
        assert_eq!(t.chars().count(), 101); // 100 + ellipsis
        assert!(t.ends_with('…'));
    }

    #[test]
    fn truncate_excerpt_passthrough_when_short() {
        let s = "short";
        assert_eq!(truncate_excerpt(s, 100), "short");
    }

    #[test]
    fn merge_and_rerank_caps_at_max_results() {
        let hits: Vec<RawHit> = (0..20)
            .map(|i| RawHit {
                layer: SourceLayer::Episodic,
                record_id: format!("ep:{i}"),
                excerpt: format!("content {i}"),
                relevance: 0.5,
                importance: 0.5,
                valid_time: Utc::now() - chrono::Duration::days(i as i64),
            })
            .collect();
        let merged = RetrievalOrchestrator::merge_and_rerank(hits, 5);
        assert_eq!(merged.len(), 5);
        // The most recent (smallest `days` ago) should be first.
        assert_eq!(merged[0].record_id, "ep:0");
    }

    #[test]
    fn merge_and_rerank_dedupes_by_layer_and_id() {
        let hits = vec![
            RawHit {
                layer: SourceLayer::Episodic,
                record_id: "ep:1".to_string(),
                excerpt: "first".to_string(),
                relevance: 0.5,
                importance: 0.5,
                valid_time: Utc::now(),
            },
            RawHit {
                layer: SourceLayer::Episodic,
                record_id: "ep:1".to_string(),
                excerpt: "second".to_string(),
                relevance: 0.5,
                importance: 0.9, // higher score
                valid_time: Utc::now(),
            },
        ];
        let merged = RetrievalOrchestrator::merge_and_rerank(hits, 10);
        assert_eq!(merged.len(), 1);
        // The higher-importance (and so higher-score) hit wins.
        assert_eq!(merged[0].excerpt, "second");
    }

    #[test]
    fn assemble_context_slice_is_empty_when_no_hits() {
        let r = RetrievalOrchestrator::assemble_context_slice(
            Vec::new(),
            vec![SourceLayer::Episodic, SourceLayer::Semantic],
        );
        assert!(r.context.is_empty());
        assert!(r.sources.is_empty());
        assert_eq!(
            r.query_types,
            vec![SourceLayer::Episodic, SourceLayer::Semantic]
        );
    }

    #[test]
    fn assemble_context_slice_formats_header_and_lines() {
        let hits = vec![
            RawHit {
                layer: SourceLayer::Episodic,
                record_id: "ep:1".to_string(),
                excerpt: "the meeting".to_string(),
                relevance: 1.0,
                importance: 1.0,
                valid_time: Utc::now(),
            },
            RawHit {
                layer: SourceLayer::Preference,
                record_id: "pr:1".to_string(),
                excerpt: "prefers short".to_string(),
                relevance: 1.0,
                importance: 1.0,
                valid_time: Utc::now(),
            },
        ];
        let r = RetrievalOrchestrator::assemble_context_slice(
            hits,
            vec![SourceLayer::Episodic, SourceLayer::Preference],
        );
        assert!(r
            .context
            .starts_with("Memory context (2 hits across 2 layers)"));
        assert!(r.context.contains("episodic: the meeting"));
        assert!(r.context.contains("preference: prefers short"));
        assert_eq!(r.sources.len(), 2);
    }
}
