//! End-to-end integration tests for the retrieval orchestrator
//! (Phase 2, issue #5).
//!
//! These tests cover the four stages from README §6.2 against a
//! real SurrealDB-backed `MemoryStore`:
//!
//! 1. Query classification — small classifier picks the active
//!    layer subset
//! 2. Fan out — parallel execution across the active layers
//! 3. Merge and re-rank — `score = relevance × recency ×
//!    importance`, dedupe, cap
//! 4. Assemble context slice — prompt-ready text + provenance
//!
//! The acceptance criterion from issue #5 is "`recall()`
//! returns assembled context slice + raw records with sources.
//! Tested across each memory layer type and mixed queries."
//! Each of the five memory layers gets a dedicated test that
//! writes a known fixture and asserts the orchestrator
//! surfaces it; a mixed-query test exercises the
//! "one query → multiple layers" path the README §6.2 step 1
//! implies.
//!
//! The orchestrator is model-agnostic: tests use the
//! deterministic default embedder and heuristic default
//! classifier, so they run in CI without external API keys.
//! Production deployments swap in real models per design Q1
//! (issue #15) and Q2 (issue #16); the trait surface is the
//! same.

use std::path::PathBuf;

use chrono::Utc;
use engram_core::embedding::DeterministicEmbedding;
use engram_core::pipeline::{IngestionPipeline, IngestionPipelineBuilder, TierBasedImportanceScorer};
use engram_core::extraction::HeuristicEntityExtractor;
use engram_core::retrieval::RetrievalOrchestratorBuilder;
use engram_core::{
    ContentType, RecallRequest, SourceLayer, StoreRequest,
};
use engram_storage::open;
use engram_storage::MemoryStoreConfig;
use engram_storage::StoreKind;
use engram_storage::{
    Concept, Preference, PreferenceDirection, Procedure, SignalTier, Task, TaskStatus,
};

fn manifest_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // repo root
    p.push("schema");
    p.push("manifest.toml");
    p
}

async fn fresh_store() -> Box<dyn engram_storage::MemoryStore> {
    let unique = format!("engram_recall_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );
    open(&config).await.expect("opening embedded store")
}

fn default_orchestrator(
    store: std::sync::Arc<Box<dyn engram_storage::MemoryStore>>,
) -> engram_core::RetrievalOrchestrator {
    RetrievalOrchestratorBuilder::new(store)
        .with_embedder(DeterministicEmbedding::default())
        .build()
}

fn default_pipeline(
    store: std::sync::Arc<Box<dyn engram_storage::MemoryStore>>,
) -> IngestionPipeline {
    IngestionPipelineBuilder::new(store)
        .with_embedder(DeterministicEmbedding::default())
        .with_extractor(HeuristicEntityExtractor::default())
        .with_scorer(TierBasedImportanceScorer::default())
        .build()
}

// --- 0. Validation ---------------------------------------------------------

#[tokio::test]
async fn recall_rejects_empty_query() {
    let store = std::sync::Arc::new(fresh_store().await);
    let orch = default_orchestrator(store.clone());
    let r = orch.recall(RecallRequest::new("agent-x", "  ")).await;
    assert!(r.is_err(), "empty query must be rejected");
}

#[tokio::test]
async fn recall_rejects_empty_agent() {
    let store = std::sync::Arc::new(fresh_store().await);
    let orch = default_orchestrator(store.clone());
    let r = orch.recall(RecallRequest::new("", "hi")).await;
    assert!(r.is_err(), "empty agent_id must be rejected");
}

// --- 1. Episodic layer ----------------------------------------------------

#[tokio::test]
async fn recall_returns_episodic_records() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-ep-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    // Seed: a Tier 1 assertion ("important" triggers the
    // importance bump on the heuristic scorer) and a Tier
    // 3 conversation.
    pipeline
        .ingest(
            StoreRequest::new(&agent, "Important: Atlas ships Friday.")
                .with_content_type(ContentType::Assertion)
                .with_importance(0.9),
        )
        .await
        .expect("ingest assertion");
    pipeline
        .ingest(
            StoreRequest::new(&agent, "We had coffee Tuesday.")
                .with_content_type(ContentType::Conversation)
                .with_importance(0.5),
        )
        .await
        .expect("ingest conversation");

    // A "when"-shaped query classifies to Episodic, so the
    // orchestrator's episodic fan-out runs and surfaces
    // both episodes.
    let resp = orch
        .recall(RecallRequest::new(&agent, "When did Atlas ship?"))
        .await
        .expect("recall");
    assert!(
        resp.sources
            .iter()
            .any(|s| s.layer == SourceLayer::Episodic),
        "episodic layer should contribute a source, got {:?}",
        resp.sources
    );
    assert!(
        resp.query_types.contains(&SourceLayer::Episodic),
        "episodic should be in query_types, got {:?}",
        resp.query_types
    );
    // The context slice is non-empty (we have hits).
    assert!(!resp.context.is_empty());
    assert!(resp.context.contains("episodic:"));
}

#[tokio::test]
async fn recall_prefers_recent_high_importance_episode() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-rank-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    // Two episodes: the first has higher importance but
    // the second is more recent. The recency × importance
    // product should put the recent one ahead.
    pipeline
        .ingest(
            StoreRequest::new(&agent, "old but important: the moon is made of cheese")
                .with_content_type(ContentType::Assertion)
                .with_importance(0.95)
                .with_valid_time(Utc::now() - chrono::Duration::days(30)),
        )
        .await
        .expect("ingest old");
    pipeline
        .ingest(
            StoreRequest::new(&agent, "recent and notable: the moon is made of rock")
                .with_content_type(ContentType::Assertion)
                .with_importance(0.7)
                .with_valid_time(Utc::now()),
        )
        .await
        .expect("ingest recent");

    let resp = orch
        .recall(RecallRequest::new(&agent, "When did we discuss the moon?"))
        .await
        .expect("recall");
    let sources = &resp.sources;
    assert!(sources.len() >= 2, "should have both episodes, got {sources:?}");
    // The top hit is the more recent one (recency weight
    // outweighs the 0.25 importance gap at 30 days).
    assert!(
        sources[0].excerpt.contains("rock"),
        "the more recent episode should rank first, got: {:#?}",
        sources
    );
    assert!(
        sources[0].score >= sources[1].score,
        "scores should be monotonically non-increasing: {:#?}",
        sources.iter().map(|s| s.score).collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn recall_filters_episodic_by_entity_name() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-ent-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    pipeline
        .ingest(
            StoreRequest::new(&agent, "Sarah Chen reviewed the Atlas spec.")
                .with_content_type(ContentType::Conversation),
        )
        .await
        .expect("ingest 1");
    pipeline
        .ingest(
            StoreRequest::new(&agent, "We discussed the budget on Tuesday.")
                .with_content_type(ContentType::Conversation),
        )
        .await
        .expect("ingest 2");

    let resp = orch
        .recall(
            RecallRequest::new(&agent, "When did we discuss the budget?")
                .with_entities(vec!["Sarah".to_string()]),
        )
        .await
        .expect("recall");
    // The entity filter is a substring match on content;
    // only the Sarah-episode mentions "Sarah".
    let episodes_returned: Vec<&str> = resp
        .sources
        .iter()
        .filter(|s| s.layer == SourceLayer::Episodic)
        .map(|s| s.excerpt.as_str())
        .collect();
    assert!(
        episodes_returned.iter().all(|e| e.contains("Sarah")),
        "entity filter should keep only Sarah-mentioning episodes, got {episodes_returned:?}"
    );
    assert!(
        !episodes_returned.iter().any(|e| e.contains("budget")),
        "the budget episode should be filtered out"
    );
}

// --- 2. Semantic layer ---------------------------------------------------

#[tokio::test]
async fn recall_returns_semantic_records() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-sem-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    // Seed a concept directly through the store (the
    // consolidation engine that creates them in production
    // is Phase 3; the orchestrator can read them today).
    let c = Concept {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        content: "Atlas ships on Friday".to_string(),
        embedding: Some(vec![0.0; 768]),
        confidence: 0.8,
        source_tier: SignalTier::Tier2Structured,
        reinforcement_count: 1,
        last_reinforced: Some(Utc::now()),
        decay_rate: 0.01,
        inferred: false,
        inference_chain: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    store.upsert_concept(&c).await.expect("upsert concept");

    let resp = orch
        .recall(RecallRequest::new(&agent, "What do I know about Atlas?"))
        .await
        .expect("recall");
    assert!(
        resp.sources
            .iter()
            .any(|s| s.layer == SourceLayer::Semantic),
        "semantic layer should contribute, got {:?}",
        resp.sources
    );
    assert!(
        resp.query_types.contains(&SourceLayer::Semantic),
        "semantic should be in query_types"
    );
}

// --- 3. Procedural layer -------------------------------------------------

#[tokio::test]
async fn recall_returns_procedural_records() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-proc-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    let p = Procedure {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        name: "deploy-staging".to_string(),
        procedure_type: "tool_definition".to_string(),
        content: "Run `deploy.sh --env=staging` to deploy the staging service."
            .to_string(),
        embedding: Some(vec![0.0; 768]),
        trigger_patterns: vec!["deploy".to_string(), "staging".to_string()],
        usage_count: 1,
        last_used: Some(Utc::now()),
        created_at: Some(Utc::now()),
    };
    store.write_procedure(&p).await.expect("write procedure");

    let resp = orch
        .recall(RecallRequest::new(
            &agent,
            "How do I deploy the staging service?",
        ))
        .await
        .expect("recall");
    assert!(
        resp.sources
            .iter()
            .any(|s| s.layer == SourceLayer::Procedural),
        "procedural layer should contribute, got {:?}",
        resp.sources
    );
    assert!(resp.query_types.contains(&SourceLayer::Procedural));
    // The trigger-pattern path should have surfaced the
    // procedure (its content is in the excerpt).
    let proc_excerpts: Vec<&str> = resp
        .sources
        .iter()
        .filter(|s| s.layer == SourceLayer::Procedural)
        .map(|s| s.excerpt.as_str())
        .collect();
    assert!(
        proc_excerpts
            .iter()
            .any(|e| e.contains("deploy.sh")),
        "procedure excerpt should be in the context, got {proc_excerpts:?}"
    );
}

// --- 4. Prospective layer ------------------------------------------------

#[tokio::test]
async fn recall_returns_prospective_records() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-task-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    let t = Task {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        user_id: None,
        content: "Email the Q3 report to the leadership team".to_string(),
        trigger_type: "time".to_string(),
        trigger_value: "2026-06-05T00:00:00Z".to_string(),
        status: TaskStatus::Pending,
        created_at: Some(Utc::now()),
        triggered_at: None,
    };
    store.write_task(&t).await.expect("write task");

    let resp = orch
        .recall(RecallRequest::new(&agent, "What's next on my todo list?"))
        .await
        .expect("recall");
    assert!(
        resp.sources
            .iter()
            .any(|s| s.layer == SourceLayer::Prospective),
        "prospective layer should contribute, got {:?}",
        resp.sources
    );
    let task_excerpts: Vec<&str> = resp
        .sources
        .iter()
        .filter(|s| s.layer == SourceLayer::Prospective)
        .map(|s| s.excerpt.as_str())
        .collect();
    assert!(
        task_excerpts.iter().any(|e| e.contains("Q3 report")),
        "task content should be in context, got {task_excerpts:?}"
    );
}

// --- 5. Preference layer -------------------------------------------------

#[tokio::test]
async fn recall_returns_preference_records() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-pref-{}", uuid::Uuid::new_v4().simple());
    let user = format!("user-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    let p = Preference {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        user_id: Some(user.clone()),
        category: "format".to_string(),
        content: "Prefers bullet points over prose.".to_string(),
        direction: PreferenceDirection::Positive,
        strength: 0.8,
        source_tier: SignalTier::Tier3Conversational,
        evidence_count: 1,
        last_reinforced: Some(Utc::now()),
        created_at: Some(Utc::now()),
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    store.write_preference(&p).await.expect("write preference");

    let resp = orch
        .recall(
            RecallRequest::new(&agent, "What format does the user prefer?")
                .with_user_id(&user),
        )
        .await
        .expect("recall");
    assert!(
        resp.sources
            .iter()
            .any(|s| s.layer == SourceLayer::Preference),
        "preference layer should contribute, got {:?}",
        resp.sources
    );
    let pref_excerpts: Vec<&str> = resp
        .sources
        .iter()
        .filter(|s| s.layer == SourceLayer::Preference)
        .map(|s| s.excerpt.as_str())
        .collect();
    assert!(
        pref_excerpts.iter().any(|e| e.contains("bullet points")),
        "preference content should be in context, got {pref_excerpts:?}"
    );
}

// --- 6. Mixed query: multiple layers in one recall ----------------------

#[tokio::test]
async fn recall_mixed_query_surfaces_multiple_layers() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-mix-{}", uuid::Uuid::new_v4().simple());
    let user = format!("user-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    // Seed across three layers: episodic (via pipeline),
    // procedural (via store), preference (via store).
    pipeline
        .ingest(
            StoreRequest::new(&agent, "When we discussed Atlas, Sarah said she prefers bullet points.")
                .with_content_type(ContentType::Conversation)
                .with_user_id(&user),
        )
        .await
        .expect("ingest episodic");

    let p = Procedure {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        name: "atlas-deploy".to_string(),
        procedure_type: "tool_definition".to_string(),
        content: "Use `make atlas-deploy` to ship the Atlas release.".to_string(),
        embedding: Some(vec![0.0; 768]),
        trigger_patterns: vec!["atlas".to_string(), "deploy".to_string()],
        usage_count: 1,
        last_used: Some(Utc::now()),
        created_at: Some(Utc::now()),
    };
    store.write_procedure(&p).await.expect("write procedure");

    let pref = Preference {
        id: None,
        agent_id: agent.clone(),
        org_id: None,
        user_id: Some(user.clone()),
        category: "format".to_string(),
        content: "Prefers bullet points over prose for Atlas specs.".to_string(),
        direction: PreferenceDirection::Positive,
        strength: 0.9,
        source_tier: SignalTier::Tier3Conversational,
        evidence_count: 1,
        last_reinforced: Some(Utc::now()),
        created_at: Some(Utc::now()),
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    store.write_preference(&pref).await.expect("write pref");

    // The query "When did Sarah say she prefers the Atlas
    // format?" lights up episodic ("when"), procedural
    // ("atlas" is a trigger pattern, though only loosely),
    // and preference ("prefers").
    let resp = orch
        .recall(RecallRequest::new(&agent, "When did Sarah say she prefers the Atlas format?")
            .with_user_id(&user))
        .await
        .expect("recall");

    let layers_seen: std::collections::HashSet<SourceLayer> =
        resp.sources.iter().map(|s| s.layer).collect();
    assert!(
        layers_seen.contains(&SourceLayer::Episodic),
        "episodic missing from {layers_seen:?}"
    );
    assert!(
        layers_seen.contains(&SourceLayer::Preference),
        "preference missing from {layers_seen:?}"
    );
    // Procedural may or may not be in the result set
    // depending on the trigger pattern match; the test
    // doesn't require it, but logs the set for the
    // reader.
    eprintln!("mixed-query layers seen: {layers_seen:?}");
}

// --- 7. Empty store: orchestrator returns an empty context slice --------

#[tokio::test]
async fn recall_empty_store_returns_empty_context() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-empty-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    let resp = orch
        .recall(RecallRequest::new(&agent, "When did anything happen?"))
        .await
        .expect("recall");
    assert!(resp.context.is_empty(), "no records → empty context");
    assert!(resp.sources.is_empty(), "no records → no sources");
    // query_types is non-empty: the classifier activated
    // the episodic layer (the "when" keyword matched), so
    // the orchestrator knows which layers were queried.
    assert!(!resp.query_types.is_empty());
}

// --- 8. Classifier fallback: empty keywords → all layers ---------------

#[tokio::test]
async fn recall_with_empty_classifier_falls_back_to_all_layers() {
    use engram_core::retrieval::{HeuristicQueryClassifier, KeywordSets};
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-fallback-{}", uuid::Uuid::new_v4().simple());
    let orch = RetrievalOrchestratorBuilder::new(store.clone())
        .with_embedder(DeterministicEmbedding::default())
        .with_classifier(HeuristicQueryClassifier::with_keywords(
            KeywordSets::empty(),
        ))
        .build();

    let resp = orch
        .recall(RecallRequest::new(&agent, "anything goes"))
        .await
        .expect("recall");
    // The empty-keyword classifier returns no layers, so
    // the orchestrator falls back to all five.
    assert_eq!(
        resp.query_types.len(),
        5,
        "empty classifier should default to all five layers, got {:?}",
        resp.query_types
    );
}

// --- 9. Caller-supplied types override the classifier -------------------

#[tokio::test]
async fn recall_with_caller_types_overrides_classifier() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-override-{}", uuid::Uuid::new_v4().simple());
    let orch = default_orchestrator(store.clone());

    // The query would normally light up episodic, but the
    // caller says "only preference". The orchestrator
    // should honour that and skip the episodic fan-out.
    let resp = orch
        .recall(
            RecallRequest::new(&agent, "When did anything happen?")
                .with_types(vec![SourceLayer::Preference]),
        )
        .await
        .expect("recall");
    assert_eq!(
        resp.query_types,
        vec![SourceLayer::Preference],
        "caller's types override the classifier, got {:?}",
        resp.query_types
    );
}

// --- 10. Cap honours max_results ----------------------------------------

#[tokio::test]
async fn recall_caps_results_at_max_results() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-cap-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    for i in 0..20 {
        pipeline
            .ingest(
                StoreRequest::new(&agent, format!("When we discussed item {i}."))
                    .with_content_type(ContentType::Conversation),
            )
            .await
            .expect("ingest");
    }
    let resp = orch
        .recall(RecallRequest::new(&agent, "When did we discuss anything?")
            .with_max_results(3))
        .await
        .expect("recall");
    assert!(
        resp.sources.len() <= 3,
        "max_results should be honoured, got {}",
        resp.sources.len()
    );
}

// --- 11. Context slice is the prompt-ready text -------------------------

#[tokio::test]
async fn recall_context_slice_includes_layer_label() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-ctx-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    pipeline
        .ingest(
            StoreRequest::new(&agent, "Important: the Atlas deadline is Friday.")
                .with_content_type(ContentType::Assertion)
                .with_importance(0.9),
        )
        .await
        .expect("ingest");
    let resp = orch
        .recall(RecallRequest::new(&agent, "When is the Atlas deadline?"))
        .await
        .expect("recall");
    // Each contributing record is rendered as
    // "<layer>: <excerpt>".
    assert!(resp.context.contains("episodic:"), "context: {resp:?}");
    assert!(
        resp.context.contains("Friday"),
        "episode excerpt should appear in the context, got: {resp:?}"
    );
    // The header records the hit count and layer count
    // for downstream observability.
    assert!(resp.context.contains("Memory context"));
}

// --- 12. Sources carry provenance metadata -----------------------------

#[tokio::test]
async fn recall_sources_carry_provenance() {
    let store = std::sync::Arc::new(fresh_store().await);
    let agent = format!("agent-prov-{}", uuid::Uuid::new_v4().simple());
    let pipeline = default_pipeline(store.clone());
    let orch = default_orchestrator(store.clone());

    pipeline
        .ingest(
            StoreRequest::new(&agent, "Important: the Atlas deadline is Friday.")
                .with_content_type(ContentType::Assertion)
                .with_importance(0.9),
        )
        .await
        .expect("ingest");
    let resp = orch
        .recall(RecallRequest::new(&agent, "When is the Atlas deadline?"))
        .await
        .expect("recall");
    for src in &resp.sources {
        assert!(!src.record_id.is_empty(), "record_id should be set");
        assert!(
            src.record_id.starts_with("episode:") || src.record_id.starts_with("concept:") || src.record_id.starts_with("procedure:") || src.record_id.starts_with("task:") || src.record_id.starts_with("preference:"),
            "record_id should be <table>:<key>, got {}",
            src.record_id
        );
        assert!(src.score > 0.0, "score should be > 0 for a real hit, got {}", src.score);
        assert!(src.score <= 1.0, "score should be in [0, 1], got {}", src.score);
    }
}
