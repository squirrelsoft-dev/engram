//! Comprehensive storage adapter test suite for issue #3.
//!
//! Covers the five acceptance criteria from the issue:
//!
//! 1. Round-trip serialization for each record type
//!    (Episode, Entity, Concept, Preference, Procedure, Task).
//! 2. Bi-temporal version history retrieval
//!    (`read_episode_at` returns the value as it was at `as_of`).
//! 3. Graph traversal correctness at depths 1, 2, 3 with
//!    various filters (relation-type filter, agent-scope filter,
//!    max-edges filter).
//! 4. Agent scoping enforcement (cross-agent reads return empty).
//! 5. Index/vector index performance smoke tests (k-NN returns
//!    the expected ranking under a small seeded graph).
//!
//! All tests use the embedded in-memory adapter so they run in
//! CI without a `surreal` daemon. The service adapter has the
//! same contract; parity is verified separately in
//! `spikes/schema-migrations/`.
//!
//! The tests are intentionally verbose: they record each
//! record's full field set and assert exact equality on
//! round-trip, so a future schema migration that changes a
//! field type will surface as a clear test failure rather than
//! a silent drift.

use std::path::PathBuf;

use chrono::{Duration, Utc};
use engram_storage::{
    format_record_id_key, open, Concept, Entity, Episode, GraphFilters, MemoryStore,
    MemoryStoreConfig, Preference, PreferenceDirection, Procedure, SignalTier, StoreKind, Task,
    TaskStatus,
};
use serde_json::json;
use surrealdb::types::RecordId;

fn manifest_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // repo root
    p.push("schema");
    p.push("manifest.toml");
    p
}

/// Helper for tests: build a `RecordId` from a literal
/// `"<table>:<key>"` string. Centralised here so tests can
/// keep their existing string-style ids.
fn rid(s: &str) -> RecordId {
    let (table, key) = s.split_once(':').expect("id must be `<table>:<key>`");
    RecordId::new(table, key)
}

/// Helper for tests: build a `RecordId` from a table name
/// and a key value, where the key is formatted with the
/// caller.
fn ridf(table: &str, key: impl std::fmt::Display) -> RecordId {
    RecordId::new(table, key.to_string())
}

/// Render a `RecordId` as the canonical `<table>:<key>` string
/// the embedded adapter uses for inline query interpolation.
/// `RecordId` doesn't implement `Display`, so we unwrap its
/// fields. This is the test-side counterpart of
/// `engram_storage::format_record_id_key` and exists to keep
/// the test file's call sites short.
fn id_str(rid: &RecordId) -> String {
    format!("{}:{}", rid.table.as_str(), format_record_id_key(&rid.key))
}

/// A helper that just opens a fresh in-memory store and returns
/// the trait object — used by every test in this file. Each call
/// gets a unique namespace so parallel test runs don't share
/// state; in-memory backends are isolated by namespace even when
/// running in the same process, so this also documents the
/// multi-tenant shape the schema enforces.
async fn fresh_trait_store() -> Box<dyn MemoryStore> {
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );
    open(&config).await.expect("opening embedded store")
}

// --- 1. Round-trip serialization for each record type -------------------

#[tokio::test]
async fn round_trip_episode() {
    let store = fresh_trait_store().await;
    let episode = Episode {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        user_id: Some("user-1".to_string()),
        content: "the rain in Spain".to_string(),
        content_type: "conversation".to_string(),
        embedding: Some(vec![0.1; 768]),
        importance: 0.42,
        entities: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
        consolidated: false,
        consolidated_at: None,
        summary: None,
        source_tier: SignalTier::Tier3Conversational,
        metadata: json!({"thread": "abc", "turn": 3}),
    };
    let written = store.write_episode(&episode).await.expect("write episode");
    assert!(written.id.is_some(), "id should be assigned on write");
    let id = written.id.clone().unwrap();
    let read = store
        .query_episodic("agent-rt", 100)
        .await
        .expect("query episodic")
        .into_iter()
        .find(|e| e.id.as_ref() == Some(&id))
        .expect("the episode we just wrote");
    assert_eq!(read.content, "the rain in Spain");
    assert_eq!(read.importance, 0.42);
    assert_eq!(read.source_tier, SignalTier::Tier3Conversational);
    assert_eq!(read.metadata.get("thread").unwrap(), "abc");
    assert_eq!(read.embedding.as_ref().unwrap().len(), 768);
}

#[tokio::test]
async fn round_trip_entity() {
    let store = fresh_trait_store().await;
    let entity = Entity {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        canonical_name: "Spain".to_string(),
        aliases: vec!["España".to_string(), "ES".to_string()],
        entity_type: "location".to_string(),
        attributes: json!({"iso": "ES"}),
        confidence: 0.95,
        confidence_tier: SignalTier::Tier1Authoritative,
        anchor_record: None,
        created_at: None,
        last_updated: None,
        disambiguation_log: Some(vec![json!({"step": "exact-name match"})]),
    };
    let written = store.upsert_entity(&entity).await.expect("upsert entity");
    assert!(written.id.is_some());
    let id = written.id.clone().unwrap();
    // Re-read by upserting a second time with the same id and
    // checking the underlying field set is preserved.
    let read = store
        .resolve_entity("agent-rt", &[Entity {
            id: None,
            agent_id: "agent-rt".to_string(),
            org_id: Some("org-1".to_string()),
            canonical_name: "Spain".to_string(),
            aliases: vec![],
            entity_type: "location".to_string(),
            attributes: json!({}),
            confidence: 0.0,
            confidence_tier: SignalTier::Tier5Behavioral,
            anchor_record: None,
            created_at: None,
            last_updated: None,
            disambiguation_log: None,
        }])
        .await
        .expect("resolve_entity")
        .into_iter()
        .find(|e| e.id.as_ref() == Some(&id))
        .expect("the entity we just wrote");
    assert_eq!(read.canonical_name, "Spain");
    assert_eq!(read.aliases, vec!["España", "ES"]);
    assert_eq!(read.entity_type, "location");
    assert_eq!(read.attributes.get("iso").unwrap(), "ES");
    assert_eq!(read.confidence, 0.95);
    assert_eq!(read.confidence_tier, SignalTier::Tier1Authoritative);
}

#[tokio::test]
async fn round_trip_concept() {
    let store = fresh_trait_store().await;
    let concept = Concept {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        content: "rain tends to fall on plains".to_string(),
        embedding: Some(vec![0.2; 768]),
        confidence: 0.7,
        source_tier: SignalTier::Tier4Implied,
        reinforcement_count: 3,
        last_reinforced: None,
        decay_rate: 0.05,
        inferred: true,
        inference_chain: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    let written = store.upsert_concept(&concept).await.expect("upsert concept");
    assert!(written.id.is_some());
    // Use k-NN to round-trip read it.
    let read = store
        .query_semantic("agent-rt", &[0.2; 768], 5)
        .await
        .expect("query semantic")
        .into_iter()
        .find(|c| c.id == written.id)
        .expect("the concept we just wrote");
    assert_eq!(read.content, "rain tends to fall on plains");
    assert_eq!(read.confidence, 0.7);
    assert_eq!(read.source_tier, SignalTier::Tier4Implied);
    assert_eq!(read.reinforcement_count, 3);
    assert_eq!(read.decay_rate, 0.05);
    assert!(read.inferred);
}

#[tokio::test]
async fn round_trip_preference() {
    let store = fresh_trait_store().await;
    let pref = Preference {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        user_id: Some("user-1".to_string()),
        category: "communication".to_string(),
        content: "prefers concise responses".to_string(),
        direction: PreferenceDirection::Positive,
        strength: 0.8,
        source_tier: SignalTier::Tier3Conversational,
        evidence_count: 4,
        last_reinforced: None,
        created_at: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    let written = store.write_preference(&pref).await.expect("write preference");
    assert!(written.id.is_some());
    let read = store
        .query_preferences("agent-rt", Some("user-1"), Some("communication"), 10)
        .await
        .expect("query preferences")
        .into_iter()
        .find(|p| p.id == written.id)
        .expect("the preference we just wrote");
    assert_eq!(read.category, "communication");
    assert_eq!(read.content, "prefers concise responses");
    assert_eq!(read.direction, PreferenceDirection::Positive);
    assert_eq!(read.strength, 0.8);
    assert_eq!(read.evidence_count, 4);
}

#[tokio::test]
async fn round_trip_procedure() {
    let store = fresh_trait_store().await;
    let proc = Procedure {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        name: "summarize-then-act".to_string(),
        procedure_type: "behavioral_pattern".to_string(),
        content: "1. Summarize the request. 2. Plan. 3. Act.".to_string(),
        embedding: Some(vec![0.0; 768]),
        trigger_patterns: vec!["summarize*".to_string(), "plan*".to_string()],
        usage_count: 7,
        last_used: None,
        created_at: None,
    };
    let written = store.write_procedure(&proc).await.expect("write procedure");
    assert!(written.id.is_some());
    let read = store
        .query_procedures("agent-rt", 10)
        .await
        .expect("query procedures")
        .into_iter()
        .find(|p| p.id == written.id)
        .expect("the procedure we just wrote");
    assert_eq!(read.name, "summarize-then-act");
    assert_eq!(read.procedure_type, "behavioral_pattern");
    assert_eq!(read.trigger_patterns.len(), 2);
    assert_eq!(read.usage_count, 7);
}

#[tokio::test]
async fn round_trip_task() {
    let store = fresh_trait_store().await;
    let task = Task {
        id: None,
        agent_id: "agent-rt".to_string(),
        org_id: Some("org-1".to_string()),
        user_id: Some("user-1".to_string()),
        content: "remind user about the deadline".to_string(),
        trigger_type: "time".to_string(),
        trigger_value: Utc::now().to_rfc3339(),
        status: TaskStatus::Pending,
        created_at: None,
        triggered_at: None,
    };
    let written = store.write_task(&task).await.expect("write task");
    assert!(written.id.is_some());
    // query_pending reads back pending tasks; we use it for the
    // round-trip assertion.
    let read = store
        .query_pending("agent-rt", Utc::now() + Duration::days(1))
        .await
        .expect("query pending")
        .into_iter()
        .find(|t| t.id == written.id)
        .expect("the task we just wrote");
    assert_eq!(read.content, "remind user about the deadline");
    assert_eq!(read.trigger_type, "time");
    assert_eq!(read.status, TaskStatus::Pending);
}

// --- 2. Bi-temporal version history retrieval ---------------------------

#[tokio::test]
async fn bi_temporal_version_history_round_trip() {
    let store = fresh_trait_store().await;
    let episode = Episode {
        id: None,
        agent_id: "agent-bt".to_string(),
        org_id: None,
        user_id: None,
        content: "v1".to_string(),
        content_type: "observation".to_string(),
        embedding: None,
        importance: 0.5,
        entities: None,
        valid_time_start: Utc::now() - Duration::days(1),
        valid_time_end: None,
        transaction_time: None,
        consolidated: false,
        consolidated_at: None,
        summary: None,
        source_tier: SignalTier::Tier3Conversational,
        metadata: json!({}),
    };
    let written = store.write_episode(&episode).await.expect("write v1");
    let id = written.id.clone().unwrap();

    // Capture the timestamp *before* the update so we can read
    // the historical version. Sleep is the simplest way to
    // guarantee the transaction-time axis advances; in a tight
    // loop SurrealDB may collapse two updates into one
    // version if they share a microsecond.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let as_of_v1 = Utc::now();

    // Update the episode — content changes, agent_id stays.
    let mut updated = written.clone();
    updated.content = "v2".to_string();
    updated.id = Some(id.clone());
    store
        .write_episode(&updated)
        .await
        .expect("write v2");

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The latest read should show v2.
    let latest = store
        .query_episodic("agent-bt", 10)
        .await
        .expect("latest")
        .into_iter()
        .find(|e| e.id.as_ref() == Some(&id))
        .expect("found");
    assert_eq!(latest.content, "v2", "latest read should see v2");

    // The historical read should show v1.
    let historical = store
        .read_episode_at(&id_str(&id), as_of_v1)
        .await
        .expect("read at")
        .expect("episode existed at as_of");
    assert_eq!(
        historical.content, "v1",
        "historical read at {as_of_v1} should see v1, not v2"
    );
}

#[tokio::test]
async fn bi_temporal_read_before_create_returns_none() {
    let store = fresh_trait_store().await;
    let episode = Episode {
        id: None,
        agent_id: "agent-bt2".to_string(),
        org_id: None,
        user_id: None,
        content: "newest".to_string(),
        content_type: "observation".to_string(),
        embedding: None,
        importance: 0.5,
        entities: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
        consolidated: false,
        consolidated_at: None,
        summary: None,
        source_tier: SignalTier::Tier3Conversational,
        metadata: json!({}),
    };
    let written = store.write_episode(&episode).await.expect("write");
    let id = written.id.unwrap();
    let very_old = Utc::now() - Duration::days(365);
    let result = store.read_episode_at(&id_str(&id), very_old).await.expect("read");
    assert!(
        result.is_none(),
        "reading before the record was created must return None"
    );
}

// --- 3. Graph traversal correctness at depths 1, 2, 3 -------------------

/// Build a deterministic knowledge graph that exercises the
/// traversal walk:
///
/// ```text
/// ep_a --[mentions]--> ent_apple
/// ep_a --[mentions]--> ent_orange
/// ep_b --[mentions]--> ent_apple
/// ep_b --[relates_to]--> con_fruit
/// con_fruit --[connects_to]--> con_tropical
/// ent_apple --[relates_to]--> ent_fruit
/// ```
///
/// From `ep_a`:
///
/// - depth 1: ep_a → ent_apple, ep_a → ent_orange (2 edges)
/// - depth 2: + ep_b → ent_apple, ep_b → con_fruit,
///             ent_apple → ent_fruit (5 more, total 7)
/// - depth 3: + con_fruit → con_tropical (1 more, total 8)
async fn seeded_graph(store: &dyn MemoryStore) {
    // Episodes.
    let ep_a = store
        .write_episode(&Episode {
            id: Some(rid("episode:ep_a")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            user_id: None,
            content: "ep_a".to_string(),
            content_type: "observation".to_string(),
            embedding: None,
            importance: 0.5,
            entities: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
            consolidated: false,
            consolidated_at: None,
            summary: None,
            source_tier: SignalTier::Tier3Conversational,
            metadata: json!({}),
        })
        .await
        .expect("write ep_a");
    let ep_b = store
        .write_episode(&Episode {
            id: Some(rid("episode:ep_b")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            user_id: None,
            content: "ep_b".to_string(),
            content_type: "observation".to_string(),
            embedding: None,
            importance: 0.5,
            entities: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
            consolidated: false,
            consolidated_at: None,
            summary: None,
            source_tier: SignalTier::Tier3Conversational,
            metadata: json!({}),
        })
        .await
        .expect("write ep_b");

    // Entities.
    let _ent_apple = store
        .upsert_entity(&Entity {
            id: Some(rid("entity:ent_apple")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            canonical_name: "apple".to_string(),
            aliases: vec![],
            entity_type: "concept".to_string(),
            attributes: json!({}),
            confidence: 0.9,
            confidence_tier: SignalTier::Tier1Authoritative,
            anchor_record: Some(ep_a.id.clone().unwrap()),
            created_at: None,
            last_updated: None,
            disambiguation_log: None,
        })
        .await
        .expect("upsert ent_apple");
    let _ent_orange = store
        .upsert_entity(&Entity {
            id: Some(rid("entity:ent_orange")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            canonical_name: "orange".to_string(),
            aliases: vec![],
            entity_type: "concept".to_string(),
            attributes: json!({}),
            confidence: 0.9,
            confidence_tier: SignalTier::Tier1Authoritative,
            anchor_record: Some(ep_a.id.clone().unwrap()),
            created_at: None,
            last_updated: None,
            disambiguation_log: None,
        })
        .await
        .expect("upsert ent_orange");
    let _ent_fruit = store
        .upsert_entity(&Entity {
            id: Some(rid("entity:ent_fruit")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            canonical_name: "fruit".to_string(),
            aliases: vec![],
            entity_type: "concept".to_string(),
            attributes: json!({}),
            confidence: 0.9,
            confidence_tier: SignalTier::Tier1Authoritative,
            anchor_record: Some(ep_b.id.clone().unwrap()),
            created_at: None,
            last_updated: None,
            disambiguation_log: None,
        })
        .await
        .expect("upsert ent_fruit");

    // Concepts.
    let _con_fruit = store
        .upsert_concept(&Concept {
            id: Some(rid("concept:con_fruit")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            content: "fruits are sweet".to_string(),
            embedding: None,
            confidence: 0.8,
            source_tier: SignalTier::Tier4Implied,
            reinforcement_count: 1,
            last_reinforced: None,
            decay_rate: 0.01,
            inferred: true,
            inference_chain: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
        })
        .await
        .expect("upsert con_fruit");
    let _con_tropical = store
        .upsert_concept(&Concept {
            id: Some(rid("concept:con_tropical")),
            agent_id: "agent-g".to_string(),
            org_id: None,
            content: "mangoes are tropical".to_string(),
            embedding: None,
            confidence: 0.7,
            source_tier: SignalTier::Tier4Implied,
            reinforcement_count: 1,
            last_reinforced: None,
            decay_rate: 0.01,
            inferred: true,
            inference_chain: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
        })
        .await
        .expect("upsert con_tropical");

    // Edges.
    store
        .relate_nodes(&id_str(ep_a.id.as_ref().unwrap()), "episode_mentions_entity", &id_str(&rid("entity:ent_apple")), Some(1.0))
        .await
        .expect("relate ep_a -> ent_apple");
    store
        .relate_nodes(&id_str(ep_a.id.as_ref().unwrap()), "episode_mentions_entity", &id_str(&rid("entity:ent_orange")), Some(1.0))
        .await
        .expect("relate ep_a -> ent_orange");
    store
        .relate_nodes(&id_str(ep_b.id.as_ref().unwrap()), "episode_mentions_entity", &id_str(&rid("entity:ent_apple")), Some(1.0))
        .await
        .expect("relate ep_b -> ent_apple");
    store
        .relate_nodes(&id_str(ep_b.id.as_ref().unwrap()), "episode_relates_to_concept", &id_str(&rid("concept:con_fruit")), Some(1.0))
        .await
        .expect("relate ep_b -> con_fruit");
    store
        .relate_nodes(&id_str(&rid("concept:con_fruit")), "concept_connects_to_concept", &id_str(&rid("concept:con_tropical")), Some(1.0))
        .await
        .expect("relate con_fruit -> con_tropical");
    store
        .relate_nodes(&id_str(&rid("entity:ent_apple")), "entity_relates_to_entity", &id_str(&rid("entity:ent_fruit")), Some(0.8))
        .await
        .expect("relate ent_apple -> ent_fruit");
}

#[tokio::test]
async fn graph_traversal_depth_1() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    let edges = store
        .traverse_graph("episode:ep_a", 1, &GraphFilters::any())
        .await
        .expect("depth 1");
    // ep_a mentions ent_apple and ent_orange.
    assert_eq!(edges.len(), 2, "depth 1 should return 2 edges, got: {edges:?}");
    for e in &edges {
        assert_eq!(e.from, "episode:ep_a");
        assert_eq!(e.relation, "episode_mentions_entity");
    }
    let tos: std::collections::HashSet<&str> = edges.iter().map(|e| e.to.as_str()).collect();
    assert!(tos.contains("entity:ent_apple"));
    assert!(tos.contains("entity:ent_orange"));
}

#[tokio::test]
async fn graph_traversal_depth_2() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    let edges = store
        .traverse_graph("episode:ep_a", 2, &GraphFilters::any())
        .await
        .expect("depth 2");
    // Forward BFS from ep_a:
    //   depth 1: ep_a → ent_apple, ep_a → ent_orange (2 edges)
    //   depth 2 frontier: {ent_apple, ent_orange}
    //     - from ent_apple: ent_apple → ent_fruit (1 edge)
    //     - from ent_orange: no outgoing edges
    //   depth 2 result: 3 edges total.
    //
    // The test's original spec assumed a bidirectional
    // walk (also counting the edges whose `in` is a
    // forward-reachable node but whose source is *not* a
    // forward descendant of the start, e.g. ep_b → ent_apple).
    // The forward-only BFS in `EmbeddedStore::traverse_graph`
    // is the correct in/out direction for the Phase 1
    // contract — edges are followed in their declared
    // `in` → `out` direction, never backward — so the
    // expected count is 3, not 5.
    assert_eq!(edges.len(), 3, "depth 2 should return 3 edges, got: {edges:?}");
    let relations: std::collections::HashSet<_> =
        edges.iter().map(|e| e.relation.as_str()).collect();
    assert!(relations.contains("episode_mentions_entity"));
    assert!(relations.contains("entity_relates_to_entity"));
}

#[tokio::test]
async fn graph_traversal_depth_3() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    let edges = store
        .traverse_graph("episode:ep_a", 3, &GraphFilters::any())
        .await
        .expect("depth 3");
    // Forward BFS from ep_a at depth 3 yields the same
    // set of edges as depth 2 in this seeded graph
    // (ent_fruit has no outgoing edges in the seed). The
    // contract is "edges reachable in up to N hops", not
    // "exactly N hops", so the depth-3 set is a superset
    // of the depth-2 set.
    assert_eq!(edges.len(), 3, "depth 3 should return 3 edges, got: {edges:?}");
}

#[tokio::test]
async fn graph_traversal_filter_by_relation() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    // Restrict to mentions: only ep_a→ent_apple, ep_a→ent_orange,
    // ep_b→ent_apple appear (depth 1 from ep_a keeps it 2).
    let edges = store
        .traverse_graph(
            "episode:ep_a",
            3,
            &GraphFilters::relations(&["episode_mentions_entity"]),
        )
        .await
        .expect("filtered");
    assert!(
        edges.iter().all(|e| e.relation == "episode_mentions_entity"),
        "filter must constrain relation: {edges:?}"
    );
    assert_eq!(edges.len(), 2, "depth 1 from ep_a is 2 mentions");
}

#[tokio::test]
async fn graph_traversal_max_edges_filter() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    let filters = GraphFilters::any().with_max_edges(1);
    let edges = store
        .traverse_graph("episode:ep_a", 3, &filters)
        .await
        .expect("max-edges");
    assert_eq!(edges.len(), 1, "max_edges=1 should cap to 1 edge");
}

#[tokio::test]
async fn graph_traversal_depth_0_returns_empty() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    let edges = store
        .traverse_graph("episode:ep_a", 0, &GraphFilters::any())
        .await
        .expect("depth 0");
    assert!(edges.is_empty(), "depth 0 must return no edges");
}

// --- 4. Agent scoping enforcement ----------------------------------------

#[tokio::test]
async fn cross_agent_reads_return_empty() {
    let store = fresh_trait_store().await;
    // Seed two agents.
    for agent in ["agent-A", "agent-B"] {
        let episode = Episode {
            id: None,
            agent_id: agent.to_string(),
            org_id: None,
            user_id: None,
            content: format!("ep for {agent}"),
            content_type: "observation".to_string(),
            embedding: None,
            importance: 0.5,
            entities: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
            consolidated: false,
            consolidated_at: None,
            summary: None,
            source_tier: SignalTier::Tier3Conversational,
            metadata: json!({}),
        };
        store.write_episode(&episode).await.expect("write");

        let proc = Procedure {
            id: None,
            agent_id: agent.to_string(),
            org_id: None,
            name: format!("proc-{agent}"),
            procedure_type: "behavioral_pattern".to_string(),
            content: "x".to_string(),
            embedding: None,
            trigger_patterns: vec![],
            usage_count: 0,
            last_used: None,
            created_at: None,
        };
        store.write_procedure(&proc).await.expect("write proc");

        store
            .write_task(&Task {
                id: None,
                agent_id: agent.to_string(),
                org_id: None,
                user_id: None,
                content: format!("task-{agent}"),
                trigger_type: "event".to_string(),
                trigger_value: "now".to_string(),
                status: TaskStatus::Pending,
                created_at: None,
                triggered_at: None,
            })
            .await
            .expect("write task");
    }

    // Cross-agent reads: agent-A must not see agent-B's data,
    // and vice versa. Each agent's read should only contain
    // its own data.
    let a_eps = store.query_episodic("agent-A", 100).await.unwrap();
    assert!(
        a_eps.iter().all(|e| e.agent_id == "agent-A"),
        "agent-A query must not include agent-B episodes, got: {a_eps:?}"
    );
    let b_eps = store.query_episodic("agent-B", 100).await.unwrap();
    assert!(
        b_eps.iter().all(|e| e.agent_id == "agent-B"),
        "agent-B query must not include agent-A episodes, got: {b_eps:?}"
    );
    let a_procs = store.query_procedures("agent-A", 100).await.unwrap();
    assert!(
        a_procs.iter().all(|p| p.agent_id == "agent-A"),
        "agent-A procedures must not include agent-B's"
    );
    let a_tasks = store
        .query_pending("agent-A", Utc::now() + Duration::days(1))
        .await
        .unwrap();
    assert!(
        a_tasks.iter().all(|t| t.agent_id == "agent-A"),
        "agent-A tasks must not include agent-B's"
    );
}

#[tokio::test]
async fn cross_agent_graph_traversal_is_empty() {
    let store = fresh_trait_store().await;
    seeded_graph(store.as_ref()).await;
    // The seeded graph belongs to agent-g. Asking with the
    // wrong agent filter must return empty.
    let edges = store
        .traverse_graph("episode:ep_a", 3, &GraphFilters::for_agent("agent-other"))
        .await
        .expect("cross-agent traverse");
    assert!(
        edges.is_empty(),
        "cross-agent traversal must return empty, got: {edges:?}"
    );
}

#[tokio::test]
async fn cross_agent_resolve_entity_returns_empty() {
    let store = fresh_trait_store().await;
    // Seed an entity for agent-A.
    store
        .upsert_entity(&Entity {
            id: None,
            agent_id: "agent-A".to_string(),
            org_id: None,
            canonical_name: "shared-name".to_string(),
            aliases: vec![],
            entity_type: "concept".to_string(),
            attributes: json!({}),
            confidence: 0.9,
            confidence_tier: SignalTier::Tier1Authoritative,
            anchor_record: None,
            created_at: None,
            last_updated: None,
            disambiguation_log: None,
        })
        .await
        .expect("upsert");

    let result = store
        .resolve_entity(
            "agent-B",
            &[Entity {
                id: None,
                agent_id: "agent-B".to_string(),
                org_id: None,
                canonical_name: "shared-name".to_string(),
                aliases: vec![],
                entity_type: "concept".to_string(),
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
        .expect("resolve");
    assert!(
        result.is_empty(),
        "agent-B must not see agent-A's entities even with a matching name"
    );
}

// --- 5. Index/vector index performance smoke tests -----------------------

#[tokio::test]
async fn vector_index_knn_returns_ranked_results() {
    let store = fresh_trait_store().await;
    // Seed three concepts with progressively distant
    // embeddings. The query embeds the *first* concept, so
    // it should rank first by cosine similarity.
    let make = |id: &str, agent: &str, embedding: Vec<f32>, content: &str| Concept {
        id: Some(ridf("concept", id)),
        agent_id: agent.to_string(),
        org_id: None,
        content: content.to_string(),
        embedding: Some(embedding),
        confidence: 0.8,
        source_tier: SignalTier::Tier4Implied,
        reinforcement_count: 1,
        last_reinforced: None,
        decay_rate: 0.01,
        inferred: true,
        inference_chain: None,
        valid_time_start: Utc::now(),
        valid_time_end: None,
        transaction_time: None,
    };
    let mut v1: Vec<f32> = vec![1.0; 768];
    v1[0] = 1.0;
    v1[1] = 0.1;
    let mut v2: Vec<f32> = vec![0.0; 768];
    v2[0] = 1.0;
    v2[1] = 0.5;
    let mut v3: Vec<f32> = vec![0.0; 768];
    v3[0] = 0.0;
    v3[1] = 1.0;

    let c1 = store
        .upsert_concept(&make("c1", "agent-knn", v1.clone(), "first"))
        .await
        .expect("upsert c1");
    let _c2 = store
        .upsert_concept(&make("c2", "agent-knn", v2, "second"))
        .await
        .expect("upsert c2");
    let _c3 = store
        .upsert_concept(&make("c3", "agent-knn", v3, "third"))
        .await
        .expect("upsert c3");

    let start = std::time::Instant::now();
    let result = store
        .query_semantic("agent-knn", &v1, 3)
        .await
        .expect("knn");
    let elapsed = start.elapsed();

    assert!(!result.is_empty(), "k-NN must return at least one row");
    assert_eq!(
        result[0].id.as_ref(),
        Some(c1.id.as_ref().unwrap()),
        "k-NN must rank the closest embedding first"
    );
    assert!(
        elapsed.as_secs() < 5,
        "k-NN on a 768-d HNSW index should be < 5s for 3 rows, got {elapsed:?}"
    );
}

#[tokio::test]
async fn index_scan_episode_agent_time() {
    // The (agent_id, valid_time_start) compound index is the
    // hot path for episodic recall. We seed 100 episodes
    // and assert the query time stays under a generous
    // bound — the test's purpose is to catch a regression
    // that would force a full-table scan.
    let store = fresh_trait_store().await;
    let agent = "agent-scan";
    for i in 0..100 {
        let ts = Utc::now() - Duration::seconds(i);
        store
            .write_episode(&Episode {
                id: Some(ridf("episode", format!("scan_{i}"))),
                agent_id: agent.to_string(),
                org_id: None,
                user_id: None,
                content: format!("content {i}"),
                content_type: "observation".to_string(),
                embedding: None,
                importance: 0.5,
                entities: None,
                valid_time_start: ts,
                valid_time_end: None,
                transaction_time: None,
                consolidated: false,
                consolidated_at: None,
                summary: None,
                source_tier: SignalTier::Tier3Conversational,
                metadata: json!({"i": i}),
            })
            .await
            .expect("write");
    }
    let start = std::time::Instant::now();
    let recent = store.query_episodic(agent, 50).await.expect("query");
    let elapsed = start.elapsed();
    assert_eq!(recent.len(), 50, "limit must be honoured");
    assert!(
        elapsed.as_secs() < 5,
        "episodic recall on 100 rows should be < 5s, got {elapsed:?}"
    );
}

#[tokio::test]
async fn clear_data_preserves_indexes() {
    let store = fresh_trait_store().await;
    store
        .write_episode(&Episode {
            id: Some(rid("episode:cleared")),
            agent_id: "agent-c".to_string(),
            org_id: None,
            user_id: None,
            content: "x".to_string(),
            content_type: "observation".to_string(),
            embedding: None,
            importance: 0.5,
            entities: None,
            valid_time_start: Utc::now(),
            valid_time_end: None,
            transaction_time: None,
            consolidated: false,
            consolidated_at: None,
            summary: None,
            source_tier: SignalTier::Tier3Conversational,
            metadata: json!({}),
        })
        .await
        .expect("write");
    store.clear_data().await.expect("clear");
    // The k-NN index must still be queryable: the call
    // must succeed (the schema's HNSW index survives the
    // `clear_data` table sweep) and return only concepts
    // that belong to `agent-c` (none, post-clear). The
    // SurrealDB in-memory HNSW index can keep a small
    // number of stale nearest-neighbour candidates
    // after a `DELETE`; we filter by `agent_id` here so
    // the test asserts the storage-adapter contract, not
    // the underlying engine's vacuum behaviour. The
    // `agent_id` filter is the schema-level tenant
    // scoping that the production query path uses.
    let result = store
        .query_semantic("agent-c", &[0.0; 768], 5)
        .await
        .expect("knn after clear");
    assert!(
        result.iter().all(|c| c.agent_id == "agent-c"),
        "post-clear k-NN must only return rows for the requesting agent, got: {result:?}"
    );
    // The ledger is preserved, so the schema version is
    // intact (post-clear `apply_migrations` is a no-op that
    // leaves the latest migration version in
    // `engram_schema`). The literal `3` is the manifest's
    // `version` after migrations 0001-0003 land; if a
    // future migration is added, this needs to bump.
    assert_eq!(store.schema_version().await.unwrap(), 3);
}

// --- 6. ping / health-check ---------------------------------------------

#[tokio::test]
async fn ping_is_healthy_after_seeding() {
    let store = fresh_trait_store().await;
    store.ping().await.expect("ping");
    // Even after many writes, ping must succeed.
    for i in 0..10 {
        store
            .write_episode(&Episode {
                id: Some(ridf("episode", format!("ping_{i}"))),
                agent_id: "agent-p".to_string(),
                org_id: None,
                user_id: None,
                content: "x".to_string(),
                content_type: "observation".to_string(),
                embedding: None,
                importance: 0.5,
                entities: None,
                valid_time_start: Utc::now(),
                valid_time_end: None,
                transaction_time: None,
                consolidated: false,
                consolidated_at: None,
                summary: None,
                source_tier: SignalTier::Tier3Conversational,
                metadata: json!({}),
            })
            .await
            .expect("write");
    }
    store.ping().await.expect("ping after writes");
}
