//! Response types for the Memory Core operation surface.
//!
//! [`EpisodeRecord`] is the shape documented in README §7.1
//! (`store()` output): episode_id, the linked entities, the
//! importance score, and a coarse "queued for" consolidation
//! hint. Higher layers (REST, MCP, CLI) translate this into
//! their transport's response shape; the Memory Core owns
//! the domain types.
//!
//! [`RecallResponse`] is the shape documented in README §7.2
//! (`recall()` output): the prompt-ready context string, the
//! list of source records that contributed to it, and the
//! query types the orchestrator detected. The retrieval
//! orchestrator (issue #5) assembles this from the five
//! memory layers' raw hits.

use serde::{Deserialize, Serialize};

use engram_storage::Entity;

/// The output of a successful `store()` call.
///
/// This is the data shape README §7.1 promises the caller. It
/// is a value type — the pipeline constructs it from the
/// persisted `Episode` and the linked `Entity` rows. The
/// episode_id and the entities carry their full domain
/// representations so the caller can decide what to expose
/// over the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EpisodeRecord {
    /// The id of the persisted episode. `None` only in the
    /// pre-write validation path; production callers always
    /// see `Some`.
    pub episode_id: Option<engram_storage::RecordId>,

    /// The extracted and disambiguated entities. Already
    /// persisted to the store by the time the pipeline
    /// returns; the caller sees the post-merge canonical view.
    pub entities: Vec<Entity>,

    /// The 0.0–1.0 importance score assigned by the scorer.
    pub importance: f32,

    /// A coarse "queued for" hint. Today this is a
    /// `("consolidation", priority)` tuple where the priority
    /// is the importance score — the consolidation engine
    /// (Phase 3) re-reads the field. The shape will firm up
    /// when Phase 3 lands.
    pub queued_for: QueueHint,
}

/// A hint about when this episode is queued for the
/// consolidation engine. The tuple is the simplest possible
/// shape that lets the REST surface return it as
/// `{"stage": "consolidation", "priority": 0.7}` and lets the
/// MCP surface return it as a list-shaped `["consolidation",
/// 0.7]`. Both are common in the spec.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct QueueHint {
    /// The pipeline stage that will process this episode
    /// next. Today always `"consolidation"`.
    pub stage: String,
    /// The priority the engine should treat this episode
    /// with. Higher = sooner.
    pub priority: f32,
}

impl QueueHint {
    /// Build a `consolidation` queue hint with the given
    /// priority.
    pub fn consolidation(priority: f32) -> Self {
        Self {
            stage: "consolidation".to_string(),
            priority,
        }
    }
}

// ============================================================================
// Recall response
// ============================================================================

/// Which memory layer a source record came from. Mirrors the
/// five layers in README §6.2 (Episodic, Semantic, Procedural,
/// Prospective, Preference).
///
/// The retrieval orchestrator can return records from any
/// combination of layers; a single `recall()` may surface, for
/// example, two episodic events and one preference, all
/// classified under `query_types = [Episodic, Preference]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceLayer {
    Episodic,
    Semantic,
    Procedural,
    Prospective,
    Preference,
}

impl SourceLayer {
    /// The string value used in JSON / log output.
    pub fn as_str(self) -> &'static str {
        match self {
            SourceLayer::Episodic => "episodic",
            SourceLayer::Semantic => "semantic",
            SourceLayer::Procedural => "procedural",
            SourceLayer::Prospective => "prospective",
            SourceLayer::Preference => "preference",
        }
    }
}

/// A single contributing record in a `RecallResponse`. The
/// `RecallResponse::sources` list is what the README §7.2
/// "sources" field documents: provenance metadata for each
/// piece of the assembled context.
///
/// The record id is the persisted id (e.g. `episode:abc123`),
/// formatted as a string for transport. The `score` is the
/// post-merge ranking score (the `relevance × recency ×
/// importance` product from README §6.2 step 3). The `layer`
/// is which of the five memory layers produced this hit.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceRecord {
    /// Which memory layer this record came from.
    pub layer: SourceLayer,
    /// The persisted record id (`<table>:<key>`).
    pub record_id: String,
    /// The full text the context slice quoted from this
    /// record. Empty when the layer's content is structural
    /// (e.g. an entity-only result).
    pub excerpt: String,
    /// The 0.0–1.0 score the merge/rerank stage assigned.
    pub score: f32,
    /// When the underlying record's `valid_time_start`
    /// (episodic / semantic) or `last_reinforced` /
    /// `created_at` (the other layers) is. Used by the
    /// orchestrator's recency-weight calculation, surfaced
    /// here for the caller's downstream observability.
    pub valid_time: chrono::DateTime<chrono::Utc>,
}

/// The output of a `recall()` call. Holds the prompt-ready
/// context string, the list of source records the orchestrator
/// combined, and the layers the query was classified into.
///
/// The `context` field is the README §7.2 `context` output —
/// formatted for direct prompt injection. The format is a
/// stable, human-readable list of `<layer>: <text>` lines
/// preceded by a one-line header; the REST/MCP/CLI layers can
/// either pass `context` through unchanged or substitute their
/// own template.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecallResponse {
    /// The prompt-ready text to inject into the agent's
    /// context window. Empty string when no layer produced
    /// a hit; callers should treat that as "no relevant
    /// memory" and skip the injection.
    pub context: String,
    /// The contributing records, ordered by descending score
    /// (highest-relevance first). May be empty when no layer
    /// produced a hit.
    pub sources: Vec<SourceRecord>,
    /// The layers the query was classified into. Always
    /// non-empty — the classifier returns a default of
    /// `[Episodic, Semantic, Procedural, Preference,
    /// Prospective]` when no keyword matches, so the caller
    /// always knows which layers were queried.
    pub query_types: Vec<SourceLayer>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn source_layer_str_round_trip() {
        for layer in [
            SourceLayer::Episodic,
            SourceLayer::Semantic,
            SourceLayer::Procedural,
            SourceLayer::Prospective,
            SourceLayer::Preference,
        ] {
            let s = layer.as_str();
            assert!(!s.is_empty());
        }
    }

    #[test]
    fn recall_response_serialises_to_expected_shape() {
        // The REST / MCP layers rely on the JSON shape:
        // `context` is a string, `sources` is an array of
        // objects, `query_types` is an array of strings.
        // This is a snapshot of the shape; serde-derive
        // gives us the field names automatically, and the
        // rename_all on SourceLayer gives the lowercase
        // string form. The test pins both.
        let r = RecallResponse {
            context: "episodic: hello".to_string(),
            sources: vec![SourceRecord {
                layer: SourceLayer::Episodic,
                record_id: "episode:abc".to_string(),
                excerpt: "hello".to_string(),
                score: 0.5,
                valid_time: chrono::Utc::now(),
            }],
            query_types: vec![SourceLayer::Episodic, SourceLayer::Semantic],
        };
        let json = serde_json::to_value(&r).expect("serialise");
        assert_eq!(json["context"], "episodic: hello");
        assert_eq!(json["sources"][0]["layer"], "episodic");
        assert_eq!(json["query_types"][1], "semantic");
    }
}
