//! Response types for the Memory Core operation surface.
//!
//! [`EpisodeRecord`] is the shape documented in README §7.1
//! (`store()` output): episode_id, the linked entities, the
//! importance score, and a coarse "queued for" consolidation
//! hint. Higher layers (REST, MCP, CLI) translate this into
//! their transport's response shape; the Memory Core owns
//! the domain types.

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
