//! Request types for the Memory Core operation surface.
//!
//! The `StoreRequest` is the input to `IngestionPipeline::ingest`
//! and corresponds to `store(content, metadata?)` in README §7.1.
//! It carries the raw content, an explicit content type, the
//! agent id, and a bag of optional overrides the caller can
//! supply (valid time, importance, pre-identified entities).
//!
//! The pipeline also accepts the `metadata` object from §7.1 —
//! we mirror those fields as named struct fields so the
//! type-checker can catch missing required values. The
//! `metadata` object in the README is a *logical* grouping; the
//! Rust surface keeps each field at the top level for clarity.
//!
//! The `RecallRequest` is the input to the retrieval
//! orchestrator's `recall()` (issue #5) and corresponds to
//! `recall(query, filters?)` in README §7.2. The orchestrator
//! reads the query, classifies it, and dispatches to the
//! appropriate memory layers per the filters.

use chrono::{DateTime, Utc};

use crate::error::{IngestError, IngestResult};

/// The kind of content being ingested. Mirrors the schema's
/// `episode.content_type` enum from
/// `schema/migrations/0001_init.sql`.
///
/// README §6.1 step 1 (Normalize) and §5.2 (signal strength
/// hierarchy) together fix a default source tier per content
/// type: an `Assertion` is Tier 1, `Document` and `ToolResult`
/// are Tier 2, `Conversation` is Tier 3, and `Observation` is
/// Tier 5. The caller can override with
/// [`StoreRequest::with_source_tier`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContentType {
    Conversation,
    Document,
    ToolResult,
    Observation,
    Assertion,
}

impl ContentType {
    /// The string value persisted to the schema. Must match
    /// the `INSIDE [...]` assertion on `episode.content_type`.
    pub fn as_str(self) -> &'static str {
        match self {
            ContentType::Conversation => "conversation",
            ContentType::Document => "document",
            ContentType::ToolResult => "tool_result",
            ContentType::Observation => "observation",
            ContentType::Assertion => "assertion",
        }
    }

    /// The default source-tier for this content type, per the
    /// README §5.2 / §6.1 mapping. The pipeline uses this when
    /// the caller doesn't supply an explicit override.
    pub fn default_source_tier(self) -> engram_storage::SignalTier {
        use engram_storage::SignalTier::*;
        match self {
            ContentType::Conversation => Tier3Conversational,
            ContentType::Document | ContentType::ToolResult => Tier2Structured,
            ContentType::Observation => Tier5Behavioral,
            ContentType::Assertion => Tier1Authoritative,
        }
    }
}

impl std::str::FromStr for ContentType {
    type Err = IngestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "conversation" => Ok(ContentType::Conversation),
            "document" => Ok(ContentType::Document),
            "tool_result" => Ok(ContentType::ToolResult),
            "observation" => Ok(ContentType::Observation),
            "assertion" => Ok(ContentType::Assertion),
            other => Err(IngestError::Invalid(format!(
                "unknown content type: {other}"
            ))),
        }
    }
}

/// The input to a single `store()` call.
///
/// README §7.1 lists the fields the caller can supply. We
/// model them as named struct fields with builders; the
/// `metadata` object from the README is the top-level shape
/// here.
#[derive(Debug, Clone)]
pub struct StoreRequest {
    /// The agent that owns this memory. Required. Maps to
    /// `episode.agent_id` and the multi-tenant scoping rule in
    /// README §9.1.
    pub agent_id: String,

    /// The org scope (optional). Maps to `episode.org_id`.
    pub org_id: Option<String>,

    /// The user scope (optional). Maps to `episode.user_id`.
    pub user_id: Option<String>,

    /// The raw content to remember. Must be non-empty
    /// (`assert string::len($value) > 0` on the schema).
    pub content: String,

    /// The content type. Drives the default source-tier and
    /// the normalization path.
    pub content_type: ContentType,

    /// Override the automatic source-tier detection. When
    /// `None`, the pipeline uses
    /// [`ContentType::default_source_tier`].
    pub source_tier: Option<engram_storage::SignalTier>,

    /// When the event occurred in the world. Defaults to now.
    /// Maps to `episode.valid_time_start`.
    pub valid_time: Option<DateTime<Utc>>,

    /// End of the valid-time interval, if known. Maps to
    /// `episode.valid_time_end`.
    pub valid_time_end: Option<DateTime<Utc>>,

    /// Manual importance override. When `Some`, the pipeline
    /// uses this value directly and skips the importance
    /// scorer. Useful for callers that already have a
    /// domain-specific importance signal.
    pub importance_override: Option<f32>,

    /// Pre-identified entities the caller wants the pipeline
    /// to consider during extraction. The pipeline will
    /// *add* to this list based on its own extraction pass;
    /// it will not drop caller-supplied entities.
    pub preidentified_entities: Vec<String>,

    /// Free-form metadata the caller wants stamped on the
    /// episode. Maps to `episode.metadata` (the
    /// `FLEXIBLE TYPE object` field).
    pub metadata: serde_json::Value,
}

impl StoreRequest {
    /// Build a request with the minimum required fields. The
    /// caller adds optional fields via the `with_*` builders.
    pub fn new(agent_id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            org_id: None,
            user_id: None,
            content: content.into(),
            content_type: ContentType::Conversation,
            source_tier: None,
            valid_time: None,
            valid_time_end: None,
            importance_override: None,
            preidentified_entities: Vec::new(),
            metadata: serde_json::json!({}),
        }
    }

    /// Set the content type.
    pub fn with_content_type(mut self, ct: ContentType) -> Self {
        self.content_type = ct;
        self
    }

    /// Set the org id.
    pub fn with_org_id(mut self, org_id: impl Into<String>) -> Self {
        self.org_id = Some(org_id.into());
        self
    }

    /// Set the user id.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Override the automatic source-tier detection.
    pub fn with_source_tier(mut self, tier: engram_storage::SignalTier) -> Self {
        self.source_tier = Some(tier);
        self
    }

    /// Set the valid-time start. Defaults to "now" when not
    /// supplied.
    pub fn with_valid_time(mut self, t: DateTime<Utc>) -> Self {
        self.valid_time = Some(t);
        self
    }

    /// Set the valid-time end.
    pub fn with_valid_time_end(mut self, t: DateTime<Utc>) -> Self {
        self.valid_time_end = Some(t);
        self
    }

    /// Override the importance scorer. Useful when the caller
    /// has a domain-specific signal (e.g. "this is a billing
    /// event" → 0.95).
    pub fn with_importance(mut self, importance: f32) -> Self {
        self.importance_override = Some(importance);
        self
    }

    /// Add a pre-identified entity. The pipeline's own
    /// extraction pass is additive — it does not drop
    /// caller-supplied entities — so the caller can use this
    /// to seed the entity graph with names it already knows
    /// about (e.g. "the current user is Alice").
    pub fn with_preidentified_entity(mut self, name: impl Into<String>) -> Self {
        self.preidentified_entities.push(name.into());
        self
    }

    /// Attach a metadata object. The schema's `metadata` field
    /// is `FLEXIBLE TYPE object`, so any nested shape is
    /// accepted.
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = metadata;
        self
    }

    /// Validate the request. Returns `Ok(())` if every
    /// required field is present and the time range (if any)
    /// is well-formed.
    pub fn validate(&self) -> IngestResult<()> {
        if self.agent_id.trim().is_empty() {
            return Err(IngestError::Invalid("agent_id is required".to_string()));
        }
        if self.content.trim().is_empty() {
            return Err(IngestError::Invalid("content is required".to_string()));
        }
        if let (Some(start), Some(end)) = (self.valid_time, self.valid_time_end) {
            if end < start {
                return Err(IngestError::TimeRange(format!(
                    "valid_time_end ({end}) is before valid_time_start ({start})"
                )));
            }
        }
        if let Some(imp) = self.importance_override {
            if !(0.0..=1.0).contains(&imp) {
                return Err(IngestError::Invalid(format!(
                    "importance_override {imp} out of [0.0, 1.0]"
                )));
            }
        }
        Ok(())
    }
}

// ============================================================================
// RecallRequest
// ============================================================================

/// The input to a `recall()` call. Mirrors the README §7.2
/// `recall(query, filters?)` shape.
///
/// The `query` is the natural-language question the agent is
/// asking its memory. The filters restrict which layers are
/// consulted, which time range is searched, which entities are
/// involved, which user scope is read, and the per-layer cap.
/// All filters are optional; an empty filter set means "all
/// layers, all time, all entities, all users, default cap".
#[derive(Debug, Clone)]
pub struct RecallRequest {
    /// The agent that owns this recall. Maps to
    /// `episode.agent_id` and the multi-tenant scoping rule
    /// in README §9.1. Required.
    pub agent_id: String,

    /// The org scope (optional). Maps to `episode.org_id`.
    pub org_id: Option<String>,

    /// The user scope (optional). Maps to `episode.user_id`
    /// and to the `preference.user_id` filter on the
    /// preference layer.
    pub user_id: Option<String>,

    /// The natural-language query. Drives the query
    /// classification step (which layers to consult) and the
    /// per-layer retrieval (vector search, trigger pattern
    /// matching, etc.). Must be non-empty.
    pub query: String,

    /// Restrict the search to a subset of memory layers.
    /// When empty, the orchestrator runs the classifier and
    /// dispatches to every layer the classifier selects
    /// (the classifier's default with no keyword match is
    /// "all five layers", per the README §6.2 step 1
    /// behaviour of running all layers when classification
    /// is uncertain).
    pub types: Vec<crate::response::SourceLayer>,

    /// Valid-time bounds for episodic and semantic results.
    /// When `Some`, the orchestrator applies
    /// `valid_time_start >= range.start` and
    /// `valid_time_end <= range.end` (or `IS NULL`) to the
    /// query. Episodic / semantic records outside the range
    /// are not returned.
    pub time_range: Option<TimeRange>,

    /// Restrict the search to episodes that mention any of
    /// these entity names (case-insensitive canonical-name
    /// match on the entity records the episode links to).
    /// Empty list disables the filter.
    pub entities: Vec<String>,

    /// Per-layer result cap. Defaults to 10 when the caller
    /// doesn't supply one. The orchestrator never returns
    /// more than this from any single layer; the merge step
    /// may return up to `max_results × n_active_layers` rows
    /// before its own cap kicks in (the merge cap is
    /// `max_results` to match the README §7.2 semantics).
    pub max_results: u32,
}

/// A half-open valid-time range. `start` is inclusive,
/// `end` is exclusive. Matches the bi-temporal contract
/// documented in `docs/design/schema-migrations.md` §5.5.
#[derive(Debug, Clone, Copy)]
pub struct TimeRange {
    pub start: chrono::DateTime<chrono::Utc>,
    pub end: chrono::DateTime<chrono::Utc>,
}

impl RecallRequest {
    /// Build a request with the minimum required fields.
    /// Optional filters are added via the `with_*` builders
    /// or by direct field assignment.
    pub fn new(agent_id: impl Into<String>, query: impl Into<String>) -> Self {
        Self {
            agent_id: agent_id.into(),
            org_id: None,
            user_id: None,
            query: query.into(),
            types: Vec::new(),
            time_range: None,
            entities: Vec::new(),
            max_results: 10,
        }
    }

    /// Set the org id.
    pub fn with_org_id(mut self, org_id: impl Into<String>) -> Self {
        self.org_id = Some(org_id.into());
        self
    }

    /// Set the user id.
    pub fn with_user_id(mut self, user_id: impl Into<String>) -> Self {
        self.user_id = Some(user_id.into());
        self
    }

    /// Restrict the orchestrator to a specific subset of
    /// memory layers. An empty list means "let the
    /// classifier decide" (the default).
    pub fn with_types(
        mut self,
        types: Vec<crate::response::SourceLayer>,
    ) -> Self {
        self.types = types;
        self
    }

    /// Apply a valid-time bound to the search. Episodic and
    /// semantic results outside the range are filtered out.
    pub fn with_time_range(
        mut self,
        start: chrono::DateTime<chrono::Utc>,
        end: chrono::DateTime<chrono::Utc>,
    ) -> Self {
        self.time_range = Some(TimeRange { start, end });
        self
    }

    /// Restrict the search to episodes that mention one of
    /// the given entity names. The match is case-insensitive
    /// on the entity's `canonical_name`.
    pub fn with_entities(
        mut self,
        entities: Vec<String>,
    ) -> Self {
        self.entities = entities;
        self
    }

    /// Override the per-layer result cap. The merge step
    /// uses this same value as its own cap.
    pub fn with_max_results(mut self, max_results: u32) -> Self {
        self.max_results = max_results;
        self
    }

    /// Validate the request. Returns `Ok(())` when the
    /// required fields are present and the time range (if
    /// any) is well-formed.
    pub fn validate(&self) -> IngestResult<()> {
        if self.agent_id.trim().is_empty() {
            return Err(IngestError::Invalid("agent_id is required".to_string()));
        }
        if self.query.trim().is_empty() {
            return Err(IngestError::Invalid("query is required".to_string()));
        }
        if let Some(r) = self.time_range {
            if r.end < r.start {
                return Err(IngestError::TimeRange(format!(
                    "time_range end ({}) is before start ({})",
                    r.end, r.start
                )));
            }
        }
        if self.max_results == 0 {
            return Err(IngestError::Invalid(
                "max_results must be > 0".to_string(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn content_type_round_trip() {
        for ct in [
            ContentType::Conversation,
            ContentType::Document,
            ContentType::ToolResult,
            ContentType::Observation,
            ContentType::Assertion,
        ] {
            assert_eq!(ct.as_str().parse::<ContentType>().unwrap(), ct);
        }
    }

    #[test]
    fn content_type_default_tiers() {
        assert_eq!(
            ContentType::Conversation.default_source_tier(),
            engram_storage::SignalTier::Tier3Conversational
        );
        assert_eq!(
            ContentType::Document.default_source_tier(),
            engram_storage::SignalTier::Tier2Structured
        );
        assert_eq!(
            ContentType::ToolResult.default_source_tier(),
            engram_storage::SignalTier::Tier2Structured
        );
        assert_eq!(
            ContentType::Observation.default_source_tier(),
            engram_storage::SignalTier::Tier5Behavioral
        );
        assert_eq!(
            ContentType::Assertion.default_source_tier(),
            engram_storage::SignalTier::Tier1Authoritative
        );
    }

    #[test]
    fn request_validates_minimum() {
        let r = StoreRequest::new("agent-1", "hello");
        r.validate().expect("valid request");
    }

    #[test]
    fn request_rejects_empty_content() {
        let r = StoreRequest::new("agent-1", "  ");
        assert!(r.validate().is_err());
    }

    #[test]
    fn request_rejects_empty_agent() {
        let r = StoreRequest::new("", "hello");
        assert!(r.validate().is_err());
    }

    #[test]
    fn request_rejects_inverted_time_range() {
        let r = StoreRequest::new("agent-1", "hi")
            .with_valid_time(Utc::now())
            .with_valid_time_end(Utc::now() - chrono::Duration::days(1));
        assert!(r.validate().is_err());
    }

    #[test]
    fn request_rejects_out_of_range_importance() {
        let r = StoreRequest::new("agent-1", "hi").with_importance(1.5);
        assert!(r.validate().is_err());
    }

    // --- RecallRequest validation --------------------------------------

    #[test]
    fn recall_request_validates_minimum() {
        let r = RecallRequest::new("agent-1", "what happened with Sarah?");
        r.validate().expect("valid request");
    }

    #[test]
    fn recall_request_rejects_empty_agent() {
        let r = RecallRequest::new("", "hello");
        assert!(r.validate().is_err());
    }

    #[test]
    fn recall_request_rejects_empty_query() {
        let r = RecallRequest::new("agent-1", "   ");
        assert!(r.validate().is_err());
    }

    #[test]
    fn recall_request_rejects_zero_max_results() {
        let r = RecallRequest::new("agent-1", "hi").with_max_results(0);
        assert!(r.validate().is_err());
    }

    #[test]
    fn recall_request_rejects_inverted_time_range() {
        let r = RecallRequest::new("agent-1", "hi")
            .with_time_range(Utc::now(), Utc::now() - chrono::Duration::days(1));
        assert!(r.validate().is_err());
    }
}
