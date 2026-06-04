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
}
