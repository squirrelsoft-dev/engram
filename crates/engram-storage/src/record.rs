//! Record types for the Phase 1 MemoryStore interface.
//!
//! The shapes are minimal here: Phase 1 is the storage adapter
//! itself (issue #2), not the Memory Core that populates records
//! (Phase 2). Each record carries the fields that the schema in
//! `schema/migrations/0001_init.sql` declares; the operation
//! methods on [`MemoryStore`](crate::store::MemoryStore) accept and
//! return these types so the adapter is usable end-to-end before
//! the full ingestion pipeline is built.
//!
//! Bi-temporal semantics follow `docs/design/schema-migrations.md`
//! §5.5: the application carries `valid_time_*` explicitly and
//! relies on SurrealDB's transaction-time versioning for the
//! system-time axis. RecordVersioned-equivalent fields
//! (transaction_time) are typed as `chrono::DateTime<Utc>` to keep
//! the boundary free of SurrealDB-specific types where possible.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use surrealdb::types::{SurrealValue, Value};

/// Source-signal tier per README §5.2.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(into = "u8", try_from = "u8")]
pub enum SignalTier {
    Tier1Authoritative,
    Tier2Structured,
    Tier3Conversational,
    Tier4Implied,
    Tier5Behavioral,
}

impl From<SignalTier> for u8 {
    fn from(t: SignalTier) -> u8 {
        match t {
            SignalTier::Tier1Authoritative => 1,
            SignalTier::Tier2Structured => 2,
            SignalTier::Tier3Conversational => 3,
            SignalTier::Tier4Implied => 4,
            SignalTier::Tier5Behavioral => 5,
        }
    }
}

impl TryFrom<u8> for SignalTier {
    type Error = String;
    fn try_from(v: u8) -> Result<Self, Self::Error> {
        match v {
            1 => Ok(SignalTier::Tier1Authoritative),
            2 => Ok(SignalTier::Tier2Structured),
            3 => Ok(SignalTier::Tier3Conversational),
            4 => Ok(SignalTier::Tier4Implied),
            5 => Ok(SignalTier::Tier5Behavioral),
            other => Err(format!("invalid signal tier: {other}")),
        }
    }
}

/// A single episodic record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Episode {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub user_id: Option<String>,
    pub content: String,
    pub content_type: String,
    pub embedding: Option<Vec<f32>>,
    pub importance: f32,
    pub entities: Option<Vec<String>>,
    pub valid_time_start: DateTime<Utc>,
    pub valid_time_end: Option<DateTime<Utc>>,
    pub transaction_time: Option<DateTime<Utc>>,
    pub consolidated: bool,
    pub consolidated_at: Option<DateTime<Utc>>,
    pub summary: Option<String>,
    pub source_tier: SignalTier,
    pub metadata: serde_json::Value,
}

/// A resolved entity record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Entity {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub canonical_name: String,
    pub aliases: Vec<String>,
    pub entity_type: String,
    pub attributes: serde_json::Value,
    pub confidence: f32,
    pub confidence_tier: SignalTier,
    pub anchor_record: Option<String>,
    pub created_at: Option<DateTime<Utc>>,
    pub last_updated: Option<DateTime<Utc>>,
    pub disambiguation_log: Option<Vec<serde_json::Value>>,
}

/// A semantic fact record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Concept {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub confidence: f32,
    pub source_tier: SignalTier,
    pub reinforcement_count: u32,
    pub last_reinforced: Option<DateTime<Utc>>,
    pub decay_rate: f32,
    pub inferred: bool,
    pub inference_chain: Option<Vec<String>>,
    pub valid_time_start: DateTime<Utc>,
    pub valid_time_end: Option<DateTime<Utc>>,
    pub transaction_time: Option<DateTime<Utc>>,
}

/// A user-preference record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Preference {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub user_id: Option<String>,
    pub category: String,
    pub content: String,
    pub direction: PreferenceDirection,
    pub strength: f32,
    pub source_tier: SignalTier,
    pub evidence_count: u32,
    pub last_reinforced: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
    pub valid_time_start: DateTime<Utc>,
    pub valid_time_end: Option<DateTime<Utc>>,
    pub transaction_time: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PreferenceDirection {
    Positive,
    Negative,
}

/// A procedure record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Procedure {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub name: String,
    pub procedure_type: String,
    pub content: String,
    pub embedding: Option<Vec<f32>>,
    pub trigger_patterns: Vec<String>,
    pub usage_count: u32,
    pub last_used: Option<DateTime<Utc>>,
    pub created_at: Option<DateTime<Utc>>,
}

/// A prospective-memory record. README §4.2.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct Task {
    pub id: Option<String>,
    pub agent_id: String,
    pub org_id: Option<String>,
    pub user_id: Option<String>,
    pub content: String,
    pub trigger_type: String,
    pub trigger_value: String,
    pub status: TaskStatus,
    pub created_at: Option<DateTime<Utc>>,
    pub triggered_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TaskStatus {
    Pending,
    Triggered,
    Completed,
    Cancelled,
}

/// A graph-traversal result row. The full graph traverser is
/// Phase 2 work; this is the basic return shape so the
/// `MemoryStore::traverse_graph` method is callable today.
#[derive(Debug, Clone, Serialize, Deserialize, SurrealValue)]
pub struct GraphResult {
    pub from: String,
    pub relation: String,
    pub to: String,
    pub attributes: serde_json::Value,
}

// --- Manual `SurrealValue` impls -------------------------------------------
//
// `SurrealValue` has a `#[derive]` form, but it doesn't play well
// with the `#[serde(into = "...", try_from = "...")]` shim on
// `SignalTier` (the macro expects plain unit variants). We
// implement the trait by hand for the three enums that need it.
// The schema is the source of truth for which integers or strings
// are valid in each column; the Rust type just round-trips.

impl SurrealValue for SignalTier {
    fn into_value(self) -> Value {
        Value::Number(surrealdb::types::Number::Int(i64::from(u8::from(self))))
    }
    fn from_value(value: Value) -> Result<Self, surrealdb::Error> {
        match value {
            Value::Number(surrealdb::types::Number::Int(n)) => u8::try_from(n)
                .ok()
                .and_then(|b| SignalTier::try_from(b).ok())
                .ok_or_else(|| surrealdb::Error::internal(format!("invalid signal tier: {n}"))),
            other => Err(surrealdb::Error::internal(format!(
                "expected int signal tier, got {other:?}"
            ))),
        }
    }
    fn is_value(value: &Value) -> bool {
        matches!(value, Value::Number(surrealdb::types::Number::Int(_)))
    }
    fn kind_of() -> surrealdb::types::Kind {
        surrealdb::types::Kind::Int
    }
}

impl SurrealValue for PreferenceDirection {
    fn into_value(self) -> Value {
        match self {
            PreferenceDirection::Positive => Value::String("positive".to_string()),
            PreferenceDirection::Negative => Value::String("negative".to_string()),
        }
    }
    fn from_value(value: Value) -> Result<Self, surrealdb::Error> {
        match value {
            Value::String(s) if s == "positive" => Ok(PreferenceDirection::Positive),
            Value::String(s) if s == "negative" => Ok(PreferenceDirection::Negative),
            other => Err(surrealdb::Error::internal(format!(
                "expected preference direction, got {other:?}"
            ))),
        }
    }
    fn is_value(value: &Value) -> bool {
        matches!(value, Value::String(_))
    }
    fn kind_of() -> surrealdb::types::Kind {
        surrealdb::types::Kind::String
    }
}

impl SurrealValue for TaskStatus {
    fn into_value(self) -> Value {
        match self {
            TaskStatus::Pending => Value::String("pending".to_string()),
            TaskStatus::Triggered => Value::String("triggered".to_string()),
            TaskStatus::Completed => Value::String("completed".to_string()),
            TaskStatus::Cancelled => Value::String("cancelled".to_string()),
        }
    }
    fn from_value(value: Value) -> Result<Self, surrealdb::Error> {
        match value {
            Value::String(s) if s == "pending" => Ok(TaskStatus::Pending),
            Value::String(s) if s == "triggered" => Ok(TaskStatus::Triggered),
            Value::String(s) if s == "completed" => Ok(TaskStatus::Completed),
            Value::String(s) if s == "cancelled" => Ok(TaskStatus::Cancelled),
            other => Err(surrealdb::Error::internal(format!(
                "expected task status, got {other:?}"
            ))),
        }
    }
    fn is_value(value: &Value) -> bool {
        matches!(value, Value::String(_))
    }
    fn kind_of() -> surrealdb::types::Kind {
        surrealdb::types::Kind::String
    }
}
