//! Error types for the Memory Core.
//!
//! The crate's public surface (the `IngestionPipeline::ingest`
//! method and the trait methods on `EmbeddingModel` /
//! `EntityExtractor` / `ImportanceScorer`) returns
//! [`IngestError`]. Variants are coarse: the caller is the
//! `engram` entry points, which turn these into HTTP / MCP /
//! CLI error responses per the operation surface in README §7.

use std::result::Result as StdResult;

use thiserror::Error;

/// The public error type for Memory Core operations.
#[derive(Debug, Error)]
pub enum IngestError {
    /// The caller-supplied input failed validation (empty
    /// content, missing agent id, etc.). This is a 4xx-class
    /// error in the eventual REST surface.
    #[error("invalid input: {0}")]
    Invalid(String),

    /// The storage layer (`engram-storage`) returned an error.
    /// Wrapped with a context message so the caller can
    /// attribute the failure to the right pipeline stage.
    #[error("storage error during {stage}: {source}")]
    Storage {
        stage: &'static str,
        #[source]
        source: engram_storage::Error,
    },

    /// The embedding model failed to produce a vector. Wrapped
    /// with the model identifier so the caller can swap in a
    /// different implementation.
    #[error("embedding model {model} failed: {message}")]
    Embedding { model: String, message: String },

    /// The entity extractor failed (e.g. an LLM call errored).
    /// Wrapped with the extractor identifier.
    #[error("entity extractor {extractor} failed: {message}")]
    Extraction { extractor: String, message: String },

    /// The bi-temporal `valid_time` provided by the caller is
    /// out of bounds (e.g. `valid_time_end` is before
    /// `valid_time_start`).
    #[error("invalid time range: {0}")]
    TimeRange(String),

    /// A catch-all for unexpected runtime conditions that
    /// don't fit the other variants.
    #[error("pipeline error: {0}")]
    Other(String),
}

/// Convenience: `Result` alias for Memory Core operations.
pub type IngestResult<T> = StdResult<T, IngestError>;
