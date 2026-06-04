//! Engram Memory Core: the ingestion pipeline, retrieval
//! orchestrator, and supporting types for Phase 2.
//!
//! The crate is the application layer that sits on top of
//! `engram-storage` (the SurrealDB-backed `MemoryStore`). The
//! entry points (REST, MCP, CLI, SDK) wire to it; it owns the
//! business logic for the five operations in README §7.
//!
//! The pipeline is built on three pluggable traits:
//!
//! - [`embedding::EmbeddingModel`] — turns text into a vector.
//!   The default [`embedding::DeterministicEmbedding`] is a
//!   hashing-based embedder that works without external services
//!   and produces 768-d vectors to match the schema's HNSW
//!   indexes. Production deployments swap in a real model
//!   (issue #15) by implementing the trait.
//!
//! - [`extraction::EntityExtractor`] — pulls named entities
//!   and implicit references from normalized text. The default
//!   [`extraction::HeuristicEntityExtractor`] uses a
//!   rule-based approach (capitalisation, role-prefix
//!   patterns, dictionary lookup) that runs in-process. The
//!   production path (issue #16) is an LLM call; the trait
//!   surface is identical.
//!
//! - [`pipeline::ImportanceScorer`] — combines source tier,
//!   entity count, explicit priority signals, and recency into
//!   a 0.0–1.0 score. The default [`pipeline::TierBasedImportanceScorer`]
//!   implements the formula in README §6.1 step 5.
//!
//! The ingestion pipeline itself lives in [`pipeline::IngestionPipeline`]
//! and corresponds to README §6.1 step by step. The retrieval
//! orchestrator (README §6.2) is Phase 2 issue #5 and will land
//! in the same module tree.
//!
//! The output of a successful `store()` call is a
//! [`pipeline::IngestionResult`] wrapping the persisted
//! [`engram_storage::Episode`] and any linked entities.
//! Higher layers turn this into the [`EpisodeRecord`] shape
//! documented in README §7.1.

#![deny(rust_2018_idioms)]
#![warn(missing_debug_implementations)]

pub mod embedding;
pub mod error;
pub mod extraction;
pub mod pipeline;
pub mod request;
pub mod response;
pub mod tier;

pub use error::{IngestError, IngestResult};
pub use pipeline::{IngestionPipeline, IngestionPipelineBuilder, IngestionResult};
pub use request::{ContentType, StoreRequest};
pub use response::EpisodeRecord;
pub use tier::SignalTier;
