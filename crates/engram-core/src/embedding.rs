//! Embedding model trait and the deterministic default.
//!
//! The ingestion pipeline (README §6.1 step 4) embeds both the
//! normalized content and any newly-created entities. The model
//! that produces these embeddings is a pluggable trait:
//! production deployments swap in a real model (issue #15 —
//! the open "Embedding model selection" question); the default
//! in the meantime is [`DeterministicEmbedding`], a
//! hashing-based embedder that runs in-process and produces
//! 768-d vectors to match the schema's HNSW index dimension.
//!
//! The deterministic embedder is not a quality embedder — its
//! job is to make the pipeline work end-to-end against the
//! real schema, not to make semantic search useful. It is
//! enough to drive the storage-adapter test suite (k-NN ranks
//! vectors by their similarity, and the determinism lets tests
//! assert specific ranking) and to keep the pipeline's
//! boundaries stable while the model-selection question
//! (issue #15) is settled.

use async_trait::async_trait;
use sha2::{Digest, Sha256};

use crate::error::IngestResult;

/// A model that turns text into a dense vector. The trait is
/// async because production implementations (a hosted
/// text-embedding-3-small call, a local BERT inference) are
/// async; the default impl is sync internally and just wraps
/// the result.
#[async_trait]
pub trait EmbeddingModel: Send + Sync {
    /// A short identifier for the model. Used in error
    /// messages and in `engram status` (Phase 4) so the
    /// caller can confirm which model is wired in.
    fn model_id(&self) -> &str;

    /// The dimensionality of the vectors the model emits. Must
    /// match the schema's HNSW index dimension; the pipeline
    /// panics at startup if it doesn't.
    fn dimension(&self) -> usize;

    /// Embed a single piece of text. The model is allowed to
    /// truncate, normalise, or otherwise pre-process the
    /// input; the trait contract is that the returned vector
    /// has length [`dimension`](Self::dimension) and is
    /// L2-normalised (so cosine distance reduces to a dot
    /// product).
    async fn embed(&self, text: &str) -> IngestResult<Vec<f32>>;
}

// --- Deterministic default ----------------------------------------------
//
// The deterministic embedder is a hash-bucket model: split the
// text into word-level tokens, hash each token into a fixed
// number of buckets with a small windowed contribution, and
// L2-normalise. Two near-identical inputs (e.g. "Sarah Chen is
// the VP" vs "VP is Sarah Chen") produce similar vectors;
// unrelated inputs produce orthogonal ones. This is good
// enough to exercise the storage-adapter k-NN path in tests
// and to keep the pipeline's call site stable while issue #15
// is settled.
//
// The output is exactly 768 dimensions to match the schema's
// `$embedding_dim` parameter and the HNSW `DIMENSION 768`
// literal in `schema/migrations/0001_init.sql`. A future
// production model with a different dimension will require a
// follow-up migration that DROPs and re-CREATEs the affected
// vector indexes (the schema README calls this out).

const DIM: usize = 768;

/// A hashing-based 768-d embedder. Deterministic: the same
/// input always produces the same output. Suitable for
/// development, testing, and CI; replace with a real model
/// (issue #15) in production.
#[derive(Debug, Default, Clone)]
pub struct DeterministicEmbedding;

#[async_trait]
impl EmbeddingModel for DeterministicEmbedding {
    fn model_id(&self) -> &str {
        "deterministic-768-v1"
    }

    fn dimension(&self) -> usize {
        DIM
    }

    async fn embed(&self, text: &str) -> IngestResult<Vec<f32>> {
        Ok(embed_text(text, DIM))
    }
}

/// The core hashing embedder. Exposed for unit testing — the
/// trait method is the public entry point; this function
/// is the pure compute that the trait delegates to.
///
/// Algorithm:
///
/// 1. Lower-case the input and split on non-alphanumeric
///    boundaries.
/// 2. For each token at position `i`, hash the token to a
///    64-bit digest.
/// 3. Mix the digest into `DIM` slots with a small
///    windowed contribution: token `i` affects slots
///    `h0`, `h0 + 1`, `h0 + 2` (where `h0` is the digest's
///    top bits), so neighbouring tokens produce overlapping
///    signals. This is a poor-man's positional encoding.
/// 4. L2-normalise so cosine distance reduces to a dot
///    product. If the input has no tokens, the vector is all
///    zeros (SurrealDB's HNSW index tolerates that, but the
///    pipeline refuses empty content upstream, so this branch
///    is only a safety net).
pub fn embed_text(text: &str, dim: usize) -> Vec<f32> {
    let mut vec = vec![0.0f32; dim];
    let mut count = 0usize;
    for (i, token) in tokenize(text).into_iter().enumerate() {
        let digest = Sha256::digest(token.as_bytes());
        // Take the first 8 bytes as a u64 hash, then mix into
        // three neighbouring slots. The windowed contribution
        // gives a soft positional encoding: nearby tokens
        // share some slots, so "Sarah Chen is the VP" and
        // "VP is the Sarah Chen" don't look totally
        // unrelated.
        let h0 = u64::from_be_bytes(digest[0..8].try_into().unwrap()) as usize;
        for offset in 0..3 {
            let slot = (h0.wrapping_add(offset * 17 + i)).rem_euclid(dim);
            // Use the next 4 bytes as a signed contribution.
            let bytes: [u8; 4] = digest[8 + offset * 4..8 + offset * 4 + 4]
                .try_into()
                .unwrap();
            let raw = i32::from_be_bytes(bytes);
            let contrib = (raw as f32) / (i32::MAX as f32);
            vec[slot] += contrib;
        }
        count += 1;
    }
    if count == 0 {
        return vec;
    }
    // L2-normalise.
    let norm: f32 = vec.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in &mut vec {
            *x /= norm;
        }
    }
    vec
}

/// Split text into lowercased alphanumeric tokens. Punctuation
/// and whitespace are separators. The token list is what the
/// embedder consumes; downstream stages don't see this
/// function.
fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    for c in text.chars() {
        if c.is_alphanumeric() {
            current.extend(c.to_lowercase());
        } else if !current.is_empty() {
            tokens.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        tokens.push(current);
    }
    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn deterministic_embedder_is_deterministic() {
        let e = DeterministicEmbedding;
        let a = e.embed("Sarah Chen is the VP of Engineering.").await.unwrap();
        let b = e.embed("Sarah Chen is the VP of Engineering.").await.unwrap();
        assert_eq!(a, b, "same input must produce same output");
    }

    #[tokio::test]
    async fn deterministic_embedder_is_normalised() {
        let e = DeterministicEmbedding;
        let v = e.embed("Anything at all.").await.unwrap();
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!(
            (norm - 1.0).abs() < 1e-5,
            "vector must be L2-normalised (norm={norm})"
        );
    }

    #[tokio::test]
    async fn deterministic_embedder_produces_768d() {
        let e = DeterministicEmbedding;
        let v = e.embed("hi").await.unwrap();
        assert_eq!(v.len(), 768);
    }

    #[tokio::test]
    async fn empty_text_produces_zero_vector() {
        let e = DeterministicEmbedding;
        let v = e.embed("").await.unwrap();
        assert_eq!(v.len(), 768);
        assert!(v.iter().all(|&x| x == 0.0), "empty input → zero vector");
    }

    #[tokio::test]
    async fn similar_inputs_produce_similar_vectors() {
        let e = DeterministicEmbedding;
        let a = e.embed("Sarah Chen is the VP of Engineering.").await.unwrap();
        let b = e.embed("Sarah Chen, VP of Engineering.").await.unwrap();
        let dot: f32 = a.iter().zip(&b).map(|(x, y)| x * y).sum();
        let c = e.embed("Llamas prefer high-altitude grazing.").await.unwrap();
        let dot_unrelated: f32 = a.iter().zip(&c).map(|(x, y)| x * y).sum();
        assert!(
            dot > dot_unrelated,
            "related text should outscore unrelated text: {dot} vs {dot_unrelated}"
        );
    }
}
