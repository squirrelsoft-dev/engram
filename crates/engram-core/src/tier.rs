//! Signal strength tier (re-exported from `engram-storage`).
//!
//! README §5.2 defines five tiers of evidence, from Tier 1
//! (authoritative assertion) to Tier 5 (behavioral pattern).
//! The `SignalTier` enum is the source of truth in the storage
//! layer; we re-export it here so callers of the Memory Core
//! don't need to import both crates for the most common type.

pub use engram_storage::SignalTier;
