//! End-to-end integration tests for the embedded adapter.
//!
//! These tests are the smoke tests for issue #2 itself — they
//! confirm the migration runner applies the canonical schema
//! from `schema/migrations/0001_init.sql`, records the apply in
//! `engram_schema`, and that a second run is a no-op.
//!
//! The full storage test suite (issue #3) will reuse these
//! patterns. For Phase 1 the goal is "the runner works against
//! the real schema in a fresh in-memory database," not full
//! coverage of every record type.

use std::path::PathBuf;

use engram_storage::{open, MemoryStoreConfig, StoreKind};

/// Path to the canonical schema manifest. The test is run from
/// the crate root (`crates/engram-storage`), so the manifest is
/// two directories up.
fn manifest_path() -> PathBuf {
    let mut p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    p.pop(); // crates/
    p.pop(); // repo root
    p.push("schema");
    p.push("manifest.toml");
    p
}

/// The schema version the canonical manifest declares after
/// the 0001-0003 migrations that ship with Phase 1 land.
/// Bump this literal alongside any new migration.
const EXPECTED_SCHEMA_VERSION: u32 = 3;

#[tokio::test]
async fn embedded_migration_runner_applies_canonical_schema() {
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );

    let store = open(&config).await.expect("opening embedded store");
    let version = store.schema_version().await.expect("schema version");
    assert_eq!(
        version, EXPECTED_SCHEMA_VERSION,
        "after first run, schema_version should equal the manifest version"
    );
}

#[tokio::test]
async fn embedded_migration_runner_is_idempotent() {
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );

    // First open applies the migration.
    let store = open(&config).await.expect("first open");
    let v1 = store.schema_version().await.expect("v1");
    assert_eq!(v1, EXPECTED_SCHEMA_VERSION);

    // Drop the store and re-open. The migration runner should
    // see the existing ledger row and skip.
    drop(store);

    let store = open(&config).await.expect("second open");
    let v2 = store.schema_version().await.expect("v2");
    assert_eq!(v2, EXPECTED_SCHEMA_VERSION);

    // Explicit re-apply should be a no-op: the migration is
    // already in the ledger, so it appears in `skipped` and
    // nothing is added to `applied`.
    let result = store.apply_migrations().await.expect("re-apply");
    assert_eq!(result.current_version, EXPECTED_SCHEMA_VERSION);
    assert!(
        result.applied.is_empty(),
        "no new migrations should be applied on a no-op run"
    );
    let expected_skipped: Vec<u32> =
        (1..=EXPECTED_SCHEMA_VERSION).collect();
    let skipped: std::collections::HashSet<u32> =
        result.skipped.iter().copied().collect();
    for v in &expected_skipped {
        assert!(
            skipped.contains(v),
            "migration {v} should be in the skipped list"
        );
    }
}

#[tokio::test]
async fn embedded_ping_succeeds_after_migration() {
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );
    let store = open(&config).await.expect("open");
    store
        .ping()
        .await
        .expect("ping should succeed against a migrated DB");
}

#[tokio::test]
async fn clear_data_does_not_drop_ledger() {
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path(),
        StoreKind::Embedded { path: None },
    );
    let store = open(&config).await.expect("open");
    store.clear_data().await.expect("clear_data");
    // The schema_version call should still work because the
    // ledger is preserved across `clear_data`.
    let v = store.schema_version().await.expect("schema_version after clear");
    assert_eq!(v, EXPECTED_SCHEMA_VERSION);
}

#[tokio::test]
async fn rejects_manifest_and_disk_mismatch() {
    // Synthesise a manifest that declares a migration file that
    // isn't on disk, and expect the runner to refuse.
    let dir = tempfile::tempdir().expect("tempdir");
    let manifest_text = r#"
version = 1
engram_version_min = "0.1.0"

[[applied_migrations]]
version = 1
file = "9999_nonexistent.sql"
description = "This file is intentionally missing."
"#;
    let manifest_path = dir.path().join("manifest.toml");
    std::fs::write(&manifest_path, manifest_text).expect("write manifest");
    // No migrations dir created on purpose.
    let unique = format!("engram_test_{}", uuid::Uuid::new_v4().simple());
    let config = MemoryStoreConfig::new(
        "0.1.0-test",
        unique,
        "main",
        manifest_path,
        StoreKind::Embedded { path: None },
    );
    let result = open(&config).await;
    let err = result.err().expect("expected the runner to refuse the inconsistent manifest");
    let msg = format!("{err:?}");
    // Either a manifest error (migrations dir missing) or an
    // invariant error (declared file not on disk) is acceptable
    // — both reject the inconsistent state.
    assert!(
        msg.contains("manifest") || msg.contains("invariant") || msg.contains("directory"),
        "expected a manifest/invariant error, got: {msg}"
    );
}
