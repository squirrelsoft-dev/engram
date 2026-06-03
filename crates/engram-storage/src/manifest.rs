//! Reader for the `schema/manifest.toml` file that drives the
//! migration runner.
//!
//! Per `docs/design/schema-migrations.md` §3.1 the manifest is the
//! source of truth for "what schema version is the database
//! supposed to be." The runner compares the manifest's `version`
//! field against the highest applied migration recorded in the
//! `engram_schema` table to determine work to do.
//!
//! The manifest is intentionally a static file (not a queryable
//! record) — it answers "what should the database be at" rather than
//! "what is the database at" — per the design's §4.2 reasoning.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::error::Error;

/// Parsed contents of `schema/manifest.toml`.
#[derive(Debug, Clone, Deserialize)]
pub struct Manifest {
    /// Highest applied migration number, e.g. `1` after migration
    /// `0001_init.sql` has been applied.
    pub version: u32,

    /// Minimum Engram version that can apply this manifest. The
    /// runner refuses to start if the running Engram is older.
    #[serde(rename = "engram_version_min")]
    pub engram_version_min: String,

    /// Migrations that this manifest expects to be on disk and
    /// applied. The runner validates that the on-disk migrations
    /// match this list.
    #[serde(rename = "applied_migrations")]
    pub applied_migrations: Vec<ManifestMigration>,
}

/// A migration entry as recorded in the manifest.
#[derive(Debug, Clone, Deserialize)]
pub struct ManifestMigration {
    pub version: u32,
    pub file: String,
    #[serde(default)]
    pub description: String,
}

impl Manifest {
    /// Read and parse the manifest from a `Path`.
    ///
    /// The path is the file itself, not the directory. The
    /// `migrations/` subdirectory is resolved relative to the
    /// manifest's parent directory at apply time.
    pub fn read(path: &Path) -> Result<Self, Error> {
        let text = std::fs::read_to_string(path).map_err(|e| Error::MigrationFile {
            path: path.to_path_buf(),
            source: e,
        })?;
        let manifest: Manifest =
            toml::from_str(&text).map_err(|e| Error::Manifest(format!("parsing manifest: {e}")))?;
        Ok(manifest)
    }

    /// Directory containing the `*.sql` migration files referenced by
    /// this manifest. Resolved as `<manifest_dir>/migrations`.
    pub fn migrations_dir(&self, manifest_path: &Path) -> PathBuf {
        manifest_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join("migrations")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = r#"
version = 1
engram_version_min = "0.1.0"

[[applied_migrations]]
version = 1
file = "0001_init.sql"
description = "Initial schema."
"#;

    #[test]
    fn parses_a_minimal_manifest() {
        let m: Manifest = toml::from_str(SAMPLE).unwrap();
        assert_eq!(m.version, 1);
        assert_eq!(m.engram_version_min, "0.1.0");
        assert_eq!(m.applied_migrations.len(), 1);
        assert_eq!(m.applied_migrations[0].file, "0001_init.sql");
    }
}
