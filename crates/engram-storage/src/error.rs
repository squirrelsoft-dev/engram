//! Error types for the Engram storage crate.
//!
//! The crate is the public face of Phase 1 issue #2 (MemoryStore +
//! adapter). Errors are split into:
//!
//! - [`Error`]: the public error type returned by every `MemoryStore`
//!   method and by the migration runner.
//! - [`MigrationError`]: a richer shape carrying the migration version
//!   and statement index, per `docs/design/schema-migrations.md` §5.2.

use std::path::PathBuf;

/// The public error type for the Engram storage crate.
///
/// Variants are deliberately coarse: storage backends can be
/// substituted and the caller-facing message should not leak
/// SurrealDB-internal details that the alternative implementation
/// would not produce.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The schema manifest could not be read, parsed, or is
    /// inconsistent with the on-disk migrations directory.
    #[error("manifest error: {0}")]
    Manifest(String),

    /// A migration file could not be read.
    #[error("migration file {path}: {source}")]
    MigrationFile {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    /// The migration runner was given an inconsistent set of inputs:
    /// duplicate versions, gaps in the version sequence, or a
    /// checksum mismatch on an already-applied migration.
    #[error("migration invariant violated: {0}")]
    MigrationInvariant(String),

    /// A migration that should have been a no-op recorded itself in
    /// the `engram_schema` table with a different checksum than the
    /// file on disk. Per `docs/design/schema-migrations.md` §4.3 this
    /// is a hard error.
    #[error(
        "checksum mismatch on already-applied migration {version} \
         ({file}): stored={stored}, file={file_checksum}"
    )]
    ChecksumMismatch {
        version: u32,
        file: String,
        stored: String,
        file_checksum: String,
    },

    /// A migration was applied, but the database's `engram_schema`
    /// ledger shows a schema version newer than the running Engram
    /// supports. Per `docs/design/schema-migrations.md` §7.
    #[error(
        "database schema version {db} is newer than supported version {supported}"
    )]
    SchemaTooNew { db: u32, supported: u32 },

    /// A `SurrealDB` operation failed at runtime. The cause is
    /// preserved for diagnostics.
    #[error("surreal error: {0}")]
    Surreal(String),

    /// An `engram_schema` record was missing a field, or the
    /// migration ledger was in an unexpected shape.
    #[error("schema ledger is malformed: {0}")]
    LedgerMalformed(String),

    /// Catch-all for I/O and configuration issues that bubble up
    /// from the adapter layer.
    #[error("storage i/o: {0}")]
    Io(#[from] std::io::Error),

    /// Catch-all for unexpected backend behaviour that the migration
    /// runner's invariants do not cover.
    #[error("storage error: {0}")]
    Other(String),
}

impl From<surrealdb::Error> for Error {
    fn from(value: surrealdb::Error) -> Self {
        Error::Surreal(value.to_string())
    }
}

/// A migration-specific error that carries the version and the
/// statement index that produced it, per
/// `docs/design/schema-migrations.md` §5.2: "errors with migration
/// version and statement index."
#[derive(Debug, thiserror::Error)]
#[error("migration {version} ({file}) failed at statement {statement_index}: {source}")]
pub struct MigrationError {
    pub version: u32,
    pub file: String,
    pub statement_index: usize,
    #[source]
    pub source: Error,
}

impl MigrationError {
    pub fn new(
        version: u32,
        file: impl Into<String>,
        statement_index: usize,
        source: Error,
    ) -> Self {
        MigrationError {
            version,
            file: file.into(),
            statement_index,
            source,
        }
    }
}

/// The shape returned by `MemoryStore::apply_migrations`, per
/// `docs/design/schema-migrations.md` §5.2: "applied count, skipped
/// count, errors with migration version and statement index."
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MigrationResult {
    /// Migrations newly applied during this run.
    pub applied: Vec<AppliedMigration>,
    /// Migrations whose record in `engram_schema` matched the file on
    /// disk and were skipped.
    pub skipped: Vec<u32>,
    /// The schema version the database is at after this run. Equals
    /// the highest applied migration number.
    pub current_version: u32,
    /// Non-fatal warnings the runner wants to surface (destructive
    /// operation, schema drift on a benign table, etc.). Per
    /// `docs/design/schema-migrations.md` §5.4 destructive
    /// operations are flagged with a header comment; the runner
    /// surfaces the warning here without aborting.
    pub warnings: Vec<MigrationWarning>,
}

/// A migration that was newly applied during this run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppliedMigration {
    pub version: u32,
    pub file: String,
    pub checksum: String,
    /// Direction recorded in the `engram_schema` ledger; today
    /// always "up" because the runner is forward-only per
    /// `docs/design/schema-migrations.md` §5.6.
    pub direction: MigrationDirection,
}

/// Direction recorded in the `engram_schema` table per
/// `docs/design/schema-migrations.md` §4.1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationDirection {
    Up,
    Down,
}

impl MigrationDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            MigrationDirection::Up => "up",
            MigrationDirection::Down => "down",
        }
    }
}

impl std::str::FromStr for MigrationDirection {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "up" => Ok(MigrationDirection::Up),
            "down" => Ok(MigrationDirection::Down),
            other => Err(Error::LedgerMalformed(format!(
                "unknown migration direction: {other}"
            ))),
        }
    }
}

/// A non-fatal warning surfaced by the migration runner. The runner
/// never aborts on a warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationWarning {
    pub version: u32,
    pub file: String,
    pub message: String,
}

impl std::fmt::Display for MigrationWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "migration {} ({}): {}", self.version, self.file, self.message)
    }
}

/// Convenience: convert a `MigrationError` into the runner's flat
/// error type, preserving the version context in the message.
impl From<MigrationError> for Error {
    fn from(value: MigrationError) -> Self {
        Error::Other(value.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn direction_round_trip() {
        assert_eq!(MigrationDirection::Up.as_str(), "up");
        assert_eq!(MigrationDirection::Down.as_str(), "down");
        assert_eq!(
            "up".parse::<MigrationDirection>().unwrap(),
            MigrationDirection::Up
        );
        assert_eq!(
            "down".parse::<MigrationDirection>().unwrap(),
            MigrationDirection::Down
        );
    }

    #[test]
    fn direction_rejects_unknown() {
        assert!("sideways".parse::<MigrationDirection>().is_err());
    }
}
