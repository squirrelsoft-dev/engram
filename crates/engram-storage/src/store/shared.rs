//! Helpers shared by the embedded and service adapters.
//!
//! The two adapters are very different in how they connect to
//! SurrealDB, but the migration runner and the read/write methods
//! are identical. This module contains the shared logic:
//!
//! - `with_ns_db`: apply a closure that has a `Surreal<Db>` (or
//!   `Surreal<Http>`, or any other connection) bound to a
//!   namespace+database.
//! - The migration runner itself, parameterised on connection type.
//!
//! The trait-bound `surreal::Connection` (in `surrealdb::conn`) is
//! what makes this possible — both `Db` (embedded) and `Http`
//! (service) implement it.

use std::path::Path;

use sha2::{Digest, Sha256};
use surrealdb::types::SurrealValue;
use surrealdb::{Connection, Surreal};

use crate::error::{AppliedMigration, Error, MigrationDirection, MigrationResult, MigrationWarning};
use crate::manifest::Manifest;

/// Bind a SurrealDB connection to a namespace and database. The
/// `engram_schema` ledger lives there, so every adapter goes
/// through this at the top of every operation.
pub async fn with_ns_db<C, F, T>(db: &Surreal<C>, ns: &str, database: &str, f: F) -> Result<T, Error>
where
    C: Connection,
    F: std::future::Future<Output = Result<T, Error>>,
{
    db.use_ns(ns).use_db(database).await?;
    f.await
}

/// Compute the SHA-256 hex digest of a file's contents. Used for
/// both the per-file checksum and the integrity check in §4.3.
pub fn sha256_of_text(text: &str) -> String {
    let mut h = Sha256::new();
    h.update(text.as_bytes());
    hex::encode(h.finalize())
}

/// Return the list of migration files in a directory, sorted in the
/// order the runner should apply them: ascending by the numeric
/// prefix of the filename (e.g. `0001_init.sql`, `0002_x.sql`).
///
/// The `sort_by_key` form guarantees a total order even on
/// filesystems whose `read_dir` does not — and on inputs that have
/// not been lexically pre-sorted.
pub fn list_migration_files(dir: &Path) -> Result<Vec<(u32, std::path::PathBuf)>, Error> {
    if !dir.exists() {
        return Err(Error::Manifest(format!(
            "migrations directory does not exist: {}",
            dir.display()
        )));
    }
    let mut out: Vec<(u32, std::path::PathBuf)> = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(version_str) = stem.split('_').next() else {
            continue;
        };
        let Ok(version) = version_str.parse::<u32>() else {
            // Skip files whose leading prefix is not a u32. The
            // runner never touches them; the manifest's
            // `applied_migrations` is the authoritative list.
            continue;
        };
        out.push((version, path));
    }
    out.sort_by_key(|(v, _)| *v);
    Ok(out)
}

/// A row from the `engram_schema` table. We deserialize to a
/// concrete struct rather than `serde_json::Value` so the runner
/// fails fast on a malformed ledger.
#[derive(Debug, Clone, serde::Deserialize, SurrealValue)]
pub struct LedgerRow {
    pub version: u32,
    pub applied_at: chrono::DateTime<chrono::Utc>,
    pub engram_ver: String,
    pub migration: String,
    pub checksum: String,
    pub direction: String,
}

/// Read the current `engram_schema` ledger. Used by the migration
/// runner to decide what's already applied and to validate
/// checksums.
///
/// If the `engram_schema` table does not exist yet (i.e. this is
/// the very first migration run, and the table is itself defined
/// by the first migration), this returns an empty vector. The
/// "table doesn't exist" error from SurrealDB is recognised by
/// string-matching the message; the alternative is to use
/// `INFO FOR DB` to ask whether the table is declared, but
/// `INFO FOR DB` is a top-level summary that does not surface
/// that detail for tables that have not yet been defined.
pub async fn read_ledger<C>(db: &Surreal<C>) -> Result<Vec<LedgerRow>, Error>
where
    C: Connection,
{
    tracing::debug!("read_ledger: querying engram_schema");
    let response = db.query("SELECT * FROM engram_schema ORDER BY version ASC").await?;
    // `check()` consumes the response. The first error wins; we
    // only continue if there is none.
    let mut response = match response.check() {
        Ok(r) => r,
        Err(e) => {
            let msg = e.to_string();
            tracing::warn!("read_ledger: engram_schema query errored: {msg}");
            if msg.contains("does not exist") {
                // Treat missing table as "no ledger yet". This is
                // the chicken-and-egg case for the very first
                // migration, which is itself the file that creates
                // the `engram_schema` table.
                return Ok(Vec::new());
            }
            return Err(Error::Surreal(msg));
        }
    };
    let rows: Vec<LedgerRow> = response.take(0)?;
    Ok(rows)
}

/// Run the migration runner against any Surreal connection. Both
/// the embedded and service adapters call this; the only
/// difference between the two adapters is how they construct
/// `db`.
///
/// `strict == true` makes the runner refuse to apply a file whose
/// header comment includes the `BREAKING:` marker (per
/// `docs/design/schema-migrations.md` §5.4). Destructive
/// operations remain in `warnings` either way; the `strict` flag
/// is the gate that turns them into errors.
pub async fn run_migrations<C>(
    db: &Surreal<C>,
    manifest: &Manifest,
    migrations_dir: &Path,
    engram_version: &str,
    strict: bool,
) -> Result<MigrationResult, Error>
where
    C: Connection,
{
    tracing::info!(
        "migration runner: scanning {} for new migrations",
        migrations_dir.display()
    );
    // 1. Read the on-disk migrations.
    let on_disk = list_migration_files(migrations_dir)?;

    // 2. Cross-check the manifest's expectations.
    let mut by_version: std::collections::BTreeMap<u32, &crate::manifest::ManifestMigration> =
        std::collections::BTreeMap::new();
    for m in &manifest.applied_migrations {
        by_version.insert(m.version, m);
    }
    for (v, _) in &on_disk {
        if !by_version.contains_key(v) {
            return Err(Error::MigrationInvariant(format!(
                "migration file version {v} is on disk but not declared in the manifest"
            )));
        }
    }
    for (v, _) in &by_version {
        if !on_disk.iter().any(|(disk_v, _)| disk_v == v) {
            return Err(Error::MigrationInvariant(format!(
                "manifest declares migration {v} but the file is missing from disk"
            )));
        }
    }

    // 3. Read the ledger.
    let ledger = read_ledger(db).await?;
    tracing::debug!(
        "migration runner: read {} ledger rows from engram_schema",
        ledger.len()
    );
    let mut by_ledger_version: std::collections::HashMap<u32, LedgerRow> =
        ledger.iter().map(|r| (r.version, r.clone())).collect();

    // 4. Reject a database that is ahead of the running Engram.
    if let Some(&max_db) = by_ledger_version.keys().max() {
        if max_db > manifest.version {
            return Err(Error::SchemaTooNew {
                db: max_db,
                supported: manifest.version,
            });
        }
    }

    // 5. Verify checksums of already-applied migrations.
    for row in &ledger {
        let path = migrations_dir.join(&row.migration);
        let text = std::fs::read_to_string(&path).map_err(|e| Error::MigrationFile {
            path: path.clone(),
            source: e,
        })?;
        let file_checksum = sha256_of_text(&text);
        if file_checksum != row.checksum {
            return Err(Error::ChecksumMismatch {
                version: row.version,
                file: row.migration.clone(),
                stored: row.checksum.clone(),
                file_checksum,
            });
        }
    }

    // 6. Apply pending migrations in order.
    let mut applied: Vec<AppliedMigration> = Vec::new();
    let mut skipped: Vec<u32> = Vec::new();
    let mut warnings: Vec<MigrationWarning> = Vec::new();

    for (version, path) in &on_disk {
        if by_ledger_version.contains_key(version) {
            skipped.push(*version);
            continue;
        }
        let text = std::fs::read_to_string(path).map_err(|e| Error::MigrationFile {
            path: path.clone(),
            source: e,
        })?;
        let checksum = sha256_of_text(&text);

        // Destructive-op header check (§5.4).
        let is_destructive = contains_breaking_marker(&text);
        if is_destructive {
            let message = format!(
                "migration file contains a BREAKING: header and may drop or transform data"
            );
            if strict {
                return Err(Error::MigrationInvariant(format!(
                    "strict mode refuses destructive migration {version} ({path}): {message}",
                    path = path.display()
                )));
            }
            warnings.push(MigrationWarning {
                version: *version,
                file: path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string(),
                message,
            });
        }

        // Apply the file as a single SurrealQL batch. The file is
        // a single transaction in both modes per §5.3.
        tracing::info!("applying migration {version} from {}", path.display());
        let response = db.query(&text).await?;
        tracing::info!("migration {version} query returned, calling check()");
        response.check().map_err(|e| {
            tracing::error!("migration {version} failed during apply: {e}");
            Error::Surreal(e.to_string())
        })?;
        tracing::info!("migration {version} applied and checked");

        // Record the apply. We use the Engram version from the
        // config; `applied_at` is `time::now()` server-side.
        tracing::info!("recording engram_schema row for migration {version}");
        let record_sql = format!(
            "CREATE engram_schema SET \
                version = {version}, \
                engram_ver = $engram_ver, \
                migration = $migration, \
                checksum = $checksum, \
                direction = 'up', \
                applied_at = time::now()",
        );
        let r = db
            .query(record_sql)
            .bind(("engram_ver", engram_version.to_string()))
            .bind(("migration", path.file_name().unwrap().to_string_lossy().into_owned()))
            .bind(("checksum", checksum.clone()))
            .await?;
        tracing::info!("engram_schema row query returned, calling check()");
        r.check().map_err(|e| {
            tracing::error!("engram_schema row failed: {e}");
            Error::Surreal(e.to_string())
        })?;
        tracing::info!("engram_schema row recorded");

        applied.push(AppliedMigration {
            version: *version,
            file: path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("?")
                .to_string(),
            checksum,
            direction: MigrationDirection::Up,
        });
        by_ledger_version.insert(
            *version,
            LedgerRow {
                version: *version,
                applied_at: chrono::Utc::now(),
                engram_ver: engram_version.to_string(),
                migration: path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .unwrap_or("?")
                    .to_string(),
                checksum: String::new(),
                direction: "up".to_string(),
            },
        );
    }

    Ok(MigrationResult {
        current_version: by_ledger_version.keys().max().copied().unwrap_or(0),
        applied,
        skipped,
        warnings,
    })
}

/// Return true if the file's header comments include a
/// `BREAKING:` marker, used as the destructive-operation signal
/// per `docs/design/schema-migrations.md` §5.4.
fn contains_breaking_marker(text: &str) -> bool {
    for line in text.lines() {
        let trimmed = line.trim_start();
        if !trimmed.starts_with("--") {
            // Header comment block ends at the first non-comment line.
            if trimmed.is_empty() {
                continue;
            }
            return false;
        }
        if trimmed.to_ascii_uppercase().contains("BREAKING:") {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn temp_dir_with(files: &[(&str, &str)]) -> tempfile::TempDir {
        let d = tempfile::tempdir().unwrap();
        for (name, contents) in files {
            let p = d.path().join(name);
            let mut f = std::fs::File::create(&p).unwrap();
            f.write_all(contents.as_bytes()).unwrap();
        }
        d
    }

    #[test]
    fn sha256_is_stable() {
        let h1 = sha256_of_text("hello");
        let h2 = sha256_of_text("hello");
        assert_eq!(h1, h2);
        assert!(sha256_of_text("a") != sha256_of_text("b"));
    }

    #[test]
    fn list_sorts_by_numeric_prefix() {
        let d = temp_dir_with(&[
            ("0002_b.sql", ""),
            ("0001_a.sql", ""),
            ("not_a_migration.txt", "x"),
            ("README", "x"),
        ]);
        let listed = list_migration_files(d.path()).unwrap();
        let versions: Vec<u32> = listed.iter().map(|(v, _)| *v).collect();
        assert_eq!(versions, vec![1, 2]);
    }

    #[test]
    fn destructive_marker_detection() {
        assert!(contains_breaking_marker(
            "-- BREAKING: drops the obsolete table\nDEFINE TABLE x;"
        ));
        assert!(contains_breaking_marker(
            "-- Some header\n-- breaking: this is destructive\nDEFINE TABLE x;"
        ));
        assert!(!contains_breaking_marker(
            "-- Some header\nDEFINE TABLE x;"
        ));
    }

    #[test]
    fn ledger_row_direction_parses() {
        let row = LedgerRow {
            version: 1,
            applied_at: chrono::Utc::now(),
            engram_ver: "0.1.0".to_string(),
            migration: "0001_init.sql".to_string(),
            checksum: "x".to_string(),
            direction: "up".to_string(),
        };
        let parsed: MigrationDirection = row.direction.parse().unwrap();
        assert_eq!(parsed, MigrationDirection::Up);
    }
}
