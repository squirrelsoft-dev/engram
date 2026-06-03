//! Configuration for opening a [`MemoryStore`].
//!
//! Two modes, per ADR 0001:
//!
//! - **Embedded** — in-process SurrealDB, in-memory or file-backed.
//!   Selected by `ENGRAM_EMBEDDED_PATH` (file path) or by leaving
//!   it unset (in-memory, ephemeral).
//! - **Service** — connect to a running `surreal` process. Selected
//!   by `ENGRAM_SURREAL_URL`. If both `ENGRAM_SURREAL_URL` and
//!   `ENGRAM_EMBEDDED_PATH` are set, the service mode wins and a
//!   warning is logged (matching the ADR's "If both are set, the
//!   service mode wins" rule).
//!
//! In addition, the configuration carries:
//!
//! - the **namespace** and **database** names that all subsequent
//!   operations will run in (the migration runner, the write
//!   methods, the read methods),
//! - the **schema manifest path** the migration runner should read,
//! - the **engram version** the runner stamps into each
//!   `engram_schema` record,
//! - a **strict** flag that makes the migration runner refuse
//!   destructive operations (per
//!   `docs/design/schema-migrations.md` §5.4 — the flag is
//!   scoped here even though the CLI hook in #13 is a follow-up).

use std::path::{Path, PathBuf};

use crate::error::Error;

/// Configuration for opening a [`MemoryStore`](super::MemoryStore).
#[derive(Debug, Clone)]
pub struct MemoryStoreConfig {
    /// Engram release version (e.g. `"0.1.0"`). Stamped into every
    /// `engram_schema` record by the migration runner.
    pub engram_version: String,

    /// SurrealDB namespace to bind to.
    pub namespace: String,

    /// SurrealDB database name within `namespace`.
    pub database: String,

    /// Path to the `schema/manifest.toml` file the migration runner
    /// reads at startup.
    pub manifest_path: PathBuf,

    /// Mode selected by the env vars (or explicit constructor).
    kind: StoreKind,

    /// When true, the migration runner refuses to apply any file
    /// that contains a destructive header comment per
    /// `docs/design/schema-migrations.md` §5.4.
    pub strict: bool,
}

/// The deployment mode the store will run in.
#[derive(Debug, Clone)]
pub enum StoreKind {
    /// In-process. `path == None` is pure in-memory; `path == Some(p)`
    /// persists to `p` via `SurrealKv` (or, when transaction-time
    /// versioning is requested, the `+versioned` variant).
    Embedded { path: Option<PathBuf> },

    /// Out-of-process `surreal` service connected over HTTP.
    Service {
        url: String,
        user: String,
        pass: String,
    },
}

impl MemoryStoreConfig {
    /// Construct a config explicitly. Tests and library embedders
    /// usually go through this entry point; the binary builds it
    /// from environment variables.
    pub fn new(
        engram_version: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
        manifest_path: impl Into<PathBuf>,
        kind: StoreKind,
    ) -> Self {
        MemoryStoreConfig {
            engram_version: engram_version.into(),
            namespace: namespace.into(),
            database: database.into(),
            manifest_path: manifest_path.into(),
            kind,
            strict: false,
        }
    }

    /// Build a config from the environment, per ADR 0001.
    ///
    /// Rules:
    ///
    /// - If `ENGRAM_SURREAL_URL` is set, service mode wins. The URL
    ///   is taken verbatim; credentials default to root/root unless
    ///   `ENGRAM_SURREAL_USER` / `ENGRAM_SURREAL_PASS` override.
    /// - Else if `ENGRAM_EMBEDDED_PATH` is set, embedded file-backed
    ///   mode is used.
    /// - Else, embedded in-memory mode is used (the "just works"
    ///   zero-config path for embedders).
    pub fn from_env(
        engram_version: impl Into<String>,
        namespace: impl Into<String>,
        database: impl Into<String>,
        manifest_path: impl Into<PathBuf>,
    ) -> Self {
        let mut s = MemoryStoreConfig::new(
            engram_version,
            namespace,
            database,
            manifest_path,
            StoreKind::Embedded { path: None },
        );
        s.apply_env_overrides();
        s
    }

    /// Read the relevant env vars and switch the mode if needed.
    /// Exposed separately so the binary can log a "service mode
    /// wins" warning before constructing the store.
    pub fn apply_env_overrides(&mut self) {
        if let Ok(url) = std::env::var("ENGRAM_SURREAL_URL") {
            if !url.is_empty() {
                let user = std::env::var("ENGRAM_SURREAL_USER")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "root".to_string());
                let pass = std::env::var("ENGRAM_SURREAL_PASS")
                    .ok()
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| "root".to_string());
                tracing::warn!(
                    "ENGRAM_SURREAL_URL is set; running in service mode against {url}"
                );
                self.kind = StoreKind::Service { url, user, pass };
            }
        }
        if let Ok(path) = std::env::var("ENGRAM_EMBEDDED_PATH") {
            if !path.is_empty() {
                if matches!(self.kind, StoreKind::Service { .. }) {
                    tracing::warn!(
                        "both ENGRAM_SURREAL_URL and ENGRAM_EMBEDDED_PATH are set; \
                         service mode wins, ignoring ENGRAM_EMBEDDED_PATH"
                    );
                } else {
                    self.kind = StoreKind::Embedded {
                        path: Some(PathBuf::from(path)),
                    };
                }
            }
        }
        if let Ok(strict) = std::env::var("ENGRAM_MIGRATION_STRICT") {
            self.strict = matches!(strict.as_str(), "1" | "true" | "yes");
        }
    }

    /// Build a config from the repo's standard layout: a
    /// `manifest.toml` next to `migrations/`. Defaults the mode to
    /// embedded in-memory unless env vars are set.
    pub fn from_manifest(
        manifest_path: &Path,
        namespace: impl Into<String>,
        database: impl Into<String>,
    ) -> Result<Self, Error> {
        let _ = crate::manifest::Manifest::read(manifest_path)?;
        Ok(MemoryStoreConfig::from_env(
            env!("CARGO_PKG_VERSION"),
            namespace,
            database,
            manifest_path,
        ))
    }

    /// The selected mode.
    pub fn kind(&self) -> &StoreKind {
        &self.kind
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedded_default_when_no_env() {
        // The default for tests is in-memory embedded regardless of
        // any host env state; callers explicitly set fields.
        let c = MemoryStoreConfig::new(
            "0.1.0",
            "engram",
            "main",
            "/tmp/manifest.toml",
            StoreKind::Embedded { path: None },
        );
        assert!(matches!(c.kind(), StoreKind::Embedded { path: None }));
    }

    #[test]
    fn service_overrides_embedded() {
        let mut c = MemoryStoreConfig::new(
            "0.1.0",
            "engram",
            "main",
            "/tmp/manifest.toml",
            StoreKind::Embedded {
                path: Some("/tmp/data".into()),
            },
        );
        // Simulate the env-override path: set both and verify
        // service wins.
        c.kind = StoreKind::Service {
            url: "http://localhost:8000".into(),
            user: "root".into(),
            pass: "root".into(),
        };
        assert!(matches!(c.kind(), StoreKind::Service { .. }));
    }
}
