//! `engram` — Phase 1 entry point.
//!
//! This binary exists to make the Phase 1 storage adapter runnable
//! from the command line. The full CLI surface (subcommands per
//! operation, like `engram store` / `engram recall` / `engram
//! status`) is Phase 6 work (#13). Phase 1 only exposes a few
//! startup-time subcommands:
//!
//! - `engram start` — open the configured store, apply
//!   migrations, then wait for SIGINT. This is what the REST API
//!   (#11) and MCP server (#12) will eventually wrap.
//! - `engram migrate` — apply migrations and exit. Used in
//!   pre-deployment checks and CI.
//! - `engram status` — open the store, run a `ping`, and print
//!   the current schema version.
//!
//! The mode (embedded vs service) is selected from environment
//! variables per ADR 0001:
//!
//! - `ENGRAM_SURREAL_URL` (preferred)
//! - `ENGRAM_EMBEDDED_PATH` (file-backed embedded)
//! - (unset) — in-memory embedded
//!
//! All paths and the manifest location can be overridden on the
//! command line; the env vars are the canonical selectors for
//! production use.

use std::path::PathBuf;
use std::process::ExitCode;

use anyhow::{anyhow, Context, Result};
use clap::{Parser, Subcommand};
use engram_storage::{
    open, MemoryStoreConfig, StoreKind,
};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(
    name = "engram",
    version,
    about = "Engram: agent memory service. Phase 1 (issue #2) entry point."
)]
struct Cli {
    /// Path to the schema manifest (`schema/manifest.toml`).
    /// Defaults to `<repo>/schema/manifest.toml` resolved from the
    /// current working directory.
    #[arg(long, env = "ENGRAM_MANIFEST_PATH", global = true)]
    manifest: Option<PathBuf>,

    /// SurrealDB namespace. Defaults to `engram`.
    #[arg(long, env = "ENGRAM_NS", global = true, default_value = "engram")]
    namespace: String,

    /// SurrealDB database name. Defaults to `main`.
    #[arg(long, env = "ENGRAM_DB", global = true, default_value = "main")]
    database: String,

    /// Engram version stamped into the `engram_schema` ledger.
    #[arg(
        long,
        env = "ENGRAM_VERSION",
        global = true,
        default_value = env!("CARGO_PKG_VERSION")
    )]
    engram_version: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Open the configured store, apply migrations, and stay
    /// running until SIGINT.
    Start,
    /// Apply migrations and exit. Used by CI and pre-deploy hooks.
    Migrate,
    /// Print the current schema version and a `ping` result.
    Status,
}

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("engram_storage=info,engram=info")),
        )
        .init();

    let cli = Cli::parse();
    let manifest_path = cli
        .manifest
        .clone()
        .unwrap_or_else(|| default_manifest_path());

    let config = build_config(
        &cli.engram_version,
        &cli.namespace,
        &cli.database,
        &manifest_path,
    );

    match run(cli.cmd, config).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            tracing::error!("{e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cmd: Cmd, config: MemoryStoreConfig) -> Result<()> {
    match cmd {
        Cmd::Start => {
            // Open + apply migrations, then park on a Ctrl-C
            // future. The store handle is held in scope to keep
            // the connection alive; dropping it would close the
            // embedded engine or release the HTTP client.
            let store = open(&config)
                .await
                .context("opening MemoryStore")?;
            store.ping().await.context("ping after start")?;
            tracing::info!(
                "engram started: namespace={}, database={}, schema_version={}",
                config.namespace,
                config.database,
                store.schema_version().await.unwrap_or(0),
            );
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("shutdown signal received");
            drop(store);
            Ok(())
        }
        Cmd::Migrate => {
            let store = open(&config).await.context("opening MemoryStore")?;
            // `open` already applied migrations; the explicit
            // re-call is idempotent and surfaces warnings to the
            // log so destructive-op messages aren't missed.
            let result = store.apply_migrations().await?;
            tracing::info!(
                "migrations complete: applied={}, skipped={}, version={}, warnings={}",
                result.applied.len(),
                result.skipped.len(),
                result.current_version,
                result.warnings.len(),
            );
            for w in &result.warnings {
                tracing::warn!("{w}");
            }
            Ok(())
        }
        Cmd::Status => {
            tracing::info!("status: opening store");
            let store = open(&config).await.context("opening MemoryStore")?;
            tracing::info!("status: store opened, pinging");
            store.ping().await.context("ping")?;
            tracing::info!("status: pinged, reading schema_version");
            let version = store.schema_version().await?;
            tracing::info!("status: schema_version = {version}");
            println!("namespace:  {}", config.namespace);
            println!("database:   {}", config.database);
            println!("schema:     v{version}");
            match config.kind() {
                StoreKind::Embedded { path } => match path {
                    Some(p) => println!("mode:       embedded (file: {})", p.display()),
                    None => println!("mode:       embedded (in-memory)"),
                },
                StoreKind::Service { url, .. } => println!("mode:       service ({url})"),
            }
            Ok(())
        }
    }
}

fn build_config(
    engram_version: &str,
    namespace: &str,
    database: &str,
    manifest_path: &std::path::Path,
) -> MemoryStoreConfig {
    MemoryStoreConfig::new(
        engram_version.to_string(),
        namespace.to_string(),
        database.to_string(),
        manifest_path.to_path_buf(),
        default_kind_from_env(),
    )
}

fn default_kind_from_env() -> StoreKind {
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
            return StoreKind::Service { url, user, pass };
        }
    }
    if let Ok(path) = std::env::var("ENGRAM_EMBEDDED_PATH") {
        if !path.is_empty() {
            return StoreKind::Embedded {
                path: Some(PathBuf::from(path)),
            };
        }
    }
    StoreKind::Embedded { path: None }
}

fn default_manifest_path() -> PathBuf {
    // Walk up from the CWD looking for a `schema/manifest.toml`.
    // This matches the repo layout Phase 1 ships with.
    let mut here = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    loop {
        let candidate = here.join("schema").join("manifest.toml");
        if candidate.exists() {
            return candidate;
        }
        if !here.pop() {
            return PathBuf::from("schema/manifest.toml");
        }
    }
}

// Used to silence the dead-code warning on `_unused_path_marker` in
// the adapter modules without `#[allow(dead_code)]` at the call
// site.
#[allow(dead_code)]
fn _unused() -> Result<()> {
    Err(anyhow!("unused"))
}
