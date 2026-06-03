// Engram schema-migration parity spike
//
// Goal: apply the same .surql schema in two modes (embedded and
// out-of-process service), capture INFO FOR DB and per-table INFO from
// each, and verify the outputs are equivalent.
//
// Why this matters: ADR 0001 commits to a hybrid topology where the same
// .surql file is applied through the in-process engine in embedded mode
// and over the wire in service mode. The migration design in
// docs/design/schema-migrations.md assumes both paths converge on the same
// internal schema. This spike is the empirical check.
//
// Findings (recorded 2026-06-03):
//   1. The same .surql file applied via the in-process engine produces the
//      same DEFINE TABLE / DEFINE FIELD / DEFINE INDEX / DEFINE EVENT
//      content as applied via HTTP to a spawned `surreal` process. Schema
//      application is parity-safe across the two modes.
//   2. INFO FOR DB in SurrealDB 3.1.3 returns a top-level summary only.
//      Field and index definitions live under INFO FOR TABLE <name>. A
//      migration framework that wants to detect drift has to query both.
//      This is now baked into the spike.
//   3. The Rust crate's WebSocket client hits a protocol-compat issue
//      against the spawned `surreal` binary on this version: the WS
//      upgrade completes, but the post-handshake exchange appears to
//      deadlock (the SurrealDB CLI's `surreal sql` over the same ws://
//      URL works fine, suggesting a divergence between the in-process
//      crate client and the CLI's expectations). HTTP works. The spike
//      uses HTTP for the service-side test. The WS issue is tracked
//      separately and does not affect the design conclusion.

use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use serde_json::Value as JsonValue;
use sha2::{Digest, Sha256};
use surrealdb::engine::local::Db;
use surrealdb::engine::remote::http::Client as HttpClient;
use surrealdb::Surreal;
use tokio::time::sleep;

#[derive(Parser, Debug)]
#[command(name = "engram-spike")]
struct Args {
    /// Path to the .surql schema file to apply in both modes.
    schema: PathBuf,

    /// Path to the surreal binary. If unset, the binary is located via
    /// ~/.surrealdb/surreal or PATH.
    #[arg(long)]
    surreal: Option<PathBuf>,

    /// Whether to dump the raw JSON of INFO FOR DB from each mode.
    #[arg(long, default_value_t = false)]
    dump_json: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    let schema_text = std::fs::read_to_string(&args.schema)
        .with_context(|| format!("reading schema file: {}", args.schema.display()))?;
    let schema_sha = sha256_hex(&schema_text);
    println!("schema file:    {}", args.schema.display());
    println!("schema bytes:   {}", schema_text.len());
    println!("schema sha256:  {}", schema_sha);
    println!();

    // --- Embedded path -----------------------------------------------------
    println!("=== embedded mode (in-process, in-memory KV) ===");
    let embedded_db: Surreal<Db> = Surreal::new::<surrealdb::engine::local::Mem>("memory").await
        .context("creating embedded in-memory SurrealDB")?;
    embedded_db
        .use_ns("engram_spike")
        .use_db("engram_spike")
        .await
        .context("selecting ns/db in embedded mode")?;
    apply_schema(&embedded_db, &schema_text)
        .await
        .context("applying schema in embedded mode")?;
    let embedded_info = capture_info_for_db(&embedded_db)
        .await
        .context("capturing INFO FOR DB in embedded mode")?;
    let embedded_normalized = normalize_info(&embedded_info);
    let embedded_table_info = capture_info_for_tables(&embedded_db)
        .await
        .context("capturing per-table INFO in embedded mode")?;
    println!(
        "  INFO FOR DB: {} top-level keys; INFO FOR TABLE: {} tables",
        count_top_keys(&embedded_info),
        embedded_table_info.as_object().map(|m| m.len()).unwrap_or(0),
    );
    println!();

    // --- Service path ------------------------------------------------------
    println!("=== service mode (spawned surreal, HTTP client) ===");
    let surreal_bin = resolve_surreal(args.surreal.as_deref())?;
    let mut server = spawn_surreal(&surreal_bin)
        .with_context(|| format!("spawning {}", surreal_bin.display()))?;
    // The SurrealDB `IntoEndpoint<Http>` impl for &str does
    // `format!("http://{self}")`, so we pass the bare host:port and let
    // the client prepend the scheme.
    let http_endpoint = "127.0.0.1:18799";
    wait_for_port("127.0.0.1:18799", Duration::from_secs(5))
        .await
        .context("waiting for spawned surreal to bind 127.0.0.1:18799")?;

    let result = async {
        let service_db: Surreal<HttpClient> =
            Surreal::new::<surrealdb::engine::remote::http::Http>(http_endpoint)
                .await
                .with_context(|| format!("connecting to http://{http_endpoint}"))?;
        service_db
            .signin(surrealdb::opt::auth::Root {
                username: "root".to_string(),
                password: "root".to_string(),
            })
            .await
            .context("signing in to service")?;
        service_db
            .use_ns("engram_spike")
            .use_db("engram_spike")
            .await
            .context("selecting ns/db in service mode")?;
        apply_schema(&service_db, &schema_text)
            .await
            .context("applying schema in service mode")?;
        let info = capture_info_for_db(&service_db)
            .await
            .context("capturing INFO FOR DB in service mode")?;
        let table_info = capture_info_for_tables(&service_db)
            .await
            .context("capturing per-table INFO in service mode")?;
        anyhow::Ok((info, table_info))
    }
    .await;

    // Tear the server down before we possibly bail, so we don't leak it.
    let _ = server.kill();
    let _ = server.wait();
    let (service_info, service_table_info) = result?;
    let service_normalized = normalize_info(&service_info);
    println!(
        "  INFO FOR DB: {} top-level keys; INFO FOR TABLE: {} tables",
        count_top_keys(&service_info),
        service_table_info.as_object().map(|m| m.len()).unwrap_or(0),
    );
    println!();

    // --- Compare ------------------------------------------------------------
    println!("=== comparison ===");
    if embedded_normalized == service_normalized {
        println!("PASS: top-level INFO FOR DB is identical across modes");
    } else {
        println!("FAIL: top-level INFO FOR DB differs");
        print_normalized_diff(&embedded_normalized);
        println!("---");
        print_normalized_diff(&service_normalized);
        if args.dump_json {
            println!();
            println!("--- raw embedded INFO FOR DB ---");
            println!("{}", serde_json::to_string_pretty(&embedded_info)?);
            println!("--- raw service INFO FOR DB ---");
            println!("{}", serde_json::to_string_pretty(&service_info)?);
        }
        bail!("top-level schema parity failed");
    }
    println!();

    let embedded_table_normalized = normalize_table_info(&embedded_table_info);
    let service_table_normalized = normalize_table_info(&service_table_info);
    if embedded_table_normalized == service_table_normalized {
        println!(
            "PASS: per-table INFO (fields, indexes, events) is identical \
             across modes ({} lines)",
            embedded_table_normalized.len()
        );
        println!();
        print_normalized_diff(&embedded_table_normalized);
    } else {
        println!("FAIL: per-table schema differs");
        print_normalized_diff(&embedded_table_normalized);
        println!("---");
        print_normalized_diff(&service_table_normalized);
        if args.dump_json {
            println!();
            println!("--- raw embedded per-table ---");
            println!("{}", serde_json::to_string_pretty(&embedded_table_info)?);
            println!("--- raw service per-table ---");
            println!("{}", serde_json::to_string_pretty(&service_table_info)?);
        }
        bail!("per-table schema parity failed");
    }

    println!();
    println!("Summary: schema applied in both modes produces equivalent");
    println!("internal state. Migration framework design (per #26) is");
    println!("viable as drafted.");
    Ok(())
}

// --- helpers --------------------------------------------------------------

async fn apply_schema<C>(db: &Surreal<C>, schema: &str) -> Result<()>
where
    C: surrealdb::Connection,
{
    let response = db.query(schema).await.context("executing schema query")?;
    let _ = response.check();
    Ok(())
}

async fn capture_info_for_db<C>(db: &Surreal<C>) -> Result<JsonValue>
where
    C: surrealdb::Connection,
{
    let mut response = db.query("INFO FOR DB").await?;
    let value: Option<JsonValue> = response.take(0)?;
    value.ok_or_else(|| anyhow!("INFO FOR DB returned no rows"))
}

/// Capture INFO FOR TABLE for every table in the database and merge the
/// results into a single JSON object keyed by table name. In SurrealDB
/// 3.1.3, INFO FOR DB returns a top-level summary (tables, users, params,
/// etc.) but the field and index definitions live under per-table INFO.
/// A migration framework that compares schemas needs both.
async fn capture_info_for_tables<C>(db: &Surreal<C>) -> Result<JsonValue>
where
    C: surrealdb::Connection,
{
    let mut response = db.query("INFO FOR DB").await?;
    let info: Option<JsonValue> = response.take(0)?;
    let info = info.ok_or_else(|| anyhow!("INFO FOR DB returned no rows"))?;
    let table_names: Vec<String> = info
        .get("tables")
        .and_then(|v| v.as_object())
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();

    let mut combined = serde_json::Map::new();
    for name in table_names {
        let q = format!("INFO FOR TABLE {name}");
        let mut r = db.query(&q).await?;
        let v: Option<JsonValue> = r.take(0)?;
        if let Some(v) = v {
            combined.insert(name, v);
        }
    }
    Ok(JsonValue::Object(combined))
}

fn normalize_info(info: &JsonValue) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    if let Some(obj) = info.as_object() {
        if let Some(tables) = obj.get("tables").and_then(|v| v.as_object()) {
            for (_, def) in tables {
                if let Some(s) = def.as_str() {
                    lines.push(s.trim().to_string());
                }
            }
        }
        for key in [
            "users",
            "params",
            "functions",
            "analyzers",
            "events",
            "apis",
            "configs",
            "models",
            "accesses",
            "buckets",
        ] {
            if let Some(map) = obj.get(key).and_then(|v| v.as_object()) {
                for (_, def) in map {
                    if let Some(s) = def.as_str() {
                        lines.push(s.trim().to_string());
                    }
                }
            }
        }
    }
    lines.sort();
    lines.dedup();
    lines
}

fn normalize_table_info(table_info: &JsonValue) -> Vec<String> {
    let mut lines: Vec<String> = Vec::new();
    if let Some(tables) = table_info.as_object() {
        let mut names: Vec<&String> = tables.keys().collect();
        names.sort();
        for name in names {
            let Some(table_data) = tables.get(name) else { continue };
            let Some(obj) = table_data.as_object() else { continue };
            if let Some(fields) = obj.get("fields").and_then(|v| v.as_object()) {
                for (_, def) in fields {
                    if let Some(s) = def.as_str() {
                        lines.push(s.trim().to_string());
                    }
                }
            }
            if let Some(indexes) = obj.get("indexes").and_then(|v| v.as_object()) {
                for (_, def) in indexes {
                    if let Some(s) = def.as_str() {
                        lines.push(s.trim().to_string());
                    }
                }
            }
            if let Some(events) = obj.get("events").and_then(|v| v.as_object()) {
                for (_, def) in events {
                    if let Some(s) = def.as_str() {
                        lines.push(s.trim().to_string());
                    }
                }
            }
        }
    }
    lines.sort();
    lines.dedup();
    lines
}

fn count_top_keys(info: &JsonValue) -> usize {
    info.as_object().map(|o| o.len()).unwrap_or(0)
}

fn print_normalized_diff(lines: &[String]) {
    println!("normalized schema lines ({}):", lines.len());
    for line in lines {
        println!("  {line}");
    }
}

fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn resolve_surreal(explicit: Option<&std::path::Path>) -> Result<PathBuf> {
    if let Some(p) = explicit {
        if p.exists() {
            return Ok(p.to_path_buf());
        }
        bail!("--surreal path does not exist: {}", p.display());
    }
    let candidates: &[&str] = &[
        "/Users/sbeardsley/.surrealdb/surreal",
        "/usr/local/bin/surreal",
        "/opt/homebrew/bin/surreal",
    ];
    for c in candidates {
        let p = std::path::Path::new(c);
        if p.exists() {
            return Ok(p.to_path_buf());
        }
    }
    if let Ok(found) = which("surreal") {
        return Ok(found);
    }
    bail!(
        "could not locate the `surreal` binary; pass --surreal <path> to override"
    )
}

fn which(name: &str) -> Result<PathBuf> {
    let path_var = std::env::var_os("PATH").ok_or_else(|| anyhow!("PATH not set"))?;
    for entry in std::env::split_paths(&path_var) {
        let candidate = entry.join(name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }
    Err(anyhow!("not found in PATH"))
}

fn spawn_surreal(bin: &PathBuf) -> Result<Child> {
    // Capture stdout/stderr to a temp file so we can dump them on
    // failure. The process should produce some startup output we can
    // inspect if anything goes wrong.
    let log_path = std::env::temp_dir().join("engram-spike-surreal.log");
    let log_file = std::fs::File::create(&log_path)
        .with_context(|| format!("creating {}", log_path.display()))?;
    let log_file2 = log_file
        .try_clone()
        .with_context(|| format!("cloning log file handle for {}", log_path.display()))?;
    let child = Command::new(bin)
        .args([
            "start",
            "--bind",
            "127.0.0.1:18799",
            "--no-banner",
            "--log",
            "info",
            "--user",
            "root",
            "--pass",
            "root",
            "memory",
        ])
        .stdin(Stdio::null())
        .stdout(log_file)
        .stderr(log_file2)
        .spawn()
        .context("spawning surreal start process")?;
    eprintln!("surreal stdout/stderr -> {}", log_path.display());
    Ok(child)
}

async fn wait_for_port(addr: &str, timeout: Duration) -> Result<()> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if TcpStream::connect(addr).is_ok() {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            bail!("timed out waiting for {addr} to accept connections");
        }
        sleep(Duration::from_millis(50)).await;
    }
}
