# engram-storage

Phase 1 storage layer for [Engram](../../README.md): the
`MemoryStore` interface that the Memory Core uses to talk
to SurrealDB, plus two adapter implementations behind it
(in-process `Mem`/`SurrealKv` and HTTP `surreal` service).

## Surface

```rust
use engram_storage::{
    open, MemoryStoreConfig, MemoryStore, StoreKind,
    Episode, Entity, Concept, Preference, Procedure, Task,
    GraphFilters,
};

let config = MemoryStoreConfig::new(
    "0.1.0",            // engram_version
    "my_ns",            // namespace
    "main",             // database
    "schema/manifest.toml",
    StoreKind::Embedded { path: None },
);
let store = open(&config).await?;
```

The trait methods are listed in
[`src/store/store.rs`](src/store/store.rs). Highlights:

- `apply_migrations` / `schema_version` — the schema-ledger
  API. Idempotent; the runner records each applied migration
  in `engram_schema` and validates checksums on re-open.
- `write_episode` / `query_episodic` / `read_episode_at` —
  the bi-temporal episodic surface. The `read_episode_at`
  call returns the episode as it existed at a past
  transaction-time instant (the storage adapter falls back
  from the `VERSION d'...'` MVCC clause to an
  application-level `transaction_time` filter when the
  versioned engine reports a missing table for some
  id-literal lookups — see the long comment in
  `src/store/embedded.rs`).
- `traverse_graph` — iterative BFS over the seven graph
  edge tables declared in
  [`schema/migrations/0001_init.sql`](../../schema/migrations/0001_init.sql).
  Filters include agent scoping, relation-type
  whitelisting, and a post-filter `max_edges` cap.
- `relate_nodes` / `query_semantic` / `query_preferences`
  / `query_pending` / `query_procedures` / `resolve_entity`
  — the read paths for the other record types.

## Adapters

- `EmbeddedStore` — `Mem` (process-local) or `SurrealKv`
  (file-backed). The `Mem` backend is shared across
  namespaces within a process; tests use a unique namespace
  per case to stay isolated. Both backends run with
  `.versioned()` so bi-temporal reads work.
- `ServiceStore` — HTTP client against a `surreal` daemon.
  Carries the same migration runner and method surface as
  the embedded path.

## Bind-vs-literal strategy

SurrealDB 3.1.x has a parser quirk around `INSIDE [...]`
schema assertions and bound string parameters (the error
is the misleading "Expected object, got string" on a
typed value). The adapters work around this by:

- **Interpolating server-generated ids and
  enum-constrained fields as SQL literals** (record id
  literals, `RELATE in/out` positions, etc.).
- **Binding the rest** as query parameters
  (`$a`, `$l`, `Datetime`, `Vec<f32>`, `Vec<RecordId>`,
  `Object` for whole-record writes).

The `CONTENT $content SET <field> = '<literal>'` form
attempted in the handoff-era code is *invalid* SurrealQL
(the `SET` clause can't follow `CONTENT`), so whole-record
writes bind the entire record as a typed `Object` and
let the engine validate the inner fields. See the
"Bind-vs-literal query strategy" section of
[`src/lib.rs`](src/lib.rs) for the full rationale and the
list of exceptions to the bind-only rule.

## Tests

```bash
cargo test -p engram-storage
```

The test suite has 38 tests across four binaries:

- 9 unit tests in the `engram_storage` crate
- 5 tests in `tests/integration_embedded.rs` (issue #2
  smoke tests for the migration runner)
- 3 tests in `tests/migration_runner.rs` (issue #2
  manifest/checksum invariant tests)
- 21 tests in `tests/storage_adapter.rs` (issue #3:
  round-trip serialization, bi-temporal reads, graph
  traversal, agent scoping, k-NN smoke tests)

The Phase 1 acceptance criteria from issue #3 are all
covered by the `tests/storage_adapter.rs` suite.
