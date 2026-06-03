# Engram Schema-Migration Parity Spike

## What this is

A small Rust program that applies the same `.surql` schema in two ways:

1. **Embedded mode** — using the SurrealDB Rust crate linked into the
   process, in-memory KV engine.
2. **Service mode** — spawning a `surreal` binary and applying the same
   schema over HTTP via the same crate's client.

It captures `INFO FOR DB` and per-table `INFO FOR TABLE` from each, and
compares them after sorting/dedup. If they match, the schema is
"parity-safe" — applied in either mode, SurrealDB ends up in the same
internal state.

## Why this exists

The migration design in `docs/design/schema-migrations.md` (issue #26)
assumes that the same `.surql` file produces the same DB state whether
applied in embedded or service mode. This spike is the empirical check
that the assumption holds with the actual SurrealDB 3.1.3 stack.

## How to run

```sh
# Install the matching surreal binary (3.1.3 is what the spike targets)
curl -sSf https://install.surrealdb.com | sh

# Build and run against the Engram initial schema
cargo run -- ../../schema/migrations/0001_init.sql

# Or against any other .surql file
cargo run -- path/to/schema.sql
```

The schema file path is the only required argument. `--surreal <path>`
overrides the binary lookup. `--dump-json` prints the raw `INFO FOR DB`
output on mismatch.

## What it produces

When the schemas match, you get a PASS for both:

- Top-level `INFO FOR DB` (tables, users, params, ...)
- Per-table `INFO FOR TABLE` (fields, indexes, events)

When they differ, it prints the normalized line lists side by side
and exits non-zero.

## Findings (2026-06-03)

1. **Parity confirmed.** The same `.surql` applied in both modes
   produces identical normalized output for the Engram initial schema
   in `schema/migrations/0001_init.sql` (123 DEFINE statements, 14
   tables, 14 indexes, 3 vector indexes).

2. **`INFO FOR DB` is a top-level summary only.** In SurrealDB 3.1.3,
   `INFO FOR DB` returns an object like
   `{ tables, users, params, analyzers, ... }` where each top-level
   entry is a map from name to a `DEFINE ...` string. Field and index
   definitions for each table are not in that top-level output; they
   live under `INFO FOR TABLE <name>`. A migration framework that wants
   to detect drift has to query both. This is now baked into the spike.

3. **`INFO FOR TABLE` returns a structured object** with `events`,
   `fields`, `indexes`, `lives`, and `tables` keys, each a map from
   name to `DEFINE ...` string. Useful for diffing.

4. **WebSocket transport has a protocol-compat issue in this version.**
   The SurrealDB Rust crate's WebSocket client deadlocks against the
   spawned `surreal` binary on 3.1.3 (the WS upgrade completes but the
   post-handshake protocol exchange doesn't progress; the
   `surreal sql` CLI over the same URL works fine, suggesting a
   divergence between the in-process crate client and the CLI).
   HTTP works. The spike uses HTTP for the service-side test. This
   is a separate concern from the design conclusion.

5. **The `IntoEndpoint<Http>` impl for `&str` is unusual.** It does
   `format!("http://{self}")`, so you pass the bare `host:port` and
   the client prepends the scheme. Passing `http://host:port` produces
   a malformed URL (`http://http://host:port`).

## SurrealDB 3.1.3 idioms worth noting for the design

- `DEFINE FIELD ... ASSERT $value INSIDE [...]` is the canonical way to
  constrain a field to an enum set, vs. the `IN [...]` form I used in
  the initial schema. Both work; `INSIDE` is what SurrealDB emits
  when it round-trips.

- `SCHEMAFULL` makes SurrealDB reject any field not declared on the
  table. Useful for catching drift between the application and the
  schema. The 3.0 release notes mention a new behavior where
  `SCHEMAFULL` + extra undeclared fields returns an error rather
  than silently filtering.

- `FLEXIBLE TYPE object` (or `TYPE object FLEXIBLE` — both parse)
  declares a flexible key-value store on a `SCHEMAFULL` table. The
  field cannot be `NONE`, so a `DEFAULT {}` is required to make the
  field optional-from-the-caller's-perspective while still accepting
  any nested object shape.

- `HNSW DIMENSION ...` requires a literal integer at define time; the
  dimension cannot be parameterised with `DEFINE PARAM $dim`. The
  schema keeps `$embedding_dim` as a runtime-readable parameter and
  hardcodes the same value as a literal in the three vector indexes.
  Changing the embedding model requires a follow-up migration that
  drops and recreates the affected indexes.

- The `engram_schema` table itself needs to be declared in the same
  migration that uses it. The schema bootstrap (migration 0001) must
  include this table's `DEFINE TABLE` and `DEFINE FIELD` statements
  before any other migration can record its application.
