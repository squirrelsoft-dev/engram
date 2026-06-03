# Schema Migration Strategy

**Status:** Draft (proposed design for issue #26)
**Last updated:** 2026-06-03
**Blocks:** #1 (Phase 1 SurrealDB schema)
**Related:** ADR 0001 (SurrealDB deployment topology), #24 (embedded backup)

This document specifies how Engram's SurrealDB schema is defined, versioned,
and applied at startup, in both deployment modes established by ADR 0001
(embedded in-process and out-of-process `surreal` service).

## 1. Goals and constraints

### 1.1 What "schema" means here

The Engram schema consists of all `DEFINE` statements needed to materialize
the record types and graph edges from README §4.2–§4.3:

- `DEFINE TABLE` declarations for Episode, Entity, Concept, Preference,
  Procedure, Task (and any future record types)
- `DEFINE FIELD` declarations for each record's typed fields
- `DEFINE INDEX` declarations for vector indexes, uniqueness, and lookup
  indexes used by §6.1 and §6.2
- `DEFINE EVENT` declarations only if a record's behavior requires
  server-side triggers (initially: none; flagged for future use)
- Versioning metadata (see §4 below)

This is what ADR 0001 §"Schema application" calls "the schema" — the same
content applied in-process by the embedded adapter and over-the-wire by the
service adapter.

### 1.2 Goals

1. **One canonical source.** The same definition text drives both
   deployment modes, with no per-mode duplication or fork.
2. **Reproducible.** A fresh database, given the Engram version, applies
   the same schema and ends in the same state, regardless of mode.
3. **Upgradable.** A populated database can be migrated forward across
   Engram versions without manual intervention, and the migration
   progress is observable.
4. **Downgradable-resistant.** A populated database is never
   automatically downgraded. Once a migration runs, the database is
   committed to that schema version (or later).
5. **Rollback-aware.** Where a migration is destructive, the design
   surfaces this — it does not silently lose data.
6. **Honest about bi-temporal limits.** Engram's design assumes
   bi-temporal versioned records (§4.4 of the README). SurrealDB's
   current system-time `VERSION` clause is *transaction-time only*;
   true bi-temporal (valid time + transaction time) is on SurrealDB's
   roadmap as an Enterprise+ feature. The design must work with
   what's available now and not pretend otherwise.

### 1.3 Non-goals (initial version)

- Zero-downtime online migrations. Phase 1 only needs startup-time
  migration; lock-free online schema change is a much larger problem.
- Cross-DB version skew tolerance. If two Engram processes talk to the
  same SurrealDB instance, they are expected to be the same Engram
  version. Service mode where multiple versions of Engram share one
  DB is **out of scope** for the initial design.
- SurrealDB version compatibility matrix. We pin a minimum SurrealDB
  version (see §6) and call it out, but do not dynamically adapt to
  older releases.

## 2. What SurrealDB gives us, and what it does not

### 2.1 Native capabilities we rely on

- **`DEFINE TABLE` / `DEFINE FIELD` / `DEFINE INDEX` / `DEFINE EVENT`.**
  Schema is built incrementally from idempotent definition statements
  that can be re-issued safely.
- **`DEFINE TABLE ... OVERWRITE`** (since v2.0). Useful for changing a
  table's kind or permissions; *destructive to the table definition*
  and does not preserve data. Used with care, see §5.4.
- **`DEFINE FIELD ... ON TABLE ... ASSERT ...`.** Constraint logic runs
  inside SurrealDB, which is good — but it constrains our migration
  options: an `ASSERT` that a new field would violate cannot be
  retroactively checked against existing rows.
- **`INFO FOR DB` / `INFO FOR TABLE`.** Since v3.0, this returns a
  reconstructable value: the output is the exact SurrealQL that
  defines each resource. Two `INFO FOR DB` snapshots can be `diff()`ed
  to produce a list of changes. **This is the foundation of our
  change detection** — we don't need to invent a diff format.
- **`SHOW CHANGES FOR TABLE ... SINCE <versionstamp>`.** With a
  `CHANGEFEED` enabled, we can read the historical mutation stream
  for a table. Useful for our own migration audit log.
- **`USE NS ... DB ...`** selects the namespace+database for subsequent
  statements. We use this to isolate Engram from other tenants of the
  same SurrealDB instance.

### 2.2 What SurrealDB does not give us

- **No first-class migration runner.** There is no `migrate up` /
  `migrate down` concept. We build it.
- **No ordered migration ledger.** SurrealDB has versionstamps for
  data mutations, but not for schema migrations as such. We add our
  own ledger.
- **No bi-temporal records (yet).** As noted in §1.2, only
  transaction-time versioning exists today. The README's bi-temporal
  design is implemented in our application code by carrying
  `valid_time_start` / `valid_time_end` fields on records and using
  SurrealDB's `VERSION` clause for the system-time axis. **This is
  workable but not native** — see §5.5 for the implications.
- **No schema diff tool we trust across modes.** `INFO FOR DB` gives
  us text-level diffs. We do not have a guaranteed-stable diff format
  across SurrealDB versions, so we treat `INFO FOR DB` as *input* to
  our own diff, not as the diff itself.

## 3. Canonical schema source

### 3.1 Choice: numbered, ordered, embedded `.surql` files

A single source of truth, versioned alongside Engram code:

```
schema/
  migrations/
    0001_init.sql              # initial Episode, Entity, Concept, ...
    0002_add_procedure.sql     # adds Procedure record type (hypothetical)
    0003_episode_index.sql     # adds a vector index
    ...
  manifest.toml                # declares current schema version, ordering
```

Each migration is a single `.surql` file containing one or more
`DEFINE` statements, with a header comment that names what it does.
The numbering is a monotonic integer. The `manifest.toml` declares
the latest version; the file system is the source of truth for
ordering and content.

### 3.2 Why not the alternatives

- **Single big `.surql` file with `IF NOT EXISTS`.** Trivial to apply
  on a fresh database, but no upgrade path. Any change is "replace
  the whole thing," which doesn't work once data exists.
- **Typed builder in the host language (Rust/Node).** Engram's
  language-neutral design (REST API, MCP, CLI, multiple SDKs) makes a
  host-language builder awkward. A builder written in Rust has to be
  re-implemented in Node; a builder that emits a portable `.surql`
  file is a second step that adds nothing. Plain `.surql` is the
  smallest thing that works across all modes.
- **ORM-style auto-migration from Rust structs.** Loses fidelity to
  SurrealDB-specific features (`DEFINE FIELD ... ASSERT` expressions,
  `TYPE RELATION FROM ... TO ... ENFORCED`, vector index parameters).
  We need the full SurrealQL surface.

### 3.3 Why `.surql` and not embedded strings in code

The files live in the repository, are diffable in code review, and can
be applied by hand against a development `surreal` instance for
debugging. They are loaded at runtime by the migration runner (see
§5), not embedded as string literals.

## 4. Schema version tracking

### 4.1 Choice: a single metadata record in SurrealDB

The applied schema version is stored as a regular record in a
dedicated table:

```surql
DEFINE TABLE engram_schema SCHEMAFULL;
DEFINE FIELD version     ON engram_schema TYPE int;
DEFINE FIELD applied_at  ON engram_schema TYPE datetime;
DEFINE FIELD engram_ver  ON engram_schema TYPE string;  -- e.g. "0.1.0"
DEFINE FIELD migration   ON engram_schema TYPE string;  -- e.g. "0003_episode_index.sql"
DEFINE FIELD checksum    ON engram_schema TYPE string;  -- SHA-256 of the migration file
DEFINE FIELD direction   ON engram_schema TYPE string
    ASSERT $value IN ['up', 'down'];
```

This table holds one record per applied migration. To get "current
version," query `SELECT version FROM engram_schema ORDER BY version DESC LIMIT 1`.

### 4.2 Why not the alternatives

- **Version as a file alongside the data (e.g. `engram.version`).**
  Works for embedded mode, but in service mode the DB lives behind a
  network boundary; the data directory may not even be visible to the
  Engram client. Storing the version *in* the database makes the
  database self-describing and consistent across modes.
- **Version as a constant in code.** Works for "what should the DB
  be" but not for "what is it now." The migration runner needs to
  compare current state against target state.
- **`CHANGEFEED` versionstamps.** Versionstamps count data mutations,
  not migrations, and a schema migration is a small number of
  mutations out of many. Not the right counter.
- **Reading the highest-numbered file in the migrations directory.**
  That's "what should the DB be" (the *target* version), not "what is
  the DB" (the *current* version). The two diverge between
  deployments and especially during a failed migration.

### 4.3 Checksum and integrity

The `checksum` field stores a SHA-256 of the migration file as it was
applied. On startup, the runner recomputes the checksum of each
already-applied migration and fails fast if any don't match the file
on disk. This catches the case where someone hand-edits a migration
file in a deployed image.

A mismatch is treated as a hard error: the embedded adapter exits the
process; the service adapter refuses to start. The remediation is
"revert the file or restore the database" — the design does not try
to be cleverer than that.

## 5. The migration runner

### 5.1 Overview

A small piece of code that lives in Memory Core (so it's available
to both adapters via the `MemoryStore` interface) runs at startup
before any other operation:

```
for migration in migrations_dir, ordered by version ascending:
    applied = engram_schema{version = migration.version}?
    if not applied:
        apply(migration)
        record engram_schema{...}
    elif applied.checksum != sha256(migration.file):
        fail("checksum mismatch on already-applied migration")
    else:
        skip
```

The same loop runs in both modes; the difference is *how* `apply()`
talks to SurrealDB.

### 5.2 The interface

The runner does not live in either adapter — it lives above them.
The `MemoryStore` interface gains one method:

```
applyMigrations(migrationsDir: Path | URL) → MigrationResult
```

Each adapter implements this by translating the same `.surql` content
into the appropriate transport:

- **Embedded adapter.** Splits the file on `;`, runs each statement
  through the in-process engine. Atomic per-statement; transaction
  boundaries within a single file are up to the file's author.
- **Service adapter.** Sends the file content as a single SurrealQL
  query over the WebSocket/HTTP connection. SurrealDB parses and
  applies the statements as one batch.

Both adapters must return the same `MigrationResult` shape: applied
count, skipped count, errors with migration version and statement
index.

### 5.3 Transaction semantics

A single `.surql` migration file runs as one SurrealDB transaction
in service mode. In embedded mode the same applies. If any statement
in the file fails, the entire file rolls back and the
`engram_schema` table is not updated — the database is exactly as
it was before the run.

This means a migration file is **all-or-nothing**. A failed
migration never leaves a partially-migrated database.

### 5.4 Destructive operations

Migrations that drop or alter existing data (drop a table, change a
field's type, narrow an `ASSERT`) must be **explicit**:

- The migration file must contain a header comment naming what data
  will be lost or transformed.
- The runner must log this at WARN level before applying.
- The runner must refuse to apply if the `--strict` flag is set
  (planned CLI flag for #13).

The initial schema (#1) and any near-term migrations are purely
additive and are not subject to the destructive-op check.

### 5.5 Bi-temporal compatibility

The README assumes bi-temporal records (§4.4), but SurrealDB's
`VERSION` clause today gives us only transaction-time. The design
handles this by:

- Using SurrealDB's `VERSION` clause for the transaction-time axis.
- Implementing the valid-time axis in our application code, by
  carrying `valid_time_start` and `valid_time_end` as ordinary
  fields on Episode, Concept, and Preference records, and writing
  queries that filter on these fields explicitly.

This is honest: we are not relying on a SurrealDB feature that does
not exist. When SurrealDB's bi-temporal support lands, our
application-level valid-time handling can be replaced with native
predicates, and the field set on each record can be revisited. Until
then, queries must include valid-time filters explicitly, and that
becomes a convention enforced in code review.

### 5.6 What the runner does *not* do

- It does not run data backfills. A migration that adds a new field
  with a default value relies on SurrealDB's `DEFAULT` clause; it
  does not iterate the table. If a backfill is needed, it's a
  separate concern handled by the consolidation engine or a one-off
  script.
- It does not coordinate between multiple SurrealDB databases. Each
  database is migrated independently.
- It does not support a separate "down" file per migration. Migrations
  are forward-only. The runner tracks direction ('up' vs 'down') in
  the metadata record to support a future rollback tool, but writing
  the down-migration is the responsibility of whoever writes the up
  migration, and the runner does not assume one exists.

## 6. SurrealDB version pinning

ADR 0001 commits us to a `surrealdb` crate/package version in
embedded mode. The same principle extends to the SurrealDB server
version in service mode. We commit, in `Cargo.toml` and the Node
package manifest, to a minimum supported version, currently
**SurrealDB 3.0** (the version that introduced `INFO FOR DB` as a
first-class value and the `USE` statement's strict mode).

The `engram_schema` table records `engram_ver` (the Engram version
that applied each migration), not the SurrealDB version. A user
running an old SurrealDB against a new Engram will get a clear error
at the first incompatible `DEFINE` statement and the migration
runner will not silently corrupt anything.

## 7. Failure modes

| Scenario | Embedded behavior | Service behavior |
| --- | --- | --- |
| Fresh database | All migrations apply, engram starts normally. | Same. |
| Database ahead of Engram (newer schema) | Process exits with a clear error: "database schema version X is newer than supported version Y." | Service adapter refuses to start; `engram start` exits non-zero. |
| Database behind Engram (older schema) | Migrations apply, engram starts. | Same. |
| Migration file edited after application | Process exits with checksum mismatch error. | Service adapter refuses to start. |
| Migration fails mid-apply | Transaction rolls back, database unchanged, engram_schema unchanged, process exits with error. | Same. |
| SurrealDB version too old | First incompatible statement fails the migration; process exits. | Same. |
| Concurrent Engram processes (service mode) | N/A. | **Out of scope** — see §1.3. If two Engram processes start against the same DB at the same time, behavior is undefined; the design does not attempt to handle it. |

## 8. Observable state

Two things must be inspectable to debug a misbehaving deployment:

1. **`status()` from §7.5 of the README** gains a `schema_version`
   field with the currently applied version, the target version, and
   whether they match. This is the public API.
2. **The `engram_schema` table** itself is queryable by anyone with
   database access. The metadata is intentionally stored in the open.

## 9. Open questions

These are flagged for the implementation, not blocking the design:

1. **Migration discovery in service mode.** The embedded adapter
   reads the migrations directory off the local filesystem. The
   service adapter connects over the network — does it ship the
   migrations with each Engram client, or fetch them from a known
   location, or expect the operator to apply them by hand before
   starting Engram? **Likely answer:** migrations ship with the
   Engram binary and are applied automatically. But this needs
   confirmation against the deployment stories in #11, #12, #13.

2. **Checksum of the full file vs. canonicalized form.** If a
   migration contains only comments that change, do we flag a
   checksum mismatch? Pragmatic answer: no — we hash the *statements*
   after stripping comments. Implementation detail, not a design
   issue.

3. **Bumping Engram version vs. bumping schema version.** Are these
   the same number, or can schema versions increment without a
   corresponding Engram release? **Likely answer:** they can
   decouple — multiple schema versions can ship in one Engram
   release. Tracking `engram_ver` per applied migration is the
   mechanism. Final call deferred to first migration that wants to
   do this.

4. **Cross-database atomicity for multi-tenant service deployments.**
   If a single `surreal` instance hosts multiple Engram databases
   (one per org or per agent), how do we migrate them all?
   **Likely answer:** each database is migrated independently on
   first connection, lazy-style. Out of scope for #1; flagged for
   multi-tenant work in §9 of the README.

5. **Bi-temporal transition.** When SurrealDB ships bi-temporal
   support, what is the migration path from "valid time as a
   field" to "valid time as a native predicate"? This will be its
   own migration, eventually. Not a Phase 1 concern.

## 10. Acceptance criteria for this design

This design is "done enough" for #1 to start when:

- The four open questions in §9 have a stated default answer (above)
  and an owner for any that need a final decision.
- The schema directory structure in §3.1 exists in the repo with at
  least a stub `0001_init.sql` and a `manifest.toml`.
- A spike confirms that `INFO FOR DB` on a database with the initial
  schema applied round-trips: applying the file's `DEFINE`
  statements and re-emitting via `INFO FOR DB` produces the same
  text. (This is the in-process vs. over-the-wire parity check
  called out in ADR 0001.)

The spike in particular is what unblocks #1. Without it we are
guessing that the dual-adapter schema application actually works
the same way in both modes.
