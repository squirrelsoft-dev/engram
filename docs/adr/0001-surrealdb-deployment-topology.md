# ADR 0001: SurrealDB Deployment Topology

## Status

Accepted

## Date

2026-06-03

## Context

The README commits to SurrealDB as Engram's sole local datastore
(§1.3, §3.3) and defines the `MemoryStore` interface (§3.3) as the only
boundary at which storage is addressed. It does not, however, specify
how SurrealDB itself is run alongside Engram — whether it is linked into
the Engram process as an embedded library, or runs as a separate local
service that Engram connects to over the network.

This decision is raised in [issue #23](../../issues/23). The choice has
consequences for every Phase 1 deliverable (schema, storage adapter,
tests) and for how the REST API (#11), MCP server (#12), and CLI (#13)
are deployed together in later phases.

### Forces

- **Embedding use case.** Users who want to embed Engram into a custom
  Rust (and eventually Node) coding-agent harness want the minimum
  moving pieces: one library, one process, no lifecycle to manage.
  Embedded is the right default for a library API.

- **Operator use case.** Users running Engram as a long-lived service
  for MCP, REST, and CLI consumers want standard SurrealDB admin
  tooling (`surreal sql`, `surreal export`, `surreal import`) to work
  while Engram is running, and want entry points to be restartable
  independently.

- **Multi-process on one host.** The REST daemon and the MCP server
  may need to run as separate processes on the same host. They must
  share a single logical database. Embedded mode makes this
  impossible without a custom IPC layer.

- **MemoryStore boundary.** §3.3 already isolates storage behind an
  interface, so the topology choice can be made at the adapter layer
  without changing Memory Core.

### Options Considered

**A. Embedded only.** Single process, simplest ops, but breaks
multi-process on one host and makes standard `surreal` admin tooling
harder to use while Engram runs.

**B. Spawned local service only.** `engram start` manages a
`surreal start` child process. Standard tooling works, multiple Engram
processes can share a DB, but adds lifecycle and port management for
embedders who don't need any of it.

**C. Hybrid — both supported behind the `MemoryStore` interface.**
Two adapter implementations, selected by mode. Maximum flexibility at
the cost of writing and testing two adapters.

## Decision

**Adopt option C — hybrid, with the following split.**

- **Embedded mode** is the path for users embedding Engram into a
  custom coding-agent harness written in **Rust** (and eventually
  **Node**). The Engram crate/package links the `surrealdb`
  crate/package directly and uses an in-memory or file-backed engine.
  No extra processes, no sockets, no lifecycle.

- **Service mode** is the path for everything else. `engram start` (or
  the equivalent launcher) manages a `surreal start` child process, or
  accepts an already-running `surreal` URL and connects to it. The
  REST API (#11), MCP server (#12), and CLI (#13) all run as clients
  of this service. Standard SurrealDB admin tooling works against the
  same instance.

The choice between modes is made at the `MemoryStore` adapter layer.
Memory Core is unaware of the topology.

### Selection mechanism

A single environment variable selects the mode and supplies connection
details. Both modes apply the schema on startup, but the mechanism
differs:

| Mode | Env var | Default | Notes |
| --- | --- | --- | --- |
| Embedded | `ENGRAM_EMBEDDED_PATH` | unset (in-memory) | A file path turns on file-backed persistence; unset means a pure in-memory DB that does not survive process exit. |
| Service | `ENGRAM_SURREAL_URL` | `http://localhost:8000` | Credentials via `ENGRAM_SURREAL_USER` and `ENGRAM_SURREAL_PASS`. |

If both are set, the service mode wins and a warning is logged. If
neither is set, Engram starts in embedded in-memory mode. This makes
the zero-config path "just works" for embedders and one env var away
for service users.

### `engram start` behavior

- If `ENGRAM_SURREAL_URL` is set, `engram start` connects to that URL
  and does not spawn a process.
- Otherwise, `engram start` spawns `surreal start` as a child process
  with the bind address, credentials, and storage path derived from
  the embedded env vars, and waits for it to be ready before
  proceeding. On Engram shutdown, the child is terminated cleanly.

### Testing

The storage test suite (#3) runs the same cases against both modes via
a parameterized fixture. CI runs both. New adapter features must add
coverage in both modes unless explicitly scoped to one.

### Version coupling (embedded mode)

For embedded mode, Engram pins a specific `surrealdb` crate/package
version and re-vendors on upgrades. SurrealDB version bumps that
include storage-format changes are documented in release notes.
Embedders get a stable, in-process contract.

## Consequences

### Positive

- Embedders get a one-library experience with no extra processes.
- Operators get standard SurrealDB tooling and independent restart of
  REST / MCP / CLI.
- The `MemoryStore` boundary from §3.3 absorbs the difference, so
  Memory Core has a single contract.
- In-memory embedded mode gives a zero-config path for development
  and tests.

### Negative

- Two adapter implementations to build, test, and keep in sync.
- Schema-application logic exists in two forms (in-process call vs.
  over-the-wire `USE NS DB` / `surreal import`).
- Version coupling in embedded mode means upgrade lag relative to
  upstream SurrealDB releases.
- Operators must understand both modes to pick the right one for
  their deployment.

### Neutral

- The README design doc does not change. The topology is an
  implementation concern, not an architectural one. §3.3 already
  establishes the interface boundary this ADR builds on.

## Open Questions

These were tracked for follow-up. Three were promoted to their own issues
and remain open; the fourth (external-service acceptance) was resolved
by the env-var path.

1. ~~**External-service acceptance.**~~ Resolved: the `ENGRAM_SURREAL_URL`
   env-var path is the connect-only mechanism; no separate
   `--connect-only` flag.
2. **Embedded backup story.** Tracked in
   [issue #24](../../issues/24). Options: `exportSnapshot()` on
   `MemoryStore`, or document the file as a standard SurrealDB data
   file.
3. **Node embedded mode.** Tracked in
   [issue #25](../../issues/25). Decision deferred until #14 is in
   scope.
4. **Schema migration path.** Tracked in
   [issue #26](../../issues/26). Promoted out because it deserves its
   own design pass before #1 lands.

## References

- README §1.3 (Goals)
- README §3.3 (Storage Adapter Interface)
- README §10 (Entry Point Specifications)
- [Issue #1: Phase 1 schema](../../issues/1)
- [Issue #2: Phase 1 MemoryStore + adapter](../../issues/2)
- [Issue #3: Phase 1 storage tests](../../issues/3)
- [Issue #11: Phase 4 REST API](../../issues/11)
- [Issue #12: Phase 5 MCP server](../../issues/12)
- [Issue #13: Phase 6 CLI](../../issues/13)
- [Issue #14: Phase 7 SDKs](../../issues/14)
- [Issue #23: Design Q8 (this decision, now closed)](../../issues/23)
- [Issue #24: Followup — embedded backup story](../../issues/24)
- [Issue #25: Followup — Node SDK embedded-mode timing](../../issues/25)
- [Issue #26: Schema migration strategy](../../issues/26)
