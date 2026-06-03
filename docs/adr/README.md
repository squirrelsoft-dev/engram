# Architecture Decision Records

This directory contains ADRs for Engram. Each ADR documents a significant
architectural decision, its context, and its consequences.

ADRs are immutable once accepted. Superseding decisions go in a new ADR that
links back to the one it replaces.

## Index

| Number | Title | Status |
| --- | --- | --- |
| [0001](0001-surrealdb-deployment-topology.md) | SurrealDB deployment topology (embedded vs. local service) | Accepted |

## Conventions

- Filenames: `NNNN-short-slug.md` (zero-padded four-digit number, kebab-case slug).
- Status values: `Proposed`, `Accepted`, `Superseded`, `Deprecated`.
- Each ADR has the sections: **Status**, **Date**, **Context**, **Decision**,
  **Consequences**, **Open Questions** (if any), **References**.
- New ADRs are added to the index above in numerical order.
