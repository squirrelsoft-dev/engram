# Engram schema

The canonical SurrealDB schema for Engram, versioned alongside the code per
`docs/design/schema-migrations.md`.

## Layout

```
schema/
  README.md            # this file
  manifest.toml        # current schema version and applied-migration list
  migrations/
    NNNN_*.sql         # one numbered, ordered migration per file
```

A migration file contains one or more `DEFINE` statements. The file is
applied as a single SurrealDB transaction by the migration runner. If any
statement fails, the whole file rolls back and the `engram_schema` ledger
is not updated (per `docs/design/schema-migrations.md` §5.3).

## Records

The initial migration (`0001_init.sql`) defines every record type from
README §4.2 and every graph edge from README §4.3:

| Record type | Table       | Bi-temporal | Vector index |
| ----------- | ----------- | ----------- | ------------ |
| Episode     | `episode`   | yes (§4.4)  | yes          |
| Entity      | `entity`    | no          | no           |
| Concept     | `concept`   | yes (§4.4)  | yes          |
| Preference  | `preference`| yes (§4.4)  | no           |
| Procedure   | `procedure` | no          | yes          |
| Task        | `task`      | no          | no           |

Plus the migration ledger itself (`engram_schema`) per the
schema-migrations design.

| Graph edge                                  | Relation table                          |
| ------------------------------------------- | --------------------------------------- |
| Episode →\[relates_to\]→ Concept            | `episode_relates_to_concept`            |
| Episode →\[precedes\]→ Episode              | `episode_precedes_episode`              |
| Episode →\[mentions\]→ Entity               | `episode_mentions_entity`               |
| Concept →\[connects_to\]→ Concept           | `concept_connects_to_concept`           |
| Concept →\[about\]→ Entity                  | `concept_about_entity`                  |
| Entity →\[relates_to\]→ Entity              | `entity_relates_to_entity`              |
| Episode →\[triggered\]→ Task                | `episode_triggered_task`                |

All relation tables are `TYPE RELATION IN ... OUT ... ENFORCED` so the
graph cannot be left dangling at `RELATE` time.

## Bi-temporal model

Per `docs/design/schema-migrations.md` §5.5, the schema carries the
explicit `valid_time_start` / `valid_time_end` / `transaction_time`
fields on fact-carrying records (Episode, Concept, Preference). The
valid-time axis is filtered in queries by application code. The
transaction-time axis is implemented at the storage engine layer by
opening the underlying connection with the versioned variant (e.g.
`surrealkv+versioned://`) — that decision lives in the storage adapter
(issue #2), not in the schema.

When SurrealDB ships native bi-temporal support, the application-level
valid-time filtering can be replaced with native predicates; see
`docs/design/schema-migrations.md` §5.5 and the open question §9.5.

## Tunable parameters

`$embedding_dim` (default 768) is the runtime-readable embedding-model
dimension. The application reads it at startup to size the vectors it
generates. The vector indexes below hardcode the same value as a
literal, because SurrealDB's `HNSW DIMENSION ...` clause requires a
literal integer at define time. When the embedding model changes
(issue #15), the new model size requires a follow-up migration that
drops and recreates the three vector indexes (`idx_episode_embedding`,
`idx_concept_embedding`, `idx_procedure_embedding`).

Defaults: 768 fits BERT-base and nomic-embed-text; 384 fits MiniLM;
1536 fits OpenAI ada-002; 3072 fits OpenAI text-embedding-3-large.

## Conventions

- **Enums** use `INSIDE [...]` (the canonical SurrealDB form, per the
  spike in `spikes/schema-migrations/`).
- **Timestamps** that the system owns (`transaction_time`,
  `last_reinforced`, `last_updated`) use the `VALUE` clause, which
  ignores caller input. Timestamps that the caller may override
  (`valid_time_start`, `created_at`) use `DEFAULT`.
- **Flexible key-value stores** (`metadata`, `attributes`) use
  `FLEXIBLE TYPE object` so they accept any nested document while the
  table as a whole remains `SCHEMAFULL`.
- **Permissions** are `NONE` in this initial migration. The multi-tenant
  access model (README §9) and the corresponding `DEFINE ACCESS` /
  scope definitions land in a later migration once the auth model is
  settled.

## Applying the schema

The migration runner is implemented in the storage adapter (issue #2).
For ad-hoc application during development, the file is plain SurrealQL
and can be applied directly with `surreal import` or via the spike in
`spikes/schema-migrations/`:

```sh
# Embedded + service-mode parity check
cargo run --manifest-path spikes/schema-migrations/Cargo.toml -- schema/migrations/0001_init.sql
```

## Adding a new migration

1. Pick the next number (e.g. `0002_add_xyz.sql`).
2. Write additive `DEFINE` statements only. Destructive operations
   require a `--strict` opt-in per `docs/design/schema-migrations.md`
   §5.4 and are out of scope for the initial series.
3. Add a row to `manifest.toml`'s `applied_migrations` array so the
   manifest stays in sync.
4. Verify parity with the spike (see above).
5. Bump `version` in `manifest.toml`.
