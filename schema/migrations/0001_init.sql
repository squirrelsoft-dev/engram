-- =============================================================================
-- Engram initial schema  (migration 0001)
-- =============================================================================
--
-- Implements the core record types and graph edges from README §4.2 and §4.3.
-- This is the first migration in the schema-migrations framework proposed
-- in docs/design/schema-migrations.md, applied to a fresh database by the
-- migration runner at startup.
--
-- Reading order:
--   1. Migration ledger (engram_schema) — must exist before any subsequent
--      migration can record its own application.
--   2. Database parameters — embedding dimension and other tunables that
--      a deployment may want to override before apply.
--   3. Core record types (Episode, Entity, Concept, Preference, Procedure,
--      Task) per README §4.2.
--   4. Graph edges per README §4.3.
--   5. Indexes — tenant scoping, lookup, and vector indexes.
--
-- Conventions:
--   * All tenant-scoped tables carry `agent_id`. `org_id` and `user_id` are
--     reserved per README §4.1 even when not yet used by the application.
--   * Fact-carrying tables (Episode, Concept, Preference) carry the bi-temporal
--     fields per README §4.4. Storage-engine versioning (transaction-time axis)
--     is enabled by the adapter at URL-construction time
--     (e.g. `surrealkv+versioned://`); the schema carries the explicit
--     `valid_time_*` and `transaction_time` fields as the application-level
--     bi-temporal surface described in docs/design/schema-migrations.md §5.5.
--   * Enums use `INSIDE [...]` (the canonical SurrealDB form per the spike
--     in spikes/schema-migrations/).
--   * `SCHEMAFULL` is the default. `metadata` and `attributes` flexible
--     key-value stores are typed `FLEXIBLE TYPE object` so they accept any
--     nested document at the field level while still being schemafull-enforced
--     at the table level.
--   * `DEFAULT` is used for values the caller may override (e.g. importance
--     scores, embedding dimensions, time defaults). `VALUE` is used for
--     system-managed timestamps that should ignore caller input
--     (transaction_time, last_reinforced, last_updated).
--   * Permissions are `NONE` in this initial migration. The multi-tenant
--     access model (README §9) and the `engram_` user/scope definitions
--     will land in a later migration once the auth model is settled.
--   * This file is applied as a single transaction by the migration runner
--     (per docs/design/schema-migrations.md §5.3). If any statement fails,
--     the whole file rolls back and `engram_schema` is not updated.
-- =============================================================================


-- ----------------------------------------------------------------------------
-- 1. Migration ledger
-- ----------------------------------------------------------------------------
--
-- Holds one record per applied migration. The runner reads this to decide
-- which migrations to apply at startup. See docs/design/schema-migrations.md
-- §4 for the rationale.

DEFINE TABLE engram_schema SCHEMAFULL;
DEFINE FIELD version     ON engram_schema TYPE int;
DEFINE FIELD applied_at  ON engram_schema TYPE datetime;
DEFINE FIELD engram_ver  ON engram_schema TYPE string;
DEFINE FIELD migration   ON engram_schema TYPE string;
DEFINE FIELD checksum    ON engram_schema TYPE string;
DEFINE FIELD direction   ON engram_schema TYPE string
    ASSERT $value INSIDE ['up', 'down'];


-- ----------------------------------------------------------------------------
-- 2. Tunable parameters
-- ----------------------------------------------------------------------------
--
-- `$embedding_dim` is the runtime-readable dimension of the embedding model
-- in use. It is defined as a parameter so the application can read it at
-- runtime and generate embeddings of the right size. The vector indexes
-- below also hardcode the same value as a literal because SurrealDB's
-- `HNSW DIMENSION ...` clause requires a literal integer at define time.
-- When the embedding model changes (tracked in issue #15), a follow-up
-- migration must DROP and re-CREATE the affected vector indexes with the
-- new dimension. The default 768 is compatible with BERT-base and
-- nomic-embed-text; 384 also fits MiniLM, 1536 fits OpenAI ada-002,
-- 3072 fits OpenAI text-embedding-3-large.

DEFINE PARAM $embedding_dim VALUE 768
    COMMENT "Dimension of all vector indexes. Override before initial apply if your embedding model differs from the 768-d default. Changing this after apply requires a follow-up migration that drops and recreates the affected vector indexes.";


-- ----------------------------------------------------------------------------
-- 3. Core record types
-- ----------------------------------------------------------------------------

-- Episode ------------------------------------------------------------------------
-- A single timestamped event. README §4.2. Versioned (transaction-time) per
-- §4.4. Valid time is carried explicitly via `valid_time_start`/`valid_time_end`.

DEFINE TABLE episode SCHEMAFULL;
DEFINE FIELD agent_id            ON episode TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON episode TYPE option<string>;
DEFINE FIELD user_id             ON episode TYPE option<string>;
DEFINE FIELD content             ON episode TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD content_type        ON episode TYPE string
    ASSERT $value INSIDE ['conversation', 'document', 'tool_result', 'observation', 'assertion'];
DEFINE FIELD embedding           ON episode TYPE option<array<float>>;
DEFINE FIELD importance          ON episode TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD entities            ON episode TYPE option<array<record<entity>>>;
DEFINE FIELD valid_time_start    ON episode TYPE datetime
    DEFAULT time::now();
DEFINE FIELD valid_time_end      ON episode TYPE option<datetime>;
DEFINE FIELD transaction_time    ON episode TYPE datetime
    VALUE time::now();
DEFINE FIELD consolidated        ON episode TYPE bool
    DEFAULT false;
DEFINE FIELD consolidated_at     ON episode TYPE option<datetime>;
DEFINE FIELD summary             ON episode TYPE option<string>;
DEFINE FIELD source_tier         ON episode TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 3;
DEFINE FIELD metadata            ON episode TYPE object FLEXIBLE
    DEFAULT {};


-- Entity -------------------------------------------------------------------------
-- The resolved, canonical representation of a real-world entity. README §4.2.

DEFINE TABLE entity SCHEMAFULL;
DEFINE FIELD agent_id            ON entity TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON entity TYPE option<string>;
DEFINE FIELD canonical_name      ON entity TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD aliases             ON entity TYPE array<string>
    DEFAULT [];
DEFINE FIELD entity_type         ON entity TYPE string
    ASSERT $value INSIDE ['person', 'organization', 'project', 'location', 'concept', 'other'];
DEFINE FIELD attributes          ON entity TYPE object FLEXIBLE
    DEFAULT {};
DEFINE FIELD confidence          ON entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD confidence_tier     ON entity TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 5;
DEFINE FIELD anchor_record       ON entity TYPE option<record<episode>>;
DEFINE FIELD created_at          ON entity TYPE datetime
    DEFAULT time::now();
DEFINE FIELD last_updated        ON entity TYPE datetime
    VALUE time::now();
DEFINE FIELD disambiguation_log  ON entity TYPE option<array<object>>;


-- Concept ------------------------------------------------------------------------
-- A semantic fact extracted during consolidation. README §4.2. Bi-temporal per
-- §4.4. `inferred=true` flags facts derived by the implicit-inference pass (§6.3).

DEFINE TABLE concept SCHEMAFULL;
DEFINE FIELD agent_id            ON concept TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON concept TYPE option<string>;
DEFINE FIELD content             ON concept TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD embedding           ON concept TYPE option<array<float>>;
DEFINE FIELD confidence          ON concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD source_tier         ON concept TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 3;
DEFINE FIELD reinforcement_count ON concept TYPE int
    ASSERT $value >= 0
    DEFAULT 1;
DEFINE FIELD last_reinforced     ON concept TYPE datetime
    VALUE time::now();
DEFINE FIELD decay_rate          ON concept TYPE float
    ASSERT $value >= 0.0
    DEFAULT 0.01;
DEFINE FIELD inferred            ON concept TYPE bool
    DEFAULT false;
DEFINE FIELD inference_chain     ON concept TYPE option<array<record<concept>>>;
DEFINE FIELD valid_time_start    ON concept TYPE datetime
    DEFAULT time::now();
DEFINE FIELD valid_time_end      ON concept TYPE option<datetime>;
DEFINE FIELD transaction_time    ON concept TYPE datetime
    VALUE time::now();


-- Preference ---------------------------------------------------------------------
-- User-specific behavioral pattern. README §4.2.

DEFINE TABLE preference SCHEMAFULL;
DEFINE FIELD agent_id            ON preference TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON preference TYPE option<string>;
DEFINE FIELD user_id             ON preference TYPE option<string>;
DEFINE FIELD category            ON preference TYPE string
    ASSERT $value INSIDE ['communication', 'topic', 'format', 'behavior', 'other'];
DEFINE FIELD content             ON preference TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD direction           ON preference TYPE string
    ASSERT $value INSIDE ['positive', 'negative'];
DEFINE FIELD strength            ON preference TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD source_tier         ON preference TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 3;
DEFINE FIELD evidence_count      ON preference TYPE int
    ASSERT $value >= 0
    DEFAULT 1;
DEFINE FIELD last_reinforced     ON preference TYPE datetime
    VALUE time::now();
DEFINE FIELD created_at          ON preference TYPE datetime
    DEFAULT time::now();
-- Bi-temporal per §4.4 — a preference that decays past its `valid_time_end` is
-- effectively invalidated while still queryable for history.
DEFINE FIELD valid_time_start    ON preference TYPE datetime
    DEFAULT time::now();
DEFINE FIELD valid_time_end      ON preference TYPE option<datetime>;
DEFINE FIELD transaction_time    ON preference TYPE datetime
    VALUE time::now();


-- Procedure ----------------------------------------------------------------------
-- A skill or learned behavior — tool spec, few-shot set, or pattern. README §4.2.

DEFINE TABLE procedure SCHEMAFULL;
DEFINE FIELD agent_id            ON procedure TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON procedure TYPE option<string>;
DEFINE FIELD name                ON procedure TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD procedure_type      ON procedure TYPE string
    ASSERT $value INSIDE ['tool_definition', 'few_shot_set', 'behavioral_pattern'];
DEFINE FIELD content             ON procedure TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD embedding           ON procedure TYPE option<array<float>>;
DEFINE FIELD trigger_patterns    ON procedure TYPE array<string>
    DEFAULT [];
DEFINE FIELD usage_count         ON procedure TYPE int
    ASSERT $value >= 0
    DEFAULT 0;
DEFINE FIELD last_used           ON procedure TYPE option<datetime>;
DEFINE FIELD created_at          ON procedure TYPE datetime
    DEFAULT time::now();


-- Task ---------------------------------------------------------------------------
-- Prospective memory — a future intention. README §4.2.

DEFINE TABLE task SCHEMAFULL;
DEFINE FIELD agent_id            ON task TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD org_id              ON task TYPE option<string>;
DEFINE FIELD user_id             ON task TYPE option<string>;
DEFINE FIELD content             ON task TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD trigger_type        ON task TYPE string
    ASSERT $value INSIDE ['time', 'event', 'condition'];
DEFINE FIELD trigger_value       ON task TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD status              ON task TYPE string
    ASSERT $value INSIDE ['pending', 'triggered', 'completed', 'cancelled']
    DEFAULT 'pending';
DEFINE FIELD created_at          ON task TYPE datetime
    DEFAULT time::now();
DEFINE FIELD triggered_at        ON task TYPE option<datetime>;


-- ----------------------------------------------------------------------------
-- 4. Graph edges  (README §4.3)
-- ----------------------------------------------------------------------------
--
-- Each edge is a `TYPE RELATION` table with the type signature shown below.
-- `ENFORCED` requires the linked records to exist at `RELATE` time, which
-- protects graph integrity per the spike's findings and the design note in
-- docs/design/schema-migrations.md §5.7.
--
-- The `in` and `out` columns are implicit on a RELATION table and need not
-- be declared; SurrealDB sets them automatically.

-- Episode ->[relates_to]-> Concept
DEFINE TABLE episode_relates_to_concept
    SCHEMAFULL
    TYPE RELATION IN episode OUT concept ENFORCED;
DEFINE FIELD weight      ON episode_relates_to_concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 1.0;
DEFINE FIELD created_at  ON episode_relates_to_concept TYPE datetime
    DEFAULT time::now();

-- Episode ->[precedes]-> Episode
-- README §4.3 lists no additional attributes on this edge. The implicit
-- `in`/`out` is the full content.
DEFINE TABLE episode_precedes_episode
    SCHEMAFULL
    TYPE RELATION IN episode OUT episode ENFORCED;

-- Episode ->[mentions]-> Entity
DEFINE TABLE episode_mentions_entity
    SCHEMAFULL
    TYPE RELATION IN episode OUT entity ENFORCED;
DEFINE FIELD confidence  ON episode_mentions_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD source_tier ON episode_mentions_entity TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 3;

-- Concept ->[connects_to]-> Concept
DEFINE TABLE concept_connects_to_concept
    SCHEMAFULL
    TYPE RELATION IN concept OUT concept ENFORCED;
DEFINE FIELD strength    ON concept_connects_to_concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD inferred    ON concept_connects_to_concept TYPE bool
    DEFAULT false;
DEFINE FIELD created_at  ON concept_connects_to_concept TYPE datetime
    DEFAULT time::now();

-- Concept ->[about]-> Entity
DEFINE TABLE concept_about_entity
    SCHEMAFULL
    TYPE RELATION IN concept OUT entity ENFORCED;
DEFINE FIELD strength    ON concept_about_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;

-- Entity ->[relates_to]-> Entity
DEFINE TABLE entity_relates_to_entity
    SCHEMAFULL
    TYPE RELATION IN entity OUT entity ENFORCED;
DEFINE FIELD relationship_type ON entity_relates_to_entity TYPE string
    ASSERT string::len($value) > 0;
DEFINE FIELD strength          ON entity_relates_to_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD source_tier       ON entity_relates_to_entity TYPE int
    ASSERT $value INSIDE [1, 2, 3, 4, 5]
    DEFAULT 3;

-- Episode ->[triggered]-> Task
DEFINE TABLE episode_triggered_task
    SCHEMAFULL
    TYPE RELATION IN episode OUT task ENFORCED;
DEFINE FIELD created_at ON episode_triggered_task TYPE datetime
    DEFAULT time::now();


-- ----------------------------------------------------------------------------
-- 5. Indexes
-- ----------------------------------------------------------------------------
--
-- Tenant scoping and lookup indexes come first; vector indexes after.

-- Tenant scoping -----------------------------------------------------------------
-- Every query that returns records for a given agent MUST filter by agent_id,
-- per README §9.1. A non-unique index on agent_id accelerates these filters.

DEFINE INDEX idx_episode_agent         ON episode    FIELDS agent_id;
DEFINE INDEX idx_entity_agent          ON entity     FIELDS agent_id;
DEFINE INDEX idx_concept_agent         ON concept    FIELDS agent_id;
DEFINE INDEX idx_preference_agent      ON preference FIELDS agent_id;
DEFINE INDEX idx_procedure_agent       ON procedure  FIELDS agent_id;
DEFINE INDEX idx_task_agent            ON task       FIELDS agent_id;

-- Lookup indexes used by the ingestion pipeline and retrieval orchestrator ---

-- Episode: tenant + recency scan for episodic recall
DEFINE INDEX idx_episode_agent_time
    ON episode FIELDS agent_id, valid_time_start;

-- Episode: filter by consolidation state (the consolidation engine reads this)
DEFINE INDEX idx_episode_consolidated
    ON episode FIELDS agent_id, consolidated;

-- Entity: lookup by canonical_name within an agent.
-- The pipeline merges candidates by name; this index accelerates the candidate
-- query. Uniqueness is NOT enforced at the schema level because disambiguation
-- is a runtime concern, not a constraint.
DEFINE INDEX idx_entity_agent_name
    ON entity FIELDS agent_id, canonical_name;

-- Entity: filter by entity_type
DEFINE INDEX idx_entity_agent_type
    ON entity FIELDS agent_id, entity_type;

-- Concept: tenant + recency scan for semantic recall
DEFINE INDEX idx_concept_agent_time
    ON concept FIELDS agent_id, last_reinforced;

-- Preference: filter by user and category
DEFINE INDEX idx_preference_agent_user_category
    ON preference FIELDS agent_id, user_id, category;

-- Task: filter by status for the prospective-memory layer
DEFINE INDEX idx_task_agent_status
    ON task FIELDS agent_id, status;

-- Vector indexes ----------------------------------------------------------------
-- HNSW with COSINE distance, the typical default for normalized embedding
-- similarity search. `TYPE F32` halves the storage vs F64 with negligible
-- recall loss in practice; revisit if a future embedding model proves
-- sensitive to precision.
--
-- The `DIMENSION 768` literal below MUST match the value of `$embedding_dim`
-- above. SurrealDB does not allow `DIMENSION` to reference a parameter, so
-- any change to the embedding dimension requires a follow-up migration
-- that DROPs and re-CREATEs these indexes.

DEFINE INDEX idx_episode_embedding
    ON episode FIELDS embedding
    HNSW DIMENSION 768 DIST COSINE TYPE F32;

DEFINE INDEX idx_concept_embedding
    ON concept FIELDS embedding
    HNSW DIMENSION 768 DIST COSINE TYPE F32;

DEFINE INDEX idx_procedure_embedding
    ON procedure FIELDS embedding
    HNSW DIMENSION 768 DIST COSINE TYPE F32;
