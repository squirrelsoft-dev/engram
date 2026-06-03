-- Engram schema (spike subset)
-- This is a small but representative slice of the schema proposed in
-- docs/design/schema-migrations.md, intended to exercise the round-trip
-- in both embedded and service modes.

-- ----------------------------------------------------------------------------
-- Core record types
-- ----------------------------------------------------------------------------

DEFINE TABLE engram_schema SCHEMAFULL;
DEFINE FIELD version     ON engram_schema TYPE int;
DEFINE FIELD applied_at  ON engram_schema TYPE datetime;
DEFINE FIELD engram_ver  ON engram_schema TYPE string;
DEFINE FIELD migration   ON engram_schema TYPE string;
DEFINE FIELD checksum    ON engram_schema TYPE string;
DEFINE FIELD direction   ON engram_schema TYPE string
    ASSERT $value IN ['up', 'down'];

DEFINE TABLE episode SCHEMAFULL;
DEFINE FIELD agent_id         ON episode TYPE string;
DEFINE FIELD user_id          ON episode TYPE option<string>;
DEFINE FIELD content          ON episode TYPE string;
DEFINE FIELD content_type     ON episode TYPE string
    ASSERT $value IN ['conversation', 'document', 'tool_result', 'observation', 'assertion'];
DEFINE FIELD importance       ON episode TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE FIELD valid_time_start ON episode TYPE datetime;
DEFINE FIELD valid_time_end   ON episode TYPE option<datetime>;
DEFINE FIELD transaction_time ON episode TYPE datetime;
DEFINE FIELD consolidated     ON episode TYPE bool DEFAULT false;
DEFINE INDEX idx_episode_agent ON episode FIELDS agent_id;

DEFINE TABLE entity SCHEMAFULL;
DEFINE FIELD agent_id      ON entity TYPE string;
DEFINE FIELD canonical_name ON entity TYPE string;
DEFINE FIELD entity_type   ON entity TYPE string
    ASSERT $value IN ['person', 'organization', 'project', 'location', 'concept', 'other'];
DEFINE FIELD confidence    ON entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE INDEX idx_entity_agent ON entity FIELDS agent_id;
DEFINE INDEX idx_entity_name  ON entity FIELDS canonical_name;

DEFINE TABLE concept SCHEMAFULL;
DEFINE FIELD agent_id    ON concept TYPE string;
DEFINE FIELD content     ON concept TYPE string;
DEFINE FIELD confidence  ON concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0;
DEFINE INDEX idx_concept_agent ON concept FIELDS agent_id;
