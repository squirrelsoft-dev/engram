-- Migration 0003: normalise graph-edge weight field name and
-- default.
--
-- Migration 0001 used different per-relation attribute names
-- for what is, at the storage-adapter boundary, the same
-- concept: a float in [0.0, 1.0] representing the
-- strength/confidence of the edge. The relations used:
--
--   - episode_relates_to_concept.weight  (default 1.0)
--   - episode_mentions_entity.confidence
--   - concept_connects_to_concept.strength
--   - concept_about_entity.strength
--   - entity_relates_to_entity.strength
--   - episode_precedes_episode  (none)
--   - episode_triggered_task   (none)
--
-- The Phase 1 `MemoryStore::relate_nodes` API exposes a
-- single `weight: Option<f32>` parameter and the Phase 1
-- `GraphResult.attributes` shape is a single bag, so the
-- adapter has no place to put differently-named fields. The
-- Phase 2 consolidation engine will work in terms of the
-- uniform `weight` concept (a soft truth value, fed into the
-- inference pass), so the schema should match the API.
--
-- This migration:
--
--   1. Removes the old per-relation attribute fields.
--   2. Adds a uniform `weight: float [0.0, 1.0] DEFAULT 0.5`
--      to every graph-edge relation table (the
--      `episode_relates_to_concept` form is overwritten so
--      its default also becomes 0.5 — the 1.0 default was a
--      holdover from a pre-Phase 1 design where the edge
--      carried a binary "stated explicitly" signal).
--
-- The remove-then-(re)add order avoids the SurrealDB 3.1.x
-- "Cannot redefine field" guard that fires when a field is
-- redefined with a different type or assertion. The default
-- of 0.5 is the schema-versioned neutral weight from the
-- consolidation engine's source-of-truth.
--
-- If a future relation wants a distinct per-edge attribute
-- (e.g. `relationship_type` on entity_relates_to_entity,
-- which the disambiguation engine still reads), it should be
-- added back in a *new* migration, not folded into this one.

REMOVE FIELD IF EXISTS confidence ON episode_mentions_entity;
REMOVE FIELD IF EXISTS strength  ON concept_connects_to_concept;
REMOVE FIELD IF EXISTS strength  ON concept_about_entity;
-- `entity_relates_to_entity.relationship_type` was a required
-- string in 0001. We can't leave it as-is because Phase 1's
-- `relate_nodes(weight: Option<f32>)` doesn't carry a
-- relationship_type, and the field has no default. We
-- redeclare it as a non-required string with a default of
-- the empty string. The disambiguation engine (Phase 2)
-- can populate it explicitly on writes that want a
-- non-default value. Removing-then-redefining with a
-- default is the only way to relax a non-optional field in
-- SurrealDB 3.1.x (the `DROP ASSERT` ALTER FIELD clause
-- does not change a field's nullability).
REMOVE FIELD IF EXISTS strength  ON entity_relates_to_entity;

DEFINE FIELD OVERWRITE weight ON episode_relates_to_concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD weight ON episode_mentions_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD weight ON concept_connects_to_concept TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD weight ON concept_about_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD weight ON entity_relates_to_entity TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD OVERWRITE relationship_type ON entity_relates_to_entity TYPE string
    ASSERT string::len($value) >= 0
    DEFAULT "";
DEFINE FIELD weight ON episode_precedes_episode TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
DEFINE FIELD weight ON episode_triggered_task TYPE float
    ASSERT $value >= 0.0 AND $value <= 1.0
    DEFAULT 0.5;
