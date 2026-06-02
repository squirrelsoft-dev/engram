# Technical Design Document

## Engram — Agent Memory Service

**Version:** 0.1 (Draft)
**Status:** Concept
**Authors:** Scott Beardsley
**Organization:** SquirrelSoft LLC
**Last Updated:** 2026-05-23

---

## 1. Overview

### 1.1 Purpose

This document describes the architecture, data model, pipeline design, and implementation plan for Engram — a memory-as-a-service system that provides any AI agent with human-analogous memory capabilities regardless of the agent's underlying model, framework, or integration method.

### 1.2 Problem Statement

Current approaches to agent memory fall into two categories:

**Context stuffing** — all relevant information is manually loaded into the context window at the start of each session. This does not scale, has no mechanism for forgetting or prioritization, and produces no emergent understanding over time.

**Vector search (the "2nd brain" approach)** — content is embedded and stored, then retrieved via cosine similarity. This approximates semantic memory only, has no temporal awareness, no graph relationships, no disambiguation, and no consolidation. It is a searchable log, not a memory system.

Neither approach produces an agent that genuinely remembers. Engram addresses this gap by implementing all layers of human memory as a persistent, queryable, autonomously improving service.

### 1.3 Goals

- Provide a complete multi-layer memory system covering episodic, semantic, procedural, and prospective memory
- Expose a minimal, stable operation surface that any agent can use without understanding the internals
- Support multiple integration methods — MCP, REST API, CLI, and language SDKs
- Implement autonomous background consolidation that deepens understanding between sessions
- Support multi-tenant deployments with org, agent, and user scoping
- Use SurrealDB as the sole local datastore, leveraging its native graph, vector, document, and temporal capabilities

### 1.4 Non-Goals

- This is not an agent framework — it is a memory layer that agents attach to
- This is not a RAG pipeline — it is a structured memory system with multiple retrieval strategies
- This is not a general-purpose database — all data models are purpose-built for memory semantics
- This does not replace context window management within the agent itself

---

## 2. Background

### 2.1 Human Memory as the Design Model

Human memory is not a single system. Research identifies several distinct layers, each with different storage mechanisms, retrieval characteristics, and decay properties:

| Memory Type       | Human Analogue                              | Engram Implementation                     |
| ----------------- | ------------------------------------------- | ----------------------------------------- |
| Sensory / Working | Context window (active thought)             | Not stored — handled by the agent         |
| Episodic          | Autobiographical events with time and place | Episode records with bi-temporal tracking |
| Semantic          | General facts and knowledge                 | Concept records in the knowledge graph    |
| Procedural        | Skills and learned behaviors                | Procedure records — tool specs, patterns  |
| Prospective       | Intentions about the future                 | Task records with trigger conditions      |

### 2.2 Why Existing Approaches Fall Short

The vector search approach (embedding + cosine similarity) only addresses semantic memory retrieval. It produces no understanding of relationships between concepts, no temporal reasoning, no disambiguation of entities mentioned in different ways, and no mechanism for knowledge to improve between sessions. It is passive storage with search, not memory.

### 2.3 The SurrealDB Fit

SurrealDB is selected as the sole datastore because it natively supports all required data models in a single system:

- **Document model** for flexible episodic records
- **Graph model** for the knowledge graph and entity relationships
- **Vector search** for embedding-based similarity retrieval
- **Relational model** for structured agent/user/org data
- **Bi-temporal tables** for valid-time and transaction-time tracking
- **ACID transactions** ensuring graph integrity across multi-record writes

This eliminates the need to coordinate multiple specialized databases, removes consistency gaps between systems, and simplifies the operational footprint for local deployment.

---

## 3. Architecture

### 3.1 High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Entry Points                            │
│                                                                 │
│   MCP Server    REST API    CLI    Node SDK    Python SDK       │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                      Operation Router                           │
│          Auth · Tenant resolution · Input validation            │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                        Memory Core                              │
│                                                                 │
│  ┌─────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Ingestion        │  │ Retrieval         │  │ Consolidation │  │
│  │ Pipeline         │  │ Orchestrator      │  │ Engine        │  │
│  │                  │  │                   │  │               │  │
│  │ Normalize        │  │ Classify query    │  │ Scheduled /   │  │
│  │ Extract entities │  │ Fan out to layers │  │ on-demand     │  │
│  │ Disambiguate     │  │ Merge + re-rank   │  │ background    │  │
│  │ Embed            │  │ Assemble context  │  │ processing    │  │
│  │ Score importance │  └──────────────────┘  └───────────────┘  │
│  └─────────────────┘                                            │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                      Storage Adapter                            │
│                    (MemoryStore interface)                       │
└────────────────────────────┬────────────────────────────────────┘
                             │
┌────────────────────────────▼────────────────────────────────────┐
│                          SurrealDB                              │
│                                                                 │
│   Episodes · Concepts · Entities · Procedures · Tasks           │
│   Graph edges · Embeddings · Bi-temporal records                │
└─────────────────────────────────────────────────────────────────┘
```

### 3.2 Entry Points as Adapters

All entry points are thin adapters over the Operation Router. The router is transport-agnostic — it receives a normalized operation request and returns a normalized response. No business logic lives in the entry points.

```
Entry Point responsibility:
  - Deserialize incoming request
  - Map to normalized OperationRequest
  - Pass to Operation Router
  - Serialize response for transport

Operation Router responsibility:
  - Authenticate caller
  - Resolve tenant (org / agent / user)
  - Validate input
  - Route to Memory Core operation
  - Return normalized OperationResponse
```

### 3.3 Storage Adapter Interface

The Memory Core interacts with storage exclusively through the MemoryStore interface. SurrealDB is the local implementation. This boundary ensures that a production deployment could substitute an alternative storage backend without modifying the Memory Core.

```
MemoryStore interface:

  writeEpisode(episode)           → EpisodeRecord
  queryEpisodic(query, filters)   → EpisodeRecord[]
  querySemantic(embedding, k)     → ConceptRecord[]
  upsertEntity(entity)            → EntityRecord
  resolveEntity(candidates)       → EntityRecord
  upsertConcept(concept)          → ConceptRecord
  relateNodes(from, relation, to, weight)
  traverseGraph(start, depth, filters) → GraphResult
  writeTask(task)                 → TaskRecord
  queryPending(agentId, now)      → TaskRecord[]
  writeProcedure(procedure)       → ProcedureRecord
  queryProcedures(query)          → ProcedureRecord[]
```

---

## 4. Data Model

### 4.1 Tenancy Model

```
Organization  (optional top-level grouping)
  └── Agent   (required — minimum scope for all operations)
        └── User  (optional — for agents serving multiple humans)
```

Every record carries `agent_id` as a minimum. `org_id` and `user_id` are optional but reserved in the schema from day one — they cannot be retrofitted cleanly after launch.

### 4.2 Core Record Types

#### Episode

An episodic record represents a single timestamped event — a conversation turn, an observation, a document ingestion, a tool result, or any other discrete unit of experience.

```
Episode {
  id                  : unique identifier
  agent_id            : required
  user_id             : optional
  content             : normalized text content
  content_type        : conversation | document | tool_result | observation | assertion
  embedding           : vector representation
  importance          : float 0.0–1.0 (scored at ingestion)
  entities            : EntityRef[]  (extracted entity references)
  valid_time_start    : when the event occurred (user/world time)
  valid_time_end      : for facts that are true over a range
  transaction_time    : when this record was written to the system
  consolidated        : boolean
  consolidated_at     : timestamp of last consolidation pass
  summary             : compressed representation (populated post-consolidation)
  source_tier         : signal strength tier of the originating source
  metadata            : flexible key-value store for agent-specific data
}
```

Episodes use SurrealDB's versioned records so the full history of any episode is recoverable. Consolidation compresses the record but never destroys version history.

#### Entity

Entities are the primary disambiguation target. A single real-world entity (a person, organization, project, location) may be referenced in many ways across many episodes. The entity record is the resolved, canonical representation.

```
Entity {
  id                  : unique identifier
  agent_id            : required
  canonical_name      : resolved authoritative name
  aliases             : string[]  (all observed references)
  entity_type         : person | organization | project | location | concept | other
  attributes          : flexible key-value store (role, email, title, etc.)
  confidence          : float 0.0–1.0
  confidence_tier     : highest tier signal that has contributed
  anchor_record       : episode_id of the authoritative assertion, if any
  created_at          : timestamp
  last_updated        : timestamp
  disambiguation_log  : history of merge events and evidence contributions
}
```

#### Concept

Concepts are semantic facts extracted from episodes during consolidation. They represent what the agent knows about the world, independent of when or how it learned it.

```
Concept {
  id                  : unique identifier
  agent_id            : required
  content             : the fact or piece of knowledge
  embedding           : vector representation
  confidence          : float 0.0–1.0
  source_tier         : highest signal tier that contributed to this concept
  reinforcement_count : how many times this concept has been corroborated
  last_reinforced     : timestamp
  decay_rate          : how quickly confidence degrades without reinforcement
  inferred            : boolean (true if derived by implication, not stated)
  inference_chain     : concept_ids used in the inference, if inferred
  valid_time_start    : when this concept became true
  valid_time_end      : when this concept stopped being true (null = still true)
  transaction_time    : when this was written
}
```

#### Preference

Preferences are user-specific behavioral patterns and stated preferences. Separated from Concepts because retrieval timing, accumulation mechanics, and decay behavior differ.

```
Preference {
  id                  : unique identifier
  agent_id            : required
  user_id             : optional
  category            : communication | topic | format | behavior | other
  content             : description of the preference
  direction           : positive | negative  (prefer / avoid)
  strength            : float 0.0–1.0
  source_tier         : signal tier of originating evidence
  evidence_count      : number of contributing signals
  last_reinforced     : timestamp
  created_at          : timestamp
}
```

#### Procedure

Procedures represent skills and learned behaviors — tool definitions, few-shot example sets, or behavioral patterns detected by the consolidation engine.

```
Procedure {
  id                  : unique identifier
  agent_id            : required
  name                : human-readable label
  procedure_type      : tool_definition | few_shot_set | behavioral_pattern
  content             : the procedure definition or example set
  embedding           : vector representation for retrieval
  trigger_patterns    : conditions under which this procedure is relevant
  usage_count         : how often this has been retrieved
  last_used           : timestamp
  created_at          : timestamp
}
```

#### Task

Tasks are prospective memory — future intentions the agent needs to act on.

```
Task {
  id                  : unique identifier
  agent_id            : required
  user_id             : optional
  content             : description of the intended action
  trigger_type        : time | event | condition
  trigger_value       : the specific trigger (timestamp, event name, condition expression)
  status              : pending | triggered | completed | cancelled
  created_at          : timestamp
  triggered_at        : timestamp (populated when fired)
}
```

### 4.3 Graph Edges

Graph edges are first-class records in SurrealDB, not join tables. Each edge type carries its own attributes.

```
Episode  →[relates_to]→    Concept       weight, created_at
Episode  →[precedes]→      Episode       temporal ordering
Episode  →[mentions]→      Entity        confidence, source_tier
Concept  →[connects_to]→   Concept       strength, inferred, created_at
Concept  →[about]→         Entity        strength
Entity   →[relates_to]→    Entity        relationship_type, strength, source_tier
Episode  →[triggered]→     Task          created_at
```

### 4.4 Bi-Temporal Design

All fact-carrying records (Episode, Concept, Preference) carry two time dimensions:

**Valid time** — when the fact was true in the world. Set by the caller. Allows reasoning like "what was true last month" even if that fact was recorded today.

**Transaction time** — when the record was written to Engram. Set by the system. Allows auditing and replay of what the agent knew at any point in its operation history.

This distinction is important. An agent might learn today that a decision was made three months ago. Valid time captures the real-world timestamp of the decision. Transaction time captures when the agent learned of it. Both are preserved.

SurrealDB's versioned tables ensure that updates to records append new versions rather than overwriting, preserving the full history of any record at any transaction time.

---

## 5. Signal Strength Hierarchy

### 5.1 Overview

Signal strength determines how much weight a piece of evidence carries in entity disambiguation, fact confidence, and preference accumulation. The source of a signal is more important than the quantity of signals — a hundred conversational mentions do not automatically override a single authoritative assertion.

### 5.2 Signal Tiers

```
Tier 1 — Authoritative Assertion
  Source:   Direct user declaration, contact card, explicit correction
  Examples: "This is my boss Sarah Chen" + contact card
            "That's wrong — the project is called Atlas"
  Behavior: Overrides all lower tiers immediately
            Triggers retroactive relink of all prior mentions
            Sets anchor_record on the entity
            No threshold required — immediate effect

Tier 2 — Structured Source
  Source:   Document, calendar entry, email header, form data, API response
  Examples: Word doc with "Sarah Chen, VP of Engineering" as recipient
            Calendar invite listing participants with titles
  Behavior: High confidence — may trigger immediate merge at sufficient overlap
            Sets confidence_tier if higher than existing
            Weighted 3x relative to Tier 3

Tier 3 — Explicit Conversational Statement
  Source:   Direct statement within a conversation
  Examples: "Sarah is my manager"
            "The project deadline is next Friday"
  Behavior: Medium confidence
            Accumulates toward disambiguation threshold
            Weighted 2x relative to Tier 4

Tier 4 — Implied Conversational Reference
  Source:   Indirect reference, pronoun resolution, contextual implication
  Examples: "Run it past Sarah before we commit"
            "My boss reviewed it"
  Behavior: Low confidence — creates or reinforces candidate entity
            Requires accumulation or corroboration from higher tiers

Tier 5 — Behavioral Pattern
  Source:   Observed behavior over time without explicit statement
  Examples: User consistently edits responses to be shorter
            Agent always queries X before Y in a certain context
  Behavior: Weakest signal — only meaningful in volume or combination
            Feeds Preference accumulation primarily
            Does not independently trigger entity merges
```

### 5.3 Cross-Tier Rules

**Higher tier always wins on direct conflict.** Tier 1 asserting X and Tier 4 implying not-X — Tier 1 wins without evaluation. The conflicting signal is flagged, not discarded.

**Tiers combine to reach thresholds.** Multiple lower-tier signals can collectively reach a threshold that a single signal of that tier would not. A weighted accumulation model is used: `score = Σ(signal_weight × tier_multiplier)`.

**Tier 1 is retroactive.** When a Tier 1 assertion arrives, it triggers a retroactive pass that reweights and relinks all prior signals related to that entity or fact, not just future ones.

**Same-tier conflicts are flagged.** Two Tier 2 structured sources that disagree on a fact produce an ambiguity flag rather than a silent resolution. The agent or user may need to resolve these.

---

## 6. Pipeline Design

### 6.1 Ingestion Pipeline

The ingestion pipeline is triggered by every `store()` call. It transforms raw input into structured, queryable memory.

```
Input
  │
  ▼
1. Normalize
   Detect content type (conversation, document, tool result, observation)
   Clean and structure raw text
   Extract metadata (timestamps, source URL, author, etc.)
  │
  ▼
2. Entity Extraction
   Identify named entities (persons, organizations, projects, locations)
   Identify implicit references ("my manager", "the project", "she")
   Assign source tier based on content type and context
  │
  ▼
3. Entity Disambiguation
   For each extracted entity:
     a. Query existing entities for candidates (embedding + attribute match)
     b. Score candidate matches using signal strength and overlap
     c. If score > auto-merge threshold → merge and relink
     d. If score in review band → merge with flag, allow rollback
     e. If score < threshold → create new candidate entity
     f. If Tier 1 signal → assert as anchor, trigger retroactive relink
  │
  ▼
4. Embedding Generation
   Generate embedding for the normalized content
   Generate embeddings for extracted entities not yet in the store
  │
  ▼
5. Importance Scoring
   Score 0.0–1.0 based on:
     - Source tier
     - Entity count and known entity overlap
     - Explicit priority signals ("important", "remember this", "deadline")
     - Recency and session context
  │
  ▼
6. Write to Storage
   Write Episode record (atomic with entity and graph edge writes)
   Write or update Entity records
   Write graph edges: Episode →[mentions]→ Entity
   Queue episode for consolidation
  │
  ▼
Output: EpisodeRecord with entity links and consolidation queue entry
```

### 6.2 Retrieval Orchestrator

The retrieval orchestrator is the brain of `recall()`. It routes queries to the appropriate memory layer(s) and assembles the result.

```
Input: query string, optional filters (time range, entity, type, agent_id)
  │
  ▼
1. Query Classification (small fast LLM call)
   Classify as one or more of:
     - Episodic  ("what happened with X", "when did we discuss Y")
     - Semantic  ("what do I know about Y", "what is X")
     - Procedural ("how do I do X", "what tool handles Y")
     - Prospective ("what am I supposed to do", "any pending tasks")
     - Preference ("how does the user like X", "what format does she prefer")
  │
  ▼
2. Fan Out to Relevant Layers
   Each active layer executes in parallel:
   
   Episodic:    Vector search on Episode embeddings
                + time filters
                + entity filters
                + graph traversal from matching entities
   
   Semantic:    Vector search on Concept embeddings
                + graph traversal for related concepts
                + implicit inference results
   
   Procedural:  Vector search on Procedure embeddings
                + trigger pattern matching
   
   Prospective: Direct query on pending Tasks
                + time and event filter
   
   Preference:  Direct query on Preference records
                + category and user filter
  │
  ▼
3. Merge and Re-rank
   Combine results across layers
   Re-rank by: relevance score × recency weight × importance score
   Deduplicate overlapping content
   Cap result set size
  │
  ▼
4. Assemble Context Slice
   Format results as ready-to-inject context text
   Include provenance metadata for agent awareness
   Return ranked list with source attribution
  │
  ▼
Output: Assembled context slice + raw records for agent use
```

### 6.3 Consolidation Engine

The consolidation engine runs as a background process. It is the mechanism by which raw episodic experience becomes structured knowledge.

**Triggers:**

- Scheduled (configurable per agent — post-session, hourly, nightly)
- On-demand via `reflect()` operation
- Event-driven (N new unconsolidated episodes, session end signal)

**Process:**

```
1. Fetch unconsolidated Episodes for this agent (batched)

2. Entity Consolidation Pass
   Scan for entity candidates that have accumulated enough evidence to merge
   Resolve ambiguities that have crossed confidence thresholds
   Retroactively relink episodes pointing to merged entities

3. Semantic Extraction Pass
   For each episode batch, LLM pass to extract semantic facts
   Upsert Concept records (new facts created, existing facts reinforced)
   Strengthen Concept→Concept graph edges where both concepts appear together
   Create new Concept→Entity edges

4. Implicit Inference Pass
   Traverse the current knowledge graph for inferable facts
   Apply inference rules (transitive relationships, role membership, etc.)
   Write inferred Concept records with inferred=true and inference_chain
   Example: Entity A manages Project B → Project B is in Domain C
            → infer Entity A works in Domain C

5. Preference Accumulation Pass
   Scan episodes for behavioral signals (Tier 5)
   Accumulate against existing Preference records
   Create new Preference records where patterns emerge

6. Ambiguity Resolution Pass
   Review flagged ambiguities from earlier disambiguation
   Check if accumulated evidence since flagging is sufficient to resolve
   Resolve or escalate unresolvable conflicts

7. Compression Pass
   Identify old, low-importance, fully-consolidated episodes
   Generate summary representation
   Compress episode record, retain full content in version history
   Mark as compressed

8. Concept Decay Pass
   Apply decay to Concept and Preference records not recently reinforced
   Reduce confidence scores based on time elapsed and decay_rate
   Flag records that have decayed below minimum threshold

9. Mark consolidation complete
   Set consolidated=true and consolidated_at on processed episodes
   Write consolidation run record (timing, counts, changes made)
```

---

## 7. Operation Surface

Five operations. The agent never needs to know what happens beneath them.

### 7.1 store(content, metadata?)

Ingests a new memory. Triggers the full ingestion pipeline.

```
Input:
  content       : string — the raw content to remember
  metadata:
    type        : conversation | document | tool_result | observation | assertion
    source_tier : override automatic tier detection (optional)
    valid_time  : when this event occurred (defaults to now)
    user_id     : user scope (optional)
    importance  : manual importance override (optional)
    entities    : pre-identified entities to assist extraction (optional)

Output:
  episode_id    : identifier for this memory
  entities      : extracted and disambiguated entity references
  importance    : scored importance
  queued_for    : estimated consolidation time
```

### 7.2 recall(query, filters?)

Retrieves relevant memory context. Triggers the retrieval orchestrator.

```
Input:
  query         : string — natural language query
  filters:
    types       : which memory layers to include (default: all)
    time_range  : valid_time bounds
    entities    : filter by entity involvement
    user_id     : user scope
    max_results : result cap per layer

Output:
  context       : assembled context string ready for prompt injection
  sources       : list of contributing records with provenance
  query_type    : detected classification(s) from orchestrator
```

### 7.3 reflect()

Manually triggers the consolidation engine for this agent. Useful at session end or when the agent wants to force knowledge integration before a complex task.

```
Input:
  agent_id      : (resolved from auth context)
  scope         : full | incremental (default: incremental)

Output:
  episodes_processed  : count
  concepts_created    : count
  concepts_updated    : count
  entities_merged     : count
  inferences_made     : count
  ambiguities_flagged : count
  duration_ms         : processing time
```

### 7.4 forget(target)

Decays or removes specific memories. Soft delete — tombstone preserved, graph integrity maintained.

```
Input:
  target — one of:
    id          : specific episode_id or concept_id
    entity      : forget all memories about a named entity
    time_range  : forget all memories before/after a date
    query       : semantic match — forget what matches this query

Output:
  records_tombstoned  : count
  graph_edges_removed : count
  concepts_decayed    : count
```

### 7.5 status()

Returns current memory state for this agent.

```
Output:
  episode_count         : total episodes
  consolidated_count    : consolidated episodes
  pending_consolidation : episodes awaiting consolidation
  concept_count         : total concepts
  entity_count          : total resolved entities
  preference_count      : total preferences
  task_count            : pending tasks
  last_consolidation    : timestamp
  next_scheduled        : timestamp
  storage_size          : approximate storage usage
```

---

## 8. Entity Disambiguation Detail

### 8.1 Two-Track Model

Entity disambiguation operates on two tracks simultaneously:

**Explicit track** — triggered by Tier 1 or Tier 2 signals. The entity is asserted with high confidence and becomes an anchor or near-anchor record immediately. Prior candidate entities are evaluated for merge against the new anchor.

**Implicit track** — triggered by Tier 3–5 signals. Candidate entities accumulate evidence over time. The consolidation engine periodically evaluates accumulated candidates against merge thresholds.

### 8.2 Candidate Scoring

When a new entity mention is processed, it is scored against all existing entity candidates for potential match:

```
candidate_score = (
  name_similarity_score     × 0.35  +
  attribute_overlap_score   × 0.30  +
  context_similarity_score  × 0.20  +
  co_occurrence_score       × 0.15
) × tier_multiplier
```

Where `tier_multiplier` is 3.0 for Tier 2, 2.0 for Tier 3, 1.0 for Tier 4, 0.5 for Tier 5.

### 8.3 Merge Thresholds

```
score ≥ 0.85    → auto-merge (silent)
score 0.65–0.84 → merge with flag (rollback available for 30 days)
score < 0.65    → accumulate as candidate
```

These thresholds are configurable per agent.

### 8.4 Retroactive Relinking

When two entity candidates merge — regardless of what triggered the merge — all graph edges pointing to either entity are relinked to the merged canonical entity. Episode mentions are updated. The original candidate records are tombstoned but not destroyed, preserving the disambiguation history.

---

## 9. Multi-Tenancy

### 9.1 Scoping Rules

- Every operation requires a valid `agent_id`
- `org_id` is optional; if present, agents inherit org-level configuration defaults
- `user_id` is optional; if present, some memory layers can be user-scoped (preferences always are)
- Cross-agent memory sharing is opt-in, configured at the org level
- All SurrealDB queries are scoped by `agent_id` at the query layer — no agent can access another agent's memory without explicit sharing configuration

### 9.2 Shared Memory (Multi-Agent)

When multiple agents share memory (configured at org level), they write to and read from a shared namespace in addition to their private namespace. The shared namespace has its own consolidation schedule. Reads return a merged result from both namespaces, ranked with private memory taking precedence on conflict.

---

## 10. Entry Point Specifications

### 10.1 MCP Server

The MCP entry point exposes five tools mapping 1:1 to the operation surface. It is the highest-priority entry point because it makes Engram immediately available to any MCP-compatible agent (Claude, Cursor, and others) without custom integration code.

```
Tool definitions:
  memory_store    → store()
  memory_recall   → recall()
  memory_reflect  → reflect()
  memory_forget   → forget()
  memory_status   → status()
```

Auth is handled via MCP server configuration. `agent_id` is resolved from the MCP client identity.

### 10.2 REST API

Standard JSON API over HTTPS. Auth via API key in the Authorization header. All endpoints are versioned under `/v1/memory/`.

```
POST   /v1/memory/store
POST   /v1/memory/recall
POST   /v1/memory/reflect
POST   /v1/memory/forget
GET    /v1/memory/status
```

`agent_id` is resolved from the API key. `user_id` is passed in the request body where applicable.

### 10.3 CLI

A command-line interface for development, testing, and scripting. Thin wrapper over the REST API. Configuration via environment variables or a local config file.

```
engram store "content here" [--type conversation] [--tier 3]
engram recall "query here" [--types episodic,semantic] [--limit 10]
engram reflect [--scope full]
engram forget --entity "Sarah" [--confirm]
engram status
```

### 10.4 Language SDKs

Node.js and Python packages wrapping the REST API with ergonomic interfaces. Published to npm and PyPI. SDKs are built last — after the REST API is stable.

---

## 11. Implementation Plan

### Phase 1 — Foundation

**Deliverable:** Working SurrealDB schema and storage adapter

- Define complete SurrealDB schema for all record types and graph edges
- Implement bi-temporal record patterns
- Build MemoryStore interface and SurrealDB implementation
- Write storage adapter unit tests
- Validate graph traversal queries

### Phase 2 — Memory Core

**Deliverable:** All five operations functional against the storage adapter

- Implement ingestion pipeline (normalize, extract, embed, score, write)
- Implement basic entity disambiguation (explicit track only)
- Implement retrieval orchestrator (all five memory layers)
- Implement operation surface (store, recall, reflect, forget, status)
- Integration tests across all operations

### Phase 3 — Consolidation Engine

**Deliverable:** Autonomous background consolidation running on schedule

- Implement semantic extraction pass
- Implement implicit inference pass
- Implement preference accumulation pass
- Implement implicit disambiguation track (evidence accumulation)
- Implement compression pass
- Implement concept decay pass
- Configurable scheduling per agent
- Consolidation observability (run logs, change counts)

### Phase 4 — REST API

**Deliverable:** All operations accessible over HTTP, end-to-end tested

- REST API server with all five endpoints
- API key authentication
- Tenant resolution middleware
- Request/response validation
- Rate limiting
- End-to-end integration tests

### Phase 5 — MCP Server

**Deliverable:** Engram accessible as MCP tools in compatible agents

- MCP server with five tool definitions
- Tool schema generation from operation surface definitions
- Auth and agent_id resolution from MCP client context
- Test in Claude and at least one other MCP-compatible agent

### Phase 6 — CLI

**Deliverable:** Developer-facing CLI for all operations

- All five operations exposed as CLI commands
- Config file and environment variable support
- Interactive mode for recall
- Human-readable output formatting

### Phase 7 — SDKs

**Deliverable:** Published Node.js and Python packages

- REST API wrapper with ergonomic interfaces
- TypeScript types for all inputs and outputs
- Python type hints
- Published to npm and PyPI
- Usage examples in documentation

---

## 12. Open Questions

The following decisions are deferred pending further design or prototyping:

1. **Embedding model selection** — which embedding model is used for Episode, Concept, and Entity embeddings? This affects vector dimensionality and similarity behavior throughout the system.

2. **LLM selection for pipeline steps** — the ingestion pipeline (entity extraction) and retrieval orchestrator (query classification) both require LLM calls. Which model, and how is this configured per deployment?

3. **Implicit inference rule set** — what inference rules does the consolidation engine apply? The transitive relationship example is straightforward, but the full rule set needs definition to bound scope.

4. **Disambiguation review interface** — flagged ambiguities (merges in the 0.65–0.84 confidence band) need a resolution mechanism. Is this an API endpoint, a CLI command, or deferred to the calling agent?

5. **Consolidation scheduling granularity** — is per-agent schedule configuration sufficient, or is per-user-session scheduling needed for agents serving many users?

6. **Forgetting semantics** — the current design uses tombstoning. Should there be a true hard-delete path for compliance purposes (GDPR, etc.)?

7. **Cross-agent memory sharing protocol** — the multi-agent shared memory namespace is described at a high level. The conflict resolution and access control model needs detailed design.

---

## 13. Glossary

**Agent** — an AI system that uses Engram to store and retrieve memory. The minimum required scope for all operations.

**Anchor record** — the authoritative episode record that a Tier 1 assertion produces, against which all future entity mentions are evaluated.

**Bi-temporal** — a data modeling approach tracking both when a fact was true in the world (valid time) and when it was recorded in the system (transaction time).

**Candidate entity** — an unresolved entity mention that has not yet accumulated sufficient evidence to be merged with or distinguished from existing entities.

**Concept** — a semantic fact extracted from episodes during consolidation and stored as a node in the knowledge graph.

**Consolidation** — the autonomous background process that transforms raw episodic records into structured knowledge by extracting concepts, resolving entities, performing inference, and accumulating preferences.

**Context slice** — the assembled, prompt-ready text returned by `recall()` for injection into an agent's context window.

**Disambiguation** — the process of resolving multiple references to a real-world entity into a single canonical entity record.

**Episode** — a timestamped record of a discrete event — a conversation turn, observation, document ingestion, or other unit of experience.

**Importance score** — a 0.0–1.0 float assigned to each episode at ingestion, influencing retrieval ranking and compression decisions during consolidation.

**Signal strength tier** — a five-level classification of evidence sources from Tier 1 (authoritative assertion) to Tier 5 (behavioral pattern). Determines how much weight a signal carries in confidence and disambiguation calculations.

**Valid time** — the time at which a fact was true in the world, distinct from transaction time (when it was recorded in Engram).

**Transaction time** — the time at which a record was written to Engram, distinct from valid time (when the fact was true in the world).