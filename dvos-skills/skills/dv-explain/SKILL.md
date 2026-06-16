---
name: dv-explain
description: Explain any Pragmatic Data Vault concept, pattern, rule, or modeling decision in plain language
enabled: true
---

# /dv-explain — Pragmatic DV Knowledge Base

Answer questions about Pragmatic Data Vault concepts, modeling decisions, and doctrine rules. No subagents needed — pure knowledge.

## Usage

`/dv-explain <concept>` — e.g.:
- `/dv-explain hub`
- `/dv-explain why insert-only`
- `/dv-explain multi-active satellite`
- `/dv-explain hash key`
- `/dv-explain same-as link`
- `/dv-explain PIT table`
- `/dv-explain SNOPIT`
- `/dv-explain kappa vault`
- `/dv-explain rule 07`
- `/dv-explain collision-code`
- `/dv-explain smart-keys`
- `/dv-explain namespace-id`
- `/dv-explain hub-anti-patterns`
- `/dv-explain business-key`
- `/dv-explain zero-key`
- `/dv-explain satellite-splitting`
- `/dv-explain unit-of-work`
- `/dv-explain uow`
- `/dv-explain staggered-loading`
- `/dv-explain all-the-data`
- `/dv-explain trap-reject`
- `/dv-explain dep-child-in-link`
- `/dv-explain zero-key-vs-ghost`
- `/dv-explain effectivity-sparingly`
- `/dv-explain sal-surrogate`
- `/dv-explain 3-valued-logic`
- `/dv-explain schema-evolution`
- `/dv-explain information-mapping`
- `/dv-explain semantic-layer`
- `/dv-explain semantic-model`
- `/dv-explain ddd-source-patterns`
- `/dv-explain knowledge-graph`

If the user types `/dv-explain` with no argument, ask: "What would you like me to explain?"

## Knowledge Base

### Core constructs

**Hub** — stores a unique list of business keys for one business entity. Contains only: hash key, business key, load date, record source. Nothing else. It is the "anchor of truth" for that entity.

**Satellite** — stores the context and history of a hub or link. All descriptive attributes live here. Multiple satellites per hub are encouraged (split by rate of change or source system).

**Link** — stores the relationship between two or more hubs. Keys only — no descriptive attributes. If a relationship has attributes, they go in `SAT_RV_LNK_{badge}_{file}`.

**PIT table** — a query-assist structure that pre-computes the "as-of" timestamp for each satellite attached to a hub. Makes queries dramatically faster by avoiding correlated subqueries across satellites. Two variants: Legacy PIT (2 columns per satellite: hashkey + applied_ts) and SNOPIT (1 column per satellite: integer dv_sid). Use `/dv-explain SNOPIT` for the decision guide.

**SNOPIT (Surrogate Number Only PIT)** — a compact PIT variant that replaces the two-column per-satellite projection (hashkey + applied_ts) with a single integer column (`dv_sid`). Requires `enable_dv_sid: true` on each satellite, which adds a `NUMBER(38,0) IDENTITY START 0` column. Ghost record gets `dv_sid = 0` automatically (autoincrement, not explicitly set). The SNOPIT population query uses `COALESCE(s.dv_sid, 0)` — null fills to `0`, which is the ghost row. IM view uses plain `INNER JOIN ON dv_sid` — always finds a row (`0` = ghost, `1+` = real record). No `-1` sentinel exists.

Choose SNOPIT when: compact projection, faster integer IM joins, and happy to add `dv_sid` to satellite DDL. Dynamic Tables are supported for SNOPIT.

**Why SNOPIT is faster \u2014 the physical mechanism:** Integer equijoins (`dv_sid = pit.dv_sid`) trigger Hash Joins on Snowflake, which produce a **Right Deep Join Tree** \u2014 the SNOPIT anchors at the deep right, satellites resolve as parallel hash lookups. Without a PIT, DV queries produce a Left Deep Join Tree (Sort-Merge), which executes sequentially and is dramatically slower for multi-satellite fan-out (benchmark: 8+ minutes without PIT vs. seconds with SNOPIT).

**Zone map advantage:** `dv_sid` is a linear autoincrement integer. Snowflake zone maps (min/max per micro-partition) are highly effective for linear values \u2014 partition pruning is precise. Binary hash keys are pseudo-random, causing near-zero zone map effectiveness. This is the Snowflake-specific reason SNOPIT outperforms traditional PIT in partition scan count.

**No clustering keys on satellites:** Do not add `CLUSTER BY` to satellite tables in a SNOPIT-enabled vault. The natural load order is the clustering \u2014 it is what makes DV_SID values linear and zone maps effective. Explicit clustering reorganises the physical data, scatters DV_SID values, and destroys the zone map benefit. See `/dv-pit-bridge` for the full SNOPIT performance analysis.

Choose Legacy PIT when: satellites already exist without `dv_sid`, or you have multi-active satellites and need all active sibling rows visible per snapshot (SNOPIT collapses them to a single row via `MIN(dv_sid)`).

A hub can have both: a SNOPIT for the batch IM and a legacy PIT Dynamic Table for operational dashboards.

**Bridge table** — a query-assist structure that pre-joins a hub to one or more related links and their connected hubs. Snapshot-based. Not a real vault layer — it's a view optimization.

### Satellite variants

DVOS supports 8 satellite types. PII is a **naming suffix** applied to any satellite type, not a separate type.

**Standard satellite** — one active row per business key at any point in time. No end-date — current row via `QUALIFY ROW_NUMBER()`. Most common type. Manifest `type: standard`.

**Multi-active satellite** — multiple rows can be active simultaneously for the same business key (e.g. multiple phone numbers, multiple addresses). Composite PK includes `dv_sequence` (synthetic discriminator). Manifest `type: ma`.

**`dv_sequence` column — critical rules**

The `dv_sequence` column is an auto-generated internal counter added to the MSAT composite PK. It exists solely to make the table structure uniquely indexable. Four rules govern its use:

1. **Not included in `dv_hashdiff`** — the sequence is an arbitrary ordinal, not a business attribute. Including it in the record hash would make every row appear unique by default, defeating SET-level change detection.
2. **Never exposed to business users or BI tools** — `dv_sequence` has no business meaning (same rule as hash keys). IM views must not include this column.
3. **May change when the SET is superseded** — when a SET change is detected and a new SET is inserted, the same record that existed in the old SET may receive a different sequence number in the new SET. Do not use `dv_sequence` to track a single record's identity across time.
4. **Never use `dv_sequence` to join to a single record** — it is not a stable identifier for any individual row.

**MSAT vs. dep-child key satellite — SET tracking vs. individual record tracking**

These two patterns are often confused. The functional distinction is critical:

| | **Multi-active satellite (MSAT)** | **Dep-child key satellite** |
|---|---|---|
| **What is tracked** | The **entire SET** of records for a parent key | Each individual `(parent_key, child_key)` combination |
| **When is a new row inserted** | When **ANY record in the SET changes** (or the count changes) — the entire set is historised as a new state | Only when **that specific record** changes — other records in the same parent are unaffected |
| **Use when** | The business cares about the SET as a whole (e.g. "did the complete set of addresses for this customer change?") | Items change independently; individual item history is needed |
| **LoadHashDiff** | Hash of ALL attribute values for ALL records in the set, combined | Hash of the individual row's attributes only |

**MSAT as debt-masking anti-pattern**

MSAT is sometimes reached for as a quick fix when a source delivers overloaded or malformed data that cannot (yet) be cleanly split into meaningful hub/satellite structures. This is **using MSAT to mask technical debt** — not a genuine SET semantics requirement. When MSAT is used this way, it prioritises convenience over correctness and defers the upstream data quality problem rather than solving it. If MSAT is not genuinely needed for SET-level change detection, the correct response is to escalate to the source team and solve the data quality problem at origin.

**Do not default to MSAT.** On the spectrum of satellite table choices, MSAT is at the extreme end — the use case is very specific (tracking changes to a SET of active records for a parent entity). A dep-child key satellite can look similar but is not MSAT — it tracks changes to a record at finer grain, not changes to a set. Profile the source data to confirm SET semantics are genuinely required before choosing MSAT.

**Partitioned Multi-Active Satellite (PMAS)** — an advanced pattern that combines MSAT and dependent-child key, used when a parent entity has **multiple independent subsets of multi-record state** that must each be versioned independently. The subsequence key is scoped to `(hash-key, dep-child-key)` rather than just `hash-key`:

| Pattern | Subsequence key scope | What it tracks |
|---|---|---|
| Standard MSAT | Per hash-key | Changes to the entire set of active records for a parent entity |
| Dep-child satellite | Per `(hash-key, dep-child-key)` — **single row** | Changes to an individual row identified by (hash-key, dep-child-key) |
| **PMAS** | Per `(hash-key, dep-child-key)` — **sub-SET** | Changes to a SET of records within each dep-child partition |

**All three have the same PK: `(hashkey, dv_sequence, dv_load_timestamp)`**. The dep-child key is a regular NOT NULL column — it is NOT part of the PK. What differs is the **load logic** (change detection scope):

| Variant | Change detection scope | When to re-insert |
|---|---|---|
| MSAT | Per hashkey (full SET) | Any hashdiff change OR count change in the full set → insert new full SET |
| Dep-child | Per (hashkey, dep_child_key) as a ROW | Hashdiff change for a specific (hashkey, dep-child-key) → insert one new row |
| PMAS | Per (hashkey, dep_child_key) as a SET | Any hashdiff/count change within the (hashkey, dep-child-key) partition → insert new sub-SET |

When to use PMAS: when a parent entity has dependent codes (e.g. chart of accounts sub-codes, hospital diagnosis subcodes, manufacturing part numbers) that each carry their own multi-record active state, and changes to one subset must not trigger versioning of another. These codes depend on a parent key for uniqueness — they are not standalone hub business keys.

PMAS naming convention: `sat_pma_{rv|bv}_{hub|lnk}_{{source-badge}_{source_table}|{concept}}`

**This is an advanced use case — not the default.** Profile upstream data before choosing PMAS. It does not deviate from Pragmatic DV standards; it combines existing MSAT and dep-child patterns into a single satellite structure with a standardisable load pattern.

**Hybrid Satellite Table** — an **Operational Data Vault (ODV)** satellite variant built on Snowflake's Hybrid Tables (HTAP: Hybrid Transactional/Analytical Processing). Used when an application needs sub-300ms latency and up to 1000 TPS directly from vault data, without shipping data to a separate OLTP database. This is not the "source-system DV anti-pattern" — it is the part of DV that serves low-latency operational data needs.

**When to use:** only when an application genuinely requires OLTP latency and concurrency. Do not make all vault tables hybrid — traditional Snowflake tables are cheaper and appropriate for analytical-only workloads.

| Property | Hybrid Satellite | Regular Satellite |
|---|---|---|
| PK enforcement | Enforced: `(hashkey, dv_loaddate)` required | Declared, not enforced |
| FK to parent hub | Enforced (or include BK in satellite to avoid FK) | Declared, not enforced |
| Hash keys | **Not recommended** — generate latency at insert time | Standard |
| Row locking | Inherited (unlike traditional Snowflake tables) | None |
| Storage cost | Higher — dual storage (blob + block) | Standard (blob only) |
| Input sources | ETL bulk load AND live OLTP inserts (only satellite type that accepts both) | ETL only |

**Dep-child and MSAT variants** are supported: dep-child PK includes the dep-child key; MSAT PK includes the generated sub-sequence number.

**FK from hybrid → regular table:** a FK defined from a hybrid satellite to a traditional Snowflake hub IS enforced at DML time, even though the hub's own declared constraints are not.

**Limits (default / scalable upon request):** 100GB / 5TB table size; 1000 / 5000 TPS; 300ms / 30ms latency.

**Trade-off principle:** weigh the cost of dual storage and row-locking overhead against the cost of ETL latency from an external OLTP platform. Only apply to the portions of the vault that genuinely need real-time access.

**Effectivity satellite** — **Link-only** (never off a hub). Uses `dv_start_date` and `dv_end_date` columns populated by the loader from driver-key staging. Insert-only — never updated. The `dv_end_date` is set to the configured high-date for open/active records. Requires `driver_keys` config. Has **no business attributes**. Answers "was this link active at time T?" Doctrine rule DV-EFS-001 enforces `dv_start_date`/`dv_end_date` physically present; forbids `ACTIVE_FLAG`, `dv_startts`, `dv_endts`, UPDATE, MERGE, DELETE. Manifest `type: ef`.

**⚠️ Common misconception — effectivity satellite ≠ virtual end-date view.** A virtual end-date (`LEAD()` SQL view over a satellite) is query-time computation, not an effectivity satellite. An effectivity satellite has a **physically persisted** `dv_end_date` column set at staging time, never computed at query time and never updated.

**Primary use case — flip-flopping relationships.** The scenario that justifies an effectivity satellite: when a driver key **returns to a previous relationship** (e.g. Account A linked to Product X → then to Product Y → back to Product X). This round-trip is impossible to reconstruct from the link table alone. The effectivity satellite, keyed by the driver key, is the only way to track and replay this history.

**Driver key is not designated in the link table.** The link table has no column or attribute marking which participant is the driver key — that would make the link single-purpose and unscalable. The driver key designation exists only in staging logic. Multiple effectivity satellites with different driver keys can be built on the same link table without changing the link DDL.

**Never add start/end dates to the link table itself.** Effectivity tracking belongs in a separate satellite — not in the link table. Adding start/end dates to the link destroys the link's scalability and makes it single-purpose.

**Second-level staging requirement.** Effectivity satellite staging cannot be mixed with regular staging. A separate second-level staging step is required that: (a) queries the target effectivity satellite for currently active records by driver key, (b) infers relationship closures when the staged relationship differs from the active record, (c) generates close records that do not exist in the source application. Once second-level staging completes, the load step is identical to a regular satellite loader (HashDiff comparison, insert if different).

**Zero-key for M:0 — relationship termination.** When a driver key is no longer related to any business object (zero cardinality), assign the **zero-key** (all-zeros binary hash) as the non-driver participant in the effectivity satellite. This creates an explicit, auditable record that the relationship terminated — without removing any data.

**Dependent-child satellite** — used when the parent hub key alone doesn't uniquely identify a row. A child key (e.g. order line number) provides sub-grain identity within the parent. Manifest `type: dp`.

The dep-child key is a regular NOT NULL column — it is **NOT** part of the PK. PK: `(hashkey, dv_sequence, dv_load_timestamp)` — identical structure to MSAT and PMAS. What differs is the load logic: change detection is scoped to `(hashkey, dep_child_key)` per ROW (not per SET like MSAT). Ghost record: dep-child key = NULL (no bogus sentinel needed since it's not in the PK).

**Dep-child key for intraday loading**

A named use case of the dependent-child satellite: when a source delivers **multiple records per entity within the same business day** (intraday events), use the **business event datetime** as the dependent-child key. Each event gets its own satellite row keyed by `(parent_hashkey, event_datetime)`. This enables fine-grained intraday change tracking without artificial sequence numbers and without confusing multiple same-day events as a single state change.

When to use this pattern:
- Source delivers multiple state updates per entity per batch window (e.g. order status changes multiple times per day: `PENDING → PROCESSING → SHIPPED`)
- Standard satellite would only capture the last state in the window if multiple rows share the same `dv_applied_timestamp`
- The business event datetime is supplied by the source and is reliable (not a system-generated load time)

The dep-child key must be a **source-supplied business datetime** — never a vault-generated metadata column like `dv_load_timestamp`. Using `dv_load_timestamp` as the dep-child key makes every record unique by definition, defeating change detection and creating infinite satellite growth.

**Non-historized satellite** — no `dv_hashdiff`; no deduplication. Two distinct use cases:

1. **Reference/lookup data** — the business treats the data as "always current" with no history required (e.g. country codes, product categories, reference tables). Every load inserts a new row; the current view (`VC_*`) surfaces only the latest via `QUALIFY ROW_NUMBER() = 1`. Manifest `type: nh`.
2. **High-frequency real-time / streaming events** — the data producer delivers individual event records continuously where every new row is a true change by definition. Checking hashdiff against a prior state is redundant because the nature of the feed guarantees novelty. The data product's value decays rapidly unless acted on in real time (e.g. IoT sensor readings, clickstream events, payment authorisations).

**⚠️ NSAT is NOT intended for file-based (batch) workloads.** In a batch context (daily file drop, ERP extract), the source may re-send unchanged records with each extract. Without hashdiff comparison, every resent record would be loaded as a duplicate. Use a standard satellite with hashdiff for any batch or file-based source. Only use NSAT for streaming/event-driven sources where the pipeline guarantees that every delivered record is genuinely new.

**Non-historised link (NHL) — streaming counterpart to NSAT**

When a streaming source delivers transactional events with **exactly-once semantics** (e.g. a Kafka topic with guaranteed delivery), the same principle applies to link tables: because the upstream application or message broker guarantees true-change delivery, no record hash is needed on the link. A **non-historised link** (NHL) is a link table without a `dv_hashdiff` column, used for high-velocity streaming event ingestion.

| | Standard link | Non-historised link (NHL) |
|---|---|---|
| Record hash | Yes — deduplication via hash | None — source guarantees true changes |
| Use for | Batch / file-based sources | Streaming / exactly-once event sources |
| Insert pattern | Anti-semi join (MERGE / WHERE NOT EXISTS) | Direct INSERT (source manages uniqueness) |

Like NSAT, NHL should **not** be used for batch workloads. If a streaming source does not guarantee exactly-once delivery, use a standard link with hash comparison.

**Status tracking satellite (STS)** — records INSERT/UPDATE/DELETE status changes to a business entity or relationship over time, using a secondary staging view. Manifest `type: st`. Used to mark entities as *missing* or *deceased* when they stop appearing in source feeds (see `last_seen_date` and entity aging). Also used to block re-insertion of GDPR-erased entities if they reappear in a subsequent source extract.

**Record tracking satellite (RTS)** — tracks whether a record exists (present/absent) in the source per `dv_applied_timestamp`. Manifest `type: rt`. Primary instrument for the entity aging rule: by comparing the latest RTS entry against the current date, a Business Vault rule can determine how long an entity has been absent from source. Also used to **prevent accidental GDPR reappearance** — after an erasure is processed, RTS detects a reappearing entity and blocks re-insertion into the vault.

**Extended tracking satellite (XTS)** — advanced pattern for file-based ingestion with timeline correction. Requires XTS config. Manifest `type: xt`.

**Supernova** — a 5-layer data modelling pattern (Rick F. van der Lans, ~2015) that pre-materialises the satellite join above the Raw Vault as INCREMENTAL Dynamic Tables. Eliminates query-time joins in the IM layer. Implemented in a dedicated `supernova` schema. See `/dv-supernova` for the full pattern.

**PII naming suffix** — any satellite type can have a `_pii` suffix in its name to segregate sensitive columns into a separate physical table with independent access control. Not a distinct `type` value in the manifest.

**Why PII satellite isolation works so well in a vault — the rarely-changes property**

PII attributes (legal name, date of birth, national ID, passport number, home address) are **identifying attributes that almost never change** over an entity's lifetime. This means a PII satellite typically has **only one or two rows ever loaded** per business key — far fewer than a transactional satellite. This has two important vault implications:

1. **Minimal storage, no complex QUALIFY logic** — the PII satellite is the thinnest satellite in the vault. Current-row retrieval with `QUALIFY ROW_NUMBER() = 1` works fine but is rarely even needed in practice since there is typically only one row.
2. **Natural fit for XTS disposal on GDPR Article 17 (right to erasure)** — because the PII satellite has so few rows per entity, the XTS disposal pattern (marking rows as erased via the extended tracking satellite) can cleanly suppress the PII data without needing a full satellite reload. The vault's insert-only audit trail is preserved for non-PII satellites while the PII satellite rows are logically hidden from all downstream queries. See `/dv-explain XTS` for the disposal pattern.

**Satellite splitting and dep-child key grain:** when a source table has dep-child keys (e.g. transaction ID, order line number), not every split satellite needs to track at that grain. The PII satellite for the same source typically should **not** be given the dep-child key — identifying attributes (name, DOB, national ID) are properties of the entity, not of the transaction. Applying the dep-child key to a PII satellite would create one PII row per transaction per entity — inflating the PII satellite for no governance benefit and complicating GDPR erasure management.

### Key concepts

**Hash key** — hash of the business key concatenated with the `dv_collisioncode` (BKCC — Business Key Collision Code) and, when multi-tenancy is enabled, `dv_tenant_id`. **Record source is NOT part of the hash key.** BKCC is a short stable discriminator (e.g. `'default'`, `'zoho'`) that distinguishes overlapping key spaces from different source systems.

**Hash algorithm** is project-configured. The chosen algorithm applies to all hashkeys and hashdiffs in the project — do not mix.

| Algorithm | Function | Column type | Ghost hex | When to use |
|---|---|---|---|---|
| MD5 | `MD5_BINARY(...)` | `BINARY(16)` | `REPEAT('0', 32)` | Small key spaces, legacy. Known collision risk. |
| **SHA1** | `SHA1_BINARY(...)` | `BINARY(20)` | `REPEAT('0', 40)` | **Default.** Recommended for most data vaults. |
| SHA256 | `SHA2_BINARY(...)` | `BINARY(32)` | `REPEAT('0', 64)` | Large key spaces, high-security/regulatory environments. |

Formula depends on `tenant.enabled` in the manifest:
- Multi-tenancy enabled: `hash_fn(UPPER(CONCAT(tenant_id || '||' || bkcc || '||' || COALESCE(NULLIF(TRIM(bk), ''), '-1'))))`
- Multi-tenancy disabled: `hash_fn(UPPER(CONCAT(bkcc || '||' || COALESCE(NULLIF(TRIM(bk), ''), '-1'))))`

Default values: `dv_tenant_id = 'default'`, `dv_collisioncode = 'default'`. Per-source overrides (e.g. `bkcc_value: zoho`, `tenant_id_value: fraud`) are set in the manifest hub sources.

**Hash diff (dv_hashdiff)** — hash of all tracked attribute columns concatenated. Used in the satellite load pattern to detect whether a row has changed without comparing every column individually. Column name is `dv_hashdiff`. Algorithm matches the project's configured hash algorithm.

**Hashdiff as schema auto-migration engine** — when a new column is added to a satellite's staging view hashdiff computation, the hash value changes for every entity. On the next normal load, the anti-semi join (`WHERE NOT EXISTS hashdiff = ...`) finds no matching row for any entity, and new state records are inserted for all of them — capturing the new column's value. The satellite migrates itself through the normal load process. No replay or reload is needed. This is why schema evolution in Data Vault is non-destructive: the hashdiff change *is* the migration event.

**dv_load_timestamp** — when the record was loaded into the vault. Not the business date — the technical load time. Every table has this. Column name is `dv_load_timestamp`.

**dv_applied_timestamp** — the business time the data was produced at the source. Distinct from `dv_load_timestamp` (when it was loaded into the vault). Used for ordering within a load batch.

**Streaming vs. batch applied timestamp:**
- **Event-sourced / streaming** — `dv_applied_timestamp` is the **event timestamp from the source event itself**: the exact moment the business event occurred in the operational system. Each event row carries its own high-resolution timestamp.
- **Batch / file-based** — `dv_applied_timestamp` is the **extract timestamp**: when the source system produced the extract file. All rows in a given batch file typically share the same applied timestamp.

**Bi-temporal correction — same applied timestamp + newer load timestamp = authoritative version**

When a source replays a corrected event (e.g. a price calculation was wrong and is reprocessed), the corrected record arrives with the **same `dv_applied_timestamp`** as the original (it represents the same business-time event) but a **newer `dv_load_timestamp`** (it was loaded into the vault later). The vault inserts both rows — the original and the correction — and no record is ever deleted.

To retrieve the authoritative (most recent corrected) version for a given business-time event:

```sql
-- Retrieve the authoritative version for each (entity, applied_timestamp) combination
SELECT *
FROM SAT_RV_HUB_<ENTITY>_<CONTEXT>
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dv_hashkey_hub_<entity>, dv_applied_timestamp
    ORDER BY dv_load_timestamp DESC        -- newest load = authoritative correction
) = 1;
```

The previous (erroneous) version remains permanently in the vault — auditable proof that the correction was made. The two timestamps together form the **bi-temporal record**: applied timestamp answers "when was this true in the business?"; load timestamp answers "when did we know about it?" Querying both together exposes the full correction history for any entity at any point in time.

**Applied timestamp as a "package of time"**

A useful mental model: an applied timestamp is not simply a single datetime — it is a **package** grouping all the business data that was true at that moment. Business events come in three temporal forms, all of which are captured under one applied timestamp:

| Type | Description | Examples |
|---|---|---|
| **Discrete event** | Something that happened at a specific instant | A payment made, an order placed, a status change |
| **Recurring timestamp** | Something that happens periodically on a schedule | Monthly statement generation, daily account snapshot, quarterly valuation |
| **Evolving timestamp** | Something ongoing with a duration | Active policy state, current contract terms, open loan |

The load timestamp is then the **version of that package** — when the vault received and recorded it. If the source re-delivers a corrected version of the same business moment (same applied timestamp), the vault records a new load timestamp for the same package. This is why `ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC` always retrieves the current authoritative state: latest package first, then latest version of that package.

**dv_recordsource** — identifies which source system produced this record. Column name is `dv_recordsource`. Every table has this. Enables traceability and multi-source integration.

**`dv_task_id`, `dv_jira_id`, `dv_user_id` — agile delivery traceability**

Every vault row can be tagged with the operational context of the load that produced it:

- **`dv_task_id`** — the pipeline task/job ID that loaded this record. Enables correlation between vault rows and pipeline execution logs.
- **`dv_jira_id`** — the Jira story, epic, or task ID that delivered the data pipeline loading this record. In an agile enterprise, every data requirement starts as a Jira ticket. Tagging vault rows with `dv_jira_id` means you can trace any row back to the agile story that caused it to be loaded — audit trail from data record to delivery ticket.
- **`dv_user_id`** — the service account or user that ran the load. Identifies who (or which process) loaded this record.

These three columns are optional per row (can be `NULL` for automated pipelines that don't set them), but their presence enables surgical investigation: given any suspicious record, you can identify exactly which pipeline run, which Jira story, and which account produced it.

**Insert-only loading** — the vault never updates or deletes raw data. New versions are appended. History is implicit in `dv_load_timestamp` / `dv_applied_timestamp` ordering. **There is no end-date column (LEDTS) in DVOS.** Current row is retrieved in views via `QUALIFY ROW_NUMBER() OVER (PARTITION BY parent_hk ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1`. This is the single most important operational rule.

**Virtual end-date view pattern (`LEAD()`)**

Because Pragmatic DV satellites have no physical end-date column, downstream consumers that need to query history with a time-range predicate ("what was the state at time T?") require a virtualised end-date. The approved pattern is a view over the satellite that uses `LEAD()`:

```sql
CREATE OR REPLACE VIEW SAT_RV_HUB_CUSTOMER_V AS
SELECT
    dv_hash_key,
    dv_applied_timestamp                                         AS dv_start_timestamp,
    LEAD(dv_applied_timestamp, 1, '9999-12-31'::TIMESTAMP_NTZ)
        OVER (PARTITION BY dv_hash_key ORDER BY dv_applied_timestamp) AS dv_end_timestamp,
    dv_load_timestamp,
    dv_recordsource,
    -- business attributes
    customer_name,
    customer_status
FROM SAT_RV_HUB_CUSTOMER;
```

This view is created immediately after the satellite table and never needs to be rebuilt — it is always current. Consumers can then filter with `WHERE dv_start_timestamp <= :query_ts AND :query_ts < dv_end_timestamp`.

**QUALIFY vs. LEAD() — when to use each:**
- `QUALIFY ROW_NUMBER()` — use in IM views that only need the **current** state of each entity (the most common case)
- `LEAD()` view — use when the consumer needs to query **any point in time** across the full satellite history (e.g. regulatory snapshots, as-of reporting)

**Hub/link MERGE exception** — hubs and links use `MERGE` with `WHEN NOT MATCHED THEN INSERT` + `WHEN MATCHED THEN UPDATE SET last_seen_date`. This is the only UPDATE permitted in the vault. It tracks when a business key was last seen in a source delivery. Satellites remain purely INSERT-only with no MERGE.

**`last_seen_date` and entity aging** — `last_seen_date` on every hub and link is the primary mechanism for detecting aged or missing entities. When a source stops sending a business key, the hub row stops being updated. A Business Vault rule compares `last_seen_date` against a threshold:
1. Key not seen for ~1 month → flag as *missing* (BV status satellite: `'M'`)
2. Key not seen for ~2 months, upon business agreement → mark as *deceased* (`'D'`)
3. If the entity reappears in a future source extract → mark as *reanimated* (`'R'`) and investigate

This is sometimes called the **"declared dead in absentia"** pattern — a business rule asserts death despite no explicit deletion event from the source. The threshold is project-configurable; the ~2-month convention is a starting point. Implement as a BV satellite (`SAT_BV_{ENTITY}_LIFECYCLE`) with a status value column. See `/dv-bv` for the aging rule as a BV pattern.

**Anti-semi join pattern** — the standard satellite load query uses `WHERE NOT EXISTS (SELECT 1 FROM <target> WHERE hash_key = ... AND hashdiff = ...)` instead of MERGE. Simpler, cheaper, and safe for insert-only semantics. Hubs and links use MERGE instead (see above).

**Critical: compare against CURRENT state, not full history**

The NOT EXISTS subquery must compare against the **current (most recent) row per entity**, not all historical rows. The correct pattern uses `QUALIFY ROW_NUMBER()` inside the NOT EXISTS subquery to isolate the current row:

```sql
WHERE NOT EXISTS (
    SELECT 1 FROM (
        SELECT hash_key, dv_hashdiff
        FROM <satellite>
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY hash_key
            ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
        ) = 1
    ) cur
    WHERE cur.hash_key     = src.hash_key
      AND cur.dv_hashdiff  = src.dv_hashdiff
)
```

**Why this matters — state reversion:** A business entity can revert to a previous state (A → B → A). If the NOT EXISTS compared against ALL historical rows, the third load (back to A) would find hashdiff-A already exists in history and skip the insert. That is **wrong** — the entity genuinely changed, and the transition must be recorded. By comparing only against the current state, the load correctly detects that the current state is B and inserts the new A row, preserving the full A → B → A history.

**Ghost record** — a row with all-zero hash key inserted into every satellite. Required for PIT tables to perform null-safe joins when no satellite record exists for a given snapshot date.

**Same-as link** — a raw vault entity (not a business layer concept) that connects two records in the same hub that represent the same real-world entity. Lives in the raw vault alongside hubs, links, and satellites. Insert-only, always paired with an effectivity satellite that tracks whether the match assertion is currently active. The SAL does not merge records — it asserts identity; survivorship logic (which record "wins") lives in the Information Mart. Naming: `SAL_<ENTITY>`. SAL hash key is computed from both hub hash keys (not record source).

**Same-as and hierarchical links are logical labels, not distinct physical structures.** There is no special DDL that distinguishes a "hierarchical link" from any other link. The distinction is semantic:
- A **same-as link** connects two hub keys that represent the same real-world entity across different sources (de-duplication / identity assertion)
- A **hierarchical link** connects two keys *within the same hub* where one record is a child of another in the same entity type (e.g. employee → manager, part → sub-part)

Both are implemented as standard link tables where both FK columns point to the same hub. The "type" label is documentation and naming convention only — a flag satellite or effectivity satellite can further qualify the nature of the relationship.

**Record source granularity** — record source should be as specific as useful: `CRM.SALESFORCE.ACCOUNTS` not just `CRM`. Enables surgical re-loading and audit.

### Why insert-only? — and what happens when data "dies"

Data Vault is insert-only even for data that becomes stale, superseded, or legally required to be erased. **Data is never physically deleted from the vault.** Instead, satellite structures record all lifecycle states including death:

| Event | Vault response |
|---|---|
| Entity stops appearing in source | `last_seen_date` stops updating; aging BV rule flags as missing/deceased |
| Relationship ends | Effectivity satellite: `dv_end_date` set to the closure timestamp |
| Source feed retired intentionally | STS records the final status; artefact decommissioned (see `/dv-deploy`) |
| GDPR erasure request | XTS disposal columns + RTS to prevent reappearance |
| Data record deleted in source | RTS records absence; STS records `'D'` status |

The consequence is that an IM view can always be rebuilt from vault history regardless of what has "died" in the source. The vault is the corporate memory; deletion events in the source are just another state transition.

### Why insert-only?

Four reasons:
1. **Auditability** — every version of every record is preserved. You can reconstruct any state of the business at any point in time.
2. **Parallelism** — inserts never block each other. Concurrent loads from multiple sources don't need locks.
3. **Simplicity** — `INSERT WHERE NOT EXISTS` is simpler, faster, and less error-prone than MERGE.
4. **Snowflake micro-partition efficiency** — on Snowflake, every UPDATE causes the old record's micro-partitions to be committed to Time-Travel and Fail-Safe storage. On a high-churn satellite this inflates storage costs significantly. INSERT-only avoids this entirely: no UPDATE means no time-travel/fail-safe bloat, and the satellite table grows only by appending new micro-partitions. The table also naturally self-clusters by load timestamp — no `CLUSTER BY` maintenance needed.

### Snowflake Time-Travel ≠ DV satellite history

**Time-Travel is a live backup, not the vault's historical record.**

| | Snowflake Time-Travel | DV satellite history |
|---|---|---|
| Purpose | Infrastructure recovery (accidental drop, load error) | Business audit trail, regulatory compliance, as-of reporting |
| Retention | 0–90 days (permanent tables); 0–1 day (transient) — then purged | Indefinite — never deleted from the vault |
| Access | Snowflake extended SQL (`AT`, `BEFORE`) | Standard SQL against satellite tables or the LEAD() view |
| Governed by | `DATA_RETENTION_TIME_IN_DAYS` table parameter | Vault insert-only doctrine |

Teams that rely on Time-Travel as their historical record are structurally at risk: after 90 days (or 1 day for transient tables) the data is permanently gone. The satellite is the corporate memory. Time-Travel is a safety net for operational accidents.

### When to split a satellite? — the decision rule

The sharper question is: **"Does this attribute exist without the other business object in this interaction?"**

- **Yes** — the attribute describes the business object regardless of context → it belongs in a **hub satellite**.
- **No** — the attribute only makes sense because of the relationship → it belongs in a **link satellite**.

Example: a customer's credit rating exists whether or not they have a transaction. It goes in `SAT_RV_HUB_CUSTOMER`. A transaction fee only exists because a customer had a transaction. It goes in `SAT_NH_RV_LNK_CUSTOMER_TXN`.

Additionally split when:
- Attributes come from different source systems
- Attributes change at very different rates (e.g. demographic vs. transactional)
- Some attributes are PII and need access control
- The satellite has more than ~30 columns (performance and maintainability)

Benefits of correct splitting: isolated state change detection per entity, no attribute replication across every transaction row, natural PII isolation (GDPR article 17), dependent-child key support.

**"All the data all the time"** \u2014 a core DV principle. If a source has 600 columns, split them appropriately and load all of them. Do not cherry-pick columns. Reasons: (1) full auditability requires the ability to recreate the source record at any point in time, (2) regulatory compliance may require columns you didn't think you needed, (3) the cost of re-loading from source to add missed columns is high. Load everything; split appropriately; let the satellite structure provide access control.

**`SELECT DISTINCT` smell \u2014 link satellite misplacement diagnostic**

If an IM query must use `SELECT DISTINCT` on a business key column to get entity-level grain from a satellite, that satellite is attached to the wrong parent. The content belongs in a hub satellite, not a link satellite. The `SELECT DISTINCT` is the workaround for a modelling mistake: the satellite was placed at the link grain but carries attributes that only describe one of the link's participant entities.

Correct response: move the entity-level attributes to the appropriate hub satellite. The link satellite should contain only attributes that describe the relationship itself — attributes that do not exist without both participant entities being present.

### When to deploy a link satellite

A **link satellite** is any satellite type that hangs from a link instead of a hub. All satellite variants (standard, dep-child, MSAT, PMAS, NH, PII, effectivity) can be deployed as link satellites — the variant selection logic is identical to hub satellites. The Pattern Recommender decision tree applies the same way.

**The distinguishing question is not "what variant?" but "does the attribute describe the relationship or one of the participants?"**

Deploy a link satellite when:
- The attribute only exists **because of the interaction** between the participating entities (e.g. transaction amount exists because a customer transacted with an account)
- The attribute would lose meaning if either participant entity was removed from the context
- The source record represents an **event** or **transaction** that involves multiple business objects

Deploy a hub satellite instead when:
- The attribute describes a single entity regardless of its relationships (e.g. customer name, account type)
- The attribute exists whether or not the entity has any interactions with other entities

**All satellite variants on a link:**

| Variant | Example use case on a link |
|---|---|
| Standard link-sat | Contract terms between two parties (vendor ↔ retailer) — changes tracked via hashdiff |
| Dep-child link-sat | Individual transactions on a customer-account relationship (transaction_id or event_datetime as dep-child key) |
| MSAT link-sat | Multiple simultaneous discount tiers between supplier and retailer — the SET of active tiers is versioned |
| PMAS link-sat | Multiple line items per order (dep-child = line_number), each with concurrent pricing rules (dv_sequence within line) |
| NH link-sat | Current delivery SLA between vendor and warehouse — latest value only, no history |
| PII link-sat | Signed contract with personal guarantor details — segregated for access control |
| Effectivity | Relationship lifecycle (start/end/restart) — no business attributes |

**Transactions are link satellites because:** a transaction stipulates a relationship between 2+ entities (customer ↔ account, buyer ↔ seller). The measures (amount, quantity, fee) describe the transaction event — not either entity alone. They naturally belong on the link satellite, not a hub satellite.

**There is no special "fact satellite" type.** When a link satellite carries measures (amounts, quantities, counts), it is simply a standard or dep-child link satellite that happens to contain additive/semi-additive metrics. The term "fact satellite" is not used in DVOS — it's just a link satellite with measures. These link satellites often drive fact bridge construction (see `/dv-pit-bridge`).

### 3-valued logic and satellite schema evolution

When a source system adds a new column and you extend an existing satellite, **add the column — do not reload the satellite**.

SQL has three truth values: TRUE, FALSE, and NULL. In a satellite that has been extended with a new column:
- `NULL` in an original row = **the column didn't exist at the time that row was loaded** — this is not the same as a value being null
- `NULL` in a new row = **the value was genuinely absent in the source**

Reloading the satellite to backfill NULLs destroys this distinction and corrupts the audit trail. The correct approach:

```sql
-- Source adds new column
ALTER TABLE LIB_PRD01_EDW.SAL.SAT_RV_HUB_CUSTOMER_DEMOGRAPHICS
    ADD COLUMN loyalty_tier VARCHAR(50);  -- NULL in all historical rows = column was absent

-- From next load onwards, staging view computes and passes loyalty_tier
-- Historical rows carry NULL to signify absence, not a null value
```

Additionally, **never rename columns in a raw vault satellite**. Column renaming creates technical debt — downstream views, staging hashdiff computations, and IM projections all break. If the source renames a column, add the new column and deprecate the old one by leaving it populated from historical loads only.

**When schema flexibility is deliberately required — OHS VARIANT satellite**

When a source system's schema changes are frequent, uncontrolled, or intentional (e.g. a versioned SaaS API, partner event feed, or JSON webhook), the structured schema approach above may be impractical. In these cases, apply the **Open Host Service (OHS)** satellite pattern: store the full source payload as a Snowflake `VARIANT` column; only the stable key and metadata columns are typed in the DDL. The VARIANT body evolves without any satellite DDL change. IM consumers extract only the fields they need via Snowflake semi-structured path notation.

### Hub anti-patterns

Four named anti-patterns that produce technical debt and incorrect models:

| Anti-pattern | Description | Correct approach |
|---|---|---|
| **Weak hub** | Hub designed around a join key rather than a business object the business itself recognises and refers to by that key | Confirm the entity with a business owner — if no one in the business uses that key to refer to something, it is not a hub |
| **Keyed-instance-hub** | Date, timestamp, or other non-identifier data included in the hub business key (e.g. `customer_id + effective_date` as the BK) | Dates are attributes, not identifiers — they belong in a satellite. If a snapshot per date is needed, use a dependent-child key or a link |
| **Reference code as business key** | Lookup/reference codes (status codes, type codes, short categorisation values) loaded as hub business keys | Reference data belongs in a satellite or used as an enrichment dimension — not a hub. Reference codes may be appropriate as dependent-child keys on a link |
| **Numeric business key** | Business key column defined as `INT`, `NUMBER`, or `BIGINT` — even when the source stores it as a number | Business keys are always `VARCHAR`. A numeric ID in the source is still a string identifier for modelling purposes. Numeric types are fragile (zero-padding, leading zeros, type coercion) |
| **Overloaded BK column (Bag of Keys)** | A single column contains multiple entity types — e.g. both account IDs and customer IDs in the same column, distinguished by a type code. A hub named `HUB_ENTITY` that needs a `type_code` to give the key meaning is a Bag of Keys anti-pattern | The type code reveals that the key only has meaning relative to a category — this is a dependent-child key, not a hub BK. Resolution preference: (1) ask the source to split into independent files per entity type, (2) if source can't change, pre-stage split with error traps |
| **Concatenated composite key** | Combining multiple key components into a single BK column to avoid modelling a proper composite key hub (e.g. storing `branch_code \|\| account_number` as a single `account_key` string in `HUB_ACCOUNT`) | Model the composite key correctly as a multi-column BK in `HUB_BANK_ACCOUNT`. Never concatenate composite keys to force them into a single-column hub. **Never leave integration debt to the presentation layer** — if every IM query must de-concatenate the key to get the components, the integration debt is being paid repeatedly in every query instead of once at modelling time. |

The last point is especially important: **business keys are always, always, always VARCHAR**. A numeric key that looks like a number is still a label — it should be defined as a string, even if that feels counterintuitive.

### Business key — properties and passive integration

A business key has five properties:

1. **Universally agreed** — the business itself uses this key to refer to the entity across all systems and processes
2. **Immutable** — it does not change over the entity's lifetime; if it changes, a new hub record is created
3. **String data type** — always VARCHAR, regardless of whether the source stores it as a number
4. **Not a surrogate key** — surrogate keys (auto-incremented integers, sequence IDs) are implementation artefacts; they can be reloaded and resequenced. Not stable.
5. **Not a government-issued identifier** — SSNs, passport numbers, and tax IDs are PII and belong in a PII satellite, not a hub business key

**Passive integration — two distinct mechanisms**

Passive integration is the vault's ability to integrate data from multiple sources without explicit transformation or mapping code. There are two mechanisms, and they are often confused:

**Data Vault is not Master Data Management (MDM)**

Passive integration via shared business keys is sometimes confused with MDM. They are different systems with different purposes:

| | Data Vault (passive integration) | MDM |
|---|---|---|
| **Role** | Records what source systems say, as they say it | Actively manages and governs the authoritative record |
| **Key assignment** | Business keys come from source systems — the vault records them as-is | MDM assigns and enforces canonical keys across systems |
| **Identity resolution** | Hash-determinism (same key → same hub row) + SAL for explicit assertion | Active deduplication, stewardship workflows, golden record selection |
| **Data changes** | Insert-only — no modification of source data | Active updates to master records |

DV and MDM are complementary, not competing: **MDM can be a data source to the Data Vault.** If an MDM system issues a canonical customer ID, that ID is loaded into the vault as any other source would be. The vault's passive integration then harmonises it with other source-system keys via BKCC and SAL patterns.

**1. Hash-determinism integration (automatic)**
When two sources share the same natural key AND the same namespace (BKCC), they hash to the same surrogate key and produce a single hub row — automatically, with no extra code. This is why the hash algorithm and BK treatment rules must be applied consistently across all sources.

```sql
-- Source A loads account '29469' with BKCC = 'default':
SHA1_BINARY(UPPER(CONCAT('default' || '||' || '29469')))  -- produces hash H1

-- Source B loads account '29469' with BKCC = 'default':
SHA1_BINARY(UPPER(CONCAT('default' || '||' || '29469')))  -- produces same hash H1

-- Result: one hub row. Both sources' satellite data joins to it correctly.
```

This is the primary integration mechanism and the most powerful property of Data Vault. It requires no configuration — it is a consequence of deterministic hashing.

**2. SAL-based integration (explicit assertion)**
When two sources use *different* key values for the same real-world entity (e.g. `CUST-001` in CRM and `C001` in billing), hash-determinism integration cannot help — the hashes will differ. A Same-As Link (SAL) explicitly asserts the identity relationship. The enterprise agrees on a canonical key for analytics; survivorship logic lives in the IM.

**Business key resolution preference order (shift-left)**

When source systems don't share a canonical business key, resolution must happen before raw vault loading. Apply these options in preference order — always resolve as far upstream as possible:

| Priority | Where | Approach |
|---|---|---|
| 1st (preferred) | **Source application** | Request the source team to supply a universal BK or harmonise keys before delivery. The source owns the process and is best placed to provide the correct identifier. |
| 2nd | **Pre-staging** | If the source cannot be changed, resolve keys in the curated/pre-staging zone using transformation logic. Resolution stays close to the source, before any vault artefacts are created. |
| 3rd (last resort) | **Raw vault via SAL** | If neither option is feasible, use a Same-As Link in the raw vault to assert identity between source keys. This is the least desirable option — it adds vault artefacts (SAL + effectivity satellite) purely to manage an integration problem that should have been resolved upstream. |

SAL should be used for genuine multi-source identity assertions — not as a substitute for upstream key harmonisation.

| Scenario | Integration mechanism |
|---|---|
| Same key, same namespace, multiple sources | Hash-determinism (automatic) |
| Different keys for the same entity | SAL-based (explicit assertion) |
| Same key format, different entities (acquisition) | BKCC (namespace separation) |

The business key is the integration point in two dimensions:
- **Horizontal** — connects data across all source systems that reference the same entity
- **Vertical** — connects operational data through to business architecture, analytics, and reporting

### Collision code — namespace ID, not source-system ID

The `dv_collisioncode` (BKCC — Business Key Collision Code) is widely misunderstood. The most common mistake is defaulting it to the source-system name (e.g. `'SALESFORCE'`, `'SAP'`).

**This is wrong and creates unnecessary hub bloat.**

The collision code is a **business key namespace discriminator**. It answers the question: "In what namespace does this business key value uniquely identify an entity?" The collision is in the *business key value space*, not in the hash computation.

**The acquisition scenario — the most common real trigger for BKCC**

Montague Systems acquires Capulet Corp. Both track account numbers in `nnnnn` format. Post-acquisition, both sources load to the same `HUB_ACCOUNT`.

Account number `29469` appears in both systems — but they are completely different entities:
- `29469` in Montague Systems → balance of $160
- `29469` in Capulet Corp → balance of $200

Without BKCC: both hash to the same surrogate key → Capulet's $200 account appears to be additional data on Montague's $160 account. The vault silently serves wrong data.

With BKCC:
```sql
-- Montague Systems account:
SHA1_BINARY(UPPER(CONCAT('MONTAGUE' || '||' || '29469')))  -- unique hash

-- Capulet Corp account:
SHA1_BINARY(UPPER(CONCAT('CAPULET'  || '||' || '29469')))  -- different hash
```

The two accounts coexist cleanly in `HUB_ACCOUNT` as separate rows. Joining a Montague hub row to a Capulet satellite returns **zero rows** — not wrong rows. This is correct by design.

**The rule: only use BKCC when there could be a collision**

In some enterprises, natural keys representing the same business entity are shared across all source systems (e.g. a single MDM-issued customer ID used everywhere). In that case, BKCC = `'default'` for all sources — they all hash to the same hub row, and that is the correct passive integration result.

| Scenario | BKCC needed? |
|---|---|
| Acquisition: two companies, same key format, different entities | Yes — one BKCC per company namespace |
| Single MDM key used universally across all sources | No — `'default'` for all; same hash = one hub row |
| Organic growth: new source, same key used for same entity | No — `'default'`; passive integration is automatic |
| New source with overlapping key space, unknown overlap | Yes — use BKCC to be safe; consolidate with SAL later |

**Equi-join correctness by design**

Because BKCC is baked into the hash key, namespace isolation is a structural property of the vault — you cannot accidentally join across namespaces:

- Joining a Montague hub row to a Montague satellite → returns the correct Montague data
- Joining a Montague hub row to a Capulet satellite → returns **zero rows** (not Capulet data)
- Joining a Capulet hub row to a Montague link → returns **zero rows** (not Montague relationships)

In a traditional surrogate key model, a wrong join returns rows from the wrong entity — a silent data quality failure. In Data Vault with correct BKCC usage, the wrong join returns nothing — the error is structural and observable. This is why a single vault model can safely host multiple business processes and acquisitions without cross-contamination.

If you default BKCC to the source-system ID for every source, you create:
- Duplicate hub rows for the same real-world entity (one per source)
- Unnecessary links and technical debt to consolidate them downstream
- More joins in every IM query

A better mental model: think of BKCC as a **namespace ID** — the business key namespace that makes this value unambiguous. Most enterprises with good MDM practices will use `'default'` for the majority of their hubs.

**SSDV \u2014 Source System Data Vault (named anti-pattern)**

When BKCC is defaulted to the source-system name for every source without considering whether those sources share the same key values for the same entity, the result is a **Source System Data Vault (SSDV)**. This is a recognised Fake Vault pattern.

SSDV consequences (compounding):
1. More hub rows than necessary \u2014 one per source-system namespace, even when they represent the same entity
2. More joins required \u2014 every IM query must traverse the unnecessary namespace splits
3. More Business Vault artefacts required \u2014 a SAL or BV link is needed just to resolve the integration debt that the BKCC choice introduced

The correct mental model: start with `'default'` for all sources. Only introduce a BKCC namespace when you have confirmed that two sources use the same key value for genuinely different entities (e.g. a corporate acquisition).

### Smart keys — when they work, when they fail

A **smart key** (also called an intelligent key) is a business key that encodes information in its structure. Examples:
- `INV-2024-001` — invoice type (`INV`), year (`2024`), sequence (`001`)
- `CUS-AU-00842` — entity type (`CUS`), country (`AU`), sequence (`00842`)
- `ACCT-CHK-29469` — account type (`ACCT-CHK`), number (`29469`)

The article says "smart-keys excepted" when noting that keys are meaningless in isolation — because smart keys *do* carry information in their structure. This creates both a legitimate use case and a significant anti-pattern.

**When smart keys are acceptable as Data Vault business keys:**
If the business genuinely uses the full value as their identifier — it appears on invoices, in customer communications, in system references — then the full value is a valid business key, even though it looks structured. The key is treated as an opaque string by the vault; its internal structure is irrelevant to the vault mechanics.

**The parsing anti-pattern:**
The danger arises when practitioners try to decompose the smart key into its components and use those components as hub columns or derived attributes:

```sql
-- WRONG: extracting parts of the smart key into hub columns
CREATE TABLE HUB_INVOICE (
    dv_hashkey_hub_invoice  BINARY(20),
    invoice_id              VARCHAR(255),   -- 'INV-2024-001' (correct BK)
    invoice_type            VARCHAR(10),    -- 'INV' extracted from key — WRONG
    invoice_year            INTEGER,        -- 2024 extracted from key — WRONG
    ...
);
```

Extracting components from the smart key and loading them into the hub treats the key as a data structure. Those components are **attributes** and belong in a satellite. The hub should hold only the opaque key.

**Immutability risk:**
If the encoding convention changes — e.g. `INV-` becomes `INVOICE-` or the year component is removed — the vault correctly creates a new hub row for the new key format. Old key `INV-2024-001` and new key `INVOICE-2024-001` become separate hub records. If they represent the same entity under a new naming convention, a SAL is needed to assert the identity. This is the correct vault response to key format changes, but it surprises teams that expected the vault to recognise the relationship automatically.

**Rule:** Use the full smart key as the BK (opaque string). Never decompose it in the hub. If the components are analytically useful, derive them in a satellite or BV satellite using the smart key as input.

### Zero-key in link tables — cardinality flexibility

Null business keys in a link are coalesced to the **zero-key** — the same all-zeros binary hash that is the ghost record in hub tables.

```sql
-- When a participant business key is null or unknown, hash to the zero-key:
SHA1_BINARY(UPPER(CONCAT('default' || '||' || '-1')))  -- '-1' is the null substitute
-- This produces the same all-zeros binary as the ghost record
```

**Why this matters for cardinality:**

A link table with zero-key support can represent any cardinality without model changes:

| Cardinality | How it works |
|---|---|
| M:M | Both participants always present — standard |
| 1:M | One side always present, the other is the "many" |
| 0:M | One participant may be absent — coalesces to zero-key for that side |
| Optional relationships | Either participant may be unknown at load time |

An `INNER JOIN` on a link always resolves — if a participant key is absent, it joins to the ghost row in the hub. **Zero keys naturally exist in hub tables via the ghost record — you do not need to pre-load them separately for links to work.**

This is fundamentally different from adding start/end dates or nullable FK columns to the link table — both of which break the link table pattern and force comparison logic into every query that traverses the link.

**Peg-legged link — named anti-pattern**

A **peg-legged link** is a link table where one or more FK columns are nullable or optional — a "one-legged" relationship where a participant is sometimes absent. This is poor data vault modelling practice and does not exist in a correct Pragmatic DV model.

The correct approach for optional participants: use the **zero-key** (all-zeros binary hash). The link always has the full set of FK columns populated — absent participants are coalesced to the zero-key in staging, which then joins to the ghost row in the hub. Equi-joins always resolve. No nullable FK columns needed.

### Information mapping — DV as a business ontology

Data Vault is not a storage format or a normalisation technique — it is an **information mapping** framework. It maps the state of business automation into a canonical data model that can serve as corporate memory.

Every business, in every industry, tracks three things:
1. **Business objects** — uniquely identified entities the business manages
2. **Interactions** — events, transactions, and relationships between those objects
3. **Information state** — how those objects and interactions change over time

These map directly and completely to Hub → Link → Satellite. When a vault is built correctly against real business objects, the model:
- Grows **horizontally** as new sources and business processes are added (new satellites, new links)
- Grows **vertically** as history accumulates (insert-only appends)
- Rarely acquires new hub tables in an established enterprise — because real business objects are stable

The vault is a **business ontology** — a data representation of the information landscape automated by business software. The business key is the "shared kernel" that connects all bounded contexts: operational systems, analytics, reporting, and regulatory audit all converge on the same business key.

**Knowledge graph parallel** — a data vault maps directly onto the components of a knowledge graph:

| Knowledge graph concept | Data Vault equivalent |
|---|---|
| **Nodes** | Hub tables (business objects) |
| **Edges** | Link tables (relationships and interactions) |
| **Attributes** | Satellite tables (information state) |
| **Ontology** | The vault model itself — defines node types, edge types, and constraints |

This is not a coincidence: both knowledge graphs and data vaults are representations of a business ontology. A well-built vault is only a small step away from a queryable knowledge graph — the entities, relationships, and attributes are already modelled correctly.

**DV as the canonical node resolver for knowledge graphs**

A knowledge graph requires **consistent, canonicalised node identifiers** — it cannot resolve multi-source key conflicts itself. When multiple source systems use different business keys for the same real-world entity, the KG would produce duplicate nodes or incorrect edges unless that conflict is resolved first.

The data vault resolves this upstream through the **same-as link (SAL)**: all source keys for the same entity are harmonised, and only the canonical business key is surfaced to the KG layer. The SAL is a DV-internal resolution mechanism — it tracks the identity assertion relationship between source keys and the agreed canonical key, but this plumbing must **not propagate to the knowledge graph**. The KG sees clean, deduplicated nodes; the integration complexity of multi-source key spaces is absorbed entirely inside the vault.

**Implication:** a knowledge graph built without DV's key resolution will encounter the same multi-source integration complexity DV was designed to solve — and will likely solve it inconsistently. The vault is the natural prerequisite for any enterprise knowledge graph that spans multiple source systems.

**DV provides the temporal layer KG lacks**

Knowledge graphs are typically point-in-time snapshots of entities and relationships. By using a data vault as the underlying data store, KG elements can be instantiated across time: vault history (all satellite rows, all link history) enables querying the state of the knowledge graph at any point in the past. The vault is the enterprise's corporate memory; the KG is a query interface over it.

### Hub vs. Link — the key question

If the table represents **one business entity** → Hub.
If the table represents **a relationship between entities** → Link.

A purchase order is a Hub (one business key: order_id).
An order line is a Link between HUB_ORDER and HUB_PRODUCT (it joins two entities).

**Unit of Work (UoW)** — a link captures one unit of work: the complete set of business entities involved in a single transaction, relationship, or event. The UoW grain is the source grain. The rule is: if you had to recreate the source record, does this link (plus its satellite) give you everything you need? If not, the link has been incorrectly modelled.

**UoW decomposition anti-pattern** — breaking a multi-entity transaction into multiple bi-relationship links (2-way links) is the most common link modelling mistake. A mortgage application involving customer + account + property + broker should be one 4-way link, not four 2-way links. Decomposition:
- Makes it impossible to recreate the source record (breaks auditability)
- Adds downstream reassembly cost and query latency
- Increases join complexity as consumers must re-join the decomposed pieces
- **Creates false-positive relationships** — joining two decomposed 2-way links produces a Cartesian product of combinations that never existed in the source. If customer A was in link 1 with product X, and separately in link 2 with warehouse Y, the join asserts A+X+Y existed together — which is false. Only a single multi-participant link correctly constrains the relationship to what actually happened.

The test: if you must join multiple link tables together to answer "what happened in this transaction?", the UoW was decomposed.

**Transactional link (t-link) — deprecated**

The **transactional link** (t-link) was an early DV pattern where individual transaction rows were loaded directly into a link table — one row per transaction event, rather than one row per unique relationship combination. **This pattern is deprecated in Pragmatic DV.** Loading batched transactions as a link table breaks the fundamental definition of a link: a link captures a *relationship* (a unique unit of work between business objects), not an individual transaction event.

The correct pattern for batched transactional data:

| Component | Role |
|---|---|
| **Link table** | Records the *relationship* between the participating business objects (e.g. `LNK_CUSTOMER_PRODUCT`) — one row per unique relationship combination |
| **Link satellite with dep-child key** | Records each individual transaction event hanging off the link — transaction ID or transaction date as the intraday dep-child key |

This separates the relationship grain (link) from the event grain (link-satellite with dep-child), enabling correct change detection, correct UoW auditability, and appropriate satellite splitting between dimensions and facts.

**Information Mart (IM)** — query-ready views or tables built on top of the raw vault, designed for BI tools and analysts. Hash keys are an implementation detail of the vault and must never appear in IM views — business keys (`_BK`) are used instead. IM views join hubs, satellites, and (when needed) PIT tables to produce flat, readable structures. Use `/dv-mart` to build them.

**Calculated / derived attributes** — when a business process or algorithm produces a derived attribute (e.g. customer lifetime value, order total, risk score), treat the calculation result as a source and load it into a standard satellite using the normal insert-only anti-semi join pattern. No separate "business vault" layer is needed. The calculation is the source; the satellite is the container.

```
Calculation result → SAT_CUSTOMER_METRICS  (standard satellite, RSRC = 'CALC_ENGINE')
                              ↓
                     DIM_CUSTOMER view (IM) — attribute exposed alongside raw attributes
```

### Why hash keys must not appear in the IM

Hash keys (BINARY columns — size is algorithm-dependent: SHA1 default → `BINARY(20)`, MD5 → `BINARY(16)`) are meaningless to business users and BI tools. They exist to:
1. Join tables efficiently inside the vault
2. Decouple from source-system surrogate keys

Once you cross the vault/IM boundary, you substitute the `_BK` (natural business key) that the business actually uses. This keeps the IM comprehensible and portable — if you rebuild hash keys with a different algorithm, the IM is unaffected.

**Kappa Vault** — a Pragmatic Data Vault loading pattern that uses Snowflake Streams placed on **staging views** (not tables) as the pipeline trigger. Named by analogy to the Kappa Architecture (stream-first processing).

Key mechanics:
- Staging view SQL is identical to standard Data Vault — the pattern difference is in what consumes it
- `APPEND_ONLY` streams are placed on the staging view — one stream per loader (hub gets its own stream, each satellite gets its own stream)
- Loaders read from the stream, not the view directly. On `COMMIT`, the stream advances — tracking exactly what was processed
- Tasks fire only when `SYSTEM$STREAM_HAS_DATA()` returns true — event-driven, not cron-based
- Multi-cadence handling: when landing occurs multiple times between loads, the `discard_view` CTE (satellites) and `distinct_view` CTE (hubs) use `LAG()` window functions to deduplicate within the stream batch, loading only genuine changes regardless of how many times the same record appeared in landing
- Repeatable Read Isolation: load + reconciliation test are wrapped in `BEGIN TRANSACTION / COMMIT`. The test shares the loader's stream, so it operates on exactly the records that were loaded — no phantom reads or gap between load and test

All vault DDL, hash key formulas, ghost records, satellite variants, PIT/SNOPIT, and IM views are identical between Standard and Kappa Vault. A single vault can mix both patterns — Kappa Vault for high-frequency/continuous sources, standard batch for daily ERP loads.

Landing table requirement: must be **append-only** (`INSERT`, never `INSERT OVERWRITE` or `TRUNCATE`). If landing overwrites, use standard batch loading.

**Supernova — the 5 layers:**
- **Layer 1**: Source systems
- **Layer 2**: Raw Vault + Business Vault (Hub / Link / Satellite — insert-only, auditable)
- **Layer 3**: Supernova DTs — merges hub + satellites into a wide, versioned Dynamic Table with physicalised `startdate` / `enddate` columns. Built in two steps: (1) a versions DT (`dt_{hub}_versions`, `TARGET_LAG = DOWNSTREAM`) that UNION ALLs all satellite `dv_applieddate` values into a time spine, and (2) a Supernova DT (`dt_supernova_{hub}`) that equi-joins the time spine to each satellite. Ghost records excluded from the time spine via `WHERE dv_recsource <> 'GHOST'`.
- **Layer 4**: Extended Supernova (`dt_xsn_supernova_*`) — adds computed attributes, column renames, CASE tiers on top of the Supernova DT.
- **Layer 5**: Data Delivery — shaped views for BI (filtered, aggregate, OBT, star schema) built on top of the XSN DT.

**Supernova — equi-join rule**: The join from the versions DT to each satellite must be `sat.dv_applieddate = versions.startdate` — a strict equi-join, not a range join. This is what enables Snowflake INCREMENTAL refresh mode. Range joins force a full rebuild on every refresh. The versions DT converts temporal alignment into point lookups, making incremental refresh possible.

**Supernova vs PIT**: PIT stores only `dv_sid` locators per snapshot date — lightweight, supports cross-hub traversal via Bridge. Supernova stores ALL satellite columns in a wide pre-joined table — heavier storage, zero query-time joins. Use PIT when storage is constrained or cross-hub traversal is needed. Use Supernova when BI query performance is the priority and result caching is insufficient.

---

### Semantic layer — what it should and should not do

The semantic layer sits on top of the Information Mart. It translates technical data structures into business-friendly terms: entities, dimensions, metrics, hierarchies. It is a vocabulary and accessibility layer — not a modelling or business rules layer.

**DV → semantic layer mapping:**

| Vault construct | Semantic concept |
|---|---|
| Hub | Entity |
| Hub satellite attributes | Dimensions (descriptive attributes for slicing/dicing) |
| Satellite date columns | Time dimensions |
| Link satellite measures | Metrics / measures (additive, semi-additive) |
| Hub-to-hub path via link | Hierarchy (drill path) |

**What the semantic layer IS:**
- The business vocabulary translation of the IM — business terms, not column names
- The end state of harmonised information — all conflicts resolved upstream
- Ephemeral: deployed as views cached in memory, not a physical storage layer
- Business-vetted: every definition confirmed by business users

**What the semantic layer IS NOT:**
- A modelling layer — data modelling is solved in raw vault and BV; the semantic model inherits the result
- A technical debt resolution layer — tech debt is always resolved as far upstream (left) as possible
- A layered/stacked structure — no semantic model should depend on another semantic model; one flat layer only
- An audit trail — it is ephemeral; calculations needing historization must be pushed left into the vault
- A business rules engine — business rules belong in the vault or BV, not the semantic layer
- A hardcoded-values store — all data-driven changes come from reference data, not hardcoded semantic definitions

Full principle list: see `/dv-mart` "Semantic layer doctrine" section (12 rules).

### DDD source relationship patterns

When profiling source systems (see `/dv-discover`), classify each source against three Domain-Driven Design context mappings:

| Pattern | When | Implication |
|---|---|---|
| **Customer-supplier** | In-house software that honours data contracts | Standard staging + load. Push issues upstream. First prize. |
| **Anti-corruption layer (ACL)** | Legacy/vendor software; downstream must be shielded | Pre-staging absorbs upstream churn. Second prize. |
| **Conformist** | Vendor software that cannot be changed | Accept source model as-is. BV artefacts needed to reshape into business terms. Last prize. |

The pattern determines whether BV artefacts will be needed and how much technical debt the coherent zone will carry from this source.

---

### Stakeholder pitch \u2014 explaining DV by role

When explaining Data Vault to stakeholders, tailor the message to the audience. Use these one-paragraph pitches as a starting point:

| Stakeholder | Core pitch |
|---|---|
| **Business executive** | Data Vault brings proven patterns for integrating business processes into repeatable automation. Delivery is agile and predictable — adding new sources doesn't change existing tables. Full audit history is maintained by default, which satisfies regulatory and dispute requirements. |
| **Data modeller** | Data Vault doesn't replace dimensional models. The vault sits underneath and provides non-conformed audit history. Dimensional models (star schema, Kimball) still deliver the BI presentation layer on top. Because history lives in the vault, Information Marts are disposable — drop and rebuild without data loss. |
| **Enterprise architect** | DV maps your enterprise ontology to the data warehouse: hubs are business entities, links are relationships between them, satellites track historical state. The model is platform-agnostic and grows with the business. Data governance and privacy controls are structural, not bolted on. |
| **Product owner** | A steel thread implementation proves the end-to-end pipeline for one use case: landed file → vault → analytic value. Once established, the cadence of delivery increases with each sprint because every new source follows the same repeatable patterns. |
| **Scrum master** | Every DV use case follows the same sprint pattern: model, stage, load, test. Sprints are predictable. The non-modelling work is identical across all use cases — once the patterns are established, teams can run multiple use cases in parallel. |
| **Business analyst** | The vault records the history of business processes. When a new data source is added, only new artefacts are added — existing tables are never modified. Analysts can always trace any data point back to its source system and the exact time it was loaded. |
| **Solution architect** | Raw vault holds source data in three table types. Business vault fills technical and business gaps. Information marts sit on top and are disposable — because audit history lives in the vault, the IM can be rebuilt from scratch at any time. |
| **DataOps engineer** | Once loading patterns are established, the workload is mechanical: staged files load hubs, satellites, and links. That's three loader types for every source, every use case. Repeatable loading patterns produce repeatable test patterns. Scheduling, task management, and monitoring is the remaining operational surface. |
| **Data scientist** | Holistically, the vault resembles a mathematical hyper-graph: nodes are business entity hubs, edges are multi-node relationships in links. This enterprise ontology — with historised attributes attached to every node and edge — is the foundation for feature engineering, model training, and neural network design over the business's complete application landscape. |
| **Source-system SME** | We need to understand three things from your system: the business keys, the relationships between them, and the grain of the data. We won't model your entire platform — only the tables and entities in scope for the current use case. |

For a full stakeholder engagement guide, use `/dv-when` for the decision context and the steel thread approach.

### Information Marts are disposable

Because the vault holds full insert-only history, **every IM view can be dropped and rebuilt from scratch with zero data loss**. The vault is the corporate memory; the IM is a presentation layer over it.

This means:
- IM design can evolve freely without data migration risk
- Changing reporting requirements never require vault changes
- The cost of rebuilding an IM is near zero — the history that drives it never disappears

> When asked "what happens if the IM needs to change?": "Drop and recreate it. The history is in the vault, not the IM."

See `/dv-mart` "Information Marts are disposable" for the full explanation.

### Data lifecycle \u2014 when vault artefacts and data die

Data naturally decays, becomes superseded, or must be retired for legal reasons. Two failure modes exist:

**"Do nothing"** \u2014 a feed silently stops loading. Dimensions built on the affected artefacts serve stale data without anyone knowing. Trust erodes when the business discovers the gap. This is the most common failure mode and the one that damages data platform credibility.

**"Do something"** \u2014 establish operational and governance procedures for both unintentional and intentional disruption:

| Scenario | Action |
|---|---|
| **Unintentional disruption** (feed fails) | Operational alert + recovery workflow. DMF monitoring via `/dv-test` catches this. |
| **Entity absent from source** | `last_seen_date` stops updating; aging BV rule declares entity *missing* then *deceased* (see entity aging above) |
| **Source feed intentionally retired** | Formal decommissioning: notify all consumers, assess downstream lineage, archive the artefact. See `/dv-deploy` data lifecycle section. |
| **GDPR erasure request** | XTS disposal columns + RTS to prevent accidental reappearance. PII satellite isolates the sensitive data. |

> "The misinformed are misaligned" \u2014 change information must spread **vertically** (through data lineage to all consuming lines of business) AND **horizontally** (across all scrum teams building on the affected artefacts).

### Data lifecycle \u2014 the obituary pattern

When deliberately retiring a vault artefact, issue a formal change communication covering:

1. **Name** \u2014 the exact artefact being retired (e.g. `LINK_BV_CUSTOMER_ACCOUNT`)
2. **Replacement** \u2014 what replaces it, if anything (e.g. `LINK_CUSTOMER_ACCOUNT`)
3. **Date** \u2014 when the retirement takes effect
4. **Who is affected** \u2014 which reports, dashboards, and analytics built on this artefact
5. **Downstream confirmation** \u2014 explicit confirmation that downstream consumers have been migrated and are unaffected
6. **Archive location** \u2014 where the retired artefact is stored (secured, no longer queried)

The "obituary" makes the governance trail auditable. The vault history of the retired artefact is preserved \u2014 only access and active use is removed.

### No time dimension in a Data Vault

Data Vault does not use a separate time dimension table. This is a key difference from Kimball dimensional modelling and a common misconception for practitioners transitioning from star schema.

In Kimball, a `DIM_DATE` or `DIM_TIME` table is a standard component. In Data Vault, temporal data is embedded in every row:
- `dv_applied_timestamp` — when the data was true in the source
- `dv_load_timestamp` — when it was loaded to the vault

These ARE the temporal dimensions. There is no `HUB_DATE`, no `LNK_DATE`, no date satellite. Time is not a business object in Data Vault — it is a metadata attribute of every row. If a date dimension is needed for BI tool calendar joins, build it as an Information Mart utility view — never as a vault construct.

### Landing Zone is NOT the Audit

The landing zone stores raw data before curation, staging, and loading. It is temporary. **It is not the audit record.**

Only the vault (RV + BV) with its insert-only structure and full audit metadata columns is the auditable corporate memory. The landing zone should have a defined retention period (typically 30 days) and should be purged. Teams that rely on landing zone retention as their regulatory backup are at risk — the vault is the authoritative record.

The vault's audit metadata per row: `dv_recordsource` (where), `dv_load_timestamp` (when loaded), `dv_applied_timestamp` (when true in source), `dv_user_id` (who loaded), `dv_jira_id` (which initiative), `dv_tenant_id` (which tenant), `dv_task_id` (which job).

### Staggered loading \u2014 the Fake Vault anti-pattern

**Staggered loading** (all hubs first, then all links, then all satellites) is a named Fake Vault anti-pattern inherited from pre-hash-key Data Vault. It is no longer needed and its use signals a misunderstanding of how hash keys work.

**Why it was needed before hash keys:** Surrogate sequence keys required link loaders to look up the parent hub's sequence key before inserting. This created hard load-time dependencies: hub must finish before link can start.

**Why hash keys eliminate it:** Hash keys are computed at staging time. No lookup is needed. Every vault table loads independently as soon as its staging data is ready. The vault is **eventually consistent** \u2014 a fundamental design property, not a limitation.

Two reasons hash keys were introduced:
1. **MPP data distribution** \u2014 pseudo-random hash values spread rows evenly across Snowflake nodes; prevents hotspots
2. **Single-column joins** \u2014 one `BINARY(20)` join column is simpler and faster than composite natural key joins with BKCC

See `/dv-load` for the correct DVOS Task DAG pattern (parallel hubs, sequential same-hub sources, independent satellite loads).

### XTS prerequisites \u2014 why Applied Date and no end-date enable timeline correction

The XTS (eXtended record Tracking Satellite) pattern can correct out-of-sequence satellite timelines. This is only possible because of two fundamental Pragmatic Data Vault design decisions:

**1. `dv_applied_timestamp` (Applied Date)**

Every vault record carries the business timestamp of the source extract it came from \u2014 when the data was *true* in the source system, not when it was loaded. XTS uses this column to determine exactly where in the entity timeline a late-arriving record belongs. Without it, there is no way to detect that a staged record is earlier than the current satellite state.

**2. No end-date column in satellites**

Because Pragmatic DV satellites have no `dv_end_date` or `dv_ledts` column, there is nothing to "fix" when a late record arrives. The timeline is reconstructed from insert-only records ordered by `dv_applied_timestamp`. When XTS inserts a COPY row to restore a subsequent state, it adds a new row \u2014 it never updates an existing row. Insert-only is never violated.

If an end-date column existed, XTS would also need to update the end-date of the record before the late arrival \u2014 which would require an UPDATE on an existing row, violating insert-only doctrine. The no-end-date design choice is what makes correction patterns like XTS structurally possible.

**Logical vs. physical timeline correction (Snowflake performance note)**

XTS corrects the *logical* timeline — the order of states as seen through `dv_applied_timestamp` in views and IM queries. The *physical* correction is simply a new INSERT appended to the most recent micro-partition. No existing records are touched. No micro-partition reorganisation occurs. No overlap is introduced.

This means XTS corrections have zero partition-level overhead on Snowflake. SNOPIT tables built on XTS-corrected satellites continue to benefit from the linear `dv_sid` zone map advantage — the correction record has a new (higher) `dv_sid` at the physical tail of the table, which is exactly where it belongs for optimal zone map effectiveness.

---

### Trap & Reject — DQ pattern before vault load

The recommended approach when source data fails quality checks:

1. **Trap** — pre-load quality checks run. Failing records route to a **SCD Type 4 error mart** (same structure as source + `error_code` + `error_reason`). The vault receives only clean data.
2. **Alert** — if the error mart has current-period records, alert the source team and send offending record IDs back for correction.
3. **Resubmit** — corrected records re-enter with **the same `dv_applied_timestamp` as the original rejection**. This places the correction at the correct point in the business timeline, not at the correction-load time. Without preserving the applied date, corrections corrupt the entity timeline.

"All the data, all the time" does not mean loading garbage. The cost of removing contaminated data from an immutable vault after load is far higher than preventing it at the gate.

### Dep-child keys in link tables — explicit anti-pattern

Loading dependent-child keys into link tables is a named Data Vault anti-pattern. **Dep-child keys belong in satellite tables, not link tables.**

Why link tables cannot carry dep-child keys:
1. **State reversion**: if the relationship returns to a previous state, which dep-child key version is the correct one?
2. **Timeline fragmentation**: if the dep-child key is part of the link hash, multiple timelines emerge in the link satellite — queries must consolidate them across dep-child values
3. **Shortest path broken**: a link table should represent the shortest path between hub tables. Adding a dep-child key to the link hash makes the link specific to a sub-relationship rather than the parent relationship

**The correct approach:** Load dep-child keys into satellite tables, with the parent hub or link hash key + dep-child key as the composite PK. This is simpler, produces fewer tables to join, and tracks dep-child key state changes cleanly.

> "Multi-active satellite tables are exception patterns intended for malformed data or special use cases — dep-child key satellite tables are easier to manage and cheaper to maintain."

**Rare valid exception — dep-child key in a Business Vault link**

The anti-pattern rule above applies to **Raw Vault links**. There is a specific rare exception for **Business Vault links** where a dep-child key genuinely belongs in the link itself:

When a BV link captures an **inferred congregated relationship** (e.g. a knowledge graph householding inference that groups customers into a household unit), the congregation identifier may be intrinsically part of the link's unit of work — not a categorisation added after the fact, but the defining grain of the relationship itself. In this case, placing the dep-child key in the BV link hash is valid.

**Qualifying condition:** the dep-child key is part of the BV link when:
- The BV relationship cannot be expressed without it (removing it would make the link lose meaning or grain)
- The dep-child is supplied by the business rule or inference engine as part of the relationship definition — not derived from attributes after the link is loaded
- The BV link is **not** a raw vault table — it is a business rule outcome, not a source-system observation

If these conditions are not met, place the dep-child key in a satellite on the BV link, not in the link hash.

### Zero Key vs. Ghost Record — they are not the same

**Zero Key** and **Ghost Record** share the same all-zeros binary value but serve completely different purposes:

| | Zero Key | Ghost Record |
|---|---|---|
| What it is | All-zeros hash produced by hashing a null/absent BK (`hash(BKCC \|\| '-1')`) | A physical row pre-inserted into every satellite with an all-zeros hash key |
| Where it lives | In link tables as the coalesced hash of a null participant | As a stored row in satellite tables (one per satellite) |
| Pre-loaded? | **No** — computed dynamically in staging when a BK is null | **Yes** — must be explicitly inserted before PIT tables can work |
| Purpose | Makes link cardinality-agnostic; joins to the hub's ghost row (which is always present via the hub ghost record) | Allows PIT/SNOPIT null-safe joins when no satellite record exists at a snapshot date |

A zero key in a link does not require pre-loading because it joins to the hub's ghost row (pre-loaded into the hub). A satellite ghost record must be explicitly pre-loaded before any PIT table can use it.

### Effectivity satellite — use sparingly, prefer source-supplied indicator

Effectivity satellites should be used sparingly. They exist to compensate for a gap in the source application — the inability to supply a relationship end/start indicator directly.

**Preference order:**
1. **First prize** — the source application supplies an active/inactive indicator for the relationship. Load it as an attribute in the link satellite. The raw vault reflects exactly what the source recorded.
2. **Second prize** — only if the source cannot supply an indicator, use an effectivity satellite (EFS) with a driver key to track when the active relationship changed.

**Risk of late EFS addition:** if an effectivity satellite is added after a link has been loaded, the history of relationship changes before the EFS was added is permanently lost from that satellite's perspective. The link table still has the full history, but the EFS has no record of it. Model EFS from the start if you know it will be needed.

### SAL — not for source surrogate key mapping

A Same-As Link (SAL) is for asserting that **two different business keys represent the same real-world entity**. It is NOT for mapping source surrogate keys to business keys.

**SAL is appropriate for:**
- Two different business keys in different source systems that represent the same entity (passive integration via SAL)
- Source system key → MDM-assigned canonical key mappings
- Match-merge identity resolution results

**SAL is NOT appropriate for:**
- Recording a source application's surrogate key (auto-increment primary key) alongside the business key. Surrogate keys are implementation artefacts of the source application — they belong in a satellite attribute, tracked as they change over time. If the source is reloaded and the surrogate changes, the satellite records that as a new state; no SAL is needed.

---

### Ontology abstraction levels (Hay's framework)

Four-level hierarchy for deciding hub granularity and model scope:

| Level | Scope | Examples |
|---|---|---|
| **Level 0** | Metadata | Information assets, accounting structures, templates |
| **Level 1** | Enterprise | Who / Where / What / How / When — universal concepts |
| **Level 2** | Functional | Facility, HR, marketing, contracts — department-scoped |
| **Level 3** | Industry | Banking, insurance, energy, healthcare — domain-specific |

Data Vault absorbs any mix of abstraction levels into the same vault. Hub tables may be shared across ontology levels (e.g. HUB_PARTY used in L1 enterprise and L3 banking contexts) but links and satellites are typically level-specific.

Use this framework during `/dv-discover` sessions to calibrate hub granularity: too low (L3 only) and you lose enterprise integration; too high (L1 only) and hubs become weak/generic.

---

### Graph theory connection

Data Vault has a direct correspondence to graph theory:

| DV Construct | Graph Concept |
|---|---|
| Hub | Vertex (node) |
| Link (2-hub) | Edge |
| Link (3+ hubs) | Hyperedge (hypergraph) |
| Same-as link / hierarchy link | Cyclic edge (self-referencing vertex) |

This makes DV directly representable as a property graph for visualization, topology analysis, and automated model validation. Graph algorithms (shortest path, connected components, cycle detection) can be applied to the DV model to verify:
- No isolated hubs (every hub should be reachable via at least one link)
- No hub islands (all hubs should form a single connected component in the enterprise model)
- Hierarchy depth (cyclic edges in SAL/hierarchy links are bounded)

---

### Operational Data Vault (ODV) — the end-state architecture

The Operational Data Vault represents the end-state architecture where source systems query the data vault back through governed APIs. The vault becomes the single source of facts for **both** analytics AND operational systems.

Key characteristics:
- Sub-300ms query latency requirement (see Hybrid Satellite Table in satellite variants)
- Source applications consume data from the vault/lake via APIs — the same data serves analytics and operations
- Requires Interactive Tables or Hybrid Tables (Snowflake) for the hot path
- Read and write models may be separated (CQRS pattern — see `/dv-mart` CQRS streaming IM pattern)
- Governance and access control are unified — one security model for all consumers

ODV is not a starting point. It is the aspirational end-state for mature DV implementations that have achieved full enterprise integration and are ready to expose governed data back to operational systems.

---

### Effectivity satellite vs status tracking satellite — formal distinction

These are NOT interchangeable despite superficial similarity:

| Aspect | Effectivity Satellite | Status Tracking Satellite |
|---|---|---|
| **Source requirement** | Works with both deltas AND snapshots | Requires **full snapshot** source |
| **What it tracks** | Driving-key movement against non-driving keys in a link | Appearance/disappearance of business keys |
| **Grain** | Link-level (hangs off a link) | Hub-level (hangs off a hub) |
| **Relationship re-appearance** | If driving key re-appears with same non-driving key, nothing happens (relationship already matches) | Inserts a new 'I' (active) record |
| **Derivation** | BV-derived (effectivity logic is a business rule) | Can be RV (source-supplied) or BV-derived |

**When to use which:**
- Source supplies start/end dates for a relationship → standard link-satellite attribute (not effectivity)
- Need to track when a driving key moved between non-driving keys → effectivity satellite
- Need to detect entities appearing/disappearing from a full-snapshot feed → status tracking satellite

---

### When to choose Data Vault
Use `/dv-when` for a full decision guide. Short answer:
- Many source systems → yes
- Frequent schema changes → yes
- Full auditability required → yes
- One stable source, fast delivery needed → consider Medallion first
