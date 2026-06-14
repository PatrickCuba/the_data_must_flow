---
name: dv-explain
description: Explain any Data Vault 2.0 concept, pattern, rule, or modeling decision in plain language
enabled: true
---

# /dv-explain — DV2.0 Knowledge Base

Answer questions about Data Vault 2.0 concepts, modeling decisions, and doctrine rules. No subagents needed — pure knowledge.

## Usage

`/dv-explain <concept>` — e.g.:
- `/dv-explain hub`
- `/dv-explain why insert-only`
- `/dv-explain multi-active satellite`
- `/dv-explain hash key`
- `/dv-explain same-as link`
- `/dv-explain PIT table`
- `/dv-explain rule 07`

If the user types `/dv-explain` with no argument, ask: "What would you like me to explain?"

## Knowledge Base

### Core constructs

**Hub** — stores a unique list of business keys for one business entity. Contains only: hash key, business key, load date, record source. Nothing else. It is the "anchor of truth" for that entity.

**Satellite** — stores the context and history of a hub or link. All descriptive attributes live here. Multiple satellites per hub are encouraged (split by rate of change or source system).

**Link** — stores the relationship between two or more hubs. Keys only — no descriptive attributes. If a relationship has attributes, they go in SAT_LNK_<NAME>.

**PIT table** — a query-assist structure that pre-computes the "as-of" timestamp for each satellite attached to a hub. Makes queries dramatically faster by avoiding correlated subqueries across satellites.

**Bridge table** — a query-assist structure that pre-joins a hub to one or more related links and their connected hubs. Snapshot-based. Not a real vault layer — it's a view optimization.

### Satellite variants

DVOS supports 8 satellite types. PII is a **naming suffix** applied to any satellite type, not a separate type.

**Standard satellite** — one active row per business key at any point in time. No end-date — current row via `QUALIFY ROW_NUMBER()`. Most common type. Manifest `type: standard`.

**Multi-active satellite** — multiple rows can be active simultaneously for the same business key (e.g. multiple phone numbers, multiple addresses). Composite PK includes `dv_sequence` (synthetic discriminator). Manifest `type: ma`.

**Effectivity satellite** — tracks when a link relationship is active. **Link-only** (never off a hub). Uses `dv_start_date` and `dv_end_date` columns populated by the loader from driver-key staging. Insert-only — never updated. The `dv_end_date` is set to the configured high-date for open/active records. Requires `driver_keys` config. Has **no business attributes**. Answers "was this link active at time T?" Doctrine rule DV-EFS-001 enforces `dv_start_date`/`dv_end_date` physically present; forbids `ACTIVE_FLAG`, `dv_startts`, `dv_endts`, UPDATE, MERGE, DELETE. Manifest `type: ef`.

**Dependent-child satellite** — used when the parent hub key alone doesn't uniquely identify a row. Adds a child key (e.g. order line number) to the PK via dependent_child_keys config. The parent key is a FK, not a unique identifier within this satellite. Manifest `type: dp`.

**Non-historized satellite** — no `dv_hashdiff`; no deduplication. Used for reference/lookup data the business treats as "always current" with no history required. Manifest `type: nh`.

**Status tracking satellite** — tracks a status/state column over time, using a secondary staging view. Manifest `type: st`.

**Record tracking satellite** — tracks whether a record exists (present/absent) in the source, using a secondary staging view. Manifest `type: rt`.

**Extended tracking satellite (XTS)** — advanced pattern for file-based ingestion with timeline correction. Requires XTS config. Manifest `type: xt`.

**PII naming suffix** — any satellite type can have a `_pii` suffix in its name to segregate sensitive columns into a separate physical table with independent access control. Not a distinct `type` value in the manifest.

### Key concepts

**Hash key** — hash of the business key concatenated with the `dv_collisioncode` (BKCC — Business Key Collision Code). **Record source is NOT part of the hash key.** BKCC is a short stable discriminator (e.g. `'CRM'`) that distinguishes overlapping key spaces from different source systems. Hash algorithm is project-configured (default SHA1 → `BINARY(20)`). Formula: `hash_fn(UPPER(CONCAT(bkcc || '||' || COALESCE(NULLIF(TRIM(bk), ''), '-1'))))`.

**Hash diff (dv_hashdiff)** — hash of all tracked attribute columns concatenated. Used in the satellite load pattern to detect whether a row has changed without comparing every column individually. Column name is `dv_hashdiff`. Algorithm matches the project's configured hash algorithm.

**dv_load_timestamp** — when the record was loaded into the vault. Not the business date — the technical load time. Every table has this. Column name is `dv_load_timestamp`.

**dv_applied_timestamp** — timestamp of the data from the source batch/file (the business time the data was produced). Distinct from `dv_load_timestamp` (when it was loaded into the vault). Used for ordering within a load batch.

**dv_recordsource** — identifies which source system produced this record. Column name is `dv_recordsource`. Every table has this. Enables traceability and multi-source integration.

**Insert-only loading** — the vault never updates or deletes raw data. New versions are appended. History is implicit in `dv_load_timestamp` / `dv_applied_timestamp` ordering. **There is no end-date column (LEDTS) in DVOS.** Current row is retrieved in views via `QUALIFY ROW_NUMBER() OVER (PARTITION BY parent_hk ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1`. This is the single most important operational rule.

**Hub/link MERGE exception** — hubs and links use `MERGE` with `WHEN NOT MATCHED THEN INSERT` + `WHEN MATCHED THEN UPDATE SET last_seen_date`. This is the only UPDATE permitted in the vault. It tracks when a business key was last seen in a source delivery. Satellites remain purely INSERT-only with no MERGE.

**Anti-semi join pattern** — the standard satellite load query uses `WHERE NOT EXISTS (SELECT 1 FROM <target> WHERE hash_key = ...)` instead of MERGE. Simpler, cheaper, and safe for insert-only semantics. Hubs and links use MERGE instead (see above).

**Ghost record** — a row with all-zero hash key inserted into every satellite. Required for PIT tables to perform null-safe joins when no satellite record exists for a given snapshot date.

**Same-as link** — a raw vault entity (not a business layer concept) that connects two records in the same hub that represent the same real-world entity. Lives in the raw vault alongside hubs, links, and satellites. Insert-only, always paired with an effectivity satellite that tracks whether the match assertion is currently active. The SAL does not merge records — it asserts identity; survivorship logic (which record "wins") lives in the Information Mart. Naming: `SAL_<ENTITY>`. SAL hash key is computed from both hub hash keys (not record source).

**Record source granularity** — record source should be as specific as useful: `CRM.SALESFORCE.ACCOUNTS` not just `CRM`. Enables surgical re-loading and audit.

### Why insert-only?

Three reasons:
1. **Auditability** — every version of every record is preserved. You can reconstruct any state of the business at any point in time.
2. **Parallelism** — inserts never block each other. Concurrent loads from multiple sources don't need locks.
3. **Simplicity** — `INSERT WHERE NOT EXISTS` is simpler, faster, and less error-prone than MERGE.

### When to split a satellite?

Split a satellite when:
- Attributes come from different source systems
- Attributes change at very different rates (e.g. demographic vs. transactional)
- Some attributes are PII and need access control
- The satellite has more than ~30 columns (performance and maintainability)

### Hub vs. Link — the key question

If the table represents **one business entity** → Hub.
If the table represents **a relationship between entities** → Link.

A purchase order is a Hub (one business key: order_id).
An order line is a Link between HUB_ORDER and HUB_PRODUCT (it joins two entities).

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

### When to choose Data Vault

Use `/dv-when` for a full decision guide. Short answer:
- Many source systems → yes
- Frequent schema changes → yes
- Full auditability required → yes
- One stable source, fast delivery needed → consider Medallion first
