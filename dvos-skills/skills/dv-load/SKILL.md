---
name: dv-load
description: Generate Snowflake orchestration (Tasks, DAGs, execution order) for vault loading. Handles dependency ordering, sequential same-hub loads, and ghost record deployment.
enabled: true
---

# /dv-load — Vault Load Orchestration

Generate Snowflake Task DAGs and execution scripts for loading the vault from staging views.

## Input

**DV-LOAD-001 — ALL satellites are INSERT only.** No UPDATE, no DELETE, no MERGE on any satellite table — no exceptions, including non-historized and BV satellites. "Non-historized" means the current view (`VC_*`) shows only the latest row via `QUALIFY ROW_NUMBER() = 1`; the TABLE still accumulates all rows via INSERT. Hubs and links use MERGE (for `last_seen_date` UPDATE and new-row INSERT). Satellites NEVER use MERGE.

Ask the user:
1. Which constructs need orchestration? (all, or a specific subset)
2. What warehouse should the tasks use?
3. **Loading mode: Standard (batch/cron) or Kappa Vault (event-driven, streams on views)?**
4. What schedule? (cron for standard; `SYSTEM$STREAM_HAS_DATA()` for Kappa Vault)
5. Should ghost records be deployed as part of this run?

If a validated model exists in the conversation, use it directly.

---

## Steps

### 1 — Resolve dependency order

**Loading order is mandatory:**
1. Hubs (all hubs can load in parallel — different target tables)
2. Links (after all participant hubs are loaded)
3. Satellites (after their parent hub or link is loaded)
4. PIT / Bridge (after all contributing satellites are loaded)

**Exception: same-hub sequential loads.** When multiple sources feed the same hub, those hub MERGE statements must execute sequentially — never in parallel. This prevents race conditions and duplicate hashkey inserts.

**Why the race condition happens:** Two concurrent MERGE statements for the same hub both execute the `WHEN NOT MATCHED` check at the same instant. Both see the business key as absent. Both proceed to INSERT. Both succeed. The hub now contains a duplicate BK — which is the one thing a hub must never have.

**Hub locking rules:**
- Always run same-hub loads sequentially (explicit `AFTER` dependency in the Task DAG)
- Even if you believe two sources will never produce the same BK: use sequential loading anyway. The cost of a sequential task dependency is minimal; building a conditional "lock if collision-risk, skip-lock otherwise" (switch architecture — see below) creates more technical debt than it saves
- **No locking needed for satellite tables** — each satellite is bound to a single source system. No two sources compete to write the same satellite, so concurrent satellite loads are safe

### 2 — Generate Task DAG

Produce a Snowflake Task graph:

```sql
-- Root task (schedule trigger)
CREATE OR REPLACE TASK <schema>.TASK_DV_LOAD_ROOT
  WAREHOUSE = <warehouse>
  SCHEDULE = '<schedule>'
AS SELECT 1;

-- Hub tasks (parallel — different target hubs)
CREATE OR REPLACE TASK <schema>.TASK_LOAD_HUB_PARTY
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_DV_LOAD_ROOT
AS
MERGE INTO HUB_PARTY AS tgt ...;

CREATE OR REPLACE TASK <schema>.TASK_LOAD_HUB_ACCOUNT
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_DV_LOAD_ROOT
AS
MERGE INTO HUB_ACCOUNT AS tgt ...;

-- Same-hub sequential (source A before source B)
CREATE OR REPLACE TASK <schema>.TASK_LOAD_HUB_ACCOUNT__SOURCE_A
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_DV_LOAD_ROOT
AS
MERGE INTO HUB_ACCOUNT AS tgt USING stg_source_a ...;

CREATE OR REPLACE TASK <schema>.TASK_LOAD_HUB_ACCOUNT__SOURCE_B
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_LOAD_HUB_ACCOUNT__SOURCE_A   -- sequential dependency
AS
MERGE INTO HUB_ACCOUNT AS tgt USING stg_source_b ...;

-- Link tasks (after all participant hubs)
CREATE OR REPLACE TASK <schema>.TASK_LOAD_LNK_CUSTOMER_ACCOUNT
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_LOAD_HUB_PARTY, <schema>.TASK_LOAD_HUB_ACCOUNT
AS
MERGE INTO LNK_CUSTOMER_ACCOUNT AS tgt ...;

-- Satellite tasks (after parent hub/link)
CREATE OR REPLACE TASK <schema>.TASK_LOAD_SAT_PARTY_DEMOGRAPHICS
  WAREHOUSE = <warehouse>
  AFTER <schema>.TASK_LOAD_HUB_PARTY
AS
INSERT INTO SAT_PARTY_DEMOGRAPHICS ...
WHERE NOT EXISTS ...;

-- Resume task tree
ALTER TASK <schema>.TASK_DV_LOAD_ROOT RESUME;
```

### 3 — Ghost record deployment

Generate ghost record INSERT statements for every satellite (executed once during initial deployment, idempotent via `WHERE NOT EXISTS`):

```sql
-- Ghost record for SAT_<PARENT>_<CONTEXT>
INSERT INTO <schema>.SAT_<PARENT>_<CONTEXT> (
    dv_hashkey_hub_<parent>, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp
)
SELECT
    TO_BINARY(REPEAT(0, 20)),   -- all-zeros hashkey
    NULL,
    'GHOST', 'GHOST', 'GHOST',
    'GHOST',
    TO_BINARY(REPEAT(0, 20)),   -- all-zeros hashdiff
    '1900-01-01'::TIMESTAMP_NTZ,
    '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (
    SELECT 1 FROM <schema>.SAT_<PARENT>_<CONTEXT>
    WHERE dv_hashkey_hub_<parent> = TO_BINARY(REPEAT(0, 20))
);
```

### 4 — Warehouse sizing guidance

| Load type | Recommended size | Rationale |
|---|---|---|
| Hub/Link MERGE | X-Small | IO-bound; small compute, hash key lookups |
| Satellite INSERT | Small–Medium | Hashdiff comparison across full staging set |
| PIT / Bridge DT refresh | Medium–Large | Cross-satellite joins, date spine expansion |

### 5 — Present and confirm

Show the generated Task DAG to the user. Ask:
> "Does the execution order look correct? Any dependencies to add or remove?"

---

---

## Kappa Vault loading mode

Use when: sources land data continuously or at a different cadence than the vault loads. Streams on staging views handle multi-cadence automatically.

### How it differs from Standard loading

| Aspect | Standard | Kappa Vault |
|---|---|---|
| Trigger | Cron schedule | `SYSTEM$STREAM_HAS_DATA()` — event-driven |
| Loader reads from | Staging view | Stream on staging view |
| Multi-cadence handling | Not addressed | `discard_view` / `distinct_view` CTEs |
| Reconciliation isolation | Separate reconciliation streams | Shared stream in `BEGIN TRANSACTION / COMMIT` |
| Landing table | Can be overwrite | Must be **append-only** |

### Kappa Vault Task DAG pattern

```sql
-- Root task: fires when stream has data
CREATE OR REPLACE TASK <schema>.TSK_ROOT_<BADGE>_<FILE>_RECON_DUMMY
  WAREHOUSE = <warehouse>
  SCHEDULE = '1 MINUTE'
  ALLOW_OVERLAPPING_EXECUTION = FALSE
  WHEN SYSTEM$STREAM_HAS_DATA('<staging_schema>.STR_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>_DV_HASHKEY_HUB_<PARENT>')
AS SELECT 1;

-- Hub loader task (reads stream, deduplicates with distinct_view CTE)
CREATE OR REPLACE TASK <schema>.TSK_<BADGE>_<FILE>_TO_HUB_<HUB>
  WAREHOUSE = <warehouse>
  AFTER <schema>.TSK_ROOT_<BADGE>_<FILE>_RECON_DUMMY
AS
MERGE INTO <vault_schema>.HUB_<HUB> h
USING (
    WITH distinct_view AS (
        SELECT *,
            LAG(dv_hashkey_hub_<hub>) OVER (PARTITION BY dv_hashkey_hub_<hub> ORDER BY dv_applied_timestamp) AS prev_dv_hashkey
        FROM <staging_schema>.STR_<BADGE>_<FILE>_TO_HUB_<HUB>_DV_HASHKEY_HUB_<HUB>
        QUALIFY dv_hashkey_hub_<hub> <> prev_dv_hashkey OR prev_dv_hashkey IS NULL
    )
    SELECT DISTINCT dv_tenant_id, dv_collisioncode, dv_hashkey_hub_<hub>,
                    dv_load_timestamp, dv_applied_timestamp, dv_recordsource,
                    dv_task_id, dv_jira_id, dv_user_id, <bk_col>
    FROM distinct_view
) stg
ON h.dv_hashkey_hub_<hub> = stg.dv_hashkey_hub_<hub>
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
WHEN MATCHED THEN UPDATE SET h.last_seen_date = stg.dv_applied_timestamp;

-- Satellite loader task (reads stream, deduplicates with discard_view CTE)
CREATE OR REPLACE TASK <schema>.TSK_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>
  WAREHOUSE = <warehouse>
  AFTER <schema>.TSK_ROOT_<BADGE>_<FILE>_RECON_DUMMY
AS
INSERT INTO <vault_schema>.SAT_<PARENT>_<CONTEXT> (...)
WITH discard_view AS (
    SELECT *,
        LAG(dv_hashdiff_<sat_name>) OVER (PARTITION BY dv_hashkey_hub_<parent> ORDER BY dv_applied_timestamp) AS prev_dv_hashdiff,
        RANK() OVER (PARTITION BY dv_hashkey_hub_<parent> ORDER BY dv_applied_timestamp) AS dv_cnt
    FROM <staging_schema>.STR_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>_DV_HASHKEY_HUB_<PARENT>
    QUALIFY dv_hashdiff_<sat_name> <> prev_dv_hashdiff OR prev_dv_hashdiff IS NULL
)
SELECT ...
FROM discard_view stg
WHERE NOT EXISTS (
    SELECT 1 FROM (
        SELECT dv_hashkey_hub_<parent>, dv_hashdiff,
               RANK() OVER (PARTITION BY dv_hashkey_hub_<parent> ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) AS dv_rnk
        FROM <vault_schema>.SAT_<PARENT>_<CONTEXT>
        QUALIFY dv_rnk = 1
    ) cur
    WHERE stg.dv_hashkey_hub_<parent> = cur.dv_hashkey_hub_<parent>
      AND stg.dv_hashdiff_<sat_name> = cur.dv_hashdiff
) OR dv_cnt > 1;
```

### Repeatable Read Isolation (load + test in same transaction)

When reconciliation tests need to operate on exactly the same records that were loaded, wrap loader + test in `BEGIN TRANSACTION / COMMIT`. The test shares the loader's stream — not a separate reconciliation stream:

```sql
BEGIN TRANSACTION;

-- LOAD (reads and advances the stream on commit)
INSERT INTO <vault_schema>.SAT_<PARENT>_<CONTEXT> (...)
... FROM <staging_schema>.STR_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>_... stg
WHERE NOT EXISTS (...);

-- TEST (reads the SAME stream — before commit it still sees the data)
INSERT INTO <utilities_schema>.reconcile_sat_reconciliation_errors (...)
... FROM <staging_schema>.STR_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>_... sg
WHERE NOT EXISTS (SELECT 1 FROM <vault_schema>.SAT_<PARENT>_<CONTEXT> s WHERE sg.dv_hashkey_hub_<parent> = s.dv_hashkey_hub_<parent>);

COMMIT; -- stream advances here; test results are committed atomically with the load
```

**Why this works:** Within the transaction, the stream still contains the unprocessed rows. The test query sees the rows, checks them against the target (which now contains the loaded rows), and reports any discrepancies. On COMMIT, both the load and the test result are written atomically.

### Enable the task DAG

```sql
SELECT SYSTEM$TASK_DEPENDENTS_ENABLE('<schema>.TSK_ROOT_<BADGE>_<FILE>_RECON_DUMMY');
ALTER TASK <schema>.TSK_ROOT_<BADGE>_<FILE>_RECON_DUMMY RESUME;
```

---

## Rules

- Hubs load before links. Links load before their satellites. Satellites load before PIT/Bridge.
- Multiple sources feeding the same hub: sequential execution (explicit `AFTER` dependency).
- Multiple sources feeding different hubs: parallel execution (all depend on root task).
- Ghost records are idempotent — safe to re-run.
- Task names follow: `TASK_LOAD_<TABLE_NAME>` or `TASK_LOAD_<TABLE_NAME>__<SOURCE>` for same-hub multiples.
- **Each DAG vertex is single-purpose** — one hub loader loads one hub; one satellite loader loads one satellite. Do not combine multiple hub loads into a single multi-table INSERT statement (see switch architecture anti-pattern below).

## Switch architecture \u2014 loading anti-pattern

A **switch architecture** exists when different loading strategies are used for different scenarios:
- "This source uses multi-table INSERT; that source uses individual loaders"
- "This hub needs locking; that one doesn't"
- "For this modelling scenario use one code path; for that scenario use another"

This is a named anti-pattern. It creates technical debt: more code paths to maintain, more failure modes to handle, harder to reason about under pressure.

**Why multi-table INSERT is wrong for hub loads:** Two distinct failure modes:

1. **Race condition** — A single multi-table INSERT attempting to load the same hub from multiple portions simultaneously has the same race condition as concurrent MERGE statements. Both portions see a BK as absent (Snowflake is READ COMMITTED: parallel threads see only the last committed state, not each other's uncommitted inserts) and both insert it. If any one portion fails, the entire statement fails.

2. **Record condensing** — When satellite splitting is applied, a `SELECT DISTINCT` over the entire staged file doesn't distinguish which portion of the file applies to which satellite. The deduplication rule collapses records that should be distinct per satellite, producing incorrect satellite content from the very first load.

Together these make multi-table INSERT structurally incompatible with raw vault loading regardless of source complexity.

**Exception: multi-table INSERT is valid for PIT and Bridge tables**

PIT and Bridge tables are the legitimate use case for multi-table INSERT. Unlike hub/link/satellite loads, PIT rows are keyed by `(entity_hash, snapshot_date)` — not by a business key that could race-condition duplicate. Multi-table INSERT is safe and efficient for PITs because:
- Each `(entity_hash, snapshot_date)` combination is unique by construction — no deduplication or `WHERE NOT EXISTS` logic is needed
- Loading multiple snapshot windows (daily, weekly, monthly PIT tables) in a single statement is efficient and correct
- If one snapshot portion fails, it doesn't corrupt other portions (each row represents a distinct snapshot point)

**Idempotency guard:** each WHEN clause in the multi-table INSERT must include a subquery that prevents re-inserting a snapshot date that was already populated:

```sql
WHEN (aof_week_lastday = 1)
AND (SELECT COUNT(1) FROM target_pit WHERE snapshotdate = src_snapshotdate) = 0
THEN INTO target_pit ...
```

Without this guard, re-running the pipeline (e.g. after a failure and retry) inserts duplicate snapshot rows. The guard makes the PIT population statement safe to re-run at any cadence.

Use multi-table INSERT for PIT/Bridge population, not for raw vault loaders.

**The correct approach: one loading pattern for all scenarios**
- Hub loader = one MERGE statement per hub, one task per source
- Satellite loader = one INSERT WHERE NOT EXISTS per satellite, one task per source
- All hub loaders for the same hub are chained sequentially
- Every other loader runs in parallel
- No exceptions, no switches

A single loading strategy means fewer patterns to maintain, fewer failure modes, and easier test automation — the same test framework applies to every load unit.

## Delivery architecture \u2014 three-hop is the endorsed pattern

Delivery architecture is classified by "hop count" \u2014 the number of physical persist points data passes through:

| Hops | Pattern | Verdict |
|---|---|---|
| 1-hop | PSA + DV views (no physical vault) | Non-sustainable \u2014 views are unexecuted code; query performance degrades as source grows |
| 1-hop | VSA + parallel RV+BV loads (embed BV logic in load scripts) | Anti-pattern \u2014 embeds business rules in load code; couples derived and raw content |
| 2-hop | Stage > RV (BV as views over RV) | Acceptable for simple vaults; BV rule versioning is impossible |
| 3-hop | **Stage > RV > BV (persisted)** | **Endorsed sustainable pattern** \u2014 repeated templates, decoupled, versioned |

The three-hop pattern (Stage > Raw Vault > Business Vault as persisted tables) is the endorsed delivery architecture because:
- Business rules are **decoupled** from raw ingestion \u2014 can be versioned, replaced, and audited independently
- Raw vault is a faithful copy of source \u2014 not contaminated by derived logic
- BV can be re-run from RV at any time without touching source
- Loading patterns are repeatable templates (same hub loader, same sat loader, same BV loader)

**Anti-pattern signal:** "We load BV in parallel with RV from the same staged file" \u2014 this embeds business rule logic into load scripts, creating a legacy data platform symptom where derived and raw content are coupled.

## Waitfor=ALL vs Waitfor=ANY \u2014 dependent load triggering

When BV loads depend on multiple RV satellites from different sources:

| Strategy | Behaviour | Trade-off |
|---|---|---|
| **Waitfor=ALL** | BV task fires only after ALL contributing source loads complete | Consistent: BV always computed from complete input; higher latency |
| Waitfor=ANY | BV task fires after EACH contributing source completes | Lower latency but may process with stale secondary data |

**Recommendation:** Use Waitfor=ALL. Consistency is more valuable than speed for derived content. The Task DAG `AFTER` clause naturally implements this \u2014 list all contributing satellite tasks as predecessors of the BV task.

Waitfor=ANY is acceptable only for non-historised BV satellites where eventual consistency is tolerable (e.g. latest-value lookups).

## Historical backload \u2014 the rehash migration query

When migrating historical data into a satellite (e.g. initial vault load from a source with pre-existing history), the source timeline must be replayed to reconstruct change history:

```sql
-- Replay change timeline from historical source
INSERT INTO SAT_RV_HUB_CUSTOMER_PROFILE (...)
WITH rehash AS (
    SELECT
        dv_hashkey_hub_customer,
        source_business_date AS dv_applied_timestamp,
        CURRENT_TIMESTAMP()  AS dv_load_timestamp,   -- migration execution date
        'MIGRATION:v1'       AS dv_recordsource,
        SHA2(CONCAT_WS('||', col1, col2, col3)) AS new_hashdiff,
        LAG(SHA2(CONCAT_WS('||', col1, col2, col3)))
            OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY source_business_date)
            AS prev_hashdiff,
        col1, col2, col3
    FROM staged_historical_source
)
SELECT dv_hashkey_hub_customer, dv_applied_timestamp, dv_load_timestamp,
       dv_recordsource, new_hashdiff, col1, col2, col3
FROM rehash
WHERE new_hashdiff <> prev_hashdiff
   OR prev_hashdiff IS NULL;   -- first record always loads
```

Key rules:
- **`dv_load_timestamp`** = migration execution date (not the source date) \u2014 preserves audit trail
- **`dv_applied_timestamp`** = source business date or original load timestamp \u2014 preserves chronology
- **`dv_recordsource`** = migration identifier (e.g. `'MIGRATION:v1'`) so backloaded records are distinguishable
- LAG() comparison eliminates duplicate states \u2014 only true changes are loaded
- If the source contained derived rules (calculated columns), decide whether to treat as RV (source faithfulness) or back-populate a BV artefact (derived content)

---

## Staggered loading \u2014 the named anti-pattern

**Staggered loading** is loading all hub tables first, then all link tables, then all satellite tables \u2014 waiting for every table at each tier to complete before the next tier starts. This is a Fake Vault anti-pattern.

**Why it was necessary before hash keys:** Pre-hash-key Data Vault used surrogate sequence keys. Link tables needed to look up the parent hub's sequence key at load time to maintain referential integrity. This created hard dependencies: hub must finish before link can start; link must finish before satellite can start.

**Why hash keys eliminate the need:** Hash keys are computed at staging time from the business key and BKCC. No lookup is needed. Every vault table can start loading as soon as its staging data is ready, independently of every other table. Referential integrity is guaranteed either by post-load orphan checks (DMF) or deferred RI constraints.

The vault is **eventually consistent** \u2014 each section loads independently and the full model converges as all loads complete. This is by design, not a limitation.

**Two reasons hash keys were introduced (beyond eliminating staggered loads):**
1. **MPP data distribution** \u2014 hash values are pseudo-random, which spreads rows evenly across nodes in a massively parallel platform (like Snowflake). This prevents hotspots and ensures data co-location on disk.
2. **Single-column joins** \u2014 joining on one `BINARY(20)` column is simpler and faster than joining on composite natural keys that may include BKCC and multi-part business keys.

**The correct loading pattern for DVOS** is already implemented in the Task DAG above: different hubs load in parallel from the root task; same-hub sources load sequentially; links load after all their participant hubs; satellites load after their parent hub or link. PIT/Bridge Dynamic Tables refresh automatically based on TARGET_LAG.

---

## Fail nosily and fast \u2014 data pipeline philosophy

> "When your Data Pipeline must fail, fail nosily and as soon as possible."

Do not build silent fallbacks, auto-repairs, or hidden retries into vault pipelines. When something fails, the failure should be:
- **Loud** \u2014 alert immediately (DMF, task failure notification, alerting integration)
- **Specific** \u2014 include the table, the task, the applied date, and the record count affected
- **Fast** \u2014 stop processing and escalate rather than continuing with potentially corrupt data

**Why:** A pipeline that silently compensates for a failure hides the root cause. The symptom is suppressed; the underlying problem accumulates. Example: automatically rebuilding a dropped index every time it's missing doesn't fix the process that's dropping it — it runs expensive rebuilds indefinitely and hides a configuration or permissions problem. Better to fail loudly the first time so the root cause is identified and removed permanently.

Applied to DV: if a hub load fails, do not auto-skip and continue loading satellites and links. Those downstream objects depend on the hub being current. Let the DAG fail at the hub task and surface the alert. Fix the root cause; then re-run.

## Subagent files

- SQL Generator: `agents/sql-generator.md`
- Doctrine Enforcer: `agents/doctrine-enforcer.md`
