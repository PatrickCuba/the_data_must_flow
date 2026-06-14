---
name: dv-load
description: Generate Snowflake orchestration (Tasks, DAGs, execution order) for vault loading. Handles dependency ordering, sequential same-hub loads, and ghost record deployment.
enabled: true
---

# /dv-load — Vault Load Orchestration

Generate Snowflake Task DAGs and execution scripts for loading the vault from staging views.

## Input

Ask the user:
1. Which constructs need orchestration? (all, or a specific subset)
2. What warehouse should the tasks use?
3. What schedule? (cron expression or interval)
4. Should ghost records be deployed as part of this run?

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

## Rules

- Hubs load before links. Links load before their satellites. Satellites load before PIT/Bridge.
- Multiple sources feeding the same hub: sequential execution (explicit `AFTER` dependency).
- Multiple sources feeding different hubs: parallel execution (all depend on root task).
- Ghost records are idempotent — safe to re-run.
- Task names follow: `TASK_LOAD_<TABLE_NAME>` or `TASK_LOAD_<TABLE_NAME>__<SOURCE>` for same-hub multiples.

---

## Subagent files

- SQL Generator: `agents/sql-generator.md`
- Doctrine Enforcer: `agents/doctrine-enforcer.md`
