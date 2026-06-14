---
name: dv-generate
description: Generate SQL DDL and load patterns for a validated Data Vault 2.0 construct
enabled: true
---

# /dv-generate — SQL Generation

Generate Snowflake SQL DDL and insert-only load patterns for a vault construct. Doctrine validation is a hard gate — code is not produced until the model is clean.

## Input

The user provides a validated construct definition (from `/dv-model`) or says "generate for <construct name>".

If no definition is available, ask them to run `/dv-model` first.

## Steps

### 1 — Run the doctrine gate

**Spawn the Doctrine Enforcer subagent** (see `agents/doctrine-enforcer.md`) with the construct definition.

If ANY violations are returned:
- Show the violations
- Stop. Do not produce SQL.
- Say: "Fix the violations above before generating. Use `/dv-validate` for details."

If CLEAN: proceed.

### 2 — Spawn the SQL Generator subagent

Read `agents/sql-generator.md` for the full system prompt.

Pass the validated construct definition. Ask the Generator to produce:
1. `CREATE TABLE` DDL
2. Insert-only load pattern (anti-semi join)
3. Hash key computation expression

### 3 — Present and confirm

Show the SQL to the user before finalizing. Ask:
> "Does this look right? Reply 'yes' to finalize, or describe any changes."

Do not write to files unless the user asks.

## Generated patterns by construct type

**Note on column names:** DVOS uses `dv_hashkey_hub_<name>` (not `<NAME>_HK`), `dv_load_timestamp` (not `LDTS`), `dv_hashdiff` (not `HDIFF`), `dv_recordsource` (not `RSRC`). There is **no end-date column** — DVOS satellites are insert-only; current row is retrieved via `QUALIFY ROW_NUMBER()`.

**Note on hash key:** Hash key uses `dv_collisioncode` (BKCC) as the discriminator — **not record source**. Algorithm is project-configured (default SHA1 → `SHA1_BINARY(...) :: BINARY(20)`).

### Hub load pattern
```sql
-- Hash keys are pre-computed in the staging view. NEVER recalculate in-flight.
-- MERGE inserts new hub records and updates last_seen_date for existing ones.
MERGE INTO HUB_<NAME> AS tgt
USING <staging_view> AS src
ON tgt.dv_hashkey_hub_<name> = src.dv_hashkey_hub_<name>
WHEN NOT MATCHED THEN INSERT (
    dv_hashkey_hub_<name>,
    <bk_column>,
    dv_tenant_id,
    dv_collisioncode,
    dv_applied_timestamp,
    dv_recordsource,
    dv_load_timestamp,
    last_seen_date
) VALUES (
    src.dv_hashkey_hub_<name>,
    src.<bk_col>,
    src.dv_tenant_id,
    src.dv_collisioncode,
    src.dv_applied_timestamp,
    src.dv_recordsource,
    src.dv_load_timestamp,
    src.dv_applied_timestamp
)
WHEN MATCHED THEN UPDATE SET
    tgt.last_seen_date = src.dv_applied_timestamp;
```

### Satellite load pattern (standard)
```sql
-- DVOS standard satellite — insert-only, anti-semi join on (parent_hk, hashdiff)
-- No end-date column. Current row via QUALIFY ROW_NUMBER() in views.
INSERT INTO SAT_<NAME> (
    dv_hashkey_hub_<parent>,
    dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource,
    dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    <attribute_columns>
)
SELECT
    src.dv_hashkey_hub_<parent>,
    src.dv_tenant_id, src.dv_task_id, src.dv_jira_id, src.dv_user_id,
    src.dv_recordsource,
    src.dv_hashdiff,
    src.dv_applied_timestamp,
    src.dv_load_timestamp,
    src.<attributes>
FROM <staging_view> src
WHERE NOT EXISTS (
    SELECT 1 FROM SAT_<NAME> s
    WHERE s.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
      AND s.dv_hashdiff = src.dv_hashdiff
);
```

### Link load pattern
```sql
-- DVOS link load — MERGE inserts new relationships, updates last_seen_date for existing.
-- No FK constraints inline — deferred to orphan-check phase.
MERGE INTO LNK_<NAME> AS tgt
USING <staging_view> AS src
ON tgt.dv_hashkey_<lnk_name> = src.dv_hashkey_<lnk_name>
WHEN NOT MATCHED THEN INSERT (
    dv_hashkey_<lnk_name>,
    dv_hashkey_hub_<hub_a>,
    dv_hashkey_hub_<hub_b>,
    dv_tenant_id,
    dv_applied_timestamp,
    dv_recordsource,
    dv_load_timestamp,
    last_seen_date
) VALUES (
    src.dv_hashkey_<lnk_name>,
    src.dv_hashkey_hub_<hub_a>,
    src.dv_hashkey_hub_<hub_b>,
    src.dv_tenant_id,
    src.dv_applied_timestamp,
    src.dv_recordsource,
    src.dv_load_timestamp,
    src.dv_applied_timestamp
)
WHEN MATCHED THEN UPDATE SET
    tgt.last_seen_date = src.dv_applied_timestamp;
```

## Rules

- **Hubs and links** use MERGE: `WHEN NOT MATCHED THEN INSERT` + `WHEN MATCHED THEN UPDATE SET last_seen_date`.
- **Satellites** use INSERT-only with anti-semi join `NOT EXISTS` — never MERGE, UPDATE, or DELETE.
- The `WHEN MATCHED` clause on hubs/links ONLY updates `last_seen_date`. No other column is ever updated.
- Hash keys use `dv_collisioncode` (BKCC) as discriminator — NOT record source.
- Hash algorithm is project-configured (default SHA1 → `SHA1_BINARY`, size `BINARY(20)`).
- Record source (`dv_recordsource`) is stored for traceability but NOT part of hash computation.
- No end-date (LEDTS) column exists in any DVOS table. Satellites are purely insert-only.
- FK constraints are intentionally omitted from link DDL — orphan checks run post-load.
- Staging views are assumed to exist; the generator does not create them.
- Ghost records (for PIT null-join support) must be inserted — flag this if PIT tables exist.

## Subagent files

- Doctrine Enforcer (gate): `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
- Naming Advisor: `agents/naming-advisor.md`
