---
name: dv-generate
description: Generate SQL DDL and load patterns for a validated Pragmatic Data Vault construct
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

### 2 — Confirm project-level settings

Before generating, confirm two project-level settings if not already known:

**a) Hash algorithm** (ask once per project):
> **Which hash algorithm does this project use?** (MD5, SHA1, or SHA256)

Default: SHA1. See "Hash algorithm configuration" below for the full mapping.

**b) BKCC per source** (ask per data source being onboarded):
> **Does this source need a custom BKCC (collision code), or should we use `'default'`?**

Default: `'default'`. Only set a custom value when the source has overlapping key spaces with other sources that represent **different** business entities. If the user does not set a BKCC, use `'default'` — do not prompt further.

**c) Tenant ID per source** (ask per data source being onboarded):
> **Does this source need a custom tenant ID, or should we use `'default'`?**

Default: `'default'`. Only set a custom value when the vault serves multiple business units or brands that must be logically separated. If the user does not set a tenant ID, use `'default'` — do not prompt further. When multi-tenancy is disabled in the manifest (`tenant.enabled: false`), the tenant ID column is still present but always `'default'`.

### 3 — Spawn the SQL Generator subagent

Read `agents/sql-generator.md` for the full system prompt.

Pass the validated construct definition plus the confirmed hash algorithm, BKCC value, and tenant ID. Ask the Generator to produce:
1. `CREATE TABLE` DDL (with correct BINARY size for chosen algorithm)
2. Insert-only load pattern (anti-semi join)
3. Hash key computation expression (using chosen algorithm function, BKCC value, and tenant ID)
4. Ghost record INSERT (using correct ghost hex length)

### 4 — Present and confirm

Show the SQL to the user before finalizing. Ask:
> "Does this look right? Reply 'yes' to finalize, or describe any changes."

Do not write to files unless the user asks.

## Generated patterns by construct type

**Note on column names:** DVOS uses `dv_hashkey_hub_<name>` (not `<NAME>_HK`), `dv_load_timestamp` (not `LDTS`), `dv_hashdiff` (not `HDIFF`), `dv_recordsource` (not `RSRC`). There is **no end-date column** — DVOS satellites are insert-only; current row is retrieved via `QUALIFY ROW_NUMBER()`.

**Note on hash key:** Hash key uses `dv_collisioncode` (BKCC) as the discriminator — **not record source**. Algorithm is project-configured — ask the user which algorithm before generating DDL.

### Hash algorithm configuration (project-level)

Ask the user:
> **Which hash algorithm does this project use?** (MD5, SHA1, or SHA256)

| Algorithm | Snowflake function | Column type | Ghost record hex | Collision resistance |
|---|---|---|---|---|
| MD5 | `MD5_BINARY(...)` | `BINARY(16)` | `TO_BINARY(REPEAT('0', 32), 'HEX')` | Low — 128-bit, known collisions. Use only for small key spaces or legacy compatibility. |
| **SHA1** (default) | `SHA1_BINARY(...)` | `BINARY(20)` | `TO_BINARY(REPEAT('0', 40), 'HEX')` | Moderate — 160-bit. Recommended default for most data vaults. |
| SHA256 | `SHA2_BINARY(...)` | `BINARY(32)` | `TO_BINARY(REPEAT('0', 64), 'HEX')` | High — 256-bit. Use for very large key spaces or high-security requirements. |

**Rules:**
- The chosen algorithm applies to **all** hashkeys (`dv_hashkey_*`) and hashdiffs (`dv_hashdiff`) in the project — do not mix algorithms within a single vault.
- All BINARY columns in DDL must match the algorithm's output size.
- Ghost records must use the correct hex string length for the chosen algorithm.
- Staging views must use the correct function (`MD5_BINARY`, `SHA1_BINARY`, or `SHA2_BINARY`).

**In templates below**, replace:
- `<hash_fn>` with the Snowflake function name
- `<hash_size>` with the BINARY size (16, 20, or 32)
- `<ghost_hex>` with `REPEAT('0', <hash_size * 2>)` (32, 40, or 64 hex chars)

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
- Hash algorithm is project-configured (see "Hash algorithm configuration" above). All BINARY column sizes and ghost records must match the chosen algorithm's output size.
- Record source (`dv_recordsource`) is stored for traceability but NOT part of hash computation.
- No end-date (LEDTS) column exists in any DVOS table. Satellites are purely insert-only.
- FK constraints are intentionally omitted from link DDL — orphan checks run post-load.
- Staging views are assumed to exist; the generator does not create them.
- Ghost records (for PIT null-join support) must be inserted — flag this if PIT tables exist.

## Subagent files

- Doctrine Enforcer (gate): `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
- Naming Advisor: `agents/naming-advisor.md`
