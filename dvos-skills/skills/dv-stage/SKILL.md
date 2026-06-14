---
name: dv-stage
description: Design or review a DVOS staging view — source staging or BV staging. Covers metadata columns, hashkey/hashdiff computation, and the no-business-logic rule.
enabled: true
---

# /dv-stage — Staging View Design

DVOS generates staging views automatically from the manifest. This skill helps you understand, design, or review a staging view structure and diagnose staging doctrine violations.

## Two types of staging view

| Type | Naming | Source | Purpose |
|---|---|---|---|
| Source staging | `stg_{source_badge}_{source_file}` | Landing table | Wraps raw source data, adds all DV metadata |
| BV staging | `stg_bv_{concept_name}` | BV rule view (`bv_{concept_name}`) | Wraps business rule output, adds all DV metadata |
| Effectivity staging | `stg_ef_{source_badge}_{source_file}` | Base staging view | Generates OPEN/CLOSE records for effectivity satellites |
| Status tracking staging | `stg_st_{source_badge}_{source_file}_{parent_type}_{parent}` | Base staging view + STS satellite | Generates INSERT/DELETE status change records |
| Record tracking staging | `stg_rt_{source_file}_{parent_type}_{parent}_{hashkey}` | Base staging view | Tracks entity presence per applied timestamp |
| Extended tracking staging | `stg_xt_{source_file}_{parent_type}_{parent}_{hashkey}` | Base staging view | Tracks adjacent satellite hashdiffs for XTS |

**Source badge** (`{source_badge}`) is the system-level identifier for the source (e.g. `sapbw`, `mdm`, `zoho`). It must be defined before staging views are named. See `reference/naming-conventions.md` for the full source badge definition and rules.

---

## Core doctrine: staging is metadata-only

**DV-STG-008**: Staging views are a pure passthrough with DV metadata added. No business logic.

| Allowed in staging | NOT allowed in staging |
|---|---|
| Hash key computation (`dv_hashkey_*`) | `CASE WHEN` for business logic |
| Hashdiff computation (`dv_hashdiff_*`) | String concatenation (`\|\|`) for derived columns |
| Metadata columns (`dv_load_timestamp` etc.) | `DATEADD`, `DATEDIFF`, `DATE_TRUNC` |
| Pass-through of source columns | `SUBSTR`, `LEFT`, `RIGHT` for string manipulation |
| | Mathematical derivations |

Business logic belongs in **landing tables only**. If a column needs derivation before loading — it belongs in the landing layer, not staging.

---

## Source staging view

### Required metadata columns (DV-STG-001)

Every staging view must expose:
- `dv_load_timestamp` — when the record was loaded
- `dv_applied_timestamp` — business time from source batch/file (carried from source, NOT `CURRENT_TIMESTAMP`)
- `dv_recordsource` — source system identifier
- `dv_tenant_id` — tenant discriminator
- `dv_collisioncode` — BKCC (hub-only; staging still carries it for hub hashkey computation)
- `dv_task_id` — task/job identifier (default `'N/A'`, overridden by loader with run_id)
- `dv_jira_id` — JIRA ticket for traceability
- `dv_user_id` — loading user/service account (typically `CURRENT_USER()`)

### Hash key columns (DV-STG-003 / DV-STG-004)

For each hub fed by this staging view:
```
dv_hashkey_hub_<hub_name>   — computed from BKCC + business key
```

For each link fed by this staging view:
```
dv_hashkey_lnk_<link_name>  — computed from BKCC + all participant business keys
dv_hashkey_hub_<hub_a>      — per participant hub
dv_hashkey_hub_<hub_b>
```

### Hashdiff columns (DV-STG-005)

For each satellite loaded from this staging view:
```
dv_hashdiff_<satellite_full_name>   — one per satellite target
```

Hashdiff naming uses the **full satellite name** (e.g. `dv_hashdiff_sat_customer_demographics`).

### Hash computation rules

**Hash keys** — use `UPPER()`, null substitute `-1`. Whether `dv_tenant_id` is included depends on `tenant.enabled` in the manifest:

```sql
-- Multi-tenancy ENABLED (tenant.enabled: true):
hash_fn(UPPER(CONCAT(
    '<tenant_id_value>' || '||' || '<bkcc>' || '||' || COALESCE(TRIM(CAST(<bk_col> AS STRING)), '-1')
))) AS dv_hashkey_hub_<name>

-- Multi-tenancy DISABLED (tenant.enabled: false):
hash_fn(UPPER(CONCAT(
    '<bkcc>' || '||' || COALESCE(TRIM(CAST(<bk_col> AS STRING)), '-1')
))) AS dv_hashkey_hub_<name>
```

Default values: `dv_tenant_id = 'default'`, `dv_collisioncode = 'default'`. Override per source using `bkcc_value` and `tenant_id_value` in the manifest hub sources (e.g. `bkcc_value: zoho` for Zoho-sourced accounts).

**Hashdiffs** — **NO `UPPER()` or `LOWER()`** (DV-STG-007), null substitute `''`, no tenant_id/bkcc:
```sql
hash_fn(CONCAT(
    COALESCE(TRIM(CAST(<attr1> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr2> AS STRING)), '')
)) AS dv_hashdiff_<sat_full_name>
```

### Canonical dv-tag names (DV-STG-006)

Never alias. Exact names:

| Column | Correct | Wrong |
|---|---|---|
| Load timestamp | `dv_load_timestamp` | `load_ts`, `LDTS` |
| Applied timestamp | `dv_applied_timestamp` | `applied_ts`, `ADTS` |
| Record source | `dv_recordsource` | `record_source`, `RSRC` |
| Tenant ID | `dv_tenant_id` | `tenant_id` |
| Collision code | `dv_collisioncode` | `bkcc`, `collision_code` |
| Hash key | `dv_hashkey_hub_<name>` | `<NAME>_HK` |
| Hash diff | `dv_hashdiff_<sat_name>` | `HDIFF`, `dv_hashdiff` (bare) |

---

## BV staging view

BV staging wraps the business rule view (`bv_{concept_name}`) which lives in the BV staging schema. The rule view outputs **only business keys + `dv_applied_timestamp`** — no hashkeys, no hashdiff. DVOS computes everything else in the staging layer.

```
bv_{concept_name}  (business rule view — BK + dv_applied_timestamp only)
        ↓
stg_bv_{concept_name}  (DVOS-generated staging — adds hashkeys, hashdiff, all metadata)
        ↓
sat_bv_{concept_name}  (BV satellite — INSERT-only load)
```

**Business rule view must NOT contain any `dv_hashkey_*` or `dv_hashdiff_*` columns.** DVOS is the sole generator of those.

`dv_applied_timestamp` in BV staging is carried from the rule view — it must be derived from contributing RV satellite timestamps (`GREATEST` of sources), never `CURRENT_TIMESTAMP` (DV-BV-111).

---

## Secondary staging views

Secondary staging views sit between the base staging view and the satellite loader. They perform comparison logic that the base staging (metadata-only) cannot handle. The satellite loader remains standard (INSERT WHERE NOT EXISTS) — all intelligence lives in the secondary staging view.

```
Landing table
    ↓
stg_{source_badge}_{source_file}         ← base staging (metadata-only)
    ↓
stg_ef_* / stg_st_* / stg_rt_* / stg_xt_*   ← secondary staging (comparison logic)
    ↓
SAT_*_EFF / SAT_ST_* / SAT_RT_* / SAT_XT_*   ← satellite loader (standard INSERT)
```

### Effectivity staging (`stg_ef_*`)

Generates OPEN and CLOSE records for effectivity satellites by comparing the current source delivery against the currently active relationships in the target.

**Pattern:** `stg_ef_{source_badge}_{source_file}`

**Logic:**

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_EF_<SOURCE_BADGE>_<SOURCE_FILE> AS

WITH latest_effs AS (
    -- Get currently active relationships from the effectivity satellite
    SELECT
        hub_a.dv_hashkey_hub_<hub_a>,
        hub_b.dv_hashkey_hub_<hub_b>,
        lnk.dv_hashkey_lnk_<link>,
        ef.dv_start_date,
        ef.dv_end_date
    FROM <vault_schema>.HUB_<HUB_A> hub_a
    JOIN <vault_schema>.LNK_<LINK> lnk ON lnk.dv_hashkey_hub_<hub_a> = hub_a.dv_hashkey_hub_<hub_a>
    JOIN <vault_schema>.HUB_<HUB_B> hub_b ON hub_b.dv_hashkey_hub_<hub_b> = lnk.dv_hashkey_hub_<hub_b>
    JOIN <vault_schema>.SAT_<LINK>_EFF ef ON ef.dv_hashkey_lnk_<link> = lnk.dv_hashkey_lnk_<link>
    WHERE ef.dv_end_date = '<high_date>'::TIMESTAMP_NTZ
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lnk.dv_hashkey_lnk_<link>
        ORDER BY ef.dv_applied_timestamp DESC, ef.dv_load_timestamp DESC
    ) = 1
),

src_date AS (
    -- Get distinct driver key hashkeys + timestamps from base staging
    SELECT DISTINCT
        dv_hashkey_hub_<driver_key>,
        dv_applied_timestamp
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>
),

-- OPEN records: new relationships not in target
open_records AS (
    SELECT
        src.dv_hashkey_lnk_<link>,
        src.dv_applied_timestamp AS dv_start_date,
        '<high_date>'::TIMESTAMP_NTZ AS dv_end_date,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM latest_effs le
        WHERE le.dv_hashkey_hub_<hub_a> = src.dv_hashkey_hub_<hub_a>
          AND le.dv_hashkey_hub_<hub_b> = src.dv_hashkey_hub_<hub_b>
    )
),

-- CLOSE records: relationships that changed (driver key exists but non-driver keys differ)
close_records AS (
    SELECT
        le.dv_hashkey_lnk_<link>,
        le.dv_start_date AS dv_start_date,
        sd.dv_applied_timestamp AS dv_end_date,
        sd.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM latest_effs le
    JOIN src_date sd ON sd.dv_hashkey_hub_<driver_key> = le.dv_hashkey_hub_<driver_key>
    WHERE NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src2
        WHERE src2.dv_hashkey_hub_<hub_a> = le.dv_hashkey_hub_<hub_a>
          AND src2.dv_hashkey_hub_<hub_b> = le.dv_hashkey_hub_<hub_b>
    )
)

SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM close_records
UNION ALL
SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM open_records;
```

**Key rules:**
- Compares ALL participant hashkeys (not just composite link hashkey) to detect flip-flop scenarios
- Hashdiff = hash of (`dv_start_date || dv_end_date`)
- CLOSE records use the original `dv_start_date` from the target
- Driver key determines which entity "owns" the relationship tracking
- The satellite loader uses the standard INSERT WHERE NOT EXISTS pattern — no special logic

---

### Status tracking staging (`stg_st_*`)

Detects INSERT/DELETE changes by comparing the current source snapshot against the previous state stored in the STS satellite itself.

**Pattern:** `stg_st_{source_badge}_{source_file}_{parent_type}_{parent_name}`

**Logic:**

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_ST_<SOURCE_BADGE>_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT> AS

WITH current_status AS (
    -- Latest status record per hashkey from the STS satellite (excl ghost)
    SELECT dv_hashkey_hub_<parent>, dv_hashdiff
    FROM <vault_schema>.SAT_ST_<PARENT>_<SOURCE>
    WHERE dv_recordsource != 'GHOST'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<parent>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
),

-- INSERT: records in staging NOT in STS (or last status was 'D')
gen_inserts AS (
    SELECT
        src.dv_hashkey_hub_<parent>,
        SHA1_BINARY('I') AS dv_hashdiff,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM current_status cs
        WHERE cs.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
          AND cs.dv_hashdiff != SHA1_BINARY('D')
    )
),

-- DELETE: records in STS with active status but NOT in current staging
gen_deletes AS (
    SELECT
        cs.dv_hashkey_hub_<parent>,
        SHA1_BINARY('D') AS dv_hashdiff,
        src_date.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        src_date.dv_tenant_id,
        src_date.dv_recordsource
    FROM current_status cs
    CROSS JOIN (SELECT MAX(dv_applied_timestamp) AS dv_applied_timestamp,
                       MAX(dv_tenant_id) AS dv_tenant_id,
                       MAX(dv_recordsource) AS dv_recordsource
                FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>) src_date
    WHERE cs.dv_hashdiff != SHA1_BINARY('D')
      AND NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
        WHERE src.dv_hashkey_hub_<parent> = cs.dv_hashkey_hub_<parent>
    )
)

SELECT * FROM gen_inserts
UNION ALL
SELECT * FROM gen_deletes;
```

**Key rules:**
- `'I'` (insert/present) and `'D'` (delete/absent) are the only two status values
- Hashdiff is a hash of the status letter itself: `SHA1_BINARY('I')` or `SHA1_BINARY('D')`
- Compares against the STS satellite itself (self-referencing)
- DELETE records are generated when an entity disappears from the current source delivery
- Supports role-playing via hashkey aliasing (one view per role)

---

### Record tracking staging (`stg_rt_*`)

Simplest secondary staging — records entity presence per `dv_applied_timestamp`. No comparison logic.

**Pattern:** `stg_rt_{source_file}_{parent_type}_{parent_name}_{hashkey_col}`

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_RT_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT>_<HASHKEY> AS
SELECT
    dv_hashkey_hub_<parent>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_applied_timestamp AS STRING)), '')
    )) AS dv_hashdiff_sat_rt_<parent>_<source>,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;
```

**Key rules:**
- Pure passthrough with a hashdiff derived from `dv_applied_timestamp` only
- One view per (source_file, hashkey_column) — deduplicated across multiple RTS satellites

---

### Extended tracking staging (`stg_xt_*`)

Tracks adjacent satellite hashdiffs for XTS (Extended Tracking Satellites). UNION ALLs one SELECT per related satellite.

**Pattern:** `stg_xt_{source_file}_{parent_type}_{parent_name}_{hashkey_col}`

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_XT_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT>_<HASHKEY> AS

-- One SELECT per related satellite (excluding EF, RT, ST, NH, XTS types)
SELECT
    dv_hashkey_hub_<parent>,
    '<SAT_NAME_1>' AS dv_record_target,
    dv_hashdiff_sat_<name_1> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>

UNION ALL

SELECT
    dv_hashkey_hub_<parent>,
    '<SAT_NAME_2>' AS dv_record_target,
    dv_hashdiff_sat_<name_2> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;
```

**Key rules:**
- `dv_record_target` identifies which satellite the hashdiff belongs to
- Only includes satellites whose hashdiff is present in the base staging view
- Excludes peripheral types (EF, RT, ST, NH, XTS itself) from the UNION
- Deployed to a separate schema (`staging_secondary_ext_schema`) when configured

---

## Secondary staging doctrine rules

| Rule | Severity | Description |
|---|---|---|
| DV-STG-SEC-001 | ERROR | Secondary staging views must reference the base staging view (not the landing table directly) |
| DV-STG-SEC-002 | ERROR | Effectivity staging must compare ALL participant hashkeys (not just composite link hashkey) |
| DV-STG-SEC-003 | ERROR | Status tracking hashdiff must be `SHA1_BINARY('I')` or `SHA1_BINARY('D')` only |
| DV-STG-SEC-004 | ERROR | CLOSE records must carry the original `dv_start_date` from the target (not a new timestamp) |
| DV-STG-SEC-005 | ERROR | The satellite loader downstream of secondary staging remains standard INSERT WHERE NOT EXISTS |

---

## Snowflake ingestion patterns

The landing layer feeds the staging views. These Snowflake-native patterns cover how data arrives into landing tables.

### External tables (files on cloud storage)

Use when raw files land in S3, Azure Blob, or GCS and you want queryable access without copying:

```sql
CREATE OR REPLACE EXTERNAL TABLE <landing_schema>.<source_file>
  WITH LOCATION = @<stage_name>/<path>/
  AUTO_REFRESH = TRUE
  FILE_FORMAT = (TYPE = PARQUET)
  PATTERN = '.*[.]parquet';
```

The staging view sits on top of this external table — same DV metadata enrichment pattern.

### Snowpipe (continuous ingestion)

Use when files arrive continuously and need near-real-time loading into the landing layer:

```sql
CREATE OR REPLACE PIPE <landing_schema>.pipe_<source_file>
  AUTO_INGEST = TRUE
AS
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### COPY INTO with MATCH_BY_COLUMN_NAME

Use when the target landing table already exists and you want flexible schema mapping that tolerates column reordering in source files:

```sql
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Source columns map to target columns by name rather than position. New source columns are silently ignored unless the target table has them.

### INFER_SCHEMA + USING TEMPLATE (semi-structured schematisation)

Use when a new semi-structured source (Parquet, Avro, JSON) arrives and you need to auto-discover and materialize a structured schema:

```sql
-- Step 1: Discover the schema
SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION => '@<stage_name>/<path>/',
    FILE_FORMAT => '<file_format_name>'
));

-- Step 2: Create a structured landing table from the discovered schema
CREATE OR REPLACE TABLE <landing_schema>.<source_file>
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@<stage_name>/<path>/',
        FILE_FORMAT => '<file_format_name>'
    ))
);

-- Step 3: Load with MATCH_BY_COLUMN_NAME
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = '<file_format_name>'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

This is Snowflake's native schematisation — it turns semi-structured data into fully typed structured columns without manual DDL authoring.

### When to use which pattern

| Scenario | Pattern |
|---|---|
| Target table exists, batch load | `COPY INTO` with `MATCH_BY_COLUMN_NAME` |
| New source, unknown schema | `INFER_SCHEMA` + `USING TEMPLATE` then `COPY INTO` |
| Near-real-time continuous files | Snowpipe |
| Query files without copying | External table |

### METADATA$ columns for recordsource derivation

When loading from files, Snowflake exposes metadata columns useful for DV staging:

```sql
-- In the staging view, derive dv_recordsource from the source filename:
METADATA$FILENAME AS dv_recordsource,
METADATA$FILE_ROW_NUMBER AS source_row_number,   -- useful for XTS pattern ordering
METADATA$FILE_LAST_MODIFIED AS file_timestamp     -- useful for dv_applied_timestamp derivation
```

These are available in `COPY INTO` statements and external table queries.

---

## Staging doctrine rules summary

| Rule | Severity | Description |
|---|---|---|
| DV-STG-001 | ERROR | Required metadata columns present |
| DV-STG-002 | ERROR | `dv_collisioncode` required in HKV mode |
| DV-STG-003 | ERROR | Hub hashkeys present for each hub fed |
| DV-STG-004 | ERROR | Link + participant hashkeys present for each link fed |
| DV-STG-005 | ERROR | `dv_hashdiff_<sat>` present for each satellite fed |
| DV-STG-006 | WARNING | Canonical dv-tag names used (no aliases) |
| DV-STG-007 | ERROR | Hashdiff must NOT use `UPPER()` or `LOWER()` |
| DV-STG-008 | ERROR | No business logic in staging (passthrough only) |

---

## Subagent files

- Staging Validator: `agents/staging-validator.md`
- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- Naming Advisor: `agents/naming-advisor.md`
