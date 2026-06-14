---
name: dv-test
description: Generate Data Vault integrity tests — ad-hoc validation queries OR Snowflake DMF-based continuous monitoring with 17 reusable Data Metric Functions.
enabled: true
---

# /dv-test — Vault Integrity Tests

Two modes: **ad-hoc queries** for one-time validation/debugging, or **DMF-based continuous monitoring** using Snowflake Data Metric Functions that fire automatically after every DML commit.

## Input

Ask the user:
1. Which constructs to test? (all, or a specific hub/link/satellite)
2. What schema are the vault tables in?
3. **Mode: ad-hoc queries or DMF-based continuous monitoring?**
4. (DMF mode only) What database/schema should the DMFs live in?

---

## Mode 1: Ad-hoc validation queries

Standalone SELECT queries for one-time integrity checks. Use for debugging, post-migration validation, or environments where DMFs are not available.

### 1 — Hub hashkey uniqueness

Verify that each hub has exactly one row per hashkey:

```sql
-- TEST: HUB_<NAME> hashkey uniqueness
SELECT dv_hashkey_hub_<name>, COUNT(*) AS cnt
FROM <schema>.HUB_<NAME>
GROUP BY dv_hashkey_hub_<name>
HAVING cnt > 1;
-- Expected: 0 rows
```

### 2 — Link hashkey uniqueness

```sql
-- TEST: LNK_<NAME> hashkey uniqueness
SELECT dv_hashkey_<lnk_name>, COUNT(*) AS cnt
FROM <schema>.LNK_<NAME>
GROUP BY dv_hashkey_<lnk_name>
HAVING cnt > 1;
-- Expected: 0 rows
```

### 3 — Ghost record existence

Every satellite must have exactly one ghost record:

```sql
-- TEST: SAT_<PARENT>_<CONTEXT> ghost record exists
SELECT COUNT(*) AS ghost_count
FROM <schema>.SAT_<PARENT>_<CONTEXT>
WHERE dv_hashkey_hub_<parent> = TO_BINARY(REPEAT(0, 20));
-- Expected: 1
```

### 4 — Orphan detection (link → hub FK validation)

Links must reference existing hub records. This is the deferred FK check:

```sql
-- TEST: LNK_<NAME> → HUB_<HUB_A> orphan check
SELECT lnk.dv_hashkey_hub_<hub_a>, COUNT(*) AS orphan_count
FROM <schema>.LNK_<NAME> lnk
LEFT JOIN <schema>.HUB_<HUB_A> h
    ON h.dv_hashkey_hub_<hub_a> = lnk.dv_hashkey_hub_<hub_a>
WHERE h.dv_hashkey_hub_<hub_a> IS NULL
GROUP BY lnk.dv_hashkey_hub_<hub_a>;
-- Expected: 0 rows

-- Repeat for each hub participant in the link
```

### 5 — Row count reconciliation (staging vs. target delta)

Verify that the expected number of new rows arrived:

```sql
-- TEST: Satellite load reconciliation
WITH staging_count AS (
    SELECT COUNT(*) AS stg_rows
    FROM <staging_view> src
    WHERE NOT EXISTS (
        SELECT 1 FROM <schema>.SAT_<PARENT>_<CONTEXT> s
        WHERE s.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
          AND s.dv_hashdiff = src.dv_hashdiff
    )
),
recent_loads AS (
    SELECT COUNT(*) AS loaded_rows
    FROM <schema>.SAT_<PARENT>_<CONTEXT>
    WHERE dv_load_timestamp >= DATEADD(HOUR, -1, CURRENT_TIMESTAMP())
)
SELECT stg_rows, loaded_rows, stg_rows - loaded_rows AS delta
FROM staging_count, recent_loads;
-- Expected: delta = 0
```

### 6 — Hashdiff collision detection

Check for different attribute values producing the same hashdiff (extremely rare but validates hash function):

```sql
-- TEST: SAT_<PARENT>_<CONTEXT> hashdiff collisions
SELECT dv_hashkey_hub_<parent>, dv_hashdiff, COUNT(DISTINCT <attr_concat>) AS distinct_values
FROM (
    SELECT
        dv_hashkey_hub_<parent>,
        dv_hashdiff,
        CONCAT_WS('||', <attr1>, <attr2>, <attr3>) AS <attr_concat>
    FROM <schema>.SAT_<PARENT>_<CONTEXT>
    WHERE dv_recordsource != 'GHOST'
)
GROUP BY dv_hashkey_hub_<parent>, dv_hashdiff
HAVING distinct_values > 1;
-- Expected: 0 rows
```

### 7 — Multi-active sequence integrity

For multi-active satellites, verify no duplicate sequences per (hashkey, load_timestamp):

```sql
-- TEST: SAT_<PARENT>_<CONTEXT> sequence uniqueness
SELECT dv_hashkey_hub_<parent>, dv_sequence, dv_load_timestamp, COUNT(*) AS cnt
FROM <schema>.SAT_<PARENT>_<CONTEXT>
GROUP BY dv_hashkey_hub_<parent>, dv_sequence, dv_load_timestamp
HAVING cnt > 1;
-- Expected: 0 rows
```

### 8 — Effectivity satellite timeline integrity

No overlapping open periods per driver key:

```sql
-- TEST: SAT_<LINK>_EFF overlapping open periods
WITH active_records AS (
    SELECT
        dv_hashkey_lnk_<link>,
        dv_start_date,
        dv_end_date,
        LEAD(dv_start_date) OVER (
            PARTITION BY dv_hashkey_lnk_<link>
            ORDER BY dv_start_date
        ) AS next_start
    FROM <schema>.SAT_<LINK>_EFF
    WHERE dv_recordsource != 'GHOST'
)
SELECT *
FROM active_records
WHERE dv_end_date > next_start;
-- Expected: 0 rows (no overlaps)
```

### 9 — Business key consistency (hub vs. satellite)

Verify that every satellite record has a matching hub parent:

```sql
-- TEST: SAT_<PARENT>_<CONTEXT> → HUB_<PARENT> referential integrity
SELECT sat.dv_hashkey_hub_<parent>, COUNT(*) AS orphan_sats
FROM <schema>.SAT_<PARENT>_<CONTEXT> sat
LEFT JOIN <schema>.HUB_<PARENT> h
    ON h.dv_hashkey_hub_<parent> = sat.dv_hashkey_hub_<parent>
WHERE h.dv_hashkey_hub_<parent> IS NULL
  AND sat.dv_recordsource != 'GHOST'
GROUP BY sat.dv_hashkey_hub_<parent>;
-- Expected: 0 rows
```

---

## Mode 2: DMF-based continuous monitoring

Snowflake Data Metric Functions (DMFs) run automatically after every DML commit (`TRIGGER_ON_CHANGES`). Results are queryable from `SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS`. All expectations use `VALUE = 0` — zero errors means pass.

This mode generates:
1. A DQ schema with 17 reusable DMFs (create once, attach to many tables)
2. `ALTER TABLE` statements to attach DMFs to each vault table
3. Schedule configuration per table

Reference implementation: [github.com/PatrickCuba/the_data_must_flow/data-auto-testing/snowflake_dmf](https://github.com/PatrickCuba/the_data_must_flow/tree/master/data-auto-testing/snowflake_dmf)

---

### DMF library (17 functions)

#### Duplicate checks — HUB

| DMF | Checks | Signature |
|---|---|---|
| `DV_DMF_HUB_SKEY_DUPE_err` | Hub surrogate hash key uniqueness | `TABLE(skey BINARY)` |
| `DV_DMF_HUB_1BKEY_DUPE_err` | 1-part business key (tenant + colcode + bkey1) | `TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR)` |
| `DV_DMF_HUB_2BKEY_DUPE_err` | 2-part business key | `TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR)` |
| `DV_DMF_HUB_3BKEY_DUPE_err` | 3-part business key | `TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR, bkey3 VARCHAR)` |

#### Duplicate checks — LNK

| DMF | Checks | Signature |
|---|---|---|
| `DV_DMF_LNK_SKEY_DUPE_err` | Link hash key uniqueness | `TABLE(skey BINARY)` |
| `DV_DMF_LNK_2HKEY_DUPE_err` | 2-hub FK combination | `TABLE(hkey1 BINARY, hkey2 BINARY)` |
| `DV_DMF_LNK_3HKEY_DUPE_err` | 3-hub FK combination | `TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY)` |
| `DV_DMF_LNK_4HKEY_DUPE_err` | 4-hub FK combination | `TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY)` |
| `DV_DMF_LNK_5HKEY_DUPE_err` | 5-hub FK combination | `TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY, hkey5 BINARY)` |

#### Duplicate checks — SAT

| DMF | Checks | Signature |
|---|---|---|
| `DMF_DV_SAT_DUPE` | Regular SAT (skey + load_ts + tenant + hashdiff) | `TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY)` |
| `DMF_DV_SAT_MA_DUPE` | Multi-active SAT (+ sequence key) | `TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, seq_key NUMBER, hashdiff BINARY)` |
| `DMF_DV_SAT_DP_DUPE` | Dependent child SAT (+ dep child key) | `TABLE(skey BINARY, dep_key VARCHAR, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY)` |

#### Orphan checks

| DMF | Checks | Signature |
|---|---|---|
| `DV_DMF_SAT_SKEY_ORPH_ERR` | SAT FK not in parent HUB/LNK (excl GHOST) | `TABLE(fk_col BINARY, rec_source VARCHAR), TABLE(pk_col BINARY)` |
| `DV_DMF_LNK_SKEY_ORPH_ERR` | LNK FK not in parent HUB | `TABLE(fk_col BINARY), TABLE(pk_col BINARY)` |

#### Reconciliation checks

| DMF | Checks | Signature |
|---|---|---|
| `DMF_DV_HUB_RECON` | Staged hub keys missing from target | `TABLE(skey BINARY, load_ts TIMESTAMP_NTZ), TABLE(pk_col BINARY)` |
| `DMF_DV_LNK_RECON` | Staged link keys missing from target | `TABLE(skey BINARY, load_ts TIMESTAMP_NTZ), TABLE(pk_col BINARY)` |
| `DMF_DV_SAT_RECON` | Staged (skey+tenant+hashdiff) vs current SAT | `TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, load_ts TIMESTAMP_NTZ), TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, applied_ts TIMESTAMP_NTZ, load_ts TIMESTAMP_NTZ)` |

---

### DMF creation template

Generate the DQ schema and all DMFs:

```sql
CREATE SCHEMA IF NOT EXISTS <dq_database>.<dq_schema>
    COMMENT = 'Data Vault 2.0 Quality Framework — custom DMFs and monitoring views';

USE SCHEMA <dq_database>.<dq_schema>;

-- HUB: surrogate hash key uniqueness
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_HUB_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where the hub surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

-- HUB: 1 business key (tenant_id + bkeycolcode + bkey1)
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_HUB_1BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1 HAVING COUNT(*) > 1)
$$;

-- LNK: own link hash key uniqueness
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where the link surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

-- SAT (regular): skey + load_ts + tenant + hashdiff
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT: Count of rows where (skey, load_ts, tenant_id, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1)
$$;

-- SAT (multi-active): + sequence key
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_MA_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, seq_key NUMBER, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT (multi-active): Count of rows where (skey, load_ts, tenant_id, seq_key, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, seq_key, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, seq_key, hashdiff HAVING COUNT(*) > 1)
$$;

-- SAT (dependent child): + dep child key
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_DP_DUPE(
    arg_t TABLE(skey BINARY, dep_key VARCHAR, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT (dependent child): Count of rows where (skey, dep_key, load_ts, tenant_id, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, dep_key, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, dep_key, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1)
$$;

-- SAT orphan → parent HUB or LNK (excludes GHOST records)
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_SAT_SKEY_ORPH_ERR(
    arg_sat    TABLE(fk_col BINARY, rec_source VARCHAR),
    arg_parent TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT ORPHAN: Count of SAT FK keys (excl GHOST) not in parent HUB/LNK. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_sat s
    WHERE s.rec_source <> 'GHOST'
      AND NOT EXISTS (SELECT 1 FROM arg_parent p WHERE p.pk_col = s.fk_col)
$$;

-- LNK orphan → parent HUB (no GHOST filter needed for links)
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_SKEY_ORPH_ERR(
    arg_lnk TABLE(fk_col BINARY),
    arg_hub TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK ORPHAN: Count of LNK FK keys not in parent HUB. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_lnk l
    WHERE NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = l.fk_col)
$$;

-- HUB recon: staged hub keys not in target
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_HUB_RECON(
    arg_stg TABLE(skey BINARY, load_ts TIMESTAMP_NTZ),
    arg_hub TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 RECON HUB: Count of latest-batch staged hub keys not in target HUB. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = s.skey)
$$;

-- SAT recon: staged (skey+tenant+hashdiff) vs current SAT record
CREATE OR REPLACE DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_RECON(
    arg_stg TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, load_ts TIMESTAMP_NTZ),
    arg_sat TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, applied_ts TIMESTAMP_NTZ, load_ts TIMESTAMP_NTZ))
RETURNS NUMBER
COMMENT = 'DV2 RECON SAT: Count of latest-batch staged records not in current SAT. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (
          SELECT 1 FROM (
              SELECT skey, tenant_id, hashdiff
              FROM arg_sat
              QUALIFY ROW_NUMBER() OVER (
                  PARTITION BY skey, tenant_id
                  ORDER BY applied_ts DESC, load_ts DESC
              ) = 1
          ) curr
          WHERE curr.skey      = s.skey
            AND curr.tenant_id = s.tenant_id
            AND curr.hashdiff  = s.hashdiff
      )
$$;
```

For the full set of all 17 DMFs (including LNK 2-5 HKEY variants and HUB 2-3 BKEY variants), see the reference implementation.

---

### DMF attachment patterns per artefact type

#### Hub attachment

```sql
-- Attach to HUB_<NAME>: surrogate key uniqueness + business key uniqueness
ALTER TABLE <edw_schema>.HUB_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_HUB_SKEY_DUPE_err
        ON (DV_HASHKEY_HUB_<NAME>)
        EXPECTATION hub_<name>_skey_no_dupes (VALUE = 0);

-- Choose 1BKEY, 2BKEY, or 3BKEY depending on number of business keys:
ALTER TABLE <edw_schema>.HUB_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_HUB_1BKEY_DUPE_err
        ON (DV_TENANT_ID, DV_COLLISIONCODE, <BK_COLUMN>)
        EXPECTATION hub_<name>_bkey_no_dupes (VALUE = 0);

ALTER TABLE <edw_schema>.HUB_<NAME>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

#### Link attachment

```sql
-- Attach to LNK_<NAME>: surrogate key + FK combination + orphan checks
ALTER TABLE <edw_schema>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_<NAME>)
        EXPECTATION lnk_<name>_skey_no_dupes (VALUE = 0);

-- Choose 2HKEY, 3HKEY, 4HKEY, or 5HKEY based on participant count:
ALTER TABLE <edw_schema>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_<HUB_A>, DV_HASHKEY_HUB_<HUB_B>)
        EXPECTATION lnk_<name>_hkey_no_dupes (VALUE = 0);

-- Orphan check per FK column (one per hub participant):
ALTER TABLE <edw_schema>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<HUB_A>,
            TABLE(<edw_schema>.HUB_<HUB_A>(DV_HASHKEY_HUB_<HUB_A>)))
        EXPECTATION lnk_<name>_<hub_a>_no_orphans (VALUE = 0);

ALTER TABLE <edw_schema>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<HUB_B>,
            TABLE(<edw_schema>.HUB_<HUB_B>(DV_HASHKEY_HUB_<HUB_B>)))
        EXPECTATION lnk_<name>_<hub_b>_no_orphans (VALUE = 0);

ALTER TABLE <edw_schema>.LNK_<NAME>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

#### Satellite attachment — standard

```sql
-- Attach to SAT_<PARENT>_<CONTEXT>: composite key uniqueness + orphan check
ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_no_dupes (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<PARENT>, DV_RECORDSOURCE,
            TABLE(<edw_schema>.HUB_<PARENT>(DV_HASHKEY_HUB_<PARENT>)))
        EXPECTATION sat_<parent>_<context>_no_orphans (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

#### Satellite attachment — multi-active

```sql
-- Multi-active variant: includes dv_sequence in composite key check
ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_MA_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_SEQUENCE, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_ma_no_dupes (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<PARENT>, DV_RECORDSOURCE,
            TABLE(<edw_schema>.HUB_<PARENT>(DV_HASHKEY_HUB_<PARENT>)))
        EXPECTATION sat_<parent>_<context>_no_orphans (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

#### Satellite attachment — dependent child

```sql
-- Dependent child variant: includes dep child key in composite key check
ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DMF_DV_SAT_DP_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, <DEP_KEY_COLUMN>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_dp_no_dupes (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <dq_database>.<dq_schema>.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<PARENT>, DV_RECORDSOURCE,
            TABLE(<edw_schema>.HUB_<PARENT>(DV_HASHKEY_HUB_<PARENT>)))
        EXPECTATION sat_<parent>_<context>_no_orphans (VALUE = 0);

ALTER TABLE <edw_schema>.SAT_<PARENT>_<CONTEXT>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
```

---

### DMF selection guide

| Artefact type | Required DMFs | Choose variant |
|---|---|---|
| Hub (1 BK) | `HUB_SKEY_DUPE` + `HUB_1BKEY_DUPE` | — |
| Hub (2 BK) | `HUB_SKEY_DUPE` + `HUB_2BKEY_DUPE` | — |
| Hub (3 BK) | `HUB_SKEY_DUPE` + `HUB_3BKEY_DUPE` | — |
| Link (2 hubs) | `LNK_SKEY_DUPE` + `LNK_2HKEY_DUPE` + `LNK_SKEY_ORPH_ERR` x2 | — |
| Link (3 hubs) | `LNK_SKEY_DUPE` + `LNK_3HKEY_DUPE` + `LNK_SKEY_ORPH_ERR` x3 | — |
| Link (4 hubs) | `LNK_SKEY_DUPE` + `LNK_4HKEY_DUPE` + `LNK_SKEY_ORPH_ERR` x4 | — |
| Link (5 hubs) | `LNK_SKEY_DUPE` + `LNK_5HKEY_DUPE` + `LNK_SKEY_ORPH_ERR` x5 | — |
| Standard SAT | `SAT_DUPE` + `SAT_SKEY_ORPH_ERR` | — |
| Multi-active SAT | `SAT_MA_DUPE` + `SAT_SKEY_ORPH_ERR` | — |
| Dependent child SAT | `SAT_DP_DUPE` + `SAT_SKEY_ORPH_ERR` | — |

---

### Querying DMF results

After DMFs are attached and have run at least once, query results from the Snowflake-managed view:

```sql
-- Current expectation status (pass/fail per table per DMF)
SELECT
    metric_database,
    metric_schema,
    metric_name,
    table_database,
    table_schema,
    table_name,
    expectation_name,
    expectation_status,    -- 'MET' or 'NOT_MET'
    metric_value,          -- 0 = pass, >0 = violation count
    measurement_time
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
WHERE expectation_status = 'NOT_MET'
ORDER BY measurement_time DESC;

-- Summary: how many tables are passing vs failing
SELECT
    expectation_status,
    COUNT(*) AS check_count
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
GROUP BY expectation_status;
```

### Schedule options

| Schedule | When to use |
|---|---|
| `TRIGGER_ON_CHANGES` | Default — fires asynchronously after each DML commit. Best for most vault loads. |
| `USING CRON '0 */6 * * *'` | Fixed schedule — every 6 hours. Use when TRIGGER_ON_CHANGES is too frequent. |
| `'5 MINUTE'` | Interval-based — every 5 minutes. Use for near-real-time monitoring. |

---

## Output format

Present all test queries (ad-hoc) or DMF attachments (DMF mode) grouped by category. For each test, show:
- Test name
- SQL (query or DDL)
- Expected result
- Severity (CRITICAL / WARNING)

| Category | Severity |
|---|---|
| Hashkey uniqueness | CRITICAL |
| Ghost record existence | CRITICAL |
| Orphan detection | CRITICAL |
| Row count reconciliation | WARNING |
| Hashdiff collision | WARNING |
| Sequence integrity | CRITICAL |
| Timeline integrity | CRITICAL |
| Referential integrity | CRITICAL |

---

## Rules

- All ad-hoc test queries must be read-only (SELECT only)
- Ghost record filters: use `WHERE dv_recordsource != 'GHOST'` when excluding ghosts from counts
- Do not generate tests for tables that don't exist yet — confirm with the user first
- DMF expectations always use `VALUE = 0` — zero errors = pass
- DMFs are created once in the DQ schema and reused across all vault tables
- Each `ALTER TABLE ADD DATA METRIC FUNCTION` must match the exact DMF signature (column order matters)
- DMF schedule `TRIGGER_ON_CHANGES` is recommended as default — it fires asynchronously after each DML commit

---

## Reference implementation

The full framework (17 DMFs + Streamlit dashboard + Slack alerts + deploy script) is available at:

[github.com/PatrickCuba/the_data_must_flow/data-auto-testing/snowflake_dmf](https://github.com/PatrickCuba/the_data_must_flow/tree/master/data-auto-testing/snowflake_dmf)

Features beyond this skill:
- 9-tab Streamlit monitoring dashboard (deploy with `snow streamlit deploy`)
- Slack alert integration (immediate + daily EOD report)
- DMF coverage analysis via `SNOWFLAKE.ACCOUNT_USAGE.DATA_METRIC_FUNCTION_REFERENCES`
- One-command deploy script (`./scripts/deploy.sh`)

---

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
