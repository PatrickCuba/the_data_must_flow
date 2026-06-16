---
name: dv-test
description: Generate Data Vault integrity tests — ad-hoc validation queries OR Snowflake DMF-based continuous monitoring with 17 reusable Data Metric Functions.
enabled: true
---

# /dv-test — Vault Integrity Tests

Two modes: **ad-hoc queries** for one-time validation/debugging, or **DMF-based continuous monitoring** using Snowflake Data Metric Functions that fire automatically after every DML commit.

**Three-tier testing taxonomy**

All data vault tests fall into one of three tiers by purpose:

| Tier | Purpose | When to run | Examples in this skill |
|---|---|---|---|
| **Pro-active (warranty)** | Prevent bad data entering the vault — run *before* loading | Before each load cycle, in the staging layer | Vertical hash key integrity, horizontal hash key cross-check, staging row count vs. source |
| **Reactive (trust)** | Detect integrity degradation after loading — run *forever* | After each load cycle, or continuously via DMF | Hub/link duplicate detection, orphan checks, satellite composite key uniqueness, hashdiff collision |
| **Periodic integrity** | Verify source recreation capability — run on a schedule | Weekly or monthly (not daily — expensive) | Row count reconciliation back to source, business key consistency hub ↔ satellite |

Pro-active tests run in staging before the vault is touched — they act as a quality gate. Reactive tests run continuously and alert when existing data degrades. Periodic integrity tests are the most thorough and confirm the vault's audit guarantee: "Can I recreate the source at any point in time?" Run them when you need to verify the audit trail, not on every load.

## Input

Ask the user:
1. Which constructs to test? (all, or a specific hub/link/satellite)
2. What schema are the vault tables in?
3. **Which test mode(s)?**
   - Mode 1: Ad-hoc queries (one-time debugging/validation)
   - Mode 2: DMF continuous monitoring (async, dashboard-friendly, TRIGGER_ON_CHANGES)
   - Mode 3: Reconciliation table framework (stateful, INSERT results per load, same-transaction for Kappa)
4. (Mode 2 — DMF) What database/schema should the DMFs live in?
5. (Mode 3 — Recon) Is this Kappa Vault? (if yes, tests run inside the same BEGIN TRANSACTION / COMMIT as the load)

**Deployment decision matrix:**

| Your setup | Recommended mode(s) |
|---|---|
| Kappa Vault (stream-triggered, need load-time validation) | Mode 3 (recon) — same transaction guarantees exact-record validation |
| Standard batch (want continuous monitoring between loads) | Mode 2 (DMF) — async, dashboard, Slack alerts |
| Production (both load-time + continuous) | Mode 2 + Mode 3 — recon validates each load; DMF catches drift between loads |
| One-time migration check or debugging | Mode 1 (ad-hoc) — no infrastructure needed |

---

## Six technical DQ dimensions — conceptual taxonomy

The tests in this skill map to six standard data quality dimensions. Understanding which dimension a test addresses helps prioritise coverage and communicate results to business stakeholders.

| Dimension | What it measures | DV artefacts most affected | Example tests in this skill |
|---|---|---|---|
| **Accuracy / Veracity** | Is the data correct and free from errors? | Satellites (state records), staging | Hashdiff collision, row count reconciliation vs. source |
| **Completeness** | Are all expected records and attributes present? | Hubs (missing BKs), staging | Row count reconciliation, staged BKs present in hub, orphan detection |
| **Conformity** | Does data match agreed format and SLA standards? | Staging (hard rule boundary), satellites | Business key format checks, column NOT NULL checks |
| **Consistency** | Is the data internally consistent across sources and over time? | Links (cross-source joins), satellites | Orphan checks, hub/satellite BK alignment, multi-active sequence integrity |
| **Timeliness / Freshness** | Did data arrive on time and in the correct sequence? | All artefacts | Load timestamp ordering, out-of-sequence detection, satellite timeline integrity |
| **Uniqueness** | Is there exactly one current state per business key? | Hubs, satellites | Hub hashkey uniqueness, satellite composite key uniqueness, multi-active sequence integrity |

Business process quality (a 7th dimension) measures coverage of business rules — e.g. a customer receiving a home loan but no address supplied. These checks are best captured as BV satellites in a **Business Quality Vault** — derived from raw vault content and subject to the same auditability guarantees.

---

## Mode 1: Ad-hoc validation queries

Standalone SELECT queries for one-time integrity checks. Use for debugging, post-migration validation, or environments where DMFs are not available.

---

## Optional — Vertical and horizontal hash key tests (pro-active / pre-load)

> **These tests are optional.** They are most valuable in complex multi-source vaults or when a hash algorithm misconfiguration is suspected. Run them in staging before loading to the vault.

**Vertical test — hash key uniqueness and determinism within staging**

Verifies that the hash function is applied consistently within the staging layer: the same business key combination always produces the same hash, and the same hash never maps to two different business key combinations.

```sql
-- Vertical test 1: does same (BKCC + BK) always produce exactly one hash key?
SELECT dv_collisioncode, <bk_column>, COUNT(DISTINCT dv_hashkey_hub_<name>) AS hash_count
FROM <staging_view>
GROUP BY dv_collisioncode, <bk_column>
HAVING hash_count > 1;  -- Any result = hash collision — investigate

-- Vertical test 2: does one hash key always map to exactly one (BKCC + BK)?
SELECT dv_hashkey_hub_<name>, COUNT(DISTINCT dv_collisioncode || '||' || <bk_column>) AS key_count
FROM <staging_view>
GROUP BY dv_hashkey_hub_<name>
HAVING key_count > 1;  -- Any result = hash collision — investigate
```

**Horizontal test — staging hash matches vault hash for same business key**

Cross-checks that the staging view generates the same hash key as what is already loaded in the target hub. Detects hash algorithm drift (e.g. staging was reconfigured to MD5 but vault was loaded with SHA1).

```sql
-- Horizontal test: does staging produce the same hash as the loaded hub for matching BKs?
SELECT s.dv_collisioncode, s.<bk_column>,
       s.dv_hashkey_hub_<name>  AS staging_hash,
       h.dv_hashkey_hub_<name>  AS vault_hash
FROM <staging_view> s
JOIN <vault_schema>.HUB_<NAME> h
    ON h.<bk_column> = s.<bk_column>
    AND h.dv_collisioncode = s.dv_collisioncode
WHERE s.dv_hashkey_hub_<name> != h.dv_hashkey_hub_<name>;
-- Any result = hash algorithm mismatch between staging and vault — STOP LOADING
```

Run both tests in the staging pipeline as a pre-load quality gate. If either returns rows, **abort processing immediately and investigate**. A hash collision in production means two different business entities share the same hash key — every downstream join, PIT, and bridge will silently produce incorrect results. This is not a warning; it is a hard stop.

**Formal protocol name:** Lateral (horizontal: stage-vs-target) and Vertical (within-stage) collision checks. The vertical check validates internal staging consistency; the lateral check validates cross-system consistency between staging and the loaded vault.

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
    COMMENT = 'Pragmatic Data Vault Quality Framework — custom DMFs and monitoring views';

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

## Mode 3: Reconciliation streams on vault tables

An alternative to DMF-based monitoring is placing Snowflake Streams **directly on vault tables** for load-cycle reconciliation. This approach is distinct from Kappa Vault loading streams (which sit on staging views) and provides per-load-cycle test triggers.

**How it works:**
- After each hub/link/satellite table is created, place a Snowflake Stream on it: `CREATE STREAM <schema>.RECON_<TABLE_NAME> ON TABLE <table>`
- Tests downstream of each loader task check `SYSTEM$STREAM_HAS_DATA('RECON_<TABLE_NAME>')` before executing
- If the stream is empty (no new data in this load cycle), the test task skips — no wasted compute
- If the stream has data, tests run against the delta (exactly what was just loaded)

**Per-table test checklist using reconciliation streams:**

| Artefact | Tests to run when stream has data |
|---|---|
| **Hub** | BK duplicates in this load; hash key duplicates; staged BKs and hash keys are in the hub table; row count (new in stream, total in hub) |
| **Link** | Hash key duplicates; staged link + hub hash keys are in the link table; row count; for each parent hub: check link's hub hash key exists in the parent hub (orphan check) |
| **Satellite** | Hash key + load_timestamp duplicates for current records; staged parent hash keys exist in parent hub/link; staged hashdiff exists in satellite current view; row count; satellite hash key exists in parent (orphan check) |

**Critical for satellite orphan checks:** Run orphan checks AFTER the parent hub or link has loaded (use explicit `AFTER` task dependency). An empty orphan stream means the check can be skipped entirely.

**This is distinct from:**
- **Kappa Vault streams** — placed on staging views, used for loading (not testing)
- **DMF monitoring** — asynchronous, platform-managed, always-on monitoring (not load-cycle-scoped)

Reconciliation streams give tighter coupling to the load cycle: each test knows exactly which records were loaded and tests only those. DMFs give broader continuous monitoring across all loads over time. Both can coexist.

---

## Reference implementation

The full framework (17 DMFs + Streamlit dashboard + Slack alerts + deploy script) is available at:

[github.com/PatrickCuba/the_data_must_flow/data-auto-testing/snowflake_dmf](https://github.com/PatrickCuba/the_data_must_flow/tree/master/data-auto-testing/snowflake_dmf)

## Standard test counts as business observability metrics

The standard DV reconciliation tests produce counts that are not just integrity signals \u2014 they are also **business observability metrics** collected at the finest grain (per load cycle):

| Test | What it counts | Business interpretation |
|---|---|---|
| Hub reconciliation (new BKs in load) | New business entity keys loaded | Business entity growth: new customers, new accounts, new products added per period |
| Link reconciliation (new relationships in load) | New interactions/transactions loaded | Business process uptake: new account-customer relationships, new order-product interactions |
| Satellite reconciliation (new state records in load) | True changes to entity or relationship state | Business process activity: how actively the source application is processing changes (business effectiveness) |

These metrics are available at zero marginal cost \u2014 they are a by-product of the integrity check. Aggregate them across load cycles to build business performance dashboards without any additional data modelling. The thinnest grain of data (staged \u2192 loaded delta) is the most efficient place to capture them.

Use Business Vault to extend this into a **Business Quality Vault** \u2014 BV satellites that aggregate and curate these raw test metrics into business-facing KPIs (customer growth rate, transaction volume trend, data freshness SLA compliance).

Features beyond this skill:
- 9-tab Streamlit monitoring dashboard (deploy with `snow streamlit deploy`)
- Slack alert integration (immediate + daily EOD report)
- DMF coverage analysis via `SNOWFLAKE.ACCOUNT_USAGE.DATA_METRIC_FUNCTION_REFERENCES`
- One-command deploy script (`./scripts/deploy.sh`)

---

## Distribution statistics — gather at load time, not full-table scan

Running distribution statistics (skewness, kurtosis, cardinality) directly against large historical satellite tables is expensive — billions of rows, full-table scans. A more efficient approach: **gather statistics on staged content at load time**.

Staged content is already in memory for the current load cycle. It is a small subset of the full historical table. Computing distribution metrics on staged data gives continuous statistical monitoring at near-zero marginal cost, aggregated per `(dv_applied_timestamp, dv_load_timestamp)` — the natural velocity of vault loading.

Store per-load statistics as a JSON/VARIANT column in the reconciliation table alongside the existing duplicate/orphan check results. Not all columns need measurement — select columns of interest (typically join key columns and high-cardinality attributes relevant to PIT query performance).

### Snowflake approximation functions for distribution monitoring

Exact distribution calculations require full-table scans (expensive). Use Snowflake's built-in approximation functions instead:

| Function | Algorithm | Use in DV context |
|---|---|---|
| `APPROX_TOP_K(col, k)` | Sketch-based | Identify the most frequent hash key values in staged content — high frequency concentration signals probe-side skewness that could degrade hash-join performance |
| `APPROX_COUNT_DISTINCT(col)` | HyperLogLog | Estimate distinct cardinality of a column without a full sort. Use on hub BK columns and satellite hash keys to monitor key space growth |
| `APPROX_PERCENTILE(col, percentile)` | t-Digest | Estimate percentile values (e.g. p50, p95, p99) for numeric columns. Use to detect outliers and distribution drift in attribute columns |

Example — checking hash key frequency concentration in staged content:

```sql
SELECT
    dv_applied_timestamp,
    dv_load_timestamp,
    APPROX_TOP_K(dv_hashkey_hub_party, 10)    AS top_10_keys,
    APPROX_COUNT_DISTINCT(dv_hashkey_hub_party) AS distinct_key_count,
    COUNT(*)                                    AS total_staged_rows
FROM LIB_PRD01_ODS.ODS_STG.STG_SAPBW_COMM_CUSTOMER
GROUP BY dv_applied_timestamp, dv_load_timestamp;
```

High concentration in `top_10_keys` relative to `distinct_key_count` signals skewness that will impact hash-join probe performance for this satellite.

### PII caution

Do not capture plain text values for PII columns (SSN, name, DOB, passport) in the statistics store. The stats table becomes another location to manage for data privacy and GDPR compliance. Instead:
- Use the surrogate hash key column (`dv_hashkey_hub_*`) as the distribution measure key — it encodes no PII
- If plain text column statistics are needed, tokenize the values before storing them in the stats table

---

## Mode 3: Reconciliation table framework

A stateful test framework that INSERTs test results into dedicated reconciliation tables after each load. This is the **original Data Vault test automation pattern** — designed for load-time validation where each load cycle produces an auditable record of data quality.

**When to use Mode 3 (instead of or alongside DMF):**

| Scenario | Use Mode 3 | Use DMF | Use both |
|---|---|---|---|
| Kappa Vault (same-transaction validation) | YES — only recon can run inside `BEGIN TRANSACTION / COMMIT` | Cannot participate in same transaction | Recon for load-time, DMF for async monitoring |
| Standard batch (scheduled loads) | YES — test runs inline with load task | YES — async monitoring between loads | Recommended |
| Need auditable test history (when did quality degrade?) | YES — each load produces a timestamped row | YES — `SNOWFLAKE.LOCAL` stores results | Either or both |
| Dashboard-first monitoring | Not ideal — requires custom queries | YES — Streamlit dashboard built-in | DMF preferred for dashboards |
| Post-migration spot check | Ad-hoc queries (Mode 1) are simpler | Overkill for one-time check | No |

**Key advantage over DMF**: the reconciliation framework runs in the **same transaction** as the load. This means:
1. The test validates the exact same records that were just loaded (Repeatable Read isolation)
2. If the test finds errors, you can ROLLBACK — the load is undone
3. The stream advances ONLY on COMMIT — if anything fails, it rewinds

DMFs fire asynchronously after the DML commits. They cannot roll back the load.

### Decision question

Add this to the decision flow:

> **Which test framework(s) should be deployed?**
> - Recon table framework (Mode 3): for load-time validation, same-transaction integrity, auditable test history
> - DMF continuous monitoring (Mode 2): for async monitoring, Streamlit dashboard, Slack alerts, TRIGGER_ON_CHANGES automation
> - Both (recommended for production): recon validates each load; DMF catches issues between loads

### Reconciliation tables — DDL (8 tables in `utilities` schema)

```sql
-- 1. Hub duplicate errors
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_HUB_DUPLICATE_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    HUB_SKEY_DUPE_err       INT      NOT NULL,  -- surrogate key dupe count
    HUB_SKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    HUB_BKEY_DUPE_err       INT      NOT NULL,  -- business key dupe count
    HUB_BKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_hub_dupe PRIMARY KEY (tablename, source_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 2. Hub reconciliation errors (staged → target)
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_HUB_RECONCILIATION_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    HUB_SKEY_SGTG_ncnt     INT      NOT NULL,  -- new count (from recon stream)
    HUB_SKEY_SGTG_scnt     INT      NOT NULL,  -- staged count
    HUB_SKEY_SGTG_dcnt     INT      NOT NULL,  -- distinct count
    HUB_SKEY_SGTG_total    INT      NOT NULL,  -- target total
    HUB_SKEY_SGTG_err      INT      NOT NULL,  -- staged keys missing from target
    HUB_SKEY_SGTG_src_columns ARRAY  NOT NULL,
    HUB_SKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    HUB_BKEY_SGTG_err      INT      NOT NULL,  -- BK reconciliation errors
    HUB_BKEY_SGTG_src_columns ARRAY  NOT NULL,
    HUB_BKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_hub_recon PRIMARY KEY (tablename, source_tablename, loaddate, rundate, HUB_SKEY_SGTG_src_columns) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 3. Satellite duplicate errors
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_SAT_DUPLICATE_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    SAT_SKEY_DUPE_err       INT      NOT NULL,  -- composite PK dupe count
    SAT_SKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_sat_dupe PRIMARY KEY (tablename, source_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 4. Satellite reconciliation errors (staged → target)
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    SAT_SKEY_SGTG_ncnt     INT      NOT NULL,
    SAT_SKEY_SGTG_scnt     INT      NOT NULL,
    SAT_SKEY_SGTG_dcnt     INT      NOT NULL,
    SAT_SKEY_SGTG_total    INT      NOT NULL,
    SAT_SKEY_SGTG_err      INT      NOT NULL,
    SAT_SKEY_SGTG_src_columns ARRAY  NOT NULL,
    SAT_SKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    SAT_HDIF_SGTG_err      INT      NOT NULL,  -- hashdiff reconciliation errors
    SAT_HDIF_SGTG_src_columns ARRAY  NOT NULL,
    SAT_HDIF_SGTG_src_hdiff_columns ARRAY NOT NULL,
    SAT_HDIF_SGTG_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_sat_recon PRIMARY KEY (tablename, source_tablename, loaddate, rundate, SAT_SKEY_SGTG_src_columns) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 5. Satellite referential errors (orphans)
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_SAT_REFERENTIAL_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    parent_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    SAT_SKEY_ORPH_err       INT      NOT NULL,  -- orphan count
    CONSTRAINT pk_recon_sat_orph PRIMARY KEY (tablename, parent_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 6. Link duplicate errors
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_LNK_DUPLICATE_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    LNK_SKEY_DUPE_err      INT      NOT NULL,
    LNK_SKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    LNK_HKEY_DUPE_err      INT      NOT NULL,
    LNK_HKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_lnk_dupe PRIMARY KEY (tablename, source_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 7. Link reconciliation errors
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_LNK_RECONCILIATION_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    LNK_SKEY_SGTG_scnt     INT      NOT NULL,
    LNK_SKEY_SGTG_dcnt     INT      NOT NULL,
    LNK_SKEY_SGTG_ncnt     INT      NOT NULL,
    LNK_SKEY_SGTG_total    INT      NOT NULL,
    LNK_SKEY_SGTG_err      INT      NOT NULL,
    LNK_SKEY_SGTG_src_columns ARRAY  NOT NULL,
    LNK_SKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    LNK_HKEY_SGTG_err      INT      NOT NULL,
    LNK_BKEY_SGTG_src_columns ARRAY  NOT NULL,
    LNK_HKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_lnk_recon PRIMARY KEY (tablename, source_tablename, loaddate, rundate, LNK_SKEY_SGTG_src_columns) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- 8. Link referential errors (orphans — one check per FK column)
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_LNK_REFERENTIAL_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    parent_tablename    VARCHAR(200) NOT NULL,
    link_columnname     ARRAY        NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    LNK_SKEY_ORPH_err      INT      NOT NULL,
    CONSTRAINT pk_recon_lnk_orph PRIMARY KEY (tablename, parent_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);
```

### Reconciliation streams — one pair per target table

```sql
-- Orphan check stream (fires when satellite inserts)
CREATE STREAM IF NOT EXISTS <DATABASE>.UTILITIES.ORPHANCHECK_<SAT_NAME>
    ON TABLE <DATABASE>.VAULT.<SAT_NAME>
    APPEND_ONLY = TRUE;

-- Reconciliation stream (fires when target inserts)
CREATE STREAM IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_<TABLE_NAME>
    ON TABLE <DATABASE>.VAULT.<TABLE_NAME>
    APPEND_ONLY = TRUE;
```

### Test queries — Hub duplicate check

```sql
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_HUB_DUPLICATE_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     HUB_SKEY_DUPE_err, HUB_SKEY_DUPE_tgt_columns,
     HUB_BKEY_DUPE_err, HUB_BKEY_DUPE_tgt_columns)
WITH HUB_SKEY_DUPE AS (
    SELECT COUNT(e) AS HUB_SKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_<hub>') AS HUB_SKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.HUB_<HUB>
          GROUP BY dv_hashkey_hub_<hub> HAVING COUNT(*) > 1) sq
),
HUB_BKEY_DUPE AS (
    SELECT COUNT(e) AS HUB_BKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_tenant_id', 'dv_collisioncode', '<bk_col>') AS HUB_BKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.HUB_<HUB>
          GROUP BY dv_tenant_id, dv_collisioncode, <bk_col> HAVING COUNT(*) > 1) sq
)
SELECT '<hub_table>' AS tablename,
       '<source_table>' AS source_tablename,
       <loaddate> AS loaddate,
       CURRENT_TIMESTAMP() AS rundate,
       HUB_SKEY_DUPE_err, HUB_SKEY_DUPE_tgt_columns,
       HUB_BKEY_DUPE_err, HUB_BKEY_DUPE_tgt_columns
FROM HUB_SKEY_DUPE, HUB_BKEY_DUPE;
```

### Test queries — Satellite reconciliation check

```sql
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     SAT_SKEY_SGTG_ncnt, SAT_SKEY_SGTG_scnt, SAT_SKEY_SGTG_dcnt, SAT_SKEY_SGTG_total,
     SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
     SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_src_hdiff_columns, SAT_HDIF_SGTG_tgt_columns)
WITH SAT_SKEY_SGTG AS (
    SELECT COUNT(*) AS SAT_SKEY_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_<parent>') AS SAT_SKEY_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_<parent>') AS SAT_SKEY_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.<STREAM_OR_STAGING> sg
    WHERE NOT EXISTS (
        SELECT 1 FROM <DATABASE>.VAULT.<SAT_NAME> s
        WHERE sg.dv_hashkey_hub_<parent> = s.dv_hashkey_hub_<parent>
    )
),
SAT_HDIF_SGTG AS (
    SELECT COUNT(*) AS SAT_HDIF_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_<parent>', 'dv_hashdiff_<sat>', 'dv_tenant_id') AS SAT_HDIF_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_<parent>', 'dv_hashdiff', 'dv_tenant_id') AS SAT_HDIF_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.<STREAM_OR_STAGING> sg
    WHERE NOT EXISTS (
        SELECT 1 FROM <DATABASE>.VAULT.VC_<SAT_NAME> s
        WHERE sg.dv_hashkey_hub_<parent> = s.dv_hashkey_hub_<parent>
          AND sg.dv_hashdiff_<sat> = s.dv_hashdiff
          AND sg.dv_tenant_id = s.dv_tenant_id
    )
),
Fetch_Stats_New AS (SELECT COUNT(*) AS SAT_SKEY_SGTG_ncnt FROM <DATABASE>.UTILITIES.RECONCILE_<SAT_NAME>),
Fetch_Stats_Staged AS (SELECT COUNT(*) AS SAT_SKEY_SGTG_scnt, COUNT(DISTINCT dv_hashkey_hub_<parent>) AS SAT_SKEY_SGTG_dcnt FROM <DATABASE>.STAGED.<STREAM_OR_STAGING>),
Fetch_Stats_Total AS (SELECT COUNT(*) AS SAT_SKEY_SGTG_total FROM <DATABASE>.VAULT.<SAT_NAME>)
SELECT '<sat_table>' AS tablename,
       '<source_table>' AS source_tablename,
       <loaddate> AS loaddate,
       CURRENT_TIMESTAMP() AS rundate,
       SAT_SKEY_SGTG_ncnt, SAT_SKEY_SGTG_scnt, SAT_SKEY_SGTG_dcnt, SAT_SKEY_SGTG_total,
       SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
       SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns,
       ARRAY_CONSTRUCT(<hashdiff_source_columns>) AS SAT_HDIF_SGTG_src_hdiff_columns,
       SAT_HDIF_SGTG_tgt_columns
FROM SAT_SKEY_SGTG, SAT_HDIF_SGTG, Fetch_Stats_New, Fetch_Stats_Staged, Fetch_Stats_Total;
```

### Test queries — Satellite orphan check

```sql
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_REFERENTIAL_ERRORS
    (tablename, parent_tablename, loaddate, rundate, SAT_SKEY_ORPH_err)
SELECT '<sat_table>' AS tablename,
       '<hub_table>' AS parent_tablename,
       <loaddate> AS loaddate,
       CURRENT_TIMESTAMP() AS rundate,
       COUNT(*) AS SAT_SKEY_ORPH_err
FROM <DATABASE>.UTILITIES.ORPHANCHECK_<SAT_NAME> s
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.HUB_<PARENT> p
    WHERE s.dv_hashkey_hub_<parent> = p.dv_hashkey_hub_<parent>
)
AND s.dv_recordsource <> 'GHOST';
```

### Kappa Vault integration — load + test in same transaction

In Kappa Vault mode, the test reads the **same stream** as the loader inside `BEGIN TRANSACTION / COMMIT`. No separate reconciliation stream is needed — the stream still shows the unprocessed rows within the transaction:

```sql
BEGIN TRANSACTION;

-- LOAD (satellite with discard_view)
INSERT INTO <DATABASE>.VAULT.<SAT_NAME> (...)
WITH discard_view AS (...) SELECT ... FROM discard_view stg WHERE NOT EXISTS (...) OR dv_cnt > 1;

-- TEST (same stream, same transaction)
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS (...)
WITH SAT_SKEY_SGTG AS (
    SELECT COUNT(*) AS SAT_SKEY_SGTG_err, ...
    FROM <DATABASE>.STAGED.<STREAM> sg   -- same stream as the loader
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.<SAT_NAME> s
                      WHERE sg.dv_hashkey_hub_<parent> = s.dv_hashkey_hub_<parent>)
), ...
SELECT ...;

COMMIT;
-- Stream advances here. Both load AND test results committed atomically.
-- If either fails → ROLLBACK → stream rewinds → retry on next task execution.
```

### Querying reconciliation results

```sql
-- Most recent test run per table
SELECT tablename, loaddate, rundate,
       HUB_SKEY_DUPE_err, HUB_BKEY_DUPE_err
FROM <DATABASE>.UTILITIES.RECONCILE_HUB_DUPLICATE_ERRORS
QUALIFY ROW_NUMBER() OVER (PARTITION BY tablename ORDER BY rundate DESC) = 1;

-- Error trend over time
SELECT DATE_TRUNC('day', rundate) AS day,
       tablename,
       SUM(SAT_SKEY_SGTG_err) AS total_recon_errors
FROM <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS
GROUP BY 1, 2
ORDER BY 1, 2;
```

### Reference implementation

[github.com/PatrickCuba/the_data_must_flow/data-auto-testing](https://github.com/PatrickCuba/the_data_must_flow/tree/master/data-auto-testing)

---

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
