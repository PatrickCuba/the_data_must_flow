-- DVOS Example: XTS-Assisted Late-Arriving Data
-- Extended Tracking Satellite handles out-of-sequence records that arrive
-- with an applied_timestamp older than the latest loaded record.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- Scenario: File-based source delivers corrections/amendments for past dates.
-- Without XTS, these late records would be silently ignored or inserted out of order.
-- XTS detects and replays the correct timeline.
--
-- IMPORTANT: XTS is INCOMPATIBLE with Kappa Vault (no batch boundary for SWITCH).

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE TRANSIENT SCHEMA IF NOT EXISTS <DATABASE>.STAGED;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.VAULT;

-- ============================================================================
-- XTS TABLE — Extended Tracking Satellite (one per hub, tracks all satellites)
-- Naming: SAT_XT_{PARENT_TYPE}_{PARENT}
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_XT_HUB_POLICY (
    dv_tenant_id             VARCHAR(50)   NOT NULL,
    dv_hashkey_hub_policy    BINARY(20)    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ NOT NULL,
    dv_recordsource          VARCHAR(255)  NOT NULL,
    dv_hashdiff              BINARY(20)    NOT NULL,
    dv_rectarget             VARCHAR(40)   NOT NULL,
    dv_sequence_violation    BOOLEAN       NOT NULL,
    CONSTRAINT pk_sat_xt_hub_policy
        PRIMARY KEY (dv_hashkey_hub_policy, dv_load_timestamp) NOT ENFORCED
);

-- ============================================================================
-- TARGET SATELLITE (standard, but XTS-assisted)
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE (
    dv_hashkey_hub_policy    BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_xts_event             VARCHAR(20),      -- 'insert' (new record) or 'copy' (timeline correction)
    coverage_type            VARCHAR,
    premium_amount           NUMBER(18,2),
    coverage_start_date      DATE,
    coverage_end_date        DATE,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_hub_policy_ins_coverage
        PRIMARY KEY (dv_hashkey_hub_policy, dv_load_timestamp) NOT ENFORCED
);

-- Ghost record (required for PIT/SNOPIT equi-joins)
INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE (
    dv_hashkey_hub_policy, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp, dv_xts_event
)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST',
       'GHOST', TO_BINARY(REPEAT(0, 20)),
       '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ, NULL
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE
    WHERE dv_hashkey_hub_policy = TO_BINARY(REPEAT(0, 20))
);

-- ============================================================================
-- STAGING VIEW — standard hashkey/hashdiff computation
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE AS
SELECT
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' || 'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(policy_number AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_policy,

    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(coverage_type AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(premium_amount AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(coverage_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(coverage_end_date AS STRING)), '')
    )) AS dv_hashdiff,

    'default'                   AS dv_tenant_id,
    'default'                   AS dv_collisioncode,
    CURRENT_TIMESTAMP()         AS dv_load_timestamp,
    effective_date              AS dv_applied_timestamp,  -- source-supplied business date
    'INSURANCE.POLICY_COVERAGE' AS dv_recordsource,

    policy_number, coverage_type, premium_amount,
    coverage_start_date, coverage_end_date
FROM <DATABASE>.STAGED.LANDING_INS_POLICY_COVERAGE;

-- ============================================================================
-- XTS LOAD — Step 1: Insert timeline entries for all records in this batch
-- ============================================================================

INSERT INTO <DATABASE>.VAULT.SAT_XT_HUB_POLICY (
    dv_tenant_id, dv_hashkey_hub_policy, dv_load_timestamp, dv_applied_timestamp,
    dv_recordsource, dv_hashdiff, dv_rectarget, dv_sequence_violation
)
SELECT DISTINCT
    src.dv_tenant_id,
    src.dv_hashkey_hub_policy,
    src.dv_load_timestamp,
    src.dv_applied_timestamp,
    src.dv_recordsource,
    src.dv_hashdiff AS dv_hashdiff,
    'sat_rv_hub_policy_ins_coverage' AS dv_rectarget,
    FALSE AS dv_sequence_violation  -- updated by SWITCH logic below
FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE src
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_XT_HUB_POLICY x
    WHERE x.dv_hashkey_hub_policy = src.dv_hashkey_hub_policy
      AND x.dv_applied_timestamp = src.dv_applied_timestamp
      AND x.dv_rectarget = 'sat_rv_hub_policy_ins_coverage'
);

-- ============================================================================
-- OUT-OF-SEQUENCE SWITCH — Step 2: Detect late arrivals
-- Compare MAX(staged applied) vs MAX(satellite applied).
-- If staged is OLDER, the batch is out-of-sequence → use XTS-influenced load.
-- If staged is NEWER, use normal satellite load (but still populate XTS above).
-- ============================================================================

-- Session variable approach (used in stored procedure automation):
SET xts_out_of_sequence_event = (
    WITH xts_staged AS (
        SELECT MAX(dv_applied_timestamp) AS stg_max_date
        FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE
    ),
    xts_loaded AS (
        SELECT MAX(dv_applied_timestamp) AS sat_max_date
        FROM <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE
    )
    SELECT CASE WHEN stg_max_date < sat_max_date THEN TRUE ELSE FALSE END
    FROM xts_staged, xts_loaded
);

-- ============================================================================
-- SATELLITE LOAD — Step 3: XTS-influenced timeline-correcting UNION ALL
-- Uses previous_xts / next_xts CTEs to determine what state existed before
-- and after the staged record in the XTS timeline.
-- Run when $xts_out_of_sequence_event = TRUE.
-- When FALSE, use the normal satellite load (standard anti-semi join).
-- ============================================================================

-- IF $xts_out_of_sequence_event = TRUE THEN:

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE (
    dv_hashkey_hub_policy, dv_tenant_id, dv_recordsource,
    dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
    dv_xts_event, coverage_type, premium_amount,
    coverage_start_date, coverage_end_date
)
WITH previous_xts AS (
    -- Find the XTS entry OLDER than the staged record for each entity.
    -- This represents the state that existed BEFORE the late-arriving record.
    SELECT dv_hashkey_hub_policy, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
           RANK() OVER (PARTITION BY dv_hashkey_hub_policy
                        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) AS dv_rnk
    FROM <DATABASE>.VAULT.SAT_XT_HUB_POLICY xts
    WHERE dv_rectarget = 'sat_rv_hub_policy_ins_coverage'
      AND EXISTS (SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE stg
                  WHERE stg.dv_hashkey_hub_policy = xts.dv_hashkey_hub_policy
                    AND stg.dv_applied_timestamp > xts.dv_applied_timestamp)
    QUALIFY dv_rnk = 1
),
next_xts AS (
    SELECT dv_hashkey_hub_policy, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
           RANK() OVER (PARTITION BY dv_hashkey_hub_policy
                        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) AS dv_rnk
    FROM <DATABASE>.VAULT.SAT_XT_HUB_POLICY xts
    WHERE dv_rectarget = 'sat_rv_hub_policy_ins_coverage'
      AND EXISTS (SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE stg
                  WHERE stg.dv_hashkey_hub_policy = xts.dv_hashkey_hub_policy
                    AND stg.dv_applied_timestamp < xts.dv_applied_timestamp)
    QUALIFY dv_rnk = 1
)
-- Part 1: INSERT the new record
-- Conditions: hashdiff not already in previous_xts, and applied date not already in satellite
SELECT DISTINCT
    stg.dv_hashkey_hub_policy, stg.dv_tenant_id, stg.dv_recordsource,
    stg.dv_hashdiff AS dv_hashdiff, stg.dv_applied_timestamp, stg.dv_load_timestamp,
    'insert' AS dv_xts_event,
    stg.coverage_type, stg.premium_amount,
    stg.coverage_start_date, stg.coverage_end_date
FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE stg
WHERE EXISTS (
    SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE dlt
    WHERE NOT EXISTS (SELECT 1 FROM previous_xts xts
                      WHERE xts.dv_hashkey_hub_policy = dlt.dv_hashkey_hub_policy
                        AND xts.dv_hashdiff = dlt.dv_hashdiff)
      AND stg.dv_hashkey_hub_policy = dlt.dv_hashkey_hub_policy
)
AND NOT EXISTS (
    SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE dlt
    INNER JOIN <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE sat
        ON dlt.dv_hashkey_hub_policy = sat.dv_hashkey_hub_policy
       AND dlt.dv_applied_timestamp = sat.dv_applied_timestamp
)

UNION ALL

-- Part 2: COPY timeline correction
-- When previous_xts.dv_hashdiff = next_xts.dv_hashdiff, the state before and
-- after are the same — re-insert that state at the next_xts applied date with
-- the current load timestamp to restore correct timeline ordering.
SELECT DISTINCT
    sat.dv_hashkey_hub_policy, stg.dv_tenant_id, stg.dv_recordsource,
    sat.dv_hashdiff, next_xts.dv_applied_timestamp AS dv_applied_timestamp, stg.dv_load_timestamp,
    'copy' AS dv_xts_event,
    sat.coverage_type, sat.premium_amount,
    sat.coverage_start_date, sat.coverage_end_date
FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE stg
INNER JOIN <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE sat
    ON stg.dv_hashkey_hub_policy = sat.dv_hashkey_hub_policy
INNER JOIN next_xts
    ON stg.dv_hashkey_hub_policy = next_xts.dv_hashkey_hub_policy
INNER JOIN previous_xts
    ON stg.dv_hashkey_hub_policy = previous_xts.dv_hashkey_hub_policy
   AND previous_xts.dv_hashdiff = next_xts.dv_hashdiff
WHERE EXISTS (
    SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE dlt
    WHERE NOT EXISTS (SELECT 1 FROM previous_xts xts
                      WHERE xts.dv_hashkey_hub_policy = dlt.dv_hashkey_hub_policy
                        AND xts.dv_hashdiff = dlt.dv_hashdiff)
      AND stg.dv_hashkey_hub_policy = dlt.dv_hashkey_hub_policy
)
AND NOT EXISTS (
    SELECT 1 FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE dlt
    INNER JOIN <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE sat
        ON dlt.dv_hashkey_hub_policy = sat.dv_hashkey_hub_policy
       AND dlt.dv_applied_timestamp = sat.dv_applied_timestamp
);

-- ELSE (normal load when $xts_out_of_sequence_event = FALSE):
-- Use standard satellite INSERT WHERE NOT EXISTS (hashdiff comparison)
-- INSERT INTO ... SELECT ... 'insert' AS dv_xts_event ...
-- WHERE NOT EXISTS (SELECT 1 FROM satellite WHERE hashkey = ... AND hashdiff = ...)

-- ============================================================================
-- KEY POINTS:
-- 1. XTS tracks the timeline of ALL records seen per entity (regardless of order)
-- 2. The SWITCH detects late arrivals by comparing MAX(staged) vs MAX(satellite)
-- 3. dv_xts_event = 'insert' (new record) or 'copy' (timeline correction)
-- 4. 'copy' re-asserts the existing state at a later timeline position
-- 5. XTS is per-hub (covers all satellites off that hub)
-- 6. INCOMPATIBLE with Kappa Vault (needs batch boundary for SWITCH evaluation)
-- 7. previous_xts = what came BEFORE; next_xts = what comes AFTER
-- 8. COPY fires when previous_xts.hashdiff = next_xts.hashdiff (same state flanks the gap)
-- ============================================================================

-- ============================================================================
-- TEST FRAMEWORK: Reconciliation after XTS-assisted load
-- Validates the satellite load produced correct results (run inline with load)
-- ============================================================================

-- Satellite reconciliation: staged keys should now exist in target
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
     SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns)
WITH SAT_SKEY_SGTG AS (
    SELECT COUNT(*) AS SAT_SKEY_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_policy') AS SAT_SKEY_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_policy') AS SAT_SKEY_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE sg
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_POLICY_INS_COVERAGE s
                      WHERE sg.dv_hashkey_hub_policy = s.dv_hashkey_hub_policy)
),
SAT_HDIF_SGTG AS (
    SELECT COUNT(*) AS SAT_HDIF_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_policy', 'dv_hashdiff') AS SAT_HDIF_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_policy', 'dv_hashdiff') AS SAT_HDIF_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.STG_INS_POLICY_COVERAGE sg
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.VC_SAT_RV_HUB_POLICY_INS_COVERAGE s
                      WHERE sg.dv_hashkey_hub_policy = s.dv_hashkey_hub_policy
                        AND sg.dv_hashdiff = s.dv_hashdiff)
)
SELECT 'SAT_RV_HUB_POLICY_INS_COVERAGE', 'STG_INS_POLICY_COVERAGE',
       CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
       SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns
FROM SAT_SKEY_SGTG, SAT_HDIF_SGTG;

-- XTS sequence violation monitoring query
SELECT dv_rectarget, COUNT(*) AS violation_count
FROM <DATABASE>.VAULT.SAT_XT_HUB_POLICY
WHERE dv_sequence_violation = TRUE
GROUP BY 1 ORDER BY 2 DESC;
