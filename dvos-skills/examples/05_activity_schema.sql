-- DVOS Example: Activity Schema (Business Vault Pattern)
-- Implements Activity Schema 2.0 as a BV non-historized satellite with
-- stream-triggered loading and per-activity IM Dynamic Tables.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- Scenario: Customer activities from multiple source satellites are unified
-- into a single non-historized BV satellite for fast analytical access.
-- Each activity type gets its own IM Dynamic Table for consumption.

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.VAULT;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.BV_STAGING;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.INFORMATION_MARTS;

-- ============================================================================
-- BV STAGING VIEW — the business rule that unifies activities
-- Pulls from multiple raw vault satellites and normalises into activity format
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_ACTIVITY AS
SELECT
    h.dv_hashkey_hub_customer,
    h.customer_id,
    txn.dv_applied_timestamp    AS activity_ts,
    'TRANSACTION'               AS activity,
    txn.transaction_type        AS feature_1,
    txn.amount::STRING          AS feature_2,
    txn.channel                 AS feature_3,
    txn.dv_recordsource         AS activity_source
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
JOIN <DATABASE>.VAULT.SAT_RV_LNK_CUST_TXN_ERP_DETAIL txn
    ON txn.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY txn.dv_hashkey_hub_customer, txn.dv_applied_timestamp, txn.transaction_type
    ORDER BY txn.dv_load_timestamp DESC
) = 1

UNION ALL

SELECT
    h.dv_hashkey_hub_customer,
    h.customer_id,
    login.dv_applied_timestamp  AS activity_ts,
    'LOGIN'                     AS activity,
    login.device_type           AS feature_1,
    login.ip_address            AS feature_2,
    login.login_result          AS feature_3,
    login.dv_recordsource       AS activity_source
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
JOIN <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_APP_LOGINS login
    ON login.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY login.dv_hashkey_hub_customer, login.dv_applied_timestamp
    ORDER BY login.dv_load_timestamp DESC
) = 1;

-- ============================================================================
-- BV NON-HISTORIZED SATELLITE — Activity Schema target table
-- INSERT only (like all satellites). Current state via VC_ view (ROW_NUMBER = 1).
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM (
    dv_tenant_id             VARCHAR(50)   NOT NULL,
    dv_collisioncode         VARCHAR(50),
    dv_hashkey_hub_customer  BINARY(20)    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ NOT NULL,
    dv_recordsource          VARCHAR(255)  NOT NULL,
    dv_task_id               VARCHAR(255)  NOT NULL,
    dv_jira_id               VARCHAR(255)  NOT NULL,
    dv_user_id               VARCHAR(255),
    dv_sid                   NUMBER        IDENTITY START 0 INCREMENT 1 ORDER,
    customer_id              VARCHAR       NOT NULL,
    -- Activity Schema columns
    activity_id              VARCHAR(50)   NOT NULL,
    activity                 VARCHAR(50)   NOT NULL,
    anonymous_customer_id    VARCHAR(50)   NULL,
    feature_json             VARIANT       NOT NULL,
    revenue_impact           NUMBER(18,2)  NULL,
    link                     VARCHAR(255)  NULL,
    CONSTRAINT pk_sat_bv_nh_customer_stream
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Ghost record
INSERT INTO <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM (
    dv_tenant_id, dv_hashkey_hub_customer, dv_load_timestamp, dv_applied_timestamp,
    dv_recordsource, dv_task_id, dv_jira_id, customer_id,
    activity_id, activity, feature_json, revenue_impact, link
)
SELECT 'default', TO_BINARY(REPEAT(0, 20)),
       '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ,
       'GHOST', 'GHOST', 'GHOST', 'GHOST',
       'GHOST', 'GHOST', PARSE_JSON('{}'), 0, 'GHOST'
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM
    WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20))
);

-- ============================================================================
-- STREAM + TASK — event-driven loading of the activity satellite
-- ============================================================================

CREATE OR REPLACE STREAM <DATABASE>.BV_STAGING.STR_BV_CUSTOMER_ACTIVITY_TO_SAT_BV_NH_CUSTOMER_STREAM
    ON VIEW <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_ACTIVITY
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE TASK <DATABASE>.BV_STAGING.TSK_BV_CUSTOMER_ACTIVITY_TO_SAT_BV_NH_CUSTOMER_STREAM
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = '5 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('<DATABASE>.BV_STAGING.STR_BV_CUSTOMER_ACTIVITY_TO_SAT_BV_NH_CUSTOMER_STREAM')
AS
INSERT INTO <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM (
    dv_tenant_id, dv_hashkey_hub_customer, dv_load_timestamp, dv_applied_timestamp,
    dv_recordsource, dv_task_id, dv_jira_id, customer_id,
    activity_id, activity, anonymous_customer_id, feature_json, revenue_impact, link
)
SELECT
    'default'                       AS dv_tenant_id,
    src.dv_hashkey_hub_customer,
    CURRENT_TIMESTAMP()             AS dv_load_timestamp,
    src.activity_ts                 AS dv_applied_timestamp,
    'BV_CUSTOMER_ACTIVITY'          AS dv_recordsource,
    'TSK_BV_CUSTOMER_ACTIVITY'      AS dv_task_id,
    'default'                       AS dv_jira_id,
    src.customer_id,
    src.activity_id,
    src.activity,
    ''                              AS anonymous_customer_id,
    src.feature_json,
    src.revenue_impact,
    ''                              AS link
FROM <DATABASE>.BV_STAGING.STR_BV_CUSTOMER_ACTIVITY_TO_SAT_BV_NH_CUSTOMER_STREAM src;

ALTER TASK <DATABASE>.BV_STAGING.TSK_BV_CUSTOMER_ACTIVITY_TO_SAT_BV_NH_CUSTOMER_STREAM RESUME;

-- ============================================================================
-- PER-ACTIVITY IM DYNAMIC TABLES — one DT per activity type
-- Consumers query only the activity they care about (filtered, pre-materialised)
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.INFORMATION_MARTS.DT_CUSTOMER_STREAM_TRANSACTION
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    customer_id,
    activity_ts,
    feature_1   AS transaction_type,
    feature_2   AS amount,
    feature_3   AS channel,
    activity_source
FROM <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM
WHERE activity = 'TRANSACTION';

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.INFORMATION_MARTS.DT_CUSTOMER_STREAM_LOGIN
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    customer_id,
    activity_ts,
    feature_1   AS device_type,
    feature_2   AS ip_address,
    feature_3   AS login_result,
    activity_source
FROM <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM
WHERE activity = 'LOGIN';

-- ============================================================================
-- RELATIONSHIP DT — enriched with dimension attributes via ASOF JOIN
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.INFORMATION_MARTS.DT_CUSTOMER_ACTIVITY_ENRICHED
    TARGET_LAG = '5 minutes'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    a.customer_id,
    a.activity_ts,
    a.activity,
    a.feature_1,
    a.feature_2,
    a.feature_3,
    dim.segment,
    dim.industry
FROM <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM a
ASOF JOIN <DATABASE>.INFORMATION_MARTS.DIM_CUSTOMER dim
    MATCH_CONDITION (a.activity_ts >= dim.effective_from)
    ON a.customer_id = dim.customer_id;

-- ============================================================================
-- KEY POINTS:
-- 1. Activity Schema is a BV pattern — it unifies activities from multiple sources
-- 2. Non-historized satellite (INSERT only) — current state via VC_ view (ROW_NUMBER = 1)
-- 3. Stream on the BV staging view triggers the load task
-- 4. Per-activity DTs give consumers pre-filtered, pre-materialised access
-- 5. ASOF JOIN enriches activities with point-in-time dimension attributes
-- 6. feature_json (VARIANT) is polymorphic — meaning depends on activity type
-- 7. ALL satellites are INSERT only — no MERGE, no UPDATE, no exceptions
-- ============================================================================

-- ============================================================================
-- TEST FRAMEWORK: BV satellite reconciliation (Kappa mode — same transaction)
-- ============================================================================

-- Reconcile BV satellite load — verifies activities reached the target
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
     SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns)
WITH SAT_SKEY_SGTG AS (
    SELECT COUNT(*) AS SAT_SKEY_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer') AS SAT_SKEY_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer') AS SAT_SKEY_SGTG_tgt_columns
    FROM <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_ACTIVITY sg
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_NH_CUSTOMER_STREAM s
                      WHERE sg.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer)
),
SAT_HDIF_SGTG AS (
    SELECT 0 AS SAT_HDIF_SGTG_err,  -- NH satellite has no hashdiff comparison
           ARRAY_CONSTRUCT('N/A') AS SAT_HDIF_SGTG_src_columns,
           ARRAY_CONSTRUCT('N/A') AS SAT_HDIF_SGTG_tgt_columns
)
SELECT 'SAT_BV_NH_CUSTOMER_STREAM', 'BV_CUSTOMER_ACTIVITY',
       CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
       SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns
FROM SAT_SKEY_SGTG, SAT_HDIF_SGTG;

