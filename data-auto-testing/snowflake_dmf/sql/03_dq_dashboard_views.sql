/* =============================================================================
   DATA VAULT 2.0 QUALITY FRAMEWORK — Part 3: DQ Monitoring Dashboard View

   Creates V_DQ_DV_MONITOR_STATUS in <% database %>.DQ — a unified view that
   surfaces DMF results across all monitored DV tables, showing PASS/FAIL
   status for each check, analogous to querying the RECON error tables but
   using the native Snowflake monitoring functions.
============================================================================= */

USE ROLE IDENTIFIER('<% role %>');
USE WAREHOUSE IDENTIFIER('<% warehouse %>');
USE SCHEMA IDENTIFIER('<% database %>.DQ');

/* ─────────────────────────────────────────────────────────────────────────── */
/* V_DQ_DV_MONITOR_STATUS                                                      */
/* Shows the latest DMF result per metric per DV table, with PASS/FAIL.        */
/* ─────────────────────────────────────────────────────────────────────────── */

CREATE OR REPLACE VIEW <% database %>.DQ.V_DQ_DV_MONITOR_STATUS
COMMENT = 'Latest DMF result per metric per monitored DV table. PASS = VALUE is 0, FAIL = violations detected.'
AS
WITH monitored_tables AS (
    SELECT '<% edw_database %>' AS db, 'SAL'     AS schema, 'HUB_ACCOUNT'                                       AS tbl, 'HUB'  AS dv_type
    UNION ALL SELECT '<% edw_database %>', 'SAL',     'HUB_PARTY',                                              'HUB'
    UNION ALL SELECT '<% edw_database %>', 'SAL',     'LNK_RV_CUSTOMER_ACCOUNT_PRODUCT',                        'LNK'
    UNION ALL SELECT '<% edw_database %>', 'SAL',     'SAT_RV_HUB_SAPBW_COMM_CUSTOMER',                        'SAT'
    UNION ALL SELECT '<% edw_database %>', 'SAL_EXT', 'SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST',            'SAT'
),
hub_account_results AS (
    SELECT
        r.MEASUREMENT_TIME,
        '<% edw_database %>' AS db, 'SAL' AS schema, 'HUB_ACCOUNT' AS tbl, 'HUB' AS dv_type,
        r.METRIC_NAME,
        r.METRIC_SCHEMA,
        r.VALUE::NUMBER AS metric_value,
        r.ARGUMENT_NAMES[0]::VARCHAR AS primary_column
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME  => '<% edw_database %>.SAL.HUB_ACCOUNT',
        REF_ENTITY_DOMAIN => 'TABLE'
    )) r
),
hub_party_results AS (
    SELECT
        r.MEASUREMENT_TIME,
        '<% edw_database %>', 'SAL', 'HUB_PARTY', 'HUB',
        r.METRIC_NAME, r.METRIC_SCHEMA, r.VALUE::NUMBER, r.ARGUMENT_NAMES[0]::VARCHAR
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME  => '<% edw_database %>.SAL.HUB_PARTY',
        REF_ENTITY_DOMAIN => 'TABLE'
    )) r
),
lnk_cap_results AS (
    SELECT
        r.MEASUREMENT_TIME,
        '<% edw_database %>', 'SAL', 'LNK_RV_CUSTOMER_ACCOUNT_PRODUCT', 'LNK',
        r.METRIC_NAME, r.METRIC_SCHEMA, r.VALUE::NUMBER, r.ARGUMENT_NAMES[0]::VARCHAR
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME  => '<% edw_database %>.SAL.LNK_RV_CUSTOMER_ACCOUNT_PRODUCT',
        REF_ENTITY_DOMAIN => 'TABLE'
    )) r
),
sat_comm_cust_results AS (
    SELECT
        r.MEASUREMENT_TIME,
        '<% edw_database %>', 'SAL', 'SAT_RV_HUB_SAPBW_COMM_CUSTOMER', 'SAT',
        r.METRIC_NAME, r.METRIC_SCHEMA, r.VALUE::NUMBER, r.ARGUMENT_NAMES[0]::VARCHAR
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME  => '<% edw_database %>.SAL.SAT_RV_HUB_SAPBW_COMM_CUSTOMER',
        REF_ENTITY_DOMAIN => 'TABLE'
    )) r
),
sat_ext_results AS (
    SELECT
        r.MEASUREMENT_TIME,
        '<% edw_database %>', 'SAL_EXT', 'SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST', 'SAT',
        r.METRIC_NAME, r.METRIC_SCHEMA, r.VALUE::NUMBER, r.ARGUMENT_NAMES[0]::VARCHAR
    FROM TABLE(SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS(
        REF_ENTITY_NAME  => '<% edw_database %>.SAL_EXT.SAT_ST_RV_HUB_ACCOUNT_SAPBW_CUST_ACCT_MAST',
        REF_ENTITY_DOMAIN => 'TABLE'
    )) r
),
all_results AS (
    SELECT * FROM hub_account_results
    UNION ALL SELECT * FROM hub_party_results
    UNION ALL SELECT * FROM lnk_cap_results
    UNION ALL SELECT * FROM sat_comm_cust_results
    UNION ALL SELECT * FROM sat_ext_results
),
latest_per_metric AS (
    SELECT *
    FROM all_results
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY db, schema, tbl, METRIC_NAME, primary_column
        ORDER BY MEASUREMENT_TIME DESC
    ) = 1
)
SELECT
    db                                                         AS table_database,
    schema                                                     AS table_schema,
    tbl                                                        AS table_name,
    dv_type                                                    AS dv_layer,
    METRIC_SCHEMA || '.' || METRIC_NAME                       AS check_name,
    CASE
        WHEN METRIC_NAME = 'DUPLICATE_COUNT'               THEN 'SKEY Duplicate Count'
        WHEN METRIC_NAME = 'NULL_COUNT'                    THEN 'SKEY Null Count'
        WHEN METRIC_NAME = 'DMF_DV_BKEY_DUPE_COUNT'       THEN 'BKey + Tenant Duplicate Count'
        WHEN METRIC_NAME = 'DMF_DV_SAT_COMPOSITE_DUPE_COUNT' THEN 'SAT Composite Key Duplicate Count'
        WHEN METRIC_NAME = 'DMF_DV_LNK_HKEY_DUPE_COUNT'  THEN 'LNK Hub-Key Combo Duplicate Count'
        WHEN METRIC_NAME = 'DMF_DV_LNK_HKEY3_DUPE_COUNT' THEN 'LNK 3-Key Hub Combo Duplicate Count'
        WHEN METRIC_NAME = 'DMF_DV_ORPHAN_COUNT'          THEN 'Referential Integrity Orphan Count'
        ELSE METRIC_NAME
    END                                                        AS check_description,
    primary_column                                             AS monitored_column,
    metric_value                                               AS error_count,
    CASE WHEN metric_value = 0 THEN 'PASS' ELSE 'FAIL' END   AS status,
    MEASUREMENT_TIME                                           AS last_checked
FROM latest_per_metric
ORDER BY
    CASE WHEN metric_value > 0 THEN 0 ELSE 1 END,
    table_database, table_schema, table_name, check_name;


/* ─────────────────────────────────────────────────────────────────────────── */
/* V_DQ_DV_SCHEMA_HEALTH                                                       */
/* Aggregates PASS/FAIL counts into a single schema health score.              */
/* ─────────────────────────────────────────────────────────────────────────── */

CREATE OR REPLACE VIEW <% database %>.DQ.V_DQ_DV_SCHEMA_HEALTH
COMMENT = 'Data Vault quality health score: % of checks passing across all monitored tables.'
AS
SELECT
    COUNT(*)                                                AS total_checks,
    COUNT_IF(status = 'PASS')                              AS passing_checks,
    COUNT_IF(status = 'FAIL')                              AS failing_checks,
    ROUND(COUNT_IF(status = 'PASS') * 100.0 / NULLIF(COUNT(*), 0), 1) AS health_pct,
    CASE
        WHEN COUNT(*) = 0 THEN 'NO_DATA'
        WHEN ROUND(COUNT_IF(status = 'PASS') * 100.0 / COUNT(*), 1) = 100  THEN 'HEALTHY'
        WHEN ROUND(COUNT_IF(status = 'PASS') * 100.0 / COUNT(*), 1) >= 90  THEN 'GOOD'
        WHEN ROUND(COUNT_IF(status = 'PASS') * 100.0 / COUNT(*), 1) >= 75  THEN 'DEGRADED'
        ELSE 'CRITICAL'
    END                                                     AS health_status,
    MAX(last_checked)                                       AS last_measured
FROM <% database %>.DQ.V_DQ_DV_MONITOR_STATUS;


/* ─────────────────────────────────────────────────────────────────────────── */
/* V_DQ_DV_EXPECTATION_VIOLATIONS                                              */
/* Shows current expectation violations (DMF results that breach thresholds).  */
/* Equivalent to querying the RECON error tables for non-zero error counts.    */
/* ─────────────────────────────────────────────────────────────────────────── */

CREATE OR REPLACE VIEW <% database %>.DQ.V_DQ_DV_EXPECTATION_VIOLATIONS
COMMENT = 'DMF expectation violations — equivalent of non-zero rows in RECON error tables.'
AS
SELECT
    ref_database_name   AS table_database,
    ref_schema_name     AS table_schema,
    ref_entity_name     AS table_name,
    metric_name         AS check_name,
    expectation_name,
    expectation_expression,
    measurement_time,
    value               AS metric_value
FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
WHERE violated = TRUE
ORDER BY measurement_time DESC;


/* ─────────────────────────────────────────────────────────────────────────── */
/* Verify views created                                                         */
/* ─────────────────────────────────────────────────────────────────────────── */

SHOW VIEWS IN SCHEMA <% database %>.DQ;
