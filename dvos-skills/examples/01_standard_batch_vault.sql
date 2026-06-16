-- DVOS Example: Complete CRM Customer Vault
-- A runnable example covering: schemas, hubs, links, satellites, staging, loads, IM views.
-- Target: Snowflake. Replace <DATABASE> with your target database.

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;

CREATE TRANSIENT SCHEMA IF NOT EXISTS <DATABASE>.STAGED
    COMMENT = 'Landing and staging layer (TRANSIENT — no Fail-safe)';

CREATE SCHEMA IF NOT EXISTS <DATABASE>.VAULT
    COMMENT = 'Raw Vault — hubs, links, satellites';

CREATE SCHEMA IF NOT EXISTS <DATABASE>.INFORMATION_MARTS
    COMMENT = 'Query-ready views for BI tools';

-- ============================================================================
-- HUBS
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.HUB_CUSTOMER (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    customer_id              VARCHAR          NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    last_seen_date           TIMESTAMP_NTZ,
    CONSTRAINT pk_hub_customer PRIMARY KEY (dv_hashkey_hub_customer) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.HUB_ACCOUNT (
    dv_hashkey_hub_account   BINARY(20)       NOT NULL,
    account_id               VARCHAR          NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    last_seen_date           TIMESTAMP_NTZ,
    CONSTRAINT pk_hub_account PRIMARY KEY (dv_hashkey_hub_account) NOT ENFORCED
);

-- ============================================================================
-- LINKS
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.LNK_RV_CUSTOMER_ACCOUNT (
    dv_hashkey_lnk_rv_customer_account  BINARY(20)  NOT NULL,
    dv_hashkey_hub_customer          BINARY(20)  NOT NULL,
    dv_hashkey_hub_account           BINARY(20)  NOT NULL,
    dv_tenant_id                     VARCHAR(50),
    dv_applied_timestamp             TIMESTAMP_NTZ NOT NULL,
    dv_recordsource                  VARCHAR(255) NOT NULL,
    dv_load_timestamp                TIMESTAMP_NTZ NOT NULL,
    dv_task_id                       VARCHAR(255),
    dv_jira_id                       VARCHAR(255),
    dv_user_id                       VARCHAR(255),
    last_seen_date                   TIMESTAMP_NTZ,
    CONSTRAINT pk_lnk_rv_customer_account PRIMARY KEY (dv_hashkey_lnk_rv_customer_account) NOT ENFORCED
);

-- ============================================================================
-- SATELLITES
-- ============================================================================

-- Standard satellite: customer demographics from Salesforce
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    industry                 VARCHAR,
    segment                  VARCHAR,
    annual_revenue           NUMBER(18,2),
    employee_count           NUMBER,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_customer_sf PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- PII satellite: sensitive attributes segregated for access control
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS_PII (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    email                    VARCHAR,
    phone                    VARCHAR,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_customer_sf_pii PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Standard satellite: customer data from ERP
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    credit_limit             NUMBER(18,2),
    payment_terms            VARCHAR,
    account_status           VARCHAR,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_customer_erp PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- ============================================================================
-- GHOST RECORDS (one per satellite, idempotent)
-- ============================================================================

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0, 20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20)));

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS_PII (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0, 20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS_PII WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20)));

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0, 20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20)));

-- ============================================================================
-- SATELLITE VIEWS (VC_ current, VH_ history)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS AS
SELECT * FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS AS
SELECT * FROM <DATABASE>.VAULT.SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS_PII AS
SELECT * FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS_PII
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

-- ============================================================================
-- STAGING VIEW (Salesforce example)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.STAGED.STG_SF_CUSTOMER AS
SELECT
    -- Hash keys (multi-tenancy ENABLED — includes tenant_id)
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' ||
        'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(customer_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_customer,
    -- Note: use 'default' as placeholder; set bkcc_value/tenant_id_value per source in manifest

    -- Hashdiff (no UPPER, empty string for nulls)
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(industry AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(segment AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(annual_revenue AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(employee_count AS STRING)), '')
    )) AS dv_hashdiff_sat_rv_hub_sf_customer_demographics,

    -- Metadata
    'default' AS dv_tenant_id,
    'default' AS dv_collisioncode,
    CURRENT_TIMESTAMP() AS dv_load_timestamp,
    batch_timestamp AS dv_applied_timestamp,
    'SALESFORCE.CRM.ACCOUNTS' AS dv_recordsource,

    -- Business keys and attributes (passthrough)
    customer_id,
    industry,
    segment,
    annual_revenue,
    employee_count
FROM <DATABASE>.STAGED.LANDING_SF_CUSTOMER;

-- ============================================================================
-- LOAD: Hub (MERGE with last_seen_date)
-- ============================================================================

MERGE INTO <DATABASE>.VAULT.HUB_CUSTOMER AS tgt
USING <DATABASE>.STAGED.STG_SF_CUSTOMER AS src
ON tgt.dv_hashkey_hub_customer = src.dv_hashkey_hub_customer
WHEN NOT MATCHED THEN INSERT (
    dv_hashkey_hub_customer, customer_id, dv_tenant_id, dv_collisioncode,
    dv_applied_timestamp, dv_recordsource, dv_load_timestamp, last_seen_date
) VALUES (
    src.dv_hashkey_hub_customer, src.customer_id, src.dv_tenant_id, src.dv_collisioncode,
    src.dv_applied_timestamp, src.dv_recordsource, src.dv_load_timestamp, src.dv_applied_timestamp
)
WHEN MATCHED THEN UPDATE SET
    tgt.last_seen_date = src.dv_applied_timestamp;

-- ============================================================================
-- LOAD: Satellite (INSERT WHERE NOT EXISTS)
-- ============================================================================

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS (
    dv_hashkey_hub_customer, dv_tenant_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
    industry, segment, annual_revenue, employee_count
)
SELECT
    src.dv_hashkey_hub_customer, src.dv_tenant_id,
    src.dv_recordsource, src.dv_hashdiff_sat_rv_hub_sf_customer_demographics, src.dv_applied_timestamp, src.dv_load_timestamp,
    src.industry, src.segment, src.annual_revenue, src.employee_count
FROM <DATABASE>.STAGED.STG_SF_CUSTOMER src
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS s
    WHERE s.dv_hashkey_hub_customer = src.dv_hashkey_hub_customer
      AND s.dv_hashdiff = src.dv_hashdiff_sat_rv_hub_sf_customer_demographics
);

-- ============================================================================
-- INFORMATION MART: DIM_CUSTOMER (current state, no hash keys exposed)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.INFORMATION_MARTS.DIM_CUSTOMER AS
SELECT
    h.customer_id,
    sf.industry,
    sf.segment,
    sf.annual_revenue,
    sf.employee_count,
    erp.credit_limit,
    erp.payment_terms,
    erp.account_status
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS sf
    ON sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_ERP_CUSTOMER_FINANCIALS erp
    ON erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer;

-- ============================================================================
-- TEST FRAMEWORK: Mode 3 — Reconciliation Tables (stateful, per-load results)
-- ============================================================================

-- Utilities schema for test infrastructure
CREATE SCHEMA IF NOT EXISTS <DATABASE>.UTILITIES;

-- Recon tables (subset — hub duplicate + satellite reconciliation)
CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_HUB_DUPLICATE_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    HUB_SKEY_DUPE_err       INT      NOT NULL,
    HUB_SKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    HUB_BKEY_DUPE_err       INT      NOT NULL,
    HUB_BKEY_DUPE_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_hub_dupe PRIMARY KEY (tablename, source_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    source_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    SAT_SKEY_SGTG_err      INT      NOT NULL,
    SAT_SKEY_SGTG_src_columns ARRAY  NOT NULL,
    SAT_SKEY_SGTG_tgt_columns ARRAY  NOT NULL,
    SAT_HDIF_SGTG_err      INT      NOT NULL,
    SAT_HDIF_SGTG_src_columns ARRAY  NOT NULL,
    SAT_HDIF_SGTG_tgt_columns ARRAY  NOT NULL,
    CONSTRAINT pk_recon_sat_recon PRIMARY KEY (tablename, source_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

CREATE TRANSIENT TABLE IF NOT EXISTS <DATABASE>.UTILITIES.RECONCILE_SAT_REFERENTIAL_ERRORS (
    tablename           VARCHAR(200) NOT NULL,
    parent_tablename    VARCHAR(200) NOT NULL,
    loaddate            DATETIME     NOT NULL,
    rundate             DATETIME     NOT NULL,
    SAT_SKEY_ORPH_err       INT      NOT NULL,
    CONSTRAINT pk_recon_sat_orph PRIMARY KEY (tablename, parent_tablename, loaddate, rundate) ENFORCED
) CLUSTER BY (loaddate, tablename);

-- Recon streams (append-only, one per target table)
CREATE STREAM IF NOT EXISTS <DATABASE>.UTILITIES.STR_ORPHANCHECK_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS
    ON TABLE <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS APPEND_ONLY = TRUE;

CREATE STREAM IF NOT EXISTS <DATABASE>.UTILITIES.STR_RECONCILE_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS
    ON TABLE <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS APPEND_ONLY = TRUE;

-- Test: Hub duplicate check (run after hub MERGE)
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_HUB_DUPLICATE_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     HUB_SKEY_DUPE_err, HUB_SKEY_DUPE_tgt_columns,
     HUB_BKEY_DUPE_err, HUB_BKEY_DUPE_tgt_columns)
WITH HUB_SKEY_DUPE AS (
    SELECT COUNT(e) AS HUB_SKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer') AS HUB_SKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.HUB_CUSTOMER
          GROUP BY dv_hashkey_hub_customer HAVING COUNT(*) > 1) sq
),
HUB_BKEY_DUPE AS (
    SELECT COUNT(e) AS HUB_BKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_tenant_id', 'dv_collisioncode', 'customer_id') AS HUB_BKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.HUB_CUSTOMER
          GROUP BY dv_tenant_id, dv_collisioncode, customer_id HAVING COUNT(*) > 1) sq
)
SELECT 'HUB_CUSTOMER', 'STG_SF_CUSTOMER', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       HUB_SKEY_DUPE_err, HUB_SKEY_DUPE_tgt_columns,
       HUB_BKEY_DUPE_err, HUB_BKEY_DUPE_tgt_columns
FROM HUB_SKEY_DUPE, HUB_BKEY_DUPE;

-- Test: Satellite reconciliation (staged keys present in target)
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_RECONCILIATION_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
     SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns)
WITH SAT_SKEY_SGTG AS (
    SELECT COUNT(*) AS SAT_SKEY_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer') AS SAT_SKEY_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer') AS SAT_SKEY_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.STG_SF_CUSTOMER sg
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS s
                      WHERE sg.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer)
),
SAT_HDIF_SGTG AS (
    SELECT COUNT(*) AS SAT_HDIF_SGTG_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer', 'dv_hashdiff_sat_rv_hub_sf_customer_demographics') AS SAT_HDIF_SGTG_src_columns,
           ARRAY_CONSTRUCT('dv_hashkey_hub_customer', 'dv_hashdiff') AS SAT_HDIF_SGTG_tgt_columns
    FROM <DATABASE>.STAGED.STG_SF_CUSTOMER sg
    WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.VC_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS s
                      WHERE sg.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer
                        AND sg.dv_hashdiff_sat_rv_hub_sf_customer_demographics = s.dv_hashdiff)
)
SELECT 'SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS', 'STG_SF_CUSTOMER', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       SAT_SKEY_SGTG_err, SAT_SKEY_SGTG_src_columns, SAT_SKEY_SGTG_tgt_columns,
       SAT_HDIF_SGTG_err, SAT_HDIF_SGTG_src_columns, SAT_HDIF_SGTG_tgt_columns
FROM SAT_SKEY_SGTG, SAT_HDIF_SGTG;

-- Test: Satellite orphan check (FK integrity)
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_SAT_REFERENTIAL_ERRORS
    (tablename, parent_tablename, loaddate, rundate, SAT_SKEY_ORPH_err)
SELECT 'SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS', 'HUB_CUSTOMER', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       COUNT(*)
FROM <DATABASE>.UTILITIES.STR_ORPHANCHECK_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS s
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.HUB_CUSTOMER p
                  WHERE s.dv_hashkey_hub_customer = p.dv_hashkey_hub_customer)
  AND s.dv_recordsource <> 'GHOST';

-- ============================================================================
-- TEST FRAMEWORK: Mode 2 — DMF Continuous Monitoring (async, dashboard-friendly)
-- ============================================================================

-- DQ schema for DMFs
CREATE SCHEMA IF NOT EXISTS <DATABASE>.DQ;

-- Example DMF: Hub surrogate key uniqueness
CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.DQ.DV_DMF_HUB_SKEY_DUPE_ERR(
    ARG_T TABLE(dv_hashkey BINARY(20))
)
RETURNS NUMBER AS
'SELECT COUNT(e) FROM (SELECT COUNT(*) e FROM ARG_T GROUP BY dv_hashkey HAVING COUNT(*) > 1)';

-- Example DMF: Satellite orphan detection
CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.DQ.DV_DMF_SAT_SKEY_ORPH_ERR(
    ARG_T TABLE(dv_hashkey BINARY(20), dv_recordsource VARCHAR(255)),
    ARG_P TABLE(dv_hashkey BINARY(20))
)
RETURNS NUMBER AS
'SELECT COUNT(*) FROM ARG_T t WHERE t.dv_recordsource <> ''GHOST'' AND NOT EXISTS (SELECT 1 FROM ARG_P p WHERE t.dv_hashkey = p.dv_hashkey)';

-- Attach DMFs to vault tables
ALTER TABLE <DATABASE>.VAULT.HUB_CUSTOMER
    ADD DATA METRIC FUNCTION <DATABASE>.DQ.DV_DMF_HUB_SKEY_DUPE_ERR
    ON (dv_hashkey_hub_customer)
    EXPECTATION hub_customer_skey_unique (VALUE = 0);

ALTER TABLE <DATABASE>.VAULT.SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

-- Query DMF results
SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
WHERE TABLE_NAME IN ('HUB_CUSTOMER', 'SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS')
ORDER BY MEASUREMENT_TIME DESC;
