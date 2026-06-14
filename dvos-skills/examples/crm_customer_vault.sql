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

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.LNK_CUSTOMER_ACCOUNT (
    dv_hashkey_lnk_customer_account  BINARY(20)  NOT NULL,
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
    CONSTRAINT pk_lnk_customer_account PRIMARY KEY (dv_hashkey_lnk_customer_account) NOT ENFORCED
);

-- ============================================================================
-- SATELLITES
-- ============================================================================

-- Standard satellite: customer demographics from Salesforce
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_CUSTOMER_SF (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
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
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_CUSTOMER_SF_PII (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
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
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_CUSTOMER_ERP (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
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

INSERT INTO <DATABASE>.VAULT.SAT_CUSTOMER_SF (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0,20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0,20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_CUSTOMER_SF WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0,20)));

INSERT INTO <DATABASE>.VAULT.SAT_CUSTOMER_SF_PII (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0,20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0,20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_CUSTOMER_SF_PII WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0,20)));

INSERT INTO <DATABASE>.VAULT.SAT_CUSTOMER_ERP (dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id, dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp)
SELECT TO_BINARY(REPEAT(0,20)), NULL, 'GHOST', 'GHOST', 'GHOST', 'GHOST', TO_BINARY(REPEAT(0,20)), '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_CUSTOMER_ERP WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0,20)));

-- ============================================================================
-- SATELLITE VIEWS (VC_ current, VH_ history)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_CUSTOMER_SF AS
SELECT * FROM <DATABASE>.VAULT.SAT_CUSTOMER_SF
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_CUSTOMER_ERP AS
SELECT * FROM <DATABASE>.VAULT.SAT_CUSTOMER_ERP
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

CREATE OR REPLACE VIEW <DATABASE>.VAULT.VC_SAT_CUSTOMER_SF_PII AS
SELECT * FROM <DATABASE>.VAULT.SAT_CUSTOMER_SF_PII
QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) = 1;

-- ============================================================================
-- STAGING VIEW (Salesforce example)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.STAGED.STG_SF_CUSTOMER AS
SELECT
    -- Hash keys
    SHA1_BINARY(UPPER(CONCAT(
        COALESCE(dv_tenant_id, '0') || '||' ||
        COALESCE(dv_collisioncode, '0') || '||' ||
        COALESCE(NULLIF(TRIM(CAST(customer_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_customer,

    -- Hashdiff (no UPPER, empty string for nulls)
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(industry AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(segment AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(annual_revenue AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(employee_count AS STRING)), '')
    )) AS dv_hashdiff_sat_customer_sf,

    -- Metadata
    '0' AS dv_tenant_id,
    '0' AS dv_collisioncode,
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

INSERT INTO <DATABASE>.VAULT.SAT_CUSTOMER_SF (
    dv_hashkey_hub_customer, dv_tenant_id, dv_collisioncode,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
    industry, segment, annual_revenue, employee_count
)
SELECT
    src.dv_hashkey_hub_customer, src.dv_tenant_id, src.dv_collisioncode,
    src.dv_recordsource, src.dv_hashdiff_sat_customer_sf, src.dv_applied_timestamp, src.dv_load_timestamp,
    src.industry, src.segment, src.annual_revenue, src.employee_count
FROM <DATABASE>.STAGED.STG_SF_CUSTOMER src
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_CUSTOMER_SF s
    WHERE s.dv_hashkey_hub_customer = src.dv_hashkey_hub_customer
      AND s.dv_hashdiff = src.dv_hashdiff_sat_customer_sf
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
LEFT JOIN <DATABASE>.VAULT.VC_SAT_CUSTOMER_SF sf
    ON sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_CUSTOMER_ERP erp
    ON erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer;
