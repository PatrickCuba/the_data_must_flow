-- DVOS Example: All Satellite Variants
-- Demonstrates every satellite type in the Pragmatic DV framework.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- Variants covered:
--   1. Standard satellite
--   2. PII satellite (naming suffix, same structure)
--   3. Multi-active satellite (MSAT)
--   4. Dependent-child satellite
--   5. Partitioned multi-active satellite (PMAS)
--   6. Effectivity satellite (link-only)
--   7. Non-historized satellite
--   8. Hybrid satellite (ODV — Snowflake Hybrid Tables)

-- ============================================================================
-- PREREQUISITE: Hub and Link (minimal, for FK context)
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

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.LNK_RV_CUSTOMER_PRODUCT (
    dv_hashkey_lnk_rv_customer_product  BINARY(20)  NOT NULL,
    dv_hashkey_hub_customer          BINARY(20)  NOT NULL,
    dv_hashkey_hub_product           BINARY(20)  NOT NULL,
    dv_tenant_id                     VARCHAR(50),
    dv_applied_timestamp             TIMESTAMP_NTZ NOT NULL,
    dv_recordsource                  VARCHAR(255) NOT NULL,
    dv_load_timestamp                TIMESTAMP_NTZ NOT NULL,
    dv_task_id                       VARCHAR(255),
    dv_jira_id                       VARCHAR(255),
    dv_user_id                       VARCHAR(255),
    last_seen_date                   TIMESTAMP_NTZ,
    CONSTRAINT pk_lnk_rv_customer_product PRIMARY KEY (dv_hashkey_lnk_rv_customer_product) NOT ENFORCED
);

-- ============================================================================
-- 1. STANDARD SATELLITE — one active row per BK, history tracked via hashdiff
-- Use when: attributes describe an entity and change over time
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_DEMO (
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
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_hub_crm_customer_demo
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Load pattern: INSERT WHERE NOT EXISTS (hashdiff comparison)
INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_DEMO (
    dv_hashkey_hub_customer, dv_tenant_id, dv_recordsource,
    dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
    industry, segment, annual_revenue
)
SELECT
    src.dv_hashkey_hub_customer, src.dv_tenant_id, src.dv_recordsource,
    src.dv_hashdiff, src.dv_applied_timestamp, src.dv_load_timestamp,
    src.industry, src.segment, src.annual_revenue
FROM <DATABASE>.STAGED.STG_CRM_CUSTOMER src
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_DEMO s
    WHERE s.dv_hashkey_hub_customer = src.dv_hashkey_hub_customer
      AND s.dv_hashdiff = src.dv_hashdiff
);

-- ============================================================================
-- 2. PII SATELLITE — same structure, naming suffix segregates sensitive data
-- Use when: GDPR/privacy requires separate access control on identifying columns
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_PII (
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
    date_of_birth            DATE,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_hub_crm_customer_pii
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Same INSERT WHERE NOT EXISTS load pattern as standard satellite.
-- Access controlled separately: GRANT SELECT to PII-authorised roles only.

-- ============================================================================
-- 3. MULTI-ACTIVE SATELLITE (MSAT) — multiple rows active per BK simultaneously
-- Use when: the SET of records for a parent key is tracked as a whole
-- Example: a customer's set of phone numbers, addresses, or contact methods
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_MA_RV_HUB_CRM_CUSTOMER_CONTACTS (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sequence              NUMBER           NOT NULL,  -- synthetic ordinal within the SET
    contact_type             VARCHAR,
    contact_value            VARCHAR,
    is_primary               VARCHAR(1),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_hub_customer_crm_contacts
        PRIMARY KEY (dv_hashkey_hub_customer, dv_sequence, dv_load_timestamp) NOT ENFORCED
);

-- MSAT load: entire SET is compared. If ANY record in the SET changed (or count changed),
-- the full new SET is inserted with new dv_sequence values.
-- dv_sequence is NOT in dv_hashdiff (it's arbitrary, not a business attribute).

-- ============================================================================
-- 4. DEPENDENT-CHILD SATELLITE — child key creates sub-grain within parent
-- Use when: items change independently; individual item history needed
-- Example: order lines (order_line_number is the dep-child key)
-- DDL is IDENTICAL to MSAT — PK: (hashkey, dv_sequence, dv_load_timestamp)
-- The dep-child key is a regular NOT NULL column, NOT part of the PK.
-- What differs from MSAT is the LOAD LOGIC: change detection per (hashkey, dep-child-key) row.
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_DP_RV_LNK_ERP_ORDER_LINES (
    dv_hashkey_lnk_order     BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sequence              NUMBER           NOT NULL,  -- sub-sequence ordinal
    order_line_number        NUMBER           NOT NULL,  -- dep-child key (NOT in PK)
    product_code             VARCHAR,
    quantity                 NUMBER,
    unit_price               NUMBER(18,2),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_lnk_order_erp_lines
        PRIMARY KEY (dv_hashkey_lnk_order, dv_sequence, dv_load_timestamp) NOT ENFORCED
);

-- Load pattern: change detection per (hashkey + dep-child-key) ROW.
-- If the hashdiff for a specific (hashkey, order_line_number) pair changes,
-- insert only that new row with a new dv_sequence value.
-- Unlike MSAT: does NOT re-insert the full SET when one row changes.

-- ============================================================================
-- 5. PMAS — Partitioned Multi-Active Satellite
-- Use when: multiple independent subsets per dep-child key each need SET versioning
-- Example: chart of accounts sub-codes, each with multiple active detail rows
-- DDL is IDENTICAL to MSAT and DP — PK: (hashkey, dv_sequence, dv_load_timestamp)
-- Dep-child key is a regular NOT NULL column, NOT part of the PK.
-- What differs: load logic is SET comparison scoped to (hashkey, dep-child-key).
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_MA_RV_HUB_GL_ACCOUNT_SUBCODES (
    dv_hashkey_hub_account   BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sequence              NUMBER           NOT NULL,  -- sub-sequence ordinal
    gl_category_code         VARCHAR(20)      NOT NULL,  -- dep-child key (NOT in PK)
    sub_code                 VARCHAR(50),
    description              VARCHAR,
    effective_date           DATE,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_ma_rv_hub_gl_account_subcodes
        PRIMARY KEY (dv_hashkey_hub_account, dv_sequence, dv_load_timestamp) NOT ENFORCED
);

-- PMAS load: SET comparison scoped to (hashkey, dep-child-key).
-- Changes to one partition do NOT trigger re-versioning of other partitions.

-- ============================================================================
-- 6. EFFECTIVITY SATELLITE — link-only, tracks relationship lifecycle
-- Use when: relationship can start/end/restart (flip-flop)
-- No business attributes. dv_start_date/dv_end_date only.
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_EF_RV_LNK_CRM_CUSTOMER_PRODUCT (
    dv_hashkey_lnk_rv_customer_product  BINARY(20)  NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_start_date            TIMESTAMP_NTZ    NOT NULL,  -- start of active period
    dv_end_date              TIMESTAMP_NTZ    NOT NULL,  -- high-date (9999-12-31) when open
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_ef_rv_lnk_crm_customer_product
        PRIMARY KEY (dv_hashkey_lnk_rv_customer_product, dv_load_timestamp) NOT ENFORCED
);

-- INSERT-only. Never UPDATE dv_end_date — a new row with the end-date set is inserted
-- when the relationship ends. High-date = '9999-12-31 00:00:00' for active records.

-- ============================================================================
-- 7. NON-HISTORIZED SATELLITE — latest value only, no hashdiff needed
-- Use when: reference data that is overwritten (e.g. config, lookup codes)
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_NH_RV_HUB_MDM_CUSTOMER_STATUS (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    customer_status          VARCHAR(20),
    risk_category            VARCHAR(20),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_nh_rv_hub_mdm_customer_status
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Load pattern: INSERT only — same as all other satellites.
-- "Non-historized" means the CURRENT VIEW (VC_*) shows only the latest row
-- via QUALIFY ROW_NUMBER() = 1. The TABLE still accumulates all rows.
-- This is NOT a MERGE/UPDATE pattern — all satellites are insert-only.

-- ============================================================================
-- 8. HYBRID SATELLITE (ODV) — Snowflake Hybrid Table for sub-300ms OLTP access
-- Use when: application needs low-latency reads/writes directly to vault data
-- Requires Snowflake Hybrid Tables feature.
-- ============================================================================

CREATE OR REPLACE HYBRID TABLE <DATABASE>.VAULT.SAT_RV_HUB_APP_CUSTOMER_LIVE (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    loyalty_points           NUMBER,
    last_login_dttm          TIMESTAMP_NTZ,
    session_count            NUMBER,
    CONSTRAINT pk_sat_rv_hub_app_customer_live
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp),
    -- FK IS enforced on hybrid tables (even though hub PK is NOT ENFORCED)
    CONSTRAINT fk_sat_app_customer_live_hub
        FOREIGN KEY (dv_hashkey_hub_customer) REFERENCES <DATABASE>.VAULT.HUB_CUSTOMER(dv_hashkey_hub_customer)
);

-- Hybrid table properties:
--   - PK IS enforced (unlike regular Snowflake tables)
--   - FK IS enforced at DML time
--   - Supports row-level locking
--   - Accepts both ETL bulk INSERTs and live OLTP inserts from applications
--   - Dual storage (blob + block) — higher cost than regular tables
--   - Limits: 100GB default table size, 1000 TPS, 300ms latency

-- ============================================================================
-- GHOST RECORDS — one per satellite (idempotent insert)
-- ============================================================================

-- Ghost record for standard satellite (pattern applies to all variants)
INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_DEMO (
    dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
    industry, segment, annual_revenue
)
SELECT
    TO_BINARY(REPEAT(0, 20)),  -- all-zeros hash (20 bytes = 40 hex chars)
    NULL, 'GHOST', 'GHOST', 'GHOST',
    'GHOST',
    TO_BINARY(REPEAT(0, 20)),
    '1900-01-01'::TIMESTAMP_NTZ,
    '1900-01-01'::TIMESTAMP_NTZ,
    NULL, NULL, NULL
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_CRM_CUSTOMER_DEMO
    WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20))
);
