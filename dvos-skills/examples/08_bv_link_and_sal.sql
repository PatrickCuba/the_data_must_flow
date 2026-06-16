-- DVOS Example: Business Vault Link + Same-As Link (SAL)
-- BV Link: derived relationship not in any source system.
-- SAL: entity resolution — two hub keys refer to the same real-world entity.
-- Target: Snowflake. Replace <DATABASE> with your target database.

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.VAULT;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.BV_STAGING;

-- ============================================================================
-- PART 1: BV LINK — Derived relationship (account lineage via recursive CTE)
-- Scenario: source tracks card-level movements, but business needs account-level
-- lineage. The BV link transforms the source UoW into the business UoW.
-- ============================================================================

-- Business rule view: derive account lineage from card replacement chain
-- Outputs BUSINESS KEYS ONLY — no hashkeys (DVOS adds those in staging)
CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.BV_ACCOUNT_LINEAGE AS
WITH RECURSIVE lineage AS (
    -- Base: original card (no predecessor)
    SELECT
        dv_hashkey_hub_account,
        account_id AS original_account_id,
        account_id AS current_account_id,
        0 AS depth
    FROM <DATABASE>.VAULT.HUB_ACCOUNT
    WHERE dv_hashkey_hub_account NOT IN (
        SELECT dv_hashkey_hub_account_successor
        FROM <DATABASE>.VAULT.LNK_RV_CARD_REPLACEMENT
    )

    UNION ALL

    -- Recursive: follow replacement chain
    SELECT
        lnk.dv_hashkey_hub_account_successor,
        lin.original_account_id,
        h.account_id,
        lin.depth + 1
    FROM lineage lin
    JOIN <DATABASE>.VAULT.LNK_RV_CARD_REPLACEMENT lnk
        ON lnk.dv_hashkey_hub_account_predecessor = lin.dv_hashkey_hub_account
    JOIN <DATABASE>.VAULT.HUB_ACCOUNT h
        ON h.dv_hashkey_hub_account = lnk.dv_hashkey_hub_account_successor
    WHERE lin.depth < 20  -- safety limit
)
SELECT
    original_account_id,
    current_account_id,
    depth AS lineage_depth,
    -- DV-BV-111: NEVER use CURRENT_TIMESTAMP() — derive from contributing RV sources.
    -- Use GREATEST of the source link's applied_timestamp across the chain.
    (SELECT MAX(dv_applied_timestamp)
     FROM <DATABASE>.VAULT.LNK_RV_CARD_REPLACEMENT) AS dv_applied_timestamp
FROM lineage
WHERE depth > 0;  -- exclude self-references

-- BV Staging View: adds hashkeys (DVOS generates this from the BV rule view)
CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.STG_BV_ACCOUNT_LINEAGE AS
SELECT
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(original_account_id AS STRING)), ''), '-1') || '||' ||
        COALESCE(NULLIF(TRIM(CAST(current_account_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_lnk_account_lineage,
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' || 'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(original_account_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_account_original,
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' || 'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(current_account_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_account_current,
    'default'                 AS dv_tenant_id,
    'BV_ACCOUNT_LINEAGE'      AS dv_recordsource,
    CURRENT_TIMESTAMP()       AS dv_load_timestamp,
    dv_applied_timestamp,
    original_account_id,
    current_account_id,
    lineage_depth
FROM <DATABASE>.BV_STAGING.BV_ACCOUNT_LINEAGE;

-- BV Link table (same structure as raw vault link, vault_layer = bv)
-- INSERT-only default: no last_seen_date column. Add it only if MERGE is declared.
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE (
    dv_hashkey_lnk_account_lineage   BINARY(20)  NOT NULL,
    dv_hashkey_hub_account_original  BINARY(20)  NOT NULL,
    dv_hashkey_hub_account_current   BINARY(20)  NOT NULL,
    dv_tenant_id                     VARCHAR(50),
    dv_applied_timestamp             TIMESTAMP_NTZ NOT NULL,
    dv_recordsource                  VARCHAR(255) NOT NULL,
    dv_load_timestamp                TIMESTAMP_NTZ NOT NULL,
    dv_task_id                       VARCHAR(255),
    dv_jira_id                       VARCHAR(255),
    dv_user_id                       VARCHAR(255),
    CONSTRAINT pk_lnk_bv_account_lineage
        PRIMARY KEY (dv_hashkey_lnk_account_lineage) NOT ENFORCED
);

-- Load BV link (INSERT-only — default pattern, no last_seen_date update)
-- If last_seen_date tracking is needed, declare it in manifest and use MERGE instead.
INSERT INTO <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE (
    dv_hashkey_lnk_account_lineage, dv_hashkey_hub_account_original,
    dv_hashkey_hub_account_current, dv_tenant_id,
    dv_applied_timestamp, dv_recordsource, dv_load_timestamp
)
SELECT DISTINCT
    dv_hashkey_lnk_account_lineage,
    dv_hashkey_hub_account_original,
    dv_hashkey_hub_account_current,
    dv_tenant_id,
    dv_applied_timestamp,
    dv_recordsource,
    dv_load_timestamp
FROM <DATABASE>.BV_STAGING.STG_BV_ACCOUNT_LINEAGE stg
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE tgt
    WHERE stg.dv_hashkey_lnk_account_lineage = tgt.dv_hashkey_lnk_account_lineage
);

-- ============================================================================
-- PART 2: SAME-AS LINK (SAL) — Entity Resolution
-- Scenario: CRM and ERP both have customer records. MDM asserts matches.
-- The SAL captures "these two hub keys refer to the same real-world customer."
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.LNK_RV_SA_CUSTOMER_MATCH (
    dv_hashkey_lnk_rv_sa_customer_match      BINARY(20)    NOT NULL,
    dv_hashkey_hub_customer_a    BINARY(20)    NOT NULL,  -- e.g. CRM record
    dv_hashkey_hub_customer_b    BINARY(20)    NOT NULL,  -- e.g. ERP record
    dv_tenant_id                 VARCHAR(50),
    dv_applied_timestamp         TIMESTAMP_NTZ NOT NULL,
    dv_recordsource              VARCHAR(255)  NOT NULL,
    dv_load_timestamp            TIMESTAMP_NTZ NOT NULL,
    dv_task_id                   VARCHAR(255),
    dv_jira_id                   VARCHAR(255),
    dv_user_id                   VARCHAR(255),
    CONSTRAINT pk_lnk_rv_sa_customer_match PRIMARY KEY (dv_hashkey_lnk_rv_sa_customer_match) NOT ENFORCED
);

-- Effectivity satellite (REQUIRED — SAL has no lifecycle without it)
-- Note: EFF satellites have NO dv_hashdiff (they track start/end dates only)
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_EF_RV_LNK_MDM_CUSTOMER_MATCH (
    dv_hashkey_lnk_rv_sa_customer_match  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_start_date            TIMESTAMP_NTZ    NOT NULL,
    dv_end_date              TIMESTAMP_NTZ    NOT NULL,  -- '9999-12-31' when active
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_ef_rv_lnk_mdm_customer_match
        PRIMARY KEY (dv_hashkey_lnk_rv_sa_customer_match, dv_load_timestamp) NOT ENFORCED
);

-- Optional: match metadata satellite (confidence, algorithm used)
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_LNK_MDM_CUSTOMER_MATCH_META (
    dv_hashkey_lnk_rv_sa_customer_match  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    confidence_score         NUMBER(5,4),      -- e.g. 0.9500
    match_algorithm          VARCHAR(100),     -- e.g. 'FUZZY_NAME_ADDRESS_V2'
    match_reason             VARCHAR,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_lnk_mdm_customer_match_meta
        PRIMARY KEY (dv_hashkey_lnk_rv_sa_customer_match, dv_load_timestamp) NOT ENFORCED
);

-- ============================================================================
-- SAL in the IM — survivorship logic (which record "wins")
-- The SAL asserts identity; the IM decides which source's attributes to show
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.INFORMATION_MARTS.DIM_CUSTOMER_RESOLVED AS
WITH active_matches AS (
    SELECT
        sal.dv_hashkey_hub_customer_a,
        sal.dv_hashkey_hub_customer_b,
        eff.dv_start_date,
        eff.dv_end_date
    FROM <DATABASE>.VAULT.LNK_RV_SA_CUSTOMER_MATCH sal
    JOIN <DATABASE>.VAULT.SAT_EF_RV_LNK_MDM_CUSTOMER_MATCH eff
        ON eff.dv_hashkey_lnk_rv_sa_customer_match = sal.dv_hashkey_lnk_rv_sa_customer_match
    WHERE eff.dv_end_date = '9999-12-31'::TIMESTAMP_NTZ  -- currently active assertions
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY sal.dv_hashkey_lnk_rv_sa_customer_match
        ORDER BY eff.dv_load_timestamp DESC
    ) = 1
)
SELECT
    -- Survivorship: prefer CRM (record A) for name/contact, ERP (record B) for financial
    h.customer_id,
    crm.industry,
    crm.segment,
    erp.credit_limit,
    erp.payment_terms
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
LEFT JOIN active_matches am
    ON am.dv_hashkey_hub_customer_a = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_CRM crm
    ON crm.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_ERP erp
    ON erp.dv_hashkey_hub_customer = COALESCE(am.dv_hashkey_hub_customer_b, h.dv_hashkey_hub_customer);

-- ============================================================================
-- PART 3: BV SATELLITE — Two Delivery Modes
-- The same rules apply to BV links and BV satellites:
--   dv_applied_timestamp = GREATEST of contributing RV source timestamps
--   This ensures PIT/SNOPIT snapshots are aligned between RV and BV layers.
-- ============================================================================

-- --------------------------------------------------------------------------
-- MODE A: LANDED — BV rule output treated like a landed file
-- The business rule is a SQL view that reads from RV satellites and outputs:
--   - business keys (for hashkey computation in stg_bv_*)
--   - derived attributes (the BV business rule output)
--   - dv_applied_timestamp = GREATEST(contributing RV applied_timestamps)
-- Then DVOS staging adds hashkeys/hashdiff, and the loader does a standard INSERT.
-- --------------------------------------------------------------------------

-- Step 1: BV Rule View (you write this — the business logic)
CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.BV_CUSTOMER_CREDIT_SCORE AS
SELECT
    h.customer_id,
    sf.annual_revenue,
    erp.credit_limit,
    -- Derived attribute: the business rule output
    CASE
        WHEN erp.credit_limit > 100000 AND sf.annual_revenue > 5000000 THEN 'AAA'
        WHEN erp.credit_limit > 50000  AND sf.annual_revenue > 1000000 THEN 'AA'
        WHEN erp.credit_limit > 10000  THEN 'A'
        ELSE 'B'
    END AS credit_score,
    -- DV-BV-111: dv_applied_timestamp = GREATEST of source satellite timestamps
    -- This ensures BV snapshots align with RV snapshots in PIT/SNOPIT
    GREATEST(sf.dv_applied_timestamp, erp.dv_applied_timestamp) AS dv_applied_timestamp
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_SF sf
    ON sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_ERP erp
    ON erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
WHERE sf.dv_hashkey_hub_customer IS NOT NULL;  -- only score customers with data

-- Step 2: BV Staging View (DVOS generates this — adds hashkeys + hashdiff)
CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_CREDIT_SCORE AS
SELECT
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' || 'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(customer_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_customer,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(credit_score AS STRING)), '')
    )) AS dv_hashdiff_sat_bv_customer_credit_score,
    'default'                     AS dv_tenant_id,
    'BV_RULE.CREDIT_SCORE'        AS dv_recordsource,
    CURRENT_TIMESTAMP()           AS dv_load_timestamp,
    dv_applied_timestamp,  -- passed through from the BV rule view (GREATEST)
    customer_id,
    credit_score
FROM <DATABASE>.BV_STAGING.BV_CUSTOMER_CREDIT_SCORE;

-- Step 3: BV Satellite DDL
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_BV_CUSTOMER_CREDIT_SCORE (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    credit_score             VARCHAR(10),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_bv_customer_credit_score
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Ghost record
INSERT INTO <DATABASE>.VAULT.SAT_BV_CUSTOMER_CREDIT_SCORE (
    dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp
)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST',
       'GHOST', TO_BINARY(REPEAT(0, 20)),
       '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_CUSTOMER_CREDIT_SCORE
                  WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20)));

-- Step 4: BV Satellite Load (standard INSERT WHERE NOT EXISTS — same as RV)
INSERT INTO <DATABASE>.VAULT.SAT_BV_CUSTOMER_CREDIT_SCORE (
    dv_hashkey_hub_customer, dv_tenant_id, dv_recordsource,
    dv_hashdiff, dv_applied_timestamp, dv_load_timestamp, credit_score
)
SELECT
    dv_hashkey_hub_customer, dv_tenant_id, dv_recordsource,
    dv_hashdiff_sat_bv_customer_credit_score, dv_applied_timestamp, dv_load_timestamp,
    credit_score
FROM <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_CREDIT_SCORE stg
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_CUSTOMER_CREDIT_SCORE s
    WHERE s.dv_hashkey_hub_customer = stg.dv_hashkey_hub_customer
      AND s.dv_hashdiff = stg.dv_hashdiff_sat_bv_customer_credit_score
);

-- --------------------------------------------------------------------------
-- MODE B: VIEW (virtual) — transformation logic lives directly in stg_bv_*
-- No separate BV rule view. The staging view IS the business rule + DV metadata.
-- Lighter weight: no landing table, no execution unit for the rule view.
-- The stg_bv_* view must bring through the correct dv_applied_timestamp from RV.
-- --------------------------------------------------------------------------

-- Single view that combines business rule + staging metadata
CREATE OR REPLACE VIEW <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_RISK_TIER AS
SELECT
    -- Hashkey
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' || 'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(h.customer_id AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_customer,
    -- Hashdiff (on the derived attribute)
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(
            CASE WHEN erp.credit_limit < 5000 THEN 'HIGH_RISK'
                 WHEN erp.credit_limit < 25000 THEN 'MEDIUM_RISK'
                 ELSE 'LOW_RISK' END
        AS STRING)), '')
    )) AS dv_hashdiff_sat_bv_customer_risk_tier,
    -- Metadata
    'default'                     AS dv_tenant_id,
    'BV_RULE.RISK_TIER'           AS dv_recordsource,
    CURRENT_TIMESTAMP()           AS dv_load_timestamp,
    -- dv_applied_timestamp: GREATEST from contributing RV satellites
    GREATEST(sf.dv_applied_timestamp, erp.dv_applied_timestamp) AS dv_applied_timestamp,
    -- Derived attribute (the business rule output)
    h.customer_id,
    CASE WHEN erp.credit_limit < 5000 THEN 'HIGH_RISK'
         WHEN erp.credit_limit < 25000 THEN 'MEDIUM_RISK'
         ELSE 'LOW_RISK' END AS risk_tier
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_SF sf
    ON sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_CUSTOMER_ERP erp
    ON erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
WHERE erp.dv_hashkey_hub_customer IS NOT NULL;

-- BV Satellite DDL (same structure regardless of delivery mode)
CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_BV_CUSTOMER_RISK_TIER (
    dv_hashkey_hub_customer  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    risk_tier                VARCHAR(20),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_bv_customer_risk_tier
        PRIMARY KEY (dv_hashkey_hub_customer, dv_load_timestamp) NOT ENFORCED
);

-- Ghost record
INSERT INTO <DATABASE>.VAULT.SAT_BV_CUSTOMER_RISK_TIER (
    dv_hashkey_hub_customer, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp
)
SELECT TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST',
       'GHOST', TO_BINARY(REPEAT(0, 20)),
       '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_CUSTOMER_RISK_TIER
                  WHERE dv_hashkey_hub_customer = TO_BINARY(REPEAT(0, 20)));

-- Load directly from the virtual staging view (same INSERT pattern)
INSERT INTO <DATABASE>.VAULT.SAT_BV_CUSTOMER_RISK_TIER (
    dv_hashkey_hub_customer, dv_tenant_id, dv_recordsource,
    dv_hashdiff, dv_applied_timestamp, dv_load_timestamp, risk_tier
)
SELECT
    dv_hashkey_hub_customer, dv_tenant_id, dv_recordsource,
    dv_hashdiff_sat_bv_customer_risk_tier, dv_applied_timestamp, dv_load_timestamp,
    risk_tier
FROM <DATABASE>.BV_STAGING.STG_BV_CUSTOMER_RISK_TIER stg
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_BV_CUSTOMER_RISK_TIER s
    WHERE s.dv_hashkey_hub_customer = stg.dv_hashkey_hub_customer
      AND s.dv_hashdiff = stg.dv_hashdiff_sat_bv_customer_risk_tier
);

-- ============================================================================
-- KEY POINTS:
-- 1. BV Link: derived relationship not in source — computed by business rule
-- 2. Recursive CTE (Snowflake-native) finds the first record in a lineage chain
-- 3. First-record-in-lineage = stable anchor (never changes) — use as surrogate
-- 4. SAL: asserts two hub keys = same entity (raw vault, not BV)
-- 5. SAL MUST have effectivity satellite (tracks when assertion is active)
-- 6. Match metadata (confidence, algorithm) in SEPARATE satellite, not in EFF
-- 7. Survivorship logic lives in IM — SAL only asserts identity, never merges
-- 8. BV Satellite Mode A (landed): separate BV rule view → stg_bv_* staging → INSERT
-- 9. BV Satellite Mode B (view): stg_bv_* IS the business rule + staging combined
-- 10. CRITICAL: dv_applied_timestamp = GREATEST(contributing RV applied_timestamps)
--     This ensures PIT/SNOPIT snapshots align between RV and BV layers.
--     NEVER use CURRENT_TIMESTAMP() for BV applied_timestamp. (DV-BV-111)
-- 11. BV rule view outputs BUSINESS KEYS ONLY — hashkeys are added in stg_bv_*
-- 12. BV links default to INSERT-only (anti-semi-join). MERGE+last_seen_date is
--     available only if explicitly declared in manifest (applies to SAL and HY too).
-- ============================================================================

-- ============================================================================
-- TEST FRAMEWORK: Link reconciliation + orphan check
-- ============================================================================

-- Link duplicate check
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_LNK_DUPLICATE_ERRORS
    (tablename, source_tablename, loaddate, rundate,
     LNK_SKEY_DUPE_err, LNK_SKEY_DUPE_tgt_columns,
     LNK_HKEY_DUPE_err, LNK_HKEY_DUPE_tgt_columns)
WITH LNK_SKEY_DUPE AS (
    SELECT COUNT(e) AS LNK_SKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_hashkey_lnk_account_lineage') AS LNK_SKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE
          GROUP BY dv_hashkey_lnk_account_lineage HAVING COUNT(*) > 1) sq
),
LNK_HKEY_DUPE AS (
    SELECT COUNT(e) AS LNK_HKEY_DUPE_err,
           ARRAY_CONSTRUCT('dv_hashkey_hub_account_original', 'dv_hashkey_hub_account_current') AS LNK_HKEY_DUPE_tgt_columns
    FROM (SELECT COUNT(*) e FROM <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE
          GROUP BY dv_hashkey_hub_account_original, dv_hashkey_hub_account_current HAVING COUNT(*) > 1) sq
)
SELECT 'LNK_BV_ACCOUNT_LINEAGE', 'BV_ACCOUNT_LINEAGE',
       CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       LNK_SKEY_DUPE_err, LNK_SKEY_DUPE_tgt_columns,
       LNK_HKEY_DUPE_err, LNK_HKEY_DUPE_tgt_columns
FROM LNK_SKEY_DUPE, LNK_HKEY_DUPE;

-- Link orphan check (FK integrity — each hub key must exist in parent hub)
INSERT INTO <DATABASE>.UTILITIES.RECONCILE_LNK_REFERENTIAL_ERRORS
    (tablename, parent_tablename, link_columnname, loaddate, rundate, LNK_SKEY_ORPH_err)
SELECT 'LNK_BV_ACCOUNT_LINEAGE', 'HUB_ACCOUNT',
       ARRAY_CONSTRUCT('dv_hashkey_hub_account_original'),
       CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
       COUNT(*)
FROM <DATABASE>.VAULT.LNK_BV_ACCOUNT_LINEAGE lnk
WHERE NOT EXISTS (SELECT 1 FROM <DATABASE>.VAULT.HUB_ACCOUNT h
                  WHERE lnk.dv_hashkey_hub_account_original = h.dv_hashkey_hub_account);
