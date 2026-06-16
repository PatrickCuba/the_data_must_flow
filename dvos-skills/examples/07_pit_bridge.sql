-- DVOS Example: PIT Table + Bridge Table
-- Query-assistance structures that pre-resolve join locators at build time.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- PIT: solves temporal alignment (many satellites per hub)
-- Bridge: solves multi-hop traversal (many links between hubs)
-- Both are DISPOSABLE — rebuild anytime from vault data.
-- Both are driven by an ASOF calendar table (data-driven, not hardcoded).
--
-- Variants shown:
--   A. Legacy PIT (hashkey + applied_ts per satellite — no DDL change required)
--   B. SNOPIT (dv_sid integer per satellite — faster, requires dv_sid IDENTITY)

-- ============================================================================
-- PREREQUISITE: Hub with multiple satellites (from 01_standard_batch_vault)
-- ============================================================================

-- Assume HUB_CUSTOMER with SAT_RV_HUB_CUSTOMER_SF, SAT_RV_HUB_CUSTOMER_ERP,
-- SAT_RV_HUB_CUSTOMER_SF_PII exist with data and ghost records loaded.
-- All satellites have dv_sid IDENTITY columns (already in our standard DDL).

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS <DATABASE>.QUERYASSISTANCE
    COMMENT = 'PIT, Bridge, ASOF calendar — query assistance structures';

-- ============================================================================
-- PART A: ASOF CALENDAR TABLE — data-driven PIT controller
-- The date spine that drives snapshot cadence. PIT DTs reference this table.
-- Changing reporting scope is a data operation, not a code deployment.
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.QUERYASSISTANCE.AS_OF_DATE (
    as_of           DATE      NOT NULL,
    year            SMALLINT  NOT NULL,
    month           SMALLINT  NOT NULL,
    day_of_month    SMALLINT  NOT NULL,
    week_of_year    SMALLINT  NOT NULL,
    day_of_year     SMALLINT  NOT NULL,
    month_lastday   SMALLINT  NOT NULL,  -- 1 on last day of month
    week_lastday    SMALLINT  NOT NULL,  -- 1 on last day of week (Sunday)
    week_firstday   SMALLINT  NOT NULL,  -- 1 on first day of week (Monday)
    CONSTRAINT pk_as_of_date PRIMARY KEY (as_of) ENFORCED
);

-- Populate: rolling 2 years of dates
INSERT INTO <DATABASE>.QUERYASSISTANCE.AS_OF_DATE
SELECT
    DATEADD(DAY, SEQ4(), DATEADD(YEAR, -2, CURRENT_DATE()))::DATE AS as_of,
    YEAR(as_of)                                                     AS year,
    MONTH(as_of)                                                    AS month,
    DAY(as_of)                                                      AS day_of_month,
    WEEKOFYEAR(as_of)                                               AS week_of_year,
    DAYOFYEAR(as_of)                                                AS day_of_year,
    CASE WHEN as_of = LAST_DAY(as_of) THEN 1 ELSE 0 END            AS month_lastday,
    CASE WHEN DAYOFWEEK(as_of) = 0 THEN 1 ELSE 0 END               AS week_lastday,
    CASE WHEN DAYOFWEEK(as_of) = 1 THEN 1 ELSE 0 END               AS week_firstday
FROM TABLE(GENERATOR(ROWCOUNT => 730))
WHERE as_of <= CURRENT_DATE();

-- ============================================================================
-- PART B: LEGACY PIT — hashkey + applied_timestamp locators per satellite
-- No satellite DDL change needed. Works with existing tables.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.QUERYASSISTANCE.PIT_CUSTOMER_DAILY
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORM_WH
AS
WITH date_spine AS (
    SELECT as_of AS snapshot_date
    FROM <DATABASE>.QUERYASSISTANCE.AS_OF_DATE
),
sat_sf_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_applied_timestamp
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
),
sat_erp_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_applied_timestamp
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
),
sat_pii_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_applied_timestamp
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF_PII s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
)
SELECT
    h.dv_hashkey_hub_customer,
    d.snapshot_date,
    -- SAT_RV_HUB_CUSTOMER_SF locators
    COALESCE(s_sf.dv_hashkey_hub_customer, TO_BINARY(REPEAT(0, 20)))
        AS sat_sf_dv_hashkey_hub_customer,
    COALESCE(s_sf.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))
        AS sat_sf_dv_applied_timestamp,
    -- SAT_RV_HUB_CUSTOMER_ERP locators
    COALESCE(s_erp.dv_hashkey_hub_customer, TO_BINARY(REPEAT(0, 20)))
        AS sat_erp_dv_hashkey_hub_customer,
    COALESCE(s_erp.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))
        AS sat_erp_dv_applied_timestamp,
    -- SAT_RV_HUB_CUSTOMER_SF_PII locators
    COALESCE(s_pii.dv_hashkey_hub_customer, TO_BINARY(REPEAT(0, 20)))
        AS sat_pii_dv_hashkey_hub_customer,
    COALESCE(s_pii.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))
        AS sat_pii_dv_applied_timestamp
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
CROSS JOIN date_spine d
LEFT JOIN sat_sf_latest s_sf
    ON s_sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_sf.snapshot_date = d.snapshot_date
LEFT JOIN sat_erp_latest s_erp
    ON s_erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_erp.snapshot_date = d.snapshot_date
LEFT JOIN sat_pii_latest s_pii
    ON s_pii.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_pii.snapshot_date = d.snapshot_date;

-- ============================================================================
-- PART C: SNOPIT — dv_sid integer locators per satellite (RECOMMENDED)
-- One column per satellite. Faster integer equi-joins. Compact.
-- Requires dv_sid IDENTITY on all satellites (already in standard DVOS DDL).
-- Ghost record dv_sid = 0 (autoincrement START 0 assigns 0 to ghost row).
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.QUERYASSISTANCE.SNOPIT_CUSTOMER_DAILY
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORM_WH
AS
WITH date_spine AS (
    SELECT as_of AS snapshot_date
    FROM <DATABASE>.QUERYASSISTANCE.AS_OF_DATE
),
sat_sf_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_sid
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
),
sat_erp_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_sid
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
),
sat_pii_latest AS (
    SELECT
        s.dv_hashkey_hub_customer,
        d.snapshot_date,
        s.dv_sid
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF_PII s
    JOIN date_spine d ON s.dv_applied_timestamp <= d.snapshot_date
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY s.dv_hashkey_hub_customer, d.snapshot_date
        ORDER BY s.dv_applied_timestamp DESC, s.dv_load_timestamp DESC
    ) = 1
)
SELECT
    h.dv_hashkey_hub_customer,
    d.snapshot_date,
    -- One dv_sid column per satellite (ghost = 0)
    COALESCE(s_sf.dv_sid, 0)  AS sat_sf_dv_sid,
    COALESCE(s_erp.dv_sid, 0) AS sat_erp_dv_sid,
    COALESCE(s_pii.dv_sid, 0) AS sat_pii_dv_sid
FROM <DATABASE>.VAULT.HUB_CUSTOMER h
CROSS JOIN date_spine d
LEFT JOIN sat_sf_latest s_sf
    ON s_sf.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_sf.snapshot_date = d.snapshot_date
LEFT JOIN sat_erp_latest s_erp
    ON s_erp.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_erp.snapshot_date = d.snapshot_date
LEFT JOIN sat_pii_latest s_pii
    ON s_pii.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer
   AND s_pii.snapshot_date = d.snapshot_date;

-- ============================================================================
-- PART D: IM VIEW — consuming the Legacy PIT (hashkey + applied_ts equi-join)
-- No window functions at query time. Filter by snapshot_date for point-in-time.
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.INFORMATION_MARTS.V_CUSTOMER_HISTORY_PIT AS
SELECT
    h.customer_id,
    pit.snapshot_date,
    -- CRM satellite attributes
    sf.industry,
    sf.segment,
    sf.annual_revenue,
    sf.employee_count,
    -- ERP satellite attributes
    erp.credit_limit,
    erp.payment_terms,
    erp.account_status
FROM <DATABASE>.QUERYASSISTANCE.PIT_CUSTOMER_DAILY pit
JOIN <DATABASE>.VAULT.HUB_CUSTOMER h
    ON h.dv_hashkey_hub_customer = pit.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF sf
    ON sf.dv_hashkey_hub_customer = pit.sat_sf_dv_hashkey_hub_customer
   AND sf.dv_applied_timestamp = pit.sat_sf_dv_applied_timestamp
LEFT JOIN <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP erp
    ON erp.dv_hashkey_hub_customer = pit.sat_erp_dv_hashkey_hub_customer
   AND erp.dv_applied_timestamp = pit.sat_erp_dv_applied_timestamp;

-- Usage: point-in-time query — "what was this customer's state on 2024-06-15?"
-- SELECT * FROM V_CUSTOMER_HISTORY_PIT WHERE customer_id = '12345' AND snapshot_date = '2024-06-15';

-- ============================================================================
-- PART E: IM VIEW — consuming the SNOPIT (integer dv_sid equi-join — FASTEST)
-- dv_sid = 0 resolves to the ghost row (NULL attributes). No filter needed.
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.INFORMATION_MARTS.V_CUSTOMER_HISTORY_SNOPIT AS
SELECT
    h.customer_id,
    pit.snapshot_date,
    -- CRM satellite attributes (dv_sid join — integer equality)
    sf.industry,
    sf.segment,
    sf.annual_revenue,
    sf.employee_count,
    -- ERP satellite attributes
    erp.credit_limit,
    erp.payment_terms,
    erp.account_status
FROM <DATABASE>.QUERYASSISTANCE.SNOPIT_CUSTOMER_DAILY pit
JOIN <DATABASE>.VAULT.HUB_CUSTOMER h
    ON h.dv_hashkey_hub_customer = pit.dv_hashkey_hub_customer
LEFT JOIN <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF sf
    ON sf.dv_sid = pit.sat_sf_dv_sid
LEFT JOIN <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP erp
    ON erp.dv_sid = pit.sat_erp_dv_sid;

-- Why SNOPIT is faster:
-- 1. Integer equality join (dv_sid = pit.sat_x_dv_sid) uses hash-join algorithm
-- 2. Right-deep join tree: SNOPIT anchors as "fact", satellites probe as "dimensions"
-- 3. dv_sid is linear (autoincrement) → excellent zone map pruning via JoinFilter
-- 4. One column per satellite (vs. two for legacy) → half the PIT table width

-- ============================================================================
-- PART F: BRIDGE TABLE — pre-joins a multi-hop path between hubs
-- Scenario: Customer → Account → Product (two link hops)
-- Without bridge: IM query traverses HUB → LNK → HUB → LNK → HUB
-- With bridge: single equi-join gives all hash keys pre-resolved
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.QUERYASSISTANCE.BRDG_CUSTOMER_PRODUCT_DAILY
    TARGET_LAG = '1 day'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    h_cust.dv_hashkey_hub_customer,
    lnk_ca.dv_hashkey_lnk_rv_customer_account,
    h_acct.dv_hashkey_hub_account,
    lnk_ap.dv_hashkey_lnk_rv_account_product,
    h_prod.dv_hashkey_hub_product,
    CURRENT_DATE() AS snapshot_date
FROM <DATABASE>.VAULT.HUB_CUSTOMER h_cust
-- Hop 1: Customer → Account
JOIN <DATABASE>.VAULT.LNK_RV_CUSTOMER_ACCOUNT lnk_ca
    ON lnk_ca.dv_hashkey_hub_customer = h_cust.dv_hashkey_hub_customer
JOIN <DATABASE>.VAULT.HUB_ACCOUNT h_acct
    ON h_acct.dv_hashkey_hub_account = lnk_ca.dv_hashkey_hub_account
-- Hop 2: Account → Product
JOIN <DATABASE>.VAULT.LNK_RV_ACCOUNT_PRODUCT lnk_ap
    ON lnk_ap.dv_hashkey_hub_account = h_acct.dv_hashkey_hub_account
JOIN <DATABASE>.VAULT.HUB_PRODUCT h_prod
    ON h_prod.dv_hashkey_hub_product = lnk_ap.dv_hashkey_hub_product;

-- ============================================================================
-- PART G: IM VIEW — consuming the Bridge (single join replaces multi-hop)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.INFORMATION_MARTS.FACT_CUSTOMER_PRODUCT AS
SELECT
    h_cust.customer_id,
    h_prod.product_id,
    sat_prod.product_name,
    sat_prod.product_category,
    bdg.snapshot_date
FROM <DATABASE>.QUERYASSISTANCE.BRDG_CUSTOMER_PRODUCT_DAILY bdg
JOIN <DATABASE>.VAULT.HUB_CUSTOMER h_cust
    ON h_cust.dv_hashkey_hub_customer = bdg.dv_hashkey_hub_customer
JOIN <DATABASE>.VAULT.HUB_PRODUCT h_prod
    ON h_prod.dv_hashkey_hub_product = bdg.dv_hashkey_hub_product
LEFT JOIN <DATABASE>.VAULT.VC_SAT_RV_HUB_PRODUCT_DETAIL sat_prod
    ON sat_prod.dv_hashkey_hub_product = bdg.dv_hashkey_hub_product;

-- ============================================================================
-- KEY POINTS:
-- 1. PIT = pre-computed JOIN-INDEX driven by an ASOF calendar. NOT a dimension.
-- 2. PIT stores LOCATORS only (hashkey+ts or dv_sid) — no business attributes.
-- 3. ASOF calendar drives snapshot cadence — changing scope is a data operation.
-- 4. SNOPIT (integer dv_sid) is RECOMMENDED — faster, thinner, better zone maps.
-- 5. Legacy PIT (hashkey+ts) needs no satellite DDL change — safer initial choice.
-- 6. Ghost records (dv_sid=0, hash=all-zeros) anchor PIT COALESCE fallbacks.
-- 7. Bridge pre-joins multi-hop traversals — single equi-join replaces N hops.
-- 8. Both PIT and Bridge are DISPOSABLE — drop and rebuild without data loss.
-- 9. No CLUSTER BY on PIT, Bridge, or satellite tables — natural order is optimal.
-- 10. Escalation: stem-leaf → PIT → Bridge → PIT+Bridge → Supernova
-- ============================================================================
