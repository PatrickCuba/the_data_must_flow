-- DVOS Example: Supernova Pattern (5-Layer Pre-Materialised IM)
-- Replaces query-time vault joins with INCREMENTAL Dynamic Tables.
-- Use when: BI SLAs demand sub-second response; data changes frequently.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- Layers:
--   L1: Source (raw vault satellites — already exist)
--   L2: Preparation (VC_/VH_ current/history views — already exist)
--   L3: Foundation
--       a. Versions DT — union of all applied dates across all satellites for a hub
--       b. Supernova DT — equi-join satellites to versions timeline
--   L4: Extended Supernova (XSN) — computed/derived BV attributes
--   L5: Delivery — consumption-specific views/DTs

-- ============================================================================
-- PREREQUISITE: Raw Vault exists (hub + satellites from 01_standard_batch_vault)
-- ============================================================================

-- Assume HUB_CUSTOMER, SAT_RV_HUB_CUSTOMER_SF, SAT_RV_HUB_CUSTOMER_ERP
-- already exist with data loaded.

-- ============================================================================
-- SCHEMA
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS <DATABASE>.SUPERNOVA WITH MANAGED ACCESS;

-- ============================================================================
-- LAYER 3a: VERSIONS DT — unified timeline from all satellites off a hub
-- UNION ALL of every dv_applied_timestamp from every satellite, grouped.
-- Produces one row per (hashkey, startdate) with a computed enddate.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.SUPERNOVA.DT_HUB_CUSTOMER_VERSIONS
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = TRANSFORM_WH
AS
WITH twine AS (
    SELECT dv_tenant_id, dv_hashkey_hub_customer, dv_applied_timestamp AS startdate
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF
    WHERE dv_recordsource <> 'GHOST'
    UNION ALL
    SELECT dv_tenant_id, dv_hashkey_hub_customer, dv_applied_timestamp AS startdate
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP
    WHERE dv_recordsource <> 'GHOST'
),
group_by AS (
    SELECT dv_tenant_id, dv_hashkey_hub_customer, startdate
    FROM twine
    GROUP BY 1, 2, 3
)
SELECT
    hub.customer_id,
    grp.dv_tenant_id,
    grp.dv_hashkey_hub_customer,
    grp.startdate,
    COALESCE(
        DATEADD(SECONDS, -1, LEAD(startdate) OVER (
            PARTITION BY grp.dv_hashkey_hub_customer ORDER BY startdate
        )),
        TO_TIMESTAMP('9999-12-31 23:59:59')
    ) AS enddate
FROM group_by grp
INNER JOIN <DATABASE>.VAULT.HUB_CUSTOMER hub
    ON grp.dv_hashkey_hub_customer = hub.dv_hashkey_hub_customer;

-- ============================================================================
-- LAYER 3b: SUPERNOVA DT — equi-join each satellite to the versions timeline
-- CRITICAL: joins use AND s.dv_applied_timestamp = hub.startdate (equi-join)
-- This is REQUIRED for Snowflake INCREMENTAL refresh compatibility.
-- Range joins (>=, BETWEEN) force full rebuild — always use equi-join on startdate.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.SUPERNOVA.DT_SUPERNOVA_HUB_CUSTOMER
    TARGET_LAG = '1 min'
    WAREHOUSE  = TRANSFORM_WH
AS
WITH leaf_sat_customer_sf AS (
    SELECT s.*,
           COALESCE(
               LEAD(s.dv_applied_timestamp) OVER (
                   PARTITION BY s.dv_hashkey_hub_customer
                   ORDER BY s.dv_applied_timestamp
               ),
               TO_TIMESTAMP('9999-12-31 23:59:59')
           ) AS dv_applied_timestamp_end
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_SF s
),
leaf_sat_customer_erp AS (
    SELECT s.*,
           COALESCE(
               LEAD(s.dv_applied_timestamp) OVER (
                   PARTITION BY s.dv_hashkey_hub_customer
                   ORDER BY s.dv_applied_timestamp
               ),
               TO_TIMESTAMP('9999-12-31 23:59:59')
           ) AS dv_applied_timestamp_end
    FROM <DATABASE>.VAULT.SAT_RV_HUB_CUSTOMER_ERP s
)
SELECT
    hub.dv_tenant_id,
    hub.dv_hashkey_hub_customer,
    hub.customer_id,
    hub.startdate,
    hub.enddate,
    -- SAT_RV_HUB_CUSTOMER_SF attributes
    s1.industry,
    s1.segment,
    s1.annual_revenue,
    s1.employee_count,
    -- SAT_RV_HUB_CUSTOMER_ERP attributes
    s2.credit_limit,
    s2.payment_terms,
    s2.account_status
FROM <DATABASE>.SUPERNOVA.DT_HUB_CUSTOMER_VERSIONS hub
LEFT JOIN leaf_sat_customer_sf s1
    ON hub.dv_hashkey_hub_customer = s1.dv_hashkey_hub_customer
   AND s1.dv_applied_timestamp = hub.startdate
LEFT JOIN leaf_sat_customer_erp s2
    ON hub.dv_hashkey_hub_customer = s2.dv_hashkey_hub_customer
   AND s2.dv_applied_timestamp = hub.startdate;

-- ============================================================================
-- LAYER 4: EXTENDED SUPERNOVA (XSN) — computed/derived BV attributes
-- Adds business logic on top of the Supernova DT.
-- These are BV-style calculations computed once and stored.
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.SUPERNOVA.DT_XSN_SUPERNOVA_HUB_CUSTOMER
    TARGET_LAG = '1 min'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT *,
    CASE
        WHEN credit_limit > 100000 AND annual_revenue > 1000000 THEN 'PREMIUM'
        WHEN credit_limit > 50000 THEN 'STANDARD'
        ELSE 'BASIC'
    END AS customer_tier,
    CASE
        WHEN employee_count > 1000 THEN 'ENTERPRISE'
        WHEN employee_count > 100 THEN 'MID_MARKET'
        ELSE 'SMB'
    END AS customer_size
FROM <DATABASE>.SUPERNOVA.DT_SUPERNOVA_HUB_CUSTOMER;

-- ============================================================================
-- LAYER 5: DATA DELIVERY — consumption-specific views/DTs
-- ============================================================================

-- 5a. Current-state view (latest version per customer)
CREATE OR REPLACE VIEW <DATABASE>.SUPERNOVA.V_CURRENT_CUSTOMER AS
SELECT *
FROM <DATABASE>.SUPERNOVA.DT_XSN_SUPERNOVA_HUB_CUSTOMER
WHERE enddate = TO_TIMESTAMP('9999-12-31 23:59:59');

-- 5b. Premium customers filter
CREATE OR REPLACE VIEW <DATABASE>.SUPERNOVA.V_PREMIUM_CUSTOMERS AS
SELECT *
FROM <DATABASE>.SUPERNOVA.DT_XSN_SUPERNOVA_HUB_CUSTOMER
WHERE customer_tier = 'PREMIUM'
  AND enddate = TO_TIMESTAMP('9999-12-31 23:59:59');

-- 5c. Aggregate DT (pre-computed KPIs)
CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.SUPERNOVA.DT_CUSTOMER_SEGMENT_SUMMARY
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    segment,
    customer_tier,
    customer_size,
    COUNT(*)                    AS customer_count,
    SUM(annual_revenue)         AS total_revenue,
    AVG(credit_limit)           AS avg_credit_limit
FROM <DATABASE>.SUPERNOVA.DT_XSN_SUPERNOVA_HUB_CUSTOMER
WHERE enddate = TO_TIMESTAMP('9999-12-31 23:59:59')
GROUP BY segment, customer_tier, customer_size;

-- 5d. Full timeline query (all versions for a specific customer)
-- SELECT * FROM <DATABASE>.SUPERNOVA.DT_XSN_SUPERNOVA_HUB_CUSTOMER
-- WHERE customer_id = '12345'
-- ORDER BY startdate;

-- ============================================================================
-- KEY POINTS:
-- 1. Versions DT creates a UNIFIED timeline from ALL satellites for a hub
-- 2. Supernova DT equi-joins satellites to versions ON dv_applied_timestamp = startdate
-- 3. EQUI-JOIN on startdate is REQUIRED for Snowflake INCREMENTAL refresh
-- 4. Range joins (>=, BETWEEN) force full DT rebuild — never use them here
-- 5. Leaf CTEs add LEAD-based dv_applied_timestamp_end (full timeline, not just current)
-- 6. XSN (Extended Supernova) adds BV-style computed attributes on top
-- 7. L5 Delivery serves multiple patterns: current view, filters, aggregates, timeline
-- 8. Trade-off: storage cost for speed — each layer materialises data
-- 9. TARGET_LAG = DOWNSTREAM on versions DT chains refresh to the Supernova DT
-- ============================================================================
