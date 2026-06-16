-- DVOS Example: When Data Vault Is NOT a Good Fit
-- Simple medallion/staging-to-reporting pipeline for scenarios where
-- DV complexity is not justified.
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- When to use THIS approach instead of Data Vault:
--   - 1-3 source systems (no multi-source integration challenge)
--   - No regulatory audit requirement for full history
--   - Single team owns the entire pipeline (no parallel development)
--   - Schema changes are infrequent and controlled
--   - Time-to-value is the primary constraint (prototype/POC/MVP)
--   - The business question is well-defined and unlikely to change
--
-- When to SWITCH to Data Vault:
--   - Source count grows beyond 3 (integration complexity rises)
--   - Audit/compliance requirements appear
--   - Multiple teams need to work on the same data simultaneously
--   - Business questions keep changing (need flexible model)
--   - "We need to go back in time" requests start arriving

-- ============================================================================
-- SCHEMAS — simple 3-layer medallion
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE TRANSIENT SCHEMA IF NOT EXISTS <DATABASE>.BRONZE;   -- raw landing
CREATE SCHEMA IF NOT EXISTS <DATABASE>.SILVER;             -- cleaned/typed
CREATE SCHEMA IF NOT EXISTS <DATABASE>.GOLD;               -- business-ready

-- ============================================================================
-- BRONZE: Raw landing (Snowpipe or COPY INTO — data as-is from source)
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.BRONZE.RAW_SALES (
    raw_data         VARIANT       NOT NULL,
    source_file      VARCHAR(500),
    load_datetime    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Load via COPY INTO from external stage
COPY INTO <DATABASE>.BRONZE.RAW_SALES (raw_data, source_file)
FROM (
    SELECT $1, METADATA$FILENAME
    FROM @<DATABASE>.BRONZE.SALES_STAGE
)
FILE_FORMAT = (TYPE = JSON);

-- ============================================================================
-- SILVER: Cleaned, typed, deduplicated (Dynamic Table — incremental)
-- ============================================================================

CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.SILVER.SALES_CLEANED
    TARGET_LAG = '30 minutes'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    raw_data:order_id::VARCHAR        AS order_id,
    raw_data:customer_id::VARCHAR     AS customer_id,
    raw_data:product_id::VARCHAR      AS product_id,
    raw_data:quantity::NUMBER         AS quantity,
    raw_data:unit_price::NUMBER(18,2) AS unit_price,
    raw_data:order_date::DATE         AS order_date,
    raw_data:status::VARCHAR          AS order_status,
    source_file,
    load_datetime
FROM <DATABASE>.BRONZE.RAW_SALES
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_data:order_id ORDER BY load_datetime DESC) = 1;

-- ============================================================================
-- GOLD: Business-ready aggregates and dimensions
-- ============================================================================

-- Simple dimension (no history tracking — latest state only)
CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.GOLD.DIM_PRODUCT
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT DISTINCT
    raw_data:product_id::VARCHAR      AS product_id,
    raw_data:product_name::VARCHAR    AS product_name,
    raw_data:category::VARCHAR        AS category,
    raw_data:brand::VARCHAR           AS brand
FROM <DATABASE>.BRONZE.RAW_PRODUCTS
QUALIFY ROW_NUMBER() OVER (PARTITION BY raw_data:product_id ORDER BY load_datetime DESC) = 1;

-- Fact table (aggregated for reporting)
CREATE OR REPLACE DYNAMIC TABLE <DATABASE>.GOLD.FACT_DAILY_SALES
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORM_WH
AS
SELECT
    order_date,
    product_id,
    COUNT(DISTINCT order_id)    AS order_count,
    SUM(quantity)               AS total_quantity,
    SUM(quantity * unit_price)  AS total_revenue
FROM <DATABASE>.SILVER.SALES_CLEANED
WHERE order_status != 'CANCELLED'
GROUP BY order_date, product_id;

-- Dashboard-ready view (star schema join)
CREATE OR REPLACE VIEW <DATABASE>.GOLD.RPT_SALES_BY_PRODUCT AS
SELECT
    f.order_date,
    d.product_name,
    d.category,
    d.brand,
    f.order_count,
    f.total_quantity,
    f.total_revenue
FROM <DATABASE>.GOLD.FACT_DAILY_SALES f
JOIN <DATABASE>.GOLD.DIM_PRODUCT d ON d.product_id = f.product_id;

-- ============================================================================
-- WHY THIS IS SIMPLER THAN DATA VAULT:
--
-- 1. No hash keys — natural keys used throughout
-- 2. No satellite splitting — all attributes in one place
-- 3. No MERGE patterns — simple INSERT/overwrite via Dynamic Tables
-- 4. No ghost records, no PIT tables, no bridge tables
-- 5. No staging views with hashkey/hashdiff computation
-- 6. 3 layers instead of 5+ (landing, staging, vault, BV, IM)
-- 7. Single team can build and maintain the entire pipeline
--
-- BUT YOU LOSE:
--
-- 1. Full audit trail (can't answer "what was the value at date X?")
-- 2. Multi-source integration (adding source 4 requires rework)
-- 3. Parallel development (team B can't work independently of team A)
-- 4. Schema change resilience (source changes break silver layer)
-- 5. Non-destructive changes (adding a column may require reload)
-- 6. Regulatory compliance (no insert-only history = no proof of lineage)
--
-- DECISION RULE: Start here if you have <3 sources, no audit need, single team.
-- Migrate to Data Vault when complexity outgrows this approach.
-- See /dv-when for the full decision framework.
-- ============================================================================
