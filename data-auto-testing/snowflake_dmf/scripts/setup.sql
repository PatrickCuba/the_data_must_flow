-- =============================================================================
-- Data Vault 2.0 DQ Framework — Infrastructure Setup
-- Run once per target account. Fully idempotent (safe to re-run).
--
-- Variables injected by deploy.sh via: snow sql --variable key=value
--   <% database %>       Target database  (default: DV_DQ)
--   <% schema %>         Target schema    (default: DQ)
--   <% warehouse %>      Warehouse name   (default: DV_DQ_WH)
--   <% role %>           Deploying role   (default: ACCOUNTADMIN)
-- =============================================================================

USE ROLE IDENTIFIER('<% role %>');

-- ── Warehouse ────────────────────────────────────────────────────────────────
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER('<% warehouse %>')
    WAREHOUSE_SIZE  = 'XSMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    COMMENT         = 'DV2 DQ Framework — query warehouse';

-- ── Database + Schema ────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS IDENTIFIER('<% database %>');
CREATE SCHEMA   IF NOT EXISTS IDENTIFIER('<% database %>.<% schema %>');

-- ── ACCOUNT_USAGE + LOCAL access ─────────────────────────────────────────────
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE IDENTIFIER('<% role %>');

-- ── App object privileges ────────────────────────────────────────────────────
GRANT USAGE            ON WAREHOUSE IDENTIFIER('<% warehouse %>')              TO ROLE IDENTIFIER('<% role %>');
GRANT USAGE            ON DATABASE  IDENTIFIER('<% database %>')               TO ROLE IDENTIFIER('<% role %>');
GRANT ALL              ON SCHEMA    IDENTIFIER('<% database %>.<% schema %>')  TO ROLE IDENTIFIER('<% role %>');
GRANT CREATE STREAMLIT ON SCHEMA    IDENTIFIER('<% database %>.<% schema %>')  TO ROLE IDENTIFIER('<% role %>');
GRANT CREATE STAGE     ON SCHEMA    IDENTIFIER('<% database %>.<% schema %>')  TO ROLE IDENTIFIER('<% role %>');
GRANT CREATE DATA METRIC FUNCTION ON SCHEMA IDENTIFIER('<% database %>.<% schema %>') TO ROLE IDENTIFIER('<% role %>');
