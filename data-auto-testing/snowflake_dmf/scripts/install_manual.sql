-- =============================================================================
-- Data Vault 2.0 DQ Framework — Manual Installation Script
-- Copy-paste into a Snowsight worksheet and run as ACCOUNTADMIN
-- (or a role with equivalent privileges — see Step 0 for custom role setup)
--
-- STEP 0  Edit the variables below to customise names
-- STEP 1  Run the full script in Snowsight (Ctrl+Shift+Enter / Cmd+Shift+Enter)
-- STEP 2  Upload the app files (see "Upload App Files" at the bottom)
-- =============================================================================

-- ┌─────────────────────────────────────────────────────────────────────────┐
-- │  CONFIGURATION — edit these values before running                      │
-- └─────────────────────────────────────────────────────────────────────────┘
SET v_role      = 'ACCOUNTADMIN';
SET v_warehouse = 'DV_DQ_WH';
SET v_database  = 'DV_DQ';
SET v_schema    = 'DQ';
SET v_app_name  = 'DV_DMF_METRICS';

-- ── Use the target role ─────────────────────────────────────────────────────
USE ROLE IDENTIFIER($v_role);

-- =============================================================================
-- STEP 1 — Warehouse
-- =============================================================================
CREATE WAREHOUSE IF NOT EXISTS IDENTIFIER($v_warehouse)
    WAREHOUSE_SIZE  = 'XSMALL'
    AUTO_SUSPEND    = 60
    AUTO_RESUME     = TRUE
    COMMENT         = 'DV2 DQ Framework — query warehouse';

-- =============================================================================
-- STEP 2 — Database and Schema
-- =============================================================================
CREATE DATABASE IF NOT EXISTS IDENTIFIER($v_database);

CREATE SCHEMA IF NOT EXISTS IDENTIFIER($v_database || '.' || $v_schema);

-- =============================================================================
-- STEP 3 — Privileges required by the app
-- =============================================================================

GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE
    TO ROLE IDENTIFIER($v_role);

GRANT USAGE ON WAREHOUSE IDENTIFIER($v_warehouse)
    TO ROLE IDENTIFIER($v_role);

GRANT USAGE ON DATABASE IDENTIFIER($v_database)
    TO ROLE IDENTIFIER($v_role);

GRANT ALL ON SCHEMA IDENTIFIER($v_database || '.' || $v_schema)
    TO ROLE IDENTIFIER($v_role);

GRANT CREATE STREAMLIT ON SCHEMA IDENTIFIER($v_database || '.' || $v_schema)
    TO ROLE IDENTIFIER($v_role);

GRANT CREATE STAGE ON SCHEMA IDENTIFIER($v_database || '.' || $v_schema)
    TO ROLE IDENTIFIER($v_role);

GRANT CREATE DATA METRIC FUNCTION ON SCHEMA IDENTIFIER($v_database || '.' || $v_schema)
    TO ROLE IDENTIFIER($v_role);

-- =============================================================================
-- STEP 4 — Stage for Streamlit app files
-- =============================================================================
CREATE STAGE IF NOT EXISTS IDENTIFIER($v_database || '.' || $v_schema || '.' || $v_app_name)
    DIRECTORY = (ENABLE = TRUE)
    COMMENT   = 'Hosts DV DMF Metrics Streamlit source files';

-- =============================================================================
-- STEP 5 — Streamlit app shell
-- (The app will show a blank page until files are uploaded in Step 6)
-- =============================================================================
CREATE STREAMLIT IF NOT EXISTS IDENTIFIER($v_database || '.' || $v_schema || '.' || $v_app_name)
    ROOT_LOCATION   = '@' || $v_database || '.' || $v_schema || '.' || $v_app_name
    MAIN_FILE       = 'streamlit_app.py'
    QUERY_WAREHOUSE = $v_warehouse
    TITLE           = 'DV DMF Metrics'
    COMMENT         = 'Data Vault 2.0 DQ Framework — 9-tab DMF monitoring dashboard';

-- =============================================================================
-- STEP 6 — Verify
-- =============================================================================
SHOW STREAMLITS LIKE 'DV_DMF_METRICS'
    IN SCHEMA IDENTIFIER($v_database || '.' || $v_schema);

-- =============================================================================
-- UPLOAD APP FILES
-- =============================================================================
--
-- After running the SQL above, upload the app source files to the stage.
-- You need the Snowflake CLI (snow) installed for this step.
--
-- From the repo root directory, run ONE of the following:
--
-- ── Option A: snow CLI (recommended) ─────────────────────────────────────────
--
--   If you used the default database/schema:
--
--     snow streamlit deploy --connection <your_connection_name> --replace
--
--   If you used custom database/schema, first generate snowflake.yml:
--
--     sed -e 's/__DATABASE__/MY_DB/g' \
--         -e 's/__SCHEMA__/MY_SCHEMA/g' \
--         -e 's/__WAREHOUSE__/MY_WH/g' \
--         snowflake.yml.template > snowflake.yml
--
--     snow streamlit deploy --connection <your_connection_name> --replace
--
-- ── Option B: Manual upload via Snowsight ────────────────────────────────────
--
--   1. In Snowsight, go to:
--        Data → Databases → <database> → <schema> → Stages → DV_DMF_METRICS
--   2. Click "+ Files" and upload:
--        streamlit_app.py
--        pyproject.toml
--
-- ── Option C: PUT via SnowSQL ────────────────────────────────────────────────
--
--   Run the following from a SnowSQL session (replace paths as needed):
--
--     PUT file:///path/to/repo/streamlit_app.py
--         @DV_DQ.DQ.DV_DMF_METRICS/
--         AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
--     PUT file:///path/to/repo/pyproject.toml
--         @DV_DQ.DQ.DV_DMF_METRICS/
--         AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
--
-- =============================================================================
-- DMF SETUP
-- =============================================================================
--
-- After uploading the app files, create the DMFs and attach them to your tables:
--
-- 1. Open sql/01_create_dq_schema_and_dmfs.sql in a Snowsight worksheet
--    - Replace <% database %> with your database name (e.g., DV_DQ)
--    - Replace <% role %> with your role (e.g., ACCOUNTADMIN)
--    - Replace <% warehouse %> with your warehouse (e.g., DV_DQ_WH)
--    - Run the full script
--
-- 2. Open sql/02_attach_dmfs_to_dv_tables.sql in a Snowsight worksheet
--    - Replace <% database %> with your database name
--    - Replace <% edw_database %> with your EDW database name
--    - Update table/column names to match YOUR Data Vault model
--    - Run the full script
--
-- =============================================================================
-- POST-INSTALL
-- =============================================================================
--
-- 1. Open Snowsight → Streamlit Apps → DV_DMF_METRICS
-- 2. The Overview tab shows overall health — data appears after DMFs run
-- 3. Use the sidebar to filter by DV layer, schema, or time window
-- 4. The DMF Coverage tab (powered by ACCOUNT_USAGE) may take up to 3h
--    after initial attachment to populate
--
-- =============================================================================
