-- DVOS Example: Kappa Vault Loading Pattern
-- Event-driven loading using Snowflake Streams + Tasks on staging views.
-- Use when: source lands continuously via Snowpipe (not batch).
-- Target: Snowflake. Replace <DATABASE> with your target database.
--
-- Key difference from standard batch:
--   - Streams placed on STAGING VIEWS (not landing tables)
--   - One stream per loader (hub stream, sat stream — separate offsets)
--   - Tasks fire only when SYSTEM$STREAM_HAS_DATA() is true
--   - Load + reconciliation wrapped in BEGIN TRANSACTION / COMMIT
--   - All vault DDL is IDENTICAL to batch — only the pipeline layer differs

-- ============================================================================
-- SCHEMAS
-- ============================================================================

CREATE DATABASE IF NOT EXISTS <DATABASE>;
CREATE TRANSIENT SCHEMA IF NOT EXISTS <DATABASE>.STAGED;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.VAULT;
CREATE SCHEMA IF NOT EXISTS <DATABASE>.INFORMATION_MARTS;

-- ============================================================================
-- LANDING TABLE (Snowpipe continuously loads JSON from external stage)
-- ============================================================================

CREATE TABLE IF NOT EXISTS <DATABASE>.STAGED.LANDING_XERO_ACCOUNTS (
    raw_json         VARIANT       NOT NULL,
    metadata_filename VARCHAR(500),
    metadata_row_number NUMBER,
    load_datetime    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Snowpipe definition (external stage assumed pre-configured)
CREATE OR REPLACE PIPE <DATABASE>.STAGED.PIPE_XERO_ACCOUNTS
    AUTO_INGEST = TRUE
    AS COPY INTO <DATABASE>.STAGED.LANDING_XERO_ACCOUNTS
    FROM @<DATABASE>.STAGED.XERO_STAGE
    FILE_FORMAT = (TYPE = JSON);

-- ============================================================================
-- HUB + SATELLITE (same DDL as batch — no difference)
-- ============================================================================

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

CREATE TABLE IF NOT EXISTS <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL (
    dv_hashkey_hub_account   BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    account_name             VARCHAR,
    account_type             VARCHAR,
    balance                  NUMBER(18,2),
    currency_code            VARCHAR(3),
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rv_hub_xero_account_detail
        PRIMARY KEY (dv_hashkey_hub_account, dv_load_timestamp) NOT ENFORCED
);

-- ============================================================================
-- GHOST RECORD (idempotent — required for PIT/SNOPIT equi-joins)
-- ============================================================================

INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL (
    dv_hashkey_hub_account, dv_tenant_id, dv_task_id, dv_jira_id, dv_user_id,
    dv_recordsource, dv_hashdiff, dv_applied_timestamp, dv_load_timestamp
)
SELECT
    TO_BINARY(REPEAT(0, 20)), NULL, 'GHOST', 'GHOST', 'GHOST',
    'GHOST', TO_BINARY(REPEAT(0, 20)),
    '1900-01-01'::TIMESTAMP_NTZ, '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (
    SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL
    WHERE dv_hashkey_hub_account = TO_BINARY(REPEAT(0, 20))
);

-- ============================================================================
-- STAGING VIEW (Kappa: stream reads from THIS view, not the landing table)
-- ============================================================================

CREATE OR REPLACE VIEW <DATABASE>.STAGED.STG_XERO_ACCOUNTS AS
SELECT
    -- Hash key
    SHA1_BINARY(UPPER(CONCAT(
        'default' || '||' ||
        'default' || '||' ||
        COALESCE(NULLIF(TRIM(CAST(raw_json:account_id::STRING AS STRING)), ''), '-1')
    ))) AS dv_hashkey_hub_account,

    -- Hashdiff (no UPPER on attributes)
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(raw_json:account_name::STRING AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(raw_json:account_type::STRING AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(raw_json:balance::STRING AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(raw_json:currency_code::STRING AS STRING)), '')
    )) AS dv_hashdiff_sat_account_xero,

    -- Metadata
    'default'                           AS dv_tenant_id,
    'default'                           AS dv_collisioncode,
    CURRENT_TIMESTAMP()                 AS dv_load_timestamp,
    COALESCE(raw_json:updated_at::TIMESTAMP_NTZ, load_datetime) AS dv_applied_timestamp,
    'XERO.ACCOUNTS'                     AS dv_recordsource,

    -- Business key + attributes
    raw_json:account_id::STRING         AS account_id,
    raw_json:account_name::STRING       AS account_name,
    raw_json:account_type::STRING       AS account_type,
    raw_json:balance::NUMBER(18,2)      AS balance,
    raw_json:currency_code::STRING      AS currency_code
FROM <DATABASE>.STAGED.LANDING_XERO_ACCOUNTS;

-- ============================================================================
-- STREAMS — one per loader (separate offsets for hub vs satellite)
-- ============================================================================

CREATE OR REPLACE STREAM <DATABASE>.STAGED.STR_STG_XERO_HUB_ACCOUNT
    ON VIEW <DATABASE>.STAGED.STG_XERO_ACCOUNTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

CREATE OR REPLACE STREAM <DATABASE>.STAGED.STR_STG_XERO_SAT_ACCOUNT
    ON VIEW <DATABASE>.STAGED.STG_XERO_ACCOUNTS
    APPEND_ONLY = TRUE
    SHOW_INITIAL_ROWS = TRUE;

-- ============================================================================
-- TASKS — event-driven, fire only when stream has data
-- Load + test wrapped in BEGIN TRANSACTION / COMMIT for Repeatable Read Isolation:
-- the test validates the EXACT same records that were just loaded.
-- ============================================================================

CREATE OR REPLACE TASK <DATABASE>.STAGED.TSK_KAPPA_LOAD_HUB_ACCOUNT
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('<DATABASE>.STAGED.STR_STG_XERO_HUB_ACCOUNT')
AS
BEGIN
    BEGIN TRANSACTION;

    -- LOAD: Hub MERGE with distinct_view CTE
    -- distinct_view deduplicates the stream — if the same hashkey appears multiple
    -- times in the stream batch (Snowpipe landed multiple records before task fired),
    -- only the first occurrence per entity is kept.
    MERGE INTO <DATABASE>.VAULT.HUB_ACCOUNT AS tgt
    USING (
        WITH distinct_view AS (
            SELECT *,
                LAG(dv_hashkey_hub_account) OVER (
                    PARTITION BY dv_hashkey_hub_account ORDER BY dv_applied_timestamp
                ) AS prev_dv_hashkey
            FROM <DATABASE>.STAGED.STR_STG_XERO_HUB_ACCOUNT
            QUALIFY dv_hashkey_hub_account <> prev_dv_hashkey OR prev_dv_hashkey IS NULL
        )
        SELECT DISTINCT dv_hashkey_hub_account, account_id, dv_tenant_id,
               dv_collisioncode, dv_applied_timestamp, dv_recordsource, dv_load_timestamp
        FROM distinct_view
    ) AS src
    ON tgt.dv_hashkey_hub_account = src.dv_hashkey_hub_account
    WHEN NOT MATCHED THEN INSERT (
        dv_hashkey_hub_account, account_id, dv_tenant_id, dv_collisioncode,
        dv_applied_timestamp, dv_recordsource, dv_load_timestamp, last_seen_date
    ) VALUES (
        src.dv_hashkey_hub_account, src.account_id, src.dv_tenant_id, src.dv_collisioncode,
        src.dv_applied_timestamp, src.dv_recordsource, src.dv_load_timestamp, src.dv_applied_timestamp
    )
    WHEN MATCHED THEN UPDATE SET tgt.last_seen_date = src.dv_applied_timestamp;

    -- TEST: reconcile hub (runs on same stream, same transaction)
    -- Validates every staged hashkey now exists in the hub.
    -- On COMMIT both the load and the test result are written atomically.
    INSERT INTO <DATABASE>.STAGED.RECONCILE_HUB_ERRORS (tablename, loaddate, rundate, missing_count)
    SELECT 'HUB_ACCOUNT', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        COUNT(*)
    FROM <DATABASE>.STAGED.STR_STG_XERO_HUB_ACCOUNT sg
    WHERE NOT EXISTS (
        SELECT 1 FROM <DATABASE>.VAULT.HUB_ACCOUNT h
        WHERE sg.dv_hashkey_hub_account = h.dv_hashkey_hub_account
    );

    COMMIT;
END;

CREATE OR REPLACE TASK <DATABASE>.STAGED.TSK_KAPPA_LOAD_SAT_ACCOUNT
    WAREHOUSE = TRANSFORM_WH
    SCHEDULE  = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('<DATABASE>.STAGED.STR_STG_XERO_SAT_ACCOUNT')
AS
BEGIN
    BEGIN TRANSACTION;

    -- LOAD: Satellite INSERT with discard_view CTE
    -- discard_view discards consecutive duplicate hashdiffs WITHIN the stream batch
    -- before comparing against the target satellite. Only true changes survive.
    INSERT INTO <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL (
        dv_hashkey_hub_account, dv_tenant_id, dv_recordsource,
        dv_hashdiff, dv_applied_timestamp, dv_load_timestamp,
        account_name, account_type, balance, currency_code
    )
    WITH discard_view AS (
        SELECT *,
            LAG(dv_hashdiff_sat_account_xero) OVER (
                PARTITION BY dv_hashkey_hub_account ORDER BY dv_applied_timestamp
            ) AS prev_dv_hashdiff,
            RANK() OVER (
                PARTITION BY dv_hashkey_hub_account ORDER BY dv_applied_timestamp
            ) AS dv_cnt
        FROM <DATABASE>.STAGED.STR_STG_XERO_SAT_ACCOUNT
        QUALIFY dv_hashdiff_sat_account_xero <> prev_dv_hashdiff OR prev_dv_hashdiff IS NULL
    )
    SELECT
        stg.dv_hashkey_hub_account, stg.dv_tenant_id, stg.dv_recordsource,
        stg.dv_hashdiff_sat_account_xero, stg.dv_applied_timestamp, stg.dv_load_timestamp,
        stg.account_name, stg.account_type, stg.balance, stg.currency_code
    FROM discard_view stg
    WHERE NOT EXISTS (
        SELECT 1 FROM (
            SELECT dv_hashkey_hub_account, dv_hashdiff,
                   RANK() OVER (PARTITION BY dv_hashkey_hub_account
                                ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC) AS dv_rnk
            FROM <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL
            QUALIFY dv_rnk = 1
        ) cur
        WHERE stg.dv_hashkey_hub_account = cur.dv_hashkey_hub_account
          AND stg.dv_hashdiff_sat_account_xero = cur.dv_hashdiff
    ) OR stg.dv_cnt > 1;

    -- TEST: reconcile satellite (runs on same stream, same transaction)
    -- Validates loaded records now exist in the satellite.
    INSERT INTO <DATABASE>.STAGED.RECONCILE_SAT_ERRORS (tablename, loaddate, rundate, missing_count)
    SELECT 'SAT_RV_HUB_XERO_ACCOUNT_DETAIL', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
        COUNT(*)
    FROM <DATABASE>.STAGED.STR_STG_XERO_SAT_ACCOUNT sg
    WHERE NOT EXISTS (
        SELECT 1 FROM <DATABASE>.VAULT.SAT_RV_HUB_XERO_ACCOUNT_DETAIL s
        WHERE sg.dv_hashkey_hub_account = s.dv_hashkey_hub_account
          AND sg.dv_hashdiff_sat_account_xero = s.dv_hashdiff
    );

    COMMIT;
END;

-- Resume tasks to activate
ALTER TASK <DATABASE>.STAGED.TSK_KAPPA_LOAD_SAT_ACCOUNT RESUME;
ALTER TASK <DATABASE>.STAGED.TSK_KAPPA_LOAD_HUB_ACCOUNT RESUME;

-- ============================================================================
-- KEY POINTS:
-- 1. Vault DDL is IDENTICAL to batch — only pipeline layer differs
-- 2. Streams on VIEWS (not tables) — view computes hashkeys/hashdiff live
-- 3. One stream per loader — hub and sat advance independently
-- 4. APPEND_ONLY = TRUE — Kappa only sees new inserts (Snowpipe is append-only)
-- 5. BEGIN TRANSACTION / COMMIT wraps LOAD + TEST together:
--    - Repeatable Read isolation ensures test validates exact same records loaded
--    - Stream advances ONLY on COMMIT — if load or test fails, stream rewinds
-- 6. distinct_view CTE (hub): deduplicates stream when same BK appears multiple times
-- 7. discard_view CTE (sat): discards consecutive duplicate hashdiffs within batch
--    before comparing against target — only true changes survive
-- 8. A single vault can mix Kappa (for continuous sources) and batch (for daily)
-- ============================================================================
