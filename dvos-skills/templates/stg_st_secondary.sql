-- DVOS Template: Status Tracking Secondary Staging View
-- Replace <parent>, <source_badge>, <source_file>
-- Generates INSERT ('I') and DELETE ('D') status change records.

CREATE OR REPLACE VIEW <staging_schema>.STG_ST_<SOURCE_BADGE>_<SOURCE_FILE>_HUB_<PARENT> AS

WITH current_status AS (
    -- Latest status per hashkey from the STS satellite itself
    SELECT dv_hashkey_hub_<parent>, dv_hashdiff
    FROM <vault_schema>.SAT_ST_<PARENT>_<SOURCE>
    WHERE dv_recordsource != 'GHOST'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<parent>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
),

-- INSERT: present in staging but not active in STS (or last status was 'D')
gen_inserts AS (
    SELECT
        src.dv_hashkey_hub_<parent>,
        SHA1_BINARY('I') AS dv_hashdiff,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM current_status cs
        WHERE cs.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
          AND cs.dv_hashdiff != SHA1_BINARY('D')
    )
),

-- DELETE: active in STS but absent from current staging snapshot
gen_deletes AS (
    SELECT
        cs.dv_hashkey_hub_<parent>,
        SHA1_BINARY('D') AS dv_hashdiff,
        src_ts.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        src_ts.dv_tenant_id,
        src_ts.dv_recordsource
    FROM current_status cs
    CROSS JOIN (
        SELECT MAX(dv_applied_timestamp) AS dv_applied_timestamp,
               MAX(dv_tenant_id) AS dv_tenant_id,
               MAX(dv_recordsource) AS dv_recordsource
        FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>
    ) src_ts
    WHERE cs.dv_hashdiff != SHA1_BINARY('D')
      AND NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
        WHERE src.dv_hashkey_hub_<parent> = cs.dv_hashkey_hub_<parent>
    )
)

SELECT * FROM gen_inserts
UNION ALL
SELECT * FROM gen_deletes;
