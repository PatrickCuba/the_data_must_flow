-- DVOS Template: Effectivity Secondary Staging View
-- Replace <driver_key>, <hub_a>, <hub_b>, <link>, <source_badge>, <source_file>, <high_date>
-- This view generates OPEN and CLOSE records for the effectivity satellite loader.

CREATE OR REPLACE VIEW <staging_schema>.STG_EF_<SOURCE_BADGE>_<SOURCE_FILE> AS

WITH latest_effs AS (
    SELECT
        lnk.dv_hashkey_hub_<hub_a>,
        lnk.dv_hashkey_hub_<hub_b>,
        lnk.dv_hashkey_lnk_<link>,
        ef.dv_start_date,
        ef.dv_end_date
    FROM <vault_schema>.LNK_<LINK> lnk
    JOIN <vault_schema>.SAT_<LINK>_EFF ef
        ON ef.dv_hashkey_lnk_<link> = lnk.dv_hashkey_lnk_<link>
    WHERE ef.dv_end_date = '<high_date>'::TIMESTAMP_NTZ
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lnk.dv_hashkey_lnk_<link>
        ORDER BY ef.dv_applied_timestamp DESC, ef.dv_load_timestamp DESC
    ) = 1
),

src_date AS (
    SELECT DISTINCT
        dv_hashkey_hub_<driver_key>,
        dv_applied_timestamp
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>
),

-- OPEN: new relationships (in staging but not active in target)
open_records AS (
    SELECT
        src.dv_hashkey_lnk_<link>,
        src.dv_applied_timestamp AS dv_start_date,
        '<high_date>'::TIMESTAMP_NTZ AS dv_end_date,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM latest_effs le
        WHERE le.dv_hashkey_hub_<hub_a> = src.dv_hashkey_hub_<hub_a>
          AND le.dv_hashkey_hub_<hub_b> = src.dv_hashkey_hub_<hub_b>
    )
),

-- CLOSE: relationships that changed (driver key present but participants differ)
close_records AS (
    SELECT
        le.dv_hashkey_lnk_<link>,
        le.dv_start_date AS dv_start_date,
        sd.dv_applied_timestamp AS dv_end_date,
        sd.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        le.dv_tenant_id,
        le.dv_recordsource
    FROM latest_effs le
    JOIN src_date sd ON sd.dv_hashkey_hub_<driver_key> = le.dv_hashkey_hub_<driver_key>
    WHERE NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src2
        WHERE src2.dv_hashkey_hub_<hub_a> = le.dv_hashkey_hub_<hub_a>
          AND src2.dv_hashkey_hub_<hub_b> = le.dv_hashkey_hub_<hub_b>
    )
)

-- Output: CLOSE first, then OPEN
SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM close_records

UNION ALL

SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM open_records;
