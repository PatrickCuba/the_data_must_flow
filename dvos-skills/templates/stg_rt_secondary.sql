-- DVOS Template: Record Tracking Secondary Staging View
-- Simple passthrough from base staging — records entity presence per applied_timestamp.
-- Hashdiff = hash of dv_applied_timestamp only.

CREATE OR REPLACE VIEW <staging_schema>.STG_RT_<SOURCE_FILE>_HUB_<PARENT>_<HASHKEY_COL> AS
SELECT
    <hashkey_col> AS dv_hashkey_hub_<parent>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_applied_timestamp AS STRING)), '')
    )) AS dv_hashdiff_sat_rt_<parent>_<source>,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_collisioncode,
    dv_recordsource,
    dv_task_id,
    dv_jira_id,
    dv_user_id
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;
