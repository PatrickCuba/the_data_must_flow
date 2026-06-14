-- DVOS Template: Extended Tracking Secondary Staging View
-- UNION ALLs one SELECT per related satellite, carrying each satellite's hashdiff.
-- dv_record_target identifies which satellite the hashdiff row belongs to.
-- Excludes peripheral types (EF, RT, ST, NH, XTS) from the UNION.

CREATE OR REPLACE VIEW <staging_schema>.STG_XT_<SOURCE_FILE>_HUB_<PARENT>_<HASHKEY_COL> AS

-- Satellite 1
SELECT
    <hashkey_col> AS dv_hashkey_hub_<parent>,
    '<SAT_NAME_1>' AS dv_record_target,
    dv_hashdiff_sat_<name_1> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_collisioncode,
    dv_recordsource,
    dv_task_id,
    dv_jira_id,
    dv_user_id
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>

UNION ALL

-- Satellite 2
SELECT
    <hashkey_col> AS dv_hashkey_hub_<parent>,
    '<SAT_NAME_2>' AS dv_record_target,
    dv_hashdiff_sat_<name_2> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_collisioncode,
    dv_recordsource,
    dv_task_id,
    dv_jira_id,
    dv_user_id
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;

-- Extend with additional UNION ALL blocks per related satellite.
-- Only include satellites whose dv_hashdiff_* column exists in the base staging view.
