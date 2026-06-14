-- DVOS Template: Satellite INSERT Load Pattern
-- Anti-semi join on (parent_hk, hashdiff). Never MERGE on satellites.

INSERT INTO <schema>.SAT_<PARENT>_<CONTEXT> (
    dv_hashkey_hub_<parent>,
    dv_tenant_id,
    dv_collisioncode,
    dv_task_id,
    dv_jira_id,
    dv_user_id,
    dv_recordsource,
    dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    <attr1>,
    <attr2>
)
SELECT
    src.dv_hashkey_hub_<parent>,
    src.dv_tenant_id,
    src.dv_collisioncode,
    src.dv_task_id,
    src.dv_jira_id,
    src.dv_user_id,
    src.dv_recordsource,
    src.dv_hashdiff_sat_<parent>_<context>,
    src.dv_applied_timestamp,
    src.dv_load_timestamp,
    src.<attr1>,
    src.<attr2>
FROM <staging_view> src
WHERE NOT EXISTS (
    SELECT 1 FROM <schema>.SAT_<PARENT>_<CONTEXT> s
    WHERE s.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
      AND s.dv_hashdiff = src.dv_hashdiff_sat_<parent>_<context>
);
