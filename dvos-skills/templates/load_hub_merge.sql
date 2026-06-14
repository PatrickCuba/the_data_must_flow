-- DVOS Template: Hub/Link MERGE Load Pattern
-- WHEN MATCHED only updates last_seen_date. Nothing else.

MERGE INTO <schema>.HUB_<NAME> AS tgt
USING <staging_view> AS src
ON tgt.dv_hashkey_hub_<name> = src.dv_hashkey_hub_<name>
WHEN NOT MATCHED THEN INSERT (
    dv_hashkey_hub_<name>,
    <bk_column>,
    dv_tenant_id,
    dv_collisioncode,
    dv_applied_timestamp,
    dv_recordsource,
    dv_load_timestamp,
    dv_task_id,
    dv_jira_id,
    dv_user_id,
    last_seen_date
) VALUES (
    src.dv_hashkey_hub_<name>,
    src.<bk_column>,
    src.dv_tenant_id,
    src.dv_collisioncode,
    src.dv_applied_timestamp,
    src.dv_recordsource,
    src.dv_load_timestamp,
    src.dv_task_id,
    src.dv_jira_id,
    src.dv_user_id,
    src.dv_applied_timestamp
)
WHEN MATCHED THEN UPDATE SET
    tgt.last_seen_date = src.dv_applied_timestamp;
