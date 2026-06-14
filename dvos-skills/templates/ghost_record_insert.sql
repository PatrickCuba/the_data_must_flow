-- DVOS Template: Ghost Record INSERT (per satellite)
-- Idempotent via WHERE NOT EXISTS. Run once at DDL time.

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
    dv_load_timestamp
)
SELECT
    TO_BINARY(REPEAT(0, 20)),
    NULL,
    NULL,
    'GHOST',
    'GHOST',
    'GHOST',
    'GHOST',
    TO_BINARY(REPEAT(0, 20)),
    '1900-01-01'::TIMESTAMP_NTZ,
    '1900-01-01'::TIMESTAMP_NTZ
WHERE NOT EXISTS (
    SELECT 1 FROM <schema>.SAT_<PARENT>_<CONTEXT>
    WHERE dv_hashkey_hub_<parent> = TO_BINARY(REPEAT(0, 20))
);
