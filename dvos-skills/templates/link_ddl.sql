-- DVOS Template: Link DDL
-- Replace <NAME>, <hub_a>, <hub_b>, <schema> with actual values.
-- FK constraints intentionally omitted — deferred to orphan-check phase.
-- Note: Links do NOT have dv_collisioncode (that is hub-only).

CREATE TABLE IF NOT EXISTS <schema>.LNK_<NAME> (
    dv_hashkey_lnk_<name>   BINARY(20)       NOT NULL,
    dv_hashkey_hub_<hub_a>  BINARY(20)       NOT NULL,
    dv_hashkey_hub_<hub_b>  BINARY(20)       NOT NULL,
    dv_tenant_id            VARCHAR(50),
    dv_applied_timestamp    TIMESTAMP_NTZ    NOT NULL,
    dv_recordsource         VARCHAR(255)     NOT NULL,
    dv_load_timestamp       TIMESTAMP_NTZ    NOT NULL,
    dv_task_id              VARCHAR(255),
    dv_jira_id              VARCHAR(255),
    dv_user_id              VARCHAR(255),
    last_seen_date          TIMESTAMP_NTZ,
    CONSTRAINT pk_lnk_<name> PRIMARY KEY (dv_hashkey_lnk_<name>) NOT ENFORCED
);
