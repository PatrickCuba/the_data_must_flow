-- DVOS Template: Hub DDL
-- Replace <NAME>, <bk_column>, <schema> with actual values.
-- Hash algorithm default: SHA1 → BINARY(20). Adjust for MD5 (16) or SHA256 (32).

CREATE TABLE IF NOT EXISTS <schema>.HUB_<NAME> (
    dv_hashkey_hub_<name>   BINARY(20)       NOT NULL,
    <bk_column>             VARCHAR          NOT NULL,
    dv_tenant_id            VARCHAR(50),
    dv_collisioncode        VARCHAR(50),
    dv_applied_timestamp    TIMESTAMP_NTZ    NOT NULL,
    dv_recordsource         VARCHAR(255)     NOT NULL,
    dv_load_timestamp       TIMESTAMP_NTZ    NOT NULL,
    dv_task_id              VARCHAR(255),
    dv_jira_id              VARCHAR(255),
    dv_user_id              VARCHAR(255),
    last_seen_date          TIMESTAMP_NTZ,
    CONSTRAINT pk_hub_<name> PRIMARY KEY (dv_hashkey_hub_<name>) NOT ENFORCED
);
