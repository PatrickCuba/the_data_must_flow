-- DVOS Template: Record Tracking Satellite (RT) DDL
-- Tracks entity presence per dv_applied_timestamp.
-- One record per (hashkey, applied_timestamp) — records "I saw this entity at this time".

CREATE TABLE IF NOT EXISTS <schema>.SAT_RT_<PARENT>_<SOURCE> (
    dv_hashkey_hub_<parent>  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_rt_<parent>_<source> PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp) NOT ENFORCED
);
