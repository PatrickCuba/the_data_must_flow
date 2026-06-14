-- DVOS Template: Status Tracking Satellite (ST) DDL
-- Tracks INSERT/DELETE status of entity presence in source.
-- dv_status holds 'I' (present) or 'D' (absent).
-- Hashdiff is SHA1_BINARY('I') or SHA1_BINARY('D').

CREATE TABLE IF NOT EXISTS <schema>.SAT_ST_<PARENT>_<SOURCE> (
    dv_hashkey_hub_<parent>  BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_status                VARCHAR(1)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_st_<parent>_<source> PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp) NOT ENFORCED
);
