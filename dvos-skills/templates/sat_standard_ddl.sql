-- DVOS Template: Standard Satellite DDL
-- No end-date column. Current row via QUALIFY ROW_NUMBER() in VC_ views.

CREATE TABLE IF NOT EXISTS <schema>.SAT_<PARENT>_<CONTEXT> (
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
    -- business attributes below this line
    <attr1>                  VARCHAR,
    <attr2>                  VARCHAR,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_<parent>_<context> PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp) NOT ENFORCED
);
