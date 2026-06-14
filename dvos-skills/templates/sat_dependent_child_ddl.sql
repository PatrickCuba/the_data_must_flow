-- DVOS Template: Dependent-Child Satellite DDL
-- Dep-child keys are nullable attributes, NOT in the PK.
-- dv_sequence is a synthetic PK discriminator.

CREATE TABLE IF NOT EXISTS <schema>.SAT_DP_<PARENT>_<CONTEXT> (
    dv_hashkey_hub_<parent>  BINARY(20)       NOT NULL,
    <dep_key_column>         VARCHAR,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_sequence              NUMBER,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    -- business attributes below this line
    <attr1>                  VARCHAR,
    <attr2>                  VARCHAR,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_dp_<parent>_<context> PRIMARY KEY (dv_hashkey_hub_<parent>, dv_sequence, dv_load_timestamp) NOT ENFORCED
);
