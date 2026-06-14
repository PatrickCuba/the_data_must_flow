-- DVOS Template: Effectivity Satellite DDL
-- Link-only. No business attributes. Insert-only. Driver-key driven.

CREATE TABLE IF NOT EXISTS <schema>.SAT_<LINK>_EFF (
    dv_hashkey_lnk_<link>    BINARY(20)       NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_collisioncode         VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_start_date            TIMESTAMP_NTZ    NOT NULL,
    dv_end_date              TIMESTAMP_NTZ    NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    dv_sid                   NUMBER           IDENTITY START 0 INCREMENT 1 ORDER,
    CONSTRAINT pk_sat_<link>_eff PRIMARY KEY (dv_hashkey_lnk_<link>, dv_load_timestamp) NOT ENFORCED
);
