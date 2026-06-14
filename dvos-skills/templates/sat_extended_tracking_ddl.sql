-- DVOS Template: Extended Tracking Satellite (XTS) DDL
-- Tracks adjacent satellite hashdiffs per entity per applied_timestamp.
-- dv_record_target identifies which satellite the hashdiff belongs to.
-- Note: XTS has no dv_sid and no dv_collisioncode.

CREATE TABLE IF NOT EXISTS <schema>.SAT_XT_<PARENT>_<SOURCE> (
    dv_hashkey_hub_<parent>  BINARY(20)       NOT NULL,
    dv_record_target         VARCHAR(255)     NOT NULL,
    dv_tenant_id             VARCHAR(50),
    dv_task_id               VARCHAR(255),
    dv_jira_id               VARCHAR(255),
    dv_user_id               VARCHAR(255),
    dv_recordsource          VARCHAR(255)     NOT NULL,
    dv_hashdiff              BINARY(20)       NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ    NOT NULL,
    CONSTRAINT pk_sat_xt_<parent>_<source> PRIMARY KEY (dv_hashkey_hub_<parent>, dv_record_target, dv_load_timestamp) NOT ENFORCED
);
