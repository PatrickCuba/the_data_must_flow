-- DVOS Template: Extended Tracking Satellite (XTS) DDL
-- One XTS per hub, one per link.
-- Tracks adjacent satellite hashdiffs per entity per applied date.
-- dv_rectarget identifies which satellite the hashdiff belongs to.
-- Note: XTS has no dv_sid and no dv_collisioncode.

CREATE TRANSIENT TABLE IF NOT EXISTS <schema>.SAT_XT_<PARENT_TYPE>_<PARENT>
(
    dv_tenant_id              VARCHAR(50),
    dv_hashkey_hub_<parent>  BINARY(20)    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ NOT NULL,
    dv_recordsource          VARCHAR(255)  NOT NULL,
    dv_hashdiff              BINARY(20)    NOT NULL,
    dv_rectarget             VARCHAR(40)   NOT NULL,  -- name of the adjacent satellite being tracked
    dv_sequence_violation    BOOLEAN       NOT NULL,  -- TRUE = staged record is older than target satellite

    -- Optional: GDPR/retention columns — add when dv_disposal_required: true in manifest
    -- dv_record_retention_state    VARCHAR(10)   -- 'Active', 'Archived', 'Purged'
    -- dv_disposal_record_requested BOOLEAN
    -- dv_disposed_record_reemerged BOOLEAN

    CONSTRAINT pk_sat_xt_<parent_type>_<parent>
        PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp) NOT ENFORCED
)
DATA_RETENTION_TIME_IN_DAYS = 1;
