-- PIT Table DDL — Link + 3 Satellites
-- Pre-computed join-index for link-based constellations

CREATE OR REPLACE TABLE <database>.<queryassistance_schema>.pit_<link_name>_<cadence>
(
  dv_hashkey_lnk_<link_name>                          <hashkey_type>
, <sat1_name>_dv_hashkey_lnk_<link_name>              <hashkey_type>
, <sat1_name>_dv_applied_timestamp                          DATETIME
, <sat2_name>_dv_hashkey_lnk_<link_name>              <hashkey_type>
, <sat2_name>_dv_applied_timestamp                          DATETIME
, <sat3_name>_dv_hashkey_lnk_<link_name>              <hashkey_type>
, <sat3_name>_dv_applied_timestamp                          DATETIME
, snapshotdate                                        DATE

, CONSTRAINT fk_pit_<link_name>_<cadence>_<sat1_name>
    FOREIGN KEY (<sat1_name>_dv_hashkey_lnk_<link_name>, <sat1_name>_dv_applied_timestamp)
    REFERENCES <datavault_schema>.<sat1_name> (dv_hashkey_lnk_<link_name>, dv_applied_timestamp) ENFORCED
, CONSTRAINT fk_pit_<link_name>_<cadence>_<sat2_name>
    FOREIGN KEY (<sat2_name>_dv_hashkey_lnk_<link_name>, <sat2_name>_dv_applied_timestamp)
    REFERENCES <datavault_schema>.<sat2_name> (dv_hashkey_lnk_<link_name>, dv_applied_timestamp) ENFORCED
, CONSTRAINT fk_pit_<link_name>_<cadence>_<sat3_name>
    FOREIGN KEY (<sat3_name>_dv_hashkey_lnk_<link_name>, <sat3_name>_dv_applied_timestamp)
    REFERENCES <datavault_schema>.<sat3_name> (dv_hashkey_lnk_<link_name>, dv_applied_timestamp) ENFORCED
)
;
