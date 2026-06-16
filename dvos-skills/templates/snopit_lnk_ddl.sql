-- SNOPIT Table DDL — Link + 3 Satellites
-- Sequence-Number-Only PIT for link constellations

CREATE OR REPLACE TABLE <database>.<queryassistance_schema>.snopit_<link_name>_<cadence>
(
  dv_hashkey_lnk_<link_name>             <hashkey_type>
, <sat1_name>_dv_sid                     INT
, <sat2_name>_dv_sid                     INT
, <sat3_name>_dv_sid                     INT
, snapshotdate                           DATE
)
;
