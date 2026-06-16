-- SNOPIT Table DDL — Hub + 3 Satellites
-- Sequence-Number-Only PIT: uses dv_sid (INT) instead of hashkey+loaddate pairs
-- Reduces network traffic between remote storage and cloud services

CREATE OR REPLACE TABLE <database>.<queryassistance_schema>.snopit_<hub_name>_<cadence>
(
  dv_hashkey_hub_<hub_name>              <hashkey_type>
, <business_key_name>                    <business_key_type>
, <sat1_name>_dv_sid                     INT
, <sat2_name>_dv_sid                     INT
, <sat3_name>_dv_sid                     INT
, snapshotdate                           DATE
)
;
