-- Information Mart View — consuming SNOPIT (dv_sid equi-join)
-- Single-column join per satellite — reduced network I/O vs PIT

CREATE OR REPLACE VIEW <database>.<im_schema>.<mart_name>_snopit AS
SELECT pit.snapshotdate
     , pit.dv_hashkey_<parent_type>_<parent_name>
     , pit.<business_key_name>
     , s1.<col1_from_sat1>
     , s1.<col2_from_sat1>
     , s2.<col1_from_sat2>
     , s2.<col2_from_sat2>
     , s3.<col1_from_sat3>
     , s3.<col2_from_sat3>
FROM <queryassistance_schema>.snopit_<parent_name>_<cadence> pit
INNER JOIN <datavault_schema>.<sat1_name> s1
  ON pit.<sat1_name>_dv_sid = s1.dv_sid
INNER JOIN <datavault_schema>.<sat2_name> s2
  ON pit.<sat2_name>_dv_sid = s2.dv_sid
INNER JOIN <datavault_schema>.<sat3_name> s3
  ON pit.<sat3_name>_dv_sid = s3.dv_sid
;
