-- Information Mart View — consuming PIT (hashkey + applieddate equi-join)
-- Right-Deep Join Tree pattern: PIT drives all satellite lookups

CREATE OR REPLACE VIEW <database>.<im_schema>.<mart_name>_pit AS
SELECT pit.snapshotdate
     , pit.dv_hashkey_<parent_type>_<parent_name>
     , pit.<business_key_name>
     , s1.<col1_from_sat1>
     , s1.<col2_from_sat1>
     , s2.<col1_from_sat2>
     , s2.<col2_from_sat2>
     , s3.<col1_from_sat3>
     , s3.<col2_from_sat3>
FROM <queryassistance_schema>.pit_<parent_name>_<cadence> pit
INNER JOIN <datavault_schema>.<sat1_name> s1
  ON pit.<sat1_name>_dv_hashkey_<parent_type>_<parent_name> = s1.dv_hashkey_<parent_type>_<parent_name>
 AND pit.<sat1_name>_dv_applied_timestamp = s1.dv_applied_timestamp
INNER JOIN <datavault_schema>.<sat2_name> s2
  ON pit.<sat2_name>_dv_hashkey_<parent_type>_<parent_name> = s2.dv_hashkey_<parent_type>_<parent_name>
 AND pit.<sat2_name>_dv_applied_timestamp = s2.dv_applied_timestamp
INNER JOIN <datavault_schema>.<sat3_name> s3
  ON pit.<sat3_name>_dv_hashkey_<parent_type>_<parent_name> = s3.dv_hashkey_<parent_type>_<parent_name>
 AND pit.<sat3_name>_dv_applied_timestamp = s3.dv_applied_timestamp
;
