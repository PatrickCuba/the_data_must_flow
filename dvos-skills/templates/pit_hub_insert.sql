-- Standalone PIT INSERT (Hub)
-- Populates a single PIT table using correlated subquery MAX(dv_applied_timestamp) WHERE dv_applied_timestamp <= as_of
-- Use when PITs are loaded independently (not via multi-table INSERT)

INSERT OVERWRITE INTO <database>.<queryassistance_schema>.pit_<hub_name>_<cadence>
(
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_hashkey_hub_<hub_name>
, <sat1_name>_dv_applied_timestamp
, <sat2_name>_dv_hashkey_hub_<hub_name>
, <sat2_name>_dv_applied_timestamp
, <sat3_name>_dv_hashkey_hub_<hub_name>
, <sat3_name>_dv_applied_timestamp
, snapshotdate
)

WITH as_of_date AS (
  SELECT as_of
  FROM <queryassistance_schema>.as_of_date
  -- For weekly: WHERE week_lastday = 1
  -- For monthly: WHERE month_lastday = 1
)

SELECT hub.dv_hashkey_hub_<hub_name>
     , hub.<business_key_name>

     , COALESCE(s1.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat1_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s1.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))  AS <sat1_name>_dv_applied_timestamp

     , COALESCE(s2.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat2_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s2.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))  AS <sat2_name>_dv_applied_timestamp

     , COALESCE(s3.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat3_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s3.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00'))  AS <sat3_name>_dv_applied_timestamp

     , aof.as_of AS snapshotdate

FROM <datavault_schema>.hub_<hub_name> hub
INNER JOIN as_of_date aof ON (1=1)

LEFT JOIN <datavault_schema>.<sat1_name> s1
  ON (hub.dv_hashkey_hub_<hub_name> = s1.dv_hashkey_hub_<hub_name>)
 AND s1.dv_applied_timestamp = (SELECT MAX(dv_applied_timestamp)
                          FROM <datavault_schema>.<sat1_name> sub
                          WHERE sub.dv_hashkey_hub_<hub_name> = hub.dv_hashkey_hub_<hub_name>
                            AND sub.dv_applied_timestamp <= aof.as_of)

LEFT JOIN <datavault_schema>.<sat2_name> s2
  ON (hub.dv_hashkey_hub_<hub_name> = s2.dv_hashkey_hub_<hub_name>)
 AND s2.dv_applied_timestamp = (SELECT MAX(dv_applied_timestamp)
                          FROM <datavault_schema>.<sat2_name> sub
                          WHERE sub.dv_hashkey_hub_<hub_name> = hub.dv_hashkey_hub_<hub_name>
                            AND sub.dv_applied_timestamp <= aof.as_of)

LEFT JOIN <datavault_schema>.<sat3_name> s3
  ON (hub.dv_hashkey_hub_<hub_name> = s3.dv_hashkey_hub_<hub_name>)
 AND s3.dv_applied_timestamp = (SELECT MAX(dv_applied_timestamp)
                          FROM <datavault_schema>.<sat3_name> sub
                          WHERE sub.dv_hashkey_hub_<hub_name> = hub.dv_hashkey_hub_<hub_name>
                            AND sub.dv_applied_timestamp <= aof.as_of)
;
