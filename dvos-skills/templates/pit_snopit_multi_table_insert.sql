-- Conditional Multi-Table INSERT — PIT + SNOPIT (Daily/Weekly/Monthly)
-- Single SELECT produces all columns, WHEN clauses route via ASOF flags
-- Pattern: INSERT OVERWRITE ALL for idempotent rebuild

INSERT OVERWRITE ALL

-- WEEKLY PIT (only if week_lastday flag and not already loaded)
WHEN (aof_week_lastday = 1)
AND (SELECT COUNT(1) FROM <queryassistance_schema>.pit_<hub_name>_weekly tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.pit_<hub_name>_weekly
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
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_hashkey_hub_<hub_name>
, <sat1_name>_dv_applied_timestamp
, <sat2_name>_dv_hashkey_hub_<hub_name>
, <sat2_name>_dv_applied_timestamp
, <sat3_name>_dv_hashkey_hub_<hub_name>
, <sat3_name>_dv_applied_timestamp
, src_snapshotdate
)

-- WEEKLY SNOPIT
WHEN (aof_week_lastday = 1)
AND (SELECT COUNT(1) FROM <queryassistance_schema>.snopit_<hub_name>_weekly tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.snopit_<hub_name>_weekly
(
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, snapshotdate
)
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, src_snapshotdate
)

-- MONTHLY PIT (only if month_lastday flag and not already loaded)
WHEN (aof_month_lastday = 1)
AND (SELECT COUNT(1) FROM <queryassistance_schema>.pit_<hub_name>_monthly tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.pit_<hub_name>_monthly
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
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_hashkey_hub_<hub_name>
, <sat1_name>_dv_applied_timestamp
, <sat2_name>_dv_hashkey_hub_<hub_name>
, <sat2_name>_dv_applied_timestamp
, <sat3_name>_dv_hashkey_hub_<hub_name>
, <sat3_name>_dv_applied_timestamp
, src_snapshotdate
)

-- MONTHLY SNOPIT
WHEN (aof_month_lastday = 1)
AND (SELECT COUNT(1) FROM <queryassistance_schema>.snopit_<hub_name>_monthly tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.snopit_<hub_name>_monthly
(
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, snapshotdate
)
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, src_snapshotdate
)

-- DAILY PIT (all dates, no flag needed)
WHEN (SELECT COUNT(1) FROM <queryassistance_schema>.pit_<hub_name>_daily tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.pit_<hub_name>_daily
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
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_hashkey_hub_<hub_name>
, <sat1_name>_dv_applied_timestamp
, <sat2_name>_dv_hashkey_hub_<hub_name>
, <sat2_name>_dv_applied_timestamp
, <sat3_name>_dv_hashkey_hub_<hub_name>
, <sat3_name>_dv_applied_timestamp
, src_snapshotdate
)

-- DAILY SNOPIT
WHEN (SELECT COUNT(1) FROM <queryassistance_schema>.snopit_<hub_name>_daily tgt WHERE tgt.snapshotdate = src_snapshotdate) = 0 THEN
INTO <queryassistance_schema>.snopit_<hub_name>_daily
(
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, snapshotdate
)
VALUES (
  dv_hashkey_hub_<hub_name>
, <business_key_name>
, <sat1_name>_dv_sid
, <sat2_name>_dv_sid
, <sat3_name>_dv_sid
, src_snapshotdate
)

-- SOURCE: Hub CROSS JOIN ASOF with correlated subquery per satellite
WITH as_of_date AS (
  SELECT as_of, month_lastday, week_lastday, week_firstday
  FROM <queryassistance_schema>.as_of_date
)
SELECT hub.dv_hashkey_hub_<hub_name>
     , hub.<business_key_name>

     -- PIT columns: correlated subquery MAX(dv_applied_timestamp) WHERE dv_applied_timestamp <= as_of
     , COALESCE(s1.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat1_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s1.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00')) AS <sat1_name>_dv_applied_timestamp
     , COALESCE(s2.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat2_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s2.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00')) AS <sat2_name>_dv_applied_timestamp
     , COALESCE(s3.dv_hashkey_hub_<hub_name>, TO_BINARY(REPEAT(0, 20))) AS <sat3_name>_dv_hashkey_hub_<hub_name>
     , COALESCE(s3.dv_applied_timestamp, TO_TIMESTAMP('1900-01-01 00:00:00')) AS <sat3_name>_dv_applied_timestamp

     -- SNOPIT columns: ghost dv_sid = 0
     , COALESCE(s1.dv_sid, 0) AS <sat1_name>_dv_sid
     , COALESCE(s2.dv_sid, 0) AS <sat2_name>_dv_sid
     , COALESCE(s3.dv_sid, 0) AS <sat3_name>_dv_sid

     -- Routing columns
     , aof.as_of             AS src_snapshotdate
     , aof.month_lastday     AS aof_month_lastday
     , aof.week_lastday      AS aof_week_lastday
     , aof.week_firstday     AS aof_week_firstday

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
