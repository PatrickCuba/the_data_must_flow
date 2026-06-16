-- Dynamic Table SNOPIT — Link + 3 Satellites
-- LAG IGNORE NULLS forward-fill for dv_sid (ghost = 0)
-- Equi-join on snapshotdate = dv_applied_timestamp (required for INCREMENTAL)

CREATE OR REPLACE DYNAMIC TABLE <database>.<queryassistance_schema>.dt_snopit_<link_name>_<cadence>
TARGET_LAG = '<target_lag>'
WAREHOUSE = <warehouse>
REFRESH_MODE = INCREMENTAL
AS
WITH as_of AS (
  SELECT as_of
  FROM <queryassistance_schema>.as_of_date
  -- For weekly: WHERE week_lastday = 1
  -- For monthly: WHERE month_lastday = 1
)
, lnk AS (
  SELECT dv_hashkey_lnk_<link_name>
       , dv_applied_timestamp AS lnk_begin_date
  FROM <datavault_schema>.lnk_<link_name>
)
, stalk AS (
  SELECT dv_hashkey_lnk_<link_name>
       , as_of AS snapshotdate
  FROM as_of
  INNER JOIN lnk ON 1=1
)

SELECT st.*

-- sat1: dv_sid forward-fill with ghost = 0
, COALESCE(s1.dv_sid
    , LAG(s1.dv_sid) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , 0)
    AS <sat1_name>_dv_sid

-- sat2: dv_sid forward-fill
, COALESCE(s2.dv_sid
    , LAG(s2.dv_sid) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , 0)
    AS <sat2_name>_dv_sid

-- sat3: dv_sid forward-fill
, COALESCE(s3.dv_sid
    , LAG(s3.dv_sid) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , 0)
    AS <sat3_name>_dv_sid

FROM stalk st
LEFT JOIN <datavault_schema>.<sat1_name> s1
  ON st.dv_hashkey_lnk_<link_name> = s1.dv_hashkey_lnk_<link_name>
 AND st.snapshotdate = s1.dv_applied_timestamp
LEFT JOIN <datavault_schema>.<sat2_name> s2
  ON st.dv_hashkey_lnk_<link_name> = s2.dv_hashkey_lnk_<link_name>
 AND st.snapshotdate = s2.dv_applied_timestamp
LEFT JOIN <datavault_schema>.<sat3_name> s3
  ON st.dv_hashkey_lnk_<link_name> = s3.dv_hashkey_lnk_<link_name>
 AND st.snapshotdate = s3.dv_applied_timestamp
;
