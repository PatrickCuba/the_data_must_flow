-- Dynamic Table PIT — Link + 3 Satellites
-- LAG IGNORE NULLS forward-fill pattern for INCREMENTAL refresh
-- Equi-join on snapshotdate = dv_applied_timestamp (required for INCREMENTAL)

CREATE OR REPLACE DYNAMIC TABLE <database>.<queryassistance_schema>.dt_pit_<link_name>_<cadence>
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

-- sat1: hashkey forward-fill with ghost fallback
, COALESCE(s1.dv_hashkey_lnk_<link_name>
    , LAG(s1.dv_hashkey_lnk_<link_name>) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_BINARY(REPEAT(0, 20)))
    AS <sat1_name>_dv_hashkey_lnk_<link_name>

-- sat2: hashkey forward-fill
, COALESCE(s2.dv_hashkey_lnk_<link_name>
    , LAG(s2.dv_hashkey_lnk_<link_name>) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_BINARY(REPEAT(0, 20)))
    AS <sat2_name>_dv_hashkey_lnk_<link_name>

-- sat3: hashkey forward-fill
, COALESCE(s3.dv_hashkey_lnk_<link_name>
    , LAG(s3.dv_hashkey_lnk_<link_name>) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_BINARY(REPEAT(0, 20)))
    AS <sat3_name>_dv_hashkey_lnk_<link_name>

-- sat1: applieddate forward-fill with ghost date
, COALESCE(s1.dv_applied_timestamp
    , LAG(s1.dv_applied_timestamp) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_TIMESTAMP('1900-01-01 00:00:00'))
    AS <sat1_name>_dv_applied_timestamp

-- sat2: applieddate forward-fill
, COALESCE(s2.dv_applied_timestamp
    , LAG(s2.dv_applied_timestamp) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_TIMESTAMP('1900-01-01 00:00:00'))
    AS <sat2_name>_dv_applied_timestamp

-- sat3: applieddate forward-fill
, COALESCE(s3.dv_applied_timestamp
    , LAG(s3.dv_applied_timestamp) IGNORE NULLS OVER
      (PARTITION BY st.dv_hashkey_lnk_<link_name> ORDER BY st.snapshotdate)
    , TO_TIMESTAMP('1900-01-01 00:00:00'))
    AS <sat3_name>_dv_applied_timestamp

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
