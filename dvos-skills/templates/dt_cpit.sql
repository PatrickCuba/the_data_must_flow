-- Dynamic Table Current PIT (CPIT)
-- Returns only the latest hashkey+applieddate per entity
-- QUALIFY ROW_NUMBER pattern — thin schema for JoinFilter pruning

CREATE OR REPLACE DYNAMIC TABLE <database>.<queryassistance_schema>.dt_cpit_<sat_name>
TARGET_LAG = '<target_lag>'
WAREHOUSE = <warehouse>
REFRESH_MODE = INCREMENTAL
AS
SELECT dv_hashkey_<parent_type>_<parent_name>
     , dv_applied_timestamp
     , dv_hashdiff
FROM <datavault_schema>.<sat_name>
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY dv_hashkey_<parent_type>_<parent_name>
  ORDER BY dv_applied_timestamp DESC, dv_loaddate DESC
) = 1
;
