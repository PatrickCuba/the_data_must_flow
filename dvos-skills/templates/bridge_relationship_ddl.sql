-- Bridge Table DDL + INSERT
-- Traverses relationships (link) and pre-joins surrounding satellites
-- Includes metrics and SNOPIT-style dv_sid references for star-schema querying

-- DDL
CREATE OR REPLACE TABLE <database>.<queryassistance_schema>.brdg_<bridge_name>_<cadence>
(
  dv_hashkey_hub_<hub1_name>              <hashkey_type>
, <hub1_business_key>                     <hub1_bkey_type>
, dv_hashkey_hub_<hub2_name>              <hashkey_type>
, <hub2_business_key>                     <hub2_bkey_type>
, <sat_on_hub1>_dv_sid                    INT
, <sat_on_link>_dv_sid                    INT
, <sat_on_hub2>_dv_sid                    INT
, date_sid                                INT

-- metrics
, <metric1_name>                          <metric1_type>
, <metric2_name>                          <metric2_type>
, <running_total_name>                    <running_total_type>

-- constraints
, CONSTRAINT fk_brdg_<bridge_name>_hub_<hub1_name>
    FOREIGN KEY (dv_hashkey_hub_<hub1_name>)
    REFERENCES <datavault_schema>.hub_<hub1_name> (dv_hashkey_hub_<hub1_name>) ENFORCED
, CONSTRAINT fk_brdg_<bridge_name>_hub_<hub2_name>
    FOREIGN KEY (dv_hashkey_hub_<hub2_name>)
    REFERENCES <datavault_schema>.hub_<hub2_name> (dv_hashkey_hub_<hub2_name>) ENFORCED
)
;

-- INSERT (idempotent rebuild)
INSERT OVERWRITE INTO <queryassistance_schema>.brdg_<bridge_name>_<cadence>

WITH <sat_on_link> AS (
  SELECT dv_hashkey_lnk_<link_name>
       , dv_loaddate
       , dv_applied_timestamp
       , dv_sid
       , <metric1_name>
       , <metric2_name>
       , SUM(<metric1_name>) OVER (PARTITION BY dv_hashkey_lnk_<link_name>
           ORDER BY dv_loaddate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS <running_total_name>
       , COALESCE(LEAD(dv_applied_timestamp) OVER (PARTITION BY dv_hashkey_lnk_<link_name>
           ORDER BY dv_applied_timestamp), CAST('9999-12-31' AS DATE)) AS dv_applied_timestamp_end
  FROM <datavault_schema>.<sat_on_link>
)
, <sat_on_hub2> AS (
  SELECT dv_hashkey_hub_<hub2_name>
       , dv_loaddate
       , dv_applied_timestamp
       , dv_sid
       , COALESCE(LEAD(dv_applied_timestamp) OVER (PARTITION BY dv_hashkey_hub_<hub2_name>
           ORDER BY dv_applied_timestamp), CAST('9999-12-31' AS DATE)) AS dv_applied_timestamp_end
  FROM <datavault_schema>.<sat_on_hub2>
)
, <sat_on_hub1> AS (
  SELECT dv_hashkey_hub_<hub1_name>
       , dv_loaddate
       , dv_applied_timestamp
       , dv_sid
       , COALESCE(LEAD(dv_applied_timestamp) OVER (PARTITION BY dv_hashkey_hub_<hub1_name>
           ORDER BY dv_applied_timestamp), CAST('9999-12-31' AS DATE)) AS dv_applied_timestamp_end
  FROM <datavault_schema>.<sat_on_hub1>
)

SELECT lnk.dv_hashkey_hub_<hub1_name>
     , hub1.<hub1_business_key>
     , lnk.dv_hashkey_hub_<hub2_name>
     , hub2.<hub2_business_key>
     , COALESCE(sat_h1.dv_sid, 0)   AS <sat_on_hub1>_dv_sid
     , COALESCE(sat_lnk.dv_sid, 0)  AS <sat_on_link>_dv_sid
     , COALESCE(sat_h2.dv_sid, 0)   AS <sat_on_hub2>_dv_sid
     , YEAR(sat_lnk.dv_loaddate) * 10000 + MONTH(sat_lnk.dv_loaddate) * 100 + DAY(sat_lnk.dv_loaddate) AS date_sid
     , sat_lnk.<metric1_name>
     , sat_lnk.<metric2_name>
     , sat_lnk.<running_total_name>

FROM <datavault_schema>.lnk_<link_name> lnk
INNER JOIN <datavault_schema>.hub_<hub1_name> hub1
  ON lnk.dv_hashkey_hub_<hub1_name> = hub1.dv_hashkey_hub_<hub1_name>
INNER JOIN <datavault_schema>.hub_<hub2_name> hub2
  ON lnk.dv_hashkey_hub_<hub2_name> = hub2.dv_hashkey_hub_<hub2_name>

-- chief table (link satellite drives the grain)
INNER JOIN <sat_on_link> sat_lnk
  ON lnk.dv_hashkey_lnk_<link_name> = sat_lnk.dv_hashkey_lnk_<link_name>

LEFT JOIN <sat_on_hub2> sat_h2
  ON lnk.dv_hashkey_hub_<hub2_name> = sat_h2.dv_hashkey_hub_<hub2_name>
 AND sat_h2.dv_applied_timestamp <= sat_lnk.dv_applied_timestamp
 AND sat_h2.dv_applied_timestamp_end >= sat_lnk.dv_applied_timestamp_end

LEFT JOIN <sat_on_hub1> sat_h1
  ON lnk.dv_hashkey_hub_<hub1_name> = sat_h1.dv_hashkey_hub_<hub1_name>
 AND sat_h1.dv_applied_timestamp <= sat_lnk.dv_applied_timestamp
 AND sat_h1.dv_applied_timestamp_end >= sat_lnk.dv_applied_timestamp_end
;
