-- Link INSERT (anti-semi-join, no last_seen_date)
-- Default pattern for all link types: standard, BV, same-as, hierarchical
-- Use when last_seen_date tracking is NOT required

INSERT INTO <database>.<datavault_schema>.lnk_<link_prefix>_<link_name>
(
  dv_tenantid
, dv_hashkey_lnk_<link_name>
, dv_hashkey_hub_<hub1_name>
, dv_hashkey_hub_<hub2_name>
, dv_applied_timestamp
, dv_loaddate
, dv_recsource
, dv_taskid
, dv_jiraid
)
SELECT DISTINCT
  stg.dv_tenantid
, stg.dv_hashkey_lnk_<link_name>
, stg.dv_hashkey_hub_<hub1_name>
, stg.dv_hashkey_hub_<hub2_name>
, stg.dv_applied_timestamp
, stg.dv_loaddate
, stg.dv_recsource
, '<task_id>' AS dv_taskid
, '<jira_id>' AS dv_jiraid
FROM <staged_schema>.<source_table> stg
WHERE NOT EXISTS (
  SELECT 1
  FROM <datavault_schema>.lnk_<link_prefix>_<link_name> tgt
  WHERE stg.dv_hashkey_lnk_<link_name> = tgt.dv_hashkey_lnk_<link_name>
)
;
