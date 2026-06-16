-- Current PIT (CPIT) DDL
-- TRANSIENT, disposable — thin schema holding only current hashkey+loaddate per entity
-- Used with JoinFilter (Bloom Filter) for dynamic pruning on large satellites

CREATE OR REPLACE TRANSIENT TABLE <database>.<queryassistance_schema>.cpit_<sat_name>
(
  dv_hashkey_<parent_type>_<parent_name>    <hashkey_type>
, dv_applied_timestamp                            DATETIME
, dv_hashdiff                               <hashkey_type>

, CONSTRAINT fk_cpit_<sat_name>
    FOREIGN KEY (dv_hashkey_<parent_type>_<parent_name>, dv_applied_timestamp)
    REFERENCES <datavault_schema>.<sat_name> (dv_hashkey_<parent_type>_<parent_name>, dv_applied_timestamp) ENFORCED
)
;

-- Population: CTAS from current view
-- INSERT INTO <database>.<queryassistance_schema>.cpit_<sat_name>
-- SELECT dv_hashkey_<parent_type>_<parent_name>, dv_applied_timestamp, dv_hashdiff
-- FROM <datavault_schema>.vc_<sat_name>;

-- Pipeline maintenance: MERGE from staging
-- MERGE INTO <queryassistance_schema>.cpit_<sat_name> c
-- USING (SELECT dv_hashkey_<parent_type>_<parent_name>, dv_applied_timestamp, dv_hashdiff
--        FROM <staged_schema>.<source_table>) stg
-- ON c.dv_hashkey_<parent_type>_<parent_name> = stg.dv_hashkey_<parent_type>_<parent_name>
-- WHEN MATCHED THEN UPDATE SET c.dv_applied_timestamp = stg.dv_applied_timestamp, c.dv_hashdiff = stg.dv_hashdiff
-- WHEN NOT MATCHED THEN INSERT (dv_hashkey_<parent_type>_<parent_name>, dv_applied_timestamp, dv_hashdiff)
-- VALUES (stg.dv_hashkey_<parent_type>_<parent_name>, stg.dv_applied_timestamp, stg.dv_hashdiff);
