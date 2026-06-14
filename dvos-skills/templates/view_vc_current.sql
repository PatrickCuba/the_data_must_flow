-- DVOS Template: VC_ (Current Record) View
-- Returns exactly one row per parent hash key: the latest active record.

CREATE OR REPLACE VIEW <vault_schema>.VC_SAT_<PARENT>_<CONTEXT> AS
SELECT *
FROM <vault_schema>.SAT_<PARENT>_<CONTEXT>
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dv_hashkey_hub_<parent>
    ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
) = 1;
