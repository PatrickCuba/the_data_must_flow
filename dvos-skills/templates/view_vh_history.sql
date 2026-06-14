-- DVOS Template: VH_ (History) View
-- Returns all records with computed end-date and current flag.

CREATE OR REPLACE VIEW <vault_schema>.VH_SAT_<PARENT>_<CONTEXT> AS
SELECT
    *,
    COALESCE(
        LEAD(dv_applied_timestamp) OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp, dv_load_timestamp
        ),
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ
    ) AS dv_applied_timestamp_end,
    CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
        ) = 1 THEN TRUE
        ELSE FALSE
    END AS dv_currentflag
FROM <vault_schema>.SAT_<PARENT>_<CONTEXT>;
