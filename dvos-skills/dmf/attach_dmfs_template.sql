/*
=============================================================================
   DVOS DMF ATTACHMENT TEMPLATE
   
   Copy and adapt per vault table. Replace:
   - <DATABASE>.<SCHEMA> → your DQ database.schema (where DMFs live)
   - <EDW_SCHEMA> → your vault schema (where DV tables live)
   - <TABLE>, <COLUMNS> → actual table and column names
   
   Schedule: TRIGGER_ON_CHANGES fires after each DML commit.
   Expectation: VALUE = 0 (zero errors = pass).
=============================================================================
*/

/* ═══════════════════════════════════════════════════════════════════════════ */
/* HUB ATTACHMENT PATTERN                                                    */
/* ═══════════════════════════════════════════════════════════════════════════ */

-- Surrogate key uniqueness
ALTER TABLE <EDW_SCHEMA>.HUB_<NAME>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_SKEY_DUPE_err
        ON (DV_HASHKEY_HUB_<NAME>)
        EXPECTATION hub_<name>_skey_no_dupes (VALUE = 0);

-- Business key uniqueness (choose 1BKEY, 2BKEY, or 3BKEY based on BK count)
ALTER TABLE <EDW_SCHEMA>.HUB_<NAME>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_1BKEY_DUPE_err
        ON (DV_TENANT_ID, DV_COLLISIONCODE, <BK_COLUMN>)
        EXPECTATION hub_<name>_bkey_no_dupes (VALUE = 0);

-- Set schedule
ALTER TABLE <EDW_SCHEMA>.HUB_<NAME>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* LINK ATTACHMENT PATTERN                                                   */
/* ═══════════════════════════════════════════════════════════════════════════ */

-- Surrogate key uniqueness
ALTER TABLE <EDW_SCHEMA>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_SKEY_DUPE_err
        ON (DV_HASHKEY_LNK_<NAME>)
        EXPECTATION lnk_<name>_skey_no_dupes (VALUE = 0);

-- FK combination uniqueness (choose 2HKEY..5HKEY based on participant count)
ALTER TABLE <EDW_SCHEMA>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_2HKEY_DUPE_err
        ON (DV_HASHKEY_HUB_<HUB_A>, DV_HASHKEY_HUB_<HUB_B>)
        EXPECTATION lnk_<name>_hkey_no_dupes (VALUE = 0);

-- Orphan check per FK (repeat for each hub participant)
ALTER TABLE <EDW_SCHEMA>.LNK_<NAME>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<HUB_A>,
            TABLE(<EDW_SCHEMA>.HUB_<HUB_A>(DV_HASHKEY_HUB_<HUB_A>)))
        EXPECTATION lnk_<name>_<hub_a>_no_orphans (VALUE = 0);

-- Set schedule
ALTER TABLE <EDW_SCHEMA>.LNK_<NAME>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* SATELLITE ATTACHMENT PATTERN (standard)                                   */
/* ═══════════════════════════════════════════════════════════════════════════ */

-- Composite key uniqueness
ALTER TABLE <EDW_SCHEMA>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_no_dupes (VALUE = 0);

-- Orphan check (excludes GHOST records)
ALTER TABLE <EDW_SCHEMA>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_SAT_SKEY_ORPH_ERR
        ON (DV_HASHKEY_HUB_<PARENT>, DV_RECORDSOURCE,
            TABLE(<EDW_SCHEMA>.HUB_<PARENT>(DV_HASHKEY_HUB_<PARENT>)))
        EXPECTATION sat_<parent>_<context>_no_orphans (VALUE = 0);

-- Set schedule
ALTER TABLE <EDW_SCHEMA>.SAT_<PARENT>_<CONTEXT>
    SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';

/* ═══════════════════════════════════════════════════════════════════════════ */
/* SATELLITE ATTACHMENT PATTERN (multi-active)                               */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <EDW_SCHEMA>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_MA_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_SEQUENCE, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_ma_no_dupes (VALUE = 0);

/* ═══════════════════════════════════════════════════════════════════════════ */
/* SATELLITE ATTACHMENT PATTERN (dependent child)                            */
/* ═══════════════════════════════════════════════════════════════════════════ */

ALTER TABLE <EDW_SCHEMA>.SAT_<PARENT>_<CONTEXT>
    ADD DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_DP_DUPE
        ON (DV_HASHKEY_HUB_<PARENT>, <DEP_KEY_COLUMN>, DV_LOAD_TIMESTAMP, DV_TENANT_ID, DV_HASHDIFF)
        EXPECTATION sat_<parent>_<context>_dp_no_dupes (VALUE = 0);
