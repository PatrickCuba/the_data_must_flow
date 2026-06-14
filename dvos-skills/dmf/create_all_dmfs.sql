/*
=============================================================================
   DVOS DATA VAULT 2.0 QUALITY FRAMEWORK — 17 Custom Data Metric Functions
   
   Deploy this file to create all DMFs in one schema.
   Replace <DATABASE> and <SCHEMA> with your DQ target (e.g. DV_DQ.DQ).
   
   All DMFs return NUMBER. Expectation at attachment: VALUE = 0.
   Schedule recommendation: TRIGGER_ON_CHANGES per table.
   
   Reference: github.com/PatrickCuba/the_data_must_flow/data-auto-testing/snowflake_dmf
=============================================================================
*/

CREATE SCHEMA IF NOT EXISTS <DATABASE>.<SCHEMA>
    COMMENT = 'Data Vault 2.0 Quality Framework — custom DMFs and monitoring views';

USE SCHEMA <DATABASE>.<SCHEMA>;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — HUB                                                    */
/* ═══════════════════════════════════════════════════════════════════════════ */

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where the hub surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_1BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1 HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_2BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1, bkey2) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1, bkey2 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1, bkey2 HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_HUB_3BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR, bkey3 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1, bkey2, bkey3) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1, bkey2, bkey3 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1, bkey2, bkey3 HAVING COUNT(*) > 1)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — LNK                                                    */
/* ═══════════════════════════════════════════════════════════════════════════ */

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where the link surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_2HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1, hkey2) FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2 FROM arg_t
        GROUP BY hkey1, hkey2 HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_3HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1, hkey2, hkey3) FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3 HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_4HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1..hkey4) FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3, hkey4 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3, hkey4 HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_5HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY, hkey5 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1..hkey5) FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3, hkey4, hkey5 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3, hkey4, hkey5 HAVING COUNT(*) > 1)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — SAT                                                    */
/* ═══════════════════════════════════════════════════════════════════════════ */

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT: Count of rows where (skey, load_ts, tenant_id, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_MA_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, seq_key NUMBER, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT (multi-active): Count of rows where (skey, load_ts, tenant_id, seq_key, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, seq_key, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, seq_key, hashdiff HAVING COUNT(*) > 1)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_DP_DUPE(
    arg_t TABLE(skey BINARY, dep_key VARCHAR, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT (dependent child): Count of rows where (skey, dep_key, load_ts, tenant_id, hashdiff) is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, dep_key, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, dep_key, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* ORPHAN CHECKS                                                             */
/* ═══════════════════════════════════════════════════════════════════════════ */

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_SAT_SKEY_ORPH_ERR(
    arg_sat    TABLE(fk_col BINARY, rec_source VARCHAR),
    arg_parent TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 SAT ORPHAN: Count of SAT FK keys (excl GHOST) not in parent HUB/LNK. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_sat s
    WHERE s.rec_source <> 'GHOST'
      AND NOT EXISTS (SELECT 1 FROM arg_parent p WHERE p.pk_col = s.fk_col)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DV_DMF_LNK_SKEY_ORPH_ERR(
    arg_lnk TABLE(fk_col BINARY),
    arg_hub TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK ORPHAN: Count of LNK FK keys not in parent HUB. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_lnk l
    WHERE NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = l.fk_col)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* RECONCILIATION CHECKS                                                     */
/* ═══════════════════════════════════════════════════════════════════════════ */

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_HUB_RECON(
    arg_stg TABLE(skey BINARY, load_ts TIMESTAMP_NTZ),
    arg_hub TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 RECON HUB: Count of latest-batch staged hub keys not in target HUB. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = s.skey)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_LNK_RECON(
    arg_stg TABLE(skey BINARY, load_ts TIMESTAMP_NTZ),
    arg_lnk TABLE(pk_col BINARY))
RETURNS NUMBER
COMMENT = 'DV2 RECON LNK: Count of latest-batch staged link keys not in target LNK. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (SELECT 1 FROM arg_lnk l WHERE l.pk_col = s.skey)
$$;

CREATE OR REPLACE DATA METRIC FUNCTION <DATABASE>.<SCHEMA>.DMF_DV_SAT_RECON(
    arg_stg TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, load_ts TIMESTAMP_NTZ),
    arg_sat TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, applied_ts TIMESTAMP_NTZ, load_ts TIMESTAMP_NTZ))
RETURNS NUMBER
COMMENT = 'DV2 RECON SAT: Count of latest-batch staged records not matching current SAT record. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (
          SELECT 1 FROM (
              SELECT skey, tenant_id, hashdiff
              FROM arg_sat
              QUALIFY ROW_NUMBER() OVER (
                  PARTITION BY skey, tenant_id
                  ORDER BY applied_ts DESC, load_ts DESC
              ) = 1
          ) curr
          WHERE curr.skey      = s.skey
            AND curr.tenant_id = s.tenant_id
            AND curr.hashdiff  = s.hashdiff
      )
$$;

/* ─────────────────────────────────────────────────────────────────────────── */
/* VERIFY                                                                      */
/* ─────────────────────────────────────────────────────────────────────────── */

SHOW DATA METRIC FUNCTIONS IN SCHEMA <DATABASE>.<SCHEMA>;
