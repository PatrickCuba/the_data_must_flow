/* =============================================================================
   DATA VAULT 2.0 QUALITY FRAMEWORK — <% database %>.DQ
   Custom Data Metric Functions (DMFs)

   All DMFs are pure measures (return NUMBER).
   Expectations (VALUE = 0) are declared at attachment time per table.
   DMF naming convention: DV_DMF_{ARTEFACT}_{CHECK}_err

   DMF inventory:
   ─────────────────────────────────────────────────────────────────────────────
   DUPLICATE CHECKS — HUB
     DV_DMF_HUB_SKEY_DUPE_err       HUB surrogate hash key uniqueness
     DV_DMF_HUB_1BKEY_DUPE_err      HUB with 1 business key (tenant + colcode + bkey1)
     DV_DMF_HUB_2BKEY_DUPE_err      HUB with 2 business keys
     DV_DMF_HUB_3BKEY_DUPE_err      HUB with 3 business keys

   DUPLICATE CHECKS — LNK
     DV_DMF_LNK_SKEY_DUPE_err       LNK own link hash key uniqueness
     DV_DMF_LNK_2HKEY_DUPE_err      LNK with 2 hub FK keys — combination uniqueness
     DV_DMF_LNK_3HKEY_DUPE_err      LNK with 3 hub FK keys — combination uniqueness
     DV_DMF_LNK_4HKEY_DUPE_err      LNK with 4 hub FK keys — combination uniqueness
     DV_DMF_LNK_5HKEY_DUPE_err      LNK with 5 hub FK keys — combination uniqueness

   DUPLICATE CHECKS — SAT  (old naming — pending rename)
     DMF_DV_SAT_DUPE                Regular SAT (skey + load_ts + tenant + hashdiff)
     DMF_DV_SAT_MA_DUPE             Multi-active SAT (+ sequence key)
     DMF_DV_SAT_DP_DUPE             Dependent child SAT (+ dependent child key)

   ORPHAN CHECKS  (full-table scan, no batch filter — safe with scheduled DMFs)
     DV_DMF_SAT_SKEY_ORPH_ERR      SAT → parent HUB or LNK, excludes GHOST records
     DV_DMF_LNK_SKEY_ORPH_ERR      LNK → parent HUB, no GHOST filter needed

   RECONCILIATION CHECKS  (staged source → target DV artefact, latest batch only)
     DMF_DV_HUB_RECON               Staged hub hash keys not found in target HUB
     DMF_DV_LNK_RECON               Staged link hash keys not found in target LNK
     DMF_DV_SAT_RECON               Staged (skey+tenant+hashdiff) vs current SAT record

   Run order:
     01_create_dq_schema_and_dmfs.sql   ← this file
     02_attach_dmfs_to_dv_tables.sql
============================================================================= */

USE ROLE IDENTIFIER('<% role %>');
USE WAREHOUSE IDENTIFIER('<% warehouse %>');

/* ─────────────────────────────────────────────────────────────────────────── */
/* SCHEMA                                                                       */
/* ─────────────────────────────────────────────────────────────────────────── */

CREATE SCHEMA IF NOT EXISTS <% database %>.DQ
    COMMENT = 'Data Vault 2.0 Quality Framework — custom DMFs and monitoring views';

USE SCHEMA <% database %>.DQ;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — HUB                                                       */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* ── HUB: surrogate hash key uniqueness ─────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where the hub surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

/* ── HUB: 1 business key (tenant_id + bkeycolcode + bkey1) ──────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_1BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1) composite is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1 HAVING COUNT(*) > 1)
$$;

/* ── HUB: 2 business keys ────────────────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_2BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1, bkey2) composite is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1, bkey2 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1, bkey2 HAVING COUNT(*) > 1)
$$;

/* ── HUB: 3 business keys ────────────────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_HUB_3BKEY_DUPE_err(
    arg_t TABLE(tenant_id VARCHAR, bkeycolcode VARCHAR, bkey1 VARCHAR, bkey2 VARCHAR, bkey3 VARCHAR))
RETURNS NUMBER
COMMENT = 'DV2 HUB: Count of rows where (tenant_id, bkeycolcode, bkey1, bkey2, bkey3) composite is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT tenant_id, bkeycolcode, bkey1, bkey2, bkey3 FROM arg_t
        GROUP BY tenant_id, bkeycolcode, bkey1, bkey2, bkey3 HAVING COUNT(*) > 1)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — LNK                                                       */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* ── LNK: own link hash key uniqueness ──────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_DUPE_err(
    arg_t TABLE(skey BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where the link surrogate hash key is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (SELECT skey FROM arg_t GROUP BY skey HAVING COUNT(*) > 1)
$$;

/* ── LNK: 2-hub FK key combination ──────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_2HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1, hkey2) hub FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2 FROM arg_t
        GROUP BY hkey1, hkey2 HAVING COUNT(*) > 1)
$$;

/* ── LNK: 3-hub FK key combination ──────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_3HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1, hkey2, hkey3) hub FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3 HAVING COUNT(*) > 1)
$$;

/* ── LNK: 4-hub FK key combination ──────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_4HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1..hkey4) hub FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3, hkey4 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3, hkey4 HAVING COUNT(*) > 1)
$$;

/* ── LNK: 5-hub FK key combination ──────────────────────────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_5HKEY_DUPE_err(
    arg_t TABLE(hkey1 BINARY, hkey2 BINARY, hkey3 BINARY, hkey4 BINARY, hkey5 BINARY))
RETURNS NUMBER
COMMENT = 'DV2 LNK: Count of rows where (hkey1..hkey5) hub FK combination is duplicated. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT hkey1, hkey2, hkey3, hkey4, hkey5 FROM arg_t
        GROUP BY hkey1, hkey2, hkey3, hkey4, hkey5 HAVING COUNT(*) > 1)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* DUPLICATE CHECKS — SAT                                                       */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* ── SAT (regular): skey + load_ts + tenant + hashdiff ──────────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 SAT: Count of rows where (skey, load_ts, tenant_id, hashdiff) composite is duplicated. Must be 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1
    )
$$;

/* ── SAT (multi-active): skey + load_ts + tenant + seq + hashdiff ────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_MA_DUPE(
    arg_t TABLE(skey BINARY, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, seq_key NUMBER, hashdiff BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 SAT (multi-active): Count of rows where (skey, load_ts, tenant_id, seq_key, hashdiff) composite is duplicated. Must be 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, load_ts, tenant_id, seq_key, hashdiff FROM arg_t
        GROUP BY skey, load_ts, tenant_id, seq_key, hashdiff HAVING COUNT(*) > 1
    )
$$;

/* ── SAT (dependent child): skey + dep_key + load_ts + tenant + hashdiff ── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_DP_DUPE(
    arg_t TABLE(skey BINARY, dep_key VARCHAR, load_ts TIMESTAMP_NTZ, tenant_id VARCHAR, hashdiff BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 SAT (dependent child): Count of rows where (skey, dep_key, load_ts, tenant_id, hashdiff) composite is duplicated. Must be 0.'
AS $$
    SELECT COUNT(*) FROM (
        SELECT skey, dep_key, load_ts, tenant_id, hashdiff FROM arg_t
        GROUP BY skey, dep_key, load_ts, tenant_id, hashdiff HAVING COUNT(*) > 1
    )
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* ORPHAN CHECKS  (full-table scan — no batch filter, safe with scheduling)    */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* ── SAT orphan → parent HUB or LNK (excludes GHOST records) ────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_SAT_SKEY_ORPH_ERR(
    arg_sat    TABLE(fk_col BINARY, rec_source VARCHAR),
    arg_parent TABLE(pk_col BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 SAT ORPHAN: Count of SAT FK keys (excl GHOST) not found in parent HUB/LNK. Checks all rows. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_sat s
    WHERE s.rec_source <> 'GHOST'
      AND NOT EXISTS (SELECT 1 FROM arg_parent p WHERE p.pk_col = s.fk_col)
$$;

/* ── LNK orphan → parent HUB (no GHOST filter — links don't carry GHOST) ── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DV_DMF_LNK_SKEY_ORPH_ERR(
    arg_lnk TABLE(fk_col BINARY),
    arg_hub TABLE(pk_col BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 LNK ORPHAN: Count of LNK FK keys not found in parent HUB. Checks all rows. Expectation: 0.'
AS $$
    SELECT COUNT(*) FROM arg_lnk l
    WHERE NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = l.fk_col)
$$;

/* ═══════════════════════════════════════════════════════════════════════════ */
/* RECONCILIATION CHECKS  (staged source → target DV artefact)                 */
/* Latest batch from staging via MAX load_ts for partition pruning.             */
/* ═══════════════════════════════════════════════════════════════════════════ */

/* ── HUB recon: staged hub hash keys not found in target HUB ────────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_HUB_RECON(
    arg_stg TABLE(skey BINARY, load_ts TIMESTAMP_NTZ),
    arg_hub TABLE(pk_col BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 RECON HUB: Count of latest-batch staged hub hash keys not present in the target HUB table. Must be 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (SELECT 1 FROM arg_hub h WHERE h.pk_col = s.skey)
$$;

/* ── LNK recon: staged link hash keys not found in target LNK ───────────── */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_LNK_RECON(
    arg_stg TABLE(skey BINARY, load_ts TIMESTAMP_NTZ),
    arg_lnk TABLE(pk_col BINARY)
)
RETURNS NUMBER
COMMENT = 'DV2 RECON LNK: Count of latest-batch staged link hash keys not present in the target LNK table. Must be 0.'
AS $$
    SELECT COUNT(*) FROM arg_stg s
    WHERE s.load_ts = (SELECT MAX(load_ts) FROM arg_stg)
      AND NOT EXISTS (SELECT 1 FROM arg_lnk l WHERE l.pk_col = s.skey)
$$;

/* ── SAT recon: staged (skey+tenant+hashdiff) vs current record in SAT ───── */
/*   "Current" = latest record per (skey, tenant_id) ordered by              */
/*    applied_ts DESC, load_ts DESC — matching the vc_* view pattern.         */
CREATE OR REPLACE DATA METRIC FUNCTION <% database %>.DQ.DMF_DV_SAT_RECON(
    arg_stg TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY, load_ts TIMESTAMP_NTZ),
    arg_sat TABLE(skey BINARY, tenant_id VARCHAR, hashdiff BINARY,
                  applied_ts TIMESTAMP_NTZ, load_ts TIMESTAMP_NTZ)
)
RETURNS NUMBER
COMMENT = 'DV2 RECON SAT: Count of latest-batch staged (skey, tenant, hashdiff) combinations not present in the current (latest) record of the target SAT. Must be 0.'
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
/* VERIFY                                                                       */
/* ─────────────────────────────────────────────────────────────────────────── */

SHOW DATA METRIC FUNCTIONS IN SCHEMA <% database %>.DQ;
