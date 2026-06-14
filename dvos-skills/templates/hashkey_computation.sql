-- DVOS Template: Hashkey Computation (Staging View)
-- Replace <bkcc>, <bk_col>, <hub_name>, <tenant_id_value> with actual values.
--
-- MULTI-TENANCY NOTE:
-- When manifest has tenant.enabled: true, dv_tenant_id IS included in the hash.
-- When manifest has tenant.enabled: false, dv_tenant_id is OMITTED from the hash.
-- Default values: dv_tenant_id = 'default', dv_collisioncode = 'default'
-- Per-source overrides: set bkcc_value or tenant_id_value per source in the manifest.

-- Hub hash key (multi-tenancy ENABLED — includes tenant_id):
SHA1_BINARY(UPPER(CONCAT(
    '<tenant_id_value>' || '||' ||
    '<bkcc>' || '||' ||
    COALESCE(NULLIF(TRIM(CAST(<bk_col> AS STRING)), ''), '-1')
))) AS dv_hashkey_hub_<hub_name>

-- Hub hash key (multi-tenancy DISABLED — bkcc only):
SHA1_BINARY(UPPER(CONCAT(
    '<bkcc>' || '||' ||
    COALESCE(NULLIF(TRIM(CAST(<bk_col> AS STRING)), ''), '-1')
))) AS dv_hashkey_hub_<hub_name>

-- Link hash key (multi-tenancy ENABLED — 2 participants):
SHA1_BINARY(UPPER(CONCAT(
    '<tenant_id_value_a>' || '||' || '<bkcc_a>' || '||' || COALESCE(NULLIF(TRIM(CAST(<bk_col_a> AS STRING)), ''), '-1') || '||' ||
    '<tenant_id_value_b>' || '||' || '<bkcc_b>' || '||' || COALESCE(NULLIF(TRIM(CAST(<bk_col_b> AS STRING)), ''), '-1')
))) AS dv_hashkey_lnk_<link_name>

-- Hashdiff (no UPPER, no tenant_id/bkcc, empty string for nulls):
SHA1_BINARY(CONCAT(
    COALESCE(TRIM(CAST(<attr1> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr2> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr3> AS STRING)), '')
)) AS dv_hashdiff_sat_<sat_name>
