-- DVOS Template: Hashkey Computation (Staging View)
-- Replace <bkcc>, <bk_col>, <hub_name> with actual values.

-- Hub hash key (single business key):
SHA1_BINARY(UPPER(CONCAT(
    '<bkcc>' || '||' || COALESCE(NULLIF(TRIM(CAST(<bk_col> AS STRING)), ''), '-1')
))) AS dv_hashkey_hub_<hub_name>

-- Link hash key (2 participants — from business keys, NEVER from hub hashkeys):
SHA1_BINARY(UPPER(CONCAT(
    '<bkcc_a>' || '||' || COALESCE(NULLIF(TRIM(CAST(<bk_col_a> AS STRING)), ''), '-1') || '||' ||
    '<bkcc_b>' || '||' || COALESCE(NULLIF(TRIM(CAST(<bk_col_b> AS STRING)), ''), '-1')
))) AS dv_hashkey_lnk_<link_name>

-- Hashdiff (no UPPER, no tenant/bkcc, empty string for nulls):
SHA1_BINARY(CONCAT(
    COALESCE(TRIM(CAST(<attr1> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr2> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr3> AS STRING)), '')
)) AS dv_hashdiff_sat_<sat_name>
