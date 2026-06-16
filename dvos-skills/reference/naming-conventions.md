# DVOS Naming Conventions — Quick Reference

## Step 0: Define source badges before naming anything

A **source badge** is a short lowercase identifier for the source system. It is a mandatory component of every Raw Vault satellite name and staging view name. Define all source badges before starting vault modeling.

**Format:** `[a-z0-9_]+` — lowercase alphanumeric + underscores. No uppercase, no hyphens.

| Badge | Source system |
|---|---|
| `sapbw` | SAP BW (all SAP BW tables) |
| `mdm` | Master Data Management |
| `zoho` | Zoho HR system |
| `xero` | Xero accounting |
| `cards` | Card operations system |
| `bv` | Business Vault (implicit — used for all BV-derived constructs) |

The badge is the **system**, not the table. `SAPBW_COMM_CUSTOMER` and `SAPBW_RETAIL_CUSTOMER` both have badge `sapbw`.

---

## Object Prefixes

| Object | Prefix | Example |
|---|---|---|
| Hub | `HUB_` | `HUB_PARTY` |
| RV Standard Link | `LNK_RV_` | `LNK_RV_CUSTOMER_ACCOUNT_PRODUCT` |
| RV Same-As Link | `LNK_RV_SA_` | `LNK_RV_SA_CUSTOMER_MATCH` |
| RV Hierarchical Link | `LNK_RV_HY_` | `LNK_RV_HY_EMPLOYEE_MANAGER` |
| RV Non-Historized Link | `LNK_NH_RV_` | `LNK_NH_RV_EMPLOYEE_ACCOUNT` |
| BV Standard Link | `LNK_BV_` | `LNK_BV_CARD_ACCOUNT_ASSIGNMENT` |
| BV Same-As Link | `LNK_BV_SA_` | `LNK_BV_SA_CUSTOMER_GOLDEN` |
| PIT table | `PIT_` | `PIT_PARTY` |
| Bridge table | `BDG_` | `BDG_PARTYACCOUNT_DAILY` |
| Current view | `VC_` | `VC_SAT_RV_HUB_SAPBW_COMM_CUSTOMER` |
| History view | `VH_` | `VH_SAT_RV_HUB_SAPBW_COMM_CUSTOMER` |
| IM dimension | `DIM_` | `DIM_CUSTOMER` |
| IM fact | `FACT_` | `FACT_ORDER` |
| IM audit | `AUDIT_` | `AUDIT_CUSTOMER` |
| Activity Schema per-activity DT | `dt_{entity}_stream_{activity}` | `dt_customer_stream_debit_account` |
| Activity Schema enriched DT | `dt_{entity}_stream_enriched` | `dt_customer_stream_enriched` |
| Supernova hub versions DT | `dt_{hub}_versions` | `dt_hub_account_versions` |
| Supernova link versions DT | `dt_{link}_versions` | `dt_lnk_account_customer_versions` |
| Supernova hub DT | `dt_supernova_{hub}` | `dt_supernova_hub_account` |
| Supernova link DT | `dt_supernova_{link}` | `dt_supernova_lnk_account_customer` |
| Extended Supernova DT | `dt_xsn_supernova_{hub_or_link}` | `dt_xsn_supernova_hub_account` |
| Source staging | `STG_` | `STG_SAPBW_COMM_CUSTOMER` |

## Prohibited Prefixes

| Wrong | Correct |
|---|---|
| `BRDG_` | `BDG_` |
| `BRIDGE_` | `BDG_` |
| `LNK_<name>` (no vault layer) | `LNK_RV_<name>` or `LNK_BV_<name>` |
| `SAT_<PARENT>_<CONTEXT>` (generic, no RV/BV) | `SAT_RV_<hub\|lnk>_{badge}_{file}` or `SAT_BV_{concept}` |
| `SAL_<entity>` | `LNK_RV_SA_<name>` (same-as links use link prefix) |
| `SAT_BV_HUB_<parent>_<concept>` | `SAT_BV_<concept>` (no parent_type in BV sats) |

---

## Raw Vault Satellite Patterns

Source badge (`{badge}`) and source file (`{file}`) are mandatory in every RV satellite name.

| Type | Pattern | Example |
|---|---|---|
| Standard hub sat | `SAT_RV_HUB_{badge}_{file}` | `SAT_RV_HUB_SAPBW_COMM_CUSTOMER` |
| Multi-active hub sat | `SAT_MA_RV_HUB_{badge}_{file}` | `SAT_MA_RV_HUB_XERO_ACCOUNT_DETAILS` |
| Dependent-child hub sat | `SAT_DP_RV_HUB_{badge}_{file}` | `SAT_DP_RV_HUB_SAPBW_RETAIL_CUSTOMER` |
| Non-historized hub sat | `SAT_NH_RV_HUB_{badge}_{file}` | `SAT_NH_RV_HUB_SAPBW_API_TXNS` |
| Standard link sat | `SAT_RV_LNK_{badge}_{file}` | `SAT_RV_LNK_SAPBW_CUST_ACCT_MAST` |
| Effectivity link sat | `SAT_EF_RV_LNK_{badge}_{file}` | `SAT_EF_RV_LNK_MDM_ACCOUNT_MAP` |
| PII suffix | `SAT_RV_HUB_{badge}_{file}_PII` | `SAT_RV_HUB_SAPBW_RETAIL_CUSTOMER_PII` |
| Status tracking (STS) | `SAT_ST_RV_{parent_type}_{parent}_{badge}_{file}` | `SAT_ST_RV_HUB_PARTY_SAPBW_COMM_CUSTOMER` |
| Record tracking (RTS) | `SAT_RT_{parent_type}_{parent}` | `SAT_RT_HUB_PARTY` |
| Extended tracking (XTS) | `SAT_XT_{parent_type}_{parent}` | `SAT_XT_HUB_ACCOUNT` |

---

## Business Vault Satellite Patterns

BV satellites use a business `concept_name` instead of `{badge}_{file}`. No parent_type in BV satellite names.

| Type | Pattern | Example |
|---|---|---|
| BV Standard | `SAT_BV_{concept_name}` | `SAT_BV_CUSTOMER_CREDIT_SCORE` |
| BV Multi-Active | `SAT_MA_BV_{concept_name}` | `SAT_MA_BV_GRANDFATHERING_CARDS` |
| BV Non-Historized | `SAT_NH_BV_{concept_name}` | `SAT_NH_BV_PARTY_ACTIVITY_STREAM` |
| BV Effectivity | `SAT_EF_BV_{concept_name}` | `SAT_EF_BV_CUSTOMER_PRODUCT_ELIGIBILITY` |
| Activity Schema BV satellite | `SAT_BV_NH_{ENTITY}_STREAM` | `SAT_BV_NH_CUSTOMER_STREAM` |

---

## Staging View Patterns

| Type | Pattern | Example |
|---|---|---|
| Source staging | `STG_{badge}_{file}` | `STG_SAPBW_COMM_CUSTOMER` |
| BV staging | `STG_BV_{concept_name}` | `STG_BV_CREDITSCORE` |
| Activity Schema BV transformation view | `stg_bv_{entity}_activity` | `stg_bv_customer_activity` |
| Activity Schema stream on BV view | `str_bv_{entity}_activity_to_sat_bv_nh_{entity}_stream` | `str_bv_customer_activity_to_sat_bv_nh_customer_stream` |
| Effectivity secondary | `STG_EF_{badge}_{file}` | `STG_EF_MDM_ACCOUNT_MAP` |
| Status tracking secondary | `STG_ST_{badge}_{file}_{parent_type}_{parent}` | `STG_ST_SAPBW_COMM_CUSTOMER_HUB_PARTY` |
| Record tracking secondary | `STG_RT_{file}_{parent_type}_{parent}_{hashkey}` | `STG_RT_COMM_CUSTOMER_HUB_PARTY_DV_HASHKEY_HUB_PARTY` |
| Extended tracking secondary | `STG_XT_{file}_{parent_type}_{parent}_{hashkey}` | `STG_XT_RETAIL_CUSTOMER_HUB_PARTY_DV_HASHKEY_HUB_PARTY` |

---

## Column Names (canonical — no aliases)

| Concept | Column name | NOT |
|---------|-------------|-----|
| Hub hash key | `dv_hashkey_hub_<name>` | `<NAME>_HK` |
| Link hash key | `dv_hashkey_lnk_<name>` | `<NAME>_HK` |
| Same-As Link hash key | `dv_hashkey_lnk_rv_sa_<name>` | `SAL_<ENTITY>_HK` |
| Hash diff | `dv_hashdiff` | `HDIFF`, `HASH_DIFF` |
| Load timestamp | `dv_load_timestamp` | `LDTS`, `LOAD_DATE` |
| Applied timestamp | `dv_applied_timestamp` | `RDTS`, `BATCH_DATE` |
| Record source | `dv_recordsource` | `RSRC`, `RECORD_SOURCE` |
| Collision code | `dv_collisioncode` | `BKCC`, `COLLISION_CODE` |
| Tenant | `dv_tenant_id` | — |
| Task ID | `dv_task_id` | — |
| JIRA ID | `dv_jira_id` | — |
| User ID | `dv_user_id` | — |
| Sequence (MA/DC) | `dv_sequence` | `SEQ` |
| Effectivity start | `dv_start_date` | `dv_startts`, `LEDTS` |
| Effectivity end | `dv_end_date` | `dv_endts`, `LEDTS` |
| Last seen | `last_seen_date` | `dv_last_updated_date` |
| Status (STS only) | `dv_status` | — |
| XTS target (XTS only) | `dv_record_target` | — |
| **End-date** | **DOES NOT EXIST** | `LEDTS` |
| **Active flag** | **DOES NOT EXIST in effectivity sats** | `ACTIVE_FLAG` |

---

## Hash Algorithm Configuration

The hash algorithm is set once per project and applies to ALL hashkeys and hashdiffs.

| Algorithm | Snowflake function | Column type | Ghost record | Hex chars |
|---|---|---|---|---|
| MD5 | `MD5_BINARY(...)` | `BINARY(16)` | `TO_BINARY(REPEAT('0', 32), 'HEX')` | 32 |
| **SHA1** (default) | `SHA1_BINARY(...)` | `BINARY(20)` | `TO_BINARY(REPEAT('0', 40), 'HEX')` | 40 |
| SHA256 | `SHA2_BINARY(...)` | `BINARY(32)` | `TO_BINARY(REPEAT('0', 64), 'HEX')` | 64 |

**Rules:**
- Never mix algorithms within a vault
- `BINARY(n)` size must match the algorithm everywhere — DDL, ghost records, COALESCE defaults in PITs
- Ghost record formula: `TO_BINARY(REPEAT('0', n * 2), 'HEX')` where `n` = algorithm output bytes
- Staging views must use the matching function consistently

---

## General Rules

- All object names: UPPER_SNAKE_CASE
- Hub names: always singular (PARTY not PARTIES)
- Source badge: always lowercase in manifest (`sapbw`, not `SAPBW`)
- No spaces, no camelCase
- No SQL reserved words as column names (DATE, TIME, VALUE, NAME, TYPE, STATUS)
- Abbreviations must be consistent across the vault (if `CUST` once, CUST everywhere)
