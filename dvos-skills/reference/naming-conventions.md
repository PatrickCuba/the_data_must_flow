# DVOS Naming Conventions — Quick Reference

## Object Prefixes

| Object | Prefix | Example |
|--------|--------|---------|
| Hub | `HUB_` | `HUB_CUSTOMER` |
| Link | `LNK_` | `LNK_CUSTOMER_ORDER` |
| Satellite | `SAT_` | `SAT_CUSTOMER_DEMOGRAPHICS` |
| Same-as link | `SAL_` | `SAL_CUSTOMER` |
| PIT table | `PIT_` | `PIT_CUSTOMER` |
| Bridge table | `BDG_` | `BDG_PARTYACCOUNT_DAILY` |
| Current view | `VC_` | `VC_SAT_CUSTOMER_SF` |
| History view | `VH_` | `VH_SAT_CUSTOMER_SF` |
| IM dimension | `DIM_` | `DIM_CUSTOMER` |
| IM fact | `FACT_` | `FACT_ORDER` |
| IM audit | `AUDIT_` | `AUDIT_CUSTOMER` |
| Staging view | `STG_` | `STG_SF_CUSTOMER` |

## Prohibited Prefixes

| Wrong | Correct |
|-------|---------|
| `BRDG_` | `BDG_` |
| `BRIDGE_` | `BDG_` |

## Column Names (canonical — no aliases)

| Concept | Column name | NOT |
|---------|-------------|-----|
| Hub hash key | `dv_hashkey_hub_<name>` | `<NAME>_HK` |
| Link hash key | `dv_hashkey_lnk_<name>` | `<NAME>_HK` |
| SAL hash key | `dv_hashkey_sal_<entity>` | `SAL_<ENTITY>_HK` |
| Hash diff | `dv_hashdiff` | `HDIFF`, `HASH_DIFF` |
| Load timestamp | `dv_load_timestamp` | `LDTS`, `LOAD_DATE` |
| Applied timestamp | `dv_applied_timestamp` | `RDTS`, `BATCH_DATE` |
| Record source | `dv_recordsource` | `RSRC`, `RECORD_SOURCE` |
| Collision code | `dv_collisioncode` | `BKCC`, `COLLISION_CODE` |
| Tenant | `dv_tenant_id` | `tenant_id` |
| Sequence (MA/DC) | `dv_sequence` | `SEQ`, `ROW_SEQ` |
| Effectivity start | `dv_start_date` | `START_TS`, `dv_startts` |
| Effectivity end | `dv_end_date` | `END_TS`, `dv_endts` |
| Last seen | `last_seen_date` | `dv_last_updated_date` |
| End-date | **DOES NOT EXIST** | `LEDTS` |
| Active flag | **DOES NOT EXIST** (in effectivity sats) | `ACTIVE_FLAG` |

## Naming Patterns

| Object | Pattern |
|--------|---------|
| Hub | `HUB_<SINGULAR_NOUN>` |
| Link | `LNK_<RELATIONSHIP>` |
| Satellite | `SAT_<PARENT>_<CONTEXT>` |
| PII satellite | `SAT_<PARENT>_<CONTEXT>_PII` |
| Effectivity satellite | `SAT_<LINK>_EFF` |
| SAL effectivity | `SAT_SAL_<ENTITY>_EFF` |
| PIT | `PIT_<HUB_NAME>` |
| Bridge | `BDG_<CONCEPT>_<PERIOD>` |
| Current view | `VC_SAT_<PARENT>_<CONTEXT>` |
| History view | `VH_SAT_<PARENT>_<CONTEXT>` |

## Staging View Naming

| Type | Pattern | Example |
|------|---------|---------|
| Source staging | `STG_<SOURCE_BADGE>_<SOURCE_FILE>` | `STG_SF_CUSTOMER` |
| BV staging | `STG_BV_<CONCEPT_NAME>` | `STG_BV_CREDIT_SCORE` |

## General Rules

- All object names: UPPER_SNAKE_CASE
- Hub names: always singular (CUSTOMER not CUSTOMERS)
- No spaces, no camelCase
- No SQL reserved words as column names (DATE, TIME, VALUE, NAME, TYPE, STATUS)
- Abbreviations must be consistent across the vault (if CUST once, CUST everywhere)
