# DVOS Skills Demo Script

## Prerequisites

Install the plugin:

```bash
cortex skill add /Users/pcuba/Downloads/DV_Practice/dvos-skills
```

Verify it's registered:

```bash
cortex skill list
```

You should see all 13 `/dv-*` skills listed.

---

## Demo Flow

### Step 1: `/dv-when`

Type:
```
/dv-when
```

When asked about context, respond:
> We're a retail bank with 9 source systems: SAP BW (customer master, commercial customers, transactions, account relationships), Xero (account details), MDM (account deduplication mappings), Zoho (employee-account assignments, employee-manager hierarchy), and a Cards platform. Regulated industry (banking), need full audit history, schemas change quarterly.

**Expected:** Recommendation for Data Vault 2.0, confidence HIGH.

---

### Step 2: `/dv-discover`

Type:
```
/dv-discover
```

When asked for source tables, respond:
> Here are the source tables in LIB_PRD01_ODS.ODS:
> - SAPBW_CUST_ACCT_MAST (party_id, account_id, secondary_account_id + 40 address/relationship columns)
> - SAPBW_RETAIL_CUSTOMER (party_id, contact_id, contact_type, contact_value + PII: SSN, tax_id, passport)
> - SAPBW_COMM_CUSTOMER (party_id + 25 commercial customer attributes)
> - SAPBW_API_TXNS (account_id + transaction attributes)
> - XERO_ACCOUNT_DETAILS (account_id + account attributes, 8 rows)
> - MDM_ACCOUNT_MAP (source_account_id, target_account_id + confidence score, map_type)
> - ZOHO_EMPLOYEE_ACCOUNT (employee_id, account_id)
> - ZOHO_EMPLOYEE_MANAGER (employee_id, manager_id)
> - CARDS_TRANSFER_CARDS (card_id, transfer_card_id, control_card_id, bc_consol_card_acct_type)

When asked open questions, answer:
- "party_id is the same person across all SAP BW sources"
- "MDM_ACCOUNT_MAP is a true same-as link (entity resolution)"
- "card_id, transfer_card_id, control_card_id are all separate accounts in HUB_ACCOUNT"
- "SAPBW_RETAIL_CUSTOMER contacts are dependent-child (each contact tracked independently by contact_id)"

**Expected:** Vault model proposal with 2 hubs, 5 links, 10+ satellites.

---

### Step 3: `/dv-model`

Type:
```
/dv-model hub
```

When asked, respond:
> HUB_PARTY. Business key: party_id. Natural customer identifier across all SAP BW systems.

Then type:
```
/dv-model satellite
```

When asked, respond:
> Parent: HUB_PARTY. Source: SAPBW_RETAIL_CUSTOMER. Attributes: social_security_number, tax_id_number, passport_number. These are PII. Dependent child key: contact_id.

**Expected:** Pattern Recommender chooses dependent-child + PII suffix. Produces SAT_RV_HUB_SAPBW_RETAIL_CUSTOMER_PII DDL.

Then type:
```
/dv-model sal
```

When asked, respond:
> SAL for HUB_ACCOUNT. Source: MDM_ACCOUNT_MAP. source_account_id and target_account_id are the two accounts being matched. Match assertion comes from MDM system. Has confidence score and match reason as optional attributes.

**Expected:** Produces LNK_SA_RV_MAP_MDM_ACCOUNT + SAT_EF_RV_LNK_SA_MAP_MDM_ACCOUNT + SAT_RV_LNK_MDM_ACCOUNT_MAP (for match attributes).

---

### Step 4: `/dv-validate`

Type:
```
/dv-validate manifest
```

Paste or reference the model from Step 3.

**Expected:** All constructs CLEAN. Warnings: WARN-02 (PII detected — already segregated), WARN-04 (3+ satellites on HUB_PARTY and HUB_ACCOUNT — recommend PIT).

---

### Step 5: `/dv-generate`

Type:
```
/dv-generate
```

When asked which construct:
> Generate for HUB_PARTY and SAT_RV_HUB_SAPBW_COMM_CUSTOMER.

**Expected:** DDL with NOT ENFORCED PKs, MERGE hub load with last_seen_date, INSERT satellite load with NOT EXISTS, ghost record INSERT, VC_ and VH_ views.

---

### Step 6: `/dv-stage`

Type:
```
/dv-stage
```

When asked:
> Design a staging view for SAPBW_COMM_CUSTOMER. Source: LIB_PRD01_ODS.ODS.SAPBW_COMM_CUSTOMER. Batch timestamp column: event_timestamp.

**Expected:** Staging view with hashkey (UPPER + BKCC), hashdiff (no UPPER), all 8 metadata columns (including dv_task_id, dv_jira_id, dv_user_id).

Then ask:
> Also show me the secondary staging for the MDM_ACCOUNT_MAP effectivity satellite.

**Expected:** stg_ef_mdm_account_map pattern with OPEN/CLOSE records, driver key = source_account_id.

---

### Step 7: `/dv-bv`

Type:
```
/dv-bv
```

When asked:
> I want to derive a relationship_quality_score from MDM confidence scores and SAPBW relationship data. Landed mode. Parent: LNK_SA_RV_MAP_MDM_ACCOUNT.

**Expected:** Pipeline: bv_relationship_quality (you write) -> stg_bv_relationship_quality (DVOS generates) -> SAT_BV_RELATIONSHIP_QUALITY. dv_applied_timestamp = GREATEST(source timestamps).

---

### Step 8: `/dv-pit-bridge`

Type:
```
/dv-pit-bridge
```

When asked:
> Build a PIT for HUB_PARTY. Include SAT_RV_HUB_SAPBW_COMM_CUSTOMER, SAT_RV_HUB_SAPBW_RETAIL_CUSTOMER, SAT_DP_RV_HUB_SAPBW_CUST_ACCT_MAST_ADDRESS. Dynamic Table, 1 hour lag.

**Expected:** PIT_PARTY as Dynamic Table with TARGET_LAG = '1 hour', ghost key fallbacks.

---

### Step 9: `/dv-load`

Type:
```
/dv-load
```

When asked:
> Generate a Task DAG for the full model. Warehouse: TRANSFORM_WH. Schedule: daily at 6 AM AEDT.

**Expected:** Task DAG with sequential same-hub loads (HUB_PARTY from 3 sources in order, HUB_ACCOUNT from 5+ sources in order), links after hubs, satellites after parents.

---

### Step 10: `/dv-deploy`

Type:
```
/dv-deploy
```

When asked:
> Database: LIB_PRD01_EDW. Vault schema: SAL. Staging schema: ODS_STG. IM schema: INFORMATION_MARTS. Roles: BANK_LOADER, BANK_READER, BANK_ADMIN. Generate a dev clone.

**Expected:** Schema DDL (TRANSIENT staging), role/grant setup, snow sql execution script, zero-copy clone command.

---

### Step 11: `/dv-test`

Type:
```
/dv-test
```

When asked:
> DMF mode. DQ database: DV_DQ. DQ schema: DQ. Vault tables in LIB_PRD01_EDW.SAL.

**Expected:** CREATE DATA METRIC FUNCTION for all 17 DMFs, ALTER TABLE attachments for HUB_PARTY + SAT_RV_HUB_SAPBW_COMM_CUSTOMER, TRIGGER_ON_CHANGES schedule, results query.

---

### Step 12: `/dv-mart`

Type:
```
/dv-mart
```

When asked:
> DIM_CUSTOMER. Anchor: HUB_PARTY. Satellites: SAT_RV_HUB_SAPBW_COMM_CUSTOMER, SAT_RV_HUB_SAPBW_RETAIL_CUSTOMER. Current state. Use VC_ views.

**Expected:** CREATE OR REPLACE VIEW joining HUB_PARTY to VC_ satellite views. No BINARY columns, no hash keys in SELECT. Business keys only.

---

### Step 13: `/dv-explain`

Type any of:
```
/dv-explain ghost record
/dv-explain why insert-only
/dv-explain same-as link
/dv-explain effectivity satellite
/dv-explain secondary staging
```

**Expected:** Plain-language explanations of each concept.

---

## Demo Talking Points

1. **Doctrine enforcement** — "I can't skip validation. The gate blocks SQL generation until the model is clean."
2. **Pattern automation** — "I didn't choose dependent-child. The Pattern Recommender analysed the data and chose for me."
3. **Snowflake-native** — "MERGE for hubs, Dynamic Tables for PITs, DMFs for continuous monitoring, zero-copy clones for dev."
4. **PII segregation** — "SSN and passport were automatically flagged and split into a separate satellite."
5. **No end-dates** — "DVOS satellites are purely insert-only. Current row via QUALIFY ROW_NUMBER. No LEDTS corruption."
6. **Secondary staging** — "All comparison logic lives in the staging layer. The satellite loader is always a dumb INSERT."

---

## Cleanup

To remove the plugin after the demo:
```bash
cortex skill remove dvos-data-vault
```
