---
name: dv-stage
description: Design or review a DVOS staging view — source staging or BV staging. Covers metadata columns, hashkey/hashdiff computation, and the no-business-logic rule.
enabled: true
---

# /dv-stage — Staging View Design

DVOS generates staging views automatically from the manifest. This skill helps you understand, design, or review a staging view structure and diagnose staging doctrine violations.

## Hard rules vs. soft rules — where each belongs

Pragmatic Data Vault distinguishes between **hard rules** (applied before data enters the vault) and **soft rules** (applied in the Business Vault after the vault is loaded). This boundary is critical: mixing them in the wrong layer creates technical debt and corrupts the audit trail.

| Rule type | Where applied | Examples |
|---|---|---|
| **Hard rules** | Curated zone (landing/pre-staging) — before vault load | Data quality thresholds, schema change detection, PII classification, BK identification, attribute splitting, hash computation |
| **Soft rules** | Business Vault (SAL/CAL) — after vault is loaded | Derivations, aggregations, calculated attributes, business process logic, entity resolution |

### Hard business rules — 7 curated zone treatments

Before staging views are created and before data is loaded to the vault, apply these treatments in the curated (landing) zone:

1. **Data quality threshold** — Grade incoming data against integrity rules. Decide whether records that fail a threshold should be rejected, quarantined, or sent back to the source for reprocessing. Never load known-bad data into the vault.

   **Trap & Reject pattern** — the recommended mechanism for handling quality-failing records:

   1. **Trap** — run a suite of data quality checks before loading to the vault. Records that fail are routed to a **SCD Type 4 error mart** (same structure as the source + `error_code` + `error_reason` columns). This keeps the vault clean while preserving the rejected record for investigation.
   2. **Alert** — if the error mart's current portion has any records, trigger an alert to the source application team. Send the offending record IDs back (API or notification) so the source can correct them at the origin.
   3. **Resubmit** — the corrected record re-enters the pipeline and undergoes the same quality checks. If it passes, it loads to the vault. **The corrected record must carry the same `dv_applied_timestamp` as when it was originally rejected** — this ensures the correction lands at the correct point in the business timeline, not at the correction-load time.

   > "All the data, all the time" does not mean loading garbage into the vault too. The cost of removing contaminated data from an immutable target after the fact is far higher than preventing the load in the first place.

2. **Schema evolution detection** — Detect column additions, removals, and type changes before loading. Validate against the data contract. A schema change may signal that the upstream system's intent has changed — understand the change before accepting it.

   **Schema drift 4-step workflow** — when a new column is detected in a source file:

   1. **Detect** — the pipeline detects new column(s) in the arriving source. Monitoring should track the historical column count of each satellite; a count increase is the drift signal. Alert the data team. Do not halt the pipeline permanently.
   2. **Profile** — assess the new column: Is it a new business key? Is it PII? How does it affect the data grain? Which satellite does it belong to (hub satellite, link satellite, or a new satellite)?
   3. **Map** — decide the target satellite, update the staging view to include the column, and update the staging hashdiff computation to include it. `ALTER TABLE ADD COLUMN` on the target satellite.
   4. **Continue processing** — once mapped, resume normal load. The hashdiff change will automatically trigger new state records for all entities on the next load cycle (see `/dv-deploy` hashdiff auto-migration). No reload of existing data is needed.

   The pipeline should load without the new column until it is mapped (step 3). Missing the column temporarily is preferable to halting the entire pipeline. Schema drift is a normal lifecycle event, not an emergency.

3. **PII auto-classification** — Run automatic classification to identify, quasi-identify, and tag sensitive data. Classification results feed masking policies and determine which attributes route to PII satellites. Run periodically (not just on first load).

   **PII treatment options at load time** — choose one or combine:

   | Strategy | When to use | Vault result |
   |---|---|---|
   | **PII satellite splitting** | Standard approach — segregate PII into `_pii` satellite for access control | Raw PII stored, access-controlled by role |
   | **Tokenization** | When raw PII must never enter the vault (contractual or regulatory) | Token stored; original value held in a secure external token vault |
   | **Obfuscation** | When approximate values satisfy analytics (e.g. birth year instead of DOB) | Transformed value stored; original not recoverable from vault |
   | **Combination** | PII satellite + tokenization for fields needing GDPR erasure support | Token in vault; raw value erasable from external token store |

   Tokenization and obfuscation are applied as **hard rules in the curated zone** — the staging view receives the token/obfuscated value; the raw PII value never enters the staging or vault layer.

4. **Business key identification** — Profile source columns to confirm which column(s) uniquely identify each business object. Ambiguous or composite keys must be resolved before modelling begins. See `/dv-model hub` for anti-patterns.

5. **Attribute split decision** — Decide which attributes belong to hub satellites vs. link satellites before staging views are written. The question: "Does this attribute exist without the other business object in this interaction?" Incorrect splits propagate through the model. See `/dv-explain satellite-splitting`.

6. **BK collision code (namespace ID) assignment** — For each data source being onboarded, ask the user:

   > **Does this source need a custom BKCC (collision code)?**
   > - If **no** (default): use `'default'`. This means the source's business keys are assumed to be in the same namespace as all other `'default'` sources — identical keys will integrate to the same hub row.
   > - If **yes**: ask them to provide the BKCC value (e.g. `'zoho'`, `'sap1'`, `'card'`). This creates a separate namespace — even identical key values will hash to different hub rows.

   **When to suggest a custom BKCC:**
   - Multiple source systems share overlapping key spaces for **different** business entities (e.g. both SAP and Salesforce use `'10001'` as an account ID, but they mean different accounts)
   - The source is known to have a synthetic/internal numbering scheme that conflicts with other sources

   **When to keep `'default'`:**
   - The source's business keys are globally unique (e.g. email addresses, GUIDs, ABNs)
   - Only one source feeds this hub (no collision risk)
   - Multiple sources share the key space intentionally (passive integration — same customer in both systems)

   Record the BKCC assignment in the source badge registry table above.

7. **Tenant ID assignment** — For each data source being onboarded, ask the user:

   > **Does this source need a custom tenant ID, or should we use `'default'`?**
   > - If **no** (default): use `'default'`. Single-tenant vaults use `'default'` for all sources.
   > - If **yes**: ask them to provide the tenant ID value (e.g. `'fraud'`, `'retail'`, `'wholesale'`). This partitions the vault logically — the tenant ID becomes part of the hash key computation when multi-tenancy is enabled.

   **When to suggest a custom tenant ID:**
   - The vault serves multiple business units, brands, or legal entities that must be logically separated
   - Data from different tenants may share the same business keys but represent different entities (e.g. customer `'1001'` in the retail division vs. the wholesale division)
   - Regulatory or compliance requirements mandate logical data separation within the same physical vault

   **When to keep `'default'`:**
   - Single-tenant vault (one business unit, one brand)
   - No logical separation requirement
   - Multi-tenancy is disabled in the project manifest (`tenant.enabled: false`)

   Record the tenant ID assignment in the source badge registry table above.

8. **Hash computation (once)** — All hash keys (`dv_hashkey_*`) and hashdiffs (`dv_hashdiff_*`) are computed **once** in the staging view SQL. They are never recalculated downstream. This is a hard rule: any downstream re-computation risks hash inconsistency if the algorithm or input format changes.

**Data type inheritance rule** — raw vault inherits the data type of every attribute column from the source. Only the business key column is always `VARCHAR` regardless of source type. All other columns use the source's native type:

| Column type | Rule |
|---|---|
| Business key | Always `VARCHAR` — no exceptions |
| Attribute columns | Inherit from source (e.g. `NUMBER`, `DATE`, `BOOLEAN`, `VARIANT`) |
| Hash keys | `BINARY(20)` for SHA1 (default), `BINARY(16)` for MD5, `BINARY(32)` for SHA256 |
| Timestamps (metadata) | Always `TIMESTAMP_NTZ` |

**Why BINARY for hash columns (not VARCHAR):** `BINARY` uses half the storage footprint of an equivalent `VARCHAR` hash digest and performs better in SQL equi-joins. A SHA1 digest stored as `BINARY(20)` is 20 bytes; stored as `VARCHAR(40)` (hex-encoded) it is 40 bytes. Snowflake queries data from object storage over the cloud network — smaller column widths reduce the bytes sent and improve join performance at scale.

If a source column arrives with the wrong data type (e.g. a date that should be `DATE` arrives as `VARCHAR`), apply a hard rule to either (a) reject the record and alert the source, or (b) cast the value at the curated zone boundary. Never let a mistyped column silently propagate into the vault. Physical data modelling in raw vault is less critical than logical/conceptual modelling — the source domain has already done the data typing, mostly.

> Staging views are a passthrough with metadata added. They do not apply soft business rules. All logic above belongs in the landing/pre-staging layer, not in the staging view.

---

## Two types of staging view

| Type | Naming | Source | Purpose |
|---|---|---|---|
| Source staging | `stg_{source_badge}_{source_file}` | Landing table | Wraps raw source data, adds all DV metadata |
| BV staging | `stg_bv_{concept_name}` | BV rule view (`bv_{concept_name}`) | Wraps business rule output, adds all DV metadata |
| Effectivity staging | `stg_ef_{source_badge}_{source_file}` | Base staging view | Generates OPEN/CLOSE records for effectivity satellites |
| Status tracking staging | `stg_st_{source_badge}_{source_file}_{parent_type}_{parent}` | Base staging view + STS satellite | Generates INSERT/DELETE status change records |
| Record tracking staging | `stg_rt_{source_file}_{parent_type}_{parent}_{hashkey}` | Base staging view | Tracks entity presence per applied timestamp |
| Extended tracking staging | `stg_xt_{source_file}_{parent_type}_{parent}_{hashkey}` | Base staging view | Tracks adjacent satellite hashdiffs for XTS |

**Source badge** (`{source_badge}`) is the system-level identifier for the source (e.g. `sapbw`, `mdm`, `zoho`). It must be defined before staging views are named. See `reference/naming-conventions.md` for the full source badge definition and rules.

**Source badge registry** — maintain a formal lookup table mapping each source system to its badge, BKCC value, and tenant ID. This is a governance artifact, not an ad-hoc convention:

| Badge | System description | BKCC | Tenant ID |
|---|---|---|---|
| `mdm` | Master data management | `default` | `default` |
| `sapbw` | SAP ERP instance 1 | `sap1` | `default` |
| `sfrc` | Salesforce CRM | `sfrc` | `default` |
| `card` | 3rd party card management | `card` | `default` |

Register new source badges before modelling begins. The badge appears in table names (`SAT_{badge}_{file}`), record source values, and BKCC assignments — changing it after deployment requires renaming tables and reloading data. Get it right upfront.

---

## Kappa Vault staging mode

In Kappa Vault, the staging view SQL is **identical** to standard staging. The difference is in what consumes it:

**Standard mode:** Staging view is read directly by the loader task.

**Kappa Vault mode:** Staging view has **Snowflake Streams placed on it** — one `APPEND_ONLY` stream per loader. The loader reads from the stream, not the view. When the transaction commits, the stream advances, marking those records as processed.

### Stream naming convention

```
STR_{source_badge}_{source_file}_TO_{target_type}_{target_name}_{hashkey_col}
```

Examples:
```
STR_XERO_ACCOUNT_BALANCES_TO_HUB_ACCOUNT_DV_HASHKEY_HUB_ACCOUNT
STR_XERO_ACCOUNT_BALANCES_TO_SAT_XERO_ACCOUNT_BALANCES_DV_HASHKEY_HUB_ACCOUNT
```

One stream per loader — not one stream per source. A staging view feeding 1 hub + 1 satellite creates 2 streams.

### Create stream on staging view

```sql
-- Hub loader stream
CREATE OR REPLACE STREAM <staging_schema>.STR_<BADGE>_<FILE>_TO_HUB_<HUB>_DV_HASHKEY_HUB_<HUB>
ON VIEW <staging_schema>.STG_<BADGE>_<FILE>
APPEND_ONLY = TRUE;

-- Satellite loader stream (one per satellite fed by this staging view)
CREATE OR REPLACE STREAM <staging_schema>.STR_<BADGE>_<FILE>_TO_SAT_<SAT_NAME>_DV_HASHKEY_HUB_<PARENT>
ON VIEW <staging_schema>.STG_<BADGE>_<FILE>
APPEND_ONLY = TRUE;
```

### Landing table requirements for Kappa Vault

When streams are placed on staging views, the **landing table must be append-only**:
- Never use `INSERT OVERWRITE` or `TRUNCATE` on the landing table
- New deliveries are appended with new `metadata_date` / `event_timestamp` values
- The stream tracks unprocessed rows — advancing only on loader commit
- Multiple landings between loads are handled by the `discard_view` / `distinct_view` CTEs in the loader (see `/dv-load`)

Ask the user:
> "Is your landing table append-only, or does each delivery overwrite the previous batch?"
> - Append-only → Kappa Vault is safe to use
> - Overwrite → Kappa Vault cannot be used (stream would lose records on overwrite)

---

## Core doctrine: staging is metadata-only

**DV-STG-008**: Staging views are a pure passthrough with DV metadata added. No business logic.

| Allowed in staging | NOT allowed in staging |
|---|---|
| Hash key computation (`dv_hashkey_*`) | `CASE WHEN` for business logic |
| Hashdiff computation (`dv_hashdiff_*`) | String concatenation (`\|\|`) for derived columns |
| Metadata columns (`dv_load_timestamp` etc.) | `DATEADD`, `DATEDIFF`, `DATE_TRUNC` |
| Pass-through of source columns | `SUBSTR`, `LEFT`, `RIGHT` for string manipulation |
| | Mathematical derivations |

Business logic belongs in **landing tables only**. If a column needs derivation before loading — it belongs in the landing layer, not staging.

---

## Source staging view

### Required metadata columns (DV-STG-001)

Every staging view must expose:
- `dv_load_timestamp` — when the record was loaded
- `dv_applied_timestamp` — business time from source batch/file (carried from source, NOT `CURRENT_TIMESTAMP`)
- `dv_recordsource` — source system identifier
- `dv_tenant_id` — tenant discriminator
- `dv_collisioncode` — BKCC (hub-only; staging still carries it for hub hashkey computation)
- `dv_task_id` — task/job identifier (default `'N/A'`, overridden by loader with run_id)
- `dv_jira_id` — JIRA ticket for traceability
- `dv_user_id` — loading user/service account (typically `CURRENT_USER()`)

### Hash key columns (DV-STG-003 / DV-STG-004)

For each hub fed by this staging view:
```
dv_hashkey_hub_<hub_name>   — computed from BKCC + business key
```

For each link fed by this staging view:
```
dv_hashkey_lnk_<link_name>  — computed from BKCC + all participant business keys
dv_hashkey_hub_<hub_a>      — per participant hub
dv_hashkey_hub_<hub_b>
```

### Hashdiff columns (DV-STG-005)

For each satellite loaded from this staging view:
```
dv_hashdiff_<satellite_full_name>   — one per satellite target
```

Hashdiff naming uses the **full satellite name** (e.g. `dv_hashdiff_sat_customer_demographics`).

### Hash computation rules

**Hash keys** — use `UPPER()`, null substitute `-1`. Whether `dv_tenant_id` is included depends on `tenant.enabled` in the manifest:

```sql
-- Multi-tenancy ENABLED (tenant.enabled: true):
hash_fn(UPPER(CONCAT(
    '<tenant_id_value>' || '||' || '<bkcc>' || '||' || COALESCE(TRIM(CAST(<bk_col> AS STRING)), '-1')
))) AS dv_hashkey_hub_<name>

-- Multi-tenancy DISABLED (tenant.enabled: false):
hash_fn(UPPER(CONCAT(
    '<bkcc>' || '||' || COALESCE(TRIM(CAST(<bk_col> AS STRING)), '-1')
))) AS dv_hashkey_hub_<name>
```

Default values: `dv_tenant_id = 'default'`, `dv_collisioncode = 'default'`. Override per source using `bkcc_value` and `tenant_id_value` in the manifest hub sources (e.g. `bkcc_value: zoho` for Zoho-sourced accounts).

**BK treatment boundary — normalisation vs. business rule**

The default treatments (`UPPER`, `TRIM`, `COALESCE(..., '-1')`) normalise **form** without changing **meaning**. They ensure consistent integration regardless of source-system formatting differences.

If a treatment changes the **semantic meaning** of the business key (e.g. extracting a sub-key, reformatting a composite key into components, applying a lookup-based mapping), it is a **business rule** — not a hard rule. The output is a derived business key that belongs in a **Business Vault hub**, not in the staging hashkey computation.

Non-default BK treatments (disabling UPPER, skipping TRIM for reference data codes) should be the exception, not the rule. Always attempt to resolve key-formatting issues at the source system first.

**Variable BK Treatments — when standard normalisation is insufficient**

The standard (default) treatment is: `UPPER(TRIM(COALESCE(CAST(bk AS STRING), '-1')))` — this provides **passive integration** where two sources with different key casing or whitespace will naturally resolve to the same hash.

Some sources require **variable treatment** — the standard normalisation must be modified or bypassed:

| Scenario | Treatment | Example |
|---|---|---|
| **Case-sensitive BK** | Skip `UPPER()` — preserve original case | OAuth tokens, API keys, Git commit SHAs |
| **Salesforce CaseSafe ID** | Convert 15-char case-sensitive ID to 18-char case-insensitive ID | `0015000000Abc` → `0015000000AbcDEF` (append 3-char checksum suffix) |
| **Leading zeros significant** | Skip `CAST AS STRING` if already string; do NOT trim leading zeros | Account numbers: `007` ≠ `7` |
| **Whitespace-embedded BK** | Skip `TRIM()` — internal spaces are meaningful | Composite codes: `AB 123` ≠ `AB123` |
| **Unicode normalisation** | Apply `NORMALIZE()` or NFC form before hashing | Accented characters: `café` vs `café` (composed vs decomposed) |
| **Reference data codes** | Preserve exact original format — no normalisation | ISO codes: `USD`, `NZD` (already standardised) |

**Implementation rule:** variable treatments are declared per-source in the staging manifest. The manifest `bk_treatment` property overrides the default pipeline:

```yaml
hub_sources:
  - hub: customer
    source: salesforce_contacts
    bk_column: sf_contact_id
    bk_treatment: casesafe_18  # converts 15→18 digit Salesforce ID before hashing
```

**Passive vs. active integration:**
- **Passive** (standard treatment): two sources resolve to the same hash automatically because normalisation strips noise. No human decision needed.
- **Active** (variable treatment): a deliberate decision to override default normalisation because the source's key format carries meaning that would be destroyed by `UPPER`/`TRIM`.

Document all variable treatments in the project standards register. Each override requires a justification referencing the source system's key semantics.

**Hashdiffs** — **NO `UPPER()` or `LOWER()`** (DV-STG-007), null substitute `''`, no tenant_id/bkcc:
```sql
hash_fn(CONCAT(
    COALESCE(TRIM(CAST(<attr1> AS STRING)), '') || '||' ||
    COALESCE(TRIM(CAST(<attr2> AS STRING)), '')
)) AS dv_hashdiff_<sat_full_name>
```

**Hashdiff — what to include and what to exclude**

| | Rule | Why |
|---|---|---|
| ✅ **Include** | Business descriptive attributes (the satellite's tracked columns) | These are the values whose change the hashdiff detects |
| ✅ **Include** | Dependent-child key columns (e.g. transaction ID, order line number) | Dep-child keys are business attributes, not metadata — they must be in the hashdiff so that a change in the dep-child key value is detected as a true change |
| ❌ **Never include** | `dv_load_timestamp` | Changes every load cycle — every record appears new on every load, defeating change detection entirely |
| ❌ **Never include** | `dv_applied_timestamp` | Same problem — a DV metadata column that is load-cycle-specific |
| ❌ **Never include** | `dv_recordsource`, `dv_tenant_id`, `dv_collisioncode`, `dv_task_id` | DV metadata — none of these are business attributes |
| ❌ **Never include** | `SYSDATE` / `CURRENT_TIMESTAMP` / `CURRENT_DATE` | System datetime always differs — every record is perpetually "new", producing infinite satellite rows |
| ❌ **Never include** | The business key | The load code already performs a by-hash-key comparison; adding the BK to the hashdiff has zero effect on uniqueness and inflates the hash input unnecessarily |

**Snowflake performance rule: use `CONCAT` (or `\|\|`), not `ARRAY_CONSTRUCT`**

`ARRAY_CONSTRUCT` is a semi-structured function designed for variant/JSON construction. Using it to concatenate structured columns before hashing is an incorrect and slower approach. On Snowflake's cloud utilisation billing model, a slower hash function runs for longer and directly increases compute cost on every satellite load.

Always use `CONCAT` or the `\|\|` operator for structured column concatenation in hashdiff computation.

**Encoding standardization rule (DV-STG-009)**

Always cast to `CHAR` (ASCII) before hashing, never `NCHAR` (Unicode). The same string cast as CHAR vs NCHAR produces **different byte representations** and therefore **different hash digests**, even when the text appears identical to a human reader. This breaks cross-platform hash portability and same-key matching between systems.

Rule: standardize all hash inputs as `CAST(<col> AS VARCHAR)` (Snowflake's VARCHAR is UTF-8/ASCII-compatible). Never use `TO_NCHAR()` or NVARCHAR data types in hash computation. If the source stores data in a non-ASCII encoding (EBCDIC, extended Unicode), explicitly convert to UTF-8 before hashing.

**Schema evolution in hashdiff (DV-STG-010)**

When source attributes are added or deprecated:
1. **New attributes** — add to the **rightmost** position in the hashdiff `CONCAT` (after the last existing attribute). Never insert in the middle — this changes existing hashdiff values and triggers false-positive change detection for every entity on next load.
2. **Deprecated attributes** — continue including them in the hashdiff with a NULL substitute (`''`). Never remove a deprecated column from the hashdiff computation. The column loads as NULL going forward; existing hashdiff values remain stable.

See `/dv-deploy` Satellite schema evolution for the DDL-side rules.

### Canonical dv-tag names (DV-STG-006)

Never alias. Exact names:

| Column | Correct | Wrong |
|---|---|---|
| Load timestamp | `dv_load_timestamp` | `load_ts`, `LDTS` |
| Applied timestamp | `dv_applied_timestamp` | `applied_ts`, `ADTS` |
| Record source | `dv_recordsource` | `record_source`, `RSRC` |
| Tenant ID | `dv_tenant_id` | `tenant_id` |
| Collision code | `dv_collisioncode` | `bkcc`, `collision_code` |
| Hash key | `dv_hashkey_hub_<name>` | `<NAME>_HK` |
| Hash diff | `dv_hashdiff_<sat_name>` | `HDIFF`, `dv_hashdiff` (bare) |

---

## Business key array unravelling protocol

When source data delivers business keys serialized as arrays (comma-delimited strings, JSON arrays, or repeated columns within a single row):

1. **Unravel** — explode the array into individual rows using `LATERAL FLATTEN` (Snowflake) or equivalent
2. **Model as a link** — the unravelled keys represent a multi-participant unit of work, not a single entity hub. Create a link connecting the parent entity to each unravelled key's hub.
3. **Preserve the original** — store the raw serialized column in a satellite for source recreation (auditability)
4. **Do not sort or deduplicate the array** — if the order changes, it must NOT be recorded as a new business entity. Hash the unravelled individual keys, not the serialized string.

**Why:** Storing arrays of keys in a single hub row violates the one-BK-per-hub principle. If the order changes, a hash of the serialized string produces a different key — creating a false new entity. Unravelling into a link preserves the correct grain and allows individual key-level tracking.

```sql
-- Example: unravelling a JSON array of product IDs from an order source
SELECT
    src.order_id,
    f.value::VARCHAR AS product_bk,
    SHA2(UPPER(CONCAT('default', '||', f.value::VARCHAR))) AS dv_hashkey_hub_product
FROM landing_orders src,
    LATERAL FLATTEN(input => PARSE_JSON(src.product_ids_json)) f;
```

---

## BV staging view

BV staging wraps the business rule view (`bv_{concept_name}`) which lives in the BV staging schema. The rule view outputs **only business keys + `dv_applied_timestamp`** — no hashkeys, no hashdiff. DVOS computes everything else in the staging layer.

```
bv_{concept_name}  (business rule view — BK + dv_applied_timestamp only)
        ↓
stg_bv_{concept_name}  (DVOS-generated staging — adds hashkeys, hashdiff, all metadata)
        ↓
sat_bv_{concept_name}  (BV satellite — INSERT-only load)
```

**Business rule view must NOT contain any `dv_hashkey_*` or `dv_hashdiff_*` columns.** DVOS is the sole generator of those.

`dv_applied_timestamp` in BV staging is carried from the rule view — it must be derived from contributing RV satellite timestamps (`GREATEST` of sources), never `CURRENT_TIMESTAMP` (DV-BV-111).

---

## Secondary staging views

Secondary staging views sit between the base staging view and the satellite loader. They perform comparison logic that the base staging (metadata-only) cannot handle. The satellite loader remains standard (INSERT WHERE NOT EXISTS) — all intelligence lives in the secondary staging view.

**Satellite anti-semi join must compare against CURRENT state** \u2014 the NOT EXISTS in the satellite loader must compare against the most recent row per entity key (via `QUALIFY ROW_NUMBER() ... ORDER BY dv_applied_timestamp DESC` inside the subquery), not against all historical rows. This is essential for correctly handling **state reversions**:

A business entity can revert to a previous state (A \u2192 B \u2192 A). If the NOT EXISTS compared against ALL history, the reversion load (back to A) would find hashdiff-A already in the satellite and skip the insert \u2014 which is wrong. Comparing only against the CURRENT state correctly detects the change and loads the reversion. See `/dv-explain anti-semi join` for the full pattern.

```
Landing table
    ↓
stg_{source_badge}_{source_file}         ← base staging (metadata-only)
    ↓
stg_ef_* / stg_st_* / stg_rt_* / stg_xt_*   ← secondary staging (comparison logic)
    ↓
SAT_*_EFF / SAT_ST_* / SAT_RT_* / SAT_XT_*   ← satellite loader (standard INSERT)
```

### Effectivity staging (`stg_ef_*`)

Generates OPEN and CLOSE records for effectivity satellites by comparing the current source delivery against the currently active relationships in the target.

**Pattern:** `stg_ef_{source_badge}_{source_file}`

**Logic:**

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_EF_<SOURCE_BADGE>_<SOURCE_FILE> AS

WITH latest_effs AS (
    -- Get currently active relationships from the effectivity satellite
    SELECT
        hub_a.dv_hashkey_hub_<hub_a>,
        hub_b.dv_hashkey_hub_<hub_b>,
        lnk.dv_hashkey_lnk_<link>,
        ef.dv_start_date,
        ef.dv_end_date
    FROM <vault_schema>.HUB_<HUB_A> hub_a
    JOIN <vault_schema>.LNK_<LINK> lnk ON lnk.dv_hashkey_hub_<hub_a> = hub_a.dv_hashkey_hub_<hub_a>
    JOIN <vault_schema>.HUB_<HUB_B> hub_b ON hub_b.dv_hashkey_hub_<hub_b> = lnk.dv_hashkey_hub_<hub_b>
    JOIN <vault_schema>.SAT_<LINK>_EFF ef ON ef.dv_hashkey_lnk_<link> = lnk.dv_hashkey_lnk_<link>
    WHERE ef.dv_end_date = '<high_date>'::TIMESTAMP_NTZ
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lnk.dv_hashkey_lnk_<link>
        ORDER BY ef.dv_applied_timestamp DESC, ef.dv_load_timestamp DESC
    ) = 1
),

src_date AS (
    -- Get distinct driver key hashkeys + timestamps from base staging
    SELECT DISTINCT
        dv_hashkey_hub_<driver_key>,
        dv_applied_timestamp
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>
),

-- OPEN records: new relationships not in target
open_records AS (
    SELECT
        src.dv_hashkey_lnk_<link>,
        src.dv_applied_timestamp AS dv_start_date,
        '<high_date>'::TIMESTAMP_NTZ AS dv_end_date,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM latest_effs le
        WHERE le.dv_hashkey_hub_<hub_a> = src.dv_hashkey_hub_<hub_a>
          AND le.dv_hashkey_hub_<hub_b> = src.dv_hashkey_hub_<hub_b>
    )
),

-- CLOSE records: relationships that changed (driver key exists but non-driver keys differ)
close_records AS (
    SELECT
        le.dv_hashkey_lnk_<link>,
        le.dv_start_date AS dv_start_date,
        sd.dv_applied_timestamp AS dv_end_date,
        sd.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM latest_effs le
    JOIN src_date sd ON sd.dv_hashkey_hub_<driver_key> = le.dv_hashkey_hub_<driver_key>
    WHERE NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src2
        WHERE src2.dv_hashkey_hub_<hub_a> = le.dv_hashkey_hub_<hub_a>
          AND src2.dv_hashkey_hub_<hub_b> = le.dv_hashkey_hub_<hub_b>
    )
)

SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM close_records
UNION ALL
SELECT
    dv_hashkey_lnk_<link>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_start_date AS STRING)), '') || '||' ||
        COALESCE(TRIM(CAST(dv_end_date AS STRING)), '')
    )) AS dv_hashdiff_sat_<link>_eff,
    dv_start_date,
    dv_end_date,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM open_records;
```

**Key rules:**
- Compares ALL participant hashkeys (not just composite link hashkey) to detect flip-flop scenarios
- Hashdiff = hash of (`dv_start_date || dv_end_date`)
- CLOSE records use the original `dv_start_date` from the target
- Driver key determines which entity "owns" the relationship tracking
- The satellite loader uses the standard INSERT WHERE NOT EXISTS pattern — no special logic

---

### Status tracking staging (`stg_st_*`)

Detects INSERT/DELETE changes by comparing the current source snapshot against the previous state stored in the STS satellite itself.

**Pattern:** `stg_st_{source_badge}_{source_file}_{parent_type}_{parent_name}`

**Logic:**

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_ST_<SOURCE_BADGE>_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT> AS

WITH current_status AS (
    -- Latest status record per hashkey from the STS satellite (excl ghost)
    SELECT dv_hashkey_hub_<parent>, dv_hashdiff
    FROM <vault_schema>.SAT_ST_<PARENT>_<SOURCE>
    WHERE dv_recordsource != 'GHOST'
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<parent>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
),

-- INSERT: records in staging NOT in STS (or last status was 'D')
gen_inserts AS (
    SELECT
        src.dv_hashkey_hub_<parent>,
        SHA1_BINARY('I') AS dv_hashdiff,
        src.dv_applied_timestamp,
        src.dv_load_timestamp,
        src.dv_tenant_id,
        src.dv_recordsource
    FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
    WHERE NOT EXISTS (
        SELECT 1 FROM current_status cs
        WHERE cs.dv_hashkey_hub_<parent> = src.dv_hashkey_hub_<parent>
          AND cs.dv_hashdiff != SHA1_BINARY('D')
    )
),

-- DELETE: records in STS with active status but NOT in current staging
gen_deletes AS (
    SELECT
        cs.dv_hashkey_hub_<parent>,
        SHA1_BINARY('D') AS dv_hashdiff,
        src_date.dv_applied_timestamp,
        CURRENT_TIMESTAMP() AS dv_load_timestamp,
        src_date.dv_tenant_id,
        src_date.dv_recordsource
    FROM current_status cs
    CROSS JOIN (SELECT MAX(dv_applied_timestamp) AS dv_applied_timestamp,
                       MAX(dv_tenant_id) AS dv_tenant_id,
                       MAX(dv_recordsource) AS dv_recordsource
                FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>) src_date
    WHERE cs.dv_hashdiff != SHA1_BINARY('D')
      AND NOT EXISTS (
        SELECT 1 FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE> src
        WHERE src.dv_hashkey_hub_<parent> = cs.dv_hashkey_hub_<parent>
    )
)

SELECT * FROM gen_inserts
UNION ALL
SELECT * FROM gen_deletes;
```

**Key rules:**
- `'I'` (insert/present) and `'D'` (delete/absent) are the only two status values
- Hashdiff is a hash of the status letter itself: `SHA1_BINARY('I')` or `SHA1_BINARY('D')`
- Compares against the STS satellite itself (self-referencing)
- DELETE records are generated when an entity disappears from the current source delivery
- Supports role-playing via hashkey aliasing (one view per role)

---

### Record tracking staging (`stg_rt_*`)

Simplest secondary staging — records entity presence per `dv_applied_timestamp`. No comparison logic.

**Pattern:** `stg_rt_{source_file}_{parent_type}_{parent_name}_{hashkey_col}`

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_RT_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT>_<HASHKEY> AS
SELECT
    dv_hashkey_hub_<parent>,
    SHA1_BINARY(CONCAT(
        COALESCE(TRIM(CAST(dv_applied_timestamp AS STRING)), '')
    )) AS dv_hashdiff_sat_rt_<parent>_<source>,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;
```

**Key rules:**
- Pure passthrough with a hashdiff derived from `dv_applied_timestamp` only
- One view per (source_file, hashkey_column) — deduplicated across multiple RTS satellites

---

### Extended tracking staging (`stg_xt_*`)

Tracks adjacent satellite hashdiffs for XTS (Extended Tracking Satellites). UNION ALLs one SELECT per related satellite.

**Pattern:** `stg_xt_{source_file}_{parent_type}_{parent_name}_{hashkey_col}`

```sql
CREATE OR REPLACE VIEW <staging_schema>.STG_XT_<SOURCE_FILE>_<PARENT_TYPE>_<PARENT>_<HASHKEY> AS

-- One SELECT per related satellite (excluding EF, RT, ST, NH, XTS types)
SELECT
    dv_hashkey_hub_<parent>,
    '<SAT_NAME_1>' AS dv_record_target,
    dv_hashdiff_sat_<name_1> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>

UNION ALL

SELECT
    dv_hashkey_hub_<parent>,
    '<SAT_NAME_2>' AS dv_record_target,
    dv_hashdiff_sat_<name_2> AS dv_hashdiff,
    dv_applied_timestamp,
    dv_load_timestamp,
    dv_tenant_id,
    dv_recordsource
FROM <staging_schema>.STG_<SOURCE_BADGE>_<SOURCE_FILE>;
```

**Key rules:**
- `dv_record_target` identifies which satellite the hashdiff belongs to
- Only includes satellites whose hashdiff is present in the base staging view
- Excludes peripheral types (EF, RT, ST, NH, XTS itself) from the UNION
- Deployed to a separate schema (`staging_secondary_ext_schema`) when configured

---

## Secondary staging doctrine rules

| Rule | Severity | Description |
|---|---|---|
| DV-STG-SEC-001 | ERROR | Secondary staging views must reference the base staging view (not the landing table directly) |
| DV-STG-SEC-002 | ERROR | Effectivity staging must compare ALL participant hashkeys (not just composite link hashkey) |
| DV-STG-SEC-003 | ERROR | Status tracking hashdiff must be `SHA1_BINARY('I')` or `SHA1_BINARY('D')` only |
| DV-STG-SEC-004 | ERROR | CLOSE records must carry the original `dv_start_date` from the target (not a new timestamp) |
| DV-STG-SEC-005 | ERROR | The satellite loader downstream of secondary staging remains standard INSERT WHERE NOT EXISTS |

---

## Snowflake ingestion patterns

The landing layer feeds the staging views. These Snowflake-native patterns cover how data arrives into landing tables.

### External tables (files on cloud storage)

Use when raw files land in S3, Azure Blob, or GCS and you want queryable access without copying:

```sql
CREATE OR REPLACE EXTERNAL TABLE <landing_schema>.<source_file>
  WITH LOCATION = @<stage_name>/<path>/
  AUTO_REFRESH = TRUE
  FILE_FORMAT = (TYPE = PARQUET)
  PATTERN = '.*[.]parquet';
```

The staging view sits on top of this external table — same DV metadata enrichment pattern.

### Snowpipe (continuous ingestion)

Use when files arrive continuously and need near-real-time loading into the landing layer:

```sql
CREATE OR REPLACE PIPE <landing_schema>.pipe_<source_file>
  AUTO_INGEST = TRUE
AS
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

### COPY INTO with MATCH_BY_COLUMN_NAME

Use when the target landing table already exists and you want flexible schema mapping that tolerates column reordering in source files:

```sql
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = (TYPE = PARQUET)
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

Source columns map to target columns by name rather than position. New source columns are silently ignored unless the target table has them.

### INFER_SCHEMA + USING TEMPLATE (semi-structured schematisation)

Use when a new semi-structured source (Parquet, Avro, JSON) arrives and you need to auto-discover and materialize a structured schema:

```sql
-- Step 1: Discover the schema
SELECT *
FROM TABLE(INFER_SCHEMA(
    LOCATION => '@<stage_name>/<path>/',
    FILE_FORMAT => '<file_format_name>'
));

-- Step 2: Create a structured landing table from the discovered schema
CREATE OR REPLACE TABLE <landing_schema>.<source_file>
USING TEMPLATE (
    SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
    FROM TABLE(INFER_SCHEMA(
        LOCATION => '@<stage_name>/<path>/',
        FILE_FORMAT => '<file_format_name>'
    ))
);

-- Step 3: Load with MATCH_BY_COLUMN_NAME
COPY INTO <landing_schema>.<source_file>
FROM @<stage_name>/<path>/
FILE_FORMAT = '<file_format_name>'
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
```

This is Snowflake's native schematisation — it turns semi-structured data into fully typed structured columns without manual DDL authoring.

### When to use which pattern

| Scenario | Pattern |
|---|---|
| Target table exists, batch load | `COPY INTO` with `MATCH_BY_COLUMN_NAME` |
| New source, unknown schema | `INFER_SCHEMA` + `USING TEMPLATE` then `COPY INTO` |
| Near-real-time continuous files | Snowpipe |
| Query files without copying | External table |

### METADATA$ columns for recordsource derivation

When loading from files, Snowflake exposes metadata columns useful for DV staging:

```sql
-- In the staging view, derive dv_recordsource from the source filename:
METADATA$FILENAME AS dv_recordsource,
METADATA$FILE_ROW_NUMBER AS source_row_number,   -- useful for XTS pattern ordering
METADATA$FILE_LAST_MODIFIED AS file_timestamp     -- useful for dv_applied_timestamp derivation
```

These are available in `COPY INTO` statements and external table queries.

**Document sources — DV metadata mapping**

When the source is an unstructured document file (PDF, image, audio, email), apply these specific DV metadata mappings:

| DV column | Source | Rationale |
|---|---|---|
| `dv_recordsource` | `METADATA$FILENAME` | Provides direct traceability to the source document. If GDPR Article 17 erasure is triggered, `dv_recordsource` identifies exactly which document contains the PII. |
| `dv_applied_timestamp` | `METADATA$FILE_LAST_MODIFIED` | The business time the document was produced (when the event captured in the document occurred). |
| `dv_load_timestamp` | `CURRENT_TIMESTAMP()` | Standard — when the record was loaded into the vault. |

---

## Unstructured data staging

Unstructured data (PDFs, images, audio, video, email, social media) has no fixed schema. Processing it requires AI/NLP in a **pre-staging layer** before normal vault loading patterns apply.

**Architectural principle: AI/NLP happens before the vault, not inside it**

The vault only receives structured outputs. The same standard hub/link/satellite loading patterns apply regardless of whether the original source was a relational database or an AI-extracted document. AI/NLP is a transformation step that produces structured staging output — it is upstream of the staging view, not part of it.

```
Document files (stage)
        │
        ▼
AI/NLP model (e.g. Snowflake Document AI, AI_PARSE_DOCUMENT)
        │  — produces structured JSON payload
        ▼
Transient staging table  (raw prediction output)
        │
        ▼
Staging view             (maps AI output to DV metadata columns)
        │
        ▼
Hub / Link / Satellite loaders  (standard vault loading patterns)
```

**AI false-positive exception table — unstructured data variant of Trap & Reject**

When AI identifies a business object from a document, it may extract a business key that does not match any existing hub entity. This is an AI false positive (model error or genuinely new entity not yet modelled). **Do not load unrecognised business objects directly into the vault** — this would contaminate the vault with unverified keys.

Divert these records to an **exception table** for SME review:

| Outcome | What it means | Action |
|---|---|---|
| **True positive** | AI was right; entity exists but wasn't in vault yet | SME confirms; onboard the new hub entity; allow the record to proceed |
| **False positive** | AI was wrong; entity does not exist | Discard record; use as negative training example to retrain the model |

This is distinct from the Trap & Reject pattern (which handles records that fail hard quality rules). The AI exception pattern handles records where the entity itself is uncertain — the question is not "is this data valid?" but "does this entity exist in our business ontology?"

**Continuous model monitoring**

AI model accuracy degrades over time as document formats evolve (model drift). Include monitoring tasks in the pipeline that:
- Track the ratio of true positive vs. false positive exceptions over time
- Alert when the false positive rate exceeds an agreed threshold
- Trigger a retraining cycle using the accumulated true/false positive examples

| Rule | Severity | Description |
|---|---|---|
| DV-STG-001 | ERROR | Required metadata columns present |
| DV-STG-002 | ERROR | `dv_collisioncode` required in HKV mode |
| DV-STG-003 | ERROR | Hub hashkeys present for each hub fed |
| DV-STG-004 | ERROR | Link + participant hashkeys present for each link fed |
| DV-STG-005 | ERROR | `dv_hashdiff_<sat>` present for each satellite fed |
| DV-STG-006 | WARNING | Canonical dv-tag names used (no aliases) |
| DV-STG-007 | ERROR | Hashdiff must NOT use `UPPER()` or `LOWER()` |
| DV-STG-008 | ERROR | No business logic in staging (passthrough only) |
| DV-STG-009 | ERROR | **Never add artificial microseconds to `dv_load_timestamp`** to force uniqueness of duplicate records. Adding microseconds makes two records that arrived in the same load appear to represent two separate load events — this corrupts the audit trail. If duplicate records arrive with the same BK + timestamp, use Trap & Reject to quarantine them and alert the source. The source application must supply distinct records; the vault must not manufacture false uniqueness. |

---

## Delta views — intermediate change-detection layer

An optional reusable layer of views between staging and loading that computes the **difference** between staged content and target vault tables. The delta view encapsulates the `WHERE NOT EXISTS` (satellite) or `WHEN NOT MATCHED` (hub) logic in a single place.

**Pattern:**
```sql
CREATE OR REPLACE VIEW delta_sat_customer_profile AS
SELECT stg.*
FROM stg_customer_profile stg
WHERE NOT EXISTS (
    SELECT 1 FROM SAT_RV_HUB_CUSTOMER_PROFILE tgt
    WHERE stg.dv_hashkey_hub_customer = tgt.dv_hashkey_hub_customer
    QUALIFY ROW_NUMBER() OVER (PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp DESC) = 1
      AND stg.dv_hashdiff_sat_customer_profile = tgt.dv_hashdiff
);
```

**Advantages:**
- **Idempotent** — after a successful load, the delta view returns zero rows (the records now exist in the target). Safe to re-run the pipeline.
- **Debugging** — query the delta view at any time to see what would be loaded next; a non-zero result set after a load indicates a problem.
- **Decoupled** — load code simply reads from the delta view (`INSERT INTO ... SELECT * FROM delta_...`); change-detection logic is maintained in one place.

**When to use:** Useful for complex environments with many staging sources or when debugging pipelines. Not required for simple single-source vaults where the `WHERE NOT EXISTS` inline in the loader is sufficient.

---

## Subagent files

- Staging Validator: `agents/staging-validator.md`
- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- Naming Advisor: `agents/naming-advisor.md`
