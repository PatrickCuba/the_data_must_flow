---
name: dv-model
description: Design a specific Data Vault 2.0 construct — hub, link, satellite, PIT, bridge, or same-as link. Subcommands: hub | link | satellite | pit | bridge | sal
enabled: true
---

# /dv-model — Design a DV2.0 Construct

Design a specific vault construct with full column definitions, naming, and rationale.

## Subcommands

### `/dv-model hub`

Design a hub table for a business concept.

**Ask the user:**
1. What is the business concept? (customer, product, account, etc.)
2. What is the natural business key? (the identifier used by the business, not a DB surrogate)
3. What source system does it come from?

**Produce:**
```sql
-- HUB_<NAME>  (DVOS canonical column names)
dv_hashkey_hub_<name>   <hashkey_type>    NOT NULL   -- hash of BKCC + business key (record source is NOT in the hash)
<bk_column>             VARCHAR(...)      NOT NULL   -- natural business key
dv_tenant_id            <tenant_id_type>
dv_collisioncode        <collisioncode_type>
dv_applied_timestamp    TIMESTAMP_NTZ     NOT NULL
dv_recordsource         VARCHAR(255)      NOT NULL
dv_load_timestamp       TIMESTAMP_NTZ     NOT NULL
dv_task_id              <task_id_type>
dv_jira_id              <jira_id_type>
dv_user_id              <user_id_type>
last_seen_date          TIMESTAMP_NTZ
PRIMARY KEY (dv_hashkey_hub_<name>)
```

**Rules:**
- One business key per hub. If the user wants multiple keys for the same entity, create a same-as link.
- Hash algorithm is project-configured (SHA1 or MD5). Hash key = `hash_fn(UPPER(CONCAT(bkcc || '||' || COALESCE(NULLIF(TRIM(bk), ''), '-1'))))`. **BKCC (`dv_collisioncode`) is the discriminator, not record source.**
- No descriptive attributes in the hub — those belong in satellites
- Hub name is singular: HUB_CUSTOMER not HUB_CUSTOMERS

---

### `/dv-model link`

Design a link table connecting two or more hubs.

**Ask the user:**
1. Which hubs does this link connect?
2. What is the source table or relationship name?
3. Is this relationship time-limited? (if yes, pair with an effectivity satellite)
4. Can the same pair of hub keys appear multiple times? (if yes, add a driving key or dependent-child pattern)

**Produce:**
```sql
-- LNK_<NAME>  (DVOS canonical column names — no FK constraints, deferred to orphan-check)
dv_hashkey_lnk_<name>   <hashkey_type>    NOT NULL
dv_hashkey_hub_<hub_a>  <hashkey_type>    NOT NULL
dv_hashkey_hub_<hub_b>  <hashkey_type>    NOT NULL
dv_tenant_id            <tenant_id_type>
dv_applied_timestamp    TIMESTAMP_NTZ     NOT NULL
dv_recordsource         VARCHAR(255)      NOT NULL
dv_load_timestamp       TIMESTAMP_NTZ     NOT NULL
dv_task_id              <task_id_type>
dv_jira_id              <jira_id_type>
dv_user_id              <user_id_type>
last_seen_date          TIMESTAMP_NTZ
PRIMARY KEY (dv_hashkey_lnk_<name>)
```

**Rules:**
- Links are insert-only — never update or delete
- If the relationship has descriptive attributes, put them in `SAT_LNK_<NAME>`
- If the relationship has a lifecycle (active/inactive), add `SAT_LNK_<NAME>_EFF` (effectivity satellite)
- FK constraints intentionally omitted — deferred to orphan-check post-load phase
- If there are more than 5 hub keys in a link, question whether this is a correct model

---

### `/dv-model satellite`

Design a satellite for a hub or link. Chooses the right variant.

**Ask the user:**
1. Which hub or link does this satellite hang from?
2. What attributes does it track? (paste column list)
3. Does each row represent a snapshot in time, or can multiple rows be active simultaneously?
4. Are the attributes sensitive / PII?
5. Is this a reference table that rarely changes?

**Spawn the Pattern Recommender subagent** (see `agents/pattern-recommender.md`) to choose the variant, then produce the DDL.

**Standard satellite:**
```sql
-- SAT_<PARENT>_<CONTEXT>  (DVOS canonical column names)
-- No end-date column. Current row via QUALIFY ROW_NUMBER() in views.
dv_hashkey_hub_<parent>  <hashkey_type>    NOT NULL
dv_tenant_id             <tenant_id_type>
dv_task_id               <task_id_type>
dv_jira_id               <jira_id_type>
dv_user_id               <user_id_type>
dv_recordsource          VARCHAR(255)      NOT NULL
dv_hashdiff              <hashdiff_type>   NOT NULL
dv_applied_timestamp     TIMESTAMP_NTZ     NOT NULL
dv_load_timestamp        TIMESTAMP_NTZ     NOT NULL
<attribute columns>
PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp)
```

**Variant differences** (Pattern Recommender chooses):

| Variant | Key difference |
|---|---|
| Standard | One active row per parent key |
| Multi-active | `dv_sequence` + `dv_load_timestamp` composite PK; multiple active rows |
| Effectivity | `dv_start_date` + `dv_end_date`; link-only, driver-key driven, insert-only, **no business attributes** |
| Dependent-child | Adds a child key column to the PK; parent key is not unique alone |
| Non-historized | No `dv_hashdiff`; no QUALIFY pattern needed; latest insert is authoritative |
| PII | Naming suffix (`_pii`) on any satellite type — segregates sensitive columns into separate physical table |

---

### `/dv-model pit`

Design a Point-in-Time (PIT) table for a hub.

**Ask the user:** Which hub? Which satellites should be included in the PIT?

**Produce:**
```sql
-- PIT_<HUB>  (DVOS dynamic table — per-satellite columns forward-filled via LAST_VALUE IGNORE NULLS)
dv_hashkey_hub_<hub>              <hashkey_type>    NOT NULL
SNAPSHOT_DATE                     DATE              NOT NULL
<sat1_alias>_dv_applied_timestamp TIMESTAMP_NTZ     -- temporal alignment; NULL if no record at snapshot
<sat1_alias>_dv_hashkey_hub_<hub> <hashkey_type>    -- ghost key if no record (LAST_VALUE IGNORE NULLS)
-- repeat per satellite
PRIMARY KEY (dv_hashkey_hub_<hub>, SNAPSHOT_DATE)
```

**Rules:**
- A ghost record (all-zero hash key) must exist in every satellite for the null-join to work
- PIT tables are rebuilt on a schedule, not incrementally loaded

---

### `/dv-model bridge`

Design a Bridge table to pre-join a hub to its related links for query performance.

**Ask the user:** Which hub is the anchor? Which links and their connected hubs should be traversable?

**Produce:**
```sql
-- BDG_<HUB>_<CONTEXT>  (DVOS: manifest name = bdg_*, output_table = BDG_* — BRDG_ not permitted)
dv_hashkey_hub_<anchor_hub>     <hashkey_type>    NOT NULL
SNAPSHOT_DATE                   DATE              NOT NULL
dv_hashkey_lnk_<lnk1>          <hashkey_type>
dv_hashkey_hub_<related_hub>   <hashkey_type>
-- repeat for each link in scope
PRIMARY KEY (dv_hashkey_hub_<anchor_hub>, SNAPSHOT_DATE)
```

## After each construct

Ask:
> “Should I run doctrine validation on this definition? Use `/dv-validate` or say yes to check it now.”

---

### `/dv-model sal`

Design a Same-As Link (SAL) — a raw vault entity that connects two records in the same hub that represent the same real-world business entity. Used for deduplication and entity resolution. The SAL lives in the raw vault, not a separate business layer.

**Ask the user:**
1. Which hub are the two records from?
2. What is the source of the match assertion (manual curation, matching algorithm, MDM system)?
3. Is there a master/duplicate directionality, or are they symmetric?

**Produce:**
```sql
-- SAL_<ENTITY>  (same-as link — no FK constraints, deferred to orphan-check)
dv_hashkey_sal_<entity>      <hashkey_type>    NOT NULL
dv_hashkey_hub_<entity>_a    <hashkey_type>    NOT NULL
dv_hashkey_hub_<entity>_b    <hashkey_type>    NOT NULL
dv_tenant_id                 <tenant_id_type>
dv_applied_timestamp         TIMESTAMP_NTZ     NOT NULL
dv_recordsource              VARCHAR(255)      NOT NULL
dv_load_timestamp            TIMESTAMP_NTZ     NOT NULL
dv_task_id                   <task_id_type>
dv_jira_id                   <jira_id_type>
dv_user_id                   <user_id_type>
PRIMARY KEY (dv_hashkey_sal_<entity>)
```

**Always pair with an effectivity satellite:**
```sql
-- SAT_SAL_<ENTITY>_EFF  (effectivity satellite — tracks when the match assertion is active)
-- Link-only, insert-only, driver-key driven. NO business attributes.
dv_hashkey_sal_<entity>   <hashkey_type>    NOT NULL   -- FK to SAL_<ENTITY>
dv_tenant_id              <tenant_id_type>
dv_task_id                <task_id_type>
dv_jira_id                <jira_id_type>
dv_user_id                <user_id_type>
dv_recordsource           VARCHAR(255)      NOT NULL
dv_hashdiff               <hashdiff_type>   NOT NULL
dv_start_date             TIMESTAMP_NTZ     NOT NULL   -- start of active period (loader-set from driver key)
dv_end_date               TIMESTAMP_NTZ     NOT NULL   -- high-date when open; set when assertion ends
dv_applied_timestamp      TIMESTAMP_NTZ     NOT NULL
dv_load_timestamp         TIMESTAMP_NTZ     NOT NULL
PRIMARY KEY (dv_hashkey_sal_<entity>, dv_load_timestamp)
```
**Note on optional attributes** (confidence score, match reason, etc.): these are **business attributes** and belong in a separate standard satellite `SAT_SAL_<ENTITY>_CONTEXT`, not in the effectivity satellite. Effectivity satellites in DVOS have no business attributes.

**Rules:**
- The SAL is insert-only like all raw vault entities
- Both hub hash keys must already exist in `HUB_<ENTITY>`
- SAL hash key is computed from both hub hash keys (not record source)
- The effectivity satellite tracks whether the assertion is currently active — without it the SAL has no lifecycle
- A SAL does not merge records — it asserts that two hub keys refer to the same entity; survivorship logic lives in the Information Mart
- Optional match metadata (confidence score, match reason, etc.) belong in a separate standard satellite `SAT_SAL_<ENTITY>_CONTEXT`, not in the effectivity satellite

## After each construct

Ask:
> "Should I run doctrine validation on this definition? Use `/dv-validate` or say yes to check it now."

## Subagent files

- Pattern Recommender: `agents/pattern-recommender.md`
- Naming Advisor: `agents/naming-advisor.md`
