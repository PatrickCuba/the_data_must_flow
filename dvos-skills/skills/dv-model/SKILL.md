---
name: dv-model
description: Design a specific Pragmatic Data Vault construct — hub, link, satellite, PIT, bridge, or same-as link. Subcommands: hub | link | satellite | pit | bridge | sal
enabled: true
---

# /dv-model — Design a Pragmatic DV Construct

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
- Hash algorithm is project-configured (MD5, SHA1, or SHA256). Hash key = `hash_fn(UPPER(CONCAT(bkcc || '||' || COALESCE(NULLIF(TRIM(bk), ''), '-1'))))`. **BKCC (`dv_collisioncode`) is the discriminator, not record source.**
- No descriptive attributes in the hub — those belong in satellites
- Hub name is singular: HUB_CUSTOMER not HUB_CUSTOMERS
- **Business key must always be VARCHAR** — never INT, NUMBER, or BIGINT, even when the source stores it as a number
- **BKCC (`dv_collisioncode`) is a namespace ID**, not a source-system ID. Default to `'default'` unless two sources have genuinely overlapping key spaces for different entities. Do not default BKCC to the source-system name — this creates unnecessary hub duplication.
- **No Business Vault hubs** — hubs exist in the Raw Vault only. Business keys are manufactured exclusively by source systems and applications, never derived inside the analytics platform. If a user asks for a BV hub, flag it as `DV-BV-120 ERROR` and explain the correct alternative (BV satellite or BV link).

**Seven hub naming principles**

When naming a hub table, apply these seven principles in order:

| Principle | Meaning | Anti-example → Correct |
|---|---|---|
| **Consistency** | Each business concept has exactly one name — no synonyms in different parts of the model | `HUB_CLIENT` and `HUB_CUSTOMER` for the same entity → pick one |
| **Understandability** | The name describes the concept in domain-specific business terms | `HUB_RECORD` → `HUB_LOAN_ACCOUNT` |
| **Specificity** | Neither overly vague nor overly precise | `HUB_THING` (too vague) or `HUB_VISA_PLATINUM_CARD_ACCOUNT` (too narrow) |
| **Brevity** | Neither too short nor too long; avoids unnecessary filler words | `HUB_ACCOUNT_DATA_RECORD` → `HUB_ACCOUNT` |
| **Searchability** | Easily found in code, docs, and queries; avoids names so generic they match everything | `HUB_USER` clashes with system users → `HUB_CUSTOMER` is more domain-specific |
| **Pronounceability** | Easy to say in a meeting or conversation | `HUB_ACCT_GRP_INST` → `HUB_ACCOUNT_GROUP` |
| **Austerity** | Not clever; avoids jargon, acronyms, or temporary concepts | `HUB_UNICORN_ENTITY` → `HUB_STARTUP` |

**Anti-pattern checks — before producing hub DDL, warn the user if:**

| Check | Warning | Code |
|---|---|---|
| Proposed BK column type is numeric (INT, NUMBER, BIGINT) | "Business keys must be VARCHAR. Cast the column to string — numeric types are fragile (zero-padding, leading zeros, type coercion)." | WARN-BK-001 |
| Proposed BK is a reference code / short category value (status, type, code) | "Reference codes are not business keys — they are attributes. Consider a dependent-child key or reference enrichment instead." | WARN-HUB-REF |

**Reference data doctrine: reference tables or RDM — not hub+satellite**

Reference data (lookup codes, status types, product categories, hierarchical reference structures like a chart of accounts) must **not** be modelled as hub and satellite tables. Doing so:
- Creates unnecessary joins for every query that uses the reference data
- Pollutes the hub layer with entries that are not true business objects
- Produces a "constellation" data model rather than a clean vault topology

**Correct approach:**
- Simple code-description pairs → load to a dedicated **reference table** with change tracking (no hash key, no DV metadata columns needed — or a Non-historised Satellite off a dedicated reference hub if change history matters)
- Complex hierarchical reference data (e.g. chart of accounts, product taxonomy) → manage in a **Reference Data Management (RDM) solution** or reference tables; access in vault queries via a simple lookup join
- Reference data that qualifies as a genuine business object (has its own immutable BK used by the business to identify it) → hub + satellite is correct; but validate this carefully with domain experts before modelling

If you find yourself needing a `type_code` join on every query, it is a signal that the reference data was incorrectly modelled as a hub.
| Proposed BK includes a date, timestamp, or sequence number | "Keyed-instance-hub anti-pattern: dates and non-identifier data in the hub key create technical debt. The key should identify the entity, not a snapshot of it. Move dates to a satellite." | ERR-HUB-KIH |
| No clear business owner or business recognition of the key | "Weak hub risk: if the business does not use this key to refer to this entity, it may not be a real business object. Confirm with a business owner before generating." | WARN-HUB-WEAK |
| BKCC proposed as source-system name (e.g. 'SALESFORCE', 'SAP', 'CRM') | "BKCC should be a business key namespace, not a source-system name. Use 'default' unless two sources genuinely use the same key value for different entities." | WARN-BK-002 |
| Proposed BK appears to be a smart key (contains embedded type codes, year, country, or sequence patterns, e.g. `INV-2024-001`) AND the user wants to extract components as separate hub columns | "Smart key parsing anti-pattern: extracted components (type, year, sequence) are attributes, not identifiers. Store the full smart key as the BK (opaque string). Derive components in a satellite or BV satellite." | WARN-HUB-SMART |
| BK described as a concatenation of multiple component keys (e.g. `branch_code \|\| account_number` combined into one column) | "Concatenated key anti-pattern: model the composite key correctly as a multi-column BK in a properly named hub (e.g. HUB_BANK_ACCOUNT with branch_code + account_number). Never concatenate to force a composite key into a single-column hub — this pushes integration debt to the IM layer where every query must de-concatenate it." | WARN-HUB-CONCAT |
| Hub name is semantically generic (e.g. `HUB_ACCOUNT`, `HUB_ENTITY`, `HUB_RECORD`) when the domain has semantically distinct sub-types | "Generic hub name — check sub-type specificity. If the business distinguishes between card_account, loan_account, and deposit_account as different concepts with different keys and different attributes, each should be its own hub. A hub named `HUB_ACCOUNT` that needs a `type_code` column to distinguish record types is an overloaded hub. Use a published business ontology (e.g. FIBO for financial services) as a starting guide, then validate with domain experts." | WARN-HUB-GENERIC |

**Using industry ontologies for hub naming**

When establishing hub names, a published **business ontology** for the industry (e.g. FIBO for financial services, HL7 FHIR for healthcare, GS1 for retail/supply chain) provides a taxonomy of semantically distinct business concepts as a starting point. Use the ontology to identify whether a proposed generic name (like "account") should be split into semantically distinct sub-types (card_account, loan_account, deposit_account).

**Important:** ontologies are a guide, not an authority. Always validate hub name definitions and granularity with domain experts and business stakeholders. If no published ontology exists for the domain, build one through mob modelling, domain storytelling, or event storming before starting the vault model.

**Super-type/sub-type resolution — separate hubs + link**

When a source system uses type inheritance (super-type/sub-type pattern, e.g. `ACCOUNT` with child types `CARD_ACCOUNT`, `LOAN_ACCOUNT`, `DEPOSIT_ACCOUNT`), model as:
- Separate hub tables per sub-type (each with its own grain and business key)
- An umbrella/group hub for the super-type if the business uses a shared parent ID
- A link table capturing the super-type ↔ sub-type relationship

**Never merge sub-type grains into a single hub.** A hub with a `type_code` column to distinguish record types is an overloaded hub (weak hub anti-pattern). The super-type hub captures the parent business key; sub-type hubs capture the specific identifiers the business uses at the granular level.

**Party model warning — WARN-HUB-PARTY**

Industry data models (especially in financial services) often use a **party** super-type representing the generic concept of "an entity party to a transaction or contract" — with person and organisation as sub-types. Do not impose a party model unless the business actually uses party semantics to refer to its entities.

Signs that a party model is inappropriate:
- The business calls their entities "customers" and "suppliers" — not "parties"
- A `HUB_PARTY` with a `type_code` column to distinguish person from organisation (Weak Hub / Bag of Keys)
- The party model adds a level of indirection every query must navigate without delivering business value

If a party model is justified (e.g. the source system is modelled as parties, or the business explicitly manages counterparty relationships): let the **business define what constitutes a party**, not the data engineering team. Hub naming and grain for a party model must be validated by domain experts who understand the business ontology — not inferred from source-system table names.

**Hub Decision Matrix — same entity or separate hubs?**

When two sources supply what appears to be the same business concept, use this 2×2 matrix to decide whether they share one hub or need separate hubs:

| | **Same granularity** | **Different granularity** |
|---|---|---|
| **Same semantic meaning** | ✅ One hub — integrate via BKCC if key spaces overlap | ⚠️ One hub per grain — link the grains with a hierarchical link |
| **Different semantic meaning** | ❌ Separate hubs — same key format does not mean same entity | ❌ Separate hubs — different concept AND different grain |

**How to apply:**
1. **Semantic meaning**: Do both sources use this key to refer to the same real-world business concept? Ask the business, not the tech team.
2. **Granularity**: Is one key a summary/aggregate of the other? (e.g. account vs. sub-account, order-header vs. order-line)
3. If same meaning + same grain → single hub, use BKCC to separate overlapping key spaces
4. If same meaning + different grain → separate hubs at each grain level, linked by a hierarchical link
5. If different meaning (regardless of grain) → separate hubs, even if the key column name or format is identical

**Hub sprawl → link sprawl — named anti-pattern**

Hub sprawl occurs when a data modeller models dependent codes and sub-keys as hub tables rather than as satellite attributes or dependent-child keys. The cascade effect:

1. Every modelled "hub" needs a link table to join it to its parent hub
2. Every link table produces link-satellite tables that must either inherit the dep-child key or force downstream IM queries to compensate for it
3. IM queries become complex switch architectures where every query must navigate multiple link/satellite hops that could have been a single satellite column

**Fix:** push dependent-child keys into satellite tables. If a code or sub-key depends on a parent key to uniquely identify it, it is a dep-child key — not a hub business key. Use WARN-HUB-REF when a reference code is proposed as a business key. The model stays compact, join paths stay short, and IM queries remain simple.

Note: hub tables remain essential as the integration layer between source systems (horizontal integration) and the canonical business key map (vertical mapping). Hub sprawl is distinct from the correct use of multiple hubs for semantically distinct business objects.

**Hub & Link Decision Flowchart**

Use this decision tree when profiling a source entity to determine the correct vault construct:

```
1. Identify the business object in the source
   │
   ├─ Is it a business ENTITY (thing with an identity)?
   │   │
   │   ├─ Does it have a single immutable business key?
   │   │   └─ YES → HUB (standard)
   │   │
   │   ├─ Does it have a composite key (two+ columns)?
   │   │   └─ YES → HUB with multi-column BK (never concatenate)
   │   │
   │   └─ Is the key dependent on a parent key?
   │       └─ YES → Dep-child key in SATELLITE (not a hub)
   │
   ├─ Is it a RELATIONSHIP or TRANSACTION (event connecting entities)?
   │   │
   │   ├─ How many entities participate?
   │   │   └─ Model ALL participants in ONE link (unit of work)
   │   │
   │   ├─ Can the same combination repeat over time?
   │   │   └─ YES → Add driving key or dep-child pattern
   │   │
   │   └─ Does the relationship have a lifecycle (active/inactive)?
   │       └─ YES → Pair with effectivity satellite
   │
   └─ Is it REFERENCE DATA (codes, lookups, categories)?
       └─ YES → Reference table or RDM — NOT a hub
```

---

### `/dv-model link`

Design a link table connecting two or more hubs.

**Unit of Work (UoW) — the grain of a link**

A link captures one **Unit of Work**: the complete set of business entities involved in a single transaction, relationship, or event as the source system records it. The link grain should match the source grain — it must be possible to recreate the source record from the link and its satellite data at any point in time.

If a mortgage application involves customer + account + property + broker, that is a 4-way link (`LNK_MORTGAGE_APPLICATION`). **Do not decompose it into four 2-way links.** Decomposing a multi-entity UoW:
- Breaks the unit of work and makes source recreation impossible (fails auditability)
- Adds latency: downstream consumers must reassemble what was broken
- Increases join complexity and query cost
- Creates staggered dependency chains between link tables

The rule: **one link per unit of work**. Ask: "If I had to recreate the source record, do I have everything I need from this link + its satellite?" If the answer is no, the link has been incorrectly decomposed.

**Ask the user:**
1. What is the source transaction or relationship event? (this defines the UoW)
2. Which hubs does this unit of work involve? (all participants, not just two)
3. Is this relationship time-limited? (if yes, pair with an effectivity satellite)
4. Can the same combination of hub keys appear multiple times? (if yes, add a driving key or dependent-child pattern)

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
- If the relationship has descriptive attributes, put them in `SAT_RV_LNK_{badge}_{file}`
- If the relationship has a lifecycle (active/inactive), add `SAT_EF_RV_LNK_{badge}_{file}` (effectivity satellite)
- FK constraints intentionally omitted — deferred to orphan-check post-load phase
- If there are more than 5 hub keys in a link, question whether this is a correct model
- **Dep-child key immutability constraint** — a dependent-child key placed in a link table MUST be immutable for the life of that relationship. If the degenerate value can become inactive, change state, or be updated, placing it in the link is incorrect — it belongs in a satellite instead. The link hash does NOT include the dependent-child key. Test: "will this value ever change for this relationship instance?" If yes → satellite column, not link column.
- **Link deprecation protocol** — when a new business entity participant column appears in an existing source file's unit of work:
  1. Create a **new** link table with all participants (old + new)
  2. **Deprecate** the old link table — stop loading new records to it
  3. Do NOT delete the original link table, especially within a data retention window (regulatory reporting may require querying historical data)
  4. Update downstream PITs, bridges, and IM views to point to the new link
  5. Adding a new participant to an existing link (ALTER TABLE ADD COLUMN) is an anti-pattern — it changes the grain and produces NULL FKs for all historical records, representing incorrect facts
- **Never break a multi-participant UoW into two-hub links** — if a source business process involves 3 or more business objects in a single unit of work (e.g. an order involving a customer, a product, and a warehouse), model them all as a single link with all participant FK columns. Splitting into multiple 2-hub links destroys the integrity of the unit of work: the Cartesian product of two 2-hub links creates relationships that never existed in the source data (false positives). The source's unit of work is the authority — model it faithfully. See `/dv-explain multi-participant-link`.
- **Zero-key**: null or unknown participant business keys are coalesced to the zero-key (all-zeros hash, the ghost record) in staging. An `INNER JOIN` on a link always resolves — the zero-key joins to the ghost row in the parent hub. This makes the link cardinality-agnostic (M:M, 1:M, 0:M) without any model change.
- **Effectivity satellite trigger**: if the relationship can return to a previous state (e.g. an account can be closed and then re-opened), add an effectivity satellite. Without it, the vault cannot distinguish a relationship ending from it never having existed. **Do not add start/end date columns to the link table itself** — this breaks the link pattern and forces timeline resolution into every query that traverses the link.
- Do not add `null` FK columns or optional participant columns to a link — use the zero-key approach instead. A link with nullable FK columns is called a **peg-legged link** — this is a named anti-pattern in Pragmatic DV. Absent participants must be coalesced to the zero-key at staging time so all FK columns are always populated and equi-joins always resolve.
- **Dep-child keys belong in satellite tables, not link tables** — loading dep-child keys into the link hash is an anti-pattern. It fragments link timelines, breaks the "shortest path between hub tables" principle, and complicates relationship state tracking. Use a satellite with the dep-child key as a regular NOT NULL column (not in the PK). The satellite PK remains `(hashkey, dv_sequence, dv_load_timestamp)` — load logic is scoped to `(hashkey, dep_child_key)` per row (see `/dv-explain dep-child-in-link`).
- **Effectivity satellite: use sparingly** — prefer getting the relationship status indicator directly from the source application as a link-satellite attribute. Only use an effectivity satellite when the source cannot supply an active/inactive indicator. If you know an EFS will be needed, model it from the start — adding it later loses all historical relationship change context. See `/dv-explain effectivity-sparingly`.
- **Multiple sources claiming the same relationship — one link or many?** When two source systems both supply the same real-world relationship (e.g. CRM and billing both assert account-to-product), there are two options:
  1. **One link, multiple record-sources** — both feeds load into the same link table. Relationship history from all sources is consolidated. The correct default when sources represent the same relationship. `dv_recordsource` distinguishes origin.
  2. **Separate links per source** — two link tables (e.g. `LNK_ACCOUNT_PRODUCT_CRM`, `LNK_ACCOUNT_PRODUCT_BILLING`). Use this when the sources represent *structurally different* relationships that should not be merged (different business meaning, different granularity, different lifecycle rules).
  The BV layer can unify separate source links into a single BV link representing the consolidated business view. See `/dv-bv` for BV link patterns.

---

### `/dv-model satellite`

Design a satellite for a hub or link. Chooses the right variant.

**Ask the user:**
1. Which hub or link does this satellite hang from?
2. What attributes does it track? (paste column list)
3. Does each row represent a snapshot in time, or can multiple rows be active simultaneously?
4. Are the attributes sensitive / PII?
5. Is this a reference table that rarely changes?
6. **Late-arriving data check**: Does this source have a history of late-arriving or out-of-sequence records — i.e. records that arrive with an `applied_timestamp` older than the most recently loaded record in the satellite?

**For question 1 — hub vs link placement:**

If the user says the satellite hangs from a **link**, confirm with this test:
> "Does this attribute exist without the other business object in this interaction? If yes → it belongs in a hub satellite. If no → it belongs on the link."

All satellite variants (standard, dep-child, MSAT, PMAS, NH, PII, effectivity) can hang from a link. The variant selection logic below applies identically — the only difference is the parent hash key (`dv_hashkey_lnk_*` instead of `dv_hashkey_hub_*`).

Common link-satellite patterns:
- **Standard link-sat**: relationship state that changes over time (e.g. contract terms between two parties)
- **Dep-child link-sat**: individual transactions/events (transaction_id or event_datetime as dep-child key)
- **NH link-sat**: latest reference value for the relationship (e.g. current SLA between vendor and warehouse)
- **Link-sat with measures**: carries amounts, quantities, counts — often drives fact bridge construction (see `/dv-pit-bridge`). This is not a separate type — it's a standard or dep-child link satellite that happens to contain metrics.

For question 6, act on the answer as follows:

| Answer | Action |
|---|---|
| Yes | Set `xts_assisted: true` in the satellite manifest. Route to `/dv-xts` for the XTS DDL and load pattern. Add `dv_xts_event VARCHAR(20)` to the satellite DDL. |
| No | No action. Standard satellite load applies. |
| Using Kappa Vault | XTS is **incompatible** with Kappa Vault. Stream-triggered tasks do not have a batch boundary at which to evaluate the out-of-sequence SWITCH. Do not set `xts_assisted: true` for any Kappa Vault satellite. |

**Satellite Variant Decision Flowchart**

Use this tree after determining attributes belong in a satellite:

```
0. Does this satellite require sub-300ms OLTP latency (ODV use case)?
   │
   ├─ YES → HYBRID SATELLITE (Snowflake Hybrid Table — see /dv-explain hybrid)
   │         Then continue below to choose the logical variant (standard/MA/dep-child)
   │
   └─ NO → continue (standard Snowflake table)
       │
       1. Should these attributes be split from other attributes?
       │
       ├─ YES (different change rate, PII segregation, or different source)
       │   └─ Create a separate satellite (split by rate-of-change or sensitivity)
       │
       └─ NO → continue
           │
           2. Does the satellite hang from a HUB or a LINK?
           │
           ├─ HUB → continue to step 3
           │
           └─ LINK → Is it tracking relationship lifecycle (no business attrs)?
               ├─ YES → EFFECTIVITY SATELLITE
               └─ NO → Standard or dep-child link-satellite → continue to step 3
                   │
                   3. Is this a non-historized reference (overwrite, no history needed)?
                   │
                   ├─ YES → NON-HISTORIZED SATELLITE
                   │
                   └─ NO → continue
                       │
                       4. Are attributes unique to the parent key alone?
                       │
                       ├─ YES → STANDARD SATELLITE
                       │
                       └─ NO → Is there a child key that creates sub-grain?
                           │
                           ├─ YES → DEPENDENT-CHILD SATELLITE
                           │         │
                           │         └─ Do multiple independent subsets need
                           │            separate versioning per child key?
                           │             │
                           │             ├─ YES → PMAS (Partitioned Multi-Active)
                           │             └─ NO  → Standard dep-child satellite
                           │
                           └─ NO → Can multiple rows be active simultaneously?
                               │
                               ├─ YES → MULTI-ACTIVE SATELLITE
                               └─ NO → Re-examine grain (possible data quality issue)
```

**Spawn the Pattern Recommender subagent** (see `agents/pattern-recommender.md`) to choose the variant, then produce the DDL.

**Source-badge — mandatory satellite naming component**

Every satellite table name MUST include a **source-badge**: a short acronym from a governed registry identifying the source system instance. Format: `SAT_<PARENT>_<SOURCE_BADGE>_<CONTEXT>`. The source-badge:
- Distinguishes multiple instances of the same software (e.g. `SAP1`, `SAP2`)
- Is distinct from BKCC — BKCC is a key namespace discriminator, source-badge is a table naming element
- Comes from the same source badge registry used in staging (`/dv-stage` source badge registry)
- Ensures satellites from different sources are never accidentally merged at the physical level

Examples: `SAT_RV_HUB_CUSTOMER_SFRC_PROFILE`, `SAT_RV_HUB_CUSTOMER_MDM_DEMO`, `SAT_RV_LNK_ORDER_XERO_DETAIL`

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

**Open Host Service (OHS) — semi-structured VARIANT satellite pattern**

When a source system operates on a schema-change cadence that is uncontrollable (versioned SaaS APIs, partner feeds, event streams with evolving payloads), use the OHS pattern: store the full source payload as a Snowflake `VARIANT` column in the satellite body. Only the key columns and stable metadata columns are materialised as typed DDL columns.

```sql
-- OHS satellite: key columns typed, payload in VARIANT
CREATE TABLE SAT_RV_HUB_PARTNER_PAYLOAD (
    dv_hashkey_hub_partner  BINARY(20)     NOT NULL,
    dv_load_timestamp       TIMESTAMP_NTZ  NOT NULL,
    dv_applied_timestamp    TIMESTAMP_NTZ  NOT NULL,
    dv_recordsource         VARCHAR(255)   NOT NULL,
    dv_hashdiff             BINARY(20)     NOT NULL,
    api_version             VARCHAR(20),              -- only truly stable key columns typed
    payload                 VARIANT        NOT NULL,  -- full source document
    PRIMARY KEY (dv_hashkey_hub_partner, dv_load_timestamp)
);
```

Downstream IM consumers then extract only the attributes they need using Snowflake semi-structured path notation:

```sql
SELECT
    h.partner_bk,
    s.payload:customer_name::VARCHAR         AS customer_name,
    s.payload:contract_value::NUMBER(18,2)   AS contract_value
FROM HUB_PARTNER h
JOIN SAT_RV_HUB_PARTNER_PAYLOAD s ON h.dv_hashkey_hub_partner = s.dv_hashkey_hub_partner
QUALIFY ROW_NUMBER() OVER (PARTITION BY s.dv_hashkey_hub_partner ORDER BY s.dv_applied_timestamp DESC) = 1;
```

**When to use OHS vs. structured satellite:**
- OHS: source schema changes frequently and without notice; or source payload is intentionally schema-flexible (JSON API, event bus message)
- Structured: source schema is stable and agreed under a data contract; columns are known and typed

**OHS caveat:** VARIANT columns are not included in hashdiff computation by default (no `UPPER()`/null-substitute pattern applies cleanly to arbitrary JSON). Use a `SHA2(TO_JSON(payload))` or hash the raw stage string as the VARIANT hashdiff. Agree the approach at standards council time.

> **Dependent-child and multi-active satellites must only be modelled by design, never by default.** A standard satellite should load one state change per entity per load cycle. If staged content unexpectedly contains multiple rows per entity key, that is either a data quality issue or an indication that the source grain was not profiled correctly — not a reason to reach for dep-child or MA. Use these variants only when the source explicitly requires tracking multiple simultaneous active records per entity.

**Satellite placement diagnostic — `SELECT DISTINCT` smell**

If a satellite is placed on a link but its attributes only describe one of the link's participant entities (not the relationship itself), IM queries will require `SELECT DISTINCT` on the business key to get entity-level grain. This is a modelling mistake. The test: does this attribute exist without *both* participant entities being present? If yes, it belongs in a hub satellite, not a link satellite.

**"All the data all the time"** — load all source columns. If a source has 600 columns, split them appropriately across satellites and load all of them. Never cherry-pick. Selective loading breaks auditability: you can no longer recreate the source record. The cost of having columns you don't immediately need is storage. The cost of not having columns when you need them is a reload from source.

**No `CLUSTER BY` on satellite tables** — do not add explicit clustering to satellites. Natural load-order provides optimal zone map effectiveness for SNOPIT joins. See `/dv-pit-bridge` Rule 1 and the SNOPIT performance analysis for the full rationale.

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
- **SAL is NOT for source surrogate key mapping** — do not use a SAL to track a source application's auto-increment primary key alongside the business key. Surrogate keys are implementation artefacts that can change if a source is reloaded. They belong as attributes in a satellite table. SAL is for asserting that two *different business keys* refer to the same real-world entity (e.g. source system key ↔ MDM canonical key, or matching keys across two different source systems).

## After each construct

Ask:
> "Should I run doctrine validation on this definition? Use `/dv-validate` or say yes to check it now."

## Security / RBAC modelling pattern

When modelling users, groups, roles, and capabilities (access control) as a data vault:

| Construct | Role |
|---|---|
| `HUB_USER`, `HUB_ROLE`, `HUB_GROUP` | Business entities |
| `LNK_USER_ROLE`, `LNK_ROLE_GROUP` | Role assignments |
| `SAT_EF_RV_LNK_{badge}_USER_ROLE` | Effectivity satellite tracking when a user gained/lost a role |
| `BDG_USER_ROLE_PRIVILEGE` | Bridge table mapping user → role hierarchy → privileges |

Key rules:
- **Row-level security**: implement through the bridge table. Because bridges are disposable, regenerate when the role hierarchy changes — consumers always see the current access map.
- **Object-level security**: implement via Snowflake secure views using `CURRENT_ROLE()` or `IS_ROLE_IN_SESSION()`. The IM view filters rows based on the querying user's role, referencing the bridge for the current role-to-data mapping.
- **Effectivity satellites** track access changes (grant/revoke) with start/end dates. A revoked role has its end-date set; re-granting creates a new open record.
- This pattern enables point-in-time access auditing: "who had access to what at date X?" answered by querying the effectivity satellite timeline.

## Subagent files

- Pattern Recommender: `agents/pattern-recommender.md`
- Naming Advisor: `agents/naming-advisor.md`
