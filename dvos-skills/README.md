# DVOS Skills — Pragmatic Data Vault Builder for Snowflake

A Cortex Code plugin that guides you through designing, validating, generating, and deploying a Pragmatic Data Vault implementation on Snowflake. No prior tooling required — just a source system description and your Cortex Code session.

---

## What this is

These skills encode Pragmatic Data Vault methodology as interactive commands. 17 skills handle every phase of the vault-building process. They enforce 42 doctrine rules, suggest the right patterns, and produce Snowflake-optimized SQL you can use directly — without requiring any external framework or pre-built project.

Target platform: **Snowflake**. Generated SQL uses Snowflake-native features: MERGE with last_seen_date for hubs/links, Dynamic Tables for PIT, TRANSIENT schemas for staging, zero-copy cloning for dev/test.

---

## Prerequisites

- **Cortex Code** installed and running (CoCo desktop or CLI)
- No additional packages or databases required to design and model
- A Snowflake account when you are ready to execute the generated SQL

## Installation

Copy the plugin to CoCo's home directory:

```bash
cp -r /path/to/dvos-skills ~/.snowflake/cortex/plugins/dvos-data-vault
```

Restart CoCo (or open a new session). The `/dv-*` slash commands will be available immediately.

---

## Folder structure

```
dvos-skills/
│
├── .cortex-plugin/
│   └── plugin.json              Plugin manifest (skills + agents declarations)
│
├── README.md                    This file
│
├── skills/                      User-invokable skills (slash commands)
│   ├── dv-when/SKILL.md         /dv-when              Should I use Data Vault?
│   ├── dv-discover/SKILL.md     /dv-discover          Analyse sources → propose vault model
│   ├── dv-model/SKILL.md        /dv-model             Design a specific construct
│   ├── dv-validate/SKILL.md     /dv-validate          Check model against doctrine rules
│   ├── dv-generate/SKILL.md     /dv-generate          Produce SQL DDL + load patterns
│   ├── dv-stage/SKILL.md        /dv-stage             Staging view design + ingestion
│   ├── dv-mart/SKILL.md         /dv-mart              Build Information Mart views
│   ├── dv-explain/SKILL.md      /dv-explain           Explain any DV concept
│   ├── dv-bv/SKILL.md           /dv-bv                Business Vault links and satellites
│   ├── dv-bv-activity-schema/   /dv-bv-activity-schema  Activity Schema BV
│   ├── dv-pit-bridge/SKILL.md   /dv-pit-bridge        PIT tables and Bridge tables
│   ├── dv-load/SKILL.md         /dv-load              Task DAG orchestration
│   ├── dv-test/SKILL.md         /dv-test              Integrity test generation
│   ├── dv-deploy/SKILL.md       /dv-deploy            Deployment artifacts
│   ├── dv-xts/SKILL.md          /dv-xts               XTS late-arriving data
│   ├── dv-supernova/SKILL.md    /dv-supernova         Pre-materialised IM (DTs)
│   └── dv-migrate/SKILL.md      /dv-migrate           Platform migration
│
├── agents/                      Subagent instruction files (not invoked directly)
│   ├── doctrine-enforcer.md     42 doctrine rules, returns structured violation list
│   ├── source-profiler.md       Reads source schemas, surfaces business key signals
│   ├── pattern-recommender.md   Chooses the right satellite variant (10 options)
│   ├── sql-generator.md         Produces DDL and Snowflake-native load SQL
│   ├── naming-advisor.md        Checks all naming conventions
│   └── staging-validator.md     Validates staging views against DV-STG rules
│
├── examples/                    Runnable SQL examples (one per pattern)
│   ├── 01_standard_batch_vault.sql    Standard batch hub/link/sat/IM
│   ├── 02_satellite_variants.sql      All 8 satellite types + hybrid
│   ├── 03_kappa_vault.sql             Event-driven streams + tasks
│   ├── 04_xts_late_arriving.sql       XTS timeline correction
│   ├── 05_activity_schema.sql         Activity Schema BV pattern
│   ├── 06_supernova.sql               5-layer pre-materialised DTs
│   ├── 07_pit_bridge.sql              SNOPIT + Bridge as DTs
│   ├── 08_bv_link_and_sal.sql         BV link + SAL entity resolution
│   └── 09_not_a_good_fit.sql          When DV is overkill (medallion)
│
├── templates/                   Reusable SQL templates (DDL, loads, views)
│   ├── hub_ddl.sql              Hub CREATE TABLE
│   ├── link_ddl.sql             Link CREATE TABLE
│   ├── sat_standard_ddl.sql     Standard satellite DDL
│   ├── sat_multi_active_ddl.sql Multi-active satellite DDL
│   ├── sat_dependent_child_ddl.sql  Dependent-child satellite DDL
│   ├── sat_effectivity_ddl.sql  Effectivity satellite DDL (link-only)
│   ├── sat_status_tracking_ddl.sql  Status tracking satellite DDL
│   ├── sat_record_tracking_ddl.sql  Record tracking satellite DDL
│   ├── sat_extended_tracking_ddl.sql XTS satellite DDL
│   ├── load_hub_merge.sql       Hub MERGE load (INSERT + last_seen_date)
│   ├── load_lnk_insert.sql      Link INSERT (anti-semi-join, no last_seen)
│   ├── load_sat_insert.sql      Satellite INSERT (hashdiff anti-semi-join)
│   ├── ghost_record_insert.sql  Ghost record idempotent INSERT
│   ├── hashkey_computation.sql  SHA1_BINARY hashkey expression
│   ├── view_vc_current.sql      Current-state satellite view (ROW_NUMBER)
│   ├── view_vh_history.sql      History satellite view (LEAD end-date)
│   ├── stg_ef_secondary.sql     Effectivity secondary staging
│   ├── stg_rt_secondary.sql     Record tracking secondary staging
│   ├── stg_st_secondary.sql     Status tracking secondary staging
│   ├── stg_xt_secondary.sql     Extended tracking secondary staging
│   ├── asof_date_ddl.sql        ASOF calendar table (PIT cadence control)
│   ├── pit_hub_ddl.sql          PIT DDL (hub + 3 sats, FK constraints)
│   ├── pit_lnk_ddl.sql          PIT DDL (link + 3 sats)
│   ├── pit_hub_insert.sql       PIT standalone INSERT (correlated subquery)
│   ├── snopit_hub_ddl.sql       SNOPIT DDL (hub + 3 sats, dv_sid)
│   ├── snopit_lnk_ddl.sql       SNOPIT DDL (link + 3 sats)
│   ├── snopit_hub_insert.sql    SNOPIT standalone INSERT
│   ├── cpit_ddl.sql             Current PIT (TRANSIENT, thin schema)
│   ├── pit_snopit_multi_table_insert.sql  Multi-table INSERT (ASOF routing)
│   ├── bridge_relationship_ddl.sql  Bridge DDL + INSERT (metrics + dv_sid)
│   ├── dt_pit_hub.sql           DT PIT hub (LAG IGNORE NULLS, INCREMENTAL)
│   ├── dt_pit_lnk.sql           DT PIT link
│   ├── dt_snopit_hub.sql        DT SNOPIT hub
│   ├── dt_snopit_lnk.sql        DT SNOPIT link
│   ├── dt_cpit.sql              DT Current PIT (QUALIFY ROW_NUMBER)
│   ├── im_view_pit.sql          IM view consuming PIT (hashkey+appliedts)
│   └── im_view_snopit.sql       IM view consuming SNOPIT (dv_sid equi-join)
├── dmf/                         Data Metric Functions (DQ framework)
├── reference/                   Doctrine rules + naming conventions
└── hooks/                       Pre-generation doctrine check hook
```

### Skills vs. agents — what is the difference?

**Skills** (`skills/*.md`) are the commands you type. They guide the conversation, ask you questions, orchestrate the work, and present results.

**Agents** (`agents/*.md`) are subagent instruction files. You never invoke them directly. When a skill needs specialised work — profiling a schema, choosing a satellite variant, validating a construct — it spawns a focused subagent using the relevant instruction file. Each subagent has a single bounded job and returns structured output back to the skill.

This separation keeps each piece expert at one thing:

```
You → /dv-discover
         └── spawns Source Profiler     (reads your schema, finds BK candidates)
         └── spawns Pattern Recommender (one per entity, chooses construct type)
         → presents combined proposal to you
```

You never need to read the `agents/` files. They are there for Cortex Code to reference.

---

## The workflow

Building a vault typically follows this sequence. You do not have to complete every step in one session — each skill saves context in the conversation.

```
┌──────────────┐
│  /dv-when    │  Should I use Data Vault for this use case?
└──────┬───────┘
       │  yes
       ▼
┌──────────────┐
│ /dv-discover │  Paste DDL or describe your source tables.
│              │  Get back: hubs, links, satellite suggestions, open questions.
└──────┬───────┘
       │  model looks right
       ▼
┌──────────────┐
│  /dv-model   │  Define each construct in detail.
│  hub         │  Ask about business keys, source systems, attribute lists.
│  link        │  Produce full column definitions with hash key formulae.
│  satellite   │  Choose the right satellite variant automatically.
│  pit         │
│  bridge      │
│  sal         │  Same-as links for multi-source entity resolution.
└──────┬───────┘
       │  definitions ready
       ▼
┌──────────────┐
│ /dv-validate │  Check every construct against the 42 doctrine rules.
│  model       │  Reports violations and warnings. Never auto-fixes.
│  manifest    │  You decide what to do with each finding.
│  naming      │
└──────┬───────┘
       │  clean
       ▼
┌──────────────┐
│ /dv-generate │  Doctrine gate runs again (cannot be skipped).
│              │  Produces: CREATE TABLE DDL, MERGE/INSERT load patterns,
│              │            hash key expressions.
└──────┬───────┘
       │  SQL ready
       ▼
┌──────────────┐
│  /dv-load    │  Generate Snowflake Task DAGs with dependency ordering.
│              │  Sequential same-hub loads. Ghost record deployment.
└──────┬───────┘
       │  orchestration ready
       ▼
┌──────────────┐
│  /dv-deploy  │  Schema DDL, roles/grants, snow sql scripts, zero-copy clones.
└──────┬───────┘
       │  deployed
       ▼
┌──────────────┐
│  /dv-test    │  Integrity tests: uniqueness, orphans, ghosts, reconciliation.
└──────┬───────┘
       │  validated
       ▼
┌──────────────┐
│  /dv-mart    │  Build Information Mart views for BI tools.
│              │  Hash keys are never exposed — business keys only.
│              │  Supports current-state views and PIT-based history views.
└──────────────┘
```

At any point: `/dv-explain <concept>` to understand any term, rule, or decision.

---

## Skills reference

### `/dv-when` — Architecture decision guide

Use this before committing to Data Vault. It takes your context (number of sources, audit requirements, team experience, delivery timeline) and gives you a clear recommendation: Data Vault, Medallion, or start simple and migrate.

**When to use:** At the start of a new project, or when someone questions whether Data Vault is the right choice.

```
/dv-when
```

You will be asked about your context. The output is a recommendation with reasons for and against, and clear next steps.

---

### `/dv-discover` — Source analysis and vault model proposal

Provide one or more source tables (as DDL, column lists, CSV headers, or plain-language description). The skill analyses business key candidates, relationship signals, PII indicators, and multi-active patterns — then proposes a complete vault model.

**When to use:** When you are starting from source tables and need to know what hubs, links, and satellites to build.

```
/dv-discover

-- then paste DDL, e.g.:
CREATE TABLE crm.customers (
    customer_id    INTEGER PRIMARY KEY,
    email          VARCHAR(255),
    first_name     VARCHAR(100),
    ...
);
```

Output: a structured proposal listing HUBs, LINKs, SATs with rationale, and a list of open questions for you to confirm before proceeding.

**You confirm the model before any code is generated.**

---

### `/dv-model` — Design a construct

Design one construct at a time with full column definitions, naming, hash key formulae, and rationale. Each subcommand targets a specific construct type.

**When to use:** After `/dv-discover` proposes a model, use `/dv-model` to define each construct in detail. Also use it directly when you know what you need.

#### Subcommands

| Command | Use for |
|---|---|
| `/dv-model hub` | A business entity (customer, product, order) |
| `/dv-model link` | A relationship between two or more hubs |
| `/dv-model satellite` | Descriptive attributes on a hub or link |
| `/dv-model pit` | Point-in-Time table (query performance across satellites) |
| `/dv-model bridge` | Bridge table (query performance across links) |
| `/dv-model sal` | Same-as link (entity resolution, multi-source deduplication) |

```
/dv-model hub
-- CoCo asks: what is the business concept? what is the natural key?

/dv-model satellite
-- CoCo asks: which hub? what columns? single-active or multi-active? PII?
-- The Pattern Recommender subagent chooses the right variant automatically.
```

**Satellite variants chosen automatically:**

| Variant | When it applies |
|---|---|
| Standard (`standard`) | One active row per key, history tracked |
| Multi-active (`ma`) | Multiple rows active simultaneously (e.g. phone numbers) |
| Partitioned multi-active (`pma`) | Independent subsets versioned per dep-child key (advanced) |
| Effectivity (`ef`) | Tracks relationship lifecycle — link-only, `dv_start_date` + `dv_end_date` |
| Dependent-child (`dp`) | Parent key is not unique alone (needs a child discriminator) |
| Non-historized (`nh`) | Reference data, no history needed, no `dv_hashdiff` |
| Status tracking (`st`) | Tracks a status/state column over time via secondary staging |
| Record tracking (`rt`) | Tracks presence/absence of a record in the source |
| Extended tracking/XTS (`xt`) | File-based ingestion with timeline correction (advanced) |
| Hybrid (`hybrid`) | ODV — sub-300ms OLTP latency via Snowflake Hybrid Tables |

**PII is a naming suffix** (`_pii` in the satellite name), not a distinct type. Any satellite variant can have a PII suffix to segregate sensitive columns into a separate physical table with independent access control.

---

### `/dv-validate` — Doctrine validation

Check any construct or full vault design against the 42 Data Vault 2.0 doctrine rules. Validation always runs before generation — it cannot be skipped.

**When to use:** After designing constructs, and any time you want to check a model before proceeding.

| Command | Use for |
|---|---|
| `/dv-validate model` | Validate a single construct definition |
| `/dv-validate manifest` | Validate a complete vault design (all constructs) |
| `/dv-validate naming` | Check naming conventions only |

```
/dv-validate model
-- paste construct definition
-- get back: ✅ CLEAN or ❌ VIOLATIONS with plain-language explanations
```

The output groups findings as **violations** (blockers — must fix before generating) and **warnings** (non-blocking — consider fixing).

**CoCo never auto-fixes violations.** Each finding is explained and you decide what to do.

---

### `/dv-generate` — SQL generation

Generate `CREATE TABLE` DDL and insert-only load patterns for a validated construct. The Doctrine Enforcer runs as an internal gate — if there are violations, no SQL is produced.

**When to use:** Once a construct is designed and validated.

```
/dv-generate
-- describe or paste the validated construct
-- get back: DDL, hash key expression, insert-only load pattern
```

**All generated hub/link loads use MERGE** (`WHEN NOT MATCHED THEN INSERT` + `WHEN MATCHED THEN UPDATE SET last_seen_date`). All satellite loads use the anti-semi join pattern (`INSERT ... WHERE NOT EXISTS`). No other UPDATE or DELETE — ever.

The generated SQL targets **Snowflake** exclusively. Features used: Dynamic Tables, TRANSIENT schemas, MERGE, NOT ENFORCED primary keys, zero-copy cloning.

---

### `/dv-mart` — Information Mart views

Build query-ready views for BI tools from the raw vault. **Hash keys (`BINARY` columns) are never exposed** in Information Mart views — business keys and descriptive attributes only.

**When to use:** After generating vault tables, when you want to make the data accessible to BI tools or analysts.

```
/dv-mart
-- CoCo asks: which hub is the anchor? which satellites? current-state or point-in-time?
-- produces: CREATE OR REPLACE VIEW DDL
```

**View types:**

| View type | When to use |
|---|---|
| Current-state | Latest active row per entity — for most BI use cases |
| PIT-based | Point-in-time snapshots — when you need "as-of" queries across multiple satellites |
| Fact (link traversal) | Traverse a link to join two hubs into a fact-style view |

**Calculated / derived attributes:** If you have a calculation (e.g. customer lifetime value), load the result into a standard satellite treating the calculation as the source, then include it in the IM view like any other satellite. No separate layer is needed.

---

### `/dv-explain` — Knowledge base

Plain-language explanations of any Data Vault 2.0 concept, pattern, rule, or modeling decision. No subagents — pure knowledge.

**When to use:** Anytime. Before starting, during modeling, when reviewing someone else's vault design.

```
/dv-explain hub
/dv-explain why insert-only
/dv-explain multi-active satellite
/dv-explain hash key
/dv-explain same-as link
/dv-explain PIT table
/dv-explain ghost record
/dv-explain why no hash keys in the IM
```

---

## Key principles enforced by these skills

**1. Satellites are insert-only, always**
Satellites never update or delete. New versions of records are appended. This is not optional.

**1b. Hubs and links use MERGE with last_seen_date**
Hubs and links use `MERGE` to insert new records and update `last_seen_date` for existing ones. This is the only UPDATE permitted in the vault.

**2. Doctrine is a gate, not a suggestion**
`/dv-generate` runs the Doctrine Enforcer internally. If there are violations, no SQL is produced. There is no override.

**3. Hash keys are internal**
Hash keys (BINARY columns — size depends on project hash algorithm: SHA1 default → `BINARY(20)`, MD5 → `BINARY(16)`) are how the vault joins tables internally. They never appear in Information Mart views. Business users see business keys and descriptive attributes.

**4. Never auto-fix**
When validation finds a problem, CoCo explains it and asks what you want to do. No silent mutation of your model.

**5. Confirm before generating**
After discovery and modeling, CoCo presents a proposal and asks you to confirm before producing any SQL. You stay in control.

**6. Same-as links are raw vault entities**
Same-as links live in the raw vault alongside hubs and links — not in a separate business layer. They assert identity between two hub records; survivorship logic (which record wins) belongs in the Information Mart.

**7. Calculated attributes are just a source**
Derived or calculated attributes (e.g. metrics, scores) are loaded into a standard satellite where the RSRC is the calculation engine. No special layer is required.

---

## Quick reference card

| Skill | Subcommands | What you provide | What you get |
|---|---|---|---|
| `/dv-when` | — | Context about your project | Architecture recommendation |
| `/dv-discover` | — | DDL, column lists, or description | Vault model proposal |
| `/dv-model` | `hub link satellite pit bridge sal` | Business concept or attribute list | Full construct definition with DDL |
| `/dv-validate` | `model manifest naming` | Construct definition | Violation and warning report |
| `/dv-generate` | — | Validated construct | CREATE TABLE DDL + load pattern |
| `/dv-stage` | — | Source landing tables | Staging views with hashkey/hashdiff |
| `/dv-load` | — | Generated constructs | Snowflake Task DAG with dependencies |
| `/dv-deploy` | — | Project schema names | Schemas, roles, grants, deploy scripts |
| `/dv-test` | — | Vault tables | Integrity test SQL queries |
| `/dv-mart` | — | Hub + satellite list | IM view SQL |
| `/dv-bv` | — | Business rule definition | BV link/satellite + staging |
| `/dv-pit-bridge` | — | Hub with 3+ satellites | PIT/Bridge as Dynamic Tables |
| `/dv-xts` | — | Late-arriving source | XTS DDL + timeline correction load |
| `/dv-supernova` | — | Performance-critical IM | 5-layer pre-materialised DTs |
| `/dv-migrate` | — | Legacy vault platform | Migration strategy + validation |
| `/dv-bv-activity-schema` | — | Activity unification need | Activity Schema BV pipeline |
| `/dv-explain` | `<any concept>` | Concept name or question | Plain-language explanation |

---

## Example session: CRM customer vault

A typical end-to-end session building a vault from three source systems.

```
/dv-when
→ You: "We have Salesforce, ERP, and a billing system. All feed customer data.
        5 source systems total, regulated industry, need full audit history."
→ Recommendation: Data Vault — multiple sources, multi-source integration needed
→ Confidence: high
→ Next step: /dv-discover

/dv-discover
→ You: paste Salesforce, ERP, and billing DDL
→ Source Profiler identifies:
   - customer_id (BK candidate, unique per system)
   - account_number (BK candidate, shared across systems)
   - email, phone, ssn (PII flagged)
   - order_lines table has composite key (order_id + line_number) → Link signal
→ Proposal:
   HUBs:  HUB_CUSTOMER, HUB_ACCOUNT, HUB_ORDER
   LINKs: LNK_CUSTOMER_ACCOUNT, LNK_ORDER_ACCOUNT
   SATs:  SAT_CUSTOMER_SF, SAT_CUSTOMER_ERP, SAT_CUSTOMER_BILLING,
          SAT_CUSTOMER_SF_PII (email, phone, ssn segregated)
→ Open question: "Is customer_id the same across all three systems?"
→ You: "No, each system has its own ID"
→ Updated proposal adds SAL_CUSTOMER for entity resolution

/dv-model hub
→ CoCo asks: "What is the business concept? What is the natural key?"
→ You: "Customer. Business key is customer_id."
→ Produces: HUB_CUSTOMER with dv_hashkey_hub_customer, customer_id, full DV metadata

/dv-model satellite
→ CoCo asks: "Which hub? What attributes? Single-active or multi-active? PII?"
→ You: "HUB_CUSTOMER, from Salesforce: email, phone, industry, segment.
        email and phone are PII."
→ Pattern Recommender: standard satellite + PII suffix
→ Produces: SAT_CUSTOMER_SF (industry, segment) + SAT_CUSTOMER_SF_PII (email, phone)

/dv-model satellite
→ You: "Same hub, from ERP: credit_limit, payment_terms, account_status"
→ Pattern Recommender: standard satellite
→ Produces: SAT_CUSTOMER_ERP

/dv-model sal
→ You: "SAL_CUSTOMER — match between Salesforce and ERP customer records"
→ Produces: SAL_CUSTOMER + SAT_SAL_CUSTOMER_EFF (effectivity satellite)
→ CoCo: "Optional match attributes (confidence score)? Those go in SAT_SAL_CUSTOMER_MATCH."
→ You: "Yes, add confidence_score and match_algorithm."
→ Produces: SAT_SAL_CUSTOMER_MATCH (standard satellite off the SAL)

/dv-validate manifest
→ Validates all 8 constructs against 42 rules
→ Result:
   HUB_CUSTOMER         ✅ clean
   HUB_ACCOUNT          ✅ clean
   LNK_CUSTOMER_ACCOUNT ✅ clean
   SAT_CUSTOMER_SF      ✅ clean
   SAT_CUSTOMER_SF_PII  ✅ clean
   SAT_CUSTOMER_ERP     ⚠️  WARN-04: hub has 3+ satellites, consider PIT
   SAL_CUSTOMER         ✅ clean
   SAT_SAL_CUSTOMER_EFF ✅ clean

/dv-generate
→ Doctrine gate: clean (warnings are non-blocking)
→ Produces per construct:
   - CREATE TABLE DDL (NOT ENFORCED PKs, TRANSIENT for staging)
   - Hub/Link: MERGE with last_seen_date
   - Satellites: INSERT WHERE NOT EXISTS
   - Hash key expressions
   - Ghost record INSERT for each satellite
→ Also generates: VC_SAT_CUSTOMER_SF, VH_SAT_CUSTOMER_SF (satellite views)

/dv-stage
→ CoCo asks: "How does source data arrive? Files on stage, Snowpipe, batch?"
→ You: "Salesforce via Snowpipe (JSON), ERP via daily Parquet files on S3"
→ Produces:
   - stg_sf_customer: staging view over Snowpipe-loaded landing table
   - stg_erp_customer: staging view over external table with METADATA$FILENAME
   - Both add: hashkeys, hashdiff, dv_tenant_id, dv_collisioncode, dv_load_timestamp

/dv-load
→ Generates Task DAG:
   Root → TASK_LOAD_HUB_CUSTOMER (SF) → TASK_LOAD_HUB_CUSTOMER (ERP) [sequential]
       → TASK_LOAD_HUB_ACCOUNT
       → TASK_LOAD_LNK_CUSTOMER_ACCOUNT [after both hubs]
       → TASK_LOAD_SAT_CUSTOMER_SF [after hub]
       → TASK_LOAD_SAT_CUSTOMER_ERP [after hub]
→ Ghost record deployment script included

/dv-deploy
→ Produces:
   - Schema DDL (TRANSIENT staging, managed access vault)
   - Roles: CRM_LOADER, CRM_READER, CRM_ADMIN
   - Grants with FUTURE TABLE/VIEW privileges
   - snow sql execution script (dependency-ordered)
   - Zero-copy clone script for dev: CREATE DATABASE CRM_VAULT_DEV CLONE CRM_VAULT
   - DATA_RETENTION_TIME_IN_DAYS: 1 for hubs, 7 for satellites

/dv-test
→ CoCo asks: "Ad-hoc queries or DMF-based continuous monitoring?"
→ You: "DMFs"
→ Produces:
   - CREATE DATA METRIC FUNCTION statements (17 DMFs in DQ schema)
   - ALTER TABLE attachments:
     HUB_CUSTOMER: SKEY_DUPE + 1BKEY_DUPE
     LNK_CUSTOMER_ACCOUNT: SKEY_DUPE + 2HKEY_DUPE + ORPH x2
     SAT_CUSTOMER_SF: SAT_DUPE + SAT_SKEY_ORPH
   - Schedule: TRIGGER_ON_CHANGES on all tables
→ Query: SELECT * FROM SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_EXPECTATION_STATUS
         WHERE expectation_status = 'NOT_MET'

/dv-bv
→ You: "I need a derived credit_score based on payment_terms and credit_limit from ERP"
→ Produces:
   - bv_credit_score (business rule view — you write the SQL logic)
   - stg_bv_credit_score (DVOS generates staging — adds hashkeys/hashdiff)
   - SAT_BV_CREDIT_SCORE (INSERT-only satellite, dv_applied_timestamp from GREATEST)

/dv-pit-bridge
→ CoCo asks: "Hub has 3+ satellites. Build a PIT?"
→ You: "Yes, Dynamic Table, 1 hour lag"
→ Produces: PIT_CUSTOMER as Dynamic Table (TARGET_LAG = '1 hour')

/dv-mart
→ You: "DIM_CUSTOMER — current state, all satellites including BV credit score"
→ Produces: IM view joining VC_ satellite views to HUB_CUSTOMER
   - Business keys only (no BINARY columns)
   - Includes: industry, segment, credit_limit, payment_terms, credit_score
   - PII excluded (separate secure access via SAT_CUSTOMER_SF_PII)
```

---

## Doctrine rules summary

The Doctrine Enforcer checks 42 rules across five categories. High-level summary:

| Category | Key rules |
|---|---|
| Hubs | One business key, `dv_hashkey_hub_<name>` required, no descriptive attributes, singular name |
| Links | Two or more hub hash keys, own hash key (`dv_hashkey_<link>`), no descriptive attributes, no FK constraints in DDL |
| Satellites | `(parent_hk, dv_load_timestamp)` PK; `dv_hashdiff` on standard variants; `dv_recordsource` always; **no LEDTS** |
| SALs (same-as links) | Two hub hash keys from same hub, effectivity satellite required |
| Information Mart | No BINARY columns, no `_HK` in SELECT, no `dv_hashdiff`/`dv_load_timestamp`/`dv_recordsource` exposed |
| Loading | Hub/link: MERGE (insert + last_seen_date update). Satellite: INSERT-only, anti-semi join. Hash key uses `dv_collisioncode` (BKCC) NOT record source |

Full rule list lives in `agents/doctrine-enforcer.md`.

---

## Naming conventions enforced

| Object | Convention | Example |
|---|---|---|
| Hub | `HUB_<SINGULAR>` | `HUB_CUSTOMER` |
| Link (RV) | `LNK_RV_<RELATIONSHIP>` | `LNK_RV_CUSTOMER_ACCOUNT` |
| Link (BV) | `LNK_BV_<RELATIONSHIP>` | `LNK_BV_ACCOUNT_LINEAGE` |
| Same-as link | `LNK_RV_SA_<NAME>` | `LNK_RV_SA_CUSTOMER_MATCH` |
| Hierarchical link | `LNK_RV_HY_<NAME>` | `LNK_RV_HY_EMPLOYEE_MANAGER` |
| Satellite (RV) | `SAT_RV_<HUB\|LNK>_<BADGE>_<FILE>[_SPEC]` | `SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS` |
| Satellite (RV variant) | `SAT_<TYPE>_RV_<HUB\|LNK>_<BADGE>_<FILE>` | `SAT_MA_RV_HUB_CRM_CUSTOMER_CONTACTS` |
| Satellite (BV) | `SAT_BV_<CONCEPT>` | `SAT_BV_CUSTOMER_CREDIT_SCORE` |
| Satellite (BV variant) | `SAT_BV_<TYPE>_<CONCEPT>` | `SAT_BV_NH_CUSTOMER_STREAM` |
| XTS | `SAT_XT_<PARENT_TYPE>_<PARENT>` | `SAT_XT_HUB_POLICY` |
| PIT | `PIT_<PARENT>_<CADENCE>` | `PIT_CUSTOMER_DAILY` |
| SNOPIT | `SNOPIT_<PARENT>_<CADENCE>` | `SNOPIT_CUSTOMER_DAILY` |
| Bridge | `BRDG_<CONCEPT>_<CADENCE>` | `BRDG_CUSTOMER_PRODUCT_DAILY` |
| IM dimension | `DIM_<ENTITY>` | `DIM_CUSTOMER` |
| IM fact | `FACT_<RELATIONSHIP>` | `FACT_ORDER` |
| Satellite current view | `VC_<SAT_NAME>` | `VC_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS` |
| Satellite history view | `VH_<SAT_NAME>` | `VH_SAT_RV_HUB_SF_CUSTOMER_DEMOGRAPHICS` |
| Stream | `STR_<PURPOSE>` | `STR_STG_XERO_HUB_ACCOUNT` |
| Task | `TSK_<PURPOSE>` | `TSK_KAPPA_LOAD_HUB_ACCOUNT` |
| Staging view | `STG_<BADGE>_<SOURCE>` | `STG_SF_CUSTOMER` |
| Hash key (hub) | `dv_hashkey_hub_<name>` | `dv_hashkey_hub_customer` |
| Hash key (link) | `dv_hashkey_lnk_<prefix>_<name>` | `dv_hashkey_lnk_rv_customer_account` |
| Load timestamp | `dv_load_timestamp` | TIMESTAMP_NTZ |
| Applied timestamp | `dv_applied_timestamp` | TIMESTAMP_NTZ |
| Hash diff | `dv_hashdiff` | BINARY(20) |
| Record source | `dv_recordsource` | VARCHAR(255) |
| Collision code | `dv_collisioncode` | VARCHAR(50) |
| Tenant ID | `dv_tenant_id` | VARCHAR(50) |
| Sequence ID | `dv_sid` | NUMBER IDENTITY START 0 INCREMENT 1 ORDER |
| Ghost hashkey | `TO_BINARY(REPEAT(0, 20))` | 20 zero-bytes |
| Ghost timestamp | `TO_TIMESTAMP('1900-01-01 00:00:00')` | Epoch placeholder |
| End-date | **does not exist** — DVOS is insert-only, no LEDTS | |
