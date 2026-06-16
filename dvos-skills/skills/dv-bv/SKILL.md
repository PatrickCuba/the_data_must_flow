---
name: dv-bv
description: Design and deploy Business Vault constructs — BV links and BV satellites. Covers delivery modes, rule views, staging, and doctrine rules.
enabled: true
---

# /dv-bv — Business Vault Links and Satellites

Business Vault (BV) constructs sit above the Raw Vault. They capture derived relationships and enriched attributes produced by business rules — not raw source data. All BV constructs are INSERT-only, same as Raw Vault.

**The one-line routing decision: where does the business rule engine live?**

| Rule engine location | Vault layer | Rationale |
|---|---|---|
| **Source-system application** (ERP, CRM, SaaS, custom app) | **Raw Vault** | The application automates the business rule; the vault records the outcome as raw data. The source is the authority. |
| **Analytics platform** (SQL, Python, Spark, ML model running inside the data warehouse) | **Business Vault** | The analytics team implements and owns the rule; the outcome is a derived business insight that needs the same auditability as raw data. |

If a result comes from a source application → it's raw vault, regardless of how complex the logic inside the source is.
If a result is computed inside your analytics platform → it's business vault, regardless of how simple the calculation is.

This distinction is the boundary between RV and BV. "Where was the rule executed?" is the only question you need to answer.

---

## BV Links

A BV link captures a relationship that does not exist in any source system directly — it is derived by a business rule (e.g. an account-to-product link inferred from transaction history, or a hierarchy link derived from a rules engine).

**BV links are needed in three situations:**

1. **Relationship doesn't exist in source** — the link must be derived from raw vault data (the classic BV link use case above)
2. **Source UoW ≠ Business UoW** — the source system depicts the unit of work, but in a shape the business doesn't use for reporting or regulatory purposes (e.g. a vendor COTS system tracks card-level movement, but the business needs account-level lineage). The BV link transforms the source UoW into the business UoW.
3. **Multiple sources claim the same relationship** — the BV link unifies competing source representations into a single version of the facts

**Why BV links matter: solve UoW complexity once for all users**

One of the core jobs of a data vault is: *solve unit-of-work complexity once for all users*. Without a BV link, every analyst and IM view must independently resolve the gap between the source's UoW and the business's UoW. With a BV link, that complexity is solved in one place — the BV rule view — and all downstream consumers query from the single approved result.

**Deterministic anchor for surrogate key derivation**

When the business needs a stable identifier that the source doesn't supply (e.g. an account number derived from a card lineage):
- **Do not** assign a random value — it is unstable and not deterministic
- **Do not** invent a clever algorithm — the result may become PII that must be managed
- **Use the first record in the entity lineage** — the earliest-issued key in a sequence of relationships is immutable and will never change

The first record in a lineage (grandparent) can be identified using a recursive CTE. Snowflake supports recursive CTEs natively. Note: Apache Spark SQL and HiveSQL do NOT support recursive CTEs (at time of writing) — if BV business rules must run on those platforms, the lineage traversal requires an iterative alternative.

### Manifest declaration

```yaml
links:
  - name: <link_name>
    vault_layer: bv                 # marks this as a Business Vault link
    source_name: bv_<concept_name>  # must match the BV rule view name
    participants:
      - hub: <hub_a>
        business_key_column: <bk_col_a>
        role: <role_a>              # required when same hub appears more than once
      - hub: <hub_b>
        business_key_column: <bk_col_b>
        role: <role_b>
    depends_on_sources:             # declare RV source dependencies for scheduler
      - <rv_source_name>
```

**Naming:** DVOS generates table `lnk_bv_{name}` for a BV link.

### What DVOS generates

- Staging view: `stg_bv_{concept_name}` — computes all hub hashkeys and composite link hashkey from the rule view
- Loader macro: `load__lnk_bv_{name}` — INSERT-only, anti-semi join

### What you write

The BV rule view (`bv_{concept_name}`) in the BV staging schema. It must output:
- Business key columns for each participant (one per `business_key_column`)
- `dv_applied_timestamp` — derived from contributing RV satellite timestamps (`GREATEST` of sources)
- **No** `dv_hashkey_*` columns — DVOS computes these in staging
- **No** `dv_hashdiff_*` columns

### BV link for simplified query access (account assignment pattern)

A BV link can assign a derived key to simplify multi-hop raw vault traversals. Example: an account hierarchy requires recursive CTE traversal in the RV (card → account → parent account). The BV link pre-resolves the hierarchy and stores the final "account number" directly:

- The BV link coexists with the RV link — it does not replace it (RV link is needed for source recreation)
- `dv_recordsource` contains the business rule name + version (e.g. `'BV_RULE:account_lineage:v2'`)
- The BV link's hash key is computed from the resolved business keys, not from raw vault hash keys

**Exploration links:** BV links can include speculative "exploration links" in an isolated schema for testing hypothetical relationships. These are exempt from audit and governance requirements until validated and promoted to the governed BV schema.

---

## BV Satellites

A BV satellite stores attributes derived from business rules over Raw Vault data. Structure and loading are identical to a Raw Vault satellite — INSERT-only, anti-semi join on `dv_hashdiff`.

### Two delivery modes

| Mode | When to use | Landing table? | Execution unit? |
|---|---|---|---|
| `landed` | Business rule output is a physical ODS table | Yes | Yes |
| `view` | Business rule is a SQL view over RV (no materialisation) | No | No |

### Manifest declaration — landed

```yaml
business_satellites:
  - concept_name: <concept_name>
    mode: landed
    parent: <hub_or_link_name>
    parent_type: hub                # or lnk
    source_badge: bv
    source_file: <concept_name>     # matches the landing table name
    depends_on_sources:
      - <rv_source_name>
    attributes:
      - <attr1>
      - <attr2>
```

**Naming:** DVOS generates table `sat_bv_{concept_name}`.

### Manifest declaration — view (virtual)

```yaml
business_satellites:
  - concept_name: <concept_name>
    mode: view
    parent: <hub_or_link_name>
    parent_type: hub
    attributes:
      - <attr1>
      - <attr2>
```

Virtual BV satellites do not produce execution units. The business rule runs as a SQL view. No landing table is needed.

---

## The BV delivery pipeline (landed mode)

```
Raw Vault satellites
        │
        ▼
bv_{concept_name}          ← you write this (SQL view or proc output)
  (business rule view)       outputs: BKs + dv_applied_timestamp only
        │
        ▼
stg_bv_{concept_name}      ← DVOS generates this
  (BV staging view)          adds: hashkeys, hashdiff, dv_recordsource,
                               dv_tenant_id, dv_collisioncode, dv_load_timestamp
        │
        ▼
sat_bv_{concept_name}      ← DVOS generates INSERT-only loader
  (BV satellite table)
```

---

## Business logic principles

Business Vault artefacts implement **soft rules** — transformations, derivations, and calculations applied after raw data has been loaded into the vault. Every piece of BV logic must satisfy these six principles:

1. **Domain owned, defined once, public** — BV logic is owned by the domain that produces it. Once defined, it is shared and made discoverable across all consumer domains. It is not reproduced in each consumer's private layer.

2. **Idempotent** — executing the same BV rule multiple times against the same input data with the same parameters must produce the same output, every time. Idempotence enables safe replay after orchestration failures, rule version changes, or dataset rebuilds.

3. **Versioned** — every change to a BV rule is tracked. The version change is recorded in the `dv_recordsource` metadata column of the BV satellite row that results from the new rule version. This means the vault carries a complete audit trail of *which version of the rule* produced each row.

   **Recommended `dv_recordsource` format for BV:** `bv_<rule_name>_v<version>` — for example `bv_relationship_quality_v1.0`, `bv_account_lifecycle_v2.3`. This makes every BV row traceable to the exact named rule and version that produced it, enabling both forward (what did this rule produce?) and backward (who produced this row?) tracing. When a business rule changes, increment the version in the recordsource — older rows retain the old version string, new rows carry the new one.

4. **Iterative** — BV logic will evolve. The structure (satellite DDL) is designed to be stable while the rule code (the `bv_` view) changes independently. See `/dv-bv` Key rules: physicalise BV satellites so rule code can evolve from a point in time without hardcoded dates.

5. **Autonomous** — BV logic units should have minimal dependencies on each other. **Avoid aggregations on aggregations** — chaining BV satellites introduces compounding complexity and change management risk. If BV satellite B depends on BV satellite A, a change in A cascades to B and all of B's consumers. Design each BV unit to operate as close to the raw vault as possible.

6. **Maximise reuse** — BV logic closer to source has greater reuse potential. A rule defined over the raw vault can serve all consumer domains. A rule defined within a consumer's private layer serves only that consumer. Always define shared logic in SAL/CAL before replicating it in consumer BALs.

> **Soft rules only** — BV is for soft business rules (derivations, aggregations, entity resolution, calculated attributes). Hard rules (data quality thresholds, PII tagging, schema validation, BK identification) belong in the **curated zone** before data reaches staging. See `/dv-stage` for the hard rule boundary.

**Business Rule Register**

Every BV rule should be tracked in a business rule register — a governance artifact that makes rules discoverable, auditable, and owned. Minimum required fields:

| Field | Required | Purpose |
|---|---|---|
| Code | Yes | Internal rule code (matches the `bv_<rule_name>` in `dv_recordsource`) |
| Business name | Yes | Human-readable rule name as it appears in the record source column |
| Description | Yes | What the rule does, dependencies, and which business processes use it |
| Owner | Yes | Data steward or role with direct responsibility for the rule |
| Category / domain | Yes | Ties to the business ontology; flags whether the rule exists for regulatory compliance |
| Implementation | No | JIRA ticket, code location, flow diagram, test results |
| Classification | No | Rule type: Term, Fact, Compute, Trigger, Validate |
| Tags | No | Search keywords sourced from the business glossary — enables discovery in data governance tooling |

The register is the authoritative catalogue of what soft rules exist, who owns them, and what they produce. It is not a code repository — it is a governance document that makes rule audit and impact analysis possible without reading SQL.

---

## BV doctrine rules

| Rule | Severity | Description |
|---|---|---|
| DV-BV-100 | WARNING | BV satellite must declare `mode: landed` or `mode: view` |
| DV-BV-101 | ERROR | Landed BV satellite must declare `source_badge` and `source_file` |
| DV-BV-102 | WARNING | Virtual BV satellite must not produce execution units |
| DV-BV-103 | WARNING | Landed BV satellite should declare `depends_on_sources` |
| DV-BV-110 | ERROR | BV staging and loaders must not contain UPDATE/DELETE/TRUNCATE. **Exception:** links (standard, BV, SA, HY) MAY use MERGE with `last_seen_date` update if explicitly declared in manifest (`last_seen: true`). Default is INSERT-only (anti-semi-join). |
| DV-BV-111 | ERROR | `dv_applied_timestamp` must NOT use `CURRENT_TIMESTAMP()` — derive from RV sources |
| DV-BV-120 | ERROR | **No BV hubs** — a Business Vault does not contain hub tables. A BV hub would imply the business key was derived inside the analytics platform, which is incorrect. Business keys are manufactured exclusively by source systems and applications. If a derived identifier is needed, it is a surrogate computed in a BV satellite or BV link — not a hub. |

---

## Key rules

- BV constructs are INSERT-only — same immutability doctrine as Raw Vault
- `dv_applied_timestamp` must be derived from contributing RV data (`GREATEST` of source timestamps), never `CURRENT_TIMESTAMP`. **Why GREATEST**: if you take a PIT/SNOPIT snapshot at time T, you need the BV satellite to have a record for time T that is derived from the RV data at time T. Using `GREATEST` ensures the BV record is anchored to the same temporal frame as the RV records it was derived from. Without this, PIT/SNOPIT snapshots of RV and BV will be misaligned.
- The business rule view (`bv_{concept_name}`) must output business keys only — DVOS adds all hashkeys and hashdiff in staging
- `depends_on_sources` controls scheduler dependency on RV load units — always declare it for correct execution ordering
- BV links with role-playing participants must declare `role` per participant
- **BV satellites are never copies of RV satellites** — even for the purpose of column renaming. If a downstream consumer needs different column names, create a SQL VIEW over the RV satellite. Copying a satellite to rename columns introduces redundant storage and a second load that must stay in sync.
- **BV and RV share the same database and schema** — the Business Vault is not a separate physical area. Raw Vault and Business Vault artefacts coexist in the same schema and are distinguished by table name convention only (`SAT_RV_` vs `SAT_BV_`, `LNK_` vs `LNK_BV_`). Creating a separate schema or database for BV is an anti-pattern — it severs the logical connection between raw and derived content and introduces unnecessary promotion pipelines.
- **Physicalise BV satellites** — separate the business rule code (the `bv_` view) from the satellite structure. This allows the business rule to evolve from a specific point in time without hardcoded dates in the rule code. The satellite structure remains stable; the rule view changes independently.

  **Why this matters — code vs. data are fundamentally different:**
  - **Code is declarative** — a business rule expressed as SQL is intended to be timeless; it applies whenever it runs, to any data it encounters
  - **Data is imperative** — a data row is anchored to the specific point in time when it was produced; it reflects what was true under the rules that applied then
  
  A manipulation of past data using today's updated rule code is **misinformation** — the old data was correct under the rule that existed at the time. Physicalising BV satellites preserves this: the `dv_recordsource` column records which rule version produced each row, making every result traceable to the code version that was current when that row was inserted. If rule code changes but produces no true-change output, the satellite correctly records no change.

- **Do not stack BV views** — deploying chains of BV logic as stacked SQL views (view A depends on view B which depends on view C) creates a fragile dependency graph. Any change to a lower-level view immediately cascades to all dependent views, creating unexpected downstream impacts that are difficult to test and control. The correct approach: physicalise intermediate BV outcomes as BV satellite tables. Each table is a stable interface; the rule view feeding it can change independently.
- **BV satellites may be multi-sourced** — they can consolidate, combine, or rationalise data from multiple RV satellites or even multiple BV artefacts. This is intentional and correct; BV is the integration and enrichment layer. Raw vault satellites remain single-sourced.

  **Why raw vault satellites must be single-sourced — schema drift and grain safety:** If multiple sources are merged into one raw vault satellite and one source experiences schema drift (a column is added, removed, or renamed), the mapping must be updated for all sources simultaneously — or the satellite grain no longer matches across sources. Worse: if two sources supply a column with the same name but different semantic interpretations, the satellite becomes ambiguous. Once data is mixed at the raw vault level, it cannot be unmixed without a reload. Raw vault is sold as never requiring refactoring — merging sources into one raw satellite inevitably breaks this guarantee. Each source gets its own raw vault satellite; BV satellites are where integration and merging legitimately occur.
- **Shared BV vs. private BV** — BV artefacts default to shared: defined once, domain-owned, published and discoverable across all consumer domains (see Business logic principle 1). However, a **private BV** is a legitimate pattern when:
  - The derived content is specific to one business domain and the ubiquitous language of that domain does not translate across other domains
  - Privacy, regulatory, or ownership requirements prevent the artefact from being shared
  - The BV rule is experimental or exploratory and not yet ready for enterprise adoption
  
  A private BV artefact is managed entirely within the owning domain — its own naming, its own governance, its own lifecycle. The trade-off is that reusability is sacrificed for domain autonomy. When the same private BV artefact is independently built by multiple domains, that is a signal to promote it to a shared BV artefact under centralised ownership.

---

## BV use cases — named patterns

The Business Vault has several well-recognised use case categories. Each produces auditable, insert-only BV satellites (and/or BV links) with the same guarantees as the Raw Vault.

| Use case category | What it solves | Output artefact |
|---|---|---|
| **Derived relationship** | Relationship not in source, or source UoW ≠ business UoW | BV link |
| **Entity resolution** | Two sources use different keys for the same real-world entity | BV link + BV satellite (or SAL in RV) |
| **Entity lifecycle** | Source never sends explicit deletion events — vault must infer aging/death | BV satellite (status: M/D/R) |
| **Derived attributes** | Business-agreed calculations or derivations that need an audit trail | BV satellite |
| **Data quality / Business Quality Vault** | DQ rule results persisted for downstream BI and SLA reporting | BV satellite |
| **Data preparation / Feature engineering** | ML-ready attributes prepared once, persisted with full auditability | BV satellite |
| **Write-back** | Insights or derived outputs from the semantic/analytics layer fed back into the vault | BV satellite (see below) |

### Write-back patterns — analytics insights into the vault

When insights or derived outputs from the semantic layer or downstream analytics need to flow back into the data platform, two named patterns apply:

**Pattern 1 — Via source application (→ Raw Vault)**

The derived insight is fed back into the source operational system (e.g. a recommendation engine updates a CRM record). The source system then processes it and delivers the updated record to the vault in the normal ingestion flow. The vault receives it as a standard source delivery and models it into **Raw Vault**. The source application is the authority; the vault records what the source says.
- Result: single-domain BV artefacts if the source only serves one domain
- Audit trail: the vault records the source's version — the analytical derivation is transparent via `dv_recordsource`

**Pattern 2 — As landed content (→ Business Vault)**

The derived insight bypasses the source application entirely and lands directly in the analytics platform (e.g. ML model output scores, semantic-layer-derived aggregations, dashboard-derived metrics). Because it is not passing through an operational source, it is modelled as **Business Vault** artefacts — derived content that completes or extends the raw vault.
- Result: potentially cross-domain BV artefacts (the insight may combine data from multiple domains)
- Audit trail: full BV guarantees apply (insert-only, idempotent, versioned `dv_recordsource = bv_<rule_name>_v<version>`)

**Doctrine:** the routing decision is simple — if it went through an operational source system, it's RV. If it came directly from analytics/ML without a source system intermediary, it's BV.

**Pattern 3 — Knowledge graph inference (→ Business Vault link)**

When a knowledge graph **infers a relationship** not directly observable in any source system (e.g. "customer A knows customer B" derived through householding analysis, co-transaction co-occurrence, or network graph inference), that inferred relationship can be persisted back into the vault as a **Business Vault link table**.

Why BV link (not BV satellite)?
- The inferred relationship is a new *unit of work* — it connects two or more business entities in a way the source systems did not record
- The BV link carries the same auditability guarantees as a raw vault link — insert-only, versioned `dv_recordsource = 'bv_kg_<rule>_v<version>'`
- The vault's `dv_applied_timestamp` historises the inference: if the KG rule is re-run with updated data, the new inference carries a new applied timestamp, and both old and new inferences coexist for audit

**Benefit:** the vault provides the temporal dimension that knowledge graphs typically lack. By persisting KG inferences as BV links, the inferred knowledge graph can be queried across time — enabling analysis of how relationships evolved as the underlying data and model changed.

### Data preparation and feature engineering as a BV use case

Data scientists traditionally spend most of their time cleaning and organising data. If that preparation work is persisted in the BV it becomes auditable, reusable, and available to all consumers — not just the data scientist who originally built it.

**Shareable vs. non-shareable features — the BV boundary**

Not all feature engineering belongs in Business Vault. The deciding test: *"Would another ML model or analytical consumer benefit from this feature?"*

| Feature category | BV? | Examples |
|---|---|---|
| **Shareable / reusable** | Yes | Time-windowed aggregations (rolling counts, min/max, avg, stddev), derived metrics, normalised attributes consistent across use cases |
| **Non-shareable / model-specific** | No | Imputation, outlier treatment, model-specific one-hot encoding, binning tuned for a specific algorithm — apply at training pipeline time |

The non-shareable transformations have point-in-time state tied to a specific model's input requirements. Storing them in BV creates satellite columns that no other consumer will use, inflates the satellite schema, and may even mislead other consumers.

Common feature engineering techniques that are appropriate BV patterns:

| Technique | Description |
|---|---|
| **Grouping / aggregation** | Rolling counts, min/max, avg, stddev over time windows — reusable across models |
| **Log transform** | Normalising skewed distributions to reduce outlier impact — if consistent across use cases |
| **Feature splits** | Combining or splitting attribute columns into analytically useful groupings |
| **Scaling** | Normalising or standardising attribute ranges — if the scaling is common across consumers |
| **Date extraction** | Extracting year, month, day, week, quarter as separate columns for time-based features |
| **One-hot encoding** | Only if the encoding is consistent and reusable across models — otherwise apply at training time |

**Doctrine note:** Each prepared feature set should be stored as a separate BV satellite on the relevant hub or link — not all features in one wide table. This preserves the separation-of-concern and rate-of-change isolation that the DV pattern depends on.

**Feature store support — offline and online**

The vault's insert-only audit history makes it a natural foundation for both types of feature store:

| Feature store type | Description | Vault source |
|---|---|---|
| **Offline feature store** | Historical feature values used for model training and backtesting | Full satellite history: `SELECT * FROM SAT_BV_CUSTOMER_FEATURES` (all rows, ordered by `dv_applied_timestamp`) |
| **Online feature store** | Low-latency current feature values for model serving / real-time inference | Current-state view: `QUALIFY ROW_NUMBER() = 1` over BV satellite |

**Versioning feature labels when algorithms change**

When a ML model algorithm is improved or a feature value definition changes, the new feature values must coexist with the old values in the audit trail. The BV satellite's insert-only, versioned loading pattern handles this automatically:

- Old algorithm runs → inserts BV rows with `dv_recordsource = 'bv_churn_model_v1.0'`
- Algorithm updated → new runs insert BV rows with `dv_recordsource = 'bv_churn_model_v2.0'`
- Both coexist in the same satellite; the `dv_recordsource` column distinguishes them

This means the vault carries a complete audit trail of *which version of the feature engineering logic* produced each row — enabling model comparison, rollback analysis, and regulatory explainability without any data loss.

**Backfilling BV features**

Raw vault can be used to backfill BV feature history when ML model training requires more historical data than the BV satellite currently holds. Three rules for a correct backfill:

1. `dv_recordsource` must indicate the backfill origin — e.g. `bv_churn_model_v1.0_backfill` — so the reload is traceable and distinguishable from normal loads
2. `dv_applied_timestamp` must match the source raw vault applied timestamp — the historical business context is preserved accurately
3. `dv_load_timestamp` is the backfill execution timestamp — it documents when the one-time operation occurred without altering the business timeline

**ML model drift monitoring**

Two named phenomena cause ML model performance to degrade over time. Store model predictions in a BV satellite per entity to support ongoing drift analysis:

| Phenomenon | Definition | BV monitoring approach |
|---|---|---|
| **Concept drift** | Statistical properties of the target variable (prediction) change in unforeseen ways | Compare prediction distribution over time against training baseline |
| **Data drift** | Statistical properties of the predictor variable (model input) change | Track feature value distributions in BV satellite over time |

Both types are detected by comparing current BV satellite values against the historical baseline used to train the model. The vault's insert-only history makes this comparison straightforward without requiring separate monitoring infrastructure.

**Snowflake Cortex AI functions as named BV feature sources**

When running on Snowflake, the following native Cortex AI functions produce structured outputs that are well-suited for BV satellite attributes. Each maps to a specific BV feature engineering use case:

| Cortex function | BV satellite use case | Notes |
|---|---|---|
| `SNOWFLAKE.CORTEX.SENTIMENT(text)` | Customer communication sentiment score per entity | Returns a score between -1 (negative) and 1 (positive) |
| `SNOWFLAKE.CORTEX.CLASSIFY_TEXT(text, labels)` | Document or event classification | Returns a label from the provided list |
| `SNOWFLAKE.CORTEX.SUMMARIZE(text)` | Condensed narrative attribute for large text fields | Useful for long-form satellite attributes |
| `SNOWFLAKE.CORTEX.EXTRACT_ANSWER(question, context)` | Structured extraction from unstructured text | Extracts a specific field value from a document column |
| `SNOWFLAKE.CORTEX.TRANSLATE(text, source_lang, target_lang)` | Normalised language dimension for multi-language content | Standardise language in satellite text attributes |
| `SNOWFLAKE.ML.FORECAST(...)` | Predicted metric values (time-series) | Persisted as auditable BV attributes per entity |
| `SNOWFLAKE.ML.DETECT_ANOMALIES(...)` | Anomaly flag/score per entity or time series | Attach anomaly flag to relevant hub or link satellite |

All of the above follow the standard BV write-back pattern (Pattern 2 — landed content → BV satellite) — the Cortex function runs over vault data, the output is treated as a derived source, and the result loads into a physicalised BV satellite with the standard DV metadata columns.

**"Humans in the loop" — quality rule for AI-produced BV content**

When a BV satellite contains AI or LLM-generated outputs, the standard BV quality guarantees (idempotent, versioned, auditable via `dv_recordsource`) are necessary but not sufficient. LLMs and AI models are vulnerable to **hallucinations** (confidently incorrect outputs) and model drift. Two additional governance rules apply:

1. **Include a confidence or citation column** alongside the AI-derived attribute — e.g. a `confidence_score` (float) or `source_reference` (VARCHAR) column that records the basis for the derived value. This enables downstream consumers to filter on confidence and gives the SME review process a reference point.
2. **Subject matter expert review before enterprise promotion** — AI-produced BV artefacts should be reviewed by domain experts before the `dv_recordsource` pattern is promoted to shared/enterprise status. Mark experimental AI BV artefacts with a private BV designation until their accuracy is validated across enough iterations to meet the agreed SLA.

---

## Entity aging rule — BV pattern

The **entity aging rule** is one of the most common Business Vault use cases. It detects entities (hubs) or relationships (links) that have stopped appearing in source feeds and progressively marks them as missing, then deceased.

### When to use

- The business needs to know which customers, accounts, or products are no longer active in source systems
- A feed may stop sending a key without any explicit deletion event (common in batch extracts from legacy systems)
- GDPR or regulatory compliance requires tracking when an entity was last present

### Implementation

Create a BV satellite to record the lifecycle status of each entity:

```sql
-- BV satellite: entity lifecycle status
-- sat_bv_hub_{entity}_lifecycle
-- mode: landed
-- dv_applied_timestamp = CURRENT_TIMESTAMP() is acceptable here (the rule runs on schedule,
--   not from source data — this is the one legitimate use of CURRENT_TIMESTAMP in BV)

CREATE OR REPLACE VIEW bv_{entity}_lifecycle AS
SELECT
    h.dv_hashkey_hub_{entity},
    h.{bk_column},
    CASE
        WHEN h.last_seen_date IS NULL                                   THEN 'UNKNOWN'
        WHEN DATEDIFF('day', h.last_seen_date, CURRENT_TIMESTAMP()) < 30 THEN 'ACTIVE'
        WHEN DATEDIFF('day', h.last_seen_date, CURRENT_TIMESTAMP()) < 60 THEN 'MISSING'
        ELSE                                                                 'DECEASED'
    END                                                              AS lifecycle_status,
    h.last_seen_date,
    CURRENT_TIMESTAMP()                                              AS dv_applied_timestamp
FROM LIB_PRD01_EDW.SAL.HUB_{ENTITY} h
WHERE h.dv_recordsource != 'GHOST';
```

DVOS generates staging (`stg_bv_{entity}_lifecycle`) and the INSERT-only satellite loader from this rule view.

### Conventions

| Threshold | Status | Meaning |
|---|---|---|
| Seen in last 30 days | `ACTIVE` | Currently present in source |
| Not seen 30\u201360 days | `MISSING` | Absent; may be transient |
| Not seen 60+ days | `DECEASED` | Declared dead in absentia — upon business agreement |
| Reappears after DECEASED | `REANIMATED` | Investigate: data quality issue or legitimate reactivation |

The ~60-day threshold for `DECEASED` is a starting convention. Confirm the appropriate threshold with the business before implementation — some industries require shorter or longer periods.

### Reanimation handling

If a `DECEASED` entity reappears in a source extract, the vault will insert a new hub row (the hub BK already exists; `MERGE` updates `last_seen_date`). The BV rule detects the reappearance and inserts a `REANIMATED` row into the lifecycle satellite. Flag this for business investigation — it may indicate a data quality issue, or it may be legitimate (a customer account reactivated after dormancy).

For GDPR-erased entities, a Record Tracking Satellite (RTS) is the guard: configure it to detect the reappearing BK and block insertion rather than recording it as `REANIMATED`.

## Activity Schema BV satellites

If the BV satellite models a **stream of entity events** following the [Activity Schema 2.0](https://github.com/ActivitySchema/ActivitySchema/blob/main/2.0.md) pattern — rows with `activity_id`, `activity`, `feature_json`, `revenue_impact`, `link` — use `/dv-bv-activity-schema` instead of this skill.

`/dv-bv-activity-schema` handles the full pipeline:
- BV staging transformation view (`stg_bv_{entity}_activity`) — maps event codes to activity names, projects authoritative `feature_json` via `OBJECT_CONSTRUCT`
- `SAT_BV_NH_{ENTITY}_STREAM` DDL (standardised Activity Schema column set)
- Stream on BV staging view + triggered task (Kappa Vault or batch)
- Per-activity IM Dynamic Tables (`dt_{entity}_stream_{activity}`)
- Cross-reference to `/dv-mart` for relationship DTs and `ASOF JOIN` dim enrichment

---

## Virtual vs physical BV \u2014 decision framework

Not all BV constructs need to be persisted tables. Use this escalation path:

| Stage | Form | When |
|---|---|---|
| 1 | **IM views referencing RV directly** | Simple derivations; < 5 contributing satellites; query latency acceptable |
| 2 | **IM views + PIT/Bridge** | Multiple satellites; timeline consolidation needed; query latency acceptable with pre-joined assistance |
| 3 | **Persisted BV tables** | Complex rules; rule versioning needed; multiple downstream consumers; reprocessing from RV is expensive |

**Arguments for physical BV (tables):**
- Calculated once, stored forever \u2014 downstream queries don't inherit computation cost
- Uncompromised audit trail \u2014 the derived result is historised with `dv_applied_timestamp`
- Rule versioning \u2014 each version of the rule produces its own `dv_recordsource`; views cannot version
- Decouples technology from data \u2014 multiple rule engines (SQL, Python, ML) can write BV; consumers don't know or care which engine produced the result

**Arguments for virtual BV (views):**
- Zero storage cost
- Always up-to-date (no refresh lag)
- Appropriate for simple transformations that will never need versioning

**Rule of thumb:** Start with IM views. As they become larger or rule complexity grows, escalate to PIT/Bridge assistance. If the rule needs versioning, or multiple consumers depend on the same derived result, persist as a BV table.

## Gaps-and-islands detection \u2014 BV pattern

A SQL pattern that detects timeline gaps in source-supplied start/end date ranges. Useful for data quality monitoring of insurance policies, contracts, subscriptions — any entity with expected continuous coverage.

```sql
-- Detect gaps in policy coverage timeline
CREATE OR REPLACE VIEW bv_policy_gap_detection AS
SELECT
    dv_hashkey_hub_policy,
    dv_applied_timestamp,
    coverage_start_date,
    coverage_end_date,
    LAG(coverage_end_date) OVER (
        PARTITION BY dv_hashkey_hub_policy ORDER BY coverage_start_date
    ) AS prev_end_date,
    DATEDIFF('day', prev_end_date, coverage_start_date) AS gap_days
FROM SAT_RV_HUB_POLICY_COVERAGE
WHERE gap_days > 1;  -- gap detected
```

**Why BV satellite, not IM view:** Implementing this as a persisted BV satellite (not just an IM view) inherits full audit trail. The gap detection result is historised — you can answer "was this gap known at date X?" which is impossible with a view.

Persist the gap detection output as `SAT_BV_POLICY_GAPS` with the gap metadata (start, end, gap_days, detection_date) as satellite attributes.

## BV Column Suffix Taxonomy

> **Starter set** — this taxonomy helps teams begin establishing naming standards. It is not exhaustive. Each project should extend it with domain-specific suffixes documented in their standards register.

When naming derived columns in Business Vault satellites, use these standard suffixes to communicate intent and data type at a glance:

| Suffix | Meaning | Example |
|---|---|---|
| `*_amt` | Monetary amount (single transaction or event) | `transaction_amt`, `fee_amt` |
| `*_bal` | Running balance or point-in-time position | `closing_bal`, `available_bal` |
| `*_pmt_?` | Payment-related (suffix `_d` = debit, `_c` = credit) | `mortgage_pmt_d`, `interest_pmt_c` |
| `*_ind` | Boolean indicator (Y/N or 1/0) | `active_ind`, `overdue_ind` |
| `*_cd` | Code value (short reference classification) | `status_cd`, `currency_cd` |
| `*_bnd` | Band / bucket / tier classification | `risk_bnd`, `age_bnd` |
| `nxt_*` | Next-period / forward-looking value | `nxt_review_dt`, `nxt_payment_amt` |
| `prv_*` | Previous-period / backward-looking value | `prv_balance_amt`, `prv_status_cd` |
| `cur_*` | Current-period / as-of-now value | `cur_balance_amt`, `cur_rate_amt` |
| `*_dt` | Date (DATE type, no time component) | `effective_dt`, `expiry_dt` |
| `*_dttm` | Datetime / timestamp (with time component) | `created_dttm`, `modified_dttm` |
| `*_stmt_*` | Statement-period value (financial reporting cycle) | `monthly_stmt_bal`, `stmt_closing_amt` |

**Usage rules:**
- Apply these suffixes consistently across all BV satellites in the project
- The suffix replaces generic column names — `amount` → `transaction_amt`; `balance` → `closing_bal`
- Combine prefixes and suffixes when needed: `prv_closing_bal`, `nxt_payment_dt`
- Document project-specific suffixes beyond this list in the standards register

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
