---
name: dv-when
description: Decide whether Pragmatic Data Vault is the right architecture for a given use case, and what to consider before starting
enabled: true
---

# /dv-when — When to Choose Pragmatic Data Vault

Help the user decide whether Pragmatic DV is the right fit, and what to consider before committing.

## Ask first

If the user hasn't described their context, ask:

> "Tell me about your situation: How many source systems? What's driving the project (audit, BI, integration, compliance)? Do schemas change often? How big is the team?"

Then use the guide below to give a recommendation.

---

## Why Data Vault has exactly three table types

Every business, in every industry, tracks three things:

1. **Business objects** — what entities exist and how to uniquely identify them (customers, accounts, products, contracts)
2. **Interactions and relationships** — what events, transactions, or associations exist between those entities
3. **Information state** — how the attributes of those entities and relationships change over time

Data Vault maps directly to these three things:

| Business concept | Vault table type |
|---|---|
| Business objects (uniquely identified by a stable business key) | **Hub** |
| Interactions and relationships between objects | **Link** |
| Information state over time (true changes) | **Satellite** |

This is not a coincidence. It's why DV has exactly three table types and why a vault built correctly never needs a fourth. When you find yourself reaching for a fourth table type, you have likely not correctly identified a business object, relationship, or state change.

The business key is the integration point: it connects data horizontally across all source systems (passive integration) and vertically across all architecture layers (from operational systems to analytics). This is the "shared kernel" — the rod through every layer holding all related data together for a business concept.

**Enterprise Architecture hierarchy — why data engineers must care about business architecture**

Enterprise architecture frameworks (TOGAF, Zachman, DoDAF) define a hierarchy of architecture knowledge areas. Business architecture is the foundation:

| Architecture layer | What it defines | Why DV cares |
|---|---|---|
| **Business architecture** | Business capabilities, value streams, business objects — what the business *is* and *does* | Hub tables must map to business objects; link tables must map to business processes. Without BA alignment, the vault is technically correct but business-meaningless. |
| **Application architecture** | Software that automates business capabilities | Source systems are application architecture automating business architecture; they emit the data that flows into the vault |
| **Data architecture** | How information is represented, integrated, and historised | This is the data vault's domain — mapping BA's business objects into hub/link/satellite structures |
| **Technology architecture** | Infrastructure and platforms enabling the above | Snowflake, cloud storage, compute — the physical layer under the vault |

**All lower architectures exist only to automate business architecture.** A data model that doesn't reflect business architecture is a data model that serves itself, not the business — and will require refactoring the moment the business changes its capabilities or processes. This is why DV hub names must come from domain experts and business ontology, not from engineering teams inventing names for database tables.

---

## The Modern Data Vault Stack — four zones

Data Vault sits within a broader architecture. Understanding which zone each skill operates in helps practitioners place their work correctly.

| Zone | Also called | Layers | DVOS skills |
|---|---|---|---|
| **Curated** (Producer Domain) | Bronze | Landing (ODS), Inbound Shares | `/dv-stage`, `/dv-discover` |
| **Coherent** (Aggregation Domain) | Silver | SAL (Source-Aligned Layer), CAL (Common Access Layer) | `/dv-model`, `/dv-generate`, `/dv-validate`, `/dv-bv`, `/dv-pit-bridge`, `/dv-load`, `/dv-xts` |
| **Intelligence** (Consumer Domain) | Gold | BAL (Business Access Layer), BPL, BRL, Lab, Reference | `/dv-mart` |
| **Knowledge** (Metadata Domain) | — | Passive metadata, Active monitoring, Proactive governance | `/dv-test`, `/dv-deploy` |

**Curated zone** — raw data arrives here from source systems (batch snapshots, CDC streams, real-time shares). Hard business rules are applied: data quality thresholds, schema change detection, PII classification, business key identification, attribute split decisions. Staging views compute hash keys and hashdiffs once — never recalculated downstream. Landing data is retained for 30 days then purged to limit breach exposure.

**Coherent zone** — raw vault integration (hubs, links, satellites) happens in the **SAL**. Business Vault artefacts, PITs, Bridges, reference data, and Information Mart views live in the **CAL**. Data vault is non-destructive to change here — new satellites extend the model without touching existing tables.

**Intelligence zone** — consumer domains extend the shared model with their own business vault, reporting views, and outbound data products. Each consumer domain owns its BAL (private business vault) and BRL (reporting layer).

**Knowledge zone** — metadata about the data: passive (data dictionaries, ownership), active (DMF monitoring, row counts, run times), proactive (RBAC, masking policies, PII tagging, data contracts).

**Data flows up only — the unidirectional layer doctrine**

Data always moves upward through the zone model, one layer at a time. **Data must never move downwards.**

| Direction | What it means |
|---|---|
| ✅ **Upward** (correct) | Raw source data enters the Curated zone (ODS/Landing) → moves to Coherent zone (SAL/RV) → promoted to Intelligence zone (BAL/IM) |
| ❌ **Downward** (forbidden) | Intelligence (IM results, BV outputs) cannot be pushed back down into a lower layer directly |

**Sharing derived intelligence across domains:** If a consumer domain's private business rule output or IM result needs to be made available to other domains, it must be re-ingested as a new data source through the Curated zone (ODS) and follow the same governed ingestion path as any other source. This ensures the audit trail is preserved — every row in the vault can be traced to a source ingestion event. Derived intelligence that bypasses ODS and is injected directly into the vault violates this audit guarantee.

**Exception — private business vault:** A consumer domain's private BV artefacts (used only by that domain) do not need to flow through ODS — they are computed directly from SAL content and land in the domain's own BAL. Only content that crosses domain boundaries requires the ODS re-ingestion path.

---

## Strong fit — choose Data Vault

✅ **Multiple source systems** (3+) feeding the same business entities from different angles

✅ **Schema volatility** — sources change structure frequently; you need the vault to absorb changes without breaking downstream consumers

✅ **Full history required** — you need to reconstruct any past state of the business (regulatory, audit, dispute resolution)

✅ **Multi-source integration** — same business entity (e.g. customer) exists in CRM, ERP, billing, and you need to reconcile them

✅ **Separation of concerns matters** — business rules change often; you need raw history separated from business logic so you can re-derive without re-loading

✅ **Long-lived platform** — you're building a backbone that will be extended over 5+ years by multiple teams

✅ **Regulated industries** — finance, healthcare, government — where lineage and traceability to source are non-negotiable

---

## Weak fit — consider alternatives

⚠️ **One or two stable sources** → Medallion (bronze/silver/gold) is simpler and faster to deliver

⚠️ **Speed to first delivery is the primary constraint** → Vault has upfront design cost; Medallion gets you to BI faster

⚠️ **Small team with no Vault experience** → The insert-only discipline and hash key patterns require training; Medallion is more forgiving

⚠️ **Purely operational reporting** (today's data, no history needed) → A simple staging + reporting layer is sufficient

⚠️ **One-time analytics project** → Not worth the structural investment

⚠️ **Single source, low volume** → If you are ingesting from only one source system and not managing millions of records, a data vault may be overkill. DV's primary value is passive integration across multiple sources. A single-source analytical requirement is better served by a simpler pattern. The vault pattern becomes increasingly compelling as source count and cross-domain analytics requirements grow.

---

### Named alternatives that appear at scale — and their failure modes

Two patterns commonly emerge when data modelling discipline is skipped. Both work at small scale; both fail at enterprise scale:

**Popcorn analytics** — Query-on-demand analytics using SQL views (e.g. hundreds or thousands of dbt models) without a governed data model underneath.

Problems at scale:
- SQL views are unexecuted code — every user re-runs the same computation; cost scales linearly with concurrency
- Duplicate metrics with competing definitions emerge across teams; no single version of the truth
- No OLAP join optimisation — views don't exploit hash-join algorithms
- Model proliferation: unused views accumulate; no governance on what's still needed
- High entertainment value (fast answers), but not sustainable

**OBT (One Big Table)** — All data in one wide denormalised table; complexity not resolved in the model but at query time.

Problems at scale:
- Every query must resolve dimensional complexity (current state, SCD logic, join conditions) at query time rather than once at the model level
- `SELECT DISTINCT` workarounds multiply as the table grows
- No OLAP optimisation — hash-join algorithms require fact + dimension separation
- Compute cost grows with every user since no shared pre-resolved structure exists

**When these patterns are acceptable:** For small teams, single-source projects, or exploratory analytics, both patterns are pragmatic. The failure mode only manifests at scale — when dozens of sources, hundreds of users, and regulatory history requirements emerge. Data vault is the correct response at that point, not before.

---

## Not either/or

Data Vault and Medallion can coexist:

```
Sources → Raw Vault (Pragmatic DV) → Information Mart → BI tools
                                     ↑
              (Marts can be star schema / dimensional — built on top of the vault)
```

The vault handles multi-source history. The Information Mart handles BI access. They serve different layers.

---

## Inmon / Kimball / Data Vault — three-way comparison

| | Inmon (3NF) | Kimball (Dimensional) | Pragmatic Data Vault |
|---|---|---|---|
| **Design philosophy** | 3rd normal form; enterprise-wide relational model | Question-driven; build around known analytics needs | Business process mapping; map to three repeatable table types |
| **When to change the model** | Change is destructive — requires migration, regression testing, potential downtime | Change is also destructive — adding dimensions/facts to existing models risks breaking consumers | Change is **non-destructive** — new satellites/links extend the model without touching existing tables |
| **Enforced constraints** | Yes — referential integrity and PK/FK constraints enforced; load fails if data doesn't fit | No constraints in star schema; transformation enforces the shape | No constraints in vault; all source data is absorbed as-is (hard rules applied in staging) |
| **History** | Depends on implementation; generally requires SCD Type 2 pattern | Requires SCD Type 2 dimensional tables | Insert-only by doctrine; full history is automatic |
| **Analytics delivery** | Requires dimensional layer on top | Native BI delivery; ready to query | Requires IM layer on top — but IM is disposable and can use Kimball patterns |
| **Best use** | When constraints and industry models provide governance value | When questions are well-known and stable | When multi-source integration, historical audit, and model adaptability are required |

**The combination:** Data Vault handles multi-source history with non-destructive change. Kimball dimensional models (star schema) are built on top as disposable Information Marts. When the model changes, drop and rebuild the IM — the vault history is untouched. This is `DV + Kimball = best of both`.

> *"Dimensional models are not easy to change but Data Vault is. Data Vault models are not easy to join but dimensional models are."*

The two are complementary, not competing: Data Vault provides the flexible, auditable enterprise model; Kimball-style dimensional models shape that data for BI consumption on top of it.

Inmon's industry data models can inform the naming and structure of vault hubs and links — use them as a reference for understanding business concepts, not as a DDL template to apply directly.

---

## Common objections and answers

**"Data Vault is too complex."**
The initial design (hash keys, insert-only, satellite variants) has a learning curve. Once it's running, adding a new source is mechanical — you add a satellite without touching existing tables. That's the payoff.

**"We don't need full history."**
You don't know that yet. Regulations change. Disputes happen. The cost of having history you don't use is storage. The cost of not having history when you need it is a re-load from source — if the source still has it.

**"Hash keys are ugly in reports."**
They never appear in reports. Information Mart views substitute business keys. End users never see a BINARY column.

**"We tried it and it was slow to query."**
Usually caused by querying Raw Vault directly from BI tools, skipping PIT tables, or not building Information Mart views. Vault + PIT + IM views is fast.

**"Why can't we just run analytics on the source system?"**
Six reasons why OLTP and OLAP must be separate:
1. **Resource competition** — analytics workloads compete with real-time customer-facing operations for CPU, memory, and storage. Running them on the same infrastructure degrades application performance.
2. **Incompatible data models** — source application models are optimised for per-row transactional operations, not columnar historical analysis.
3. **Different incentives** — source system engineers are incentivised to support real-time customer needs, not historical analytics. They optimise for OLTP, not OLAP.
4. **Vendor lock-in** — vendor software often can't be changed to meet analytics requirements. The data vault absorbs the model difference.
5. **Surrogate key instability** — surrogate keys in source applications are implementation artefacts that can be reloaded and resequenced. The vault uses stable business keys.
6. **Corporate memory outlasts software** — source applications are replaced more often than a well-built vault. The vault is the stable integration point that persists across application changes.

---

## "Integration is not the purpose" — named pitfall

A common failure mode: the vault project successfully integrates multiple source systems but never delivers business value. The vault becomes a technically impressive data store that nobody queries because no Information Mart was built, no business question was answered, and no use case was served.

**Symptoms:**
- "We've loaded 50 sources into the vault" but no IM views exist
- Hub/link/satellite counts are celebrated as progress metrics
- No business sponsor can articulate what question the vault answers
- The vault team measures success by tables loaded, not insights delivered

**Root cause:** treating data integration as the goal rather than as a means to deliver business outcomes. Data Vault is an *architecture* — it enables agility, auditability, and scalability. But architecture without delivery is shelf-ware.

**The rule:** every vault sprint must deliver at least one Information Mart view that answers a business question. Integration without consumption is waste. The steel thread (see below) enforces this: step 6 is "structure data product" — if no data product is delivered, the steel thread is incomplete.

**"No lightweight DV"** — Data Vault must be "all in" or risk building legacy data vault. Half-implementing the methodology (skipping ghost records, omitting hash keys, ignoring effectivity satellites, not building PITs) creates a system that has the complexity of a vault without the benefits. Either commit to the full pattern or choose a simpler architecture.

---

## The four A's — Data Vault's value proposition

When pitching Data Vault to any stakeholder, these five properties summarise the value:

- **Agile** — the model grows horizontally as you integrate more data sources (new satellites, new links added without touching existing tables) and vertically as those sources push data in (insert-only history). Changes are non-destructive. Multiple teams can work on the same vault model simultaneously — each team adds their own artefacts independently.
- **Automated** — only three loader types (hub, link, satellite). Once loading patterns are established, every new source follows the same pattern. DataOps pipelines run continuously; as data arrives it is loaded and tested. The vault is in constant growth — an eventually consistent model of the enterprise.
- **Auditable** — all the data, all the time. The vault is the enterprise's corporate history. Information Marts are conformed and **disposable** — because audit history lives in the vault, any IM view can be dropped and rebuilt from scratch at any point in time with no data loss.
- **Adaptable** — Raw Vault records source application data; Business Vault records derived outcomes. The two are fully decoupled: RV evolves with source systems, BV evolves with business rules, and neither forces a rebuild of the other. Independent agile teams extend the model without impacting each other. Source schema changes are absorbed non-destructively (deprecated columns are never discarded; new columns are appended). `RV + BV = DV`.
- **Autonomous** — each data vault component (hub loader, link loader, satellite loader, staging view) has a single purpose and does nothing else. A hub loader loads new business keys — it does not assign metadata, calculate values, or apply hard rules. Those are separate autonomous components. Each is configuration-driven: the code stays the same; only the parameters change per target. This autonomy makes the pattern infinitely scalable — each component can be deployed, tested, and upgraded independently without touching any other component.

Use the five A's as the opening frame when pitching to any stakeholder, then tailor with the role-specific pitch from `/dv-explain stakeholder-pitch`.

---

## Analytics maturity — which vault layer supports each level

Data Vault is designed to support all analytics maturity levels — from simple operational reporting to prescriptive AI. Each level has a different vault layer dependency and a different reasoning mode:

| Maturity level | Business question | Reasoning | Vault layer | Examples |
|---|---|---|---|---|
| **Operational reporting** | What has happened? | None / descriptive | RV current state only | Sales by channel today; assets under management; arrears by bucket |
| **Business intelligence** | Why is it happening? | Inductive (pattern → premise) | RV history + some BV | Why is arrears bucket 2 increasing? What's driving self-service adoption? |
| **Advanced analytics** | What could happen? | Deductive (facts → conclusion) | RV + versioned BV rules | Probability of customer default; key customer segments driving product adoption |
| **Prescriptive analytics** | What should happen? | Automation / AI | All layers (RV + BV + IM) | When default probability exceeds X%, trigger outreach; campaign segment B via email |

**Vault layer implications:**
- Operational reporting needs only the current-state RV view (`QUALIFY ROW_NUMBER() = 1`) — no BI history required
- Business intelligence requires full RV history + BV-derived attributes to provide context for the "why"
- Advanced analytics requires auditable BV rule outcomes that are **versioned** — as algorithms improve, old and new rule versions coexist so model performance can be tracked across versions
- Prescriptive analytics relies on information architecture trust at all layers — the vault's audit trail is the foundation that makes AI decisions explainable and defensible

---

## Service delivery models — how the vault team engages with the business

Before designing the vault, agree on the operating model. Three patterns exist, and implementations often combine them across different business units:

| Model | What the central team provides | What business teams do | DVOS scope |
|---|---|---|---|
| **Platform as a Service (PaaS)** | Infrastructure, tooling, and environment provisioning | Build and manage their own data acquisition, modelling, and BI solutions | Business teams run DVOS in their own space |
| **Data as a Service (DaaS)** | Raw data acquisition and provisioning (landing zone + raw vault) | Integration, transformation, modelling, and analytics work on top of the raw vault | Central team uses DVOS for RV; business teams use DVOS for BV + IM |
| **Analytics as a Service (AaaS)** | End-to-end: acquisition, transformation, modelling, and analytic solutions | Consume insights and reports | Central team uses all of DVOS |

**Why this matters for vault design:** The delivery model determines who owns business key definitions, who manages source-system SME engagement, and where BV rules are authored. In PaaS, BV rules are distributed across teams and need stricter governance. In AaaS, BV rules are centralised but must still be domain-owned and discoverable.

---

## Data Vault deployment configurations

Three named configurations for how Data Vault maps to an organisation's data platform architecture. The right choice depends on governance requirements, domain autonomy needs, and infrastructure constraints.

| Configuration | Description | When to use | Trade-offs |
|---|---|---|---|
| **Single platform, single vault** | All domains share one Snowflake account; one enterprise vault in SAL; each domain operates autonomously via their own BAL | Default for most organisations; easy centralised governance; simpler data provenance | All domains must share compute and governance framework; political/autonomy challenges between domains |
| **Multi-platform, single vault** | Each domain has its own infrastructure platform; all domains pull from a shared enterprise vault via a standardised interface | Compliance requires infrastructure separation between domains | Data provenance must be managed with an external tool across platforms; interface versioning needed |
| **Multi-platform, multiple vaults** | Each domain owns its own vault on its own platform | Domains are genuinely independent (separate legal entities, M&A integration, partner data sharing) | Passive integration is hard — keys won't automatically harmonise; consistent naming standards require a central code repository; teams may use different DV automation tools |

**Governance implication by config:** In single-platform configurations, row-access policies and column masking applied once at the SAL/vault layer propagate through all IM views automatically — no per-domain re-implementation needed. In multi-platform configurations, governance must be applied at the interface boundary and potentially replicated per domain.

**Cross-organisation sharing** — when data must be shared between organisations or with external partners, a **data clean room** establishes a virtual collaboration space where only non-sensitive or appropriately obfuscated data products are exchanged. See the `data-cleanrooms` skill for implementation patterns.

---

## Before you start

Confirm these five things before designing the vault:

1. **Business keys are identified** — you know the natural identifier for each entity across all source systems
2. **Record source granularity is agreed** — how specific your RSRC values will be (system + schema + table, not just "CRM")
3. **Load frequency is known** — batch daily, near-real-time, or event-driven? This affects staging and pipeline design. **Qualify near-realtime needs carefully before building for them** — see the qualification checkpoint below.
4. **History scope is agreed** — full history from day one, or from a cutover date?
5. **Landing cadence vs. load cadence** — do sources land at a different rate than the vault loads? If yes, Kappa Vault loading is worth considering.

If any of these are unknown, resolve them before modeling. Wrong business key choices are expensive to fix.

**Near-realtime qualification checkpoint**

Before investing in a streaming or near-realtime ingestion pipeline, answer these questions honestly:

| Question | If yes → | If no → |
|---|---|---|
| Does the business decision require data < 15 minutes old? | Streaming may be warranted | Batch (even hourly) is almost certainly sufficient |
| Is the inference based on data in motion (the event itself)? | True real-time required | History-based inference: batch accumulation + batch inference |
| Does the real-time decision require historical context? | History must be accumulated first → near-realtime ingestion alone is not sufficient | May be a pure event-stream pattern |
| What is the cost of a 1-hour data lag for this use case? | Quantify it — is it worth the streaming infrastructure investment? | Strong signal that batch is the right choice |

**Most analytics is based on the current state of business objects** — which a well-designed batch vault with hourly loads will serve adequately. Real-time analytics on data in motion is a distinct and more complex architectural pattern. The vault's insert-only architecture supports both, but the pipeline complexity and cost of streaming is significantly higher. Qualify the need before building for it.

6. **Automation tooling chosen** — never handcraft each data vault pipeline manually. Use a data vault automation tool. The repeatable patterns of DV only deliver their value at scale when loading is automated. Handcrafting each loader introduces inconsistencies that accumulate into the technical debt DV is designed to avoid.

7. **One DV standard, consistently applied** — the DV community has multiple standards (DV 1.0, Pragmatic DV, and variations). Pick one and apply it consistently. Deviating or mixing standards leads to **YALP (Yet Another Legacy Platform)** — the same technical debt accumulation DV was chosen to prevent.

8. **Governance decided at the lowest layer possible** — the earlier in the architecture layers data governance is applied, the less it needs to be replicated upstream. A row-access policy or column-masking policy applied at the vault layer (SAL) propagates automatically through all IM views over it — no need to re-implement per consumer domain or per IM view. Using Snowflake object tags on vault tables makes governance scalable: tag once at the vault layer, policies apply everywhere the tagged object is queried. Deferring governance to the IM layer means every new IM view must independently re-apply the same controls — a replication debt that grows with every new consumer.

9. **Source data format understood** — know the format of each source before designing the staging layer:

| Format | Schema approach | Examples | Vault staging approach |
|---|---|---|---|
| **Structured** | Schema-on-write — column definitions agreed and fixed upfront | Relational DB tables, CSV with agreed format, ERP exports | Standard staging view with typed columns and hashdiff |
| **Semi-structured** | Schema-on-read — column definitions defined when data is needed | JSON, Parquet, Avro, XML | Use `INFER_SCHEMA + USING TEMPLATE` to materialise schema, or OHS VARIANT satellite for schema-flexible sources |
| **Unstructured** | **Bias-on-read** — no fixed schema; extraction intent (bias) is applied at processing time, separate from the data | PDFs, images, video, audio, email, social media | Land as stage files; process with AI/NLP in **pre-staging** (e.g. `AI_PARSE_DOCUMENT`, Document AI model); load structured outputs into vault satellites. See `/dv-stage unstructured` |

**Bias-on-read — why unstructured data is unique**

Unlike schema-on-read (semi-structured, where the schema is defined once at read time), bias-on-read means the **same document can be processed multiple times under entirely different extraction intents**:
- First pass: extract customer identity (hub BK)
- Second pass: extract transaction relationships (link)
- Third pass: extract sentiment and descriptive attributes (satellite)

The bias (constraint/intent) is never tied to the data. This makes a set of unstructured documents a **renewable resource** — as new business questions arise, the same documents can be reprocessed with new AI models to derive new vault content. The vault's insert-only structure means each new extraction simply adds new rows without disturbing previous extractions.

---

## Steel thread — minimum viable DV implementation

A **steel thread** is a prototype that is the minimum size required to completely implement the main elements of a design: a vertical prototype that dives deep into the technical implementation but offers only a thin horizontal slice of functionality.

For a data vault, a steel thread proves the full pipeline end-to-end (landed data → vault → IM) for a single, well-chosen use case. Once established, subsequent use cases reuse the same patterns — only steps 4 and 5 below repeat, and they can run in parallel across teams.

### 6 steps to establish the steel thread

1. **Standards council** — Before writing any code, establish governance over:
   - Table and column naming conventions (see `reference/naming-conventions.md`)
   - Surrogate hash key vs. natural key vault choice
   - Hash collision strategies and BKCC namespace rules
   - Business rule management approach (who owns BV logic, how it is versioned)
   - Data quality framework (thresholds, rejection rules, DMF monitoring)

2. **Infrastructure design** — Decide:
   - Cloud platform and warehouse/lake architecture
   - Build vs. buy DV automation tooling
   - Separation of concerns: database, schema, role structure, secure vs. non-secure content

3. **Training, coaching, skilled people** — Ensure the team understands Pragmatic DV doctrine and the chosen automation tool before modelling begins.

4. **Choose a use case** — Select one bounded, well-understood business problem. The use case should span at least one hub, one link, and one satellite so all three table types are exercised.

5. **Mob modelling session** — Run a collaborative session (business stakeholders + modellers + engineers) to:
   - Identify business entities and their business keys
   - Map relationships and interactions between entities
   - Define the unit of work for raw vault and business vault
   - Agree on the IM/report/dashboard target
   See `/dv-discover` for the source profiling and mob modelling approach.

6. **Set KPIs to measure cadence** — Define delivery metrics before the sprint starts (e.g. number of hub/sat/link tables per sprint, pipeline run time, row count reconciliation pass rate). These become the benchmark for subsequent use cases.

After steps 1–6, only steps 4 and 5 repeat for each new use case. The rest of the infrastructure, standards, and patterns are reused.

---

## Loading pattern: Standard vs. Kappa Vault

Once Data Vault is confirmed, ask:

> "How frequently do sources land data, and how frequently do you want to load the vault?"

| Scenario | Recommended loading pattern |
|---|---|
| Batch (daily / hourly), landing and loading same cadence | **Standard**: Scheduled Task DAG, `SYSTEM$STREAM_HAS_DATA()` optional |
| Continuous / near-real-time, Snowpipe-fed landing | **Kappa Vault**: Streams on Staging Views, event-driven Tasks |
| Mixed — some sources batch, some continuous | **Mixed**: Kappa Vault for high-frequency sources, standard for batch |
| Same source lands multiple times per load cycle | **Kappa Vault**: `discard_view` / `distinct_view` CTEs handle multi-cadence automatically |

**Kappa Vault** uses Snowflake Streams placed on **staging views** (not tables). The stream advances per loader — one stream per hub loader, one per satellite loader. Tasks fire only when `SYSTEM$STREAM_HAS_DATA()` returns true. Load + reconciliation test are wrapped in `BEGIN TRANSACTION / COMMIT` (Repeatable Read Isolation).

All vault DDL, hash key formulas, satellite variants, and PIT/Bridge patterns are **identical** between Standard and Kappa Vault. The difference is only in the pipeline layer. A single vault can use both patterns for different sources.

Use `/dv-explain kappa vault` for a deeper explanation.

---

## Output

Summarize the recommendation:

```
RECOMMENDATION
==============
Pattern: Pragmatic Data Vault  /  Medallion  /  Start with Medallion, migrate to Vault later
Confidence: high / medium / low

Reasons for:
  - <reason 1>
  - <reason 2>

Reasons against / risks:
  - <risk 1>

Next step: /dv-discover to begin source analysis
       or: discuss further with /dv-explain
```
