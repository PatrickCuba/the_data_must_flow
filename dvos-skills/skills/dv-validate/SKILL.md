---
name: dv-validate
description: Validate a Pragmatic Data Vault model definition against Pragmatic DV doctrine rules. Subcommands: model | manifest | naming
enabled: true
---

# /dv-validate — Doctrine Validation

Check a vault construct or full model against Pragmatic Data Vault doctrine rules. Always runs before generation.

## Subcommands

### `/dv-validate model`  (default)

Validate a single construct definition pasted or described by the user.

**Spawn the Doctrine Enforcer subagent** (see `agents/doctrine-enforcer.md`) and pass the model definition. The Enforcer returns a structured list of violations.

Present results as:

```
DOCTRINE VALIDATION
===================
Construct: <name>
Status: ✅ CLEAN  /  ❌ VIOLATIONS FOUND

Violations:
  [DV-MAN-001] Hub HUB_ORDER has no business key defined
  [DV-SAT-001] SAT_CUSTOMER_CONTACT is missing dv_hashdiff column
  ...

Warnings (not blockers):
  [WARN-02] SAT_CUSTOMER_DEMO — consider splitting PII columns into a PII satellite
```

**Never auto-fix violations.** Explain each one and ask what the user wants to do.

---

### `/dv-validate manifest`

Validate an entire vault design (multiple hubs, links, satellites).

Ask the user to paste or point to a file containing their full model.

Run the Doctrine Enforcer subagent once per construct, then aggregate results:

```
MANIFEST VALIDATION SUMMARY
============================
  HUB_CUSTOMER       ✅ clean
  HUB_PRODUCT        ✅ clean
  LNK_ORDER          ❌ 2 violations
  SAT_CUSTOMER_DEMO  ⚠️  1 warning
  SAT_ORDER_DETAIL   ✅ clean

Total: 4 clean, 1 with violations, 1 with warnings
```

Drill into each violation with plain-language explanation.

---

### `/dv-validate naming`

Check naming conventions only — no structural doctrine.

**Spawn the Naming Advisor subagent** (see `agents/naming-advisor.md`) and pass all construct names and column names.

Check:
- Hub: `HUB_` prefix, singular noun, UPPER_SNAKE_CASE
- Link: `LNK_` prefix
- Satellite: `SAT_` prefix, `SAT_<PARENT>_<CONTEXT>` format
- PIT: `PIT_` prefix
- Bridge: `BDG_` prefix (NOT `BRDG_` or `BRIDGE_`)
- Hash key columns: `dv_hashkey_hub_<name>` (hubs) / `dv_hashkey_<link_full_name>` (links)
- Hash diff column: `dv_hashdiff` (not `HDIFF`, `HASH_DIFF`, `HD`)
- Load timestamp: `dv_load_timestamp` (not `LDTS`, `LOAD_DATE`, `LOAD_DTS`)
- Applied timestamp: `dv_applied_timestamp` (batch/file timestamp)
- Record source: `dv_recordsource` (not `RSRC`, `RECORD_SOURCE`, `SRC`)
- **No end-date column** — DVOS satellites are insert-only with no LEDTS. Current row via `QUALIFY ROW_NUMBER()`.
- **DV-LOAD-001 — ALL satellites are INSERT only** — no UPDATE, no DELETE, no MERGE on any satellite table. No exceptions (including non-historized and BV satellites). Hubs and links use MERGE; satellites NEVER do.
- Effectivity columns (type `ef` only): `dv_start_date`, `dv_end_date` (not `ACTIVE_FLAG`, not `dv_startts`)

### Additional warning codes

The following warnings are raised during model and manifest validation in addition to structural doctrine:

| Code | Severity | Description |
|---|---|---|
| `WARN-BK-001` | WARNING | Hub business key column has a numeric data type (`INT`, `NUMBER`, `BIGINT`, `DECIMAL`). Business keys must be `VARCHAR`. Cast to string in staging and define the column as `VARCHAR` in the hub DDL. |
| `WARN-BK-002` | WARNING | `dv_collisioncode` value appears to be a source-system name (e.g. `'SALESFORCE'`, `'SAP'`, `'CRM'`). BKCC is a business key namespace discriminator, not a source-system label. This pattern is known as SSDV (Source System Data Vault) and creates hub bloat, unnecessary joins, and downstream BV integration debt. Default to `'default'` unless two sources genuinely share overlapping key values for different entities. |
| `WARN-HUB-SMART` | WARNING | Hub business key appears to be a smart key (contains embedded type codes, year, country, or sequence patterns) AND additional hub columns have been derived from components of that key. Smart key components are attributes — store the full BK as an opaque string and derive components in a satellite or BV satellite. |
| `WARN-HUB-CONCAT` | WARNING | Hub business key appears to be a concatenation of multiple component keys (e.g. `branch_code \|\| account_number`). Concatenated composite keys push integration debt to the IM layer where every query must de-concatenate. Model the composite key correctly as a multi-column BK in a properly named hub. |
| `WARN-HUB-WEAK` | WARNING | Hub construct has no clearly identifiable business owner or does not correspond to a business object the organisation refers to by this key. Confirm with a business stakeholder before generating — weak hubs create technical debt. |
| `ERR-HUB-KIH` | ERROR (blocks generation) | Hub business key includes a date, timestamp, or sequence number. Keyed-instance-hub anti-pattern — keys must identify entities, not snapshots. Move temporal values to a satellite. |
| `WARN-HUB-REF` | WARNING | Hub business key appears to be a reference/lookup code (short categorical value, status code, type code). Reference codes are attributes, not entity identifiers. Consider dependent-child key or reference enrichment instead. |

---

## Data Vault Model Scorecard

A 10-category rubric for assessing the quality of a proposed or existing Data Vault model. Score each category out of its maximum and sum for an overall model quality percentage.

| # | Category | Max | What to assess |
|---|---|---|---|
| 1 | **Correctness** | 15 | Model captures business requirements; stakeholder sign-off exists; evidence of interviews or report analysis |
| 2 | **Completeness** | 15 | Only models what is needed (no over-modelling); all metadata definitions complete (business purpose, model version, source names, expected rate of change) |
| 3 | **Schema** | 10 | Model matches the appropriate level (conceptual, logical, physical); relationship cardinality correctly depicted; RV diagrams are source-specific |
| 4 | **Structure** | 15 | Objects modelled in the correct place (attributes on correct hub/link); PII split out; correct stencils used; definitions consistent across models |
| 5 | **Abstraction** | 10 | How well the model fits an enterprise/industry ontology; extensibility vs. readability balance |
| 6 | **Standards** | 5 | Naming standards followed (table prefixes, column names, singular nouns, approved abbreviations); column names include prime + modifier + class word |
| 7 | **Readability** | 5 | Correct stencil artefacts; large models broken into smaller diagrams; minimal line crossings; easy-to-spot "heart" of the model |
| 8 | **Definitions** | 10 | No ambiguity; correct business terms from glossary; links to explanation pages or examples where appropriate |
| 9 | **Consistency** | 5 | Model is comparable to enterprise model; consistent naming (no synonyms for same concept) |
| 10 | **Data** | 10 | Metadata matches actual data; profiled data validates the model design |
| | **TOTAL** | **100** | |

Use this scorecard during model reviews and mob modelling sessions. A model scoring below 70% should be revised before generating DDL.

## Fake Vault anti-patterns \u2014 definitive catalog

A "Fake Vault" is an implementation that superficially resembles Data Vault (has hubs, links, satellites) but violates core principles, producing a system with none of DV's benefits (auditability, scalability, agility). Check for these 13 anti-patterns during validation:

| # | Anti-pattern | What it looks like | Why it fails |
|---|---|---|---|
| 1 | **Weak hubs** | Keys with no business concept loaded as hubs; "multi-master hub" | Hub has no business meaning; queries cannot navigate it without type-code disambiguation |
| 2 | **Staggered loads** | Sequence-key dependencies forcing serial loading (overnight batch) | Eliminates hash-key parallelism advantage; reverts to pre-DV era |
| 3 | **Source-system BKCC** | Using run-stream/source-system codes as collision codes | Creates a "source-system data vault" (SSDV) / "legacy data vault" \u2014 separate hubs per source |
| 4 | **Loading satellites as-is** | Not splitting by what attributes describe | Pushes integration to downstream; every IM query must compensate |
| 5 | **No link-satellites** | Compensated by weak hubs with extra columns | Adds unnecessary joins; loses relationship-level attributes |
| 6 | **Non-unique hubs/links** | Adding FKs to hubs, making hubs temporal, evolving link schema | Violates grain immutability; hub/link becomes a satellite |
| 7 | **Loading DV from PITs/Bridges** | Query-assistance tables used as operational dependencies | Circular dependency; PIT/Bridge are disposable, not authoritative |
| 8 | **Deleting data in DV** | Removing rows from vault tables | Violates audit-trail requirement; archive to cold storage instead |
| 9 | **Sub-typing on ingestion** | Conforming raw data before it reaches raw vault | BV rule applied too early; raw vault no longer represents source faithfully |
| 10 | **Skimming source columns** | Cherry-picking columns instead of loading all | Breaks auditability: cannot recreate source record |
| 11 | **Missing mandatory metadata** | No record source, no load date, no natural/hash key | Cannot trace data lineage or determine record provenance |
| 12 | **Vendor lock-in** | Vault tied to proprietary tool features; non-ANSI SQL | Vault must be tool-agnostic; ANSI SQL 2003 minimum |
| 13 | **Multiple vaults in same ontology** | Departmental silos each with their own vault | Defeats integration purpose of DV; creates data islands |

During `/dv-validate model`, check for signals of these anti-patterns and flag with `ERR-FAKE-<number>`.

## Doctrine Gate

If validation is called from within `/dv-generate`, a CLEAN result is required before any code is produced. The gate is enforced by the generate skill \u2014 not by the user saying "ignore errors."

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- Naming Advisor: `agents/naming-advisor.md`
