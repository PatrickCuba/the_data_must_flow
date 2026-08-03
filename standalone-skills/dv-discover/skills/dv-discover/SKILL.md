---
name: dv-discover
description: Profile live source tables in parallel, detect relationships, and propose a single-universe Pragmatic Data Vault model (hubs, links, satellites). Works exclusively from landed Snowflake tables — no text input.
triggers:
  - source discovery
  - analyze source tables
  - propose vault model
  - what hubs do I need
  - model from source
  - reverse engineer vault
  - discover source
  - profile schema
enabled: true
---

**Form display rule:** All `ask_user_question` headers must use the exact bold label from the reference files. Never abbreviate Data Vault terms to acronyms in user-facing form headers or question text.

**File lookup rule — no filesystem search:** `vault_config.yml`, `manifest_index.yml`, and `hub_registry.yml` live ONLY in the current working directory (project root). When a step says "read `<file>`," check ONLY that exact path — never run `find`, `ls -R`, glob searches, or look elsewhere. If the file is not found, treat it as **not existing** and follow the documented fallback.

**No unscripted diagnostics:** Do not run `git status`, `git log`, `git stash list`, `git diff`, or any other exploratory command not explicitly listed as a step below.

---

# /dv-discover — Source Discovery & Vault Model Proposal

## Purpose

Profile a set of live source tables from a single source system, detect relationships between them, and propose a complete Data Vault model (hubs, links, satellites) for one universe. This is the pre-modelling discovery tool that feeds into `/dv-universe`.

**Scope:** One source system = one badge = one universe. If the user has tables from multiple source systems, they run `/dv-discover` once per source.

---

## Input

The user provides:
- **Source system name** and **badge** (short uppercase code)
- **Table location:** a schema path (e.g., `RAW_DB.LANDING_CB`) OR an explicit list of fully-qualified table names

**No text input accepted.** Tables must exist in Snowflake. If they don't exist yet, the user must land them first (even as empty DDL with sample rows).

---

## Prerequisites

- Tables must exist and be queryable (user's current role must have SELECT access)
- `vault_config.yml` should exist for schema locations and naming conventions (optional — if missing, user provides the schema path directly)
- `manifest_index.yml` and `hub_registry.yml` are optional — used for cross-universe collision detection if other universes already exist

---

## Workflow

### Step 1 — Gather source context

Ask the following questions:

**1a — Source system:** Ask as a **text** question with header `"Source System"`:
> "What is the source system name?"
> defaultValue: ``

**1b — Badge:** Ask as a **text** question with header `"Source Badge"`:
> "What short uppercase badge identifies this source? (e.g., CRM, ERP, CB)"
> defaultValue: ``

**1c — Table location:** Ask as a **text** question with header `"Table Location"`:
> "Where are the landed source tables? Provide a schema path (e.g., RAW_DB.LANDING) or comma-separated fully-qualified table names."
> defaultValue: `<landing_db>.<landing_schema>` (derive from vault_config if available)

---

### Step 2 — Enumerate tables

If the user provided a schema path, run:
```sql
SHOW TABLES IN SCHEMA <schema_path>;
```

Extract the table names from the result. If the user provided explicit table names, use those directly.

Present the table list to the user for confirmation. Ask as a **multi-select options** question with header `"Tables to Profile"`:
> "Found {N} tables in `<location>`. Which ones should I profile for this universe?"

List all tables as options (all pre-selected). The user can deselect tables that aren't part of this source system.



---

### Step 3 — Parallel profiling

For each confirmed table, spawn a **parallel profiling agent** using the Task tool. Each agent runs the full `/dv-profile` methodology (auto-detectable steps only — no interactive questions since agents run in background):

```
task(
  subagent_type: "source-profiler",
  description: "Profile <TABLE_NAME>",
  prompt: "Profile the table <FULLY_QUALIFIED_TABLE_NAME> for Data Vault modelling.
    Run the full /dv-profile methodology (Mode 3) against this live table.
    All other tables in this source system: <ALL_TABLE_NAMES>

    Execute ALL of the following steps:

    STEP 1 — Metadata extraction:
    - Run DESCRIBE TABLE to get column inventory (names, types, nullable, comments)
    - Query INFORMATION_SCHEMA.COLUMNS for full metadata

    STEP 2 — Per-column profiling:
    - For each column: COUNT(*), COUNT(DISTINCT col), null count, min/max length
    - Use SAMPLE (1000 ROWS) for efficiency on large tables
    - For high-cardinality columns (distinct/total > 0.5, nulls = 0): sample 50 distinct values

    STEP 3 — BK candidate scoring:
    - HIGH: cardinality_ratio > 0.9 AND null_count = 0 AND (name matches *_id, *_key, *_code, *_no, *_number OR comment contains identifier/primary key)
    - MEDIUM: cardinality_ratio > 0.5 AND null_count = 0 AND type in (VARCHAR, NUMBER) AND NOT a measure pattern (*_amount, *_qty, *_balance)
    - NONE: cardinality_ratio < 0.3 OR nulls > 0 OR type in (BOOLEAN, DATE, TIMESTAMP)

    STEP 4 — BK format pattern derivation:
    - From sampled values, derive regex patterns:
      All numeric fixed length → numeric(<len>)
      UUID pattern → uuid
      Alpha-numeric with separators → describe as regex (e.g., [A-Z]{3}-\\d{5})
      Mixed/unpredictable → freeform

    STEP 5 — BK classification (auto-detect only):
    - Key nature: surrogate if column name contains 'surrogate', 'sys_', 'auto_', or is IDENTITY; natural otherwise
    - Composition: composite if table has multiple HIGH-confidence BKs; simple otherwise
    - Smart key: flag if value segments encode meaning (consistent prefix/suffix patterns with separators)
    - Overloaded: flag if same column appears to contain multiple entity types (heterogeneous format patterns in samples)
    - Sensitivity: flag if column name matches PII patterns (ssn, tax_id, email, phone)

    STEP 6 — Hub resolution:
    - Check if BK concept matches any existing hub in hub_registry.yml (if provided): <HUB_REGISTRY_CONTENT>
    - For each BK candidate: determine if it maps to a new hub or existing hub
    - Flag anti-patterns: BK concatenation, over-generalisation, supertype/subtype confusion

    STEP 7 — Link discovery (unit of work):
    - If 2+ BK candidates map to different hub concepts in this table:
      Determine relationship nature from cardinality analysis
    - Detect FK signals:
      Column name matches a PK column in another table in this set
      Column name pattern: <entity>_id where <entity> matches another table name
    - Determine: optional keys (nullable FKs), degenerate keys (sequence/line numbers), driving key signals

    STEP 8 — Satellite classification:
    - PII columns: email, phone, ssn, dob, date_of_birth, name patterns → separate _pii satellite
    - High-change-rate columns: balance, amount, score, status → split from slow-changing descriptors
    - Multi-value signals: multiple rows per BK → multi-active satellite candidate
    - Non-historized signals: lookup/reference tables (< 10k rows, low change rate) → NH satellite
    - Dependent-child signals: composite grain with parent FK → dep-child satellite
    - Remaining descriptive columns → standard satellite grouped by parent entity

    STEP 9 — Collision risk (if hub_registry.yml provided):
    - For each BK mapped to an existing hub: compare format pattern against existing sources
    - Flag: same hub, overlapping format → collision risk (may need BKCC differentiation)
    - Flag: same hub, non-overlapping format → safe passive integration

    Return a structured JSON report:
    {
      \"table\": \"<name>\",
      \"row_count\": N,
      \"columns\": [{\"name\": \"\", \"type\": \"\", \"nullable\": bool, \"distinct_count\": N, \"null_count\": N, \"bk_score\": 0-100, \"classification\": \"\"}],
      \"candidate_bks\": [{\"column\": \"\", \"confidence\": \"HIGH|MEDIUM\", \"format_pattern\": \"\", \"sample_values\": [], \"key_nature\": \"natural|surrogate\", \"composition\": \"simple|composite\", \"smart_key\": bool, \"overloaded\": bool, \"sensitive\": bool}],
      \"hub_resolutions\": [{\"bk_column\": \"\", \"hub_name\": \"\", \"resolution\": \"new|existing\", \"collision_risk\": \"\"}],
      \"link_signals\": [{\"participants\": [], \"relationship\": \"\", \"driving_key\": bool, \"degenerate_keys\": [], \"descriptive\": bool}],
      \"fk_signals\": [{\"column\": \"\", \"references_table\": \"\", \"references_column\": \"\"}],
      \"satellite_groups\": [{\"parent\": \"\", \"variant\": \"standard|pii|multi_active|non_historized|dep_child\", \"columns\": [], \"reason\": \"\"}],
      \"pii_columns\": [],
      \"high_change_rate_columns\": [],
      \"anti_patterns_flagged\": []
    }",
  run_in_background: true
)
```

**Spawn ALL agents in parallel** — do not wait for one to finish before starting the next. Use `agent_output` to collect all results once spawned.

**Hub registry context:** If `hub_registry.yml` exists in CWD, include its content in the agent prompt (replacing `<HUB_REGISTRY_CONTENT>`). If it does not exist, replace with "No hub registry available — treat all hubs as new."

---

### Step 4 — Cross-table aggregation

With all per-table profiling results collected, apply the following analysis:

#### 4a — Hub identification

For each table's candidate BKs:
- If a BK column has uniqueness ratio > 0.95 and is not a FK → candidate hub
- Apply the **interaction/event table rule**: if a table's PK has NO inbound FK references from other tables in this set, it records an event between participants — NOT a hub

#### 4b — FK relationship mapping

For each detected FK signal:
- Confirm the relationship: source table FK column → target table PK column
- This becomes a link between the two hub entities

#### 4c — Interaction/event table detection

Tables where:
- The PK is a surrogate (auto-increment pattern or UUID-like)
- The PK has NO inbound FK references from any other table in the set
- The table has 2+ FK columns pointing to other tables

→ Model as: **link** between participant hubs + **dep-child satellite** using the event PK as the dep-child key. Do NOT create a hub.

**Exception:** If another table in the set DOES have a FK referencing this table's PK, the entity has an independent lifecycle → hub IS warranted.

#### 4d — Shared hub resolution

If the same BK concept appears as a FK in multiple tables:
- One hub, shared across all referencing tables
- Each referencing table contributes its own satellite(s)

#### 4e — Satellite classification

For each table's non-BK, non-FK columns:
- **PII columns** → separate `_pii` satellite (or user-defined sensitive data label)
- **High-change-rate columns** (balance, amount, score, status) → separate satellite from slow-changing descriptors
- **Multi-value signals** (multiple rows per BK) → multi-active satellite candidate
- **Remaining descriptive columns** → standard satellite grouped by parent entity

---

### Step 5 — Present model proposal

Present the aggregated vault model to the user:

```
PROPOSED VAULT MODEL
====================
Source: <system name> (badge: <BADGE>)
Universe scope: <N> tables from <schema_path>

HUBs
  HUB_<NAME>     BK: <column> (<format_pattern>)     from: <source_table>
  ...

LINKs
  LNK_RV_<BADGE>_<NAME>    connects: HUB_A ↔ HUB_B    from: <source_table>
  ...

SATELLITEs
  SAT_RV_HUB_<HUB>_<BADGE>        variant: standard       tracks: <column_list>
  SAT_RV_HUB_<HUB>_<BADGE>_PII    variant: standard       tracks: <pii_columns>  (segregated)
  SAT_DP_RV_LNK_<LNK>             variant: dep-child      dep-key: <surrogate>   tracks: <column_list>
  ...

OPEN QUESTIONS
  ? <anything ambiguous — e.g., "Is TRANSACTION_ID ever referenced externally?">
```

Also render a **mermaid erDiagram** showing the proposed model visually.

---

### Step 6 — User confirmation

Ask the user to confirm or adjust the proposal. Ask as an **options** question with header `"Model Review"`:
> "Does this model look correct? What would you like to change?"
> - **Looks good — proceed** — model is confirmed
> - **Adjust hubs** — change hub identification (promote/demote entities)
> - **Adjust links** — change link participants or add/remove links
> - **Adjust satellites** — change satellite splits or variants
> - **Start over** — re-run with different table selection

If the user requests adjustments, apply them and re-present Step 5.

Once confirmed:
> "Model confirmed. Run `/dv-universe` for each landing file/table to author the manifests. The profiling results above give you the answers for the universe questions (BKs, links, satellites, delivery modes)."

Present a summary table mapping each source table to its recommended `/dv-universe` invocation:

```
NEXT STEPS — /dv-universe invocations
======================================
  1. /dv-universe → badge: <BADGE>, file: <TABLE_1> → HUB_X, SAT_RV_HUB_X_...
  2. /dv-universe → badge: <BADGE>, file: <TABLE_2> → HUB_Y (existing), LNK_RV_...
  ...
```

Note which hubs will be **new** (first universe to define them) vs **existing** (already defined by a prior invocation — passive integration).

---

### Step 7 — Discovery report (optional)

After the model is confirmed, ask as an **options** question with header `"Discovery Report"`:
> "Would you like me to generate a discovery report? This produces a shareable HTML file with diagrams, profiling results, and the confirmed model."
> - **Yes — generate report** — produce the HTML report
> - **No — I'm done** — end the workflow

If yes, generate an HTML report file saved to the project directory as `discovery_report_<BADGE>.html`. The report must include:

**Report sections:**

1. **Header** — Source system name, badge, schema location, date

2. **Source table inventory** — Table name, row count, column count

3. **Profiling results per table:**
   - Column inventory (name, type, nullable, distinct count, null rate)
   - BK candidates with scores and sample values
   - FK signals detected
   - PII columns flagged
   - High-change-rate columns

4. **Cross-table relationship map** — Mermaid erDiagram rendered as SVG showing FK relationships between source tables (the source model, not the vault model)

5. **Proposed vault model diagram** — Mermaid erDiagram rendered as SVG showing proposed hubs, links, and satellites with their relationships

6. **Model detail tables:**
   - Hubs: name, BK column, source table, format pattern
   - Links: name, participants, source table, dep-child key (if applicable)
   - Satellites: name, parent, variant, columns tracked, PII flag

7. **Next steps** — The /dv-universe invocation plan from Step 6

8. **Open questions** — Any unresolved ambiguities flagged during profiling

**Report format requirements:**
- Self-contained HTML (inline CSS, inline SVG diagrams)
- Use the `/html-authoring` skill rules for sandboxed rendering (no CDN, no inline event handlers)
- Mermaid diagrams rendered as static SVG embedded in the HTML (use the mermaid library from `/libs/` if available, otherwise render as code blocks)
- Professional layout suitable for sharing in mob modelling sessions or attaching to JIRA tickets
- File saved as: `discovery_report_<BADGE>.html` in the current working directory

---

## Rules

- **No text input** — tables must exist in Snowflake. If a user tries to paste DDL or descriptions, respond: "This skill works from live tables only. Please land the tables first (even as empty DDL with sample rows), then re-run /dv-discover."
- **Single universe** — one invocation = one source system = one badge. Multiple sources = multiple invocations.
- **Parallel profiling** — always spawn profiling agents concurrently, never sequentially. This is a hard requirement for performance.
- **Never generate DDL** — this skill proposes and explains only. No CREATE TABLE statements.
- **Interaction/event table rule** — a table whose PK has no inbound FK references from other tables in the set is an event/transaction, not a hub. Model as link + dep-child satellite. Only promote to hub if another table references its PK.
- **Hub naming is a business responsibility** — flag any hub name as "proposed" and note it requires business domain expert sign-off.
- **Composite natural keys** → likely a link, not a hub (flag for user confirmation).
- **Reference/lookup tables** (< ~10k rows, rarely updated) → flag as candidates for non-historized satellites.

---

## Integration with /dv-profile

This skill uses `/dv-profile`'s profiling logic (Steps 1-4) internally via parallel agents. The key difference:

| | /dv-profile | /dv-discover |
|---|---|---|
| Scope | Single table | Multiple tables (whole universe) |
| Execution | Sequential (interactive) | Parallel agents (non-interactive, auto-detect only) |
| Methodology | Full 10-step with user questions | Full 10-step auto-detectable parts (no interactive Qs) |
| Output | Per-table profiling report | Confirmed vault model + next-step plan for /dv-universe |
| Cross-table analysis | No (single table only) | Yes (FK detection, shared hubs, event-table rule) |
| Next step | Feeds into /dv-universe Q5 | User runs /dv-universe per table with answers pre-known |

---

## Reference: Mob Modelling

See [references/mob-modelling.md](references/mob-modelling.md) for:
- Mob modelling guidance (roles, polysemy, false cognates, complementary techniques)
- Data profiling questionnaire (Sections A-D, 41 questions for pre-mob homework)
- CDM / LDM / PDM context in Data Vault

---

For full eval scenarios and pass criteria, see `TESTING.md` in the plugin root.
