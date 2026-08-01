---
name: dv-kg-2
description: "Standalone Knowledge Graph for any Data Vault. Discovers vault topology from Snowflake metadata, deploys KG views for AI-agent discovery and traversal, and optionally generates a data semantic view, Cortex Analyst, and Agent template. No DVOS or manifest required."
triggers:
  - dv-kg-2
  - standalone knowledge graph
  - kg without dvos
  - knowledge graph standalone
  - deploy kg
  - vault knowledge graph
  - data vault graph
enabled: true
---

# /dv-kg-2 — Standalone Knowledge Graph for Data Vault

Deploy a Knowledge Graph over **any** existing Data Vault without requiring DVOS,
a manifest, or any external framework. This skill:

1. **Discovers** vault topology by scanning Snowflake metadata (INFORMATION_SCHEMA)
2. **Deploys** KG views for AI-agent discovery and graph traversal
3. **Optionally** generates a data semantic view, Cortex Analyst, and Agent template

**Premise:** Hub = Node, Link = Edge, Satellite = Property. The vault IS the graph.

---

## Phase 1: Discovery

### Gate A — Vault Location

Ask (text, header "Vault Location"):
> "Which database and schema contain your Data Vault tables? (e.g. `ANALYTICS.RAW_VAULT`)"

Validate the schema exists:
```sql
SELECT COUNT(*) AS table_count
FROM <database>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<schema>'
  AND TABLE_TYPE = 'BASE TABLE';
```

If zero tables: warn and ask user to verify.

Then ask (options, header "Multi-schema"):
> "Do your vault tables span multiple schemas (e.g. hubs in one, sats in another)?"
> - **Single schema** — everything is in the schema above
> - **Multiple schemas** — I'll need additional schema locations

If multiple: ask for each additional schema and what it contains (hubs/links/sats).

Store as `vault_schemas` — a list of `{database, schema, contains: [hub|link|sat]}`.

---

### Gate B — Convention Detection

Run this query against each vault schema:
```sql
SELECT TABLE_NAME
FROM <database>.INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = '<schema>'
  AND TABLE_TYPE = 'BASE TABLE'
ORDER BY TABLE_NAME;
```

Apply heuristic pattern matching against the full table list:

**Detection rules (try in order):**

| Priority | Pattern | Classification |
|----------|---------|----------------|
| 1 | Prefix `HUB_` (case-insensitive) | Hub |
| 2 | Prefix `H_` | Hub |
| 3 | Suffix `_HUB` | Hub |
| 4 | Prefix `LNK_` or `LINK_` | Link |
| 5 | Prefix `L_` | Link |
| 6 | Suffix `_LNK` or `_LINK` | Link |
| 7 | Prefix `SAT_` or `SATELLITE_` | Satellite |
| 8 | Prefix `S_` | Satellite |
| 9 | Suffix `_SAT` | Satellite |

**If confident** (at least one prefix covers >80% of expected artefact count per type):

Present findings:
> "I detected the following naming patterns:
> - Hubs (N tables): prefix `HUB_` — e.g. HUB_CUSTOMER, HUB_ACCOUNT, ...
> - Links (N tables): prefix `LNK_` — e.g. LNK_CUSTOMER_ACCOUNT, ...
> - Satellites (N tables): prefix `SAT_` — e.g. SAT_CUSTOMER_CRM, ...
>
> Does this look correct?"

Ask (options, header "Confirm Patterns"):
> - **Yes, that's correct**
> - **Partially correct** — let me adjust

If partially correct or no clear pattern detected, ask (text, header "Hub Pattern"):
> "What prefix (or suffix) identifies hub tables? Include the underscore. (e.g. `HUB_`)"

Repeat for link and satellite patterns.

**Unclassified tables:** Any tables not matching hub/link/sat patterns are reported as
"unclassified" and ignored. The user can manually assign them if relevant.

---

### Gate C — Hashkey vs Natural Key Detection

For one representative hub and one representative link, inspect columns:
```sql
SELECT COLUMN_NAME, DATA_TYPE
FROM <database>.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<sample_hub>'
ORDER BY ORDINAL_POSITION;
```

**Detection logic:**
- If any column has `DATA_TYPE = 'BINARY'` and name contains `HASHKEY` or `HK` → **hashkey vault**
- If no BINARY hashkey columns found → **natural-key vault**

Store as `vault_style: hashkey | natural_key`.

For hashkey vaults, also detect the hashkey naming pattern:
- `DV_HASHKEY_HUB_<name>` (DVOS-style)
- `HK_<name>` or `<name>_HK`
- `HASHKEY_<name>`

Store as `hashkey_pattern` for use in association resolution.

---

### Gate D — Hub Business Key Extraction

For each discovered hub, extract business key columns:
```sql
SELECT COLUMN_NAME, DATA_TYPE, ORDINAL_POSITION
FROM <database>.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<hub_table>'
ORDER BY ORDINAL_POSITION;
```

**Business key identification:**
- Skip metadata columns: any column matching these patterns is NOT a BK:
  - `*HASHKEY*`, `*HK*`, `DV_*`, `LOAD_*`, `RECORD_SOURCE*`, `*_TIMESTAMP`,
    `*_DATE` (when named `LOAD_DATE` or `INSERT_DATE`), `TENANT*`, `COLLISION*`,
    `LAST_SEEN*`, `SOURCE*`
- Remaining columns (typically VARCHAR/NUMBER) are business keys

Store as `hubs[].business_keys: list[str]`.

---

### Gate E — Association Resolution (Links)

For each discovered link, resolve which hubs it connects using the algorithm in
[references/association-resolution.md]. The priority order is:
1. Exact hub name match in FK column
2. Suffix match (for role-playing links)
3. Substring match (longest match wins)
4. Same-hub classification (hierarchy/same-as/role-playing)
5. User input (for unresolvable columns)

**Unresolvable links:** If a FK column cannot be resolved, ask the user which hub it references.

Expected automatic resolution rate: ~96% of links in a well-structured vault.

---

### Gate F — Association Resolution (Satellites)

For each discovered satellite, determine its parent (one hub OR one link):

**Hashkey vault:**
- Inspect the first BINARY/hashkey column after the satellite's own hashkey (if it has one)
  or the first BINARY column overall
- Match the embedded name to a discovered hub or link

**Natural-key vault:**
- Inspect non-metadata columns and find which hub or link's key columns they contain
- A satellite's parent is the entity whose PK/hashkey it carries as its own FK

**Name-based fallback:**
- Strip the satellite prefix. If the remaining name contains a hub name → that's the parent.
  E.g. `SAT_CUSTOMER_CRM` → contains `CUSTOMER` → parent is `HUB_CUSTOMER`
- If remaining name contains a link name → parent is that link.
  E.g. `SAT_CUSTOMER_ACCOUNT_DETAILS` → contains `CUSTOMER_ACCOUNT` → parent is `LNK_CUSTOMER_ACCOUNT`

**Unresolvable satellites:**

If a satellite's parent cannot be determined, ask:

> "I couldn't determine the parent for these satellites. Each satellite belongs to
> exactly ONE parent — either a hub or a link:
>
> | Satellite | Columns suggest | Your correction |
> |-----------|----------------|-----------------|
> | SAT_MYSTERY_ONE | ? | [hub or link name] |
> | SAT_OTHER_THING | ? | [hub or link name] |"

---

### Gate G — Confirmation

Present the complete discovered graph:

```
=== DISCOVERED DATA VAULT TOPOLOGY ===

NODES (Hubs): <count>
  <HUB_TABLE> (BK: <bk_col_1>, <bk_col_2>)
  ...

EDGES (Links): <count>
  <LNK_TABLE>: <HUB_A> <--> <HUB_B>
  <LNK_HIERARCHY>: <HUB_X> (parent) <--> <HUB_X> (child)  [hierarchy]
  ...

PROPERTIES (Satellites): <count>
  <SAT_TABLE> --> <PARENT_HUB_OR_LINK>
  ...

UNCLASSIFIED (ignored): <count>
  <TABLE_1>, <TABLE_2>, ...
```

Ask (options, header "Confirm Graph"):
> "Does this graph look correct?"
> - **Yes, proceed**
> - **I need to make corrections** — (then ask what to fix)

After confirmation, inform the user:
> "Note: KG_NODE includes enrichment columns (domain, criticality, description, agent_hint)
> that start NULL. These can be populated later via UPDATE statements or an AI enrichment pass."

---

## Phase 2: KG Deployment

### Gate H — Target Location

Ask (text, header "KG Schema"):
> "Where should the KG views be deployed? Provide database.schema (will be created if it doesn't exist). Example: `ANALYTICS.KG`"

### Gate I — Deployment Mode

Ask (options, header "Mode"):
> "How should I deploy?"
> - **Execute directly** — run all DDL against Snowflake now
> - **Generate script** — produce a .sql file for you to review and run

### Gate J — Execute

**Schema setup:**
```sql
CREATE DATABASE IF NOT EXISTS <kg_database>;
CREATE SCHEMA IF NOT EXISTS <kg_database>.<kg_schema>;
```

**Base views:** Generate the 4 base KG views using the SQL templates in [references/base-views.md]:
- **KG_NODE** — one UNION ALL branch per hub (node_key, entity_label, grain, business_keys, NULL enrichment cols)
- **KG_EDGE** — one UNION ALL branch per link (edge_key, relationship_label, NULL enrichment cols)
- **KG_EDGE_ENDPOINT** — one branch per link-participant (edge_key, node_key, role_description)
- **KG_PROPERTY** — one branch per satellite (property_key, attached_to, attached_to_type)

For hierarchy/same-as links, KG_EDGE_ENDPOINT has both rows pointing to the same hub with different role_description values.

### Derived views — SEQUENTIAL EXECUTION (do NOT run in parallel)

Execute the 4 view SQL statements from [references/derived-views.md] ONE AT A TIME, in order.
Wait for each to succeed before running the next. The order is:
1. KG_NODE_V
2. KG_EDGE_V
3. KG_ADJACENCY — **CRITICAL:** The join uses COALESCE to handle NULL role_description:
   ```sql
   AND NOT (e1.node_key = e2.node_key AND COALESCE(e1.role_description,'') = COALESCE(e2.role_description,''))
   AND COALESCE(e1.node_key,'') || COALESCE(e1.role_description,'')
       <= COALESCE(e2.node_key,'') || COALESCE(e2.role_description,'')
   ```
   Copy this exactly from [references/derived-views.md]. Without COALESCE, NULL comparisons filter out most rows.
4. KG_NODE_DEGREE — uses UNION ALL of from_node + to_node (copy from reference)

See [references/derived-views.md] for the full SQL templates.

**Script mode:** If user chose "Generate script", write all SQL statements to a file at
`standalone-kg-plugin/output/deploy_kg.sql` and present the path. Do not execute.

---

### Verify

After deployment, run:
```sql
SELECT * FROM <kg_db>.<kg_schema>.KG_NODE_V;
SELECT * FROM <kg_db>.<kg_schema>.KG_ADJACENCY;
SELECT COUNT(*) AS nodes FROM <kg_db>.<kg_schema>.KG_NODE;
SELECT COUNT(*) AS edges FROM <kg_db>.<kg_schema>.KG_EDGE;
SELECT COUNT(*) AS properties FROM <kg_db>.<kg_schema>.KG_PROPERTY;
```

Present results to confirm expected counts match discovery.

### Gate J2 — Enrichment

After verification, note that the KG views have NULL enrichment columns (`domain`, `criticality`,
`description`, `agent_hint` on KG_NODE; `purpose`, `description`, `agent_hint` on KG_EDGE).
These improve AI-agent routing but require manual curation or inference.

Ask (options, header "Enrichment"):
> "The KG views are deployed but have empty enrichment columns (domain, description, agent_hint).
> Would you like me to populate these from your vault table COMMENT metadata?"
> - **Yes, infer from table comments** — read COMMENT_ON from INFORMATION_SCHEMA and populate
> - **No, leave empty for now** — enrichment will be done manually later

If yes: query `INFORMATION_SCHEMA.TABLES` for `COMMENT` on each hub/link/satellite, then
regenerate the KG_NODE and KG_EDGE views with the comments mapped to `description`.
For `domain`, infer from the source-system badge in the satellite names (e.g. `_CRM_` → `crm`,
`_HC_` → `healthcare`, `_INS_` → `insurance`).

### Gate J3 — Data Semantic View

Ask (options, header "Data Semantic View"):
> "Would you like me to generate a semantic view over your actual vault data?
> This creates a queryable model where hubs are entities, satellites provide
> attributes/facts, and links define the joins between them."
> - **Yes, generate it**
> - **No, skip this**

If no: skip to Phase 3b.

### Gate L — Target

Ask (text, header "Semantic View Location"):
> "Where should the data semantic view be deployed? (database.schema.view_name)
> Example: `ANALYTICS.SEMANTIC.VAULT_DATA_MODEL`"

### Generate Semantic View YAML

Build a semantic view YAML with:

**Tables section:**
- Each hub becomes a table entry with its business keys as dimensions
- Each satellite becomes a table entry with:
  - Parent hashkey/FK as the join key
  - All non-metadata columns as dimensions (VARCHAR/DATE) or measures (NUMBER)
  - Measures default to `aggregation: sum` for numeric columns

**Relationships section:**
- Each satellite → its parent hub/link: join on hashkey/FK, one_to_many
- Each link → its participant hubs: join on respective hashkey/FK columns

**Column classification heuristics:**
- `*_ID`, `*_CODE`, `*_KEY`, `*_NAME`, `*_TYPE`, `*_STATUS` → dimension
- `*_AMOUNT`, `*_QTY`, `*_COUNT`, `*_TOTAL`, `*_BALANCE`, `*_RATE` → measure
- `*_DATE`, `*_TIMESTAMP` (non-DV metadata) → time_dimension
- Everything else → dimension (safer default)

**Output:** Write YAML to `standalone-kg-plugin/output/data_semantic_model.yml`

**Deploy the semantic view** by calling the stored procedure with the YAML inlined:

```sql
-- Concrete example: creates DATA_SEMANTIC_MODEL in LIB_DEV01_EDW.ANALYTICS
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'LIB_DEV01_EDW.ANALYTICS',
  $$
name: DATA_SEMANTIC_MODEL
description: Vault data model for Cortex Analyst

tables:
  - name: CUSTOMERS
    base_table:
      database: LIB_DEV01_EDW
      schema: SAL
      table: HUB_CUSTOMER
    primary_key:
      columns:
        - CUSTOMER_HK
    dimensions:
      - name: CUSTOMER_ID
        expr: CUSTOMER_ID
        data_type: VARCHAR(16777216)
    metrics:
      - name: TOTAL_CUSTOMERS
        expr: COUNT(CUSTOMER_HK)
  $$
);
```

Adapt this pattern for your deployment:
- Replace `'LIB_DEV01_EDW.ANALYTICS'` with `'<target_db>.<target_schema>'` from Gate G
- Replace the YAML body with the full generated content from the previous step
- The `name:` field inside the YAML becomes the semantic view object name

---

## Phase 3b: KG Semantic View + Cortex Analyst + Agent Template (Optional)

### Gate M — Offer

Ask (options, multiSelect, header "AI Products"):
> "Which additional AI products would you like?"
> - **KG Semantic View** — a semantic model over the KG views for Cortex Analyst
> - **Cortex Agent template** — a ready-to-deploy agent config with KG + data tools

### KG Semantic View

Generate the KG semantic view YAML inline using the semantic view format required by
`SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML`. The YAML must include:
- `name:` — the semantic view object name (e.g. `KG_SEMANTIC_MODEL`)
- `base_table:` per logical table with `database`, `schema`, `table`
- `primary_key:` on tables referenced by relationships (KG_NODE_V, KG_EDGE_V)
- `dimensions:` with `expr:` and `data_type:` for each column
- `facts:` / `metrics:` for numeric aggregations
- `relationships:` using `many_to_one` from FK table to PK table

Use `reference/kg-semantic-model.yml` as the structural template. Replace placeholders
with actual database/schema values discovered in Phase 1.

Deploy:

Call the stored procedure with the generated YAML inlined (see [references/semantic-view-deployment.md] for a full worked example):

```sql
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  '<kg_db>.<kg_schema>',
  $$
name: KG_SEMANTIC_MODEL
description: Knowledge Graph catalog for a Data Vault.
tables:
  - name: KG_NODE_V
    base_table:
      database: <kg_db>
      schema: <kg_schema>
      table: KG_NODE_V
    primary_key:
      columns:
        - NODE_KEY
    dimensions:
      - name: NODE_KEY
        expr: NODE_KEY
        data_type: VARCHAR(16777216)
      ...
  $$
);
```

Use the full YAML generated from `reference/kg-semantic-model.yml` with all 4 KG tables (KG_NODE_V, KG_EDGE_V, KG_ADJACENCY, KG_PROPERTY).

### Cortex Agent Template

Generate an agent YAML configuration file using `reference/agent-template.yml` as the
base structure. Parameterize with:
- KG semantic view location
- Data semantic view location (if generated)
- Vault topology summary in the system prompt
- List of discovered entities and relationships

Write to `standalone-kg-plugin/output/agent_config.yml`.

Present to user:
> "Agent template generated at `output/agent_config.yml`.
> Review the system prompt and tool definitions, then deploy with:
> ```sql
> CREATE OR REPLACE CORTEX AGENT <name> FROM '<stage>/agent_config.yml';
> ```"

Do NOT auto-deploy the agent — it requires user review of the system prompt and
guardrails.
---

### Semantic View Deployment Reference

See [references/semantic-view-deployment.md] for a complete working example with full YAML body.

Key points:
- Call `SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML('DATABASE.SCHEMA', $$yaml$$)`
- First arg = 2-part target schema (e.g. `'LIB_DEV01_EDW.KG'`)
- Second arg = full YAML content as `$$` dollar-quoted string
- View name comes from `name:` field inside the YAML

---

## Pass Criteria

A successful run produces:
- KG_NODE returns one row per hub (node count = hub count)
- KG_EDGE returns one row per link (edge count = link count)
- KG_EDGE_ENDPOINT returns one row per link-participant
- KG_ADJACENCY returns bidirectional edges (at least 2 rows per binary link)
- KG_NODE_DEGREE returns one row per connected node
- Object comments visible via INFORMATION_SCHEMA
- (Optional) Data semantic view compiles and is queryable
- (Optional) Agent template is valid YAML

---

## When NOT to Use This Skill

- If you have DVOS installed and a manifest → use `/dv-kg` instead (richer metadata)
- To build marts, PIT tables, or bridges → those require DVOS
- To author vault entities (create new hubs/links/sats) → this skill is read-only against the vault