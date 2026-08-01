# standalone-kg — Knowledge Graph for Data Vault

Deploy a Knowledge Graph over any existing Data Vault without DVOS or manifests.
Discovers vault topology from Snowflake metadata, builds KG views, and optionally
generates a data semantic view, Cortex Analyst model, and Agent template.

## Installation

1. Copy the `standalone-kg` folder into your Cortex Code plugins directory:

```bash
cp -r standalone-kg ~/.snowflake/cortex/plugins/
```

2. Restart Cortex Code (or reload plugins). The skill will appear as `/dv-kg-2`.

## Usage

Invoke the skill in Cortex Code:

```
/dv-kg-2
```

The skill runs interactively through these phases:

### Phase 1 — Discovery
- Asks for your vault database/schema location
- Scans INFORMATION_SCHEMA to discover hubs, links, and satellites
- Resolves link-to-hub associations (which hubs each link connects)
- Presents a summary for confirmation

### Phase 2 — KG Deployment
- Asks for a target schema for the KG views (e.g. `MY_DB.KG`)
- Deploys 8 views:
  - **KG_NODE** — one row per hub (node)
  - **KG_EDGE** — one row per link (edge)
  - **KG_EDGE_ENDPOINT** — link-to-hub participation (with roles for hierarchy/same-as)
  - **KG_PROPERTY** — one row per satellite (property)
  - **KG_NODE_V** — enriched node view with property counts
  - **KG_EDGE_V** — enriched edge view with endpoint info
  - **KG_ADJACENCY** — deduplicated from/to pairs for graph traversal
  - **KG_NODE_DEGREE** — connection count per node
- Offers enrichment from table COMMENT metadata (domain, description, agent_hint)
- Offers a **data semantic view** over the vault tables (hubs as entities, sats as attributes)

### Phase 3 — AI Products (Optional)
- **KG Semantic View** — a semantic model over KG views for Cortex Analyst
- **Cortex Agent template** — ready-to-deploy agent config with KG tools

## Requirements

- Snowflake account with access to the vault schema
- Cortex Code Desktop (CoCo) with plugin support
- CREATE VIEW privilege on the target KG schema
- CALL privilege on SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML (for semantic views)

## File Structure

```
standalone-kg/
  .cortex-plugin/plugin.json          — Plugin manifest
  skills/dv-kg-2/SKILL.md             — Main skill instructions
  skills/dv-kg-2/references/          — SQL templates and reference docs
    base-views.md                     — KG_NODE, KG_EDGE, KG_EDGE_ENDPOINT, KG_PROPERTY
    derived-views.md                  — KG_NODE_V, KG_EDGE_V, KG_ADJACENCY, KG_NODE_DEGREE
    association-resolution.md         — Link-to-hub resolution algorithm
    semantic-view-deployment.md       — SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML examples
  reference/                          — YAML templates
    kg-semantic-model.yml             — KG semantic view template
    agent-template.yml                — Cortex Agent config template
  output/                             — Generated artifacts (created during use)
```

## Notes

- The `output/` folder contains artifacts from previous runs and can be deleted safely.
- The skill handles multi-hub links (3+ participants), hierarchy links (same hub, different roles),
  and same-as links automatically.
- Generated semantic views use `CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML` — this is the only
  valid deployment method.
