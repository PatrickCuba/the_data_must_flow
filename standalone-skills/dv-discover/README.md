# standalone-discover — Data Vault Discovery & Model Proposal

Profile live Snowflake source tables, detect relationships, and propose a complete
Data Vault model (hubs, links, satellites) for a single source system. No DVOS
installation or other skills required.

## Installation

**Option A — from tarball:**

```bash
tar -xzf standalone-discover-plugin.tar.gz -C ~/.snowflake/cortex/plugins/
```

**Option B — from folder:**

```bash
cp -r standalone-discover ~/.snowflake/cortex/plugins/
```

Restart Cortex Code (or reload plugins). The skill will appear as `/dv-discover`.

## Usage

```
/dv-discover
```

The skill runs interactively through these phases:

### Phase 1 — Table Selection
- Asks for the Snowflake schema containing your source/landing tables
- Lists available tables and asks which to include in discovery
- Confirms source system badge (e.g. CRM, ERP, XERO)

### Phase 2 — Parallel Profiling
- Spawns one profiling agent per table (runs in parallel)
- Each agent executes the full 9-step profiling methodology:
  1. Metadata extraction (row count, column inventory)
  2. Per-column profiling (cardinality, nulls, min/max, patterns)
  3. Business key scoring (uniqueness, stability, format)
  4. BK format derivation (regex patterns, smart key detection)
  5. BK classification (natural, surrogate, composite)
  6. Hub resolution (which hub each BK belongs to)
  7. Link discovery (FK relationships, intersection patterns)
  8. Satellite classification (descriptive, status, multi-active, XTS)
  9. Collision risk assessment (BK overlap with existing vault)

### Phase 3 — Model Proposal
- Aggregates profiling results across all tables
- Proposes hubs, links, and satellites
- Presents the model for user confirmation/adjustment

### Phase 4 — Optional Report
- Generates an HTML discovery report with diagrams
- Includes entity-relationship overview, profiling details, and recommendations

## Requirements

- Snowflake connection configured in `~/.snowflake/connections.toml`
- Source tables must exist in Snowflake (landed, even if empty with sample rows)
- Cortex Code Desktop with agent/task tool support

## What This Does NOT Do

- Does not create manifests or YAML files (use `/dv-universe` from the full DVOS plugin for that)
- Does not deploy anything to Snowflake
- Does not require DVOS CLI or any Python packages

## Contents

```
standalone-discover/
  .cortex-plugin/plugin.json    — manifest
  README.md                     — this file
  skills/dv-discover/SKILL.md   — main skill (367 lines)
  skills/dv-discover/references/mob-modelling.md — discovery questionnaire reference
```
