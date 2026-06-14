---
name: dv-discover
description: Analyze source tables or schema descriptions and propose a Data Vault 2.0 model (hubs, links, satellites)
enabled: true
---

# /dv-discover — Source Discovery & Vault Model Proposal

Analyze one or more source systems and propose a Data Vault 2.0 model. Works from table names, DDL, CSV headers, or plain-language descriptions.

## Input

The user may provide:
- DDL (`CREATE TABLE ...`)
- A list of table names with column names
- A CSV header row
- A plain-language description ("we have a CRM with customers, orders, and products")
- A path to a SQL or schema file

If the user provides nothing, ask:
> "What source tables or systems are you modeling? You can paste DDL, column names, or just describe what you have."

## Steps

### 1 — Spawn the Source Profiler subagent

Read `agents/source-profiler.md` for the full system prompt.

Pass the user's source description as input. Ask the Profiler to return a structured report with:
- For each table: candidate business keys (with uniqueness rationale), relationship signals, row-level behaviour (insert-only vs. updates), PII indicators
- Cross-table relationships detected

### 2 — Spawn the Pattern Recommender subagent (once per entity)

Read `agents/pattern-recommender.md` for the full system prompt.

For each entity identified by the Profiler, ask the Recommender to choose:
- Hub or Link (is this a business entity or a relationship between entities?)
- Which satellite variant fits (`standard`, `ma`, `ef`, `dp`, `nh`, `st`, `rt`, `xt`) — see `/dv-explain satellite variants`
- Whether a PII naming suffix is needed (any type can have `_pii` suffix for access segregation)
- Any overlays needed (same-as link, PIT table)

### 3 — Assemble the vault model proposal

Present the proposed model to the user as a structured summary:

```
PROPOSED VAULT MODEL
====================
Source: <system name>

HUBs
  HUB_<NAME>     business key: <column>     from: <source table>
  ...

LINKs
  LNK_<NAME>     connects: HUB_A ↔ HUB_B   from: <source table>
  ...

SATELLITEs
  SAT_<HUB>_<CONTEXT>   variant: <type>   tracks: <column list>
  ...

OPEN QUESTIONS
  ? <anything ambiguous that the user needs to confirm>
```

### 4 — Ask for confirmation before proceeding

Do not generate code or a manifest automatically. Ask:
> "Does this look right? Any hubs or links missing? Once you confirm, use `/dv-model` to define each construct in detail, or `/dv-generate` to produce SQL."

## Rules

- Never invent a business key — if it's unclear, flag it as an open question
- A table with a composite natural key should be a Link, not a Hub, unless the user says otherwise
- Reference/lookup tables (< ~10k rows, rarely updated) should be flagged as candidates for non-historized satellites
- Tables with multiple rows per entity key are multi-active satellite candidates
- If PII columns are detected (email, SSN, DOB, phone, name patterns), flag for PII satellite segregation

## Subagent files

- Source Profiler: `agents/source-profiler.md`
- Pattern Recommender: `agents/pattern-recommender.md`
