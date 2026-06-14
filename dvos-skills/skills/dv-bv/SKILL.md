---
name: dv-bv
description: Design and deploy Business Vault constructs — BV links and BV satellites. Covers delivery modes, rule views, staging, and doctrine rules.
enabled: true
---

# /dv-bv — Business Vault Links and Satellites

Business Vault (BV) constructs sit above the Raw Vault. They capture derived relationships and enriched attributes produced by business rules — not raw source data. All BV constructs are INSERT-only, same as Raw Vault.

---

## BV Links

A BV link captures a relationship that does not exist in any source system directly — it is derived by a business rule (e.g. an account-to-product link inferred from transaction history, or a hierarchy link derived from a rules engine).

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

## BV doctrine rules

| Rule | Severity | Description |
|---|---|---|
| DV-BV-100 | WARNING | BV satellite must declare `mode: landed` or `mode: view` |
| DV-BV-101 | ERROR | Landed BV satellite must declare `source_badge` and `source_file` |
| DV-BV-102 | WARNING | Virtual BV satellite must not produce execution units |
| DV-BV-103 | WARNING | Landed BV satellite should declare `depends_on_sources` |
| DV-BV-110 | ERROR | BV staging and loaders must not contain UPDATE/DELETE/TRUNCATE |
| DV-BV-111 | ERROR | `dv_applied_timestamp` must NOT use `CURRENT_TIMESTAMP()` — derive from RV sources |

---

## Key rules

- BV constructs are INSERT-only — same immutability doctrine as Raw Vault
- `dv_applied_timestamp` must be derived from contributing RV data (`GREATEST` of source timestamps), never `CURRENT_TIMESTAMP`
- The business rule view (`bv_{concept_name}`) must output business keys only — DVOS adds all hashkeys and hashdiff in staging
- `depends_on_sources` controls scheduler dependency on RV load units — always declare it for correct execution ordering
- BV links with role-playing participants must declare `role` per participant

---

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
