---
name: dv-pit-bridge
description: When and how to build PIT tables and Bridge tables in DVOS — manifest config, materialisation options, bridge types, naming rules.
enabled: true
---

# /dv-pit-bridge — PIT Tables and Bridge Tables

Query-assist structures built on top of the Raw Vault. They never store raw source data — they pre-compute joins and temporal alignments for IM query performance.

---

## Point-in-Time (PIT) Tables

### When to build a PIT

Build a PIT when:
- A hub has **3 or more satellites** and the IM needs to join them together
- Queries need **as-of snapshots** across multiple satellites at the same point in time
- Correlated subqueries across satellites are hurting IM query performance

Not needed when:
- Only one satellite per hub (QUALIFY ROW_NUMBER directly in the view is sufficient)
- The IM never needs historical snapshots — current-state only views work fine

### Two PIT variants

| Variant | Use when |
|---|---|
| `PIT` | Standard satellites, effectivity satellites, standard temporal alignment on `dv_applied_timestamp` |
| `SNOPIT` | Multi-active (`ma`) or dependent-child (`dp`) satellites — uses `dv_sid` row locator instead of `dv_applied_timestamp` |

### Manifest declaration

```yaml
pits:
  - name: <pit_name>                     # lowercase, e.g. customer
    base_artefact: hub_<name>            # or lnk_<name>
    output_table: pit_<name>             # e.g. pit_customer
    cadence_flags:                       # which AS_OF dates drive this PIT
      - daily                            # default
      # - monthly, weekly, hourly, etc.
    satellites:
      - sat_<name>_<context>: standard
      - sat_<name>_<context2>: standard
      # - sat_<name>_<context3>: ma      # use 'ma' for multi-active
    materialization: dynamic_table       # or ctas
    lag: "1 hour"                        # Dynamic Table only
    warehouse: TRANSFORM_WH              # Dynamic Table only
    tenant_id: <value>                   # optional: filter to one tenant
    collisioncode: <value>               # optional: filter to one BKCC
```

### Materialisation options

| Option | Behaviour |
|---|---|
| `dynamic_table` | Snowflake Dynamic Table — auto-refreshes based on `TARGET_LAG` schedule. **Preferred for production.** |
| `ctas` | `CREATE OR REPLACE TABLE AS SELECT` — full rebuild on every run. Simpler for dev/UAT. |

`dynamic_table` is preferred for production because:
- Auto-refreshes when upstream vault tables change
- No orchestration needed — Snowflake handles scheduling
- Supports incremental refresh when possible
- `TARGET_LAG` controls freshness vs. cost tradeoff

`ctas` is simpler for dev/UAT where manual control is preferred.

### PIT columns produced

Per satellite in the PIT:
- `{sat_alias}_dv_applied_timestamp` — temporal alignment column; NULL if no record at snapshot
- `{sat_alias}_{hashkey_col}` — forward-filled via `LAST_VALUE IGNORE NULLS`; ghost key if no record

Plus: `dv_hashkey_hub_<name>` (or link hashkey) + `SNAPSHOT_DATE`.

### Ghost records

Every satellite included in a PIT must have a ghost record (all-zero hash key row). DVOS inserts these automatically. Without them, PIT null-joins fail.

---

## Bridge Tables

### When to build a Bridge

Build a Bridge when:
- An IM view must traverse **multiple links** from an anchor hub
- Cross-link joins are frequent and expensive
- You need to "shorten the distance" between hubs connected through several relationships
- A fact-grain pre-join is needed (fact bridge)

Not needed when:
- There is only one link to traverse (join directly in the IM view)
- The relationship is static and rarely queried across multiple hops

### Three bridge types

| Type | Use when |
|---|---|
| `relationship` | Full cross-product of all current relationships along the path. No temporal filter. |
| `current_relationship` | Relationship is time-bounded. Joins an effectivity satellite and filters by `as_of` date: `BETWEEN dv_start_date AND dv_end_date`. |
| `fact` | Pre-computes satellite joins at the grain of a chief satellite. Stores `DV_SID` locators + metrics. IM view joins satellites via single-row equi-join on `DV_SID` — no temporal logic at query time. |

### Naming rules (BDG-NAME-001)

- Name: `bdg_{business_concept}_{snapshot_or_period}` — lowercase, e.g. `bdg_partyaccount_daily`
- Output table: uppercase mirror — `BDG_PARTYACCOUNT_DAILY`
- **`brdg_` and `bridge_` prefixes are not permitted**

### Path rules

The `path` field must alternate hub → link → hub:
- Odd number of elements (minimum 3)
- Even positions (0, 2, 4...): hubs (`hub_*`)
- Odd positions (1, 3, 5...): links (`lnk_*`)

```
hub_party → lnk_rv_customer_account → hub_account                     (3 elements, 1 hop)
hub_party → lnk_rv_customer_account → hub_account → lnk_account_product → hub_product   (5 elements, 2 hops)
```

### Manifest declaration — relationship bridge

```yaml
bridges:
  - name: bdg_partyaccount_daily
    bridge_type: relationship
    path:
      - hub_party
      - lnk_rv_customer_account
      - hub_account
    output_table: BDG_PARTYACCOUNT_DAILY
    tenant_id: <value>          # optional
    collisioncode: <value>      # optional
```

### Manifest declaration — current_relationship bridge

```yaml
bridges:
  - name: bdg_partyaccount_current
    bridge_type: current_relationship
    path:
      - hub_party
      - lnk_rv_customer_account
      - hub_account
    effectivity_satellite: sat_lnk_rv_customer_account_eff   # required — must be type: ef
    output_table: BDG_PARTYACCOUNT_CURRENT
```

### Manifest declaration — fact bridge

```yaml
bridges:
  - name: bdg_transactions_daily
    bridge_type: fact
    path:
      - hub_account
      - lnk_rv_account_transaction
      - hub_transaction
    chief_satellite: sat_nh_rv_hub_transaction_detail    # drives the fact grain
    satellite_locators:                                  # satellites to resolve DV_SID for
      - sat_account_demographics
      - sat_transaction_enrichment
    metrics:                                             # columns from chief sat to include
      - amount
      - currency_code
    output_table: BDG_TRANSACTIONS_DAILY
```

### Using a Bridge in the IM

See `/dv-mart` for the bridge-based view pattern. Bridge-based IM views join the pre-built `BDG_*` table instead of traversing links at query time — one join replaces N.

---

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
