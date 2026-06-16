---
name: dv-supernova
description: 5-layer Supernova data modelling pattern as Snowflake INCREMENTAL Dynamic Tables above the Data Vault. Pre-materialises satellite joins, computed attributes, and data delivery shapes to eliminate query-time joins.
enabled: true
---

# /dv-supernova — Supernova Data Modelling Pattern

Supernova (Rick F. van der Lans, ~2015) reorganises the Data Vault above the raw vault layer into a pre-materialised wide format. On Snowflake this means INCREMENTAL Dynamic Tables — refreshed automatically as vault data changes, with no query-time joins in the delivery layer.

The vault remains the auditable source of record. Supernova is a derivative layer: it can be dropped and rebuilt at any time without data loss.

---

## The 5-layer framework

| Layer | Objects | Purpose |
|---|---|---|
| 1 | Source systems | Operational data sources |
| 2 | Raw Vault + Business Vault | Hub / Link / Satellite tables (insert-only, auditable) |
| **3** | **Supernova DTs** | Merges hub + satellites into a wide, versioned Dynamic Table with physicalised `startdate` / `enddate` |
| **4** | **Extended Supernova (XSN) DTs** | Adds computed attributes, column standardisation, filtering |
| **5** | **Data Delivery** | Shaped views for BI tools — filtered, aggregate, OBT, star schema |

Layers 3–5 live in a dedicated `supernova` schema.

---

## When to choose Supernova over PIT + IM view

| Choose Supernova when | Choose PIT + IM view when |
|---|---|
| BI query performance is a bottleneck | Storage cost is a constraint |
| Result cache is insufficient (data changes too frequently) | Cross-hub traversal is needed — use Bridge instead |
| Computed attributes should be pre-materialised | Thin materialization is sufficient |
| Multi-tenancy filtering must be at the materialised layer | IM views already serve the purpose |
| The consumption layer should have zero joins | Layer count simplicity matters more than performance |

---

## Decision questions

Before generating Supernova artefacts, confirm:

1. **Which hub(s)?** One set of Supernova DTs per hub.
2. **Which link(s)?** Links get a simplified Supernova DT (no versions DT unless they have adjacent satellites).
3. **Which adjacent satellites?** Include both RV and BV satellites. Satellites with `type: nh` (non-historised) can be included — they have no hashdiff comparison so the LEAD window is simpler.
4. **Does the hub have no satellites yet?** If yes, use the simplified versions DT (hub applied date only).
5. **Target lag for Supernova DT?** Default `'1 minute'`. Versions DT is always `DOWNSTREAM`.
6. **Computed attributes for Layer 4?** Common examples: balance tiers, age buckets, currency conversion, standardised column names, filtered flags.
7. **Layer 5 delivery shapes needed?** Filtered view (multi-tenancy), aggregate view, OBT (one big table), or star schema.

---

## Naming rules

All Supernova objects live in the `supernova` schema. Names are lowercase.

| Object | Pattern | Example |
|---|---|---|
| Hub versions DT | `dt_{hub}_versions` | `dt_hub_account_versions` |
| Link versions DT | `dt_{link}_versions` | `dt_lnk_account_customer_versions` |
| Supernova hub DT | `dt_supernova_{hub}` | `dt_supernova_hub_account` |
| Supernova link DT | `dt_supernova_{link}` | `dt_supernova_lnk_account_customer` |
| Extended Supernova DT | `dt_xsn_supernova_{hub_or_link}` | `dt_xsn_supernova_hub_account` |

Layer 5 delivery objects live in `information_marts` or a dedicated delivery schema.

---

## Layer 3 — Versions DT (hub with satellites)

The versions DT is the **time spine** — a deduplicated UNION ALL of every satellite's `dv_applieddate` for each entity, with `enddate` computed as `LEAD(startdate) - 1 second`.

```sql
-- Step 3a: time spine — one SELECT per adjacent satellite
CREATE OR REPLACE DYNAMIC TABLE supernova.dt_<hub>_versions
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = <wh>
AS
WITH twine AS (
    SELECT dv_tenantid, dv_hashkey_<hub>, dv_applieddate AS startdate
    FROM <vault_schema>.<sat_1>
    WHERE dv_recsource <> 'GHOST'

    UNION ALL

    SELECT dv_tenantid, dv_hashkey_<hub>, dv_applieddate AS startdate
    FROM <vault_schema>.<sat_2>
    WHERE dv_recsource <> 'GHOST'

    -- add one UNION ALL block per additional adjacent satellite
),
group_by AS (
    SELECT dv_tenantid, dv_hashkey_<hub>, startdate
    FROM twine
    GROUP BY 1, 2, 3
)
SELECT
    hub.<bk_column>,
    grp.dv_tenantid,
    grp.dv_hashkey_<hub>,
    grp.startdate,
    COALESCE(
        DATEADD(seconds, -1,
            LEAD(grp.startdate) OVER (PARTITION BY grp.dv_hashkey_<hub> ORDER BY grp.startdate)
        ),
        TO_TIMESTAMP('9999-12-31 23:59:59')
    ) AS enddate
FROM group_by grp
INNER JOIN <vault_schema>.<hub> hub
    ON grp.dv_hashkey_<hub> = hub.dv_hashkey_<hub>;
```

---

## Layer 3 — Versions DT (hub with no satellites)

When a hub has no adjacent satellites yet, use the hub's own `dv_applieddate` as the time spine. `enddate` is always `9999-12-31` (no version changes until satellites are added).

```sql
CREATE OR REPLACE DYNAMIC TABLE supernova.dt_<hub>_versions
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = <wh>
AS
SELECT
    <bk_column>,
    dv_tenantid,
    dv_hashkey_<hub>,
    dv_applieddate                          AS startdate,
    TO_TIMESTAMP('9999-12-31 23:59:59')     AS enddate
FROM <vault_schema>.<hub>;
```

---

## Layer 3 — Supernova hub DT

Joins the versions DT to each satellite using an **equi-join** on `sat.dv_applieddate = versions.startdate`. Each satellite gets a leaf CTE that adds `dv_applieddate_end` via `LEAD`.

> **Critical rule — equi-join required for INCREMENTAL refresh**: Snowflake's incremental DT refresh mode requires equi-joins between the upstream DTs. A range join (`BETWEEN startdate AND enddate`) forces a full rebuild on every refresh. The versions DT time spine converts temporal alignment into a set of equi-joins, making incremental refresh possible.

```sql
-- Step 3b: wide pre-join — one leaf CTE per satellite
CREATE OR REPLACE DYNAMIC TABLE supernova.dt_supernova_<hub>
    TARGET_LAG = '<lag>'     -- default '1 minute'
    WAREHOUSE  = <wh>
AS
WITH leaf_<sat_1> AS (
    SELECT
        s.*,
        COALESCE(
            LEAD(s.dv_applieddate) OVER (PARTITION BY s.dv_hashkey_<hub>
                                         ORDER BY s.dv_applieddate),
            TO_TIMESTAMP('9999-12-31 23:59:59')
        ) AS dv_applieddate_end
    FROM <vault_schema>.<sat_1> s
),
leaf_<sat_2> AS (
    SELECT
        s.*,
        COALESCE(
            LEAD(s.dv_applieddate) OVER (PARTITION BY s.dv_hashkey_<hub>
                                         ORDER BY s.dv_applieddate),
            TO_TIMESTAMP('9999-12-31 23:59:59')
        ) AS dv_applieddate_end
    FROM <vault_schema>.<sat_2> s
)
-- add one leaf CTE per additional satellite

SELECT
    hub.dv_tenantid,
    hub.dv_hashkey_<hub>,
    hub.startdate,
    hub.enddate,
    hub.<bk_column>,
    -- satellite 1 columns
    s1.<col_a>, s1.<col_b>,
    -- satellite 2 columns
    s2.<col_c>, s2.<col_d>
    -- add columns per satellite
FROM supernova.dt_<hub>_versions hub
LEFT JOIN leaf_<sat_1> s1
    ON  hub.dv_hashkey_<hub>  = s1.dv_hashkey_<hub>
    AND s1.dv_applieddate     = hub.startdate       -- equi-join: no range join
LEFT JOIN leaf_<sat_2> s2
    ON  hub.dv_hashkey_<hub>  = s2.dv_hashkey_<hub>
    AND s2.dv_applieddate     = hub.startdate;
```

---

## Layer 3 — Supernova link DT

Links with no adjacent satellites (just recording the relationship):

```sql
CREATE OR REPLACE DYNAMIC TABLE supernova.dt_<link>_versions
    TARGET_LAG = DOWNSTREAM
    WAREHOUSE  = <wh>
AS
SELECT
    dv_tenantid,
    dv_hashkey_<link>,
    dv_hashkey_<hub_a>,
    dv_hashkey_<hub_b>,
    dv_applieddate   AS startdate,
    TO_TIMESTAMP('9999-12-31 23:59:59') AS enddate
FROM <vault_schema>.<link>;

CREATE OR REPLACE DYNAMIC TABLE supernova.dt_supernova_<link>
    TARGET_LAG = '<lag>'
    WAREHOUSE  = <wh>
AS
SELECT
    lnk.dv_tenantid,
    lnk.dv_hashkey_<link>,
    lnk.dv_hashkey_<hub_a>,
    lnk.dv_hashkey_<hub_b>,
    lnk.startdate
FROM supernova.dt_<link>_versions lnk;
```

For links with adjacent satellites, apply the same leaf CTE + equi-join pattern as the hub Supernova DT, partitioned by `dv_hashkey_<link>` instead.

---

## Layer 4 — Extended Supernova DT

Adds computed attributes, column renames, and lightweight business rules. Always built on top of the Supernova DT — never directly on vault tables.

```sql
-- Step 4: computed attributes on top of the Supernova DT
CREATE OR REPLACE DYNAMIC TABLE supernova.dt_xsn_supernova_<hub>
    TARGET_LAG = '<lag>'
    WAREHOUSE  = <wh>
AS
SELECT
    *,
    -- examples of computed attributes:
    CASE
        WHEN <metric_col> > 50000 THEN 'high'
        WHEN <metric_col> > 10000 THEN 'medium'
        ELSE 'low'
    END                                     AS <tier_column>,
    DATEDIFF(year, <date_col>, CURRENT_DATE) AS <age_column>
FROM supernova.dt_supernova_<hub>;
```

Common Layer 4 patterns:
- Tier/band classification (balance tiers, risk bands, age groups)
- Standardised column names (rename source-specific columns to canonical business terms)
- Currency conversion columns
- `dv_tenantid`-based flags for multi-tenancy filtering in Layer 5

---

## Layer 5 — Data Delivery

Layer 5 shapes the Extended Supernova DT for specific BI tools, consumers, or use cases. `dv_tenantid` is available throughout for row access policy filtering.

### Filtered view (multi-tenancy)

```sql
-- Row-access-policy-friendly: dv_tenantid carried through from Layer 2
CREATE OR REPLACE VIEW information_marts.v_filtered_<hub> AS
SELECT * FROM supernova.dt_xsn_supernova_<hub>
WHERE dv_tenantid = '<tenant>';  -- or replace with row access policy attachment
```

### Aggregate view

```sql
CREATE OR REPLACE VIEW information_marts.v_agg_<hub>_<period> AS
SELECT
    DATE_TRUNC('month', startdate) AS period,
    <bk_column>,
    SUM(<metric_col>)              AS total_<metric>,
    COUNT(*)                       AS event_count
FROM supernova.dt_xsn_supernova_<hub>
GROUP BY 1, 2;
```

### One Big Table (OBT)

```sql
-- Flat wide join of hub supernova + linked entity supernova
CREATE OR REPLACE VIEW information_marts.v_obt_<subject> AS
SELECT
    sn_hub.<bk_column>,
    sn_hub.startdate,
    sn_hub.enddate,
    sn_hub.<cols_hub>,
    sn_lnk.<cols_link>,
    sn_related_hub.<cols_related_hub>
FROM supernova.dt_xsn_supernova_<hub>          sn_hub
LEFT JOIN supernova.dt_supernova_<link>         sn_lnk
    ON sn_hub.dv_hashkey_<hub> = sn_lnk.dv_hashkey_<hub>
LEFT JOIN supernova.dt_xsn_supernova_<hub_b>    sn_related_hub
    ON sn_lnk.dv_hashkey_<hub_b> = sn_related_hub.dv_hashkey_<hub_b>
    AND sn_lnk.startdate         = sn_related_hub.startdate;
```

### Star schema view

```sql
-- Dimension: hub supernova current state
CREATE OR REPLACE VIEW information_marts.dim_<entity> AS
SELECT <bk_column>, <descriptive_cols>
FROM supernova.dt_xsn_supernova_<hub>
WHERE enddate = TO_TIMESTAMP('9999-12-31 23:59:59');  -- current rows only

-- Fact: link supernova + measures
CREATE OR REPLACE VIEW information_marts.fact_<subject> AS
SELECT
    sn_lnk.startdate,
    hub_a.<bk_column_a>,
    hub_b.<bk_column_b>,
    sn_lnk.<measure_cols>
FROM supernova.dt_supernova_<link>       sn_lnk
JOIN supernova.dt_supernova_<hub_a>      hub_a
    ON sn_lnk.dv_hashkey_<hub_a> = hub_a.dv_hashkey_<hub_a>
JOIN supernova.dt_supernova_<hub_b>      hub_b
    ON sn_lnk.dv_hashkey_<hub_b> = hub_b.dv_hashkey_<hub_b>;
```

---

## Dynamic Table pipeline management

```sql
-- Check all Supernova DTs and their refresh status
SHOW DYNAMIC TABLES IN SCHEMA supernova;

-- Retrieve via result_scan for programmatic access
SET supernova_dts = (SELECT LAST_QUERY_ID());
SELECT "name", "target_lag", "refresh_mode", "scheduling_state"
FROM TABLE(RESULT_SCAN($supernova_dts));

-- Suspend all Supernova DTs (e.g. for maintenance)
ALTER DYNAMIC TABLE supernova.dt_<hub>_versions SUSPEND;
ALTER DYNAMIC TABLE supernova.dt_supernova_<hub> SUSPEND;
ALTER DYNAMIC TABLE supernova.dt_xsn_supernova_<hub> SUSPEND;

-- Resume
ALTER DYNAMIC TABLE supernova.dt_xsn_supernova_<hub> RESUME;
ALTER DYNAMIC TABLE supernova.dt_supernova_<hub> RESUME;
ALTER DYNAMIC TABLE supernova.dt_<hub>_versions RESUME;
```

> **Resume order**: versions DT first, then supernova DT, then XSN DT. Snowflake respects dependency ordering but resuming from the leaf upward avoids transient errors.

---

## Subagent files

- SQL Generator: `agents/sql-generator.md` — Supernova DT templates
- Doctrine Enforcer: `agents/doctrine-enforcer.md` — validate no hash keys in Layer 5
