---
name: dv-pit-bridge
description: When and how to build PIT tables and Bridge tables in DVOS — manifest config, materialisation options, bridge types, naming rules.
enabled: true
---

# /dv-pit-bridge — PIT Tables and Bridge Tables

Query-assist structures built on top of the Raw Vault. They never store raw source data — they pre-compute joins and temporal alignments for IM query performance.

**PIT and Bridge tables are Information Mart query assistance structures — they are NOT Business Vault artefacts.**

This is a critical doctrine point:
- PIT and Bridge tables do **not** carry the auditability, agility, and automation guarantees of hub, link, and satellite tables
- They are **derived and disposable** — rebuilt from vault data at any time with no data loss
- They must never be treated as permanent vault artefacts or included in the corporate audit trail
- When a vault correction is applied (e.g. XTS corrects an out-of-sequence satellite timeline), PIT/Bridge tables must be **rebuilt** to reflect the corrected vault state — they do not self-correct like IM views do

If a team is storing PIT or Bridge output as if it were a permanent record of the vault's history, that is an anti-pattern. The vault's history is in the hub, link, and satellite tables. PIT/Bridge are performance aids over that history.

**PIT tables are never SQL views.** A PIT must be physicalised (CTAS, Dynamic Table, or multi-table INSERT). An SQL view over the vault does not pre-resolve join keys — it forces the equi-join locator logic to execute on every query, defeating the PIT's entire purpose of amortising that cost once at build time.

---

## Point-in-Time (PIT) Tables

A PIT table serves two explicit purposes:

1. **SQL simplification** — pre-resolves satellite join keys (`hash-key + load_timestamp` or `dv_sid`) for every snapshot date so IM queries don't recompute them on every execution
2. **Star-optimised query** — signals to Snowflake's query optimiser that a right-deep hash join should be used; the PIT acts as the central "fact-like" table and the satellites as "dimension-like" tables

**Date filter — DV vs. Kimball:** in Kimball dimensional modelling the date filter is applied on the date dimension. In a Data Vault, **the date filter is the snapshot date in the PIT table itself** — the PIT is both the join-index and the temporal anchor for the IM query.

### When to build a PIT

Build a PIT when:
- A hub has **3 or more satellites** and the IM needs to join them together
- Queries need **as-of snapshots** across multiple satellites at the same point in time
- Correlated subqueries across satellites are hurting IM query performance

Not needed when:
- Only one satellite per hub (QUALIFY ROW_NUMBER directly in the view is sufficient)
- The IM never needs historical snapshots — current-state only views work fine

**Design principle: short, thin, and filtered to the use case**

PIT and Bridge tables are IM-focused query assistance structures — they should be scoped to the specific IM use case they serve, not built as catch-all structures. Each PIT/Bridge should be:

- **Short** — limit the snapshot date range to what the IM consumers actually query (e.g. rolling 2 years, not all-time). An unbounded PIT table grows indefinitely and becomes expensive to rebuild.
- **Thin** — include only the satellites the IM use case needs. A PIT that includes all satellites for a hub (even those the IM doesn't join) is heavier than necessary.
- **Filtered** — if the IM only serves one business unit or one tenant, apply that filter in the PIT so it doesn't materialise data irrelevant to those consumers.

Build **multiple PIT tables per hub** when different use cases need different satellite subsets, different snapshot frequencies, or different date ranges — rather than one universal PIT that tries to serve all use cases simultaneously.

### Supernova alternative

If query-time join performance is a bottleneck and result caching is insufficient, consider `/dv-supernova` instead of PIT + IM view.

| | PIT + IM view | Supernova |
|---|---|---|
| Satellite columns in materialised object | DV_SID locators only | All columns (wide) |
| Join timing | Query time (via VC_/VH_ views) | DT refresh time (no query joins) |
| Cross-hub traversal | Yes — use Bridge | No — hub-centric |
| Storage cost | Low | Higher |
| Computed attributes | In IM view (live) | In Extended Supernova DT (pre-built) |
| Multi-tenancy filtering | Row access policy on IM view | Carried via `dv_tenantid` through all layers |

Use PIT when storage cost is constrained or cross-hub traversal is needed. Use Supernova when BI query performance is the primary driver and full column pre-materialisation is acceptable.

### Two PIT variants — decision checkpoint

Ask the user these questions before choosing a variant:

> 1. Do you need near-real-time refresh (Dynamic Table)? Both pit_type: legacy and pit_type: snopit support Dynamic Tables.
> 2. Do any satellites on this hub use `ma` (multi-active) or `dp` (dependent-child) type? → affects behaviour
> 3. Are you comfortable adding a `dv_sid` IDENTITY column to every satellite in this PIT?
> 4. Do you want integer joins in the IM (faster) or timestamp+hashkey joins (more flexible)?

**Decision logic:**

```
Need Dynamic Table refresh?
├── YES → pit_type: legacy or snopit (both support Dynamic Tables)
└── NO (batch / CTAS)
    ├── Want compact projection + integer joins?
    │   ├── YES, and can add dv_sid to satellite DDL → pit_type: snopit
    │   └── NO → pit_type: legacy
    └── Have multi-active satellites and want ALL active rows per snapshot?
        └── YES → pit_type: legacy (SNOPIT collapses multi-active to single row)
```

**You can have both on the same hub** — e.g. a static SNOPIT for the information mart and a dynamic legacy PIT for an operational dashboard.

---

### PIT variant comparison

| Aspect | Legacy PIT | SNOPIT |
|---|---|---|
| Columns per satellite | **2** (hashkey + applied_ts) | **1** (dv_sid integer) |
| Materialization | `dynamic_table` or `ctas` | `dynamic_table` or `ctas` |
| Satellite DDL change | None | Must add `dv_sid IDENTITY` column (`enable_dv_sid: true`) |
| Ghost sentinel | All-zeros binary + 1900 timestamp | `0` (integer — autoincrement START 0) |
| Multi-active output | Fan-out preserved (all active rows visible) | Collapsed to `MIN(dv_sid)` — single row per snapshot |
| IM join pattern | `JOIN ON hashkey + applied_ts` | `JOIN ON dv_sid` (integer equality — faster) |
| Forward-fill | `LAG() IGNORE NULLS` on timestamp | Not needed — `COALESCE(dv_sid, 0)` in population query handles missing rows |
| Best for | Real-time dashboards, existing satellites, when fan-out matters | Batch IM, wide vaults (many satellites), performance-critical marts |

**Why SNOPIT is faster in the IM:** Integer equality joins (`dv_sid = pit.sat_x_dv_sid`) are measurably faster than composite joins on `(BINARY(20) + TIMESTAMP_NTZ)`. For vaults with 10+ satellites per hub, this matters.

**The physical mechanism \u2014 Right Deep Join Tree**

The performance difference is not just "smaller data types." It comes from a fundamentally different physical join plan:

| Join type | Condition | Physical join | Join tree shape |
|---|---|---|---|
| Complex / range | `sat.applied_ts BETWEEN ...` | Sort-Merge Join | Left Deep — sequential, slow |
| Equijoin (hash) | `sat.dv_sid = pit.dv_sid` | Hash Join | Right Deep \u2014 parallel, fast |

In a **Right Deep Join Tree**, the SNOPIT table anchors at the deep right of the plan. The query engine loads the SNOPIT into memory as a hash table, then probes each satellite in parallel. The query plan resembles a **star schema** \u2014 SNOPIT acts as the fact table, satellites as dimensions \u2014 even though the underlying data model is not a star schema.

Without a PIT table at all: the query devolves into a Left Deep Join Tree. Benchmark result: without PIT = 8 minutes 20 seconds; with SNOPIT daily PIT = seconds. Both return identical row counts.

**Snowflake zone map advantage \u2014 why DV_SID outperforms hash keys**

Snowflake uses zone maps (min/max per column per micro-partition) for partition pruning. DV_SID and binary hash keys behave very differently:

| Column | Value pattern | Zone map effectiveness | Partition pruning |
|---|---|---|---|
| `dv_hashkey` (BINARY) | Pseudo-random \u2014 scattered across all partitions | Near-zero: min/max range covers full key space | Almost none |
| `dv_sid` (INTEGER) | Linear (autoincrement in load order) | Excellent: tight min/max ranges per partition | Highly effective |

Because DV_SID increments sequentially with each load, Snowflake knows precisely which micro-partitions contain any given integer range. Hash keys, by design, distribute randomly across all partitions \u2014 the zone map can never exclude a partition for a hash key lookup.

Evidence: `snopit_cardaccount_daily` = 406 partitions vs. `pit_cardaccount_daily` = 1446 partitions for identical data.

**No clustering keys on satellite tables**

> Adding an explicit `CLUSTER BY` clause to a satellite table breaks the natural load-order clustering and destroys the DV_SID zone map advantage.

The natural load order \u2014 records appended in time order \u2014 IS the clustering. DV_SID's linear values are linear *because* data was appended in load order. Explicit clustering reorganises the physical data away from load order, causing DV_SID values to scatter across micro-partitions, negating the zone map benefit.

**Rule:** Do not add `CLUSTER BY` to any satellite table in a SNOPIT-enabled vault. Let Snowflake's natural micro-partition structure do the work.

**Static vs. dynamic pruning**

Snowflake achieves partition elimination through two mechanisms that work together in SNOPIT queries:

- **Static pruning** \u2014 the Snowflake optimizer evaluates zone maps (min/max per column per micro-partition) at query-parse time, using literal values or bound parameters from a WHERE clause. Micro-partitions outside the range are excluded before execution begins.

- **Dynamic pruning (JoinFilter)** \u2014 at runtime, Snowflake generates a filter based on the build side of a hash-join, then applies it to prune the probe side before the probe scan begins. This is how SNOPIT achieves effective partition pruning even when the integer range is not known at parse time \u2014 the PIT/SNOPIT table (build side) produces the range of `dv_sid` values, and Snowflake uses that range to prune the satellite's micro-partitions (probe side) before scanning.

The combination of DV_SID's linear ordering (enabling tight zone maps) and dynamic JoinFilter pruning is why SNOPIT queries dramatically outperform traditional range-join PIT queries on Snowflake. Traditional PIT uses a range condition (`applied_ts BETWEEN ...`), which requires a sort-merge join and cannot benefit from JoinFilter pruning.

**Why Legacy PIT is safer as default:** No satellite DDL change required. Works with Dynamic Tables. Preserves multi-active fan-out if you need it.

---

| Variant | Use when |
|---|---|
| `pit_type: legacy` | Dynamic Table refresh needed, or satellites already exist without `dv_sid`, or you need fan-out from multi-active satellites |
| `pit_type: snopit` | Compact projection, faster integer IM joins, happy to add `dv_sid` to satellite DDL — Dynamic Tables supported |

### Manifest declaration

```yaml
pits:
  - name: <pit_name>                     # lowercase, e.g. customer
    base_artefact: hub_<name>            # or lnk_<name>
    output_table: pit_<name>             # e.g. pit_customer
    cadence_flags:                       # which AS_OF dates drive this PIT
      - daily                            # default
      # - monthly, weekly, hourly, etc.
    pit_type: legacy                     # legacy (default) or snopit
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

**SNOPIT requires `enable_dv_sid: true` on every satellite in the PIT definition:**

```yaml
raw_satellites:
  - parent: <hub_name>
    parent_type: hub
    source_badge: <badge>
    source_file: <file>
    enable_dv_sid: true    # adds dv_sid NUMBER(38,0) IDENTITY to satellite DDL
    type: standard
```

Ghost record for SNOPIT-enabled satellites: `dv_sid` is NOT specified in the INSERT — `IDENTITY START 0` auto-assigns `dv_sid = 0` to the ghost row. The SNOPIT population query uses `COALESCE(s.dv_sid, 0)` so a null satellite match stores `0`, which is the ghost row's `dv_sid`. The IM view uses a plain `INNER JOIN ON dv_sid` — always finds a row (`0` = ghost, `1+` = real record). No `-1` sentinel exists.

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

**Multi-table INSERT for PIT population**

When manually loading multiple PIT snapshot windows (daily, weekly, monthly) in a single run, multi-table INSERT is a valid and efficient pattern for PIT/Bridge tables. This is the one legitimate use case for multi-table INSERT in a data vault context — it must not be used for hub, link, or satellite loaders (see `/dv-load` for the prohibition and race-condition rationale). PIT rows are safe for multi-table INSERT because each `(entity_hash, snapshot_date)` combination is unique by construction; there is no race condition or duplicate key risk.

### PIT columns produced

**Legacy PIT** — 2 columns per satellite:
- `{sat_alias}_dv_applied_timestamp` — temporal alignment column; `'1900-01-01'` if no record at snapshot
- `{sat_alias}_{hashkey_col}` — forward-filled via `LAG() IGNORE NULLS`; ghost key (`TO_BINARY(REPEAT(0,20))`) if no record

**SNOPIT** — 1 column per satellite:
- `{sat_alias}_dv_sid` — integer row locator; `0` if no record at snapshot (points to ghost row via `COALESCE(s.dv_sid, 0)` in population query)

Both variants include: `dv_hashkey_hub_<name>` (or link hashkey) + `SNAPSHOT_DATE`.

### IM join pattern

```sql
-- Legacy PIT: join on hashkey + timestamp
LEFT JOIN SAT_RV_HUB_SAPBW_COMM_CUSTOMER s
    ON  s.dv_hashkey_hub_party = pit.dv_hashkey_hub_party
    AND s.dv_applied_timestamp = pit.sat_rv_hub_sapbw_comm_customer_dv_applied_timestamp

-- SNOPIT: join on integer dv_sid (faster)
-- No != 0 filter needed — dv_sid = 0 finds the ghost row, which returns NULL attributes
LEFT JOIN SAT_RV_HUB_SAPBW_COMM_CUSTOMER s
    ON s.dv_sid = pit.sat_rv_hub_sapbw_comm_customer_dv_sid
```

### Ghost records

Every satellite included in a PIT must have a ghost record (all-zero hash key row). DVOS inserts these automatically. Without them, PIT null-joins fail.

**Ghost skew — no longer needs to be isolated (Snowflake October 2024)**

Ghost records in a PIT or SNOPIT table create a data skew pattern: all entities that have no satellite record for a snapshot period point to the same ghost row. This was historically a performance concern requiring IM queries to explicitly filter out ghost records to avoid probe-side skew degrading hash-join performance.

**Snowflake introduced native probe-side skew optimisations in October 2024.** These handle ghost record skew automatically. There is no longer any performance benefit in attempting to isolate ghost records in IM queries. Remove any `WHERE ... != ghost_hash` or `WHERE dv_recordsource != 'GHOST'` predicates added for skew reasons — they add query complexity without performance gain.

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
| `fact` | Pre-computes satellite joins at the grain of a **link satellite (measures)** — a link satellite carrying transaction amounts, quantities, or counts. Stores `DV_SID` locators + persisted metrics. IM view joins satellites via single-row equi-join on `DV_SID` — no temporal logic at query time. |

### Naming rules (BDG-NAME-001)

- Name: `brdg_{business_concept}_{snapshot_or_period}` — lowercase, e.g. `brdg_partyaccount_daily`
- Output table: uppercase mirror — `BRDG_PARTYACCOUNT_DAILY`
- **`bdg_` and `bridge_` prefixes are not permitted**

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
  - name: brdg_partyaccount_daily
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
  - name: brdg_partyaccount_current
    bridge_type: current_relationship
    path:
      - hub_party
      - lnk_rv_customer_account
      - hub_account
    effectivity_satellite: sat_lnk_rv_customer_account_eff   # required — must be type: ef
    output_table: BDG_PARTYACCOUNT_CURRENT
```

### Fact bridge — building with a link satellite (measures)

A **fact bridge** is driven by a **link satellite carrying measures** — amounts, quantities, counts relating to the transaction between two or more business objects. This is not a separate satellite type — it is a standard or dep-child link satellite that happens to contain additive/semi-additive metrics. It is **not** driven by a hub satellite.

Decision questions to ask before building a fact bridge:
1. Which link satellite carries the core metrics? (This is the grain-driving link satellite.)
2. Which hub satellites provide dimensional context for each hub end of the link?
3. What metrics should be persisted into the bridge? (amounts, running sums, counts)
4. Are any running/cumulative metrics needed? (window functions at load time, not query time)
5. Is a `date_sid` column needed for joining to a date dimension?
6. Is refresh stream-driven (Kappa Vault, incremental) or batch INSERT (full pass each run)?

#### Temporal alignment pattern

When joining hub satellites to align with the link satellite's effective period, use:
```sql
sat_hub.dv_applieddate     <= sat_link.dv_applieddate
AND sat_hub.dv_applieddate_end >= sat_link.dv_applieddate_end
```
`dv_applieddate_end` is computed per satellite using a `LEAD` window function:
```sql
WITH sat_link_versioned AS (
    SELECT *,
        LEAD(dv_applieddate, 1, '9999-12-31'::DATE)
            OVER (PARTITION BY dv_hashkey_lnk_<link_name> ORDER BY dv_applieddate) AS dv_applieddate_end
    FROM SAT_NH_RV_LNK_<LINK_NAME>_<BADGE>
),
sat_hub_versioned AS (
    SELECT *,
        LEAD(dv_applieddate, 1, '9999-12-31'::DATE)
            OVER (PARTITION BY dv_hashkey_hub_<hub_name> ORDER BY dv_applieddate) AS dv_applieddate_end
    FROM SAT_RV_HUB_<HUB_NAME>_<BADGE>
)
```

#### Persisted metrics — including running sums

Metrics are computed at load time and stored in the bridge. Running sums use window functions over load date:
```sql
SUM(s.transaction_amount)
    OVER (PARTITION BY s.dv_hashkey_hub_<hub_name> ORDER BY s.dv_loaddate
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_total_amount
```

#### `date_sid` column

Add a `date_sid` integer column to allow joining the bridge to a date dimension:
```sql
YEAR(s.dv_loaddate) * 10000
+ MONTH(s.dv_loaddate) * 100
+ DAY(s.dv_loaddate)                AS date_sid
```

#### AS_OF calendar table

The `as_of` sequence in the manifest controls both:
- The PIT manifold snapshot cadence (daily, hourly, etc.)
- The queryable date spine — the `AS_OF` calendar table (`CAL_ASOF`) can be joined directly in IM views for period filtering

#### IM view equi-join pattern (no temporal logic at query time)

```sql
SELECT
    brdg.date_sid,
    sat_link.transaction_amount,
    brdg.running_total_amount,
    sat_hub1.customer_name,
    sat_hub2.account_type
FROM BDG_<NAME> brdg
LEFT JOIN SAT_NH_RV_LNK_<LINK_NAME>_<BADGE>    sat_link
    ON brdg.sat_lnk_<link_name>_dv_sid    = sat_link.dv_sid
LEFT JOIN SAT_RV_HUB_<HUB1_NAME>_<BADGE>       sat_hub1
    ON brdg.sat_hub_<hub1_name>_dv_sid    = sat_hub1.dv_sid
LEFT JOIN SAT_RV_HUB_<HUB2_NAME>_<BADGE>       sat_hub2
    ON brdg.sat_hub_<hub2_name>_dv_sid    = sat_hub2.dv_sid
-- No temporal logic here — all resolved at bridge load time
```
`COALESCE(sat_hub.dv_sid, 0)` stores 0 in the bridge when no matching satellite record exists at that point in time.

### Manifest declaration — fact bridge

```yaml
bridges:
  - name: brdg_transactions_daily
    bridge_type: fact
    path:
      - hub_account
      - lnk_rv_account_transaction
      - hub_transaction
    fact_satellite: sat_nh_rv_lnk_rv_account_transaction_<badge>  # link satellite (measures) — drives the fact grain
    hub_satellites:                                                # hub satellites for dimensional context
      hub_account:      sat_rv_hub_account_<badge>
      hub_transaction:  sat_rv_hub_transaction_<badge>
    metrics:                                                       # columns from link satellite to persist
      - transaction_amount
      - currency_code
    running_metrics:                                               # window-aggregated metrics to persist
      - running_total_amount: SUM(transaction_amount)
    date_sid: true                                                 # add date_sid column from dv_loaddate
    output_table: BDG_TRANSACTIONS_DAILY
```

### Using a Bridge in the IM

See `/dv-mart` for the bridge-based view pattern. Bridge-based IM views join the pre-built `BDG_*` table instead of traversing links at query time — one join replaces N.

**Bridge build tip: include all dimensional columns at build time**

The bridge build step uses a right-deep hash join (efficient to build). If you build a thin bridge (only `dv_sid` locators + metrics) and re-join the dimension attribute columns at every IM query, you pay the dimensional join cost on every query. The lesson: **include all dimensional attribute columns the IM will need in the bridge build step** — not just the locators. This amortises the join cost once at build time rather than paying it on every query execution.

---

## Snowflake performance rules for PIT and Bridge (mid-2025)

Performance guidance specific to Snowflake + Data Vault, validated by benchmarking against realistic DV workloads (satellite tables in the hundreds of millions to billions of rows).

### Rule 1 — Never cluster PIT or Bridge tables

**⚠️ Do NOT apply explicit `CLUSTER BY` to PIT or Bridge tables.**

Clustering PIT/Bridge tables causes severe performance degradation. PIT and Bridge tables are pre-computed join locators — their value comes from Snowflake's hash-join algorithm operating on compact integer (`dv_sid`) or binary (`dv_hashkey`) columns. Explicit clustering forces a complete rewrite of all micro-partitions and disrupts the query optimiser's ability to use the compact key columns efficiently.

Clustering satellites is also generally not beneficial for most DV query patterns (see `/dv-model` — no `CLUSTER BY` on satellites in SNOPIT-enabled vaults). Testing shows clustering by applied date makes single-satellite querying performance worse overall, not better. The natural insertion order (insert-only, chronological) already provides adequate zone-map pruning for range queries.

### Rule 2 — SNOPIT + Gen-2 is the recommended configuration

SNOPIT (integer `dv_sid` join keys) consistently outperforms binary-hashkey PIT and natural-key PIT across all warehouse sizes and configurations. At medium warehouse sizes, the performance difference is conclusive.

**Recommended configuration for high-concurrency DV IM workloads:**

| Component | Recommendation | Rationale |
|---|---|---|
| PIT type | **SNOPIT** | Integer equi-join is the fastest join algorithm; half the column count of binary PIT |
| Warehouse generation | **Gen-2** | Higher per-credit cost but lower total credit consumption; faster execution means fewer credits spent |
| Warehouse size | **Medium** for multi-satellite joins | Medium Gen-2 provides the memory bandwidth needed for large PIT equi-joins without spillage |

Gen-2 X-Small and Gen-2 Medium both outperform their Gen-1 equivalents for DV query patterns. Despite the premium credit rate, the faster execution results in lower total cost.

### Rule 3 — ASOF join is NOT for multi-satellite PIT joins

**⚠️ Do NOT use `ASOF JOIN` to join multiple satellite tables via a PIT.**

`ASOF JOIN` is designed for time-series point lookups (e.g. Activity Schema enrichment — see `/dv-mart Activity Schema`). When applied to PIT-based multi-satellite IM queries, Snowflake executes a **nested loop algorithm** rather than a hash join. This causes:
- Excessive local disk spillage
- Query times of hours vs. seconds for equivalent PIT+SNOPIT queries
- Rapidly escalating cost at scale

**When to use ASOF:** joining a slowly-changing dimension to a time-series event table at the correct point in time (one lookup per event row). This is the pattern documented in `/dv-mart Activity Schema Step 3`.

**When NOT to use ASOF:** joining multiple satellite tables together via a PIT or SNOPIT to produce an IM view. Use hash-join equi-joins via PIT/SNOPIT instead.

### Rule 4 — SOS provides no benefit for DV hash-key joins

Snowflake's **Search Optimization Service (SOS)** creates a secondary index for high-cardinality equality point-lookups. Despite documentation suggesting equality searches can optimise SQL joins, benchmarking shows **no observed performance benefit for DV join patterns** involving hash keys. In some cases, performance is worse.

**When SOS is appropriate in a DV context:**
- Point-lookups on **non-key columns** with high cardinality (e.g. searching by a customer email, account number, or reference code in a satellite)
- NOT on `dv_hashkey_*` or `dv_sid` columns — these are always join columns, not point-lookup columns

**Caution:** SOS incurs a serverless compute charge when initialised and maintained, plus a small storage overhead. Do not enable it unless you have confirmed point-lookup performance problems on specific non-key columns.

### Rule 5 — CPIT (Current PIT) as a Dynamic Table for current-state queries

The most common DV IM query pattern is: *"give me the current state of each entity."* Two approaches handle this:

| Approach | Mechanism | Cost trade-off |
|---|---|---|
| **Standard QUALIFY ROW_NUMBER()** | Query-time calculation per consumer | Free at rest; CPU consumed per query, repeated for every user |
| **CPIT — Current PIT Dynamic Table** | Pre-materialised current-state locators per satellite | Serverless compute to maintain; near-zero query-time cost |
| **Cluster by materialised end-date flag** | Explicit clustering of satellite | Serverless clustering charge + ongoing recluster cost |

**Recommended approach: CPIT as a Dynamic Table.** Create a Dynamic Table per satellite (or per hub's set of satellites) that materialises only the current `dv_sid` or `(hashkey, load_timestamp)` per entity. IM views join via the CPIT rather than recalculating `QUALIFY ROW_NUMBER()` on every query.

```sql
-- CPIT Dynamic Table: current row per entity per satellite
CREATE OR REPLACE DYNAMIC TABLE <schema>.CPIT_SAT_<ENTITY>_<CONTEXT>
    TARGET_LAG = '1 minute'
    WAREHOUSE = <wh>
AS
SELECT
    dv_hashkey_hub_<entity>,
    dv_sid,
    dv_load_timestamp
FROM <vault_schema>.SAT_<ENTITY>_<CONTEXT>
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dv_hashkey_hub_<entity>
    ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
) = 1;
```

This is preferable to clustering the satellite (which incurs ongoing serverless recluster cost and has marginal or negative query benefit).

**C-PIT table design (TRANSIENT, thin schema)**

Whether implemented as a Dynamic Table (recommended) or as a static TRANSIENT table refreshed with MERGE, the C-PIT schema is intentionally thin:

| Column | Purpose |
|---|---|
| Business key | Hub-satellite: the hub BK. Link-satellite: the relationship keys |
| `dv_hashkey` | The satellite's parent hash key (for equi-join) |
| `dv_load_timestamp` | The load timestamp of the current record |
| `dv_hashdiff` | The record hash — enables idempotent parallel load from the same staged source |

**Nothing else.** C-PIT contains no business attributes — those remain in the satellite.

C-PIT is **disposable** and must be defined as a **TRANSIENT table** (no Fail-Safe, max 1-day Time-Travel). The satellite itself remains INSERT-ONLY and PERMANENT, retaining its full Time-Travel and Fail-Safe protection. High churn on C-PIT is not a concern because there is no clustering to maintain and the satellite is not touched.

**Larger warehouse is SLOWER with C-PIT + JoinFilter**

Counter-intuitively, upsizing the virtual warehouse when using the C-PIT + JoinFilter pattern produces *slower* results than an X-SMALL. Snowflake must orchestrate and schedule work across every node in the cluster — the more nodes, the more scheduling latency. The JoinFilter + dynamic pruning pattern is already so efficient on an X-SMALL that adding nodes adds overhead, not throughput. For C-PIT-backed IM queries, resist the instinct to upsize.

**Materialised views are not viable for DV current-record queries**

Two hard limitations prevent materialised views from serving as a C-PIT equivalent:
1. Snowflake materialised views do not support window functions (`LEAD()`, `QUALIFY`) — inferring the current record per entity requires a window function
2. Snowflake materialised views do not support join conditions — C-PIT must be joined to its satellite in the IM view

Use CPIT as a Dynamic Table (preferred) or as a TRANSIENT MERGE-maintained table instead.

When designing CPIT and PIT/Bridge Dynamic Tables, you must design the SQL to enable **incremental refresh** — not full refresh. This is a critical cost and performance rule:

- **Full refresh** — Snowflake recalculates every row in the Dynamic Table on every refresh cycle. For a CPIT over a large satellite (hundreds of millions of rows), this is prohibitively expensive and slow.
- **Incremental refresh** — Snowflake detects only the changed rows using stream semantics under the hood and refreshes only the delta. This is fast and cheap.

**How to enable incremental refresh:** Snowflake determines eligibility automatically based on your SQL. Incremental refresh is supported when:
- The Dynamic Table reads from a single base table (or a simple join between a small number of tables)
- No complex aggregations, lateral joins, or non-deterministic functions block change detection

If Snowflake cannot determine an incremental plan for your SQL, it falls back to full refresh silently. Check your Dynamic Table's refresh mode via `SHOW DYNAMIC TABLES` and look at the `REFRESH_MODE` column — it should show `INCREMENTAL`, not `FULL`. If it shows `FULL`, simplify the SQL or split into multiple Dynamic Tables.

---

## ASOF table — data-driven PIT controller

The ASOF table is the DV equivalent of a time dimension. It stores a date spine with standard calendar attributes plus **boolean flag columns** that act as reporting cadence switches:

```sql
CREATE TABLE queryassistance.as_of_date (
  as_of          DATE    NOT NULL,
  year           SMALLINT NOT NULL,
  month          SMALLINT NOT NULL,
  day_of_month   SMALLINT NOT NULL,
  week_of_year   SMALLINT NOT NULL,
  day_of_year    SMALLINT NOT NULL,
  month_lastday  SMALLINT NOT NULL,  -- 1 on last day of month
  week_lastday   SMALLINT NOT NULL,  -- 1 on last day of week
  week_firstday  SMALLINT NOT NULL   -- 1 on first day of week
  -- add business-specific flags (quarter_end, fiscal_year_end, etc.)
)
```

**Data-driven PIT Dynamic Tables** reference the ASOF table rather than hardcoding date ranges. Each PIT cadence filters on a different flag — no SQL changes needed when the reporting scope changes:

```
as_of_date (control table)
  ├── dt_pit_daily   → SELECT as_of FROM as_of_date                         (all rows)
  ├── dt_pit_weekly  → SELECT as_of FROM as_of_date WHERE week_lastday  = 1
  └── dt_pit_monthly → SELECT as_of FROM as_of_date WHERE month_lastday = 1
```

Operations on the ASOF table automatically propagate to all downstream PIT Dynamic Tables:

| ASOF operation | Effect on downstream PITs |
|---|---|
| `INSERT` new dates | New snapshot rows appear in all PITs at next refresh |
| `UPDATE` flag columns | Only the affected cadence PITs change scope |
| `TRUNCATE` | All downstream PITs empty (useful for test resets) |

This is the **code/data separation principle** applied to PIT design: the Dynamic Table SQL is timeless and never changes; the ASOF table is the point-in-time configuration. Changing reporting scope is a data operation, not a code deployment.

The ASOF table lives in the query assistance schema alongside PIT and bridge tables — not in the vault layer.

**Logarithmic PIT retention pattern**

As a vault ages, the business rarely needs daily granularity for data that is many months or years old. A named strategy for managing long-term PIT table size:

| Time horizon | Snapshot cadence | ASOF filter |
|---|---|---|
| Recent (e.g. rolling 90 days) | Daily | All rows |
| Medium-term (e.g. 90 days – 2 years) | Weekly | `WHERE week_lastday = 1` |
| Long-term (2+ years) | Monthly | `WHERE month_lastday = 1` |

All three PIT tables are populated from a single multi-table INSERT driven by the same ASOF table. The coarser the cadence, the fewer rows in that PIT and the cheaper the IM queries that target historical analysis at lower granularity. The older the data, the less grain you typically need — this is the logarithmic principle applied to snapshot frequency.

**Tumbling PIT windows — cost optimization for logarithmic tiers**

Rather than querying the full vault to populate each tier separately, extract keys and dates from the already-populated higher-tier PIT:

```
Tier 1 (Daily PIT)  ← populated from vault (via ASOF + hub cross-join)
Tier 2 (Weekly PIT) ← extracted FROM Tier 1 WHERE week_lastday = 1
Tier 3 (Monthly PIT) ← extracted FROM Tier 2 WHERE month_lastday = 1
```

The daily PIT already contains all necessary locators for the weekly and monthly snapshots. Extracting from PIT to PIT avoids redundant vault scans for lower tiers and reduces build cost — especially on vaults with very large satellite tables (billions of rows) where the vault-scan cost is significant. Orchestrate the cascade sequentially: Tier 1 completes first, then Tier 2 is extracted, then Tier 3.

---

## PIT / SNOPIT / Bridge scope — multi-record state is not their responsibility

PITs, SNOPITs, and Bridge tables are **join-index structures**. Their sole purpose is to guide the query engine to where data resides in adjacent satellite tables — locating the correct `(hash-key, load_timestamp)` or `dv_sid` for a given snapshot date. They are not designed to resolve multi-record constructs into a join-index.

**Multi-record state satellite tables (MSAT, dep-child, PMAS) should have zero impact on PIT/SNOPIT/Bridge construction.** The PIT stores one locator per satellite per snapshot point; it cannot predict how many active records will be present for a parent entity at any point in time, and it should not try to.

The resolution of multi-record state is the **IM query's responsibility** — performed after the PIT/SNOPIT/Bridge has located the data. The IM view joins via the PIT, then fans out across the active records in the multi-record satellite as required by the business case.

**Known SNOPIT behaviour with MSAT:** SNOPIT collapses multi-active satellite records to a single row per snapshot via `MIN(dv_sid)`. This is a trade-off, not a design goal for multi-record resolution. When multi-active fan-out must be preserved at snapshot level, use Legacy PIT instead.

**Edge case — HasdDiff in PIT for dep-child satellite cross-matching**

When a PIT includes dep-child satellites and the IM needs to match across satellites by sub-category (not just by parent hash key), the standard PIT columns (hash key + applied timestamp) are insufficient. The IM query needs to join each dep-child satellite row to the correct sub-category row in adjacent dep-child satellites for the same parent entity.

In this scenario, add a `dv_hashdiff` column per dep-child satellite to the PIT table. The hashdiff provides the equi-join match point for the sub-category grain across satellites. This is an **exception to the thin-PIT principle** — use only when cross-satellite dep-child matching is genuinely required by the IM use case and no alternative flattening strategy in the IM view is viable.

---

## PIT reconciliation rule — eventual consistency with satellites

A PIT table is derived from its parent satellites. The integrity invariant: **a PIT row must never reference a `(hashkey, load_timestamp)` combination that does not exist in the corresponding satellite.**

If it does:
- The PIT equi-join will resolve to a row in the satellite that doesn't exist → the join silently returns no satellite data for that entity at that snapshot point
- Downstream IM queries produce missing data without any error — a silent correctness problem

**Reconciliation rule:** upon satellite load completion, verify that every `(sat_alias_dv_hashkey, sat_alias_dv_load_timestamp)` pair stored in the PIT exists in the target satellite. Any orphaned PIT row indicates a partial or failed satellite load that must be remediated before the PIT is consumed by IM queries.

**PIT and satellites are eventually consistent** — during a load cycle, the satellite may be updated before the PIT is refreshed. This is expected and normal: the satellite is the authoritative record; the PIT is the derived access structure. The consistency requirement is that both converge before IM queries are served.

---

## Bridge table as fact table in Snowflake Semantic View (Cortex Analyst)

When exposing a Data Vault to **Cortex Analyst** via a Snowflake **Semantic View**, the bridge table is the natural fact table equivalent:

| Semantic View component | DV source |
|---|---|
| Fact table | Bridge table (`BRDG_`) |
| Dimension tables | Satellite tables (`SAT_`) |
| Relationships | Bridge → satellite joins via `dv_sid` |

The Semantic View `relationships` clause references `dv_sid` (INTEGER) as the join key between the bridge and each satellite — matching the SNOPIT integer equality join pattern. This is the preferred join mechanism in the Semantic View DDL:

```sql
relationships (
  SAT_X as BRDG_MY_BRIDGE(SAT_X_DV_SID) references SAT_X(DV_SID),
  DATE_DIM as BRDG_MY_BRIDGE(DATE_SID) references DATE_DIM(DATE_SID)
)
```

**Prerequisites for Semantic View compatibility:**
- Business keys must be present in satellite tables (natural key approach) — enables entity identification in the semantic model
- Every satellite in the bridge must have a `dv_sid IDENTITY` column — without it, the bridge cannot reference an integer row locator, and the `relationships` join fails

Both prerequisites are already required for SNOPIT; if SNOPIT is in use, the vault is Semantic View–ready.

---

## Parallel PIT loading via hashdiff breadcrumbs

Instead of building PIT tables by querying satellite data after the fact (snapshot approach), embed satellite hashdiff values directly into the PIT table and load the PIT **in parallel with satellite loads** from the same staged content.

**Concept:** The same change-detection logic that drives satellite inserts independently drives PIT updates. The hashdiff acts as a "breadcrumb" \u2014 if the staged hashdiff differs from the PIT's recorded hashdiff for that entity, a new PIT snapshot row is needed.

```sql
-- PIT table with hashdiff breadcrumbs
CREATE TABLE PIT_HUB_CUSTOMER (
    dv_hashkey_hub_customer  BINARY(20)    NOT NULL,
    snapshot_date            DATE          NOT NULL,
    sat_demo_dv_appts        TIMESTAMP_NTZ,
    sat_demo_dv_hashdiff     BINARY(20),   -- breadcrumb from SAT_DEMO
    sat_fin_dv_appts         TIMESTAMP_NTZ,
    sat_fin_dv_hashdiff      BINARY(20),   -- breadcrumb from SAT_FIN
    PRIMARY KEY (dv_hashkey_hub_customer, snapshot_date)
);
```

**Loading pattern:** The PIT loader reads from the same staging view as the satellite loader. It compares the staged hashdiff against the PIT's current hashdiff for that entity. If different \u2014 insert a new PIT row with the new hashdiff value.

**Benefits:**
- PIT and satellite load in parallel (no dependency chain)
- PIT is always current with the satellite (no refresh lag)
- Change detection is consistent (same hashdiff comparison in both)

**Caveat:** This is an advanced pattern. The standard approach (PIT refreshed after satellite via Dynamic Table or scheduled task) is simpler and sufficient for most workloads.

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- SQL Generator: `agents/sql-generator.md`
