---
name: dv-mart
description: Build Information Mart views from the Raw Vault. Hash keys are never exposed — BI tools see only business keys and descriptive attributes.
enabled: true
---

# /dv-mart — Information Mart Views

Build query-ready views on top of the Raw Vault for BI tools and analysts. Hash keys are an internal vault implementation detail — they must never appear in Information Mart views.

**Why data modelling discipline in the IM matters: storage is cheap, compute is not**

Columnar storage on cloud object stores is near-free. This has led to the **OBT (One Big Table)** pattern — storing everything in one wide table and resolving complexity at query time. The problem: compute is not cheap.

Without a modelled IM layer, **every query must resolve the same complexity** — current state detection, SCD logic, join conditions — every time any user runs it. Cost scales linearly with concurrency. In a properly modelled IM (or data vault + IM), complexity is **solved once in the model** and all consumers inherit the savings:

- A `VC_SAT_*` current-state view resolves `QUALIFY ROW_NUMBER() = 1` once — thousands of users query it for free
- A CPIT Dynamic Table pre-materialises the current locators — queries are O(1) joins
- A PIT or Bridge table pre-computes multi-satellite join paths — every IM view benefits

The correct principle: **solve complexity once at the model layer; let all consumers inherit the result.** The alternative — OBT or unmodelled SQL views — forces every user to pay the computation cost every time.

## Rule: no hash keys in the IM

Every IM view must:
- Substitute `<NAME>_BK` for `<NAME>_HK` wherever a key is needed
- Expose natural business keys that the business recognises
- Never include `BINARY` columns
- Never expose `dv_hashdiff`, `dv_load_timestamp`, `dv_applied_timestamp`, or `dv_recordsource` unless the view is explicitly an audit view

If validation is needed, the Doctrine Enforcer checks this automatically.

---

## Input

Ask the user:
1. Which hub (or hubs) is the mart anchored on?
2. Which satellites should be included?
3. Do you need current-state only, or point-in-time snapshots?
4. Are there links to traverse (e.g. customer → orders → products)?
5. Does a PIT table exist for the anchor hub? (recommend one if not)

---

## Satellite views (`vc_` and `vh_`)

DVOS generates two views per satellite when `satellite_views` is enabled in the manifest. These encapsulate the temporal logic so that IM views and ad-hoc queries can join directly without repeating window functions.

### `vc_<sat_name>` — Current record view

Returns exactly one row per parent hash key: the latest active record.

```sql
CREATE OR REPLACE VIEW <vault_schema>.VC_<SAT_NAME> AS
SELECT *
FROM <vault_schema>.SAT_<SAT_NAME>
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dv_hashkey_hub_<parent>
    ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
) = 1;
```

### `vh_<sat_name>` — History view

Returns all records with computed `dv_applied_timestamp_end` and `dv_currentflag` columns, enabling range-based "as-of" queries without a PIT table.

```sql
CREATE OR REPLACE VIEW <vault_schema>.VH_<SAT_NAME> AS
SELECT
    *,
    COALESCE(
        LEAD(dv_applied_timestamp) OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp, dv_load_timestamp
        ),
        '9999-12-31 23:59:59'::TIMESTAMP_NTZ
    ) AS dv_applied_timestamp_end,
    CASE
        WHEN ROW_NUMBER() OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
        ) = 1 THEN TRUE
        ELSE FALSE
    END AS dv_currentflag
FROM <vault_schema>.SAT_<SAT_NAME>;
```

### Using satellite views in IM construction

Instead of repeating QUALIFY logic in every IM view, join the `vc_` view directly:

```sql
CREATE OR REPLACE VIEW <im_schema>.DIM_<ENTITY> AS
SELECT
    h.<bk_column>,
    s1.<attr1>,
    s2.<attr2>
FROM <vault_schema>.HUB_<ENTITY> h
LEFT JOIN <vault_schema>.VC_SAT_<ENTITY>_<CONTEXT1> s1
    ON s1.dv_hashkey_hub_<entity> = h.dv_hashkey_hub_<entity>
LEFT JOIN <vault_schema>.VC_SAT_<ENTITY>_<CONTEXT2> s2
    ON s2.dv_hashkey_hub_<entity> = h.dv_hashkey_hub_<entity>;
```

### Naming rules

| View type | Prefix | Example |
|---|---|---|
| Current (latest record) | `VC_` | `VC_SAT_RV_HUB_{badge}_CUSTOMER_DEMOGRAPHICS` |
| History (all records + end-date + flag) | `VH_` | `VH_SAT_CUSTOMER_DEMOGRAPHICS` |

- Prefix is uppercase in DDL, lowercase in manifest references (`vc_sat_customer_demographics`)
- The satellite name follows unchanged after the prefix
- These views live in the **vault schema** (not the IM schema) — they are internal to the vault layer

### When to use which

| Need | Use |
|---|---|
| Latest record per entity | `vc_` view |
| "As-of" query without PIT | `vh_` view with `WHERE dv_applied_timestamp <= @as_of AND dv_applied_timestamp_end > @as_of` |
| Multiple satellites aligned at same point in time | PIT table (satellite views alone can't align across satellites) |
| IM dimension (current state) | Join `vc_` views to hub |
| IM dimension (history) | PIT-based view or join `vh_` views for single-satellite history |

---

## View patterns

### Current-state view (no PIT needed)

**DVOS satellites have no end-date column.** Current row is selected via `QUALIFY ROW_NUMBER()` ordered by `dv_applied_timestamp DESC, dv_load_timestamp DESC`. DVOS also generates `vc_*` (current) and `vh_*` (history) views per satellite when `satellite_views` is enabled in the manifest — prefer joining those if available.

```sql
CREATE OR REPLACE VIEW <schema>.DIM_<ENTITY> AS
SELECT
    h.<bk_column>,                  -- business key, not hash key
    s1.<attr1>,
    s1.<attr2>,
    s2.<attr3>
FROM <vault_schema>.HUB_<ENTITY> h
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<ENTITY>_<CONTEXT1>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<entity>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s1 ON s1.dv_hashkey_hub_<entity> = h.dv_hashkey_hub_<entity>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<ENTITY>_<CONTEXT2>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<entity>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s2 ON s2.dv_hashkey_hub_<entity> = h.dv_hashkey_hub_<entity>;
```

**Alternative (if satellite_views enabled):** Join the generated `vc_<sat_name>` view directly — it already encapsulates the QUALIFY pattern.

### Point-in-time view (PIT required)

When the hub has multiple satellites, use the PIT table to avoid correlated subqueries and correctly align snapshot dates. PIT stores per-satellite `dv_applied_timestamp` — join satellites on that column:

```sql
CREATE OR REPLACE VIEW <schema>.DIM_<ENTITY>_HISTORY AS
SELECT
    h.<bk_column>,
    pit.SNAPSHOT_DATE,
    s1.<attr1>,
    s1.<attr2>,
    s2.<attr3>
FROM <vault_schema>.PIT_<ENTITY> pit
JOIN <vault_schema>.HUB_<ENTITY> h
    ON h.dv_hashkey_hub_<entity> = pit.dv_hashkey_hub_<entity>
LEFT JOIN <vault_schema>.SAT_<ENTITY>_<CONTEXT1> s1
    ON s1.dv_hashkey_hub_<entity> = pit.dv_hashkey_hub_<entity>
   AND s1.dv_applied_timestamp = pit.<sat1_alias>_dv_applied_timestamp
LEFT JOIN <vault_schema>.SAT_<ENTITY>_<CONTEXT2> s2
    ON s2.dv_hashkey_hub_<entity> = pit.dv_hashkey_hub_<entity>
   AND s2.dv_applied_timestamp = pit.<sat2_alias>_dv_applied_timestamp;
```

### Bridge-based view (multi-link traversal)

Use a Bridge table to traverse multiple links from an anchor hub without expensive ad-hoc joins:

```sql
CREATE OR REPLACE VIEW <schema>.FACT_<CONTEXT> AS
SELECT
    h_anchor.<bk_col_anchor>,
    h_related.<bk_col_related>,
    bdg.SNAPSHOT_DATE,
    s_anchor.<attr1>,
    s_related.<attr2>
FROM <vault_schema>.BDG_<ANCHOR>_<CONTEXT> bdg
JOIN <vault_schema>.HUB_<ANCHOR> h_anchor
    ON h_anchor.dv_hashkey_hub_<anchor> = bdg.dv_hashkey_hub_<anchor>
JOIN <vault_schema>.HUB_<RELATED> h_related
    ON h_related.dv_hashkey_hub_<related> = bdg.dv_hashkey_hub_<related>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<ANCHOR>_<CONTEXT>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<anchor>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_anchor ON s_anchor.dv_hashkey_hub_<anchor> = bdg.dv_hashkey_hub_<anchor>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<RELATED>_<CONTEXT>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<related>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_related ON s_related.dv_hashkey_hub_<related> = bdg.dv_hashkey_hub_<related>;
```

### Fact-style view (traversing a link)

Traverse a link to join two hubs and their satellites into a fact-style view:

```sql
CREATE OR REPLACE VIEW <schema>.FACT_<RELATIONSHIP> AS
SELECT
    h_a.<bk_col_a>,
    h_b.<bk_col_b>,
    s_a.<attr1>,
    s_b.<attr2>,
    s_lnk.<rel_attr1>,
    s_lnk.<rel_attr2>
FROM <vault_schema>.LNK_<NAME> lnk
JOIN <vault_schema>.HUB_<ENTITY_A> h_a
    ON h_a.dv_hashkey_hub_<entity_a> = lnk.dv_hashkey_hub_<entity_a>
JOIN <vault_schema>.HUB_<ENTITY_B> h_b
    ON h_b.dv_hashkey_hub_<entity_b> = lnk.dv_hashkey_hub_<entity_b>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<ENTITY_A>_<CONTEXT>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<entity_a>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_a ON s_a.dv_hashkey_hub_<entity_a> = lnk.dv_hashkey_hub_<entity_a>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_<ENTITY_B>_<CONTEXT>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_hub_<entity_b>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_b ON s_b.dv_hashkey_hub_<entity_b> = lnk.dv_hashkey_hub_<entity_b>
LEFT JOIN (
    SELECT * FROM <vault_schema>.SAT_RV_LNK_{badge}_{file}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_<lnk_name>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_lnk ON s_lnk.dv_hashkey_<lnk_name> = lnk.dv_hashkey_<lnk_name>;
```

---

### Effectivity satellite query templates

Three standard query patterns for effectivity satellites, each as a reusable template:

**1. Active relationships only:**
```sql
SELECT lnk.*, eff.dv_start_date
FROM LNK_<NAME> lnk
JOIN SAT_EF_RV_LNK_{badge}_{file} eff ON lnk.dv_hashkey_lnk_rv_<name> = eff.dv_hashkey_lnk_rv_<name>
QUALIFY ROW_NUMBER() OVER (PARTITION BY eff.dv_hashkey_lnk_<name> ORDER BY eff.dv_applied_timestamp DESC) = 1
WHERE eff.dv_end_date = '9999-12-31'::TIMESTAMP_NTZ;
```

**2. Full history (active + all closed):**
```sql
SELECT lnk.*, eff.dv_start_date, eff.dv_end_date,
    CASE WHEN eff.dv_end_date = '9999-12-31'::TIMESTAMP_NTZ THEN 'ACTIVE' ELSE 'CLOSED' END AS relationship_status
FROM LNK_<NAME> lnk
JOIN SAT_EF_RV_LNK_{badge}_{file} eff ON lnk.dv_hashkey_lnk_rv_<name> = eff.dv_hashkey_lnk_rv_<name>;
```

**3. Active + replaced (latest slice showing current and predecessor):**
```sql
SELECT lnk.*, eff.dv_start_date, eff.dv_end_date,
    LAG(lnk.dv_hashkey_hub_<non_driving>) OVER (
        PARTITION BY lnk.dv_hashkey_hub_<driving> ORDER BY eff.dv_start_date
    ) AS prev_relationship
FROM LNK_<NAME> lnk
JOIN SAT_EF_RV_LNK_{badge}_{file} eff ON lnk.dv_hashkey_lnk_rv_<name> = eff.dv_hashkey_lnk_rv_<name>
QUALIFY ROW_NUMBER() OVER (PARTITION BY lnk.dv_hashkey_hub_<driving> ORDER BY eff.dv_start_date DESC) = 1;
```

The third template is useful for showing "customer moved from account X to account Y" in a single row.

---

## Stem-and-leaf view pattern

A two-phase templated approach for constructing consolidated views over multiple satellites around a hub or link when **full history** (not just current state) is required.

**Problem:** Each satellite around an entity has its own independent change timeline. A change in satellite A at date X and a change in satellite B at date Y means neither satellite has the full consolidated timeline. Simple joins produce incorrect results (misaligned timestamps).

**Solution:** Stem-and-leaf pattern:

**Phase 1 — Leaf CTEs** (one per satellite):
- Select only the columns needed for the IM (not all satellite columns)
- Rehash the selected subset: `SHA2(CONCAT_WS('||', selected_cols))` — if the subset produces duplicates that weren't duplicates in the full satellite, this rehash deduplicates them
- Compute virtual end-date via `LEAD(dv_applied_timestamp) OVER (...) - INTERVAL '1 day'`
- Result: each leaf has `(hashkey, start_date, end_date, selected_attributes)`

**Phase 2 — Stem CTE:**
- UNION ALL of all leaf start_dates to create a consolidated timeline (all event dates from all satellites merged)
- Left-join each leaf back using range predicates: `leaf.start_date <= stem.snapshot_date AND leaf.end_date >= stem.snapshot_date`
- Result: one row per entity per consolidated snapshot date, with columns from all satellites aligned

```sql
CREATE OR REPLACE VIEW DIM_CUSTOMER_HISTORY AS
WITH
-- LEAF 1: demographics satellite (subset of columns)
leaf_demo AS (
    SELECT dv_hashkey_hub_customer,
           dv_applied_timestamp AS start_date,
           COALESCE(LEAD(dv_applied_timestamp) OVER (
               PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp
           ) - INTERVAL '1 day', '9999-12-31') AS end_date,
           first_name, last_name, email
    FROM SAT_RV_HUB_CUSTOMER_DEMO
),
-- LEAF 2: financial satellite (subset of columns)
leaf_fin AS (
    SELECT dv_hashkey_hub_customer,
           dv_applied_timestamp AS start_date,
           COALESCE(LEAD(dv_applied_timestamp) OVER (
               PARTITION BY dv_hashkey_hub_customer ORDER BY dv_applied_timestamp
           ) - INTERVAL '1 day', '9999-12-31') AS end_date,
           credit_limit, account_status
    FROM SAT_RV_HUB_CUSTOMER_FIN
),
-- STEM: consolidated timeline from all leaves
stem AS (
    SELECT dv_hashkey_hub_customer, start_date AS snapshot_date FROM leaf_demo
    UNION
    SELECT dv_hashkey_hub_customer, start_date FROM leaf_fin
)
SELECT
    h.customer_bk,
    s.snapshot_date,
    d.first_name, d.last_name, d.email,
    f.credit_limit, f.account_status
FROM stem s
JOIN HUB_CUSTOMER h ON h.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer
LEFT JOIN leaf_demo d ON d.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer
    AND d.start_date <= s.snapshot_date AND d.end_date >= s.snapshot_date
LEFT JOIN leaf_fin f ON f.dv_hashkey_hub_customer = s.dv_hashkey_hub_customer
    AND f.start_date <= s.snapshot_date AND f.end_date >= s.snapshot_date;
```

**Current-only shortcut:** For current-state views (no history needed), skip the stem construction entirely — use `GREATEST(leaf1.start_date, leaf2.start_date)` with INNER JOINs on the latest row from each leaf.

**Intra-day leaf consolidation:** For satellites with intra-day keys, the virtual end-date must be derived from `SELECT DISTINCT hashkey, DATE(dv_applied_timestamp)` (grouped to day level) because sub-day end-dates produce overlapping ranges.

---

## Hash-join dimension pattern (parallel fact + dimension loading)

Instead of traditional surrogate sequence keys that force sequential dimension-then-fact loading, hash all dimension attributes (excluding event dates) into a binary join key:

```sql
-- Dimension table: accumulative unique-state (no SCD-2 start/end dates)
CREATE TABLE DIM_PRODUCT (
    d_product_hashjoin   BINARY(20)    NOT NULL PRIMARY KEY,  -- hash of all dim attributes
    product_name         VARCHAR,
    category             VARCHAR,
    brand                VARCHAR
    -- NO start_date, end_date, current_flag
);

-- Load: INSERT only new unique combinations
INSERT INTO DIM_PRODUCT
SELECT SHA1(CONCAT_WS('||', product_name, category, brand)), product_name, category, brand
FROM staged_data
WHERE SHA1(CONCAT_WS('||', product_name, category, brand)) NOT IN (SELECT d_product_hashjoin FROM DIM_PRODUCT);

-- Fact table references the hash-join key
CREATE TABLE FACT_SALES (
    sale_date            DATE,
    d_product_hashjoin   BINARY(20),  -- FK to DIM_PRODUCT
    d_customer_hashjoin  BINARY(20),  -- FK to DIM_CUSTOMER
    quantity             NUMBER,
    amount               NUMBER(18,2)
);
```

**Key properties:**
- **Parallel loading** — facts and dimensions load from the same staged file simultaneously (no lookup dependency)
- **No SCD-2 overhead** — dimensions are accumulative (unique states only, no start/end dates, no current flags). Each unique attribute combination = one row forever.
- **Hash-join determinism** — the hash key is derived from the attribute values; same values always produce same key regardless of load order
- Event dates (sale_date, order_date) are NEVER included in the dimension hash — they belong in the fact table

**Trade-off:** Only viable when all dimension attributes and measures are available in the same source file. If dimensions arrive from a different source, traditional sequence-key patterns may still be needed.

---

## Dynamic source-selection mart (prime-number modulus)

During a system migration where both legacy and new sources supply the same business keys with an overlap period, use a data-driven source-precedence pattern:

1. Assign each source a unique **prime number** (e.g. legacy=2, new_system=3)
2. UNION ALL both sources with their prime identifier
3. Use `SUM(prime) % COUNT(*)` per business key — if modulus is non-zero, the key exists in both sources

```sql
CREATE OR REPLACE VIEW MART_POLICY_MIGRATED AS
WITH combined AS (
    SELECT policy_bk, 2 AS src_prime, attributes.* FROM legacy_policy_view
    UNION ALL
    SELECT policy_bk, 3 AS src_prime, attributes.* FROM new_system_policy_view
),
source_check AS (
    SELECT *,
        SUM(src_prime) OVER (PARTITION BY policy_bk) % COUNT(*) OVER (PARTITION BY policy_bk) AS overlap_flag
    FROM combined
)
SELECT * FROM source_check
WHERE overlap_flag = 0           -- key exists in only one source: keep it
   OR (overlap_flag != 0 AND src_prime = 3);  -- key exists in both: prefer new system
```

**Key property:** Entirely data-driven — as the migration completes and legacy stops supplying records, the mart auto-adjusts without code changes. No manual cutover date logic required.

---

---

## Lambda view pattern — blending historical and real-time data

A **lambda view** is an IM view that augments historical vault data with real-time or near-real-time data in a single consumer-facing query. Named after the lambda architecture (batch layer + speed layer), it uses `UNION ALL` to combine two data sources:

- **Historical layer** — the standard vault satellite query (full history, validated, fully auditable)
- **Speed layer** — a direct join to a real-time staging table, Kappa Vault stream, or an NSAT that captures intraday/live events not yet in the main satellite

```sql
-- Lambda view: historical satellite + real-time staging UNION ALL
CREATE OR REPLACE VIEW <schema>.LAMBDA_DIM_<ENTITY> AS

-- Historical: from the vault satellite (audited, full history)
SELECT
    h.<bk_column>,
    s.attribute_1,
    s.attribute_2,
    s.dv_applied_timestamp,
    'VAULT'   AS data_layer
FROM <vault_schema>.HUB_<ENTITY> h
JOIN <vault_schema>.VC_SAT_<ENTITY>_<CONTEXT> s
    ON s.dv_hashkey_hub_<entity> = h.dv_hashkey_hub_<entity>

UNION ALL

-- Speed layer: from real-time staging (not yet in vault satellite)
SELECT
    <bk_column>,
    attribute_1,
    attribute_2,
    event_timestamp AS dv_applied_timestamp,
    'REALTIME' AS data_layer
FROM <staging_schema>.STG_<ENTITY>_REALTIME
WHERE event_timestamp > (
    SELECT COALESCE(MAX(dv_applied_timestamp), '1900-01-01') FROM <vault_schema>.VC_SAT_<ENTITY>_<CONTEXT>
);
```

**When to use:** consumers need to see the most recent state including events that have landed in staging but haven't completed the vault load cycle yet (e.g. dashboards with sub-minute freshness SLAs where batch vault loads run hourly).

**When not to use:** the speed layer content is not yet validated by hard rules — the lambda view exposes pre-vault data. If the vault's hard rules would reject some real-time records, those records appear valid in the lambda view until the vault load runs. Make this trade-off explicit to consumers via the `data_layer` discriminator column.

---

## Calculated attributes in the IM

If a calculation or derivation produces an attribute that belongs on a vault entity (e.g. customer lifetime value, order total), load it as a standard satellite first — treating the calculation result as a source — then expose it through the IM view like any other satellite. No special layer is needed.

```
Calculation/derived source → SAT_CUSTOMER_METRICS (standard satellite load)
                                        ↓
                             DIM_CUSTOMER view (IM)
```

---

## Pre-materialised alternative — Supernova

For high-volume, continuously-updated data where result caching is insufficient and query-time join performance is a bottleneck, use `/dv-supernova` instead of VC_/VH_ view-based IM views.

Supernova implements the same IM delivery goal as this skill but materialises the satellite joins as Incremental Dynamic Tables rather than resolving them at query time.

| Approach | When to use |
|---|---|
| VC_/VH_ views + IM view (this skill) | Storage cost matters; cross-hub traversal needed (use Bridge); data doesn't change frequently |
| Supernova DTs (`/dv-supernova`) | BI query speed is the priority; full column pre-materialisation is acceptable; computed attributes should be pre-built |

The Layer 5 delivery view patterns in `/dv-supernova` (filtered, aggregate, OBT, star schema) directly mirror the patterns in this skill — the only difference is that the source is `supernova.dt_xsn_supernova_*` instead of raw vault VC_/VH_ views.

---

## IM Performance Escalation Flowchart

When an IM view underperforms, escalate through these stages in order. Each stage adds complexity — do not skip ahead without validating the previous stage is insufficient:

```
Stage 1: STEM-AND-LEAF VIEWS
   │  Simple VC_/VH_ satellite views joined directly
   │  ├─ Performance acceptable? → STOP (simplest solution wins)
   │  └─ Too slow? → escalate
   │
Stage 2: PIT TABLE
   │  Pre-computed temporal alignment; eliminates QUALIFY at query time
   │  ├─ Performance acceptable? → STOP
   │  └─ Too slow (multi-link traversal needed)? → escalate
   │
Stage 3: BRIDGE TABLE
   │  Pre-joined hub-to-hub paths; eliminates multi-hop link joins
   │  ├─ Performance acceptable? → STOP
   │  └─ Too slow (both temporal + traversal)? → escalate
   │
Stage 4: PIT + BRIDGE (hybrid)
   │  Combine temporal alignment with pre-joined traversal
   │  ├─ Performance acceptable? → STOP
   │  └─ Still insufficient? → escalate
   │
Stage 5: PHYSICALIZE (Supernova / Dynamic Table)
   │  Materialise the entire IM result as an incremental DT
   └─ Use `/dv-supernova` — this is the last resort
```

**Key principles:**
- Always start at Stage 1 — premature optimisation creates maintenance debt
- PIT solves *temporal* performance (many satellites per hub); Bridge solves *traversal* performance (many hops between hubs)
- Stage 5 trades storage for speed — acceptable when BI SLAs demand sub-second response on large fact tables
- Each stage is disposable — escalation is reversible if data volumes or query patterns change

---

## Dynamic Iceberg Tables — delivering IMs to the data lakehouse

IMs can also be delivered as **Dynamic Iceberg Tables** when the enterprise requires curated BI content to be accessible outside of Snowflake's proprietary perimeter — for example, by Spark, Trino, or other Iceberg-compatible compute engines operating on the same object storage.

The mechanics are identical to FDN-backed Dynamic Tables. Write the same SQL in Snowflake, but the output is materialised as Apache Iceberg files (Parquet format) in cloud object storage (S3 / Azure Blob / GCS):

```sql
CREATE OR REPLACE DYNAMIC ICEBERG TABLE <im_schema>.DIM_CUSTOMER
    TARGET_LAG = '1 hour'
    WAREHOUSE  = <wh>
    EXTERNAL_VOLUME = '<volume_name>'
    CATALOG = 'SNOWFLAKE'
    BASE_LOCATION = 'dim_customer/'
AS
SELECT
    h.customer_bk,
    s.customer_name,
    s.customer_status,
    s.dv_applied_timestamp
FROM <vault_schema>.HUB_CUSTOMER h
JOIN <vault_schema>.VC_SAT_RV_HUB_{badge}_CUSTOMER_DEMOGRAPHICS s
    ON s.dv_hashkey_hub_customer = h.dv_hashkey_hub_customer;
```

**Same rules as FDN Dynamic Tables apply:**
- Design SQL for **incremental refresh** — full refresh is the anti-pattern (see `/dv-pit-bridge` Dynamic Table full refresh rule)
- Set `TARGET_LAG` to match the business freshness SLA — not unnecessarily tight
- Source tables can be a mix of Iceberg and FDN (Snowflake resolves both transparently)

**When to choose Dynamic Iceberg Tables for IM delivery:**

| Need | Approach |
|---|---|
| IM only consumed by Snowflake users and tools | Standard VC_/VH_ view-based IM or Supernova DT (FDN) |
| IM must be queryable by non-Snowflake engines (Spark, Trino, etc.) | Dynamic Iceberg Table |
| IM contains PII | Stay with FDN Dynamic Table (keeps PII within Snowflake's encrypted perimeter) |

---

## What to put in the IM vs. the vault

| Lives in Raw Vault | Lives in Information Mart |
|---|---|
| Hash keys | Business keys |
| `dv_load_timestamp`, `dv_applied_timestamp`, `dv_hashdiff`, `dv_recordsource` | None (internal metadata) |
| All historical versions | Current or snapshot views |
| All source records | Joined, filtered, readable |
| Binary columns | Natural types only |

### Information Marts are disposable

Because the vault holds full insert-only audit history, **every IM view can be dropped and rebuilt from scratch at any time with zero data loss**. The vault is the corporate memory. The IM is a presentation layer over it.

**The IM is the Anti-Corruption Layer (ACL)**

From a Domain-Driven Design perspective, the Information Mart is the **Anti-Corruption Layer** — it shields business users and BI tools from the internal complexity of the Data Vault model. Without the IM, consumers would have to:
- Navigate hub + link + satellite join patterns with binary hash keys
- Understand QUALIFY / LEAD() view semantics for current-row retrieval
- Manage PIT / Bridge structures for cross-satellite equijoins

The IM resolves all of this and presents a clean, business-vocabulary interface. This is why:
- **Hash keys must never appear in IM output** — they are vault-internal join keys with no business meaning
- **IM naming uses business terms, not vault table names** — column aliases, view names, and entity labels must come from the business domain vocabulary
- **When the vault changes, the IM must be updated to maintain its ACL contract** — schema evolution, source migrations, or deprecated source systems must trigger an IM review to ensure the consumer-facing interface remains stable

This has three important consequences:

1. **IM views never need to be migrated** — if the IM needs to change (new columns, different joins, renamed entities), drop the view and recreate it. There is nothing in the IM that doesn't already exist in the vault. Unlike a star schema where the fact and dimension tables *are* the history, the vault holds history independently of the presentation layer.

2. **IM design can evolve freely** — because the IM is disposable, there is no lock-in to an initial design decision. The cost of changing an IM view is almost zero. This is what makes the vault model a long-lived platform while IMs adapt to changing business reporting needs.

3. **Vault corrections automatically propagate to all IMs** — when a vault correction is applied (e.g. XTS corrects an out-of-sequence satellite timeline), IM views built as SQL views over vault tables automatically reflect the corrected state on the next query. No IM rebuild is needed. The correction is applied once, in the vault, and all downstream views pick it up immediately. This is the full consequence chain: XTS corrects vault timeline → views reflect correction → no IM rebuild required.

> When asked "what happens if the IM needs to change?", the answer is: "Drop and recreate it. The history is in the vault."
> When asked "what happens if a vault correction is applied?", the answer is: "IM views automatically reflect it. No rebuild needed."

**Unix philosophy applied to data products**

A useful design heuristic for Information Mart and data product design — borrowed from Unix philosophy:

1. **Do one thing and do it well** — each IM view should serve one well-defined consumer need or persona. Avoid overloaded IMs that try to answer every possible question from every possible consumer in a single view. A wide, multi-purpose mart becomes a legacy mart.
2. **Design data products that can be assembled and work together** — composable IMs (a customer dimension view + an orders fact view + a product view) are more maintainable than one monolithic OBT. Composable patterns also allow PIT + Bridge + IM to layer cleanly on top of each other.
3. **Create data products to be tried early and retired when redundant** — the vault's disposable IM principle makes this possible: try an IM design early, let consumers validate it, and retire it without guilt when it's superseded. Data products that outlive their purpose accumulate maintenance cost. Retire them.

**IM rules vs. BV rules — the routing criterion**

Not every derived calculation or pre-computed metric needs to be physicalised in Business Vault. The decision criterion:

> **If a calculation can be easily re-created from Raw Vault and Business Vault data without any data loss, it is an Information Mart rule — keep it in the IM view or bridge table, not in BV.**

| Where it belongs | Criterion | Examples |
|---|---|---|
| **Business Vault** | The calculation produces a persistent, versioned business insight that must be auditable across rule changes | Risk scores, ML model outputs, derived lifecycle states, inferred relationships |
| **Information Mart** | The calculation can be reconstructed on-demand from RV+BV; no auditability of the formula itself is required | Rolling sums, running balances via window functions, date dimension keys, column renaming, unit conversions |

Pre-calculated metrics added to a bridge/fact table (e.g. `SUM(transaction) OVER (PARTITION BY ... ORDER BY dv_loaddate)`) are IM rules: the underlying transaction records are in the link-satellite; the rolling balance can be recalculated at any time. There is no business insight locked inside the aggregation that would be lost if the bridge were dropped and rebuilt.

## Doctrine check

Before finalising an IM view, spawn the **Doctrine Enforcer subagent** and ask it to verify:
- No BINARY columns selected
- No `_HK` columns in the SELECT list
- No `dv_hashdiff`, `dv_load_timestamp`, `dv_applied_timestamp`, `dv_recordsource` in the SELECT list (unless it's an explicit audit view)
- Every hub join uses the business key (`<bk_column>`) in the output

**Column name conformance is an IM-only concern**

Column names in Raw Vault satellites must be kept as close to source as possible. Column names in Business Vault satellites must reflect the derived concept. **Renaming and conforming column names to business vocabulary is exclusively an IM-layer responsibility.**

Reasons this rule matters:
- **RV conformance creates tech debt** — if the source later starts supplying a column with a name you've already used for a conformed alias, you have a naming collision with no clean resolution
- **BV conformance defeats separation of concerns** — BV is for derived business rules, not cosmetic renaming. A BV satellite that just renames RV columns (and adds no derivation) is an anti-pattern: it duplicates storage and introduces a second load that must stay in sync
- **IM is disposable; RV/BV is not** — IM views can be dropped and recreated freely. The correct place to solve naming for each consumer domain is in each domain's IM — different business units can conform to different vocabularies in their own IM layer without affecting the shared vault

If different consumers need different column names for the same satellite column, create separate IM views for each consumer. Do not solve this in the vault layers.

**IM query performance rules:**
- **Use `UNION ALL` instead of `UNION`** — `UNION` performs an implicit `DISTINCT` deduplication (sort + compare) on every row. If data is correctly modelled at the source grain, there should be no duplicates to remove. `UNION ALL` is always faster and should be the default. Only use `UNION` when deduplication is genuinely required.
- **Use `DISTINCT` sparingly** — a `SELECT DISTINCT` in an IM query is usually a signal of either an incorrect join (producing fanout), a satellite placed at the wrong grain, or a missing deduplication in the staging layer. Investigate the root cause rather than masking it with `DISTINCT`.
- **Join at the correct grain** — avoid cartesian products by ensuring every join condition is complete. Missing a join predicate produces row multiplication that `DISTINCT` then masks, hiding a performance and correctness problem.
- **Select only the columns you need** — wide projections reduce columnar compression effectiveness and increase bytes scanned.
- **IM views benefit from Snowflake result cache** — because IMs are SQL views over vault tables, the exact same query with no underlying data changes returns from **result cache** (24-hour window) at zero compute cost — no virtual warehouse credits consumed. This is a free performance win for frequently-repeated IM queries (e.g. daily dashboard refreshes). Two conditions invalidate result cache: (1) the underlying vault tables have changed since the last run, or (2) the query includes context functions (`CURRENT_TIMESTAMP`, `CURRENT_USER`, etc.).

  **VW auto-suspend policy affects cache availability:** Virtual warehouse cache (distinct from result cache) is flushed when the warehouse suspends. If the IM query warehouse is configured to suspend immediately after each query, the VW cache is lost — subsequent queries that overlap with previously-fetched data must re-fetch from storage. Keep the IM query warehouse alive for at least the duration of a typical reporting session (recommended minimum: 5–10 minutes auto-suspend for interactive BI workloads).

---

## Activity Schema relationship Dynamic Tables

When the source of truth for the IM is a `SAT_BV_NH_{ENTITY}_STREAM` table (Activity Schema satellite), build Dynamic Tables instead of SQL views. This section covers the full pattern from per-activity DTs through enriched relationship DTs and dimension enrichment.

### Step 1 — Per-activity Dynamic Table (prerequisite)

Each activity needs its own DT with `activity_occurrence`, `activity_previous_at`, `activity_repeated_at` columns. These are generated by `/dv-bv-activity-schema`. If they don't exist yet, generate them first.

### Step 2 — Enriched relationship Dynamic Table

The enriched DT implements Activity Schema **relationships**: for a cohort activity, find what happened before, after, or in-between using another (append) activity.

**Pattern: cohort + append (before and after)**

```sql
CREATE OR REPLACE DYNAMIC TABLE <im_schema>.dt_<entity>_stream_enriched
    TARGET_LAG = '1 minute'
    WAREHOUSE  = <wh>
AS
WITH

-- "Before": for each cohort event, find append events that occurred BEFORE it
-- (between the previous cohort event and this one)
select_before AS (
    SELECT
        cohort.<bk_column>,
        cohort.dv_applied_timestamp                                       AS cohort_ts,
        cohort.activity_repeated_at                                 AS cohort_activity_repeated_at,
        cohort.activity_occurrence                                  AS cohort_activity_occurrence,
        append_before.revenue_impact                                AS append_revenue_impact_before,
        cohort.feature_json                                         AS cohort_feature_json,
        FIRST_VALUE(append_before.dv_applied_timestamp) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_before.dv_applied_timestamp)                  AS first_before_ts,
        LAST_VALUE(append_before.dv_applied_timestamp) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_before.dv_applied_timestamp)                  AS last_before_ts,
        FIRST_VALUE(append_before.feature_json) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_before.dv_applied_timestamp)                  AS first_before_feature_json,
        LAST_VALUE(append_before.feature_json) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_before.dv_applied_timestamp)                  AS last_before_feature_json
    FROM <im_schema>.dt_<entity>_stream_<cohort_activity>   cohort
    INNER JOIN <im_schema>.dt_<entity>_stream_<append_activity> append_before
        ON  cohort.<bk_column>             = append_before.<bk_column>
        AND cohort.dv_applied_timestamp          >= append_before.dv_applied_timestamp
        AND cohort.activity_previous_at    <  append_before.dv_applied_timestamp
),
aggregate_before AS (
    SELECT <bk_column>, cohort_ts,
        COUNT(*)                         AS append_before_count,
        SUM(append_revenue_impact_before) AS revenue_impact_before
    FROM select_before
    GROUP BY 1, 2
),

-- "After": for each cohort event, find append events that occurred AFTER it
-- (between this cohort event and the next one)
select_after AS (
    SELECT
        cohort.<bk_column>,
        cohort.dv_applied_timestamp                                       AS cohort_ts,
        cohort.activity_repeated_at                                 AS cohort_activity_repeated_at,
        cohort.activity_occurrence                                  AS cohort_activity_occurrence,
        append_after.revenue_impact                                 AS append_revenue_impact_after,
        cohort.feature_json                                         AS cohort_feature_json,
        FIRST_VALUE(append_after.dv_applied_timestamp) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_after.dv_applied_timestamp)                   AS first_after_ts,
        LAST_VALUE(append_after.dv_applied_timestamp) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_after.dv_applied_timestamp)                   AS last_after_ts,
        FIRST_VALUE(append_after.feature_json) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_after.dv_applied_timestamp)                   AS first_after_feature_json,
        LAST_VALUE(append_after.feature_json) OVER (
            PARTITION BY cohort.<bk_column>, cohort.dv_applied_timestamp
            ORDER BY append_after.dv_applied_timestamp)                   AS last_after_feature_json
    FROM <im_schema>.dt_<entity>_stream_<cohort_activity>   cohort
    INNER JOIN <im_schema>.dt_<entity>_stream_<append_activity> append_after
        ON  cohort.<bk_column>             = append_after.<bk_column>
        AND cohort.dv_applied_timestamp          <= append_after.dv_applied_timestamp
        AND cohort.activity_repeated_at    >  append_after.dv_applied_timestamp
),
aggregate_after AS (
    SELECT <bk_column>, cohort_ts,
        COUNT(*)                         AS append_after_count,
        SUM(append_revenue_impact_after)  AS revenue_impact_after
    FROM select_after
    GROUP BY 1, 2
),

-- "Stalk": union of cohort rows from both sides — drives the final row set
-- Ensures every cohort event appears once, even if it has no before/after matches
stalk AS (
    SELECT <bk_column>, cohort_ts FROM select_before
    UNION ALL
    SELECT <bk_column>, cohort_ts FROM select_after
),
stalk_distinct AS (
    SELECT *,
        FIRST_VALUE(cohort_ts) OVER (PARTITION BY <bk_column> ORDER BY cohort_ts) AS first_ever_ts,
        LAST_VALUE(cohort_ts)  OVER (PARTITION BY <bk_column> ORDER BY cohort_ts) AS last_ever_ts
    FROM stalk
    QUALIFY ROW_NUMBER() OVER (PARTITION BY <bk_column>, cohort_ts ORDER BY cohort_ts) = 1
)

SELECT
    stalk.<bk_column>,
    stalk.cohort_ts,
    stalk.first_ever_ts,
    stalk.last_ever_ts,
    before.first_before_ts,
    before.last_before_ts,
    after.first_after_ts,
    after.last_after_ts,
    agg_before.append_before_count,
    agg_before.revenue_impact_before,
    agg_after.append_after_count,
    agg_after.revenue_impact_after,
    COALESCE(before.cohort_feature_json, after.cohort_feature_json) AS cohort_feature_json,
    before.first_before_feature_json,
    before.last_before_feature_json,
    after.first_after_feature_json,
    after.last_after_feature_json
FROM stalk_distinct stalk
LEFT JOIN aggregate_before agg_before
    ON stalk.<bk_column> = agg_before.<bk_column> AND stalk.cohort_ts = agg_before.cohort_ts
LEFT JOIN select_before before
    ON stalk.<bk_column> = before.<bk_column>     AND stalk.cohort_ts = before.cohort_ts
LEFT JOIN aggregate_after agg_after
    ON stalk.<bk_column> = agg_after.<bk_column>  AND stalk.cohort_ts = agg_after.cohort_ts
LEFT JOIN select_after after
    ON stalk.<bk_column> = after.<bk_column>      AND stalk.cohort_ts = after.cohort_ts
QUALIFY ROW_NUMBER() OVER (PARTITION BY stalk.<bk_column>, stalk.cohort_ts
                           ORDER BY stalk.cohort_ts) = 1;
```

**Key design decisions:**
- `INNER JOIN` for before/after selects — only cohort rows that have matching append rows contribute to the window analytics
- `STALK` union ensures all cohort rows appear in the final output, even with no matches (LEFT JOINs on aggregates handle the NULLs)
- `QUALIFY ROW_NUMBER() = 1` at the end deduplicates the stalk union

### Step 3 — Dimension enrichment with ASOF JOIN

`ASOF JOIN` resolves a slowly-changing dimension to the correct row **at the point in time** of each cohort event. No date-range predicates needed — Snowflake handles the temporal alignment.

```sql
CREATE OR REPLACE DYNAMIC TABLE <im_schema>.dt_<entity>_stream_enriched_with_<dim>
    TARGET_LAG = '1 minute'
    WAREHOUSE  = <wh>
AS
SELECT
    enriched.<bk_column>,
    enriched.cohort_ts,
    dim.dv_applied_timestamp                  AS <dim>_effective_date,
    dim.<dim_col_1>,
    dim.<dim_col_2>,
    enriched.first_ever_ts,
    enriched.last_ever_ts,
    enriched.first_before_ts,
    enriched.last_before_ts,
    enriched.first_after_ts,
    enriched.last_after_ts,
    enriched.cohort_feature_json,
    enriched.append_before_count,
    enriched.revenue_impact_before,
    enriched.append_after_count,
    enriched.revenue_impact_after
FROM <im_schema>.dt_<entity>_stream_enriched enriched
ASOF JOIN <im_schema>.dim_<entity> dim
    MATCH_CONDITION (enriched.cohort_ts >= dim.dv_applied_timestamp)
    ON enriched.<bk_column> = dim.<bk_column>;
```

`ASOF JOIN` is Snowflake-native and exactly equivalent to `WHERE dim.dv_applied_timestamp = MAX(dim.dv_applied_timestamp) WHERE dim.dv_applied_timestamp <= enriched.cohort_ts` — but without the subquery and with better performance.

### Relationship types summary

| Relationship | Pattern | Activity Schema term |
|---|---|---|
| Before (in-between) | `cohort.dv_applied_timestamp >= append.dv_applied_timestamp AND cohort.activity_previous_at < append.dv_applied_timestamp` | Aggregate In Before |
| After (in-between) | `cohort.dv_applied_timestamp <= append.dv_applied_timestamp AND cohort.activity_repeated_at > append.dv_applied_timestamp` | Aggregate In Between |
| First ever | `FIRST_VALUE(append.dv_applied_timestamp) OVER (PARTITION BY entity ORDER BY append.dv_applied_timestamp)` | First Ever |
| Last ever | `LAST_VALUE(append.dv_applied_timestamp) OVER (...)` | Last Ever |
| SCD dim join | `ASOF JOIN dim MATCH_CONDITION (cohort.ts >= dim.effective_date)` | Last Before (for dimensions) |

---

## Twine — near-real-time IM enrichment pattern

**Twine** is a named technique for enriching fact-grain records with the correct version of dimension data at a point in time. It is suited for streaming / near-real-time workloads at very large scale where a pre-built PIT or star schema is not feasible.

**Selection rule — Twine/ASOF vs. hash join:**

| Workload | Preferred approach | Join plan | Why |
|---|---|---|---|
| Data at rest, pre-built star schema / bridge | Hash join (equi-join via PIT/SNOPIT) | Right-Deep (build + probe) | Optimal for OLAP queries over pre-modelled dimensional structures |
| Data in motion (streaming), very large fact tables, near-real-time enrichment | Twine / ASOF JOIN | Left-Deep (nested loop) | Memory-efficient for enriching records at event grain without pre-modelling |

**Two implementations:**

1. **Manual twine** — `UNION ALL` all fact and dimension tables on a common key + timestamp, then use window functions to find the nearest valid dimension date for each fact row, then left-join back to each dimension on that date:

```sql
SELECT id, ${fact_columns}, d1.${dim_columns}
FROM (
  SELECT twine.id, twine.timepoint,
    MAX(CASE WHEN timeline = 'D1' THEN timepoint END)
      OVER (PARTITION BY id ORDER BY timepoint) AS d1_ValidFrom,
    ${fact_columns}
  FROM (
    SELECT id, fact_ts AS timepoint, 'F' AS timeline, ${fact_columns} FROM fact_table
    UNION ALL
    SELECT id, dim_ts AS timepoint, 'D1' AS timeline, NULL FROM dim_table_1
  ) twine
) in_effect
LEFT JOIN dim_table_1 d1
  ON in_effect.id = d1.id AND in_effect.d1_ValidFrom = d1.dim_ts
WHERE in_effect.timeline = 'F';
```

2. **ASOF JOIN** — Snowflake's simplified SQL abstraction for twine. Same semantics, simpler code, similar Left-Deep join plan. Preferred for readability:

```sql
SELECT f.*, d1.${dim_columns}
FROM fact_table f
ASOF JOIN dim_table_1 d1
  MATCH_CONDITION(f.fact_ts >= d1.dim_ts)
  ON f.id = d1.id;
```

> **Note:** ASOF JOIN is NOT suitable for PIT table construction — it produces a Left-Deep Join Tree which is less performant than the standard equi-join (Right-Deep hash join) used in PIT builds. See Rule 3 in `/dv-pit-bridge`.

**CQRS streaming IM pattern**

Command Query Responsibility Segregation (CQRS) applied to Data Vault streaming:

| Component | DV construct | Technology |
|---|---|---|
| **Write Model** | Insert-only NHL or NSAT (immutable event log) | Snowpipe Streaming / Kafka — exactly-once semantics |
| **Read Model** | Dynamic Table using ASOF JOIN | Incremental refresh as new events arrive |

The Dynamic Table (Read Model) aggregates write model events at the business grain and refreshes incrementally, producing a near-real-time IM. Only apply this when you need data within a ~5-minute window from business event to analytical value. For batch or micro-batch needs, standard vault loading patterns are sufficient.

---

## After generating IM views

Suggest:
> "Consider creating a semantic layer on top of these views — column aliases and metric definitions — before connecting BI tools. Use `/dv-explain semantic-layer` for guidance."

---

## Semantic layer doctrine

The Information Mart views produced by `/dv-mart` feed into the semantic layer. The semantic layer translates vault and IM constructs into business-friendly terms: entities, dimensions, metrics, and hierarchies. It is the final step in the data delivery chain — not a modelling layer.

The following principles govern what the semantic model should and should not do:

### What the semantic layer IS

- **A business vocabulary layer** — translates technical column names into business terms (entity = customer, dimension = account type, metric = total transactions)
- **The end state of harmonised information** — by the time data reaches the semantic layer, all modelling conflicts, source deviations, and technical debt must already be resolved
- **Ephemeral and views-first** — deployed as views cached in memory for immediate accessibility; not a physical storage layer
- **Business-vetted** — every definition in the semantic model is confirmed by business users, because they are accountable for the analytical results

### What the semantic layer IS NOT

- **A modelling layer** — data modelling is solved in raw vault and business vault. The semantic model should do as little modelling as possible.
- **A technical debt resolution layer** — tech debt accumulated upstream is resolved as far left (upstream) as possible. The semantic layer never resolves tech debt; it inherits the result of upstream resolution.
- **A layered/stacked structure** — stacking semantic models (model A depends on model B which depends on model C) introduces unvetted dependencies that make the semantic layer fragile and hard to change. There is only one semantic layer.
- **An audit trail** — the semantic layer has no history. Calculations that need historization must be pushed left into the vault or Business Vault. The semantic model is ephemeral.
- **A business rules engine** — business rules belong upstream in the vault or BV. Only very lightweight presentation logic is permitted in the semantic model.
- **A hardcoded-values store** — no hardcoded values in the semantic model. Any data-driven change must come from reference data, not from edited semantic model definitions.

### 12 semantic layer principles (summary)

| # | Principle |
|---|---|
| 1 | Do as little modelling as possible — vault and BV solve the model |
| 2 | Push technical debt left — resolve it at source, not here |
| 3 | No layering/stacking — one flat semantic layer |
| 4 | Only one semantic layer per enterprise |
| 5 | Reflect business ontology and business terms |
| 6 | End state of harmonised data — inherits, doesn't resolve |
| 7 | No audit trail — ephemeral; historization is pushed left |
| 8 | Must be vetted by business |
| 9 | Accessible, searchable, secure, and accurate |
| 10 | Provides insights without delay (views, cached in memory) |
| 11 | Does not apply business rules |
| 12 | No hardcoded values — data-driven via reference data |

### DV → semantic layer mapping

| Vault construct | Semantic layer concept |
|---|---|
| Hub | Entity |
| Satellite attributes | Dimensions (descriptive attributes for slicing/dicing) |
| Satellite date columns | Time dimensions |
| Link satellite measures | Metrics / measures (additive, semi-additive facts) |
| Hub-to-hub path via link | Hierarchy (drill path) |
| Bridge table (`BRDG_`) | Fact table — aggregable measures across snapshot periods |

Use `/dv-explain semantic-layer` for a conceptual explanation of these principles.

### Cortex Analyst delivery chain

Snowflake **Semantic Views** are first-class Snowflake objects (not YAML files) that serve as the structured data interface for **Cortex Analyst** — Snowflake's natural-language REST API for enterprise data. The full delivery chain is:

```
IM view  →  Semantic View  →  Cortex Analyst  →  Cortex Agent
```

- **IM view** — vault query patterns, hash key elimination, business vocabulary
- **Semantic View** — entities, dimensions, facts, metrics, relationships declared as a Snowflake object; synonyms and verified queries improve LLM accuracy
- **Cortex Analyst** — accepts NL questions via REST API, translates to SQL using the Semantic View, returns structured results
- **Cortex Agent** — orchestrates Cortex Analyst (structured data) and Cortex Search (unstructured data) for agentic AI workflows

The semantic layer is not a new architectural layer — it is the **AI-accessible interface** to the existing IM layer. No additional modelling is required in the Semantic View beyond what the IM already provides.

### Three metrics categories — IM and semantic layer design

When designing IM views and semantic layer metrics, three standard categories from dimensional modelling apply. Each maps to a different vault query pattern:

| Category | Description | Vault query pattern | DV source |
|---|---|---|---|
| **Transactional** | Atomic-grain fact; one row per business event; additive or non-additive | Direct join to link satellite for measures; no aggregation required | Link satellite (one row per UoW) |
| **Periodic snapshot** | Data aggregated to a defined reporting period (daily, monthly, quarterly); semi-additive measures (e.g. `AVG` balances, `SUM` volumes for the period) | Aggregate satellite rows within the reporting window using `GROUP BY` period | Satellite history + `dv_applied_timestamp` date truncation |
| **Accumulating snapshot** | Tracks a business process from initiation to completion; milestones are recorded progressively (e.g. loan origination: applied → approved → funded → closed) | PIT table with one row per business entity tracking the latest state at each milestone satellite | Hub + PIT table spanning multiple milestone satellites |

**Design implication:** transactional metrics are the simplest and most common IM pattern. Periodic snapshots need careful choice of aggregation function (avoid `SUM` on semi-additive measures like balances). Accumulating snapshots are the most complex — they require a PIT table that spans milestone satellites and often a bridge table when the process crosses hub boundaries.
