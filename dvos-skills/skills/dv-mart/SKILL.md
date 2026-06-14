---
name: dv-mart
description: Build Information Mart views from the Raw Vault. Hash keys are never exposed — BI tools see only business keys and descriptive attributes.
enabled: true
---

# /dv-mart — Information Mart Views

Build query-ready views on top of the Raw Vault for BI tools and analysts. Hash keys are an internal vault implementation detail — they must never appear in Information Mart views.

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
| Current (latest record) | `VC_` | `VC_SAT_CUSTOMER_DEMOGRAPHICS` |
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
    SELECT * FROM <vault_schema>.SAT_LNK_<NAME>
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY dv_hashkey_<lnk_name>
        ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
    ) = 1
) s_lnk ON s_lnk.dv_hashkey_<lnk_name> = lnk.dv_hashkey_<lnk_name>;
```

---

## Calculated attributes in the IM

If a calculation or derivation produces an attribute that belongs on a vault entity (e.g. customer lifetime value, order total), load it as a standard satellite first — treating the calculation result as a source — then expose it through the IM view like any other satellite. No special layer is needed.

```
Calculation/derived source → SAT_CUSTOMER_METRICS (standard satellite load)
                                        ↓
                             DIM_CUSTOMER view (IM)
```

---

## What to put in the IM vs. the vault

| Lives in Raw Vault | Lives in Information Mart |
|---|---|
| Hash keys | Business keys |
| `dv_load_timestamp`, `dv_applied_timestamp`, `dv_hashdiff`, `dv_recordsource` | None (internal metadata) |
| All historical versions | Current or snapshot views |
| All source records | Joined, filtered, readable |
| Binary columns | Natural types only |

---

## Doctrine check

Before finalising an IM view, spawn the **Doctrine Enforcer subagent** and ask it to verify:
- No BINARY columns selected
- No `_HK` columns in the SELECT list
- No `dv_hashdiff`, `dv_load_timestamp`, `dv_applied_timestamp`, `dv_recordsource` in the SELECT list (unless it's an explicit audit view)
- Every hub join uses the business key (`<bk_column>`) in the output

---

## After generating IM views

Suggest:
> "Consider creating a semantic layer on top of these views — column aliases and metric definitions — before connecting BI tools. Use `/dv-explain semantic layer` for guidance."
