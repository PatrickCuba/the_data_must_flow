---
name: dv-xts
description: XTS-assisted satellite loading pattern for late-arriving and out-of-sequence data. Generates XTS DDL, the out-of-sequence SWITCH, and the timeline-correcting UNION ALL load for bi-temporal Data Vault satellites.
enabled: true
---

# /dv-xts — Extended Record Tracking Satellite (XTS) Pattern

XTS solves the "time crime" problem: when data arrives in the wrong order, the standard satellite `NOT EXISTS` hashdiff pattern records an incorrect entity timeline. XTS tracks every staged hashdiff — even duplicates — and auto-corrects the timeline using a COPY operation when a late-arriving record is detected.

---

## What XTS solves — the 5 crime scenarios

A satellite load processes data by comparing the staged hashdiff against the **current** satellite record. If data arrives out of sequence, the comparison is made against the wrong row.

| Scenario | What happens | XTS correction |
|---|---|---|
| 1 — No change | Staged hashdiff = current satellite hashdiff. No insert. | None needed. |
| 2 — Changed every load | Staged hashdiff differs every time. All inserts are correct. | None needed. |
| 3 — Reverted state | Day 3 data same as Day 1 — no insert (correct). | None needed. |
| 4 — Invalidated by late record | A late Day 2 record arrives after Day 3. Day 3 state is now incorrect — it shows the wrong "before" state. | COPY: re-insert the Day 3 record with the new load timestamp to restore the correct "after" state. |
| 5 — Earlier than current | A late Day 2 record is inserted, creating a duplicate in the timeline. | INSERT only — the timeline is correct by applied date ordering; IM views handle the ordering via QUALIFY. |

XTS records **every occurrence** of a staged hashdiff — the adjacent satellite records **true changes only**. XTS has far more rows but is wafer-thin (same columns every time, no business attributes).

---

## When to use XTS

**Recommend XTS when:**
- The source system has a known history of late-arriving or out-of-sequence extracts (upstream ETL failures, delayed feeds, reprocessed historical files)
- Regulatory or audit requirement that the entity timeline must always be provably correct
- Extract order from the source cannot be guaranteed

**Do NOT use XTS when:**

> **Kappa Vault incompatibility**: XTS is incompatible with Kappa Vault stream-based loading. The out-of-sequence SWITCH requires comparing `MAX(staging.dv_applied_timestamp)` against `MAX(satellite.dv_applied_timestamp)` as a gate before each load. Triggered tasks on streams execute row-by-row as events arrive — there is no batch boundary at which to evaluate this comparison. Do not set `xts_assisted: true` on any satellite with a Kappa Vault loading mode.

- The source is an event stream (append-only, naturally ordered) — Kappa Vault handles these correctly without XTS
- The pipeline can fix upstream ordering problems at the source — XTS should not paper over persistent orchestration failures

---

## Decision questions

Before generating XTS artefacts, confirm the following:

1. **Which satellite(s)** are expected to receive late-arriving data from this source?
2. **Does a XTS table already exist for this hub/link?** One XTS per hub, one per link — it tracks multiple adjacent satellites via `dv_rectarget`. If one exists, add the satellite name as a new `dv_rectarget` value; no new DDL needed.
3. **Loading mode** — batch or Kappa Vault? (if Kappa → block with incompatibility warning above)
4. **GDPR/data retention requirements?** If yes, add optional `dv_record_retention_state`, `dv_disposal_record_requested`, `dv_disposed_record_reemerged` columns to XTS.
5. **What is the satellite name?** Used as the literal value in `dv_rectarget` (e.g. `'sat_sapbw_address'`).
6. **What is the staging source table?** Used in the SWITCH comparison and load queries.

---

## Naming rules

| Artefact | Pattern | Example |
|---|---|---|
| XTS table | `SAT_XT_{PARENT_TYPE}_{PARENT}` | `SAT_XT_HUB_CUSTOMER` |
| Staging `dv_rectarget` column | `dv_rectarget_{satellite_name}` | `dv_rectarget_sat_sapbw_address` |

One XTS per hub, one per link. Multiple satellites tracked in the same XTS via `dv_rectarget`.

---

## XTS table DDL

```sql
-- One XTS per hub or link — tracks ALL adjacent satellites via dv_rectarget
CREATE TRANSIENT TABLE IF NOT EXISTS <schema>.SAT_XT_HUB_<PARENT>
(
    dv_tenant_id              VARCHAR(50),
    dv_hashkey_hub_<parent>  BINARY(20)    NOT NULL,
    dv_load_timestamp        TIMESTAMP_NTZ NOT NULL,
    dv_applied_timestamp     TIMESTAMP_NTZ NOT NULL,
    dv_recordsource          VARCHAR(255)  NOT NULL,
    dv_hashdiff              BINARY(20)    NOT NULL,
    dv_rectarget             VARCHAR(40)   NOT NULL,  -- adjacent satellite name
    dv_sequence_violation    BOOLEAN       NOT NULL,  -- TRUE = staged is older than target

    -- Optional: GDPR/retention columns (add when dv_disposal_required: true)
    -- dv_record_retention_state    VARCHAR(10)  -- 'Active', 'Archived', 'Purged'
    -- dv_disposal_record_requested BOOLEAN
    -- dv_disposed_record_reemerged BOOLEAN

    CONSTRAINT pk_sat_xt_hub_<parent>
        PRIMARY KEY (dv_hashkey_hub_<parent>, dv_load_timestamp) NOT ENFORCED
)
DATA_RETENTION_TIME_IN_DAYS = 1;
```

---

## Satellite DDL modification

When `xts_assisted: true`, add one column to the adjacent satellite DDL — after `dv_sid`:

```sql
dv_xts_event  VARCHAR(20),  -- 'insert' (new record) or 'copy' (timeline correction)
```

No other satellite DDL change is needed.

---

## The SWITCH — out-of-sequence detection

This is a metadata operation (no full table scan). Run it before every load cycle. It sets a session variable used by the satellite loader.

```sql
-- Step 1: detect out-of-sequence load
SET xts_out_of_sequence_event = FALSE;

SET xts_out_of_sequence_event = (
    WITH staged_max AS (
        SELECT MAX(dv_applied_timestamp) AS stg_max_date
        FROM staged.<source>
    ),
    target_max AS (
        SELECT MAX(dv_applied_timestamp) AS sat_max_date
        FROM <vault_schema>.<satellite>
    )
    SELECT CASE
               WHEN stg_max_date < sat_max_date THEN TRUE
               ELSE FALSE
           END AS test
    FROM staged_max, target_max
);
```

**Note:** The SWITCH is evaluated across the entire staging batch, not per entity. If any entity in the batch has a late record, the full XTS-assisted load runs. This is intentional — it protects the entire satellite — but means XTS-assisted loads run more frequently than strictly necessary on large batches. This is why the SWITCH is the CPU-saving gate: XTS-assisted loading involves more joins than the standard `NOT EXISTS` pattern.

---

## XTS INSERT — always runs

Run this regardless of the SWITCH result. XTS captures every staged hashdiff occurrence.

```sql
-- Step 2: always populate XTS
INSERT INTO <vault_schema>.SAT_XT_HUB_<PARENT>
    (dv_tenant_id, dv_hashkey_hub_<parent>, dv_load_timestamp, dv_applied_timestamp,
     dv_recordsource, dv_hashdiff, dv_rectarget, dv_sequence_violation)
SELECT DISTINCT
    dv_tenant_id,
    dv_hashkey_hub_<parent>,
    dv_load_timestamp,
    dv_applied_timestamp,
    dv_recordsource,
    dv_hashdiff_<satellite>       AS dv_hashdiff,
    dv_rectarget_<satellite>      AS dv_rectarget,
    $xts_out_of_sequence_event    AS dv_sequence_violation
FROM staged.<source> stg
WHERE NOT EXISTS (
    SELECT 1
    FROM (
        SELECT dv_hashkey_hub_<parent>,
               dv_hashdiff,
               dv_rectarget,
               dv_applied_timestamp,
               dv_load_timestamp,
               RANK() OVER (
                   PARTITION BY dv_hashkey_hub_<parent>, dv_applied_timestamp
                   ORDER BY dv_load_timestamp DESC
               ) AS dv_rnk
        FROM <vault_schema>.SAT_XT_HUB_<PARENT>
        QUALIFY dv_rnk = 1
    ) cur
    WHERE stg.dv_hashkey_hub_<parent>  = cur.dv_hashkey_hub_<parent>
      AND stg.dv_applied_timestamp           = cur.dv_applied_timestamp
      AND stg.dv_load_timestamp              = cur.dv_load_timestamp
      AND stg.dv_hashdiff_<satellite>  = cur.dv_hashdiff
      AND stg.dv_rectarget_<satellite> = cur.dv_rectarget
);
```

---

## XTS-assisted satellite load (SWITCH = TRUE)

Run when `$xts_out_of_sequence_event = TRUE`. The two CTEs identify the "before" and "after" states relative to the late-arriving staged record. The UNION ALL handles both the INSERT and the COPY.

```sql
-- Step 3a: XTS-assisted load (late-arriving data detected)
INSERT INTO <vault_schema>.<satellite>
    (dv_tenant_id, dv_hashkey_hub_<parent>, dv_load_timestamp, dv_applied_timestamp,
     dv_recordsource, dv_hashdiff, dv_xts_event, <business_columns>)

WITH

-- Most recent XTS record for each entity where staging has a NEWER record
-- (i.e. the record before the point where the late data belongs)
previous_xts AS (
    SELECT
        dv_hashkey_hub_<parent>,
        dv_hashdiff,
        dv_applied_timestamp,
        dv_load_timestamp,
        RANK() OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
        ) AS dv_rnk
    FROM <vault_schema>.SAT_XT_HUB_<PARENT>
    WHERE dv_rectarget = '<satellite_name>'
      AND EXISTS (
          SELECT 1 FROM staged.<source> stg
          WHERE stg.dv_hashkey_hub_<parent>  = SAT_XT_HUB_<PARENT>.dv_hashkey_hub_<parent>
            AND stg.dv_applied_timestamp           > SAT_XT_HUB_<PARENT>.dv_applied_timestamp
      )
    QUALIFY dv_rnk = 1
),

-- Most recent XTS record for each entity where staging has an EARLIER record
-- (i.e. the record after the point where the late data belongs)
next_xts AS (
    SELECT
        dv_hashkey_hub_<parent>,
        dv_hashdiff,
        dv_applied_timestamp,
        dv_load_timestamp,
        RANK() OVER (
            PARTITION BY dv_hashkey_hub_<parent>
            ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
        ) AS dv_rnk
    FROM <vault_schema>.SAT_XT_HUB_<PARENT>
    WHERE dv_rectarget = '<satellite_name>'
      AND EXISTS (
          SELECT 1 FROM staged.<source> stg
          WHERE stg.dv_hashkey_hub_<parent>  = SAT_XT_HUB_<PARENT>.dv_hashkey_hub_<parent>
            AND stg.dv_applied_timestamp           < SAT_XT_HUB_<PARENT>.dv_applied_timestamp
      )
    QUALIFY dv_rnk = 1
)

-- Part 1: INSERT new record (not already present in previous_xts, not a duplicate applied date)
SELECT DISTINCT
    dv_tenant_id,
    dv_hashkey_hub_<parent>,
    dv_load_timestamp,
    dv_applied_timestamp,
    dv_recordsource,
    dv_hashdiff_<satellite>  AS dv_hashdiff,
    'insert'                 AS dv_xts_event,
    <business_columns>
FROM staged.<source> stg

WHERE EXISTS (
    -- Staged record not already captured in previous_xts for this entity
    SELECT 1 FROM staged.<source> dlt
    WHERE NOT EXISTS (
        SELECT 1 FROM previous_xts xts
        WHERE xts.dv_hashkey_hub_<parent>  = dlt.dv_hashkey_hub_<parent>
          AND xts.dv_hashdiff              = dlt.dv_hashdiff_<satellite>
    )
    AND stg.dv_hashkey_hub_<parent> = dlt.dv_hashkey_hub_<parent>
)
AND NOT EXISTS (
    -- No duplicate applied date already in the satellite
    SELECT 1 FROM staged.<source> dlt
    INNER JOIN <vault_schema>.<satellite> sat
        ON dlt.dv_hashkey_hub_<parent> = sat.dv_hashkey_hub_<parent>
       AND dlt.dv_applied_timestamp          = sat.dv_applied_timestamp
)

UNION ALL

-- Part 2: COPY timeline correction
-- Scenario 4: the late record invalidates the current state.
-- Re-insert the "next" state with the current load timestamp to restore timeline integrity.
-- Condition: previous_xts.dv_hashdiff = next_xts.dv_hashdiff
-- (the state before AND after the late record is the same — the late record interrupted it)
SELECT DISTINCT
    stg.dv_tenant_id,
    sat.dv_hashkey_hub_<parent>,
    stg.dv_load_timestamp,
    next_xts.dv_applied_timestamp,    -- use next_xts applied date (the "after" timestamp)
    stg.dv_recordsource,
    sat.dv_hashdiff,
    'copy'                       AS dv_xts_event,
    sat.<business_columns>       -- copy business columns from the existing satellite row
FROM staged.<source> stg
INNER JOIN <vault_schema>.<satellite> sat
    ON stg.dv_hashkey_hub_<parent> = sat.dv_hashkey_hub_<parent>
INNER JOIN next_xts
    ON stg.dv_hashkey_hub_<parent> = next_xts.dv_hashkey_hub_<parent>
INNER JOIN previous_xts
    ON stg.dv_hashkey_hub_<parent> = previous_xts.dv_hashkey_hub_<parent>
   AND previous_xts.dv_hashdiff    = next_xts.dv_hashdiff  -- bookend condition: same state before and after

WHERE EXISTS (
    SELECT 1 FROM staged.<source> dlt
    WHERE NOT EXISTS (
        SELECT 1 FROM previous_xts xts
        WHERE xts.dv_hashkey_hub_<parent>  = dlt.dv_hashkey_hub_<parent>
          AND xts.dv_hashdiff              = dlt.dv_hashdiff_<satellite>
    )
    AND stg.dv_hashkey_hub_<parent> = dlt.dv_hashkey_hub_<parent>
)
AND NOT EXISTS (
    SELECT 1 FROM staged.<source> dlt
    INNER JOIN <vault_schema>.<satellite> sat
        ON dlt.dv_hashkey_hub_<parent> = sat.dv_hashkey_hub_<parent>
       AND dlt.dv_applied_timestamp          = sat.dv_applied_timestamp
);
```

---

## Normal satellite load (SWITCH = FALSE)

Run when `$xts_out_of_sequence_event = FALSE`. Standard hashdiff comparison against the current record.

```sql
-- Step 3b: normal load (data is in sequence)
INSERT INTO <vault_schema>.<satellite>
    (dv_tenant_id, dv_hashkey_hub_<parent>, dv_load_timestamp, dv_applied_timestamp,
     dv_recordsource, dv_hashdiff, dv_xts_event, <business_columns>)
SELECT DISTINCT
    dv_tenant_id,
    dv_hashkey_hub_<parent>,
    dv_load_timestamp,
    dv_applied_timestamp,
    dv_recordsource,
    dv_hashdiff_<satellite>  AS dv_hashdiff,
    'insert'                 AS dv_xts_event,
    <business_columns>
FROM staged.<source> stg
WHERE NOT EXISTS (
    SELECT 1
    FROM (
        SELECT dv_hashkey_hub_<parent>,
               dv_hashdiff,
               RANK() OVER (
                   PARTITION BY dv_hashkey_hub_<parent>
                   ORDER BY dv_applied_timestamp DESC, dv_load_timestamp DESC
               ) AS dv_rnk
        FROM <vault_schema>.<satellite>
        QUALIFY dv_rnk = 1
    ) cur
    WHERE stg.dv_hashkey_hub_<parent>  = cur.dv_hashkey_hub_<parent>
      AND stg.dv_hashdiff_<satellite>  = cur.dv_hashdiff
);
```

---

## Full orchestration order per load cycle

```
1. Stage data        → staged.<source>
2. SWITCH            → SET $xts_out_of_sequence_event
3. XTS INSERT        → SAT_XT_HUB_<PARENT>  (always, before satellite)
4. IF SWITCH = TRUE  → XTS-assisted satellite load (UNION ALL)
   ELSE              → Normal satellite load
5. Hub MERGE         → HUB_<PARENT>  (unaffected by XTS)
```

XTS can run before or after the satellite — it corrects timelines based on already-loaded records, not the current staged record. No table locking is required.

---

## Monitoring — sequence violations

```sql
-- Summary of out-of-sequence events per satellite
SELECT
    dv_rectarget,
    COUNT(*)                                    AS total_violations,
    MAX(dv_applied_timestamp)                         AS most_recent_violation
FROM <vault_schema>.SAT_XT_HUB_<PARENT>
WHERE dv_sequence_violation = TRUE
GROUP BY 1
ORDER BY 2 DESC;

-- Detail: show which satellite rows were the result of a timeline correction
SELECT hub.customer_id,
       sat.dv_applied_timestamp,
       sat.dv_xts_event,
       xt.dv_sequence_violation
FROM <vault_schema>.<satellite> sat
INNER JOIN <vault_schema>.HUB_<PARENT> hub
    ON sat.dv_hashkey_hub_<parent> = hub.dv_hashkey_hub_<parent>
INNER JOIN <vault_schema>.SAT_XT_HUB_<PARENT> xt
    ON sat.dv_hashkey_hub_<parent> = xt.dv_hashkey_hub_<parent>
   AND sat.dv_applied_timestamp          = xt.dv_applied_timestamp
ORDER BY hub.<bk_column>, sat.dv_applied_timestamp;
```

---

## Snowflake performance \u2014 logical vs. physical timeline correction

XTS corrects the *logical* timeline \u2014 the order of satellite states as seen through `dv_applied_timestamp` in views. The *physical* correction is simply a new INSERT appended to the most recent micro-partition. No existing records are touched.

**Key properties on Snowflake:**
- XTS COPY rows introduce **no micro-partition overlap** \u2014 they are physically appended at the tail of the table, like any other INSERT
- No partition reorganisation occurs, so there is no clustering overhead after a correction event
- SNOPIT tables built on XTS-corrected satellites retain the full `dv_sid` zone map advantage \u2014 the COPY row receives a new (higher) `dv_sid`, which lands at the physical tail where zone maps expect it

The logical vs. physical distinction is important: although the COPY row represents a state correction at an earlier point in the business timeline (`dv_applied_timestamp`), it is physically the newest row in the table. Snowflake query plans resolve this correctly via the IM view's `QUALIFY ROW_NUMBER() ORDER BY dv_applied_timestamp DESC` — the logical order is the query-time sort, not the physical storage order.

---

## XTS and data retention — record freshness and lifecycle

**True changes mask record aging**

A satellite table records only *true changes* — it cannot tell you whether a record is still active. If a satellite row is archived or purged and then the same record reappears from the source system with the same applied date, the satellite's MERGE/INSERT logic sees it as a new record it has never encountered and inserts it with a fresh load timestamp. This creates a false timeline — the record appears newer than it actually is.

XTS records a heartbeat on every load for every business key regardless of whether the record changed. This makes XTS the only accurate indicator of record freshness in a Data Vault. When XTS shows no recent heartbeat for a business key, the record has genuinely not been provided by the source — it may be aged, inactive, or eligible for tiering.

Use the `dv_record_retention_state` column on XTS (`Active` / `Archived` / `Purged` / `Restored`) as the data-driven control for storage lifecycle policy enforcement. XTS serves as the lookup table for any data lifecycle management automation.

**Ghost record must never be tiered**

Never apply an archival, purge, or storage lifecycle policy to the ghost record. Every PIT equi-join depends on the ghost record's existence — if the ghost row is archived or purged, PIT joins silently return no satellite data for all entities at all snapshot points. Explicitly exclude the ghost record (business key = zero binary / `dv_sid = 0`) from any lifecycle policy predicate.

**Obfuscation: never update the HashDiff**

When satisfying a right-to-be-forgotten request by obfuscating a satellite record in place, do **not** update the `dv_hashdiff` column. If the HashDiff is changed, the vault treats the obfuscated state as a new true change. On the next load from the source, the original (un-obfuscated) data is reintroduced because the MERGE logic no longer recognises it as a duplicate. Keeping the original HashDiff prevents this accidental reintroduction and keeps the obfuscated record stable.

**Right to be Forgotten is not universal across all downstream domains**

Receiving a GDPR Article 17 (right to erasure) request does not mean the deletion must automatically cascade to every consumer domain that holds data about that individual. Some domains have a substantive legal or regulatory reason to retain the data:

- A financial fraud prevention domain may be legally required to retain a customer's data even after an erasure request — to support ongoing fraud investigation or regulatory reporting obligations
- A regulatory compliance domain may need to retain records for a fixed audit period regardless of the individual's request

**Governance principle:** domain owners have the right to authorise — or decline — deletion requests within their domain. The vault records that a disposal request was made (`dv_disposal_record_requested = TRUE` in XTS); whether to act on that request in a downstream domain is an authorisation decision by the domain owner, not an automatic cascade from the vault layer.

Coordinate with legal, regulatory, and compliance stakeholders for each domain before implementing automatic deletion propagation.

## Three XTS deployment styles

XTS can be deployed in three architectural configurations:

| Style | Topology | Trade-off |
|---|---|---|
| **Cuttle** (default) | One XTS per hub/link, shared by all adjacent satellites | Minimal table count; requires resource locking when multiple sources load concurrently |
| **Remora** | One XTS per satellite table | Eliminates contention entirely; proliferates tables (1 XTS per satellite) |
| **Angler** (passive) | XTS never physically corrects satellites; correction pushed to consumption layer | No back-population of satellites; data is correct faster (no additional IO or storage); bi-table dependency only at consumption time |

**Angler style details:** The XTS is populated with out-of-sequence events as usual, but satellite tables are NOT corrected. Instead, consumption views (or PIT tables) read both the satellite and its XTS together, using the XTS to virtually present the correct chronological order. This avoids expensive back-population and is useful when:
- Storage cost of satellite re-inserts is significant
- Many satellites share one XTS (cuttle topology) and physical correction would cascade
- Speed of "correct data available" is more important than satellite table cleanliness

**Recommendation:** Start with Cuttle (simplest). Move to Remora only if contention becomes a measured problem. Use Angler only when physical correction cost is prohibitive and consumers can handle the virtual-timeline complexity.

## Satellite split evolution and XTS

When a satellite evolves (splits into multiple satellites), late-arriving data that predates the split requires special handling:

1. **Migrate** the entire old satellite content into the new split structures (both new satellites receive their respective columns from the historical records)
2. **Back-populate XTS** with the record hashes from the migrated content — the XTS must reflect the full timeline including pre-split history
3. **Only XTS scenarios 2, 4, and 5** (daily change, timeline correction, change occurred earlier) require downstream PIT table correction; scenarios 1 and 3 (no change, same change) do not affect PITs

**Why this matters:** If the XTS is not back-populated with pre-split history, any late-arriving record from before the split date will fail the SWITCH comparison (no historical reference point exists in the XTS). The satellite loader will treat it as a first-ever record rather than a timeline correction.

## Subagent files

- SQL Generator: `agents/sql-generator.md` — XTS-assisted load templates
- Doctrine Enforcer: `agents/doctrine-enforcer.md`
