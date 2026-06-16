---
name: dv-deploy
description: Generate deployment artifacts for a Data Vault — schema DDL, dependency-ordered execution scripts, role/grant setup, and environment promotion.
enabled: true
---

# /dv-deploy — Vault Deployment

Generate deployment artifacts for provisioning a Data Vault environment on Snowflake.

## Input

Ask the user:
1. What database and schema names? (landing, staging, vault, IM)
2. What roles are needed? (loader, reader, admin)
3. Target environment? (dev, uat, prod — or all)
4. Should I generate a zero-copy clone script for dev/test?

---

## Steps

### 1 — Schema creation

```sql
-- Landing layer (source data arrives here)
CREATE DATABASE IF NOT EXISTS <landing_db>;
CREATE SCHEMA IF NOT EXISTS <landing_db>.<landing_schema>;

-- Staging layer (TRANSIENT — no Fail-safe cost for ephemeral data)
CREATE TRANSIENT SCHEMA IF NOT EXISTS <vault_db>.<staging_schema>;

-- Vault layer (Raw Vault + Business Vault)
CREATE SCHEMA IF NOT EXISTS <vault_db>.<vault_schema>;

-- Information Mart layer
CREATE SCHEMA IF NOT EXISTS <vault_db>.<im_schema>;
```

### 2 — Role and grants setup

```sql
-- Loader role: can write to landing, staging, vault
CREATE ROLE IF NOT EXISTS <project>_LOADER;
GRANT USAGE ON DATABASE <landing_db> TO ROLE <project>_LOADER;
GRANT USAGE ON DATABASE <vault_db> TO ROLE <project>_LOADER;
GRANT USAGE ON SCHEMA <landing_db>.<landing_schema> TO ROLE <project>_LOADER;
GRANT USAGE ON SCHEMA <vault_db>.<staging_schema> TO ROLE <project>_LOADER;
GRANT USAGE ON SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_LOADER;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_LOADER;
GRANT INSERT, UPDATE ON ALL TABLES IN SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_LOADER;
GRANT SELECT ON ALL TABLES IN SCHEMA <landing_db>.<landing_schema> TO ROLE <project>_LOADER;
GRANT SELECT ON ALL VIEWS IN SCHEMA <vault_db>.<staging_schema> TO ROLE <project>_LOADER;

-- Reader role: SELECT on vault + IM only
CREATE ROLE IF NOT EXISTS <project>_READER;
GRANT USAGE ON DATABASE <vault_db> TO ROLE <project>_READER;
GRANT USAGE ON SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_READER;
GRANT USAGE ON SCHEMA <vault_db>.<im_schema> TO ROLE <project>_READER;
GRANT SELECT ON ALL TABLES IN SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA <vault_db>.<im_schema> TO ROLE <project>_READER;
GRANT SELECT ON FUTURE TABLES IN SCHEMA <vault_db>.<vault_schema> TO ROLE <project>_READER;
GRANT SELECT ON FUTURE VIEWS IN SCHEMA <vault_db>.<im_schema> TO ROLE <project>_READER;

-- Admin role: full control
CREATE ROLE IF NOT EXISTS <project>_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE <vault_db> TO ROLE <project>_ADMIN;
GRANT ALL PRIVILEGES ON DATABASE <landing_db> TO ROLE <project>_ADMIN;
```

### 3 — Dependency-ordered DDL execution

Generate an execution script that creates objects in the correct order:

```bash
# Deploy vault DDL in dependency order
snow sql -f 01_schemas.sql --connection <conn>
snow sql -f 02_hubs.sql --connection <conn>
snow sql -f 03_links.sql --connection <conn>
snow sql -f 04_satellites.sql --connection <conn>       # includes ghost record inserts
snow sql -f 05_staging_views.sql --connection <conn>
snow sql -f 06_pit_tables.sql --connection <conn>
snow sql -f 07_bridge_tables.sql --connection <conn>
snow sql -f 08_im_views.sql --connection <conn>
snow sql -f 09_tasks.sql --connection <conn>
snow sql -f 10_grants.sql --connection <conn>
```

**Ordering rules:**
1. Schemas first
2. Hubs (no dependencies)
3. Links (depend on hubs existing for orphan checks — but FK is deferred, so can run after hubs)
4. Satellites (depend on parent hub/link for ghost record insert validation)
5. Staging views (depend on landing tables + reference vault tables for `NOT EXISTS`)
6. PIT / Bridge (depend on satellites)
7. IM views (depend on vault + PIT/Bridge)
8. Tasks (depend on all objects they reference)
9. Grants (last — ensures objects exist)

### 3.1 — Index and Constraint Matrix

When deploying to platforms that support indexes (not Snowflake — Snowflake uses micro-partitions and zone maps instead), apply this matrix for constraint and index types per column per table type:

| Column | Hub | Link | Satellite | PIT | Bridge |
|---|---|---|---|---|---|
| **Hash key (PK)** | Unique, Not Null | Unique, Not Null | Composite PK (hash + timestamp), Not Null | Composite PK, Not Null | Composite PK, Not Null |
| **Business key** | Not Null | — | — | — | — |
| **FK hash keys** | — | Not Null (each) | Not Null (parent FK) | Not Null (each sat FK) | Not Null (each FK) |
| **dv_applied_timestamp** | Not Null | Not Null | Not Null, part of PK sort | Not Null | Not Null |
| **dv_load_timestamp** | Not Null | Not Null | Not Null, part of composite PK | Not Null | Not Null |
| **dv_hashdiff** | — | — | Not Null | — | — |
| **dv_recordsource** | Not Null | Not Null | Not Null | — | — |
| **Attribute columns** | — | — | Nullable (default) | — | — |

**Snowflake-specific notes:**
- Snowflake does not enforce PK/FK constraints but **does** use them for query optimisation (join elimination). Always declare them even though they are not enforced.
- Snowflake has no user-managed indexes — micro-partition zone maps provide automatic pruning. Natural load order (sorted by hash key + timestamp in staging) gives optimal zone map effectiveness.
- `NOT NULL` constraints ARE enforced on Snowflake and should be applied per the matrix above.
- `CLUSTER BY` is explicitly prohibited on satellite tables (natural load order is optimal — see `/dv-model satellite` no-cluster rule).

### 4 — Zero-copy clone for dev/test

```sql
-- Create dev environment as zero-cost clone of production
CREATE DATABASE <vault_db>_DEV CLONE <vault_db>;
CREATE DATABASE <landing_db>_DEV CLONE <landing_db>;

-- Grant dev roles
GRANT OWNERSHIP ON DATABASE <vault_db>_DEV TO ROLE <project>_DEV_ADMIN;
GRANT OWNERSHIP ON DATABASE <landing_db>_DEV TO ROLE <project>_DEV_ADMIN;
```

Zero-copy clones:
- Cost nothing until data diverges
- Include all table data, views, and grants at clone time
- Perfect for testing load scripts against real data shapes
- Can be dropped and recreated freely

### 5 — Environment promotion checklist

When promoting from dev → uat → prod:

```
PRE-FLIGHT CHECKS
=================
[ ] All /dv-test queries pass on source environment
[ ] Ghost records exist in every satellite (1 per table)
[ ] No orphan links (FK validation queries clean)
[ ] Row counts reconcile between staging and target
[ ] IM views return expected results
[ ] Task DAG executes without error

PROMOTION STEPS
===============
[ ] Export DDL from source environment
[ ] Review diff against target environment
[ ] Apply DDL changes to target (additive only — never DROP in prod)
[ ] Run ghost record deployment (idempotent)
[ ] Resume tasks in target environment
[ ] Validate with /dv-test post-deployment
```

---

## DATA_RETENTION_TIME_IN_DAYS guidance

### Permanent vs. Transient table types

Snowflake has two relevant table kinds for vault objects:

| Table kind | Max Time-Travel | Fail-Safe | Use for |
|---|---|---|---|
| **PERMANENT** | 0–90 days | 7 days (mandatory, cannot be changed) | All vault core objects (hubs, links, satellites, PIT, bridge) |
| **TRANSIENT** | 0–1 day | None | Staging tables only |

**Rule: vault core objects must be PERMANENT.** Fail-Safe is the last line of defence against catastrophic data loss (Snowflake support can recover within the 7-day fail-safe window). Transient tables have no fail-safe — if a satellite is accidentally dropped and the time-travel window has passed, the data is gone permanently. Since the satellite is the corporate memory, it must be PERMANENT.

**Rule: staging schemas should be TRANSIENT.** Staging tables are ephemeral — they are reloaded from the landing zone each batch. Paying fail-safe costs on data that can be recreated is waste. Create staging schemas with `CREATE TRANSIENT SCHEMA <name>`.

### Recommended `DATA_RETENTION_TIME_IN_DAYS` per object type

| Object type | Table kind | Recommended | Rationale |
|---|---|---|---|
| Hubs | PERMANENT | 1 day | Append-only, MERGE handles duplicates — low risk |
| Links | PERMANENT | 1 day | Same as hubs |
| Satellites | PERMANENT | 7–14 days | History is valuable; allows Time-Travel recovery of recent loads |
| Staging (TRANSIENT) | TRANSIENT | 0 days | Ephemeral — no Time-Travel needed, no fail-safe cost |
| PIT / Bridge | PERMANENT | 1 day | Rebuilt from vault — can always recreate |
| IM views | N/A | N/A | Views have no storage |

```sql
ALTER TABLE <schema>.HUB_<NAME> SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE <schema>.SAT_<PARENT>_<CONTEXT> SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

**GDPR / right-to-be-forgotten — PII satellite TIME_TRAVEL window**

When isolating identifying attributes into a dedicated PII satellite (the recommended satellite-splitting approach for GDPR compliance), set `DATA_RETENTION_TIME_IN_DAYS` on that satellite to align with your compliance deadline. The effective retention window includes both the TIME_TRAVEL period **and** the mandatory 7-day Fail-Safe:

| Compliance deadline | Set `DATA_RETENTION_TIME_IN_DAYS` to |
|---|---|
| 30 days | 23 days (23 + 7 = 30) |
| 14 days | 7 days (7 + 7 = 14) |
| 7 days | 0 days (0 + 7 = 7, minimum via Fail-Safe only) |

> Records in Fail-Safe are still technically retrievable by Snowflake support — coordinate with legal/compliance to confirm whether Fail-Safe retention satisfies your specific regulatory obligation before setting `DATA_RETENTION_TIME_IN_DAYS = 0` on a PII satellite.

Non-identifying satellite tables adjacent to the same hub are unaffected — they retain their standard retention window and remain analytically intact after the identifying record is disposed.

---

## Rules

- Never DROP tables in production — use additive DDL only (CREATE IF NOT EXISTS, ALTER ADD COLUMN)
- Staging schemas should be TRANSIENT to avoid Fail-safe storage costs
- Always deploy ghost records as part of satellite creation (idempotent INSERT)
- Use `snow sql` CLI for deployment — never `snowsql`
- Zero-copy clones are disposable — recreate rather than maintain
- **No FK constraints between vault tables** — foreign key constraints force staggered loads (an anti-pattern). Satellite loads would have to wait for parent hub/link loads to complete before FK validation passes. Instead, referential integrity is validated **post-load** via the test framework (`/dv-test` orphan detection) following an eventual consistency model. The vault converges as all loads complete — RI is guaranteed by construction (hash key determinism) and validated by testing.
- **Non-clustered index doctrine** — all indexes on vault tables must be non-clustered. On Snowflake this is moot (no user-managed indexes), but on relational platforms: clustered indexes force explicit sorting on ingest, causing fragmentation. Instead, pre-sort data before loading (ORDER BY hashkey, dv_applied_timestamp in staging) to achieve implicit clustering without explicit enforcement. On Snowflake, natural load-order achieves optimal zone-map effectiveness — explicit `CLUSTER BY` on satellite tables is an anti-pattern (see `/dv-model satellite` no-cluster rule).
- **Landing zone is NOT the audit** — the landing zone is a temporary staging area with a defined retention period (30 days recommended). It must not be relied upon as a regulatory or audit backup. The vault (RV + BV with full audit metadata columns) is the authoritative corporate memory. Teams that treat the landing zone as an audit trail are structurally at risk.
- **Separate loading and querying warehouses** — vault loading (hub/link/satellite loaders) and IM querying have fundamentally different caching requirements and should use separate virtual warehouses:

  | Warehouse purpose | Recommended auto-suspend | Why |
  |---|---|---|
  | **Vault loading** (hub, link, satellite loaders) | 1–5 minutes | Each load job is unique; VW cache provides minimal benefit. Short suspend minimises idle credit spend. |
  | **IM querying** (BI tools, dashboards, ad-hoc analysis) | 5–15 minutes (or longer for shared BI workloads) | Repeated IM queries benefit from VW cache for overlapping data. Suspending immediately after each query flushes the cache and forces re-fetching from storage on the next query. |

  Snowflake charges a minimum of 60 seconds when a warehouse spins up. If a load job runs for 30 seconds, you still pay 60 seconds. Sizing loading warehouses appropriately and suspending them quickly avoids paying for idle capacity. Result cache (24-hour window, zero VW credits) provides free repeat-query performance for stable IM queries — this benefit is warehouse-independent.

---

## Satellite schema evolution

When a source system adds a new column, extend the satellite in place — do not reload it.

```sql
-- Source adds new column: extend the satellite with ALTER TABLE ADD COLUMN
ALTER TABLE LIB_PRD01_EDW.SAL.SAT_RV_HUB_CUSTOMER_DEMOGRAPHICS
    ADD COLUMN loyalty_tier VARCHAR(50);
-- Historical rows carry NULL in this column = "column was absent at that time"
-- This is semantically different from a NULL value in a row loaded after the column existed
```

**3-valued logic in satellite evolution**

SQL has three truth values: TRUE, FALSE, and NULL. In an extended satellite:
- `NULL` in a historical row = **"this column didn't exist when this row was loaded"**
- `NULL` in a new row = **"the source had a null value for this column"**

These are different facts. Reloading the satellite to backfill NULLs destroys this distinction and corrupts the audit trail. The correct approach is always `ALTER TABLE ADD COLUMN` — let history speak for itself.

**Schema evolution rules:**
1. New source column → `ALTER TABLE ADD COLUMN` on the satellite. **Update staging hashdiff to include the new column.** History rows carry `NULL` for the new column.
2. Source column renamed → **do not rename the satellite column**. Add the new column name and stop populating the old one from new loads. The old column remains populated in historical rows.
3. Source column removed → **do not drop the satellite column**. Stop populating it in staging. Historical rows retain the value.
4. Never `TRUNCATE` or reload a raw vault satellite — this is always wrong. If you think you need to reload, investigate whether the issue is in staging or the landing layer instead.

**Hashdiff auto-migration \u2014 why no reload is ever needed**

When step 1 above is applied (new column added to staging hashdiff), something important happens automatically on the next load:

> The hashdiff for **every affected entity** changes — because the hash input now includes the new column's value. On the next normal load cycle, the `WHERE NOT EXISTS (hashdiff = ...)` anti-semi join finds no matching row for any entity (the old hashdiff no longer matches). A new state record is inserted for every entity, capturing the new column's value alongside all existing attributes.

The satellite **auto-migrates itself** through the normal load process. No replay, no reload, no intervention needed.

The distinction this creates:
- Rows loaded *before* the column existed: `NULL` in the new column = "this concept didn't exist then"
- Rows loaded *after* the column was added: `NULL` in the new column = "the value was genuinely absent in the source"

These are semantically different facts, and the insert-only model preserves both correctly. The fact that a person doesn't have a middle name is a different fact from the data model not having a concept of "middle name".

---

## Data lifecycle \u2014 feed disruption and artefact decommissioning

Deploying a vault is not the end of the operational lifecycle. Feeds stop. Sources get retired. GDPR requests arrive. This section covers what to do in each case.

### Unintentional feed disruption

When a feed silently stops loading, the business continues consuming stale data without knowing. This is the most trust-damaging failure mode.

**Prevention:** `/dv-test` DMF monitoring (`TRIGGER_ON_CHANGES`) catches when a table stops receiving new rows. Set up an alert on the DMF results table so that a zero-delta load is flagged immediately \u2014 not discovered by a business user days later.

**Recovery checklist:**
1. Identify when the last successful load ran (query `MAX(dv_load_timestamp)` on the affected table)
2. Determine root cause: upstream extract failure, schema change, credential expiry, network issue
3. Communicate to all consumers before restarting the feed
4. Replay missing rows from the landing table if retained; otherwise coordinate with source for a point-in-time re-extract

### Intentional feed/artefact retirement

When a data source or vault artefact is deliberately retired (replaced, decommissioned, or source decommissioned), follow this checklist:

**Step 1 \u2014 Impact assessment (before announcing)**
- Use data lineage to identify every downstream object that references the artefact
- Identify all IM views, BV satellites, PITs, and Bridges that depend on it
- Identify all teams and reports consuming those downstream objects

**Step 2 \u2014 Communication (vertical + horizontal)**
- **Vertical:** communicate through data lineage and lines of business \u2014 every team from the source through to the BI dashboard
- **Horizontal:** communicate across all scrum teams building on the affected artefacts simultaneously
- "The misinformed are misaligned" \u2014 silent retirements destroy trust

**Step 3 \u2014 The obituary**
Issue a formal change communication (the "obituary") covering:
- Exact name of the artefact being retired
- What replaces it (if anything)
- Retirement date
- Which downstream objects are affected
- Confirmation that downstream consumers have been migrated
- Archive location (where the retired artefact is secured, no longer queried)

**Step 4 \u2014 Archive and secure**
```sql
-- Do NOT drop the artefact. Move it to an archive schema.
ALTER TABLE LIB_PRD01_EDW.SAL.<ARTEFACT>
    RENAME TO LIB_PRD01_EDW.SAL_ARCHIVE.<ARTEFACT>_RETIRED_YYYYMMDD;

-- Revoke active consumer role access
REVOKE SELECT ON TABLE LIB_PRD01_EDW.SAL_ARCHIVE.<ARTEFACT>_RETIRED_YYYYMMDD
    FROM ROLE BANK_READER;
```

The vault history of the retired artefact is **preserved** \u2014 only active query access is removed. Never `DROP` a vault artefact.

**Step 5 \u2014 Suspend dependent tasks**
```sql
-- Suspend any load tasks targeting the retired artefact
ALTER TASK LIB_PRD01_EDW.SAL.TASK_LOAD_<ARTEFACT> SUSPEND;
```

### GDPR erasure request handling

When a data subject requests erasure (GDPR article 17), the enterprise must respond within one month. The vault response is:

1. **Identify** \u2014 locate all PII satellite rows for the affected business key
2. **Nullify/anonymise** \u2014 in the PII satellite, set PII columns to `NULL` or a tokenized value. This is an exception to the no-update rule, justified by legal obligation. Document the erasure event.
3. **Record** \u2014 use the XTS disposal columns (`dv_disposal_record_requested = TRUE`, `dv_record_retention_state = 'Purged'`) to mark the erasure
4. **Prevent reappearance** \u2014 configure the Record Tracking Satellite (RTS) for this entity to detect and block re-insertion if the BK reappears in a future source extract. Without this guard, the next batch load may re-insert the erased PII.
5. **Confirm** \u2014 document the erasure with a timestamp and the responding user for the audit trail

> GDPR data retention can still be justified in certain circumstances (fraud detection, legal proceedings). The answer always is context-dependent: consult legal before processing erasure requests for flagged entities.

## Subagent files

- SQL Generator: `agents/sql-generator.md`
