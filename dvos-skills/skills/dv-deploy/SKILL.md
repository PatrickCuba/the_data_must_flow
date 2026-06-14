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

| Object type | Recommended | Rationale |
|---|---|---|
| Hubs | 1 day | Append-only, MERGE handles duplicates — low risk |
| Links | 1 day | Same as hubs |
| Satellites | 7–14 days | History is valuable; allows Time Travel recovery |
| Staging (TRANSIENT) | 0 days | Ephemeral — no Time Travel needed |
| PIT / Bridge | 1 day | Rebuilt from vault — can always recreate |
| IM views | N/A | Views have no storage |

```sql
ALTER TABLE <schema>.HUB_<NAME> SET DATA_RETENTION_TIME_IN_DAYS = 1;
ALTER TABLE <schema>.SAT_<PARENT>_<CONTEXT> SET DATA_RETENTION_TIME_IN_DAYS = 7;
```

---

## Rules

- Never DROP tables in production — use additive DDL only (CREATE IF NOT EXISTS, ALTER ADD COLUMN)
- Staging schemas should be TRANSIENT to avoid Fail-safe storage costs
- Always deploy ghost records as part of satellite creation (idempotent INSERT)
- Use `snow sql` CLI for deployment — never `snowsql`
- Zero-copy clones are disposable — recreate rather than maintain

---

## Subagent files

- SQL Generator: `agents/sql-generator.md`
