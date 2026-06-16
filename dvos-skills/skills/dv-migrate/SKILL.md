---
name: dv-migrate
description: Migrate an existing Data Vault from a legacy platform to Snowflake. Covers strategy selection (Lift & Shift / Lift & Adjust / Remodel), hash digest portability, PK/FK constraints, natural key refactoring, and parallel pipeline validation.
enabled: true
---

# /dv-migrate — Migrate a Data Vault to Snowflake

Guide a practitioner through migrating an existing Data Vault from a legacy platform to Snowflake, covering strategy selection, data representation risks, hash digest portability, and validation approach.

## Input

Ask the user:
1. What is the source platform? (SQL Server, Oracle, Teradata, other)
2. What is driving the migration? (platform end-of-life / performance / cloud modernisation / AI/analytics ambition)
3. Does the vault use hash-key joins or natural-key / sequence-key joins?
4. Which hash function is in use? (MD5 / SHA1 / SHA2-256 / other)
5. Is there a hard deadline to vacate the legacy platform?

---

## Three migration strategies

| Strategy | When to choose | Complexity |
|---|---|---|
| **Lift & Shift (Rehost)** | Must vacate platform quickly; vault is standards-compliant; need reconcilable results fast | Low |
| **Lift & Adjust** | Platform vacate + adopt Snowflake-optimal patterns (natural keys, performance structures) | Medium |
| **Remodel** | Funding secured; opportunity to change modelling methodology (e.g. move to Kimball/dbt) | High |

**Key principle:** only Raw Vault and Business Vault are the auditable record. The Information Delivery / IM layer is disposable — rebuild it from scratch on Snowflake rather than migrating it. If legacy data in the vault is confirmed as no longer needed, there is no obligation to migrate it; the decoupled nature of the DV model affords this flexibility.

---

## What to migrate and what to skip

| Layer | Migrate? | Notes |
|---|---|---|
| Raw Vault (hubs, links, satellites) | Yes — always | Auditable record of source system outputs |
| Business Vault (BV links, BV satellites) | Yes | Auditable output of business rule automation |
| PIT / Bridge tables | Optional | Ephemeral query-assistance structures; safe to recreate on Snowflake |
| Information Marts / IM views | No — rebuild | IM is disposable; recreating avoids carrying over legacy anti-patterns |
| Data confirmed as no longer needed | No | DV's decoupled model allows selective migration |

---

## Keys and constraints

Snowflake does not enforce primary key or foreign key constraints — but declare them anyway. BI tools and data modelling tools can reverse-engineer declared constraints to understand:
- Table uniqueness (hub hash key / natural key as PK)
- Relationships between hub, link and satellite tables (FK references)

Include `CONSTRAINT ... PRIMARY KEY` and `CONSTRAINT ... FOREIGN KEY ... REFERENCES` clauses in your Snowflake DDL even though Snowflake will not enforce them at load time. This metadata is useful for downstream tooling and documentation.

Snowflake does not support B-tree indexes. If query performance on large satellite tables is unsatisfactory post-migration, refer to the `dv-pit-bridge` skill for query-assistance structures (PIT/Bridge, CPIT as Dynamic Table, Gen-2 + SNOPIT patterns).

---

## Hash digest cross-platform portability

The same byte sequence fed into the same hash function always produces the same digest — across platforms. The migration risk is ensuring that the byte sequences are truly identical between the legacy platform and Snowflake.

**Key portability risks:**

| Risk | Snowflake behaviour | Mitigation |
|---|---|---|
| Character encoding | Snowflake is UTF-8 | If source platform uses ASCII or EBCDIC, convert all hash digests to UTF-8 before or during migration (preferred over converting in the pipeline) |
| Collation | Snowflake hashes on bytes, not characters — binary comparison | Even if both platforms treat strings as case-insensitive at the query layer, raw byte values differ for upper vs. lower case → digests will differ |
| Date / time representation | Snowflake stores timezone-aware timestamps | Normalise all date/time values to UTC before hashing |
| Precision and scale | Different rounding rules change string representation before hashing | Align numeric precision/scale between platforms before hash generation |
| SQL Server display prefix | SSMS displays binary values with a `0x` prefix | `0x` is a display artifact — it is not part of the digest; do not include it when comparing |

**One hash function rule:** choose one cryptographic hash function (MD5 minimum; SHA2-256 recommended) and use it exclusively throughout the vault. Never mix hash functions across tables or load pipelines — doing so breaks join integrity.

Hash function collision probability (weakest → strongest): MD5 → SHA1 → SHA2-256 → SHA2-512.

---

## Lift & Adjust — natural key refactor steps

For Snowflake, natural-key based hub, link and satellite tables outperform hash-key based vaults: hash digests appear as random fixed-length strings that compress poorly and incur a network transfer penalty (Snowflake queries from blob storage over the cloud backbone).

**To refactor from hash keys to natural keys:**
1. Merge the business key, BKCC (business key collision code) and multi-tenancy IDs into hub-adjacent satellites and link tables
2. Propagate those business keys through to link-satellite tables
3. Drop the surrogate hash key columns from hub, link and satellite tables

**Retain the hashdiff — do not drop it.** Moving to natural keys removes the hash join key, but the hashdiff still serves a critical purpose: comparing the full attribute set at load time without a column-by-column scan. Dropping the hashdiff means Snowflake must compare every attribute individually at load time. As satellite data volumes grow, this degrades load performance progressively. The hashdiff pays for itself in load efficiency.

---

## Parallel pipeline validation

Run the legacy platform and Snowflake pipelines in **parallel** with identical source data loads for a predetermined validation period before cutover. This allows:
- Direct comparison of hash digest outputs between platforms (surface encoding/collation mismatches)
- Row count and aggregation reconciliation across layers
- Detection of data type precision differences before they affect downstream consumers

Cutover only after the validation period produces reconcilable results or all differences are explained.

---

## Migration Consideration Flowchart

Use this decision tree to plan the migration approach for existing vault history:

```
1. Assess data volume of vault history to migrate
   │
   ├─ HIGH VOLUME (TB+ of satellite history)
   │   │
   │   └─ Rehash in a single bulk query (most efficient)
   │       - SELECT all rows from legacy vault
   │       - Recompute hash keys using Snowflake's hash function
   │       - INSERT into new Snowflake vault tables in one pass
   │       - Validate row counts + spot-check hash matches
   │
   └─ LOW VOLUME (manageable row counts, or selective migration)
       │
       └─ Loop through history iteratively
           - Process entity-by-entity or table-by-table
           - Recompute hashes per batch
           - Allows checkpoint/restart on failure
           │
           2. Has the source schema drifted since the vault was built?
           │
           ├─ YES — schema drift detected
           │   │
           │   ├─ Does the existing vault already handle the drift?
           │   │   ├─ YES → Migrate as-is (ALTER TABLE ADD COLUMN already applied)
           │   │   └─ NO → Backfill: apply schema evolution rules before migrating
           │   │
           │   └─ Are there new artefacts needed? (new hubs, links, satellites)
           │       ├─ YES → Create new artefacts on Snowflake; backfill from source history
           │       └─ NO → Migrate existing structure; apply ADD COLUMN post-migration
           │
           └─ NO — schema stable
               └─ Standard migration (Lift & Shift or Lift & Adjust per above)
```

**Key rules:**
- Always migrate Raw Vault + Business Vault; never migrate IM (rebuild from scratch)
- High-volume rehash is preferred — a single SQL query on Snowflake is parallelised across all nodes automatically
- Schema drift must be resolved BEFORE migration, not discovered after cutover
- If the legacy vault has known modelling defects, prefer Lift & Adjust (fix during migration) over Lift & Shift (carry the defect forward)

---

## Code conversion

This skill covers **Data Vault architecture migration** decisions. For converting legacy SQL stored procedures, ETL scripts, or Spark/PySpark jobs to Snowflake-compatible syntax, invoke the bundled **`migration-guide`** skill — it installs Snowflake's SnowConvert and Snowpark Migration Accelerator tooling to handle code-level conversion.

DV automation tooling that deploys well on Snowflake: AutomateDV, Coalesce, VaultSpeed.
