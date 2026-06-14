# DVOS Doctrine Rules — Quick Reference Card

## Hub Rules

| Rule | Description |
|------|-------------|
| DV-MAN-001 | Hub must have at least one business key column |
| DV-NAME-001 | Hub hash key: `dv_hashkey_hub_<name>`, type matches project hash algorithm |
| DV-MAN-002 | Hub must have `dv_load_timestamp` (TIMESTAMP_NTZ) |
| DV-MAN-003 | Hub must have `dv_recordsource` (VARCHAR) |
| DV-MAN-004 | Hub must have `dv_applied_timestamp` (TIMESTAMP_NTZ) |
| DV-MAN-005 | Hub must NOT contain descriptive attributes |
| DV-MAN-006 | Hub name must be singular (HUB_CUSTOMER not HUB_CUSTOMERS) |
| DV-MAN-007 | Hub primary key must be the hash key only |

## Link Rules

| Rule | Description |
|------|-------------|
| DV-LNK-001 | Link must reference at least 2 hub hash keys |
| DV-LNK-002 | Link must have its own hash key: `dv_hashkey_<link_full_name>` |
| DV-LNK-003 | Link must have `dv_load_timestamp` and `dv_recordsource` |
| DV-LNK-004 | Link must NOT contain descriptive attributes |
| DV-LNK-005 | Link name must begin with `LNK_` |
| DV-LNK-006 | FK constraints must NOT be in link DDL (deferred to orphan-check) |

## Satellite Rules

| Rule | Description |
|------|-------------|
| DV-SAT-001 | Satellite must reference exactly one parent (hub or link) via its hash key |
| DV-SAT-002 | Standard satellite must have `dv_load_timestamp`, `dv_hashdiff`, `dv_recordsource` |
| DV-SAT-003 | **No LEDTS / end-date column allowed** — satellites are insert-only |
| DV-SAT-004 | Satellite name must begin with `SAT_` |
| DV-SAT-005 | Satellite name must include parent name: `SAT_<PARENT>_<CONTEXT>` |
| DV-SAT-006 | Multi-active satellite must have `dv_sequence` in the PK |
| DV-SAT-007 | Non-historized satellite must NOT have `dv_hashdiff` |
| DV-SAT-008 | Satellite must NOT contain business keys (those belong in the hub) |
| DV-EFS-001 | Effectivity: `dv_start_date` + `dv_end_date`, no ACTIVE_FLAG, no business attributes, link-only |
| DV-EFS-002 | Effectivity satellite can only be a child of a link table |

## Hash Key Rules

| Rule | Description |
|------|-------------|
| DV-HASH-001 | Hash key: UPPER + TRIM + COALESCE with '-1' zero-key substitute |
| DV-HASH-002 | Hash key discriminator: `dv_collisioncode` (BKCC) — record source NOT in hash |
| DV-HASH-003 | Hash algorithm is project-configured (default SHA1). Do not assume MD5. |
| DV-HASH-004 | Hashdiff: TRIM + COALESCE with '' (empty string) — NO UPPER/LOWER |

## Load Pattern Rules

| Rule | Description |
|------|-------------|
| DV-LOAD-001 | Satellite loads must be INSERT-only — no UPDATE, DELETE, or MERGE |
| DV-LOAD-002 | Hub/link loads: MERGE with INSERT + `UPDATE SET last_seen_date` only |
| DV-LOAD-003 | No WHEN MATCHED clause in satellite loaders |

## Same-As Link Rules

| Rule | Description |
|------|-------------|
| DV-SAL-001 | SAL must reference exactly two hash keys from the same hub |
| DV-SAL-002 | SAL must have its own hash key: `dv_hashkey_sal_<entity>` |
| DV-SAL-003 | SAL must have `dv_load_timestamp` and `dv_recordsource` |
| DV-SAL-004 | SAL name must begin with `SAL_` |
| DV-SAL-005 | SAL must be paired with an effectivity satellite |
| DV-SAL-006 | SAL effectivity satellite must follow DV-EFS-001 |

## Information Mart Rules

| Rule | Description |
|------|-------------|
| DV-IM-001 | IM views must NOT SELECT any BINARY column |
| DV-IM-002 | IM views must NOT include `_HK`-named columns |
| DV-IM-003 | IM views must NOT include `dv_hashdiff`, `dv_load_timestamp`, `dv_applied_timestamp`, `dv_recordsource` |
| DV-IM-004 | IM views must include the business key column |

## General Rules

| Rule | Description |
|------|-------------|
| DV-GEN-001 | Every table must have `dv_load_timestamp` |
| DV-GEN-002 | Every table must have `dv_recordsource` |
| DV-GEN-003 | Every table must have `dv_applied_timestamp` |
| DV-GEN-004 | LEDTS / end-date column must NEVER appear in any DVOS table |

## Warnings (non-blocking)

| Rule | Description |
|------|-------------|
| WARN-01 | Satellite has more than 30 columns — consider splitting |
| WARN-02 | Satellite contains likely PII columns — consider PII satellite |
| WARN-03 | Link connects more than 5 hubs — verify intentional |
| WARN-04 | No PIT table defined for hub with 3+ satellites |
