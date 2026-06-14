---
name: dv-validate
description: Validate a Data Vault 2.0 model definition against DV2.0 doctrine rules. Subcommands: model | manifest | naming
enabled: true
---

# /dv-validate — Doctrine Validation

Check a vault construct or full model against Data Vault 2.0 doctrine rules. Always runs before generation.

## Subcommands

### `/dv-validate model`  (default)

Validate a single construct definition pasted or described by the user.

**Spawn the Doctrine Enforcer subagent** (see `agents/doctrine-enforcer.md`) and pass the model definition. The Enforcer returns a structured list of violations.

Present results as:

```
DOCTRINE VALIDATION
===================
Construct: <name>
Status: ✅ CLEAN  /  ❌ VIOLATIONS FOUND

Violations:
  [DV-MAN-001] Hub HUB_ORDER has no business key defined
  [DV-SAT-001] SAT_CUSTOMER_CONTACT is missing dv_hashdiff column
  ...

Warnings (not blockers):
  [WARN-02] SAT_CUSTOMER_DEMO — consider splitting PII columns into a PII satellite
```

**Never auto-fix violations.** Explain each one and ask what the user wants to do.

---

### `/dv-validate manifest`

Validate an entire vault design (multiple hubs, links, satellites).

Ask the user to paste or point to a file containing their full model.

Run the Doctrine Enforcer subagent once per construct, then aggregate results:

```
MANIFEST VALIDATION SUMMARY
============================
  HUB_CUSTOMER       ✅ clean
  HUB_PRODUCT        ✅ clean
  LNK_ORDER          ❌ 2 violations
  SAT_CUSTOMER_DEMO  ⚠️  1 warning
  SAT_ORDER_DETAIL   ✅ clean

Total: 4 clean, 1 with violations, 1 with warnings
```

Drill into each violation with plain-language explanation.

---

### `/dv-validate naming`

Check naming conventions only — no structural doctrine.

**Spawn the Naming Advisor subagent** (see `agents/naming-advisor.md`) and pass all construct names and column names.

Check:
- Hub: `HUB_` prefix, singular noun, UPPER_SNAKE_CASE
- Link: `LNK_` prefix
- Satellite: `SAT_` prefix, `SAT_<PARENT>_<CONTEXT>` format
- PIT: `PIT_` prefix
- Bridge: `BDG_` prefix (NOT `BRDG_` or `BRIDGE_`)
- Hash key columns: `dv_hashkey_hub_<name>` (hubs) / `dv_hashkey_<link_full_name>` (links)
- Hash diff column: `dv_hashdiff` (not `HDIFF`, `HASH_DIFF`, `HD`)
- Load timestamp: `dv_load_timestamp` (not `LDTS`, `LOAD_DATE`, `LOAD_DTS`)
- Applied timestamp: `dv_applied_timestamp` (batch/file timestamp)
- Record source: `dv_recordsource` (not `RSRC`, `RECORD_SOURCE`, `SRC`)
- **No end-date column** — DVOS satellites are insert-only with no LEDTS. Current row via `QUALIFY ROW_NUMBER()`.
- Effectivity columns (type `ef` only): `dv_start_date`, `dv_end_date` (not `ACTIVE_FLAG`, not `dv_startts`)

## Doctrine Gate

If validation is called from within `/dv-generate`, a CLEAN result is required before any code is produced. The gate is enforced by the generate skill — not by the user saying "ignore errors."

## Subagent files

- Doctrine Enforcer: `agents/doctrine-enforcer.md`
- Naming Advisor: `agents/naming-advisor.md`
