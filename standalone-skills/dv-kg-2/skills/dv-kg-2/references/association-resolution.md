# Association Resolution — Link Resolution Algorithm

For each discovered link, inspect its columns and resolve which hubs it connects:

```sql
SELECT COLUMN_NAME, DATA_TYPE
FROM <database>.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = '<schema>' AND TABLE_NAME = '<link_table>'
ORDER BY ORDINAL_POSITION;
```

## Link prefix hints (informational — does not replace per-column resolution)

- `LNK_HY_*` → likely a hierarchy link (same hub, parent/child roles)
- `LNK_SA_*` → likely a same-as link (same hub, source-system roles)
- `LNK_NH_*` → non-historized link (standard resolution, no special handling)
- These prefixes guide the same-hub classification in step 4 but do not affect
  which hub each FK column resolves to (steps 1-3 still run independently).

## Hashkey vault resolution (priority order)

1. **Exact hub name match in column:** Strip the hashkey prefix (e.g. `DV_HASHKEY_HUB_`)
   from the column name. If the remainder exactly matches a discovered hub's name
   (minus its prefix), resolve directly. E.g. `DV_HASHKEY_HUB_CUSTOMER` → remainder
   `CUSTOMER` → matches `HUB_CUSTOMER`.

2. **Suffix match (role-playing):** If no exact match, check whether any discovered
   hub name (prefix-stripped) appears as a **suffix** of the remainder. E.g.
   `DV_HASHKEY_HUB_FROM_ACCOUNT` → remainder `FROM_ACCOUNT` → suffix `ACCOUNT`
   matches `HUB_ACCOUNT`. The prefix (`FROM_`) becomes the role label.
   Similarly: `DV_HASHKEY_HUB_TO_ACCOUNT` → role `TO`, hub `HUB_ACCOUNT`.

3. **Substring match:** If no suffix match, check whether any hub name is a
   **substring** of the remainder. Use the longest match to avoid false positives.

4. **Role detection (same hub multiple times):** If after resolution the same hub
   appears in multiple FK columns within one link (e.g. both `FROM_ACCOUNT` and
   `TO_ACCOUNT` → `HUB_ACCOUNT`), classify the link:
   
   - **Hierarchy link:** The link table prefix contains `HY` (e.g. `LNK_HY_*`), OR
     the role labels suggest parent/child semantics (e.g. `PARENT_`, `CHILD_`,
     `MANAGER_`, `REPORTS_TO_`).
   - **Same-as link:** The link table prefix contains `SA` (e.g. `LNK_SA_*`), OR
     the role labels are source-system badges (e.g. `CRM_`, `ERP_`, `MDM_`,
     `SOURCE_`, `MASTER_`).
   - **Role-playing link:** Neither hierarchy nor same-as — directional roles like
     `FROM_`/`TO_` (e.g. bank transfers between accounts).
   - **Ambiguous:** If none of the above patterns are clear, present both options
     to the user.
   
   Record both endpoints pointing to the same hub with respective role labels
   derived from the non-matching prefix/suffix portion.

5. **Unresolvable columns:** If a FK column cannot be resolved by any of the above
   (e.g. `DV_HASHKEY_HUB_MANAGER` and no `HUB_MANAGER` exists), this requires user
   input. Common case: hierarchy links where the role name doesn't embed the hub name
   (e.g. `MANAGER` is a role of `HUB_EMPLOYEE`).

## Natural-key vault resolution

1. **Column name match:** For each non-metadata column in the link, check if its name
   matches a business key column in any discovered hub. If match → that hub is a participant.

2. **Multiple columns matching same hub:** May indicate a hierarchy link (e.g.
   `PARENT_ACCOUNT_ID` and `CHILD_ACCOUNT_ID` both matching `HUB_ACCOUNT.ACCOUNT_ID`).

## Resolution Pattern Reference (field-tested against a 101-table DVOS vault)

| Pattern | Example | Strategy | User Input? |
|---------|---------|----------|-------------|
| Exact match | `DV_HASHKEY_HUB_CUSTOMER` → `CUSTOMER` → `HUB_CUSTOMER` | Step 1 | No |
| Role as prefix | `DV_HASHKEY_HUB_FROM_ACCOUNT` → `FROM_ACCOUNT` → suffix `ACCOUNT` | Step 2 | No |
| Source badge prefix | `DV_HASHKEY_HUB_CRM_CUSTOMER` → `CRM_CUSTOMER` → suffix `CUSTOMER` | Step 2 | No |
| N-ary link (3+ hubs) | 3 FK columns each resolve independently | Steps 1-3 | No |
| Hierarchy/same-as | Same hub resolved twice with different roles | Step 4 | No |
| Semantic role ≠ hub name | `DV_HASHKEY_HUB_MANAGER` (no `HUB_MANAGER`) | Step 5 | **Yes** |

Expected automatic resolution rate: ~96% of links in a well-structured vault.
