# Base Views — SQL Templates

## KG_NODE

Generate one `UNION ALL` branch per hub:
```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_NODE AS
SELECT
  '<HUB_TABLE_NAME>' AS node_key,
  '<Auto Label>' AS entity_label,
  '<vault_db>.<vault_schema>.<HUB_TABLE_NAME>' AS physical_table,
  'One row per unique <bk_cols_csv>' AS grain,
  ARRAY_CONSTRUCT('<bk1>', '<bk2>') AS business_keys,
  NULL AS domain,
  NULL AS criticality,
  NULL AS description,
  NULL AS agent_hint
UNION ALL
...
```

Label generation: strip hub prefix, replace underscores with spaces, title-case.
E.g. `HUB_RETAIL_CUSTOMER` → `Retail Customer`.

**Note on NULL enrichment columns:** `domain`, `criticality`, `description`, and `agent_hint`
are **enrichment columns** — they start as NULL and are populated later by the vault steward
or an AI enrichment pass. They exist so KG_NODE_V can surface governance metadata without
requiring a schema change later.

## KG_EDGE

One branch per link:
```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_EDGE AS
SELECT
  '<LNK_TABLE_NAME>' AS edge_key,
  '<Auto Label>' AS relationship_label,
  '<vault_db>.<vault_schema>.<LNK_TABLE_NAME>' AS physical_table,
  NULL AS cardinality,
  NULL AS relationship_semantics,
  NULL AS purpose,
  NULL AS description,
  NULL AS agent_hint
UNION ALL
...
```

## KG_EDGE_ENDPOINT

One branch per link-participant (hub FK in the link):
```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_EDGE_ENDPOINT AS
SELECT
  '<LNK_TABLE_NAME>' AS edge_key,
  '<HUB_TABLE_NAME>' AS node_key,
  '<role_or_null>' AS role_description
UNION ALL
...
```

For hierarchy/same-as links, both rows point to the same hub with different role_description values.

## KG_PROPERTY

One branch per satellite:
```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_PROPERTY AS
SELECT
  '<SAT_TABLE_NAME>' AS property_key,
  '<PARENT_HUB_OR_LINK>' AS attached_to,
  '<NODE|EDGE>' AS attached_to_type,
  '<vault_db>.<vault_schema>.<SAT_TABLE_NAME>' AS physical_table,
  NULL AS source_universe,
  NULL AS source_system,
  NULL AS pii_present,
  NULL AS description,
  NULL AS agent_hint
UNION ALL
...
```
