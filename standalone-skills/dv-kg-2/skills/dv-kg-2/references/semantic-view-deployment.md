# Semantic View Deployment Reference

Semantic views are created by calling a stored procedure. The first argument is where to create it (`'database.schema'`). The second argument is the full YAML content as a `$$` dollar-quoted string. The view name comes from the `name:` field inside the YAML.

## Complete working example

This exact SQL succeeds:

```sql
CALL SYSTEM$CREATE_SEMANTIC_VIEW_FROM_YAML(
  'LIB_DEV01_EDW.KG',
  $$
name: KG_SEMANTIC_MODEL
description: Knowledge Graph catalog for a Data Vault.

tables:
  - name: KG_NODE_V
    base_table:
      database: LIB_DEV01_EDW
      schema: KG
      table: KG_NODE_V
    primary_key:
      columns:
        - NODE_KEY
    description: Business entities in the vault.
    dimensions:
      - name: NODE_KEY
        description: Entity identifier
        expr: NODE_KEY
        data_type: VARCHAR(16777216)
      - name: ENTITY_LABEL
        description: Business name
        expr: ENTITY_LABEL
        data_type: VARCHAR(16777216)
      - name: PII_PRESENT
        description: Whether entity has PII
        expr: PII_PRESENT
        data_type: BOOLEAN
    facts:
      - name: PROPERTY_COUNT
        description: Satellite count
        expr: PROPERTY_COUNT
        data_type: "NUMBER(38,0)"
    metrics:
      - name: TOTAL_ENTITIES
        expr: COUNT(NODE_KEY)

  - name: KG_ADJACENCY
    base_table:
      database: LIB_DEV01_EDW
      schema: KG
      table: KG_ADJACENCY
    description: Graph traversal adjacency.
    dimensions:
      - name: FROM_NODE
        expr: FROM_NODE
        data_type: VARCHAR(16777216)
      - name: TO_NODE
        expr: TO_NODE
        data_type: VARCHAR(16777216)
      - name: VIA_EDGE
        expr: VIA_EDGE
        data_type: VARCHAR(16777216)
      - name: RELATIONSHIP_LABEL
        expr: RELATIONSHIP_LABEL
        data_type: VARCHAR(16777216)

relationships:
  - name: ADJACENCY_TO_NODE
    left_table: KG_ADJACENCY
    right_table: KG_NODE_V
    relationship_columns:
      - left_column: FROM_NODE
        right_column: NODE_KEY
    relationship_type: many_to_one
  $$
);
```

## Troubleshooting

- `Unexpected number of qualifiers` → first argument must be exactly `'DATABASE.SCHEMA'` (2 parts)
- `referenced key must be primary or unique key` → add `primary_key: columns: [COL]` to the right_table
- `invalid identifier` → column name in YAML doesn't match view; run DESCRIBE to check
