# Derived Views — SQL Templates

Execute these 4 statements ONE AT A TIME, in order. Wait for each to succeed before running the next.
Failure to follow this order WILL cause errors (KG_NODE_DEGREE references KG_ADJACENCY).

## STEP 1 of 4 — KG_NODE_V

```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_NODE_V AS
SELECT n.*,
  COALESCE((SELECT BOOLOR_AGG(pr.pii_present)
            FROM <kg_db>.<kg_schema>.KG_PROPERTY pr
            WHERE pr.attached_to = n.node_key), FALSE) AS pii_present,
  (SELECT COUNT(*)
   FROM <kg_db>.<kg_schema>.KG_PROPERTY pr
   WHERE pr.attached_to = n.node_key) AS property_count,
  (SELECT ARRAY_AGG(DISTINCT pr.source_universe)
   FROM <kg_db>.<kg_schema>.KG_PROPERTY pr
   WHERE pr.attached_to = n.node_key) AS contributing_universes,
  (SELECT ARRAY_AGG(DISTINCT pr.source_system)
   FROM <kg_db>.<kg_schema>.KG_PROPERTY pr
   WHERE pr.attached_to = n.node_key) AS contributing_sources
FROM <kg_db>.<kg_schema>.KG_NODE n;
```

## STEP 2 of 4 — KG_EDGE_V

```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_EDGE_V AS
SELECT e.*,
  (SELECT ARRAY_AGG(ep.node_key)
   FROM <kg_db>.<kg_schema>.KG_EDGE_ENDPOINT ep
   WHERE ep.edge_key = e.edge_key) AS endpoint_nodes,
  (SELECT COUNT(DISTINCT ep.node_key)
   FROM <kg_db>.<kg_schema>.KG_EDGE_ENDPOINT ep
   WHERE ep.edge_key = e.edge_key) AS arity,
  (SELECT COUNT(*)
   FROM <kg_db>.<kg_schema>.KG_PROPERTY pr
   WHERE pr.attached_to = e.edge_key) AS property_count
FROM <kg_db>.<kg_schema>.KG_EDGE e;
```

## STEP 3 of 4 — KG_ADJACENCY

```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_ADJACENCY AS
SELECT
  e1.node_key AS from_node,
  e2.node_key AS to_node,
  e1.role_description AS from_role,
  e2.role_description AS to_role,
  e1.edge_key AS via_edge,
  edg.cardinality,
  edg.relationship_label
FROM <kg_db>.<kg_schema>.KG_EDGE_ENDPOINT e1
JOIN <kg_db>.<kg_schema>.KG_EDGE_ENDPOINT e2
  ON e1.edge_key = e2.edge_key
  AND NOT (e1.node_key = e2.node_key AND COALESCE(e1.role_description,'') = COALESCE(e2.role_description,''))
  AND COALESCE(e1.node_key,'') || COALESCE(e1.role_description,'') 
      <= COALESCE(e2.node_key,'') || COALESCE(e2.role_description,'')
JOIN <kg_db>.<kg_schema>.KG_EDGE edg
  ON edg.edge_key = e1.edge_key;
```

**Why this join condition:**
- The NOT(...) clause excludes same-endpoint-to-itself joins (same node AND same role).
- The `<=` dedup on `node_key||role_description` ensures each edge appears once. Using COALESCE prevents NULL comparisons from filtering out standard binary links (which have NULL role_description).
- Hierarchy links (LNK_HY_*) and same-as links (LNK_SA_*) connect the SAME hub with different roles (e.g. EMPLOYEE→MANAGER) — the concatenation differentiates them correctly.

## STEP 4 of 4 — KG_NODE_DEGREE

Since KG_ADJACENCY is deduplicated (each edge appears once, not bidirectionally), the degree view must count both `from_node` and `to_node` appearances:

```sql
CREATE OR REPLACE VIEW <kg_db>.<kg_schema>.KG_NODE_DEGREE AS
SELECT node_key, COUNT(DISTINCT via_edge) AS degree
FROM (
  SELECT from_node AS node_key, via_edge FROM <kg_db>.<kg_schema>.KG_ADJACENCY
  UNION ALL
  SELECT to_node AS node_key, via_edge FROM <kg_db>.<kg_schema>.KG_ADJACENCY
)
GROUP BY node_key;
```

## Object Comments

```sql
COMMENT ON TABLE <vault_db>.<vault_schema>.<HUB_TABLE> IS 'Node: <label>';
COMMENT ON TABLE <vault_db>.<vault_schema>.<LNK_TABLE> IS 'Edge: <label>';
COMMENT ON TABLE <vault_db>.<vault_schema>.<SAT_TABLE> IS 'Property: <label> — attached to <parent>';
```
