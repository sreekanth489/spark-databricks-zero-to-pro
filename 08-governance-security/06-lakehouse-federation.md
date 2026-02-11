# Lakehouse Federation
> Module 08 — Topic 06 | Level: Advanced | Time: 35 min

---

## Learning Objectives

By the end of this topic you will be able to:

1. Explain Lakehouse Federation and when to use it.
2. Create connections to external databases (PostgreSQL, MySQL, Snowflake,
   BigQuery, SQL Server).
3. Create foreign catalogs that surface external tables in Unity Catalog.
4. Understand query pushdown optimization and how it improves performance.
5. Apply Lakehouse Federation for real-time access, migration, and cross-platform
   analytics use cases.
6. Identify the limitations and trade-offs of federated queries.

---

## Conceptual Overview

### What Is Lakehouse Federation?

Lakehouse Federation allows you to query external databases through Unity Catalog
**without copying data** into the Lakehouse. External tables appear in Unity
Catalog alongside your native Delta tables, governed by the same access controls.

```
  Without Federation                With Federation
  ===================               ==================

  +----------+  +----------+        +---------------------------+
  | Lakehouse|  | External |        |     Unity Catalog         |
  | (Delta)  |  | Database |        |                           |
  +----------+  +----------+        |  +-------+  +---------+  |
      |              |              |  | Delta |  | Foreign |  |
      |              |              |  | Tables|  | Catalog |  |
  Separate queries,               |  +-------+  +----+----+  |
  no unified view                  |                  |        |
                                   |              +---+---+    |
                                   |              |Postgres|    |
                                   |              |MySQL   |    |
                                   |              |Snowflake|   |
                                   |              |BigQuery |   |
                                   |              +---------+   |
                                   +---------------------------+
                                   One query engine, unified governance
```

### How It Works

Lakehouse Federation uses two objects in Unity Catalog:

1. **CONNECTION** — stores the connection details (host, port, credentials) for
   an external database system.
2. **FOREIGN CATALOG** — maps an external database into Unity Catalog's namespace,
   making its schemas and tables discoverable and queryable.

```
  Setup Flow:
  ===========

  Step 1: Create a CONNECTION (admin)
  +--------------------+
  | CONNECTION         |
  | - Type: postgresql |
  | - Host: db.example |
  | - Port: 5432       |
  | - User/Password    |
  +--------------------+
          |
          v
  Step 2: Create a FOREIGN CATALOG (admin)
  +--------------------+
  | FOREIGN CATALOG    |
  | Name: ext_postgres |
  | Using: CONNECTION  |
  +--------------------+
          |
          v
  Step 3: Query as normal (users)
  SELECT * FROM ext_postgres.public.customers;
  -- Looks like any other Unity Catalog table!
```

### Supported External Systems

| Database | Connection Type | Notes |
|----------|----------------|-------|
| PostgreSQL | `postgresql` | Full pushdown support |
| MySQL | `mysql` | Full pushdown support |
| SQL Server | `sqlserver` | Full pushdown support |
| Snowflake | `snowflake` | Full pushdown support |
| Google BigQuery | `bigquery` | Full pushdown support |
| Amazon Redshift | `redshift` | Full pushdown support |
| Oracle | `oracle` | Supported with JDBC |
| Teradata | `teradata` | Supported with JDBC |

### Query Pushdown Optimization

When you query a federated table, Spark does not always pull all data into the
Lakehouse. It pushes **filters, projections, and aggregations** down to the
external database, reducing data transfer.

```
  Query Pushdown Example:
  =======================

  User query:
    SELECT region, SUM(revenue)
    FROM ext_postgres.sales.orders
    WHERE order_date >= '2024-01-01'
    GROUP BY region;

  WITHOUT pushdown:
    1. Spark reads ALL rows from PostgreSQL        <-- slow, expensive
    2. Spark filters order_date >= '2024-01-01'
    3. Spark groups by region and sums revenue
    Result: Millions of rows transferred

  WITH pushdown (what Lakehouse Federation does):
    1. Spark sends optimized query to PostgreSQL:
       "SELECT region, SUM(revenue)
        FROM sales.orders
        WHERE order_date >= '2024-01-01'
        GROUP BY region"
    2. PostgreSQL processes the query
    3. Only the aggregated result (4 rows) is returned to Spark
    Result: Minimal data transferred
```

You can verify pushdown by examining the query plan:

```sql
EXPLAIN SELECT region, SUM(revenue)
FROM ext_postgres.sales.orders
WHERE order_date >= '2024-01-01'
GROUP BY region;

-- Look for "PushedFilters" and "PushedAggregates" in the plan
```

### Use Cases

**Use Case 1: Real-Time Access**
```
  Query live operational data without waiting for ETL to run.
  Example: Join live customer data in PostgreSQL with analytics in Delta.

  SELECT d.customer_segment, p.current_balance
  FROM delta_catalog.analytics.customer_segments d
  JOIN ext_postgres.operations.accounts p
    ON d.customer_id = p.customer_id;
```

**Use Case 2: Migration Bridge**
```
  During migration from a legacy database to the Lakehouse, query both
  systems through a single interface.

  -- Migrate data incrementally
  INSERT INTO delta_catalog.bronze.orders
  SELECT * FROM ext_postgres.legacy.orders
  WHERE updated_at > (SELECT MAX(updated_at) FROM delta_catalog.bronze.orders);
```

**Use Case 3: Cross-Platform Analytics**
```
  Combine data from multiple platforms without building separate ETL for each.

  SELECT
      s.customer_id,
      s.snowflake_metric,
      p.postgres_metric,
      d.delta_metric
  FROM ext_snowflake.analytics.metrics s
  JOIN ext_postgres.operations.metrics p ON s.customer_id = p.customer_id
  JOIN delta_catalog.gold.metrics d ON s.customer_id = d.customer_id;
```

**Use Case 4: Data Validation**
```
  Compare source system data with migrated data to validate ETL correctness.

  SELECT
      'source' AS system,
      COUNT(*) AS row_count,
      SUM(amount) AS total_amount
  FROM ext_postgres.sales.orders
  UNION ALL
  SELECT
      'lakehouse' AS system,
      COUNT(*) AS row_count,
      SUM(amount) AS total_amount
  FROM delta_catalog.bronze.orders;
```

### Limitations

| Limitation | Impact | Workaround |
|-----------|--------|------------|
| Read-only | Cannot write to external tables | Use native database tools for writes |
| Performance | Slower than native Delta queries | Use pushdown; cache frequently accessed data |
| Not all SQL | Some Spark SQL features may not push down | Check EXPLAIN plan; simplify complex queries |
| Network latency | External DB must be reachable | Use VPC peering or private endpoints |
| Connection limits | Concurrent queries limited by external DB | Monitor connection pools |
| No streaming | Cannot use as streaming source | Use CDC tools (Debezium, etc.) for streaming |
| Schema sync | External schema changes not auto-detected | Refresh foreign catalog periodically |

### Governance Benefits

Federated tables benefit from Unity Catalog governance:

- **Access control**: GRANT/REVOKE on foreign catalogs, schemas, and tables
- **Audit logging**: All queries to federated tables are logged
- **Discovery**: External tables appear in the data catalog alongside Delta tables
- **Lineage**: Federated queries are tracked in the lineage graph

---

## Hands-On Walkthrough

Open the companion notebook `06-lakehouse-federation_notebook.py` and follow
along. The notebook covers:

1. SQL for creating connections and foreign catalogs (templates)
2. Querying federated tables
3. DESCRIBE EXTENDED for foreign tables
4. Query pushdown verification
5. Cross-platform query patterns

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Network connectivity | VPC Peering, PrivateLink | VNet Peering, Private Link | VPC Peering, Private Service Connect |
| Credential storage | Unity Catalog secrets | Unity Catalog secrets | Unity Catalog secrets |
| Supported sources | All listed above | All listed above | All listed above |
| Native integration | Redshift (optimized) | SQL Server (optimized) | BigQuery (optimized) |
| Firewall config | Security group rules | NSG rules | Firewall rules |

---

## Certification Tip

For Databricks Certified Data Engineer exams:

- Lakehouse Federation uses **CONNECTION** and **FOREIGN CATALOG** objects
- Federated tables are **read-only** — you cannot write to them
- **Query pushdown** sends filters and aggregations to the external database
- Federated tables appear in Unity Catalog and follow the same **GRANT/REVOKE**
  model as native tables
- Federation is for **real-time access** to external data without copying
- Use cases: migration bridge, cross-platform analytics, data validation
- Know the supported databases: PostgreSQL, MySQL, Snowflake, BigQuery, etc.

---

## Key Takeaways

1. **Lakehouse Federation** lets you query external databases through Unity Catalog
   without copying data.
2. Two objects: **CONNECTION** (credentials) and **FOREIGN CATALOG** (namespace
   mapping).
3. **Query pushdown** optimizes performance by executing filters and aggregations
   in the external database.
4. Federated tables are **read-only** but fully governed by Unity Catalog access
   controls.
5. Key use cases: **real-time access**, **migration**, **cross-platform analytics**,
   and **data validation**.
6. All supported databases (PostgreSQL, MySQL, Snowflake, BigQuery, etc.) work
   with the same SQL patterns.
7. Limitations include performance overhead, read-only access, and network latency
   considerations.

---

## Next Steps

You have completed Module 08 — Governance & Security. Review the key concepts
from all six topics and proceed to Module 09 — Performance Tuning & Optimization.
