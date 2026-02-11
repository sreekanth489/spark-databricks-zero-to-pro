# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakehouse Federation
# MAGIC > Module 08 — Topic 06 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - SQL for creating connections and foreign catalogs (configuration templates)
# MAGIC - Querying federated tables and understanding the query model
# MAGIC - DESCRIBE EXTENDED for foreign table metadata
# MAGIC - Query pushdown optimization verification
# MAGIC - Cross-platform query patterns
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog for actual federation
# MAGIC - Community Edition users: all SQL is shown as reference templates
# MAGIC - External database connectivity requires network access from Databricks
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Understanding the Federation Model
# MAGIC
# MAGIC Lakehouse Federation introduces two new Unity Catalog objects:
# MAGIC
# MAGIC 1. **CONNECTION** — stores connection details for an external database
# MAGIC 2. **FOREIGN CATALOG** — maps external database schemas into Unity Catalog
# MAGIC
# MAGIC Together, they let you query external data as if it were local.

# COMMAND ----------

# Visualize the federation architecture
print("=" * 70)
print("LAKEHOUSE FEDERATION ARCHITECTURE")
print("=" * 70)
print("""
  +--------------------------------------------------+
  |              Unity Catalog Namespace              |
  |                                                   |
  |  Native Catalogs      Foreign Catalogs            |
  |  +-----------+        +-----------+ +-----------+ |
  |  | prod      |        | ext_pg    | | ext_sf    | |
  |  | .sales    |        | .public   | | .analytics| |
  |  | .analytics|        | .orders   | | .metrics  | |
  |  +-----------+        +-----+-----+ +-----+-----+ |
  |                             |              |       |
  +--------------------------------------------------+
                                |              |
                          CONNECTION      CONNECTION
                                |              |
                          +-----+-----+  +-----+-----+
                          | PostgreSQL |  | Snowflake  |
                          | (external) |  | (external) |
                          +-----------+  +-----------+
""")
print("Key points:")
print("  - Foreign catalogs appear alongside native catalogs in Unity Catalog")
print("  - Same GRANT/REVOKE permissions model applies")
print("  - Queries use the same three-level namespace: catalog.schema.table")
print("  - Read-only access (cannot write to external tables)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Creating Connections
# MAGIC
# MAGIC A CONNECTION stores the credentials and endpoint information needed
# MAGIC to access an external database. Only admins typically create connections.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- CONNECTION TEMPLATES FOR SUPPORTED DATABASES
# MAGIC -- (requires Unity Catalog and network access to external DB)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- PostgreSQL Connection
# MAGIC -- CREATE CONNECTION pg_connection
# MAGIC -- TYPE postgresql
# MAGIC -- OPTIONS (
# MAGIC --     host 'db.example.com',
# MAGIC --     port '5432',
# MAGIC --     user 'readonly_user',
# MAGIC --     password secret('scope', 'pg_password')
# MAGIC -- );
# MAGIC --
# MAGIC -- MySQL Connection
# MAGIC -- CREATE CONNECTION mysql_connection
# MAGIC -- TYPE mysql
# MAGIC -- OPTIONS (
# MAGIC --     host 'mysql.example.com',
# MAGIC --     port '3306',
# MAGIC --     user 'readonly_user',
# MAGIC --     password secret('scope', 'mysql_password')
# MAGIC -- );
# MAGIC --
# MAGIC -- SQL Server Connection
# MAGIC -- CREATE CONNECTION sqlserver_connection
# MAGIC -- TYPE sqlserver
# MAGIC -- OPTIONS (
# MAGIC --     host 'sqlserver.example.com',
# MAGIC --     port '1433',
# MAGIC --     user 'readonly_user',
# MAGIC --     password secret('scope', 'sqlserver_password'),
# MAGIC --     database 'production_db'
# MAGIC -- );
# MAGIC --
# MAGIC -- Snowflake Connection
# MAGIC -- CREATE CONNECTION snowflake_connection
# MAGIC -- TYPE snowflake
# MAGIC -- OPTIONS (
# MAGIC --     host 'account.snowflakecomputing.com',
# MAGIC --     user 'readonly_user',
# MAGIC --     password secret('scope', 'snowflake_password'),
# MAGIC --     sfWarehouse 'COMPUTE_WH'
# MAGIC -- );
# MAGIC --
# MAGIC -- Google BigQuery Connection
# MAGIC -- CREATE CONNECTION bigquery_connection
# MAGIC -- TYPE bigquery
# MAGIC -- OPTIONS (
# MAGIC --     GoogleServiceAccountKeyJson secret('scope', 'bq_service_account')
# MAGIC -- );
# MAGIC --
# MAGIC -- Amazon Redshift Connection
# MAGIC -- CREATE CONNECTION redshift_connection
# MAGIC -- TYPE redshift
# MAGIC -- OPTIONS (
# MAGIC --     host 'cluster.region.redshift.amazonaws.com',
# MAGIC --     port '5439',
# MAGIC --     user 'readonly_user',
# MAGIC --     password secret('scope', 'redshift_password'),
# MAGIC --     database 'analytics_db'
# MAGIC -- );
# MAGIC
# MAGIC SELECT 'See comments above for CONNECTION templates' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Connection Management Commands

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- CONNECTION MANAGEMENT (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- List all connections
# MAGIC -- SHOW CONNECTIONS;
# MAGIC --
# MAGIC -- Describe a connection (shows type, host, but not password)
# MAGIC -- DESCRIBE CONNECTION pg_connection;
# MAGIC --
# MAGIC -- Drop a connection
# MAGIC -- DROP CONNECTION pg_connection;
# MAGIC --
# MAGIC -- Grant permission to use a connection
# MAGIC -- GRANT USE CONNECTION ON CONNECTION pg_connection TO `data_engineers`;
# MAGIC --
# MAGIC -- Best practice: Use Databricks secrets for passwords
# MAGIC -- Never hardcode passwords in connection definitions
# MAGIC -- password secret('secret_scope', 'secret_key')
# MAGIC
# MAGIC SELECT 'See comments above for connection management' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Creating Foreign Catalogs
# MAGIC
# MAGIC A FOREIGN CATALOG maps an external database into the Unity Catalog
# MAGIC namespace. All schemas and tables in the external database become
# MAGIC discoverable and queryable.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- FOREIGN CATALOG TEMPLATES (requires Unity Catalog + CONNECTION)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Create a foreign catalog from a PostgreSQL connection
# MAGIC -- CREATE FOREIGN CATALOG ext_postgres
# MAGIC -- USING CONNECTION pg_connection
# MAGIC -- COMMENT 'Federated access to production PostgreSQL database';
# MAGIC --
# MAGIC -- Create a foreign catalog from a Snowflake connection
# MAGIC -- CREATE FOREIGN CATALOG ext_snowflake
# MAGIC -- USING CONNECTION snowflake_connection
# MAGIC -- OPTIONS (database 'ANALYTICS_DB')
# MAGIC -- COMMENT 'Federated access to Snowflake analytics warehouse';
# MAGIC --
# MAGIC -- Create a foreign catalog from BigQuery
# MAGIC -- CREATE FOREIGN CATALOG ext_bigquery
# MAGIC -- USING CONNECTION bigquery_connection
# MAGIC -- OPTIONS (project 'my-gcp-project')
# MAGIC -- COMMENT 'Federated access to BigQuery datasets';
# MAGIC --
# MAGIC -- List all catalogs (foreign catalogs appear alongside native ones)
# MAGIC -- SHOW CATALOGS;
# MAGIC --
# MAGIC -- Browse schemas in a foreign catalog
# MAGIC -- SHOW SCHEMAS IN ext_postgres;
# MAGIC --
# MAGIC -- Browse tables in a foreign schema
# MAGIC -- SHOW TABLES IN ext_postgres.public;
# MAGIC --
# MAGIC -- Drop a foreign catalog
# MAGIC -- DROP CATALOG ext_postgres;
# MAGIC
# MAGIC SELECT 'See comments above for FOREIGN CATALOG templates' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Querying Federated Tables
# MAGIC
# MAGIC Once a foreign catalog is set up, you query external tables exactly
# MAGIC like native tables, using the standard three-level namespace.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- QUERYING FEDERATED TABLES (requires active federation setup)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Simple query on an external PostgreSQL table
# MAGIC -- SELECT * FROM ext_postgres.public.customers LIMIT 100;
# MAGIC --
# MAGIC -- Filtered query (filter pushes down to PostgreSQL)
# MAGIC -- SELECT customer_id, customer_name, region
# MAGIC -- FROM ext_postgres.public.customers
# MAGIC -- WHERE region = 'North' AND status = 'active';
# MAGIC --
# MAGIC -- Aggregation query (pushes down if supported)
# MAGIC -- SELECT region, COUNT(*) as customer_count, AVG(lifetime_value) as avg_ltv
# MAGIC -- FROM ext_postgres.public.customers
# MAGIC -- GROUP BY region;
# MAGIC --
# MAGIC -- Join between native Delta and federated tables
# MAGIC -- SELECT
# MAGIC --     d.customer_segment,
# MAGIC --     d.segment_description,
# MAGIC --     p.customer_name,
# MAGIC --     p.current_balance
# MAGIC -- FROM prod.analytics.customer_segments d
# MAGIC -- JOIN ext_postgres.operations.accounts p
# MAGIC --     ON d.customer_id = p.customer_id
# MAGIC -- WHERE d.customer_segment = 'premium';
# MAGIC
# MAGIC SELECT 'See comments above for federated query examples' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Simulated Federation with Local Data
# MAGIC
# MAGIC Since federation requires external databases, we simulate the pattern
# MAGIC using local tables to demonstrate the query patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS m08_federation_demo
# MAGIC COMMENT 'Lakehouse Federation demonstration';
# MAGIC USE m08_federation_demo;

# COMMAND ----------

# Simulate "external" PostgreSQL data (operations system)
from pyspark.sql import Row
from datetime import date, timedelta
import random

random.seed(42)

# Simulated external: operational customers table
ext_customers = [
    Row(customer_id=i,
        customer_name=f"Customer_{i:04d}",
        email=f"customer_{i}@example.com",
        region=random.choice(["North", "South", "East", "West"]),
        status=random.choice(["active", "active", "active", "inactive"]),
        current_balance=round(random.uniform(100, 10000), 2),
        last_login=date(2024, 1, 1) + timedelta(days=random.randint(0, 60)))
    for i in range(1, 101)
]
ext_cust_df = spark.createDataFrame(ext_customers)
ext_cust_df.write.mode("overwrite").saveAsTable("m08_federation_demo.ext_pg_customers")

# Simulated external: operational orders table
ext_orders = []
for i in range(1, 201):
    ext_orders.append(Row(
        order_id=i,
        customer_id=random.randint(1, 100),
        product_name=f"Product_{random.randint(1, 50):03d}",
        amount=round(random.uniform(10, 500), 2),
        order_date=date(2024, 1, 1) + timedelta(days=random.randint(0, 60)),
        fulfillment_status=random.choice(["shipped", "delivered", "processing", "returned"])
    ))
ext_orders_df = spark.createDataFrame(ext_orders)
ext_orders_df.write.mode("overwrite").saveAsTable("m08_federation_demo.ext_pg_orders")

# Native Delta: analytics customer segments
segments = [
    Row(customer_id=i,
        customer_segment=random.choice(["premium", "standard", "basic"]),
        predicted_churn_risk=round(random.uniform(0, 1), 3),
        lifetime_value=round(random.uniform(500, 50000), 2))
    for i in range(1, 101)
]
seg_df = spark.createDataFrame(segments)
seg_df.write.mode("overwrite").saveAsTable("m08_federation_demo.customer_segments")

print("Created simulated tables:")
print("  - ext_pg_customers (100 rows) — simulates PostgreSQL operational data")
print("  - ext_pg_orders (200 rows) — simulates PostgreSQL order data")
print("  - customer_segments (100 rows) — native Delta analytics data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5a. Simple Federated Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulates: SELECT * FROM ext_postgres.public.customers
# MAGIC -- In production, this query would hit PostgreSQL directly
# MAGIC SELECT * FROM m08_federation_demo.ext_pg_customers
# MAGIC WHERE status = 'active'
# MAGIC ORDER BY current_balance DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5b. Aggregation with Pushdown

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulates: Aggregation query pushed down to external database
# MAGIC -- In production, PostgreSQL/Snowflake would compute this and return only 4 rows
# MAGIC SELECT
# MAGIC     region,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(current_balance), 2) AS avg_balance,
# MAGIC     ROUND(SUM(current_balance), 2) AS total_balance
# MAGIC FROM m08_federation_demo.ext_pg_customers
# MAGIC WHERE status = 'active'
# MAGIC GROUP BY region
# MAGIC ORDER BY total_balance DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5c. Cross-Platform Join (Delta + "External")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulates joining native Delta table with external (federated) table
# MAGIC -- This is the core use case: combining analytics with live operational data
# MAGIC SELECT
# MAGIC     seg.customer_segment,
# MAGIC     COUNT(*) AS customer_count,
# MAGIC     ROUND(AVG(ext.current_balance), 2) AS avg_current_balance,
# MAGIC     ROUND(AVG(seg.lifetime_value), 2) AS avg_lifetime_value,
# MAGIC     ROUND(AVG(seg.predicted_churn_risk), 3) AS avg_churn_risk
# MAGIC FROM m08_federation_demo.customer_segments seg
# MAGIC JOIN m08_federation_demo.ext_pg_customers ext
# MAGIC     ON seg.customer_id = ext.customer_id
# MAGIC WHERE ext.status = 'active'
# MAGIC GROUP BY seg.customer_segment
# MAGIC ORDER BY avg_lifetime_value DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5d. Migration Pattern — Incremental Load

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Simulates the incremental migration pattern:
# MAGIC -- Copy new/updated records from external DB to Delta Lake
# MAGIC
# MAGIC -- Step 1: Check what we have in Delta already
# MAGIC SELECT MAX(order_date) AS latest_in_delta
# MAGIC FROM m08_federation_demo.ext_pg_orders;
# MAGIC -- In production, this would be your Delta target table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: Fetch only new records from external DB
# MAGIC -- In production: SELECT ... FROM ext_postgres.operations.orders WHERE order_date > latest_in_delta
# MAGIC SELECT COUNT(*) AS new_records,
# MAGIC        MIN(order_date) AS earliest_new,
# MAGIC        MAX(order_date) AS latest_new
# MAGIC FROM m08_federation_demo.ext_pg_orders
# MAGIC WHERE order_date >= '2024-02-15';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: DESCRIBE EXTENDED for Foreign Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- INSPECTING FOREIGN TABLES (requires active federation)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- DESCRIBE EXTENDED shows connection info for foreign tables
# MAGIC -- DESCRIBE TABLE EXTENDED ext_postgres.public.customers;
# MAGIC --
# MAGIC -- Expected output includes:
# MAGIC -- - Column names and types (mapped from PostgreSQL types to Spark types)
# MAGIC -- - Catalog: ext_postgres (foreign)
# MAGIC -- - Type: FOREIGN
# MAGIC -- - Provider: postgresql
# MAGIC -- - Connection: pg_connection
# MAGIC --
# MAGIC -- For our simulated table:
# MAGIC DESCRIBE TABLE EXTENDED m08_federation_demo.ext_pg_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Query Pushdown — What Gets Pushed Down?

# COMMAND ----------

# Demonstrate what gets pushed down to external databases
print("=" * 70)
print("QUERY PUSHDOWN REFERENCE")
print("=" * 70)
print()

pushdown_ops = [
    ("Filter (WHERE)",          "YES", "WHERE status = 'active'"),
    ("Projection (SELECT cols)","YES", "SELECT col1, col2 (not SELECT *)"),
    ("Aggregation (GROUP BY)",  "YES", "COUNT, SUM, AVG, MIN, MAX"),
    ("Sorting (ORDER BY)",      "Partial", "Pushed down with LIMIT"),
    ("LIMIT",                   "YES", "LIMIT 100"),
    ("JOIN (two external)",     "Partial", "Same-connection joins may push down"),
    ("JOIN (external + Delta)", "NO",  "Delta data is local; join happens in Spark"),
    ("Subqueries",              "Partial", "Simple subqueries may push down"),
    ("Window functions",        "NO",  "Computed in Spark"),
    ("UDFs (Python/Scala)",     "NO",  "Always computed in Spark"),
    ("LIKE / REGEXP",           "YES", "Pattern matching pushes down"),
    ("CAST / type conversion",  "Partial", "Depends on type compatibility"),
]

print(f"{'Operation':<30s} {'Pushdown?':<10s} {'Example/Notes'}")
print("-" * 80)
for op, pushdown, notes in pushdown_ops:
    print(f"{op:<30s} {pushdown:<10s} {notes}")

print()
print("How to verify pushdown:")
print("  EXPLAIN SELECT ... FROM ext_postgres.public.customers WHERE ...");
print("  Look for 'PushedFilters' and 'PushedAggregates' in the plan")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Access Control on Foreign Catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- ACCESS CONTROL ON FOREIGN CATALOGS (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Foreign catalogs use the same GRANT/REVOKE model as native catalogs
# MAGIC --
# MAGIC -- Grant access to browse the foreign catalog
# MAGIC -- GRANT USE CATALOG ON CATALOG ext_postgres TO `data_analysts`;
# MAGIC -- GRANT USE SCHEMA ON SCHEMA ext_postgres.public TO `data_analysts`;
# MAGIC --
# MAGIC -- Grant SELECT on specific tables
# MAGIC -- GRANT SELECT ON TABLE ext_postgres.public.customers TO `data_analysts`;
# MAGIC --
# MAGIC -- Grant SELECT on all tables in a schema
# MAGIC -- GRANT SELECT ON SCHEMA ext_postgres.public TO `data_analysts`;
# MAGIC --
# MAGIC -- Important: These grants control who can query through federation.
# MAGIC -- They do NOT affect the underlying database's own access controls.
# MAGIC -- Both Unity Catalog AND the external DB must allow the access.
# MAGIC --
# MAGIC -- Grant permission to create foreign catalogs
# MAGIC -- GRANT CREATE FOREIGN CATALOG ON METASTORE TO `platform_admins`;
# MAGIC --
# MAGIC -- Grant permission to use connections
# MAGIC -- GRANT USE CONNECTION ON CONNECTION pg_connection TO `data_engineers`;
# MAGIC
# MAGIC SELECT 'See comments above for access control on foreign catalogs' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Practical Patterns and Best Practices

# COMMAND ----------

# Print best practices for federation
print("=" * 70)
print("LAKEHOUSE FEDERATION BEST PRACTICES")
print("=" * 70)
print()
print("1. SECURITY")
print("   - Use Databricks secrets for connection passwords (never hardcode)")
print("   - Create read-only database users for federation connections")
print("   - Apply Unity Catalog GRANT to control who can query federated tables")
print("   - Use network security (VPC peering, Private Link) for connectivity")
print()
print("2. PERFORMANCE")
print("   - Always filter in the WHERE clause to enable pushdown")
print("   - Select only needed columns (avoid SELECT *)")
print("   - Use EXPLAIN to verify pushdown is happening")
print("   - Cache frequently accessed external data as Delta tables")
print("   - Set connection timeouts to avoid hanging queries")
print()
print("3. GOVERNANCE")
print("   - Document all connections and their purposes")
print("   - Regularly review who has access to foreign catalogs")
print("   - Monitor query patterns via audit logs")
print("   - Refresh foreign catalogs when external schemas change")
print()
print("4. ARCHITECTURE")
print("   - Use federation for real-time access; use ETL for analytics at scale")
print("   - Do NOT replace ETL pipelines with federation for heavy analytics")
print("   - Ideal for: validation, migration, real-time lookups, ad-hoc queries")
print("   - Not ideal for: heavy joins across millions of rows, ML training data")

# COMMAND ----------

# Decision guide
print("=" * 70)
print("DECISION GUIDE: Federation vs ETL")
print("=" * 70)
print()
print("Use FEDERATION when:")
print("  - You need real-time access to external data")
print("  - The query is simple (filter + aggregate on a single external table)")
print("  - Data volumes are small to medium (< 1M rows per query)")
print("  - You are validating migrated data against the source")
print("  - You need a migration bridge (query old and new systems together)")
print()
print("Use ETL (copy to Delta) when:")
print("  - You run heavy analytical queries repeatedly")
print("  - You need to join large external datasets")
print("  - You need ML training data at scale")
print("  - You need data versioning (Delta time travel)")
print("  - Network latency to the external DB is high")
print("  - The external DB cannot handle additional query load")
print()
print("Hybrid approach (recommended):")
print("  - Use federation for real-time lookups and validation")
print("  - Use ETL for analytics-grade copies in the Lakehouse")
print("  - Both governed by the same Unity Catalog access controls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Full Connection Template Reference

# COMMAND ----------

# Print a consolidated connection template reference
templates = {
    "PostgreSQL": {
        "type": "postgresql",
        "options": "host, port, user, password",
        "example_host": "db.example.com:5432",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_pg USING CONNECTION pg_conn",
    },
    "MySQL": {
        "type": "mysql",
        "options": "host, port, user, password",
        "example_host": "mysql.example.com:3306",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_mysql USING CONNECTION mysql_conn",
    },
    "SQL Server": {
        "type": "sqlserver",
        "options": "host, port, user, password, database",
        "example_host": "sqlserver.example.com:1433",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_mssql USING CONNECTION mssql_conn",
    },
    "Snowflake": {
        "type": "snowflake",
        "options": "host, user, password, sfWarehouse",
        "example_host": "account.snowflakecomputing.com",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_sf USING CONNECTION sf_conn OPTIONS (database 'DB')",
    },
    "BigQuery": {
        "type": "bigquery",
        "options": "GoogleServiceAccountKeyJson",
        "example_host": "bigquery.googleapis.com",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_bq USING CONNECTION bq_conn OPTIONS (project 'proj')",
    },
    "Redshift": {
        "type": "redshift",
        "options": "host, port, user, password, database",
        "example_host": "cluster.region.redshift.amazonaws.com:5439",
        "foreign_catalog": "CREATE FOREIGN CATALOG ext_rs USING CONNECTION rs_conn",
    },
}

print("=" * 80)
print("CONNECTION TEMPLATE QUICK REFERENCE")
print("=" * 80)
for db_name, info in templates.items():
    print(f"\n--- {db_name} ---")
    print(f"  Type:            {info['type']}")
    print(f"  Options:         {info['options']}")
    print(f"  Example Host:    {info['example_host']}")
    print(f"  Foreign Catalog: {info['foreign_catalog']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS m08_federation_demo.ext_pg_customers;
# MAGIC DROP TABLE IF EXISTS m08_federation_demo.ext_pg_orders;
# MAGIC DROP TABLE IF EXISTS m08_federation_demo.customer_segments;
# MAGIC DROP DATABASE IF EXISTS m08_federation_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. Lakehouse Federation: query external DBs through Unity Catalog")
print("  2. Two objects: CONNECTION (credentials) + FOREIGN CATALOG (namespace)")
print("  3. Query pushdown sends filters/aggregations to external DB")
print("  4. Foreign tables are read-only but fully governed by UC")
print("  5. Use cases: real-time access, migration, cross-platform analytics")
print("  6. Supported: PostgreSQL, MySQL, SQL Server, Snowflake, BigQuery, Redshift")
print("  7. Use federation for real-time; use ETL for heavy analytics")
print()
print("Module 08 — Governance & Security: COMPLETE")
print("Proceed to Module 09 — Performance Tuning & Optimization")
