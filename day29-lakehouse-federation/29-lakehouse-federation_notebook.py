# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 29: Lakehouse Federation — Hands-On Lab
# MAGIC
# MAGIC **Objective**: Query external databases live from Databricks using Unity Catalog Lakehouse Federation
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Understand the three Federation objects: Connection → Foreign Catalog → Foreign Table
# MAGIC 2. Create a Connection to an external source
# MAGIC 3. Create a Foreign Catalog and explore auto-discovered schemas
# MAGIC 4. Query foreign tables with standard SQL
# MAGIC 5. Run cross-source joins (foreign + local Delta)
# MAGIC 6. Govern foreign catalogs with GRANT/REVOKE
# MAGIC 7. Inspect federation metadata via information_schema
# MAGIC
# MAGIC **Lakehouse Federation — One Governance Model for All Data**:
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                                                                      │
# MAGIC │   External Sources              Unity Catalog         Your Query    │
# MAGIC │                                                                      │
# MAGIC │   Snowflake  ──────────┐                                            │
# MAGIC │   MySQL      ──────────┤       ┌──────────────┐                    │
# MAGIC │   PostgreSQL ──────────┼──────▶│  Foreign     │◀── SELECT *        │
# MAGIC │   Redshift   ──────────┤       │  Catalog     │    FROM snowflake  │
# MAGIC │   SQL Server ──────────┤       │  (UC-governed│    _catalog.fin    │
# MAGIC │   Synapse    ──────────┘       │  )           │    ance.revenue    │
# MAGIC │                                └──────────────┘                    │
# MAGIC │                                                                      │
# MAGIC │   No data copy. No ETL. Same GRANT/REVOKE. Pushdown optimization.  │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Key concept — three UC objects**:
# MAGIC ```
# MAGIC  CONNECTION       → stores credentials to reach the external system
# MAGIC  FOREIGN CATALOG  → maps a remote database into UC namespace
# MAGIC  FOREIGN TABLE    → auto-discovered, queryable via SQL (reads live from source)
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS/Azure with Unity Catalog (DBR 13.0+)
# MAGIC
# MAGIC **Prerequisites**: Metastore Admin role to create connections and foreign catalogs

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Understand Connections
# MAGIC
# MAGIC A **Connection** is the first object — it stores credentials to reach an external system.
# MAGIC Created at the **metastore level** (not inside a catalog).
# MAGIC
# MAGIC Only a **Metastore Admin** can create connections.
# MAGIC Once created, you can grant `CREATE FOREIGN CATALOG` on the connection to other groups.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List existing connections in this metastore
# MAGIC SHOW CONNECTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a Connection (Snowflake example)
# MAGIC
# MAGIC Store credentials in Databricks Secrets first, then reference them in the connection:
# MAGIC
# MAGIC ```sql
# MAGIC -- Requires Metastore Admin
# MAGIC CREATE CONNECTION snowflake_prod
# MAGIC   TYPE SNOWFLAKE
# MAGIC   OPTIONS (
# MAGIC     host        'myorg.snowflakecomputing.com',
# MAGIC     port        '443',
# MAGIC     sfWarehouse 'COMPUTE_WH',
# MAGIC     user        'databricks_federation_user',
# MAGIC     password    secret('federation-secrets', 'snowflake-password')
# MAGIC   )
# MAGIC   COMMENT 'Snowflake PROD — Lakehouse Federation';
# MAGIC ```
# MAGIC
# MAGIC Other connection types follow the same pattern:
# MAGIC
# MAGIC ```sql
# MAGIC -- MySQL
# MAGIC CREATE CONNECTION mysql_app
# MAGIC   TYPE MYSQL
# MAGIC   OPTIONS (host 'mysql.internal.com', port '3306',
# MAGIC            user 'ro_user', password secret('scope', 'key'));
# MAGIC
# MAGIC -- PostgreSQL
# MAGIC CREATE CONNECTION postgres_analytics
# MAGIC   TYPE POSTGRESQL
# MAGIC   OPTIONS (host 'pg.internal.com', port '5432',
# MAGIC            user 'ro_user', password secret('scope', 'key'));
# MAGIC
# MAGIC -- SQL Server
# MAGIC CREATE CONNECTION sqlserver_erp
# MAGIC   TYPE SQLSERVER
# MAGIC   OPTIONS (host 'sqlserver.internal.com', port '1433',
# MAGIC            user 'ro_user', password secret('scope', 'key'));
# MAGIC
# MAGIC -- Redshift (AWS only)
# MAGIC CREATE CONNECTION redshift_mkt
# MAGIC   TYPE REDSHIFT
# MAGIC   OPTIONS (host 'cluster.abc.us-east-1.redshift.amazonaws.com', port '5439',
# MAGIC            user 'ro_user', password secret('scope', 'key'));
# MAGIC
# MAGIC -- Azure Synapse
# MAGIC CREATE CONNECTION synapse_dw
# MAGIC   TYPE SYNAPSE
# MAGIC   OPTIONS (host 'myserver.sql.azuresynapse.net', port '1433',
# MAGIC            user 'ro_user', password secret('scope', 'key'));
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create a Foreign Catalog
# MAGIC
# MAGIC A **Foreign Catalog** maps a remote database into UC's 3-level namespace.
# MAGIC Schemas and tables are **auto-discovered** from the source.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Map Snowflake database PROD_DB into UC
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS snowflake_prod_catalog
# MAGIC   USING CONNECTION snowflake_prod
# MAGIC   OPTIONS (database 'PROD_DB')
# MAGIC   COMMENT 'Snowflake PROD_DB — federated';
# MAGIC
# MAGIC -- Map PostgreSQL database analytics_db into UC
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS postgres_catalog
# MAGIC   USING CONNECTION postgres_analytics
# MAGIC   OPTIONS (database 'analytics_db');
# MAGIC
# MAGIC -- Map MySQL database app_production into UC
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS mysql_catalog
# MAGIC   USING CONNECTION mysql_app
# MAGIC   OPTIONS (database 'app_production');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC Once created, browse the foreign catalog just like any other UC catalog:
# MAGIC
# MAGIC ```sql
# MAGIC -- List schemas auto-discovered from Snowflake
# MAGIC SHOW SCHEMAS IN snowflake_prod_catalog;
# MAGIC
# MAGIC -- List tables auto-discovered in a schema
# MAGIC SHOW TABLES IN snowflake_prod_catalog.finance;
# MAGIC
# MAGIC -- Describe a foreign table's schema
# MAGIC DESCRIBE TABLE snowflake_prod_catalog.finance.revenue;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Query Foreign Tables
# MAGIC
# MAGIC Foreign tables are queried with standard SQL — no special syntax.
# MAGIC Databricks translates your query, pushes it to the source, returns results.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Query a Snowflake table directly (no data copy, live from Snowflake)
# MAGIC SELECT region, product_category, SUM(revenue_usd) AS total_revenue
# MAGIC FROM snowflake_prod_catalog.finance.revenue
# MAGIC WHERE year = 2024
# MAGIC GROUP BY region, product_category
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC -- Pushdown in action: the WHERE + GROUP BY is executed IN Snowflake
# MAGIC -- Only the aggregated result set travels back to Databricks
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Explain Plan — See Pushdown
# MAGIC
# MAGIC Use `EXPLAIN` to verify pushdown is happening:
# MAGIC
# MAGIC ```sql
# MAGIC EXPLAIN
# MAGIC SELECT region, SUM(revenue_usd)
# MAGIC FROM snowflake_prod_catalog.finance.revenue
# MAGIC WHERE year = 2024
# MAGIC GROUP BY region;
# MAGIC
# MAGIC -- Look for: JDBCScan with pushedFilters and pushedAggregates
# MAGIC -- This confirms the WHERE clause and GROUP BY were sent to Snowflake
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Cross-Source Joins
# MAGIC
# MAGIC The real power of Lakehouse Federation: join across systems with one SQL query.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Join Snowflake (finance) + PostgreSQL (analytics) + local Delta (ML scores)
# MAGIC -- Three different systems, one SQL query
# MAGIC SELECT
# MAGIC   m.customer_id,
# MAGIC   m.signup_date,
# MAGIC   p.lifetime_value,
# MAGIC   d.churn_probability,
# MAGIC   s.revenue_ytd
# MAGIC FROM mysql_catalog.app_production.customers m
# MAGIC JOIN postgres_catalog.analytics_db.customer_ltv p
# MAGIC   ON m.customer_id = p.customer_id
# MAGIC JOIN prod_catalog.gold.churn_model_scores d
# MAGIC   ON m.customer_id = d.customer_id
# MAGIC JOIN snowflake_prod_catalog.finance.customer_revenue s
# MAGIC   ON m.customer_id = s.customer_id
# MAGIC WHERE p.lifetime_value > 1000
# MAGIC   AND d.churn_probability > 0.7
# MAGIC ORDER BY d.churn_probability DESC
# MAGIC LIMIT 50;
# MAGIC
# MAGIC -- Result: high-value, at-risk customers — combining live operational data
# MAGIC -- from MySQL, analytics from PostgreSQL, ML scores from Delta Gold,
# MAGIC -- and finance data from Snowflake — NO data copy, NO pipelines
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Govern Foreign Catalogs
# MAGIC
# MAGIC Foreign catalogs use the **same GRANT/REVOKE model** as regular UC catalogs.
# MAGIC Users granted access through UC do NOT need credentials to the source system.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Grant finance team access to Snowflake data via UC
# MAGIC -- They never touch Snowflake directly — UC is the single access point
# MAGIC GRANT USE CATALOG ON CATALOG snowflake_prod_catalog  TO `finance_team`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA snowflake_prod_catalog.finance TO `finance_team`;
# MAGIC GRANT SELECT      ON TABLE snowflake_prod_catalog.finance.revenue TO `finance_team`;
# MAGIC
# MAGIC -- Grant analytics team access to PostgreSQL via UC
# MAGIC GRANT USE CATALOG ON CATALOG postgres_catalog         TO `analytics_team`;
# MAGIC GRANT USE SCHEMA  ON SCHEMA postgres_catalog.analytics_db TO `analytics_team`;
# MAGIC GRANT SELECT      ON SCHEMA postgres_catalog.analytics_db TO `analytics_team`;
# MAGIC
# MAGIC -- Show grants on foreign catalog
# MAGIC SHOW GRANTS ON CATALOG snowflake_prod_catalog;
# MAGIC
# MAGIC -- Revoke (same syntax as regular tables)
# MAGIC REVOKE SELECT ON TABLE snowflake_prod_catalog.finance.revenue FROM `finance_team`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC **Key governance benefit**: When an employee leaves the finance team, you revoke
# MAGIC their UC access once — they lose access to Snowflake, PostgreSQL, MySQL,
# MAGIC and Delta all at the same time. No need to manage credentials in each system separately.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Inspect Federation Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all connections in the metastore
# MAGIC SHOW CONNECTIONS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all catalogs — foreign catalogs appear here too
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Foreign catalogs appear in information_schema with catalog_type = 'FOREIGN'
# MAGIC SELECT catalog_name, catalog_type, connection_name, comment
# MAGIC FROM system.information_schema.catalogs
# MAGIC WHERE catalog_type = 'FOREIGN';
# MAGIC
# MAGIC -- Foreign tables have table_type = 'FOREIGN'
# MAGIC SELECT table_catalog, table_schema, table_name, table_type
# MAGIC FROM snowflake_prod_catalog.information_schema.tables
# MAGIC WHERE table_type = 'FOREIGN'
# MAGIC ORDER BY table_schema, table_name;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: When to Use Federation vs Ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC  Decision guide:
# MAGIC
# MAGIC  USE FEDERATION when:                    USE INGESTION (ETL) when:
# MAGIC  ───────────────────────────             ──────────────────────────────
# MAGIC  ✓ Need live / real-time data            ✓ Heavy transformations needed
# MAGIC  ✓ Ad-hoc exploration                    ✓ Repeated large aggregations
# MAGIC  ✓ Source schema changes often           ✓ Source can't handle extra load
# MAGIC  ✓ One-time or infrequent joins          ✓ Data must survive source outage
# MAGIC  ✓ No storage budget for copies          ✓ ML training (needs snapshot)
# MAGIC  ✓ Compliance: data can't leave source   ✓ Sub-second dashboard latency
# MAGIC
# MAGIC  BEST PRACTICE: Federate for exploration → confirm patterns →
# MAGIC                 then build an ingestion pipeline for production use
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What You Learned |
# MAGIC |---------|-----------------|
# MAGIC | Lakehouse Federation | Query external DBs live — no copy, no ETL |
# MAGIC | Connection | Credential object at metastore level |
# MAGIC | Foreign Catalog | Maps remote DB into UC 3-level namespace |
# MAGIC | Foreign Table | Auto-discovered, queryable via SQL |
# MAGIC | Pushdown | Filters/aggregations run in the source system |
# MAGIC | Governance | Same GRANT/REVOKE as regular UC — one access point |
# MAGIC | GA sources | MySQL, PostgreSQL, Redshift, Snowflake, SQL Server, Synapse, Databricks |
# MAGIC | Federation vs ETL | Federation = live/ad-hoc; ETL = production pipelines |
# MAGIC
# MAGIC **Next**: [Day 30: Delta Sharing](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day30-delta-sharing)
# MAGIC → Share data externally across organizations and clouds without copying
