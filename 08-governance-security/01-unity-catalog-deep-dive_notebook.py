# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Unity Catalog Deep Dive
# MAGIC > Module 08 — Topic 01 | Companion Notebook
# MAGIC
# MAGIC **What you will practice:**
# MAGIC - Creating catalogs, schemas, and tables using SQL DDL
# MAGIC - Navigating the three-level namespace with SHOW and DESCRIBE
# MAGIC - Understanding managed vs external tables
# MAGIC - Working with Volumes for non-tabular data
# MAGIC - Exploring metastore metadata
# MAGIC
# MAGIC **Requirements:**
# MAGIC - Full Databricks workspace with Unity Catalog enabled (recommended)
# MAGIC - Community Edition users: follow the Hive metastore alternatives marked below
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Environment Check
# MAGIC
# MAGIC Let us first check whether Unity Catalog is available in this workspace.

# COMMAND ----------

# Check if Unity Catalog is available
try:
    result = spark.sql("SELECT current_catalog()").collect()[0][0]
    unity_catalog_available = result != "spark_catalog"
    print(f"Current catalog: {result}")
    print(f"Unity Catalog available: {unity_catalog_available}")
except Exception as e:
    unity_catalog_available = False
    print(f"Unity Catalog not available: {e}")
    print("We will use Hive metastore alternatives throughout this notebook.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: The Three-Level Namespace
# MAGIC
# MAGIC Unity Catalog uses a three-level namespace: `catalog.schema.table`
# MAGIC
# MAGIC Think of it as:
# MAGIC - **Catalog** = environment or domain (dev, prod, finance)
# MAGIC - **Schema** = logical grouping (like a traditional database)
# MAGIC - **Table** = the actual data asset
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Listing Catalogs
# MAGIC
# MAGIC The SHOW CATALOGS command lists all catalogs you have access to.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all catalogs (Unity Catalog workspaces)
# MAGIC -- On Community Edition, this returns 'spark_catalog' (the Hive metastore)
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Creating a Catalog
# MAGIC
# MAGIC **Note:** Creating catalogs requires Unity Catalog and appropriate privileges
# MAGIC (CREATE CATALOG on the metastore). This will fail on Community Edition.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- UNITY CATALOG VERSION (requires full Databricks workspace)
# MAGIC -- ============================================================
# MAGIC -- CREATE CATALOG IF NOT EXISTS m08_demo_catalog
# MAGIC -- COMMENT 'Demo catalog for Module 08 governance exercises';
# MAGIC
# MAGIC -- USE CATALOG m08_demo_catalog;
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- COMMUNITY EDITION ALTERNATIVE
# MAGIC -- ============================================================
# MAGIC -- Community Edition uses the default 'spark_catalog' (Hive metastore).
# MAGIC -- You cannot create new catalogs. Instead, we work with schemas directly.
# MAGIC -- The concept is the same — just one level fewer.
# MAGIC
# MAGIC SELECT current_catalog() AS current_catalog,
# MAGIC        'Three-level namespace: catalog.schema.table' AS concept;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Creating a Schema (Database)
# MAGIC
# MAGIC Schemas live inside catalogs. On Community Edition, schemas live inside
# MAGIC the default `spark_catalog`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This works on both Unity Catalog and Community Edition
# MAGIC CREATE DATABASE IF NOT EXISTS m08_governance_demo
# MAGIC COMMENT 'Module 08 governance and security demonstrations';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all schemas in the current catalog
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Describe the schema we just created
# MAGIC DESCRIBE SCHEMA m08_governance_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the current schema
# MAGIC USE m08_governance_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Creating and Managing Tables
# MAGIC
# MAGIC Let us create sample tables to demonstrate Unity Catalog table management.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3a. Create a Managed Table
# MAGIC
# MAGIC A managed table stores data in the metastore's managed storage location.
# MAGIC When you DROP a managed table, the data is also deleted.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed Delta table
# MAGIC CREATE OR REPLACE TABLE m08_governance_demo.employees (
# MAGIC     employee_id     INT         COMMENT 'Unique employee identifier',
# MAGIC     first_name      STRING      COMMENT 'Employee first name',
# MAGIC     last_name       STRING      COMMENT 'Employee last name',
# MAGIC     department      STRING      COMMENT 'Department name',
# MAGIC     salary          DECIMAL(10,2) COMMENT 'Annual salary in USD',
# MAGIC     hire_date       DATE        COMMENT 'Date of hire',
# MAGIC     email           STRING      COMMENT 'Corporate email address'
# MAGIC )
# MAGIC USING DELTA
# MAGIC COMMENT 'Employee records for governance demonstrations';

# COMMAND ----------

# Insert sample data
from pyspark.sql import Row
from datetime import date

employees_data = [
    Row(1, "Alice",   "Johnson",  "Engineering", 125000.00, date(2021, 3, 15), "alice.johnson@company.com"),
    Row(2, "Bob",     "Smith",    "Marketing",    95000.00, date(2020, 7, 1),  "bob.smith@company.com"),
    Row(3, "Carol",   "Williams", "Engineering", 135000.00, date(2019, 1, 10), "carol.williams@company.com"),
    Row(4, "David",   "Brown",    "Finance",     110000.00, date(2022, 5, 20), "david.brown@company.com"),
    Row(5, "Eve",     "Davis",    "Engineering", 140000.00, date(2018, 11, 5), "eve.davis@company.com"),
    Row(6, "Frank",   "Miller",   "Marketing",    88000.00, date(2023, 2, 14), "frank.miller@company.com"),
    Row(7, "Grace",   "Wilson",   "Finance",     105000.00, date(2021, 8, 30), "grace.wilson@company.com"),
    Row(8, "Henry",   "Moore",    "HR",           92000.00, date(2020, 4, 18), "henry.moore@company.com"),
    Row(9, "Irene",   "Taylor",   "Engineering", 130000.00, date(2019, 6, 22), "irene.taylor@company.com"),
    Row(10, "Jack",   "Anderson", "HR",           97000.00, date(2022, 9, 8),  "jack.anderson@company.com"),
]

schema = "employee_id INT, first_name STRING, last_name STRING, department STRING, salary DECIMAL(10,2), hire_date DATE, email STRING"
df = spark.createDataFrame(employees_data, schema)
df.write.mode("overwrite").saveAsTable("m08_governance_demo.employees")

print("Inserted 10 employee records.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data
# MAGIC SELECT * FROM m08_governance_demo.employees ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3b. Describe Table Details
# MAGIC
# MAGIC DESCRIBE EXTENDED shows table metadata including location, provider, and owner.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Basic column information
# MAGIC DESCRIBE TABLE m08_governance_demo.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extended metadata including storage location, owner, table type
# MAGIC DESCRIBE TABLE EXTENDED m08_governance_demo.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table properties
# MAGIC SHOW TBLPROPERTIES m08_governance_demo.employees;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3c. External Tables (Concept Demonstration)
# MAGIC
# MAGIC External tables point to data stored at a user-specified cloud storage path.
# MAGIC Dropping an external table removes the metadata but NOT the underlying data.
# MAGIC
# MAGIC **Note:** Creating external tables with cloud paths requires Unity Catalog
# MAGIC external locations. Below is the syntax for reference.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- EXTERNAL TABLE SYNTAX (requires Unity Catalog + External Location)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Step 1: An admin creates a storage credential
# MAGIC -- CREATE STORAGE CREDENTIAL my_aws_credential
# MAGIC -- WITH (AWS_IAM_ROLE = 'arn:aws:iam::123456789:role/my-role');
# MAGIC --
# MAGIC -- Step 2: An admin creates an external location
# MAGIC -- CREATE EXTERNAL LOCATION my_ext_location
# MAGIC -- URL 's3://my-bucket/external-tables/'
# MAGIC -- WITH (STORAGE CREDENTIAL my_aws_credential);
# MAGIC --
# MAGIC -- Step 3: Create an external table at that location
# MAGIC -- CREATE TABLE prod.sales.transactions (
# MAGIC --     transaction_id BIGINT,
# MAGIC --     amount DECIMAL(10,2),
# MAGIC --     transaction_date DATE
# MAGIC -- )
# MAGIC -- USING DELTA
# MAGIC -- LOCATION 's3://my-bucket/external-tables/transactions/';
# MAGIC --
# MAGIC -- Key difference: DROP TABLE removes metadata only, NOT the data files.
# MAGIC
# MAGIC SELECT 'See comments above for external table syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Navigating the Namespace
# MAGIC
# MAGIC These SHOW and DESCRIBE commands are essential for exploring your
# MAGIC Unity Catalog objects.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all tables in the current schema
# MAGIC SHOW TABLES IN m08_governance_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show detailed table information
# MAGIC SHOW TABLE EXTENDED IN m08_governance_demo LIKE 'employees';

# COMMAND ----------

# Create a second table for demonstration
spark.sql("""
    CREATE OR REPLACE TABLE m08_governance_demo.departments (
        dept_id      INT     COMMENT 'Department identifier',
        dept_name    STRING  COMMENT 'Department name',
        budget       DECIMAL(12,2) COMMENT 'Annual budget in USD',
        head_count   INT     COMMENT 'Number of employees'
    )
    USING DELTA
    COMMENT 'Department reference data'
""")

departments_data = [
    (1, "Engineering", 2500000.00, 45),
    (2, "Marketing",   1200000.00, 20),
    (3, "Finance",     800000.00,  15),
    (4, "HR",          600000.00,  10),
]

dept_df = spark.createDataFrame(departments_data,
    "dept_id INT, dept_name STRING, budget DECIMAL(12,2), head_count INT")
dept_df.write.mode("overwrite").saveAsTable("m08_governance_demo.departments")

print("Created departments table with 4 records.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now show all tables — we should see both employees and departments
# MAGIC SHOW TABLES IN m08_governance_demo;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fully Qualified Names
# MAGIC
# MAGIC In Unity Catalog, you should always use fully qualified names to avoid
# MAGIC ambiguity, especially when working across catalogs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Fully qualified reference (catalog.schema.table)
# MAGIC -- On Community Edition, catalog is 'spark_catalog'
# MAGIC SELECT current_catalog() AS catalog_name,
# MAGIC        current_schema()  AS schema_name;

# COMMAND ----------

# Demonstrate programmatic namespace navigation
catalogs = spark.sql("SHOW CATALOGS").collect()
print("=== Available Catalogs ===")
for c in catalogs:
    print(f"  - {c[0]}")

schemas = spark.sql("SHOW SCHEMAS").collect()
print("\n=== Schemas in Current Catalog ===")
for s in schemas:
    print(f"  - {s[0]}")

tables = spark.sql("SHOW TABLES IN m08_governance_demo").collect()
print("\n=== Tables in m08_governance_demo ===")
for t in tables:
    print(f"  - {t[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Volumes (Non-Tabular Data)
# MAGIC
# MAGIC Volumes let you manage files (CSVs, images, ML models, JARs) under
# MAGIC Unity Catalog governance, using the same three-level namespace and
# MAGIC permission model as tables.
# MAGIC
# MAGIC **Note:** Volumes require Unity Catalog. Below is reference syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- VOLUMES SYNTAX (requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Create a managed volume (UC manages the storage path)
# MAGIC -- CREATE VOLUME m08_demo_catalog.m08_governance_demo.raw_files
# MAGIC -- COMMENT 'Raw CSV and JSON files for ingestion';
# MAGIC --
# MAGIC -- Create an external volume (you specify the cloud path)
# MAGIC -- CREATE EXTERNAL VOLUME m08_demo_catalog.m08_governance_demo.landing_zone
# MAGIC -- LOCATION 's3://my-bucket/landing/'
# MAGIC -- COMMENT 'Landing zone for incoming data files';
# MAGIC --
# MAGIC -- List volumes
# MAGIC -- SHOW VOLUMES IN m08_demo_catalog.m08_governance_demo;
# MAGIC --
# MAGIC -- Access files in a volume using the /Volumes/ path
# MAGIC -- SELECT * FROM csv.`/Volumes/m08_demo_catalog/m08_governance_demo/raw_files/data.csv`;
# MAGIC --
# MAGIC -- Grant permissions on volumes
# MAGIC -- GRANT READ VOLUME ON VOLUME raw_files TO `data_readers`;
# MAGIC -- GRANT WRITE VOLUME ON VOLUME raw_files TO `data_engineers`;
# MAGIC
# MAGIC SELECT 'See comments above for Volumes syntax' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Hive Metastore vs Unity Catalog Comparison
# MAGIC
# MAGIC This comparison helps you understand what changes when migrating.

# COMMAND ----------

# Create a comparison reference table
comparison_data = [
    ("Namespace levels",       "2 (database.table)",           "3 (catalog.schema.table)"),
    ("Scope",                  "Single workspace",             "Account-level (cross-workspace)"),
    ("Access control",         "Table ACLs (legacy)",          "GRANT/REVOKE with inheritance"),
    ("Data lineage",           "Not available",                "Automatic, table + column level"),
    ("Non-tabular data",       "DBFS (unmanaged)",             "Volumes (governed)"),
    ("Storage governance",     "Direct cloud IAM",             "Storage credentials + external locations"),
    ("Cross-workspace sharing","Manual configuration",         "Built-in via shared metastore"),
    ("Identity management",    "Workspace-level only",         "Account-level users and groups"),
    ("Migration path",         "N/A",                          "SYNC command or CTAS from hive_metastore"),
    ("Default catalog name",   "spark_catalog",                "User-defined (e.g., main, prod)"),
]

comparison_df = spark.createDataFrame(comparison_data,
    "feature STRING, hive_metastore STRING, unity_catalog STRING")
comparison_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Metastore Metadata Queries
# MAGIC
# MAGIC Unity Catalog provides INFORMATION_SCHEMA views for querying metadata
# MAGIC programmatically. On Community Edition, we can use SHOW/DESCRIBE commands.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- UNITY CATALOG: INFORMATION_SCHEMA queries
# MAGIC -- ============================================================
# MAGIC -- List all tables with their metadata
# MAGIC -- SELECT table_catalog, table_schema, table_name, table_type, comment
# MAGIC -- FROM system.information_schema.tables
# MAGIC -- WHERE table_schema = 'm08_governance_demo';
# MAGIC
# MAGIC -- List all columns for a table
# MAGIC -- SELECT column_name, data_type, is_nullable, comment
# MAGIC -- FROM system.information_schema.columns
# MAGIC -- WHERE table_schema = 'm08_governance_demo'
# MAGIC --   AND table_name = 'employees';
# MAGIC
# MAGIC -- ============================================================
# MAGIC -- COMMUNITY EDITION ALTERNATIVE
# MAGIC -- ============================================================
# MAGIC DESCRIBE TABLE EXTENDED m08_governance_demo.employees;

# COMMAND ----------

# Programmatic metadata exploration (works on all editions)
print("=" * 60)
print("TABLE METADATA: m08_governance_demo.employees")
print("=" * 60)

# Get column info
columns = spark.sql("DESCRIBE TABLE m08_governance_demo.employees").collect()
print("\nColumns:")
for col in columns:
    if col[0] and not col[0].startswith("#"):
        print(f"  {col[0]:20s} {col[1]:15s} {col[2] or ''}")

# Get table properties
print("\nTable Properties:")
props = spark.sql("SHOW TBLPROPERTIES m08_governance_demo.employees").collect()
for prop in props:
    print(f"  {prop[0]:40s} = {prop[1]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Migration Pattern — Hive to Unity Catalog
# MAGIC
# MAGIC This section demonstrates the SQL patterns used during migration.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- MIGRATION PATTERNS (reference — requires Unity Catalog)
# MAGIC -- ============================================================
# MAGIC --
# MAGIC -- Pattern 1: SYNC command (copies metadata, not data)
# MAGIC -- SYNC TABLE prod.sales.transactions
# MAGIC -- FROM hive_metastore.sales.transactions;
# MAGIC --
# MAGIC -- Pattern 2: CREATE TABLE AS SELECT (copies data)
# MAGIC -- CREATE TABLE prod.sales.transactions AS
# MAGIC -- SELECT * FROM hive_metastore.sales.transactions;
# MAGIC --
# MAGIC -- Pattern 3: DEEP CLONE (for Delta tables — copies data + metadata)
# MAGIC -- CREATE TABLE prod.sales.transactions
# MAGIC -- DEEP CLONE hive_metastore.sales.transactions;
# MAGIC --
# MAGIC -- Pattern 4: Upgrade in place (for managed tables)
# MAGIC -- This changes ownership to Unity Catalog without moving data.
# MAGIC -- Requires specific admin setup.
# MAGIC --
# MAGIC -- After migration, update all references:
# MAGIC -- OLD: SELECT * FROM sales.transactions
# MAGIC -- NEW: SELECT * FROM prod.sales.transactions
# MAGIC
# MAGIC SELECT 'See comments above for migration patterns' AS note;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove the objects we created in this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop tables
# MAGIC DROP TABLE IF EXISTS m08_governance_demo.employees;
# MAGIC DROP TABLE IF EXISTS m08_governance_demo.departments;
# MAGIC
# MAGIC -- Drop schema
# MAGIC DROP DATABASE IF EXISTS m08_governance_demo CASCADE;

# COMMAND ----------

print("Cleanup complete.")
print()
print("Key Takeaways:")
print("  1. Unity Catalog uses a three-level namespace: catalog.schema.table")
print("  2. Metastore is account-level, enabling cross-workspace governance")
print("  3. Managed tables have lifecycle managed by UC; external tables do not")
print("  4. Volumes extend governance to non-tabular data (files, models)")
print("  5. Migration from Hive metastore is incremental via SYNC, CTAS, or CLONE")
print()
print("Next: 02-access-control_notebook.py")
