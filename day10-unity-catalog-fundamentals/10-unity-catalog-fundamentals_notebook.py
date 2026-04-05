# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 10: Unity Catalog Fundamentals — Hands-On Lab
# MAGIC
# MAGIC **Objective**: Explore the Unity Catalog 3-level namespace, metastore concepts, and core governance features
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Explore the 3-level namespace: Catalog → Schema → Table
# MAGIC 2. Compare Hive Metastore vs Unity Catalog
# MAGIC 3. Create and manage catalogs and schemas
# MAGIC 4. Create managed tables and explore their storage
# MAGIC 5. Create external tables (reference existing data)
# MAGIC 6. Work with Volumes for governed file access
# MAGIC 7. Explore table metadata, history, and lineage
# MAGIC 8. Query information_schema and system tables
# MAGIC 9. Access legacy hive_metastore tables
# MAGIC
# MAGIC **Before vs After Unity Catalog**:
# MAGIC ```
# MAGIC BEFORE: schema.table         (2-level, workspace-scoped)
# MAGIC AFTER:  catalog.schema.table (3-level, account-scoped)
# MAGIC
# MAGIC  hive_metastore.hr_db.employees     ← legacy (still works)
# MAGIC  prod_catalog.hr_db.employees       ← Unity Catalog way
# MAGIC ```
# MAGIC
# MAGIC **Unity Catalog Hierarchy**:
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────┐
# MAGIC │                    UC Metastore (per region)                     │
# MAGIC │  ┌────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Catalog: databricks_pro                                   │  │
# MAGIC │  │  ├── Schema: uc_fundamentals_lab                           │  │
# MAGIC │  │  │   ├── Table: employees      (managed Delta)             │  │
# MAGIC │  │  │   ├── Table: departments    (managed Delta)             │  │
# MAGIC │  │  │   ├── View: active_emps_vw  (stored view)               │  │
# MAGIC │  │  │   └── Volume: raw_files     (governed file storage)     │  │
# MAGIC │  │  └── Schema: information_schema (auto-created)             │  │
# MAGIC │  └────────────────────────────────────────────────────────────┘  │
# MAGIC │  ┌────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Catalog: hive_metastore (legacy, always available)        │  │
# MAGIC │  └────────────────────────────────────────────────────────────┘  │
# MAGIC └─────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog enabled
# MAGIC
# MAGIC **Prerequisites**: Unity Catalog metastore assigned to this workspace

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use Unity Catalog catalog
# MAGIC -- Replace 'databricks_pro' with your catalog name
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a dedicated schema for this lab
# MAGIC CREATE SCHEMA IF NOT EXISTS uc_fundamentals_lab
# MAGIC COMMENT 'Day 10: Unity Catalog Fundamentals lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA uc_fundamentals_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Explore the Namespace
# MAGIC
# MAGIC **Key concept**: Unity Catalog adds `catalog` as the top level.
# MAGIC
# MAGIC ```
# MAGIC Hive Metastore:     SELECT * FROM hr_db.employees
# MAGIC Unity Catalog:      SELECT * FROM prod_catalog.hr_db.employees
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all catalogs available in this metastore
# MAGIC -- You should see: your catalogs + hive_metastore + system + __databricks_internal
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List schemas in our catalog
# MAGIC SHOW SCHEMAS IN databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Explore the hive_metastore (legacy workspace-local)
# MAGIC SHOW SCHEMAS IN hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare: full path to legacy vs UC tables
# MAGIC -- Legacy:
# MAGIC --   hive_metastore.default.some_table
# MAGIC -- Unity Catalog:
# MAGIC --   databricks_pro.uc_fundamentals_lab.employees
# MAGIC SELECT 'hive_metastore.default' AS legacy_2level_path,
# MAGIC        'databricks_pro.uc_fundamentals_lab' AS uc_3level_path

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create Managed Tables
# MAGIC
# MAGIC **Managed tables** are the default in Unity Catalog:
# MAGIC - Data stored in the metastore's managed storage (S3/ADLS/GCS)
# MAGIC - Dropping the table **deletes the data**
# MAGIC - UC manages the storage path — users don't need to know it
# MAGIC
# MAGIC ```
# MAGIC CREATE TABLE employees (...)     ← Managed (no LOCATION)
# MAGIC
# MAGIC Data path: <metastore_storage>/__unitystorage/catalogs/...
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id INT,
# MAGIC   first_name  STRING,
# MAGIC   last_name   STRING,
# MAGIC   email       STRING,
# MAGIC   ssn         STRING COMMENT 'PII: Social Security Number',
# MAGIC   department  STRING,
# MAGIC   region      STRING COMMENT 'Geographic region: APAC, EMEA, AMER',
# MAGIC   salary      DOUBLE COMMENT 'PII: Annual salary',
# MAGIC   hire_date   DATE,
# MAGIC   is_active   BOOLEAN
# MAGIC )
# MAGIC COMMENT 'Employee records — managed table in Unity Catalog'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1,  'Alice',   'Johnson',  'alice@company.com',   '123-45-6789', 'Engineering', 'AMER', 120000.0, '2020-03-15', true),
# MAGIC   (2,  'Bob',     'Smith',    'bob@company.com',     '234-56-7890', 'Engineering', 'AMER', 135000.0, '2019-07-01', true),
# MAGIC   (3,  'Carol',   'Williams', 'carol@company.com',   '345-67-8901', 'Marketing',   'AMER', 95000.0,  '2021-01-10', true),
# MAGIC   (4,  'David',   'Brown',    'david@company.com',   '456-78-9012', 'Finance',     'EMEA', 110000.0, '2018-11-20', true),
# MAGIC   (5,  'Eve',     'Davis',    'eve@company.com',     '567-89-0123', 'Engineering', 'EMEA', 115000.0, '2022-06-05', true),
# MAGIC   (6,  'Frank',   'Miller',   'frank@company.com',   '678-90-1234', 'Marketing',   'EMEA', 98000.0,  '2020-09-12', false),
# MAGIC   (7,  'Grace',   'Wilson',   'grace@company.com',   '789-01-2345', 'Finance',     'APAC', 105000.0, '2021-04-18', true),
# MAGIC   (8,  'Hank',    'Moore',    'hank@company.com',    '890-12-3456', 'HR',          'APAC', 88000.0,  '2017-02-28', true),
# MAGIC   (9,  'Ivy',     'Taylor',   'ivy@company.com',     '901-23-4567', 'HR',          'APAC', 82000.0,  '2023-01-15', true),
# MAGIC   (10, 'Jack',    'Anderson', 'jack@company.com',    '012-34-5678', 'Engineering', 'AMER', 128000.0, '2021-08-22', true)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query using the full 3-level namespace
# MAGIC SELECT * FROM databricks_pro.uc_fundamentals_lab.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE departments (
# MAGIC   department_name STRING,
# MAGIC   department_head STRING,
# MAGIC   budget          DOUBLE,
# MAGIC   location        STRING
# MAGIC )
# MAGIC COMMENT 'Department reference data'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO departments VALUES
# MAGIC   ('Engineering', 'Alice Johnson',  500000.0, 'San Francisco'),
# MAGIC   ('Marketing',   'Carol Williams', 250000.0, 'New York'),
# MAGIC   ('Finance',     'David Brown',    300000.0, 'Chicago'),
# MAGIC   ('HR',          'Hank Moore',     150000.0, 'San Francisco')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Explore Table Metadata
# MAGIC
# MAGIC Unity Catalog stores rich metadata for every table.
# MAGIC Notice that managed table storage location is abstracted — it lives in the
# MAGIC metastore's managed storage but you don't need to know the path.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show detailed table metadata (format, location, partitioning, etc.)
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show extended table properties including owner, location, tags
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table history (Delta transaction log)
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %md
# MAGIC **Key observations from DESCRIBE DETAIL / DESCRIBE EXTENDED**:
# MAGIC - `format`: Delta (default for managed tables in UC)
# MAGIC - `location`: UC-managed path — do NOT hardcode this in your code
# MAGIC - `tableType`: MANAGED (vs EXTERNAL for external tables)
# MAGIC - `owner`: the user who created it (can be changed with ALTER TABLE SET OWNER)
# MAGIC - For managed tables: dropping the table will delete these files

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Create Views
# MAGIC
# MAGIC Views in Unity Catalog follow the same 3-level namespace.
# MAGIC
# MAGIC ```
# MAGIC Stored view    → Persisted in UC, accessible across sessions and workspaces
# MAGIC Temp view      → Scoped to current Spark session only, NOT in UC
# MAGIC Global temp    → Scoped to cluster, NOT in UC
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a stored view (persisted in Unity Catalog)
# MAGIC CREATE OR REPLACE VIEW active_employees_vw AS
# MAGIC SELECT
# MAGIC   e.employee_id,
# MAGIC   e.first_name,
# MAGIC   e.last_name,
# MAGIC   e.email,
# MAGIC   e.department,
# MAGIC   e.region,
# MAGIC   e.salary,
# MAGIC   d.department_head,
# MAGIC   d.location AS office_location
# MAGIC FROM employees e
# MAGIC JOIN departments d ON e.department = d.department_name
# MAGIC WHERE e.is_active = true
# MAGIC COMMENT 'Active employees with department details — Day 10 lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM active_employees_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view (session-scoped, NOT stored in UC)
# MAGIC CREATE OR REPLACE TEMP VIEW high_earners_tmp AS
# MAGIC SELECT first_name, last_name, department, salary
# MAGIC FROM employees
# MAGIC WHERE salary > 110000
# MAGIC ORDER BY salary DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM high_earners_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SHOW TABLES shows stored tables AND views (but NOT temp views)
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that `active_employees_vw` appears in SHOW TABLES, but `high_earners_tmp` does not —
# MAGIC temp views are session-scoped and not stored in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Unity Catalog Volumes
# MAGIC
# MAGIC **Volumes** provide governed access to non-tabular files.
# MAGIC
# MAGIC **Before Volumes**: Files in S3/ADLS had no UC governance — anyone with the storage
# MAGIC access role could read/write any file.
# MAGIC
# MAGIC **With Volumes**: Files are governed by `GRANT READ VOLUME / WRITE VOLUME`, audited,
# MAGIC discoverable in Catalog Explorer, and tracked in lineage.
# MAGIC
# MAGIC ```
# MAGIC Schema
# MAGIC ├── Tables   (structured Delta data)
# MAGIC ├── Views    (virtual tables)
# MAGIC └── Volumes  (governed file storage)
# MAGIC     ├── Managed Volume  (UC controls storage location)
# MAGIC     └── External Volume (you point to existing storage)
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed volume
# MAGIC CREATE VOLUME IF NOT EXISTS raw_files
# MAGIC COMMENT 'Managed volume for raw file ingestion — Day 10 lab'

# COMMAND ----------

# Write a sample CSV file to the volume using dbutils
sample_csv = """employee_id,first_name,last_name,department,region
11,Karen,Thomas,Engineering,AMER
12,Leo,Martinez,Marketing,EMEA
13,Mia,Garcia,Finance,APAC
14,Nathan,Lee,HR,APAC
"""

volume_path = "/Volumes/databricks_pro/uc_fundamentals_lab/raw_files"
dbutils.fs.put(f"{volume_path}/new_employees.csv", sample_csv, overwrite=True)
print(f"Written to: {volume_path}/new_employees.csv")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in the volume (same as dbutils.fs.ls but SQL)
# MAGIC LIST '/Volumes/databricks_pro/uc_fundamentals_lab/raw_files/'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read CSV file directly from volume using read_files
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/databricks_pro/uc_fundamentals_lab/raw_files/new_employees.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Volumes are governed the same way as tables:
# MAGIC ```sql
# MAGIC GRANT READ VOLUME ON VOLUME raw_files TO `analysts`;
# MAGIC GRANT WRITE VOLUME ON VOLUME raw_files TO `data_engineers`;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: CTAS and Table Cloning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: create a new table from query results
# MAGIC CREATE OR REPLACE TABLE engineering_team AS
# MAGIC SELECT employee_id, first_name, last_name, email, salary, hire_date, region
# MAGIC FROM employees
# MAGIC WHERE department = 'Engineering' AND is_active = true

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM engineering_team

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Deep clone: full copy of data and metadata
# MAGIC CREATE OR REPLACE TABLE employees_backup
# MAGIC DEEP CLONE employees

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS backup_count FROM employees_backup

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Time Travel in Unity Catalog
# MAGIC
# MAGIC Delta Lake time travel works seamlessly with Unity Catalog managed tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current version before update
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new version by updating data
# MAGIC UPDATE employees SET salary = 140000.0 WHERE employee_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query previous version (before the update)
# MAGIC SELECT employee_id, first_name, salary AS salary_v1
# MAGIC FROM employees VERSION AS OF 1
# MAGIC WHERE employee_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query current version (after the update)
# MAGIC SELECT employee_id, first_name, salary AS salary_current
# MAGIC FROM employees
# MAGIC WHERE employee_id = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Information Schema
# MAGIC
# MAGIC Every catalog in Unity Catalog has an `information_schema` — use it to
# MAGIC programmatically query metadata about tables, columns, views, and privileges.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables and views in our lab schema
# MAGIC SELECT table_catalog, table_schema, table_name, table_type, comment
# MAGIC FROM databricks_pro.information_schema.tables
# MAGIC WHERE table_schema = 'uc_fundamentals_lab'
# MAGIC ORDER BY table_type, table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List column metadata for employees table
# MAGIC SELECT table_name, column_name, data_type, comment, ordinal_position
# MAGIC FROM databricks_pro.information_schema.columns
# MAGIC WHERE table_schema = 'uc_fundamentals_lab'
# MAGIC   AND table_name = 'employees'
# MAGIC ORDER BY ordinal_position

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find all PII columns (those with a comment containing 'PII')
# MAGIC SELECT table_name, column_name, comment
# MAGIC FROM databricks_pro.information_schema.columns
# MAGIC WHERE table_schema = 'uc_fundamentals_lab'
# MAGIC   AND comment LIKE '%PII%'

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: System Tables — Audit & Usage
# MAGIC
# MAGIC Unity Catalog provides **system tables** for governance, audit, and usage analytics.
# MAGIC These live in the `system` catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List available system table schemas
# MAGIC SHOW SCHEMAS IN system

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in system.access
# MAGIC SHOW TABLES IN system.access

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Recent audit events for our lab (your own queries)
# MAGIC -- Note: audit logs may have a few minutes delay
# MAGIC SELECT event_time, user_identity.email, action_name,
# MAGIC        request_params.full_name_arg AS object_accessed
# MAGIC FROM system.access.audit
# MAGIC WHERE request_params.full_name_arg LIKE '%uc_fundamentals_lab%'
# MAGIC   AND event_time > current_timestamp() - INTERVAL 1 HOUR
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Legacy Hive Metastore Access
# MAGIC
# MAGIC Unity Catalog is **additive** — the `hive_metastore` catalog provides access
# MAGIC to workspace-local Hive Metastore tables. No migration required.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show schemas in legacy hive_metastore
# MAGIC SHOW SCHEMAS IN hive_metastore

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table in legacy hive_metastore (for comparison)
# MAGIC CREATE TABLE IF NOT EXISTS hive_metastore.default.legacy_demo_table
# MAGIC AS SELECT 1 AS id, 'legacy_record' AS note

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Access legacy table with 3-level namespace (hive_metastore.schema.table)
# MAGIC SELECT * FROM hive_metastore.default.legacy_demo_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The old 2-level syntax also works when USE CATALOG hive_metastore is set
# MAGIC USE CATALOG hive_metastore;
# MAGIC SELECT * FROM default.legacy_demo_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch back to Unity Catalog
# MAGIC USE CATALOG databricks_pro;
# MAGIC USE SCHEMA uc_fundamentals_lab

# COMMAND ----------

# MAGIC %md
# MAGIC **Key point**: Both namespaces coexist. Teams can gradually migrate tables from
# MAGIC `hive_metastore` to Unity Catalog catalogs without breaking existing workflows.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 11: Data Lineage (Catalog Explorer)
# MAGIC
# MAGIC Unity Catalog automatically tracks lineage. The `active_employees_vw` view
# MAGIC we created reads from `employees` and `departments` — this dependency is
# MAGIC automatically recorded.
# MAGIC
# MAGIC **To view lineage in the UI**:
# MAGIC 1. Click **Catalog** in the left sidebar
# MAGIC 2. Navigate: `databricks_pro > uc_fundamentals_lab > active_employees_vw`
# MAGIC 3. Click the **Lineage** tab
# MAGIC 4. See upstream tables: `employees`, `departments`
# MAGIC
# MAGIC ```
# MAGIC  employees ──────────┐
# MAGIC                       ├──▶ active_employees_vw
# MAGIC  departments ─────────┘
# MAGIC
# MAGIC  raw_files (Volume) ──▶ read_files() ──▶ (your query)
# MAGIC ```
# MAGIC
# MAGIC Lineage is also available programmatically via system tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query lineage for our views (may need a few minutes to populate)
# MAGIC SELECT source_table_full_name, target_table_full_name, created_by, event_time
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_full_name LIKE '%uc_fundamentals_lab%'
# MAGIC ORDER BY event_time DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS uc_fundamentals_lab.employees;
# MAGIC DROP TABLE IF EXISTS uc_fundamentals_lab.departments;
# MAGIC DROP TABLE IF EXISTS uc_fundamentals_lab.engineering_team;
# MAGIC DROP TABLE IF EXISTS uc_fundamentals_lab.employees_backup;
# MAGIC DROP VIEW  IF EXISTS uc_fundamentals_lab.active_employees_vw;
# MAGIC DROP VOLUME IF EXISTS uc_fundamentals_lab.raw_files;
# MAGIC DROP SCHEMA IF EXISTS uc_fundamentals_lab CASCADE;
# MAGIC
# MAGIC -- Clean up legacy demo
# MAGIC DROP TABLE IF EXISTS hive_metastore.default.legacy_demo_table;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What You Learned |
# MAGIC |---------|-----------------|
# MAGIC | 3-Level Namespace | `catalog.schema.table` — adds catalog as env/domain organizer |
# MAGIC | Before UC | Each workspace had its own isolated Hive Metastore |
# MAGIC | After UC | Account-level governance, shared across workspaces |
# MAGIC | Managed Tables | UC controls storage; DROP deletes both metadata AND data |
# MAGIC | Volumes | Governed file access — same GRANT/REVOKE model as tables |
# MAGIC | Stored Views | Persisted in UC, cross-workspace accessible |
# MAGIC | Temp Views | Session-scoped, NOT stored in UC |
# MAGIC | Time Travel | Works on UC managed tables — VERSION AS OF N |
# MAGIC | Information Schema | Query metadata: tables, columns, privileges |
# MAGIC | System Tables | Query audit logs, lineage, usage data |
# MAGIC | Legacy Access | `hive_metastore` catalog always available — no migration required |
# MAGIC
# MAGIC **Next**: [Day 11: Unity Catalog Security](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day11-unity-catalog-security)
# MAGIC → RBAC, GRANT/REVOKE, native Row Filters, native Column Masks
