# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 10: Unity Catalog Fundamentals -- Hands-On Lab
# MAGIC
# MAGIC **Objective**: Explore the Unity Catalog 3-level namespace and core governance features
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Explore the 3-level namespace: Catalog -> Schema -> Table
# MAGIC 2. Create and manage catalogs and schemas
# MAGIC 3. Create managed tables in Unity Catalog
# MAGIC 4. Work with Volumes for governed file access
# MAGIC 5. Explore table metadata, history, and lineage
# MAGIC 6. Access legacy hive_metastore tables
# MAGIC 7. Clean up all lab resources
# MAGIC
# MAGIC **Unity Catalog Hierarchy**:
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │                        UC Metastore                                  │
# MAGIC │  (account-level, assigned to one or more workspaces)                │
# MAGIC │                                                                      │
# MAGIC │  ┌────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Catalog: databricks_pro                                       │  │
# MAGIC │  │  ├── Schema: uc_fundamentals_lab                               │  │
# MAGIC │  │  │   ├── Table: employees                                      │  │
# MAGIC │  │  │   ├── Table: departments                                    │  │
# MAGIC │  │  │   ├── View: active_employees_vw                             │  │
# MAGIC │  │  │   └── Volume: raw_files                                     │  │
# MAGIC │  │  └── Schema: information_schema                                │  │
# MAGIC │  └────────────────────────────────────────────────────────────────┘  │
# MAGIC │                                                                      │
# MAGIC │  ┌────────────────────────────────────────────────────────────────┐  │
# MAGIC │  │  Catalog: hive_metastore (legacy, always available)            │  │
# MAGIC │  │  └── Schema: default                                           │  │
# MAGIC │  └────────────────────────────────────────────────────────────────┘  │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **3-Level Namespace**:
# MAGIC ```
# MAGIC catalog.schema.table
# MAGIC   │       │      │
# MAGIC   │       │      └── Table, View, Volume, or Function
# MAGIC   │       └── Schema (database) -- groups related objects
# MAGIC   └── Catalog -- top-level organizational container
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**: See [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/blob/main/day00-environment-setup/00-databricks-cloud-setup.md) for AWS + Databricks + S3 configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use Unity Catalog catalog
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
# MAGIC ## Step 1: Explore the 3-Level Namespace
# MAGIC
# MAGIC Unity Catalog uses a **3-level namespace**: `catalog.schema.table`
# MAGIC
# MAGIC Let's explore each level.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all catalogs available in this metastore
# MAGIC SHOW CATALOGS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List schemas in our catalog
# MAGIC SHOW SCHEMAS IN databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in our lab schema (empty for now)
# MAGIC SHOW TABLES IN databricks_pro.uc_fundamentals_lab

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Create Managed Tables
# MAGIC
# MAGIC **Managed tables** are the default in Unity Catalog. The data is stored in the
# MAGIC metastore's managed storage location. Dropping the table deletes the data.
# MAGIC
# MAGIC ```
# MAGIC Managed Table Lifecycle:
# MAGIC   CREATE TABLE  -->  Data stored in UC managed storage (S3)
# MAGIC   INSERT INTO   -->  Delta files written to managed location
# MAGIC   DROP TABLE    -->  Data AND metadata deleted
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed employees table
# MAGIC CREATE OR REPLACE TABLE employees (
# MAGIC   employee_id INT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   email STRING,
# MAGIC   department STRING,
# MAGIC   hire_date DATE,
# MAGIC   salary DOUBLE,
# MAGIC   is_active BOOLEAN
# MAGIC )
# MAGIC COMMENT 'Employee records for UC fundamentals lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert sample data
# MAGIC INSERT INTO employees VALUES
# MAGIC   (1, 'Alice', 'Johnson', 'alice@company.com', 'Engineering', '2020-03-15', 120000.0, true),
# MAGIC   (2, 'Bob', 'Smith', 'bob@company.com', 'Engineering', '2019-07-01', 135000.0, true),
# MAGIC   (3, 'Carol', 'Williams', 'carol@company.com', 'Marketing', '2021-01-10', 95000.0, true),
# MAGIC   (4, 'David', 'Brown', 'david@company.com', 'Finance', '2018-11-20', 110000.0, true),
# MAGIC   (5, 'Eve', 'Davis', 'eve@company.com', 'Engineering', '2022-06-05', 115000.0, true),
# MAGIC   (6, 'Frank', 'Miller', 'frank@company.com', 'Marketing', '2020-09-12', 98000.0, false),
# MAGIC   (7, 'Grace', 'Wilson', 'grace@company.com', 'Finance', '2021-04-18', 105000.0, true),
# MAGIC   (8, 'Hank', 'Moore', 'hank@company.com', 'HR', '2017-02-28', 88000.0, true),
# MAGIC   (9, 'Ivy', 'Taylor', 'ivy@company.com', 'HR', '2023-01-15', 82000.0, true),
# MAGIC   (10, 'Jack', 'Anderson', 'jack@company.com', 'Engineering', '2021-08-22', 128000.0, true)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query using the full 3-level namespace
# MAGIC SELECT * FROM databricks_pro.uc_fundamentals_lab.employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a departments reference table
# MAGIC CREATE OR REPLACE TABLE departments (
# MAGIC   department_name STRING,
# MAGIC   department_head STRING,
# MAGIC   budget DOUBLE,
# MAGIC   location STRING
# MAGIC )
# MAGIC COMMENT 'Department reference data'

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO departments VALUES
# MAGIC   ('Engineering', 'Alice Johnson', 500000.0, 'San Francisco'),
# MAGIC   ('Marketing', 'Carol Williams', 250000.0, 'New York'),
# MAGIC   ('Finance', 'David Brown', 300000.0, 'Chicago'),
# MAGIC   ('HR', 'Hank Moore', 150000.0, 'San Francisco')

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Explore Table Metadata
# MAGIC
# MAGIC Unity Catalog stores rich metadata for every table.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show detailed table metadata
# MAGIC DESCRIBE DETAIL employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show extended table properties
# MAGIC DESCRIBE EXTENDED employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table history (Delta transaction log)
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables in current schema
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC **Key observations from DESCRIBE DETAIL**:
# MAGIC - `format`: Delta (default in UC)
# MAGIC - `location`: managed storage path (abstracted, you should NOT depend on it)
# MAGIC - `numFiles`: number of data files in current version
# MAGIC - For managed tables, the path is system-managed -- users don't need to know it

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Create Views
# MAGIC
# MAGIC Views in Unity Catalog follow the same 3-level namespace and governance model.
# MAGIC
# MAGIC ```
# MAGIC View Types:
# MAGIC   Stored View       -->  Persisted in schema, accessible across sessions
# MAGIC   Temporary View    -->  Scoped to Spark session only
# MAGIC   Global Temp View  -->  Scoped to cluster (global_temp database)
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
# MAGIC   e.salary,
# MAGIC   d.department_head,
# MAGIC   d.location
# MAGIC FROM employees e
# MAGIC JOIN departments d ON e.department = d.department_name
# MAGIC WHERE e.is_active = true

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the view
# MAGIC SELECT * FROM active_employees_vw

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a temporary view (session-scoped, not persisted in UC)
# MAGIC CREATE OR REPLACE TEMP VIEW high_earners_tmp AS
# MAGIC SELECT first_name, last_name, department, salary
# MAGIC FROM employees
# MAGIC WHERE salary > 110000

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM high_earners_tmp

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all tables AND views in the schema
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC Notice that stored views appear in `SHOW TABLES` but temporary views do not -- they exist only in the current Spark session.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Unity Catalog Volumes
# MAGIC
# MAGIC **Volumes** provide governed access to non-tabular files (CSVs, images, JARs, etc.)
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
# MAGIC COMMENT 'Managed volume for raw file ingestion'

# COMMAND ----------

# Write a sample CSV file to the volume
sample_csv = """employee_id,first_name,last_name,department
11,Karen,Thomas,Engineering
12,Leo,Martinez,Marketing
13,Mia,Garcia,Finance
"""

volume_path = "/Volumes/databricks_pro/uc_fundamentals_lab/raw_files"
dbutils.fs.put(f"{volume_path}/new_employees.csv", sample_csv, overwrite=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List files in the volume
# MAGIC LIST '/Volumes/databricks_pro/uc_fundamentals_lab/raw_files/'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read the CSV file directly from the volume using read_files
# MAGIC SELECT * FROM read_files(
# MAGIC   '/Volumes/databricks_pro/uc_fundamentals_lab/raw_files/new_employees.csv',
# MAGIC   format => 'csv',
# MAGIC   header => 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Volumes follow the same permission model as tables -- you can GRANT/REVOKE access
# MAGIC to control who can read or write files.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: CTAS and Table Cloning in Unity Catalog
# MAGIC
# MAGIC Create-Table-As-Select (CTAS) and cloning work the same way in Unity Catalog.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTAS: create a new table from query results
# MAGIC CREATE OR REPLACE TABLE engineering_team AS
# MAGIC SELECT employee_id, first_name, last_name, email, salary, hire_date
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
# MAGIC -- Check current version
# MAGIC DESCRIBE HISTORY employees

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update a record to create a new version
# MAGIC UPDATE employees SET salary = 140000.0 WHERE employee_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the previous version
# MAGIC SELECT employee_id, first_name, salary
# MAGIC FROM employees VERSION AS OF 1
# MAGIC WHERE employee_id = 2

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the current version for comparison
# MAGIC SELECT employee_id, first_name, salary
# MAGIC FROM employees
# MAGIC WHERE employee_id = 2

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Information Schema
# MAGIC
# MAGIC Unity Catalog provides an `information_schema` in every catalog for metadata queries.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the information schema to list all tables
# MAGIC SELECT table_catalog, table_schema, table_name, table_type
# MAGIC FROM databricks_pro.information_schema.tables
# MAGIC WHERE table_schema = 'uc_fundamentals_lab'
# MAGIC ORDER BY table_name

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query column metadata
# MAGIC SELECT table_name, column_name, data_type, is_nullable
# MAGIC FROM databricks_pro.information_schema.columns
# MAGIC WHERE table_schema = 'uc_fundamentals_lab'
# MAGIC   AND table_name = 'employees'
# MAGIC ORDER BY ordinal_position

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Legacy Hive Metastore Access
# MAGIC
# MAGIC Unity Catalog is **additive** -- the `hive_metastore` catalog always provides
# MAGIC access to workspace-local Hive Metastore tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show schemas in legacy hive_metastore
# MAGIC SHOW SCHEMAS IN hive_metastore

# COMMAND ----------

# MAGIC %md
# MAGIC You can query legacy tables using the `hive_metastore` catalog prefix:
# MAGIC ```sql
# MAGIC SELECT * FROM hive_metastore.default.some_old_table
# MAGIC ```
# MAGIC
# MAGIC No migration is required -- both namespaces coexist.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 10: Data Lineage
# MAGIC
# MAGIC Unity Catalog automatically tracks lineage. The `active_employees_vw` view
# MAGIC we created reads from `employees` and `departments` -- this dependency is
# MAGIC automatically recorded.
# MAGIC
# MAGIC To view lineage:
# MAGIC 1. Go to **Catalog Explorer** in the left sidebar
# MAGIC 2. Navigate to `databricks_pro > uc_fundamentals_lab > active_employees_vw`
# MAGIC 3. Click the **Lineage** tab
# MAGIC
# MAGIC ```
# MAGIC  employees ──────┐
# MAGIC                   ├──▶ active_employees_vw
# MAGIC  departments ────┘
# MAGIC ```
# MAGIC
# MAGIC Lineage is captured automatically for tables, views, notebooks, and jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS employees;
# MAGIC DROP TABLE IF EXISTS departments;
# MAGIC DROP TABLE IF EXISTS engineering_team;
# MAGIC DROP TABLE IF EXISTS employees_backup;
# MAGIC DROP VIEW IF EXISTS active_employees_vw;
# MAGIC DROP VOLUME IF EXISTS raw_files;
# MAGIC DROP SCHEMA IF EXISTS uc_fundamentals_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What You Learned |
# MAGIC |---------|-----------------|
# MAGIC | 3-Level Namespace | `catalog.schema.table` replaces 2-level Hive |
# MAGIC | Managed Tables | UC controls storage lifecycle; DROP deletes data |
# MAGIC | Volumes | Governed file access for non-tabular data |
# MAGIC | Views | Stored views persisted in UC; temp views session-scoped |
# MAGIC | Time Travel | Works seamlessly with UC managed tables |
# MAGIC | Information Schema | Metadata queries via `information_schema` |
# MAGIC | Lineage | Automatic dependency tracking across assets |
# MAGIC | Legacy Access | `hive_metastore` catalog always available |
# MAGIC
# MAGIC **Next**: [Day 11: Unity Catalog Security](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day11-unity-catalog-security) -- RBAC, privileges, row-level security, column masking
