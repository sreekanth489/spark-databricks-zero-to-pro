# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Managed Delta Tables Deep Dive
# MAGIC > Module 03 -- Topic 09 | Companion Notebook
# MAGIC
# MAGIC This notebook walks through **every major Delta Lake feature** using a managed
# MAGIC table, progressing from beginner to advanced:
# MAGIC
# MAGIC 1. Create catalog, schema, and managed Delta table
# MAGIC 2. INSERT data and query with SELECT
# MAGIC 3. Inspect metadata (DESCRIBE DETAIL, DESCRIBE HISTORY)
# MAGIC 4. UPDATE and DELETE rows
# MAGIC 5. Time Travel (VERSION AS OF, TIMESTAMP AS OF)
# MAGIC 6. MERGE (upsert) patterns
# MAGIC 7. Schema enforcement and evolution
# MAGIC 8. CHECK constraints
# MAGIC 9. OPTIMIZE and Z-ORDER
# MAGIC 10. VACUUM and retention
# MAGIC 11. Change Data Feed (CDF)
# MAGIC 12. DeltaTable Python API
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with Unity Catalog enabled.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup -- Create Catalog and Schema
# MAGIC
# MAGIC Unity Catalog uses a **three-level namespace**: `catalog.schema.table`.
# MAGIC We create a dedicated catalog and schema to keep our work isolated.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a catalog for our deep dive (skip if it already exists)
# MAGIC CREATE CATALOG IF NOT EXISTS databricks_pro;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Switch to our catalog and create a schema
# MAGIC USE CATALOG databricks_pro;
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_pro.employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set the default schema so we don't need to qualify every table
# MAGIC USE SCHEMA employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Create a Managed Delta Table
# MAGIC
# MAGIC A **managed table** stores data in the catalog's managed storage location.
# MAGIC Databricks controls the full lifecycle -- when you DROP the table, the data
# MAGIC is also deleted.
# MAGIC
# MAGIC Delta is the default format, so `USING DELTA` is optional.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS employee (
# MAGIC   employee_id  INT,
# MAGIC   first_name   STRING,
# MAGIC   last_name    STRING,
# MAGIC   department   STRING,
# MAGIC   salary       INT,
# MAGIC   hire_date    DATE
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: INSERT Data
# MAGIC
# MAGIC Insert sample employee records. This creates **version 0** in the Delta
# MAGIC transaction log (the CREATE was version 0 if empty, INSERT becomes version 1).

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO employee VALUES
# MAGIC   (1, 'John',    'Doe',      'Engineering', 80000,  '2022-01-01'),
# MAGIC   (2, 'Jane',    'Smith',    'Engineering', 75000,  '2022-02-01'),
# MAGIC   (3, 'Bob',     'Johnson',  'Marketing',   60000,  '2022-03-01'),
# MAGIC   (4, 'Alice',   'Williams', 'Marketing',   65000,  '2022-04-01'),
# MAGIC   (5, 'Charlie', 'Brown',    'Engineering', 85000,  '2022-05-01');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the data
# MAGIC SELECT * FROM employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Inspect Table Metadata
# MAGIC
# MAGIC Two essential DESCRIBE commands for Delta tables:
# MAGIC - **DESCRIBE DETAIL**: Shows physical metadata (location, size, numFiles, format)
# MAGIC - **DESCRIBE HISTORY**: Shows the full version history (operations, timestamps, users)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Physical metadata: where is the data stored? how many files? how big?
# MAGIC DESCRIBE DETAIL employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version history: every operation that modified this table
# MAGIC DESCRIBE HISTORY employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: UPDATE Rows
# MAGIC
# MAGIC Give all Engineering employees a $5,000 raise. Delta only rewrites the
# MAGIC Parquet files that contain affected rows -- it does NOT rewrite the entire table.
# MAGIC
# MAGIC This creates a **new version** in the transaction log.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE employee
# MAGIC SET salary = salary + 5000
# MAGIC WHERE department = 'Engineering';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: Engineering salaries should be +5000
# MAGIC SELECT * FROM employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check history: a new version was created for the UPDATE
# MAGIC DESCRIBE HISTORY employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Time Travel
# MAGIC
# MAGIC Delta Lake keeps **every version** of your table. You can query any past
# MAGIC version using `VERSION AS OF` or `TIMESTAMP AS OF`.
# MAGIC
# MAGIC This is invaluable for:
# MAGIC - **Auditing**: What did the data look like before that update?
# MAGIC - **Debugging**: When did this data change?
# MAGIC - **Rollback**: Restore to a known-good state

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the original data BEFORE the salary update
# MAGIC -- Version 1 = the INSERT (version 0 was CREATE TABLE)
# MAGIC SELECT * FROM employee VERSION AS OF 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query AFTER the salary update (version 2)
# MAGIC SELECT * FROM employee VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- You can also query by timestamp (use a timestamp from DESCRIBE HISTORY)
# MAGIC -- Uncomment and replace with an actual timestamp from your history:
# MAGIC -- SELECT * FROM employee TIMESTAMP AS OF '2026-02-27T20:30:00.000+00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: DELETE Rows
# MAGIC
# MAGIC Remove employee Charlie Brown (id=5). With **deletion vectors** enabled
# MAGIC (the default in Databricks), this does NOT rewrite files -- it simply marks
# MAGIC the row as deleted in a lightweight side-file.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM employee
# MAGIC WHERE employee_id = 5;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: employee 5 is gone
# MAGIC SELECT * FROM employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History now shows a DELETE operation
# MAGIC DESCRIBE HISTORY employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: MERGE (Upsert)
# MAGIC
# MAGIC MERGE is the most powerful Delta DML operation. It compares a **source** table
# MAGIC against the **target** table and atomically:
# MAGIC - Updates rows that match (WHEN MATCHED)
# MAGIC - Inserts rows that don't match (WHEN NOT MATCHED)
# MAGIC
# MAGIC This is the standard pattern for **incremental data loads** and **SCD Type 1**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a source table with updates
# MAGIC CREATE OR REPLACE TABLE employee_updates (
# MAGIC   employee_id INT,
# MAGIC   salary      INT
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employee_updates VALUES
# MAGIC   (1, 95000),   -- Employee 1 exists: UPDATE their salary
# MAGIC   (6, 70000);   -- Employee 6 is new: INSERT

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE: update existing employees, insert new ones
# MAGIC MERGE INTO employee e
# MAGIC USING employee_updates u
# MAGIC ON e.employee_id = u.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET e.salary = u.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (employee_id, salary)
# MAGIC   VALUES (u.employee_id, u.salary);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: employee 1 salary updated, employee 6 inserted
# MAGIC SELECT * FROM employee ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Schema Enforcement
# MAGIC
# MAGIC Delta Lake **rejects writes that violate the table schema**. This prevents
# MAGIC data corruption from upstream changes or bugs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This FAILS: 'wrong_salary' is a STRING, but salary column is INT
# MAGIC -- Uncomment to see the error:
# MAGIC -- INSERT INTO employee VALUES
# MAGIC --   (7, 'Test', 'User', 'Engineering', 'wrong_salary', '2022-06-01');

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Schema Evolution -- ADD COLUMNS
# MAGIC
# MAGIC You can safely add new columns to a Delta table. Existing rows get NULL
# MAGIC for the new column. This is **additive schema evolution**.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add an email column
# MAGIC ALTER TABLE employee ADD COLUMNS (email STRING);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert a new employee with the email column populated
# MAGIC INSERT INTO employee
# MAGIC VALUES (8, 'Tom', 'Hardy', 'Finance', 90000, '2022-06-01', 'tom@email.com');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Existing rows have NULL for email; new row has a value
# MAGIC SELECT * FROM employee ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: CHECK Constraints
# MAGIC
# MAGIC CHECK constraints enforce **business rules** at the table level. Any INSERT
# MAGIC or UPDATE that violates a constraint is rejected.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Business rule: salary must be positive
# MAGIC ALTER TABLE employee
# MAGIC ADD CONSTRAINT salary_positive CHECK (salary > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- This FAILS: salary is negative, violating the constraint
# MAGIC -- Uncomment to see the error:
# MAGIC -- INSERT INTO employee VALUES
# MAGIC --   (9, 'Bad', 'Salary', 'HR', -5000, '2022-07-01', NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: OPTIMIZE and Z-ORDER
# MAGIC
# MAGIC ### OPTIMIZE (File Compaction)
# MAGIC After many small writes, your table has many small files. OPTIMIZE compacts
# MAGIC them into fewer, larger files (~1 GB target), dramatically improving read
# MAGIC performance.
# MAGIC
# MAGIC ### Z-ORDER
# MAGIC Z-ORDER co-locates rows with similar values in the same files. When you
# MAGIC filter by a Z-ORDERed column, Delta can **skip entire files** that don't
# MAGIC contain matching values.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check current file count and size
# MAGIC DESCRIBE DETAIL employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compact small files into larger ones
# MAGIC OPTIMIZE employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check file count after OPTIMIZE (should be fewer files)
# MAGIC DESCRIBE DETAIL employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER by department for better data skipping on department filters
# MAGIC OPTIMIZE employee ZORDER BY (department);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the OPTIMIZE + ZORDER in history
# MAGIC DESCRIBE DETAIL employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History shows OPTIMIZE operations
# MAGIC DESCRIBE HISTORY employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 13: VACUUM -- Reclaim Storage
# MAGIC
# MAGIC After OPTIMIZE, UPDATE, or DELETE, old Parquet files remain on disk (they're
# MAGIC needed for time travel). VACUUM removes files that are:
# MAGIC - No longer referenced by the current table version
# MAGIC - Older than the retention threshold (default: 7 days / 168 hours)
# MAGIC
# MAGIC **WARNING:** After VACUUM, you CANNOT time travel to versions that relied on
# MAGIC the removed files.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- First, verify we can still time travel to version 2
# MAGIC SELECT * FROM employee VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM with default retention (7 days) -- safe
# MAGIC -- This won't remove recent files
# MAGIC VACUUM employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- To VACUUM with 0 retention (removes ALL stale files immediately):
# MAGIC -- Step 1: Disable the safety check
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- Step 2: VACUUM with 0 hours
# MAGIC -- VACUUM employee RETAIN 0 HOURS;
# MAGIC --
# MAGIC -- CAUTION: This breaks time travel for old versions!

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current data is always unaffected by VACUUM
# MAGIC SELECT * FROM employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 14: Change Data Feed (CDF)
# MAGIC
# MAGIC CDF captures **row-level changes** (insert, update_preimage, update_postimage,
# MAGIC delete) for downstream consumers. It must be explicitly enabled.
# MAGIC
# MAGIC CDF is essential for:
# MAGIC - Incremental ETL pipelines
# MAGIC - Audit and compliance trails
# MAGIC - Real-time data replication

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Change Data Feed on the table
# MAGIC ALTER TABLE employee
# MAGIC SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make a change that CDF will capture
# MAGIC UPDATE employee
# MAGIC SET salary = salary + 1000
# MAGIC WHERE employee_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check what version this created
# MAGIC DESCRIBE HISTORY employee;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read the change feed for the latest version
# MAGIC -- Replace the version number with the one from DESCRIBE HISTORY above
# MAGIC -- Shows: _change_type (update_preimage/update_postimage), _commit_version, _commit_timestamp
# MAGIC SELECT * FROM table_changes('employee', 17);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 15: Table Properties and Python API
# MAGIC
# MAGIC Inspect all table properties and use the DeltaTable Python API for
# MAGIC programmatic access.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show all table properties (Delta config, CDF, constraints, etc.)
# MAGIC SHOW TBLPROPERTIES employee;

# COMMAND ----------

from delta.tables import DeltaTable

# Access the table via the Python DeltaTable API
dt = DeltaTable.forName(spark, "employee")

# Display full history programmatically
dt.history().display(truncate=False)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extended metadata: columns, storage info, table type (MANAGED)
# MAGIC DESCRIBE EXTENDED employee;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 16: Feature Summary
# MAGIC
# MAGIC | Feature | Command | Level |
# MAGIC |---------|---------|-------|
# MAGIC | Create table | `CREATE TABLE` | Beginner |
# MAGIC | Insert data | `INSERT INTO` | Beginner |
# MAGIC | Query data | `SELECT` | Beginner |
# MAGIC | Inspect metadata | `DESCRIBE DETAIL` | Beginner |
# MAGIC | Update rows | `UPDATE ... SET ... WHERE` | Intermediate |
# MAGIC | Delete rows | `DELETE FROM ... WHERE` | Intermediate |
# MAGIC | Upsert | `MERGE INTO ... USING` | Intermediate |
# MAGIC | Time travel | `VERSION AS OF` / `TIMESTAMP AS OF` | Intermediate |
# MAGIC | Add columns | `ALTER TABLE ADD COLUMNS` | Advanced |
# MAGIC | Constraints | `ALTER TABLE ADD CONSTRAINT` | Advanced |
# MAGIC | File compaction | `OPTIMIZE` / `OPTIMIZE ... ZORDER BY` | Advanced |
# MAGIC | Stale file cleanup | `VACUUM` | Advanced |
# MAGIC | Change tracking | `table_changes()` (CDF) | Advanced |
# MAGIC | Python API | `DeltaTable.forName()` | Advanced |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Uncomment and run the cells below to clean up the tables created in this notebook.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP TABLE IF EXISTS employee;
# MAGIC -- DROP TABLE IF EXISTS employee_updates;
# MAGIC -- DROP SCHEMA IF EXISTS employee CASCADE;
# MAGIC -- DROP CATALOG IF EXISTS databricks_pro CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **[Topic 10 -- External Delta Tables on S3 & Deletion Vectors](10-external-delta-tables-s3.md)**
# MAGIC to learn how external tables store data on S3 and how deletion vectors
# MAGIC change UPDATE/DELETE behavior.
