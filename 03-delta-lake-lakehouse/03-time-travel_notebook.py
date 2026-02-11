# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Time Travel
# MAGIC > Module 03 -- Topic 03 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a Delta table and make several modifications
# MAGIC 2. Inspect the version history with DESCRIBE HISTORY
# MAGIC 3. Query past versions with VERSION AS OF
# MAGIC 4. Query by timestamp with TIMESTAMP AS OF
# MAGIC 5. Restore the table to a previous version

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Table and Build Up Versions

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.employee_travel")

schema = StructType([
    StructField("emp_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("department", StringType(), True),
    StructField("salary", DoubleType(), True),
])

# --- Version 0: Initial data ---
v0_data = [
    (1, "Alice Johnson", "Engineering", 95000.0),
    (2, "Bob Smith", "Marketing", 72000.0),
    (3, "Carol White", "Engineering", 105000.0),
    (4, "David Brown", "Sales", 68000.0),
    (5, "Eve Davis", "Marketing", 78000.0),
]

df_v0 = spark.createDataFrame(v0_data, schema=schema)
df_v0.write.format("delta").mode("overwrite").saveAsTable("module03.employee_travel")

print("Version 0 -- initial table created with 5 employees")

# COMMAND ----------

# --- Version 1: Insert more employees ---
v1_new = [
    (6, "Frank Miller", "Engineering", 112000.0),
    (7, "Grace Lee", "Sales", 71000.0),
]
spark.createDataFrame(v1_new, schema=schema).write.format("delta").mode("append").saveAsTable("module03.employee_travel")

print("Version 1 -- inserted 2 new employees (total: 7)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 2: Give Engineering a 10% raise
# MAGIC UPDATE module03.employee_travel
# MAGIC SET salary = salary * 1.10
# MAGIC WHERE department = 'Engineering';

# COMMAND ----------

print("Version 2 -- Engineering salaries increased by 10%")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 3: Delete David Brown (left the company)
# MAGIC DELETE FROM module03.employee_travel
# MAGIC WHERE emp_id = 4;

# COMMAND ----------

print("Version 3 -- deleted employee David Brown (emp_id=4)")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 4: Merge -- Bob Smith transferred to Sales with raise
# MAGIC MERGE INTO module03.employee_travel AS t
# MAGIC USING (SELECT 2 AS emp_id, 'Bob Smith' AS name, 'Sales' AS department, 80000.0 AS salary) AS s
# MAGIC ON t.emp_id = s.emp_id
# MAGIC WHEN MATCHED THEN UPDATE SET *;

# COMMAND ----------

print("Version 4 -- Bob Smith transferred to Sales with salary update")

# COMMAND ----------

# MAGIC %md
# MAGIC ## DESCRIBE HISTORY -- Full Audit Trail
# MAGIC
# MAGIC Every operation is recorded. This is your audit log.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY module03.employee_travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query by Version -- VERSION AS OF
# MAGIC
# MAGIC Let's see what the table looked like at each version.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 0: Original data (5 employees, original salaries)
# MAGIC SELECT '*** Version 0 ***' AS marker;
# MAGIC SELECT * FROM module03.employee_travel VERSION AS OF 0 ORDER BY emp_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 1: After adding Frank and Grace (7 employees)
# MAGIC SELECT * FROM module03.employee_travel VERSION AS OF 1 ORDER BY emp_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Version 2: After Engineering raises (compare salaries to v1)
# MAGIC SELECT * FROM module03.employee_travel VERSION AS OF 2 ORDER BY emp_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query by Version Using DataFrame API

# COMMAND ----------

# Read version 0 using the DataFrame API
df_v0_read = (spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("module03.employee_travel"))

print("Version 0 (DataFrame API):")
df_v0_read.show()

# Compare with current version
df_current = spark.table("module03.employee_travel")
print(f"Version 0 row count: {df_v0_read.count()}")
print(f"Current row count:   {df_current.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare Versions -- Finding What Changed
# MAGIC
# MAGIC A common pattern: diff two versions to find changes.

# COMMAND ----------

# Compare version 1 (before raises) vs version 2 (after raises)
v1 = (spark.read.format("delta")
    .option("versionAsOf", 1)
    .table("module03.employee_travel"))

v2 = (spark.read.format("delta")
    .option("versionAsOf", 2)
    .table("module03.employee_travel"))

# Join on emp_id and show salary differences
from pyspark.sql import functions as F

comparison = (v1.alias("before")
    .join(v2.alias("after"), "emp_id")
    .select(
        F.col("emp_id"),
        F.col("before.name"),
        F.col("before.department"),
        F.col("before.salary").alias("salary_v1"),
        F.col("after.salary").alias("salary_v2"),
        (F.col("after.salary") - F.col("before.salary")).alias("change"),
    )
    .filter(F.col("change") != 0))

print("Salary changes between v1 and v2:")
comparison.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## RESTORE TABLE -- Rolling Back
# MAGIC
# MAGIC We can restore the table to any previous version. This creates a NEW
# MAGIC version (does not destroy history).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Current state (version 4)
# MAGIC SELECT 'Current (v4)' AS version_label, count(*) AS rows
# MAGIC FROM module03.employee_travel

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Restore to version 1 (before raises, before delete, before merge)
# MAGIC RESTORE TABLE module03.employee_travel TO VERSION AS OF 1

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After restore -- notice this is now version 5, with v1 data
# MAGIC SELECT 'After RESTORE (v5 = v1 data)' AS version_label, count(*) AS rows
# MAGIC FROM module03.employee_travel;
# MAGIC
# MAGIC SELECT * FROM module03.employee_travel ORDER BY emp_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- History shows the RESTORE as a new entry -- nothing was lost
# MAGIC DESCRIBE HISTORY module03.employee_travel

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Observations
# MAGIC
# MAGIC 1. `VERSION AS OF` and `TIMESTAMP AS OF` let you read any past version
# MAGIC 2. `DESCRIBE HISTORY` is your audit trail for compliance and debugging
# MAGIC 3. `RESTORE` creates a new version -- it does NOT delete history
# MAGIC 4. You can still query the intermediate versions even after a RESTORE
# MAGIC 5. Time travel works until VACUUM removes old data files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.employee_travel")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
