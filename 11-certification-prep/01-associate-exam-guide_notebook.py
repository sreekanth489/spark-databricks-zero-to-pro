# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Associate Data Engineer Exam -- Hands-On Review
# MAGIC
# MAGIC This notebook provides quick exercises covering each of the five Associate exam domains.
# MAGIC Work through each section to reinforce your understanding of the key concepts.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 13.x+ recommended.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 1: Databricks Lakehouse Platform (24%)
# MAGIC
# MAGIC Key topics: architecture, clusters, notebooks, Delta Lake basics, DBFS.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Explore the SparkSession
# MAGIC
# MAGIC The `spark` variable is pre-configured in every Databricks notebook.
# MAGIC Inspect its properties to understand the runtime environment.

# COMMAND ----------

# The SparkSession is the entry point for all Spark functionality
print(f"Spark version: {spark.version}")
print(f"App name: {spark.sparkContext.appName}")
print(f"Master: {spark.sparkContext.master}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

# View key Spark configurations
configs_to_check = [
    "spark.databricks.clusterUsageTags.clusterName",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
]
for config_key in configs_to_check:
    try:
        value = spark.conf.get(config_key)
        print(f"{config_key} = {value}")
    except Exception:
        print(f"{config_key} = (not set)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: Create a Delta Table and Explore Its Properties
# MAGIC
# MAGIC Delta Lake is the default storage format in Databricks.
# MAGIC Create a table and inspect its transaction log behavior.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Define schema explicitly (best practice for production)
schema = StructType([
    StructField("employee_id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("department", StringType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
])

# Create sample data
employees_data = [
    (1, "Alice", "Engineering", 95000.0),
    (2, "Bob", "Marketing", 72000.0),
    (3, "Charlie", "Engineering", 120000.0),
    (4, "Diana", "Sales", 68000.0),
    (5, "Eve", "Marketing", 81000.0),
    (6, "Frank", "Engineering", 105000.0),
    (7, "Grace", "Sales", 73000.0),
    (8, "Henry", "Engineering", 115000.0),
]

df_employees = spark.createDataFrame(employees_data, schema=schema)
df_employees.createOrReplaceTempView("employees_view")

# Write as a Delta table
df_employees.write.format("delta").mode("overwrite").saveAsTable("cert_prep_employees")

print("Table created successfully.")
df_employees.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.3: Time Travel
# MAGIC
# MAGIC Delta Lake maintains a transaction log that enables time travel.
# MAGIC This is a key exam topic.

# COMMAND ----------

# Make a change so we have multiple versions
spark.sql("UPDATE cert_prep_employees SET salary = salary * 1.1 WHERE department = 'Engineering'")
print("Version 1: After 10% raise for Engineering")
spark.sql("SELECT * FROM cert_prep_employees WHERE department = 'Engineering'").show()

# Time travel: read the original version
print("Version 0: Original data")
spark.sql("SELECT * FROM cert_prep_employees VERSION AS OF 0 WHERE department = 'Engineering'").show()

# View table history
print("Table history:")
spark.sql("DESCRIBE HISTORY cert_prep_employees").select(
    "version", "timestamp", "operation", "operationParameters"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 2: ELT with Spark SQL and Python (29%)
# MAGIC
# MAGIC This is the highest-weight domain. Practice transformations, joins, and aggregations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: DataFrame Transformations

# COMMAND ----------

from pyspark.sql.functions import col, upper, when, lit, round as spark_round

# Transformation chain (method chaining pattern)
result = (
    df_employees
    .withColumn("name_upper", upper(col("name")))
    .withColumn(
        "salary_band",
        when(col("salary") >= 100000, lit("Senior"))
        .when(col("salary") >= 75000, lit("Mid"))
        .otherwise(lit("Junior"))
    )
    .withColumn("annual_bonus", spark_round(col("salary") * 0.15, 2))
    .select("employee_id", "name_upper", "department", "salary", "salary_band", "annual_bonus")
)

print("Transformed DataFrame:")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2: Joins
# MAGIC
# MAGIC Know all join types: inner, left, right, full outer, cross, anti, semi.

# COMMAND ----------

# Create a departments reference table
departments_data = [
    ("Engineering", "Building A", "VP_Eng"),
    ("Marketing", "Building B", "VP_Mkt"),
    ("Sales", "Building C", "VP_Sales"),
    ("HR", "Building D", "VP_HR"),  # No employees in HR
]
df_departments = spark.createDataFrame(
    departments_data, ["dept_name", "location", "manager"]
)

# Inner join -- only matching rows
print("=== INNER JOIN ===")
df_employees.join(
    df_departments,
    df_employees.department == df_departments.dept_name,
    "inner"
).select("name", "department", "location").show()

# Left anti join -- employees in departments NOT in the reference table
print("=== LEFT ANTI JOIN ===")
df_employees.join(
    df_departments,
    df_employees.department == df_departments.dept_name,
    "anti"
).show()

# Left semi join -- employees in departments that ARE in the reference table (no columns from right)
print("=== LEFT SEMI JOIN ===")
df_employees.join(
    df_departments,
    df_employees.department == df_departments.dept_name,
    "semi"
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3: Aggregations and GroupBy

# COMMAND ----------

from pyspark.sql.functions import count, avg, max as spark_max, min as spark_min, sum as spark_sum

# Aggregations per department
dept_stats = (
    df_employees
    .groupBy("department")
    .agg(
        count("*").alias("headcount"),
        spark_round(avg("salary"), 2).alias("avg_salary"),
        spark_max("salary").alias("max_salary"),
        spark_min("salary").alias("min_salary"),
        spark_sum("salary").alias("total_salary"),
    )
    .orderBy(col("headcount").desc())
)

print("Department statistics:")
dept_stats.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.4: Window Functions
# MAGIC
# MAGIC Window functions are a common exam topic. Know `row_number`, `rank`, `dense_rank`, `lag`, `lead`.

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, lag, lead

# Define window partitioned by department, ordered by salary descending
window_spec = Window.partitionBy("department").orderBy(col("salary").desc())

windowed = (
    df_employees
    .withColumn("rank_in_dept", rank().over(window_spec))
    .withColumn("dense_rank_in_dept", dense_rank().over(window_spec))
    .withColumn("row_num", row_number().over(window_spec))
    .select("name", "department", "salary", "rank_in_dept", "dense_rank_in_dept", "row_num")
)

print("Window functions -- ranking within department:")
windowed.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.5: Spark SQL and MERGE
# MAGIC
# MAGIC MERGE combines INSERT, UPDATE, and DELETE in one atomic operation.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a source table with updates and new records
# MAGIC CREATE OR REPLACE TEMP VIEW employee_updates AS
# MAGIC SELECT * FROM VALUES
# MAGIC   (1, 'Alice', 'Engineering', 100000.0),
# MAGIC   (4, 'Diana', 'Marketing', 75000.0),
# MAGIC   (9, 'Ivan', 'Sales', 65000.0)
# MAGIC AS updates(employee_id, name, department, salary);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE: update existing, insert new
# MAGIC MERGE INTO cert_prep_employees AS target
# MAGIC USING employee_updates AS source
# MAGIC ON target.employee_id = source.employee_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     target.name = source.name,
# MAGIC     target.department = source.department,
# MAGIC     target.salary = source.salary
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (employee_id, name, department, salary)
# MAGIC   VALUES (source.employee_id, source.name, source.department, source.salary);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the merge results
# MAGIC SELECT * FROM cert_prep_employees ORDER BY employee_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 3: Incremental Data Processing (22%)
# MAGIC
# MAGIC Key topics: Structured Streaming, Auto Loader, triggers, DLT expectations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: Structured Streaming Basics
# MAGIC
# MAGIC Demonstrate the streaming pattern using rate source (available in all environments).

# COMMAND ----------

# Create a streaming DataFrame using the rate source
# This generates rows with (timestamp, value) at the specified rate
streaming_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

print(f"Is streaming: {streaming_df.isStreaming}")
print(f"Schema: {streaming_df.schema.simpleString()}")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, window

# Apply transformations to the stream
transformed_stream = (
    streaming_df
    .withColumn("value_squared", col("value") ** 2)
    .withColumn("is_even", col("value") % 2 == 0)
)

# Write to an in-memory table for demonstration
query = (
    transformed_stream
    .writeStream
    .format("memory")
    .queryName("cert_prep_stream")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# Let it run briefly
import time
time.sleep(15)

# Query the results
spark.sql("SELECT * FROM cert_prep_stream ORDER BY timestamp DESC LIMIT 10").show()

# Stop the stream
query.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: Auto Loader Syntax Review
# MAGIC
# MAGIC Auto Loader uses `cloudFiles` format. Here is the typical pattern
# MAGIC (this cell demonstrates syntax only -- it would need actual cloud files to run):
# MAGIC
# MAGIC ```python
# MAGIC # Auto Loader pattern (read)
# MAGIC df_stream = (
# MAGIC     spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaLocation", "/path/to/schema")
# MAGIC     .option("cloudFiles.inferColumnTypes", "true")
# MAGIC     .load("/path/to/input/files")
# MAGIC )
# MAGIC
# MAGIC # Write to Delta (common pattern)
# MAGIC df_stream.writeStream \
# MAGIC     .format("delta") \
# MAGIC     .option("checkpointLocation", "/path/to/checkpoint") \
# MAGIC     .trigger(availableNow=True) \
# MAGIC     .toTable("catalog.schema.target_table")
# MAGIC ```
# MAGIC
# MAGIC **Key points for the exam:**
# MAGIC - `cloudFiles.format` specifies the source file format (json, csv, parquet, etc.)
# MAGIC - `cloudFiles.schemaLocation` stores the inferred/evolved schema
# MAGIC - `trigger(availableNow=True)` processes all available data then stops
# MAGIC - Checkpoint location must be unique per stream

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Trigger Modes Comparison
# MAGIC
# MAGIC | Trigger | Behavior | Use Case |
# MAGIC |---------|----------|----------|
# MAGIC | `processingTime="10 seconds"` | Micro-batches every 10 seconds | Near-real-time continuous processing |
# MAGIC | `availableNow=True` | Process all available data, then stop | Scheduled batch-style with streaming benefits |
# MAGIC | `once=True` | Process one micro-batch, then stop | **Deprecated** -- use `availableNow` instead |
# MAGIC | (no trigger) | Process as fast as possible | Low-latency continuous processing |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 4: Production Pipelines (16%)
# MAGIC
# MAGIC Key topics: jobs, workflows, scheduling, monitoring.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1: Task Values (Inter-Task Communication)
# MAGIC
# MAGIC In multi-task jobs, tasks pass data using `dbutils.jobs.taskValues`.
# MAGIC
# MAGIC ```python
# MAGIC # Task A: Set a value
# MAGIC dbutils.jobs.taskValues.set(key="row_count", value=12345)
# MAGIC
# MAGIC # Task B: Get the value (depends on Task A)
# MAGIC count = dbutils.jobs.taskValues.get(
# MAGIC     taskKey="task_a",
# MAGIC     key="row_count",
# MAGIC     default=0
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Key points for the exam:**
# MAGIC - Task values are limited to small data (strings, numbers, simple lists)
# MAGIC - The `taskKey` parameter in `.get()` must match the task name in the workflow
# MAGIC - Use these for passing metadata (row counts, file paths), NOT data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Job Cluster vs. All-Purpose Cluster
# MAGIC
# MAGIC | Feature | All-Purpose Cluster | Job Cluster |
# MAGIC |---------|-------------------|-------------|
# MAGIC | Lifecycle | Manual start/stop | Auto-created and terminated per job run |
# MAGIC | Cost | Higher (runs even when idle) | Lower (only runs during job execution) |
# MAGIC | Use case | Development, ad-hoc analysis | Production scheduled jobs |
# MAGIC | Shared access | Multiple users | Single job only |
# MAGIC | DBU rate | Higher per DBU | Lower per DBU |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 5: Data Governance (9%)
# MAGIC
# MAGIC Key topics: Unity Catalog, access control, dynamic views.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1: Unity Catalog Namespace
# MAGIC
# MAGIC Unity Catalog uses a three-level namespace: `catalog.schema.table`
# MAGIC
# MAGIC ```sql
# MAGIC -- Create a catalog (admin only)
# MAGIC CREATE CATALOG IF NOT EXISTS analytics;
# MAGIC
# MAGIC -- Create a schema within the catalog
# MAGIC CREATE SCHEMA IF NOT EXISTS analytics.sales;
# MAGIC
# MAGIC -- Create a table within the schema
# MAGIC CREATE TABLE IF NOT EXISTS analytics.sales.transactions (
# MAGIC     txn_id BIGINT,
# MAGIC     amount DOUBLE,
# MAGIC     txn_date DATE
# MAGIC );
# MAGIC
# MAGIC -- Grant access
# MAGIC GRANT USAGE ON CATALOG analytics TO `data_engineers`;
# MAGIC GRANT USAGE ON SCHEMA analytics.sales TO `data_engineers`;
# MAGIC GRANT SELECT ON TABLE analytics.sales.transactions TO `data_engineers`;
# MAGIC ```
# MAGIC
# MAGIC **Key points for the exam:**
# MAGIC - USAGE must be granted at both catalog and schema level
# MAGIC - Privileges are inherited downward (catalog -> schema -> table)
# MAGIC - Owner has all privileges and can grant to others

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Dynamic Views for Row/Column Security
# MAGIC
# MAGIC ```sql
# MAGIC -- Column masking: hide SSN from non-admins
# MAGIC CREATE VIEW secure_employees AS
# MAGIC SELECT
# MAGIC     employee_id,
# MAGIC     name,
# MAGIC     CASE
# MAGIC         WHEN is_member('hr_admins') THEN ssn
# MAGIC         ELSE 'XXX-XX-XXXX'
# MAGIC     END AS ssn,
# MAGIC     salary
# MAGIC FROM employees;
# MAGIC
# MAGIC -- Row filtering: users see only their department
# MAGIC CREATE VIEW dept_filtered_employees AS
# MAGIC SELECT * FROM employees
# MAGIC WHERE department = current_user()
# MAGIC    OR is_member('hr_admins');
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: Key Concepts Checklist
# MAGIC
# MAGIC Use this checklist to verify you know each concept before taking the exam:
# MAGIC
# MAGIC ### Domain 1 (24%)
# MAGIC - [ ] Lakehouse architecture (data lake + warehouse)
# MAGIC - [ ] Delta Lake: ACID, time travel, schema enforcement
# MAGIC - [ ] Cluster types: all-purpose vs. job
# MAGIC - [ ] SparkSession as entry point
# MAGIC
# MAGIC ### Domain 2 (29%)
# MAGIC - [ ] DataFrame read/write with format, options, mode
# MAGIC - [ ] Transformations: select, filter, join, groupBy, window
# MAGIC - [ ] MERGE INTO syntax
# MAGIC - [ ] Lazy evaluation and action triggers
# MAGIC - [ ] Common pyspark.sql.functions
# MAGIC
# MAGIC ### Domain 3 (22%)
# MAGIC - [ ] Auto Loader: cloudFiles format, schema location
# MAGIC - [ ] trigger(availableNow=True) vs trigger(once=True)
# MAGIC - [ ] Output modes: append, complete, update
# MAGIC - [ ] DLT expectations: EXPECT, EXPECT OR DROP, EXPECT OR FAIL
# MAGIC
# MAGIC ### Domain 4 (16%)
# MAGIC - [ ] Multi-task jobs with dependencies
# MAGIC - [ ] Job clusters for production
# MAGIC - [ ] Task values for inter-task communication
# MAGIC
# MAGIC ### Domain 5 (9%)
# MAGIC - [ ] Unity Catalog: catalog.schema.table
# MAGIC - [ ] GRANT / REVOKE syntax
# MAGIC - [ ] Dynamic views for security

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up tables and views created in this notebook
spark.sql("DROP TABLE IF EXISTS cert_prep_employees")
spark.sql("DROP VIEW IF EXISTS employees_view")
spark.sql("DROP VIEW IF EXISTS employee_updates")
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** Work through the Associate Practice Questions notebook for exam-style questions.
