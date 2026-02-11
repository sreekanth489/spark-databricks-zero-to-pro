# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Lake Fundamentals
# MAGIC > Module 03 -- Topic 01 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a Delta table from scratch
# MAGIC 2. Inspect the `_delta_log` directory
# MAGIC 3. Read transaction log JSON files
# MAGIC 4. Understand commit actions and file statistics
# MAGIC 5. Compare Delta vs plain Parquet behavior

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Generate Sample Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)

# Clean up any previous runs
spark.sql("DROP TABLE IF EXISTS module03.delta_fundamentals")
spark.sql("CREATE DATABASE IF NOT EXISTS module03")

# Define schema for an e-commerce orders table
schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_date", StringType(), True),
])

# Generate sample data
data = [
    (1, "Alice Johnson", "Laptop", 1, 999.99, "2025-01-15"),
    (2, "Bob Smith", "Mouse", 2, 29.99, "2025-01-15"),
    (3, "Carol White", "Keyboard", 1, 79.99, "2025-01-16"),
    (4, "David Brown", "Monitor", 1, 349.99, "2025-01-16"),
    (5, "Eve Davis", "USB Cable", 5, 9.99, "2025-01-17"),
    (6, "Frank Miller", "Webcam", 1, 59.99, "2025-01-17"),
    (7, "Grace Lee", "Headphones", 2, 149.99, "2025-01-18"),
    (8, "Hank Wilson", "Laptop", 1, 1299.99, "2025-01-18"),
    (9, "Iris Taylor", "Mouse", 3, 24.99, "2025-01-19"),
    (10, "Jack Anderson", "SSD Drive", 1, 89.99, "2025-01-19"),
]

df = spark.createDataFrame(data, schema=schema)
print(f"Row count: {df.count()}")
df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Create a Delta Table
# MAGIC
# MAGIC We write the DataFrame as a Delta table. This creates both the Parquet data
# MAGIC files and the `_delta_log` transaction log directory.

# COMMAND ----------

TABLE_PATH = "/tmp/module03/delta_fundamentals"

# Remove any previous data at this path
dbutils.fs.rm(TABLE_PATH, recurse=True)

# Write as Delta table
df.write.format("delta").mode("overwrite").save(TABLE_PATH)

print(f"Delta table created at: {TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Inspect the Table Directory Structure
# MAGIC
# MAGIC A Delta table is just Parquet files plus a `_delta_log/` directory.

# COMMAND ----------

# List all files in the table directory
files = dbutils.fs.ls(TABLE_PATH)
for f in files:
    print(f"{'[DIR] ' if f.isDir() else '      '}{f.name:50s}  {f.size:>10} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Explore the Transaction Log (`_delta_log`)
# MAGIC
# MAGIC The `_delta_log` directory is the **single source of truth** for the table.
# MAGIC Each JSON file represents one committed transaction (version).

# COMMAND ----------

# List contents of _delta_log
log_path = f"{TABLE_PATH}/_delta_log"
log_files = dbutils.fs.ls(log_path)

print("Transaction log files:")
print("-" * 60)
for f in log_files:
    print(f"  {f.name:50s}  {f.size:>8} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- Read the First Commit File
# MAGIC
# MAGIC Each commit JSON file contains **actions**: `add`, `remove`, `metaData`,
# MAGIC `commitInfo`, and `protocol`. Let's read version 0.

# COMMAND ----------

# Read the version-0 commit file as text
commit_df = spark.read.text(f"{log_path}/00000000000000000000.json")
commit_df.show(truncate=False)

# COMMAND ----------

# Parse the JSON to see structured actions
import json

commit_lines = commit_df.collect()
print("=" * 60)
print("Actions in version 0:")
print("=" * 60)
for row in commit_lines:
    parsed = json.loads(row[0])
    action_type = list(parsed.keys())[0]
    print(f"\nAction: {action_type}")
    print(json.dumps(parsed[action_type], indent=2)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Understand Data Skipping Statistics
# MAGIC
# MAGIC Each `add` action includes `stats` -- min/max values per column. Delta uses
# MAGIC these for **data skipping**: pruning files that cannot contain matching rows.

# COMMAND ----------

# Extract add actions and their stats
add_actions = []
for row in commit_lines:
    parsed = json.loads(row[0])
    if "add" in parsed:
        add_actions.append(parsed["add"])

for i, action in enumerate(add_actions):
    print(f"\nFile {i}: {action['path'][:50]}...")
    print(f"  Size: {action['size']} bytes")
    if "stats" in action and action["stats"]:
        stats = json.loads(action["stats"])
        print(f"  Num records: {stats.get('numRecords', 'N/A')}")
        if "minValues" in stats:
            print(f"  Min values: {stats['minValues']}")
        if "maxValues" in stats:
            print(f"  Max values: {stats['maxValues']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- Make a Second Commit and See Versioning
# MAGIC
# MAGIC Each new write creates a new version in the transaction log.

# COMMAND ----------

# Append more data (creates version 1)
new_data = [
    (11, "Kim Park", "Tablet", 1, 499.99, "2025-01-20"),
    (12, "Leo Martinez", "Charger", 2, 19.99, "2025-01-20"),
]

new_df = spark.createDataFrame(new_data, schema=schema)
new_df.write.format("delta").mode("append").save(TABLE_PATH)

# Check the log -- now we have version 0 AND version 1
log_files = dbutils.fs.ls(log_path)
print("Transaction log files after append:")
for f in log_files:
    print(f"  {f.name}")

# Verify row count
result_df = spark.read.format("delta").load(TABLE_PATH)
print(f"\nTotal rows after append: {result_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 -- Also Register as a Managed Table
# MAGIC
# MAGIC You can also create Delta tables using SQL, which registers them in the
# MAGIC metastore (Hive or Unity Catalog).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a managed Delta table using CTAS
# MAGIC CREATE OR REPLACE TABLE module03.orders_ctas
# MAGIC USING DELTA
# MAGIC AS SELECT * FROM delta.`/tmp/module03/delta_fundamentals`;
# MAGIC
# MAGIC -- Verify
# MAGIC SELECT count(*) AS total_rows FROM module03.orders_ctas;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 8 -- Delta vs Plain Parquet Comparison
# MAGIC
# MAGIC Let's write the same data as plain Parquet and see what's missing.

# COMMAND ----------

PARQUET_PATH = "/tmp/module03/parquet_comparison"
dbutils.fs.rm(PARQUET_PATH, recurse=True)

# Write as plain Parquet
df.write.format("parquet").mode("overwrite").save(PARQUET_PATH)

# Compare directory contents
print("DELTA table directory:")
for f in dbutils.fs.ls(TABLE_PATH):
    print(f"  {f.name}")

print("\nPARQUET directory:")
for f in dbutils.fs.ls(PARQUET_PATH):
    print(f"  {f.name}")

print("\nKey difference: Delta has _delta_log/, Parquet does not.")
print("Without the log, Parquet has no ACID guarantees, no time travel,")
print("no schema enforcement, and no DML (UPDATE/DELETE/MERGE) support.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 9 -- DESCRIBE and Table Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DESCRIBE DETAIL shows Delta-specific metadata
# MAGIC DESCRIBE DETAIL delta.`/tmp/module03/delta_fundamentals`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Remove temporary data
dbutils.fs.rm("/tmp/module03/delta_fundamentals", recurse=True)
dbutils.fs.rm("/tmp/module03/parquet_comparison", recurse=True)
spark.sql("DROP TABLE IF EXISTS module03.orders_ctas")
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
