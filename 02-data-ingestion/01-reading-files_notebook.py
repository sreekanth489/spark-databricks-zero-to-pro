# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Reading Files: CSV, JSON, Parquet, Avro
# MAGIC
# MAGIC **Module 02 -- Topic 01 | Databricks Zero-to-Pro**
# MAGIC
# MAGIC This notebook demonstrates how to use `spark.read` to load data from common
# MAGIC file formats. All sample data is generated inline so the notebook is fully
# MAGIC self-contained.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Create Temp Directory and Helper Function

# COMMAND ----------

import os, shutil, json, csv, io
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, LongType
)

# Base temp directory for this notebook
TMP_DIR = "/tmp/m02_reading_files"
dbutils.fs.mkdirs(TMP_DIR)

# Convenience function to get the local (FUSE) path for writing
def local(path):
    """Convert a dbfs path to the local FUSE mount path."""
    return path.replace("dbfs:", "/dbfs") if path.startswith("dbfs:") else f"/dbfs{path}"

print(f"Temp directory ready: {TMP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reading CSV Files
# MAGIC
# MAGIC We will create a small CSV file and read it with various options.

# COMMAND ----------

# -- Write sample CSV data --
csv_path = f"{TMP_DIR}/sample_data.csv"
csv_content = """id,name,amount,region
1,Alice,250.50,US
2,Bob,130.00,EU
3,Charlie,475.25,US
4,Diana,,APAC
5,Ethan,89.99,EU
"""

dbutils.fs.put(csv_path, csv_content, overwrite=True)
print(f"CSV written to {csv_path}")

# COMMAND ----------

# Read CSV -- schema inference (quick exploration only)
df_infer = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(csv_path)
)

print("Schema with inferSchema=true:")
df_infer.printSchema()
df_infer.show()

# COMMAND ----------

# Read CSV -- explicit schema (production best practice)
csv_schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
    StructField("region", StringType(), nullable=True),
])

df_explicit = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(csv_schema)
    .load(csv_path)
)

print("Schema with explicit definition:")
df_explicit.printSchema()
df_explicit.show()

# COMMAND ----------

# Read CSV using DDL-style schema string
ddl_schema = "id INT, name STRING, amount DOUBLE, region STRING"

df_ddl = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(ddl_schema)
    .load(csv_path)
)

print("Schema with DDL string:")
df_ddl.printSchema()
df_ddl.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Reading CSV -- Custom Delimiters
# MAGIC
# MAGIC Many source systems export pipe-delimited or tab-delimited files.

# COMMAND ----------

# Write a pipe-delimited file
pipe_path = f"{TMP_DIR}/pipe_data.csv"
pipe_content = """id|product|price
1|Widget A|19.99
2|Widget B|24.50
3|Widget C|7.25
"""
dbutils.fs.put(pipe_path, pipe_content, overwrite=True)

df_pipe = (
    spark.read.format("csv")
    .option("header", "true")
    .option("sep", "|")
    .option("inferSchema", "true")
    .load(pipe_path)
)

df_pipe.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Corrupt Record Handling
# MAGIC
# MAGIC Spark supports three parse modes: **PERMISSIVE**, **DROPMALFORMED**, and
# MAGIC **FAILFAST**. We will demonstrate each.

# COMMAND ----------

# Write CSV with some bad rows
bad_csv_path = f"{TMP_DIR}/bad_data.csv"
bad_csv_content = """id,name,amount
1,Alice,250.50
2,Bob,NOT_A_NUMBER
3,Charlie,475.25
GARBAGE_ROW
5,Ethan,89.99
"""
dbutils.fs.put(bad_csv_path, bad_csv_content, overwrite=True)

# -- PERMISSIVE mode (default): bad rows go into _corrupt_record --
permissive_schema = "id INT, name STRING, amount DOUBLE, _corrupt_record STRING"

df_permissive = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "PERMISSIVE")
    .option("columnNameOfCorruptRecord", "_corrupt_record")
    .schema(permissive_schema)
    .load(bad_csv_path)
)

print("=== PERMISSIVE mode ===")
df_permissive.show(truncate=False)

# Show only corrupt rows
print("Corrupt rows only:")
df_permissive.filter("_corrupt_record IS NOT NULL").show(truncate=False)

# COMMAND ----------

# -- DROPMALFORMED mode: bad rows are silently dropped --
drop_schema = "id INT, name STRING, amount DOUBLE"

df_drop = (
    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "DROPMALFORMED")
    .schema(drop_schema)
    .load(bad_csv_path)
)

print("=== DROPMALFORMED mode (bad rows removed) ===")
df_drop.show()
print(f"Row count: {df_drop.count()} (original had 5 data rows)")

# COMMAND ----------

# -- FAILFAST mode: throws exception on first bad row --
print("=== FAILFAST mode ===")
try:
    df_fail = (
        spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(drop_schema)
        .load(bad_csv_path)
    )
    df_fail.show()  # This triggers the read
except Exception as e:
    print(f"FAILFAST raised an exception (expected):\n  {type(e).__name__}: {str(e)[:200]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Reading JSON Files
# MAGIC
# MAGIC Spark defaults to JSON-Lines (one object per line). Use `multiLine=true`
# MAGIC for pretty-printed JSON.

# COMMAND ----------

# -- JSON Lines (one object per line) --
jsonl_path = f"{TMP_DIR}/events.jsonl"
jsonl_content = """{"event_id": 1, "user": "alice", "action": "login", "ts": "2024-01-15T08:30:00"}
{"event_id": 2, "user": "bob", "action": "purchase", "amount": 42.50, "ts": "2024-01-15T09:00:00"}
{"event_id": 3, "user": "charlie", "action": "login", "ts": "2024-01-15T09:15:00"}
"""
dbutils.fs.put(jsonl_path, jsonl_content, overwrite=True)

df_jsonl = spark.read.json(jsonl_path)
print("JSON Lines schema (inferred):")
df_jsonl.printSchema()
df_jsonl.show(truncate=False)

# COMMAND ----------

# -- Multi-line JSON --
multi_json_path = f"{TMP_DIR}/users.json"
multi_json_content = """[
  {"id": 1, "name": "Alice", "address": {"city": "Seattle", "state": "WA"}},
  {"id": 2, "name": "Bob", "address": {"city": "Austin", "state": "TX"}},
  {"id": 3, "name": "Charlie", "address": {"city": "Denver", "state": "CO"}}
]"""
dbutils.fs.put(multi_json_path, multi_json_content, overwrite=True)

df_multi = (
    spark.read.format("json")
    .option("multiLine", "true")
    .load(multi_json_path)
)

print("Multi-line JSON schema:")
df_multi.printSchema()
df_multi.show(truncate=False)

# Access nested fields with dot notation
print("Accessing nested field address.city:")
df_multi.select("id", "name", "address.city", "address.state").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Reading Parquet Files
# MAGIC
# MAGIC Parquet embeds its schema, so there is no need for `header` or
# MAGIC `inferSchema` options.

# COMMAND ----------

# Write sample Parquet data
parquet_path = f"{TMP_DIR}/sales.parquet"
sales_data = [
    (1, "Widget A", 19.99, "2024-01-10"),
    (2, "Widget B", 24.50, "2024-01-11"),
    (3, "Widget C", 7.25,  "2024-01-12"),
    (4, "Widget A", 19.99, "2024-01-13"),
    (5, "Widget D", 55.00, "2024-01-14"),
]
sales_schema = "sale_id INT, product STRING, price DOUBLE, sale_date STRING"
df_sales = spark.createDataFrame(sales_data, schema=sales_schema)
df_sales.write.mode("overwrite").parquet(parquet_path)

# Read it back
df_pq = spark.read.parquet(parquet_path)
print("Parquet schema (embedded):")
df_pq.printSchema()
df_pq.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Parquet mergeSchema
# MAGIC
# MAGIC When Parquet files in the same directory have different schemas, use
# MAGIC `mergeSchema` to unify them.

# COMMAND ----------

# Write two Parquet files with different schemas
merge_dir = f"{TMP_DIR}/merge_parquet"

# File batch 1: original schema
batch1 = spark.createDataFrame(
    [(1, "Alice", 100), (2, "Bob", 200)],
    schema="id INT, name STRING, score INT"
)
batch1.write.mode("overwrite").parquet(f"{merge_dir}/batch1")

# File batch 2: added a new column "grade"
batch2 = spark.createDataFrame(
    [(3, "Charlie", 300, "A"), (4, "Diana", 150, "B")],
    schema="id INT, name STRING, score INT, grade STRING"
)
batch2.write.mode("overwrite").parquet(f"{merge_dir}/batch2")

# Read without mergeSchema -- only sees schema of first file encountered
df_no_merge = spark.read.parquet(f"{merge_dir}/batch1", f"{merge_dir}/batch2")
print("Without mergeSchema -- may miss 'grade' column:")
df_no_merge.printSchema()

# Read with mergeSchema -- union of all schemas
df_merged = (
    spark.read.option("mergeSchema", "true")
    .parquet(f"{merge_dir}/batch1", f"{merge_dir}/batch2")
)
print("With mergeSchema -- all columns present:")
df_merged.printSchema()
df_merged.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reading Text and Binary Files

# COMMAND ----------

# -- Text file --
txt_path = f"{TMP_DIR}/readme.txt"
txt_content = """Line one of the file.
Line two of the file.
Line three of the file.
"""
dbutils.fs.put(txt_path, txt_content, overwrite=True)

df_text = spark.read.text(txt_path)
print("Text file (one row per line):")
df_text.show(truncate=False)

# -- wholetext mode --
df_whole = spark.read.option("wholetext", "true").text(txt_path)
print("Text file (whole file as one row):")
df_whole.show(truncate=False)

# COMMAND ----------

# -- Binary file --
# We will read the text file as binary to show the binaryFile format
df_binary = spark.read.format("binaryFile").load(txt_path)
print("Binary file reader columns:")
df_binary.printSchema()
df_binary.select("path", "length").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Glob Patterns and Directory Reads

# COMMAND ----------

# Create multiple CSV files in separate "partition" directories
for month in ["01", "02", "03"]:
    dir_path = f"{TMP_DIR}/partitioned/month={month}"
    dbutils.fs.mkdirs(dir_path)
    content = f"id,value\n{month}1,{int(month)*10}\n{month}2,{int(month)*20}\n"
    dbutils.fs.put(f"{dir_path}/data.csv", content, overwrite=True)

# Read entire directory (Spark discovers partitions automatically for Parquet/Delta,
# but for CSV we read the glob and partition columns are NOT auto-discovered)
df_glob = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{TMP_DIR}/partitioned/month=*/data.csv")
)

print("Glob pattern read (month=*):")
df_glob.show()

# Read only months 01 and 02
df_subset = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(f"{TMP_DIR}/partitioned/month=0[1-2]/data.csv")
)

print("Glob pattern subset (month=01 and 02 only):")
df_subset.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Summary Table
# MAGIC
# MAGIC | Format | Schema Embedded | Human Readable | Best For |
# MAGIC |--------|----------------|----------------|----------|
# MAGIC | CSV | No | Yes | Data exchange with external systems |
# MAGIC | JSON | Partial | Yes | API payloads, semi-structured logs |
# MAGIC | Parquet | Yes | No | Analytical workloads, columnar queries |
# MAGIC | Avro | Yes | No | Streaming, Kafka integration |
# MAGIC | Text | N/A | Yes | Log files, raw text processing |
# MAGIC | Binary | N/A | No | Images, PDFs, arbitrary files |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove all temp files created by this notebook.

# COMMAND ----------

dbutils.fs.rm(TMP_DIR, recurse=True)
print(f"Cleaned up {TMP_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **02 -- Auto Loader** to learn how to incrementally ingest
# MAGIC files as they arrive using `cloudFiles`.
