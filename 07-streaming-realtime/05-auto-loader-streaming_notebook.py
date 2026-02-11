# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader Streaming -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 05 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Auto Loader (cloudFiles) basic configuration
# MAGIC 2. Schema inference with Auto Loader
# MAGIC 3. Rescued data column for malformed records
# MAGIC 4. Schema evolution simulation
# MAGIC 5. Community Edition simulation using standard file streaming
# MAGIC
# MAGIC **Note**: Auto Loader (`cloudFiles` format) requires Databricks Runtime.
# MAGIC Community Edition has limited support. This notebook provides both real
# MAGIC Auto Loader code and simulated alternatives.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
import time
import json

BASE_PATH = "/tmp/module07_topic05"
LANDING_ZONE = f"{BASE_PATH}/landing"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"
SCHEMA_PATH = f"{BASE_PATH}/schema"
BRONZE_PATH = f"{BASE_PATH}/bronze"

dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(LANDING_ZONE)
dbutils.fs.mkdirs(CHECKPOINT_PATH)
dbutils.fs.mkdirs(SCHEMA_PATH)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Generate Sample Landing Zone Data
# MAGIC
# MAGIC Simulate files arriving in a cloud storage landing zone.
# MAGIC Each batch represents a new set of files dropped by an upstream system.

# COMMAND ----------

# Batch 1: Initial data with a consistent schema
batch1_data = [
    (1, "user_001", "purchase", 29.99, "2024-01-15 10:00:00"),
    (2, "user_002", "click",    0.00,  "2024-01-15 10:01:00"),
    (3, "user_003", "purchase", 49.50, "2024-01-15 10:02:00"),
    (4, "user_004", "view",     0.00,  "2024-01-15 10:03:00"),
    (5, "user_005", "purchase", 120.00,"2024-01-15 10:04:00"),
]
batch1_schema = ["event_id", "user_id", "event_type", "amount", "event_time"]
df_batch1 = spark.createDataFrame(batch1_data, schema=batch1_schema)
df_batch1.coalesce(1).write.mode("overwrite").json(f"{LANDING_ZONE}/batch_001")
print(f"Batch 1 written: {df_batch1.count()} records")
df_batch1.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Auto Loader -- Basic cloudFiles Configuration
# MAGIC
# MAGIC This is the production Auto Loader pattern. It uses the `cloudFiles`
# MAGIC format to incrementally ingest new files from the landing zone.
# MAGIC
# MAGIC **If running on Community Edition**, this cell may fail. Skip to Cell 6
# MAGIC for the simulated alternative.

# COMMAND ----------

# Auto Loader: read from landing zone
try:
    auto_loader_stream = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_ZONE)
    )

    # Add audit columns
    enriched_stream = (
        auto_loader_stream
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Write to Bronze Delta table
    query_al = (
        enriched_stream.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/auto_loader")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )

    query_al.awaitTermination()
    print("Auto Loader ingestion complete!")

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze table rows: {df_bronze.count()}")
    df_bronze.show(truncate=False)
    AUTO_LOADER_AVAILABLE = True

except Exception as e:
    print(f"Auto Loader not available: {type(e).__name__}")
    print("This is expected on Community Edition or non-Databricks environments.")
    print("Proceeding to simulated alternative in Cell 6.\n")
    AUTO_LOADER_AVAILABLE = False

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Add New Files -- Incremental Processing
# MAGIC
# MAGIC Drop more files into the landing zone and run Auto Loader again.
# MAGIC Only the new files will be processed.

# COMMAND ----------

# Batch 2: More data with the same schema
batch2_data = [
    (6, "user_006", "purchase", 75.00,  "2024-01-15 10:05:00"),
    (7, "user_007", "click",    0.00,   "2024-01-15 10:06:00"),
    (8, "user_008", "purchase", 199.99, "2024-01-15 10:07:00"),
]
df_batch2 = spark.createDataFrame(batch2_data, schema=batch1_schema)
df_batch2.coalesce(1).write.mode("overwrite").json(f"{LANDING_ZONE}/batch_002")
print(f"Batch 2 written: {df_batch2.count()} records")

if AUTO_LOADER_AVAILABLE:
    # Re-run Auto Loader -- only new files are processed
    query_al2 = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_ZONE)
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/auto_loader")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )
    query_al2.awaitTermination()

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze table rows after Batch 2: {df_bronze.count()}")
    print("Only 3 new rows were processed (incremental)!")
else:
    print("Skipped (Auto Loader not available). See Cell 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Schema Evolution -- New Columns Appear
# MAGIC
# MAGIC When upstream systems add new fields, Auto Loader can automatically
# MAGIC evolve the schema to include them.

# COMMAND ----------

# Batch 3: New column 'device_type' added by upstream
batch3_data = [
    (9,  "user_009", "purchase", 88.00,  "2024-01-15 10:08:00", "mobile"),
    (10, "user_010", "view",     0.00,   "2024-01-15 10:09:00", "desktop"),
    (11, "user_001", "purchase", 45.00,  "2024-01-15 10:10:00", "tablet"),
]
batch3_schema = ["event_id", "user_id", "event_type", "amount", "event_time", "device_type"]
df_batch3 = spark.createDataFrame(batch3_data, schema=batch3_schema)
df_batch3.coalesce(1).write.mode("overwrite").json(f"{LANDING_ZONE}/batch_003")
print(f"Batch 3 written with NEW 'device_type' column: {df_batch3.count()} records")

if AUTO_LOADER_AVAILABLE:
    query_al3 = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_ZONE)
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/auto_loader")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )
    query_al3.awaitTermination()

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print(f"Bronze table rows after schema evolution: {df_bronze.count()}")
    print("Schema now includes 'device_type':")
    df_bronze.printSchema()
    df_bronze.orderBy("event_id").show(truncate=False)
else:
    print("Skipped (Auto Loader not available). See Cell 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Rescued Data -- Handling Malformed Records
# MAGIC
# MAGIC When data does not match the expected schema (wrong types, extra fields
# MAGIC in rescue mode), the `_rescued_data` column captures the mismatch.

# COMMAND ----------

# Batch 4: Malformed records (amount is string instead of number)
batch4_data = [
    {"event_id": 12, "user_id": "user_012", "event_type": "purchase",
     "amount": "not_a_number", "event_time": "2024-01-15 10:11:00"},
    {"event_id": 13, "user_id": "user_013", "event_type": "click",
     "amount": 0.00, "event_time": "2024-01-15 10:12:00",
     "unexpected_field": "surprise"},
]

# Write as JSON manually to preserve the malformed data exactly
import json as json_lib
rdd = spark.sparkContext.parallelize([json_lib.dumps(r) for r in batch4_data])
df_batch4 = spark.read.json(rdd)
df_batch4.coalesce(1).write.mode("overwrite").json(f"{LANDING_ZONE}/batch_004")
print("Batch 4 written with malformed records")
df_batch4.show(truncate=False)

if AUTO_LOADER_AVAILABLE:
    query_al4 = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("rescuedDataColumn", "_rescued_data")
        .load(LANDING_ZONE)
        .withColumn("_ingestion_time", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/auto_loader")
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .start(BRONZE_PATH)
    )
    query_al4.awaitTermination()

    df_bronze = spark.read.format("delta").load(BRONZE_PATH)
    print("\n=== Records with Rescued Data ===")
    df_bronze.filter(F.col("_rescued_data").isNotNull()).show(truncate=False)
else:
    print("Skipped (Auto Loader not available). See Cell 6.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Community Edition Alternative -- Standard File Streaming
# MAGIC
# MAGIC If `cloudFiles` is not available, this cell demonstrates the equivalent
# MAGIC functionality using standard Spark Structured Streaming with JSON format.
# MAGIC The concepts are the same; Auto Loader adds schema inference, evolution,
# MAGIC and optimized file discovery on top.

# COMMAND ----------

# Alternative: Standard file streaming (works on Community Edition)
SIM_BRONZE_PATH = f"{BASE_PATH}/sim_bronze"
SIM_CHECKPOINT = f"{CHECKPOINT_PATH}/sim_auto_loader"

# Define schema explicitly (standard file streaming requires this)
event_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("device_type", StringType(), True),
])

# Read the same landing zone with standard JSON streaming
sim_stream = (
    spark.readStream
    .format("json")
    .schema(event_schema)
    .option("maxFilesPerTrigger", 2)
    .load(LANDING_ZONE)
    .withColumn("_ingestion_time", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
)

query_sim = (
    sim_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", SIM_CHECKPOINT)
    .trigger(availableNow=True)
    .start(SIM_BRONZE_PATH)
)

query_sim.awaitTermination()

df_sim_bronze = spark.read.format("delta").load(SIM_BRONZE_PATH)
print(f"Simulated Bronze table rows: {df_sim_bronze.count()}")
df_sim_bronze.orderBy("event_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Auto Loader vs COPY INTO -- Comparison

# COMMAND ----------

comparison = [
    ("Engine",           "Structured Streaming",   "SQL batch command"),
    ("File tracking",    "Checkpoint offsets",      "Table metadata"),
    ("Schema inference", "Automatic",               "Manual"),
    ("Schema evolution", "Automatic (configurable)","Manual ALTER TABLE"),
    ("Rescued data",     "Built-in column",         "Manual handling"),
    ("Scale (files)",    "Billions",                "Millions"),
    ("Incremental",      "Always",                  "Tracks loaded files"),
    ("Trigger support",  "All (availableNow, etc)","N/A (runs once)"),
    ("Best for",         "Production ingestion",    "Ad-hoc / simple loads"),
]

df_compare = spark.createDataFrame(comparison, schema=["feature", "auto_loader", "copy_into"])
print("=== Auto Loader vs COPY INTO ===")
df_compare.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Auto Loader Configuration Reference
# MAGIC
# MAGIC Key options for production Auto Loader pipelines.

# COMMAND ----------

config_options = [
    ("cloudFiles.format",             "json/csv/parquet/avro", "File format to read"),
    ("cloudFiles.inferColumnTypes",   "true/false",            "Infer column types (JSON/CSV)"),
    ("cloudFiles.schemaLocation",     "/path/to/schema/",      "Store inferred schema"),
    ("cloudFiles.schemaEvolutionMode","addNewColumns",          "How to handle new columns"),
    ("cloudFiles.useNotifications",   "true/false",            "Use file notification mode"),
    ("cloudFiles.maxFilesPerTrigger", "1000",                  "Max files per micro-batch"),
    ("cloudFiles.maxBytesPerTrigger", "10g",                   "Max bytes per micro-batch"),
    ("rescuedDataColumn",            "_rescued_data",          "Column for malformed records"),
    ("cloudFiles.includeExistingFiles","true/false",           "Process existing files on first run"),
    ("cloudFiles.schemaHints",        "col1 int, col2 string", "Override inferred types"),
]

df_config = spark.createDataFrame(
    config_options,
    schema=["option", "example_value", "description"]
)
print("=== Auto Loader Configuration Options ===")
df_config.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Production Pipeline Template
# MAGIC
# MAGIC Copy this template for your production Auto Loader pipelines.

# COMMAND ----------

# -- PRODUCTION TEMPLATE (adapt for your use case) --
# This is reference code that shows a complete production pattern.

print("""
=== Production Auto Loader Pipeline Template ===

query = (
    spark.readStream
    .format("cloudFiles")

    # File format and parsing
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")

    # Schema management
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/<pipeline>/schema/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")

    # Rate limiting (prevents OOM on large backlogs)
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("cloudFiles.maxBytesPerTrigger", "10g")

    # File notification mode (for >1M files)
    # .option("cloudFiles.useNotifications", "true")

    # Source path
    .load("/mnt/landing/<source>/")

    # Audit columns
    .withColumn("_ingestion_time", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())

    # Write to Bronze Delta table
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/<pipeline>/checkpoint/")
    .option("mergeSchema", "true")

    # Scheduled execution (via Databricks Workflows)
    .trigger(availableNow=True)

    # Target table
    .toTable("catalog.bronze.<table_name>")
)

query.awaitTermination()
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Inspect Schema Location
# MAGIC
# MAGIC Auto Loader stores the inferred schema in the schemaLocation directory.
# MAGIC This allows consistent schema across query restarts.

# COMMAND ----------

print("=== Schema Location Contents ===")
try:
    for item in dbutils.fs.ls(SCHEMA_PATH):
        print(f"  {item.name} ({item.size} bytes)")
        if item.name.endswith(".json") or item.name.startswith("_schema"):
            try:
                content = dbutils.fs.head(item.path, 500)
                print(f"    Content preview: {content[:200]}")
            except Exception:
                pass
except Exception as e:
    print(f"  Could not list schema location: {e}")
    print("  This is expected if Auto Loader was not available.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
    print(f"Stopped: {q.name}")

dbutils.fs.rm(BASE_PATH, recurse=True)
print("All temporary data cleaned up.")
print("Notebook complete.")
