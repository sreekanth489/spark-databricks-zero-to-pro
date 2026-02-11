# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Auto Loader (cloudFiles)
# MAGIC
# MAGIC **Module 02 -- Topic 02 | Databricks Zero-to-Pro**
# MAGIC
# MAGIC This notebook demonstrates incremental file ingestion using Auto Loader
# MAGIC with directory-listing mode. All data is generated inline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import json, time
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# Temp paths
LANDING_DIR = "/tmp/m02_auto_loader/landing"
CHECKPOINT_DIR = "/tmp/m02_auto_loader/checkpoint"
SCHEMA_DIR = "/tmp/m02_auto_loader/schema"
TARGET_TABLE = "m02_auto_loader_bronze"

# Clean up from any prior run
dbutils.fs.rm("/tmp/m02_auto_loader", recurse=True)
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")

dbutils.fs.mkdirs(LANDING_DIR)
print("Setup complete. Landing directory ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Write Initial Batch of JSON Files
# MAGIC
# MAGIC We simulate files landing in cloud storage by writing JSON files to our
# MAGIC temp landing directory.

# COMMAND ----------

def write_json_events(file_name, events):
    """Write a list of event dicts as a JSON-lines file to the landing dir."""
    content = "\n".join(json.dumps(e) for e in events)
    dbutils.fs.put(f"{LANDING_DIR}/{file_name}", content, overwrite=True)
    print(f"  Wrote {LANDING_DIR}/{file_name} ({len(events)} events)")

# Batch 1: initial events
batch1_events = [
    {"event_id": 1, "user": "alice", "action": "login",    "amount": None,  "ts": "2024-01-15T08:00:00"},
    {"event_id": 2, "user": "bob",   "action": "purchase", "amount": 42.50, "ts": "2024-01-15T08:05:00"},
    {"event_id": 3, "user": "alice", "action": "logout",   "amount": None,  "ts": "2024-01-15T08:30:00"},
]

write_json_events("events_001.json", batch1_events)

print("\nFiles in landing directory:")
display(dbutils.fs.ls(LANDING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Set Up Auto Loader Stream
# MAGIC
# MAGIC We use `trigger(availableNow=True)` so the stream processes all available
# MAGIC files and then stops -- ideal for notebook demos and scheduled jobs.

# COMMAND ----------

# Define the Auto Loader stream
auto_loader_stream = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_DIR)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.useNotifications", "false")  # directory listing mode
    .load(LANDING_DIR)
)

# Write to a Delta table
query = (
    auto_loader_stream.writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

# Wait for the stream to finish
query.awaitTermination()
print("Stream completed (batch 1).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify the Results

# COMMAND ----------

df_bronze = spark.table(TARGET_TABLE)
print(f"Row count after batch 1: {df_bronze.count()}")
print("\nSchema:")
df_bronze.printSchema()
print("Data:")
df_bronze.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Land More Files -- Incremental Processing
# MAGIC
# MAGIC Auto Loader only picks up **new** files that were not in the previous
# MAGIC checkpoint. Existing files are skipped.

# COMMAND ----------

# Batch 2: new events in a new file
batch2_events = [
    {"event_id": 4, "user": "charlie", "action": "login",    "amount": None,   "ts": "2024-01-15T09:00:00"},
    {"event_id": 5, "user": "charlie", "action": "purchase", "amount": 119.99, "ts": "2024-01-15T09:10:00"},
    {"event_id": 6, "user": "diana",   "action": "signup",   "amount": None,   "ts": "2024-01-15T09:15:00"},
]

write_json_events("events_002.json", batch2_events)

# COMMAND ----------

# Re-run the Auto Loader stream -- it picks up only events_002.json
query2 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_DIR)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_DIR)
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query2.awaitTermination()
print("Stream completed (batch 2).")

# COMMAND ----------

df_after_batch2 = spark.table(TARGET_TABLE)
print(f"Row count after batch 2: {df_after_batch2.count()} (should be 6)")
df_after_batch2.orderBy("event_id").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Idempotency -- Running Again With No New Files
# MAGIC
# MAGIC If no new files have arrived, Auto Loader processes nothing.

# COMMAND ----------

query3 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_DIR)
    .option("cloudFiles.inferColumnTypes", "true")
    .load(LANDING_DIR)
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE)
)

query3.awaitTermination()
count_after = spark.table(TARGET_TABLE).count()
print(f"Row count (unchanged): {count_after} (still 6 -- no new files)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schema Evolution -- New Column Appears
# MAGIC
# MAGIC When a new file contains a field not seen before, Auto Loader can evolve
# MAGIC the target schema. With `addNewColumns` mode, the stream restarts
# MAGIC automatically to accommodate the new column.
# MAGIC
# MAGIC For this demo, we use the `rescuedDataColumn` option instead, which does
# MAGIC not require a restart and captures unexpected fields in a separate column.

# COMMAND ----------

# Batch 3: contains a new field "device" not in the original schema
batch3_events = [
    {"event_id": 7, "user": "eve", "action": "login", "amount": None, "ts": "2024-01-15T10:00:00", "device": "mobile"},
    {"event_id": 8, "user": "eve", "action": "purchase", "amount": 29.99, "ts": "2024-01-15T10:05:00", "device": "mobile"},
]

write_json_events("events_003.json", batch3_events)

# COMMAND ----------

# New stream with rescued data column
# Note: we use a DIFFERENT checkpoint to demonstrate rescued data cleanly
CHECKPOINT_DIR_V2 = "/tmp/m02_auto_loader/checkpoint_v2"
SCHEMA_DIR_V2 = "/tmp/m02_auto_loader/schema_v2"
TARGET_TABLE_V2 = "m02_auto_loader_rescued"
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_V2}")

query4 = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", SCHEMA_DIR_V2)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("rescuedDataColumn", "_rescued_data")
    .load(LANDING_DIR)
    .writeStream
    .format("delta")
    .option("checkpointLocation", CHECKPOINT_DIR_V2)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable(TARGET_TABLE_V2)
)

query4.awaitTermination()
print("Stream completed with rescued data column.")

# COMMAND ----------

df_rescued = spark.table(TARGET_TABLE_V2)
print(f"Row count: {df_rescued.count()}")
print("\nSchema (note _rescued_data column):")
df_rescued.printSchema()

print("\nAll rows:")
df_rescued.orderBy("event_id").show(truncate=False)

print("\nRows with rescued data (unexpected fields):")
df_rescued.filter("_rescued_data IS NOT NULL").select(
    "event_id", "user", "_rescued_data"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Inspect the Inferred Schema
# MAGIC
# MAGIC Auto Loader stores the inferred schema in the schema location as a JSON
# MAGIC file. Let us inspect it.

# COMMAND ----------

schema_files = dbutils.fs.ls(SCHEMA_DIR)
print("Schema location contents:")
for f in schema_files:
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Key Configuration Summary
# MAGIC
# MAGIC | Option | Value Used | Purpose |
# MAGIC |--------|-----------|---------|
# MAGIC | `cloudFiles.format` | `json` | Source file format |
# MAGIC | `cloudFiles.schemaLocation` | temp path | Store inferred schema |
# MAGIC | `cloudFiles.inferColumnTypes` | `true` | Infer proper types (not all strings) |
# MAGIC | `cloudFiles.useNotifications` | `false` | Directory listing mode |
# MAGIC | `rescuedDataColumn` | `_rescued_data` | Capture unrecognized fields |
# MAGIC | `trigger` | `availableNow=True` | Process all then stop |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Drop tables
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE}")
spark.sql(f"DROP TABLE IF EXISTS {TARGET_TABLE_V2}")

# Remove temp files
dbutils.fs.rm("/tmp/m02_auto_loader", recurse=True)
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Continue to **03 -- COPY INTO** to learn the SQL-based alternative for
# MAGIC idempotent file loading.
