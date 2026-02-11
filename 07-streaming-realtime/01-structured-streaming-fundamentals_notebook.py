# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Structured Streaming Fundamentals -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 01 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Building a streaming pipeline with the rate source
# MAGIC 2. Streaming DataFrame inspection (`isStreaming`, schema)
# MAGIC 3. File-based streaming with generated JSON files
# MAGIC 4. Monitoring streaming query status and progress
# MAGIC 5. Checkpointing configuration and directory structure
# MAGIC
# MAGIC **All examples are self-contained** -- no external data sources required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Import Libraries and Define Paths

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType
)
import time
import json
import os

# Base path for this notebook's data (use /tmp for Community Edition compatibility)
BASE_PATH = "/tmp/module07_topic01"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"
FILE_SOURCE_PATH = f"{BASE_PATH}/json_source"
FILE_SINK_PATH = f"{BASE_PATH}/file_sink"

# Clean up from any previous runs
dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(FILE_SOURCE_PATH)
dbutils.fs.mkdirs(CHECKPOINT_PATH)

print(f"Base path: {BASE_PATH}")
print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Rate Source -- Your First Streaming Pipeline
# MAGIC
# MAGIC The `rate` source generates rows with a `timestamp` and an auto-incrementing
# MAGIC `value` column. It is perfect for learning because it requires no external data.

# COMMAND ----------

# Read from the rate source: 5 rows per second
rate_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
)

# Verify this is a streaming DataFrame
print(f"Is streaming: {rate_stream.isStreaming}")
print("Schema:")
rate_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Write Rate Stream to Memory Sink
# MAGIC
# MAGIC The `memory` sink stores results in an in-memory table that you can query with
# MAGIC SQL. This is useful for interactive exploration but is **not for production**.

# COMMAND ----------

# Write the rate stream to a memory sink
query_memory = (
    rate_stream
    .writeStream
    .format("memory")
    .queryName("rate_data")
    .outputMode("append")
    .start()
)

# Let it run for a few seconds to accumulate data
time.sleep(10)

print(f"Query name : {query_memory.name}")
print(f"Query ID   : {query_memory.id}")
print(f"Is active  : {query_memory.isActive}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Query the In-Memory Streaming Table

# COMMAND ----------

# Query the in-memory table created by the memory sink
df_memory = spark.sql("SELECT * FROM rate_data ORDER BY timestamp DESC LIMIT 10")
df_memory.show(truncate=False)

row_count = spark.sql("SELECT COUNT(*) AS total FROM rate_data").collect()[0]["total"]
print(f"Total rows accumulated so far: {row_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Streaming Query Monitoring
# MAGIC
# MAGIC Every streaming query exposes status and progress information.
# MAGIC This is essential for monitoring in production.

# COMMAND ----------

# Current status (is it processing data right now?)
print("=== Query Status ===")
print(json.dumps(query_memory.status, indent=2))

print("\n=== Last Progress ===")
last_progress = query_memory.lastProgress
if last_progress:
    print(f"  Batch ID       : {last_progress.get('batchId', 'N/A')}")
    print(f"  Num input rows : {last_progress.get('numInputRows', 'N/A')}")
    print(f"  Input rows/sec : {last_progress.get('inputRowsPerSecond', 'N/A')}")
    print(f"  Process rows/s : {last_progress.get('processedRowsPerSecond', 'N/A')}")
    print(f"  Batch duration : {last_progress.get('batchDuration', 'N/A')} ms")
else:
    print("  No progress data yet.")

# List all active streaming queries in this SparkSession
print(f"\n=== Active Streaming Queries: {len(spark.streams.active)} ===")
for q in spark.streams.active:
    print(f"  - {q.name} (id={q.id}, active={q.isActive})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Stop the Memory Sink Query

# COMMAND ----------

query_memory.stop()
print("Memory sink query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Add Transformations to a Stream
# MAGIC
# MAGIC Streaming DataFrames support the same transformations as batch DataFrames:
# MAGIC `select`, `filter`, `withColumn`, `groupBy`, etc.

# COMMAND ----------

# Create a new rate stream and add transformations
transformed_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", F.when(F.col("value") % 3 == 0, "A")
                             .when(F.col("value") % 3 == 1, "B")
                             .otherwise("C"))
    .withColumn("amount", (F.col("value") * 1.5 + 10).cast("double"))
    .select("timestamp", "value", "category", "amount")
)

print(f"Is streaming: {transformed_stream.isStreaming}")
print("Transformed schema:")
transformed_stream.printSchema()

# Write to memory to inspect results
query_transformed = (
    transformed_stream.writeStream
    .format("memory")
    .queryName("transformed_rate")
    .outputMode("append")
    .start()
)

time.sleep(8)

spark.sql("""
    SELECT category, COUNT(*) AS cnt, ROUND(AVG(amount), 2) AS avg_amount
    FROM transformed_rate
    GROUP BY category
    ORDER BY category
""").show()

query_transformed.stop()
print("Transformed query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: File-Based Streaming -- Generate JSON Source Files
# MAGIC
# MAGIC In production, file-based streaming monitors a directory for new files.
# MAGIC We will simulate this by writing JSON files in batches.

# COMMAND ----------

# Generate sample JSON files to simulate incoming data
def write_json_batch(batch_id, records, path):
    """Write a batch of records as a JSON file to the source directory."""
    data = spark.createDataFrame(records, schema=["event_id", "user_id", "action", "amount"])
    output_path = f"{path}/batch_{batch_id}.json"
    data.coalesce(1).write.mode("overwrite").json(output_path)
    return data.count()

# Batch 1: Initial data
batch1 = [
    (1, "user_001", "purchase", 29.99),
    (2, "user_002", "purchase", 49.50),
    (3, "user_001", "refund", -15.00),
    (4, "user_003", "purchase", 120.00),
    (5, "user_004", "purchase", 9.99),
]
count = write_json_batch(1, batch1, FILE_SOURCE_PATH)
print(f"Batch 1 written: {count} records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Read JSON Files as a Stream
# MAGIC
# MAGIC Use `readStream` with the `json` format to monitor the directory.
# MAGIC Spark will pick up new files as they appear.

# COMMAND ----------

# Define schema explicitly (required for file-based streaming)
event_schema = StructType([
    StructField("event_id", IntegerType(), True),
    StructField("user_id", StringType(), True),
    StructField("action", StringType(), True),
    StructField("amount", DoubleType(), True),
])

# Read the directory as a stream
file_stream = (
    spark.readStream
    .format("json")
    .schema(event_schema)
    .option("maxFilesPerTrigger", 1)  # process one file per micro-batch
    .load(FILE_SOURCE_PATH)
)

print(f"Is streaming: {file_stream.isStreaming}")
file_stream.printSchema()

# Write to memory sink for inspection
query_file = (
    file_stream.writeStream
    .format("memory")
    .queryName("file_events")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/file_events")
    .start()
)

time.sleep(10)

spark.sql("SELECT * FROM file_events ORDER BY event_id").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Add More Files -- Observe Incremental Processing
# MAGIC
# MAGIC Drop another batch of JSON files into the source directory. The streaming
# MAGIC query will automatically pick them up in the next micro-batch.

# COMMAND ----------

# Batch 2: New data arriving
batch2 = [
    (6, "user_005", "purchase", 75.00),
    (7, "user_002", "purchase", 33.25),
    (8, "user_006", "purchase", 199.99),
]
count = write_json_batch(2, batch2, FILE_SOURCE_PATH)
print(f"Batch 2 written: {count} records")

# Wait for the streaming query to pick up the new file
time.sleep(10)

print("After adding Batch 2:")
spark.sql("SELECT * FROM file_events ORDER BY event_id").show()
total = spark.sql("SELECT COUNT(*) AS total FROM file_events").collect()[0]["total"]
print(f"Total records in stream: {total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Batch 3 -- More Incremental Data

# COMMAND ----------

# Batch 3: Even more data
batch3 = [
    (9, "user_001", "purchase", 45.00),
    (10, "user_007", "purchase", 88.88),
    (11, "user_003", "refund", -50.00),
    (12, "user_008", "purchase", 12.50),
]
count = write_json_batch(3, batch3, FILE_SOURCE_PATH)
print(f"Batch 3 written: {count} records")

time.sleep(10)

print("After adding Batch 3:")
spark.sql("SELECT COUNT(*) AS total FROM file_events").show()

# Show summary by action type
spark.sql("""
    SELECT action, COUNT(*) AS cnt, ROUND(SUM(amount), 2) AS total_amount
    FROM file_events
    GROUP BY action
    ORDER BY action
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 11: Inspect the Checkpoint Directory
# MAGIC
# MAGIC The checkpoint records offsets, commits, and state. Let us examine its
# MAGIC structure to understand how fault tolerance works.

# COMMAND ----------

query_file.stop()
print("File stream query stopped.\n")

# Inspect the checkpoint directory structure
print("=== Checkpoint Directory Structure ===")
checkpoint_base = f"{CHECKPOINT_PATH}/file_events"
try:
    for item in dbutils.fs.ls(checkpoint_base):
        print(f"  {item.name}")
        if item.isDir():
            try:
                for sub_item in dbutils.fs.ls(item.path):
                    print(f"    {sub_item.name} ({sub_item.size} bytes)")
            except Exception:
                pass
except Exception as e:
    print(f"Could not list checkpoint directory: {e}")
    print("Note: Checkpoint structure may vary by Databricks Runtime version.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 12: Writing a Stream to Delta (Production Pattern)
# MAGIC
# MAGIC In production, Delta Lake is the recommended sink. It provides exactly-once
# MAGIC guarantees through idempotent writes and the transaction log.

# COMMAND ----------

DELTA_SINK_PATH = f"{BASE_PATH}/delta_sink"

# Create a new rate stream
delta_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", F.when(F.col("value") % 2 == 0, "even").otherwise("odd"))
)

# Write to Delta with checkpointing
query_delta = (
    delta_stream.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/delta_sink")
    .start(DELTA_SINK_PATH)
)

time.sleep(15)
query_delta.stop()

# Read the Delta table to verify
df_delta = spark.read.format("delta").load(DELTA_SINK_PATH)
print(f"Records written to Delta: {df_delta.count()}")
df_delta.groupBy("category").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 13: Reading a Delta Table as a Stream
# MAGIC
# MAGIC Delta tables can also be **sources** for streaming. This is the foundation
# MAGIC of the medallion architecture (bronze -> silver -> gold) in Databricks.

# COMMAND ----------

# Read the Delta table we just wrote as a stream
delta_read_stream = (
    spark.readStream
    .format("delta")
    .load(DELTA_SINK_PATH)
)

print(f"Is streaming: {delta_read_stream.isStreaming}")

# Aggregate and write to memory for inspection
query_delta_read = (
    delta_read_stream
    .groupBy("category")
    .agg(F.count("*").alias("cnt"), F.avg("value").alias("avg_value"))
    .writeStream
    .format("memory")
    .queryName("delta_agg")
    .outputMode("complete")
    .start()
)

time.sleep(10)

spark.sql("SELECT * FROM delta_agg").show()

query_delta_read.stop()
print("Delta read stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 14: Streaming vs Batch -- Side-by-Side Comparison

# COMMAND ----------

# Batch read (standard)
df_batch = spark.read.format("delta").load(DELTA_SINK_PATH)
print(f"Batch DataFrame -- isStreaming: {df_batch.isStreaming}")
print(f"Batch row count: {df_batch.count()}")

# Streaming read (note: you cannot call .count() on a streaming DataFrame)
df_streaming = spark.readStream.format("delta").load(DELTA_SINK_PATH)
print(f"\nStreaming DataFrame -- isStreaming: {df_streaming.isStreaming}")
print("Note: .count() and .show() are NOT available on streaming DataFrames.")
print("You must use writeStream to a sink to see results.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 15: Summary of Key API Methods
# MAGIC
# MAGIC | Method | Purpose |
# MAGIC |--------|---------|
# MAGIC | `spark.readStream` | Create a streaming DataFrame from a source |
# MAGIC | `df.writeStream` | Write a streaming DataFrame to a sink |
# MAGIC | `query.status` | Check if the query is active and processing |
# MAGIC | `query.lastProgress` | Get metrics from the most recent micro-batch |
# MAGIC | `query.recentProgress` | Get metrics from the last few micro-batches |
# MAGIC | `query.stop()` | Gracefully stop the streaming query |
# MAGIC | `query.awaitTermination()` | Block until the query stops (useful in jobs) |
# MAGIC | `spark.streams.active` | List all active streaming queries |
# MAGIC | `df.isStreaming` | Check if a DataFrame is streaming |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Stop any remaining active queries
for q in spark.streams.active:
    q.stop()
    print(f"Stopped query: {q.name}")

# Remove temporary data
dbutils.fs.rm(BASE_PATH, recurse=True)
print("All temporary data cleaned up.")
print("Notebook complete.")
