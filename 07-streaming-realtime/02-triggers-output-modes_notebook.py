# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Triggers & Output Modes -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 02 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Append output mode with non-aggregating transformations
# MAGIC 2. Complete output mode with aggregations
# MAGIC 3. Update output mode with aggregations
# MAGIC 4. Fixed interval trigger configuration
# MAGIC 5. trigger.availableNow for process-and-stop pipelines
# MAGIC 6. Comparison of trigger.once vs trigger.availableNow
# MAGIC
# MAGIC **All examples are self-contained** -- no external data required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time

BASE_PATH = "/tmp/module07_topic02"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

# Clean up from previous runs
dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_PATH)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Append Mode -- Non-Aggregating Stream
# MAGIC
# MAGIC Append mode writes only new rows to the sink. It is the default mode and
# MAGIC works naturally for pipelines without aggregations (map, filter, select).

# COMMAND ----------

# Create a rate stream and apply non-aggregating transformations
append_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", F.when(F.col("value") % 3 == 0, "A")
                             .when(F.col("value") % 3 == 1, "B")
                             .otherwise("C"))
    .filter(F.col("value") % 2 == 0)  # keep only even values
    .select("timestamp", "value", "category")
)

# Write with APPEND mode (default)
query_append = (
    append_stream.writeStream
    .format("memory")
    .queryName("append_demo")
    .outputMode("append")
    .start()
)

time.sleep(8)

print("=== Append Mode Results ===")
spark.sql("SELECT * FROM append_demo ORDER BY timestamp DESC LIMIT 10").show(truncate=False)
total = spark.sql("SELECT COUNT(*) AS total FROM append_demo").collect()[0]["total"]
print(f"Total rows (grows with each batch): {total}")

query_append.stop()
print("Append query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Complete Mode -- Aggregation with Full Output
# MAGIC
# MAGIC Complete mode outputs the **entire result table** on every trigger.
# MAGIC Required when you need the full aggregated state (e.g., dashboards).

# COMMAND ----------

# Create a rate stream with aggregation
complete_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", F.when(F.col("value") % 3 == 0, "A")
                             .when(F.col("value") % 3 == 1, "B")
                             .otherwise("C"))
    .groupBy("category")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("value").alias("total_value"),
        F.avg("value").alias("avg_value")
    )
)

# Write with COMPLETE mode -- entire result table every batch
query_complete = (
    complete_stream.writeStream
    .format("memory")
    .queryName("complete_demo")
    .outputMode("complete")
    .start()
)

# Check results at two different points in time
time.sleep(5)
print("=== Complete Mode -- Snapshot 1 ===")
spark.sql("SELECT * FROM complete_demo ORDER BY category").show()

time.sleep(5)
print("=== Complete Mode -- Snapshot 2 (values updated) ===")
spark.sql("SELECT * FROM complete_demo ORDER BY category").show()

query_complete.stop()
print("Complete query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Why Append Mode Fails with Aggregations (No Watermark)
# MAGIC
# MAGIC Append mode guarantees that once a row is output, it will never change.
# MAGIC Aggregations without watermarks can always change, so Spark rejects this.

# COMMAND ----------

# This WILL raise an AnalysisException
try:
    bad_query = (
        spark.readStream
        .format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .groupBy(F.window("timestamp", "10 seconds"))
        .count()
        .writeStream
        .format("memory")
        .queryName("bad_append_agg")
        .outputMode("append")  # append + aggregation without watermark
        .start()
    )
    time.sleep(3)
    bad_query.stop()
except Exception as e:
    print("EXPECTED ERROR with append + aggregation (no watermark):")
    print(f"  {type(e).__name__}: {str(e)[:200]}")
    print("\nFix: Use complete/update mode, or add a watermark for append mode.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Update Mode -- Only Changed Rows
# MAGIC
# MAGIC Update mode outputs only the rows that changed since the last trigger.
# MAGIC More efficient than complete mode when only a subset of aggregations change.

# COMMAND ----------

# Same aggregation as before, but with UPDATE mode
update_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", F.when(F.col("value") % 3 == 0, "A")
                             .when(F.col("value") % 3 == 1, "B")
                             .otherwise("C"))
    .groupBy("category")
    .agg(F.count("*").alias("row_count"))
)

# Write with UPDATE mode
query_update = (
    update_stream.writeStream
    .format("memory")
    .queryName("update_demo")
    .outputMode("update")
    .start()
)

time.sleep(8)

print("=== Update Mode Results ===")
print("Note: Memory sink accumulates all outputs, so this looks like complete mode.")
print("With a foreachBatch sink, only changed rows would be processed per batch.\n")
spark.sql("SELECT * FROM update_demo ORDER BY category").show()

query_update.stop()
print("Update query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Fixed Interval Trigger
# MAGIC
# MAGIC Process a micro-batch every N seconds/minutes. Useful for controlling
# MAGIC resource usage and downstream write frequency.

# COMMAND ----------

interval_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .select("timestamp", "value")
)

# Trigger every 5 seconds
query_interval = (
    interval_stream.writeStream
    .format("memory")
    .queryName("interval_demo")
    .outputMode("append")
    .trigger(processingTime="5 seconds")
    .start()
)

# Observe batch progression
for i in range(4):
    time.sleep(5)
    progress = query_interval.lastProgress
    if progress:
        batch_id = progress.get("batchId", "N/A")
        num_rows = progress.get("numInputRows", 0)
        print(f"Batch {batch_id}: processed {num_rows} rows")

total = spark.sql("SELECT COUNT(*) AS c FROM interval_demo").collect()[0]["c"]
print(f"\nTotal rows after ~20 seconds: {total}")

query_interval.stop()
print("Interval query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: trigger.availableNow -- Process and Stop
# MAGIC
# MAGIC `trigger(availableNow=True)` processes all available data across multiple
# MAGIC micro-batches (respecting rate limits), then stops automatically.
# MAGIC This is the recommended pattern for scheduled streaming jobs.

# COMMAND ----------

# First, generate some Delta data to serve as the streaming source
DELTA_SOURCE = f"{BASE_PATH}/delta_source"
DELTA_SINK_AVAIL = f"{BASE_PATH}/delta_sink_available_now"

sample_data = [(i, f"user_{i % 10:03d}", float(i * 2.5)) for i in range(200)]
df_source = spark.createDataFrame(sample_data, schema=["id", "user_id", "amount"])
df_source.write.format("delta").mode("overwrite").save(DELTA_SOURCE)
print(f"Source Delta table written: {df_source.count()} rows")

# Now read as a stream with rate limiting and trigger.availableNow
query_avail = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)  # rate limit: 1 file per micro-batch
    .load(DELTA_SOURCE)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/avail_now")
    .trigger(availableNow=True)
    .start(DELTA_SINK_AVAIL)
)

# Wait for the query to finish (it auto-stops after processing all data)
query_avail.awaitTermination()
print("trigger.availableNow completed!")

# Verify results
sink_count = spark.read.format("delta").load(DELTA_SINK_AVAIL).count()
print(f"Rows written to sink: {sink_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: trigger.once vs trigger.availableNow Comparison
# MAGIC
# MAGIC The key difference: `trigger.once` ignores rate limits and processes
# MAGIC everything in a single batch. `trigger.availableNow` respects rate limits.

# COMMAND ----------

# --- trigger.once: single batch, ignores maxFilesPerTrigger ---
ONCE_SINK = f"{BASE_PATH}/delta_sink_once"

query_once = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 1)  # THIS IS IGNORED by trigger.once
    .load(DELTA_SOURCE)
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/once")
    .trigger(once=True)
    .start(ONCE_SINK)
)

query_once.awaitTermination()
once_count = spark.read.format("delta").load(ONCE_SINK).count()
print(f"trigger.once -- rows in sink: {once_count}")
print("trigger.once processed ALL data in a single batch (ignoring maxFilesPerTrigger).\n")

# --- Summary ---
print("=" * 60)
print("COMPARISON SUMMARY")
print("=" * 60)
print(f"  trigger.availableNow : {sink_count} rows across MULTIPLE batches")
print(f"  trigger.once         : {once_count} rows in ONE batch")
print()
print("  Both processed the same data, but availableNow respects")
print("  rate limits (maxFilesPerTrigger), preventing OOM on large backlogs.")
print()
print("  RECOMMENDATION: Always use trigger(availableNow=True) instead of")
print("  trigger(once=True) for production scheduled pipelines.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Output Mode Behavior Summary
# MAGIC
# MAGIC Quick reference for choosing the right output mode.

# COMMAND ----------

summary = [
    ("append", "Non-aggregating ETL", "New rows only", "Yes (default)", "No*"),
    ("append", "Windowed agg + watermark", "Finalized windows", "Yes", "Yes"),
    ("complete", "Global aggregation", "Entire result table", "Yes", "No"),
    ("complete", "Dashboard refresh", "Full result every batch", "Yes", "No"),
    ("update", "Aggregation + upsert sink", "Changed rows only", "Yes", "No"),
    ("update", "Non-aggregating (same as append)", "New rows only", "Yes", "No"),
]

columns = ["output_mode", "use_case", "rows_written", "supports_agg", "needs_watermark"]
df_summary = spark.createDataFrame(summary, schema=columns)
df_summary.show(truncate=False)

print("* append + aggregation without watermark throws AnalysisException")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Production Pattern -- availableNow + Workflows
# MAGIC
# MAGIC This cell demonstrates the recommended pattern for cost-effective
# MAGIC streaming on Databricks: scheduled job using trigger.availableNow.

# COMMAND ----------

# This is the pattern you would use in a Databricks Workflow (scheduled job)
# The notebook is triggered on a schedule (e.g., every 15 minutes)

PRODUCTION_SOURCE = f"{BASE_PATH}/prod_source"
PRODUCTION_SINK = f"{BASE_PATH}/prod_sink"

# Simulate source data (in production, new files arrive between scheduled runs)
prod_data = [(i, f"event_{i}", float(i * 3.14)) for i in range(500)]
df_prod = spark.createDataFrame(prod_data, schema=["event_id", "event_type", "value"])
df_prod.write.format("delta").mode("overwrite").save(PRODUCTION_SOURCE)

# The actual pipeline: read all available, process, stop
# In a Workflow, this notebook runs on a job cluster that terminates after completion
query_prod = (
    spark.readStream
    .format("delta")
    .option("maxFilesPerTrigger", 2)  # process 2 files per micro-batch
    .load(PRODUCTION_SOURCE)
    .withColumn("processed_at", F.current_timestamp())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/prod")
    .trigger(availableNow=True)
    .start(PRODUCTION_SINK)
)

query_prod.awaitTermination()

result_count = spark.read.format("delta").load(PRODUCTION_SINK).count()
print(f"Production pattern completed: {result_count} rows processed")
print("In production, this notebook would be scheduled via Databricks Workflows.")
print("Job cluster spins up -> processes available data -> terminates.")
print("Cost: you pay only for the compute time used, not 24/7 cluster uptime.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Stop any remaining queries
for q in spark.streams.active:
    q.stop()
    print(f"Stopped: {q.name}")

# Remove all temporary data
dbutils.fs.rm(BASE_PATH, recurse=True)
print("Cleanup complete.")
