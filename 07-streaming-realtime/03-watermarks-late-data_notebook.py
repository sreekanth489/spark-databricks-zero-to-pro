# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Watermarks & Late Data -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 03 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Creating event data with simulated late arrivals
# MAGIC 2. Windowed aggregation without watermark (state grows)
# MAGIC 3. Adding a watermark to handle late data
# MAGIC 4. Visualizing watermark progression
# MAGIC 5. Append mode with watermark for finalized windows
# MAGIC
# MAGIC **All examples are self-contained** -- no external data required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType
)
from datetime import datetime, timedelta
import time

BASE_PATH = "/tmp/module07_topic03"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"
SOURCE_PATH = f"{BASE_PATH}/events"

dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_PATH)
dbutils.fs.mkdirs(SOURCE_PATH)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Understanding Event Time vs Processing Time
# MAGIC
# MAGIC We generate events where the event_time may differ significantly from
# MAGIC the current time (processing time), simulating real-world delays.

# COMMAND ----------

# Create events with varying degrees of lateness
now = datetime.now()

events_data = [
    # On-time events (event_time close to processing time)
    (1, "click",    now - timedelta(seconds=5),   "user_001"),
    (2, "purchase", now - timedelta(seconds=3),   "user_002"),
    (3, "click",    now - timedelta(seconds=1),   "user_003"),
    # Slightly late events (seconds behind)
    (4, "click",    now - timedelta(seconds=30),  "user_004"),
    (5, "purchase", now - timedelta(seconds=45),  "user_001"),
    # Late events (minutes behind)
    (6, "click",    now - timedelta(minutes=3),   "user_005"),
    (7, "purchase", now - timedelta(minutes=5),   "user_002"),
    # Very late events (would be dropped by a 2-minute watermark)
    (8, "click",    now - timedelta(minutes=10),  "user_006"),
    (9, "purchase", now - timedelta(minutes=15),  "user_003"),
]

schema = StructType([
    StructField("event_id", IntegerType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_time", TimestampType(), False),
    StructField("user_id", StringType(), False),
])

df_events = spark.createDataFrame(events_data, schema=schema)

print("Events with their lateness:")
df_events.withColumn(
    "lateness_seconds",
    (F.current_timestamp().cast("long") - F.col("event_time").cast("long"))
).select("event_id", "event_type", "event_time", "lateness_seconds").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Windowed Aggregation WITHOUT Watermark
# MAGIC
# MAGIC Without a watermark, Spark keeps state for **every window** indefinitely.
# MAGIC This is safe for short demos but dangerous in production (OOM risk).

# COMMAND ----------

# Write events as a Delta table to use as a streaming source
EVENTS_DELTA = f"{BASE_PATH}/events_delta"
df_events.write.format("delta").mode("overwrite").save(EVENTS_DELTA)

# Read as a stream and perform windowed aggregation WITHOUT watermark
stream_no_wm = (
    spark.readStream
    .format("delta")
    .load(EVENTS_DELTA)
    .groupBy(
        F.window("event_time", "2 minutes"),  # 2-minute tumbling windows
        "event_type"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.collect_list("user_id").alias("users")
    )
)

# Must use complete or update mode (no watermark = no append for aggregations)
query_no_wm = (
    stream_no_wm.writeStream
    .format("memory")
    .queryName("no_watermark_agg")
    .outputMode("complete")
    .start()
)

time.sleep(10)

print("=== Windowed Aggregation WITHOUT Watermark ===")
print("All windows are maintained in state (nothing is ever cleaned up):\n")
spark.sql("""
    SELECT window.start, window.end, event_type, event_count, users
    FROM no_watermark_agg
    ORDER BY window.start, event_type
""").show(truncate=False)

query_no_wm.stop()
print("Query stopped.")
print("\nWARNING: In production, state for ALL windows would grow forever!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Windowed Aggregation WITH Watermark
# MAGIC
# MAGIC Adding `withWatermark()` tells Spark to drop state for windows that are
# MAGIC past the watermark threshold. This bounds memory usage.

# COMMAND ----------

# Read as a stream WITH watermark
stream_with_wm = (
    spark.readStream
    .format("delta")
    .load(EVENTS_DELTA)
    .withWatermark("event_time", "5 minutes")  # tolerate 5 minutes of lateness
    .groupBy(
        F.window("event_time", "2 minutes"),
        "event_type"
    )
    .agg(
        F.count("*").alias("event_count"),
        F.collect_list("user_id").alias("users")
    )
)

# With watermark, we CAN use append mode (emits finalized windows)
query_with_wm = (
    stream_with_wm.writeStream
    .format("memory")
    .queryName("with_watermark_agg")
    .outputMode("complete")  # using complete to see all windows for comparison
    .start()
)

time.sleep(10)

print("=== Windowed Aggregation WITH Watermark (5 min delay) ===")
print("Watermark allows Spark to eventually clean up old window state.\n")
spark.sql("""
    SELECT window.start, window.end, event_type, event_count, users
    FROM with_watermark_agg
    ORDER BY window.start, event_type
""").show(truncate=False)

query_with_wm.stop()
print("Query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Simulate Late Data Arrivals with Rate Source
# MAGIC
# MAGIC Using a rate source, we add synthetic event_time values that simulate
# MAGIC both on-time and late-arriving events to observe watermark behavior.

# COMMAND ----------

# Create a stream that simulates events with varying lateness
simulated_events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    # Simulate event_time: most events are on-time, some are late
    .withColumn(
        "event_time",
        F.when(F.col("value") % 10 == 0,
               # Every 10th event is 3 minutes late
               F.col("timestamp") - F.expr("INTERVAL 3 MINUTES"))
        .when(F.col("value") % 20 == 0,
               # Every 20th event is 8 minutes late
               F.col("timestamp") - F.expr("INTERVAL 8 MINUTES"))
        .otherwise(
               # Most events: 0-10 seconds late (normal jitter)
               F.col("timestamp") - (F.col("value") % 10).cast("long") * F.expr("INTERVAL 1 SECOND"))
    )
    .withColumn("event_type",
        F.when(F.col("value") % 3 == 0, "click")
         .when(F.col("value") % 3 == 1, "view")
         .otherwise("purchase"))
    .withColumn("lateness_sec",
        (F.col("timestamp").cast("long") - F.col("event_time").cast("long")))
    .select("event_time", "timestamp", "event_type", "value", "lateness_sec")
)

# Apply watermark and window aggregation
windowed_with_wm = (
    simulated_events
    .withWatermark("event_time", "5 minutes")
    .groupBy(
        F.window("event_time", "1 minute"),
        "event_type"
    )
    .agg(F.count("*").alias("cnt"))
)

query_sim_wm = (
    windowed_with_wm.writeStream
    .format("memory")
    .queryName("simulated_watermark")
    .outputMode("complete")
    .start()
)

# Let it run to accumulate some windows
time.sleep(15)

print("=== Simulated Events with Watermark ===")
spark.sql("""
    SELECT window.start, window.end, event_type, cnt
    FROM simulated_watermark
    ORDER BY window.start, event_type
""").show(20, truncate=False)

query_sim_wm.stop()
print("Simulated watermark query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Watermark Progression Tracking
# MAGIC
# MAGIC The streaming query progress contains watermark information.
# MAGIC Let us track how the watermark advances over time.

# COMMAND ----------

# Start a new query and track watermark progression
track_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumn("event_time", F.col("timestamp") - F.expr("INTERVAL 30 SECONDS"))
    .withWatermark("event_time", "2 minutes")
    .groupBy(F.window("event_time", "30 seconds"))
    .count()
)

query_track = (
    track_stream.writeStream
    .format("memory")
    .queryName("track_watermark")
    .outputMode("complete")
    .trigger(processingTime="3 seconds")
    .start()
)

# Track watermark progression across several batches
print("=== Watermark Progression ===")
print(f"{'Batch':>6} | {'Watermark':>25} | {'Input Rows':>11} | {'State Rows':>11}")
print("-" * 65)

for i in range(6):
    time.sleep(4)
    progress = query_track.lastProgress
    if progress:
        batch_id = progress.get("batchId", "?")
        num_input = progress.get("numInputRows", 0)
        # Extract watermark from eventTime info
        event_time_info = progress.get("eventTime", {})
        watermark = event_time_info.get("watermark", "N/A")
        # Extract state info
        state_ops = progress.get("stateOperators", [{}])
        num_rows_total = state_ops[0].get("numRowsTotal", "N/A") if state_ops else "N/A"
        print(f"{batch_id:>6} | {watermark:>25} | {num_input:>11} | {str(num_rows_total):>11}")

query_track.stop()
print("\nNote: The watermark advances as new (later) events arrive.")
print("State rows remain bounded because old windows are cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Append Mode with Watermark -- Finalized Windows
# MAGIC
# MAGIC In append mode with a watermark, Spark emits a window's result **only**
# MAGIC after the watermark passes the window's end time. This guarantees that
# MAGIC the output row will never need to be updated.

# COMMAND ----------

# Write finalized windows to a Delta table using append mode
APPEND_WM_SINK = f"{BASE_PATH}/append_wm_sink"

append_wm_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("event_time", F.col("timestamp") - F.expr("INTERVAL 10 SECONDS"))
    .withColumn("category",
        F.when(F.col("value") % 2 == 0, "even").otherwise("odd"))
    .withWatermark("event_time", "30 seconds")
    .groupBy(
        F.window("event_time", "10 seconds"),
        "category"
    )
    .agg(F.count("*").alias("event_count"))
)

query_append_wm = (
    append_wm_stream.writeStream
    .format("delta")
    .outputMode("append")  # only emits finalized (closed) windows
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/append_wm")
    .start(APPEND_WM_SINK)
)

# Let it run long enough for windows to finalize
# (watermark must pass window end time before output is emitted)
time.sleep(60)

query_append_wm.stop()

# Read the finalized windows
print("=== Finalized Windows (Append Mode + Watermark) ===")
df_finalized = spark.read.format("delta").load(APPEND_WM_SINK)
print(f"Total finalized windows written: {df_finalized.count()}")
df_finalized.orderBy("window.start", "category").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Watermark Delay Trade-Off Demonstration
# MAGIC
# MAGIC Shorter watermark = faster results but more late data dropped.
# MAGIC Longer watermark = more complete but higher memory and latency.

# COMMAND ----------

# Demonstrate the concept with a simple calculation
delays = [
    ("30 seconds", 30, "IoT sensors (reliable network)"),
    ("2 minutes", 120, "Web clickstream (normal latency)"),
    ("10 minutes", 600, "Mobile apps (intermittent connectivity)"),
    ("1 hour", 3600, "Cross-region data sync"),
    ("24 hours", 86400, "Daily batch uploads with streaming sink"),
]

delay_data = [(d, s, u) for d, s, u in delays]
df_delays = spark.createDataFrame(delay_data, schema=["watermark_delay", "seconds", "use_case"])

print("=== Watermark Delay Selection Guide ===")
df_delays.show(truncate=False)

# Estimate state size: 1-minute windows with different watermarks
print("Estimated active windows (1-min windows) by watermark delay:")
for delay_name, delay_sec, _ in delays:
    active_windows = delay_sec // 60 + 1
    print(f"  {delay_name:>12}: ~{active_windows} windows in state")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Common Watermark Mistakes

# COMMAND ----------

# Mistake 1: withWatermark on wrong column (not the event time column)
print("=== Common Watermark Mistakes ===\n")

print("1. Watermark on wrong column:")
print("   BAD:  .withWatermark('processing_time', '10 minutes')")
print("   GOOD: .withWatermark('event_time', '10 minutes')")
print("   The watermark column MUST be the event time used in the window.\n")

print("2. Watermark after groupBy:")
print("   BAD:  .groupBy(window('ts', '5 min')).count().withWatermark('ts', '10 min')")
print("   GOOD: .withWatermark('ts', '10 min').groupBy(window('ts', '5 min')).count()")
print("   withWatermark() must come BEFORE groupBy.\n")

print("3. Watermark delay too short:")
print("   Risk: Dropping too many valid late events")
print("   Monitor the 'numRowsDroppedByWatermark' metric in query progress.\n")

print("4. No watermark on aggregation with append mode:")
print("   Result: AnalysisException at query start")
print("   Fix: Add withWatermark() or use complete/update mode.\n")

print("5. Forgetting that watermark is approximate:")
print("   Events within the watermark delay MAY still be processed (best-effort)")
print("   Events beyond the watermark delay are GUARANTEED to be dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
    print(f"Stopped query: {q.name}")

dbutils.fs.rm(BASE_PATH, recurse=True)
print("All temporary data cleaned up.")
print("Notebook complete.")
