# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 19: Structured Streaming -- The Streaming Engine
# MAGIC
# MAGIC **Objective**: Master Spark Structured Streaming as the core stream processing engine
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Understand Structured Streaming as an ENGINE (vs Auto Loader as a SOURCE)
# MAGIC 2. Stream data from Delta tables
# MAGIC 3. Perform stream-static joins for data enrichment
# MAGIC 4. Stream from raw file sources (standard Spark, not Auto Loader)
# MAGIC 5. Compare trigger strategies: `availableNow` vs `processingTime`
# MAGIC 6. Demonstrate output modes: append vs complete
# MAGIC 7. Apply watermarking for late data handling
# MAGIC 8. Monitor and manage streaming queries
# MAGIC
# MAGIC **Key Distinction**:
# MAGIC ```
# MAGIC Structured Streaming = the streaming ENGINE
# MAGIC   - Micro-batch execution, checkpointing, fault tolerance, state management
# MAGIC   - spark.readStream / spark.writeStream
# MAGIC
# MAGIC Auto Loader = a specialized file ingestion SOURCE (Day 20)
# MAGIC   - Built ON TOP of Structured Streaming
# MAGIC   - spark.readStream.format("cloudFiles")
# MAGIC   - Optimized file discovery, schema inference, schema evolution
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**: See [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/blob/main/day00-environment-setup/00-databricks-aws-setup.md) for AWS + Databricks + S3 configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_lab
# MAGIC COMMENT 'Day 19: Structured Streaming engine lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA streaming_lab

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import (
    col, current_timestamp, from_unixtime, lit, window,
    count, sum as _sum, avg as _avg, max as _max, min as _min,
    round as _round, date_format, expr
)
import random
import time

base_path = "s3://databricks-zero-to-pro/streaming_lab"
checkpoint_path = f"{base_path}/checkpoints"
raw_files_path = f"{base_path}/raw_files"

print(f"Base path:    {base_path}")
print(f"Checkpoints:  {checkpoint_path}")
print(f"Raw files:    {raw_files_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Generate Sample Data
# MAGIC
# MAGIC Create source Delta tables that we will stream FROM.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Source Orders Table (will stream from this)

# COMMAND ----------

random.seed(42)

orders_data = []
base_ts = 1700000000  # Nov 2023
for i in range(1, 51):
    orders_data.append((
        f"ORD-{i:04d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice([f"P{j:03d}" for j in range(1, 9)]),
        random.randint(1, 5),
        float(random.randint(10, 200)),
        base_ts + random.randint(0, 86400 * 30),
        random.choice(["credit_card", "debit_card", "paypal"]),
    ))

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_ts", LongType(), True),
    StructField("payment_method", StringType(), True),
])

df_orders = spark.createDataFrame(orders_data, orders_schema)

# Write as a Delta table -- this will be our streaming source
(df_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("source_orders")
)

print(f"Source orders table: {df_orders.count()} records")
df_orders.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Lookup Table (static, for joins)

# COMMAND ----------

customers_data = [
    ("C001", "Alice", "Johnson", "New York", "Gold"),
    ("C002", "Bob", "Smith", "Los Angeles", "Silver"),
    ("C003", "Carol", "Williams", "Chicago", "Bronze"),
    ("C004", "David", "Brown", "Houston", "Gold"),
    ("C005", "Eve", "Davis", "Phoenix", "Silver"),
    ("C006", "Frank", "Miller", "Seattle", "Gold"),
    ("C007", "Grace", "Wilson", "Denver", "Bronze"),
    ("C008", "Henry", "Moore", "Boston", "Silver"),
]

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("tier", StringType(), True),
])

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_customers.write.format("delta").mode("overwrite").saveAsTable("customers_lookup")

print("Customers lookup table created")
df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Streaming from a Delta Table
# MAGIC
# MAGIC The most common streaming pattern: read from a Delta table as a stream.
# MAGIC Delta Lake tracks new data via its transaction log -- no directory listing needed.
# MAGIC
# MAGIC **This is the recommended source for downstream streaming in Medallion Architecture.**
# MAGIC (Bronze -> Silver, Silver -> Gold)

# COMMAND ----------

# Read the source orders table as a stream
df_orders_stream = (
    spark.readStream
        .format("delta")
        .table("source_orders")
)

# Apply simple transformations
df_bronze = (
    df_orders_stream
    .filter(col("quantity") > 0)
    .withColumn("load_time", current_timestamp())
    .withColumn("order_date", from_unixtime(col("event_ts"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
)

# Write to Bronze table with trigger(availableNow=True)
query_bronze = (
    df_bronze.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/bronze_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders")
)

query_bronze.awaitTermination()
print("Delta streaming to Bronze completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, customer_id, product_id, quantity, amount,
# MAGIC        order_date, payment_method, load_time
# MAGIC FROM bronze_orders
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental: Add New Data to Source, Re-stream
# MAGIC
# MAGIC When new rows are added to the source Delta table, the stream picks up ONLY the new rows.

# COMMAND ----------

# Add 10 more orders to the source table
random.seed(99)
new_orders = []
base_ts2 = 1700000000 + 86400 * 31
for i in range(51, 61):
    new_orders.append((
        f"ORD-{i:04d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice([f"P{j:03d}" for j in range(1, 9)]),
        random.randint(1, 5),
        float(random.randint(10, 200)),
        base_ts2 + random.randint(0, 86400 * 15),
        random.choice(["credit_card", "debit_card", "paypal"]),
    ))

df_new = spark.createDataFrame(new_orders, orders_schema)
df_new.write.format("delta").mode("append").saveAsTable("source_orders")
print(f"Added {df_new.count()} new orders to source table")

# COMMAND ----------

# Re-run stream with SAME checkpoint -- only processes new rows
query_bronze_incr = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .filter(col("quantity") > 0)
        .withColumn("load_time", current_timestamp())
        .withColumn("order_date", from_unixtime(col("event_ts"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/bronze_orders")  # same checkpoint
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders")
)

query_bronze_incr.awaitTermination()
print("Incremental streaming completed -- only new rows processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Should now have 50 + 10 = 60 records
# MAGIC SELECT COUNT(*) as total_records FROM bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Stream-Static Join
# MAGIC
# MAGIC A very common pattern: join a **streaming** DataFrame with a **static** (batch) DataFrame.
# MAGIC This is how you enrich streaming events with reference data (customer names, product details, etc.)
# MAGIC
# MAGIC The static side is re-read on each micro-batch, so it always reflects the latest data.

# COMMAND ----------

# Stream from Bronze
df_bronze_stream = (
    spark.readStream
        .format("delta")
        .table("bronze_orders")
)

# Static lookup (read as batch -- NOT streaming)
df_cust_static = spark.table("customers_lookup")

# Stream-static join: enrich orders with customer info
df_enriched = (
    df_bronze_stream
    .join(df_cust_static, "customer_id", "inner")
    .select(
        "order_id", "order_date", "customer_id",
        "first_name", "last_name", "city", "tier",
        "product_id", "quantity", "amount", "payment_method"
    )
)

# Write enriched data to Silver
query_silver = (
    df_enriched.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/silver_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("silver_orders")
)

query_silver.awaitTermination()
print("Stream-static join to Silver completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, order_date, first_name, last_name, city, tier,
# MAGIC        product_id, quantity, amount, payment_method
# MAGIC FROM silver_orders
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare record counts across layers
# MAGIC SELECT 'Source' as layer, COUNT(*) as records FROM source_orders
# MAGIC UNION ALL
# MAGIC SELECT 'Bronze' as layer, COUNT(*) as records FROM bronze_orders
# MAGIC UNION ALL
# MAGIC SELECT 'Silver' as layer, COUNT(*) as records FROM silver_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Streaming from Raw Files (Standard File Source)
# MAGIC
# MAGIC Structured Streaming can read from raw files using the standard file source.
# MAGIC This is the BASIC approach -- **not** Auto Loader.
# MAGIC
# MAGIC **Limitation**: Re-lists the entire directory on every trigger. Does not scale for millions of files.
# MAGIC For production file ingestion, use **Auto Loader** (Day 20).

# COMMAND ----------

# Write sample data as Parquet files to S3
df_file_data = spark.createDataFrame(orders_data[:20], orders_schema)
df_file_data.write.mode("overwrite").parquet(f"{raw_files_path}/orders/batch1")
print(f"Wrote {df_file_data.count()} records as Parquet files to S3")

# COMMAND ----------

# Standard file streaming (NOT Auto Loader)
# NOTE: Schema MUST be provided for file sources (no schema inference)
df_file_stream = (
    spark.readStream
        .format("parquet")
        .schema(orders_schema)          # schema required!
        .load(f"{raw_files_path}/orders/")
        .withColumn("load_time", current_timestamp())
)

query_file = (
    df_file_stream.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/file_source_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("file_source_orders")
)

query_file.awaitTermination()
print("Standard file streaming completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM file_source_orders LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Standard File Source vs Auto Loader
# MAGIC
# MAGIC | Feature | Standard File Source | Auto Loader (Day 20) |
# MAGIC |---------|---------------------|----------------------|
# MAGIC | Format | `format("parquet")` | `format("cloudFiles")` |
# MAGIC | Schema | Must provide manually | Auto-inferred and persisted |
# MAGIC | File discovery | Lists entire directory every trigger | Optimized (notifications or incremental) |
# MAGIC | Schema evolution | Not supported | Automatic |
# MAGIC | Millions of files | Slow / fails | Handles well |
# MAGIC | Rescue column | No | Yes (`_rescued_data`) |
# MAGIC
# MAGIC **For production file ingestion, always use Auto Loader (Day 20).**
# MAGIC Standard file source is shown here to understand the Structured Streaming engine itself.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Trigger Strategies
# MAGIC
# MAGIC Triggers control WHEN Structured Streaming processes data.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger 1: `availableNow=True` (Recommended for Scheduled Jobs)
# MAGIC
# MAGIC Processes ALL available data in multiple micro-batches, then stops.
# MAGIC This is what we have been using throughout this lab.

# COMMAND ----------

# availableNow: process all, then stop
query_avail = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .filter(col("quantity") > 0)
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/trigger_demo_avail")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("trigger_demo_avail")
)

query_avail.awaitTermination()
count_avail = spark.table("trigger_demo_avail").count()
print(f"availableNow completed: {count_avail} records processed, stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger 2: `processingTime` (Continuous Near-Real-Time)
# MAGIC
# MAGIC Runs micro-batches at fixed intervals. The stream stays running indefinitely.
# MAGIC We start it, let it run for a few seconds, then stop it.

# COMMAND ----------

# processingTime: micro-batch every 10 seconds (runs continuously)
query_continuous = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .filter(col("quantity") > 0)
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/trigger_demo_continuous")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("continuous_demo")
        .table("trigger_demo_continuous")
)

# Let it run for 15 seconds to see at least one micro-batch
print("Continuous stream started... waiting 15 seconds")
time.sleep(15)

# Check progress
if query_continuous.lastProgress:
    print(f"Last batch processed {query_continuous.lastProgress.get('numInputRows', 0)} rows")
print(f"Stream status: {query_continuous.status}")

# Stop the stream
query_continuous.stop()
query_continuous.awaitTermination()
print("Continuous stream stopped")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Comparison
# MAGIC
# MAGIC | Trigger | Behavior | Stops? | Use Case |
# MAGIC |---------|----------|--------|----------|
# MAGIC | `availableNow=True` | All available data, multiple micro-batches | Yes | Scheduled Workflows |
# MAGIC | `processingTime="30s"` | One micro-batch every 30 seconds | No | Continuous processing |
# MAGIC | `processingTime="0s"` | ASAP, back-to-back | No | Lowest latency |
# MAGIC | `once=True` | Single micro-batch | Yes | **Deprecated** (use `availableNow`) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Output Modes
# MAGIC
# MAGIC Output modes control WHAT data is written to the sink.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append Mode (default -- no aggregations)
# MAGIC
# MAGIC Only NEW rows are written. This is the most common mode for ETL pipelines.

# COMMAND ----------

# Append mode: each micro-batch adds only new rows
query_append = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .select("order_id", "customer_id", "quantity", "amount")
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/output_mode_append")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("output_mode_append")
)
query_append.awaitTermination()

count_append = spark.table("output_mode_append").count()
print(f"Append mode: {count_append} rows written (all new rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Complete Mode (aggregations -- full result rewritten)
# MAGIC
# MAGIC The ENTIRE result table is rewritten on each micro-batch.
# MAGIC Required for aggregations without watermark.

# COMMAND ----------

# Complete mode: full aggregation result rewritten each trigger
query_complete = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("order_count"),
            _sum("amount").alias("total_spent"),
            _avg("amount").alias("avg_order_value")
        )
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/output_mode_complete")
        .outputMode("complete")       # full result rewritten
        .trigger(availableNow=True)
        .table("output_mode_complete")
)
query_complete.awaitTermination()
print("Complete mode: aggregation result written")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Complete mode result: one row per customer with aggregates
# MAGIC SELECT customer_id, order_count, ROUND(total_spent, 2) as total_spent,
# MAGIC        ROUND(avg_order_value, 2) as avg_order_value
# MAGIC FROM output_mode_complete
# MAGIC ORDER BY total_spent DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Watermarking for Late Data
# MAGIC
# MAGIC Watermarking tells Structured Streaming how long to wait for late-arriving data
# MAGIC before finalizing aggregation results. Without watermark, Spark keeps ALL state forever.

# COMMAND ----------

# Stream with watermark and window aggregation
df_watermark_source = (
    spark.readStream
        .format("delta")
        .table("source_orders")
        .withColumn("order_date",
            from_unixtime(col("event_ts"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
)

# Apply watermark: allow data up to 1 day late
df_windowed = (
    df_watermark_source
    .withWatermark("order_date", "1 day")             # watermark threshold
    .groupBy(
        window("order_date", "1 day"),                 # 1-day tumbling window
        "customer_id"
    )
    .agg(
        count("order_id").alias("orders_in_window"),
        _sum("amount").alias("window_total"),
    )
)

query_watermark = (
    df_windowed.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/watermark_demo")
        .outputMode("append")          # append works with watermark + window
        .trigger(availableNow=True)
        .table("watermark_demo")
)

query_watermark.awaitTermination()
print("Watermark aggregation completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Windowed aggregation results
# MAGIC SELECT window.start as window_start, window.end as window_end,
# MAGIC        customer_id, orders_in_window, ROUND(window_total, 2) as window_total
# MAGIC FROM watermark_demo
# MAGIC ORDER BY window_start DESC, customer_id
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Watermarking Works
# MAGIC
# MAGIC ```
# MAGIC Max event time seen: 2023-11-30 12:00:00
# MAGIC Watermark threshold: 1 day
# MAGIC Watermark boundary:  2023-11-29 12:00:00
# MAGIC
# MAGIC Data with event_time >= 2023-11-29 12:00:00  →  INCLUDED
# MAGIC Data with event_time <  2023-11-29 12:00:00  →  MAY BE DROPPED
# MAGIC ```
# MAGIC
# MAGIC **Without watermark**: State grows forever (memory risk)
# MAGIC
# MAGIC **With watermark**: Old state is discarded, memory stays bounded

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 8: Stream Monitoring

# COMMAND ----------

# Check all active streaming queries
active_streams = spark.streams.active
print(f"Currently active streams: {len(active_streams)}")

for stream in active_streams:
    print(f"\n  Name: {stream.name}")
    print(f"  ID: {stream.id}")
    print(f"  Status: {stream.status}")
    if stream.lastProgress:
        progress = stream.lastProgress
        print(f"  Input rows: {progress.get('numInputRows', 'N/A')}")
        print(f"  Input rate: {progress.get('inputRowsPerSecond', 'N/A')} rows/sec")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Table History (shows streaming writes)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: see Bronze before incremental data was added
# MAGIC SELECT COUNT(*) as records_v0 FROM bronze_orders VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT COUNT(*) as records_current FROM bronze_orders

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 9: Structured Streaming vs Auto Loader
# MAGIC
# MAGIC ### The Relationship
# MAGIC
# MAGIC ```
# MAGIC S3 / ADLS / GCS
# MAGIC       |
# MAGIC       v
# MAGIC Auto Loader (cloudFiles)          <-- Specialized SOURCE (Day 20)
# MAGIC       |
# MAGIC       v
# MAGIC Spark Structured Streaming        <-- ENGINE (this session)
# MAGIC       |
# MAGIC       v
# MAGIC Transformations
# MAGIC       |
# MAGIC       v
# MAGIC Delta Lake
# MAGIC ```
# MAGIC
# MAGIC | | Structured Streaming | Auto Loader |
# MAGIC |---|---------------------|-------------|
# MAGIC | **What** | Stream processing engine | File ingestion source |
# MAGIC | **API** | `spark.readStream` / `writeStream` | `spark.readStream.format("cloudFiles")` |
# MAGIC | **Handles** | Execution, checkpointing, state | File discovery, schema inference |
# MAGIC | **Is a streaming engine?** | Yes | No -- uses Structured Streaming |
# MAGIC
# MAGIC ### Side-by-Side Code Comparison
# MAGIC
# MAGIC **Standard file source** (Structured Streaming only -- what we used in Step 4):
# MAGIC ```python
# MAGIC df = spark.readStream.format("parquet").schema(my_schema).load("/data")
# MAGIC ```
# MAGIC
# MAGIC **Auto Loader** (Structured Streaming + optimized file discovery -- Day 20):
# MAGIC ```python
# MAGIC df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("s3://bucket/data")
# MAGIC ```
# MAGIC
# MAGIC Both use the same Structured Streaming engine underneath.
# MAGIC Auto Loader adds optimized file discovery, schema inference, and schema evolution.
# MAGIC
# MAGIC **In Medallion Architecture**:
# MAGIC - **Auto Loader** is typically used at the Bronze layer (S3 -> Bronze)
# MAGIC - **Structured Streaming** from Delta is used for Silver and Gold (Bronze -> Silver -> Gold)
# MAGIC
# MAGIC See **Day 20: Auto Loader** for hands-on labs with `cloudFiles`.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | What We Learned |
# MAGIC |---------|-----------------|
# MAGIC | **Delta streaming** | `readStream.format("delta")` -- recommended source |
# MAGIC | **File streaming** | `readStream.format("parquet")` -- basic, requires schema |
# MAGIC | **Stream-static join** | Enrich streams with batch lookup tables |
# MAGIC | **Triggers** | `availableNow` for scheduled, `processingTime` for continuous |
# MAGIC | **Output modes** | `append` for ETL, `complete` for aggregations |
# MAGIC | **Watermarking** | Bounds state, handles late data |
# MAGIC | **Checkpoints** | Enable exactly-once, never share or delete |
# MAGIC | **Auto Loader** | A SOURCE built on this ENGINE (Day 20) |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Stop any active streams
for stream in spark.streams.active:
    print(f"Stopping stream: {stream.id}")
    stream.stop()
    stream.awaitTermination()
print("All streams stopped")

# COMMAND ----------

# Drop all tables
tables = [
    "source_orders", "customers_lookup", "bronze_orders", "silver_orders",
    "file_source_orders", "trigger_demo_avail", "trigger_demo_continuous",
    "output_mode_append", "output_mode_complete", "watermark_demo"
]
for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

# COMMAND ----------

# Remove S3 data
dbutils.fs.rm(base_path, recurse=True)
print(f"Removed: {base_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS streaming_lab CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 20**: [Auto Loader](../day20-auto-loader/) -- the optimized file ingestion source built on Structured Streaming
# MAGIC   - Three modes: directory listing, managed file events, classic notifications
# MAGIC   - Schema inference and evolution
# MAGIC   - IAM setup and troubleshooting for AWS
