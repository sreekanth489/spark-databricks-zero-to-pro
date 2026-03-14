# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 19: Structured Streaming & Auto Loader
# MAGIC
# MAGIC **Objective**: Master Auto Loader for incremental file ingestion from AWS S3 with both file notification and directory listing modes
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Set up Unity Catalog schema and S3 paths
# MAGIC 2. Generate sample data files to simulate data landing in S3
# MAGIC 3. **Scenario 1**: Auto Loader with **directory listing mode** (zero infrastructure setup)
# MAGIC 4. **Scenario 2**: Auto Loader with **file notification mode** (S3 + SQS for near-real-time)
# MAGIC 5. Demonstrate incremental processing with new file batches
# MAGIC 6. Handle schema inference, evolution, and rescue columns
# MAGIC 7. Compare trigger strategies: `availableNow` vs `processingTime`
# MAGIC 8. Monitor and manage streaming queries
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Files land in S3
# MAGIC       |
# MAGIC       v
# MAGIC Auto Loader (cloudFiles)
# MAGIC   |                    |
# MAGIC   v                    v
# MAGIC Directory Listing    File Notification
# MAGIC (polls S3 dir)       (S3 -> SQS events)
# MAGIC   |                    |
# MAGIC   v                    v
# MAGIC Bronze Delta Table (append-only, with metadata)
# MAGIC       |
# MAGIC       v
# MAGIC Silver / Gold layers (downstream)
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Unity Catalog, S3 Paths, and Imports

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS streaming_lab
# MAGIC COMMENT 'Day 19: Structured Streaming and Auto Loader lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA streaming_lab

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, from_unixtime,
    lit, round as _round, date_format, to_timestamp
)
from delta.tables import DeltaTable
import random
import time

# S3 paths for this lab
base_path = "s3://databricks-zero-to-pro/streaming_lab"
raw_data_path = f"{base_path}/raw"
bronze_path = f"{base_path}/bronze"
checkpoint_path = f"{base_path}/checkpoints"
schema_path = f"{base_path}/schemas"
bad_records_path = f"{base_path}/bad_records"

print("Streaming Lab Storage Layout (AWS S3)")
print("=" * 50)
print(f"Raw data:     {raw_data_path}")
print(f"Bronze:       {bronze_path}")
print(f"Checkpoints:  {checkpoint_path}")
print(f"Schemas:      {schema_path}")
print(f"Bad records:  {bad_records_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Generate Sample Data Files
# MAGIC
# MAGIC We simulate data landing in S3 by writing multiple batches of Parquet and JSON files.
# MAGIC Each batch represents a new set of files arriving at different times.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet Files (for Scenario 1 - Directory Listing)

# COMMAND ----------

# Schema for order events
orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_timestamp", LongType(), True),
    StructField("payment_method", StringType(), True),
])

# Batch 1: initial orders
random.seed(42)
orders_batch1 = []
base_ts = 1700000000
for i in range(1, 31):
    orders_batch1.append((
        f"ORD-{i:04d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice([f"P{j:03d}" for j in range(1, 9)]),
        random.randint(1, 5),
        base_ts + random.randint(0, 86400 * 30),
        random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ))

df_batch1 = spark.createDataFrame(orders_batch1, orders_schema)
df_batch1.write.mode("overwrite").parquet(f"{raw_data_path}/parquet_orders/batch1")
print(f"Batch 1: {df_batch1.count()} orders written as Parquet")
df_batch1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Files (for Scenario 2 - File Notification)

# COMMAND ----------

# Batch 1: JSON events (clickstream-style data)
random.seed(100)
events_batch1 = []
for i in range(1, 26):
    events_batch1.append((
        f"EVT-{i:05d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice(["page_view", "add_to_cart", "checkout", "purchase", "search"]),
        random.choice(["homepage", "product_detail", "cart", "checkout", "search_results"]),
        random.choice(["mobile", "desktop", "tablet"]),
        f"session-{random.randint(1000, 9999)}",
        base_ts + random.randint(0, 86400 * 7),
    ))

events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_timestamp", LongType(), True),
])

df_events_batch1 = spark.createDataFrame(events_batch1, events_schema)
df_events_batch1.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch1")
print(f"Events Batch 1: {df_events_batch1.count()} events written as JSON")
df_events_batch1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 1: Auto Loader with Directory Listing Mode
# MAGIC
# MAGIC **Directory listing** is the default mode. Auto Loader:
# MAGIC 1. Scans the S3 directory for files
# MAGIC 2. Compares against files already processed (tracked in checkpoint)
# MAGIC 3. Reads only new/unprocessed files
# MAGIC 4. Uses incremental listing optimization on subsequent runs
# MAGIC
# MAGIC **Advantages**: Zero infrastructure setup, works immediately
# MAGIC
# MAGIC **Best for**: Development, moderate file volumes, scheduled batch ingestion

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Basic Auto Loader - Directory Listing (Parquet)
# MAGIC
# MAGIC This is the simplest way to start with Auto Loader.
# MAGIC No SQS, no SNS, no event notifications -- just point to S3 and go.

# COMMAND ----------

# Auto Loader with directory listing mode (default)
df_stream_dir = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")        # explicit: directory listing mode
        .option("cloudFiles.includeExistingFiles", "true")     # process existing files on first run
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_orders")
        .load(f"{raw_data_path}/parquet_orders/")
        # Add ingestion metadata
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
)

# Write to Bronze table using trigger(availableNow=True)
# This processes all available files, then stops -- perfect for scheduled jobs
query_dir = (
    df_stream_dir.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/dir_listing_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders_dir_listing")
)

# Wait for the stream to complete
query_dir.awaitTermination()
print("Directory listing stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze table: raw records with metadata from Auto Loader
# MAGIC SELECT order_id, customer_id, product_id, quantity, payment_method,
# MAGIC        load_time, source_file
# MAGIC FROM bronze_orders_dir_listing
# MAGIC ORDER BY order_id
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM bronze_orders_dir_listing

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Incremental Ingestion - Add New Files
# MAGIC
# MAGIC Now simulate new data arriving in S3. Auto Loader will detect and process ONLY the new files.

# COMMAND ----------

# Generate Batch 2: new orders
random.seed(55)
orders_batch2 = []
base_ts2 = 1700000000 + 86400 * 31
for i in range(31, 51):
    orders_batch2.append((
        f"ORD-{i:04d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice([f"P{j:03d}" for j in range(1, 9)]),
        random.randint(1, 5),
        base_ts2 + random.randint(0, 86400 * 15),
        random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ))

df_batch2 = spark.createDataFrame(orders_batch2, orders_schema)
df_batch2.write.mode("overwrite").parquet(f"{raw_data_path}/parquet_orders/batch2")
print(f"Batch 2: {df_batch2.count()} new orders written to S3")

# COMMAND ----------

# Re-run the SAME Auto Loader stream -- it will only process batch2 (new files)
df_stream_dir_incr = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_orders")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
)

query_dir_incr = (
    df_stream_dir_incr.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/dir_listing_bronze")  # same checkpoint!
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders_dir_listing")
)

query_dir_incr.awaitTermination()
print("Incremental ingestion completed -- only new files processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: count should now include batch 1 + batch 2
# MAGIC SELECT COUNT(*) as total_records,
# MAGIC        COUNT(DISTINCT source_file) as unique_source_files
# MAGIC FROM bronze_orders_dir_listing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See which source files were ingested
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_orders_dir_listing
# MAGIC GROUP BY source_file
# MAGIC ORDER BY ingested_at

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c. Directory Listing - Advanced Options
# MAGIC
# MAGIC Control ingestion rate, file filtering, and subdirectory scanning.

# COMMAND ----------

# Advanced directory listing with rate limiting and file filtering
df_stream_advanced = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        # Rate limiting: prevent overwhelming downstream systems
        .option("cloudFiles.maxFilesPerTrigger", "100")        # max files per micro-batch
        .option("cloudFiles.maxBytesPerTrigger", "1g")         # max bytes per micro-batch
        # File filtering
        .option("pathGlobFilter", "*.parquet")                 # only parquet files
        .option("recursiveFileLookup", "true")                 # scan subdirectories
        # Schema
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_advanced")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
)

# Preview the stream schema (does not start processing)
print("Stream schema:")
df_stream_advanced.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Scenario 2: Auto Loader with File Notification Mode (S3 + SQS)
# MAGIC
# MAGIC **File notification mode** uses AWS infrastructure for near-real-time file detection:
# MAGIC ```
# MAGIC S3 Bucket Event -> SNS Topic -> SQS Queue -> Auto Loader
# MAGIC ```
# MAGIC
# MAGIC **Advantages**: Near-real-time (seconds), scales to millions of files, lower S3 API costs
# MAGIC
# MAGIC **Best for**: Production workloads, high-volume ingestion, low-latency requirements
# MAGIC
# MAGIC ### How it works:
# MAGIC 1. When a new file lands in S3, an event notification is sent
# MAGIC 2. The event is published to an SNS topic
# MAGIC 3. An SQS queue subscribed to the topic receives the message
# MAGIC 4. Auto Loader polls the SQS queue and reads only the new file(s)
# MAGIC
# MAGIC **Two setup options**:
# MAGIC - **Auto-setup**: Databricks creates SNS/SQS resources (requires broad IAM permissions)
# MAGIC - **Pre-configured**: You provide existing SQS queue URL (recommended for production)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. File Notification - Auto Setup
# MAGIC
# MAGIC Databricks automatically creates SNS topic, SQS queue, and S3 event notifications.
# MAGIC Requires IAM permissions for `sns:*`, `sqs:*`, and `s3:PutBucketNotification`.
# MAGIC
# MAGIC **Note**: This cell requires appropriate IAM permissions. If you don't have them,
# MAGIC skip to section 2b for the pre-configured approach, or use directory listing (Scenario 1).

# COMMAND ----------

# Auto Loader with file notification mode - auto setup
# Databricks will create SNS topic, SQS queue, and S3 event notification
df_stream_notify_auto = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "true")           # enable file notification mode
        # Schema handling for JSON (more important since JSON has no embedded schema)
        .option("cloudFiles.inferColumnTypes", "true")           # infer actual types, not just strings
        .option("cloudFiles.schemaLocation", f"{schema_path}/notification_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # auto-add new columns
        .load(f"{raw_data_path}/json_events/")
        # Add metadata
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
)

# Write to Bronze with trigger(availableNow=True) for batch-style processing
query_notify_auto = (
    df_stream_notify_auto.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/notification_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_notification")
)

query_notify_auto.awaitTermination()
print("File notification (auto-setup) stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_events_notification LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_events FROM bronze_events_notification

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. File Notification - Pre-Configured SQS Queue (Production Recommended)
# MAGIC
# MAGIC In production, you typically pre-create the SQS queue and S3 notifications
# MAGIC through Terraform/CloudFormation, then pass the queue URL to Auto Loader.
# MAGIC
# MAGIC This gives you full control over IAM, encryption, dead-letter queues, and lifecycle.
# MAGIC
# MAGIC ```python
# MAGIC # Production pattern with pre-configured SQS queue
# MAGIC df_stream_preconfigured = (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.useNotifications", "true")
# MAGIC         # Point to your pre-configured SQS queue
# MAGIC         .option("cloudFiles.queueUrl",
# MAGIC                 "https://sqs.us-east-1.amazonaws.com/123456789012/my-autoloader-queue")
# MAGIC         # Optional: specify the region if different from workspace
# MAGIC         .option("cloudFiles.region", "us-east-1")
# MAGIC         # Schema
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .option("cloudFiles.schemaLocation", "s3://bucket/schemas/events")
# MAGIC         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC         .load("s3://my-data-bucket/raw/events/")
# MAGIC         .withColumn("load_time", current_timestamp())
# MAGIC         .withColumn("source_file", input_file_name())
# MAGIC )
# MAGIC
# MAGIC query_preconfigured = (
# MAGIC     df_stream_preconfigured.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://bucket/checkpoints/events_bronze")
# MAGIC         .outputMode("append")
# MAGIC         .trigger(processingTime="30 seconds")  # continuous near-real-time
# MAGIC         .table("bronze_events")
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### Terraform Example for SQS + S3 Notifications
# MAGIC
# MAGIC ```hcl
# MAGIC # SQS Queue for Auto Loader
# MAGIC resource "aws_sqs_queue" "autoloader_queue" {
# MAGIC   name                       = "autoloader-events-queue"
# MAGIC   visibility_timeout_seconds = 300
# MAGIC   message_retention_seconds  = 86400
# MAGIC
# MAGIC   policy = jsonencode({
# MAGIC     Version = "2012-10-17"
# MAGIC     Statement = [{
# MAGIC       Effect    = "Allow"
# MAGIC       Principal = { Service = "s3.amazonaws.com" }
# MAGIC       Action    = "SQS:SendMessage"
# MAGIC       Resource  = "arn:aws:sqs:us-east-1:123456789012:autoloader-events-queue"
# MAGIC       Condition = {
# MAGIC         ArnEquals = {
# MAGIC           "aws:SourceArn" = aws_s3_bucket.data_bucket.arn
# MAGIC         }
# MAGIC       }
# MAGIC     }]
# MAGIC   })
# MAGIC }
# MAGIC
# MAGIC # S3 Bucket Notification -> SQS
# MAGIC resource "aws_s3_bucket_notification" "autoloader_notification" {
# MAGIC   bucket = aws_s3_bucket.data_bucket.id
# MAGIC
# MAGIC   queue {
# MAGIC     queue_arn     = aws_sqs_queue.autoloader_queue.arn
# MAGIC     events        = ["s3:ObjectCreated:*"]
# MAGIC     filter_prefix = "raw/events/"
# MAGIC   }
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2c. Incremental Ingestion with File Notifications
# MAGIC
# MAGIC Add new JSON files and verify that only new files are processed.

# COMMAND ----------

# Generate Events Batch 2
random.seed(200)
events_batch2 = []
for i in range(26, 51):
    events_batch2.append((
        f"EVT-{i:05d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice(["page_view", "add_to_cart", "checkout", "purchase", "search"]),
        random.choice(["homepage", "product_detail", "cart", "checkout", "search_results"]),
        random.choice(["mobile", "desktop", "tablet"]),
        f"session-{random.randint(1000, 9999)}",
        base_ts + random.randint(86400 * 7, 86400 * 14),
    ))

df_events_batch2 = spark.createDataFrame(events_batch2, events_schema)
df_events_batch2.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch2")
print(f"Events Batch 2: {df_events_batch2.count()} new events written to S3")

# COMMAND ----------

# Re-run with same checkpoint -- only processes batch2
query_notify_incr = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/notification_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/json_events/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/notification_bronze")  # same checkpoint
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_notification")
)

query_notify_incr.awaitTermination()
print("Incremental notification ingestion completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify both batches ingested
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_events_notification
# MAGIC GROUP BY source_file
# MAGIC ORDER BY ingested_at

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_events FROM bronze_events_notification

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Schema Inference, Evolution, and Rescue Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Inference
# MAGIC
# MAGIC Auto Loader infers the schema from source files and persists it to `schemaLocation`.
# MAGIC On subsequent runs, the persisted schema is used (no re-inference needed).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the inferred schema of the notification table
# MAGIC DESCRIBE bronze_events_notification

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Evolution - Adding New Columns
# MAGIC
# MAGIC When source data adds new fields, Auto Loader can handle it automatically.
# MAGIC Let's simulate adding a new `referrer` field to event data.

# COMMAND ----------

# Generate Batch 3 with a NEW column: referrer
random.seed(300)
events_batch3 = []
for i in range(51, 66):
    events_batch3.append((
        f"EVT-{i:05d}",
        random.choice([f"C{j:03d}" for j in range(1, 9)]),
        random.choice(["page_view", "add_to_cart", "checkout", "purchase"]),
        random.choice(["homepage", "product_detail", "cart", "checkout"]),
        random.choice(["mobile", "desktop", "tablet"]),
        f"session-{random.randint(1000, 9999)}",
        base_ts + random.randint(86400 * 14, 86400 * 21),
        random.choice(["google", "facebook", "direct", "email", "twitter"]),  # NEW FIELD
    ))

# Schema with new column
events_schema_v2 = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("referrer", StringType(), True),          # NEW COLUMN
])

df_events_batch3 = spark.createDataFrame(events_batch3, events_schema_v2)
df_events_batch3.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch3")
print(f"Events Batch 3: {df_events_batch3.count()} events with NEW 'referrer' column written to S3")

# COMMAND ----------

# Auto Loader with schema evolution enabled
# With schemaEvolutionMode="addNewColumns", the stream will:
# 1. Detect the new 'referrer' column
# 2. Add it to the schema
# 3. Restart the stream to pick up the new schema
# 4. Backfill NULL for old records that don't have the column
query_evolution = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useNotifications", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/notification_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/json_events/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", input_file_name())
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/notification_bronze")
        .option("mergeSchema", "true")    # allow Delta table schema to evolve
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_notification")
)

query_evolution.awaitTermination()
print("Schema evolution stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution: 'referrer' column should now exist
# MAGIC -- Old records will have NULL for referrer, new records will have values
# MAGIC DESCRIBE bronze_events_notification

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the new column
# MAGIC SELECT event_id, event_type, device, referrer, source_file
# MAGIC FROM bronze_events_notification
# MAGIC WHERE referrer IS NOT NULL
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: old records have NULL referrer, new records have values
# MAGIC SELECT
# MAGIC     CASE WHEN referrer IS NULL THEN 'No referrer (old schema)'
# MAGIC          ELSE 'Has referrer (new schema)'
# MAGIC     END as schema_version,
# MAGIC     COUNT(*) as record_count
# MAGIC FROM bronze_events_notification
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Rescue Column (`_rescued_data`)
# MAGIC
# MAGIC When `schemaEvolutionMode` is set to `rescue` (the default), data that doesn't match
# MAGIC the expected schema is captured in the `_rescued_data` column instead of being dropped.
# MAGIC
# MAGIC ```python
# MAGIC # Rescue mode (default behavior)
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.schemaEvolutionMode", "rescue")  # default
# MAGIC     .option("cloudFiles.schemaLocation", f"{schema_path}/rescue_demo")
# MAGIC     .load(f"{raw_data_path}/json_events/")
# MAGIC     # _rescued_data column is automatically added
# MAGIC     # Contains JSON string of any data that doesn't match the schema
# MAGIC ```
# MAGIC
# MAGIC **When to use each mode**:
# MAGIC | Mode | Best For |
# MAGIC |------|----------|
# MAGIC | `addNewColumns` | When you want the Bronze table to automatically adapt |
# MAGIC | `rescue` | When you want to preserve mismatched data without schema changes |
# MAGIC | `failOnNewColumns` | When schema changes should halt the pipeline for review |
# MAGIC | `none` | When extra columns should be silently dropped |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trigger Strategies Comparison
# MAGIC
# MAGIC Different trigger modes for different use cases.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger 1: `availableNow=True` (Recommended for Scheduled Jobs)
# MAGIC
# MAGIC Processes ALL available data in multiple micro-batches, then stops.
# MAGIC Ideal for Databricks Workflows scheduled to run every N minutes/hours.
# MAGIC
# MAGIC This is what we've been using throughout this lab.
# MAGIC
# MAGIC ```python
# MAGIC query = (
# MAGIC     df_stream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://...")
# MAGIC         .outputMode("append")
# MAGIC         .trigger(availableNow=True)     # process all, then stop
# MAGIC         .table("my_table")
# MAGIC )
# MAGIC query.awaitTermination()  # blocks until all data is processed
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger 2: `processingTime` (Continuous Near-Real-Time)
# MAGIC
# MAGIC Runs micro-batches at fixed intervals. The stream stays running indefinitely.
# MAGIC
# MAGIC ```python
# MAGIC query = (
# MAGIC     df_stream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://...")
# MAGIC         .outputMode("append")
# MAGIC         .trigger(processingTime="30 seconds")  # micro-batch every 30s
# MAGIC         .table("my_table")
# MAGIC )
# MAGIC # Stream runs continuously -- stop with query.stop()
# MAGIC ```
# MAGIC
# MAGIC **When to use**: Live dashboards, near-real-time ETL, event processing

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger 3: `once=True` (Deprecated)
# MAGIC
# MAGIC Processes exactly ONE micro-batch then stops. **Deprecated** in favor of `availableNow`.
# MAGIC
# MAGIC The key difference: `once` processes a single micro-batch (may leave data behind),
# MAGIC while `availableNow` processes ALL available data across multiple micro-batches.
# MAGIC
# MAGIC ```python
# MAGIC # DEPRECATED - use availableNow instead
# MAGIC query = (
# MAGIC     df_stream.writeStream
# MAGIC         .trigger(once=True)  # only one micro-batch
# MAGIC         .table("my_table")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Trigger Comparison Summary
# MAGIC
# MAGIC | Trigger | Behavior | Stops? | Use Case |
# MAGIC |---------|----------|--------|----------|
# MAGIC | `availableNow=True` | All available data, multiple micro-batches | Yes | Scheduled Workflows |
# MAGIC | `processingTime="30s"` | One micro-batch every 30 seconds | No | Continuous processing |
# MAGIC | `processingTime="0s"` | ASAP, back-to-back micro-batches | No | Lowest latency |
# MAGIC | `once=True` | Single micro-batch | Yes | **Deprecated** |
# MAGIC | No trigger | ASAP (default) | No | Development/testing |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Stream Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check Active Streams

# COMMAND ----------

# List all currently active streaming queries
active_streams = spark.streams.active
print(f"Active streams: {len(active_streams)}")
for stream in active_streams:
    print(f"  Name: {stream.name}, ID: {stream.id}")
    print(f"  Status: {stream.status}")
    print(f"  Recent progress: {stream.recentProgress[-1] if stream.recentProgress else 'None'}")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Delta Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View ingestion history for the directory listing Bronze table
# MAGIC DESCRIBE HISTORY bronze_orders_dir_listing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View ingestion history for the notification Bronze table
# MAGIC DESCRIBE HISTORY bronze_events_notification

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Full Production Pipeline: Auto Loader -> Bronze -> Silver
# MAGIC
# MAGIC Putting it all together: a production-ready pipeline that ingests files from S3
# MAGIC into Bronze, then streams Bronze to Silver with transformations.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze -> Silver Streaming Pipeline

# COMMAND ----------

# Read from Bronze as a stream
df_bronze_stream = (
    spark.readStream
        .format("delta")
        .table("bronze_orders_dir_listing")
)

# Apply Silver transformations
df_silver_stream = (
    df_bronze_stream
    .filter(col("quantity") > 0)
    .filter(col("customer_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    )
    .withColumn("order_date_str", date_format(
        from_unixtime(col("order_timestamp")), "yyyy-MM-dd"
    ))
    .select(
        "order_id", "customer_id", "product_id", "quantity",
        "order_date", "order_date_str", "payment_method",
        "load_time", "source_file"
    )
)

# Write Silver table
query_silver = (
    df_silver_stream.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/silver_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("silver_orders_streaming")
)

query_silver.awaitTermination()
print("Bronze -> Silver streaming pipeline completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_orders_streaming ORDER BY order_date DESC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify record counts: Bronze vs Silver
# MAGIC SELECT 'Bronze' as layer, COUNT(*) as records FROM bronze_orders_dir_listing
# MAGIC UNION ALL
# MAGIC SELECT 'Silver' as layer, COUNT(*) as records FROM silver_orders_streaming

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: Directory Listing vs File Notification
# MAGIC
# MAGIC | Feature | Directory Listing | File Notification |
# MAGIC |---------|-------------------|-------------------|
# MAGIC | **Setup** | `cloudFiles.useNotifications = false` | `cloudFiles.useNotifications = true` |
# MAGIC | **Infrastructure** | None | S3 events + SNS + SQS |
# MAGIC | **File detection** | Polls S3 directory | Receives SQS messages |
# MAGIC | **Latency** | Depends on trigger interval | Near-real-time (seconds) |
# MAGIC | **Scale** | Good for < 100K files | Millions of files |
# MAGIC | **S3 API cost** | Higher (LIST calls) | Lower (no LIST) |
# MAGIC | **IAM needed** | S3 read only | S3, SNS, SQS |
# MAGIC | **Best for** | Dev, moderate volume, scheduled | Prod, high volume, real-time |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Auto Loader** (`cloudFiles`) is the recommended way to ingest files from S3 into Delta Lake
# MAGIC 2. **Directory listing** mode requires zero setup -- start immediately
# MAGIC 3. **File notification** mode uses S3 + SQS for near-real-time detection at scale
# MAGIC 4. **Schema inference** is persisted to `schemaLocation` and only runs on first execution
# MAGIC 5. **Schema evolution** with `addNewColumns` automatically adapts to new fields
# MAGIC 6. **`trigger(availableNow=True)`** is ideal for scheduled Workflows (processes all, then stops)
# MAGIC 7. **Checkpoints** enable exactly-once processing and fault-tolerant restarts
# MAGIC 8. Always use a **dedicated checkpoint per stream** -- never share checkpoints
# MAGIC 9. **Pre-configure SQS** via Terraform in production for full control over IAM and lifecycle

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

# Drop tables
tables = [
    "bronze_orders_dir_listing",
    "bronze_events_notification",
    "silver_orders_streaming",
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped table: {table}")

# COMMAND ----------

# Remove S3 data
dbutils.fs.rm(base_path, recurse=True)
print(f"Removed all data at: {base_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS streaming_lab CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 20**: [Advanced Streaming](../day20-advanced-streaming/) -- watermarks, windows, state management
# MAGIC - Try converting Scenario 1 to continuous processing with `processingTime`
# MAGIC - Set up SQS + S3 notifications via Terraform for file notification mode
# MAGIC - Build a complete Medallion pipeline: Auto Loader -> Bronze -> Silver -> Gold
# MAGIC - Explore Delta Live Tables (DLT) for declarative streaming pipelines
