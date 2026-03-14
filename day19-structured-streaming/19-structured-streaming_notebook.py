# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 19: Structured Streaming & Auto Loader
# MAGIC
# MAGIC **Objective**: Master Auto Loader for incremental file ingestion from AWS S3 using three distinct modes
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Set up Unity Catalog schema and S3 paths
# MAGIC 2. Generate sample data files to simulate data landing in S3
# MAGIC 3. **Track A**: Auto Loader with **directory listing mode** (simplest, always works)
# MAGIC 4. **Track B**: Auto Loader with **managed file events** (modern, Unity Catalog + Premium)
# MAGIC 5. **Track C**: Auto Loader with **classic file notifications** (older, more moving parts)
# MAGIC 6. Demonstrate incremental processing with new file batches
# MAGIC 7. Handle schema inference, evolution, and rescue columns
# MAGIC 8. Compare trigger strategies and monitor streaming queries
# MAGIC 9. Build a full Bronze -> Silver streaming pipeline
# MAGIC
# MAGIC **Three Auto Loader Modes on AWS**:
# MAGIC ```
# MAGIC Track A: Directory Listing (recommended starter path)
# MAGIC   cloudFiles.useNotifications = false
# MAGIC   Auto Loader scans S3 directory for new files
# MAGIC   Zero infrastructure setup
# MAGIC
# MAGIC Track B: Managed File Events (recommended production path)
# MAGIC   cloudFiles.useManagedFileEvents = true
# MAGIC   Requires Unity Catalog external location with file events enabled
# MAGIC   Modern, cleaner than classic notifications
# MAGIC
# MAGIC Track C: Classic File Notifications (appendix / legacy)
# MAGIC   cloudFiles.useNotifications = true
# MAGIC   Auto Loader auto-manages S3 bucket notifications + SNS/SQS per stream
# MAGIC   More moving parts, more AWS-side failure modes
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
# MAGIC ### Parquet Files (for Track A - Directory Listing)

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
# MAGIC ### JSON Files (for Track B and C - Notification Modes)

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
# MAGIC ## Track A: Auto Loader with Directory Listing Mode
# MAGIC
# MAGIC **This is the recommended starter path.** Directory listing is the default mode.
# MAGIC
# MAGIC How it works:
# MAGIC 1. Scans the S3 directory for files
# MAGIC 2. Compares against files already processed (tracked in checkpoint via RocksDB)
# MAGIC 3. Reads only new/unprocessed files
# MAGIC 4. Uses incremental listing optimization on subsequent runs
# MAGIC
# MAGIC **Why start here**:
# MAGIC - Least setup -- zero infrastructure required
# MAGIC - Most reliable -- works on Free and Premium editions
# MAGIC - Great for learning, development, and moderate-scale production
# MAGIC
# MAGIC **Key option**: `cloudFiles.useNotifications = false`

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Basic Auto Loader - Directory Listing (Parquet)
# MAGIC
# MAGIC No SQS, no SNS, no event notifications -- just point to S3 and go.
# MAGIC
# MAGIC **Important**: In Unity Catalog, use `_metadata.file_path` and `_metadata.file_name`
# MAGIC instead of `input_file_name()` for source file tracking.

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
        # Add ingestion metadata using _metadata (Unity Catalog safe)
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
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
# MAGIC ### A2. Incremental Ingestion - Add New Files
# MAGIC
# MAGIC Simulate new data arriving in S3. Auto Loader will detect and process ONLY the new files
# MAGIC because the checkpoint tracks which files have already been processed.

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

# Re-run the SAME Auto Loader stream with SAME checkpoint -- only processes batch2
df_stream_dir_incr = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_orders")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
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
# MAGIC -- See which source files were ingested and when
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_orders_dir_listing
# MAGIC GROUP BY source_file
# MAGIC ORDER BY ingested_at

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Directory Listing - Advanced Options
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
        .withColumn("source_file", col("_metadata.file_path"))
)

# Preview the stream schema (does not start processing)
print("Stream schema:")
df_stream_advanced.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Track B: Auto Loader with Managed File Events (Recommended Production)
# MAGIC
# MAGIC **This is the modern, recommended production path** on Databricks AWS Premium.
# MAGIC
# MAGIC How it works:
# MAGIC 1. You create a **Unity Catalog external location** pointing to your S3 prefix
# MAGIC 2. You **enable file events** on that external location
# MAGIC 3. Databricks manages the SNS/SQS infrastructure behind the scenes
# MAGIC 4. Auto Loader receives near-real-time notifications when new files land
# MAGIC
# MAGIC **Why use managed file events over classic notifications**:
# MAGIC - Cleaner setup -- no per-stream SNS/SQS resource management
# MAGIC - Better integration with Unity Catalog governance
# MAGIC - Databricks manages the lifecycle of notification resources
# MAGIC - No need for broad SNS/SQS IAM permissions on the Databricks runtime role
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC 1. Databricks Premium workspace
# MAGIC 2. Storage credential created in Unity Catalog for your S3 bucket
# MAGIC 3. External location created for the S3 prefix
# MAGIC 4. File events enabled on the external location
# MAGIC
# MAGIC **Key option**: `cloudFiles.useManagedFileEvents = true`
# MAGIC
# MAGIC **Do NOT combine** with `cloudFiles.useNotifications = true` -- they are mutually exclusive.

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Setup Checklist (run once per environment)
# MAGIC
# MAGIC Before using managed file events, complete these steps in your Databricks workspace:
# MAGIC
# MAGIC ```sql
# MAGIC -- Step 1: Create storage credential (Admin only)
# MAGIC -- This maps an IAM role to Unity Catalog
# MAGIC CREATE STORAGE CREDENTIAL my_s3_credential
# MAGIC WITH (AWS_IAM_ROLE = 'arn:aws:iam::015747470350:role/databricks-s3-ingest-8a85f-db_s3_iam');
# MAGIC
# MAGIC -- Step 2: Create external location for the S3 prefix
# MAGIC CREATE EXTERNAL LOCATION streaming_lab_location
# MAGIC URL 's3://databricks-zero-to-pro/streaming_lab/'
# MAGIC WITH (STORAGE CREDENTIAL my_s3_credential);
# MAGIC
# MAGIC -- Step 3: Enable file events on the external location
# MAGIC ALTER EXTERNAL LOCATION streaming_lab_location
# MAGIC ENABLE FILE EVENTS;
# MAGIC ```
# MAGIC
# MAGIC The IAM role needs these permissions on the S3 bucket:
# MAGIC - `s3:GetBucketNotification`, `s3:PutBucketNotification`, `s3:GetBucketLocation`
# MAGIC - Standard object access: `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket`

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Managed File Events - JSON Ingestion

# COMMAND ----------

# Auto Loader with managed file events (modern production pattern)
df_stream_managed = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")     # managed file events mode
        .option("cloudFiles.inferColumnTypes", "true")          # infer actual types, not just strings
        .option("cloudFiles.schemaLocation", f"{schema_path}/managed_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/json_events/")
        # Use _metadata for Unity Catalog-safe source tracking
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_file_name", col("_metadata.file_name"))
)

query_managed = (
    df_stream_managed.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/managed_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_managed")
)

query_managed.awaitTermination()
print("Managed file events stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_events_managed LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_events FROM bronze_events_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### B3. Incremental Ingestion with Managed File Events

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
query_managed_incr = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/managed_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/json_events/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_file_name", col("_metadata.file_name"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/managed_bronze")  # same checkpoint
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_managed")
)

query_managed_incr.awaitTermination()
print("Incremental managed file events ingestion completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify both batches ingested
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_events_managed
# MAGIC GROUP BY source_file
# MAGIC ORDER BY ingested_at

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_events FROM bronze_events_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Track C: Classic File Notifications (Appendix)
# MAGIC
# MAGIC **This is the older approach** where Auto Loader auto-manages S3 bucket notifications
# MAGIC plus SNS/SQS resources per stream. It has more moving parts and more AWS-side failure modes.
# MAGIC
# MAGIC **Use this only if**:
# MAGIC - You cannot use managed file events (no Unity Catalog external location)
# MAGIC - You need fine-grained control over SNS/SQS resources
# MAGIC - You are on a non-Premium workspace without Unity Catalog
# MAGIC
# MAGIC **Key option**: `cloudFiles.useNotifications = true`
# MAGIC
# MAGIC **Important**: You MUST set `cloudFiles.region` to match your S3 bucket region,
# MAGIC otherwise you will get a `PermanentRedirect` error.

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1. Classic Notifications - Auto Setup
# MAGIC
# MAGIC Databricks automatically creates SNS topic, SQS queue, and S3 event notifications.
# MAGIC
# MAGIC **Required IAM permissions** on the Databricks runtime role:
# MAGIC - `s3:GetBucketNotification`, `s3:PutBucketNotification`
# MAGIC - `sns:CreateTopic`, `sns:DeleteTopic`, `sns:GetTopicAttributes`, `sns:Subscribe`, etc.
# MAGIC - `sqs:CreateQueue`, `sqs:DeleteQueue`, `sqs:ReceiveMessage`, `sqs:DeleteMessage`, etc.
# MAGIC
# MAGIC **Plus a bucket policy** granting the Databricks role `s3:GetBucketNotification` on the bucket.

# COMMAND ----------

# Classic file notifications -- requires IAM permissions and correct region
df_stream_classic = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "true")
        .option("cloudFiles.region", "us-east-1")              # MUST match your S3 bucket region
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/classic_notif_orders")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
)

query_classic = (
    df_stream_classic.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/classic_notif_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders_classic_notif")
)

query_classic.awaitTermination()
print("Classic notification stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM bronze_orders_classic_notif

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2. Classic Notifications - Pre-Configured SQS Queue
# MAGIC
# MAGIC In production, pre-create the SQS queue and S3 notifications via Terraform/CloudFormation,
# MAGIC then pass the queue URL to Auto Loader for full control over IAM, encryption, and lifecycle.
# MAGIC
# MAGIC ```python
# MAGIC df_stream_preconfigured = (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "json")
# MAGIC         .option("cloudFiles.useNotifications", "true")
# MAGIC         .option("cloudFiles.queueUrl",
# MAGIC                 "https://sqs.us-east-1.amazonaws.com/123456789012/my-autoloader-queue")
# MAGIC         .option("cloudFiles.region", "us-east-1")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .option("cloudFiles.schemaLocation", "s3://bucket/schemas/events")
# MAGIC         .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
# MAGIC         .load("s3://my-data-bucket/raw/events/")
# MAGIC         .withColumn("load_time", current_timestamp())
# MAGIC         .withColumn("source_file", col("_metadata.file_path"))
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
# MAGIC ---
# MAGIC ## Schema Inference, Evolution, and Rescue Columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Inference
# MAGIC
# MAGIC Auto Loader infers the schema from source files and persists it to `schemaLocation`.
# MAGIC On subsequent runs, the persisted schema is used (no re-inference needed).
# MAGIC This makes restarts fast -- schema inference only happens on first execution.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View the inferred schema of the managed events table
# MAGIC DESCRIBE bronze_events_managed

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
# 1. Detect the new 'referrer' column in batch 3
# 2. Update the persisted schema at schemaLocation
# 3. Add the column to the Delta table (mergeSchema=true)
# 4. Old records retain NULL for the new column
query_evolution = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.useManagedFileEvents", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/managed_events")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(f"{raw_data_path}/json_events/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .withColumn("source_file_name", col("_metadata.file_name"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/managed_bronze")
        .option("mergeSchema", "true")    # allow Delta table schema to evolve
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_managed")
)

query_evolution.awaitTermination()
print("Schema evolution stream completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution: 'referrer' column should now exist
# MAGIC DESCRIBE bronze_events_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check the new column: batch 3 records have referrer values
# MAGIC SELECT event_id, event_type, device, referrer, source_file_name
# MAGIC FROM bronze_events_managed
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
# MAGIC FROM bronze_events_managed
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
# MAGIC Key difference: `once` processes a single micro-batch (may leave data behind),
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
# MAGIC ### Delta Table History

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View ingestion history for the directory listing Bronze table
# MAGIC DESCRIBE HISTORY bronze_orders_dir_listing

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View ingestion history for the managed events Bronze table
# MAGIC DESCRIBE HISTORY bronze_events_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Full Production Pipeline: Auto Loader -> Bronze -> Silver
# MAGIC
# MAGIC Putting it all together: stream from the directory listing Bronze table
# MAGIC into a Silver table with transformations applied.

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
# MAGIC ## Common Errors and Troubleshooting
# MAGIC
# MAGIC ### `PermanentRedirect` Error
# MAGIC Your bucket region does not match. Set the correct AWS region:
# MAGIC ```python
# MAGIC .option("cloudFiles.region", "us-east-1")  # match your S3 bucket region
# MAGIC ```
# MAGIC
# MAGIC ### `GetBucketNotification AccessDenied`
# MAGIC The Databricks runtime role needs `s3:GetBucketNotification` permission.
# MAGIC Add it to both the IAM role policy AND the S3 bucket policy.
# MAGIC
# MAGIC ### Managed File Events: "no matching external location found"
# MAGIC The S3 path is not inside a Unity Catalog external location with file events enabled.
# MAGIC Create the storage credential, create the external location, and enable file events.
# MAGIC
# MAGIC ### Managed File Events: fails during `sns.subscribe`
# MAGIC The external location was found, but Databricks could not finish SNS/SQS subscription.
# MAGIC Check SNS and SQS permissions and inspect CloudTrail for the failing AWS API.
# MAGIC
# MAGIC ### CloudTrail shows `anonymous` identity with `AccessDenied`
# MAGIC This does NOT mean public internet access. It means S3 did not recognize the request
# MAGIC as an authorized principal. Re-check:
# MAGIC - Which IAM role the Databricks runtime actually uses
# MAGIC - The bucket policy principal ARN
# MAGIC - Whether you are using classic notifications vs managed file events

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Auto Loader Mode Comparison
# MAGIC
# MAGIC | Feature | Directory Listing | Managed File Events | Classic Notifications |
# MAGIC |---------|-------------------|--------------------|-----------------------|
# MAGIC | **Option** | `useNotifications=false` | `useManagedFileEvents=true` | `useNotifications=true` |
# MAGIC | **Setup** | None | External location + file events | IAM for SNS/SQS + bucket policy |
# MAGIC | **Infrastructure** | None | Databricks-managed | Auto Loader-managed per stream |
# MAGIC | **Latency** | Trigger interval | Near-real-time | Near-real-time |
# MAGIC | **Scale** | Moderate (< 100K files) | Millions+ | Millions+ |
# MAGIC | **Edition** | Free + Premium | Premium only | Free + Premium |
# MAGIC | **Unity Catalog** | Optional | Required | Optional |
# MAGIC | **Cleanup** | Nothing | Databricks manages | Must teardown SNS/SQS per stream |
# MAGIC | **Recommendation** | Starter / Dev | Production | Legacy / Appendix |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Start with directory listing** (`useNotifications=false`) -- it always works
# MAGIC 2. **Graduate to managed file events** (`useManagedFileEvents=true`) for production
# MAGIC 3. **Avoid classic notifications** as your main path -- more moving parts, more failure modes
# MAGIC 4. Use `_metadata.file_path` and `_metadata.file_name` instead of `input_file_name()` in Unity Catalog
# MAGIC 5. **Schema inference** is persisted to `schemaLocation` and only runs once
# MAGIC 6. **Schema evolution** with `addNewColumns` automatically adapts to new fields
# MAGIC 7. **`trigger(availableNow=True)`** is ideal for scheduled Workflows
# MAGIC 8. **Checkpoints** enable exactly-once processing -- never share or delete them
# MAGIC 9. Always set `cloudFiles.region` when using classic notifications

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
    "bronze_events_managed",
    "bronze_orders_classic_notif",
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
# MAGIC - Try converting Track A to continuous processing with `processingTime`
# MAGIC - Set up Unity Catalog external location with file events for Track B
# MAGIC - Build a complete Medallion pipeline: Auto Loader -> Bronze -> Silver -> Gold
# MAGIC - Explore Delta Live Tables (DLT) for declarative streaming pipelines
