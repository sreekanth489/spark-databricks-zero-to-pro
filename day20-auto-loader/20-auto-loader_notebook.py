# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 20: Auto Loader -- Optimized File Ingestion
# MAGIC
# MAGIC **Objective**: Master Auto Loader for incremental file ingestion from AWS S3
# MAGIC
# MAGIC **Key Concept**: Auto Loader is NOT a streaming engine. It is a specialized file ingestion
# MAGIC SOURCE built on top of Spark Structured Streaming.
# MAGIC
# MAGIC ```
# MAGIC Structured Streaming = the ENGINE (Day 19)
# MAGIC Auto Loader          = a specialized SOURCE built on that engine (this session)
# MAGIC
# MAGIC Standard file source:  spark.readStream.format("parquet").load(path)
# MAGIC Auto Loader:           spark.readStream.format("cloudFiles").option("cloudFiles.format","parquet").load(path)
# MAGIC ```
# MAGIC
# MAGIC **Three Auto Loader Modes on AWS**:
# MAGIC
# MAGIC | Track | Mode | Option | Recommendation |
# MAGIC |-------|------|--------|----------------|
# MAGIC | A | Directory Listing | `useNotifications=false` | Starter / Dev |
# MAGIC | B | Managed File Events | `useManagedFileEvents=true` | Production (Premium + UC) |
# MAGIC | C | Classic Notifications | `useNotifications=true` | Legacy / Appendix |
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**: See [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/blob/main/day00-environment-setup/00-databricks-cloud-setup.md) for AWS + Databricks + S3 + external location configuration.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS autoloader_lab
# MAGIC COMMENT 'Day 20: Auto Loader lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA autoloader_lab

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import (
    col, current_timestamp, from_unixtime, lit,
    count, sum as _sum, round as _round, date_format
)
from delta.tables import DeltaTable
import random
import time

base_path = "s3://databricks-zero-to-pro/autoloader_lab"
raw_data_path = f"{base_path}/raw"
bronze_path = f"{base_path}/bronze"
checkpoint_path = f"{base_path}/checkpoints"
schema_path = f"{base_path}/schemas"

print("Auto Loader Lab Storage Layout (AWS S3)")
print("=" * 50)
print(f"Raw data:     {raw_data_path}")
print(f"Bronze:       {bronze_path}")
print(f"Checkpoints:  {checkpoint_path}")
print(f"Schemas:      {schema_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Why Auto Loader?
# MAGIC
# MAGIC Standard Spark file streaming (`format("parquet")`) has limitations:
# MAGIC - Re-lists the entire directory on every trigger (expensive)
# MAGIC - Struggles with millions of files
# MAGIC - No schema inference or evolution
# MAGIC - No built-in duplicate file detection
# MAGIC
# MAGIC Auto Loader (`format("cloudFiles")`) solves all of these:
# MAGIC - Incremental file tracking via checkpoint
# MAGIC - Schema inference persisted to `schemaLocation`
# MAGIC - Schema evolution with `addNewColumns`
# MAGIC - Optimized file discovery (notifications or incremental listing)
# MAGIC - Handles millions of files efficiently

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Generate Sample Data

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquet Files (for Track A)

# COMMAND ----------

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_timestamp", LongType(), True),
    StructField("payment_method", StringType(), True),
])

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
print(f"Batch 1: {df_batch1.count()} orders as Parquet")
df_batch1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JSON Files (for Tracks B and C)

# COMMAND ----------

events_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("page", StringType(), True),
    StructField("device", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("event_timestamp", LongType(), True),
])

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

df_events_b1 = spark.createDataFrame(events_batch1, events_schema)
df_events_b1.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch1")
print(f"Events Batch 1: {df_events_b1.count()} events as JSON")
df_events_b1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Track A: Directory Listing Mode (Recommended Starter)
# MAGIC
# MAGIC **Zero infrastructure setup.** Auto Loader scans the S3 directory for new files.
# MAGIC Works on Free and Premium editions.
# MAGIC
# MAGIC **Key option**: `cloudFiles.useNotifications = false`
# MAGIC
# MAGIC **Important**: Use `_metadata.file_path` instead of `input_file_name()` in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ### A1. Basic Ingestion

# COMMAND ----------

df_stream_dir = (
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

query_dir = (
    df_stream_dir.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/dir_listing_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders_dir_listing")
)

query_dir.awaitTermination()
print("Track A: Directory listing ingestion completed")

# COMMAND ----------

# MAGIC %sql
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
# MAGIC ### A2. Incremental Ingestion -- Add New Files
# MAGIC
# MAGIC Same checkpoint = only new files are processed.

# COMMAND ----------

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

# Re-run with SAME checkpoint -- only processes batch2
query_dir_incr = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_orders")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("load_time", current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
        .writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/dir_listing_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders_dir_listing")
)

query_dir_incr.awaitTermination()
print("Incremental ingestion completed -- only new files processed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records, COUNT(DISTINCT source_file) as source_files
# MAGIC FROM bronze_orders_dir_listing

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_orders_dir_listing
# MAGIC GROUP BY source_file ORDER BY ingested_at

# COMMAND ----------

# MAGIC %md
# MAGIC ### A3. Advanced Options

# COMMAND ----------

# Rate limiting and file filtering
df_advanced = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "false")
        .option("cloudFiles.maxFilesPerTrigger", "100")
        .option("cloudFiles.maxBytesPerTrigger", "1g")
        .option("pathGlobFilter", "*.parquet")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/dir_listing_advanced")
        .load(f"{raw_data_path}/parquet_orders/")
        .withColumn("source_file", col("_metadata.file_path"))
)

print("Advanced stream schema:")
df_advanced.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Track B: Managed File Events (Recommended Production)
# MAGIC
# MAGIC **Modern production path.** Databricks manages SNS/SQS via Unity Catalog external locations.
# MAGIC
# MAGIC **Key option**: `cloudFiles.useManagedFileEvents = true`
# MAGIC
# MAGIC **Do NOT combine** with `cloudFiles.useNotifications = true`.
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC ```sql
# MAGIC -- 1. Create storage credential (Admin)
# MAGIC CREATE STORAGE CREDENTIAL my_s3_credential
# MAGIC WITH (AWS_IAM_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/databricks-runtime-role');
# MAGIC
# MAGIC -- 2. Create external location
# MAGIC CREATE EXTERNAL LOCATION autoloader_lab_location
# MAGIC URL 's3://databricks-zero-to-pro/autoloader_lab/'
# MAGIC WITH (STORAGE CREDENTIAL my_s3_credential);
# MAGIC
# MAGIC -- 3. Enable file events
# MAGIC ALTER EXTERNAL LOCATION autoloader_lab_location ENABLE FILE EVENTS;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### B1. Managed File Events Ingestion

# COMMAND ----------

df_stream_managed = (
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
print("Track B: Managed file events ingestion completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM bronze_events_managed LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_events FROM bronze_events_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ### B2. Incremental Ingestion

# COMMAND ----------

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

df_events_b2 = spark.createDataFrame(events_batch2, events_schema)
df_events_b2.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch2")
print(f"Events Batch 2: {df_events_b2.count()} new events written to S3")

# COMMAND ----------

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
        .option("checkpointLocation", f"{checkpoint_path}/managed_bronze")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_managed")
)

query_managed_incr.awaitTermination()
print("Incremental managed file events completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_file, COUNT(*) as records, MIN(load_time) as ingested_at
# MAGIC FROM bronze_events_managed
# MAGIC GROUP BY source_file ORDER BY ingested_at

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Track C: Classic Notifications (Legacy / Appendix)
# MAGIC
# MAGIC **Older approach.** Auto Loader auto-manages S3 notifications + SNS/SQS per stream.
# MAGIC More moving parts and more AWS failure modes.
# MAGIC
# MAGIC **Key option**: `cloudFiles.useNotifications = true`
# MAGIC
# MAGIC **MUST set**: `cloudFiles.region` to match your S3 bucket region.

# COMMAND ----------

df_stream_classic = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.useNotifications", "true")
        .option("cloudFiles.region", "us-east-1")
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.schemaLocation", f"{schema_path}/classic_notif")
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
        .table("bronze_orders_classic")
)

query_classic.awaitTermination()
print("Track C: Classic notifications completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_records FROM bronze_orders_classic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pre-Configured SQS Queue (Production Variant)
# MAGIC
# MAGIC ```python
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "json")
# MAGIC     .option("cloudFiles.useNotifications", "true")
# MAGIC     .option("cloudFiles.queueUrl",
# MAGIC             "https://sqs.us-east-1.amazonaws.com/123456789012/my-autoloader-queue")
# MAGIC     .option("cloudFiles.region", "us-east-1")
# MAGIC     .option("cloudFiles.schemaLocation", "s3://bucket/schemas/events")
# MAGIC     .load("s3://bucket/raw/events/")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Schema Evolution Demo
# MAGIC
# MAGIC When source data adds new fields, Auto Loader can handle it automatically
# MAGIC with `schemaEvolutionMode = "addNewColumns"`.

# COMMAND ----------

# Generate Batch 3 with NEW column: referrer
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
        random.choice(["google", "facebook", "direct", "email", "twitter"]),
    ))

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

df_events_b3 = spark.createDataFrame(events_batch3, events_schema_v2)
df_events_b3.write.mode("overwrite").json(f"{raw_data_path}/json_events/batch3")
print(f"Batch 3: {df_events_b3.count()} events with NEW 'referrer' column")

# COMMAND ----------

# Re-run Auto Loader -- schema evolution detects the new column
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
        .option("mergeSchema", "true")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_events_managed")
)

query_evolution.awaitTermination()
print("Schema evolution completed -- 'referrer' column auto-added")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify schema evolution
# MAGIC DESCRIBE bronze_events_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC -- New records have referrer, old records have NULL
# MAGIC SELECT
# MAGIC     CASE WHEN referrer IS NULL THEN 'Old schema (no referrer)'
# MAGIC          ELSE 'New schema (has referrer)' END as schema_version,
# MAGIC     COUNT(*) as records
# MAGIC FROM bronze_events_managed
# MAGIC GROUP BY 1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_id, event_type, device, referrer, source_file_name
# MAGIC FROM bronze_events_managed
# MAGIC WHERE referrer IS NOT NULL LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Full Pipeline: Auto Loader -> Bronze -> Silver
# MAGIC
# MAGIC Auto Loader ingests at the Bronze layer. Then standard Structured Streaming
# MAGIC (from Day 19) processes Bronze -> Silver.

# COMMAND ----------

# Customers lookup for enrichment
customers_data = [
    ("C001", "Alice", "Johnson", "New York"),
    ("C002", "Bob", "Smith", "Los Angeles"),
    ("C003", "Carol", "Williams", "Chicago"),
    ("C004", "David", "Brown", "Houston"),
    ("C005", "Eve", "Davis", "Phoenix"),
    ("C006", "Frank", "Miller", "Seattle"),
    ("C007", "Grace", "Wilson", "Denver"),
    ("C008", "Henry", "Moore", "Boston"),
]

df_customers = spark.createDataFrame(customers_data,
    ["customer_id", "first_name", "last_name", "city"])
df_customers.write.format("delta").mode("overwrite").saveAsTable("customers_lookup")

# COMMAND ----------

# Bronze -> Silver using standard Structured Streaming (Day 19 engine)
df_bronze_stream = spark.readStream.format("delta").table("bronze_orders_dir_listing")
df_cust = spark.table("customers_lookup")

df_silver = (
    df_bronze_stream
    .filter(col("quantity") > 0)
    .join(df_cust, "customer_id", "inner")
    .withColumn("order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    .select("order_id", "order_date", "customer_id", "first_name", "last_name",
            "city", "product_id", "quantity", "payment_method")
)

query_silver = (
    df_silver.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/silver_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("silver_orders")
)

query_silver.awaitTermination()
print("Bronze -> Silver pipeline completed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'Bronze (dir listing)' as layer, COUNT(*) as records FROM bronze_orders_dir_listing
# MAGIC UNION ALL
# MAGIC SELECT 'Silver' as layer, COUNT(*) as records FROM silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver_orders ORDER BY order_date DESC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Troubleshooting Reference
# MAGIC
# MAGIC | Error | Cause | Fix |
# MAGIC |-------|-------|-----|
# MAGIC | `PermanentRedirect` | Bucket region mismatch | Set `cloudFiles.region` to match S3 bucket |
# MAGIC | `GetBucketNotification AccessDenied` | Missing IAM permission | Add `s3:GetBucketNotification` to role + bucket policy |
# MAGIC | "no matching external location" | S3 path not in UC external location | Create external location, enable file events |
# MAGIC | `sns.subscribe` failure | SNS/SQS permission issue | Check SNS/SQS IAM permissions in CloudTrail |
# MAGIC | CloudTrail `anonymous` | S3 didn't recognize principal | Verify Databricks runtime role ARN in bucket policy |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC | Feature | Directory Listing | Managed File Events | Classic Notifications |
# MAGIC |---------|-------------------|--------------------|-----------------------|
# MAGIC | **Option** | `useNotifications=false` | `useManagedFileEvents=true` | `useNotifications=true` |
# MAGIC | **Setup** | None | External location + file events | IAM for SNS/SQS |
# MAGIC | **Latency** | Trigger interval | Near-real-time | Near-real-time |
# MAGIC | **Scale** | < 100K files | Millions+ | Millions+ |
# MAGIC | **Edition** | Free + Premium | Premium only | Free + Premium |
# MAGIC | **Recommendation** | Starter / Dev | Production | Legacy |
# MAGIC
# MAGIC **Key Takeaway**: Auto Loader is a SOURCE, not an engine.
# MAGIC It uses Structured Streaming (Day 19) for micro-batch execution.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

for stream in spark.streams.active:
    print(f"Stopping: {stream.id}")
    stream.stop()
    stream.awaitTermination()
print("All streams stopped")

# COMMAND ----------

tables = [
    "bronze_orders_dir_listing", "bronze_events_managed",
    "bronze_orders_classic", "silver_orders", "customers_lookup"
]
for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped: {table}")

# COMMAND ----------

dbutils.fs.rm(base_path, recurse=True)
print(f"Removed: {base_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS autoloader_lab CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 21**: [Delta Live Tables](../day21-delta-live-tables/) -- declarative pipelines
# MAGIC - Set up Unity Catalog external location with file events for managed mode
# MAGIC - Build a complete Medallion pipeline: Auto Loader -> Bronze -> Silver -> Gold
