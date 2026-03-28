# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Traditional Spark Pipeline (Before DLT)
# MAGIC > Module 23 — Evolution Notebook 1 of 3 | Level: Intermediate | Time: 25 min
# MAGIC
# MAGIC ## The "Old Way" of Building Data Pipelines
# MAGIC
# MAGIC This notebook demonstrates how data engineers built Bronze-Silver-Gold pipelines
# MAGIC **before** Delta Live Tables and Lakeflow existed. Every step is manual:
# MAGIC checkpoints, schema management, data quality, CDC, orchestration.
# MAGIC
# MAGIC **Purpose**: Feel the pain first, so you appreciate the solutions that came later.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Pipeline**: E-commerce Orders (JSON from S3) -> Bronze -> Silver -> Gold
# MAGIC
# MAGIC | Layer | Table | Description |
# MAGIC |-------|-------|-------------|
# MAGIC | Bronze | `ecommerce.bronze.orders` | Raw JSON ingestion |
# MAGIC | Silver | `ecommerce.silver.orders` | Cleaned + validated |
# MAGIC | Gold | `ecommerce.gold.daily_revenue` | Daily revenue per store |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup
# MAGIC
# MAGIC Before we can even start, we need to manually create catalogs, schemas, and
# MAGIC define every path ourselves. There is no framework to manage this for us.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Manual catalog and schema creation
# MAGIC -- You must remember to do this for every environment (dev, staging, prod)
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;
# MAGIC USE CATALOG ecommerce;
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# COMMAND ----------

# Define paths manually — you are responsible for every single one
source_path = "s3://ecommerce-lakehouse/data-store/orders/"
bronze_checkpoint = "/chk/ecommerce/bronze_orders"
silver_checkpoint = "/chk/ecommerce/silver_orders"
gold_table_path = "/mnt/ecommerce/gold/daily_revenue"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Bronze Layer — Manual Ingestion
# MAGIC
# MAGIC With traditional Spark, you must manually configure:
# MAGIC - The read stream format and options
# MAGIC - The write stream format, mode, and trigger
# MAGIC - The checkpoint location (and remember where you put it)
# MAGIC - The output table name
# MAGIC
# MAGIC If any of these are wrong, you get silent failures or data loss.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col

# --- Bronze: Raw Ingestion ---

# Read from S3 using Auto Loader
bronze_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/chk/ecommerce/bronze_orders_schema")
    .load(source_path)
    .withColumn("ingest_timestamp", F.current_timestamp())
    .withColumn("source_file", col("_metadata.file_name"))
)

# Write to bronze table
# Problem: Must manually manage checkpoint location
# Problem: Must manually choose trigger mode
# Problem: Must manually specify output mode
bronze_query = (
    bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", bronze_checkpoint)      # Manual checkpoint
    .option("mergeSchema", "true")                          # Manual schema evolution
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("ecommerce.bronze.orders")
)

bronze_query.awaitTermination()

# Problem: No built-in data quality tracking
# Problem: No automatic retry on failure
# Problem: If this fails mid-stream, you must manually clean up checkpoints

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Silver Layer — Manual Transformation
# MAGIC
# MAGIC Now we must:
# MAGIC 1. Manually read from the bronze table
# MAGIC 2. Manually write filter conditions for data quality
# MAGIC 3. Manually track which records were dropped (spoiler: we won't, because it's too hard)
# MAGIC 4. Manually manage another checkpoint

# COMMAND ----------

# --- Silver: Clean + Validate ---

# Read from bronze
silver_input = spark.readStream.table("ecommerce.bronze.orders")

# Manual data quality checks — just filters, no tracking
silver_df = (
    silver_input
    # Problem: We are silently dropping bad records with no audit trail
    .filter("order_amount > 0")                              # Drop negative/zero amounts
    .filter("order_date IS NOT NULL")                        # Drop null dates
    .filter("year(order_date) >= 2020")                      # Drop invalid dates
    .filter("customer_rating BETWEEN 1 AND 5")               # Drop invalid ratings
    .select(
        F.col("order_id"),
        F.col("order_date").cast("date"),
        F.col("store_id"),
        F.col("customer_type"),
        F.col("order_amount").cast("double"),
        F.col("items_count").cast("int"),
        F.col("customer_rating").cast("int"),
        F.col("ingest_timestamp"),
    )
    .withColumn("silver_processed_at", F.current_timestamp())
)

# Write to silver table — another manual writeStream
silver_query = (
    silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", silver_checkpoint)          # Another manual checkpoint
    .outputMode("append")
    .trigger(availableNow=True)
    .toTable("ecommerce.silver.orders")
)

silver_query.awaitTermination()

# Problem: How many records were dropped? We have no idea.
# Problem: What percentage of records passed quality checks? Unknown.
# Problem: Which specific quality rule caused the most drops? Can't tell.
# Problem: No dashboard, no metrics, no alerting.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Gold Layer — Manual Aggregation
# MAGIC
# MAGIC The gold layer is "simpler" because it is batch, but you still
# MAGIC must manually manage the write mode, partitioning, and table creation.

# COMMAND ----------

# --- Gold: Daily Revenue per Store ---

# Read from silver (batch this time)
silver_batch = spark.read.table("ecommerce.silver.orders")

# Aggregate
gold_df = (
    silver_batch
    .groupBy("store_id", "order_date")
    .agg(
        F.sum("order_amount").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.avg("customer_rating").alias("avg_rating"),
    )
)

# Write to gold — must decide: overwrite or merge?
# Problem: Overwrite rewrites ALL data even if only 1 day changed
# Problem: Append creates duplicates on re-runs
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("ecommerce.gold.daily_revenue")

# Problem: No incremental refresh — full recompute every time
# Problem: No dependency tracking — gold does not "know" it depends on silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Manual CDC — The Real Pain
# MAGIC
# MAGIC What happens when an order is **updated** (e.g., amount corrected) or
# MAGIC **deleted** (e.g., fraudulent order removed)?
# MAGIC
# MAGIC With traditional Spark, you must write a full MERGE statement yourself.
# MAGIC This is error-prone, verbose, and hard to test.

# COMMAND ----------

# --- Manual CDC: MERGE for Silver Orders ---

# Suppose we receive a batch of change events (inserts, updates, deletes)
# We must build and execute MERGE INTO manually

from delta.tables import DeltaTable

def apply_cdc_manually(changes_df):
    """
    Manually apply CDC to silver orders.
    This is what you had to write for EVERY table that needed CDC.
    """
    # Problem: Must check if table exists first
    if not spark.catalog.tableExists("ecommerce.silver.orders"):
        changes_df.write.format("delta").saveAsTable("ecommerce.silver.orders")
        return

    silver_table = DeltaTable.forName(spark, "ecommerce.silver.orders")

    # Problem: Must handle inserts, updates, and deletes in one MERGE
    # Problem: Must define the sequence/ordering column to resolve conflicts
    # Problem: Must handle late-arriving data
    # Problem: Must handle duplicate events
    silver_table.alias("target").merge(
        changes_df.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdate(
        condition="source.operation != 'DELETE'",
        set={
            "order_date": "source.order_date",
            "store_id": "source.store_id",
            "customer_type": "source.customer_type",
            "order_amount": "source.order_amount",
            "items_count": "source.items_count",
            "customer_rating": "source.customer_rating",
            "silver_processed_at": "current_timestamp()",
        }
    ).whenMatchedDelete(
        condition="source.operation = 'DELETE'"
    ).whenNotMatchedInsert(
        values={
            "order_id": "source.order_id",
            "order_date": "source.order_date",
            "store_id": "source.store_id",
            "customer_type": "source.customer_type",
            "order_amount": "source.order_amount",
            "items_count": "source.items_count",
            "customer_rating": "source.customer_rating",
            "silver_processed_at": "current_timestamp()",
        }
    ).execute()

    # Problem: ~50 lines of boilerplate for ONE table
    # Problem: Copy-paste this for every CDC table in your pipeline
    # Problem: No SCD Type 2 without even more code
    # Problem: No built-in deduplication of source events

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: The Problems With This Approach
# MAGIC
# MAGIC After building this pipeline, here is everything you must manage yourself:
# MAGIC
# MAGIC | Problem | Impact |
# MAGIC |---------|--------|
# MAGIC | **No dependency management** | Bronze, Silver, Gold are independent streams. If Silver fails, Gold runs on stale data with no warning. |
# MAGIC | **Manual checkpoint management** | Forget a checkpoint path? Reuse one accidentally? Data loss or duplication. |
# MAGIC | **No data quality tracking** | Records are silently dropped. No metrics on how many or why. |
# MAGIC | **No pipeline visualization** | No DAG. You must read code to understand data flow. |
# MAGIC | **No automatic retries** | A transient failure kills the pipeline. You must build retry logic yourself. |
# MAGIC | **No parallelization** | Independent tables process sequentially unless you manually thread. |
# MAGIC | **CDC is boilerplate hell** | ~50 lines per table, easy to get wrong, hard to test. |
# MAGIC | **No incremental gold refresh** | Full recompute or manual MERGE for every aggregation table. |
# MAGIC | **Environment management** | Promote dev to prod? Rewrite all paths and catalog names. |
# MAGIC | **Scaling is painful** | 5 tables is manageable. 50 tables with this approach is a maintenance nightmare. |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **This is why Delta Live Tables was created.**
# MAGIC
# MAGIC Proceed to **Notebook 2** (`23-dlt-pipeline_notebook.py`) to see how DLT
# MAGIC solved many of these problems with a declarative approach.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cleanup
# MAGIC DROP TABLE IF EXISTS ecommerce.gold.daily_revenue;
# MAGIC DROP TABLE IF EXISTS ecommerce.silver.orders;
# MAGIC DROP TABLE IF EXISTS ecommerce.bronze.orders;

# COMMAND ----------

# Remove checkpoint directories
dbutils.fs.rm("/chk/ecommerce/", recurse=True)
