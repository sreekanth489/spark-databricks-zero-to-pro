# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Day 18: Medallion Architecture - Multi-Hop Pipeline Lab
# MAGIC
# MAGIC **Objective**: Build a complete Bronze -> Silver -> Gold pipeline using Delta Lake on AWS S3
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Generate sample retail data (orders, customers, products)
# MAGIC 2. Ingest raw data into the **Bronze** layer with metadata enrichment
# MAGIC 3. Clean and join data in the **Silver** layer
# MAGIC 4. Create business-level aggregations in the **Gold** layer
# MAGIC 5. Demonstrate incremental processing across all layers
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Raw Files (S3) -> Bronze (raw + metadata) -> Silver (cleaned + joined) -> Gold (aggregated)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Configure S3 Paths and Generate Sample Data

# COMMAND ----------

# Configure base paths - update these for your environment
# Using DBFS for portability; replace with s3://your-bucket/ for production
base_path = "dbfs:/mnt/demo/medallion_lab"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
checkpoint_path = f"{base_path}/checkpoints"
raw_data_path = f"{base_path}/raw"

# For AWS S3 production use, paths would look like:
# base_path = "s3://my-lakehouse-bucket/medallion"

print(f"Base path: {base_path}")
print(f"Bronze: {bronze_path}")
print(f"Silver: {silver_path}")
print(f"Gold:   {gold_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Sample Retail Data
# MAGIC
# MAGIC We create self-contained sample data so this notebook runs independently.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, from_unixtime,
    date_trunc, sum as _sum, count as _count, max as _max,
    round as _round, when, lit, to_json, struct
)
import time

# -- Customers lookup data --
customers_data = [
    ("C001", "Alice", "Johnson", "alice@example.com", "New York", "Gold"),
    ("C002", "Bob", "Smith", "bob@example.com", "Los Angeles", "Silver"),
    ("C003", "Carol", "Williams", "carol@example.com", "Chicago", "Bronze"),
    ("C004", "David", "Brown", "david@example.com", "Houston", "Gold"),
    ("C005", "Eve", "Davis", "eve@example.com", "Phoenix", "Silver"),
    ("C006", "Frank", "Miller", "frank@example.com", "Seattle", "Gold"),
    ("C007", "Grace", "Wilson", "grace@example.com", "Denver", "Bronze"),
    ("C008", "Henry", "Moore", "henry@example.com", "Boston", "Silver"),
]

customers_schema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("city", StringType(), True),
    StructField("tier", StringType(), True),
])

df_customers = spark.createDataFrame(customers_data, customers_schema)
df_customers.show()

# COMMAND ----------

# -- Products lookup data --
products_data = [
    ("P001", "Delta Lake: The Definitive Guide", "Books", 45.99),
    ("P002", "Learning Spark", "Books", 49.99),
    ("P003", "Spark: The Definitive Guide", "Books", 55.00),
    ("P004", "Data Engineering with Python", "Books", 39.99),
    ("P005", "Designing Data-Intensive Apps", "Books", 42.50),
    ("P006", "Streaming Systems", "Books", 48.00),
    ("P007", "Fundamentals of Data Engineering", "Books", 44.99),
    ("P008", "The Data Warehouse Toolkit", "Books", 52.00),
]

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
])

df_products = spark.createDataFrame(products_data, products_schema)
df_products.show(truncate=False)

# COMMAND ----------

# -- Generate raw order events (simulating POS/e-commerce transactions) --
import random

random.seed(42)

# Batch 1: initial orders
orders_batch1 = []
base_ts = 1700000000  # Nov 2023 epoch
for i in range(1, 51):
    orders_batch1.append((
        f"ORD-{i:04d}",
        random.choice(["C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008"]),
        random.choice(["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]),
        random.randint(1, 5),
        base_ts + random.randint(0, 86400 * 30),  # spread over 30 days
        random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ))

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_timestamp", LongType(), True),
    StructField("payment_method", StringType(), True),
])

df_orders_batch1 = spark.createDataFrame(orders_batch1, orders_schema)

# Write batch 1 as raw parquet files to simulate source data landing in S3
df_orders_batch1.write.mode("overwrite").parquet(f"{raw_data_path}/orders/batch1")
print(f"Batch 1: {df_orders_batch1.count()} orders written to {raw_data_path}/orders/batch1")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC The Bronze layer stores data **exactly as received** from source systems.
# MAGIC We add metadata columns for auditability:
# MAGIC - `load_time`: when the record was ingested
# MAGIC - `source_file`: which file the record came from
# MAGIC
# MAGIC In production, you would use **Auto Loader** (`cloudFiles` format) for streaming ingestion.
# MAGIC For this lab, we demonstrate both the batch approach and the streaming pattern.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Bronze Ingestion (Batch Approach)
# MAGIC
# MAGIC Read raw parquet files and enrich with ingestion metadata.

# COMMAND ----------

# Read raw data and add metadata columns
df_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch1")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch1"))
)

# Write to Bronze Delta table
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .save(f"{bronze_path}/orders")
)

# Register as table for SQL queries
spark.sql(f"CREATE TABLE IF NOT EXISTS orders_bronze USING DELTA LOCATION '{bronze_path}/orders'")

print(f"Bronze layer: {df_bronze.count()} records ingested")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze data: raw records with metadata
# MAGIC SELECT order_id, customer_id, product_id, quantity, order_timestamp, payment_method,
# MAGIC        load_time, source_file
# MAGIC FROM orders_bronze
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Bronze Ingestion with Auto Loader (Streaming Pattern)
# MAGIC
# MAGIC This is the **recommended production pattern** for ingesting files from S3.
# MAGIC Auto Loader automatically detects new files and processes them incrementally.
# MAGIC
# MAGIC ```python
# MAGIC # Production Auto Loader pattern (reference only - requires cloud file notifications)
# MAGIC spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "parquet")
# MAGIC     .option("cloudFiles.schemaLocation", f"{checkpoint_path}/orders_raw_schema")
# MAGIC     .load("s3://my-bucket/raw/orders/")
# MAGIC     .withColumn("load_time", current_timestamp())
# MAGIC     .withColumn("source_file", input_file_name())
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", f"{checkpoint_path}/orders_bronze")
# MAGIC     .outputMode("append")
# MAGIC     .table("orders_bronze")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Silver Layer - Cleansed and Conformed Data
# MAGIC
# MAGIC In the Silver layer we:
# MAGIC 1. **Join** order data with customer and product lookup tables
# MAGIC 2. **Parse** Unix timestamps into human-readable format
# MAGIC 3. **Filter** out invalid records (e.g., zero quantity)
# MAGIC 4. **Calculate** derived fields (total_amount)
# MAGIC 5. **Deduplicate** on order_id

# COMMAND ----------

# Save lookup tables as Delta for reuse
df_customers.write.format("delta").mode("overwrite").save(f"{silver_path}/customers_lookup")
df_products.write.format("delta").mode("overwrite").save(f"{silver_path}/products_lookup")

spark.sql(f"CREATE TABLE IF NOT EXISTS customers_lookup USING DELTA LOCATION '{silver_path}/customers_lookup'")
spark.sql(f"CREATE TABLE IF NOT EXISTS products_lookup USING DELTA LOCATION '{silver_path}/products_lookup'")

print("Lookup tables saved to Silver layer")

# COMMAND ----------

# Read Bronze data
df_bronze_orders = spark.read.format("delta").load(f"{bronze_path}/orders")

# Read lookup tables
df_cust = spark.read.format("delta").load(f"{silver_path}/customers_lookup")
df_prod = spark.read.format("delta").load(f"{silver_path}/products_lookup")

# Silver transformations
df_silver = (
    df_bronze_orders
    # Remove duplicates based on order_id
    .dropDuplicates(["order_id"])
    # Filter out invalid records
    .filter(col("quantity") > 0)
    .filter(col("customer_id").isNotNull())
    # Join with customer data
    .join(df_cust, "customer_id", "inner")
    # Join with product data
    .join(df_prod, "product_id", "inner")
    # Parse timestamp
    .withColumn(
        "order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    )
    # Calculate total amount
    .withColumn("total_amount", _round(col("quantity") * col("price"), 2))
    # Select and rename columns for clean schema
    .select(
        "order_id",
        "order_date",
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "tier",
        "product_id",
        "product_name",
        "category",
        "quantity",
        "price",
        "total_amount",
        "payment_method",
    )
)

# Write to Silver Delta table
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .save(f"{silver_path}/orders")
)

spark.sql(f"CREATE TABLE IF NOT EXISTS orders_silver USING DELTA LOCATION '{silver_path}/orders'")

print(f"Silver layer: {df_silver.count()} cleaned records")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Silver data: cleaned, joined, and enriched
# MAGIC SELECT order_id, order_date, first_name, last_name, city, tier,
# MAGIC        product_name, quantity, price, total_amount, payment_method
# MAGIC FROM orders_silver
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Quality Checks
# MAGIC
# MAGIC Verify data quality improvements from Bronze to Silver.

# COMMAND ----------

bronze_count = spark.read.format("delta").load(f"{bronze_path}/orders").count()
silver_count = spark.read.format("delta").load(f"{silver_path}/orders").count()

print(f"Bronze records: {bronze_count}")
print(f"Silver records: {silver_count}")
print(f"Records filtered: {bronze_count - silver_count}")

# Check for nulls in key fields
null_check = spark.sql("""
    SELECT
        COUNT(*) as total_records,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
        SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_order_dates,
        SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_amounts
    FROM orders_silver
""")
null_check.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Gold Layer - Business-Level Aggregations
# MAGIC
# MAGIC The Gold layer creates analytics-ready datasets optimized for specific business use cases.
# MAGIC We will create three Gold tables:
# MAGIC 1. **Daily Revenue by City** -- for regional sales dashboards
# MAGIC 2. **Customer Purchase Summary** -- for customer analytics
# MAGIC 3. **Product Performance** -- for inventory and merchandising

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Daily Revenue by City

# COMMAND ----------

df_silver_orders = spark.read.format("delta").load(f"{silver_path}/orders")

# Daily revenue aggregation by city
df_daily_revenue = (
    df_silver_orders
    .withColumn("order_day", date_trunc("day", col("order_date")))
    .groupBy("order_day", "city")
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_sold"),
    )
    .orderBy("order_day", "city")
)

(df_daily_revenue.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/daily_revenue_by_city")
)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_daily_revenue USING DELTA LOCATION '{gold_path}/daily_revenue_by_city'")

print("Gold table 'daily_revenue_by_city' created")
df_daily_revenue.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Customer Purchase Summary

# COMMAND ----------

# Customer-level aggregation
df_customer_summary = (
    df_silver_orders
    .groupBy("customer_id", "first_name", "last_name", "city", "tier")
    .agg(
        _count("order_id").alias("total_orders"),
        _sum("total_amount").alias("lifetime_spend"),
        _sum("quantity").alias("total_items"),
        _max("order_date").alias("last_order_date"),
        _round(_sum("total_amount") / _count("order_id"), 2).alias("avg_order_value"),
    )
    .orderBy(col("lifetime_spend").desc())
)

(df_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/customer_summary")
)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_customer_summary USING DELTA LOCATION '{gold_path}/customer_summary'")

print("Gold table 'customer_summary' created")
df_customer_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 3: Product Performance

# COMMAND ----------

# Product-level aggregation
df_product_performance = (
    df_silver_orders
    .groupBy("product_id", "product_name", "category", "price")
    .agg(
        _count("order_id").alias("times_ordered"),
        _sum("quantity").alias("total_units_sold"),
        _sum("total_amount").alias("total_revenue"),
    )
    .withColumn("avg_units_per_order", _round(col("total_units_sold") / col("times_ordered"), 2))
    .orderBy(col("total_revenue").desc())
)

(df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/product_performance")
)

spark.sql(f"CREATE TABLE IF NOT EXISTS gold_product_performance USING DELTA LOCATION '{gold_path}/product_performance'")

print("Gold table 'product_performance' created")
df_product_performance.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Incremental Processing - Adding New Data
# MAGIC
# MAGIC One of the key benefits of Medallion Architecture is **incremental ETL**.
# MAGIC When new data arrives, only the new records flow through the pipeline.

# COMMAND ----------

# Generate Batch 2: new orders arriving later
random.seed(99)

orders_batch2 = []
base_ts2 = 1700000000 + 86400 * 31  # starts after batch 1
for i in range(51, 76):
    orders_batch2.append((
        f"ORD-{i:04d}",
        random.choice(["C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008"]),
        random.choice(["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]),
        random.randint(1, 5),
        base_ts2 + random.randint(0, 86400 * 15),
        random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ))

df_orders_batch2 = spark.createDataFrame(orders_batch2, orders_schema)
df_orders_batch2.write.mode("overwrite").parquet(f"{raw_data_path}/orders/batch2")
print(f"Batch 2: {df_orders_batch2.count()} new orders written to raw/orders/batch2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Bronze Ingestion

# COMMAND ----------

# Ingest only the new batch into Bronze (append mode)
df_new_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch2")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch2"))
)

(df_new_bronze.write
    .format("delta")
    .mode("append")  # APPEND - do not overwrite existing Bronze data
    .save(f"{bronze_path}/orders")
)

total_bronze = spark.read.format("delta").load(f"{bronze_path}/orders").count()
print(f"Bronze layer now has {total_bronze} total records (added {df_new_bronze.count()} new)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Silver Refresh

# COMMAND ----------

# Re-process full Bronze -> Silver (in production, use streaming or MERGE for true incremental)
df_all_bronze = spark.read.format("delta").load(f"{bronze_path}/orders")

df_silver_refreshed = (
    df_all_bronze
    .dropDuplicates(["order_id"])
    .filter(col("quantity") > 0)
    .filter(col("customer_id").isNotNull())
    .join(df_cust, "customer_id", "inner")
    .join(df_prod, "product_id", "inner")
    .withColumn(
        "order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    )
    .withColumn("total_amount", _round(col("quantity") * col("price"), 2))
    .select(
        "order_id", "order_date", "customer_id", "first_name", "last_name",
        "email", "city", "tier", "product_id", "product_name", "category",
        "quantity", "price", "total_amount", "payment_method",
    )
)

(df_silver_refreshed.write
    .format("delta")
    .mode("overwrite")
    .save(f"{silver_path}/orders")
)

print(f"Silver layer refreshed: {df_silver_refreshed.count()} total records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Gold Refresh

# COMMAND ----------

# Refresh Gold: Customer Summary after new data
df_silver_all = spark.read.format("delta").load(f"{silver_path}/orders")

df_customer_updated = (
    df_silver_all
    .groupBy("customer_id", "first_name", "last_name", "city", "tier")
    .agg(
        _count("order_id").alias("total_orders"),
        _sum("total_amount").alias("lifetime_spend"),
        _sum("quantity").alias("total_items"),
        _max("order_date").alias("last_order_date"),
        _round(_sum("total_amount") / _count("order_id"), 2).alias("avg_order_value"),
    )
    .orderBy(col("lifetime_spend").desc())
)

(df_customer_updated.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/customer_summary")
)

print("Gold customer_summary refreshed with new data:")
df_customer_updated.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Streaming Multi-Hop Pipeline (Reference Pattern)
# MAGIC
# MAGIC Below is the complete streaming pattern for a production Medallion pipeline on AWS S3.
# MAGIC Each layer reads as a stream from the previous layer.
# MAGIC
# MAGIC **Note**: This section demonstrates the streaming API pattern. In production,
# MAGIC you would run these as continuous or triggered streaming jobs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Bronze -> Silver -> Gold Pattern
# MAGIC
# MAGIC ```python
# MAGIC # ---- BRONZE: Auto Loader from S3 ----
# MAGIC bronze_stream = (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "parquet")
# MAGIC         .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/orders_raw")
# MAGIC         .load("s3://bucket/raw/orders/")
# MAGIC         .withColumn("load_time", current_timestamp())
# MAGIC         .withColumn("source_file", input_file_name())
# MAGIC )
# MAGIC
# MAGIC bronze_write = (
# MAGIC     bronze_stream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://bucket/checkpoints/orders_bronze")
# MAGIC         .outputMode("append")
# MAGIC         .table("orders_bronze")
# MAGIC )
# MAGIC
# MAGIC # ---- SILVER: Stream from Bronze ----
# MAGIC silver_stream = (
# MAGIC     spark.readStream
# MAGIC         .table("orders_bronze")
# MAGIC         .join(spark.table("customers_lookup"), "customer_id", "inner")
# MAGIC         .join(spark.table("products_lookup"), "product_id", "inner")
# MAGIC         .withColumn("order_date",
# MAGIC             from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
# MAGIC         .withColumn("total_amount", col("quantity") * col("price"))
# MAGIC         .filter(col("quantity") > 0)
# MAGIC )
# MAGIC
# MAGIC silver_write = (
# MAGIC     silver_stream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://bucket/checkpoints/orders_silver")
# MAGIC         .outputMode("append")
# MAGIC         .table("orders_silver")
# MAGIC )
# MAGIC
# MAGIC # ---- GOLD: Triggered batch from Silver ----
# MAGIC gold_stream = (
# MAGIC     spark.readStream
# MAGIC         .table("orders_silver")
# MAGIC         .withColumn("order_day", date_trunc("day", col("order_date")))
# MAGIC         .groupBy("order_day", "customer_id", "first_name", "last_name")
# MAGIC         .agg(sum("quantity").alias("daily_items"),
# MAGIC              sum("total_amount").alias("daily_spend"))
# MAGIC )
# MAGIC
# MAGIC gold_write = (
# MAGIC     gold_stream.writeStream
# MAGIC         .format("delta")
# MAGIC         .option("checkpointLocation", "s3://bucket/checkpoints/daily_customer_books")
# MAGIC         .outputMode("complete")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .table("daily_customer_summary")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Query Gold Tables - Business Analytics
# MAGIC
# MAGIC The Gold layer is where business users and BI tools connect.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top customers by lifetime spend
# MAGIC SELECT first_name, last_name, city, tier,
# MAGIC        total_orders, lifetime_spend, avg_order_value, last_order_date
# MAGIC FROM gold_customer_summary
# MAGIC ORDER BY lifetime_spend DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily revenue trends by city
# MAGIC SELECT order_day, city, total_revenue, total_orders, total_items_sold
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY order_day, total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Best selling products
# MAGIC SELECT product_name, times_ordered, total_units_sold, total_revenue
# MAGIC FROM gold_product_performance
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Delta Lake Features Across Layers
# MAGIC
# MAGIC Delta Lake provides ACID transactions, time travel, and history at every layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Bronze table history (shows all writes including incremental appends)
# MAGIC DESCRIBE HISTORY orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: query Bronze as it was before Batch 2 was added
# MAGIC SELECT COUNT(*) as records_at_version_0
# MAGIC FROM orders_bronze VERSION AS OF 0

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Compare: current Bronze count
# MAGIC SELECT COUNT(*) as current_records FROM orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC | Layer | Table | Records | Purpose |
# MAGIC |-------|-------|---------|---------|
# MAGIC | Bronze | `orders_bronze` | Raw + metadata | Audit trail, reprocessing |
# MAGIC | Silver | `orders_silver` | Cleaned + joined | Enterprise view, ad-hoc analytics |
# MAGIC | Silver | `customers_lookup` | Reference data | Customer master |
# MAGIC | Silver | `products_lookup` | Reference data | Product catalog |
# MAGIC | Gold | `gold_daily_revenue` | City aggregates | Regional dashboards |
# MAGIC | Gold | `gold_customer_summary` | Customer metrics | Customer analytics |
# MAGIC | Gold | `gold_product_performance` | Product metrics | Merchandising insights |
# MAGIC
# MAGIC ### Key Takeaways
# MAGIC
# MAGIC 1. **Bronze** is append-only and immutable -- your audit trail
# MAGIC 2. **Silver** applies just-enough transformations (ELT approach)
# MAGIC 3. **Gold** is optimized for specific business use cases
# MAGIC 4. **Incremental processing** ensures only new data flows through
# MAGIC 5. **Delta Lake** provides ACID, time travel, and schema enforcement at every layer
# MAGIC 6. **Auto Loader + Structured Streaming** enable production-grade pipelines on AWS S3

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove all tables and data created in this lab.

# COMMAND ----------

# Drop all tables
for table in ["orders_bronze", "orders_silver", "customers_lookup", "products_lookup",
              "gold_daily_revenue", "gold_customer_summary", "gold_product_performance"]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped table: {table}")

# Remove data files
dbutils.fs.rm(base_path, recurse=True)
print(f"\nRemoved all data at: {base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 19**: [Structured Streaming](../day19-structured-streaming/) -- deep dive into streaming pipelines
# MAGIC - Try converting this batch pipeline to a fully streaming pipeline using Auto Loader
# MAGIC - Add data quality checks using Delta Lake constraints or Great Expectations
# MAGIC - Explore Unity Catalog for governance across layers
