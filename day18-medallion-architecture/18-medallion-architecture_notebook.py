# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 18: Medallion Architecture - Production Multi-Hop Pipeline
# MAGIC
# MAGIC **Objective**: Build a production-grade Bronze -> Silver -> Gold pipeline using Delta Lake on AWS S3 with Unity Catalog
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Set up Unity Catalog schema and S3 storage paths
# MAGIC 2. Generate sample retail data (orders, customers, products)
# MAGIC 3. Ingest raw data into the **Bronze** layer with metadata enrichment and schema enforcement
# MAGIC 4. Clean, validate, and join data in the **Silver** layer using MERGE for idempotent writes
# MAGIC 5. Create business-level aggregations in the **Gold** layer with table optimization
# MAGIC 6. Demonstrate incremental processing with new data batches
# MAGIC 7. Leverage Delta Lake features: time travel, history, constraints, OPTIMIZE
# MAGIC
# MAGIC **Architecture**:
# MAGIC ```
# MAGIC Raw Files (S3)
# MAGIC      |
# MAGIC      v
# MAGIC Bronze Layer (raw + metadata, append-only, immutable audit trail)
# MAGIC      |
# MAGIC      v
# MAGIC Silver Layer (cleansed, deduplicated, joined, validated)
# MAGIC      |
# MAGIC      v
# MAGIC Gold Layer (business aggregations, star schema, KPI-ready)
# MAGIC      |
# MAGIC      v
# MAGIC Dashboards / ML Models / Reports
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Unity Catalog, S3 Paths, and Configuration

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use Unity Catalog for governance and access control
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a dedicated schema for this lab
# MAGIC CREATE SCHEMA IF NOT EXISTS medallion_lab
# MAGIC COMMENT 'Day 18: Medallion Architecture production lab'

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA medallion_lab

# COMMAND ----------

# Production S3 paths - organized by medallion layer
base_path = "s3://databricks-zero-to-pro/medallion_lab"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
checkpoint_path = f"{base_path}/checkpoints"
raw_data_path = f"{base_path}/raw"

print("Medallion Architecture Storage Layout (AWS S3)")
print("=" * 55)
print(f"Raw data:    {raw_data_path}")
print(f"Bronze:      {bronze_path}")
print(f"Silver:      {silver_path}")
print(f"Gold:        {gold_path}")
print(f"Checkpoints: {checkpoint_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Sample Retail Data
# MAGIC
# MAGIC Self-contained sample data -- no external dependencies required.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, from_unixtime,
    date_trunc, sum as _sum, count as _count, max as _max, min as _min,
    round as _round, when, lit, to_json, struct, avg as _avg,
    countDistinct, coalesce
)
from delta.tables import DeltaTable
import random

# COMMAND ----------

# MAGIC %md
# MAGIC ### Customers Reference Data

# COMMAND ----------

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
df_customers.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Products Reference Data

# COMMAND ----------

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
df_products.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Raw Order Events (Batch 1)
# MAGIC
# MAGIC Simulates POS/e-commerce transactions landing in S3 as Parquet files.

# COMMAND ----------

random.seed(42)

orders_batch1 = []
base_ts = 1700000000  # Nov 2023 epoch
for i in range(1, 51):
    orders_batch1.append((
        f"ORD-{i:04d}",
        random.choice(["C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008"]),
        random.choice(["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]),
        random.randint(1, 5),
        base_ts + random.randint(0, 86400 * 30),
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
df_orders_batch1.write.mode("overwrite").parquet(f"{raw_data_path}/orders/batch1")

print(f"Batch 1: {df_orders_batch1.count()} orders written to {raw_data_path}/orders/batch1")
df_orders_batch1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC The Bronze layer stores data **exactly as received** from source systems.
# MAGIC
# MAGIC **Production best practices applied**:
# MAGIC - Metadata columns (`load_time`, `source_file`) for auditability
# MAGIC - Delta table properties for auto-optimization
# MAGIC - Table constraints for data integrity
# MAGIC - Append-only writes to maintain immutable audit trail
# MAGIC
# MAGIC In production, use **Auto Loader** (`cloudFiles`) for streaming ingestion.
# MAGIC This lab demonstrates the batch approach. See **Day 19** for full Auto Loader patterns.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. Bronze Ingestion (Batch Approach)

# COMMAND ----------

df_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch1")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch1"))
)

# Write to Bronze Delta table on S3
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{bronze_path}/orders")
)

# Register as managed table in Unity Catalog
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS orders_bronze
    USING DELTA
    LOCATION '{bronze_path}/orders'
    COMMENT 'Bronze layer: raw order events with ingestion metadata'
""")

print(f"Bronze layer: {df_bronze.count()} records ingested")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set Delta table properties for production optimization
# MAGIC ALTER TABLE orders_bronze SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze data: raw records with metadata
# MAGIC SELECT order_id, customer_id, product_id, quantity,
# MAGIC        order_timestamp, payment_method, load_time, source_file
# MAGIC FROM orders_bronze
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. Auto Loader Pattern (Production Reference)
# MAGIC
# MAGIC This is the **recommended production pattern** for ingesting files from S3.
# MAGIC Auto Loader automatically discovers new files and processes them incrementally.
# MAGIC
# MAGIC Two modes are available:
# MAGIC - **Directory listing mode** (default): polls S3 directory for new files
# MAGIC - **File notification mode**: uses S3 event notifications via SQS for near-real-time detection
# MAGIC
# MAGIC See **Day 19: Structured Streaming** for hands-on Auto Loader labs.
# MAGIC
# MAGIC ```python
# MAGIC # Production Auto Loader with file notification mode (S3 + SQS)
# MAGIC (spark.readStream
# MAGIC     .format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "parquet")
# MAGIC     .option("cloudFiles.useNotifications", "true")  # uses S3 -> SQS notifications
# MAGIC     .option("cloudFiles.schemaLocation", f"{checkpoint_path}/orders_raw_schema")
# MAGIC     .load(f"{raw_data_path}/orders/")
# MAGIC     .withColumn("load_time", current_timestamp())
# MAGIC     .withColumn("source_file", input_file_name())
# MAGIC     .writeStream
# MAGIC     .format("delta")
# MAGIC     .option("checkpointLocation", f"{checkpoint_path}/orders_bronze")
# MAGIC     .outputMode("append")
# MAGIC     .table("orders_bronze")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Silver Layer - Cleansed and Conformed Data
# MAGIC
# MAGIC **Production transformations applied**:
# MAGIC 1. **Deduplicate** on order_id (business key)
# MAGIC 2. **Filter** invalid records (null customer_id, zero quantity)
# MAGIC 3. **Join** with customer and product lookup tables
# MAGIC 4. **Parse** Unix timestamps into human-readable format
# MAGIC 5. **Calculate** derived fields (total_amount)
# MAGIC 6. **Schema enforcement** with explicit column selection

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Lookup Tables to Silver Layer

# COMMAND ----------

# Persist reference data as Delta tables
df_customers.write.format("delta").mode("overwrite").save(f"{silver_path}/customers_lookup")
df_products.write.format("delta").mode("overwrite").save(f"{silver_path}/products_lookup")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS customers_lookup
    USING DELTA LOCATION '{silver_path}/customers_lookup'
    COMMENT 'Silver layer: customer master reference data'
""")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS products_lookup
    USING DELTA LOCATION '{silver_path}/products_lookup'
    COMMENT 'Silver layer: product catalog reference data'
""")

print("Lookup tables registered in Unity Catalog")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM products_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Transformations: Clean, Join, Validate

# COMMAND ----------

df_bronze_orders = spark.read.format("delta").load(f"{bronze_path}/orders")
df_cust = spark.read.format("delta").load(f"{silver_path}/customers_lookup")
df_prod = spark.read.format("delta").load(f"{silver_path}/products_lookup")

df_silver = (
    df_bronze_orders
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

(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{silver_path}/orders")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS orders_silver
    USING DELTA LOCATION '{silver_path}/orders'
    COMMENT 'Silver layer: cleansed, deduplicated, and enriched order data'
""")

print(f"Silver layer: {df_silver.count()} cleaned records")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Production table properties
# MAGIC ALTER TABLE orders_silver SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add data quality constraints to Silver table
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
# MAGIC ALTER TABLE orders_silver ADD CONSTRAINT valid_amount CHECK (total_amount > 0);

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
# MAGIC Compare record counts and validate data quality between Bronze and Silver.

# COMMAND ----------

bronze_count = spark.read.format("delta").load(f"{bronze_path}/orders").count()
silver_count = spark.read.format("delta").load(f"{silver_path}/orders").count()

print(f"Bronze records:    {bronze_count}")
print(f"Silver records:    {silver_count}")
print(f"Records filtered:  {bronze_count - silver_count}")
print(f"Pass-through rate: {silver_count / bronze_count * 100:.1f}%")

# COMMAND ----------

# Null check on critical fields
null_check = spark.sql("""
    SELECT
        COUNT(*) as total_records,
        SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
        SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
        SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_order_dates,
        SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_amounts,
        SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantities
    FROM orders_silver
""")
null_check.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Gold Layer - Business-Level Aggregations
# MAGIC
# MAGIC The Gold layer creates **analytics-ready** datasets optimized for specific business use cases.
# MAGIC
# MAGIC **Three Gold tables**:
# MAGIC 1. `gold_daily_revenue` -- regional sales dashboards
# MAGIC 2. `gold_customer_summary` -- customer analytics and LTV
# MAGIC 3. `gold_product_performance` -- inventory and merchandising

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Daily Revenue by City

# COMMAND ----------

df_silver_orders = spark.read.format("delta").load(f"{silver_path}/orders")

df_daily_revenue = (
    df_silver_orders
    .withColumn("order_day", date_trunc("day", col("order_date")))
    .groupBy("order_day", "city")
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_sold"),
        _avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .orderBy("order_day", "city")
)

(df_daily_revenue.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/daily_revenue_by_city")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_daily_revenue
    USING DELTA LOCATION '{gold_path}/daily_revenue_by_city'
    COMMENT 'Gold layer: daily revenue aggregated by city for regional dashboards'
""")

print("Gold table 'gold_daily_revenue' created")
df_daily_revenue.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Customer Purchase Summary (LTV)

# COMMAND ----------

df_customer_summary = (
    df_silver_orders
    .groupBy("customer_id", "first_name", "last_name", "city", "tier")
    .agg(
        _count("order_id").alias("total_orders"),
        _sum("total_amount").alias("lifetime_spend"),
        _sum("quantity").alias("total_items"),
        _max("order_date").alias("last_order_date"),
        _min("order_date").alias("first_order_date"),
        _round(_sum("total_amount") / _count("order_id"), 2).alias("avg_order_value"),
        countDistinct("product_id").alias("unique_products_bought"),
    )
    .orderBy(col("lifetime_spend").desc())
)

(df_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/customer_summary")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_customer_summary
    USING DELTA LOCATION '{gold_path}/customer_summary'
    COMMENT 'Gold layer: customer lifetime value and purchase behavior'
""")

print("Gold table 'gold_customer_summary' created")
df_customer_summary.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 3: Product Performance

# COMMAND ----------

df_product_performance = (
    df_silver_orders
    .groupBy("product_id", "product_name", "category", "price")
    .agg(
        _count("order_id").alias("times_ordered"),
        _sum("quantity").alias("total_units_sold"),
        _sum("total_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("avg_units_per_order", _round(col("total_units_sold") / col("times_ordered"), 2))
    .withColumn("revenue_rank", _round(col("total_revenue"), 2))
    .orderBy(col("total_revenue").desc())
)

(df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/product_performance")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold_product_performance
    USING DELTA LOCATION '{gold_path}/product_performance'
    COMMENT 'Gold layer: product sales performance and merchandising metrics'
""")

print("Gold table 'gold_product_performance' created")
df_product_performance.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Incremental Processing - Adding New Data
# MAGIC
# MAGIC One of the key benefits of Medallion Architecture is **incremental ETL**.
# MAGIC When new data arrives, only the new records flow through the pipeline.
# MAGIC
# MAGIC **Production pattern**: Bronze uses APPEND, Silver uses MERGE (upsert), Gold uses full refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Batch 2: New Orders

# COMMAND ----------

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
print(f"Batch 2: {df_orders_batch2.count()} new orders written to {raw_data_path}/orders/batch2")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Bronze Ingestion (Append)
# MAGIC
# MAGIC Bronze is always **append-only** -- we never modify or delete existing records.

# COMMAND ----------

df_new_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch2")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch2"))
)

(df_new_bronze.write
    .format("delta")
    .mode("append")
    .save(f"{bronze_path}/orders")
)

total_bronze = spark.read.format("delta").load(f"{bronze_path}/orders").count()
print(f"Bronze layer: {total_bronze} total records (appended {df_new_bronze.count()} from batch2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Silver Refresh (MERGE / Upsert)
# MAGIC
# MAGIC In production, use **MERGE INTO** for idempotent Silver updates.
# MAGIC This handles both new records and updates to existing records (e.g., order amendments).

# COMMAND ----------

# Prepare the new data with Silver transformations
df_new_silver = (
    df_new_bronze
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

# MERGE INTO: insert new records, update existing ones (idempotent)
silver_delta = DeltaTable.forPath(spark, f"{silver_path}/orders")

(silver_delta.alias("target")
    .merge(df_new_silver.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

silver_count = spark.read.format("delta").load(f"{silver_path}/orders").count()
print(f"Silver layer after MERGE: {silver_count} total records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Gold Refresh
# MAGIC
# MAGIC Gold tables are typically fully refreshed from Silver since they contain aggregations.

# COMMAND ----------

df_silver_all = spark.read.format("delta").load(f"{silver_path}/orders")

# Refresh all three Gold tables
# 1. Daily Revenue
df_daily_updated = (
    df_silver_all
    .withColumn("order_day", date_trunc("day", col("order_date")))
    .groupBy("order_day", "city")
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_sold"),
        _avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
)
df_daily_updated.write.format("delta").mode("overwrite").save(f"{gold_path}/daily_revenue_by_city")

# 2. Customer Summary
df_cust_updated = (
    df_silver_all
    .groupBy("customer_id", "first_name", "last_name", "city", "tier")
    .agg(
        _count("order_id").alias("total_orders"),
        _sum("total_amount").alias("lifetime_spend"),
        _sum("quantity").alias("total_items"),
        _max("order_date").alias("last_order_date"),
        _min("order_date").alias("first_order_date"),
        _round(_sum("total_amount") / _count("order_id"), 2).alias("avg_order_value"),
        countDistinct("product_id").alias("unique_products_bought"),
    )
    .orderBy(col("lifetime_spend").desc())
)
df_cust_updated.write.format("delta").mode("overwrite").save(f"{gold_path}/customer_summary")

# 3. Product Performance
df_prod_updated = (
    df_silver_all
    .groupBy("product_id", "product_name", "category", "price")
    .agg(
        _count("order_id").alias("times_ordered"),
        _sum("quantity").alias("total_units_sold"),
        _sum("total_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("avg_units_per_order", _round(col("total_units_sold") / col("times_ordered"), 2))
    .orderBy(col("total_revenue").desc())
)
df_prod_updated.write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance")

print("All Gold tables refreshed with batch 2 data")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Query Gold Tables - Business Analytics
# MAGIC
# MAGIC The Gold layer is where business users and BI tools connect.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top customers by lifetime spend
# MAGIC SELECT first_name, last_name, city, tier,
# MAGIC        total_orders, lifetime_spend, avg_order_value,
# MAGIC        unique_products_bought, first_order_date, last_order_date
# MAGIC FROM gold_customer_summary
# MAGIC ORDER BY lifetime_spend DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Daily revenue trends by city
# MAGIC SELECT order_day, city, total_revenue, total_orders,
# MAGIC        total_items_sold, unique_customers,
# MAGIC        ROUND(avg_order_value, 2) as avg_order_value
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY order_day, total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Best selling products
# MAGIC SELECT product_name, times_ordered, total_units_sold,
# MAGIC        ROUND(total_revenue, 2) as total_revenue,
# MAGIC        unique_buyers, avg_units_per_order
# MAGIC FROM gold_product_performance
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Delta Lake Production Features
# MAGIC
# MAGIC Delta Lake provides ACID transactions, time travel, history, and optimization at every layer.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table History and Versioning

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View Bronze table history (shows initial write + incremental append)
# MAGIC DESCRIBE HISTORY orders_bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: query Bronze BEFORE batch 2 was added
# MAGIC SELECT COUNT(*) as records_at_version_0, 'Before Batch 2' as label
# MAGIC FROM orders_bronze VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT COUNT(*) as records_current, 'After Batch 2' as label
# MAGIC FROM orders_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Table History (MERGE Operations)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver history shows MERGE operations for incremental updates
# MAGIC DESCRIBE HISTORY orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Optimization
# MAGIC
# MAGIC In production, run OPTIMIZE periodically to compact small files and improve query performance.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Silver table and Z-ORDER on frequently queried columns
# MAGIC OPTIMIZE orders_silver ZORDER BY (customer_id, order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Optimize Gold tables
# MAGIC OPTIMIZE gold_daily_revenue ZORDER BY (order_day, city)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Table Metadata and Constraints

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table details including constraints, properties, and storage
# MAGIC DESCRIBE EXTENDED orders_silver

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View table constraints
# MAGIC SHOW TBLPROPERTIES orders_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 7: Streaming Multi-Hop Pipeline (Production Reference)
# MAGIC
# MAGIC The complete streaming Medallion pipeline for production use on AWS S3.
# MAGIC Each layer reads as a stream from the previous layer.
# MAGIC
# MAGIC See **Day 19: Structured Streaming** for hands-on streaming labs.
# MAGIC
# MAGIC ```python
# MAGIC # ---- BRONZE: Auto Loader from S3 ----
# MAGIC bronze_stream = (
# MAGIC     spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "parquet")
# MAGIC         .option("cloudFiles.useNotifications", "true")
# MAGIC         .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/orders_schema")
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
# MAGIC         .trigger(availableNow=True)
# MAGIC         .table("orders_bronze")
# MAGIC )
# MAGIC
# MAGIC # ---- SILVER: Stream from Bronze Delta table ----
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
# MAGIC         .trigger(availableNow=True)
# MAGIC         .table("orders_silver")
# MAGIC )
# MAGIC
# MAGIC # ---- GOLD: Triggered batch aggregation from Silver ----
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
# MAGIC         .option("checkpointLocation", "s3://bucket/checkpoints/gold_daily_summary")
# MAGIC         .outputMode("complete")
# MAGIC         .trigger(availableNow=True)
# MAGIC         .table("gold_daily_customer_summary")
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC | Layer | Table | Purpose | Write Mode | Optimization |
# MAGIC |-------|-------|---------|------------|--------------|
# MAGIC | Bronze | `orders_bronze` | Raw audit trail | APPEND only | Auto-optimize |
# MAGIC | Silver | `customers_lookup` | Customer master | OVERWRITE | - |
# MAGIC | Silver | `products_lookup` | Product catalog | OVERWRITE | - |
# MAGIC | Silver | `orders_silver` | Enriched orders | MERGE (upsert) | ZORDER by customer_id, order_date |
# MAGIC | Gold | `gold_daily_revenue` | Regional dashboards | OVERWRITE | ZORDER by order_day, city |
# MAGIC | Gold | `gold_customer_summary` | Customer LTV | OVERWRITE | - |
# MAGIC | Gold | `gold_product_performance` | Merchandising | OVERWRITE | - |
# MAGIC
# MAGIC ### Production Best Practices Applied
# MAGIC
# MAGIC 1. **Unity Catalog** for governance: `databricks_pro.medallion_lab.*`
# MAGIC 2. **Delta table properties**: auto-optimize, auto-compact enabled
# MAGIC 3. **CHECK constraints** on Silver: `quantity > 0`, `total_amount > 0`
# MAGIC 4. **MERGE (upsert)** for idempotent Silver incremental updates
# MAGIC 5. **OPTIMIZE + ZORDER** for query performance on frequently filtered columns
# MAGIC 6. **Time travel** for auditing and rollback capability
# MAGIC 7. **Table comments** for documentation and discoverability
# MAGIC 8. **Append-only Bronze** preserves immutable audit trail
# MAGIC 9. **S3 storage** with organized path hierarchy by layer

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove all tables and data created in this lab.

# COMMAND ----------

# Drop all tables from Unity Catalog
tables = [
    "orders_bronze", "orders_silver", "customers_lookup", "products_lookup",
    "gold_daily_revenue", "gold_customer_summary", "gold_product_performance"
]

for table in tables:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"Dropped table: {table}")

# COMMAND ----------

# Remove S3 data files
dbutils.fs.rm(base_path, recurse=True)
print(f"Removed all data at: {base_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop the lab schema
# MAGIC DROP SCHEMA IF EXISTS medallion_lab CASCADE

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 19**: [Structured Streaming & Auto Loader](../day19-structured-streaming/) -- streaming ingestion with S3 notifications and directory listing modes
# MAGIC - Add data quality checks with Delta Live Tables (DLT) expectations
# MAGIC - Implement row-level security using Unity Catalog
# MAGIC - Schedule pipeline orchestration with Databricks Workflows
