# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Medallion Architecture: Bronze --> Silver --> Gold
# MAGIC > Module 03 -- Topic 07 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will build a complete Medallion pipeline:
# MAGIC 1. Generate raw e-commerce event data
# MAGIC 2. Ingest into Bronze (with metadata columns)
# MAGIC 3. Clean and deduplicate into Silver
# MAGIC 4. Aggregate into Gold summary tables
# MAGIC 5. Validate data quality across all layers

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Generate Raw E-Commerce Data
# MAGIC
# MAGIC We simulate raw events as they might arrive from an event stream or API.
# MAGIC Data intentionally contains duplicates, nulls, and messy formats.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from pyspark.sql.window import Window
import random

spark.sql("CREATE DATABASE IF NOT EXISTS module03")

# Drop any existing tables
for table in ["bronze_orders_raw", "silver_orders", "silver_orders_quarantine",
              "gold_daily_sales", "gold_product_summary"]:
    spark.sql(f"DROP TABLE IF EXISTS module03.{table}")

# Generate messy raw data (simulates what arrives from a source system)
random.seed(42)
products = ["Laptop", "Phone", "Tablet", "Headphones", "Charger",
            "Keyboard", "Mouse", "Monitor", "Webcam", "Speaker"]
regions = ["US-West", "US-East", "EU-West", "EU-East", "APAC"]
statuses = ["completed", "pending", "cancelled"]

raw_events = []
for i in range(500):
    order_id = str(random.randint(1000, 1400))  # intentional ID overlap = duplicates
    product = random.choice(products)
    region = random.choice(regions)
    status = random.choice(statuses)
    quantity = str(random.randint(1, 5))
    # Messy price: sometimes with $, sometimes empty
    price_val = round(random.uniform(9.99, 1499.99), 2)
    price = f"${price_val}" if random.random() > 0.1 else str(price_val)
    if random.random() < 0.03:
        price = ""  # 3% missing prices
    date = f"2025-01-{random.randint(1, 28):02d}"
    customer = f"CUST-{random.randint(100, 300)}" if random.random() > 0.02 else None

    raw_events.append((order_id, product, region, quantity, price, date, status, customer))

raw_schema = StructType([
    StructField("order_id", StringType()),
    StructField("product", StringType()),
    StructField("region", StringType()),
    StructField("quantity", StringType()),     # String on purpose (messy)
    StructField("price", StringType()),        # String with $ signs (messy)
    StructField("order_date", StringType()),
    StructField("status", StringType()),
    StructField("customer_id", StringType()),
])

raw_df = spark.createDataFrame(raw_events, schema=raw_schema)

print(f"Generated {raw_df.count()} raw events")
print("Sample (notice messy data):")
raw_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## BRONZE LAYER -- Raw Ingestion
# MAGIC
# MAGIC Rules:
# MAGIC - Store data exactly as received
# MAGIC - Add metadata columns (ingestion time, source)
# MAGIC - Append-only (never update or delete)
# MAGIC - No data cleansing

# COMMAND ----------

bronze_df = (raw_df
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("ecommerce_api"))
    .withColumn("_batch_id", F.lit("batch_2025_01"))
)

bronze_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("module03.bronze_orders_raw")

print("BRONZE table created:")
print(f"  Rows:    {spark.table('module03.bronze_orders_raw').count()}")
print(f"  Columns: {len(spark.table('module03.bronze_orders_raw').columns)}")
spark.table("module03.bronze_orders_raw").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## SILVER LAYER -- Clean, Deduplicate, Type-Cast
# MAGIC
# MAGIC Transformations:
# MAGIC 1. Remove $ from price and cast to DOUBLE
# MAGIC 2. Cast quantity to INT
# MAGIC 3. Cast order_date to DATE
# MAGIC 4. Deduplicate on order_id (keep latest ingested)
# MAGIC 5. Quarantine records with null/invalid data

# COMMAND ----------

# Step 1: Read Bronze
bronze = spark.table("module03.bronze_orders_raw")

# Step 2: Clean price (remove $, cast to double)
cleaned = (bronze
    .withColumn("price_clean",
        F.regexp_replace(F.col("price"), "\\$", "").cast("double"))
    .withColumn("quantity_clean",
        F.col("quantity").cast("int"))
    .withColumn("order_date_clean",
        F.to_date(F.col("order_date"), "yyyy-MM-dd"))
    .withColumn("order_id_clean",
        F.col("order_id").cast("int"))
)

# Step 3: Separate good and bad records
good_records = cleaned.filter(
    "price_clean IS NOT NULL AND "
    "quantity_clean IS NOT NULL AND "
    "order_id_clean IS NOT NULL AND "
    "customer_id IS NOT NULL AND "
    "price_clean > 0"
)

bad_records = cleaned.filter(
    "price_clean IS NULL OR "
    "quantity_clean IS NULL OR "
    "order_id_clean IS NULL OR "
    "customer_id IS NULL OR "
    "price_clean <= 0"
)

print(f"Good records: {good_records.count()}")
print(f"Bad records (quarantined): {bad_records.count()}")

# COMMAND ----------

# Step 4: Deduplicate -- keep the latest record per order_id
window = Window.partitionBy("order_id_clean").orderBy(F.desc("_ingest_timestamp"))

silver_df = (good_records
    .withColumn("_row_num", F.row_number().over(window))
    .filter("_row_num = 1")
    .drop("_row_num")
    .select(
        F.col("order_id_clean").alias("order_id"),
        F.col("product"),
        F.col("region"),
        F.col("quantity_clean").alias("quantity"),
        F.col("price_clean").alias("unit_price"),
        (F.col("price_clean") * F.col("quantity_clean")).alias("total_amount"),
        F.col("order_date_clean").alias("order_date"),
        F.col("status"),
        F.col("customer_id"),
        F.col("_ingest_timestamp"),
    )
)

# Write Silver table
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("module03.silver_orders")

# Write quarantine table
bad_records.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("module03.silver_orders_quarantine")

print(f"\nSILVER table: {spark.table('module03.silver_orders').count()} rows")
print(f"QUARANTINE:   {spark.table('module03.silver_orders_quarantine').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Silver Data Quality

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver data: clean, deduplicated, properly typed
# MAGIC SELECT * FROM module03.silver_orders
# MAGIC ORDER BY order_id
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: no duplicates on order_id
# MAGIC SELECT order_id, count(*) AS cnt
# MAGIC FROM module03.silver_orders
# MAGIC GROUP BY order_id
# MAGIC HAVING cnt > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ## GOLD LAYER -- Business Aggregates
# MAGIC
# MAGIC Gold tables are pre-computed, business-oriented summaries.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 1: Daily Sales Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE module03.gold_daily_sales AS
# MAGIC SELECT
# MAGIC   order_date,
# MAGIC   region,
# MAGIC   count(order_id) AS total_orders,
# MAGIC   sum(total_amount) AS total_revenue,
# MAGIC   round(avg(total_amount), 2) AS avg_order_value,
# MAGIC   sum(quantity) AS total_units_sold
# MAGIC FROM module03.silver_orders
# MAGIC WHERE status = 'completed'
# MAGIC GROUP BY order_date, region
# MAGIC ORDER BY order_date, region;
# MAGIC
# MAGIC SELECT * FROM module03.gold_daily_sales LIMIT 15;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Table 2: Product Performance Summary

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE module03.gold_product_summary AS
# MAGIC SELECT
# MAGIC   product,
# MAGIC   count(order_id) AS total_orders,
# MAGIC   sum(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_orders,
# MAGIC   sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
# MAGIC   round(sum(total_amount), 2) AS total_revenue,
# MAGIC   round(avg(unit_price), 2) AS avg_unit_price,
# MAGIC   sum(quantity) AS total_units,
# MAGIC   round(
# MAGIC     sum(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) * 100.0
# MAGIC     / count(order_id), 1
# MAGIC   ) AS cancellation_rate_pct
# MAGIC FROM module03.silver_orders
# MAGIC GROUP BY product
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT * FROM module03.gold_product_summary;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Validation Across Layers

# COMMAND ----------

bronze_count = spark.table("module03.bronze_orders_raw").count()
silver_count = spark.table("module03.silver_orders").count()
quarantine_count = spark.table("module03.silver_orders_quarantine").count()
gold_orders = spark.table("module03.gold_daily_sales").agg(
    F.sum("total_orders")).collect()[0][0]

print("=" * 60)
print("MEDALLION PIPELINE DATA QUALITY REPORT")
print("=" * 60)
print(f"Bronze (raw events):           {bronze_count:>6}")
print(f"Silver (clean, deduped):       {silver_count:>6}")
print(f"Quarantine (bad records):      {quarantine_count:>6}")
print(f"Gold (completed order count):  {gold_orders:>6}")
print("-" * 60)
print(f"Dedup reduction:               {bronze_count - silver_count - quarantine_count:>6} rows removed")
completeness = (silver_count + quarantine_count) / bronze_count * 100
print(f"Completeness (Silver+Quarantine/Bronze): {completeness:.1f}%")
print("=" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC ```
# MAGIC Bronze (500 raw events)
# MAGIC   |  add metadata, no transforms
# MAGIC   v
# MAGIC Silver (deduped, typed, quality-checked)
# MAGIC   |  + Quarantine (bad records separated)
# MAGIC   v
# MAGIC Gold (daily sales, product performance)
# MAGIC   |  pre-aggregated for BI
# MAGIC   v
# MAGIC Dashboards & Reports
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for table in ["bronze_orders_raw", "silver_orders", "silver_orders_quarantine",
              "gold_daily_sales", "gold_product_summary"]:
    spark.sql(f"DROP TABLE IF EXISTS module03.{table}")

spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
