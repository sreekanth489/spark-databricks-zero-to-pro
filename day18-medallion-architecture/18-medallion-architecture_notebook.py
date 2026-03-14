# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 18: Medallion Architecture - Production Multi-Hop Pipeline
# MAGIC
# MAGIC **Objective**: Build a production-grade Bronze -> Silver -> Gold pipeline using Delta Lake on AWS S3 with Unity Catalog
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Set up **separate Unity Catalog schemas** for each layer (bronze, silver, gold)
# MAGIC 2. Generate sample retail data **including dirty records** (nulls, zero quantities)
# MAGIC 3. Ingest raw data into the **Bronze** layer with metadata enrichment
# MAGIC 4. Clean, validate, and join data in the **Silver** layer -- **proving dirty records are filtered**
# MAGIC 5. Create business-level aggregations in the **Gold** layer
# MAGIC 6. Demonstrate incremental processing with MERGE
# MAGIC 7. Leverage Delta Lake features: time travel, history, constraints, OPTIMIZE
# MAGIC
# MAGIC **Architecture**:
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/sreekanth489/spark-databricks-zero-to-pro/main/day18-medallion-architecture/images/medallion-architecture.png" width="800">
# MAGIC
# MAGIC <img src="https://raw.githubusercontent.com/sreekanth489/spark-databricks-zero-to-pro/main/day18-medallion-architecture/images/medallion-data-quality.png" width="800">
# MAGIC
# MAGIC ```
# MAGIC Raw Files (S3)
# MAGIC      |
# MAGIC      v
# MAGIC databricks_pro.bronze   (raw + metadata, append-only)
# MAGIC      |  Clean, Filter, Validate
# MAGIC      v
# MAGIC databricks_pro.silver   (deduplicated, enriched, quality-checked)
# MAGIC      |  Aggregate
# MAGIC      v
# MAGIC databricks_pro.gold     (business KPIs, star schema)
# MAGIC      |
# MAGIC      v
# MAGIC BI & Reporting / ML & AI / Streaming Analytics
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Unity Catalog Schemas (One per Layer)
# MAGIC
# MAGIC **Production standard**: Each medallion layer gets its own schema.
# MAGIC This provides clear separation of concerns, access control, and discoverability.
# MAGIC
# MAGIC ```
# MAGIC databricks_pro (catalog)
# MAGIC   ├── bronze   -- raw, unfiltered data
# MAGIC   ├── silver   -- cleansed, validated data
# MAGIC   └── gold     -- business-ready aggregations
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create separate schemas for each medallion layer
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Bronze layer: raw, unfiltered data with ingestion metadata';
# MAGIC CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Silver layer: cleansed, deduplicated, validated data';
# MAGIC CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Gold layer: business-ready aggregations and KPIs';

# COMMAND ----------

# Production S3 paths organized by layer
base_path = "s3://databricks-zero-to-pro/medallion_lab"
bronze_path = f"{base_path}/bronze"
silver_path = f"{base_path}/silver"
gold_path = f"{base_path}/gold"
checkpoint_path = f"{base_path}/checkpoints"
raw_data_path = f"{base_path}/raw"

print("Medallion Architecture Storage Layout")
print("=" * 55)
print(f"Raw data:    {raw_data_path}")
print(f"Bronze:      {bronze_path}  -> bronze.*")
print(f"Silver:      {silver_path}  -> silver.*")
print(f"Gold:        {gold_path}    -> gold.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Generate Sample Retail Data (Including Dirty Records)
# MAGIC
# MAGIC We intentionally include **bad data** to demonstrate Silver layer filtering:
# MAGIC - 2 orders with **NULL customer_id** (unknown customer)
# MAGIC - 1 order with **quantity = 0** (invalid order)
# MAGIC - 1 order with **negative quantity** (data error)
# MAGIC - 1 **duplicate order_id** (duplicate event)
# MAGIC
# MAGIC These will be ingested into Bronze as-is, then filtered out in Silver.

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
# MAGIC ### Generate Raw Order Events (Batch 1) -- WITH Dirty Data
# MAGIC
# MAGIC 50 normal orders + 5 dirty records = 55 total raw records.

# COMMAND ----------

random.seed(42)

# 50 valid orders
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

# DIRTY RECORDS: these should be filtered out by Silver layer
dirty_records = [
    ("ORD-9901", None, "P001", 2, base_ts + 100, "credit_card"),          # NULL customer_id
    ("ORD-9902", None, "P003", 1, base_ts + 200, "debit_card"),           # NULL customer_id
    ("ORD-9903", "C002", "P005", 0, base_ts + 300, "paypal"),             # quantity = 0
    ("ORD-9904", "C004", "P007", -1, base_ts + 400, "bank_transfer"),     # negative quantity
    ("ORD-0001", "C006", "P002", 3, base_ts + 500, "credit_card"),        # DUPLICATE of ORD-0001
]

all_orders = orders_batch1 + dirty_records

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("order_timestamp", LongType(), True),
    StructField("payment_method", StringType(), True),
])

df_orders_batch1 = spark.createDataFrame(all_orders, orders_schema)
df_orders_batch1.write.mode("overwrite").parquet(f"{raw_data_path}/orders/batch1")

print(f"Batch 1: {df_orders_batch1.count()} total records (50 valid + 5 dirty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Preview the Dirty Records
# MAGIC
# MAGIC These are the records that Silver layer should filter out.

# COMMAND ----------

# MAGIC %md
# MAGIC **Dirty records in raw data:**
# MAGIC | Order ID | Problem | Expected Silver Behavior |
# MAGIC |----------|---------|-------------------------|
# MAGIC | ORD-9901 | NULL customer_id | Filtered out (null check) |
# MAGIC | ORD-9902 | NULL customer_id | Filtered out (null check) |
# MAGIC | ORD-9903 | quantity = 0 | Filtered out (quantity > 0) |
# MAGIC | ORD-9904 | quantity = -1 | Filtered out (quantity > 0) |
# MAGIC | ORD-0001 | Duplicate order_id | Deduplicated (dropDuplicates) |

# COMMAND ----------

df_dirty_preview = df_orders_batch1.filter(
    col("order_id").isin("ORD-9901", "ORD-9902", "ORD-9903", "ORD-9904") |
    (col("customer_id").isNull()) |
    (col("quantity") <= 0)
)
print(f"Dirty records count: {df_dirty_preview.count()}")
df_dirty_preview.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Bronze Layer - Raw Data Ingestion
# MAGIC
# MAGIC Bronze stores data **exactly as received** -- including all dirty records.
# MAGIC No transformation, no filtering. Just raw data + ingestion metadata.
# MAGIC
# MAGIC **Schema**: `databricks_pro.bronze`

# COMMAND ----------

df_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch1")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch1"))
)

# Write to Bronze
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{bronze_path}/orders")
)

# Register in Bronze schema
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS bronze.orders
    USING DELTA
    LOCATION '{bronze_path}/orders'
    COMMENT 'Raw order events with ingestion metadata - includes dirty data'
""")

print(f"Bronze layer: {df_bronze.count()} records (including dirty data)")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bronze.orders SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze: ALL records including dirty data (nulls, zero qty, duplicates)
# MAGIC SELECT order_id, customer_id, product_id, quantity, payment_method,
# MAGIC        load_time, source_file
# MAGIC FROM bronze.orders
# MAGIC ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Dirty Records Exist in Bronze
# MAGIC
# MAGIC Bronze should contain ALL records -- dirty and clean alike.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show the dirty records sitting in Bronze
# MAGIC SELECT order_id, customer_id, product_id, quantity,
# MAGIC        CASE
# MAGIC            WHEN customer_id IS NULL THEN 'NULL customer_id'
# MAGIC            WHEN quantity <= 0 THEN 'Invalid quantity: ' || quantity
# MAGIC            ELSE 'Valid'
# MAGIC        END as data_quality_issue
# MAGIC FROM bronze.orders
# MAGIC WHERE customer_id IS NULL OR quantity <= 0
# MAGIC ORDER BY order_id

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Silver Layer - Cleansed and Validated Data
# MAGIC
# MAGIC The Silver layer **improves data quality** by:
# MAGIC 1. **Filtering** NULL customer_id records
# MAGIC 2. **Filtering** quantity <= 0 records
# MAGIC 3. **Deduplicating** on order_id
# MAGIC 4. **Joining** with customer and product reference data
# MAGIC 5. **Parsing** timestamps and calculating derived fields
# MAGIC
# MAGIC **Schema**: `databricks_pro.silver`

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save Reference Tables to Silver Schema

# COMMAND ----------

# Customers lookup
df_customers.write.format("delta").mode("overwrite").save(f"{silver_path}/customers")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.customers
    USING DELTA LOCATION '{silver_path}/customers'
    COMMENT 'Customer master reference data'
""")

# Products lookup
df_products.write.format("delta").mode("overwrite").save(f"{silver_path}/products")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.products
    USING DELTA LOCATION '{silver_path}/products'
    COMMENT 'Product catalog reference data'
""")

print("Reference tables registered in silver schema")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM silver.products

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Transformations: Clean, Filter, Join, Validate
# MAGIC
# MAGIC Watch the record count drop as dirty data is filtered out.

# COMMAND ----------

df_bronze_orders = spark.read.format("delta").load(f"{bronze_path}/orders")
df_cust = spark.read.format("delta").load(f"{silver_path}/customers")
df_prod = spark.read.format("delta").load(f"{silver_path}/products")

# Count before filtering
print(f"Bronze input records: {df_bronze_orders.count()}")

# Apply Silver transformations
df_silver = (
    df_bronze_orders
    # 1. Deduplicate on order_id (removes duplicate ORD-0001)
    .dropDuplicates(["order_id"])
    # 2. Filter null customer_id (removes ORD-9901, ORD-9902)
    .filter(col("customer_id").isNotNull())
    # 3. Filter invalid quantities (removes ORD-9903 qty=0, ORD-9904 qty=-1)
    .filter(col("quantity") > 0)
    # 4. Join with customer data
    .join(df_cust, "customer_id", "inner")
    # 5. Join with product data
    .join(df_prod, "product_id", "inner")
    # 6. Parse timestamp
    .withColumn(
        "order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
    )
    # 7. Calculate total amount
    .withColumn("total_amount", _round(col("quantity") * col("price"), 2))
    # 8. Select clean schema
    .select(
        "order_id", "order_date", "customer_id", "first_name", "last_name",
        "email", "city", "tier", "product_id", "product_name", "category",
        "quantity", "price", "total_amount", "payment_method",
    )
)

print(f"Silver output records: {df_silver.count()}")
print(f"Records filtered out: {df_bronze_orders.count() - df_silver.count()}")

# Write to Silver
(df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .save(f"{silver_path}/orders")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver.orders
    USING DELTA LOCATION '{silver_path}/orders'
    COMMENT 'Cleansed, deduplicated, and enriched order data'
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE silver.orders SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC ALTER TABLE silver.orders ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
# MAGIC ALTER TABLE silver.orders ADD CONSTRAINT valid_amount CHECK (total_amount > 0);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver: clean, enriched records only
# MAGIC SELECT order_id, order_date, first_name, last_name, city, tier,
# MAGIC        product_name, quantity, price, total_amount, payment_method
# MAGIC FROM silver.orders
# MAGIC ORDER BY order_date DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Quality Validation: Bronze vs Silver
# MAGIC
# MAGIC This is where we **prove** the Silver layer is working.
# MAGIC The dirty records from Bronze should NOT appear in Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Record count comparison
# MAGIC SELECT 'Bronze (raw)' as layer, COUNT(*) as records FROM bronze.orders
# MAGIC UNION ALL
# MAGIC SELECT 'Silver (clean)' as layer, COUNT(*) as records FROM silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: dirty order IDs should NOT be in Silver
# MAGIC SELECT 'ORD-9901 (null customer)' as dirty_record,
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'FILTERED (correct)' ELSE 'PRESENT (bug!)' END as status
# MAGIC FROM silver.orders WHERE order_id = 'ORD-9901'
# MAGIC UNION ALL
# MAGIC SELECT 'ORD-9902 (null customer)',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'FILTERED (correct)' ELSE 'PRESENT (bug!)' END
# MAGIC FROM silver.orders WHERE order_id = 'ORD-9902'
# MAGIC UNION ALL
# MAGIC SELECT 'ORD-9903 (qty=0)',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'FILTERED (correct)' ELSE 'PRESENT (bug!)' END
# MAGIC FROM silver.orders WHERE order_id = 'ORD-9903'
# MAGIC UNION ALL
# MAGIC SELECT 'ORD-9904 (qty=-1)',
# MAGIC        CASE WHEN COUNT(*) = 0 THEN 'FILTERED (correct)' ELSE 'PRESENT (bug!)' END
# MAGIC FROM silver.orders WHERE order_id = 'ORD-9904'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Confirm: Silver has zero nulls and zero invalid quantities
# MAGIC SELECT
# MAGIC     COUNT(*) as total_records,
# MAGIC     SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END) as null_order_ids,
# MAGIC     SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END) as null_customer_ids,
# MAGIC     SUM(CASE WHEN order_date IS NULL THEN 1 ELSE 0 END) as null_order_dates,
# MAGIC     SUM(CASE WHEN total_amount IS NULL THEN 1 ELSE 0 END) as null_amounts,
# MAGIC     SUM(CASE WHEN quantity <= 0 THEN 1 ELSE 0 END) as invalid_quantities
# MAGIC FROM silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify deduplication: no duplicate order_ids in Silver
# MAGIC SELECT order_id, COUNT(*) as occurrences
# MAGIC FROM silver.orders
# MAGIC GROUP BY order_id
# MAGIC HAVING COUNT(*) > 1

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Gold Layer - Business-Level Aggregations
# MAGIC
# MAGIC **Schema**: `databricks_pro.gold`
# MAGIC
# MAGIC Three Gold tables for different business use cases:
# MAGIC 1. `daily_revenue` -- regional sales dashboards (BI & Reporting)
# MAGIC 2. `customer_summary` -- customer LTV analytics (ML & AI)
# MAGIC 3. `product_performance` -- merchandising insights (Streaming Analytics)

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
    .save(f"{gold_path}/daily_revenue")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.daily_revenue
    USING DELTA LOCATION '{gold_path}/daily_revenue'
    COMMENT 'Daily revenue by city for regional dashboards'
""")

print("Gold: gold.daily_revenue created")
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
    CREATE TABLE IF NOT EXISTS gold.customer_summary
    USING DELTA LOCATION '{gold_path}/customer_summary'
    COMMENT 'Customer lifetime value and purchase behavior'
""")

print("Gold: gold.customer_summary created")
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
    .orderBy(col("total_revenue").desc())
)

(df_product_performance.write
    .format("delta")
    .mode("overwrite")
    .save(f"{gold_path}/product_performance")
)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS gold.product_performance
    USING DELTA LOCATION '{gold_path}/product_performance'
    COMMENT 'Product sales performance and merchandising metrics'
""")

print("Gold: gold.product_performance created")
df_product_performance.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Incremental Processing - Adding New Data
# MAGIC
# MAGIC **Production pattern**: Bronze APPEND -> Silver MERGE -> Gold full refresh.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Batch 2 (also with some dirty data)

# COMMAND ----------

random.seed(99)

orders_batch2 = []
base_ts2 = 1700000000 + 86400 * 31
for i in range(51, 76):
    orders_batch2.append((
        f"ORD-{i:04d}",
        random.choice(["C001", "C002", "C003", "C004", "C005", "C006", "C007", "C008"]),
        random.choice(["P001", "P002", "P003", "P004", "P005", "P006", "P007", "P008"]),
        random.randint(1, 5),
        base_ts2 + random.randint(0, 86400 * 15),
        random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
    ))

# Add 1 more dirty record in batch 2
orders_batch2.append(("ORD-9905", None, "P004", 3, base_ts2 + 100, "paypal"))  # NULL customer

df_orders_batch2 = spark.createDataFrame(orders_batch2, orders_schema)
df_orders_batch2.write.mode("overwrite").parquet(f"{raw_data_path}/orders/batch2")
print(f"Batch 2: {df_orders_batch2.count()} records (25 valid + 1 dirty)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Bronze Ingestion (Append)

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
print(f"Bronze: {total_bronze} total records (appended {df_new_bronze.count()} from batch2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Silver (MERGE)

# COMMAND ----------

df_new_silver = (
    df_new_bronze
    .dropDuplicates(["order_id"])
    .filter(col("quantity") > 0)
    .filter(col("customer_id").isNotNull())
    .join(df_cust, "customer_id", "inner")
    .join(df_prod, "product_id", "inner")
    .withColumn("order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    .withColumn("total_amount", _round(col("quantity") * col("price"), 2))
    .select(
        "order_id", "order_date", "customer_id", "first_name", "last_name",
        "email", "city", "tier", "product_id", "product_name", "category",
        "quantity", "price", "total_amount", "payment_method",
    )
)

print(f"New Silver records (after filtering): {df_new_silver.count()} out of {df_new_bronze.count()}")

silver_delta = DeltaTable.forPath(spark, f"{silver_path}/orders")
(silver_delta.alias("target")
    .merge(df_new_silver.alias("source"), "target.order_id = source.order_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

silver_total = spark.read.format("delta").load(f"{silver_path}/orders").count()
print(f"Silver total after MERGE: {silver_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Incremental Gold Refresh

# COMMAND ----------

df_silver_all = spark.read.format("delta").load(f"{silver_path}/orders")

# Refresh all Gold tables
(df_silver_all
    .withColumn("order_day", date_trunc("day", col("order_date")))
    .groupBy("order_day", "city")
    .agg(
        _sum("total_amount").alias("total_revenue"),
        _count("order_id").alias("total_orders"),
        _sum("quantity").alias("total_items_sold"),
        _avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .write.format("delta").mode("overwrite").save(f"{gold_path}/daily_revenue")
)

(df_silver_all
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
    .write.format("delta").mode("overwrite").save(f"{gold_path}/customer_summary")
)

(df_silver_all
    .groupBy("product_id", "product_name", "category", "price")
    .agg(
        _count("order_id").alias("times_ordered"),
        _sum("quantity").alias("total_units_sold"),
        _sum("total_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_buyers"),
    )
    .withColumn("avg_units_per_order", _round(col("total_units_sold") / col("times_ordered"), 2))
    .write.format("delta").mode("overwrite").save(f"{gold_path}/product_performance")
)

print("All Gold tables refreshed")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 5: Query Gold Tables

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT first_name, last_name, city, tier,
# MAGIC        total_orders, lifetime_spend, avg_order_value,
# MAGIC        unique_products_bought, first_order_date, last_order_date
# MAGIC FROM gold.customer_summary
# MAGIC ORDER BY lifetime_spend DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_day, city, total_revenue, total_orders, total_items_sold,
# MAGIC        unique_customers, ROUND(avg_order_value, 2) as avg_order_value
# MAGIC FROM gold.daily_revenue
# MAGIC ORDER BY order_day, total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT product_name, times_ordered, total_units_sold,
# MAGIC        ROUND(total_revenue, 2) as total_revenue, unique_buyers, avg_units_per_order
# MAGIC FROM gold.product_performance
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 6: Delta Lake Features Across Layers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Bronze history: initial write + incremental append
# MAGIC DESCRIBE HISTORY bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: Bronze before batch 2
# MAGIC SELECT COUNT(*) as records, 'Before Batch 2' as label
# MAGIC FROM bronze.orders VERSION AS OF 0
# MAGIC UNION ALL
# MAGIC SELECT COUNT(*), 'After Batch 2' FROM bronze.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Silver history: OVERWRITE + MERGE
# MAGIC DESCRIBE HISTORY silver.orders

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE silver.orders ZORDER BY (customer_id, order_date)

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE gold.daily_revenue ZORDER BY (order_day, city)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Architecture Summary
# MAGIC
# MAGIC | Schema | Table | Purpose | Write Mode |
# MAGIC |--------|-------|---------|------------|
# MAGIC | `bronze` | `orders` | Raw audit trail (includes dirty data) | APPEND |
# MAGIC | `silver` | `customers` | Customer master reference | OVERWRITE |
# MAGIC | `silver` | `products` | Product catalog reference | OVERWRITE |
# MAGIC | `silver` | `orders` | Cleansed, enriched orders | MERGE (upsert) |
# MAGIC | `gold` | `daily_revenue` | Regional dashboards | OVERWRITE |
# MAGIC | `gold` | `customer_summary` | Customer LTV analytics | OVERWRITE |
# MAGIC | `gold` | `product_performance` | Merchandising insights | OVERWRITE |
# MAGIC
# MAGIC ### Data Quality Flow
# MAGIC
# MAGIC ```
# MAGIC Bronze (55 records)  ->  Silver (50 records)  ->  Gold (aggregated)
# MAGIC   includes:                filters out:
# MAGIC   - null customer_ids      - 2 null customer_id
# MAGIC   - zero/negative qty      - 1 qty=0, 1 qty=-1
# MAGIC   - duplicate order_ids    - 1 duplicate
# MAGIC ```
# MAGIC
# MAGIC ### Production Best Practices
# MAGIC
# MAGIC 1. **Separate schemas** per layer: `bronze`, `silver`, `gold`
# MAGIC 2. **Dirty data preserved** in Bronze for audit trail
# MAGIC 3. **CHECK constraints** on Silver: `quantity > 0`, `total_amount > 0`
# MAGIC 4. **MERGE** for idempotent Silver incremental updates
# MAGIC 5. **OPTIMIZE + ZORDER** for query performance
# MAGIC 6. **Time travel** for auditing and rollback

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Drop all tables across all schemas
for schema, tables in [
    ("bronze", ["orders"]),
    ("silver", ["orders", "customers", "products"]),
    ("gold", ["daily_revenue", "customer_summary", "product_performance"]),
]:
    for table in tables:
        spark.sql(f"DROP TABLE IF EXISTS {schema}.{table}")
        print(f"Dropped: {schema}.{table}")

# COMMAND ----------

dbutils.fs.rm(base_path, recurse=True)
print(f"Removed: {base_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS bronze CASCADE;
# MAGIC DROP SCHEMA IF EXISTS silver CASCADE;
# MAGIC DROP SCHEMA IF EXISTS gold CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC - **Day 19**: [Structured Streaming](../day19-structured-streaming/) -- the streaming engine
# MAGIC - **Day 20**: [Auto Loader](../day20-auto-loader/) -- optimized file ingestion for Bronze
# MAGIC - Add data quality checks with Delta Live Tables (DLT) expectations
# MAGIC - Implement row-level security using Unity Catalog
