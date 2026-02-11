# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Project 01: E-Commerce Data Pipeline (Bronze-Silver-Gold)
# MAGIC > Module 10 -- Capstone Project | Level: Intermediate | Time: 3-4 hours
# MAGIC
# MAGIC ## What You Will Build
# MAGIC
# MAGIC A complete medallion architecture pipeline for an e-commerce company:
# MAGIC - **Bronze**: Raw ingestion of customers, products, orders, and clickstream
# MAGIC - **Silver**: Cleaned, deduplicated, enriched data with quality checks
# MAGIC - **Gold**: Business analytics -- revenue, CLV, product performance, RFM segmentation
# MAGIC
# MAGIC The notebook is fully self-contained: it generates sample data, builds
# MAGIC the pipeline, and cleans up at the end.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, DateType, TimestampType
)
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

# Configuration
DATABASE = "module10_ecommerce"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# Drop any existing tables from prior runs
existing_tables = [row.tableName for row in spark.sql("SHOW TABLES").collect()]
for t in existing_tables:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")

print(f"Database '{DATABASE}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Data Generation
# MAGIC
# MAGIC We generate four related datasets inline. Data intentionally contains
# MAGIC quality issues (duplicates, nulls, messy formats) to make the Silver
# MAGIC layer meaningful.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Generate Customers (500)

# COMMAND ----------

random.seed(42)

first_names = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Peter",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach", "Bella", "Carlos", "Dara", "Ethan"
]
last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Allen",
    "Young", "King", "Wright", "Lopez", "Hill"
]
cities = [
    ("New York", "NY"), ("Los Angeles", "CA"), ("Chicago", "IL"),
    ("Houston", "TX"), ("Phoenix", "AZ"), ("Seattle", "WA"),
    ("Denver", "CO"), ("Boston", "MA"), ("Atlanta", "GA"),
    ("Portland", "OR"), ("Austin", "TX"), ("Miami", "FL"),
    ("Dallas", "TX"), ("San Francisco", "CA"), ("Nashville", "TN")
]
tiers = ["Bronze", "Silver", "Gold", "Platinum"]
tier_weights = [0.4, 0.3, 0.2, 0.1]

customers_raw = []
for i in range(1, 501):
    first = random.choice(first_names)
    last = random.choice(last_names)
    city, state = random.choice(cities)
    signup = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
    customers_raw.append({
        "customer_id": f"C-{i:05d}",
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "city": city,
        "state": state,
        "country": "US",
        "signup_date": signup.strftime("%Y-%m-%d"),
        "tier": random.choices(tiers, weights=tier_weights, k=1)[0],
    })

customers_df = spark.createDataFrame(customers_raw)
print(f"Customers generated: {customers_df.count()}")
customers_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Generate Products (200)

# COMMAND ----------

random.seed(42)

catalog = {
    "Electronics": {
        "Smartphones": (199.99, 999.99, 0.15, 0.25),
        "Laptops": (499.99, 2499.99, 1.2, 3.0),
        "Headphones": (29.99, 349.99, 0.1, 0.4),
        "Tablets": (149.99, 1099.99, 0.3, 0.8),
        "Cameras": (199.99, 1499.99, 0.5, 1.5),
    },
    "Clothing": {
        "T-Shirts": (9.99, 49.99, 0.15, 0.3),
        "Jeans": (29.99, 129.99, 0.5, 1.0),
        "Jackets": (49.99, 299.99, 0.6, 1.5),
        "Shoes": (39.99, 199.99, 0.5, 1.2),
    },
    "Home & Kitchen": {
        "Cookware": (19.99, 199.99, 0.5, 3.0),
        "Furniture": (99.99, 999.99, 5.0, 30.0),
        "Lighting": (14.99, 149.99, 0.3, 2.0),
        "Appliances": (49.99, 499.99, 2.0, 10.0),
    },
    "Books": {
        "Fiction": (7.99, 24.99, 0.2, 0.5),
        "Non-Fiction": (9.99, 39.99, 0.2, 0.6),
        "Technical": (19.99, 79.99, 0.3, 0.8),
    },
}

products_raw = []
categories = list(catalog.keys())
for i in range(1, 201):
    cat = random.choice(categories)
    subcat = random.choice(list(catalog[cat].keys()))
    price_min, price_max, weight_min, weight_max = catalog[cat][subcat]
    products_raw.append({
        "product_id": f"P-{i:05d}",
        "name": f"{subcat.rstrip('s')} Model-{random.randint(100, 999)}",
        "category": cat,
        "subcategory": subcat,
        "price": round(random.uniform(price_min, price_max), 2),
        "weight_kg": round(random.uniform(weight_min, weight_max), 2),
    })

products_df = spark.createDataFrame(products_raw)
print(f"Products generated: {products_df.count()}")
products_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Generate Orders (5000) -- With Intentional Quality Issues

# COMMAND ----------

random.seed(42)

statuses = ["completed", "shipped", "processing", "cancelled", "returned"]
status_weights = [0.5, 0.2, 0.15, 0.1, 0.05]
payment_methods = ["credit_card", "debit_card", "paypal", "bank_transfer"]

customer_ids = [c["customer_id"] for c in customers_raw]
product_lookup = {p["product_id"]: p for p in products_raw}
product_ids = list(product_lookup.keys())

orders_raw = []
for i in range(1, 5001):
    # Intentional: use smaller ID range to create duplicates
    order_id = f"O-{random.randint(1, 4500):06d}"
    pid = random.choice(product_ids)
    qty = random.randint(1, 5)
    price = product_lookup[pid]["price"]

    # Messy price: sometimes with $, sometimes as string
    price_str = f"${round(price * qty, 2)}" if random.random() > 0.08 else str(round(price * qty, 2))
    if random.random() < 0.03:
        price_str = ""  # 3% missing

    order_date = datetime(2023, 1, 1) + timedelta(days=random.randint(0, 730))

    # 2% null customer_id
    cust_id = random.choice(customer_ids) if random.random() > 0.02 else None

    orders_raw.append((
        order_id, cust_id, pid, str(qty), price_str,
        order_date.strftime("%Y-%m-%d"),
        random.choices(statuses, weights=status_weights, k=1)[0],
        random.choice(payment_methods)
    ))

orders_schema = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("quantity", StringType()),       # String on purpose
    StructField("total_amount", StringType()),   # String with $ signs
    StructField("order_date", StringType()),
    StructField("status", StringType()),
    StructField("payment_method", StringType()),
])

orders_df = spark.createDataFrame(orders_raw, schema=orders_schema)
print(f"Orders generated: {orders_df.count()}")
print("Notice messy data (string types, $ in amounts, nulls):")
orders_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 Generate Clickstream (10000)

# COMMAND ----------

random.seed(42)

event_types = ["page_view", "click", "add_to_cart", "purchase", "search"]
event_weights = [0.4, 0.25, 0.15, 0.1, 0.1]
pages = ["home", "category", "product", "cart", "checkout", "search_results"]
devices = ["desktop", "mobile", "tablet"]
device_weights = [0.45, 0.40, 0.15]

clickstream_raw = []
for i in range(1, 10001):
    ts = datetime(2024, 1, 1) + timedelta(
        days=random.randint(0, 365),
        seconds=random.randint(0, 86399)
    )
    clickstream_raw.append({
        "event_id": f"E-{i:07d}",
        "customer_id": random.choice(customer_ids),
        "product_id": random.choice(product_ids) if random.random() > 0.3 else None,
        "event_type": random.choices(event_types, weights=event_weights, k=1)[0],
        "page": random.choice(pages),
        "timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "session_id": f"S-{random.randint(1, 2000):06d}",
        "device": random.choices(devices, weights=device_weights, k=1)[0],
    })

clickstream_df = spark.createDataFrame(clickstream_raw)
print(f"Clickstream events generated: {clickstream_df.count()}")
clickstream_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: BRONZE LAYER -- Raw Ingestion
# MAGIC
# MAGIC **Rules for Bronze**:
# MAGIC - Store data exactly as received -- no transformations
# MAGIC - Add metadata columns: `_ingest_timestamp`, `_source`, `_batch_id`
# MAGIC - Append-only mode (we use overwrite here for notebook idempotency)

# COMMAND ----------

def write_bronze(df, table_name, source_name):
    """Write a DataFrame to the Bronze layer with metadata columns."""
    bronze = (df
        .withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source", F.lit(source_name))
        .withColumn("_batch_id", F.lit("batch_2025_01"))
    )
    bronze.write.format("delta").mode("overwrite").saveAsTable(table_name)
    count = spark.table(table_name).count()
    print(f"  {table_name}: {count} rows written")
    return count

print("=== BRONZE LAYER INGESTION ===")
bronze_counts = {}
bronze_counts["customers"] = write_bronze(customers_df, "bronze_customers", "crm_system")
bronze_counts["products"] = write_bronze(products_df, "bronze_products", "product_catalog")
bronze_counts["orders"] = write_bronze(orders_df, "bronze_orders", "order_service")
bronze_counts["clickstream"] = write_bronze(clickstream_df, "bronze_clickstream", "web_tracker")
print(f"\nTotal Bronze rows: {sum(bronze_counts.values())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify Bronze Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze: raw data with metadata columns
# MAGIC SELECT order_id, customer_id, total_amount, order_date, _ingest_timestamp, _source
# MAGIC FROM bronze_orders
# MAGIC LIMIT 10

# COMMAND ----------

# Verify Bronze schemas
for tbl in ["bronze_customers", "bronze_products", "bronze_orders", "bronze_clickstream"]:
    print(f"\n--- {tbl} ---")
    spark.table(tbl).printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: SILVER LAYER -- Clean, Deduplicate, Enrich
# MAGIC
# MAGIC **Transformations**:
# MAGIC 1. Type casting (strings to proper types)
# MAGIC 2. Data cleansing (remove $, standardize formats)
# MAGIC 3. Deduplication (keep latest per primary key)
# MAGIC 4. Null handling (quarantine invalid records)
# MAGIC 5. Enrichment (join orders with customers and products)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Silver Customers

# COMMAND ----------

bronze_customers = spark.table("bronze_customers")

silver_customers = (bronze_customers
    .withColumn("signup_date", F.to_date(F.col("signup_date"), "yyyy-MM-dd"))
    .withColumn("email_valid", F.col("email").rlike(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"))
    .filter("customer_id IS NOT NULL AND name IS NOT NULL")
    .dropDuplicates(["customer_id"])
    .select(
        "customer_id", "name", "email", "city", "state",
        "country", "signup_date", "tier", "email_valid"
    )
)

silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
print(f"Silver customers: {spark.table('silver_customers').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Silver Products

# COMMAND ----------

bronze_products = spark.table("bronze_products")

silver_products = (bronze_products
    .filter("product_id IS NOT NULL AND price IS NOT NULL AND price > 0")
    .dropDuplicates(["product_id"])
    .select(
        "product_id", "name", "category", "subcategory",
        F.col("price").cast("double").alias("price"),
        F.col("weight_kg").cast("double").alias("weight_kg")
    )
)

silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
print(f"Silver products: {spark.table('silver_products').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Silver Orders -- Clean, Deduplicate, Quarantine

# COMMAND ----------

bronze_orders = spark.table("bronze_orders")

# Step 1: Clean price (remove $, cast to double) and cast other columns
cleaned_orders = (bronze_orders
    .withColumn("amount_clean",
        F.regexp_replace(F.col("total_amount"), "\\$", "").cast("double"))
    .withColumn("quantity_clean", F.col("quantity").cast("int"))
    .withColumn("order_date_clean", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
)

# Step 2: Separate good and bad records
good_orders = cleaned_orders.filter(
    (F.col("order_id").isNotNull()) &
    (F.col("customer_id").isNotNull()) &
    (F.col("product_id").isNotNull()) &
    (F.col("amount_clean").isNotNull()) &
    (F.col("amount_clean") > 0) &
    (F.col("quantity_clean").isNotNull()) &
    (F.col("quantity_clean") > 0)
)

bad_orders = cleaned_orders.filter(
    (F.col("order_id").isNull()) |
    (F.col("customer_id").isNull()) |
    (F.col("product_id").isNull()) |
    (F.col("amount_clean").isNull()) |
    (F.col("amount_clean") <= 0) |
    (F.col("quantity_clean").isNull()) |
    (F.col("quantity_clean") <= 0)
)

print(f"Good orders: {good_orders.count()}")
print(f"Bad orders (quarantine): {bad_orders.count()}")

# COMMAND ----------

# Step 3: Deduplicate -- keep latest record per order_id
window_dedup = Window.partitionBy("order_id").orderBy(F.desc("_ingest_timestamp"))

silver_orders = (good_orders
    .withColumn("_row_num", F.row_number().over(window_dedup))
    .filter("_row_num = 1")
    .drop("_row_num")
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.col("quantity_clean").alias("quantity"),
        F.col("amount_clean").alias("total_amount"),
        F.col("order_date_clean").alias("order_date"),
        F.col("status"),
        F.col("payment_method"),
        F.col("_ingest_timestamp")
    )
)

silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# Write quarantine
bad_orders.write.format("delta").mode("overwrite").saveAsTable("silver_quarantine")

silver_order_count = spark.table("silver_orders").count()
quarantine_count = spark.table("silver_quarantine").count()
print(f"\nSilver orders: {silver_order_count} rows (deduplicated)")
print(f"Quarantine:    {quarantine_count} rows")
print(f"Dedup removed: {good_orders.count() - silver_order_count} duplicate rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Silver Clickstream

# COMMAND ----------

bronze_clicks = spark.table("bronze_clickstream")

silver_clickstream = (bronze_clicks
    .withColumn("event_timestamp", F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd HH:mm:ss"))
    .filter("event_id IS NOT NULL AND customer_id IS NOT NULL")
    .dropDuplicates(["event_id"])
    .select(
        "event_id", "customer_id", "product_id", "event_type",
        "page", "event_timestamp", "session_id", "device"
    )
)

silver_clickstream.write.format("delta").mode("overwrite").saveAsTable("silver_clickstream")
print(f"Silver clickstream: {spark.table('silver_clickstream').count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Silver Enriched Orders -- Three-Way Join

# COMMAND ----------

s_orders = spark.table("silver_orders")
s_customers = spark.table("silver_customers")
s_products = spark.table("silver_products")

enriched_orders = (s_orders
    .join(s_customers.select(
        "customer_id", "name", "city", "state", "tier", "signup_date"
    ), on="customer_id", how="left")
    .join(s_products.select(
        "product_id",
        F.col("name").alias("product_name"),
        "category",
        "subcategory",
        F.col("price").alias("unit_price")
    ), on="product_id", how="left")
    .withColumnRenamed("name", "customer_name")
)

enriched_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders_enriched")
print(f"Silver enriched orders: {spark.table('silver_orders_enriched').count()} rows")
spark.table("silver_orders_enriched").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.6 Silver Layer Data Quality Report

# COMMAND ----------

print("=" * 65)
print("SILVER LAYER DATA QUALITY REPORT")
print("=" * 65)

silver_tables = {
    "silver_customers": "customer_id",
    "silver_products": "product_id",
    "silver_orders": "order_id",
    "silver_clickstream": "event_id",
    "silver_orders_enriched": "order_id",
    "silver_quarantine": "order_id",
}

for table, pk in silver_tables.items():
    df = spark.table(table)
    total = df.count()
    nulls_in_pk = df.filter(F.col(pk).isNull()).count()
    dup_count = df.groupBy(pk).count().filter("count > 1").count()
    print(f"  {table:<30} rows={total:<6} pk_nulls={nulls_in_pk:<4} duplicates={dup_count}")

print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: GOLD LAYER -- Business Analytics
# MAGIC
# MAGIC Gold tables are pre-aggregated, business-oriented, and optimized for
# MAGIC querying by BI tools and analysts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Revenue by Category and Month

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_revenue_by_category_month AS
# MAGIC SELECT
# MAGIC   category,
# MAGIC   DATE_TRUNC('month', order_date) AS order_month,
# MAGIC   COUNT(DISTINCT order_id) AS total_orders,
# MAGIC   ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC   ROUND(AVG(total_amount), 2) AS avg_order_value,
# MAGIC   SUM(quantity) AS total_units,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers
# MAGIC FROM silver_orders_enriched
# MAGIC WHERE status IN ('completed', 'shipped')
# MAGIC   AND category IS NOT NULL
# MAGIC GROUP BY category, DATE_TRUNC('month', order_date)
# MAGIC ORDER BY order_month DESC, total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview: Revenue by category and month
# MAGIC SELECT * FROM gold_revenue_by_category_month LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Customer Lifetime Value (CLV)

# COMMAND ----------

enriched = spark.table("silver_orders_enriched")

# Calculate CLV metrics per customer
clv_df = (enriched
    .filter(F.col("status").isin("completed", "shipped"))
    .groupBy("customer_id", "customer_name", "city", "state", "tier")
    .agg(
        F.round(F.sum("total_amount"), 2).alias("total_spend"),
        F.count("order_id").alias("total_orders"),
        F.round(F.avg("total_amount"), 2).alias("avg_order_value"),
        F.min("order_date").alias("first_order_date"),
        F.max("order_date").alias("last_order_date"),
        F.countDistinct("category").alias("categories_purchased"),
    )
    .withColumn("customer_tenure_days",
        F.datediff(F.col("last_order_date"), F.col("first_order_date")))
    .withColumn("orders_per_month",
        F.when(F.col("customer_tenure_days") > 0,
               F.round(F.col("total_orders") / (F.col("customer_tenure_days") / 30.0), 2))
         .otherwise(F.lit(0.0)))
    .orderBy(F.desc("total_spend"))
)

clv_df.write.format("delta").mode("overwrite").saveAsTable("gold_customer_lifetime_value")
print(f"Gold CLV table: {spark.table('gold_customer_lifetime_value').count()} rows")
spark.table("gold_customer_lifetime_value").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Product Performance

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_product_performance AS
# MAGIC WITH product_stats AS (
# MAGIC   SELECT
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     category,
# MAGIC     subcategory,
# MAGIC     unit_price,
# MAGIC     COUNT(order_id) AS total_orders,
# MAGIC     SUM(CASE WHEN status IN ('completed', 'shipped') THEN 1 ELSE 0 END) AS successful_orders,
# MAGIC     SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
# MAGIC     SUM(CASE WHEN status = 'returned' THEN 1 ELSE 0 END) AS returned_orders,
# MAGIC     ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC     SUM(quantity) AS total_units_sold,
# MAGIC     COUNT(DISTINCT customer_id) AS unique_buyers
# MAGIC   FROM silver_orders_enriched
# MAGIC   WHERE product_id IS NOT NULL
# MAGIC   GROUP BY product_id, product_name, category, subcategory, unit_price
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   ROUND(cancelled_orders * 100.0 / NULLIF(total_orders, 0), 1) AS cancellation_rate_pct,
# MAGIC   ROUND(returned_orders * 100.0 / NULLIF(total_orders, 0), 1) AS return_rate_pct,
# MAGIC   RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS revenue_rank_in_category
# MAGIC FROM product_stats
# MAGIC ORDER BY total_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Top products by revenue
# MAGIC SELECT product_id, product_name, category, total_revenue,
# MAGIC        total_units_sold, unique_buyers, revenue_rank_in_category
# MAGIC FROM gold_product_performance
# MAGIC WHERE revenue_rank_in_category <= 3
# MAGIC ORDER BY category, revenue_rank_in_category

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Customer Segmentation (RFM Analysis)
# MAGIC
# MAGIC RFM scores each customer on three dimensions:
# MAGIC - **Recency**: Days since last purchase (lower is better)
# MAGIC - **Frequency**: Number of purchases (higher is better)
# MAGIC - **Monetary**: Total spend (higher is better)
# MAGIC
# MAGIC Each dimension is divided into quartiles (1-4) and combined into
# MAGIC a segment label.

# COMMAND ----------

# Step 1: Calculate raw RFM values
reference_date = "2025-01-01"

rfm_raw = (spark.table("silver_orders_enriched")
    .filter(F.col("status").isin("completed", "shipped"))
    .groupBy("customer_id", "customer_name", "tier")
    .agg(
        F.datediff(F.lit(reference_date), F.max("order_date")).alias("recency_days"),
        F.count("order_id").alias("frequency"),
        F.round(F.sum("total_amount"), 2).alias("monetary"),
    )
)

# Step 2: Assign quartile scores using ntile
# For recency: lower is better, so quartile 4 = most recent
# For frequency and monetary: higher is better, so quartile 4 = highest
recency_window = Window.orderBy(F.asc("recency_days"))     # ascending: lowest days = best
frequency_window = Window.orderBy(F.asc("frequency"))      # ascending: ntile gives 4 to highest
monetary_window = Window.orderBy(F.asc("monetary"))

rfm_scored = (rfm_raw
    .withColumn("r_score", F.ntile(4).over(recency_window))
    .withColumn("f_score", F.ntile(4).over(frequency_window))
    .withColumn("m_score", F.ntile(4).over(monetary_window))
    .withColumn("rfm_total", F.col("r_score") + F.col("f_score") + F.col("m_score"))
)

# Step 3: Assign segment labels based on combined score
rfm_segmented = (rfm_scored
    .withColumn("segment",
        F.when(F.col("rfm_total") >= 10, "Champions")
         .when(F.col("rfm_total") >= 7, "Loyal Customers")
         .when(F.col("rfm_total") >= 5, "At Risk")
         .otherwise("Lost"))
)

rfm_segmented.write.format("delta").mode("overwrite").saveAsTable("gold_customer_segments")
print(f"Gold RFM segments: {spark.table('gold_customer_segments').count()} rows")

# COMMAND ----------

# Segment distribution summary
segment_summary = (spark.table("gold_customer_segments")
    .groupBy("segment")
    .agg(
        F.count("*").alias("customer_count"),
        F.round(F.avg("recency_days"), 0).alias("avg_recency"),
        F.round(F.avg("frequency"), 1).alias("avg_frequency"),
        F.round(F.avg("monetary"), 2).alias("avg_monetary"),
    )
    .orderBy(F.desc("avg_monetary"))
)

print("=== RFM SEGMENT DISTRIBUTION ===")
segment_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Daily Executive Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_daily_dashboard AS
# MAGIC WITH daily_orders AS (
# MAGIC   SELECT
# MAGIC     order_date,
# MAGIC     COUNT(DISTINCT order_id) AS total_orders,
# MAGIC     ROUND(SUM(total_amount), 2) AS total_revenue,
# MAGIC     ROUND(AVG(total_amount), 2) AS avg_order_value,
# MAGIC     COUNT(DISTINCT customer_id) AS active_customers,
# MAGIC     SUM(quantity) AS units_sold
# MAGIC   FROM silver_orders_enriched
# MAGIC   WHERE status IN ('completed', 'shipped')
# MAGIC   GROUP BY order_date
# MAGIC ),
# MAGIC daily_top_category AS (
# MAGIC   SELECT
# MAGIC     order_date,
# MAGIC     category AS top_category,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY SUM(total_amount) DESC) AS rn
# MAGIC   FROM silver_orders_enriched
# MAGIC   WHERE status IN ('completed', 'shipped') AND category IS NOT NULL
# MAGIC   GROUP BY order_date, category
# MAGIC ),
# MAGIC daily_top_product AS (
# MAGIC   SELECT
# MAGIC     order_date,
# MAGIC     product_name AS top_product,
# MAGIC     ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY SUM(total_amount) DESC) AS rn
# MAGIC   FROM silver_orders_enriched
# MAGIC   WHERE status IN ('completed', 'shipped') AND product_name IS NOT NULL
# MAGIC   GROUP BY order_date, product_name
# MAGIC )
# MAGIC SELECT
# MAGIC   d.order_date,
# MAGIC   d.total_orders,
# MAGIC   d.total_revenue,
# MAGIC   d.avg_order_value,
# MAGIC   d.active_customers,
# MAGIC   d.units_sold,
# MAGIC   c.top_category,
# MAGIC   p.top_product,
# MAGIC   -- Running totals
# MAGIC   SUM(d.total_revenue) OVER (ORDER BY d.order_date ROWS UNBOUNDED PRECEDING) AS cumulative_revenue,
# MAGIC   -- 7-day moving average
# MAGIC   ROUND(AVG(d.total_revenue) OVER (ORDER BY d.order_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 2) AS revenue_7day_avg
# MAGIC FROM daily_orders d
# MAGIC LEFT JOIN daily_top_category c ON d.order_date = c.order_date AND c.rn = 1
# MAGIC LEFT JOIN daily_top_product p ON d.order_date = p.order_date AND p.rn = 1
# MAGIC ORDER BY d.order_date DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the executive dashboard
# MAGIC SELECT * FROM gold_daily_dashboard LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Table Optimization
# MAGIC
# MAGIC Apply Delta Lake optimization to Silver and Gold tables for query performance.

# COMMAND ----------

# OPTIMIZE with ZORDER on frequently queried columns
print("=== OPTIMIZING TABLES ===")

optimize_targets = [
    ("silver_orders", "order_date, customer_id"),
    ("silver_orders_enriched", "order_date, category"),
    ("gold_revenue_by_category_month", "order_month"),
    ("gold_customer_lifetime_value", "total_spend"),
    ("gold_product_performance", "category"),
    ("gold_customer_segments", "segment"),
    ("gold_daily_dashboard", "order_date"),
]

for table, zorder_cols in optimize_targets:
    try:
        spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_cols})")
        print(f"  Optimized: {table} (ZORDER BY {zorder_cols})")
    except Exception as e:
        print(f"  Skipped {table}: {e}")

# COMMAND ----------

# Demonstrate VACUUM (dry run with 0 hours retention for demonstration)
# In production, use the default 168 hours (7 days) minimum
print("=== VACUUM DEMONSTRATION ===")
print("In production, run: VACUUM silver_orders RETAIN 168 HOURS")
print("For demonstration, we show the table history instead:\n")

spark.sql("DESCRIBE HISTORY silver_orders").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Cross-Layer Data Quality Validation

# COMMAND ----------

print("=" * 70)
print("MEDALLION PIPELINE -- CROSS-LAYER DATA QUALITY REPORT")
print("=" * 70)

# Bronze counts
b_customers = spark.table("bronze_customers").count()
b_products = spark.table("bronze_products").count()
b_orders = spark.table("bronze_orders").count()
b_clicks = spark.table("bronze_clickstream").count()

# Silver counts
s_customers = spark.table("silver_customers").count()
s_products = spark.table("silver_products").count()
s_orders = spark.table("silver_orders").count()
s_clicks = spark.table("silver_clickstream").count()
s_enriched = spark.table("silver_orders_enriched").count()
s_quarantine = spark.table("silver_quarantine").count()

# Gold counts
g_revenue = spark.table("gold_revenue_by_category_month").count()
g_clv = spark.table("gold_customer_lifetime_value").count()
g_product = spark.table("gold_product_performance").count()
g_rfm = spark.table("gold_customer_segments").count()
g_dashboard = spark.table("gold_daily_dashboard").count()

print(f"\n{'Layer':<12} {'Table':<35} {'Rows':>8}")
print("-" * 60)
print(f"{'BRONZE':<12} {'bronze_customers':<35} {b_customers:>8}")
print(f"{'BRONZE':<12} {'bronze_products':<35} {b_products:>8}")
print(f"{'BRONZE':<12} {'bronze_orders':<35} {b_orders:>8}")
print(f"{'BRONZE':<12} {'bronze_clickstream':<35} {b_clicks:>8}")
print("-" * 60)
print(f"{'SILVER':<12} {'silver_customers':<35} {s_customers:>8}")
print(f"{'SILVER':<12} {'silver_products':<35} {s_products:>8}")
print(f"{'SILVER':<12} {'silver_orders':<35} {s_orders:>8}")
print(f"{'SILVER':<12} {'silver_clickstream':<35} {s_clicks:>8}")
print(f"{'SILVER':<12} {'silver_orders_enriched':<35} {s_enriched:>8}")
print(f"{'SILVER':<12} {'silver_quarantine':<35} {s_quarantine:>8}")
print("-" * 60)
print(f"{'GOLD':<12} {'gold_revenue_by_category_month':<35} {g_revenue:>8}")
print(f"{'GOLD':<12} {'gold_customer_lifetime_value':<35} {g_clv:>8}")
print(f"{'GOLD':<12} {'gold_product_performance':<35} {g_product:>8}")
print(f"{'GOLD':<12} {'gold_customer_segments':<35} {g_rfm:>8}")
print(f"{'GOLD':<12} {'gold_daily_dashboard':<35} {g_dashboard:>8}")
print("=" * 70)

# Completeness check
orders_accounted = s_orders + s_quarantine
print(f"\nOrders accountability: Silver({s_orders}) + Quarantine({s_quarantine}) = {orders_accounted}")
print(f"  Bronze orders: {b_orders}")
dedup_removed = b_orders - orders_accounted
print(f"  Duplicates removed: {dedup_removed}")
print(f"  Quarantine rate: {s_quarantine / b_orders * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Business Insights Summary
# MAGIC
# MAGIC These summary DataFrames are what a BI tool (Tableau, Power BI, Databricks
# MAGIC SQL Dashboard) would consume.

# COMMAND ----------

# Insight 1: Revenue trend by month
print("=== MONTHLY REVENUE TREND ===")
spark.sql("""
    SELECT order_month,
           SUM(total_revenue) AS monthly_revenue,
           SUM(total_orders) AS monthly_orders
    FROM gold_revenue_by_category_month
    GROUP BY order_month
    ORDER BY order_month
""").show(30, truncate=False)

# COMMAND ----------

# Insight 2: Top 10 customers by CLV
print("=== TOP 10 CUSTOMERS BY LIFETIME VALUE ===")
spark.sql("""
    SELECT customer_id, customer_name, tier, total_spend,
           total_orders, avg_order_value, customer_tenure_days
    FROM gold_customer_lifetime_value
    ORDER BY total_spend DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Insight 3: Category performance comparison
print("=== CATEGORY PERFORMANCE ===")
spark.sql("""
    SELECT category,
           COUNT(*) AS products,
           SUM(total_orders) AS total_orders,
           ROUND(SUM(total_revenue), 2) AS total_revenue,
           ROUND(AVG(cancellation_rate_pct), 1) AS avg_cancel_rate,
           ROUND(AVG(return_rate_pct), 1) AS avg_return_rate
    FROM gold_product_performance
    GROUP BY category
    ORDER BY total_revenue DESC
""").show(truncate=False)

# COMMAND ----------

# Insight 4: RFM segment health
print("=== CUSTOMER SEGMENT HEALTH ===")
spark.sql("""
    SELECT segment, customer_count,
           ROUND(avg_recency, 0) AS avg_recency_days,
           ROUND(avg_frequency, 1) AS avg_frequency,
           ROUND(avg_monetary, 2) AS avg_monetary,
           ROUND(customer_count * 100.0 / SUM(customer_count) OVER (), 1) AS pct_of_total
    FROM (
        SELECT segment,
               COUNT(*) AS customer_count,
               AVG(recency_days) AS avg_recency,
               AVG(frequency) AS avg_frequency,
               AVG(monetary) AS avg_monetary
        FROM gold_customer_segments
        GROUP BY segment
    )
    ORDER BY avg_monetary DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Cleanup
# MAGIC
# MAGIC Drop all tables and the database to leave the workspace clean.

# COMMAND ----------

# Drop all tables created in this notebook
tables_to_drop = [row.tableName for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()]
for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")
    print(f"  Dropped: {DATABASE}.{t}")

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE}")
print(f"\nDatabase '{DATABASE}' dropped. Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC In this project you built a complete E-Commerce Data Pipeline:
# MAGIC
# MAGIC | Layer | Tables | Purpose |
# MAGIC |-------|--------|---------|
# MAGIC | **Bronze** | 4 tables | Raw ingestion with metadata |
# MAGIC | **Silver** | 6 tables | Cleaned, deduplicated, enriched, quarantined |
# MAGIC | **Gold** | 5 tables | Revenue analytics, CLV, product performance, RFM segments, dashboard |
# MAGIC
# MAGIC **Key techniques practiced**:
# MAGIC - Medallion architecture design and implementation
# MAGIC - Data quality checks across pipeline layers
# MAGIC - Window functions for deduplication and RFM quartile scoring
# MAGIC - Three-way joins for data enrichment
# MAGIC - SQL and DataFrame API for Gold aggregations
# MAGIC - Delta Lake OPTIMIZE and ZORDER for query performance
# MAGIC - Running totals and moving averages for dashboard metrics
