# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Professional Data Engineer Exam -- Advanced Hands-On Exercises
# MAGIC
# MAGIC This notebook provides advanced exercises covering the six Professional exam domains.
# MAGIC These exercises go beyond basic syntax and focus on design decisions, performance
# MAGIC optimization, and production patterns.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 13.x+ recommended. Photon-enabled cluster preferred.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Generate Sample Data

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, BooleanType, LongType, ArrayType, MapType
)
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, upper, lower, trim,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lag, lead, ntile,
    current_timestamp, current_date, date_format, datediff, to_date,
    date_add, date_sub, months_between, last_day,
    explode, split, collect_list, collect_set, array, struct, map_keys,
    round as spark_round, expr, broadcast, monotonically_increasing_id,
    from_json, to_json, schema_of_json,
    window as time_window, approx_count_distinct,
    transform, filter as array_filter, aggregate as array_aggregate,
    sha2, md5
)
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta
import time

# --- Large-ish sales fact table ---
sales_records = []
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Tablet", "Phone", "Cable"]
categories = {"Laptop": "Electronics", "Mouse": "Electronics", "Keyboard": "Electronics",
              "Monitor": "Electronics", "Headphones": "Audio", "Tablet": "Electronics",
              "Phone": "Electronics", "Cable": "Accessories"}
prices = {"Laptop": 1200, "Mouse": 25, "Keyboard": 75, "Monitor": 350,
          "Headphones": 150, "Tablet": 500, "Phone": 900, "Cable": 10}
regions = ["US-East", "US-West", "EU-North", "EU-South", "APAC"]

for i in range(1, 201):
    product = products[(i - 1) % len(products)]
    region = regions[(i - 1) % len(regions)]
    day_offset = (i - 1) % 30
    sales_records.append((
        i,
        f"2024-01-{(day_offset % 28) + 1:02d}",
        categories[product],
        product,
        float(prices[product]) + (i % 50),
        (i % 10) + 1,
        region,
        100 + (i % 50),
    ))

sales_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("txn_date", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("region", StringType(), False),
    StructField("customer_id", IntegerType(), False),
])

df_sales = spark.createDataFrame(sales_records, schema=sales_schema)
df_sales = df_sales.withColumn("txn_date", to_date(col("txn_date")))
df_sales.createOrReplaceTempView("pro_sales")

# --- Customer dimension (for SCD exercises) ---
customer_dim_data = [
    (100, "Alice Johnson", "alice@example.com", "US-East", "Gold", date(2023, 1, 1), None, True),
    (101, "Bob Smith", "bob@example.com", "US-West", "Silver", date(2023, 3, 15), None, True),
    (102, "Charlie Brown", "charlie@example.com", "EU-North", "Gold", date(2023, 6, 20), None, True),
    (103, "Diana Prince", "diana@example.com", "EU-South", "Bronze", date(2023, 9, 1), None, True),
    (104, "Eve Davis", "eve@example.com", "APAC", "Silver", date(2024, 1, 5), None, True),
]
customer_schema = StructType([
    StructField("customer_id", IntegerType(), False),
    StructField("name", StringType(), False),
    StructField("email", StringType(), True),
    StructField("region", StringType(), False),
    StructField("tier", StringType(), False),
    StructField("effective_date", DateType(), False),
    StructField("end_date", DateType(), True),
    StructField("is_current", BooleanType(), False),
])
df_customers = spark.createDataFrame(customer_dim_data, schema=customer_schema)
df_customers.write.format("delta").mode("overwrite").saveAsTable("pro_customer_dim")

# --- Nested/complex data ---
complex_data = [
    (1, "order_001", [{"item": "Laptop", "qty": 1, "price": 1200}, {"item": "Mouse", "qty": 2, "price": 25}], {"source": "web", "campaign": "summer_sale"}),
    (2, "order_002", [{"item": "Phone", "qty": 1, "price": 900}], {"source": "mobile", "campaign": "flash_deal"}),
    (3, "order_003", [{"item": "Keyboard", "qty": 3, "price": 75}, {"item": "Monitor", "qty": 1, "price": 350}, {"item": "Cable", "qty": 5, "price": 10}], {"source": "web", "campaign": "clearance"}),
]

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_ref", StringType(), False),
    StructField("items", ArrayType(StructType([
        StructField("item", StringType(), False),
        StructField("qty", IntegerType(), False),
        StructField("price", IntegerType(), False),
    ])), False),
    StructField("metadata", MapType(StringType(), StringType()), False),
])

df_orders = spark.createDataFrame(complex_data, schema=order_schema)
df_orders.createOrReplaceTempView("pro_orders")

print("Setup complete:")
print(f"  - pro_sales (temp view, {df_sales.count()} rows)")
print(f"  - pro_customer_dim (Delta table, {df_customers.count()} rows)")
print(f"  - pro_orders (temp view, {df_orders.count()} rows with nested data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 1: Databricks Tooling (20%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1: Widget Parameters
# MAGIC
# MAGIC Widgets parameterize notebooks. In production jobs, parameter values are
# MAGIC passed via the Jobs API or task configuration.

# COMMAND ----------

# Create widgets for parameterization
dbutils.widgets.text("target_region", "US-East", "Target Region")
dbutils.widgets.dropdown("output_format", "delta", ["delta", "parquet", "csv"], "Output Format")

# Read widget values
target_region = dbutils.widgets.get("target_region")
output_format = dbutils.widgets.get("output_format")

print(f"Target region: {target_region}")
print(f"Output format: {output_format}")

# Use in query
filtered = df_sales.filter(col("region") == target_region)
print(f"Rows for {target_region}: {filtered.count()}")
filtered.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.2: REST API Patterns (Reference)
# MAGIC
# MAGIC The exam tests your understanding of common API patterns.
# MAGIC
# MAGIC ```python
# MAGIC import requests
# MAGIC
# MAGIC # Common headers for Databricks REST API
# MAGIC headers = {
# MAGIC     "Authorization": f"Bearer {token}",
# MAGIC     "Content-Type": "application/json"
# MAGIC }
# MAGIC
# MAGIC # List jobs (paginated)
# MAGIC response = requests.get(
# MAGIC     f"{host}/api/2.1/jobs/list",
# MAGIC     headers=headers,
# MAGIC     params={"limit": 25, "offset": 0}
# MAGIC )
# MAGIC jobs = response.json().get("jobs", [])
# MAGIC
# MAGIC # Trigger a job run
# MAGIC response = requests.post(
# MAGIC     f"{host}/api/2.1/jobs/run-now",
# MAGIC     headers=headers,
# MAGIC     json={"job_id": 12345, "notebook_params": {"date": "2024-01-01"}}
# MAGIC )
# MAGIC run_id = response.json()["run_id"]
# MAGIC
# MAGIC # Check run status
# MAGIC response = requests.get(
# MAGIC     f"{host}/api/2.1/jobs/runs/get",
# MAGIC     headers=headers,
# MAGIC     params={"run_id": run_id}
# MAGIC )
# MAGIC state = response.json()["state"]["life_cycle_state"]
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 2: Data Processing (30%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1: Broadcast Join
# MAGIC
# MAGIC When joining a large table with a small lookup table, broadcasting the small
# MAGIC table avoids a shuffle of the large table.

# COMMAND ----------

# Create a small lookup table (regions)
region_lookup = spark.createDataFrame([
    ("US-East", "Americas", "USD"),
    ("US-West", "Americas", "USD"),
    ("EU-North", "Europe", "EUR"),
    ("EU-South", "Europe", "EUR"),
    ("APAC", "Asia-Pacific", "SGD"),
], ["region", "continent", "currency"])

# Without broadcast hint (Spark may auto-broadcast if small enough)
result_no_hint = df_sales.join(region_lookup, on="region", how="inner")

# With explicit broadcast hint (forces broadcast regardless of size)
result_with_hint = df_sales.join(broadcast(region_lookup), on="region", how="inner")

# Compare execution plans
print("=== Without broadcast hint ===")
result_no_hint.explain()

print("\n=== With explicit broadcast hint ===")
result_with_hint.explain()

result_with_hint.select("txn_id", "product", "region", "continent", "currency").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2: Handling Data Skew
# MAGIC
# MAGIC Data skew occurs when some partitions have significantly more data than others.
# MAGIC This causes some tasks to take much longer, slowing the entire job.

# COMMAND ----------

# Simulate skewed data: 80% of orders from US-East
skewed_data = [(i, "US-East") for i in range(1, 161)]  # 160 rows
skewed_data += [(i, "US-West") for i in range(161, 181)]  # 20 rows
skewed_data += [(i, "EU-North") for i in range(181, 196)]  # 15 rows
skewed_data += [(i, "APAC") for i in range(196, 201)]  # 5 rows

df_skewed = spark.createDataFrame(skewed_data, ["order_id", "region"])

# Show the skew
print("=== Data Distribution (Skewed) ===")
df_skewed.groupBy("region").count().orderBy(col("count").desc()).show()

# Approach 1: Salt the key to spread skewed partition across multiple partitions
from pyspark.sql.functions import floor, rand, concat as spark_concat

num_salts = 5
df_salted = df_skewed.withColumn("salt", (rand() * num_salts).cast("int"))
df_salted = df_salted.withColumn(
    "salted_key", concat_ws("_", col("region"), col("salt").cast("string"))
)

print("=== After salting (sample) ===")
df_salted.groupBy("salted_key").count().orderBy(col("count").desc()).show(10)

# Note: With AQE enabled (default in DBR 13+), skew handling is often automatic.
# Check if AQE is enabled:
print(f"AQE enabled: {spark.conf.get('spark.sql.adaptive.enabled')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3: Advanced Window Functions with Frames
# MAGIC
# MAGIC Window frames control which rows are included in the window calculation.

# COMMAND ----------

# Running total and moving average
window_running = (
    Window
    .partitionBy("category")
    .orderBy("txn_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

window_moving_3day = (
    Window
    .partitionBy("category")
    .orderBy("txn_date")
    .rowsBetween(-2, 0)  # Current row and 2 preceding
)

# Use a smaller subset for clarity
df_subset = df_sales.filter(col("category") == "Electronics").limit(15)

result_windows = (
    df_subset
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn("running_total", spark_sum("revenue").over(window_running))
    .withColumn("moving_avg_3", spark_round(avg("revenue").over(window_moving_3day), 2))
    .select("txn_date", "product", "revenue", "running_total", "moving_avg_3")
    .orderBy("txn_date")
)
result_windows.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.4: Higher-Order Functions on Arrays
# MAGIC
# MAGIC Higher-order functions operate on arrays without exploding them.

# COMMAND ----------

# Using transform, filter, and aggregate on the orders data
result_hof = (
    df_orders
    .withColumn(
        "item_totals",
        transform(col("items"), lambda x: x.qty * x.price)
    )
    .withColumn(
        "expensive_items",
        array_filter(col("items"), lambda x: x.price > 100)
    )
    .withColumn(
        "order_total",
        array_aggregate(
            col("items"),
            lit(0).cast("integer"),
            lambda acc, x: acc + x.qty * x.price
        )
    )
    .select("order_ref", "item_totals", "expensive_items", "order_total")
)

result_hof.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.5: Pandas UDF vs. Standard UDF Performance
# MAGIC
# MAGIC Pandas UDFs are significantly faster than standard Python UDFs.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import udf, pandas_udf

# Standard Python UDF (slow -- row-at-a-time serialization)
@udf(returnType=DoubleType())
def compute_discount_udf(price, quantity):
    """Apply bulk discount: 10% off for qty >= 5, 5% off for qty >= 3."""
    if price is None or quantity is None:
        return 0.0
    if quantity >= 5:
        return price * quantity * 0.10
    elif quantity >= 3:
        return price * quantity * 0.05
    return 0.0

# Pandas UDF (fast -- vectorized with Apache Arrow)
@pandas_udf(DoubleType())
def compute_discount_pandas(price: pd.Series, quantity: pd.Series) -> pd.Series:
    """Same logic but vectorized."""
    discount = pd.Series([0.0] * len(price))
    mask_high = quantity >= 5
    mask_med = (quantity >= 3) & (quantity < 5)
    discount[mask_high] = price[mask_high] * quantity[mask_high] * 0.10
    discount[mask_med] = price[mask_med] * quantity[mask_med] * 0.05
    return discount

# Compare results (both should produce identical output)
df_udf_test = df_sales.select("txn_id", "product", "price", "quantity")

print("=== Standard UDF result ===")
df_udf_test.withColumn(
    "discount", compute_discount_udf(col("price"), col("quantity"))
).show(5)

print("=== Pandas UDF result ===")
df_udf_test.withColumn(
    "discount", compute_discount_pandas(col("price"), col("quantity"))
).show(5)

# Best practice: prefer built-in functions when possible (no UDF overhead)
print("=== Built-in functions (fastest) ===")
df_udf_test.withColumn(
    "discount",
    when(col("quantity") >= 5, spark_round(col("price") * col("quantity") * 0.10, 2))
    .when(col("quantity") >= 3, spark_round(col("price") * col("quantity") * 0.05, 2))
    .otherwise(0.0)
).show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.6: Streaming with Watermarks
# MAGIC
# MAGIC Watermarks control how long the system waits for late data in stateful operations.
# MAGIC Without watermarks, state grows unboundedly.

# COMMAND ----------

# Simulate streaming with rate source and windowed aggregation
stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
)

# Windowed aggregation with watermark
windowed_stream = (
    stream_df
    .withWatermark("timestamp", "10 seconds")  # Accept data up to 10 seconds late
    .groupBy(
        time_window(col("timestamp"), "5 seconds")  # 5-second tumbling windows
    )
    .agg(
        count("*").alias("event_count"),
        spark_sum("value").alias("total_value")
    )
)

query = (
    windowed_stream
    .writeStream
    .format("memory")
    .queryName("pro_windowed_stream")
    .outputMode("update")
    .start()
)

time.sleep(15)

print("=== Windowed aggregation with watermark ===")
spark.sql("""
    SELECT window.start, window.end, event_count, total_value
    FROM pro_windowed_stream
    ORDER BY window.start DESC
    LIMIT 10
""").show(truncate=False)

query.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 3: Data Modeling (20%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1: SCD Type 2 Implementation
# MAGIC
# MAGIC Slowly Changing Dimension Type 2 maintains full history by closing the current
# MAGIC record and inserting a new one when changes are detected.

# COMMAND ----------

# Show current customer dimension
print("=== Current Customer Dimension ===")
spark.table("pro_customer_dim").show()

# Incoming changes: customer 101 changed region, customer 104 upgraded tier
customer_changes = spark.createDataFrame([
    (101, "Bob Smith", "bob@example.com", "EU-North", "Gold"),    # region + tier change
    (104, "Eve Davis", "eve@example.com", "APAC", "Gold"),        # tier upgrade
    (110, "Jack Wilson", "jack@example.com", "US-East", "Bronze"),  # new customer
], ["customer_id", "name", "email", "region", "tier"])

customer_changes.createOrReplaceTempView("customer_changes")
print("=== Incoming Changes ===")
customer_changes.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD Type 2 MERGE: close current records that changed, insert new versions
# MAGIC MERGE INTO pro_customer_dim AS target
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         c.customer_id,
# MAGIC         c.name,
# MAGIC         c.email,
# MAGIC         c.region,
# MAGIC         c.tier,
# MAGIC         current_date() AS effective_date,
# MAGIC         CAST(NULL AS DATE) AS end_date,
# MAGIC         true AS is_current
# MAGIC     FROM customer_changes c
# MAGIC ) AS source
# MAGIC ON target.customer_id = source.customer_id AND target.is_current = true
# MAGIC
# MAGIC -- Close the existing record when values have changed
# MAGIC WHEN MATCHED AND (target.region != source.region OR target.tier != source.tier) THEN
# MAGIC     UPDATE SET
# MAGIC         target.is_current = false,
# MAGIC         target.end_date = current_date()
# MAGIC
# MAGIC -- Insert new customer (no match at all)
# MAGIC WHEN NOT MATCHED THEN
# MAGIC     INSERT (customer_id, name, email, region, tier, effective_date, end_date, is_current)
# MAGIC     VALUES (source.customer_id, source.name, source.email, source.region, source.tier,
# MAGIC             source.effective_date, source.end_date, source.is_current)

# COMMAND ----------

# Now insert the new current records for changed customers
# (The MERGE above closed old records; we need to insert the new versions)
spark.sql("""
    INSERT INTO pro_customer_dim
    SELECT
        c.customer_id,
        c.name,
        c.email,
        c.region,
        c.tier,
        current_date() AS effective_date,
        CAST(NULL AS DATE) AS end_date,
        true AS is_current
    FROM customer_changes c
    WHERE c.customer_id IN (
        SELECT customer_id FROM pro_customer_dim WHERE is_current = false
        AND end_date = current_date()
    )
""")

print("=== Customer Dimension After SCD Type 2 ===")
spark.table("pro_customer_dim").orderBy("customer_id", "effective_date").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2: Star Schema Query Pattern
# MAGIC
# MAGIC Fact tables join to dimension tables on surrogate keys.
# MAGIC This is the standard analytical query pattern.

# COMMAND ----------

# Simulate a star schema query: sales fact joined to customer dimension
result_star = spark.sql("""
    SELECT
        c.tier AS customer_tier,
        c.region AS customer_region,
        s.category,
        COUNT(*) AS num_transactions,
        ROUND(SUM(s.price * s.quantity), 2) AS total_revenue,
        ROUND(AVG(s.price * s.quantity), 2) AS avg_order_value
    FROM pro_sales s
    JOIN pro_customer_dim c
        ON s.customer_id = c.customer_id
        AND c.is_current = true
    GROUP BY c.tier, c.region, s.category
    ORDER BY total_revenue DESC
""")

print("=== Star Schema Query: Revenue by Customer Tier, Region, Category ===")
result_star.show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.3: Delta Optimization -- OPTIMIZE and Z-ORDER
# MAGIC
# MAGIC OPTIMIZE compacts small files. Z-ORDER co-locates data for better data skipping.

# COMMAND ----------

# Write sales as a Delta table for optimization exercises
df_sales.write.format("delta").mode("overwrite").saveAsTable("pro_sales_delta")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE: compact small files into larger ones
# MAGIC OPTIMIZE pro_sales_delta;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Z-ORDER: co-locate data by commonly filtered columns
# MAGIC OPTIMIZE pro_sales_delta ZORDER BY (region, category);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Show table details (file count, size)
# MAGIC DESCRIBE DETAIL pro_sales_delta;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 4: Security and Governance (10%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1: PII Hashing
# MAGIC
# MAGIC Hash PII columns for pseudonymization. This allows joins on hashed keys
# MAGIC without exposing raw PII.

# COMMAND ----------

# Hash email addresses using SHA-256
df_pii = spark.table("pro_customer_dim").filter(col("is_current") == True)

df_hashed = (
    df_pii
    .withColumn("email_hash", sha2(col("email"), 256))
    .withColumn("name_hash", sha2(col("name"), 256))
    .select("customer_id", "name", "name_hash", "email", "email_hash", "tier")
)

print("=== PII Hashing Example ===")
df_hashed.show(truncate=40)

# In production: store only hashed values in the gold layer
# Keep raw PII only in the silver layer with restricted access
df_pseudonymized = (
    df_pii
    .withColumn("email", sha2(col("email"), 256))
    .withColumn("name", sha2(col("name"), 256))
)
print("=== Pseudonymized Data (for gold layer) ===")
df_pseudonymized.show(truncate=40)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2: Access Control Patterns (Reference)
# MAGIC
# MAGIC ```sql
# MAGIC -- Grant hierarchy for Unity Catalog
# MAGIC
# MAGIC -- 1. Grant USAGE on catalog (required before any schema/table access)
# MAGIC GRANT USAGE ON CATALOG production TO `data_engineers`;
# MAGIC
# MAGIC -- 2. Grant USAGE on schema
# MAGIC GRANT USAGE ON SCHEMA production.analytics TO `data_engineers`;
# MAGIC
# MAGIC -- 3. Grant table-level privileges
# MAGIC GRANT SELECT ON TABLE production.analytics.sales_fact TO `data_engineers`;
# MAGIC GRANT MODIFY ON TABLE production.analytics.sales_fact TO `data_pipeline_sp`;
# MAGIC
# MAGIC -- Row filter (Unity Catalog)
# MAGIC ALTER TABLE production.analytics.sales_fact
# MAGIC SET ROW FILTER region_filter ON (region);
# MAGIC
# MAGIC -- Column mask (Unity Catalog)
# MAGIC ALTER TABLE production.analytics.customers
# MAGIC ALTER COLUMN email SET MASK email_mask;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 5: Monitoring and Logging (10%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1: Analyzing Query Execution Plans
# MAGIC
# MAGIC Understanding execution plans is critical for performance diagnosis.

# COMMAND ----------

# Complex query to analyze
complex_query = (
    df_sales
    .join(broadcast(region_lookup), on="region")
    .filter(col("category") == "Electronics")
    .groupBy("continent", "product")
    .agg(
        spark_sum(col("price") * col("quantity")).alias("total_revenue"),
        count("*").alias("num_transactions")
    )
    .orderBy(col("total_revenue").desc())
)

# Show the physical plan
print("=== Physical Execution Plan ===")
complex_query.explain()

# Show the full plan (parsed -> analyzed -> optimized -> physical)
print("\n=== Full Plan (Extended) ===")
complex_query.explain("extended")

complex_query.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2: Streaming Monitoring
# MAGIC
# MAGIC Monitor streaming query progress and health metrics.

# COMMAND ----------

# Start a stream and inspect its progress
monitor_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withColumn("category", when(col("value") % 3 == 0, "A")
                .when(col("value") % 3 == 1, "B")
                .otherwise("C"))
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        time_window(col("timestamp"), "5 seconds"),
        col("category")
    )
    .count()
)

query = (
    monitor_stream
    .writeStream
    .format("memory")
    .queryName("pro_monitor_stream")
    .outputMode("update")
    .start()
)

time.sleep(15)

# Inspect streaming metrics
progress = query.lastProgress
if progress:
    print("=== Streaming Query Metrics ===")
    print(f"  Batch ID: {progress.get('batchId', 'N/A')}")
    print(f"  Input rows/sec: {progress.get('inputRowsPerSecond', 'N/A')}")
    print(f"  Processed rows/sec: {progress.get('processedRowsPerSecond', 'N/A')}")
    print(f"  Batch duration (ms): {progress.get('batchDuration', 'N/A')}")

    state_ops = progress.get("stateOperators", [])
    for op in state_ops:
        print(f"  State rows: {op.get('numRowsTotal', 'N/A')}")
        print(f"  State memory (bytes): {op.get('memoryUsedBytes', 'N/A')}")
        print(f"  Rows dropped by watermark: {op.get('numRowsDroppedByWatermark', 'N/A')}")
else:
    print("No progress available yet.")

query.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Domain 6: Testing and Deployment (10%)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.1: Unit Testing Pattern
# MAGIC
# MAGIC Demonstrate how to structure testable code and test it.

# COMMAND ----------

# Step 1: Write testable functions (pure Python, no Spark dependency)
def calculate_discount(price, quantity, threshold=5, rate=0.10):
    """Calculate discount for bulk orders.

    Args:
        price: Unit price
        quantity: Number of units
        threshold: Minimum quantity for discount
        rate: Discount rate (0-1)

    Returns:
        Discount amount (float)
    """
    if price is None or quantity is None:
        return 0.0
    if quantity >= threshold:
        return round(price * quantity * rate, 2)
    return 0.0

# Step 2: Test the function (mimicking pytest)
def test_calculate_discount():
    """Unit tests for calculate_discount."""
    # Test bulk discount applies
    assert calculate_discount(100, 5) == 50.0, "Should apply 10% discount for qty >= 5"

    # Test no discount below threshold
    assert calculate_discount(100, 3) == 0.0, "Should not apply discount for qty < 5"

    # Test null handling
    assert calculate_discount(None, 5) == 0.0, "Should return 0 for null price"
    assert calculate_discount(100, None) == 0.0, "Should return 0 for null quantity"

    # Test custom rate
    assert calculate_discount(100, 10, threshold=5, rate=0.20) == 200.0

    print("All unit tests passed!")

test_calculate_discount()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.2: Integration Testing Pattern
# MAGIC
# MAGIC Test Spark operations with real DataFrames.

# COMMAND ----------

def transform_sales(df):
    """Apply standard sales transformations.

    Adds revenue column and categorizes into value tiers.
    """
    return (
        df
        .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
        .withColumn(
            "value_tier",
            when(col("revenue") >= 500, "High")
            .when(col("revenue") >= 100, "Medium")
            .otherwise("Low")
        )
    )

def test_transform_sales():
    """Integration test with real Spark DataFrame."""
    # Arrange
    test_data = spark.createDataFrame([
        (1, "Widget", 100.0, 10),   # revenue = 1000 -> High
        (2, "Gadget", 50.0, 3),     # revenue = 150  -> Medium
        (3, "Thing", 5.0, 2),       # revenue = 10   -> Low
    ], ["id", "product", "price", "quantity"])

    # Act
    result = transform_sales(test_data)

    # Assert
    rows = result.collect()

    assert rows[0]["revenue"] == 1000.0, f"Expected 1000.0, got {rows[0]['revenue']}"
    assert rows[0]["value_tier"] == "High", f"Expected High, got {rows[0]['value_tier']}"

    assert rows[1]["revenue"] == 150.0, f"Expected 150.0, got {rows[1]['revenue']}"
    assert rows[1]["value_tier"] == "Medium", f"Expected Medium, got {rows[1]['value_tier']}"

    assert rows[2]["revenue"] == 10.0, f"Expected 10.0, got {rows[2]['revenue']}"
    assert rows[2]["value_tier"] == "Low", f"Expected Low, got {rows[2]['value_tier']}"

    print("All integration tests passed!")

test_transform_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 6.3: Asset Bundles Configuration (Reference)
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml -- Asset Bundle configuration
# MAGIC bundle:
# MAGIC   name: sales-pipeline
# MAGIC
# MAGIC workspace:
# MAGIC   host: https://my-workspace.cloud.databricks.com
# MAGIC
# MAGIC resources:
# MAGIC   jobs:
# MAGIC     daily_sales_etl:
# MAGIC       name: "Daily Sales ETL"
# MAGIC       schedule:
# MAGIC         quartz_cron_expression: "0 0 8 * * ?"
# MAGIC         timezone_id: "America/New_York"
# MAGIC       tasks:
# MAGIC         - task_key: bronze_ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/01_bronze_ingest.py
# MAGIC           job_cluster_key: etl_cluster
# MAGIC         - task_key: silver_transform
# MAGIC           depends_on:
# MAGIC             - task_key: bronze_ingest
# MAGIC           notebook_task:
# MAGIC             notebook_path: ./notebooks/02_silver_transform.py
# MAGIC           job_cluster_key: etl_cluster
# MAGIC
# MAGIC targets:
# MAGIC   dev:
# MAGIC     default: true
# MAGIC     workspace:
# MAGIC       host: https://dev-workspace.cloud.databricks.com
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://prod-workspace.cloud.databricks.com
# MAGIC     resources:
# MAGIC       jobs:
# MAGIC         daily_sales_etl:
# MAGIC           job_clusters:
# MAGIC             - job_cluster_key: etl_cluster
# MAGIC               new_cluster:
# MAGIC                 num_workers: 8
# MAGIC ```
# MAGIC
# MAGIC **Deployment commands:**
# MAGIC ```bash
# MAGIC databricks bundle validate       # Check configuration
# MAGIC databricks bundle deploy -t dev   # Deploy to dev target
# MAGIC databricks bundle run -t dev daily_sales_etl  # Run the job
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: Professional-Level Key Concepts
# MAGIC
# MAGIC | Concept | Associate Level | Professional Level |
# MAGIC |---------|----------------|-------------------|
# MAGIC | Joins | Know join types | Broadcast hints, skew handling, AQE |
# MAGIC | UDFs | Write basic UDFs | Pandas UDFs, performance trade-offs |
# MAGIC | Streaming | Basic readStream/writeStream | Watermarks, stream-stream joins, foreachBatch |
# MAGIC | Data Modeling | Know Delta basics | SCD Type 2, star schema design |
# MAGIC | Optimization | Know OPTIMIZE exists | Z-ORDER vs Liquid Clustering, AQE tuning |
# MAGIC | Security | GRANT/REVOKE | Row filters, column masks, service principals |
# MAGIC | Testing | Basic assertions | Testing pyramid, mocking, CI/CD integration |
# MAGIC | Tooling | Use notebooks | CLI, REST API, Asset Bundles, Databricks Connect |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up all tables and views
spark.sql("DROP TABLE IF EXISTS pro_customer_dim")
spark.sql("DROP TABLE IF EXISTS pro_sales_delta")
spark.sql("DROP VIEW IF EXISTS pro_sales")
spark.sql("DROP VIEW IF EXISTS pro_orders")
spark.sql("DROP VIEW IF EXISTS customer_changes")

# Remove widgets
dbutils.widgets.removeAll()

print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Next:** Work through the Professional Practice Questions notebook for exam-style questions.
