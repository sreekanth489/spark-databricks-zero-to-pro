# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Professional Data Engineer -- Advanced Code Challenges
# MAGIC
# MAGIC This notebook contains **20 advanced code challenges** covering the Professional exam domains.
# MAGIC Each challenge presents a realistic scenario, asks you to write code, and then reveals the solution.
# MAGIC
# MAGIC **Instructions:**
# MAGIC 1. Read the scenario in each markdown cell.
# MAGIC 2. Write your solution in the empty code cell ("YOUR CODE HERE").
# MAGIC 3. Run the solution cell to check your understanding.
# MAGIC
# MAGIC **Cluster requirement:** Databricks Runtime 13.x+ recommended. Photon-enabled cluster preferred.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Sample Data
# MAGIC
# MAGIC Run this cell first to create the datasets used throughout the challenges.

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType,
    DateType, TimestampType, BooleanType, LongType, ArrayType, MapType
)
from pyspark.sql.functions import (
    col, lit, when, coalesce, concat, concat_ws, upper, lower, trim,
    sum as spark_sum, avg, count, max as spark_max, min as spark_min,
    row_number, rank, dense_rank, lag, lead, ntile, percent_rank,
    current_timestamp, current_date, date_format, datediff, to_date,
    date_add, date_sub, months_between, last_day, to_timestamp,
    explode, posexplode, split, collect_list, collect_set, array, struct,
    map_keys, map_values, size, flatten,
    round as spark_round, expr, broadcast, monotonically_increasing_id,
    from_json, to_json, schema_of_json,
    window as time_window, approx_count_distinct,
    transform, filter as array_filter, aggregate as array_aggregate,
    sha2, md5, regexp_replace, regexp_extract, abs as spark_abs
)
from pyspark.sql.window import Window
from datetime import date, datetime, timedelta
import time

# --- Sales fact table (200 rows) ---
sales_records = []
products = ["Laptop", "Mouse", "Keyboard", "Monitor", "Headphones", "Tablet", "Phone", "Cable"]
categories = {"Laptop": "Electronics", "Mouse": "Accessories", "Keyboard": "Accessories",
              "Monitor": "Electronics", "Headphones": "Audio", "Tablet": "Electronics",
              "Phone": "Electronics", "Cable": "Accessories"}
prices = {"Laptop": 1200, "Mouse": 25, "Keyboard": 75, "Monitor": 350,
          "Headphones": 150, "Tablet": 500, "Phone": 900, "Cable": 10}
regions = ["US-East", "US-West", "EU-North", "EU-South", "APAC"]

for i in range(1, 201):
    product = products[(i - 1) % len(products)]
    region = regions[(i - 1) % len(regions)]
    day_offset = (i - 1) % 28
    cust_id = 100 + (i % 50)
    sales_records.append((
        i,
        f"2024-01-{day_offset + 1:02d}",
        categories[product],
        product,
        float(prices[product]) + (i % 50),
        (i % 10) + 1,
        region,
        cust_id,
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
df_sales.createOrReplaceTempView("challenge_sales")

# --- Customer dimension for SCD exercises ---
customer_dim_data = [
    (100, "Alice Johnson", "alice@example.com", "US-East", "Gold", date(2023, 1, 1), None, True),
    (101, "Bob Smith", "bob@example.com", "US-West", "Silver", date(2023, 3, 15), None, True),
    (102, "Charlie Brown", "charlie@example.com", "EU-North", "Gold", date(2023, 6, 20), None, True),
    (103, "Diana Prince", "diana@example.com", "EU-South", "Bronze", date(2023, 9, 1), None, True),
    (104, "Eve Davis", "eve@example.com", "APAC", "Silver", date(2024, 1, 5), None, True),
    (105, "Frank Wilson", "frank@example.com", "US-East", "Bronze", date(2023, 4, 10), None, True),
    (106, "Grace Lee", "grace@example.com", "US-West", "Gold", date(2023, 7, 22), None, True),
    (107, "Hank Chen", "hank@example.com", "APAC", "Silver", date(2023, 11, 30), None, True),
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
df_customers.write.format("delta").mode("overwrite").saveAsTable("challenge_customer_dim")

# --- Order data with nested structures ---
order_data = [
    (1, "ORD-001", "2024-01-15 10:30:00", [{"item": "Laptop", "qty": 1, "price": 1200.0}, {"item": "Mouse", "qty": 2, "price": 25.0}], "web"),
    (2, "ORD-002", "2024-01-15 11:00:00", [{"item": "Phone", "qty": 1, "price": 900.0}], "mobile"),
    (3, "ORD-003", "2024-01-16 09:15:00", [{"item": "Keyboard", "qty": 3, "price": 75.0}, {"item": "Monitor", "qty": 1, "price": 350.0}, {"item": "Cable", "qty": 5, "price": 10.0}], "web"),
    (4, "ORD-004", "2024-01-16 14:45:00", [{"item": "Tablet", "qty": 2, "price": 500.0}], "store"),
    (5, "ORD-005", "2024-01-17 08:00:00", [{"item": "Headphones", "qty": 4, "price": 150.0}, {"item": "Cable", "qty": 10, "price": 10.0}], "web"),
]

item_schema = ArrayType(StructType([
    StructField("item", StringType(), False),
    StructField("qty", IntegerType(), False),
    StructField("price", DoubleType(), False),
]))

order_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("order_ref", StringType(), False),
    StructField("order_time", StringType(), False),
    StructField("items", item_schema, False),
    StructField("channel", StringType(), False),
])

df_orders = spark.createDataFrame(order_data, schema=order_schema)
df_orders = df_orders.withColumn("order_time", to_timestamp(col("order_time")))
df_orders.createOrReplaceTempView("challenge_orders")

# --- Region lookup (small table for broadcast exercises) ---
region_lookup_data = [
    ("US-East", "Americas", "USD", 1.0),
    ("US-West", "Americas", "USD", 1.0),
    ("EU-North", "Europe", "EUR", 0.92),
    ("EU-South", "Europe", "EUR", 0.92),
    ("APAC", "Asia-Pacific", "SGD", 1.35),
]
df_region_lookup = spark.createDataFrame(
    region_lookup_data,
    ["region", "continent", "currency", "usd_rate"]
)
df_region_lookup.createOrReplaceTempView("challenge_regions")

print("Setup complete:")
print(f"  - challenge_sales (temp view, {df_sales.count()} rows)")
print(f"  - challenge_customer_dim (Delta table, {df_customers.count()} rows)")
print(f"  - challenge_orders (temp view, {df_orders.count()} rows with nested items)")
print(f"  - challenge_regions (temp view, {df_region_lookup.count()} rows)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 1: Broadcast Join with Currency Conversion
# MAGIC
# MAGIC **Scenario:** You need to join the 200-row sales table with the 5-row region lookup
# MAGIC table, then convert each transaction's revenue to USD using the exchange rate.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Broadcast-join `challenge_sales` with `challenge_regions` on `region`
# MAGIC 2. Add a column `revenue_usd` = `price * quantity / usd_rate`
# MAGIC 3. Show the top 10 transactions by `revenue_usd` descending
# MAGIC 4. Print the execution plan to confirm BroadcastHashJoin is used

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ch1 = (
    df_sales
    .join(broadcast(df_region_lookup), on="region", how="inner")
    .withColumn("revenue_usd", spark_round(col("price") * col("quantity") / col("usd_rate"), 2))
    .select("txn_id", "product", "region", "continent", "currency", "price", "quantity", "usd_rate", "revenue_usd")
    .orderBy(col("revenue_usd").desc())
)

print("=== Top 10 Transactions by USD Revenue ===")
result_ch1.show(10)

print("\n=== Execution Plan (should show BroadcastHashJoin) ===")
result_ch1.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 2: Handling Data Skew with Salting
# MAGIC
# MAGIC **Scenario:** The `challenge_sales` table has skewed data: US-East has many more
# MAGIC transactions than other regions. You need to join it with another table by region
# MAGIC without one partition becoming a bottleneck.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Show the data distribution by region (demonstrate the skew)
# MAGIC 2. Add a "salt" column (random integer 0-4) to the sales table
# MAGIC 3. Create a salted key `region_salt` = `concat(region, "_", salt)`
# MAGIC 4. Demonstrate how the salted key distributes the skewed region more evenly

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
from pyspark.sql.functions import rand, floor

# Step 1: Show the skew
print("=== Data Distribution by Region (Skewed) ===")
df_sales.groupBy("region").count().orderBy(col("count").desc()).show()

# Step 2-3: Salt the key
num_salts = 5
df_salted = (
    df_sales
    .withColumn("salt", floor(rand() * num_salts).cast("int"))
    .withColumn("salted_key", concat_ws("_", col("region"), col("salt").cast("string")))
)

# Step 4: Show the more even distribution
print("=== Distribution After Salting ===")
df_salted.groupBy("salted_key").count().orderBy(col("count").desc()).show(25)

print("Note: To join with a salted key, the lookup table must also be exploded")
print("with all salt values (cross join with salt range) so both sides match.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 3: Complex Window Functions -- Running Totals and Gaps
# MAGIC
# MAGIC **Scenario:** For each region, calculate a running total of revenue by date.
# MAGIC Also, use `lag` to find the revenue change from the previous day.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Aggregate daily revenue by region
# MAGIC 2. Add a running total (cumulative sum) within each region ordered by date
# MAGIC 3. Add a `prev_day_revenue` column using `lag`
# MAGIC 4. Add a `day_over_day_change` column (current - previous)

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1: Daily revenue by region
daily_revenue = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .groupBy("region", "txn_date")
    .agg(spark_round(spark_sum("revenue"), 2).alias("daily_revenue"))
    .orderBy("region", "txn_date")
)

# Step 2-4: Window functions
window_running = (
    Window
    .partitionBy("region")
    .orderBy("txn_date")
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)
)

window_lag = Window.partitionBy("region").orderBy("txn_date")

result_ch3 = (
    daily_revenue
    .withColumn("running_total", spark_round(spark_sum("daily_revenue").over(window_running), 2))
    .withColumn("prev_day_revenue", lag("daily_revenue", 1).over(window_lag))
    .withColumn(
        "day_over_day_change",
        spark_round(col("daily_revenue") - coalesce(col("prev_day_revenue"), lit(0)), 2)
    )
)

print("=== Running Totals and Day-over-Day Changes (US-East) ===")
result_ch3.filter(col("region") == "US-East").show(15)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 4: Higher-Order Functions on Nested Data
# MAGIC
# MAGIC **Scenario:** The `challenge_orders` table has an `items` array column. Without
# MAGIC using `explode`, calculate metrics using higher-order functions.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Use `transform` to create an array of line totals (qty * price) per item
# MAGIC 2. Use `filter` (array_filter) to keep only items costing more than $100
# MAGIC 3. Use `aggregate` to calculate the total order value
# MAGIC 4. Add a `num_expensive_items` column (count of items with price > 100)

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
result_ch4 = (
    df_orders
    .withColumn(
        "line_totals",
        transform(col("items"), lambda x: spark_round(x.qty * x.price, 2))
    )
    .withColumn(
        "expensive_items",
        array_filter(col("items"), lambda x: x.price > 100)
    )
    .withColumn(
        "order_total",
        spark_round(
            array_aggregate(
                col("items"),
                lit(0.0),
                lambda acc, x: acc + x.qty * x.price
            ),
            2
        )
    )
    .withColumn("num_expensive_items", size(col("expensive_items")))
    .select("order_ref", "channel", "line_totals", "order_total",
            "num_expensive_items", "expensive_items")
)

print("=== Higher-Order Function Results ===")
result_ch4.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 5: SCD Type 2 with MERGE
# MAGIC
# MAGIC **Scenario:** Customer updates have arrived. Some customers changed their tier,
# MAGIC and there is a new customer. Implement SCD Type 2: close old records and insert
# MAGIC new versions while preserving history.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Create a source DataFrame with customer changes
# MAGIC 2. Write a MERGE that closes current records when tier or region changes
# MAGIC 3. Insert new current records for changed customers
# MAGIC 4. Insert brand-new customers
# MAGIC 5. Verify that closed records have `is_current = false` and `end_date` set

# COMMAND ----------

# Source data: updates for existing customers and one new customer
customer_updates = spark.createDataFrame([
    (101, "Bob Smith", "bob@example.com", "US-West", "Gold"),       # tier change: Silver -> Gold
    (103, "Diana Prince", "diana@example.com", "US-East", "Silver"), # region + tier change
    (110, "Ivy Zhang", "ivy@example.com", "APAC", "Bronze"),         # new customer
], ["customer_id", "name", "email", "region", "tier"])

customer_updates.createOrReplaceTempView("challenge_customer_updates")

print("=== Current Customer Dimension ===")
spark.table("challenge_customer_dim").orderBy("customer_id").show(truncate=False)

print("=== Incoming Changes ===")
customer_updates.show()

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1: MERGE to close changed records
spark.sql("""
    MERGE INTO challenge_customer_dim AS target
    USING challenge_customer_updates AS source
    ON target.customer_id = source.customer_id AND target.is_current = true
    WHEN MATCHED AND (target.tier != source.tier OR target.region != source.region)
    THEN UPDATE SET
        target.is_current = false,
        target.end_date = current_date()
    WHEN NOT MATCHED THEN
        INSERT (customer_id, name, email, region, tier, effective_date, end_date, is_current)
        VALUES (source.customer_id, source.name, source.email, source.region, source.tier,
                current_date(), NULL, true)
""")

# Step 2: Insert new current records for customers whose old records were closed
spark.sql("""
    INSERT INTO challenge_customer_dim
    SELECT
        u.customer_id, u.name, u.email, u.region, u.tier,
        current_date() AS effective_date,
        CAST(NULL AS DATE) AS end_date,
        true AS is_current
    FROM challenge_customer_updates u
    WHERE u.customer_id IN (
        SELECT customer_id
        FROM challenge_customer_dim
        WHERE is_current = false AND end_date = current_date()
    )
""")

print("=== Customer Dimension After SCD Type 2 MERGE ===")
spark.table("challenge_customer_dim").orderBy("customer_id", "effective_date").show(truncate=False)

# Verify: Bob (101) should have 2 records, Diana (103) should have 2, Ivy (110) is new
print("=== Record Counts per Customer ===")
spark.sql("""
    SELECT customer_id, name,
           COUNT(*) AS versions,
           SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS current_versions
    FROM challenge_customer_dim
    GROUP BY customer_id, name
    ORDER BY customer_id
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 6: Streaming with Watermarks and Windowed Aggregation
# MAGIC
# MAGIC **Scenario:** You receive event data from a streaming source. Events may arrive
# MAGIC late. Implement a windowed count with a watermark to bound state growth.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Create a streaming DataFrame from the `rate` source (10 rows/second)
# MAGIC 2. Add a watermark of 10 seconds on the timestamp column
# MAGIC 3. Group by a 5-second tumbling window and count events
# MAGIC 4. Write to memory sink and display results after 15 seconds
# MAGIC 5. Stop the stream

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
stream_df = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn("category", when(col("value") % 3 == 0, "A")
                .when(col("value") % 3 == 1, "B")
                .otherwise("C"))
)

windowed_counts = (
    stream_df
    .withWatermark("timestamp", "10 seconds")
    .groupBy(
        time_window(col("timestamp"), "5 seconds"),
        col("category")
    )
    .agg(count("*").alias("event_count"))
)

query_ch6 = (
    windowed_counts
    .writeStream
    .format("memory")
    .queryName("challenge_windowed_stream")
    .outputMode("update")
    .start()
)

time.sleep(15)

print("=== Windowed Aggregation with Watermark ===")
spark.sql("""
    SELECT window.start, window.end, category, event_count
    FROM challenge_windowed_stream
    ORDER BY window.start DESC, category
    LIMIT 15
""").show(truncate=False)

# Inspect metrics
progress = query_ch6.lastProgress
if progress:
    print(f"Input rows/sec: {progress.get('inputRowsPerSecond', 'N/A')}")
    print(f"Processed rows/sec: {progress.get('processedRowsPerSecond', 'N/A')}")
    state_ops = progress.get("stateOperators", [])
    for op in state_ops:
        print(f"State rows total: {op.get('numRowsTotal', 'N/A')}")

query_ch6.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 7: foreachBatch with MERGE (Streaming Upsert)
# MAGIC
# MAGIC **Scenario:** A streaming pipeline receives updated product prices. You need to
# MAGIC upsert each micro-batch into a Delta table using `foreachBatch` with MERGE.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Create a target Delta table for product prices
# MAGIC 2. Write a `foreachBatch` function that MERGEs each micro-batch
# MAGIC 3. Use `rate` source to simulate streaming updates
# MAGIC 4. Verify the upsert behavior

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1: Create target table
product_prices = spark.createDataFrame([
    ("Laptop", 1200.00, "2024-01-01"),
    ("Mouse", 25.00, "2024-01-01"),
    ("Keyboard", 75.00, "2024-01-01"),
    ("Monitor", 350.00, "2024-01-01"),
], ["product_name", "price", "last_updated"])

product_prices = product_prices.withColumn("last_updated", to_date(col("last_updated")))
product_prices.write.format("delta").mode("overwrite").saveAsTable("challenge_product_prices")

print("=== Initial Product Prices ===")
spark.table("challenge_product_prices").show()

# Step 2: foreachBatch function
def upsert_to_product_prices(batch_df, batch_id):
    """MERGE micro-batch into product prices table."""
    # Map rate source values to product updates
    products_list = ["Laptop", "Mouse", "Keyboard", "Monitor"]
    updates = (
        batch_df
        .withColumn("product_name", expr(
            f"CASE value % 4 "
            f"WHEN 0 THEN 'Laptop' "
            f"WHEN 1 THEN 'Mouse' "
            f"WHEN 2 THEN 'Keyboard' "
            f"ELSE 'Monitor' END"
        ))
        .withColumn("price", spark_round(col("value") * 1.5 + 10, 2))
        .withColumn("last_updated", current_date())
        .select("product_name", "price", "last_updated")
    )

    updates.createOrReplaceTempView(f"batch_updates_{batch_id}")

    spark.sql(f"""
        MERGE INTO challenge_product_prices AS target
        USING batch_updates_{batch_id} AS source
        ON target.product_name = source.product_name
        WHEN MATCHED THEN UPDATE SET
            target.price = source.price,
            target.last_updated = source.last_updated
        WHEN NOT MATCHED THEN INSERT *
    """)

# Step 3: Run stream
query_ch7 = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .writeStream
    .foreachBatch(upsert_to_product_prices)
    .option("checkpointLocation", "/tmp/challenge_product_prices_ckpt")
    .start()
)

time.sleep(10)
query_ch7.stop()

# Step 4: Verify
print("=== Product Prices After Streaming Upsert ===")
spark.table("challenge_product_prices").show()

print("=== Table History ===")
spark.sql("DESCRIBE HISTORY challenge_product_prices").select(
    "version", "operation", "timestamp"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 8: Explode and Re-Aggregate Nested Data
# MAGIC
# MAGIC **Scenario:** The `challenge_orders` table has nested items. You need to explode
# MAGIC the items, calculate metrics at the item level, and then re-aggregate.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Explode the `items` array to get one row per item
# MAGIC 2. Calculate `line_total` = qty * price for each item
# MAGIC 3. Find the most expensive item (by line_total) in each order
# MAGIC 4. Calculate the total revenue per channel

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1-2: Explode and calculate line totals
exploded = (
    df_orders
    .select("order_id", "order_ref", "channel", "order_time",
            explode(col("items")).alias("item"))
    .withColumn("item_name", col("item.item"))
    .withColumn("qty", col("item.qty"))
    .withColumn("item_price", col("item.price"))
    .withColumn("line_total", spark_round(col("qty") * col("item_price"), 2))
    .drop("item")
)

print("=== Exploded Items with Line Totals ===")
exploded.show(truncate=False)

# Step 3: Most expensive item per order (using window function)
w_order = Window.partitionBy("order_id").orderBy(col("line_total").desc())
top_items = (
    exploded
    .withColumn("rn", row_number().over(w_order))
    .filter(col("rn") == 1)
    .select("order_ref", "item_name", "line_total")
)

print("=== Most Expensive Item per Order ===")
top_items.show()

# Step 4: Revenue per channel
print("=== Total Revenue per Channel ===")
(
    exploded
    .groupBy("channel")
    .agg(
        spark_round(spark_sum("line_total"), 2).alias("total_revenue"),
        count("*").alias("num_line_items")
    )
    .orderBy(col("total_revenue").desc())
).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 9: Performance Comparison -- Built-in vs UDF
# MAGIC
# MAGIC **Scenario:** You need to categorize transactions by revenue tier. Compare three
# MAGIC approaches: built-in functions, Python UDF, and Pandas UDF.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Implement the tier logic using `when/otherwise` (built-in)
# MAGIC 2. Implement the same logic as a standard Python UDF
# MAGIC 3. Implement it as a Pandas UDF
# MAGIC 4. Compare all three produce the same result

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
import pandas as pd
from pyspark.sql.functions import udf, pandas_udf

# Approach 1: Built-in functions (fastest)
df_builtin = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn(
        "tier",
        when(col("revenue") >= 1000, "Premium")
        .when(col("revenue") >= 500, "High")
        .when(col("revenue") >= 100, "Medium")
        .otherwise("Low")
    )
)

# Approach 2: Standard Python UDF (slowest)
@udf(returnType=StringType())
def tier_udf(revenue):
    if revenue is None:
        return "Unknown"
    if revenue >= 1000:
        return "Premium"
    elif revenue >= 500:
        return "High"
    elif revenue >= 100:
        return "Medium"
    return "Low"

df_udf = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn("tier", tier_udf(col("revenue")))
)

# Approach 3: Pandas UDF (fast, vectorized)
@pandas_udf(StringType())
def tier_pandas_udf(revenue: pd.Series) -> pd.Series:
    result = pd.Series(["Low"] * len(revenue))
    result[revenue >= 100] = "Medium"
    result[revenue >= 500] = "High"
    result[revenue >= 1000] = "Premium"
    result[revenue.isna()] = "Unknown"
    return result

df_pandas_udf = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn("tier", tier_pandas_udf(col("revenue")))
)

# Compare results
print("=== Built-in Functions (Sample) ===")
df_builtin.select("txn_id", "product", "revenue", "tier").show(5)

print("=== Python UDF (Sample) ===")
df_udf.select("txn_id", "product", "revenue", "tier").show(5)

print("=== Pandas UDF (Sample) ===")
df_pandas_udf.select("txn_id", "product", "revenue", "tier").show(5)

# Verify same distribution
print("=== Tier Distribution (Built-in) ===")
df_builtin.groupBy("tier").count().orderBy("tier").show()

print("=== Tier Distribution (Pandas UDF) ===")
df_pandas_udf.groupBy("tier").count().orderBy("tier").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 10: Delta Time Travel and RESTORE
# MAGIC
# MAGIC **Scenario:** After an accidental UPDATE that set all prices to 0, you need
# MAGIC to recover the table to its previous state using Delta time travel.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write the sales data to a Delta table
# MAGIC 2. Perform an accidental update (set all prices to 0)
# MAGIC 3. Use DESCRIBE HISTORY to find the version before the accident
# MAGIC 4. Use RESTORE to revert the table
# MAGIC 5. Verify the data is restored

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1: Write Delta table
df_sales.write.format("delta").mode("overwrite").saveAsTable("challenge_sales_delta")
original_avg = spark.table("challenge_sales_delta").agg(spark_round(avg("price"), 2)).collect()[0][0]
print(f"Original average price: {original_avg}")

# Step 2: Accidental update
spark.sql("UPDATE challenge_sales_delta SET price = 0")
bad_avg = spark.table("challenge_sales_delta").agg(spark_round(avg("price"), 2)).collect()[0][0]
print(f"After accident -- average price: {bad_avg}")

# Step 3: Check history
print("\n=== Table History ===")
spark.sql("DESCRIBE HISTORY challenge_sales_delta").select(
    "version", "operation", "timestamp"
).show(truncate=False)

# Step 4: Restore to version 0 (before the UPDATE)
spark.sql("RESTORE TABLE challenge_sales_delta TO VERSION AS OF 0")

# Step 5: Verify
restored_avg = spark.table("challenge_sales_delta").agg(spark_round(avg("price"), 2)).collect()[0][0]
print(f"After RESTORE -- average price: {restored_avg}")
print(f"Successfully restored: {abs(restored_avg - original_avg) < 0.01}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 11: Star Schema Query with Multiple Dimensions
# MAGIC
# MAGIC **Scenario:** Join the sales fact table with both the customer dimension and
# MAGIC the region lookup to build a comprehensive analytical view.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Join sales with current customers (is_current = true) on customer_id
# MAGIC 2. Join with region lookup on region (use broadcast for the small table)
# MAGIC 3. Calculate total revenue by customer tier, continent, and category
# MAGIC 4. Add a rank per continent based on total revenue

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
star_query = (
    df_sales
    .join(
        spark.table("challenge_customer_dim").filter(col("is_current") == True),
        on="customer_id",
        how="inner"
    )
    .join(
        broadcast(df_region_lookup),
        df_sales["region"] == df_region_lookup["region"],
        how="inner"
    )
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .groupBy("tier", "continent", "category")
    .agg(
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        count("*").alias("num_transactions"),
        spark_round(avg("revenue"), 2).alias("avg_transaction")
    )
)

# Add rank per continent
w_continent = Window.partitionBy("continent").orderBy(col("total_revenue").desc())
result_ch11 = (
    star_query
    .withColumn("revenue_rank", rank().over(w_continent))
    .orderBy("continent", "revenue_rank")
)

print("=== Star Schema Analysis: Revenue by Tier, Continent, Category ===")
result_ch11.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 12: PII Masking and Pseudonymization
# MAGIC
# MAGIC **Scenario:** Prepare customer data for the Gold layer by masking PII fields.
# MAGIC Different masking strategies are needed for different columns.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Hash email addresses using SHA-256
# MAGIC 2. Mask names: show only the first initial and last name (e.g., "Alice Johnson" -> "A. Johnson")
# MAGIC 3. Keep non-PII columns (customer_id, region, tier) unchanged
# MAGIC 4. Show both the original and masked versions side by side

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
df_pii_source = spark.table("challenge_customer_dim").filter(col("is_current") == True)

# Masking logic
df_masked = (
    df_pii_source
    .withColumn("original_name", col("name"))
    .withColumn("original_email", col("email"))
    # Mask name: first initial + ". " + last name
    .withColumn(
        "masked_name",
        concat(
            upper(expr("substring(name, 1, 1)")),
            lit(". "),
            expr("substring_index(name, ' ', -1)")
        )
    )
    # Hash email
    .withColumn("hashed_email", sha2(col("email"), 256))
    .select(
        "customer_id",
        "original_name", "masked_name",
        "original_email", "hashed_email",
        "region", "tier"
    )
)

print("=== PII Masking Results ===")
df_masked.show(truncate=40)

# Gold layer version (only masked data)
df_gold = (
    df_pii_source
    .withColumn(
        "name",
        concat(
            upper(expr("substring(name, 1, 1)")),
            lit(". "),
            expr("substring_index(name, ' ', -1)")
        )
    )
    .withColumn("email", sha2(col("email"), 256))
    .select("customer_id", "name", "email", "region", "tier")
)

print("=== Gold Layer (Masked PII) ===")
df_gold.show(truncate=50)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 13: OPTIMIZE and Table Maintenance
# MAGIC
# MAGIC **Scenario:** A Delta table has accumulated many small files from micro-batch
# MAGIC writes. Run OPTIMIZE with Z-ORDER and inspect the results.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Check the current file count using DESCRIBE DETAIL
# MAGIC 2. Run OPTIMIZE with Z-ORDER on region and category
# MAGIC 3. Check the file count again
# MAGIC 4. Show the DESCRIBE HISTORY to see the OPTIMIZE operation

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# MAGIC %sql
# MAGIC -- SOLUTION
# MAGIC -- Step 1: Check current state
# MAGIC DESCRIBE DETAIL challenge_sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 2: OPTIMIZE with Z-ORDER
# MAGIC OPTIMIZE challenge_sales_delta
# MAGIC ZORDER BY (region, category)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 3: Check after optimization
# MAGIC DESCRIBE DETAIL challenge_sales_delta

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Step 4: Show history
# MAGIC DESCRIBE HISTORY challenge_sales_delta

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 14: Monitoring a Streaming Query
# MAGIC
# MAGIC **Scenario:** You have a streaming pipeline and need to extract key health
# MAGIC metrics from `query.lastProgress` for a monitoring dashboard.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Start a windowed streaming aggregation with watermark
# MAGIC 2. After 15 seconds, extract and display:
# MAGIC    - Batch ID, input rows/sec, processed rows/sec
# MAGIC    - State size (number of rows in state)
# MAGIC    - Rows dropped by watermark
# MAGIC 3. Stop the stream

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
monitor_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 20)
    .load()
    .withWatermark("timestamp", "10 seconds")
    .groupBy(time_window(col("timestamp"), "5 seconds"))
    .agg(
        count("*").alias("event_count"),
        spark_sum("value").alias("total_value")
    )
)

query_ch14 = (
    monitor_stream
    .writeStream
    .format("memory")
    .queryName("challenge_monitoring")
    .outputMode("update")
    .start()
)

time.sleep(15)

# Extract metrics
progress = query_ch14.lastProgress
if progress:
    print("=== Streaming Health Metrics ===")
    print(f"  Batch ID:            {progress.get('batchId', 'N/A')}")
    print(f"  Input rows/sec:      {progress.get('inputRowsPerSecond', 'N/A')}")
    print(f"  Processed rows/sec:  {progress.get('processedRowsPerSecond', 'N/A')}")
    print(f"  Batch duration (ms): {progress.get('batchDuration', 'N/A')}")

    sources = progress.get("sources", [])
    for s in sources:
        print(f"  Source description:   {s.get('description', 'N/A')}")
        print(f"  Start offset:        {s.get('startOffset', 'N/A')}")
        print(f"  End offset:          {s.get('endOffset', 'N/A')}")

    state_ops = progress.get("stateOperators", [])
    for idx, op in enumerate(state_ops):
        print(f"  --- State Operator {idx} ---")
        print(f"    Total rows in state:       {op.get('numRowsTotal', 'N/A')}")
        print(f"    Rows updated:              {op.get('numRowsUpdated', 'N/A')}")
        print(f"    Memory used (bytes):       {op.get('memoryUsedBytes', 'N/A')}")
        print(f"    Rows dropped by watermark: {op.get('numRowsDroppedByWatermark', 'N/A')}")
else:
    print("No progress data available yet.")

# Show output sample
print("\n=== Stream Output Sample ===")
spark.sql("""
    SELECT window.start, window.end, event_count, total_value
    FROM challenge_monitoring
    ORDER BY window.start DESC
    LIMIT 5
""").show(truncate=False)

query_ch14.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 15: Unit Testing a Transformation Function
# MAGIC
# MAGIC **Scenario:** Write a testable transformation function and test it with
# MAGIC assertions, simulating how you would use pytest in a CI/CD pipeline.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write a function `enrich_sales(df)` that:
# MAGIC    - Adds a `revenue` column (price * quantity)
# MAGIC    - Adds a `is_high_value` boolean column (true if revenue >= 500)
# MAGIC    - Filters out rows where quantity <= 0
# MAGIC 2. Write test assertions for edge cases
# MAGIC 3. All tests should pass

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
def enrich_sales(df):
    """Enrich sales DataFrame with revenue and high-value indicator.

    Args:
        df: DataFrame with columns price (double), quantity (int)

    Returns:
        DataFrame with added revenue and is_high_value columns,
        filtered to only positive-quantity rows.
    """
    return (
        df
        .filter(col("quantity") > 0)
        .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
        .withColumn("is_high_value", col("price") * col("quantity") >= 500)
    )


def test_enrich_sales():
    """Integration tests for enrich_sales function."""

    # Test 1: Normal case -- high value
    test_df = spark.createDataFrame(
        [(1, "Widget", 100.0, 10)],
        ["id", "product", "price", "quantity"]
    )
    result = enrich_sales(test_df).collect()
    assert len(result) == 1, f"Expected 1 row, got {len(result)}"
    assert result[0]["revenue"] == 1000.0, f"Expected 1000.0, got {result[0]['revenue']}"
    assert result[0]["is_high_value"] == True, "Should be high value"

    # Test 2: Low value
    test_df2 = spark.createDataFrame(
        [(2, "Pen", 5.0, 3)],
        ["id", "product", "price", "quantity"]
    )
    result2 = enrich_sales(test_df2).collect()
    assert result2[0]["revenue"] == 15.0, f"Expected 15.0, got {result2[0]['revenue']}"
    assert result2[0]["is_high_value"] == False, "Should not be high value"

    # Test 3: Zero quantity is filtered out
    test_df3 = spark.createDataFrame(
        [(3, "Ghost", 50.0, 0)],
        ["id", "product", "price", "quantity"]
    )
    result3 = enrich_sales(test_df3).collect()
    assert len(result3) == 0, f"Expected 0 rows (filtered), got {len(result3)}"

    # Test 4: Boundary case -- exactly 500
    test_df4 = spark.createDataFrame(
        [(4, "Exact", 500.0, 1)],
        ["id", "product", "price", "quantity"]
    )
    result4 = enrich_sales(test_df4).collect()
    assert result4[0]["is_high_value"] == True, "500 should be high value (>= 500)"

    # Test 5: Multiple rows mixed
    test_df5 = spark.createDataFrame([
        (5, "A", 1000.0, 2),   # high value, quantity > 0
        (6, "B", 10.0, 1),     # low value, quantity > 0
        (7, "C", 999.0, 0),    # filtered out (qty = 0)
        (8, "D", 250.0, 2),    # high value, quantity > 0
    ], ["id", "product", "price", "quantity"])
    result5 = enrich_sales(test_df5).collect()
    assert len(result5) == 3, f"Expected 3 rows after filter, got {len(result5)}"

    print("All 5 tests passed!")


test_enrich_sales()

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 16: Pivot and Unpivot Patterns
# MAGIC
# MAGIC **Scenario:** Create a pivot table of revenue by category and region, then
# MAGIC unpivot it back to the long format.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Calculate revenue (price * quantity) per transaction
# MAGIC 2. Pivot: rows = category, columns = region, values = total revenue
# MAGIC 3. Unpivot the result back to long format using `stack()`

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1-2: Pivot
pivoted = (
    df_sales
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .groupBy("category")
    .pivot("region")
    .agg(spark_round(spark_sum("revenue"), 2))
)

print("=== Pivoted: Revenue by Category and Region ===")
pivoted.show()

# Step 3: Unpivot using stack()
region_columns = [c for c in pivoted.columns if c != "category"]
stack_expr = ", ".join([f"'{r}', `{r}`" for r in region_columns])
num_regions = len(region_columns)

unpivoted = pivoted.select(
    "category",
    expr(f"stack({num_regions}, {stack_expr}) AS (region, total_revenue)")
).filter(col("total_revenue").isNotNull())

print("=== Unpivoted: Back to Long Format ===")
unpivoted.orderBy("category", "region").show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 17: Execution Plan Analysis
# MAGIC
# MAGIC **Scenario:** Analyze and compare execution plans for two versions of the
# MAGIC same query -- one efficient and one inefficient.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Write an inefficient version: join, then filter
# MAGIC 2. Write an efficient version: filter first, then join
# MAGIC 3. Show both execution plans and identify the differences
# MAGIC 4. Note: Catalyst may optimize both to the same plan (predicate pushdown)

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Inefficient: join first, filter later
query_inefficient = (
    df_sales
    .join(broadcast(df_region_lookup), on="region")
    .filter(col("category") == "Electronics")
    .filter(col("continent") == "Americas")
    .groupBy("product")
    .agg(spark_round(spark_sum(col("price") * col("quantity")), 2).alias("total_revenue"))
)

# Efficient: filter first, then join
query_efficient = (
    df_sales
    .filter(col("category") == "Electronics")
    .join(
        broadcast(df_region_lookup.filter(col("continent") == "Americas")),
        on="region"
    )
    .groupBy("product")
    .agg(spark_round(spark_sum(col("price") * col("quantity")), 2).alias("total_revenue"))
)

print("=== Inefficient Query Plan ===")
query_inefficient.explain()

print("\n=== Efficient Query Plan ===")
query_efficient.explain()

print("\n=== Results (should be identical) ===")
print("Inefficient:")
query_inefficient.orderBy(col("total_revenue").desc()).show()
print("Efficient:")
query_efficient.orderBy(col("total_revenue").desc()).show()

print("Note: Catalyst's predicate pushdown may optimize both queries similarly.")
print("Check the plans above -- look for Filter operators and their positions.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 18: Data Quality Validation Framework
# MAGIC
# MAGIC **Scenario:** Build a simple data quality framework that checks multiple
# MAGIC rules against a DataFrame, similar to DLT expectations.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Define a list of quality rules (column, condition, severity)
# MAGIC 2. Evaluate each rule and count violations
# MAGIC 3. Report a quality summary
# MAGIC 4. Filter the DataFrame to only "clean" rows (pass all rules)

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
def run_quality_checks(df, rules):
    """Run data quality checks against a DataFrame.

    Args:
        df: Source DataFrame to validate
        rules: List of dicts with keys: name, column, condition (SQL expr), severity

    Returns:
        Tuple of (quality_report_df, clean_df)
    """
    total_rows = df.count()
    results = []
    all_conditions = []

    for rule in rules:
        violation_count = df.filter(~expr(rule["condition"])).count()
        pass_rate = round((total_rows - violation_count) / total_rows * 100, 2)
        results.append((
            rule["name"],
            rule["condition"],
            rule["severity"],
            violation_count,
            pass_rate
        ))
        all_conditions.append(rule["condition"])

    # Build report
    report_df = spark.createDataFrame(
        results,
        ["rule_name", "condition", "severity", "violations", "pass_rate_pct"]
    )

    # Clean rows: pass ALL conditions
    combined_filter = " AND ".join([f"({c})" for c in all_conditions])
    clean_df = df.filter(expr(combined_filter))

    return report_df, clean_df


# Define rules
quality_rules = [
    {"name": "non_null_region", "column": "region", "condition": "region IS NOT NULL", "severity": "ERROR"},
    {"name": "positive_price", "column": "price", "condition": "price > 0", "severity": "ERROR"},
    {"name": "positive_quantity", "column": "quantity", "condition": "quantity > 0", "severity": "WARNING"},
    {"name": "valid_category", "column": "category", "condition": "category IN ('Electronics', 'Accessories', 'Audio', 'Books', 'Clothing')", "severity": "ERROR"},
    {"name": "reasonable_price", "column": "price", "condition": "price < 10000", "severity": "WARNING"},
]

# Run checks
report, clean_data = run_quality_checks(df_sales, quality_rules)

print("=== Data Quality Report ===")
report.show(truncate=False)

print(f"Total rows: {df_sales.count()}")
print(f"Clean rows: {clean_data.count()}")
print(f"Overall quality: {clean_data.count() / df_sales.count() * 100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 19: Streaming Join -- Stream with Static Table
# MAGIC
# MAGIC **Scenario:** A streaming source produces events that need to be enriched
# MAGIC with data from a static dimension table.
# MAGIC
# MAGIC **Task:**
# MAGIC 1. Create a streaming source using `rate`
# MAGIC 2. Map the stream values to regions
# MAGIC 3. Join the stream with the static region lookup table
# MAGIC 4. Write enriched events to a memory sink
# MAGIC 5. Display results and stop

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION
# Step 1-2: Create stream and map to regions
regions_list = ["US-East", "US-West", "EU-North", "EU-South", "APAC"]

stream_events = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 10)
    .load()
    .withColumn(
        "region",
        expr(f"""
            CASE value % 5
                WHEN 0 THEN 'US-East'
                WHEN 1 THEN 'US-West'
                WHEN 2 THEN 'EU-North'
                WHEN 3 THEN 'EU-South'
                ELSE 'APAC'
            END
        """)
    )
)

# Step 3: Join stream with static table (no watermark needed)
enriched_stream = stream_events.join(
    df_region_lookup,
    on="region",
    how="inner"
)

# Step 4: Write to memory sink
query_ch19 = (
    enriched_stream
    .writeStream
    .format("memory")
    .queryName("challenge_enriched_events")
    .outputMode("append")
    .start()
)

time.sleep(10)

# Step 5: Display and stop
print("=== Enriched Streaming Events (Stream-Static Join) ===")
spark.sql("""
    SELECT timestamp, region, continent, currency, usd_rate, value
    FROM challenge_enriched_events
    ORDER BY timestamp DESC
    LIMIT 15
""").show(truncate=False)

print("=== Events by Continent ===")
spark.sql("""
    SELECT continent, COUNT(*) AS event_count
    FROM challenge_enriched_events
    GROUP BY continent
    ORDER BY event_count DESC
""").show()

query_ch19.stop()
print("Stream stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Challenge 20: Complete Pipeline -- Bronze to Gold
# MAGIC
# MAGIC **Scenario:** Build a mini medallion pipeline within a single notebook:
# MAGIC Bronze (raw ingestion) -> Silver (cleansed) -> Gold (aggregated).
# MAGIC
# MAGIC **Task:**
# MAGIC 1. **Bronze:** Write raw sales data as-is to a Delta table
# MAGIC 2. **Silver:** Read Bronze, apply cleansing (fill nulls, filter invalid rows,
# MAGIC    add computed columns), write to Silver Delta table
# MAGIC 3. **Gold:** Read Silver, aggregate by category and region, write to Gold Delta table
# MAGIC 4. Verify each layer has the expected data

# COMMAND ----------

# YOUR CODE HERE


# COMMAND ----------

# SOLUTION

# --- Raw data with some quality issues ---
raw_data = [
    (1, "2024-01-15", "Electronics", "Laptop", 1200.0, 2, "US-East"),
    (2, "2024-01-15", "Electronics", "Mouse", 25.0, 5, "US-West"),
    (3, "2024-01-16", "Accessories", "Cable", 10.0, -1, "EU-North"),   # negative qty
    (4, "2024-01-16", "Electronics", "Monitor", 350.0, 1, None),        # null region
    (5, "2024-01-17", "Audio", "Headphones", 150.0, 3, "APAC"),
    (6, "2024-01-17", "Electronics", "Phone", 900.0, 0, "US-East"),     # zero qty
    (7, "2024-01-18", "Accessories", "Keyboard", 75.0, 4, "EU-South"),
    (8, "2024-01-18", "Electronics", "Tablet", 500.0, 2, "US-West"),
    (9, "2024-01-19", None, "Unknown", 0.0, 1, "APAC"),                 # null category, zero price
    (10, "2024-01-19", "Audio", "Headphones", 150.0, 6, "US-East"),
]

df_raw = spark.createDataFrame(
    raw_data,
    ["txn_id", "txn_date", "category", "product", "price", "quantity", "region"]
).withColumn("txn_date", to_date(col("txn_date")))

# --- BRONZE: Raw ingestion ---
df_raw.write.format("delta").mode("overwrite").saveAsTable("challenge_bronze_sales")
bronze_count = spark.table("challenge_bronze_sales").count()
print(f"BRONZE: {bronze_count} rows (raw, no transformations)")

# --- SILVER: Cleansed and enriched ---
df_silver = (
    spark.table("challenge_bronze_sales")
    # Data quality filters
    .filter(col("category").isNotNull())         # Remove null categories
    .filter(col("price") > 0)                    # Remove zero/negative prices
    .filter(col("quantity") > 0)                 # Remove zero/negative quantities
    # Cleansing
    .withColumn("region", coalesce(col("region"), lit("Unknown")))
    # Enrichment
    .withColumn("revenue", spark_round(col("price") * col("quantity"), 2))
    .withColumn("processed_at", current_timestamp())
)

df_silver.write.format("delta").mode("overwrite").saveAsTable("challenge_silver_sales")
silver_count = spark.table("challenge_silver_sales").count()
print(f"SILVER: {silver_count} rows (cleansed, {bronze_count - silver_count} rows removed)")

# --- GOLD: Aggregated ---
df_gold = (
    spark.table("challenge_silver_sales")
    .groupBy("category", "region")
    .agg(
        count("*").alias("num_transactions"),
        spark_round(spark_sum("revenue"), 2).alias("total_revenue"),
        spark_round(avg("revenue"), 2).alias("avg_revenue"),
        spark_max("revenue").alias("max_revenue")
    )
    .withColumn("refreshed_at", current_timestamp())
)

df_gold.write.format("delta").mode("overwrite").saveAsTable("challenge_gold_revenue")
gold_count = spark.table("challenge_gold_revenue").count()
print(f"GOLD:   {gold_count} aggregate rows")

# --- Verification ---
print("\n=== BRONZE (Raw) ===")
spark.table("challenge_bronze_sales").show(truncate=False)

print("=== SILVER (Cleansed) ===")
spark.table("challenge_silver_sales").select(
    "txn_id", "txn_date", "category", "product", "price", "quantity", "region", "revenue"
).show(truncate=False)

print("=== GOLD (Aggregated) ===")
spark.table("challenge_gold_revenue").select(
    "category", "region", "num_transactions", "total_revenue", "avg_revenue"
).orderBy(col("total_revenue").desc()).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up all tables, views, and temporary resources
tables_to_drop = [
    "challenge_customer_dim",
    "challenge_sales_delta",
    "challenge_product_prices",
    "challenge_bronze_sales",
    "challenge_silver_sales",
    "challenge_gold_revenue",
]
views_to_drop = [
    "challenge_sales",
    "challenge_orders",
    "challenge_regions",
    "challenge_customer_updates",
]

for table in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

for view in views_to_drop:
    spark.sql(f"DROP VIEW IF EXISTS {view}")

# Clean up checkpoint directory
dbutils.fs.rm("/tmp/challenge_product_prices_ckpt", recurse=True)

print("Cleanup complete. All challenge tables, views, and checkpoints removed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC **Congratulations!** You have completed 20 advanced code challenges covering all
# MAGIC Professional exam domains.
# MAGIC
# MAGIC **Self-Assessment:**
# MAGIC - Could you write each solution without looking at the answer? Mark challenges to revisit.
# MAGIC - Did you understand WHY each approach was used (not just HOW)?
# MAGIC - Focus on challenges 1-2 (joins), 5 (SCD Type 2), 6-7 (streaming), and 15 (testing)
# MAGIC   as these represent the highest-weight exam topics.
# MAGIC
# MAGIC **Next steps:**
# MAGIC - Review the Study Plan and Tips (Topic 05) for a structured exam preparation schedule.
# MAGIC - Retake the practice questions after completing your study plan.
