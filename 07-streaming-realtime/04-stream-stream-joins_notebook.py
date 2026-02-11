# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Stream-Stream Joins -- Hands-On Notebook
# MAGIC > Module 07 -- Topic 04 | Streaming & Real-Time
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Creating two correlated streams (impressions and clicks)
# MAGIC 2. Stream-stream inner join with a shared key
# MAGIC 3. Time-bound join conditions for state management
# MAGIC 4. Stream-static join for enrichment
# MAGIC 5. Monitoring join state size
# MAGIC
# MAGIC **All examples are self-contained** -- no external data required.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, DoubleType, LongType
)
from datetime import datetime, timedelta
import time

BASE_PATH = "/tmp/module07_topic04"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints"

dbutils.fs.rm(BASE_PATH, recurse=True)
dbutils.fs.mkdirs(CHECKPOINT_PATH)

print("Setup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Create Two Correlated Streams
# MAGIC
# MAGIC We simulate an ad-tech scenario with two streams:
# MAGIC - **Impressions**: Ads shown to users
# MAGIC - **Clicks**: User clicks on those ads
# MAGIC
# MAGIC We use rate sources and transform them to create realistic correlated data.

# COMMAND ----------

# Stream A: Ad Impressions
# Every row from rate source becomes an impression with a derived impression_id
impressions_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumnRenamed("timestamp", "impression_time")
    .withColumn("impression_id", F.concat(F.lit("imp_"), F.col("value").cast("string")))
    .withColumn("ad_id", F.concat(F.lit("ad_"), (F.col("value") % 10).cast("string")))
    .withColumn("user_id", F.concat(F.lit("user_"), (F.col("value") % 20).cast("string")))
    .select("impression_id", "ad_id", "user_id", "impression_time")
)

# Stream B: Ad Clicks
# Only ~30% of impressions get clicked, with a slight delay
clicks_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load()
    .withColumn("click_time", F.col("timestamp") + F.expr("INTERVAL 2 SECONDS"))
    .withColumn("impression_id", F.concat(F.lit("imp_"), F.col("value").cast("string")))
    .withColumn("click_id", F.concat(F.lit("clk_"), F.col("value").cast("string")))
    .select("click_id", "impression_id", "click_time")
)

print("Impressions schema:")
impressions_stream.printSchema()
print("Clicks schema:")
clicks_stream.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Stream-Stream Inner Join
# MAGIC
# MAGIC Join impressions with clicks on `impression_id`. Only impressions that
# MAGIC received a click will appear in the output.

# COMMAND ----------

# Inner join: match impressions with their clicks
inner_joined = impressions_stream.join(
    clicks_stream,
    on="impression_id",
    how="inner"
)

query_inner = (
    inner_joined.writeStream
    .format("memory")
    .queryName("inner_join_demo")
    .outputMode("append")
    .start()
)

time.sleep(15)

print("=== Stream-Stream Inner Join Results ===")
spark.sql("""
    SELECT impression_id, ad_id, user_id, impression_time, click_id, click_time
    FROM inner_join_demo
    ORDER BY impression_time DESC
    LIMIT 10
""").show(truncate=False)

total_matches = spark.sql("SELECT COUNT(*) AS cnt FROM inner_join_demo").collect()[0]["cnt"]
print(f"Total matched impression-click pairs: {total_matches}")

query_inner.stop()
print("Inner join query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Inner Join with Time-Bound Condition
# MAGIC
# MAGIC Adding a time constraint limits how long Spark buffers unmatched events.
# MAGIC This is **critical** for production to prevent unbounded state growth.

# COMMAND ----------

# Recreate streams for this demo
impressions_tb = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 5)
    .load()
    .withColumnRenamed("timestamp", "impression_time")
    .withColumn("impression_id", F.concat(F.lit("imp_"), F.col("value").cast("string")))
    .withColumn("ad_id", F.concat(F.lit("ad_"), (F.col("value") % 10).cast("string")))
    .withColumn("user_id", F.concat(F.lit("user_"), (F.col("value") % 20).cast("string")))
    .select("impression_id", "ad_id", "user_id", "impression_time")
)

clicks_tb = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load()
    .withColumn("click_time", F.col("timestamp") + F.expr("INTERVAL 2 SECONDS"))
    .withColumn("impression_id", F.concat(F.lit("imp_"), F.col("value").cast("string")))
    .withColumn("click_id", F.concat(F.lit("clk_"), F.col("value").cast("string")))
    .select("click_id", "impression_id", "click_time")
)

# Inner join WITH time-bound condition
# Click must happen within 5 minutes of the impression
time_bound_joined = impressions_tb.join(
    clicks_tb,
    on=F.expr("""
        impressions_tb.impression_id = clicks_tb.impression_id
        AND clicks_tb.click_time >= impressions_tb.impression_time
        AND clicks_tb.click_time <= impressions_tb.impression_time + INTERVAL 5 MINUTES
    """),
    how="inner"
)

query_tb = (
    time_bound_joined.writeStream
    .format("memory")
    .queryName("time_bound_join")
    .outputMode("append")
    .start()
)

time.sleep(15)

print("=== Time-Bound Inner Join ===")
print("Clicks must occur within 5 minutes of the impression.\n")
spark.sql("""
    SELECT
        impressions_tb.impression_id,
        ad_id,
        impression_time,
        click_id,
        click_time,
        ROUND((unix_timestamp(click_time) - unix_timestamp(impression_time)), 1) AS response_sec
    FROM time_bound_join
    ORDER BY impression_time DESC
    LIMIT 10
""").show(truncate=False)

# Check state information from query progress
progress = query_tb.lastProgress
if progress and progress.get("stateOperators"):
    for op in progress["stateOperators"]:
        print(f"State: numRowsTotal={op.get('numRowsTotal', 'N/A')}, "
              f"numRowsDroppedByWatermark={op.get('numRowsDroppedByWatermark', 'N/A')}")

query_tb.stop()
print("Time-bound join query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Stream-Static Join (Enrichment)
# MAGIC
# MAGIC Enrich a streaming DataFrame with a static dimension table.
# MAGIC No watermark needed -- the static side is loaded once at query start.

# COMMAND ----------

# Create a static dimension table: product catalog
product_data = [
    ("prod_001", "Laptop Pro",     "Electronics", 1299.99),
    ("prod_002", "Wireless Mouse", "Electronics", 29.99),
    ("prod_003", "Coffee Maker",   "Kitchen",     89.50),
    ("prod_004", "Running Shoes",  "Sports",      125.00),
    ("prod_005", "Desk Lamp",      "Office",      45.00),
    ("prod_006", "Headphones",     "Electronics", 199.99),
    ("prod_007", "Yoga Mat",       "Sports",      35.00),
    ("prod_008", "Blender",        "Kitchen",     65.00),
    ("prod_009", "Notebook Set",   "Office",      12.99),
    ("prod_010", "Water Bottle",   "Sports",      22.50),
]

products_schema = StructType([
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("list_price", DoubleType(), False),
])

df_products = spark.createDataFrame(product_data, schema=products_schema)
print("=== Static Dimension Table: Products ===")
df_products.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Execute the Stream-Static Join

# COMMAND ----------

# Create a streaming "orders" source using rate
orders_stream = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load()
    .withColumn("order_id", F.concat(F.lit("ord_"), F.col("value").cast("string")))
    .withColumn("product_id",
        F.concat(F.lit("prod_"), F.lpad((F.col("value") % 10 + 1).cast("string"), 3, "0")))
    .withColumn("quantity", (F.col("value") % 5 + 1).cast("integer"))
    .withColumn("order_time", F.col("timestamp"))
    .select("order_id", "product_id", "quantity", "order_time")
)

# Left join: all orders, enriched with product info (null if product not found)
enriched_orders = orders_stream.join(
    df_products,     # static DataFrame
    on="product_id",
    how="left"
).withColumn("total_price",
    F.round(F.col("quantity") * F.col("list_price"), 2)
)

# Write to memory for inspection
query_static_join = (
    enriched_orders.writeStream
    .format("memory")
    .queryName("enriched_orders")
    .outputMode("append")
    .start()
)

time.sleep(12)

print("=== Stream-Static Join: Enriched Orders ===")
spark.sql("""
    SELECT order_id, product_id, product_name, category,
           quantity, list_price, total_price, order_time
    FROM enriched_orders
    ORDER BY order_time DESC
    LIMIT 15
""").show(truncate=False)

# Summary by category
print("=== Sales by Category ===")
spark.sql("""
    SELECT category, COUNT(*) AS order_count,
           ROUND(SUM(total_price), 2) AS total_revenue
    FROM enriched_orders
    GROUP BY category
    ORDER BY total_revenue DESC
""").show()

query_static_join.stop()
print("Stream-static join query stopped.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Monitoring Join State
# MAGIC
# MAGIC For stream-stream joins, state size is the primary metric to monitor.
# MAGIC State grows with unmatched events buffered on each side.

# COMMAND ----------

# Start a stream-stream join and monitor state
monitor_imp = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 8)
    .load()
    .withColumnRenamed("timestamp", "imp_time")
    .withColumn("imp_id", F.col("value").cast("string"))
    .select("imp_id", "imp_time")
)

monitor_clk = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 4)
    .load()
    .withColumn("clk_time", F.col("timestamp"))
    .withColumn("imp_id", F.col("value").cast("string"))
    .select("imp_id", "clk_time")
)

# Join with time bound
monitor_join = monitor_imp.join(
    monitor_clk,
    on=F.expr("""
        monitor_imp.imp_id = monitor_clk.imp_id
        AND monitor_clk.clk_time >= monitor_imp.imp_time
        AND monitor_clk.clk_time <= monitor_imp.imp_time + INTERVAL 2 MINUTES
    """),
    how="inner"
)

query_monitor = (
    monitor_join.writeStream
    .format("memory")
    .queryName("monitor_join_state")
    .outputMode("append")
    .trigger(processingTime="3 seconds")
    .start()
)

# Monitor state growth
print("=== Join State Monitoring ===")
print(f"{'Batch':>6} | {'State Rows':>11} | {'State Size (bytes)':>18} | {'Matches':>8}")
print("-" * 60)

for i in range(6):
    time.sleep(5)
    progress = query_monitor.lastProgress
    if progress:
        batch_id = progress.get("batchId", "?")
        state_ops = progress.get("stateOperators", [])
        for op in state_ops:
            rows = op.get("numRowsTotal", "N/A")
            memory = op.get("memoryUsedBytes", "N/A")
            print(f"{batch_id:>6} | {str(rows):>11} | {str(memory):>18} | "
                  f"{progress.get('numInputRows', 'N/A'):>8}")

query_monitor.stop()
print("\nState monitoring complete.")
print("In production, alert if state rows exceed a threshold.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Join Type Summary

# COMMAND ----------

join_types = [
    ("stream-stream inner", "No (recommended)", "Highly recommended", "Matched rows from both streams"),
    ("stream-stream left outer", "Yes (both sides)", "Required", "All left + matching right (or nulls)"),
    ("stream-stream right outer", "Yes (both sides)", "Required", "All right + matching left (or nulls)"),
    ("stream-stream full outer", "Yes (both sides)", "Required", "All rows from both sides"),
    ("stream-static inner", "No", "No", "Matched rows (static read once)"),
    ("stream-static left outer", "No", "No", "All stream + matching static (or nulls)"),
]

df_joins = spark.createDataFrame(
    join_types,
    schema=["join_type", "watermark_required", "time_bound_required", "output_description"]
)
df_joins.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: foreachBatch for Refreshing Static Side
# MAGIC
# MAGIC Since stream-static joins load the static side once, you can use
# MAGIC `foreachBatch` to re-read the dimension table periodically.

# COMMAND ----------

# Demonstration of the foreachBatch pattern for refreshing static data

# In production, the dimension table would be a Delta table updated by another pipeline
DIM_TABLE_PATH = f"{BASE_PATH}/dim_products"
df_products.write.format("delta").mode("overwrite").save(DIM_TABLE_PATH)

def enrich_with_fresh_dimensions(batch_df, batch_id):
    """
    Called for each micro-batch. Re-reads the dimension table to pick up
    any updates that occurred since the query started.
    """
    # Fresh read of the dimension table every batch
    fresh_products = spark.read.format("delta").load(DIM_TABLE_PATH)

    # Join this batch with the fresh dimension data
    enriched = batch_df.join(fresh_products, on="product_id", how="left")

    # Write to a Delta table (or any other sink)
    enriched.write.format("delta").mode("append").save(f"{BASE_PATH}/enriched_output")

# Create order stream
orders_for_batch = (
    spark.readStream
    .format("rate")
    .option("rowsPerSecond", 3)
    .load()
    .withColumn("order_id", F.concat(F.lit("ord_"), F.col("value").cast("string")))
    .withColumn("product_id",
        F.concat(F.lit("prod_"), F.lpad((F.col("value") % 10 + 1).cast("string"), 3, "0")))
    .withColumn("quantity", (F.col("value") % 5 + 1).cast("integer"))
    .select("order_id", "product_id", "quantity")
)

query_foreach = (
    orders_for_batch.writeStream
    .foreachBatch(enrich_with_fresh_dimensions)
    .option("checkpointLocation", f"{CHECKPOINT_PATH}/foreach_enrich")
    .trigger(processingTime="5 seconds")
    .start()
)

time.sleep(15)
query_foreach.stop()

# Verify results
enriched_result = spark.read.format("delta").load(f"{BASE_PATH}/enriched_output")
print(f"Enriched rows written via foreachBatch: {enriched_result.count()}")
enriched_result.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

for q in spark.streams.active:
    q.stop()
    print(f"Stopped: {q.name}")

dbutils.fs.rm(BASE_PATH, recurse=True)
print("All temporary data cleaned up.")
print("Notebook complete.")
