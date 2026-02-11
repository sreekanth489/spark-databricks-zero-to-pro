# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 -- Higher-Order Functions
# MAGIC
# MAGIC **Module 04 | Topic 06 | Level: Intermediate | Time: 35 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Use TRANSFORM, FILTER, AGGREGATE on array columns
# MAGIC - Apply EXISTS and FORALL for boolean checks
# MAGIC - Combine arrays with ZIP_WITH
# MAGIC - Sort arrays with custom comparators
# MAGIC - See why higher-order functions are faster than UDFs (run in Catalyst, no serialization)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: E-Commerce Order Data with Arrays

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, expr, transform, filter, aggregate, exists, forall,
    zip_with, array_sort, concat, lit, size, length, when,
    round as _round
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
)

spark = SparkSession.builder.getOrCreate()

orders_data = [
    ("ORD001", "C001", ["Laptop", "Mouse", "Keyboard"],     [1200.0, 25.0, 75.0],      ["electronics", "accessories"]),
    ("ORD002", "C002", ["Phone", "Case"],                    [800.0, 15.0],              ["electronics", "accessories"]),
    ("ORD003", "C003", ["Book: Spark", "Book: Python"],      [45.0, 35.0],               ["books", "education"]),
    ("ORD004", "C001", ["Monitor", "HDMI Cable"],            [350.0, 12.0],              ["electronics", "cables"]),
    ("ORD005", "C004", ["Sneakers", "Socks", "T-Shirt"],    [120.0, 8.0, 25.0],         ["clothing", "footwear"]),
    ("ORD006", "C002", ["Headphones"],                       [150.0],                    ["electronics", "audio"]),
    ("ORD007", "C005", ["Camera", "Tripod", "SD Card", "Bag"], [700.0, 45.0, 20.0, 30.0], ["electronics", "photography"]),
    ("ORD008", "C003", ["Jacket", "Boots"],                  [90.0, 180.0],              ["clothing", "footwear"]),
]

orders_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("items", ArrayType(StringType()), False),
    StructField("prices", ArrayType(DoubleType()), False),
    StructField("tags", ArrayType(StringType()), False),
])

orders_df = spark.createDataFrame(data=orders_data, schema=orders_schema)
orders_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- TRANSFORM: Apply a Function to Every Element
# MAGIC
# MAGIC Apply a 10% discount to every price in the array.
# MAGIC This runs natively in Catalyst -- no Pickle, no Arrow, no UDF overhead.

# COMMAND ----------

# Apply 10% discount to all prices
discounted_df = orders_df.select(
    "order_id",
    "items",
    "prices",
    transform("prices", lambda p: _round(p * 0.9, 2)).alias("discounted_prices"),
)
discounted_df.show(truncate=False)

# Transform items to uppercase
upper_items_df = orders_df.select(
    "order_id",
    transform("items", lambda item: upper(item)).alias("upper_items"),
)
upper_items_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- FILTER: Select Elements Matching a Condition
# MAGIC
# MAGIC Extract only the prices above $50 from each order.

# COMMAND ----------

# Filter prices above $50
expensive_df = orders_df.select(
    "order_id",
    "prices",
    filter("prices", lambda p: p > 50.0).alias("prices_above_50"),
)
expensive_df.show(truncate=False)

# Filter items that contain "Book"
book_items_df = orders_df.select(
    "order_id",
    "items",
    filter("items", lambda item: item.contains("Book")).alias("book_items"),
)
book_items_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- AGGREGATE (REDUCE): Fold Array Into Single Value
# MAGIC
# MAGIC Compute the sum and product of prices within each array.

# COMMAND ----------

# Sum all prices in the array
totals_df = orders_df.select(
    "order_id",
    "prices",
    aggregate(
        "prices",
        lit(0.0).cast(DoubleType()),
        lambda acc, x: acc + x
    ).alias("order_total"),
)
totals_df.show(truncate=False)

# Average price per item (sum / count)
avg_df = orders_df.select(
    "order_id",
    "prices",
    _round(
        aggregate("prices", lit(0.0).cast(DoubleType()), lambda acc, x: acc + x)
        / size("prices"),
        2
    ).alias("avg_price_per_item"),
)
avg_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- AGGREGATE with Finish Function
# MAGIC
# MAGIC The optional fourth argument is a "finish" function applied to the
# MAGIC final accumulator value.

# COMMAND ----------

# Compute average using the finish function
avg_finish_df = orders_df.select(
    "order_id",
    "prices",
    _round(
        aggregate(
            "prices",
            lit(0.0).cast(DoubleType()),        # initial value
            lambda acc, x: acc + x,              # merge
            lambda acc: acc / size(col("prices"))  # finish: divide by count
        ),
        2
    ).alias("avg_price"),
)
avg_finish_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- EXISTS and FORALL: Boolean Checks
# MAGIC
# MAGIC - `EXISTS`: true if ANY element matches the condition
# MAGIC - `FORALL`: true if ALL elements match the condition

# COMMAND ----------

# EXISTS: does the order contain any item priced over $500?
exists_df = orders_df.select(
    "order_id",
    "prices",
    exists("prices", lambda p: p > 500.0).alias("has_expensive_item"),
)
print("EXISTS -- has any item over $500:")
exists_df.show(truncate=False)

# FORALL: are ALL items in the order under $200?
forall_df = orders_df.select(
    "order_id",
    "prices",
    forall("prices", lambda p: p < 200.0).alias("all_under_200"),
)
print("FORALL -- all items under $200:")
forall_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- ZIP_WITH: Combine Two Arrays Element-Wise
# MAGIC
# MAGIC Merge the items and prices arrays into descriptive strings.

# COMMAND ----------

zipped_df = orders_df.select(
    "order_id",
    zip_with(
        "items",
        "prices",
        lambda item, price: concat(item, lit(": $"), price.cast("string"))
    ).alias("item_price_labels"),
)
zipped_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- array_sort with Custom Comparator
# MAGIC
# MAGIC Sort items by string length (shortest name first).

# COMMAND ----------

sorted_df = orders_df.select(
    "order_id",
    "items",
    array_sort(
        col("items"),
        lambda a, b: when(length(a) < length(b), lit(-1))
                     .when(length(a) > length(b), lit(1))
                     .otherwise(lit(0))
    ).alias("items_sorted_by_length"),
)
sorted_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- Comparison: Higher-Order vs Explode Approach
# MAGIC
# MAGIC To show why higher-order functions are simpler, compare TRANSFORM
# MAGIC with the explode + re-aggregate approach.

# COMMAND ----------

from pyspark.sql.functions import explode, posexplode, collect_list, struct

# APPROACH 1: Higher-order function (clean, no shuffle)
hof_result = orders_df.select(
    "order_id",
    transform("prices", lambda p: _round(p * 0.9, 2)).alias("discounted"),
)
print("Higher-Order (1 line, no shuffle):")
hof_result.show(3, truncate=False)

# APPROACH 2: Explode + transform + re-aggregate (verbose, causes shuffle)
exploded = orders_df.select(
    "order_id",
    posexplode("prices").alias("pos", "price"),
)
transformed = exploded.withColumn("discounted_price", _round(col("price") * 0.9, 2))
reaggregated = transformed.groupBy("order_id").agg(
    collect_list(struct("pos", "discounted_price")).alias("temp"),
)
print("Explode approach (multiple steps, shuffle required):")
reaggregated.show(3, truncate=False)

print("Clearly, higher-order functions are simpler and faster!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- SQL Syntax for Higher-Order Functions

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- TRANSFORM in SQL
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     prices,
# MAGIC     TRANSFORM(prices, p -> ROUND(p * 0.9, 2)) AS discounted_prices,
# MAGIC     FILTER(prices, p -> p > 100) AS expensive_items,
# MAGIC     AGGREGATE(prices, DOUBLE(0), (acc, p) -> acc + p) AS order_total,
# MAGIC     EXISTS(prices, p -> p > 500) AS has_over_500,
# MAGIC     FORALL(prices, p -> p < 2000) AS all_under_2000
# MAGIC FROM orders;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Chaining Higher-Order Functions
# MAGIC
# MAGIC Apply a discount, then filter for items still over $50, then sum them.

# COMMAND ----------

chained_df = orders_df.select(
    "order_id",
    "prices",
    # Step 1: Apply 20% discount
    transform("prices", lambda p: _round(p * 0.8, 2)).alias("after_discount"),
).select(
    "order_id",
    "prices",
    "after_discount",
    # Step 2: Keep only items still over $50
    filter("after_discount", lambda p: p > 50.0).alias("expensive_after_discount"),
).select(
    "order_id",
    "prices",
    "expensive_after_discount",
    # Step 3: Sum the remaining
    aggregate(
        "expensive_after_discount",
        lit(0.0).cast(DoubleType()),
        lambda acc, x: acc + x
    ).alias("filtered_total"),
)
chained_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 -- Cleanup

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS orders")
print("Cleanup complete.")
