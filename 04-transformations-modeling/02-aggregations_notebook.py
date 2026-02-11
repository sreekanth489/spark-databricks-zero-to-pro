# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 -- Aggregations
# MAGIC
# MAGIC **Module 04 | Topic 02 | Level: Intermediate | Time: 40 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Compute groupBy aggregations (count, sum, avg, min, max)
# MAGIC - Perform multiple aggregations in a single pass
# MAGIC - Pivot and unpivot data
# MAGIC - Use rollup and cube for hierarchical summaries
# MAGIC - Compare exact vs approximate distinct counts
# MAGIC - Collect grouped values into arrays

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Create E-Commerce Orders Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as _sum, avg, min as _min, max as _max,
    countDistinct, approx_count_distinct,
    collect_list, collect_set, round as _round,
    expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType
)
from datetime import date

spark = SparkSession.builder.getOrCreate()

orders_data = [
    (1, "C001", "Electronics", "Laptop",     1200.00, date(2024, 1, 10), "East",  "Q1"),
    (2, "C002", "Electronics", "Phone",       800.00, date(2024, 1, 15), "West",  "Q1"),
    (3, "C001", "Books",       "Spark Guide",  45.00, date(2024, 2, 5),  "East",  "Q1"),
    (4, "C003", "Electronics", "Tablet",      450.00, date(2024, 2, 20), "East",  "Q1"),
    (5, "C002", "Clothing",    "Jacket",       90.00, date(2024, 3, 1),  "West",  "Q1"),
    (6, "C004", "Books",       "Python 101",   35.00, date(2024, 4, 10), "South", "Q2"),
    (7, "C001", "Electronics", "Monitor",     350.00, date(2024, 4, 15), "East",  "Q2"),
    (8, "C005", "Clothing",    "Sneakers",    120.00, date(2024, 5, 1),  "West",  "Q2"),
    (9, "C003", "Books",       "SQL Basics",   40.00, date(2024, 5, 20), "East",  "Q2"),
    (10, "C002", "Electronics", "Headphones", 150.00, date(2024, 6, 5),  "West",  "Q2"),
    (11, "C006", "Electronics", "Camera",     700.00, date(2024, 7, 10), "South", "Q3"),
    (12, "C001", "Clothing",    "T-Shirt",     25.00, date(2024, 7, 20), "East",  "Q3"),
    (13, "C004", "Electronics", "Speaker",    200.00, date(2024, 8, 5),  "South", "Q3"),
    (14, "C003", "Books",       "Data Eng",    55.00, date(2024, 9, 1),  "East",  "Q3"),
    (15, "C005", "Clothing",    "Boots",      180.00, date(2024, 10, 1), "West",  "Q4"),
    (16, "C002", "Electronics", "Smartwatch", 300.00, date(2024, 10, 15),"West",  "Q4"),
    (17, "C006", "Books",       "ML Book",     60.00, date(2024, 11, 5), "South", "Q4"),
    (18, "C001", "Electronics", "SSD",        100.00, date(2024, 11, 20),"East",  "Q4"),
    (19, "C003", "Clothing",    "Scarf",       30.00, date(2024, 12, 1), "East",  "Q4"),
    (20, "C004", "Electronics", "Router",     120.00, date(2024, 12, 15),"South", "Q4"),
]

orders_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("category", StringType(), False),
    StructField("product", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("order_date", DateType(), False),
    StructField("region", StringType(), False),
    StructField("quarter", StringType(), False),
])

orders_df = spark.createDataFrame(data=orders_data, schema=orders_schema)
orders_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- Basic groupBy + agg
# MAGIC
# MAGIC Remember: `groupBy` is a **wide transformation** that causes a shuffle.
# MAGIC Due to **lazy evaluation**, this only builds a plan until we call `.show()`.

# COMMAND ----------

category_summary = orders_df.groupBy("category").agg(
    count("order_id").alias("total_orders"),
    _sum("amount").alias("total_revenue"),
    _round(avg("amount"), 2).alias("avg_order_value"),
    _min("amount").alias("min_order"),
    _max("amount").alias("max_order"),
)
category_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- Multiple Grouping Columns
# MAGIC
# MAGIC Group by both region and category for a two-dimensional breakdown.

# COMMAND ----------

region_category = orders_df.groupBy("region", "category").agg(
    count("order_id").alias("order_count"),
    _round(_sum("amount"), 2).alias("revenue"),
)
region_category.orderBy("region", "category").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- Pivot: Rows to Columns
# MAGIC
# MAGIC Convert quarters from row values into column headers.
# MAGIC **Tip**: pass the distinct values list for better performance.

# COMMAND ----------

pivot_df = orders_df.groupBy("region").pivot(
    "quarter",
    values=["Q1", "Q2", "Q3", "Q4"]   # explicit list avoids extra Spark job
).agg(
    _round(_sum("amount"), 2)
)
print("Revenue by region and quarter (pivoted):")
pivot_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- Unpivot: Columns to Rows
# MAGIC
# MAGIC Convert the pivoted wide table back to long format using `stack()`.

# COMMAND ----------

# Using stack() SQL expression to unpivot
unpivoted_df = pivot_df.select(
    "region",
    expr("stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (quarter, revenue)")
).filter(col("revenue").isNotNull())

print("Unpivoted back to long format:")
unpivoted_df.orderBy("region", "quarter").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Rollup: Hierarchical Subtotals
# MAGIC
# MAGIC `rollup("region", "category")` produces:
# MAGIC - Group by (region, category)
# MAGIC - Subtotal by (region)
# MAGIC - Grand total

# COMMAND ----------

rollup_df = orders_df.rollup("region", "category").agg(
    count("order_id").alias("order_count"),
    _round(_sum("amount"), 2).alias("revenue"),
).orderBy("region", "category")

print("Rollup -- subtotals and grand total:")
rollup_df.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Cube: All Combination Subtotals
# MAGIC
# MAGIC `cube("region", "category")` produces subtotals for ALL combinations:
# MAGIC - (region, category), (region, null), (null, category), (null, null)

# COMMAND ----------

cube_df = orders_df.cube("region", "category").agg(
    count("order_id").alias("order_count"),
    _round(_sum("amount"), 2).alias("revenue"),
).orderBy("region", "category")

print("Cube -- all-combination subtotals:")
cube_df.show(30, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- Approximate vs Exact Distinct Counts
# MAGIC
# MAGIC `approx_count_distinct` uses HyperLogLog and is much faster on large data.

# COMMAND ----------

comparison = orders_df.select(
    countDistinct("customer_id").alias("exact_distinct_customers"),
    approx_count_distinct("customer_id", rsd=0.05).alias("approx_distinct_customers"),
    countDistinct("product").alias("exact_distinct_products"),
    approx_count_distinct("product", rsd=0.05).alias("approx_distinct_products"),
)
comparison.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- collect_list and collect_set
# MAGIC
# MAGIC Aggregate values into arrays within each group.
# MAGIC - `collect_list` preserves duplicates
# MAGIC - `collect_set` removes duplicates

# COMMAND ----------

products_by_customer = orders_df.groupBy("customer_id").agg(
    collect_list("product").alias("all_products_ordered"),
    collect_set("category").alias("unique_categories"),
    count("order_id").alias("order_count"),
)
products_by_customer.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- SQL Aggregations via Temporary Views
# MAGIC
# MAGIC You can run SQL queries on top of a DataFrame using `createOrReplaceTempView`.

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Grouping sets: explicit control over which combinations to compute
# MAGIC SELECT region, category,
# MAGIC        COUNT(order_id) AS order_count,
# MAGIC        ROUND(SUM(amount), 2) AS revenue
# MAGIC FROM orders
# MAGIC GROUP BY GROUPING SETS (
# MAGIC     (region, category),
# MAGIC     (region),
# MAGIC     (category),
# MAGIC     ()
# MAGIC )
# MAGIC ORDER BY region, category;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Cleanup

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS orders")
print("Cleanup complete.")
