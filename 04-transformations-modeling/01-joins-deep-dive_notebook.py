# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 -- Joins Deep Dive
# MAGIC
# MAGIC **Module 04 | Topic 01 | Level: Intermediate | Time: 50 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Create sample movies and studios datasets
# MAGIC - Practice all seven join types (inner, left, right, full, cross, left_semi, left_anti)
# MAGIC - Inspect physical plans with `explain()`
# MAGIC - Use broadcast hints and repartitioning for performance
# MAGIC - Handle skewed join keys with salting

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Create Sample Data
# MAGIC
# MAGIC We use a movies/studios dataset throughout this notebook.

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    broadcast, col, lit, concat, floor, rand, count, avg, explain_string
)
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, LongType
)

spark = SparkSession.builder.getOrCreate()

# Movies dataset
movies_data = [
    (1, "Avengers: Endgame", 1, 2797.0, 2019),
    (2, "The Dark Knight", 2, 1005.0, 2008),
    (3, "Frozen II", 3, 1450.0, 2019),
    (4, "Inception", 2, 836.0, 2010),
    (5, "Iron Man", 1, 585.0, 2008),
    (6, "Coco", 4, 807.0, 2017),
    (7, "Tenet", 2, 363.0, 2020),
    (8, "Moana", 3, 643.0, 2016),
    (9, "Unknown Indie Film", 99, 2.0, 2021),   # studio_id 99 does NOT exist
    (10, "Mystery Movie", 100, 0.5, 2022),       # studio_id 100 does NOT exist
]

movies_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("studio_id", IntegerType(), False),
    StructField("revenue_millions", DoubleType(), True),
    StructField("release_year", IntegerType(), True),
])

movies_df = spark.createDataFrame(data=movies_data, schema=movies_schema)

# Studios dataset
studios_data = [
    (1, "Marvel Studios", "USA"),
    (2, "Warner Bros", "USA"),
    (3, "Walt Disney Animation", "USA"),
    (4, "Pixar", "USA"),
    (5, "Lionsgate", "USA"),    # No movies reference studio_id 5
]

studios_schema = StructType([
    StructField("studio_id", IntegerType(), False),
    StructField("studio_name", StringType(), False),
    StructField("country", StringType(), True),
])

studios_df = spark.createDataFrame(data=studios_data, schema=studios_schema)

print("Movies:")
movies_df.show(truncate=False)
print("Studios:")
studios_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- Inner Join
# MAGIC
# MAGIC Returns only rows where the join key matches in **both** tables.
# MAGIC Movies with studio_id 99 and 100 will be excluded (no matching studio).
# MAGIC Studio "Lionsgate" (id=5) will also be excluded (no matching movie).

# COMMAND ----------

inner_df = movies_df.join(
    other=studios_df,
    on="studio_id",
    how="inner"
)
inner_df.show(truncate=False)
print(f"Inner join row count: {inner_df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- Left, Right, and Full Outer Joins
# MAGIC
# MAGIC - **Left**: all movies, studios filled with nulls when no match
# MAGIC - **Right**: all studios, movies filled with nulls when no match
# MAGIC - **Full Outer**: all rows from both sides, nulls where no match

# COMMAND ----------

# Left join -- keeps all movies, even those without a matching studio
left_df = movies_df.join(studios_df, on="studio_id", how="left")
print("LEFT JOIN -- all movies preserved:")
left_df.show(truncate=False)

# Right join -- keeps all studios, even those without movies
right_df = movies_df.join(studios_df, on="studio_id", how="right")
print("RIGHT JOIN -- all studios preserved:")
right_df.show(truncate=False)

# Full outer join -- keeps everything
full_df = movies_df.join(studios_df, on="studio_id", how="full")
print("FULL OUTER JOIN -- all rows from both sides:")
full_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- Cross Join (Cartesian Product)
# MAGIC
# MAGIC Every row in movies paired with every row in studios.
# MAGIC **Warning**: result size = rows_A x rows_B. Use with extreme caution on large data.

# COMMAND ----------

cross_df = movies_df.crossJoin(studios_df)
print(f"Cross join produces {movies_df.count()} x {studios_df.count()} = {cross_df.count()} rows")
cross_df.select("title", "studio_name").show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- left_semi and left_anti Joins
# MAGIC
# MAGIC These are filtering joins -- they never add columns from the right table.
# MAGIC
# MAGIC - **left_semi** = "list known movies" (those that HAVE a matching studio)
# MAGIC - **left_anti** = "list unknown movies" (those that have NO matching studio)

# COMMAND ----------

# left_semi: keep movies WHERE a matching studio EXISTS
# Think: "List known customers" -- only return customers found in our CRM
known_movies = movies_df.join(studios_df, on="studio_id", how="left_semi")
print("LEFT SEMI -- known movies (have a studio):")
known_movies.show(truncate=False)

# left_anti: keep movies WHERE NO matching studio exists
# Think: "List unknown customers" -- return customers NOT found in our CRM
unknown_movies = movies_df.join(studios_df, on="studio_id", how="left_anti")
print("LEFT ANTI -- unknown movies (no matching studio):")
unknown_movies.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Inspect Join Strategies with explain()
# MAGIC
# MAGIC Spark picks a join strategy based on table sizes.
# MAGIC Use `explain(True)` to see the full plan: parsed, analyzed, optimized, physical.
# MAGIC
# MAGIC - **BroadcastHashJoin**: small table broadcast to all executors
# MAGIC - **SortMergeJoin**: both sides sorted by key, then merged
# MAGIC
# MAGIC Note: DataFrames are **immutable**. Each join creates a NEW DataFrame.

# COMMAND ----------

# Default join -- Spark will likely choose BroadcastHashJoin for this small data
print("=== Default join plan ===")
default_plan_df = movies_df.join(studios_df, on="studio_id", how="inner")
default_plan_df.explain(True)

# Force broadcast explicitly
print("\n=== Explicit broadcast join plan ===")
broadcast_df = movies_df.join(
    broadcast(studios_df),
    on="studio_id",
    how="inner"
)
broadcast_df.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Multi-Column Join
# MAGIC
# MAGIC Join on more than one column by passing a list of column names or a
# MAGIC boolean expression.

# COMMAND ----------

# Create datasets with composite keys
orders_data = [
    (1, "2024-01-15", "EAST", 100.0),
    (2, "2024-01-15", "WEST", 200.0),
    (3, "2024-01-16", "EAST", 150.0),
]
orders_df = spark.createDataFrame(
    data=orders_data,
    schema=["order_id", "order_date", "region", "amount"]
)

targets_data = [
    ("2024-01-15", "EAST", 90.0),
    ("2024-01-15", "WEST", 180.0),
    ("2024-01-16", "WEST", 220.0),
]
targets_df = spark.createDataFrame(
    data=targets_data,
    schema=["target_date", "target_region", "target_amount"]
)

# Multi-column join using a boolean expression
multi_join_df = orders_df.join(
    targets_df,
    on=(
        (orders_df.order_date == targets_df.target_date) &
        (orders_df.region == targets_df.target_region)
    ),
    how="inner"
)
multi_join_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- Non-Equi Join (Range Join)
# MAGIC
# MAGIC Join on inequality conditions like `>=` or `BETWEEN`.
# MAGIC These are more expensive because Spark cannot use hash-based strategies.

# COMMAND ----------

# Find movies whose revenue falls within a revenue tier range
tiers_data = [
    ("Blockbuster", 1000.0, 99999.0),
    ("Hit", 500.0, 999.9),
    ("Moderate", 100.0, 499.9),
    ("Indie", 0.0, 99.9),
]
tiers_df = spark.createDataFrame(
    data=tiers_data,
    schema=["tier", "min_revenue", "max_revenue"]
)

range_join_df = movies_df.join(
    tiers_df,
    on=(
        (movies_df.revenue_millions >= tiers_df.min_revenue) &
        (movies_df.revenue_millions <= tiers_df.max_revenue)
    ),
    how="inner"
)
range_join_df.select("title", "revenue_millions", "tier").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- Repartitioning by Join Key
# MAGIC
# MAGIC `df.repartition(6, "studio")` creates 6 partitions and ensures that
# MAGIC records from the same studio are available only in a single partition.
# MAGIC When both sides share the same partitioning, Spark can skip the shuffle.

# COMMAND ----------

# Repartition both sides by the join key
movies_repartitioned = movies_df.repartition(4, "studio_id")
studios_repartitioned = studios_df.repartition(4, "studio_id")

print(f"Movies partitions after repartition: {movies_repartitioned.rdd.getNumPartitions()}")
print(f"Studios partitions after repartition: {studios_repartitioned.rdd.getNumPartitions()}")

# Join on pre-partitioned data
repartitioned_join = movies_repartitioned.join(
    studios_repartitioned,
    on="studio_id",
    how="inner"
)
print("\nPlan for pre-partitioned join:")
repartitioned_join.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- Handling Skewed Join Keys (Salting)
# MAGIC
# MAGIC When one key (e.g., studio_id=1 for Marvel) has far more rows than
# MAGIC others, that partition becomes a bottleneck. Salting spreads the hot
# MAGIC key across multiple partitions.

# COMMAND ----------

from pyspark.sql.functions import explode, array, concat_ws

# Simulate skew: create 1000 Marvel movies but only 10 for others
skewed_data = [(i, "Marvel Movie " + str(i), 1) for i in range(1, 1001)]
skewed_data += [(i + 1000, "Warner Movie " + str(i), 2) for i in range(1, 11)]
skewed_df = spark.createDataFrame(data=skewed_data, schema=["id", "title", "studio_id"])

print(f"Distribution of studio_id (skewed):")
skewed_df.groupBy("studio_id").count().show()

# Salting technique
num_salt_buckets = 10

# Step 1: Add random salt to the skewed (left) side
salted_movies = skewed_df.withColumn(
    "salt", (rand() * num_salt_buckets).cast(IntegerType())
).withColumn(
    "salted_key", concat_ws("_", col("studio_id").cast("string"), col("salt").cast("string"))
)

# Step 2: Explode the smaller (right) side to match all salt values
salt_values = spark.range(num_salt_buckets).withColumnRenamed("id", "salt")
salted_studios = studios_df.crossJoin(salt_values).withColumn(
    "salted_key", concat_ws("_", col("studio_id").cast("string"), col("salt").cast("string"))
)

# Step 3: Join on salted key
salted_join = salted_movies.join(
    salted_studios,
    on="salted_key",
    how="inner"
)
print(f"Salted join result count: {salted_join.count()}")
salted_join.select("title", "studio_name").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Temporary Views for SQL Joins
# MAGIC
# MAGIC You can run SQL queries on top of a DataFrame using `createOrReplaceTempView`.

# COMMAND ----------

movies_df.createOrReplaceTempView("movies")
studios_df.createOrReplaceTempView("studios")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL left_semi equivalent using WHERE EXISTS
# MAGIC SELECT m.*
# MAGIC FROM movies m
# MAGIC WHERE EXISTS (
# MAGIC     SELECT 1 FROM studios s WHERE s.studio_id = m.studio_id
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SQL left_anti equivalent using WHERE NOT EXISTS
# MAGIC SELECT m.*
# MAGIC FROM movies m
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM studios s WHERE s.studio_id = m.studio_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12 -- Cleanup

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS movies")
spark.sql("DROP VIEW IF EXISTS studios")
print("Cleanup complete.")
