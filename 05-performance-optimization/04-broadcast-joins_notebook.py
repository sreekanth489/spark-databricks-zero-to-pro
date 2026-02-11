# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 - Broadcast Joins
# MAGIC > Module 05 — Topic 04 | Eliminate shuffles for small-large table joins
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. Sort-merge join vs broadcast join -- plan comparison and performance
# MAGIC 2. Auto-broadcast threshold and the `broadcast()` hint
# MAGIC 3. Reading join strategies in execution plans
# MAGIC 4. Broadcast variables for non-join lookups

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create Large and Small Tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

# Large table: 2 million movies
studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]

num_movies = 2_000_000
movies_data = [
    (i, f"Movie_{i}", studios[i % len(studios)], genres[i % len(genres)],
     random.randint(1970, 2024), round(random.uniform(1.0, 10.0), 1),
     random.randint(1_000_000, 500_000_000))
    for i in range(num_movies)
]

movies_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("studio", StringType(), False),
    StructField("genre", StringType(), False),
    StructField("release_year", IntegerType(), False),
    StructField("rating", DoubleType(), False),
    StructField("revenue", IntegerType(), False),
])

movies_df = spark.createDataFrame(movies_data, schema=movies_schema)
movies_df.cache()
movies_df.count()

# Small table: studio details (8 rows -- tiny!)
studio_details = [
    ("Warner Bros", "North America", "AT&T", 1923),
    ("Disney", "North America", "Disney Corp", 1923),
    ("Universal", "North America", "Comcast", 1912),
    ("Paramount", "North America", "Paramount Global", 1912),
    ("Sony", "Asia", "Sony Group", 1987),
    ("Lionsgate", "North America", "Lionsgate", 1997),
    ("MGM", "North America", "Amazon", 1924),
    ("Fox", "North America", "Disney Corp", 1935),
]

studio_schema = StructType([
    StructField("studio", StringType(), False),
    StructField("region", StringType(), False),
    StructField("parent_company", StringType(), False),
    StructField("founded_year", IntegerType(), False),
])

studios_df = spark.createDataFrame(studio_details, schema=studio_schema)

print(f"Large table (movies): {num_movies:,} rows")
print(f"Small table (studios): {len(studio_details)} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Sort-Merge Join (Default for Large Tables)
# MAGIC
# MAGIC Disable auto-broadcast to force a sort-merge join, then compare with broadcast.

# COMMAND ----------

# Disable auto-broadcast to force sort-merge join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

start = time.time()
smj_result = movies_df.join(studios_df, on="studio", how="inner")
smj_result.count()
smj_time = time.time() - start

print(f"Sort-Merge Join: {smj_time:.2f}s")
print("\n=== Sort-Merge Join Plan ===")
smj_result.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the plan shows `SortMergeJoin` with `Exchange` (shuffle) on BOTH sides.
# MAGIC Both the 2M-row movies table and the 8-row studios table are being shuffled.
# MAGIC This is wasteful -- we shuffled 2M rows unnecessarily.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Broadcast Join (with broadcast() Hint)
# MAGIC
# MAGIC Force the small studios table to be broadcast to all executors.

# COMMAND ----------

# Still have auto-broadcast disabled, but using broadcast() hint
start = time.time()
bcj_result = movies_df.join(broadcast(studios_df), on="studio", how="inner")
bcj_result.count()
bcj_time = time.time() - start

print(f"Broadcast Join:   {bcj_time:.2f}s")
print(f"Sort-Merge Join:  {smj_time:.2f}s")
print(f"Speedup: {smj_time / bcj_time:.1f}x")
print("\n=== Broadcast Join Plan ===")
bcj_result.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC Notice the plan now shows `BroadcastHashJoin` with `BroadcastExchange` only
# MAGIC on the studios side. The movies table has NO `Exchange` -- no shuffle!
# MAGIC Only 8 rows are broadcast instead of shuffling 2M rows.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Auto-Broadcast Threshold
# MAGIC
# MAGIC Re-enable auto-broadcast (default 10 MB). Since our studios table is tiny,
# MAGIC Spark will automatically choose a broadcast join.

# COMMAND ----------

# Re-enable auto-broadcast (default 10 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

# Spark automatically detects studios_df < 10 MB and broadcasts it
auto_result = movies_df.join(studios_df, on="studio", how="inner")

print("=== Auto-Broadcast Plan (Spark chooses broadcast automatically) ===")
auto_result.explain()

# Verify it chose BroadcastHashJoin
plan_str = auto_result._jdf.queryExecution().executedPlan().toString()
if "BroadcastHashJoin" in plan_str:
    print("\nSpark automatically chose BroadcastHashJoin!")
else:
    print("\nSpark chose SortMergeJoin (table may exceed threshold)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Comparing All Three Join Strategies in Plans

# COMMAND ----------

# Create a medium-sized table for shuffle-hash demonstration
medium_data = [(i, f"Tag_{i % 100}", random.randint(1, 10)) for i in range(num_movies)]
medium_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("tag", StringType(), False),
    StructField("tag_score", IntegerType(), False),
])
medium_df = spark.createDataFrame(medium_data, schema=medium_schema)
medium_df.cache()
medium_df.count()

# Disable auto-broadcast to see sort-merge join
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

print("=== Large-Large Join Plan (SortMergeJoin expected) ===")
movies_df.join(medium_df, on="movie_id").explain()

# Re-enable auto-broadcast
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")

print("\n=== Large-Small Join Plan (BroadcastHashJoin expected) ===")
movies_df.join(broadcast(studios_df), on="studio").explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Broadcast Variables for Non-Join Lookups
# MAGIC
# MAGIC Beyond joins, broadcast variables let you send a lookup dictionary to all
# MAGIC executors once, instead of including it with every task.

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# Create a lookup: studio -> tier classification
studio_tiers = {
    "Warner Bros": "Tier 1 - Major",
    "Disney": "Tier 1 - Major",
    "Universal": "Tier 1 - Major",
    "Paramount": "Tier 1 - Major",
    "Sony": "Tier 2 - Large",
    "Lionsgate": "Tier 3 - Mid",
    "MGM": "Tier 3 - Mid",
    "Fox": "Tier 2 - Large",
}

# Broadcast the lookup dictionary
bc_tiers = spark.sparkContext.broadcast(studio_tiers)

@udf(StringType())
def classify_studio(studio_name):
    return bc_tiers.value.get(studio_name, "Unknown")

enriched_df = movies_df.withColumn("studio_tier", classify_studio(F.col("studio")))
enriched_df.select("title", "studio", "studio_tier").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. What Happens When You Broadcast a Large Table (Don't!)
# MAGIC
# MAGIC This cell demonstrates the concept -- broadcasting a table that is too large
# MAGIC causes excessive memory usage. In production, this leads to OutOfMemoryError.

# COMMAND ----------

# We will just show the plan (not execute) for a broadcast of the large table
# In production, this would crash with OOM if the table were truly huge
print("=== Broadcasting the LARGE table (BAD PRACTICE) ===")
print("Plan shows BroadcastExchange on the 2M-row table -- wasteful!")
bad_plan = broadcast(movies_df).join(studios_df, on="studio")
bad_plan.explain()

print("\n=== Broadcasting the SMALL table (CORRECT) ===")
good_plan = movies_df.join(broadcast(studios_df), on="studio")
good_plan.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Join Strategy | Shuffle | Best For | Triggered By |
# MAGIC |--------------|---------|----------|--------------|
# MAGIC | BroadcastHashJoin | No (large side) | Small + Large | Auto (< 10 MB) or `broadcast()` |
# MAGIC | SortMergeJoin | Both sides | Large + Large | Default for large tables |
# MAGIC | ShuffleHashJoin | Both sides | Medium tables | Spark optimizer decision |
# MAGIC
# MAGIC **Rule of thumb**: If one table fits in executor memory, broadcast it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

movies_df.unpersist()
medium_df.unpersist()
bc_tiers.destroy()
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
print("Cleanup complete.")
