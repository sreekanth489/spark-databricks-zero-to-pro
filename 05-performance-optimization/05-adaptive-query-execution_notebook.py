# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 - Adaptive Query Execution (AQE)
# MAGIC > Module 05 — Topic 05 | Runtime re-optimization with real data statistics
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. AQE dynamically coalesces shuffle partitions (fewer tasks, right-sized)
# MAGIC 2. AQE dynamically switches join strategies (sort-merge -> broadcast)
# MAGIC 3. AQE dynamically handles skew joins (splits large partitions)
# MAGIC 4. Comparing plans and performance with AQE on vs off

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]

num_rows = 2_000_000
data = [
    (i, f"Movie_{i}", studios[i % len(studios)], genres[i % len(genres)],
     random.randint(1970, 2024), round(random.uniform(1.0, 10.0), 1),
     random.randint(1_000_000, 500_000_000))
    for i in range(num_rows)
]

schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("title", StringType(), False),
    StructField("studio", StringType(), False),
    StructField("genre", StringType(), False),
    StructField("release_year", IntegerType(), False),
    StructField("rating", DoubleType(), False),
    StructField("revenue", IntegerType(), False),
])

movies_df = spark.createDataFrame(data, schema=schema)

# Write to Delta for realistic file scans
delta_path = "/tmp/perf_module/aqe_movies"
movies_df.write.format("delta").mode("overwrite").save(delta_path)
movies_delta = spark.read.format("delta").load(delta_path)
print(f"Created {num_rows:,} rows in Delta table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dynamic Partition Coalescing
# MAGIC
# MAGIC With 200 shuffle partitions (default), small datasets get 200 tiny partitions.
# MAGIC AQE detects this and merges them into fewer, right-sized partitions.

# COMMAND ----------

# MAGIC %md
# MAGIC ### AQE OFF: 200 shuffle partitions remain after groupBy

# COMMAND ----------

# Disable AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Run a groupBy -- produces 200 post-shuffle partitions
agg_no_aqe = movies_delta.groupBy("studio").agg(F.sum("revenue").alias("total_rev"))

start = time.time()
result_no_aqe = agg_no_aqe.collect()
time_no_aqe = time.time() - start

# Check partition count of the output
print(f"AQE OFF: {agg_no_aqe.rdd.getNumPartitions()} output partitions, time={time_no_aqe:.2f}s")
print(f"Result has {len(result_no_aqe)} rows -- 200 partitions for 8 rows is wasteful!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### AQE ON: Partitions automatically coalesced

# COMMAND ----------

# Enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

agg_with_aqe = movies_delta.groupBy("studio").agg(F.sum("revenue").alias("total_rev"))

start = time.time()
result_with_aqe = agg_with_aqe.collect()
time_with_aqe = time.time() - start

print(f"AQE ON:  partitions were coalesced at runtime, time={time_with_aqe:.2f}s")
print(f"AQE reduced scheduling overhead by merging tiny post-shuffle partitions")

# Show the plan -- look for "AdaptiveSparkPlan" and "CustomShuffleReader" (coalescing)
agg_with_aqe.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dynamic Join Strategy Switching
# MAGIC
# MAGIC AQE can switch from SortMergeJoin to BroadcastHashJoin at runtime
# MAGIC if it discovers one side is smaller than the broadcast threshold.

# COMMAND ----------

# Create a filtered table -- compile-time size estimate may be too high
# but actual post-filter size is small enough to broadcast
filtered_movies = movies_delta.filter(
    (F.col("studio") == "Disney") & (F.col("release_year") == 2020)
)

# Create another table to join with
ratings_data = [(i, round(random.uniform(1.0, 5.0), 1)) for i in range(num_rows)]
ratings_schema = StructType([
    StructField("movie_id", IntegerType(), False),
    StructField("critic_rating", DoubleType(), False),
])
ratings_df = spark.createDataFrame(ratings_data, schema=ratings_schema)

# AQE ON: after the filter executes, AQE sees the filtered table is tiny
# and may switch to BroadcastHashJoin
spark.conf.set("spark.sql.adaptive.enabled", "true")

joined = filtered_movies.join(ratings_df, on="movie_id", how="inner")

print("=== Join Plan with AQE (look for AdaptiveSparkPlan) ===")
joined.explain("formatted")

start = time.time()
joined.count()
time_aqe_join = time.time() - start
print(f"\nJoin with AQE: {time_aqe_join:.2f}s")

# COMMAND ----------

# Compare: AQE OFF for the same join
spark.conf.set("spark.sql.adaptive.enabled", "false")

joined_no_aqe = filtered_movies.join(ratings_df, on="movie_id", how="inner")

print("=== Join Plan WITHOUT AQE ===")
joined_no_aqe.explain("formatted")

start = time.time()
joined_no_aqe.count()
time_no_aqe_join = time.time() - start
print(f"\nJoin without AQE: {time_no_aqe_join:.2f}s")

# Re-enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dynamic Skew Join Optimization
# MAGIC
# MAGIC When one partition is much larger than others, AQE splits it into
# MAGIC smaller sub-partitions and replicates the other side.

# COMMAND ----------

# Create a skewed dataset: 80% of records have studio = "MegaStudio"
skewed_movies = movies_delta.withColumn(
    "skewed_key",
    F.when(F.rand() < 0.8, F.lit("MegaStudio")).otherwise(F.col("studio"))
)

# Create a dimension table to join with
dim_data = [("MegaStudio", "Mega"), ("Warner Bros", "WB"), ("Disney", "DIS"),
            ("Universal", "UNI"), ("Paramount", "PAR"), ("Sony", "SON"),
            ("Lionsgate", "LG"), ("MGM", "MG"), ("Fox", "FX")]
dim_df = spark.createDataFrame(dim_data, ["skewed_key", "abbreviation"])

# AQE with skew join enabled
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

skewed_join = skewed_movies.join(dim_df, on="skewed_key", how="inner")

print("=== Skewed Join Plan with AQE ===")
skewed_join.explain("formatted")

start = time.time()
skewed_join.count()
time_skew_aqe = time.time() - start
print(f"\nSkewed join with AQE: {time_skew_aqe:.2f}s")

# COMMAND ----------

# Compare: Skewed join WITHOUT AQE
spark.conf.set("spark.sql.adaptive.enabled", "false")

skewed_join_no_aqe = skewed_movies.join(dim_df, on="skewed_key", how="inner")

start = time.time()
skewed_join_no_aqe.count()
time_skew_no_aqe = time.time() - start
print(f"Skewed join without AQE: {time_skew_no_aqe:.2f}s")

# Re-enable AQE
spark.conf.set("spark.sql.adaptive.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. AQE Configuration Parameters

# COMMAND ----------

# Show all AQE-related configuration
aqe_configs = {
    "spark.sql.adaptive.enabled": spark.conf.get("spark.sql.adaptive.enabled"),
    "spark.sql.adaptive.coalescePartitions.enabled": spark.conf.get("spark.sql.adaptive.coalescePartitions.enabled", "true"),
    "spark.sql.adaptive.skewJoin.enabled": spark.conf.get("spark.sql.adaptive.skewJoin.enabled", "true"),
    "spark.sql.adaptive.advisoryPartitionSizeInBytes": spark.conf.get("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64m"),
    "spark.sql.shuffle.partitions": spark.conf.get("spark.sql.shuffle.partitions"),
    "spark.sql.autoBroadcastJoinThreshold": spark.conf.get("spark.sql.autoBroadcastJoinThreshold"),
}

print("AQE Configuration:")
for key, value in aqe_configs.items():
    print(f"  {key} = {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. The Full Pipeline
# MAGIC
# MAGIC ```
# MAGIC SQL Query / DataFrame API
# MAGIC     |
# MAGIC     v
# MAGIC Catalyst Optimizer (compile-time)
# MAGIC     |
# MAGIC     v
# MAGIC Optimized Plan
# MAGIC     |
# MAGIC     v
# MAGIC Cost Model + AQE (runtime re-optimization at each shuffle boundary)
# MAGIC     |
# MAGIC     v
# MAGIC Best Physical Plan (continuously refined)
# MAGIC     |
# MAGIC     v
# MAGIC Cluster Execution
# MAGIC ```
# MAGIC
# MAGIC AQE sits between the optimizer and execution. After each shuffle stage,
# MAGIC it uses real data statistics to refine the remaining plan.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | AQE Feature | What It Does | When It Helps |
# MAGIC |-------------|-------------|---------------|
# MAGIC | Partition Coalescing | Merges tiny post-shuffle partitions | Over-partitioned data |
# MAGIC | Join Switching | SortMerge -> Broadcast at runtime | Filtered tables become small |
# MAGIC | Skew Handling | Splits large partitions | One key dominates |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")
dbutils.fs.rm("/tmp/perf_module/aqe_movies", recurse=True)
print("Cleanup complete.")
