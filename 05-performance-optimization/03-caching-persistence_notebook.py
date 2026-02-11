# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Caching & Persistence
# MAGIC > Module 05 — Topic 03 | cache() vs persist(), storage levels, Delta Cache
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. Dramatic speed difference with and without caching
# MAGIC 2. cache() vs persist() with different storage levels
# MAGIC 3. Lazy materialization -- cache does nothing until an action runs
# MAGIC 4. Monitoring cache in the Spark UI Storage tab
# MAGIC 5. Proper cleanup with unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create a Large Dataset

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark import StorageLevel
import time
import random

studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]

num_rows = 3_000_000
data = [
    (
        i,
        f"Movie_{i}",
        studios[i % len(studios)],
        genres[i % len(genres)],
        random.randint(1970, 2024),
        round(random.uniform(1.0, 10.0), 1),
        random.randint(1_000_000, 500_000_000),
    )
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

# Write to Delta so we can read it fresh for each test
base_path = "/tmp/perf_module/caching_test"
raw_df = spark.createDataFrame(data, schema=schema)
raw_df.write.format("delta").mode("overwrite").save(base_path)
print(f"Wrote {num_rows:,} rows to {base_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Without Caching: Every Action Recomputes Everything

# COMMAND ----------

# Read from Delta and apply a non-trivial transformation pipeline
uncached_df = (
    spark.read.format("delta").load(base_path)
    .filter(F.col("release_year") >= 2000)
    .withColumn("rating_bucket", (F.col("rating") * 2).cast("int") / 2)
    .groupBy("studio", "rating_bucket")
    .agg(
        F.count("*").alias("movie_count"),
        F.sum("revenue").alias("total_revenue"),
    )
)

# Action 1: count
start = time.time()
uncached_df.count()
t1 = time.time() - start

# Action 2: show
start = time.time()
uncached_df.show(5)
t2 = time.time() - start

# Action 3: collect
start = time.time()
uncached_df.collect()
t3 = time.time() - start

print(f"WITHOUT caching:")
print(f"  Action 1 (count):   {t1:.2f}s")
print(f"  Action 2 (show):    {t2:.2f}s  <- recomputed from scratch")
print(f"  Action 3 (collect): {t3:.2f}s  <- recomputed again")
print(f"  Total:              {t1 + t2 + t3:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. With Caching: First Action Caches, Subsequent Actions Read from Memory

# COMMAND ----------

# Same pipeline but cached
cached_df = (
    spark.read.format("delta").load(base_path)
    .filter(F.col("release_year") >= 2000)
    .withColumn("rating_bucket", (F.col("rating") * 2).cast("int") / 2)
    .groupBy("studio", "rating_bucket")
    .agg(
        F.count("*").alias("movie_count"),
        F.sum("revenue").alias("total_revenue"),
    )
)

# cache() is LAZY -- it just marks the DataFrame for caching
cached_df.cache()
print("cache() called -- but nothing is stored yet (lazy)")

# Action 1: triggers computation AND caching
start = time.time()
cached_df.count()
t1 = time.time() - start

# Action 2: reads from cache
start = time.time()
cached_df.show(5)
t2 = time.time() - start

# Action 3: reads from cache
start = time.time()
cached_df.collect()
t3 = time.time() - start

print(f"\nWITH caching:")
print(f"  Action 1 (count):   {t1:.2f}s  <- computes + caches (slower)")
print(f"  Action 2 (show):    {t2:.2f}s  <- reads from cache (fast!)")
print(f"  Action 3 (collect): {t3:.2f}s  <- reads from cache (fast!)")
print(f"  Total:              {t1 + t2 + t3:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC **Go to Spark UI -> Storage Tab** to see the cached DataFrame, its storage
# MAGIC level (Memory + Disk), size in memory, and fraction cached.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. persist() with Different Storage Levels

# COMMAND ----------

# Unpersist previous cache first
cached_df.unpersist(blocking=True)

# Read fresh
source_df = (
    spark.read.format("delta").load(base_path)
    .filter(F.col("release_year") >= 2000)
    .withColumn("decade", (F.col("release_year") / 10).cast("int") * 10)
)

# MEMORY_ONLY: fast but drops data if it doesn't fit
memory_only_df = source_df.persist(StorageLevel.MEMORY_ONLY)
start = time.time()
memory_only_df.count()  # materialize
t_mem = time.time() - start

# Read from cache
start = time.time()
memory_only_df.filter(F.col("studio") == "Disney").count()
t_mem_read = time.time() - start

print(f"MEMORY_ONLY: materialize={t_mem:.2f}s, cache read={t_mem_read:.2f}s")
memory_only_df.unpersist(blocking=True)

# DISK_ONLY: slower but handles large datasets
disk_only_df = source_df.persist(StorageLevel.DISK_ONLY)
start = time.time()
disk_only_df.count()  # materialize
t_disk = time.time() - start

start = time.time()
disk_only_df.filter(F.col("studio") == "Disney").count()
t_disk_read = time.time() - start

print(f"DISK_ONLY:   materialize={t_disk:.2f}s, cache read={t_disk_read:.2f}s")
disk_only_df.unpersist(blocking=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. When NOT to Cache
# MAGIC
# MAGIC Caching a DataFrame used only once wastes memory and adds overhead.

# COMMAND ----------

# BAD: caching a DataFrame used only once
one_use_df = (
    spark.read.format("delta").load(base_path)
    .filter(F.col("genre") == "Action")
)

start = time.time()
one_use_df.cache()
one_use_df.count()  # cache + compute
t_cached_once = time.time() - start
one_use_df.unpersist(blocking=True)

# GOOD: no cache for single-use DataFrame
one_use_df2 = (
    spark.read.format("delta").load(base_path)
    .filter(F.col("genre") == "Action")
)

start = time.time()
one_use_df2.count()  # compute only, no cache overhead
t_uncached_once = time.time() - start

print(f"Single use with cache:    {t_cached_once:.2f}s (wasted effort!)")
print(f"Single use without cache: {t_uncached_once:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Cache Invalidation: Always unpersist()
# MAGIC
# MAGIC Cached data stays in executor memory until explicitly released.
# MAGIC Forgetting to unpersist leads to memory pressure and GC overhead.

# COMMAND ----------

# Cache something
df_to_clean = (
    spark.read.format("delta").load(base_path)
    .select("movie_id", "title", "studio")
)
df_to_clean.cache()
df_to_clean.count()

print("Before unpersist -- check Spark UI Storage tab (should see cached RDD)")

# Unpersist with blocking=True to ensure memory is freed before continuing
df_to_clean.unpersist(blocking=True)
print("After unpersist  -- check Spark UI Storage tab (should be gone)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Delta Cache vs Spark Cache (Databricks)
# MAGIC
# MAGIC | Feature | Spark Cache | Delta Cache |
# MAGIC |---------|-------------|-------------|
# MAGIC | Trigger | Manual (`df.cache()`) | Automatic |
# MAGIC | Storage | Executor JVM memory | Local SSD |
# MAGIC | What's cached | Transformed results | Raw Parquet files |
# MAGIC | Lifecycle | Manual (`unpersist()`) | Managed by Databricks |
# MAGIC | Memory impact | Competes with execution | No executor memory used |
# MAGIC
# MAGIC **Recommendation**: On Databricks, rely on Delta Cache for repeated reads from
# MAGIC Delta tables. Use Spark Cache (`df.cache()`) only for expensive intermediate
# MAGIC computations that are reused multiple times.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Concept | Key Point |
# MAGIC |---------|-----------|
# MAGIC | `cache()` | Shortcut for `persist(MEMORY_AND_DISK)` |
# MAGIC | `persist(level)` | Choose your storage level |
# MAGIC | Lazy | Nothing cached until first action |
# MAGIC | `unpersist()` | Always clean up when done |
# MAGIC | Delta Cache | Automatic, uses SSD, better for Delta reads |
# MAGIC | When to cache | Multiple actions on same expensive DataFrame |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

dbutils.fs.rm("/tmp/perf_module/caching_test", recurse=True)
print("Cleanup complete.")
