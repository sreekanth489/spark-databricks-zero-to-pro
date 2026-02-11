# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Partitioning Strategies
# MAGIC > Module 05 — Topic 02 | Partition = unit of parallelism
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. Why partition count matters for performance
# MAGIC 2. `repartition(n)` (round-robin) vs `repartition(n, col)` (hash)
# MAGIC 3. `coalesce(n)` -- reduce partitions with minimum data movement
# MAGIC 4. Prove shuffle elimination with `.explain()` after hash repartition
# MAGIC 5. Write partitioned data and observe partition pruning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Movies/Studios Dataset

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]

num_rows = 2_000_000
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

movies_df = spark.createDataFrame(data, schema=schema)
movies_df.cache()
movies_df.count()
print(f"Created {num_rows:,} movies across {len(studios)} studios")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Check Default Partitioning
# MAGIC
# MAGIC Partition is a unit of parallelism. The number of partitions determines
# MAGIC how many tasks run in parallel. Let's see what Spark chose by default.

# COMMAND ----------

print(f"Default partition count: {movies_df.rdd.getNumPartitions()}")
print(f"Default shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. repartition(n) -- Round-Robin (No Key)
# MAGIC
# MAGIC Without specifying a key, repartition distributes records round-robin.
# MAGIC This is a full shuffle -- every record may move to a different node.

# COMMAND ----------

# Round-robin repartition to 6 partitions
rr_df = movies_df.repartition(6)
print(f"Partitions after repartition(6): {rr_df.rdd.getNumPartitions()}")

# Check distribution: records are spread evenly (roughly equal counts per partition)
from pyspark.sql import Row
partition_counts = (
    rr_df.withColumn("partition_id", F.spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)
partition_counts.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. repartition(n, col) -- Hash Partitioning by Studio
# MAGIC
# MAGIC **This is the key technique.** By specifying "studio" as the partitioning column,
# MAGIC Spark uses a hash function to place all records for the same studio in the same
# MAGIC partition. Now when you `groupBy("studio")`, there is NO data movement because
# MAGIC all records for a single studio are already on the same node.

# COMMAND ----------

# Hash repartition by studio
hash_df = movies_df.repartition(6, "studio")
print(f"Partitions after repartition(6, 'studio'): {hash_df.rdd.getNumPartitions()}")

# Show which studios ended up in which partition
studio_partition_map = (
    hash_df.withColumn("partition_id", F.spark_partition_id())
    .select("studio", "partition_id")
    .distinct()
    .orderBy("partition_id", "studio")
)
studio_partition_map.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice: every record for one studio is in exactly one partition.
# MAGIC The hash function determined the placement -- `Hash("Disney") = partition X`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prove Shuffle Elimination with .explain()
# MAGIC
# MAGIC This is the payoff. Compare the execution plans of `groupBy("studio")`
# MAGIC on the original DataFrame vs the hash-partitioned DataFrame.

# COMMAND ----------

# WITHOUT pre-partitioning: the plan shows Exchange (= shuffle)
print("=== PLAN WITHOUT HASH PARTITIONING (has Exchange = shuffle) ===")
movies_df.groupBy("studio").agg(F.sum("revenue").alias("total_revenue")).explain()

# COMMAND ----------

# WITH hash partitioning: NO Exchange in the plan!
print("=== PLAN WITH HASH PARTITIONING (no Exchange = no shuffle!) ===")
hash_df.groupBy("studio").agg(F.sum("revenue").alias("total_revenue")).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC **Key insight**: If you know you will do multiple operations using "studio"
# MAGIC as the key (groupBy, join, window functions), pre-partitioning the DataFrame
# MAGIC on that key eliminates shuffles. This is where the skill of data engineering
# MAGIC comes into play.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. coalesce() -- Reduce Partitions with Minimum Data Movement
# MAGIC
# MAGIC `coalesce(n)` reduces partitions by merging adjacent ones. It minimizes data
# MAGIC movement (no full shuffle). It can only REDUCE partitions, never increase.
# MAGIC
# MAGIC **Benefits of coalesce:**
# MAGIC 1. Eliminates task overhead from too many small partitions
# MAGIC 2. Optimizes file output (fewer, larger files)
# MAGIC 3. Minimizes data movement

# COMMAND ----------

# Start with 12 partitions
many_partitions_df = movies_df.repartition(12)
print(f"Before coalesce: {many_partitions_df.rdd.getNumPartitions()} partitions")

# Reduce to 3 partitions with coalesce (minimum data movement)
coalesced_df = many_partitions_df.coalesce(3)
print(f"After coalesce(3): {coalesced_df.rdd.getNumPartitions()} partitions")

# coalesce CANNOT increase partitions -- it silently stays the same
attempt_increase = many_partitions_df.coalesce(100)
print(f"After coalesce(100): {attempt_increase.rdd.getNumPartitions()} partitions (unchanged!)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Comparison: Too Many vs Right-Sized Partitions

# COMMAND ----------

def time_aggregation(df, label):
    """Time a groupBy aggregation and return duration."""
    start = time.time()
    df.groupBy("studio").agg(F.sum("revenue")).collect()
    elapsed = time.time() - start
    print(f"{label}: {elapsed:.2f}s ({df.rdd.getNumPartitions()} partitions)")
    return elapsed

# Too many partitions: 1000 tiny partitions = excessive scheduling overhead
tiny_df = movies_df.repartition(1000)
time_aggregation(tiny_df, "1000 partitions (too many)")

# Too few partitions: 2 partitions = cores sit idle
few_df = movies_df.repartition(2)
time_aggregation(few_df, "2 partitions (too few)")

# Right-sized: roughly match available cores
right_df = movies_df.repartition(8)
time_aggregation(right_df, "8 partitions (right-sized)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Writing Partitioned Data and Partition Pruning

# COMMAND ----------

# Write data partitioned by release_year
output_path = "/tmp/perf_module/movies_partitioned"
(
    movies_df
    .write
    .format("delta")
    .mode("overwrite")
    .partitionBy("release_year")
    .save(output_path)
)
print(f"Wrote partitioned Delta table to {output_path}")

# COMMAND ----------

# Read with a filter on the partition column -- partition pruning skips most directories
pruned_df = (
    spark.read.format("delta").load(output_path)
    .filter(F.col("release_year") == 2020)
)

# The plan will show PartitionFilters -- only the year=2020 directory is read
pruned_df.explain("formatted")
pruned_count = pruned_df.count()
print(f"Movies in 2020: {pruned_count} (only 1 partition directory read)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Tuning spark.sql.shuffle.partitions

# COMMAND ----------

# Default: 200 (often too many for small data, too few for large data)
print(f"Current shuffle partitions: {spark.conf.get('spark.sql.shuffle.partitions')}")

# Set to a smaller value for our dataset
spark.conf.set("spark.sql.shuffle.partitions", "8")

start = time.time()
movies_df.groupBy("studio", "genre").agg(F.avg("rating")).collect()
small_shuffle = time.time() - start
print(f"With 8 shuffle partitions: {small_shuffle:.2f}s")

# Reset to a larger value
spark.conf.set("spark.sql.shuffle.partitions", "200")

start = time.time()
movies_df.groupBy("studio", "genre").agg(F.avg("rating")).collect()
large_shuffle = time.time() - start
print(f"With 200 shuffle partitions: {large_shuffle:.2f}s")

# Reset to default
spark.conf.set("spark.sql.shuffle.partitions", "200")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Technique | When to Use | Data Movement |
# MAGIC |-----------|-------------|---------------|
# MAGIC | `repartition(n)` | Increase partitions, even distribution | Full shuffle |
# MAGIC | `repartition(n, col)` | Pre-partition by key for downstream ops | Full shuffle (once) |
# MAGIC | `coalesce(n)` | Reduce partitions, optimize file output | Minimum movement |
# MAGIC | `partitionBy(col)` | Disk-level partitioning for pruning | Write-time only |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

movies_df.unpersist()
dbutils.fs.rm("/tmp/perf_module/movies_partitioned", recurse=True)
print("Cleanup complete.")
