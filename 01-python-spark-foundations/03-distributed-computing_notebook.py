# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Distributed Computing -- Hands-On
# MAGIC
# MAGIC Explore partitioning, shuffles, narrow vs. wide transformations, and
# MAGIC observe how Spark distributes work across executors.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Understanding Partitions
# MAGIC
# MAGIC A partition is the basic unit of data that Spark processes in parallel.
# MAGIC Let us create a DataFrame and examine its partitioning.

# COMMAND ----------

# Create a DataFrame with a specific number of partitions
df = spark.range(0, 1000000, 1, numPartitions=8)
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
print(f"Total rows: {df.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecting Partition Sizes
# MAGIC
# MAGIC Not all partitions have to be the same size. Let us check.

# COMMAND ----------

from pyspark.sql.functions import spark_partition_id, count

# Show how many rows are in each partition
partition_sizes = (
    df
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .agg(count("*").alias("row_count"))
    .orderBy("partition_id")
)
partition_sizes.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Repartition vs. Coalesce
# MAGIC
# MAGIC - `repartition(n)` -- full shuffle, can increase or decrease partitions
# MAGIC - `coalesce(n)` -- no shuffle, can only decrease partitions

# COMMAND ----------

# Repartition to 16 partitions (full shuffle)
df_16 = df.repartition(16)
print(f"After repartition(16): {df_16.rdd.getNumPartitions()} partitions")

# Coalesce to 2 partitions (no shuffle)
df_2 = df.coalesce(2)
print(f"After coalesce(2): {df_2.rdd.getNumPartitions()} partitions")

# Attempt to coalesce UP (does not work -- stays at current count)
df_up = df.coalesce(100)
print(f"After coalesce(100): {df_up.rdd.getNumPartitions()} partitions (cannot increase)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Observe the Plans
# MAGIC
# MAGIC Notice: `repartition` shows an Exchange (shuffle), `coalesce` does not.

# COMMAND ----------

print("=== repartition(16) plan ===")
df.repartition(16).explain()

# COMMAND ----------

print("=== coalesce(2) plan ===")
df.coalesce(2).explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Narrow vs. Wide Transformations
# MAGIC
# MAGIC **Narrow**: each input partition maps to exactly one output partition (no shuffle).
# MAGIC **Wide**: input partitions contribute to multiple output partitions (shuffle required).

# COMMAND ----------

from pyspark.sql.functions import col, lit, rand

# Build a richer dataset
data_df = (
    spark.range(0, 500000)
    .withColumn("category", (col("id") % 5).cast("string"))
    .withColumn("amount", rand(seed=42) * 1000)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Narrow Transformations (No Shuffle)

# COMMAND ----------

# filter, select, withColumn -- all narrow
narrow_result = (
    data_df
    .filter(col("amount") > 500)
    .select("id", "category", "amount")
    .withColumn("amount_double", col("amount") * 2)
)

print("Plan for narrow-only transformations:")
narrow_result.explain()
print("\n>> Notice: No Exchange (shuffle) in the plan.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Wide Transformation (Shuffle Required)

# COMMAND ----------

from pyspark.sql.functions import avg, sum as _sum, count

# groupBy is a wide transformation
wide_result = (
    data_df
    .groupBy("category")
    .agg(
        count("*").alias("cnt"),
        avg("amount").alias("avg_amount"),
        _sum("amount").alias("total_amount"),
    )
)

print("Plan for wide transformation (groupBy):")
wide_result.explain()
print("\n>> Notice: Exchange (hashpartitioning) appears -- this is the shuffle.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Observing Shuffles with explain(mode="formatted")
# MAGIC
# MAGIC The formatted mode gives a cleaner, more readable plan.

# COMMAND ----------

# Chain two wide transformations: groupBy + orderBy
chained_wide = (
    data_df
    .groupBy("category")
    .agg(_sum("amount").alias("total"))
    .orderBy(col("total").desc())
)

chained_wide.explain(mode="formatted")
print(">> Two Exchange nodes = two shuffles = three stages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Partitioning After a Shuffle
# MAGIC
# MAGIC After a shuffle, the number of output partitions is controlled by
# MAGIC `spark.sql.shuffle.partitions` (default: 200).

# COMMAND ----------

# Check the current setting
print("shuffle.partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Perform a groupBy and check resulting partitions
grouped = data_df.groupBy("category").count()
print(f"Partitions after groupBy: {grouped.rdd.getNumPartitions()}")

# COMMAND ----------

# Lower the setting and observe
spark.conf.set("spark.sql.shuffle.partitions", "10")
grouped_10 = data_df.groupBy("category").count()
print(f"Partitions with shuffle.partitions=10: {grouped_10.rdd.getNumPartitions()}")

# Reset
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Narrow Chaining = Single Stage
# MAGIC
# MAGIC Spark pipelines multiple narrow transformations into a single stage for efficiency.

# COMMAND ----------

# All of these narrow operations run in ONE stage
pipeline = (
    data_df
    .filter(col("amount") > 100)
    .withColumn("tax", col("amount") * 0.08)
    .withColumn("total", col("amount") + col("tax"))
    .select("id", "category", "total")
    .filter(col("total") < 500)
)

# Trigger an action and check the Spark UI
pipeline_count = pipeline.count()
print(f"Rows after pipeline: {pipeline_count}")
print(">> Spark UI should show this as a SINGLE stage with all filters/projections fused.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Repartitioning by Column
# MAGIC
# MAGIC `repartition("column")` hash-partitions data by the column's value.
# MAGIC All rows with the same value end up in the same partition. This can optimize
# MAGIC subsequent groupBy operations on the same column.

# COMMAND ----------

# Repartition by category
by_cat = data_df.repartition("category")
print(f"Partitions after repartition('category'): {by_cat.rdd.getNumPartitions()}")

# Check partition contents
(
    by_cat
    .withColumn("pid", spark_partition_id())
    .groupBy("pid", "category")
    .count()
    .orderBy("pid", "category")
    .show(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Data Locality Visualization
# MAGIC
# MAGIC While we cannot directly see locality levels in a notebook, we can cache data
# MAGIC and observe that subsequent reads are faster (PROCESS_LOCAL).

# COMMAND ----------

# First read -- data from source
import time

start = time.time()
data_df.count()
first_read = time.time() - start

# Cache the DataFrame
data_df.cache()
data_df.count()  # materialize the cache

# Second read -- data from cache (PROCESS_LOCAL)
start = time.time()
data_df.count()
cached_read = time.time() - start

print(f"First read:  {first_read:.3f}s")
print(f"Cached read: {cached_read:.3f}s")
print(f"Speedup: {first_read / max(cached_read, 0.001):.1f}x")

# Unpersist to free memory
data_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Arrow-Based Serialization
# MAGIC
# MAGIC Apache Arrow dramatically speeds up conversions between Spark DataFrames
# MAGIC and Pandas DataFrames.

# COMMAND ----------

import time

small_df = spark.range(100000).withColumn("value", rand())

# Without Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
start = time.time()
pdf1 = small_df.toPandas()
no_arrow = time.time() - start

# With Arrow
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
start = time.time()
pdf2 = small_df.toPandas()
with_arrow = time.time() - start

print(f"Without Arrow: {no_arrow:.3f}s")
print(f"With Arrow:    {with_arrow:.3f}s")
print(f"Arrow speedup: {no_arrow / max(with_arrow, 0.001):.1f}x")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
print("Configuration reset. Notebook complete.")
