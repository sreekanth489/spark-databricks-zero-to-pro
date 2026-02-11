# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - Spark UI & Debugging
# MAGIC > Module 05 — Topic 01 | Explore execution plans, the Catalyst pipeline, and the Spark UI
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate a sample dataset large enough to observe meaningful Spark UI metrics
# MAGIC 2. Use `.explain()` in all four modes to read execution plans
# MAGIC 3. Trigger jobs, stages, and tasks -- then inspect them in the Spark UI
# MAGIC 4. Introduce data skew and observe the impact in stage details
# MAGIC 5. See predicate pushdown in action through plan comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Generate Sample Data

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random

# Generate a movies dataset with studios (we will reuse this across the module)
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
print(f"Row count: {movies_df.count()}")
movies_df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Reading Execution Plans with .explain()
# MAGIC
# MAGIC The Catalyst Optimizer pipeline:
# MAGIC ```
# MAGIC SQL/DataFrame -> Unresolved Logical Plan -> Resolved (Catalog) ->
# MAGIC Optimized (Catalyst) -> Physical Plans -> Cost Model + AQE -> Execution
# MAGIC ```

# COMMAND ----------

# Build a query: filter + groupBy + aggregate
query_df = (
    movies_df
    .filter(F.col("release_year") >= 2000)
    .groupBy("studio")
    .agg(
        F.count("*").alias("movie_count"),
        F.avg("rating").alias("avg_rating"),
        F.sum("revenue").alias("total_revenue"),
    )
    .orderBy(F.desc("total_revenue"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Simple mode: Physical plan only (default)

# COMMAND ----------

# Simple: print only a physical plan
query_df.explain("simple")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Extended mode: Both logical and physical plans

# COMMAND ----------

# Extended: print both logical and physical plans
query_df.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Formatted mode: Structured physical plan with per-operator details

# COMMAND ----------

query_df.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Triggering Jobs, Stages, and Tasks
# MAGIC
# MAGIC Every **action** creates a job. Every shuffle boundary creates a new **stage**.
# MAGIC Every partition within a stage creates a **task**.
# MAGIC
# MAGIC After running the cells below, open the Spark UI:
# MAGIC - **Jobs tab**: see one job per action
# MAGIC - **Stages tab**: see stages separated by shuffles (groupBy, orderBy)
# MAGIC - **SQL tab**: see the visual DAG with per-operator metrics

# COMMAND ----------

# Action 1: count -- creates a job
count_result = movies_df.filter(F.col("genre") == "Action").count()
print(f"Action movies: {count_result}")

# COMMAND ----------

# Action 2: aggregation -- creates a job with shuffle stages
result = (
    movies_df
    .groupBy("studio", "genre")
    .agg(F.sum("revenue").alias("total_revenue"))
    .orderBy(F.desc("total_revenue"))
    .collect()
)
print(f"Top result: {result[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Observing Narrow vs Wide Transformations
# MAGIC
# MAGIC - **Narrow** (map, filter, select): no shuffle, same stage
# MAGIC - **Wide** (groupBy, join, repartition): shuffle, new stage
# MAGIC
# MAGIC The plan below has narrow ops (filter, withColumn) and wide ops (groupBy).
# MAGIC Check the Spark UI -- narrow ops share a stage, the groupBy starts a new one.

# COMMAND ----------

narrow_and_wide_df = (
    movies_df
    .filter(F.col("release_year") >= 2010)           # narrow
    .withColumn("decade", (F.col("release_year") / 10).cast("int") * 10)  # narrow
    .groupBy("decade", "studio")                      # wide -- shuffle boundary
    .agg(F.avg("rating").alias("avg_rating"))
    .orderBy("decade", F.desc("avg_rating"))          # wide -- another shuffle
)

# Trigger execution
narrow_and_wide_df.show(10)

# Inspect the plan: notice Exchange nodes = shuffle boundaries = stage breaks
narrow_and_wide_df.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Introducing Data Skew
# MAGIC
# MAGIC Data skew means one partition has far more data than others.
# MAGIC In the Spark UI Stage Details, you will see one task taking much longer.
# MAGIC
# MAGIC Analogy: 4 chefs in a kitchen. 3 get 10 dishes each, 1 gets 80 dishes.
# MAGIC The entire order waits for that one overwhelmed chef.

# COMMAND ----------

# Create a skewed dataset: 80% of records belong to one studio
skewed_data = (
    movies_df
    .withColumn(
        "skewed_studio",
        F.when(F.rand() < 0.8, F.lit("MegaStudio"))
         .otherwise(F.col("studio"))
    )
)

# groupBy on the skewed column -- check Stage Details in Spark UI
# You will see one task processing ~80% of data (the "MegaStudio" partition)
skewed_result = (
    skewed_data
    .groupBy("skewed_studio")
    .agg(F.count("*").alias("cnt"), F.sum("revenue").alias("total_rev"))
)

skewed_result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **After running the cell above**, go to Spark UI -> Stages -> click the stage
# MAGIC for the aggregation. Look at the Task Duration distribution:
# MAGIC - `min` and `median` will be small
# MAGIC - `max` will be significantly larger (the skewed partition)
# MAGIC
# MAGIC This is how you detect skew in production.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Predicate Pushdown: Plans Before and After
# MAGIC
# MAGIC Catalyst pushes filters as close to the data source as possible.
# MAGIC Compare these two plans -- notice how the filter moves down in the optimized plan.

# COMMAND ----------

# Write data to Delta so we can observe predicate pushdown on file scans
movies_df.write.format("delta").mode("overwrite").save("/tmp/perf_module/movies_delta")

delta_df = spark.read.format("delta").load("/tmp/perf_module/movies_delta")

# Query with filter
filtered_df = delta_df.filter(F.col("release_year") == 2020).select("title", "studio", "revenue")

# The extended plan will show the filter pushed into the scan operator
print("=== Plan with filter (notice predicate pushdown) ===")
filtered_df.explain("extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary
# MAGIC
# MAGIC | Concept | What to Look For in Spark UI |
# MAGIC |---------|------------------------------|
# MAGIC | Shuffle | `Exchange` in plan, Shuffle Read/Write in stages |
# MAGIC | Skew | One task with much longer duration than others |
# MAGIC | Spill | Spill (Memory) and Spill (Disk) in stage metrics |
# MAGIC | Predicate Pushdown | Filter inside Scan operator in plan |
# MAGIC | Cache Hit | Storage tab shows cached DataFrame fraction |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

movies_df.unpersist()
dbutils.fs.rm("/tmp/perf_module/movies_delta", recurse=True)
print("Cleanup complete.")
