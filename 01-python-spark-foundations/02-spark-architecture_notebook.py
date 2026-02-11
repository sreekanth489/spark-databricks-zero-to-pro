# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Spark Architecture -- Hands-On Exploration
# MAGIC
# MAGIC This notebook lets you interact with Spark's architecture components:
# MAGIC SparkSession, SparkContext, configuration, and the jobs/stages/tasks hierarchy.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The SparkSession Object
# MAGIC
# MAGIC In Databricks, `spark` is pre-initialized. Let us examine it.

# COMMAND ----------

# The SparkSession is the unified entry point
print("SparkSession type:", type(spark))
print("App name:", spark.sparkContext.appName)
print("Spark version:", spark.version)
print("Master:", spark.sparkContext.master)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SparkContext -- The RDD-Level Entry Point
# MAGIC
# MAGIC SparkContext is wrapped inside SparkSession. You can still access it directly.

# COMMAND ----------

sc = spark.sparkContext
print("SparkContext type:", type(sc))
print("Default parallelism:", sc.defaultParallelism)
print("Python version:", sc.pythonVer)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Inspecting Cluster Configuration
# MAGIC
# MAGIC Spark has hundreds of configuration parameters. Here are the most important ones.

# COMMAND ----------

# Retrieve all Spark configuration as a list of tuples
all_conf = spark.sparkContext.getConf().getAll()

# Display key settings
important_keys = [
    "spark.executor.memory",
    "spark.executor.cores",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
]

print("=== Key Spark Configuration ===")
conf_dict = dict(all_conf)
for key in important_keys:
    value = conf_dict.get(key, "(not set -- using default)")
    print(f"  {key} = {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Setting Configuration at Runtime
# MAGIC
# MAGIC Some settings can be changed at runtime. Others require a cluster restart.

# COMMAND ----------

# Check current shuffle partitions
print("Current shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Change it at runtime
spark.conf.set("spark.sql.shuffle.partitions", "50")
print("Updated shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))

# Reset to a reasonable default for this notebook
spark.conf.set("spark.sql.shuffle.partitions", "8")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Triggering a Job -- Observing Jobs, Stages, and Tasks
# MAGIC
# MAGIC Every **action** triggers a **job**. Let us create a DataFrame and trigger
# MAGIC an action. Check the Spark UI Jobs tab after running this cell.

# COMMAND ----------

# Generate sample data
from pyspark.sql.functions import col, rand, floor

df = (
    spark.range(0, 1000000, 1, numPartitions=8)
    .withColumn("group", (col("id") % 10).cast("string"))
    .withColumn("value", rand(seed=42) * 100)
)

# This action triggers a job with a single stage (no shuffle)
row_count = df.count()
print(f"Row count: {row_count}")
print(">> Check the Spark UI 'Jobs' tab -- you should see a new job.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Shuffle Creates a Stage Boundary
# MAGIC
# MAGIC A `groupBy` + aggregation requires a shuffle, creating two stages.

# COMMAND ----------

from pyspark.sql.functions import avg, count

# This triggers a job with TWO stages (the groupBy causes a shuffle)
result = (
    df
    .groupBy("group")
    .agg(
        count("*").alias("cnt"),
        avg("value").alias("avg_value")
    )
)

result.show()
print(">> Check Spark UI: this job should have 2 stages.")
print("   Stage 0: read + project (8 tasks, one per partition)")
print("   Stage 1: shuffle + aggregate (8 tasks for 8 shuffle partitions)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Multiple Shuffles = Multiple Stages
# MAGIC
# MAGIC Let us chain operations that each require a shuffle.

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, desc

# Two shuffles: groupBy + sort (orderBy)
multi_stage = (
    df
    .groupBy("group")
    .agg(_sum("value").alias("total_value"))
    .orderBy(desc("total_value"))
)

multi_stage.show()
print(">> Check Spark UI: this job should have 3 stages.")
print("   Stage 0: read partitions")
print("   Stage 1: shuffle for groupBy aggregation")
print("   Stage 2: shuffle for global sort (orderBy)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Examining Partitions
# MAGIC
# MAGIC The number of partitions determines the number of tasks per stage.

# COMMAND ----------

# Check partition count
print(f"DataFrame partitions: {df.rdd.getNumPartitions()}")

# Repartition and check again
df_repart = df.repartition(16)
print(f"After repartition(16): {df_repart.rdd.getNumPartitions()}")

# Coalesce (reduce partitions without full shuffle)
df_coalesced = df.coalesce(2)
print(f"After coalesce(2): {df_coalesced.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. The explain() Method -- Peeking at the Plan
# MAGIC
# MAGIC `explain()` shows the physical execution plan without running the query.

# COMMAND ----------

# Simple plan
df.filter(col("value") > 50).explain()

# COMMAND ----------

# More complex plan with a shuffle
(
    df
    .groupBy("group")
    .agg(avg("value").alias("avg_val"))
    .filter(col("avg_val") > 50)
    .explain(mode="formatted")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Executor Information
# MAGIC
# MAGIC We can access executor metadata through the SparkContext Java gateway.

# COMMAND ----------

# List active executors
executor_info = spark.sparkContext._jsc.sc().getExecutorMemoryStatus()
executors = list(executor_info.keys())

print(f"Number of executors (including driver): {len(executors)}")
for i, executor in enumerate(executors):
    print(f"  [{i}] {executor}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Reset the shuffle partitions to the Databricks default.

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "200")
print("Reset spark.sql.shuffle.partitions to 200.")
print("Notebook complete.")
