# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Big Data & Hadoop Fundamentals — Hands-On Notebook
# MAGIC
# MAGIC > Module 01 — Topic 00 | Level: Beginner | Time: 90 min
# MAGIC
# MAGIC This notebook accompanies the [Big Data & Hadoop Fundamentals Guide](00-big-data-hadoop-fundamentals.md).
# MAGIC You will explore how Spark replaces the Hadoop stack, see distributed processing in action,
# MAGIC and understand partitions, the DAG, and lazy evaluation through live examples.
# MAGIC
# MAGIC **Prerequisites**: A running Databricks cluster (Community Edition is sufficient).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Your Spark Environment — What Replaced Hadoop
# MAGIC
# MAGIC In Databricks, a pre-initialized `SparkSession` named `spark` is available in every notebook.
# MAGIC This single object replaces the entire Hadoop stack:
# MAGIC
# MAGIC | Hadoop Component | Spark/Databricks Equivalent |
# MAGIC |------------------|-----------------------------|
# MAGIC | HDFS | DBFS / S3 / ADLS / GCS |
# MAGIC | MapReduce | Spark Engine (in-memory) |
# MAGIC | Hive | Spark SQL |
# MAGIC | YARN | Databricks Cluster Manager |

# COMMAND ----------

# Explore the SparkSession object — your gateway to all Spark functionality
print("Spark Version:", spark.version)
print("App Name:", spark.sparkContext.appName)
print("Master:", spark.sparkContext.master)
print("Default Parallelism:", spark.sparkContext.defaultParallelism)

# COMMAND ----------

# MAGIC %md
# MAGIC The `defaultParallelism` value tells you how many cores are available on your cluster.
# MAGIC This is how many tasks Spark can run simultaneously — the foundation of distributed processing.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. The Big Data Problem — Why One Machine Fails
# MAGIC
# MAGIC Let's simulate the difference between processing data on a single machine vs. a distributed system.
# MAGIC We'll generate sample data and see how Spark distributes it automatically.

# COMMAND ----------

# Generate a sample dataset — imagine this is clickstream data from an e-commerce site
from pyspark.sql.functions import col, rand, expr, lit, concat

num_rows = 1_000_000  # 1 million rows (scale up on larger clusters)

clickstream_df = (
    spark.range(0, num_rows)
    .withColumn("customer_id", (col("id") % 10000).cast("int"))
    .withColumn("event_type", expr("CASE WHEN rand() < 0.5 THEN 'click' WHEN rand() < 0.8 THEN 'view' ELSE 'purchase' END"))
    .withColumn("page", concat(lit("page_"), (col("id") % 500).cast("string")))
    .withColumn("timestamp", expr("current_timestamp() - make_interval(0, 0, 0, cast(floor(rand() * 30) as int), 0, 0, 0)"))
    .drop("id")
)

print(f"Generated {clickstream_df.count():,} clickstream events")
clickstream_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Partitions — How Spark Distributes Data
# MAGIC
# MAGIC Remember: partitions are groups of rows. Each partition is processed by one task on one core.
# MAGIC Spark automatically decides how many partitions to create.

# COMMAND ----------

# Check how many partitions Spark created for our dataset
print(f"Number of partitions: {clickstream_df.rdd.getNumPartitions()}")
print(f"Default parallelism (cores): {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# MAGIC %md
# MAGIC Spark chose the partition count based on your cluster's cores. The default is typically
# MAGIC equal to the number of available CPU cores, or based on the data size (128 MB per partition).

# COMMAND ----------

# Let's see how rows are distributed across partitions
from pyspark.sql.functions import spark_partition_id

partition_distribution = (
    clickstream_df
    .withColumn("partition_id", spark_partition_id())
    .groupBy("partition_id")
    .count()
    .orderBy("partition_id")
)

print("Row distribution across partitions:")
partition_distribution.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC Notice how Spark distributes rows roughly evenly across partitions.
# MAGIC Each partition will be processed by a separate task (thread) — this is how parallelism works.
# MAGIC
# MAGIC **Rule of thumb**: Default partition size is 128 MB. For a 1 TB file:
# MAGIC 1 TB / 128 MB = ~8,000 partitions = ~8,000 parallel tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Simulating MapReduce vs. Spark — Word Count
# MAGIC
# MAGIC The "Hello World" of Big Data is word count. Let's see how Spark makes this trivial
# MAGIC compared to the 50+ lines of Java required by Hadoop MapReduce.

# COMMAND ----------

# Create sample text data (simulating a distributed text file)
text_data = [
    "the cat sat on the mat",
    "the dog sat on the log",
    "the bird flew over the cat",
    "spark is faster than hadoop mapreduce",
    "spark processes data in memory",
    "hadoop writes intermediate results to disk",
    "spark keeps data in memory between stages",
    "distributed computing enables big data processing",
    "partitions are the foundation of parallelism in spark",
    "the catalyst optimizer rewrites your queries for efficiency"
]

text_df = spark.createDataFrame([(line,) for line in text_data], ["text"])
text_df.show(truncate=False)

# COMMAND ----------

# Word count in Spark — just 4 lines!
# This replaces 50+ lines of Java MapReduce code
from pyspark.sql.functions import explode, split, lower

word_counts = (
    text_df
    .select(explode(split(lower(col("text")), " ")).alias("word"))
    .groupBy("word")
    .count()
    .orderBy(col("count").desc())
)

print("Word Count Results (Spark DataFrame API):")
word_counts.show(20)

# COMMAND ----------

# The same thing using pure Spark SQL — even simpler!
text_df.createOrReplaceTempView("text_table")

sql_word_counts = spark.sql("""
    SELECT word, COUNT(*) as count
    FROM (
        SELECT explode(split(lower(text), ' ')) as word
        FROM text_table
    )
    GROUP BY word
    ORDER BY count DESC
""")

print("Word Count Results (Spark SQL):")
sql_word_counts.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare the Approaches
# MAGIC
# MAGIC | Approach | Lines of Code | Ease of Use |
# MAGIC |----------|--------------|-------------|
# MAGIC | Hadoop MapReduce (Java) | ~50 lines | Verbose, must compile & package |
# MAGIC | Spark RDD API (Python) | ~5 lines | Functional, interactive |
# MAGIC | Spark DataFrame API (Python) | ~4 lines | Declarative, optimized |
# MAGIC | Spark SQL | ~6 lines (SQL) | Familiar to any SQL user |
# MAGIC
# MAGIC All Spark approaches run **in memory** and are **10-100x faster** than MapReduce.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Lazy Evaluation — Transformations vs. Actions
# MAGIC
# MAGIC One of Spark's most important concepts: **transformations are lazy**.
# MAGIC They build a recipe (the DAG) but don't execute until you call an **action**.

# COMMAND ----------

# These transformations are LAZY — they execute NOTHING
# Spark just builds the DAG (recipe) internally
step1 = clickstream_df.filter(col("event_type") == "purchase")
step2 = step1.groupBy("customer_id").count()
step3 = step2.filter(col("count") > 2)
step4 = step3.orderBy(col("count").desc())

print("No computation happened yet!")
print("Spark just built the DAG (recipe) with 4 transformation steps.")
print(f"Type of step4: {type(step4)}")

# COMMAND ----------

# NOW we call an ACTION — this triggers execution of the entire DAG
print("Calling .show() — this is an ACTION that triggers the DAG execution:\n")
step4.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why Lazy Evaluation Matters
# MAGIC
# MAGIC Because Spark sees the **entire recipe** before executing, the **Catalyst Optimizer** can
# MAGIC rewrite it for efficiency:
# MAGIC
# MAGIC - **Predicate pushdown**: Moves filters as early as possible to reduce data volume
# MAGIC - **Column pruning**: Only reads columns that are actually used
# MAGIC - **Dead code elimination**: Skips computations whose results are never used
# MAGIC - **Join reordering**: Picks the most efficient join strategy

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The Catalyst Optimizer in Action
# MAGIC
# MAGIC Use `.explain()` to see how Catalyst optimizes your query.
# MAGIC The execution plan shows what Spark ACTUALLY does vs. what you wrote.

# COMMAND ----------

# Let's see the execution plan for our word count
print("Execution plan for word count:")
word_counts.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC In the plan above, notice:
# MAGIC - **Parsed Logical Plan**: What you wrote
# MAGIC - **Analyzed Logical Plan**: Resolved column names and types
# MAGIC - **Optimized Logical Plan**: Catalyst's optimized version (filters pushed down, columns pruned)
# MAGIC - **Physical Plan**: The actual execution strategy (scan, shuffle, aggregate)

# COMMAND ----------

# Demonstrate Catalyst's column pruning
# We read all columns but only use 2 — Catalyst will only scan those 2
optimized_query = (
    clickstream_df
    .select("customer_id", "event_type", "page", "timestamp")  # select 4 columns
    .filter(col("event_type") == "purchase")
    .groupBy("customer_id")
    .count()
    # Result only needs customer_id and count — Catalyst prunes page and timestamp!
)

print("Even though we selected 4 columns, Catalyst optimizes:")
optimized_query.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reading from Distributed Storage (DBFS)
# MAGIC
# MAGIC In production, data lives in distributed storage (S3, ADLS, GCS).
# MAGIC In Databricks, **DBFS** abstracts cloud storage with a unified path.
# MAGIC
# MAGIC | Cloud | Underlying Storage | DBFS Path |
# MAGIC |-------|-------------------|-----------|
# MAGIC | AWS | S3 | `dbfs:/mnt/my-bucket/` or `s3://my-bucket/` |
# MAGIC | Azure | ADLS Gen2 | `dbfs:/mnt/my-container/` or `abfss://...` |
# MAGIC | GCP | GCS | `dbfs:/mnt/my-bucket/` or `gs://my-bucket/` |

# COMMAND ----------

# Write our sample data to DBFS so we can read it back (simulating distributed storage)
output_path = "/tmp/big_data_demo/clickstream"

clickstream_df.write.mode("overwrite").parquet(output_path)
print(f"Wrote data to: {output_path}")

# COMMAND ----------

# Read it back — Spark automatically partitions the data across the cluster
distributed_df = spark.read.parquet(output_path)
print(f"Read back {distributed_df.count():,} rows")
print(f"Number of partitions: {distributed_df.rdd.getNumPartitions()}")
distributed_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC When Spark reads files from distributed storage, it automatically:
# MAGIC 1. Discovers all files in the path
# MAGIC 2. Splits them into partitions (based on file size, ~128 MB each)
# MAGIC 3. Assigns partitions to executor tasks for parallel processing
# MAGIC
# MAGIC You write code as if reading a single file — Spark handles the distribution.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Spark vs. MapReduce Performance — Live Demo
# MAGIC
# MAGIC Let's demonstrate the in-memory advantage. We'll run the same aggregation twice:
# MAGIC once without caching (like MapReduce hitting disk) and once with caching (Spark's strength).

# COMMAND ----------

import time

# Aggregation WITHOUT caching (every action re-reads data)
start = time.time()
result1 = clickstream_df.groupBy("customer_id", "event_type").count()
count1 = result1.count()
time_uncached = time.time() - start

# Cache the DataFrame in memory (Spark's in-memory advantage)
clickstream_df.cache()
clickstream_df.count()  # Trigger caching

# Aggregation WITH caching (data is already in memory)
start = time.time()
result2 = clickstream_df.groupBy("customer_id", "event_type").count()
count2 = result2.count()
time_cached = time.time() - start

print(f"Without cache (disk-like): {time_uncached:.2f} seconds")
print(f"With cache (in-memory):    {time_cached:.2f} seconds")
print(f"Speedup: {time_uncached/time_cached:.1f}x faster with caching")

# Unpersist to free memory
clickstream_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC This demonstrates Spark's core advantage over MapReduce:
# MAGIC - **MapReduce**: Every operation reads from and writes to disk
# MAGIC - **Spark**: Keeps data in memory between operations
# MAGIC
# MAGIC For iterative algorithms (ML training with 100+ passes), this difference is 10-100x.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. DataFrames — Your Primary Data Structure
# MAGIC
# MAGIC A **DataFrame** is a distributed data structure organized into named columns.
# MAGIC You interact with it just like a database table — using `SELECT`, `WHERE`, `GROUP BY`, etc.
# MAGIC Behind the scenes, the data is distributed across partitions on multiple machines.

# COMMAND ----------

# Create a DataFrame and interact with it like a database table
clickstream_df.createOrReplaceTempView("clickstream")

# SQL query — looks exactly like querying a regular database!
result = spark.sql("""
    SELECT
        customer_id,
        event_type,
        COUNT(*) as event_count,
        MIN(timestamp) as first_event,
        MAX(timestamp) as last_event
    FROM clickstream
    WHERE event_type IN ('click', 'purchase')
    GROUP BY customer_id, event_type
    HAVING COUNT(*) > 5
    ORDER BY event_count DESC
    LIMIT 10
""")

print("SQL query on a distributed DataFrame (looks like a regular table!):")
result.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Key insight**: The DataFrame is distributed across multiple machines, but you
# MAGIC write SQL as if it were a single table on your laptop. Spark handles:
# MAGIC - Distributing the query across partitions
# MAGIC - Parallel execution on multiple cores/machines
# MAGIC - Shuffling data for GROUP BY and ORDER BY
# MAGIC - Optimizing the query plan via Catalyst

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Summary — The Big Data Stack Evolution
# MAGIC
# MAGIC ```
# MAGIC  HADOOP ERA (2006-2014)          SPARK ERA (2014-now)          DATABRICKS LAKEHOUSE
# MAGIC  ══════════════════════          ════════════════════          ════════════════════
# MAGIC
# MAGIC  Storage:  HDFS          →       HDFS / S3 / ADLS      →     S3/ADLS/GCS + Delta Lake
# MAGIC  Process:  MapReduce     →       Spark (in-memory)      →     Spark + Photon
# MAGIC  SQL:      Hive          →       Spark SQL              →     Spark SQL + Serverless SQL
# MAGIC  Manage:   YARN          →       YARN / K8s             →     Databricks Autoscale
# MAGIC  ML:       Mahout        →       MLlib                  →     MLlib + MLflow + GenAI
# MAGIC  Govern:   (none)        →       (limited)              →     Unity Catalog
# MAGIC  ```
# MAGIC
# MAGIC **What you learned today**:
# MAGIC - Big Data problems forced us to move from single-machine databases to distributed systems
# MAGIC - Hadoop solved storage (HDFS) and batch processing (MapReduce)
# MAGIC - Spark solved performance (in-memory) and flexibility (Python, SQL, streaming, ML)
# MAGIC - Cloud storage (S3, ADLS) removed infrastructure complexity
# MAGIC - Databricks combines everything into a unified Lakehouse platform

# COMMAND ----------

# Clean up
spark.sql("DROP VIEW IF EXISTS clickstream")
spark.sql("DROP VIEW IF EXISTS text_table")

import shutil
try:
    dbutils.fs.rm("/tmp/big_data_demo", recurse=True)
except:
    pass  # Ignore if running outside Databricks

print("Cleanup complete. Proceed to 01-python-essentials_notebook.py")
