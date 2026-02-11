# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 — Cluster Management
# MAGIC **Module 00: Setup and Basics**
# MAGIC
# MAGIC This notebook explores the configuration of the cluster you are currently
# MAGIC attached to. You will inspect Spark settings, check resource allocation,
# MAGIC and understand how cluster mode affects execution.
# MAGIC
# MAGIC **Cluster requirement:** Any cluster with DBR 13.3 LTS or later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Identify Your Cluster and Runtime

# COMMAND ----------

# Read key cluster identification tags from Spark configuration
cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "N/A")
runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "N/A")
cluster_id = spark.conf.get("spark.databricks.clusterUsageTags.clusterId", "N/A")

print(f"Cluster name       : {cluster_name}")
print(f"Runtime version    : {runtime_version}")
print(f"Cluster ID         : {cluster_id}")
print(f"Spark version      : {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check CPU Cores and Memory
# MAGIC
# MAGIC Understanding how many cores and how much memory your cluster has
# MAGIC helps you right-size your workload.

# COMMAND ----------

# SparkContext gives us access to cluster resource information
sc = spark.sparkContext

# Total cores across all executors (does not include the driver)
total_cores = sc.defaultParallelism

# Get executor memory from Spark config
executor_memory = spark.conf.get("spark.executor.memory", "N/A")
driver_memory = spark.conf.get("spark.driver.memory", "N/A")

print(f"Default parallelism (total executor cores) : {total_cores}")
print(f"Executor memory (per executor)             : {executor_memory}")
print(f"Driver memory                              : {driver_memory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Detect Cluster Mode
# MAGIC
# MAGIC Databricks clusters can be single-node or multi-node (standard).
# MAGIC The `spark.master` value tells us the mode.

# COMMAND ----------

master = spark.conf.get("spark.master", "N/A")

if "local" in master:
    mode = "Single-Node (local mode)"
    print(f"Cluster mode: {mode}")
    print("All processing happens on the driver VM. No separate workers.")
else:
    mode = "Multi-Node (distributed)"
    print(f"Cluster mode: {mode}")
    print(f"Master URL: {master}")
    print("Work is distributed across driver + worker nodes.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Inspect Executor Details
# MAGIC
# MAGIC On a multi-node cluster, we can list the active executors.
# MAGIC On a single-node cluster, there is only the driver.

# COMMAND ----------

# Use the Java SparkContext to get executor information
java_sc = sc._jsc.sc()
executor_memory_status = java_sc.getExecutorMemoryStatus()

# Convert Java map to a Python dictionary
executor_info = dict(executor_memory_status)

print(f"Number of executor endpoints: {len(executor_info)}")
print("-" * 60)

for endpoint, mem_tuple in executor_info.items():
    # mem_tuple is a Scala Tuple2: (maxMemory, remainingMemory)
    max_mem_mb = mem_tuple._1() / (1024 * 1024)
    remaining_mb = mem_tuple._2() / (1024 * 1024)
    print(f"  Endpoint    : {endpoint}")
    print(f"  Max memory  : {max_mem_mb:,.0f} MB")
    print(f"  Remaining   : {remaining_mb:,.0f} MB")
    print("-" * 60)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explore Key Spark Configuration Parameters
# MAGIC
# MAGIC These parameters control how Spark distributes work. Understanding
# MAGIC them is essential for performance tuning in later modules.

# COMMAND ----------

# Key configuration parameters to inspect
config_keys = [
    ("spark.sql.shuffle.partitions", "Number of partitions for shuffles (joins, groupBy)"),
    ("spark.sql.adaptive.enabled", "Adaptive Query Execution (AQE) enabled"),
    ("spark.sql.adaptive.coalescePartitions.enabled", "AQE automatic partition coalescing"),
    ("spark.databricks.delta.optimizeWrite.enabled", "Delta optimized writes"),
    ("spark.databricks.photon.enabled", "Photon engine enabled"),
    ("spark.serializer", "Serializer used for data transfer"),
    ("spark.sql.execution.arrow.pyspark.enabled", "Apache Arrow for Pandas conversion"),
]

print(f"{'Parameter':<55} {'Value':<20} Description")
print("=" * 120)

for key, description in config_keys:
    try:
        value = spark.conf.get(key)
    except Exception:
        value = "(not set)"
    print(f"{key:<55} {value:<20} {description}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Understand Autoscaling Through Configuration

# COMMAND ----------

# Check autoscaling-related settings
try:
    min_workers = spark.conf.get("spark.databricks.clusterUsageTags.clusterMinWorkers", "N/A")
    max_workers = spark.conf.get("spark.databricks.clusterUsageTags.clusterMaxWorkers", "N/A")
    target_workers = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers", "N/A")

    print(f"Min workers    : {min_workers}")
    print(f"Max workers    : {max_workers}")
    print(f"Target workers : {target_workers}")

    if min_workers == max_workers:
        print("\nAutoscaling: DISABLED (min == max)")
    else:
        print("\nAutoscaling: ENABLED")
        print("Workers will scale between min and max based on load.")
except Exception as e:
    print(f"Could not read autoscaling config: {e}")
    print("This may be a single-node cluster or Community Edition.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify with a Quick Workload
# MAGIC
# MAGIC Let us create a moderately sized DataFrame to see the cluster in action.
# MAGIC Check the Spark UI after running this cell to see how tasks are distributed.

# COMMAND ----------

from pyspark.sql.functions import col, rand, expr

# Generate 1 million rows with random data
# This is enough to see partitioning and task distribution in the Spark UI
df_large = (
    spark.range(0, 1_000_000)
    .withColumn("value", rand() * 100)
    .withColumn("category", expr("CASE WHEN id % 5 = 0 THEN 'A' "
                                 "WHEN id % 5 = 1 THEN 'B' "
                                 "WHEN id % 5 = 2 THEN 'C' "
                                 "WHEN id % 5 = 3 THEN 'D' "
                                 "ELSE 'E' END"))
)

# Force an action to execute the plan
result = df_large.groupBy("category").avg("value").collect()

print("Category averages (all should be near 50.0):")
for row in sorted(result, key=lambda r: r["category"]):
    print(f"  {row['category']}: {row['avg(value)']:.2f}")

print(f"\nPartitions used: {df_large.rdd.getNumPartitions()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. View Environment Variables
# MAGIC
# MAGIC Some cluster settings are exposed as environment variables on the
# MAGIC driver node. These can be useful for debugging or in init scripts.

# COMMAND ----------

# MAGIC %sh
# MAGIC echo "=== Key Environment Variables ==="
# MAGIC echo "SPARK_HOME       = $SPARK_HOME"
# MAGIC echo "DATABRICKS_RUNTIME_VERSION = $DATABRICKS_RUNTIME_VERSION"
# MAGIC echo "JAVA_HOME        = $JAVA_HOME"
# MAGIC echo "PYTHON_VERSION   = $(python --version 2>&1)"
# MAGIC echo ""
# MAGIC echo "=== CPU Info ==="
# MAGIC nproc --all
# MAGIC echo "core(s)"
# MAGIC echo ""
# MAGIC echo "=== Memory Info ==="
# MAGIC free -h 2>/dev/null || echo "(free command not available)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Clean Up
# MAGIC
# MAGIC This notebook only created in-memory DataFrames. No files or tables
# MAGIC were written, so there is nothing to clean up.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next:** Import `03-notebook-fundamentals_notebook.py` to learn about
# MAGIC cell types, magic commands, widgets, and dbutils.
