# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 — Databricks Overview
# MAGIC **Module 00: Setup and Basics**
# MAGIC
# MAGIC This notebook gives you a first look at your Databricks environment.
# MAGIC We will explore the SparkSession, check the runtime version, list
# MAGIC databases, and create a simple DataFrame to verify everything works.
# MAGIC
# MAGIC **Cluster requirement:** Any cluster (single-node or multi-node) with DBR 13.3 LTS or later.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. The SparkSession — Your Entry Point
# MAGIC
# MAGIC In Databricks, a `SparkSession` named **`spark`** is pre-created for you.
# MAGIC You never need to call `SparkSession.builder...getOrCreate()` manually.

# COMMAND ----------

# Inspect the pre-configured SparkSession
# The 'spark' variable is automatically available in every Databricks notebook
print(f"Spark version : {spark.version}")
print(f"App name      : {spark.sparkContext.appName}")
print(f"Master        : {spark.sparkContext.master}")
print(f"Default parallelism: {spark.sparkContext.defaultParallelism}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check the Databricks Runtime Version
# MAGIC
# MAGIC The runtime version tells you which libraries and Spark version are
# MAGIC installed on your cluster. You can read it from the Spark configuration.

# COMMAND ----------

# Retrieve runtime-related Spark configuration values
runtime_version = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion", "N/A")
cluster_name = spark.conf.get("spark.databricks.clusterUsageTags.clusterName", "N/A")

print(f"Databricks Runtime version : {runtime_version}")
print(f"Cluster name               : {cluster_name}")

# You can also inspect all Spark configs (there are hundreds)
# Uncomment the next line to see them all:
# spark.sparkContext.getConf().getAll()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. List Available Databases (Schemas)
# MAGIC
# MAGIC Every Databricks workspace comes with a `default` database.
# MAGIC If Unity Catalog is enabled, you will also see catalogs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all databases (schemas) visible to the current user
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Create a Simple DataFrame
# MAGIC
# MAGIC Let us verify the cluster by creating a small in-memory DataFrame
# MAGIC and running a basic transformation.

# COMMAND ----------

# Create sample data directly — no external files needed
from pyspark.sql import Row
from pyspark.sql.functions import col, upper

# Build a DataFrame from a list of Rows
sample_data = [
    Row(id=1, name="Alice",   department="Engineering", salary=95000),
    Row(id=2, name="Bob",     department="Marketing",   salary=82000),
    Row(id=3, name="Charlie", department="Engineering", salary=105000),
    Row(id=4, name="Diana",   department="Data Science", salary=98000),
    Row(id=5, name="Eve",     department="Marketing",   salary=78000),
]

df = spark.createDataFrame(sample_data)

# Show the DataFrame
print("--- Sample Employee DataFrame ---")
df.show()

# Basic transformation: uppercase department names and filter salary > 90k
df_filtered = (
    df
    .withColumn("dept_upper", upper(col("department")))
    .filter(col("salary") > 90000)
    .select("id", "name", "dept_upper", "salary")
)

print("--- Employees with salary > 90,000 ---")
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Explore the Spark UI
# MAGIC
# MAGIC After running cells, click the **Spark Jobs** link that appears beneath
# MAGIC the cell output. This opens the Spark UI where you can inspect:
# MAGIC
# MAGIC - **Jobs** — one per action (show, count, collect, write)
# MAGIC - **Stages** — subdivisions of a job based on shuffle boundaries
# MAGIC - **Tasks** — individual units of work sent to executors
# MAGIC - **Storage** — cached DataFrames and tables
# MAGIC - **SQL/DataFrame** — visual query execution plans

# COMMAND ----------

# Trigger a few actions so the Spark UI has something to show
row_count = df.count()
partitions = df.rdd.getNumPartitions()

print(f"Row count  : {row_count}")
print(f"Partitions : {partitions}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Examine the Execution Plan
# MAGIC
# MAGIC The `explain()` method shows how Spark will physically execute a query.
# MAGIC This is critical for performance tuning (covered in later modules).

# COMMAND ----------

# View the physical execution plan for our filtered DataFrame
df_filtered.explain(mode="simple")

# For more detail, use mode="extended" or mode="formatted"
# df_filtered.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Quick Tour of dbutils
# MAGIC
# MAGIC `dbutils` is a Databricks-specific utility library. We will cover it in
# MAGIC detail in the Notebook Fundamentals topic, but here is a quick preview.

# COMMAND ----------

# List the top-level modules available in dbutils
dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Clean Up
# MAGIC
# MAGIC This notebook created only in-memory DataFrames, so there is nothing
# MAGIC to clean up. No tables or files were written.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Next:** Import `02-cluster-management_notebook.py` to learn how
# MAGIC clusters work and how to configure them effectively.
