# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Optimization: OPTIMIZE, Z-ORDER, VACUUM & Liquid Clustering
# MAGIC > Module 03 -- Topic 05 | Companion Notebook
# MAGIC
# MAGIC In this notebook you will:
# MAGIC 1. Create a table with many small files
# MAGIC 2. Run OPTIMIZE to compact files (bin-packing)
# MAGIC 3. Apply Z-ORDER and observe data skipping
# MAGIC 4. Run VACUUM (dry run) to identify stale files
# MAGIC 5. Demonstrate Liquid Clustering syntax
# MAGIC 6. Inspect data skipping statistics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup -- Generate a Table with Many Small Files

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, DateType
)
import random

spark.sql("CREATE DATABASE IF NOT EXISTS module03")
spark.sql("DROP TABLE IF EXISTS module03.optimize_demo")

TABLE_PATH = "/tmp/module03/optimize_demo"
dbutils.fs.rm(TABLE_PATH, recurse=True)

# Generate 100K rows of synthetic sales data
regions = ["US-West", "US-East", "US-Central", "EU-West", "EU-East", "APAC"]
products = ["Laptop", "Phone", "Tablet", "Monitor", "Keyboard", "Mouse",
            "Headphones", "Camera", "Speaker", "Charger"]

data = []
for i in range(100000):
    data.append((
        i + 1,
        random.choice(regions),
        random.choice(products),
        round(random.uniform(9.99, 1999.99), 2),
        random.randint(1, 10),
        f"2025-01-{random.randint(1, 28):02d}",
    ))

schema = StructType([
    StructField("sale_id", IntegerType()),
    StructField("region", StringType()),
    StructField("product", StringType()),
    StructField("price", DoubleType()),
    StructField("quantity", IntegerType()),
    StructField("sale_date", StringType()),
])

df = spark.createDataFrame(data, schema=schema)

# Force many small files by repartitioning to 50 partitions
df.repartition(50).write.format("delta").mode("overwrite").save(TABLE_PATH)

# Register as a table
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS module03.optimize_demo
    USING DELTA LOCATION '{TABLE_PATH}'
""")

# Count files
file_count = len([f for f in dbutils.fs.ls(TABLE_PATH) if f.name.endswith(".parquet")])
print(f"Initial file count: {file_count}")
print(f"Row count: {spark.table('module03.optimize_demo').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1 -- Observe the Small File Problem
# MAGIC
# MAGIC With 50 small files, every query must open all of them.

# COMMAND ----------

# Query that should benefit from data skipping (once optimized)
spark.table("module03.optimize_demo").filter("region = 'US-West'").count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2 -- Run OPTIMIZE (Bin-Packing)
# MAGIC
# MAGIC OPTIMIZE compacts the 50 small files into fewer, larger files.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE module03.optimize_demo

# COMMAND ----------

# Count files after OPTIMIZE
# Note: old files are still on disk (marked as removed in the log)
all_files = dbutils.fs.ls(TABLE_PATH)
parquet_files = [f for f in all_files if f.name.endswith(".parquet")]
print(f"Total Parquet files on disk (old + new): {len(parquet_files)}")

# Check the current version's file count via DESCRIBE DETAIL
detail = spark.sql("DESCRIBE DETAIL module03.optimize_demo").collect()[0]
print(f"Number of files (current version): {detail['numFiles']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3 -- Apply Z-ORDER for Data Skipping
# MAGIC
# MAGIC Z-ORDER co-locates rows with similar `region` and `product` values,
# MAGIC so queries filtering on those columns skip irrelevant files.

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE module03.optimize_demo
# MAGIC ZORDER BY (region, product)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Measure Data Skipping Effect
# MAGIC
# MAGIC After Z-ORDER, a query on region should read far fewer files.

# COMMAND ----------

# Enable query plan display to see data skipping metrics
spark.conf.set("spark.databricks.delta.stats.skipping", "true")

# Query with filter on Z-ordered column
result = spark.table("module03.optimize_demo").filter(
    "region = 'US-West' AND product = 'Laptop'"
)
print(f"Rows matching region='US-West' AND product='Laptop': {result.count()}")

# Check the explain plan for scan info
result.explain(True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4 -- VACUUM (Remove Stale Files)
# MAGIC
# MAGIC Old files from before OPTIMIZE are still on disk. VACUUM removes them.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Dry run first: see what would be deleted
# MAGIC VACUUM module03.optimize_demo DRY RUN

# COMMAND ----------

# To actually vacuum with retention < 7 days (for demo only!):
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM with 0 hours retention (DEMO ONLY -- never do this in production!)
# MAGIC VACUUM module03.optimize_demo RETAIN 0 HOURS

# COMMAND ----------

# Count files after vacuum -- old files are gone
parquet_after = [f for f in dbutils.fs.ls(TABLE_PATH) if f.name.endswith(".parquet")]
print(f"Parquet files after VACUUM: {len(parquet_after)}")

# Reset safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5 -- Liquid Clustering (Databricks Recommended)
# MAGIC
# MAGIC Liquid Clustering replaces partitioning + Z-ORDER with a simpler,
# MAGIC incremental approach. Let's demonstrate the syntax.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a new table with Liquid Clustering
# MAGIC DROP TABLE IF EXISTS module03.liquid_demo;
# MAGIC
# MAGIC CREATE TABLE module03.liquid_demo (
# MAGIC   sale_id INT,
# MAGIC   region STRING,
# MAGIC   product STRING,
# MAGIC   price DOUBLE,
# MAGIC   quantity INT,
# MAGIC   sale_date STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC CLUSTER BY (region, sale_date);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert data into the Liquid Clustered table
# MAGIC INSERT INTO module03.liquid_demo
# MAGIC SELECT * FROM module03.optimize_demo;
# MAGIC
# MAGIC -- Trigger clustering optimization
# MAGIC OPTIMIZE module03.liquid_demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change clustering columns -- this is a metadata-only operation!
# MAGIC ALTER TABLE module03.liquid_demo CLUSTER BY (product, region);
# MAGIC
# MAGIC DESCRIBE DETAIL module03.liquid_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6 -- Inspect Data Skipping Statistics
# MAGIC
# MAGIC Every file's min/max stats are stored in the transaction log.

# COMMAND ----------

import json

# Read the latest commit file from _delta_log
log_path = f"{TABLE_PATH}/_delta_log"
log_files = sorted([f.name for f in dbutils.fs.ls(log_path) if f.name.endswith(".json")])

if log_files:
    latest_log = f"{log_path}/{log_files[-1]}"
    print(f"Reading: {latest_log}")

    log_content = spark.read.text(latest_log).collect()
    for row in log_content[:3]:  # Show first 3 actions
        parsed = json.loads(row[0])
        action_type = list(parsed.keys())[0]
        if action_type == "add" and "stats" in parsed["add"]:
            stats = json.loads(parsed["add"]["stats"])
            print(f"\nFile: {parsed['add']['path'][:50]}...")
            print(f"  numRecords: {stats.get('numRecords')}")
            print(f"  minValues:  {stats.get('minValues', {})}")
            print(f"  maxValues:  {stats.get('maxValues', {})}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7 -- Auto-Optimize Settings
# MAGIC
# MAGIC For streaming and frequent-write tables, enable automatic optimization.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable auto-optimize on a table
# MAGIC ALTER TABLE module03.optimize_demo SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'true',
# MAGIC   'delta.autoOptimize.autoCompact' = 'true'
# MAGIC );
# MAGIC
# MAGIC -- Verify properties
# MAGIC SHOW TBLPROPERTIES module03.optimize_demo

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS module03.optimize_demo")
spark.sql("DROP TABLE IF EXISTS module03.liquid_demo")
dbutils.fs.rm("/tmp/module03/optimize_demo", recurse=True)
spark.sql("DROP DATABASE IF EXISTS module03")

print("Cleanup complete.")
