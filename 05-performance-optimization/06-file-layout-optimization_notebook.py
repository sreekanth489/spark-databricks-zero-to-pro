# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 - File Layout Optimization
# MAGIC > Module 05 — Topic 06 | OPTIMIZE, Z-ORDER, VACUUM, auto-optimize
# MAGIC
# MAGIC **What you will learn:**
# MAGIC 1. The small file problem and how OPTIMIZE fixes it
# MAGIC 2. Z-ORDER for multi-dimensional data skipping
# MAGIC 3. VACUUM to reclaim storage and reduce cloud bills
# MAGIC 4. Auto-optimize settings (optimizedWrite + autoCompact)
# MAGIC 5. Measuring scan performance before and after optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: Create a Delta Table with Many Small Files

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import random
import time

studios = ["Warner Bros", "Disney", "Universal", "Paramount", "Sony", "Lionsgate", "MGM", "Fox"]
genres = ["Action", "Comedy", "Drama", "Sci-Fi", "Horror", "Animation", "Thriller", "Romance"]

base_path = "/tmp/perf_module/file_layout"
table_path = f"{base_path}/movies_delta"

# Generate data
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

# Write in small batches to simulate many small files
# Repartition to many partitions to create many small files
movies_df.repartition(200).write.format("delta").mode("overwrite").save(table_path)
print(f"Wrote {num_rows:,} rows to {table_path}")

# Register as a table for SQL commands
spark.sql(f"CREATE TABLE IF NOT EXISTS movies_file_opt USING DELTA LOCATION '{table_path}'")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Inspect the Small File Problem

# COMMAND ----------

# Count files before OPTIMIZE
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, table_path)

# Get file count from the Delta log
file_count_before = spark.read.format("delta").load(table_path).inputFiles()
print(f"Files BEFORE OPTIMIZE: {len(file_count_before)}")

# Show some file sizes (approximate)
files_df = spark.createDataFrame(
    [(f.split("/")[-1],) for f in file_count_before[:10]],
    ["filename"]
)
files_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. OPTIMIZE: Bin-Pack Small Files

# COMMAND ----------

# Measure scan time BEFORE optimize
start = time.time()
spark.read.format("delta").load(table_path).filter(
    F.col("studio") == "Disney"
).count()
scan_before = time.time() - start
print(f"Scan time BEFORE OPTIMIZE: {scan_before:.2f}s")

# COMMAND ----------

# Run OPTIMIZE -- compacts small files into larger ones
print("Running OPTIMIZE (bin-packing)...")
spark.sql(f"OPTIMIZE movies_file_opt")

# COMMAND ----------

# Count files after OPTIMIZE
file_count_after = spark.read.format("delta").load(table_path).inputFiles()
print(f"Files BEFORE OPTIMIZE: {len(file_count_before)}")
print(f"Files AFTER OPTIMIZE:  {len(file_count_after)}")

# Measure scan time AFTER optimize
start = time.time()
spark.read.format("delta").load(table_path).filter(
    F.col("studio") == "Disney"
).count()
scan_after = time.time() - start
print(f"\nScan time BEFORE OPTIMIZE: {scan_before:.2f}s")
print(f"Scan time AFTER OPTIMIZE:  {scan_after:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Z-ORDER: Co-locate Data for Data Skipping
# MAGIC
# MAGIC Z-ORDER rearranges data so rows with similar column values are stored together.
# MAGIC This enables Spark to skip entire files when filtering.
# MAGIC
# MAGIC **Delta tables with Z-ORDER compress smaller, take less on your cloud bill.**

# COMMAND ----------

# Scan BEFORE Z-ORDER: filtering on studio scans many files
spark.read.format("delta").load(table_path).filter(
    F.col("studio") == "Disney"
).explain("formatted")

# COMMAND ----------

# Apply Z-ORDER on studio column
print("Running OPTIMIZE with Z-ORDER BY (studio)...")
spark.sql(f"OPTIMIZE movies_file_opt ZORDER BY (studio)")

# COMMAND ----------

# Scan AFTER Z-ORDER: should skip more files
start = time.time()
disney_count = spark.read.format("delta").load(table_path).filter(
    F.col("studio") == "Disney"
).count()
zorder_time = time.time() - start

print(f"Disney movies: {disney_count}")
print(f"Scan time with Z-ORDER: {zorder_time:.2f}s")
print(f"Scan time before Z-ORDER: {scan_before:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Multi-Column Z-ORDER
# MAGIC
# MAGIC You can Z-ORDER on multiple columns. The first column gets the strongest
# MAGIC clustering effect. Use this when you filter on combinations of columns.

# COMMAND ----------

print("Running OPTIMIZE with Z-ORDER BY (studio, release_year)...")
spark.sql(f"OPTIMIZE movies_file_opt ZORDER BY (studio, release_year)")

# Query filtering on both columns -- maximum benefit from Z-ORDER
start = time.time()
multi_filter_count = spark.read.format("delta").load(table_path).filter(
    (F.col("studio") == "Disney") & (F.col("release_year") >= 2020)
).count()
multi_zorder_time = time.time() - start

print(f"Disney movies since 2020: {multi_filter_count}")
print(f"Scan time with multi-column Z-ORDER: {multi_zorder_time:.2f}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. VACUUM: Remove Stale Data
# MAGIC
# MAGIC OPTIMIZE and Z-ORDER create new files but keep old ones for time travel.
# MAGIC VACUUM removes files older than the retention period.
# MAGIC
# MAGIC **Use OPTIMIZE to compact for performance, VACUUM to remove data you no longer
# MAGIC need for cost savings.**

# COMMAND ----------

# Check table history to see all operations
spark.sql("DESCRIBE HISTORY movies_file_opt").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# VACUUM with 0 hours retention (for demo only -- normally use 7+ days)
# This requires disabling the safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

print("Running VACUUM (removing old file versions)...")
spark.sql("VACUUM movies_file_opt RETAIN 0 HOURS")

# Re-enable safety check
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

# Count files after VACUUM -- old versions removed
file_count_vacuum = spark.read.format("delta").load(table_path).inputFiles()
print(f"\nFiles AFTER VACUUM: {len(file_count_vacuum)}")
print(f"(Old file versions have been removed, reducing storage cost)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Auto-Optimize: Prevent Small Files at Write Time

# COMMAND ----------

# Enable auto-optimize on the table
spark.sql("""
    ALTER TABLE movies_file_opt SET TBLPROPERTIES (
        'delta.autoOptimize.optimizeWrite' = 'true',
        'delta.autoOptimize.autoCompact' = 'true'
    )
""")

# Now writes will automatically optimize file sizes
print("Auto-optimize enabled:")
print("  optimizeWrite: repartitions data before writing to reduce small files")
print("  autoCompact: runs mini-OPTIMIZE after each write")

# Simulate a write -- it will auto-optimize
small_batch = spark.createDataFrame(
    [(9999999, "Auto_Optimized_Movie", "Disney", "Drama", 2024, 8.5, 100000000)],
    schema=schema,
)
small_batch.write.format("delta").mode("append").save(table_path)
print("\nAppended 1 row -- autoCompact will handle file consolidation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data Skipping Statistics
# MAGIC
# MAGIC Delta Lake collects min/max statistics for the first 32 columns.
# MAGIC Let's verify data skipping is working.

# COMMAND ----------

# Show table details including data skipping stats
spark.sql("DESCRIBE DETAIL movies_file_opt").select(
    "format", "numFiles", "sizeInBytes"
).show()

# A filtered query with Z-ORDER should scan fewer files than without
explain_output = spark.read.format("delta").load(table_path).filter(
    F.col("studio") == "Disney"
)
explain_output.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC | Technique | Purpose | Frequency |
# MAGIC |-----------|---------|-----------|
# MAGIC | OPTIMIZE | Compact small files (bin-pack) | Daily/weekly |
# MAGIC | Z-ORDER | Co-locate data for data skipping | With OPTIMIZE |
# MAGIC | VACUUM | Remove old file versions | Weekly |
# MAGIC | optimizedWrite | Reduce small files at write time | Always on |
# MAGIC | autoCompact | Auto-OPTIMIZE after writes | Always on |
# MAGIC
# MAGIC **Cost Optimization** = OPTIMIZE (fewer API calls) + Z-ORDER (less data scanned)
# MAGIC + VACUUM (less storage) + Auto-optimize (prevent the problem)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS movies_file_opt")
dbutils.fs.rm(base_path, recurse=True)
print("Cleanup complete.")
