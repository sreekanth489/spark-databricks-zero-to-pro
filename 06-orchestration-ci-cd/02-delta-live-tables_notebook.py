# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Live Tables (DLT) — Hands-On Notebook
# MAGIC > Module 06 — Topic 02 | Orchestration & CI/CD
# MAGIC
# MAGIC This notebook demonstrates DLT concepts through:
# MAGIC 1. DLT syntax examples (reference code — DLT requires a pipeline to execute)
# MAGIC 2. Expectations for data quality
# MAGIC 3. Medallion architecture patterns
# MAGIC 4. Event log queries for monitoring
# MAGIC
# MAGIC **Important**: DLT code cannot run interactively. Cells marked `[REFERENCE]`
# MAGIC show the syntax you would use in a DLT pipeline notebook. Cells marked
# MAGIC `[RUNNABLE]` simulate the concepts using standard PySpark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: DLT Bronze Layer — Auto Loader Ingestion [REFERENCE]
# MAGIC This is how you define a streaming bronze table that ingests raw JSON files.

# COMMAND ----------

# -- REFERENCE CODE (runs inside a DLT pipeline only) --
#
# import dlt
# from pyspark.sql.functions import current_timestamp, input_file_name
#
# @dlt.table(
#     name="bronze_sales",
#     comment="Raw sales data ingested from landing zone via Auto Loader",
#     table_properties={
#         "quality": "bronze",
#         "pipelines.autoOptimize.managed": "true"
#     }
# )
# def bronze_sales():
#     return (
#         spark.readStream
#             .format("cloudFiles")
#             .option("cloudFiles.format", "json")
#             .option("cloudFiles.inferColumnTypes", "true")
#             .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
#             .load("/mnt/landing/sales/")
#             .withColumn("_ingestion_timestamp", current_timestamp())
#             .withColumn("_source_file", input_file_name())
#     )

print("Cell 1 is DLT reference code. Deploy as a DLT pipeline to execute.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: DLT Silver Layer — Expectations for Data Quality [REFERENCE]
# MAGIC Expectations are the core data quality feature of DLT.

# COMMAND ----------

# -- REFERENCE CODE (runs inside a DLT pipeline only) --
#
# import dlt
# from pyspark.sql.functions import col
#
# @dlt.table(
#     name="silver_sales",
#     comment="Cleaned and validated sales records"
# )
# @dlt.expect("valid_quantity", "quantity > 0")
# @dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
# @dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
# def silver_sales():
#     return (
#         dlt.read_stream("bronze_sales")
#             .where("order_date IS NOT NULL")
#             .select(
#                 col("order_id").cast("long"),
#                 col("order_date").cast("date"),
#                 col("product"),
#                 col("price").cast("double"),
#                 col("quantity").cast("int"),
#                 (col("price") * col("quantity")).alias("amount"),
#                 col("region"),
#                 col("_ingestion_timestamp")
#             )
#             .dropDuplicates(["order_id"])
#     )

print("Cell 2 is DLT reference code. Shows three expectation types:")
print("  @dlt.expect          -> ALLOW (log violation, keep record)")
print("  @dlt.expect_or_drop  -> DROP  (remove violating records)")
print("  @dlt.expect_or_fail  -> FAIL  (halt pipeline on violation)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: DLT Gold Layer — Business Aggregation [REFERENCE]
# MAGIC Gold tables are typically batch (not streaming) since they aggregate.

# COMMAND ----------

# -- REFERENCE CODE (runs inside a DLT pipeline only) --
#
# import dlt
# from pyspark.sql.functions import sum, count, avg
#
# @dlt.table(
#     name="gold_daily_revenue",
#     comment="Daily revenue summary by region for dashboards"
# )
# @dlt.expect_or_fail("has_revenue", "total_revenue > 0")
# def gold_daily_revenue():
#     return (
#         dlt.read("silver_sales")      # batch read (full recompute)
#             .groupBy("order_date", "region")
#             .agg(
#                 sum("amount").alias("total_revenue"),
#                 count("order_id").alias("order_count"),
#                 avg("amount").alias("avg_order_value")
#             )
#     )

print("Cell 3 is DLT reference code.")
print("Note: Gold layer uses dlt.read() (batch), not dlt.read_stream().")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Simulate Expectations with Standard PySpark [RUNNABLE]
# MAGIC Let's demonstrate what DLT expectations do under the hood.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Sample data with quality issues
data = [
    (1, "2024-01-15", "Widget A", 29.99, 10, "West"),
    (2, "2024-01-15", "Widget B", None,   5,  "East"),    # price is NULL -> DROP
    (3, "2024-01-15", "Widget C", 19.99, -3,  "West"),    # quantity <= 0 -> ALLOW (log it)
    (4, "2024-01-15", "Widget D", 99.99,  2,  "East"),
    (None, "2024-01-15", "Widget E", 14.99, 50, "West"),  # order_id NULL -> FAIL
]
columns = ["order_id", "order_date", "product", "price", "quantity", "region"]
df = spark.createDataFrame(data, schema=columns)

print("=== Raw Data ===")
df.show()

# Simulate @dlt.expect("valid_quantity", "quantity > 0") -> ALLOW
quality_quantity = df.filter(F.col("quantity") > 0).count()
total_rows = df.count()
print(f"[ALLOW] valid_quantity: {quality_quantity}/{total_rows} rows pass")

# Simulate @dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
df_after_drop = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))
dropped = total_rows - df_after_drop.count()
print(f"[DROP]  valid_price: dropped {dropped} row(s)")

# Simulate @dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
null_ids = df.filter(F.col("order_id").isNull()).count()
if null_ids > 0:
    print(f"[FAIL]  valid_order_id: {null_ids} NULL order_id(s) detected — pipeline would FAIL")
else:
    print("[FAIL]  valid_order_id: all rows pass")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Simulate the Medallion Architecture [RUNNABLE]
# MAGIC Building Bronze -> Silver -> Gold in standard PySpark.

# COMMAND ----------

# BRONZE: Raw data as-is
bronze_data = [
    (1, "2024-01-15", "Widget A", 29.99, 10, "West"),
    (2, "2024-01-15", "Widget B", 49.99, 5,  "East"),
    (3, "2024-01-16", "Widget A", 29.99, 8,  "West"),
    (4, "2024-01-16", "Widget C", 19.99, 25, "East"),
    (5, "2024-01-16", "Widget B", 49.99, 3,  "West"),
    (6, "2024-01-17", "Widget A", 29.99, 15, "East"),
    (7, "2024-01-17", "Widget D", 99.99, 2,  "West"),
]
columns = ["order_id", "order_date", "product", "price", "quantity", "region"]
df_bronze = spark.createDataFrame(bronze_data, schema=columns)
print("=== BRONZE (raw) ===")
df_bronze.show()

# SILVER: Cleaned with derived columns
df_silver = (
    df_bronze
    .filter(F.col("price").isNotNull() & (F.col("price") > 0))
    .filter(F.col("quantity") > 0)
    .withColumn("amount", F.col("price") * F.col("quantity"))
    .dropDuplicates(["order_id"])
)
print("=== SILVER (cleaned) ===")
df_silver.show()

# GOLD: Business aggregation
df_gold = (
    df_silver
    .groupBy("order_date", "region")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.round(F.avg("amount"), 2).alias("avg_order_value")
    )
    .orderBy("order_date", "region")
)
print("=== GOLD (aggregated) ===")
df_gold.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: DLT Pipeline Configuration [REFERENCE]
# MAGIC JSON configuration for creating a DLT pipeline via the API.

# COMMAND ----------

# -- REFERENCE: DLT Pipeline configuration (JSON) --
pipeline_config = {
    "name": "sales-etl-pipeline",
    "target": "sales_dlt",
    "storage": "/mnt/dlt/sales",
    "configuration": {
        "pipelines.trigger.interval": "1 hour"
    },
    "clusters": [
        {
            "label": "default",
            "autoscale": {
                "min_workers": 1,
                "max_workers": 5,
                "mode": "ENHANCED"       # DLT-specific autoscaling
            }
        }
    ],
    "libraries": [
        {"notebook": {"path": "/Repos/prod/project/dlt_sales_pipeline"}}
    ],
    "continuous": False,                  # Triggered mode (batch)
    "development": False,                 # Production mode
    "channel": "CURRENT"                  # Use current DLT runtime
}

print("DLT Pipeline Configuration:")
for key, value in pipeline_config.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Multiple Expectations Syntax [REFERENCE]
# MAGIC You can define multiple expectations as a dictionary.

# COMMAND ----------

# -- REFERENCE CODE (DLT pipeline only) --
#
# # Single expectation
# @dlt.expect("valid_email", "email IS NOT NULL AND email LIKE '%@%'")
#
# # Multiple expectations using expect_all
# rules = {
#     "valid_quantity": "quantity > 0",
#     "valid_price": "price > 0",
#     "valid_date": "order_date >= '2020-01-01'"
# }
#
# @dlt.table
# @dlt.expect_all(rules)            # ALLOW all
# def silver_with_all_rules():
#     return dlt.read_stream("bronze_sales")
#
# @dlt.table
# @dlt.expect_all_or_drop(rules)    # DROP any violation
# def silver_strict_drop():
#     return dlt.read_stream("bronze_sales")
#
# @dlt.table
# @dlt.expect_all_or_fail(rules)    # FAIL on any violation
# def silver_strict_fail():
#     return dlt.read_stream("bronze_sales")

print("Cell 7 shows three bulk-expectation decorators:")
print("  @dlt.expect_all         -> ALLOW all violations")
print("  @dlt.expect_all_or_drop -> DROP rows with any violation")
print("  @dlt.expect_all_or_fail -> FAIL pipeline on any violation")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Query the DLT Event Log [REFERENCE]
# MAGIC The event log is a Delta table that records everything about pipeline runs.

# COMMAND ----------

# -- REFERENCE SQL (run in a SQL cell after pipeline has executed) --
#
# -- Data quality metrics
# SELECT
#     details:flow_name AS table_name,
#     details:data_quality.expectations AS expectations,
#     details:data_quality.num_records AS total_records,
#     details:data_quality.num_output_records AS passed_records,
#     timestamp
# FROM event_log(TABLE(sales_dlt.silver_sales))
# WHERE event_type = 'flow_progress'
# ORDER BY timestamp DESC
# LIMIT 10;
#
# -- Pipeline lineage
# SELECT
#     details:flow_name AS table_name,
#     details:flow_progress.data_quality.expectations AS quality_rules,
#     details:flow_progress.metrics.num_output_rows AS rows_written,
#     details:flow_progress.status AS run_status,
#     timestamp
# FROM event_log(TABLE(sales_dlt.bronze_sales))
# WHERE event_type = 'flow_progress'
# ORDER BY timestamp DESC;

print("Cell 8 shows SQL queries for the DLT event log.")
print("Run these after a DLT pipeline has completed at least one update.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Live Table vs Streaming Live Table Summary [RUNNABLE]

# COMMAND ----------

comparison = [
    ("Processing Model", "Full recompute (batch)", "Incremental (streaming)"),
    ("Input Function",   "dlt.read('table')",      "dlt.read_stream('table')"),
    ("Output",           "Overwritten each run",    "Appended incrementally"),
    ("Use Case",         "Aggregations, SCD",       "Event streams, logs"),
    ("Reprocessing",     "Automatic (full table)",  "Only new records"),
    ("Checkpoint",       "Not needed",              "Managed by DLT"),
]

print(f"{'Feature':<20} {'Live Table':<28} {'Streaming Live Table'}")
print("-" * 75)
for feature, live, streaming in comparison:
    print(f"{feature:<20} {live:<28} {streaming}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Cleanup [RUNNABLE]

# COMMAND ----------

# No persistent resources created in this notebook
# Bronze/Silver/Gold DataFrames are in-memory only and will be garbage collected
print("Cleanup complete. No persistent resources were created.")
print()
print("Next Steps:")
print("  1. Create a DLT pipeline in your workspace using the reference code above")
print("  2. Run the pipeline in Development mode first")
print("  3. Query the event log to see expectation results")
print("  4. Switch to Production mode for scheduled runs")
