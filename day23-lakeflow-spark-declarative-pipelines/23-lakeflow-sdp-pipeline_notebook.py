# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Lakeflow Spark Declarative Pipelines (Modern Era)
# MAGIC > Module 23 — Evolution Notebook 3 of 3 | Level: Intermediate | Time: 25 min
# MAGIC
# MAGIC ## The Full Evolution: From Manual Spark to Lakeflow SDP
# MAGIC
# MAGIC **Lakeflow** is Databricks' unified data engineering platform with three components:
# MAGIC
# MAGIC | Component | Purpose |
# MAGIC |-----------|---------|
# MAGIC | **Lakeflow Connect** | Data ingestion from external sources (Day 22) |
# MAGIC | **Lakeflow Spark Declarative Pipelines** | Data transformation (Day 23) -- you are here |
# MAGIC | **Lakeflow Jobs** | Orchestration and scheduling (Day 24) |
# MAGIC
# MAGIC ### What changed from DLT to Lakeflow SDP?
# MAGIC
# MAGIC Spark Declarative Pipelines (SDP) is the **evolution** of Delta Live Tables.
# MAGIC The biggest shift: SDP is now part of **Apache Spark itself**, not a
# MAGIC Databricks-only module. This means:
# MAGIC
# MAGIC - Standard PySpark API: `from pyspark import pipelines as dp`
# MAGIC - Portable across any Spark environment
# MAGIC - Better IDE support and local testing
# MAGIC - New capabilities: materialized views, auto CDC flow, graph pre-validation
# MAGIC
# MAGIC ### The Kitchen Analogy
# MAGIC
# MAGIC > **Without Lakeflow**: Manually cooking each dish — you manage the stove,
# MAGIC > the timing, the ingredients, and the plating yourself.
# MAGIC >
# MAGIC > **With Lakeflow**: Define the menu, and the kitchen handles cooking,
# MAGIC > timing, and serving automatically.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: What Changed from DLT to Lakeflow SDP
# MAGIC
# MAGIC | Feature | DLT (old) | Lakeflow SDP (new) |
# MAGIC |---------|-----------|-------------------|
# MAGIC | Import | `import dlt` | `from pyspark import pipelines as dp` |
# MAGIC | Table decorator | `@dlt.table` | `@dp.table` |
# MAGIC | View decorator | `@dlt.view` | `@dp.view` |
# MAGIC | Materialized view | `@dlt.table` | `@dp.materialized_view` |
# MAGIC | Read stream | `dlt.read_stream()` | `spark.readStream.table()` |
# MAGIC | Read batch | `dlt.read()` | `spark.read.table()` |
# MAGIC | CDC | `dlt.apply_changes()` | `dp.create_auto_cdc_flow()` |
# MAGIC | Expectations | `@dlt.expect` | `@dp.expect` |
# MAGIC | Streaming table | `dlt.create_streaming_table()` | `dp.create_streaming_table()` |
# MAGIC | Part of | Databricks-only | Apache Spark (open source) |
# MAGIC
# MAGIC The mental model is the same (declarative, expectations, DAG), but the
# MAGIC implementation is now **standard PySpark** rather than a proprietary module.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Bronze Layer with Lakeflow SDP
# MAGIC
# MAGIC Notice the import: `from pyspark import pipelines as dp`. This is a
# MAGIC standard PySpark module, not a Databricks-only library.

# COMMAND ----------

from pyspark import pipelines as dp
import pyspark.sql.functions as F

# COMMAND ----------

@dp.table(
    name="ecommerce.bronze.orders",
    comment="Raw orders ingested from S3 via Auto Loader",
    table_properties={
        "quality": "bronze",
        "delta.enableChangeDataFeed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    },
)
def orders_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load("s3://ecommerce-lakehouse/data-store/orders/")
        .withColumn("file_name", F.input_file_name())
        .withColumn("ingest_datetime", F.current_timestamp())
    )

# Standard PySpark API — works in open-source Spark too
# No writeStream, no checkpoint, no trigger — same as DLT
# NEW: schemaEvolutionMode "rescue" captures unexpected columns in _rescued_data
# NEW: table_properties set directly in the decorator (CDF, auto-optimize)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Silver Layer with Expectations + Staging View
# MAGIC
# MAGIC Lakeflow SDP introduces the **staging view pattern**: separate data quality
# MAGIC checks from CDC logic using a `@dp.view`. The view applies expectations,
# MAGIC and the downstream CDC flow consumes the validated data.
# MAGIC
# MAGIC This is cleaner than DLT, where quality checks and CDC were often mixed
# MAGIC together in a single table definition.

# COMMAND ----------

@dp.view(
    name="orders_staging",
    comment="Staging view with data quality checks applied before CDC",
)
@dp.expect("valid_amount", "order_amount > 0")
@dp.expect_or_drop("valid_rating", "customer_rating BETWEEN 1 AND 5")
@dp.expect_or_drop("valid_date", "year(order_date) >= 2020")
def orders_staging():
    return (
        spark.readStream.table("ecommerce.bronze.orders")
        .select(
            F.col("order_id"),
            F.col("order_date").cast("date"),
            F.col("store_id"),
            F.col("customer_type"),
            F.col("order_amount").cast("double"),
            F.col("items_count").cast("int"),
            F.col("customer_rating").cast("int"),
            F.col("ingest_datetime").alias("bronze_ingest_datetime"),
        )
        .withColumn("silver_processed_timestamp", F.current_timestamp())
    )

# Staging view pattern — separate quality checks from CDC logic
# Views are not materialized — they are logical transformations
# spark.readStream.table() replaces dlt.read_stream() — standard PySpark API

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Auto CDC Flow (SCD Type 1)
# MAGIC
# MAGIC `dp.create_auto_cdc_flow()` replaces `dlt.apply_changes()`. The concept
# MAGIC is identical, but the naming is clearer: it is an **automatic CDC flow**
# MAGIC that handles inserts, updates, and deletes with no manual MERGE code.

# COMMAND ----------

dp.create_streaming_table(
    name="ecommerce.silver.orders",
    comment="Cleansed orders with CDC applied (SCD Type 1)",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
    },
)

dp.create_auto_cdc_flow(
    name="orders_cdc",
    target="ecommerce.silver.orders",
    source="orders_staging",
    keys=["order_id"],
    sequence_by="bronze_ingest_datetime",
    stored_as_scd_type=1,
)

# Auto CDC — no manual MERGE, handles inserts/updates/deletes
# sequence_by ensures the latest event wins when duplicates arrive
# SCD Type 2 is just: stored_as_scd_type=2 (adds __START_AT, __END_AT columns)
# The staging view feeds clean data into CDC — separation of concerns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Gold Layer with Materialized View
# MAGIC
# MAGIC Lakeflow SDP introduces `@dp.materialized_view` as a distinct concept
# MAGIC from `@dp.table`. In DLT, both streaming tables and materialized views
# MAGIC used `@dlt.table` — which was confusing.
# MAGIC
# MAGIC A **materialized view** is auto-refreshed when its upstream data changes.
# MAGIC It reads batch (not streaming) and recomputes as needed.

# COMMAND ----------

@dp.materialized_view(
    name="ecommerce.gold.daily_revenue",
    comment="Daily revenue per store — auto-refreshed when silver data changes",
    table_properties={"quality": "gold"},
)
def daily_revenue():
    return (
        spark.read.table("ecommerce.silver.orders")
        .groupBy("store_id", "order_date")
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("customer_rating").alias("avg_rating"),
        )
    )

# @dp.materialized_view — distinct from @dp.table (clearer semantics)
# spark.read.table() replaces dlt.read() — standard PySpark API
# Auto-refreshed: when silver.orders changes, this view recomputes
# No manual overwrite/append decisions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Full Evolution Summary
# MAGIC
# MAGIC | Capability | Traditional Spark | DLT | Lakeflow SDP |
# MAGIC |-----------|-----------------|-----|-------------|
# MAGIC | Coding approach | Imperative (how) | Declarative (what) | Declarative (what) |
# MAGIC | Checkpoint management | Manual | Automatic | Automatic |
# MAGIC | Data quality | Manual filters | Expectations | Expectations |
# MAGIC | Pipeline DAG | None | Auto-created | Auto-created |
# MAGIC | CDC | Manual MERGE | `apply_changes` | Auto CDC Flow |
# MAGIC | Orchestration | Manual scripts | Limited | Lakeflow Jobs |
# MAGIC | Multi-pipeline | Separate jobs | Not native | Native (Lakeflow) |
# MAGIC | Open source | Yes (Spark) | No (Databricks-only) | Yes (Apache Spark) |
# MAGIC | Schema evolution | Manual | Supported | Supported + rescue |
# MAGIC | State management | Manual checkpoints | Automatic | Automatic |
# MAGIC | Pre-validation | None | Basic | Full graph analysis |
# MAGIC | Parallelization | Manual threading | Automatic | Automatic |
# MAGIC | Materialized views | N/A | `@dlt.table` (overloaded) | `@dp.materialized_view` (distinct) |
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### The Teaching Analogy
# MAGIC
# MAGIC ```
# MAGIC Traditional Spark = Cooking manually
# MAGIC                     (you manage the stove, timing, ingredients, plating)
# MAGIC
# MAGIC DLT              = Smart cooking assistant
# MAGIC                     (handles timing, quality checks, and coordination)
# MAGIC
# MAGIC Lakeflow SDP     = Full restaurant kitchen system
# MAGIC                     (manages the entire operation: menu, kitchen, dining room)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: What Makes Lakeflow a Scalable Data Platform
# MAGIC
# MAGIC "Scalable" means your platform grows with your business — without becoming
# MAGIC slower, more complex, or harder to manage. Lakeflow achieves scalability
# MAGIC across four dimensions:
# MAGIC
# MAGIC ### 1. Data Scalability
# MAGIC - Handles gigabytes to petabytes with the same pipeline code
# MAGIC - Auto Loader processes files incrementally (no full scans)
# MAGIC - Delta Lake provides ACID transactions at any scale
# MAGIC - Materialized views recompute only what changed
# MAGIC
# MAGIC ### 2. Compute Scalability
# MAGIC - Auto-scaling clusters adjust to workload size
# MAGIC - Serverless pipelines eliminate cluster management
# MAGIC - Independent tables in the DAG process in parallel automatically
# MAGIC - No manual threading or executor tuning
# MAGIC
# MAGIC ### 3. Pipeline Scalability
# MAGIC - From 2 tables to 50+ tables without architectural changes
# MAGIC - DAG visualization keeps complex pipelines understandable
# MAGIC - Expectations provide quality monitoring without custom dashboards
# MAGIC - Auto CDC Flow handles any number of CDC tables declaratively
# MAGIC - Pre-validation catches errors before pipeline execution
# MAGIC
# MAGIC ### 4. Team Scalability
# MAGIC - Data engineers define pipelines with `@dp.table`
# MAGIC - Data analysts query gold tables with SQL
# MAGIC - Data scientists read from silver/gold with standard PySpark
# MAGIC - Platform engineers manage infrastructure with Lakeflow Jobs
# MAGIC - Standard PySpark API means no proprietary knowledge required

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: The Complete Lakeflow Ecosystem
# MAGIC
# MAGIC Lakeflow is not just one tool — it is a platform that connects three
# MAGIC components into a unified data engineering experience:
# MAGIC
# MAGIC ```
# MAGIC +---------------------+     +---------------------+     +---------------------+
# MAGIC |  Lakeflow Connect   |     |    Lakeflow SDP     |     |   Lakeflow Jobs     |
# MAGIC |                     |     |                     |     |                     |
# MAGIC |  Data Ingestion     | --> |  Data Transformation| --> |  Orchestration      |
# MAGIC |  (Day 22)           |     |  (Day 23)           |     |  (Day 24)           |
# MAGIC |                     |     |  <-- you are here   |     |                     |
# MAGIC |  - SaaS connectors  |     |  - @dp.table        |     |  - Scheduling       |
# MAGIC |  - Database CDC     |     |  - @dp.view         |     |  - Dependencies     |
# MAGIC |  - File ingestion   |     |  - @dp.expect       |     |  - Monitoring       |
# MAGIC |  - API sources      |     |  - Auto CDC Flow    |     |  - Alerting         |
# MAGIC +---------------------+     +---------------------+     +---------------------+
# MAGIC ```
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **DLT made pipelines easy.**
# MAGIC **Lakeflow makes entire data platforms scalable and manageable.**
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Next Steps
# MAGIC - **Day 24**: Lakeflow Jobs — orchestrate multiple SDP pipelines into
# MAGIC   production workflows with dependencies, retries, and monitoring.
