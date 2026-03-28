# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Live Tables Pipeline (DLT Era)
# MAGIC > Module 23 — Evolution Notebook 2 of 3 | Level: Intermediate | Time: 20 min
# MAGIC
# MAGIC ## How DLT Solved the Pain of Traditional Spark Pipelines
# MAGIC
# MAGIC Delta Live Tables (DLT) introduced a **declarative** approach to building
# MAGIC data pipelines. Instead of telling Spark *how* to process data (imperative),
# MAGIC you tell DLT *what* your tables should look like, and it handles the rest.
# MAGIC
# MAGIC ### What DLT brought to the table:
# MAGIC - **Declarative table definitions** — define tables as functions, not streams
# MAGIC - **Automatic DAG creation** — DLT reads your code and builds the dependency graph
# MAGIC - **Built-in expectations** — data quality rules with tracking and enforcement
# MAGIC - **No manual checkpoint management** — DLT handles all state internally
# MAGIC - **Automatic CDC** — `apply_changes()` replaces manual MERGE boilerplate
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **Pipeline**: E-commerce Orders (JSON from S3) -> Bronze -> Silver -> Gold
# MAGIC
# MAGIC **Important**: This notebook must be run as a DLT pipeline, not as a
# MAGIC standalone notebook. Configure it in Workflows > Delta Live Tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Bronze Layer with DLT
# MAGIC
# MAGIC Compare this to the traditional approach: no `writeStream`, no checkpoint
# MAGIC location, no output mode, no trigger configuration. Just define what the
# MAGIC table should contain.

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr

# COMMAND ----------

@dlt.table(
    comment="Raw orders ingested from S3 via Auto Loader",
    table_properties={
        "quality": "bronze",
    },
)
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://ecommerce-lakehouse/data-store/orders/")
        .withColumn("ingest_timestamp", F.current_timestamp())
        .withColumn("source_file", col("_metadata.file_name"))
    )

# No writeStream, no checkpoint management
# No trigger configuration, no output mode
# DLT handles ALL of that automatically

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Silver Layer with DLT + Expectations
# MAGIC
# MAGIC This is where DLT really shines. Instead of silent `filter()` calls that
# MAGIC drop records with no audit trail, DLT **expectations** track quality metrics
# MAGIC automatically. You can see exactly how many records passed or failed each rule.

# COMMAND ----------

@dlt.table(
    comment="Cleaned orders with quality checks applied",
    table_properties={
        "quality": "silver",
    },
)
@dlt.expect("valid_amount", "order_amount > 0")
@dlt.expect_or_drop("valid_date", "order_date IS NOT NULL")
@dlt.expect_or_drop("valid_year", "year(order_date) >= 2020")
@dlt.expect_or_drop("valid_rating", "customer_rating BETWEEN 1 AND 5")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
        .select(
            F.col("order_id"),
            F.col("order_date").cast("date"),
            F.col("store_id"),
            F.col("customer_type"),
            F.col("order_amount").cast("double"),
            F.col("items_count").cast("int"),
            F.col("customer_rating").cast("int"),
            F.col("ingest_timestamp"),
        )
        .withColumn("silver_processed_at", F.current_timestamp())
    )

# Built-in data quality tracking
# DLT records pass/fail counts for every expectation
# Visible in the DLT pipeline UI — no custom dashboards needed
# expect() = warn but keep the record
# expect_or_drop() = silently drop the record (but still track the count)
# expect_or_fail() = fail the entire pipeline if violated

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Gold Layer with DLT
# MAGIC
# MAGIC The gold layer uses `dlt.read()` (batch) instead of `dlt.read_stream()`
# MAGIC because aggregations are materialized views that refresh on each pipeline run.

# COMMAND ----------

@dlt.table(
    comment="Daily revenue per store — refreshed each pipeline run",
    table_properties={
        "quality": "gold",
    },
)
def gold_daily_revenue():
    return (
        dlt.read("silver_orders")
        .groupBy("store_id", "order_date")
        .agg(
            F.sum("order_amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.avg("customer_rating").alias("avg_rating"),
        )
    )

# No manual write mode decisions (overwrite vs append)
# No manual partitioning
# DLT knows this depends on silver_orders — automatic DAG edge

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: CDC with DLT
# MAGIC
# MAGIC Remember the ~50 lines of manual MERGE code from the traditional approach?
# MAGIC DLT replaces all of that with `apply_changes()` — a single declarative call
# MAGIC that handles inserts, updates, deletes, deduplication, and SCD types.

# COMMAND ----------

dlt.create_streaming_table(
    name="silver_orders_cdc",
    comment="Cleansed orders with CDC applied (SCD Type 1)",
    table_properties={
        "quality": "silver",
    },
)

dlt.apply_changes(
    target="silver_orders_cdc",
    source="bronze_orders",
    keys=["order_id"],
    sequence_by=col("ingest_timestamp"),
    apply_as_deletes=expr("operation = 'DELETE'"),
    stored_as_scd_type=1,
)

# ~50 lines of manual MERGE replaced by ~15 lines of declarative code
# Handles inserts, updates, and deletes automatically
# Deduplication via sequence_by — latest event wins
# SCD Type 2 is just: stored_as_scd_type=2

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: What DLT Solved vs What Remained
# MAGIC
# MAGIC DLT was a massive improvement, but it was not the complete story.
# MAGIC
# MAGIC ### Problems Solved by DLT
# MAGIC
# MAGIC | Problem | Traditional Spark | DLT |
# MAGIC |---------|-----------------|-----|
# MAGIC | Checkpoint management | Manual paths, easy to break | Automatic, fully managed |
# MAGIC | Data quality tracking | None (silent drops) | Expectations with metrics |
# MAGIC | Pipeline visualization | None | Auto-generated DAG |
# MAGIC | Table dependencies | Manual ordering | Auto-detected from code |
# MAGIC | CDC | ~50 lines of MERGE per table | `apply_changes()` — declarative |
# MAGIC | Schema evolution | Manual `mergeSchema` | Supported natively |
# MAGIC | Retries | Manual retry logic | Built-in with backoff |
# MAGIC | Parallelization | Manual threading | Automatic for independent tables |
# MAGIC
# MAGIC ### What DLT Did NOT Solve
# MAGIC
# MAGIC | Limitation | Impact |
# MAGIC |-----------|--------|
# MAGIC | **Databricks-only** | `import dlt` works only inside Databricks DLT pipelines |
# MAGIC | **Limited orchestration** | Cannot compose multiple DLT pipelines into a workflow natively |
# MAGIC | **Separate from Workflows** | DLT pipelines are a separate concept from Databricks Jobs |
# MAGIC | **No multi-pipeline dependencies** | Pipeline A cannot declare a dependency on Pipeline B |
# MAGIC | **No materialized view distinction** | `@dlt.table` is used for both streaming tables and materialized views |
# MAGIC | **Platform lock-in** | Code is not portable to open-source Spark |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Limitations of DLT
# MAGIC
# MAGIC DLT solved the **pipeline-level** problems beautifully, but data platforms
# MAGIC are more than a single pipeline:
# MAGIC
# MAGIC 1. **Orchestration gap** — You still need external tooling (Airflow, Databricks
# MAGIC    Workflows) to schedule and chain DLT pipelines together.
# MAGIC
# MAGIC 2. **No native workflow integration** — DLT pipelines run in their own compute
# MAGIC    environment, separate from standard Spark jobs and notebooks.
# MAGIC
# MAGIC 3. **`import dlt` is non-standard** — The DLT module exists only inside the
# MAGIC    DLT runtime. You cannot import it in a regular notebook or open-source Spark.
# MAGIC    This makes local testing and development harder.
# MAGIC
# MAGIC 4. **Single-pipeline scope** — DLT optimizes within one pipeline graph, but
# MAGIC    cannot optimize across multiple pipelines that form a larger data platform.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC **This is why Lakeflow and Spark Declarative Pipelines were created.**
# MAGIC
# MAGIC Proceed to **Notebook 3** (`23-lakeflow-sdp-pipeline_notebook.py`) to see
# MAGIC how Lakeflow SDP evolved DLT into a platform-level solution built on
# MAGIC standard Apache Spark APIs.
