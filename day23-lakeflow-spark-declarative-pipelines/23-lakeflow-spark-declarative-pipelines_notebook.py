# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 23: Lakeflow Spark Declarative Pipelines
# MAGIC
# MAGIC **Objective**: Understand Spark Declarative Pipelines -- the declarative framework for building
# MAGIC production-grade data pipelines on Databricks. Formerly known as Delta Live Tables (DLT).
# MAGIC
# MAGIC **Key Insight**: SDP extends the declarative model from the **query level** (SQL) to the
# MAGIC **entire pipeline level**. You declare datasets and the queries that populate them; the
# MAGIC framework handles sequencing, parallelism, retries, and state management.
# MAGIC
# MAGIC ```
# MAGIC Imperative Pipeline (you manage everything):
# MAGIC   Step 1: Read files     -> you sequence
# MAGIC   Step 2: Write bronze   -> you manage checkpoints
# MAGIC   Step 3: Transform      -> you handle parallelism
# MAGIC   Step 4: Write silver   -> you handle retries
# MAGIC   Step 5: Aggregate      -> you track dependencies
# MAGIC   Step 6: Write gold     -> you handle failures
# MAGIC
# MAGIC Declarative Pipeline (SDP manages everything):
# MAGIC   Dataset: bronze_orders = read from S3
# MAGIC   Dataset: silver_orders = transform bronze_orders
# MAGIC   Dataset: gold_summary  = aggregate silver_orders
# MAGIC   --> SDP auto-discovers dependencies, sequences, parallelizes, retries
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - [Day 18: Medallion Architecture](../day18-medallion-architecture/)
# MAGIC - [Day 19: Structured Streaming](../day19-structured-streaming/)
# MAGIC - [Day 20: Auto Loader](../day20-auto-loader/)
# MAGIC - [Day 21: Change Data Capture](../day21-change-data-capture/)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup -- Catalog and Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create the ecommerce catalog and Bronze/Silver/Gold schemas
# MAGIC -- These map to the three layers of the Medallion Architecture
# MAGIC CREATE CATALOG IF NOT EXISTS ecommerce;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.bronze
# MAGIC COMMENT 'Raw data layer -- ingested as-is from source systems';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.silver
# MAGIC COMMENT 'Cleansed data layer -- validated, deduplicated, conformed';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ecommerce.gold
# MAGIC COMMENT 'Business data layer -- aggregated, denormalized, analytics-ready';

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: The Naming Journey
# MAGIC
# MAGIC The technology has been renamed several times. All names refer to the same thing:
# MAGIC
# MAGIC | Year | Name | Import Statement |
# MAGIC |------|------|------------------|
# MAGIC | 2021 | Delta Live Tables (DLT) | `import dlt` |
# MAGIC | 2024 | Lakeflow Declarative Pipelines | `from databricks import pipelines as dp` |
# MAGIC | 2025 | Spark Declarative Pipelines | `from pyspark import pipelines as dp` |
# MAGIC
# MAGIC The current import is `from pyspark import pipelines as dp`. Older notebooks using
# MAGIC `import dlt` will still work but should be migrated.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Core Abstractions -- Dataset Types
# MAGIC
# MAGIC SDP has four dataset types. Choosing the right one is critical for performance and correctness.
# MAGIC
# MAGIC ### Streaming Table
# MAGIC - Populated by a **streaming query** (append-only)
# MAGIC - Processes only NEW data on each run
# MAGIC - Best for: **Bronze layer ingestion**, logs, events
# MAGIC - Can be used as a streaming source downstream
# MAGIC
# MAGIC ### Materialized View
# MAGIC - Populated by a **batch query** (fully recomputed each run)
# MAGIC - Produces a complete, consistent result every time
# MAGIC - Best for: **Silver/Gold transforms**, dimension tables, aggregations
# MAGIC - Cannot be used as a streaming source
# MAGIC
# MAGIC ### View
# MAGIC - Standard SQL view (no storage, computed on read)
# MAGIC - Definition updated on each pipeline run
# MAGIC - Best for: **Reusable intermediate logic** shared across datasets
# MAGIC
# MAGIC ### Temporary View
# MAGIC - Like a View but only visible **within** the pipeline
# MAGIC - Not accessible by external queries
# MAGIC - Best for: **Staging transformations** before CDC or complex logic

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Syntax for Each Dataset Type
# MAGIC
# MAGIC ```python
# MAGIC from pyspark import pipelines as dp
# MAGIC import pyspark.sql.functions as F
# MAGIC
# MAGIC # ---- Streaming Table (append-only, for ingestion) ----
# MAGIC @dp.table(name="bronze_orders", comment="Raw orders from S3")
# MAGIC def bronze_orders():
# MAGIC     return (
# MAGIC         spark.readStream
# MAGIC         .format("cloudFiles")
# MAGIC         .option("cloudFiles.format", "csv")
# MAGIC         .option("cloudFiles.inferColumnTypes", "true")
# MAGIC         .load("s3://ecommerce-lakehouse/data-store/orders")
# MAGIC     )
# MAGIC
# MAGIC # ---- Materialized View (batch, fully recomputed) ----
# MAGIC @dp.materialized_view(name="silver_stores", comment="Cleansed stores")
# MAGIC def silver_stores():
# MAGIC     return spark.read.table("bronze_stores").select(
# MAGIC         F.col("store_id"),
# MAGIC         F.col("store_name"),
# MAGIC         F.col("city"),
# MAGIC         F.col("region")
# MAGIC     )
# MAGIC
# MAGIC # ---- Temporary View (internal pipeline logic) ----
# MAGIC @dp.view(name="orders_staging", comment="Staging with quality checks")
# MAGIC def orders_staging():
# MAGIC     return spark.read.table("bronze_orders").select("order_id", "order_date")
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL Syntax for Each Dataset Type
# MAGIC
# MAGIC ```sql
# MAGIC -- Streaming Table
# MAGIC CREATE OR REFRESH STREAMING TABLE bronze_orders
# MAGIC COMMENT 'Raw orders from S3'
# MAGIC AS SELECT * FROM STREAM read_files(
# MAGIC     's3://ecommerce-lakehouse/data-store/orders',
# MAGIC     format => 'csv', header => true
# MAGIC );
# MAGIC
# MAGIC -- Materialized View
# MAGIC CREATE OR REFRESH MATERIALIZED VIEW silver_stores
# MAGIC COMMENT 'Cleansed stores'
# MAGIC AS SELECT store_id, store_name, city, region
# MAGIC FROM bronze_stores;
# MAGIC
# MAGIC -- View (visible outside pipeline)
# MAGIC CREATE OR REPLACE VIEW gold.fact_orders AS
# MAGIC SELECT * FROM silver_orders JOIN silver_stores USING (store_id);
# MAGIC
# MAGIC -- Temporary View (pipeline-internal only)
# MAGIC CREATE TEMPORARY VIEW orders_staging AS
# MAGIC SELECT order_id, CAST(order_date AS DATE) AS order_date
# MAGIC FROM bronze_orders;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Data Quality with Expectations
# MAGIC
# MAGIC Expectations are declarative data quality constraints. They let you define rules
# MAGIC and choose what happens when data violates them.
# MAGIC
# MAGIC | Action | Bad Records | Pipeline | Use Case |
# MAGIC |--------|-------------|----------|----------|
# MAGIC | `expect` | **Kept** in table | Continues | Monitor quality metrics |
# MAGIC | `expect_or_drop` | **Dropped** | Continues | Filter invalid data |
# MAGIC | `expect_or_fail` | N/A | **Fails** | Hard constraints (PKs, etc.) |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python Expectations Syntax
# MAGIC
# MAGIC ```python
# MAGIC @dp.table(name="silver_orders")
# MAGIC @dp.expect("valid_date", "year(order_date) >= 2020")
# MAGIC @dp.expect_or_drop("valid_rating", "customer_rating BETWEEN 1 AND 5")
# MAGIC @dp.expect_or_fail("has_order_id", "order_id IS NOT NULL")
# MAGIC def silver_orders():
# MAGIC     return spark.readStream.table("bronze_orders")
# MAGIC ```
# MAGIC
# MAGIC ### SQL Expectations Syntax
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders (
# MAGIC     CONSTRAINT valid_date    EXPECT (year(order_date) >= 2020),
# MAGIC     CONSTRAINT valid_rating  EXPECT (customer_rating BETWEEN 1 AND 5)
# MAGIC                              ON VIOLATION DROP ROW,
# MAGIC     CONSTRAINT has_order_id  EXPECT (order_id IS NOT NULL)
# MAGIC                              ON VIOLATION FAIL UPDATE
# MAGIC )
# MAGIC AS SELECT * FROM STREAM bronze_orders;
# MAGIC ```
# MAGIC
# MAGIC **Key Point**: The default action (no suffix) is `expect` -- records are kept and
# MAGIC violations are tracked in the pipeline metrics. This is the safest starting point.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Change Data Capture (CDC) with Auto CDC Flow
# MAGIC
# MAGIC Auto CDC Flow handles inserts, updates, and deletes from source systems without
# MAGIC writing manual MERGE statements.
# MAGIC
# MAGIC ### Key Configuration
# MAGIC
# MAGIC | Parameter | Purpose | Example |
# MAGIC |-----------|---------|---------|
# MAGIC | `keys` | Primary key columns | `["order_id"]` |
# MAGIC | `sequence_by` | Ordering column for dedup | `"updated_at"` |
# MAGIC | `stored_as_scd_type` | 1 = overwrite, 2 = history | `1` |
# MAGIC | `apply_as_deletes` | Condition for delete ops | `F.expr("op = 'DELETE'")` |
# MAGIC
# MAGIC ### SCD Type 1 (Overwrite -- Current State Only)
# MAGIC
# MAGIC ```python
# MAGIC dp.create_streaming_table(name="silver_orders")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     name="orders_cdc",
# MAGIC     target="silver_orders",
# MAGIC     source="orders_staging",
# MAGIC     keys=["order_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=1
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### SCD Type 2 (Historical Tracking)
# MAGIC
# MAGIC ```python
# MAGIC dp.create_streaming_table(name="silver_orders")
# MAGIC
# MAGIC dp.create_auto_cdc_flow(
# MAGIC     name="orders_cdc",
# MAGIC     target="silver_orders",
# MAGIC     source="orders_staging",
# MAGIC     keys=["order_id"],
# MAGIC     sequence_by="updated_at",
# MAGIC     stored_as_scd_type=2   # Adds __start_at, __end_at, __is_current
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### SQL Equivalent
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING TABLE silver_orders;
# MAGIC
# MAGIC APPLY CHANGES INTO silver_orders
# MAGIC FROM orders_staging
# MAGIC KEYS (order_id)
# MAGIC SEQUENCE BY updated_at
# MAGIC STORED AS SCD TYPE 1;
# MAGIC ```
# MAGIC
# MAGIC **Important Limitation**: Tables populated by Auto CDC flows **cannot** be used as
# MAGIC streaming sources downstream. Use them as batch sources in Materialized Views instead.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Pipeline Configuration and Modes
# MAGIC
# MAGIC ### Triggered vs Continuous
# MAGIC
# MAGIC | Mode | Behavior | Best For |
# MAGIC |------|----------|----------|
# MAGIC | **Triggered** | Runs once, shuts down cluster | Scheduled batch (hourly/daily) |
# MAGIC | **Continuous** | Keeps running, processes new data | Low-latency streaming |
# MAGIC
# MAGIC ### Development vs Production
# MAGIC
# MAGIC | Setting | Development | Production |
# MAGIC |---------|-------------|------------|
# MAGIC | Cluster | Reused across runs | New cluster per run |
# MAGIC | Retries | Disabled (fail fast) | Enabled (resilient) |
# MAGIC | Use | Building & testing | Scheduled workloads |
# MAGIC
# MAGIC ### Cluster Modes
# MAGIC
# MAGIC | Mode | Description |
# MAGIC |------|-------------|
# MAGIC | **Fixed Size** | Static workers; predictable cost |
# MAGIC | **Enhanced Autoscaling** | SDP-optimized; recommended for production |
# MAGIC | **Legacy Autoscaling** | Standard Spark; compatibility only |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Creating a Pipeline in the UI
# MAGIC
# MAGIC ### Step-by-Step
# MAGIC
# MAGIC 1. Navigate to **Workflows > Pipelines > Create Pipeline**
# MAGIC 2. Configure:
# MAGIC    - **Pipeline name**: `ecommerce-sdp-pipeline`
# MAGIC    - **Source code**: Point to your `lab-scripts/` directory in the workspace
# MAGIC    - **Target catalog**: `ecommerce`
# MAGIC    - **Pipeline mode**: `Triggered` (for development)
# MAGIC    - **Cluster mode**: `Enhanced Autoscaling` (min 1, max 4 workers)
# MAGIC 3. Add **Configuration** parameters:
# MAGIC    - `start_date` = `2024-01-01`
# MAGIC    - `end_date` = `2024-12-31`
# MAGIC 4. Click **Create**
# MAGIC 5. Click **Start** to run the pipeline
# MAGIC
# MAGIC ### What Happens When You Click Start
# MAGIC
# MAGIC 1. **Pre-validation**: SDP analyzes all source files, validates syntax, checks dependencies
# MAGIC 2. **DAG construction**: Builds the dependency graph from your dataset definitions
# MAGIC 3. **Cluster provisioning**: Starts a cluster (or reuses in dev mode)
# MAGIC 4. **Execution**: Runs datasets in dependency order, parallelizing independent steps
# MAGIC 5. **Quality tracking**: Records expectation results for each dataset
# MAGIC 6. **Completion**: Shuts down cluster (triggered mode) or waits for new data (continuous)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Pipeline DAG Visualization
# MAGIC
# MAGIC After running the pipeline, the UI shows the DAG. For our e-commerce pipeline:
# MAGIC
# MAGIC ```
# MAGIC     S3: orders/              S3: stores/
# MAGIC         |                        |
# MAGIC         v                        v
# MAGIC   +--------------+       +--------------+
# MAGIC   | bronze.orders|       | bronze.stores|
# MAGIC   | (Streaming   |       | (Materialized|
# MAGIC   |  Table)      |       |  View)       |
# MAGIC   +--------------+       +--------------+
# MAGIC         |                        |
# MAGIC         v                        v
# MAGIC   +--------------+       +--------------+       +--------------+
# MAGIC   | silver.orders|       | silver.stores|       | silver.      |
# MAGIC   | (CDC -> ST)  |       | (Materialized|       |  calendar    |
# MAGIC   +--------------+       |  View)       |       | (Materialized|
# MAGIC         |                +--------------+       |  View)       |
# MAGIC         |                     |  |              +--------------+
# MAGIC         +----------+----------+  +----------+----------+
# MAGIC                    |                        |
# MAGIC                    v                        v
# MAGIC            +---------------+       +------------------+
# MAGIC            | gold.         |       | Regional views:  |
# MAGIC            |  fact_orders  |------>|  _northeast      |
# MAGIC            | (View)        |       |  _southeast      |
# MAGIC            +---------------+       |  _midwest        |
# MAGIC                                    |  _west           |
# MAGIC                                    +------------------+
# MAGIC ```
# MAGIC
# MAGIC Click any dataset in the DAG to see:
# MAGIC - Row count and processing time
# MAGIC - Expectation results (pass/fail/drop counts)
# MAGIC - Schema information
# MAGIC - Lineage (upstream and downstream dependencies)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Inspecting Pipeline Events and Data Quality

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying the Event Log
# MAGIC
# MAGIC Pipeline events are stored as a Delta table. You can query them programmatically
# MAGIC for monitoring, alerting, or auditing.
# MAGIC
# MAGIC ```sql
# MAGIC -- View recent pipeline events
# MAGIC SELECT
# MAGIC     timestamp,
# MAGIC     level,
# MAGIC     message,
# MAGIC     details
# MAGIC FROM event_log(TABLE(ecommerce.silver.orders))
# MAGIC WHERE level IN ('WARN', 'ERROR')
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 20;
# MAGIC ```
# MAGIC
# MAGIC ### Querying Data Quality Metrics
# MAGIC
# MAGIC ```sql
# MAGIC -- View expectation results from the latest pipeline run
# MAGIC SELECT
# MAGIC     details:flow_progress:data_quality:expectations
# MAGIC FROM event_log(TABLE(ecommerce.silver.orders))
# MAGIC WHERE details:type = 'flow_progress'
# MAGIC ORDER BY timestamp DESC
# MAGIC LIMIT 1;
# MAGIC ```
# MAGIC
# MAGIC The expectations JSON contains:
# MAGIC - `name`: expectation name (e.g., "valid_rating")
# MAGIC - `dataset`: which dataset the expectation applies to
# MAGIC - `passed_records`: number of records that passed
# MAGIC - `failed_records`: number of records that violated the constraint

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 10: Lab Scripts Overview
# MAGIC
# MAGIC The `lab-scripts/` directory contains the actual pipeline source files. These are
# MAGIC **plain Python and SQL files** (NOT notebooks) that SDP executes as a pipeline.
# MAGIC
# MAGIC ### Bronze Layer
# MAGIC | File | Type | Description |
# MAGIC |------|------|-------------|
# MAGIC | `bronze/stores.py` | Materialized View | Stores CSV from S3 with PERMISSIVE mode |
# MAGIC | `bronze/orders.py` | Streaming Table | Orders via Auto Loader (cloudFiles) |
# MAGIC
# MAGIC ### Silver Layer
# MAGIC | File | Type | Description |
# MAGIC |------|------|-------------|
# MAGIC | `silver/stores.py` | Materialized View | Cleansed store dimension |
# MAGIC | `silver/calendar.py` | Materialized View | Generated date dimension with US holidays |
# MAGIC | `silver/orders.py` | Staging View + CDC Flow | Expectations + SCD Type 1 upsert |
# MAGIC
# MAGIC ### Gold Layer
# MAGIC | File | Type | Description |
# MAGIC |------|------|-------------|
# MAGIC | `gold/fact_orders.sql` | View | Denormalized join of orders + stores + calendar |
# MAGIC | `gold/fact_orders_northeast.sql` | View | Northeast region filter |
# MAGIC | `gold/fact_orders_southeast.sql` | View | Southeast region filter |
# MAGIC | `gold/fact_orders_midwest.sql` | View | Midwest region filter |
# MAGIC | `gold/fact_orders_west.sql` | View | West region filter |
# MAGIC
# MAGIC **Important**: Lab scripts are NOT notebooks. They do not have `# Databricks notebook source`
# MAGIC headers. They are plain `.py` and `.sql` files that the pipeline framework discovers and executes.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 11: Benefits of SDP -- Why Declarative Wins
# MAGIC
# MAGIC ### Pre-Validation
# MAGIC SDP analyzes the **entire pipeline graph** before executing a single query:
# MAGIC - Validates SQL/Python syntax across all datasets
# MAGIC - Checks that referenced tables exist
# MAGIC - Verifies schema compatibility
# MAGIC - Catches circular dependencies
# MAGIC
# MAGIC Errors surface **before** any data is written.
# MAGIC
# MAGIC ### Automatic State Management
# MAGIC For Streaming Tables, SDP manages all checkpoints. You never specify `checkpointLocation`.
# MAGIC
# MAGIC ### Automatic Parallelization
# MAGIC Independent datasets (e.g., `bronze.orders` and `bronze.stores`) run simultaneously
# MAGIC without any configuration.
# MAGIC
# MAGIC ### Dependency Resolution
# MAGIC Dependencies are discovered by analyzing queries. If `silver.orders` reads from
# MAGIC `bronze.orders`, SDP knows the execution order automatically.
# MAGIC
# MAGIC ### Efficient Retries
# MAGIC In production mode, transient failures are retried automatically. Only the failed
# MAGIC step is retried, not the entire pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 12: Comparison with Imperative Approaches
# MAGIC
# MAGIC | Aspect | Imperative (Notebooks) | Declarative (SDP) |
# MAGIC |--------|----------------------|-------------------|
# MAGIC | Execution order | You sequence manually | Auto-discovered from queries |
# MAGIC | Parallelism | You manage threads | Automatic |
# MAGIC | Checkpoints | You specify locations | Automatic |
# MAGIC | Retries | You write retry logic | Built-in (production mode) |
# MAGIC | Data quality | You write IF/ELSE checks | Declarative expectations |
# MAGIC | CDC/MERGE | You write MERGE SQL | Auto CDC flow |
# MAGIC | Cluster lifecycle | You manage start/stop | Automatic |
# MAGIC | Error visibility | You parse logs | Pipeline UI with DAG |
# MAGIC
# MAGIC **Analogy**: SDP is to data pipelines what Kubernetes is to containers and Terraform
# MAGIC is to infrastructure. You describe the desired state; the framework makes it happen.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 13: Certification Exam Tips
# MAGIC
# MAGIC SDP (still called "Delta Live Tables" in most exam materials) appears in ~15-20% of
# MAGIC questions on the **Databricks Certified Data Engineer Professional** exam.
# MAGIC
# MAGIC ### High-Priority Topics
# MAGIC
# MAGIC 1. **Expectations**: Know all three levels (`expect`, `expect_or_drop`, `expect_or_fail`)
# MAGIC    and both Python decorator and SQL CONSTRAINT syntax
# MAGIC
# MAGIC 2. **Streaming Table vs Materialized View**: When to use each
# MAGIC    - Streaming Table = append-only, incremental, Bronze
# MAGIC    - Materialized View = full recompute, batch, Silver/Gold
# MAGIC
# MAGIC 3. **Pipeline modes**:
# MAGIC    - Triggered = run once, shut down (batch)
# MAGIC    - Continuous = keep running (streaming)
# MAGIC    - Development = reuse cluster, no retries
# MAGIC    - Production = new cluster, retries enabled
# MAGIC
# MAGIC 4. **Auto CDC limitations**: CDC target tables cannot be used as streaming sources
# MAGIC
# MAGIC 5. **Event logs**: Know how to query `event_log()` for quality metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup
# MAGIC
# MAGIC Uncomment and run the cells below to remove all objects created in this lab.
# MAGIC
# MAGIC **Warning**: This will drop the entire ecommerce catalog and all its contents.

# COMMAND ----------

# Uncomment to clean up:
# spark.sql("DROP CATALOG IF EXISTS ecommerce CASCADE")
# print("Cleanup complete: ecommerce catalog dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC In this notebook, you learned:
# MAGIC
# MAGIC 1. **Declarative > Imperative**: SDP lets you define *what* your data looks like,
# MAGIC    not *how* to build it
# MAGIC 2. **Four dataset types**: Streaming Table, Materialized View, View, Temporary View
# MAGIC 3. **Expectations**: Three enforcement levels for data quality
# MAGIC 4. **Auto CDC**: Built-in change data capture with SCD Type 1 and Type 2
# MAGIC 5. **Pipeline configuration**: Triggered vs Continuous, Development vs Production
# MAGIC 6. **Lab scripts**: Production-grade pipeline code in `lab-scripts/` directory
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - Run the pipeline using the `lab-scripts/` directory
# MAGIC - Experiment with different expectation actions
# MAGIC - Try SCD Type 2 in the CDC flow
# MAGIC - Continue to [Day 23: SCD Type 2 Pipelines](../day23-scd-type-2-pipelines/)
