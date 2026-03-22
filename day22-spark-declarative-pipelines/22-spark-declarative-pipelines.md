# Spark Declarative Pipelines (Lakeflow SDP)
> Module: Data Engineering Pipelines | Day 22 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain the evolution from imperative pipelines to Spark Declarative Pipelines
- Define datasets using Streaming Tables, Materialized Views, and Views
- Implement data quality constraints with expectations (warn, drop, fail)
- Configure Auto CDC flows for change data capture with SCD Type 1 and Type 2
- Choose the right pipeline mode (triggered vs continuous) and cluster strategy
- Read pipeline DAGs, event logs, and data quality metrics

---

## Conceptual Overview

### The Naming Journey

The technology covered in this module has gone through several name changes:

| Year | Name | Notes |
|------|------|-------|
| 2021 | **Delta Live Tables (DLT)** | Original launch name |
| 2024 | **Lakeflow Declarative Pipelines** | Rebrand under Lakeflow umbrella |
| 2025 | **Spark Declarative Pipelines (SDP)** | Current name; contributed to open-source Apache Spark |

The core concept has remained the same throughout: **declare what your data should look like, not how to build it**. Exam materials and older documentation may still reference "DLT" -- it is the same technology.

### From Imperative to Declarative

Ten years ago, Spark SQL made individual queries declarative. You write `SELECT ... FROM ... WHERE ...` and the Catalyst optimizer decides how to execute it. But building a **pipeline** -- multiple queries that depend on each other -- has remained imperative. Developers must manually handle:

- **Sequencing**: which tables to build first
- **Parallelism**: which steps can run concurrently
- **Retries**: recovering from transient failures
- **Error handling**: what happens when a step fails
- **State management**: tracking streaming checkpoints
- **Sub-selection**: processing only changed data

Spark Declarative Pipelines extends the declarative model from the **query level** to the **entire pipeline level**. You declare datasets and the queries that populate them. The framework handles everything else.

```
IMPERATIVE PIPELINE (you manage everything)
============================================
Step 1: Read raw files from S3           -- you sequence
Step 2: Write to bronze table            -- you manage checkpoints
Step 3: Read bronze, apply transforms    -- you handle parallelism
Step 4: Write to silver table            -- you handle retries
Step 5: Read silver, aggregate           -- you track dependencies
Step 6: Write to gold table              -- you handle failures

DECLARATIVE PIPELINE (SDP manages everything)
==============================================
Dataset: bronze_orders  = read from S3
Dataset: silver_orders  = transform bronze_orders
Dataset: gold_summary   = aggregate silver_orders

SDP automatically:
  - Discovers dependencies from your queries
  - Sequences execution (bronze before silver before gold)
  - Parallelizes independent steps
  - Manages streaming checkpoints
  - Retries transient failures
  - Validates the entire graph before execution
```

**Analogy**: Just as Kubernetes manages containers and Terraform manages infrastructure, SDP manages your data pipelines. You describe the desired state; the framework makes it happen.

---

## Core Abstractions

### Datasets

Datasets are the targets your pipeline writes to. There are four types:

```
+---------------------+-------------------+------------------+------------------+
|                     | Streaming Table   | Materialized View| View             |
+---------------------+-------------------+------------------+------------------+
| Query Type          | Streaming         | Batch            | Batch            |
| Data Refresh        | Append-only       | Full recompute   | On-read          |
| Storage             | Delta table       | Delta table      | No storage       |
| Best For            | Ingestion/Bronze  | Transforms/Gold  | Reusable logic   |
| Supports Streaming  | Yes (as source)   | No               | No               |
| Python Decorator    | @dp.table         | @dp.materialized | @dp.view         |
|                     |                   |   _view          |                  |
| SQL Syntax          | CREATE STREAMING  | CREATE           | CREATE VIEW /    |
|                     |   TABLE           |  MATERIALIZED    | CREATE TEMPORARY |
|                     |                   |  VIEW            |   VIEW           |
+---------------------+-------------------+------------------+------------------+
```

#### Streaming Table

Populated by a streaming query. Append-only by default. Ideal for ingestion and the Bronze layer because it processes only new files on each run.

**Python**:
```python
from pyspark import pipelines as dp

@dp.table(
    name="bronze_orders",
    comment="Raw orders ingested from S3 via Auto Loader"
)
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("s3://ecommerce-lakehouse/data-store/orders")
    )
```

**SQL**:
```sql
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT 'Raw orders ingested from S3 via Auto Loader'
AS SELECT * FROM STREAM read_files(
    's3://ecommerce-lakehouse/data-store/orders',
    format => 'csv',
    header => true
);
```

#### Materialized View

Populated by a batch query. Fully recomputed on each pipeline run. Ideal for Silver and Gold layers where you want a complete, consistent result.

**Python**:
```python
@dp.materialized_view(
    name="silver_stores",
    comment="Cleansed store dimension"
)
def silver_stores():
    return spark.read.table("bronze_stores").select(
        F.col("store_id"),
        F.col("store_name"),
        F.col("city"),
        F.col("region"),
        F.current_timestamp().alias("silver_processed_timestamp")
    )
```

**SQL**:
```sql
CREATE OR REFRESH MATERIALIZED VIEW silver_stores
COMMENT 'Cleansed store dimension'
AS SELECT
    store_id,
    store_name,
    city,
    region,
    current_timestamp() AS silver_processed_timestamp
FROM bronze_stores;
```

#### View and Temporary View

Standard SQL views -- the definition is updated on each pipeline run. **Views** are visible outside the pipeline; **Temporary Views** are internal only.

Views are useful for reusable intermediate logic (staging transformations, complex joins) that you do not want to materialize.

**Python** (temporary view):
```python
@dp.view(
    name="orders_staging",
    comment="Staging view with quality checks applied"
)
def orders_staging():
    return spark.read.table("bronze_orders").select(
        F.col("order_id"),
        F.col("order_date").cast("date"),
        F.col("store_id"),
        F.col("order_amount").cast("double")
    )
```

**SQL**:
```sql
CREATE TEMPORARY VIEW orders_staging AS
SELECT
    order_id,
    CAST(order_date AS DATE) AS order_date,
    store_id,
    CAST(order_amount AS DOUBLE) AS order_amount
FROM bronze_orders;
```

### Sinks

Sinks are external targets outside your lakehouse -- for example, Kafka topics or messaging systems. Sinks are Python-only and are used when your pipeline needs to publish data to external consumers.

### Flows

Flows are the fundamental units of data processing in SDP. Each flow populates a dataset.

| Flow Type | Processing | Use Case |
|-----------|-----------|----------|
| **Materialized View Flow** | Batch, full recompute | Dimension tables, aggregations |
| **Append Flow** | Streaming, insert-only | Logs, events, raw ingestion |
| **Auto CDC Flow** | Insert/Update/Delete | Source system replication, SCD |

---

## Benefits of SDP

### Pre-Validation

Before executing a single query, SDP analyzes the **entire pipeline graph**:
- Validates SQL/Python syntax across all datasets
- Checks that referenced tables exist
- Verifies schema compatibility between upstream and downstream datasets
- Catches circular dependencies

This means errors are surfaced **before** any data is written, saving time and compute.

### Automatic State Management

For Streaming Tables, SDP manages all checkpoints automatically. You never need to specify `checkpointLocation` -- the framework handles checkpoint creation, recovery, and cleanup.

### Automatic Parallelization

SDP analyzes the dependency graph and runs independent steps simultaneously. If `bronze_orders` and `bronze_stores` have no dependency on each other, they execute in parallel without any configuration.

### Dependency Resolution

Dependencies are discovered by analyzing the queries themselves. If `silver_orders` reads from `bronze_orders`, SDP knows to build `bronze_orders` first. You never specify execution order manually.

### Efficient Retries

Transient failures (network timeouts, temporary S3 issues) are retried automatically in production mode. Only the failed step is retried, not the entire pipeline.

---

## Data Quality with Expectations

Expectations are declarative data quality constraints. They are the SDP equivalent of CHECK constraints, but with flexible enforcement actions.

### Three Enforcement Levels

```
+------------------+------------------+-------------------+-------------------+
| Action           | Bad Records      | Pipeline          | Use Case          |
+------------------+------------------+-------------------+-------------------+
| expect           | KEPT in table    | Continues         | Monitor quality   |
| expect_or_drop   | DROPPED          | Continues         | Filter bad data   |
| expect_or_fail   | N/A              | FAILS immediately | Hard constraints  |
+------------------+------------------+-------------------+-------------------+
```

### Python Syntax

```python
@dp.table(name="silver_orders")
@dp.expect("valid_amount", "order_amount > 0")
@dp.expect_or_drop("valid_rating", "customer_rating BETWEEN 1 AND 5")
@dp.expect_or_fail("valid_order_id", "order_id IS NOT NULL")
def silver_orders():
    return spark.read.table("bronze_orders")
```

### SQL Syntax

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders (
    CONSTRAINT valid_amount     EXPECT (order_amount > 0),
    CONSTRAINT valid_rating     EXPECT (customer_rating BETWEEN 1 AND 5) ON VIOLATION DROP ROW,
    CONSTRAINT valid_order_id   EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
)
AS SELECT * FROM STREAM bronze_orders;
```

### Quality Metrics

Expectation results are tracked per pipeline run and visible in the pipeline UI:
- Number of records that passed each constraint
- Number of records that violated each constraint
- Violation percentage
- Which input rows caused violations (lineage tracking)

---

## Change Data Capture (CDC) with Auto CDC Flow

Auto CDC Flow handles upserts (inserts, updates, and deletes) from source systems. It is the SDP-native alternative to manually writing MERGE statements.

### Python Syntax

```python
import pyspark.sql.functions as F
from pyspark import pipelines as dp

# Step 1: Define the target streaming table
dp.create_streaming_table(
    name="silver_orders",
    comment="Orders with CDC applied"
)

# Step 2: Create the Auto CDC flow
dp.create_auto_cdc_flow(
    name="silver_orders_cdc",
    target="silver_orders",
    source="bronze_orders_staging",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1,          # Type 1: overwrite
    apply_as_deletes=F.expr("operation = 'DELETE'")
)
```

### SQL Syntax

```sql
CREATE OR REFRESH STREAMING TABLE silver_orders;

APPLY CHANGES INTO silver_orders
FROM bronze_orders_staging
KEYS (order_id)
SEQUENCE BY updated_at
STORED AS SCD TYPE 1;
```

### SCD Type 1 vs Type 2

| Feature | SCD Type 1 | SCD Type 2 |
|---------|-----------|-----------|
| History | No -- overwrites in place | Yes -- tracks full history |
| Extra Columns | None | `__start_at`, `__end_at`, `__is_current` |
| Storage | Less | More (one row per version) |
| Use Case | Current state only | Audit trail, historical analysis |

For Type 2, set `stored_as_scd_type=2`. SDP automatically manages the start/end timestamps and the current flag.

### Important Limitation

Tables populated by Auto CDC flows **cannot be used as streaming sources** downstream. If you need to stream from a CDC target, read it as a batch source in a Materialized View instead.

---

## Pipeline Configuration

### Pipeline Modes

| Mode | Behavior | Cluster | Best For |
|------|----------|---------|----------|
| **Triggered** | Runs once, then shuts down | Terminates after run | Scheduled batch jobs |
| **Continuous** | Keeps running, processes new data | Stays alive | Low-latency streaming |

### Development vs Production

| Setting | Development Mode | Production Mode |
|---------|-----------------|-----------------|
| Cluster | Reused across runs | New cluster per run |
| Retries | Disabled (fail fast) | Enabled (resilient) |
| Purpose | Fast iteration | Reliable production |

Development mode is ideal while building and testing your pipeline. Switch to production mode before scheduling.

### Cluster Modes

| Mode | Description |
|------|-------------|
| **Fixed Size** | Static number of workers; predictable cost |
| **Enhanced Autoscaling** | SDP-optimized autoscaling; recommended for production |
| **Legacy Autoscaling** | Standard Spark autoscaling; use only for compatibility |

Enhanced Autoscaling is purpose-built for SDP workloads. It scales more aggressively during initial loads and scales down faster during steady-state streaming.

---

## Pipeline Execution and DAG

### The Pipeline DAG

When you create a pipeline, SDP builds a **Directed Acyclic Graph (DAG)** from your dataset definitions. The DAG is visualized in the Databricks UI.

```
    S3: orders/              S3: stores/
        |                        |
        v                        v
  +--------------+       +--------------+
  | bronze.orders|       | bronze.stores|
  | (Streaming   |       | (Materialized|
  |  Table)      |       |  View)       |
  +--------------+       +--------------+
        |                        |
        v                        v
  +--------------+       +--------------+       +--------------+
  | silver.orders|       | silver.stores|       | silver.      |
  | (CDC Flow    |       | (Materialized|       |  calendar    |
  |  -> ST)      |       |  View)       |       | (Materialized|
  +--------------+       +--------------+       |  View)       |
        |                     |  |              +--------------+
        |                     |  |                     |
        +----------+----------+  +----------+----------+
                   |                        |
                   v                        v
           +---------------+       +------------------+
           | gold.         |       | gold.            |
           |  fact_orders  |       |  fact_orders_    |
           | (View)        |       |  northeast (View)|
           +---------------+       +------------------+
                                   | gold.            |
                                   |  fact_orders_    |
                                   |  southeast (View)|
                                   +------------------+
                                   | gold.            |
                                   |  fact_orders_    |
                                   |  midwest (View)  |
                                   +------------------+
                                   | gold.            |
                                   |  fact_orders_    |
                                   |  west (View)     |
                                   +------------------+
```

### Events Panel

The pipeline UI shows three categories of events:
- **Info**: dataset refresh started/completed, rows processed
- **Warning**: expectation violations (for `expect` mode)
- **Error**: expectation failures (for `expect_or_fail`), query errors

### System Events Table

Pipeline events are stored as a Delta table in the pipeline's storage location. You can query them for monitoring and alerting:

```sql
SELECT
    timestamp,
    level,
    message,
    details
FROM event_log(TABLE(ecommerce.gold.fact_orders))
WHERE level IN ('WARN', 'ERROR')
ORDER BY timestamp DESC;
```

---

## Incremental Processing

### Auto Loader Integration

SDP integrates naturally with Auto Loader for Bronze layer ingestion. Auto Loader tracks which files have been processed -- there is no need for manual processed/failed folder management.

```python
@dp.table(name="bronze_orders")
def bronze_orders():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("maxFilesPerTrigger", "100")
        .load("s3://ecommerce-lakehouse/data-store/orders")
    )
```

### Scheduling Options

| Approach | How | When |
|----------|-----|------|
| **Triggered + Schedule** | Run pipeline on a cron schedule | Batch: hourly, daily |
| **Continuous** | Pipeline stays running | Near-real-time: seconds |

For most batch workloads, triggered mode on a schedule is cost-effective. Continuous mode is for use cases requiring sub-minute latency.

---

## Hands-On Walkthrough

This module includes a complete e-commerce pipeline built with SDP.

### Files

| File | Description |
|------|-------------|
| [`22-spark-declarative-pipelines_notebook.py`](22-spark-declarative-pipelines_notebook.py) | Interactive learning notebook |
| [`lab-scripts/bronze/stores.py`](lab-scripts/bronze/stores.py) | Bronze: stores dimension from S3 CSV |
| [`lab-scripts/bronze/orders.py`](lab-scripts/bronze/orders.py) | Bronze: orders fact via Auto Loader |
| [`lab-scripts/silver/stores.py`](lab-scripts/silver/stores.py) | Silver: cleansed store dimension |
| [`lab-scripts/silver/calendar.py`](lab-scripts/silver/calendar.py) | Silver: generated date dimension |
| [`lab-scripts/silver/orders.py`](lab-scripts/silver/orders.py) | Silver: orders with expectations + CDC |
| [`lab-scripts/gold/fact_orders.sql`](lab-scripts/gold/fact_orders.sql) | Gold: denormalized fact view |
| [`lab-scripts/gold/fact_orders_northeast.sql`](lab-scripts/gold/fact_orders_northeast.sql) | Gold: Northeast regional view |
| [`lab-scripts/gold/fact_orders_southeast.sql`](lab-scripts/gold/fact_orders_southeast.sql) | Gold: Southeast regional view |
| [`lab-scripts/gold/fact_orders_midwest.sql`](lab-scripts/gold/fact_orders_midwest.sql) | Gold: Midwest regional view |
| [`lab-scripts/gold/fact_orders_west.sql`](lab-scripts/gold/fact_orders_west.sql) | Gold: West regional view |

### How to Run

1. Upload the `data/` directory contents to S3:
   - `stores.csv` to `s3://ecommerce-lakehouse/data-store/stores/`
   - Order CSV files to `s3://ecommerce-lakehouse/data-store/orders/`
2. Import the `lab-scripts/` directory into your Databricks workspace
3. Create a new pipeline in Databricks:
   - **Source code**: point to the `lab-scripts/` directory
   - **Target catalog**: `ecommerce`
   - **Target schema**: leave blank (scripts specify per-dataset)
   - **Pipeline mode**: Triggered
   - **Cluster mode**: Enhanced Autoscaling (1-4 workers)
4. Add pipeline configuration parameters:
   - `start_date`: `2024-01-01`
   - `end_date`: `2024-12-31`
5. Click **Start** and observe the DAG build and execute

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Storage | S3 with IAM roles | ADLS Gen2 with service principals | GCS with service accounts |
| Auto Loader | cloudFiles with S3 paths | cloudFiles with abfss:// paths | cloudFiles with gs:// paths |
| Unity Catalog | Required for managed tables | Required for managed tables | Required for managed tables |
| Pipeline UI | Databricks Workspace | Databricks Workspace | Databricks Workspace |
| Event Logs | Delta table in S3 | Delta table in ADLS | Delta table in GCS |
| Notifications | SNS/SQS for classic mode | Event Grid for classic mode | Pub/Sub for classic mode |

---

## Certification Tip

On the **Databricks Certified Data Engineer Professional** exam, SDP (referenced as "Delta Live Tables" in most exam versions) appears in approximately 15-20% of questions. Focus on:

1. **Expectations**: Know all three enforcement levels and their SQL/Python syntax
2. **Streaming Table vs Materialized View**: When to use each
3. **Pipeline modes**: Triggered vs Continuous, Development vs Production
4. **Auto CDC**: KEYS, SEQUENCE BY, SCD Type 1 vs Type 2, the streaming source limitation
5. **Event logs**: How to query pipeline events for monitoring
6. **Import syntax**: `from pyspark import pipelines as dp` (new) vs `import dlt` (legacy)

---

## Key Takeaways

1. **Declarative > Imperative**: SDP lets you define *what* your data should look like, not *how* to compute it. The framework handles sequencing, parallelism, retries, and state management.

2. **Three dataset types**: Streaming Tables for append-only ingestion, Materialized Views for batch transforms, and Views for reusable logic.

3. **Expectations enforce quality declaratively**: Choose `expect` (monitor), `expect_or_drop` (filter), or `expect_or_fail` (halt) based on business requirements.

4. **Auto CDC replaces manual MERGE**: Define keys and sequence columns; SDP handles SCD Type 1 and Type 2 automatically.

5. **Pipeline DAG is auto-discovered**: Dependencies come from your queries, not from configuration. SDP validates the entire graph before executing.

6. **Development mode for iteration, Production mode for reliability**: Dev mode reuses clusters and disables retries; Production mode creates fresh clusters and enables retries.

---

## Next Steps

- [Day 23: SCD Type 2 Pipelines](../day23-scd-type-2-pipelines/) -- deep dive into slowly changing dimensions with SDP
