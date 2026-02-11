# Delta Live Tables (DLT)
> Module 06 — Topic 02 | Level: Intermediate-Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:

1. Define declarative data pipelines using the DLT framework
2. Distinguish between live tables and streaming live tables
3. Enforce data quality with DLT expectations (FAIL, DROP, ALLOW)
4. Implement the medallion architecture (Bronze, Silver, Gold) using DLT
5. Monitor pipelines using the DLT event log

## Conceptual Overview

### What Is Delta Live Tables?

Delta Live Tables is a declarative ETL framework built into Databricks. Instead of
writing imperative Spark code that specifies *how* to process data step by step, you
declare *what* the output table should look like, and DLT handles orchestration,
dependency resolution, error handling, and data quality enforcement.

As the instructor puts it: **"Delta Live Tables — Build NRT (Near Real-Time)
Datasets."** DLT can operate in both batch and streaming modes from a single codebase.

### Declarative vs Imperative

```
  IMPERATIVE (traditional Spark)        DECLARATIVE (DLT)
  +----------------------------+        +----------------------------+
  | df = spark.read(...)       |        | @dlt.table                 |
  | df2 = df.filter(...)       |        | def clean_sales():         |
  | df3 = df2.join(...)        |        |   return (                 |
  | df3.write.mode("append")   |        |     dlt.read("raw_sales")  |
  |    .saveAsTable(...)       |        |       .filter(valid_rows)  |
  |                            |        |   )                        |
  | # YOU manage:              |        |                            |
  | # - execution order        |        | # DLT manages:             |
  | # - error recovery         |        | # - execution order        |
  | # - cluster lifecycle      |        | # - error recovery         |
  | # - table creation         |        | # - cluster lifecycle      |
  +----------------------------+        +----------------------------+
```

### The Medallion Architecture in DLT

```
  +-----------+      +------------+      +-----------+
  |  BRONZE   |----->|   SILVER   |----->|   GOLD    |
  |           |      |            |      |           |
  | Raw data  |      | Cleaned,   |      | Aggregated|
  | as-is     |      | validated, |      | business- |
  | from      |      | deduplicated|     | ready     |
  | sources   |      | records    |      | tables    |
  +-----------+      +------------+      +-----------+
       ^                   |                   |
       |            Expectations          Expectations
   Ingest from      enforce quality       ensure business
   files, Kafka,    at this layer         rules are met
   databases
```

### Live Tables vs Streaming Live Tables

| Feature | Live Table | Streaming Live Table |
|---------|-----------|---------------------|
| Decorator | `@dlt.table` | `@dlt.table` with `spark.readStream` |
| Processing | Full recompute (batch) | Incremental (append-only) |
| Input | `dlt.read("table")` | `dlt.read_stream("table")` |
| Use case | Aggregations, slowly changing dimensions | High-volume event streams |
| Reprocess | Entire table on each run | Only new records since last run |

### DLT Expectations (Data Quality)

Expectations are data quality constraints that DLT evaluates on every record:

```
  +---------------+-------------------------------------------+
  | Action        | Behavior                                  |
  +---------------+-------------------------------------------+
  | ALLOW         | Record passes through; violation logged    |
  |               | in metrics (default)                      |
  +---------------+-------------------------------------------+
  | DROP          | Violating records are silently removed     |
  +---------------+-------------------------------------------+
  | FAIL          | Pipeline fails if ANY record violates      |
  +---------------+-------------------------------------------+
```

This is how DLT handles the challenge of data quality in production:
*"It should work better than our current ETL process"* — DLT bakes quality
checks directly into the pipeline definition.

## Hands-On Walkthrough

### Step 1: Bronze Layer — Ingest Raw Data

```python
import dlt
from pyspark.sql.functions import current_timestamp, input_file_name

@dlt.table(
    name="bronze_sales",
    comment="Raw sales data ingested from landing zone",
    table_properties={"quality": "bronze"}
)
def bronze_sales():
    return (
        spark.readStream
            .format("cloudFiles")           # Auto Loader
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .load("/mnt/landing/sales/")
            .withColumn("_ingestion_ts", current_timestamp())
            .withColumn("_source_file", input_file_name())
    )
```

### Step 2: Silver Layer — Clean and Validate

```python
@dlt.table(
    name="silver_sales",
    comment="Cleaned and validated sales records"
)
@dlt.expect("valid_quantity", "quantity > 0")                  # ALLOW
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")  # DROP
@dlt.expect_or_fail("valid_order_id", "order_id IS NOT NULL")          # FAIL
def silver_sales():
    return (
        dlt.read_stream("bronze_sales")
            .where("order_date IS NOT NULL")
            .withColumn("amount", col("price") * col("quantity"))
            .dropDuplicates(["order_id"])
    )
```

### Step 3: Gold Layer — Business Aggregations

```python
@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregation for dashboards"
)
def gold_daily_revenue():
    return (
        dlt.read("silver_sales")
            .groupBy("order_date", "region")
            .agg(
                sum("amount").alias("total_revenue"),
                count("order_id").alias("order_count"),
                avg("amount").alias("avg_order_value")
            )
    )
```

### Step 4: Pipeline Configuration

Create a DLT pipeline in the Databricks UI:

1. Navigate to **Delta Live Tables > Create Pipeline**
2. Configure:
   - **Name**: `sales-etl-pipeline`
   - **Source code**: Path to your notebook(s)
   - **Target schema**: `sales_dlt`
   - **Storage location**: `/mnt/dlt/sales`
   - **Pipeline mode**: Triggered (batch) or Continuous (streaming)
3. Cluster settings:
   - **Min workers**: 1
   - **Max workers**: 5 (DLT Enhanced Autoscaling handles scaling)

### Step 5: Development vs Production Mode

| Setting | Development | Production |
|---------|------------|------------|
| Cluster reuse | Cluster kept alive between runs | Cluster terminated after run |
| Error handling | Retry failed tables | Full pipeline restart |
| Cost | Higher (cluster stays warm) | Lower (auto-terminate) |
| Use when | Building and testing | Scheduled runs |

### Step 6: Query the Event Log

The DLT event log records pipeline execution details, data quality metrics, and
lineage information:

```sql
-- View data quality results
SELECT
    details:flow_name AS table_name,
    details:data_quality.expectations AS expectations,
    timestamp
FROM event_log(TABLE(sales_dlt.bronze_sales))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;

-- View row counts per update
SELECT
    details:flow_name AS table_name,
    details:flow_progress.metrics.num_output_rows AS rows_written,
    timestamp
FROM event_log(TABLE(sales_dlt.silver_sales))
WHERE event_type = 'flow_progress'
ORDER BY timestamp DESC;
```

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Auto Loader source | S3 | ADLS Gen2 | GCS |
| Event notifications | S3 Events/SQS | Event Grid/Queue | Pub/Sub |
| DLT pricing | Per DBU (Pipelines SKU) | Per DBU (Pipelines SKU) | Per DBU |
| Enhanced Autoscaling | All providers — DLT optimizes cluster size automatically |

DLT Enhanced Autoscaling is smarter than standard autoscaling. It considers the
pipeline DAG structure and proactively scales based on data volume.

## Certification Tip

On the Databricks Data Engineer Associate exam, expect questions about:
- The difference between `@dlt.expect`, `@dlt.expect_or_drop`, and `@dlt.expect_or_fail`
- When to use `dlt.read()` vs `dlt.read_stream()`
- DLT pipeline modes (Triggered vs Continuous)
- How DLT handles schema evolution automatically with Auto Loader
- The purpose and contents of the event log

## Key Takeaways

1. **DLT is declarative** — define *what*, not *how*; DLT resolves dependencies
2. **Expectations** enforce data quality at the framework level (ALLOW/DROP/FAIL)
3. **Streaming live tables** process data incrementally for near-real-time use cases
4. **The medallion architecture** maps naturally to DLT's table dependency model
5. **Enhanced Autoscaling** optimizes cluster resources based on pipeline structure
6. **The event log** provides observability into quality metrics and pipeline health

## Next Steps

Proceed to [03 - Asset Bundles](03-asset-bundles.md) to learn how to package
DLT pipelines and Workflows as deployable infrastructure-as-code.
