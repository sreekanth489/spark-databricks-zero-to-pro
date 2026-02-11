# Multi-Hop Ingestion Patterns (Bronze / Silver / Gold)

> Module 02 -- Topic 05 | Level: Intermediate | Time: 55 min

---

## Learning Objectives

- Describe the Medallion Architecture (Bronze / Silver / Gold) and its benefits
- Design a raw zone (Bronze) that preserves source fidelity
- Apply incremental loading strategies with exactly-once semantics
- Implement data quality checks at each layer
- Build an end-to-end multi-hop pipeline from raw files to aggregated Gold tables

---

## Conceptual Overview

### The Medallion Architecture

The Medallion Architecture (also called the multi-hop or lakehouse architecture)
organizes data into three quality tiers:

```
┌──────────────────────────────────────────────────────────────────────┐
│                          DATA LAKEHOUSE                              │
│                                                                      │
│  ┌────────────┐      ┌────────────┐      ┌────────────┐            │
│  │            │      │            │      │            │            │
│  │  BRONZE    │─────>│  SILVER    │─────>│   GOLD     │            │
│  │  (Raw)     │      │  (Cleaned) │      │ (Business) │            │
│  │            │      │            │      │            │            │
│  └────────────┘      └────────────┘      └────────────┘            │
│       │                    │                    │                    │
│       v                    v                    v                    │
│  Source fidelity     Deduplication       Pre-aggregated            │
│  Append-only         Type casting        Business metrics          │
│  Metadata cols       Null handling       Star schemas              │
│  No transforms       Standardization     ML features               │
│  Quick to load       Quality checks      Dashboard-ready           │
│                                                                      │
└──────────────────────────────────────────────────────────────────────┘
```

### Why Three Layers?

| Layer | Purpose | Update Frequency | Consumers |
|-------|---------|-----------------|-----------|
| **Bronze** | Raw ingestion -- land data as-is from source systems | Real-time to hourly | Data engineers (debugging, replay) |
| **Silver** | Cleaned, validated, deduplicated, conformed | Hourly to daily | Data engineers, data scientists |
| **Gold** | Business-level aggregates, features, metrics | Daily to on-demand | Analysts, dashboards, ML models |

**Benefits of the multi-hop pattern:**

1. **Replayability** -- Bronze preserves raw data, so you can reprocess Silver/Gold without re-ingesting
2. **Isolation** -- A bug in Silver logic does not corrupt Bronze data
3. **Incremental processing** -- Each hop reads only new/changed data from the prior layer
4. **Data quality** -- Checks are applied progressively (basic in Bronze, thorough in Silver)
5. **Performance** -- Gold tables are pre-aggregated for fast queries

---

## Bronze Layer Design

### Principles

1. **Append-only** -- never update or delete Bronze rows (immutable log)
2. **Source fidelity** -- keep data in its original form (don't rename or cast columns)
3. **Add metadata** -- enrich with ingestion metadata for lineage and debugging
4. **Store as Delta** -- get ACID transactions, time travel, and compaction

### Standard Bronze Metadata Columns

| Column | Type | Source |
|--------|------|--------|
| `_ingest_ts` | timestamp | `current_timestamp()` at load time |
| `_source_file` | string | `input_file_name()` or `_metadata.file_path` |
| `_source_file_ts` | timestamp | `_metadata.file_modification_time` |
| `_batch_id` | long | Monotonically increasing batch identifier |

### Bronze Ingestion Pattern

```python
from pyspark.sql.functions import current_timestamp, input_file_name, lit

raw_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/orders/schema/")
    .load("/landing/orders/")
)

bronze_df = (
    raw_df
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

(bronze_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders/bronze/")
    .trigger(availableNow=True)
    .toTable("bronze.orders")
)
```

---

## Silver Layer Design

### Principles

1. **Cleanse** -- fix data types, handle nulls, standardize formats
2. **Deduplicate** -- remove exact duplicates and apply late-arriving data logic
3. **Validate** -- enforce data quality rules, quarantine bad records
4. **Conform** -- standardize column names, time zones, units

### Silver Processing Pattern

```python
from pyspark.sql.functions import col, to_date, trim, upper, when

# Read incrementally from Bronze
bronze_stream = spark.readStream.table("bronze.orders")

# Apply transformations
silver_df = (
    bronze_stream
    # Type casting
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("amount", col("amount").cast("double"))
    # Standardization
    .withColumn("region", upper(trim(col("region"))))
    # Null handling
    .withColumn("amount", when(col("amount").isNull(), 0.0).otherwise(col("amount")))
    # Quality filter -- drop rows missing required fields
    .filter(col("order_id").isNotNull() & col("customer_id").isNotNull())
    # Drop metadata columns not needed downstream
    .drop("_source_file", "_source_file_ts", "_batch_id")
)

# Write to Silver table
(silver_df.writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/orders/silver/")
    .trigger(availableNow=True)
    .toTable("silver.orders")
)
```

### Deduplication Strategies

| Strategy | Use Case | Method |
|----------|----------|--------|
| Exact duplicate removal | Same row ingested twice | `dropDuplicates(["order_id"])` |
| Watermark dedup | Streaming with late arrivals | `withWatermark("ts", "1 hour").dropDuplicates(["id", "ts"])` |
| SCD Type 1 (overwrite) | Keep only latest version | `MERGE INTO` with matched update |
| SCD Type 2 (history) | Keep full history | `MERGE INTO` with insert for changed rows |

### Data Quality Checks

```python
from pyspark.sql.functions import col, when, lit

# Add a quality flag column
silver_df = (
    silver_df
    .withColumn("_quality_flags",
        when(col("amount") < 0, lit("NEGATIVE_AMOUNT"))
        .when(col("order_date") > current_date(), lit("FUTURE_DATE"))
        .otherwise(lit("PASS"))
    )
)

# Quarantine bad records
quarantine_df = silver_df.filter("_quality_flags != 'PASS'")
clean_df = silver_df.filter("_quality_flags = 'PASS'")
```

---

## Gold Layer Design

### Principles

1. **Business-aligned** -- tables map to business concepts (metrics, features, dimensions)
2. **Pre-aggregated** -- reduce query time for dashboards and reports
3. **Materialized** -- stored as Delta tables, not just views
4. **Documented** -- column descriptions, business logic documentation

### Gold Processing Pattern

```python
# Read from Silver (batch, not streaming)
silver_df = spark.table("silver.orders")

# Aggregate to Gold
gold_daily_sales = (
    silver_df
    .groupBy("order_date", "region", "product")
    .agg(
        count("order_id").alias("order_count"),
        sum("amount").alias("total_amount"),
        avg("amount").alias("avg_amount"),
        min("amount").alias("min_amount"),
        max("amount").alias("max_amount"),
    )
    .orderBy("order_date", "region")
)

# Write to Gold table
(gold_daily_sales.write
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.daily_sales_summary")
)
```

### Common Gold Table Patterns

| Pattern | Description | Example |
|---------|-------------|---------|
| **Daily summary** | Aggregates by day | `gold.daily_sales` |
| **Customer 360** | One row per customer with all attributes | `gold.customer_profile` |
| **Feature store** | ML features computed from Silver | `gold.user_features` |
| **Star schema** | Fact + dimension tables for BI | `gold.fact_orders`, `gold.dim_product` |
| **KPI table** | Pre-computed business metrics | `gold.monthly_kpis` |

---

## Incremental Processing Strategies

### Strategy 1: Structured Streaming (hop-to-hop)

Each hop uses `spark.readStream` to read from the prior Delta table and
`writeStream` to write to the next:

```
Auto Loader ──stream──> Bronze ──stream──> Silver ──batch──> Gold
```

### Strategy 2: Change Data Feed (CDF)

Delta's Change Data Feed captures row-level changes (inserts, updates, deletes).
Downstream hops read only the changes:

```python
# Enable CDF on the source table
spark.sql("ALTER TABLE silver.orders SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

# Read only changes since a specific version
changes_df = (
    spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 5)
    .table("silver.orders")
)
```

### Strategy 3: Watermark-based Incremental

Track a high-watermark (e.g., max timestamp) and read only rows after it:

```python
# Read watermark from a control table
last_watermark = spark.sql(
    "SELECT max_ts FROM control.watermarks WHERE table_name = 'silver.orders'"
).collect()[0][0]

# Read only new rows
new_rows = (
    spark.table("silver.orders")
    .filter(col("_ingest_ts") > last_watermark)
)
```

---

## Exactly-Once Semantics

### Auto Loader + Delta (Recommended)

Structured Streaming with Delta provides exactly-once semantics out of the box:

1. Auto Loader checkpoint tracks which files have been processed
2. Delta's transaction log ensures atomic writes
3. If a job fails mid-batch, the checkpoint knows where to resume
4. No duplicates, no data loss

### COPY INTO + Delta

COPY INTO tracks file-level state in the Delta transaction log. Running it
twice with the same files produces no duplicates. However, if the command
fails mid-execution, partial data may or may not be committed (depends on
the Spark execution model).

### Best Practice

```
For exactly-once guarantees:
  1. Use Auto Loader (not COPY INTO) for Bronze ingestion
  2. Use Structured Streaming for Bronze → Silver
  3. Use MERGE INTO for Silver → Gold (idempotent upserts)
  4. Never delete checkpoints unless you want to reprocess
```

---

## Orchestrating Multi-Step Pipelines

### Option 1: Databricks Workflows (Jobs)

```
Job: daily_ingestion_pipeline
├── Task 1: Bronze ingestion (Auto Loader, availableNow)
├── Task 2: Silver transformation (depends on Task 1)
└── Task 3: Gold aggregation (depends on Task 2)
```

### Option 2: Delta Live Tables (DLT)

DLT automates the multi-hop pattern declaratively:

```python
import dlt

@dlt.table(comment="Raw orders from landing zone")
def bronze_orders():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load("/landing/orders/")
    )

@dlt.table(comment="Cleaned and validated orders")
@dlt.expect_or_drop("valid_amount", "amount > 0")
def silver_orders():
    return dlt.read_stream("bronze_orders").select(...)

@dlt.table(comment="Daily sales summary")
def gold_daily_sales():
    return dlt.read("silver_orders").groupBy("order_date").agg(...)
```

### Option 3: Apache Airflow

Use Airflow DAGs with `DatabricksRunNowOperator` or
`DatabricksSubmitRunOperator` to orchestrate notebook tasks.

---

## Real-World Pipeline Example

### E-Commerce Order Pipeline

```
Source: Order events (JSON) landing in S3 every 5 minutes

Bronze (raw):
  - Auto Loader reads JSON files
  - Adds _ingest_ts, _source_file
  - Writes to bronze.order_events
  - Schema: all fields as-is from source + metadata

Silver (cleaned):
  - Reads from bronze.order_events (streaming)
  - Casts order_id to LONG, amount to DECIMAL(10,2)
  - Parses timestamp to proper TIMESTAMP type
  - Deduplicates on (order_id, event_type)
  - Validates: amount > 0, customer_id IS NOT NULL
  - Quarantines bad records to silver.order_events_quarantine
  - Writes clean records to silver.order_events

Gold (business):
  - gold.daily_revenue: revenue by day, region, product category
  - gold.customer_ltv: lifetime value per customer
  - gold.product_velocity: units sold per product per week
  - gold.fraud_features: ML features for fraud detection
```

---

## Hands-On Walkthrough

Open the companion notebook `05-multi-hop-ingestion_notebook.py`. The notebook:

1. Generates sample order CSV files as the raw source
2. Builds a Bronze table with metadata columns
3. Transforms Bronze into a Silver table with cleaning and validation
4. Creates a Gold aggregation table with daily sales metrics
5. Demonstrates the full flow end-to-end
6. Cleans up all resources

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Landing zone | S3 bucket | ADLS Gen2 container | GCS bucket |
| Notification trigger | SNS + SQS | Event Grid | Pub/Sub |
| Workflow orchestration | Databricks Jobs, Airflow (MWAA) | Databricks Jobs, ADF, Airflow | Databricks Jobs, Cloud Composer |
| DLT availability | All clouds | All clouds | All clouds |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:

- Understanding the Medallion Architecture (Bronze / Silver / Gold)
- Knowing the purpose of each layer
- Understanding incremental processing with Auto Loader + Delta
- Knowing that Bronze is raw/append-only and Gold is pre-aggregated
- Being able to describe exactly-once semantics

The **Professional** exam goes deeper:

- Designing multi-hop pipelines with DLT
- Change Data Feed for incremental processing
- SCD Type 1 and Type 2 patterns
- Data quality enforcement with expectations
- Orchestration patterns with Databricks Workflows

---

## Key Takeaways

- The Medallion Architecture separates concerns: raw storage (Bronze), cleaning (Silver), business logic (Gold)
- Bronze should be append-only and preserve source fidelity with metadata columns
- Silver applies cleaning, deduplication, and data quality checks
- Gold provides pre-aggregated, business-aligned tables for fast querying
- Auto Loader + Structured Streaming + Delta gives exactly-once semantics with minimal code
- Use Databricks Workflows or Delta Live Tables to orchestrate multi-hop pipelines
- Always design for replayability -- Bronze data enables reprocessing Silver and Gold without re-ingesting

---

## Next Steps

You have completed **Module 02: Data Ingestion**. Proceed to **Module 03:
Data Transformations** to learn how to clean, reshape, enrich, and join your
ingested data.
