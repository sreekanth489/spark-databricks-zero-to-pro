# Medallion Architecture

> Module 03 -- Topic 07 | Level: Intermediate | Time: 50 min

## Learning Objectives

- Describe the Medallion (Bronze/Silver/Gold) architecture in depth
- Design a Bronze layer for raw, append-only ingestion
- Design a Silver layer for cleaned, deduplicated, standardized data
- Design a Gold layer for business-level aggregates and feature tables
- Implement cross-layer data quality checks
- Identify anti-patterns and real-world design decisions

## Conceptual Overview

### What Is the Medallion Architecture?

The Medallion Architecture is a data design pattern that organizes data in a
Lakehouse into three layers of increasing quality and refinement:

```
  +===================================================================+
  |                    MEDALLION ARCHITECTURE                          |
  +===================================================================+
  |                                                                    |
  |  +--------------------+    +--------------------+    +----------+  |
  |  |     BRONZE         |    |     SILVER         |    |   GOLD   |  |
  |  |     (Raw)          |--->|     (Cleaned)      |--->|  (Biz)   |  |
  |  +--------------------+    +--------------------+    +----------+  |
  |                                                                    |
  |  Source data as-is         Deduplicated              Aggregated    |
  |  Append-only               Type-cast                 Star schema   |
  |  Schema-on-read            Standardized              KPIs          |
  |  Full history              Quality-checked           Feature tables|
  |  Minimal transforms        Joined/enriched           BI-ready      |
  +====================================================================+

  Data Sources                                          Consumers
  ============                                          =========
  - Kafka topics           BRONZE --> SILVER --> GOLD    - BI dashboards
  - REST APIs                                           - SQL analytics
  - File drops                                          - ML models
  - Database CDC                                        - Reports
  - IoT sensors                                         - Applications
```

### Bronze Layer (Raw)

The Bronze layer is the **landing zone** for all data entering the Lakehouse.

**Design principles**:

| Principle | Details |
|-----------|---------|
| **Append-only** | Never update or delete -- raw data is immutable history |
| **Schema-on-read** | Accept any schema; infer or apply loose schemas |
| **Full fidelity** | Store data exactly as received from the source |
| **Metadata enrichment** | Add ingestion timestamp, source system, file name |
| **Minimal transformation** | Only add metadata columns, do not cleanse |

**Typical Bronze table structure**:

```
  +----------+----------+--------+-----+-----------------+---------+
  | raw_col1 | raw_col2 | raw_...|     | _ingest_ts      | _source |
  +----------+----------+--------+-----+-----------------+---------+
  | original | data     | as-is  | ... | 2025-01-15 10:00| kafka   |
  | from     | the      | source | ... | 2025-01-15 10:01| kafka   |
  +----------+----------+--------+-----+-----------------+---------+
                                        ^                  ^
                                        metadata columns added
```

**When to use Bronze**:

- Replay failed Silver processing from the raw source
- Audit what the system originally received
- Debug data quality issues by examining raw data
- Feed multiple Silver tables from a single Bronze source

**Common Bronze patterns**:

```python
# Pattern 1: Streaming ingestion from Kafka
(spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", servers)
    .option("subscribe", "orders")
    .load()
    .selectExpr(
        "CAST(key AS STRING)",
        "CAST(value AS STRING)",
        "topic",
        "partition",
        "offset",
        "timestamp AS kafka_timestamp",
        "current_timestamp() AS ingest_timestamp"
    )
    .writeStream
    .format("delta")
    .outputMode("append")
    .toTable("bronze.orders_raw"))

# Pattern 2: Batch ingestion from files
(spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("/landing/customers/")
    .withColumn("_ingest_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("bronze.customers_raw"))
```

### Silver Layer (Cleaned)

The Silver layer contains **cleaned, conformed, and enriched** data.

**Design principles**:

| Principle | Details |
|-----------|---------|
| **Schema-on-write** | Enforce strict schemas with correct data types |
| **Deduplicated** | Remove duplicate records using business keys |
| **Type-cast** | Cast strings to proper types (dates, numbers) |
| **Standardized** | Consistent naming, formats, units |
| **Quality-checked** | Apply data quality rules, quarantine bad records |
| **Enriched** | Join with reference tables, add derived columns |

**Typical Silver transformations**:

```
  Bronze (raw)                       Silver (clean)
  ============                       ==============

  order_id: "12345" (string)  --->   order_id: 12345 (int)
  amt: "99.99" (string)       --->   amount: 99.99 (double)
  dt: "01/15/2025" (string)   --->   order_date: 2025-01-15 (date)
  cust_id: "C001"             --->   customer_id: "C001"
  null customer records       --->   quarantined (separate table)
  duplicate records           --->   deduplicated
  raw column names            --->   standardized snake_case
```

**Deduplication patterns**:

```python
# Deduplicate using window function (keep latest by timestamp)
from pyspark.sql.window import Window

window = Window.partitionBy("order_id").orderBy(F.desc("_ingest_timestamp"))

silver_df = (bronze_df
    .withColumn("row_num", F.row_number().over(window))
    .filter("row_num = 1")
    .drop("row_num"))
```

**Data quality quarantine**:

```python
# Separate good and bad records
good_records = bronze_df.filter("order_id IS NOT NULL AND amount > 0")
bad_records = bronze_df.filter("order_id IS NULL OR amount <= 0")

good_records.write.format("delta").saveAsTable("silver.orders")
bad_records.write.format("delta").saveAsTable("silver.orders_quarantine")
```

### Gold Layer (Business)

The Gold layer contains **business-level aggregates and curated datasets**.

**Design principles**:

| Principle | Details |
|-----------|---------|
| **Business-oriented** | Organized by business domain, not source system |
| **Aggregated** | Pre-computed KPIs, summaries, metrics |
| **Star schema** | Fact and dimension tables for BI |
| **Performance-optimized** | Partitioned, Z-ordered, or Liquid Clustered |
| **Governed** | Column-level access controls, masking |

**Typical Gold table examples**:

```
  Gold Layer Examples:
  ====================

  gold.daily_revenue_summary
    - date, region, product_category, total_revenue, order_count, avg_order_value

  gold.customer_360
    - customer_id, name, ltv, last_order_date, preferred_category, churn_risk

  gold.product_performance
    - product_id, name, units_sold_30d, revenue_30d, return_rate, avg_rating

  gold.inventory_status
    - warehouse, product, current_stock, reorder_point, days_of_supply
```

**Gold aggregation example**:

```python
gold_daily_revenue = (
    spark.table("silver.orders")
    .groupBy("order_date", "region", "product_category")
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("avg_order_value"),
    )
)

gold_daily_revenue.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold.daily_revenue_summary")
```

### Cross-Layer Data Quality

Data quality should be enforced at every layer:

```
  +----------+     +----------+     +----------+
  |  BRONZE  |     |  SILVER  |     |   GOLD   |
  +----------+     +----------+     +----------+
  |          |     |          |     |          |
  | Ingest   |     | Type     |     | Business |
  | checks:  |     | checks:  |     | checks:  |
  | - not    |     | - not    |     | - totals |
  |   empty  |     |   null   |     |   match  |
  | - schema |     | - ranges |     | - no     |
  |   infer  |     | - formats|     |   gaps   |
  | - file   |     | - dedup  |     | - SLAs   |
  |   complete|    | - ref    |     | - fresh  |
  +----------+     +----------+     +----------+
```

You can use **Delta expectations** (Delta Live Tables) or custom checks:

```python
# Custom quality check example
silver_count = spark.table("silver.orders").count()
bronze_count = spark.table("bronze.orders_raw").select("order_id").distinct().count()

completeness = silver_count / bronze_count * 100
print(f"Silver completeness: {completeness:.1f}%")
assert completeness > 95, f"Data loss detected: only {completeness:.1f}% of records made it to Silver"
```

### Real-World Examples

**E-Commerce Platform**

```
  Bronze                     Silver                    Gold
  ======                     ======                    ====
  orders_raw                 orders                    daily_sales_summary
  customers_raw              customers                 customer_lifetime_value
  clickstream_raw            sessions                  product_recommendations
  inventory_events_raw       inventory_current         inventory_alerts
  returns_raw                returns                   return_rate_by_product
```

**IoT / Manufacturing**

```
  Bronze                     Silver                    Gold
  ======                     ======                    ====
  sensor_readings_raw        sensor_readings_clean     equipment_health_score
  machine_events_raw         machine_events            downtime_summary
  quality_inspections_raw    quality_inspections       defect_rate_by_line
  maintenance_logs_raw       maintenance_history       predictive_maintenance
```

### Anti-Patterns

| Anti-Pattern | Why It Is Bad | Better Approach |
|-------------|--------------|----------------|
| Transforming in Bronze | Lose raw data fidelity | Transform in Silver |
| Skipping Silver | Gold queries raw data, fragile | Always clean in Silver |
| One giant Gold table | Hard to maintain, slow | Domain-specific Gold tables |
| No dedup in Silver | Duplicates propagate to Gold | Dedup with window functions |
| Deleting Bronze data | Cannot replay/debug | Retain Bronze with TTL |
| Schema-on-read in Gold | Breaks dashboards | Strict schemas from Silver on |
| Over-engineering Bronze | Delays data availability | Keep Bronze simple |

### Naming Conventions

```
  Recommended naming pattern:
  ===========================

  {layer}.{source_system}_{entity}_{qualifier}

  Examples:
    bronze.kafka_orders_raw
    bronze.api_customers_raw
    silver.orders_cleaned
    silver.orders_quarantine
    gold.daily_revenue_summary
    gold.customer_360_features
```

## Hands-On Walkthrough

Open the companion notebook `07-medallion-architecture_notebook.py` in your
Databricks workspace. You will build a complete Bronze-to-Silver-to-Gold
pipeline using an e-commerce dataset:

1. Generate raw e-commerce event data
2. Ingest into a Bronze table (with metadata columns)
3. Clean, deduplicate, and type-cast into a Silver table
4. Aggregate into Gold summary tables
5. Validate data quality across layers

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Medallion Architecture | Architecture pattern, cloud-agnostic | Same | Same |
| Unity Catalog (3-level namespace) | `catalog.schema.table` | Same | Same |
| Recommended catalog layout | `bronze`, `silver`, `gold` schemas | Same | Same |
| Delta Live Tables (DLT) | Automates medallion pipelines | Same | Same |

The Medallion Architecture is a logical pattern -- it works identically on all
cloud providers. Unity Catalog's 3-level namespace maps naturally to the
medallion layers.

## Certification Tip

Medallion Architecture is a **core exam topic** for both Associate and
Professional:

- "What are the three layers?" -- Bronze (raw), Silver (cleaned), Gold (business)
- "What transformations happen in each layer?" -- Bronze: none/metadata only;
  Silver: dedup, typecast, quality; Gold: aggregate, star schema
- "Why is Bronze append-only?" -- preserves raw data for replay and audit
- "What is the purpose of the Silver layer?" -- single source of truth for
  cleaned, conformed data
- "How does the Gold layer differ from Silver?" -- Gold is business-oriented
  aggregates; Silver is cleaned entity-level data

Expect 3-5 questions on Medallion Architecture across exam formats.

## Key Takeaways

1. The Medallion Architecture organizes Lakehouse data into three layers:
   Bronze (raw), Silver (cleaned), and Gold (business aggregates).
2. Bronze is append-only and preserves raw data fidelity with metadata
   enrichment.
3. Silver enforces schemas, deduplicates, type-casts, and applies data
   quality rules.
4. Gold contains pre-computed, business-oriented aggregates optimized for
   BI and ML consumption.
5. Cross-layer data quality checks ensure data integrity throughout the
   pipeline.

## Next Steps

Proceed to [08 - Delta Sharing](08-delta-sharing.md) to learn how to share
Delta Lake data across organizations without copying.
