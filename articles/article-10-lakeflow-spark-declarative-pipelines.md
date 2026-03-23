# From Imperative Spark to Declarative Pipelines: The Evolution That Changes Everything

sreekanth keerthipati

---

In Session 3 of our community series, we built a complete Medallion Architecture.

Bronze ingested raw orders from S3 using Auto Loader. Silver cleaned and enriched them. Gold aggregated them into business metrics.

It worked.

But here's what we actually did: we opened a notebook, ran the Bronze script, waited for it to finish, opened the Silver notebook, ran that, waited again, then ran Gold.

Three scripts. Three manual executions. Constant babysitting.

And when someone in the session asked, "How do we run this in production?" — the honest answer was: "You'd need an orchestrator like Airflow, a retry framework, checkpoint management, and a lot of glue code."

That's the imperative world. You tell Spark **how** to do everything.

What if you could just tell it **what** you want?

---

## What You'll Learn

- Why imperative pipelines break down in production
- The evolution from Traditional Spark to DLT to Spark Declarative Pipelines
- Core abstractions: streaming tables, materialized views, views
- How the pipeline DAG is auto-generated from your code
- Data quality enforcement with expectations
- Auto CDC to replace manual MERGE logic
- Building an end-to-end e-commerce pipeline with SDP
- Built-in retries, dry runs, and parallelization

---

## The Problem with Imperative Pipelines

Let's be honest about what running a production Medallion pipeline looked like before Spark Declarative Pipelines.

### Manual Orchestration

You needed an external orchestrator. Apache Airflow was the most common choice.

```python
# Airflow DAG (simplified)
bronze_task = SparkSubmitOperator(task_id="bronze_orders", ...)
silver_task = SparkSubmitOperator(task_id="silver_orders", ...)
gold_task   = SparkSubmitOperator(task_id="gold_metrics", ...)

bronze_task >> silver_task >> gold_task
```

You defined the dependency graph manually. If you added a new Silver table, you had to update the DAG. If you renamed a table, you had to update the DAG. If you reordered the logic, you had to update the DAG.

The orchestration code lived in a completely different system from your transformation code.

### Manual Checkpoint Management

Every streaming query needed its own checkpoint path:

```python
.option("checkpointLocation", f"s3://bucket/checkpoints/bronze_orders")
```

Manage one checkpoint? Easy. Manage 50 checkpoints across 50 tables? A nightmare.

Delete a checkpoint accidentally? Full reprocessing from scratch.

### Manual Retry Logic

Network glitch at 3 AM? Your Bronze script fails. Silver and Gold never run.

You need retry logic. Backoff strategies. Alerting. Someone on call.

In our session, when we ran the orders pipeline and it worked on the first try, everyone was happy. But in production, "works on the first try" is the exception, not the rule.

### Manual CDC Merge Code

When your source system sends updates (not just inserts), you need MERGE logic:

```sql
MERGE INTO silver.orders AS target
USING bronze.orders_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Write this once? Manageable. Write it for every table that receives updates? That's a lot of boilerplate that looks almost identical but varies just enough to be error-prone.

### No Built-In Data Quality

Want to ensure `order_amount > 0`? Write a filter. Want to log violations? Write custom logging. Want to fail the pipeline on critical quality issues? Write custom exception handling.

Every team reinvents these patterns.

---

## The Evolution: Three Eras of Spark Pipelines

![Spark Declarative Pipelines: The Evolution](images/lakeflow-spark-declarative-pipelines.png)
<p align="center"><em>Image credit: <a href="https://www.databricks.com/product/lakeflow">Databricks</a></em></p>

The way we build data pipelines in Databricks has gone through three distinct eras.

### Era 1: Traditional Spark (2015-2021)

This is what we did in Session 3. Pure imperative code.

```python
# Bronze
df_raw = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", schema_path)
        .load("s3://bucket/raw/orders/")
)

(
    df_raw.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/bronze_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze.orders")
)

# Silver
df_bronze = spark.readStream.table("bronze.orders")
df_silver = (
    df_bronze
        .filter(col("order_amount") > 0)
        .dropDuplicates(["order_id"])
        .withColumn("processed_at", current_timestamp())
)

(
    df_silver.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/silver_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("silver.orders")
)
```

You managed everything: read, transform, write, checkpoint, trigger, output mode.

It worked. But it was verbose, fragile, and required external orchestration.

### Era 2: Delta Live Tables / DLT (2021-2024)

Databricks introduced Delta Live Tables to bring declarative thinking to data pipelines.

```python
import dlt

@dlt.table
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("s3://bucket/raw/orders/")
    )

@dlt.table
@dlt.expect("valid_amount", "order_amount > 0")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")
            .withColumn("processed_at", current_timestamp())
    )
```

Big improvements:

- **Declarative**: define what you want, not how to run it
- **Automatic orchestration**: dependencies detected from code
- **Built-in expectations**: data quality as a first-class citizen
- **Managed checkpoints**: no more manual checkpoint paths

But DLT used a **custom API** (`import dlt`, `dlt.read_stream`). It looked different from standard PySpark. Skills didn't transfer cleanly. Testing was harder. And the `dlt` module only worked inside DLT pipelines — you couldn't run it in a regular notebook.

### Era 3: Lakeflow Spark Declarative Pipelines (2024-Present)

The current evolution. Same declarative benefits as DLT, but built on **standard PySpark**.

```python
from pyspark import pipelines as dp

@dp.table
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("s3://bucket/raw/orders/")
    )

@dp.table
@dp.expect("valid_amount", "order_amount > 0")
def silver_orders():
    return (
        spark.readStream.table("LIVE.bronze_orders")
            .withColumn("processed_at", current_timestamp())
    )
```

Notice what changed:

- `import dlt` became `from pyspark import pipelines as dp`
- `dlt.read_stream("bronze_orders")` became `spark.readStream.table("LIVE.bronze_orders")`
- Everything else is standard PySpark

This matters because:

- **Standard PySpark APIs**: your existing Spark skills transfer directly
- **Testable outside pipelines**: functions return DataFrames you can test in any notebook
- **IDE-friendly**: full autocomplete, type hints, debugging support
- **Backwards compatible**: existing DLT pipelines continue to work

### The Evolution at a Glance

| Aspect | Traditional Spark | DLT | Lakeflow SDP |
|--------|------------------|-----|--------------|
| **API** | PySpark / SQL | Custom `dlt` module | Standard PySpark + decorators |
| **Orchestration** | External (Airflow) | Automatic | Automatic |
| **Checkpoints** | Manual | Managed | Managed |
| **Data quality** | Custom code | `@dlt.expect` | `@dp.expect` |
| **CDC** | Manual MERGE | `apply_changes()` | `create_auto_cdc_flow()` |
| **Testing** | Standard | DLT-only | Standard |
| **Dependency graph** | Manual DAG | Auto-detected | Auto-detected |
| **Retries** | Custom | Built-in | Built-in |

### The Cooking Analogy

In our session, I used a cooking analogy that resonated:

**Traditional Spark** is like cooking every dish yourself from scratch. You buy ingredients, chop vegetables, manage multiple pots on the stove, time everything manually. If you burn the pasta, you restart from scratch.

**DLT** is like having a smart kitchen assistant. You describe what dishes you want, and the assistant manages the cooking process. But the assistant speaks a different language than you — you have to learn their commands.

**Lakeflow SDP** is like running a full restaurant kitchen. You describe the menu (declarative), the kitchen staff handles execution (automatic orchestration), quality control checks every dish (expectations), and if something goes wrong, the sous chef retries automatically. And everyone speaks the same language — standard PySpark.

---

## Core Abstractions: Dataset Types

Spark Declarative Pipelines gives you four ways to define datasets. Choosing the right one for each layer is critical.

### Streaming Table — `@dp.table`

```python
@dp.table
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load("s3://bucket/raw/orders/")
    )
```

A streaming table is **append-only**.

New data is added. Existing data is never modified.

This is the natural fit for **Bronze** layer tables. Raw data comes in, gets appended, never changed.

Key characteristics:

- Uses `readStream` internally
- Checkpoints are managed automatically
- Supports Auto Loader, Kafka, Delta streaming sources
- Rows are never updated or deleted (append-only)

### Materialized View — `@dp.materialized_view`

```python
@dp.materialized_view
def gold_daily_revenue():
    return (
        spark.sql("""
            SELECT
                order_date,
                SUM(order_amount) AS total_revenue,
                COUNT(DISTINCT customer_id) AS unique_customers
            FROM LIVE.silver_orders
            GROUP BY order_date
        """)
    )
```

A materialized view **recomputes its entire result** on each pipeline run.

Think of it as `CREATE OR REPLACE TABLE ... AS SELECT ...` — but managed by the pipeline.

This is the natural fit for **Silver and Gold** layer tables. You want the latest, complete, correct result every time.

Key characteristics:

- Uses batch `read` internally (even if not explicitly stated)
- Recomputes fully on each run
- Supports aggregations, joins, complex transformations
- Results are always consistent and up-to-date

### View — `@dp.view`

```python
@dp.view
def orders_with_region():
    return (
        spark.sql("""
            SELECT o.*, s.region
            FROM LIVE.silver_orders o
            JOIN LIVE.silver_stores s ON o.store_id = s.store_id
        """)
    )
```

A view is an **intermediate staging step**. It's not persisted to storage.

Use it when you need to break complex logic into readable steps without creating a physical table.

Key characteristics:

- Not persisted — exists only during pipeline execution
- Useful for intermediate transformations
- Reduces code duplication
- Can be referenced by downstream tables

### Temporary View

```python
@dp.view(
    name="reusable_date_logic",
    comment="Helper view scoped to this pipeline"
)
def reusable_date_logic():
    return spark.sql("SELECT current_date() AS today, date_sub(current_date(), 30) AS thirty_days_ago")
```

A temporary view is **scoped to the current pipeline only**. It cannot be referenced by other pipelines or queried directly.

Use it for helper logic that's only relevant within one pipeline.

### When to Use Which

| Dataset Type | Persisted? | Recomputed? | Best For |
|-------------|-----------|-------------|----------|
| **Streaming Table** (`@dp.table`) | Yes | No (append-only) | Bronze layer, raw ingestion |
| **Materialized View** (`@dp.materialized_view`) | Yes | Yes (full recompute) | Silver/Gold layers, aggregations |
| **View** (`@dp.view`) | No | Yes | Intermediate staging |
| **Temporary View** (`@dp.view`) | No | Yes | Pipeline-scoped helpers |

The rule of thumb from our session:

**Bronze = Streaming Table.** Data flows in, gets appended.

**Silver/Gold = Materialized View.** Data is transformed, always fresh.

**In-between = View.** Staging logic that doesn't need its own table.

---

## The Pipeline Graph (DAG)

Here's one of the most powerful features of Spark Declarative Pipelines.

**You don't define the DAG. The pipeline builds it for you.**

When you reference `LIVE.bronze_orders` in your Silver table definition, the pipeline knows Silver depends on Bronze. When Gold references `LIVE.silver_orders`, it knows Gold depends on Silver.

```python
@dp.table
def bronze_orders():       # No dependencies — runs first
    return spark.readStream.format("cloudFiles").load(...)

@dp.materialized_view
def silver_orders():       # Depends on bronze_orders
    return spark.readStream.table("LIVE.bronze_orders").filter(...)

@dp.materialized_view
def gold_daily_revenue():  # Depends on silver_orders
    return spark.sql("SELECT ... FROM LIVE.silver_orders GROUP BY ...")
```

The pipeline analyzes the code, detects that:

1. `bronze_orders` has no upstream dependencies
2. `silver_orders` reads from `LIVE.bronze_orders`
3. `gold_daily_revenue` reads from `LIVE.silver_orders`

And builds this DAG:

```
bronze_orders → silver_orders → gold_daily_revenue
```

In the Databricks UI, you see this as a **visual graph** — boxes connected by arrows, showing the flow of data through your pipeline.

No Airflow. No manual DAG definition. No configuration files.

**Add a new table that reads from Silver? The DAG updates automatically.**

In our community session, we watched the pipeline graph grow in real time. We started with one Bronze table. Added a Silver table. The arrow appeared. Added Gold. Another arrow. Added a second Silver table. It branched. The entire dependency structure emerged from the code itself.

That moment was when the power of declarative pipelines really clicked for the group.

---

## Data Quality with Expectations

In production, bad data is inevitable. Null order amounts. Negative quantities. Future-dated timestamps.

Spark Declarative Pipelines gives you **expectations** — declarative data quality rules that are enforced automatically.

### Three Levels of Enforcement

**`@dp.expect` — Track but Don't Act**

```python
@dp.table
@dp.expect("valid_amount", "order_amount > 0")
def silver_orders():
    return spark.readStream.table("LIVE.bronze_orders")
```

Records that violate the expectation are **kept** in the table. But the violation is **logged and tracked** in the pipeline's data quality metrics.

Use this when you want visibility into data quality without blocking the pipeline.

**`@dp.expect_or_drop` — Drop Bad Records**

```python
@dp.table
@dp.expect_or_drop("valid_amount", "order_amount > 0")
def silver_orders():
    return spark.readStream.table("LIVE.bronze_orders")
```

Records that violate the expectation are **silently dropped**. They never make it into the table.

Use this for Bronze-to-Silver cleansing. Drop records that are clearly invalid — negative amounts, null primary keys, corrupted data.

**`@dp.expect_or_fail` — Stop the Pipeline**

```python
@dp.table
@dp.expect_or_fail("has_order_id", "order_id IS NOT NULL")
def silver_orders():
    return spark.readStream.table("LIVE.bronze_orders")
```

If **any** record violates the expectation, the **entire pipeline fails**.

Use this for critical constraints. If `order_id` is null, something is fundamentally wrong with your source data, and continuing would produce garbage downstream.

### Combining Expectations

You can stack multiple expectations on a single table:

```python
@dp.table
@dp.expect("valid_amount", "order_amount > 0")
@dp.expect_or_drop("not_null_customer", "customer_id IS NOT NULL")
@dp.expect_or_fail("has_order_id", "order_id IS NOT NULL")
def silver_orders():
    return spark.readStream.table("LIVE.bronze_orders")
```

This says:

- **Track** orders with non-positive amounts (keep them, but flag them)
- **Drop** orders with null customer IDs (bad but not critical)
- **Fail** if any order has a null order ID (critical data issue)

### Expectations in the UI

The pipeline UI shows expectation metrics in real time:

- How many records passed each expectation
- How many failed
- What percentage of your data meets quality standards

This gives you a **data quality dashboard for free** — no additional tooling needed.

---

## Auto CDC: Change Data Capture Made Simple

This is where Spark Declarative Pipelines saves the most boilerplate code.

### The Before: Manual MERGE

In traditional Spark, handling CDC (inserts, updates, deletes from a source system) required complex MERGE logic:

```sql
MERGE INTO silver.orders AS target
USING (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY updated_at DESC
        ) AS rn
    FROM bronze.orders_cdc
    WHERE rn = 1
) AS source
ON target.order_id = source.order_id
WHEN MATCHED AND source.operation = 'DELETE'
    THEN DELETE
WHEN MATCHED AND source.operation = 'UPDATE'
    THEN UPDATE SET
        target.order_amount = source.order_amount,
        target.status = source.status,
        target.updated_at = source.updated_at
WHEN NOT MATCHED AND source.operation != 'DELETE'
    THEN INSERT *
```

That's 20+ lines of SQL for a single table. And you need it for every table that receives updates.

Handle deduplication. Handle deletes. Handle ordering. Handle late arrivals.

Error-prone. Tedious. A breeding ground for bugs.

### The After: Auto CDC

```python
from pyspark import pipelines as dp

dp.create_auto_cdc_flow(
    name="silver_orders",
    target="silver_orders",
    source="LIVE.bronze_orders_cdc",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1
)
```

Six lines. That's it.

Auto CDC handles:

- **Deduplication** — automatically picks the latest record per key
- **Ordering** — uses `sequence_by` to determine which record is newest
- **Inserts, updates, deletes** — all handled automatically
- **SCD Type 1 or Type 2** — your choice

### SCD Type 1 vs Type 2

**SCD Type 1 (Overwrite):**

```python
dp.create_auto_cdc_flow(
    name="silver_orders",
    target="silver_orders",
    source="LIVE.bronze_orders_cdc",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type=1
)
```

When an order is updated, the old row is **overwritten** with the new values. No history is kept.

**SCD Type 2 (History Preservation):**

```python
dp.create_auto_cdc_flow(
    name="silver_orders",
    target="silver_orders",
    source="LIVE.bronze_orders_cdc",
    keys=["order_id"],
    sequence_by="updated_at",
    stored_as_scd_type=2
)
```

When an order is updated, the old row is **closed** (end date set) and a new row is **inserted**. Full history is preserved.

The pipeline automatically manages `__start_at` and `__end_at` columns for SCD Type 2.

### When to Use Which SCD Type

| Scenario | SCD Type | Why |
|----------|----------|-----|
| Customer current address | Type 1 | Only need the latest address |
| Customer address history | Type 2 | Need to track where they lived over time |
| Product current price | Type 1 | Only need today's price |
| Product price history | Type 2 | Need to track price changes over time |
| Order status tracking | Type 2 | Need to see status transitions |
| Employee current department | Type 1 | Only need current org structure |

---

## Hands-On: Building the E-Commerce Pipeline

In our community session, we built this pipeline step by step. Let me walk you through the complete implementation.

### The Pipeline Structure

```
Sources:
  - orders (JSON files in S3 — streaming)
  - stores (CSV file in S3 — static)

Bronze:
  - bronze_orders (streaming table via Auto Loader)
  - bronze_stores (materialized view from CSV)

Silver:
  - silver_calendar (generated dimension table)
  - silver_stores (cleansed store data)
  - silver_orders (cleansed with expectations + auto CDC)

Gold:
  - gold_fact_orders (join orders + stores + calendar)
  - gold_regional_revenue (aggregate by region)
```

### Bronze Layer: Ingesting Raw Data

**Orders — Streaming Table:**

```python
from pyspark import pipelines as dp

@dp.table(
    comment="Raw orders ingested from S3 via Auto Loader"
)
def bronze_orders():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaLocation", f"{schema_path}/bronze_orders")
            .load(f"{raw_data_path}/orders/")
            .withColumn("_load_timestamp", current_timestamp())
            .withColumn("_source_file", input_file_name())
    )
```

This is a **streaming table**. As new JSON files land in S3, Auto Loader detects them and appends them to the Bronze table.

We add two audit columns:

- `_load_timestamp` — when the record was ingested
- `_source_file` — which file it came from

These are invaluable for debugging in production. When something looks wrong, you can trace it back to the exact source file.

**Stores — Materialized View:**

```python
@dp.materialized_view(
    comment="Raw store reference data from CSV"
)
def bronze_stores():
    return (
        spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(f"{raw_data_path}/stores/stores.csv")
            .withColumn("_load_timestamp", current_timestamp())
    )
```

Why a materialized view instead of a streaming table?

Because store data is **reference data**. It doesn't stream in continuously. The CSV file is updated occasionally, and when it is, we want the **entire table refreshed** with the latest version.

Streaming tables append. Materialized views replace. Choose based on the data behavior.

### Silver Layer: Cleaning and Enriching

**Calendar Dimension — Materialized View:**

```python
@dp.materialized_view(
    comment="Generated calendar dimension table"
)
def silver_calendar():
    return (
        spark.sql("""
            SELECT
                date AS calendar_date,
                year(date) AS year,
                month(date) AS month,
                dayofmonth(date) AS day,
                dayofweek(date) AS day_of_week,
                CASE
                    WHEN dayofweek(date) IN (1, 7) THEN 'Weekend'
                    ELSE 'Weekday'
                END AS day_type,
                quarter(date) AS quarter,
                date_format(date, 'MMMM') AS month_name,
                date_format(date, 'EEEE') AS day_name,
                weekofyear(date) AS week_of_year
            FROM (
                SELECT explode(sequence(
                    to_date('2023-01-01'),
                    to_date('2025-12-31'),
                    interval 1 day
                )) AS date
            )
        """)
    )
```

This is one of my favorite patterns. **The calendar table is generated entirely from code.** No source file needed. No ingestion. Just pure SQL generating a date dimension.

Every analytics pipeline needs a calendar dimension. This pattern means you never need to maintain a calendar CSV.

**Stores — Cleansed:**

```python
@dp.materialized_view(
    comment="Cleansed store dimension with standardized fields"
)
@dp.expect_or_drop("valid_store_id", "store_id IS NOT NULL")
def silver_stores():
    return (
        spark.sql("""
            SELECT
                store_id,
                TRIM(store_name) AS store_name,
                UPPER(TRIM(region)) AS region,
                TRIM(city) AS city,
                TRIM(state) AS state,
                current_timestamp() AS _processed_at
            FROM LIVE.bronze_stores
        """)
    )
```

Notice the data quality expectation: `expect_or_drop` on `store_id IS NOT NULL`. If any store record has a null ID, it gets dropped silently. It's reference data — we can't join on a null key.

The transformations are standard cleansing: trim whitespace, standardize casing, add processing timestamp.

**Orders — Cleansed with Expectations and Auto CDC:**

```python
@dp.table(
    comment="Cleansed order facts with data quality expectations"
)
@dp.expect("valid_amount", "order_amount > 0")
@dp.expect_or_drop("not_null_order_id", "order_id IS NOT NULL")
@dp.expect_or_drop("not_null_customer", "customer_id IS NOT NULL")
def silver_orders_staging():
    return (
        spark.readStream
            .table("LIVE.bronze_orders")
            .withColumn("order_date", to_date("order_timestamp"))
            .withColumn("_processed_at", current_timestamp())
    )
```

Three expectations stacked:

1. **Track** orders with non-positive amounts (keep but flag)
2. **Drop** orders with null order IDs
3. **Drop** orders with null customer IDs

And if the source system sends updates to existing orders (status changes, amount corrections), we use Auto CDC:

```python
dp.create_auto_cdc_flow(
    name="silver_orders_cdc",
    target="silver_orders",
    source="LIVE.silver_orders_staging",
    keys=["order_id"],
    sequence_by="order_timestamp",
    stored_as_scd_type=1
)
```

Orders are upserted by `order_id`. The latest version (by `order_timestamp`) wins. SCD Type 1 — we only keep the current state.

### Gold Layer: Business Metrics

**Fact Orders — Materialized View:**

```python
@dp.materialized_view(
    comment="Fact table joining orders with store and calendar dimensions"
)
def gold_fact_orders():
    return (
        spark.sql("""
            SELECT
                o.order_id,
                o.customer_id,
                o.order_amount,
                o.order_date,
                o.status,
                s.store_name,
                s.region,
                s.city,
                s.state,
                c.day_of_week,
                c.day_type,
                c.quarter,
                c.month_name,
                c.day_name,
                o._processed_at
            FROM LIVE.silver_orders o
            LEFT JOIN LIVE.silver_stores s
                ON o.store_id = s.store_id
            LEFT JOIN LIVE.silver_calendar c
                ON o.order_date = c.calendar_date
        """)
    )
```

This is where everything comes together. Orders joined with stores and calendar to produce a rich fact table.

Notice the `LIVE.` prefix. This tells SDP that these are tables defined **within the same pipeline**. The pipeline uses this to build the dependency graph.

**Regional Revenue — Materialized View:**

```python
@dp.materialized_view(
    comment="Regional revenue aggregation for executive dashboards"
)
def gold_regional_revenue():
    return (
        spark.sql("""
            SELECT
                region,
                month_name,
                quarter,
                COUNT(DISTINCT order_id) AS total_orders,
                COUNT(DISTINCT customer_id) AS unique_customers,
                SUM(order_amount) AS total_revenue,
                AVG(order_amount) AS avg_order_value,
                MIN(order_date) AS first_order_date,
                MAX(order_date) AS last_order_date
            FROM LIVE.gold_fact_orders
            GROUP BY region, month_name, quarter
        """)
    )
```

An executive dashboard table. Revenue by region, by month, by quarter. Built from the fact table, which is built from Silver, which is built from Bronze, which is built from raw S3 files.

The entire chain — from raw JSON to executive dashboard — is defined declaratively.

### The Complete Pipeline DAG

When you run this pipeline in Databricks, the UI shows:

```
                                    ┌──────────────────┐
S3 Orders ──► bronze_orders ──────► │ silver_orders     │──►┐
                                    │ (staging + CDC)   │   │
                                    └──────────────────┘   │
                                                           │   ┌───────────────────┐
                                                           ├──►│ gold_fact_orders   │──► gold_regional_revenue
                                                           │   └───────────────────┘
S3 Stores ──► bronze_stores ──► silver_stores ─────────────┘           ▲
                                                                       │
                                silver_calendar ───────────────────────┘
```

Seven tables. One pipeline. Zero manual orchestration.

Add an eighth table? The DAG updates. Remove one? The DAG updates. Rename a reference? The DAG updates.

In our session, watching this graph build itself as we added tables was the "aha" moment for most participants.

---

## Built-In Benefits You Get for Free

### Automatic Retries

In traditional Spark, a network timeout at 3 AM means your pipeline fails and you get paged.

In SDP, retries are built in. A transient failure triggers an automatic retry. The pipeline picks up from the last successful state.

No on-call rotations for transient errors. No custom retry logic. No 3 AM wake-up calls.

During our session, I told the group: "This is the feature you don't appreciate until the first night you **don't** get woken up."

### Dry Run Validation

Before using any compute resources, you can run a **dry run** that validates your entire pipeline:

- Are all table references valid?
- Are there circular dependencies?
- Do the SQL queries parse correctly?
- Are expectations syntactically valid?

This catches errors **before** you spend money on compute. In production, this means your deployment pipeline can validate SDP code in CI/CD before it ever runs on a cluster.

### Automatic Parallelization

Look at the DAG again. `bronze_orders` and `bronze_stores` have no dependencies on each other. Neither do `silver_stores` and `silver_calendar`.

SDP detects this and runs independent branches **in parallel**.

You don't configure this. You don't tune thread pools. The pipeline analyzes the DAG and maximizes parallelism automatically.

### Pre-Validation of the Entire Graph

Before processing any data, SDP validates the **entire pipeline graph**:

- All `LIVE.` references resolve to actual tables
- No circular dependencies exist
- Schema compatibility between upstream and downstream tables
- Expectation expressions are valid SQL

If anything is wrong, you get a clear error **before** a single byte of data is processed.

---

## The `LIVE.` Prefix

You've seen `LIVE.` throughout this article. Let's clarify what it means.

When you reference `LIVE.bronze_orders`, you're telling SDP: "This table is defined **within this pipeline**."

SDP uses this to:

1. **Build the dependency graph** — it knows which tables depend on which
2. **Ensure freshness** — upstream tables are processed before downstream ones
3. **Route data correctly** — during development vs production, the actual catalog/schema may differ

Without `LIVE.`, SDP would look for the table in the default catalog/schema, which might not be where the pipeline's tables live.

**Always use `LIVE.` when referencing tables within the same pipeline.**

For external tables (tables not defined in this pipeline), use the full catalog path: `catalog.schema.table_name`.

---

## Migration from DLT to SDP

If you have existing DLT pipelines, the migration is straightforward:

| DLT | SDP |
|-----|-----|
| `import dlt` | `from pyspark import pipelines as dp` |
| `@dlt.table` | `@dp.table` |
| `@dlt.view` | `@dp.view` |
| `dlt.read("table")` | `spark.read.table("LIVE.table")` |
| `dlt.read_stream("table")` | `spark.readStream.table("LIVE.table")` |
| `@dlt.expect(...)` | `@dp.expect(...)` |
| `dlt.apply_changes(...)` | `dp.create_auto_cdc_flow(...)` |

The key shift: DLT used custom APIs for reading data (`dlt.read`, `dlt.read_stream`). SDP uses standard PySpark APIs (`spark.read.table`, `spark.readStream.table`) with the `LIVE.` prefix.

Existing DLT pipelines continue to work. The `dlt` module is still supported. But new development should use the `dp` module.

---

## Pipeline Configuration

When you create a pipeline in the Databricks UI, you configure:

**Target catalog and schema:**

```
Catalog: my_catalog
Schema: my_schema
```

All tables created by the pipeline land here.

**Compute:**

- **Serverless** (recommended) — no cluster management
- **Classic** — you configure the cluster

**Pipeline mode:**

- **Triggered** — runs once when triggered, processes available data, stops
- **Continuous** — runs continuously, processing data as it arrives

**Development vs Production:**

- **Development mode** — relaxed settings, faster iteration, reprocesses all data
- **Production mode** — strict settings, incremental processing, optimized for reliability

---

## One Sentence to Remember

If someone asks you in an interview or certification exam:

> *"What is the difference between a streaming table and a materialized view in Spark Declarative Pipelines?"*

Answer:

> A streaming table is append-only and processes only new data incrementally (ideal for Bronze), while a materialized view recomputes its entire result on each pipeline run (ideal for Silver and Gold aggregations).

---

## Key Takeaways

1. **Spark Declarative Pipelines replaces imperative orchestration with declarative definitions.** You define what tables you want and how they relate. The pipeline handles execution order, retries, and parallelization.

2. **The evolution from Traditional Spark to DLT to SDP** is a progression toward standard APIs. SDP uses regular PySpark with decorators — your existing skills transfer directly.

3. **Use streaming tables for Bronze** (append-only ingestion) and **materialized views for Silver/Gold** (full recompute with latest data).

4. **Expectations give you built-in data quality**: `expect` (track), `expect_or_drop` (drop bad records), `expect_or_fail` (stop the pipeline).

5. **Auto CDC replaces manual MERGE logic** with a single function call. Supports SCD Type 1 (overwrite) and Type 2 (history).

6. **The pipeline DAG is auto-generated from code.** Reference `LIVE.upstream_table` in your code, and the dependency graph builds itself.

7. **Built-in retries, dry runs, and parallelization** eliminate the need for external orchestration and custom error handling.

---

## What's Next?

We've covered how to get data in (Lakeflow Connect) and how to transform it (Spark Declarative Pipelines).

But how do we run all of this reliably in production?

How do we schedule pipelines? Handle failures? Compose multiple pipelines into a single workflow? Deploy across environments?

That's **Lakeflow Jobs** — the orchestration layer that ties everything together.

In the next article, we'll build a complete production workflow: ingestion, transformation, quality checks, and notification — all running on a schedule with automatic retries and repair runs.

---

All the lab notebooks are available on GitHub:

- [Day 23: Lakeflow Spark Declarative Pipelines](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day23-lakeflow-spark-declarative-pipelines)
- [Day 22: Lakeflow Connect](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day22-lakeflow-connect)
- [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day00-environment-setup)

---

*Previously in this series:*

- [Lakeflow Connect: Getting Data Into the Lakehouse Without Writing a Single Line of Code](https://medium.com/@sreekanth489) *(previous article)*
- [Structured Streaming & Auto Loader: Moving Data in Real Time Through the Medallion Architecture](https://medium.com/@sreekanth489)
- [Medallion Architecture: Building Production Data Pipelines with Bronze, Silver, and Gold Layers](https://medium.com/@sreekanth489)
- [Inside the Delta Log — The Complete Series](https://medium.com/@sreekanth489/inside-the-delta-log-the-complete-series-acid-internals-performance-concurrency-a5db53b2fb6f)
- [From Data Lakes to Delta Lake: A Practical Guide](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30)
- [Why Hadoop, Spark, and Databricks Exist](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148)
