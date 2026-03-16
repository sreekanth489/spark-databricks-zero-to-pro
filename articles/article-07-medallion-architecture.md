# Medallion Architecture: Building Production Data Pipelines with Bronze, Silver, and Gold Layers

sreekanth keerthipati

---

If you've been following this series, you now understand:

- Why Hadoop and Spark exist
- How Delta Lake brings ACID to object storage
- What happens inside `_delta_log` when you write, update, or delete

But knowing how Delta Lake works internally is only half the story.

The real question is:

**How do you organize your data in a lakehouse?**

That's where Medallion Architecture comes in.

---

## What Is Medallion Architecture?

![Medallion Architecture: Bronze, Silver, Gold layers with progressive data quality](images/medallion-architecture.png)

Medallion Architecture is a data design pattern that organizes your lakehouse into three layers:

**Bronze** — raw data, as-is from source

**Silver** — cleaned, validated, enriched data

**Gold** — business-ready aggregations and KPIs

Each layer progressively improves data quality.

Think of it as ETL — but structured into distinct, well-governed layers.

Databricks didn't invent ETL. But they gave it a name, a structure, and a set of best practices that make pipelines easier to build, debug, and govern.

---

## Why Do We Need Layers?

Imagine you dump all your data — raw events, cleaned records, business metrics — into one schema.

What happens?

- Debugging becomes painful. Where did the bad data come from?
- Reprocessing is risky. You can't safely re-run transformations without affecting downstream consumers.
- Access control is hard. Business analysts don't need raw event logs. Data engineers don't need dashboard KPIs.
- Data quality is invisible. You can't measure how much data was filtered or enriched at each stage.

Separating data into layers solves all of these problems.

![Data quality improves as data flows through each layer](images/medallion-data-quality.png)

---

## Bronze Layer: Raw Data

The Bronze layer stores data **exactly as received from source**.

No transformations. No filtering. No cleaning.

Just raw data plus ingestion metadata.

```python
df_bronze = (
    spark.read.parquet(f"{raw_data_path}/orders/batch1")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", lit("batch1"))
)

df_bronze.write.format("delta").mode("append").save(f"{bronze_path}/orders")
```

Notice: `mode("append")`.

Bronze is always **append-only**. You never overwrite raw data.

Why?

Because if something goes wrong downstream — a bad join, a wrong filter, a data quality issue — you need to go back to the raw data and reprocess.

If you overwrite Bronze, you lose that safety net.

And since cloud storage is cheap, there's no reason not to keep every raw record.

### How to Ingest into Bronze

For batch ingestion: `spark.read` + `df.write.mode("append")`

For production ingestion from cloud storage: **Auto Loader** (`cloudFiles`).

Databricks recommends Auto Loader as the **best practice for Bronze layer ingestion**. It provides incremental file tracking, schema inference, and schema evolution — all the things you need when ingesting raw files from S3 or ADLS.

Auto Loader is covered in detail in the [next article](#) on Structured Streaming.

### Who uses Bronze?

Mostly data engineers. For debugging, reprocessing, and auditing.

Business users almost never query Bronze directly.

---

## Silver Layer: Cleaned and Enriched

The Silver layer is where data quality happens.

This is where you:

- **Filter** invalid records (null primary keys, negative quantities)
- **Deduplicate** on business keys
- **Join** with reference data (customer names, product details)
- **Parse** timestamps into human-readable formats
- **Calculate** derived fields (total_amount = quantity * price)

```python
df_silver = (
    df_bronze_orders
    .dropDuplicates(["order_id"])
    .filter(col("quantity") > 0)
    .filter(col("customer_id").isNotNull())
    .join(df_customers, "customer_id", "inner")
    .join(df_products, "product_id", "inner")
    .withColumn("order_date",
        from_unixtime(col("order_timestamp"), "yyyy-MM-dd HH:mm:ss").cast("timestamp"))
    .withColumn("total_amount", round(col("quantity") * col("price"), 2))
)
```

### Proving Data Quality

In the lab, I intentionally included dirty records in the raw data:

| Order ID | Problem |
|----------|---------|
| ORD-9901 | NULL customer_id |
| ORD-9902 | NULL customer_id |
| ORD-9903 | quantity = 0 |
| ORD-9904 | quantity = -1 |
| ORD-0001 | Duplicate order_id |

Bronze had 55 records. Silver had 50.

Five records filtered out. Each one accounted for.

That's the whole point. You can **prove** your Silver layer is clean.

### CHECK Constraints

After writing to Silver, I added Delta Lake constraints:

```sql
ALTER TABLE silver.orders ADD CONSTRAINT valid_quantity CHECK (quantity > 0);
ALTER TABLE silver.orders ADD CONSTRAINT valid_amount CHECK (total_amount > 0);
```

Now if any future batch tries to insert a record with quantity <= 0, Delta Lake will reject it.

Constraints are your safety net at the table level.

### Incremental Updates with MERGE

For incremental processing, Silver uses **MERGE** (upsert):

```python
silver_delta = DeltaTable.forPath(spark, f"{silver_path}/orders")

silver_delta.alias("target").merge(
    df_new_silver.alias("source"),
    "target.order_id = source.order_id"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
```

MERGE handles both new records (insert) and updated records (update) in a single operation.

This makes Silver writes **idempotent** — you can safely re-run without creating duplicates.

### Who uses Silver?

Data engineers, data scientists, and analysts doing ad-hoc exploration.

Silver provides a clean, enterprise-wide view of your data.

---

## Gold Layer: Business Aggregations

Gold is where you answer business questions.

You take the clean data from Silver and create **purpose-built aggregation tables**.

```python
# Daily revenue by city
df_daily_revenue = (
    df_silver_orders
    .withColumn("order_day", date_trunc("day", col("order_date")))
    .groupBy("order_day", "city")
    .agg(
        sum("total_amount").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
    )
)
```

In the lab, I created three Gold tables:

- **Daily Revenue** — regional sales dashboards
- **Customer Summary** — lifetime value, order frequency
- **Product Performance** — best sellers, revenue ranking

### Gold is always overwritten

Unlike Bronze (append) and Silver (merge), Gold tables are **fully refreshed**.

Why?

Because aggregations depend on the complete dataset.

If a customer placed 10 orders last week and 5 more this week, the Gold table should show 15 total — not just the 5 new ones.

### Who uses Gold?

Business analysts, executives, BI tools (Tableau, Power BI, Looker).

Gold is the consumption layer. Dashboards connect here.

---

## Separate Schemas Per Layer

In production, each layer gets its own schema in Unity Catalog:

```sql
USE CATALOG databricks_pro;

CREATE SCHEMA bronze COMMENT 'Raw, unfiltered data';
CREATE SCHEMA silver COMMENT 'Cleansed, validated data';
CREATE SCHEMA gold   COMMENT 'Business-ready aggregations';
```

This gives you:

- **Clean separation**: `bronze.orders` vs `silver.orders` vs `gold.daily_revenue`
- **Access control**: Grant business users access to `gold` only
- **Discoverability**: Anyone browsing the catalog sees the purpose of each schema

---

## The Write Patterns

| Layer | Write Mode | Why |
|-------|-----------|-----|
| Bronze | APPEND | Preserve raw data. Never overwrite. |
| Silver | MERGE (upsert) | Idempotent incremental updates. |
| Gold | OVERWRITE | Full refresh of aggregations. |

This pattern works for both batch and streaming pipelines.

---

## OPTIMIZE and Z-ORDER

After loading data, you should periodically optimize your tables:

```sql
OPTIMIZE silver.orders ZORDER BY (customer_id, order_date);
```

What does this do?

**OPTIMIZE** compacts small files into larger ones for better read performance.

**Z-ORDER** co-locates related data in the same files.

Before Z-ORDER, if you query `WHERE customer_id = 'C001'`, Spark might scan all 4 files because C001's records are scattered everywhere.

After Z-ORDER, C001's records are grouped together in one file. Spark skips the other 3 files entirely.

This is **data skipping** in action — and it can speed up queries by 10-100x on large tables.

If you want the full internals of how Z-ORDER and data skipping work at the file level, read my deep dive:

> [Inside the Delta Log (Part 6): Stats, Data Skipping & Z-ORDER Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-6-stats-data-skipping-z-order-internals-59cb7f1c89c7)

---

## Benefits of Medallion Architecture

Why does this pattern work so well?

- **Simple data model** — easy to understand and implement. Three layers, three purposes.
- **Incremental ETL** — each layer processes only new or changed data, not the full dataset.
- **Mix streaming and batch** — each layer can independently be batch or streaming. Bronze can use Auto Loader while Gold uses scheduled batch.
- **Reprocessing from raw data** — since Bronze preserves everything, you can recreate Silver and Gold at any time.
- **Data governance** — separate schemas with separate access controls. Business users see Gold. Engineers see Bronze.
- **Debugging** — if Gold numbers look wrong, trace back through Silver to Bronze. The audit trail is built in.

And because every layer is Delta Lake, you get ACID transactions, time travel, and schema enforcement at every step.

---

## Time Travel Across Layers

Because every layer is Delta, you get time travel for free:

```sql
-- How many records did Bronze have before batch 2?
SELECT COUNT(*) FROM bronze.orders VERSION AS OF 0;

-- How many after batch 2?
SELECT COUNT(*) FROM bronze.orders;
```

This is invaluable for debugging.

If a Gold table looks wrong, you can trace back through Silver and Bronze to find exactly where the problem started.

---

## Metadata and AI

One thing I emphasized in the session: **always add comments and metadata**.

```sql
CREATE SCHEMA bronze COMMENT 'Raw, unfiltered data with ingestion metadata';
```

Why?

Because Databricks has recently integrated AI features like Genie and AI/BI dashboards. These tools rely on metadata to understand your tables.

The more context you provide — schema comments, table comments, column descriptions — the better AI features work.

This is a habit worth building now.

---

## What's Next?

So far we've moved data between layers using **batch processing** — manual runs, scheduled jobs.

But what if your source sends events every few minutes?

What if you need near-real-time dashboards?

That's where **Structured Streaming** and **Auto Loader** come in.

In the next article, I'll cover:

- How Structured Streaming treats data as an unbounded table
- The relationship between Structured Streaming (the engine) and Auto Loader (the source)
- Three modes of Auto Loader on AWS
- Schema evolution and incremental file detection
- How to build a streaming Medallion pipeline

If this article helped you understand how to organize data in a lakehouse, the next one will show you how to keep it flowing in real time.

---

All the lab notebooks for this session are available on GitHub:

- [Day 18: Medallion Architecture](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day18-medallion-architecture)
- [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day00-environment-setup)

---

*Previously in this series:*

- [Why Hadoop, Spark, and Databricks Exist — And Why We Even Need Delta Lake](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148)
- [From Data Lakes to Delta Lake: A Practical Guide](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30)
- [Inside the Delta Log — The Complete Series](https://medium.com/@sreekanth489/inside-the-delta-log-the-complete-series-acid-internals-performance-concurrency-a5db53b2fb6f)
