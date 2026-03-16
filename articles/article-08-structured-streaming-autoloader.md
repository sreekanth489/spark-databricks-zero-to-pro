# Structured Streaming & Auto Loader: Moving Data in Real Time Through the Medallion Architecture

sreekanth keerthipati

---

In the [previous article](https://medium.com/@sreekanth489), we organized data into Bronze, Silver, and Gold layers using batch processing.

We scheduled jobs. We ran them manually. We moved data in intervals.

That works for many use cases.

But what if your source sends events every few minutes?

What if your upstream system generates continuous data — clickstreams, IoT sensor readings, transaction events?

Batch won't cut it.

You need **streaming**.

In this article, I'll cover:

- How Structured Streaming works as the streaming engine
- How it differs from batch processing
- What Auto Loader adds on top of Structured Streaming
- Three Auto Loader modes on AWS
- Schema evolution and incremental file detection
- How all of this fits into the Medallion Architecture

---

## Structured Streaming: The Engine

Let's start with the most important concept.

**Spark Structured Streaming is the streaming engine.**

It takes the same DataFrame and SQL APIs you use for batch processing and applies them to continuously arriving data.

The key mental model:

In batch, you read a **bounded table** — it has a fixed number of rows.

In streaming, you read an **unbounded table** — new rows keep arriving.

```python
# Batch
df = spark.read.format("delta").table("source_orders")

# Streaming
df = spark.readStream.format("delta").table("source_orders")
```

Same API. Same transformations. Same SQL.

The only difference: `read` vs `readStream`, `write` vs `writeStream`.

Structured Streaming handles:

- Micro-batch execution
- Checkpointing
- Fault tolerance
- Exactly-once semantics
- State management
- Watermarking for late data

---

## Streaming from a Delta Table

The most common streaming pattern in Medallion Architecture is reading from a Delta table as a stream.

```python
df_orders_stream = (
    spark.readStream
        .format("delta")
        .table("source_orders")
)

df_bronze = (
    df_orders_stream
    .filter(col("quantity") > 0)
    .withColumn("load_time", current_timestamp())
)

query = (
    df_bronze.writeStream
        .format("delta")
        .option("checkpointLocation", f"{checkpoint_path}/bronze_orders")
        .outputMode("append")
        .trigger(availableNow=True)
        .table("bronze_orders")
)

query.awaitTermination()
```

When new rows are added to `source_orders`, the stream picks up **only the new rows**.

The checkpoint tracks what's already been processed.

No re-scanning. No duplicate processing.

---

## The Checkpoint: Your Safety Net

Every streaming query has a checkpoint directory:

```
s3://bucket/checkpoints/bronze_orders/
    commits/       # completed micro-batch IDs
    offsets/       # what data was read
    sources/       # source-specific state
```

The checkpoint ensures:

- **Exactly-once processing**: each record is processed once
- **Fault tolerance**: if the job crashes, it resumes from the last completed micro-batch
- **Incremental processing**: only new data is processed

Never share a checkpoint between different streams.

Never delete a checkpoint unless you want to reprocess everything from scratch.

---

## Trigger Strategies

Triggers control **when** your streaming job processes data.

### `trigger(availableNow=True)` — Recommended for Production

Processes all available data in multiple micro-batches, then stops.

This is the most common production pattern. You schedule a Databricks Workflow to run every 5-10 minutes. Each run processes whatever data has arrived since the last run.

```python
.trigger(availableNow=True)
```

### `trigger(processingTime="30 seconds")` — Continuous

Runs a micro-batch every 30 seconds. The stream stays running indefinitely.

```python
.trigger(processingTime="30 seconds")
```

Use this when you need near-real-time updates and are willing to keep a cluster running.

### `trigger(once=True)` — Deprecated

Processes a single micro-batch. **Don't use this.** Use `availableNow` instead.

The difference: `once` processes one micro-batch and might leave data behind. `availableNow` processes **all** available data across multiple micro-batches.

---

## Stream-Static Joins

A very common pattern: join streaming data with a static lookup table.

```python
# Streaming
df_bronze_stream = spark.readStream.format("delta").table("bronze_orders")

# Static (batch read)
df_customers = spark.table("customers_lookup")

# Join
df_enriched = df_bronze_stream.join(df_customers, "customer_id", "inner")
```

This is how you enrich streaming events with reference data — customer names, product details, store locations.

The static side is re-read on each micro-batch, so it always reflects the latest data.

---

## Watermarking

If you're doing aggregations on streaming data, you need watermarking.

Without it, Spark keeps **all state forever**. Memory grows unbounded.

With watermarking, you tell Spark: "I'm willing to accept data up to X minutes late. After that, drop it."

```python
df_windowed = (
    df_stream
    .withWatermark("order_date", "1 day")
    .groupBy(window("order_date", "1 day"), "customer_id")
    .agg(count("order_id").alias("daily_orders"))
)
```

This keeps memory bounded while still handling reasonable late arrivals.

---

## Now: Auto Loader

Here's where many people get confused.

**Auto Loader is NOT a streaming engine.**

**Auto Loader is a specialized file ingestion SOURCE built on top of Structured Streaming.**

The relationship:

```
S3 / ADLS / GCS
      |
      v
Auto Loader (cloudFiles)       ← SOURCE
      |
      v
Spark Structured Streaming     ← ENGINE
      |
      v
Delta Lake (Bronze Table)
```

Every Auto Loader pipeline IS a Structured Streaming query internally.

When you run `spark.readStream.format("cloudFiles")`, Spark uses the same micro-batch engine, checkpoints, and fault tolerance as any other streaming query.

Auto Loader just provides a **better file ingestion mechanism**.

---

## Why Auto Loader Exists

Standard Spark file streaming has limitations:

```python
# Standard file streaming
spark.readStream.format("parquet").schema(my_schema).load("/data/")
```

Problems:

- **Re-lists the entire directory** on every trigger (expensive for large directories)
- **No schema inference** — you must specify the schema manually
- **No schema evolution** — if source adds a column, the job breaks
- **Struggles with millions of files**

Auto Loader solves all of these:

```python
# Auto Loader
spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("s3://bucket/data/")
```

Same streaming engine underneath. But now you get:

- **Incremental file tracking** via checkpoint
- **Schema inference** persisted to `schemaLocation`
- **Schema evolution** with `addNewColumns`
- **Efficient file discovery** via notifications or incremental listing
- **Scales to millions of files**

---

## Three Auto Loader Modes on AWS

### Mode 1: Directory Listing — Start Here

The simplest setup. Zero infrastructure required.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.schemaLocation", f"{schema_path}/events")
    .load(f"{raw_data_path}/json_events/")
```

Auto Loader scans the directory, compares against its checkpoint, and processes only new files.

It works on both Free and Premium editions.

Start here. It always works.

### Mode 2: Managed File Events — Production

The modern production path on Databricks Premium.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useManagedFileEvents", "true")
    .option("cloudFiles.schemaLocation", f"{schema_path}/events")
    .load(f"{raw_data_path}/json_events/")
```

Instead of scanning directories, Databricks listens for S3 file events.

When a new file lands, S3 sends a notification. Auto Loader picks it up in near-real-time.

No directory listing. No scanning. Much more efficient at scale.

But it requires:

1. A Unity Catalog external location
2. File events enabled on that location
3. The right IAM permissions

### Mode 3: Classic Notifications — Legacy

The older approach where Auto Loader auto-manages SNS/SQS per stream.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.region", "us-east-1")  # must match bucket region
    .option("cloudFiles.schemaLocation", f"{schema_path}/events")
    .load(f"{raw_data_path}/json_events/")
```

More moving parts. More failure modes. Use Managed File Events instead when possible.

---

## Schema Evolution

One of Auto Loader's most valuable features.

When your source adds a new column, Auto Loader handles it automatically:

```python
.option("cloudFiles.schemaEvolutionMode", "addNewColumns")
```

In the lab, I demonstrated this:

1. Batch 1 and 2 had 7 columns
2. Batch 3 added a new `referrer` column
3. Auto Loader detected the schema change, updated the persisted schema, and added the column to the Delta table
4. Old records got `NULL` for `referrer`. New records got the actual values.

No code changes. No manual intervention. The pipeline just adapts.

---

## Where Each Tool Fits in Medallion Architecture

```
S3 Raw Files
      |
      v
Auto Loader (cloudFiles)      ← Bronze layer ingestion
      |
      v
Bronze Table
      |
      v
Structured Streaming           ← Silver and Gold transformations
(readStream from Delta)
      |
      v
Silver → Gold
```

**Auto Loader** is typically used only at the **Bronze layer** — for ingesting raw files from cloud storage.

**Structured Streaming** from Delta tables is used for **Silver and Gold** — reading from one Delta table and writing to the next.

---

## One Sentence to Remember

If someone asks you in an interview or certification exam:

> *"How are Structured Streaming and Auto Loader related?"*

Answer:

> Auto Loader is a file ingestion mechanism built on top of Spark Structured Streaming. It provides scalable file discovery, schema inference, and incremental ingestion from cloud storage, while relying on Structured Streaming to execute the micro-batch processing pipeline.

---

## What's Next?

We've now covered:

- How to **organize** data (Medallion Architecture)
- How to **move** data with batch processing
- How to **stream** data with Structured Streaming
- How to **ingest files** efficiently with Auto Loader

But we've been running each layer manually.

In a real production system, you need:

- **Orchestration** — run Bronze before Silver, Silver before Gold
- **Data quality expectations** — automatically flag bad records
- **Declarative pipelines** — define what you want, not how to run it

That's what **Delta Live Tables** (now called Spark Declarative Pipelines) provides.

In the next session, we'll build a complete declarative pipeline with built-in data quality checks, automatic dependency resolution, and continuous processing.

---

All the lab notebooks are available on GitHub:

- [Day 19: Structured Streaming](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day19-structured-streaming)
- [Day 20: Auto Loader](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day20-auto-loader)
- [Day 00: Environment Setup](https://github.com/sreekanth489/spark-databricks-zero-to-pro/tree/main/day00-environment-setup)

---

*Previously in this series:*

- [Medallion Architecture: Building Production Data Pipelines with Bronze, Silver, and Gold Layers](#) *(previous article)*
- [Inside the Delta Log — The Complete Series](https://medium.com/@sreekanth489/inside-the-delta-log-the-complete-series-acid-internals-performance-concurrency-a5db53b2fb6f)
- [From Data Lakes to Delta Lake: A Practical Guide](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30)
- [Why Hadoop, Spark, and Databricks Exist](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148)
