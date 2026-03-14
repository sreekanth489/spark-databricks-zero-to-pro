# Structured Streaming
> Module: Data Engineering Pipelines | Day 19 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain Structured Streaming as a stream processing engine
- Distinguish between Structured Streaming (engine) and Auto Loader (source)
- Build streaming pipelines from Delta tables and file sources
- Perform stream-static joins for data enrichment
- Apply watermarking for late data handling
- Choose the right trigger and output mode for your use case

---

## Conceptual Overview

### What is Structured Streaming?

**Structured Streaming** is Spark's stream processing engine. It treats a live data stream as an unbounded table that is continuously appended with new rows. You write streaming queries using the same DataFrame/SQL API as batch queries.

```
                    Unbounded Input Table
                    +---+---+---+---+---+
New data arrives -> | 1 | 2 | 3 | 4 | 5 | ...
                    +---+---+---+---+---+
                            |
                      Query (same as batch)
                            |
                            v
                    +---+---+---+---+---+
Output (Result) ->  | A | B | C | D | E | ...
                    +---+---+---+---+---+
```

**Key guarantees**:
- **Exactly-once processing**: checkpoints ensure each record is processed once
- **Fault tolerance**: automatic recovery from failures using checkpoint state
- **Consistency**: end-to-end exactly-once with Delta Lake as both source and sink

### Structured Streaming vs Batch

| Aspect | Batch | Streaming |
|--------|-------|-----------|
| Read API | `spark.read` | `spark.readStream` |
| Write API | `df.write` | `df.writeStream` |
| Execution | Runs once, processes all data | Runs continuously or triggered |
| New data | Must re-read everything | Automatically picks up new data |
| State | Stateless (each run independent) | Stateful (checkpoints track progress) |
| Use case | Historical analysis, backfills | Real-time ingestion, live dashboards |

---

## Structured Streaming vs Auto Loader

This is a critical distinction that many people confuse.

**Structured Streaming is the streaming ENGINE.**
**Auto Loader is a specialized file ingestion SOURCE built on top of Structured Streaming.**

```
S3 / ADLS / GCS
      |
      v
Auto Loader (cloudFiles)     <-- SOURCE (Day 20)
      |
      v
Spark Structured Streaming   <-- ENGINE (this session)
      |
      v
Transformations
      |
      v
Delta Lake
```

Every Auto Loader pipeline IS a Structured Streaming query internally. When you run `spark.readStream.format("cloudFiles")`, Spark internally uses its micro-batch execution engine to process the data.

| Aspect | Structured Streaming | Auto Loader |
|--------|---------------------|-------------|
| What it is | Stream processing engine | File ingestion source |
| API | `spark.readStream` / `spark.writeStream` | `spark.readStream.format("cloudFiles")` |
| Handles | Micro-batch execution, checkpointing, fault tolerance, state management | File discovery, schema inference, schema evolution, incremental tracking |
| Streaming engine? | Yes | No -- uses Structured Streaming |
| File discovery | Basic (lists entire directory) | Highly optimized (notifications, incremental listing) |
| Schema evolution | Manual | Automatic |
| Handles millions of files | Not well | Yes |

**Standard file source** (Structured Streaming only):
```python
df = spark.readStream.format("parquet").schema(my_schema).load("/data/input")
```

**Auto Loader** (Structured Streaming + optimized file discovery):
```python
df = spark.readStream.format("cloudFiles").option("cloudFiles.format", "parquet").load("s3://bucket/data")
```

Both use the same streaming engine underneath. Auto Loader just provides a better file ingestion mechanism. See **Day 20** for full Auto Loader coverage.

---

## Streaming Sources

Structured Streaming can read from multiple source types:

| Source | Format | Use Case |
|--------|--------|----------|
| **Delta Lake** | `delta` | Stream from Delta tables (recommended) |
| **File source** | `parquet`, `json`, `csv` | Stream from raw files (basic) |
| **Auto Loader** | `cloudFiles` | Optimized file ingestion (Day 20) |
| **Kafka** | `kafka` | Event streams, message queues |
| **Rate source** | `rate` | Testing and development |

### Streaming from Delta Tables

The most common and recommended source for downstream streaming:

```python
df = spark.readStream.format("delta").table("my_catalog.my_schema.orders")
```

Delta Lake as a streaming source provides:
- ACID guarantees
- Automatic tracking of new data via the transaction log
- No need to list directories
- Schema enforcement

### Streaming from Files

Basic file streaming (without Auto Loader):

```python
df = (spark.readStream
    .format("parquet")
    .schema(my_schema)          # schema required for file sources
    .load("s3://bucket/raw/")
)
```

**Limitation**: Re-lists the entire directory on every trigger. Does not scale well for millions of files. For production file ingestion, use Auto Loader (Day 20).

---

## Streaming Sinks

| Sink | Format | Use Case |
|------|--------|----------|
| **Delta Lake** | `delta` | Production tables (recommended) |
| **Parquet** | `parquet` | Raw file output |
| **Kafka** | `kafka` | Event publishing |
| **Console** | `console` | Development/debugging |
| **Memory** | `memory` | Testing (creates temp table) |

---

## Trigger Strategies

Triggers control when Structured Streaming processes data.

| Trigger | Behavior | Stops? | Use Case |
|---------|----------|--------|----------|
| `trigger(availableNow=True)` | Process all available data in multiple micro-batches | Yes | Scheduled Workflows |
| `trigger(processingTime="30 seconds")` | One micro-batch every 30 seconds | No | Near-real-time |
| `trigger(processingTime="0 seconds")` | ASAP, back-to-back | No | Lowest latency |
| `trigger(once=True)` | Single micro-batch | Yes | **Deprecated** |
| No trigger (default) | ASAP | No | Development |

**Production recommendation**: Use `trigger(availableNow=True)` for scheduled Databricks Workflows. It processes ALL pending data across multiple micro-batches then stops -- cost-efficient and reliable.

**Key difference**: `once=True` processes only ONE micro-batch (may leave data behind). `availableNow=True` processes ALL available data across multiple micro-batches. Always prefer `availableNow`.

---

## Output Modes

| Mode | Behavior | When to Use |
|------|----------|-------------|
| **append** | Only new rows written to sink | No aggregations, file ingestion (most common) |
| **complete** | Entire result table rewritten each trigger | Aggregations without watermark |
| **update** | Only changed rows written | Aggregations with watermark |

**Rules**:
- No aggregations: only `append` works
- Aggregations without watermark: only `complete` works
- Aggregations with watermark: `append` or `update`

---

## Checkpointing

Checkpoints are the backbone of exactly-once processing. They persist the streaming query's progress so it can resume from where it left off after a failure or restart.

```
s3://bucket/checkpoints/my_stream/
    commits/       # completed micro-batch IDs
    offsets/       # what data was read in each micro-batch
    sources/       # source-specific state (e.g., file list)
    state/         # aggregation state (if applicable)
    metadata       # stream metadata
```

**Best practices**:
- Store on S3 (same region as data for low latency)
- Never share a checkpoint between different streams
- Never delete a checkpoint unless you want to reprocess all data
- Use consistent naming: `s3://bucket/checkpoints/{table_name}`

---

## Stream-Static Joins

A common pattern is joining a streaming DataFrame with a static (batch) DataFrame for enrichment. This is how you add customer names, product details, etc. to streaming events.

```python
# Streaming source
df_orders_stream = spark.readStream.table("orders_bronze")

# Static lookup (read as batch)
df_customers = spark.table("customers_lookup")

# Join: streaming orders with static customers
df_enriched = df_orders_stream.join(df_customers, "customer_id", "inner")

# Write enriched stream
df_enriched.writeStream.format("delta").table("orders_silver")
```

**Important**: The static DataFrame is re-read on each micro-batch, so it always reflects the latest data.

---

## Watermarking

Watermarking tells Structured Streaming how long to wait for late-arriving data before finalizing results.

```python
df_with_watermark = (
    df_stream
    .withWatermark("event_time", "10 minutes")  # allow 10 min late
    .groupBy(
        window("event_time", "5 minutes"),       # 5-min tumbling window
        "customer_id"
    )
    .count()
)
```

**How it works**:
1. Spark tracks the maximum event time seen so far
2. The watermark = max event time - threshold (e.g., 10 minutes)
3. Data arriving before the watermark is included in aggregations
4. Data arriving after the watermark may be dropped

**Without watermark**: Spark must keep ALL state forever (memory grows unbounded)
**With watermark**: Spark can discard old state, keeping memory bounded

---

## Stream Monitoring

### Check Active Streams

```python
for stream in spark.streams.active:
    print(f"Name: {stream.name}, ID: {stream.id}")
    print(f"Status: {stream.status}")
    print(f"Progress: {stream.lastProgress}")
```

### Stream Progress Details

`stream.lastProgress` returns a dict with:
- `numInputRows`: rows processed in last micro-batch
- `inputRowsPerSecond`: ingestion rate
- `processedRowsPerSecond`: processing rate
- `durationMs`: timing breakdown

### Graceful Shutdown

```python
for stream in spark.streams.active:
    print(f"Stopping: {stream.name}")
    stream.stop()
    stream.awaitTermination()
```

---

## Micro-Batch Execution Model

Structured Streaming processes data in micro-batches:

```
Trigger fires
    |
    v
Read new data from source (Delta log, file listing, Kafka offsets)
    |
    v
Create micro-batch DataFrame
    |
    v
Execute query plan (same as batch Spark)
    |
    v
Write results to sink
    |
    v
Commit checkpoint (offset, state)
    |
    v
Wait for next trigger
```

Each micro-batch is a complete Spark job. The checkpoint records which data has been processed, enabling exactly-once semantics.

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Recommended sink | Delta Lake on S3 | Delta Lake on ADLS | Delta Lake on GCS |
| Kafka source | Amazon MSK | Azure Event Hubs | Confluent on GCP |
| Checkpoint storage | S3 | ADLS Gen2 | GCS |
| Workflow orchestration | Databricks Workflows | Databricks Workflows | Databricks Workflows |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- `spark.read` vs `spark.readStream` -- batch vs streaming
- Checkpoint purpose and exactly-once guarantees
- Trigger modes: know when to use `availableNow` vs `processingTime`
- Output modes: which modes work with aggregations
- Watermarking: syntax and effect on late data
- Stream-static joins: how static side is refreshed
- Relationship between Structured Streaming and Auto Loader

---

## Key Takeaways

1. **Structured Streaming** is Spark's stream processing ENGINE -- it handles micro-batch execution, checkpointing, fault tolerance, and state management
2. **Auto Loader** is a specialized file ingestion SOURCE built on top of Structured Streaming (covered in Day 20)
3. Every Auto Loader pipeline IS a Structured Streaming query -- same engine underneath
4. **Delta Lake** is the recommended streaming source and sink
5. **Stream-static joins** enrich streaming data with batch lookup tables
6. **Watermarking** bounds state and handles late data in aggregations
7. **`trigger(availableNow=True)`** is the recommended trigger for scheduled Workflows
8. **Checkpoints** enable exactly-once processing -- never share or delete them
9. Use **append** mode for non-aggregation pipelines, **complete** for aggregations

---

## Hands-On Walkthrough

See the accompanying notebook: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py)

The lab covers:
- Streaming from Delta tables
- Stream-static joins for enrichment
- Streaming from raw files (standard file source)
- Trigger strategies comparison
- Output modes: append vs complete
- Watermarking for late data
- Stream monitoring and graceful shutdown
- Relationship to Auto Loader (conceptual)

## Next Steps

- [Day 20: Auto Loader](../day20-auto-loader/) -- the optimized file ingestion source built on Structured Streaming
