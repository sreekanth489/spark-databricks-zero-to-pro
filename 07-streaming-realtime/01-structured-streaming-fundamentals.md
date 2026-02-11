# Structured Streaming Fundamentals
> Module 07 -- Topic 01 | Level: Beginner-Intermediate | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:

1. Explain the difference between batch and streaming processing paradigms
2. Describe Spark's micro-batch execution model and its trade-offs
3. Use `readStream` and `writeStream` to build a basic streaming pipeline
4. Identify the built-in input sources (file, socket, rate, Delta) and output sinks
5. Configure checkpointing for fault-tolerant streaming pipelines
6. Monitor streaming query status and progress

## Conceptual Overview

### Batch vs Streaming: Two Paradigms

```
  BATCH PROCESSING                    STREAM PROCESSING
  +------------------+                +------------------+
  | Bounded dataset  |                | Unbounded data   |
  | (files, tables)  |                | (events, logs)   |
  +--------+---------+                +--------+---------+
           |                                   |
     [Process ALL]                    [Process as it arrives]
           |                                   |
           v                                   v
  +------------------+                +------------------+
  | Complete result  |                | Continuously     |
  | (once)           |                | updated result   |
  +------------------+                +------------------+
```

Batch processing operates on a finite, known dataset. You read all the data, process it,
and write the result. Streaming processing operates on data that never ends. New records
arrive continuously, and your pipeline must process them incrementally.

### Spark's Unbounded Table Model

Structured Streaming's key insight is that a stream can be treated as a **table that grows
continuously**. Every new record that arrives is like a new row appended to an input table.
Your query runs against this growing table, and Spark incrementally updates the result.

```
  Time T1:     Input Table          Result Table
               +--------+           +--------+
               | row 1  |   ──>     | agg 1  |
               | row 2  |           +--------+
               +--------+

  Time T2:     Input Table          Result Table
               +--------+           +--------+
               | row 1  |   ──>     | agg 1' |  (updated)
               | row 2  |           +--------+
               | row 3  | <-- new
               +--------+

  Time T3:     Input Table          Result Table
               +--------+           +--------+
               | row 1  |   ──>     | agg 1''|  (updated again)
               | row 2  |           +--------+
               | row 3  |
               | row 4  | <-- new
               | row 5  | <-- new
               +--------+
```

This model means you write streaming queries using the **same DataFrame API** you already
know from batch processing. The engine handles the incremental execution.

### Micro-Batch Execution

Spark Structured Streaming uses a **micro-batch** architecture by default:

```
  Continuous data stream
  ════════════════════════════════════════════>  time

  |<-batch 0->|<-batch 1->|<-batch 2->|<-batch 3->|
       |            |            |            |
       v            v            v            v
  [Process]    [Process]    [Process]    [Process]
       |            |            |            |
       v            v            v            v
    Output       Output       Output       Output
```

At each trigger interval, Spark:
1. Checks for new data since the last batch
2. Processes the new data as a mini DataFrame
3. Writes results to the output sink
4. Records progress in the checkpoint

**Latency**: Typically 100ms to a few seconds per micro-batch. This is not true real-time
(sub-millisecond), but it is sufficient for the vast majority of streaming use cases.

**Continuous processing mode** exists (experimental) and provides ~1ms latency, but it
supports only a limited set of operations. Micro-batch remains the production standard.

### Input Sources

| Source | Description | Use Case |
|--------|-------------|----------|
| **File** | Monitors a directory for new files (JSON, CSV, Parquet, ORC, text) | Log ingestion, data lake landing zones |
| **Rate** | Generates rows at a configurable rate (testing only) | Development, benchmarking |
| **Socket** | Reads UTF-8 text from a TCP socket (testing only) | Tutorials, quick prototyping |
| **Delta** | Reads a Delta table as a stream (only new rows) | CDC pipelines, medallion architecture |
| **Kafka** | Reads from Apache Kafka topics | Event-driven architectures |
| **cloudFiles** | Auto Loader (Databricks-specific) | Production file ingestion |

### Output Sinks

| Sink | Description | Use Case |
|------|-------------|----------|
| **Console** | Prints to stdout (testing only) | Development, debugging |
| **Memory** | Stores in an in-memory table (testing only) | Interactive exploration |
| **File** | Writes to files (JSON, Parquet, CSV, etc.) | Data lake output |
| **Delta** | Writes to a Delta table | Production pipelines (recommended) |
| **Kafka** | Writes to Kafka topics | Event publishing |
| **foreach / foreachBatch** | Custom sink logic | Databases, APIs, custom systems |

### The readStream / writeStream API

```python
# Reading a stream
streaming_df = (
    spark.readStream
    .format("rate")            # source format
    .option("rowsPerSecond", 10)
    .load()
)

# Transformations (same as batch!)
transformed = streaming_df.select("timestamp", "value")

# Writing the stream
query = (
    transformed.writeStream
    .format("console")         # sink format
    .outputMode("append")      # append, complete, or update
    .option("checkpointLocation", "/tmp/checkpoint")
    .start()
)
```

### Checkpointing: The Foundation of Fault Tolerance

Checkpointing is **mandatory** for production streaming pipelines. It records:

1. **Offsets**: What data has been consumed from the source
2. **State**: Running aggregations, join buffers, deduplication state
3. **Commit log**: Which batches have been completed

```
  Checkpoint Directory
  /mnt/checkpoints/my-query/
  ├── offsets/        <-- what data has been read
  │   ├── 0
  │   ├── 1
  │   └── 2
  ├── commits/        <-- which batches completed
  │   ├── 0
  │   ├── 1
  │   └── 2
  ├── state/          <-- aggregation state (if any)
  │   └── 0/
  │       └── ...
  └── metadata        <-- query metadata
```

If a streaming query fails and restarts, it reads the checkpoint to determine exactly
where it left off. Combined with Delta Lake's idempotent writes, this provides
**exactly-once processing guarantees**.

**Critical rules for checkpoints**:
- Each streaming query MUST have its own unique checkpoint location
- Never delete a checkpoint while the query is running
- Never share a checkpoint between different queries
- Use durable storage (cloud object storage, not local disk)

## Hands-On Walkthrough

Open the companion notebook `01-structured-streaming-fundamentals_notebook.py` and work
through these exercises:

1. **Rate source streaming**: Generate synthetic data and stream it to a memory sink
2. **Streaming DataFrame inspection**: Check `isStreaming`, print the schema
3. **File-based streaming**: Write JSON files and read them as a stream
4. **Streaming query monitoring**: Use `query.status`, `query.recentProgress`, and
   `query.lastProgress` to inspect running queries
5. **Checkpointing**: Configure a checkpoint and observe the directory structure

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Delta streaming sink | S3 | ADLS Gen2 | GCS |
| Checkpoint storage | S3 | ADLS Gen2 | GCS |
| File source paths | `s3://bucket/path/` | `abfss://container@account.dfs.core.windows.net/path/` | `gs://bucket/path/` |
| Recommended file format | Delta (all clouds) | Delta (all clouds) | Delta (all clouds) |
| Auto Loader support | Full | Full | Full |

Regardless of cloud provider, Delta Lake is the recommended format for both streaming
sources and sinks because of its transaction log, schema enforcement, and exactly-once
write guarantees.

## Certification Tip

The Databricks Data Engineer Associate exam heavily tests Structured Streaming:
- Know the difference between `readStream` and `read` (streaming vs batch)
- Understand why checkpointing is required for production (fault tolerance)
- Know which sources and sinks are for testing only (socket, console, memory)
- Be able to identify that Structured Streaming uses the micro-batch model by default
- Understand the unbounded table concept (stream = continuously growing table)

## Key Takeaways

1. **Structured Streaming** treats a stream as an unbounded table that grows over time
2. **Micro-batch** processing checks for new data at intervals -- not true real-time, but low-latency and production-proven
3. The **same DataFrame API** works for both batch and streaming -- `readStream`/`writeStream` replaces `read`/`write`
4. **Checkpointing** is mandatory for production: it stores offsets, state, and commit logs for fault tolerance
5. **Delta Lake** is the recommended streaming sink on Databricks for exactly-once guarantees
6. **Rate** and **socket** sources are for testing only; use file, Delta, Kafka, or Auto Loader in production

## Next Steps

Proceed to [02 - Triggers & Output Modes](02-triggers-output-modes.md) to learn how to
control *when* your streaming query processes data and *how* results are written to the sink.
