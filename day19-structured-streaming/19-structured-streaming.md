# Structured Streaming & Auto Loader
> Module: Data Engineering Pipelines | Day 19 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Build streaming pipelines with Spark Structured Streaming
- Use Auto Loader to incrementally ingest files from AWS S3
- Choose between file notification mode and directory listing mode
- Configure schema inference, evolution, and rescue columns
- Select the right trigger strategy for your use case
- Monitor and manage streaming queries in production

---

## Conceptual Overview

### What is Structured Streaming?

**Structured Streaming** is Spark's stream processing engine built on the Spark SQL engine. It treats a live data stream as an unbounded table that is continuously appended with new rows. You write streaming queries using the same DataFrame/SQL API as batch queries.

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
| API | `spark.read` | `spark.readStream` |
| Output | `df.write` | `df.writeStream` |
| Execution | Runs once, processes all data | Runs continuously or triggered |
| New data | Must re-read everything | Automatically picks up new data |
| State | Stateless (each run is independent) | Stateful (checkpoints track progress) |
| Use case | Historical analysis, backfills | Real-time ingestion, live dashboards |

---

## Auto Loader (`cloudFiles`)

### What is Auto Loader?

**Auto Loader** is a Databricks-optimized streaming source for incrementally ingesting new data files from cloud storage (S3, ADLS, GCS). It is the recommended way to ingest files into the Bronze layer of a Medallion Architecture.

```
   Files land in S3
         |
         v
   Auto Loader detects new files
   (via notifications or directory listing)
         |
         v
   Reads & processes new files only
         |
         v
   Writes to Delta table
   (with checkpoint for exactly-once)
```

**Why Auto Loader over `spark.readStream.format("parquet")`?**

| Feature | Auto Loader (`cloudFiles`) | Native File Source |
|---------|---------------------------|-------------------|
| New file discovery | Optimized (notifications or incremental listing) | Lists entire directory every trigger |
| Schema inference | Built-in with `schemaLocation` | Manual schema required |
| Schema evolution | Automatic with `schemaEvolutionMode` | Not supported |
| Rescue column | Built-in `_rescued_data` | Not available |
| Scalability | Handles millions of files | Degrades with file count |
| File tracking | Tracks processed files in checkpoint | Re-lists all files |
| Cost (S3 API calls) | Lower (notifications) or optimized (listing) | Higher (full LIST per trigger) |

---

## Auto Loader: Two Modes

Auto Loader supports two file discovery mechanisms. The choice depends on your infrastructure, latency requirements, and scale.

### Mode 1: File Notification (S3 + SQS)

**How it works**:
```
  New file lands in S3
         |
         v
  S3 Event Notification
         |
         v
  SNS Topic (optional)
         |
         v
  SQS Queue
         |
         v
  Auto Loader polls SQS
         |
         v
  Reads only the new file(s)
```

**Configuration**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/orders/")
```

**What Databricks auto-configures** (when IAM permissions allow):
- Creates an SNS topic for S3 event notifications
- Creates an SQS queue subscribed to the SNS topic
- Configures S3 bucket event notifications to publish to SNS
- Auto Loader polls the SQS queue for new file events

**Required IAM Permissions** (for auto-setup):
```json
{
  "Effect": "Allow",
  "Action": [
    "s3:GetBucketNotification",
    "s3:PutBucketNotification",
    "sns:CreateTopic",
    "sns:DeleteTopic",
    "sns:GetTopicAttributes",
    "sns:Subscribe",
    "sns:Unsubscribe",
    "sqs:CreateQueue",
    "sqs:DeleteQueue",
    "sqs:DeleteMessage",
    "sqs:GetQueueAttributes",
    "sqs:GetQueueUrl",
    "sqs:ReceiveMessage",
    "sqs:SendMessage",
    "sqs:SetQueueAttributes"
  ],
  "Resource": "*"
}
```

**Or use pre-configured resources** (recommended for production):
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.queueUrl", "https://sqs.us-east-1.amazonaws.com/123456789/my-queue")
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/orders/")
```

**When to use file notification mode**:
- High-volume ingestion (thousands of files per hour)
- Near-real-time latency requirements (seconds, not minutes)
- Large directories with millions of existing files
- Cost-sensitive workloads (fewer S3 LIST API calls)

**Advantages**:
- Near-real-time file detection (seconds)
- Scales to millions of files without performance degradation
- Lower S3 API costs (no directory listing)
- Event-driven -- only triggers when new files arrive

**Limitations**:
- Requires IAM permissions for SNS/SQS
- Infrastructure setup needed (SNS topic, SQS queue, S3 notifications)
- Notification resources need lifecycle management (cleanup on teardown)

---

### Mode 2: Directory Listing (No Notifications)

**How it works**:
```
  Auto Loader trigger fires
         |
         v
  Lists files in S3 directory
  (incremental listing using checkpoint state)
         |
         v
  Compares against previously processed files
  (tracked in RocksDB checkpoint)
         |
         v
  Reads only new/unprocessed files
```

**Configuration**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")  # default
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/orders/")
```

**How it discovers files**:
1. On first run: performs a full LIST of the S3 prefix
2. On subsequent runs: uses incremental listing with lexicographic ordering
3. Tracks processed files in the checkpoint directory (RocksDB state store)
4. Only reads files that haven't been processed before

**Advanced options for directory listing**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")
    # Optimize listing for large directories
    .option("cloudFiles.includeExistingFiles", "true")     # process existing files on first run
    .option("cloudFiles.maxFilesPerTrigger", "1000")       # limit files per micro-batch
    .option("cloudFiles.maxBytesPerTrigger", "10g")        # limit bytes per micro-batch
    # File filtering
    .option("pathGlobFilter", "*.json")                    # only JSON files
    .option("recursiveFileLookup", "true")                 # scan subdirectories
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/events/")
```

**When to use directory listing mode**:
- Quick setup without infrastructure overhead
- Moderate file volumes (up to tens of thousands)
- Development and testing environments
- When you cannot configure S3 notifications (permission constraints)
- Scheduled batch-style ingestion with `trigger(availableNow=True)`

**Advantages**:
- Zero infrastructure setup -- works immediately
- No IAM permissions beyond S3 read access
- Simpler to manage and debug
- Great for development and small-to-medium workloads

**Limitations**:
- Higher latency (depends on trigger interval or directory listing time)
- More S3 LIST API calls (cost increases with directory size)
- Performance degrades with millions of files in a single directory
- Not suitable for sub-second latency requirements

---

## Comparison: File Notification vs Directory Listing

| Aspect | File Notification | Directory Listing |
|--------|-------------------|-------------------|
| **Setup complexity** | Higher (SNS, SQS, IAM) | None |
| **File detection latency** | Seconds | Depends on trigger interval |
| **Scale (file count)** | Millions+ | Up to tens of thousands |
| **S3 API cost** | Lower (no LIST) | Higher (LIST calls) |
| **IAM permissions** | SNS, SQS, S3 notifications | S3 read only |
| **Best for** | Production, high-volume | Development, moderate volume |
| **Infrastructure cleanup** | Must teardown SNS/SQS | Nothing to clean up |

---

## Schema Handling

### Schema Inference

Auto Loader can automatically infer the schema from source files:

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")      # infer types (not just strings)
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/events/")
```

The inferred schema is persisted to `schemaLocation` and reused on subsequent runs. This means the schema is only inferred once (from the first batch of files), making restarts fast.

### Schema Evolution

When source data adds new columns, Auto Loader can handle it automatically:

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # auto-add new columns
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/events/")
```

**Evolution modes**:
| Mode | Behavior |
|------|----------|
| `addNewColumns` | New columns are automatically added to the schema |
| `rescue` | New columns go into `_rescued_data` (default) |
| `failOnNewColumns` | Stream fails if new columns are detected |
| `none` | New columns are silently ignored |

### Rescue Column

Data that doesn't match the expected schema is captured in `_rescued_data`:

```python
# Rescue column is enabled by default
# Any mismatched data types or unexpected columns go here
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .option("cloudFiles.schemaEvolutionMode", "rescue")  # default
    .load("s3://bucket/raw/events/")
    # _rescued_data column is automatically added
```

---

## Trigger Strategies

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `trigger(availableNow=True)` | Process all available data then stop | Scheduled batch jobs, cost-efficient |
| `trigger(processingTime="30 seconds")` | Micro-batch every 30 seconds | Near-real-time dashboards |
| `trigger(processingTime="0 seconds")` | Process as fast as possible | Low-latency requirements |
| `trigger(once=True)` | Process one micro-batch then stop | **Deprecated**, use `availableNow` |
| No trigger (default) | Micro-batch as fast as possible | Development/testing |

**Production recommendation**: Use `trigger(availableNow=True)` for scheduled batch-style pipelines and `trigger(processingTime="30 seconds")` for continuous near-real-time processing.

---

## Output Modes

| Mode | Behavior | Supported Sinks |
|------|----------|-----------------|
| **append** | Only new rows written to sink | Delta, Parquet, Kafka |
| **complete** | Entire result table rewritten | Memory, Console, Delta |
| **update** | Only changed rows written | Delta, Memory, Console |

**Rules**:
- Aggregations without watermark: only `complete` mode
- Aggregations with watermark: `append` or `update`
- No aggregations: only `append` mode
- `append` is the most common and efficient for file ingestion

---

## Checkpoint Management

Checkpoints are the backbone of exactly-once processing in Structured Streaming.

```
s3://bucket/checkpoints/my_stream/
    commits/       # completed micro-batch IDs
    offsets/       # what data was read in each micro-batch
    sources/       # source-specific state (processed file list)
    state/         # aggregation state (if applicable)
    metadata       # stream metadata
```

**Best practices**:
- Store checkpoints on S3 (same region as data for low latency)
- Never share a checkpoint directory between different streams
- Never delete a checkpoint directory unless you want to reprocess all data
- Use a consistent naming convention: `s3://bucket/checkpoints/{table_name}`

---

## Production Best Practices

### Error Handling

```python
# Configure bad records handling
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("badRecordsPath", "s3://bucket/bad_records/orders/")  # quarantine bad files
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/orders/")
```

### Rate Limiting

```python
# Prevent overwhelming downstream systems
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.maxFilesPerTrigger", "100")      # max files per micro-batch
    .option("cloudFiles.maxBytesPerTrigger", "1g")       # max bytes per micro-batch
    .option("cloudFiles.schemaLocation", "s3://bucket/checkpoints/schema")
    .load("s3://bucket/raw/orders/")
```

### Monitoring

```python
# List all active streaming queries
for stream in spark.streams.active:
    print(f"Stream: {stream.name}, ID: {stream.id}, Status: {stream.status}")

# Get detailed progress of a specific stream
stream.lastProgress  # dict with timing, rows, state info
stream.recentProgress  # list of recent progress updates
```

### Graceful Shutdown

```python
# Stop all active streams gracefully
for stream in spark.streams.active:
    print(f"Stopping stream: {stream.name}")
    stream.stop()
    stream.awaitTermination()
```

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Object Storage | S3 | ADLS Gen2 | GCS |
| Notification Service | S3 Events -> SQS | Event Grid -> Queue Storage | Pub/Sub |
| Auto Loader notification mode | S3 + SNS + SQS | ADLS + Event Grid + Queue | GCS + Pub/Sub |
| IAM for notifications | SNS, SQS permissions | Storage Queue, Event Grid | Pub/Sub permissions |
| Directory listing | Supported | Supported | Supported |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- Auto Loader configuration: `cloudFiles.format`, `cloudFiles.schemaLocation`
- Difference between `useNotifications=true` and directory listing
- Checkpoint directories and exactly-once guarantees
- Trigger modes: `availableNow`, `processingTime`, `once` (deprecated)
- Schema evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`
- Output modes: `append`, `complete`, `update`
- When to use Auto Loader vs native file streaming

---

## Key Takeaways

1. **Structured Streaming** treats live data as a continuously appended table using the same DataFrame API
2. **Auto Loader** (`cloudFiles`) is the recommended way to ingest files from S3 into Delta Lake
3. **File notification mode** (SQS) gives near-real-time detection but requires infrastructure setup
4. **Directory listing mode** requires zero setup and works well for moderate-scale workloads
5. **Schema inference** is persisted to `schemaLocation` and only runs once
6. **Schema evolution** can automatically add new columns or rescue mismatched data
7. **Checkpoints** enable exactly-once processing and fault-tolerant restarts
8. **`trigger(availableNow=True)`** is the recommended trigger for scheduled batch-style streaming jobs
9. Always use a **dedicated checkpoint directory** per streaming query

---

## Hands-On Walkthrough

See the accompanying notebook: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py)

The lab covers:
- Scenario 1: Auto Loader with **directory listing mode** (zero-setup)
- Scenario 2: Auto Loader with **file notification mode** (S3 + SQS)
- Incremental ingestion with new file batches
- Schema inference and evolution handling
- Trigger strategies comparison
- Stream monitoring and graceful shutdown
- Full integration with Medallion Architecture Bronze layer

## Next Steps

- [Day 20: Advanced Streaming](../day20-advanced-streaming/)
