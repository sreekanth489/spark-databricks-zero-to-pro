# Auto Loader Streaming
> Module 07 -- Topic 05 | Level: Intermediate-Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:

1. Explain the Auto Loader architecture and the `cloudFiles` format
2. Compare directory listing mode vs file notification mode
3. Configure schema inference and handle schema evolution
4. Use the rescued data column for malformed records
5. Implement production Auto Loader patterns (checkpointing, schema location)
6. Compare Auto Loader with COPY INTO and know when to use each

## Conceptual Overview

### What Is Auto Loader?

Auto Loader is a Databricks-specific optimized file ingestion engine. It incrementally
and efficiently processes new data files as they arrive in cloud storage, without
reprocessing files that have already been ingested.

```
  Cloud Storage (Landing Zone)
  +----------------------------------+
  |  file_001.json  (already loaded) |
  |  file_002.json  (already loaded) |
  |  file_003.json  <-- NEW          |
  |  file_004.json  <-- NEW          |
  +----------------------------------+
           |
           | Auto Loader (cloudFiles)
           | detects only new files
           |
           v
  +----------------------------------+
  |  Delta Table (Bronze Layer)      |
  |  Rows from file_001 + 002        |
  |  + NEW rows from file_003 + 004  |
  +----------------------------------+
```

Auto Loader is implemented as a Structured Streaming source with the format `cloudFiles`.
It is the **recommended** method for ingesting files into Databricks.

### Directory Listing vs File Notification Mode

Auto Loader supports two strategies for discovering new files:

```
  DIRECTORY LISTING MODE              FILE NOTIFICATION MODE
  +---------------------+            +---------------------+
  | Auto Loader         |            | Cloud Event         |
  | periodically LISTS  |            | (S3 SQS / ADLS     |
  | the directory       |            | Event Grid / GCS    |
  |                     |            | Pub/Sub) PUSHES     |
  | O(n) per listing    |            | notifications       |
  | (n = total files)   |            |                     |
  +---------------------+            +---------------------+
  |                     |            |                     |
  | Good for:           |            | Good for:           |
  | < 1M files          |            | > 1M files          |
  | Simple setup        |            | Very large dirs     |
  | No extra infra      |            | Cost-efficient      |
  +---------------------+            +---------------------+
```

#### Directory Listing (Default)

Auto Loader lists the directory contents and compares against previously processed
files (tracked via checkpointing). Simple to set up but becomes expensive with
millions of files (every list operation scans all files).

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")  # default
    .load("/mnt/landing/events/")
)
```

#### File Notification

Auto Loader sets up cloud-native event notifications that push file arrival events
to a queue. Only new file events are received -- no directory listing needed.

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .load("/mnt/landing/events/")
)
```

**Cloud infrastructure created automatically**:

| Cloud | Event Source | Queue/Subscription |
|-------|-------------|-------------------|
| AWS | S3 Event Notification | SQS Queue |
| Azure | ADLS Gen2 Event Grid | Queue Storage |
| GCP | GCS Pub/Sub Notification | Pub/Sub Subscription |

### Schema Inference and Evolution

One of Auto Loader's most powerful features is automatic schema handling:

```
  File 1 (Jan):                File 2 (Feb):              File 3 (Mar):
  {"name": "Alice",            {"name": "Bob",            {"name": "Carol",
   "age": 30}                   "age": 25,                 "age": 28,
                                 "email": "b@x.com"}       "email": "c@x.com",
                                                            "phone": "555-0123"}
       |                             |                           |
       v                             v                           v
  Schema: name, age            Schema: name, age, email   Schema: name, age, email, phone
                                      ^ new column                ^ another new column
```

#### Schema Inference

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/")
    .load("/mnt/landing/events/")
)
```

Auto Loader:
1. Infers schema from the first batch of files
2. Stores the inferred schema in `schemaLocation`
3. Uses the stored schema for subsequent batches (no re-inference)

#### Schema Evolution

When new columns appear in incoming files:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/schema/")
    .load("/mnt/landing/events/")
)
```

Schema evolution modes:

| Mode | Behavior |
|------|----------|
| `addNewColumns` | New columns are added to the schema; query restarts automatically |
| `rescue` | New columns go to `_rescued_data` (no schema change) |
| `failOnNewColumns` | Query fails when new columns are detected |
| `none` | New columns are silently ignored |

### Rescued Data Column

When Auto Loader encounters data that does not match the expected schema, it places
the mismatched data in a special `_rescued_data` column:

```
  Expected schema: {name: string, age: int}

  Input record: {"name": "Alice", "age": "thirty", "extra": true}

  Result row:
  +-------+------+------------------------------------------+
  | name  | age  | _rescued_data                            |
  +-------+------+------------------------------------------+
  | Alice | null | {"age": "thirty", "extra": true}         |
  +-------+------+------------------------------------------+
```

This prevents data loss while maintaining schema integrity:

```python
df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", schema_path)
    .option("rescuedDataColumn", "_rescued_data")
    .load(source_path)
)
```

### Auto Loader vs COPY INTO

| Feature | Auto Loader | COPY INTO |
|---------|------------|-----------|
| Engine | Structured Streaming | SQL command (batch) |
| File tracking | Checkpoint (streaming offsets) | Per-file tracking in table metadata |
| Schema inference | Automatic | Manual |
| Schema evolution | Automatic (configurable) | Manual |
| Rescued data | Built-in | Manual handling |
| Scale | Billions of files | Millions of files |
| Incremental | Always incremental | Incremental (tracks loaded files) |
| Recommended for | Most ingestion use cases | Simple one-time or infrequent loads |
| Trigger support | All triggers (availableNow, etc.) | N/A (runs once) |

**Databricks recommendation**: Use Auto Loader for nearly all file ingestion. Use COPY
INTO only for simple, ad-hoc loads or when you need SQL-only access.

### Production Configuration

```python
# Production Auto Loader pipeline
query = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "/mnt/checkpoints/bronze_events/schema/")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("rescuedDataColumn", "_rescued_data")
    .option("cloudFiles.maxFilesPerTrigger", 1000)
    .option("cloudFiles.maxBytesPerTrigger", "10g")
    .load("/mnt/landing/events/")
    .withColumn("_ingestion_time", F.current_timestamp())
    .withColumn("_source_file", F.input_file_name())
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/mnt/checkpoints/bronze_events/checkpoint/")
    .option("mergeSchema", "true")
    .trigger(availableNow=True)
    .toTable("bronze.events")
)
```

Key production options:
- `schemaLocation`: Store inferred schema (separate from checkpoint)
- `maxFilesPerTrigger`: Rate limit files per micro-batch
- `maxBytesPerTrigger`: Rate limit bytes per micro-batch
- `_ingestion_time`: Audit column for when data was loaded
- `_source_file`: Track which file each row came from
- `trigger(availableNow=True)`: Process all available, then stop (scheduled via Workflows)

## Hands-On Walkthrough

Open `05-auto-loader-streaming_notebook.py` and work through:

1. **cloudFiles basic configuration**: Read JSON files with Auto Loader
2. **Schema inference**: Observe automatic schema detection
3. **Rescued data**: Handle malformed records gracefully
4. **Schema evolution simulation**: Add new columns to incoming files
5. **Community Edition alternative**: Simulated Auto Loader using standard file streaming

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Directory listing mode | S3 ListObjects | ADLS Gen2 List | GCS List |
| File notification mode | S3 Events + SQS | Event Grid + Queue Storage | Pub/Sub |
| Auto-setup of notifications | Requires IAM permissions | Requires Event Grid access | Requires Pub/Sub access |
| Supported file formats | JSON, CSV, Parquet, Avro, ORC, text, binary | Same | Same |
| Unity Catalog integration | Volumes supported | Volumes supported | Volumes supported |

File notification mode requires that the Databricks service principal has permissions
to create event notifications and queues in your cloud account.

## Certification Tip

Auto Loader is a major topic on the Databricks Data Engineer Professional exam:
- Know the difference between directory listing and file notification modes
- Know that `cloudFiles.schemaLocation` is required for schema inference
- Understand schema evolution modes (addNewColumns, rescue, failOnNewColumns, none)
- Know that `_rescued_data` captures schema mismatches without data loss
- Know that Auto Loader is preferred over COPY INTO for most ingestion scenarios
- Understand that Auto Loader uses Structured Streaming under the hood

## Key Takeaways

1. **Auto Loader** (`cloudFiles`) is the recommended Databricks file ingestion method
2. **Directory listing** is simple; **file notification** scales to billions of files
3. **Schema inference** detects column types automatically; schema is stored in `schemaLocation`
4. **Schema evolution** (addNewColumns) handles new columns without manual intervention
5. **Rescued data** captures malformed records to prevent data loss
6. Auto Loader is built on **Structured Streaming** -- use `trigger(availableNow=True)` for scheduled batch-like runs
7. **Prefer Auto Loader over COPY INTO** for production ingestion pipelines

## Next Steps

Proceed to [06 - Kafka Integration](06-kafka-integration.md) to learn how to read from
and write to Apache Kafka topics using Structured Streaming.
