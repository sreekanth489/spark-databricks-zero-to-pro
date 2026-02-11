# Auto Loader (cloudFiles)

> Module 02 -- Topic 02 | Level: Beginner-Intermediate | Time: 50 min

---

## Learning Objectives

- Explain what Auto Loader is and why it exists
- Compare the two file-discovery modes: directory listing vs. file notification
- Configure schema inference, schema evolution, and the rescued data column
- Set up checkpoints and understand exactly-once processing guarantees
- Monitor Auto Loader streams and troubleshoot common issues

---

## Conceptual Overview

### What Is Auto Loader?

Auto Loader is a Databricks-optimized streaming source that incrementally and
efficiently loads new files as they arrive in cloud storage. Under the hood it
is a Structured Streaming source identified by the format `cloudFiles`.

```
Cloud Storage                  Auto Loader                    Delta Table
  (landing zone)                 (cloudFiles)                  (target)
┌───────────────┐           ┌──────────────────┐          ┌──────────────┐
│ file_001.json │──────────>│                  │─────────>│              │
│ file_002.json │──────────>│  Schema Infer +  │─────────>│  Bronze /    │
│ file_003.json │──────────>│  Parse + Write   │─────────>│  Silver      │
│   ...         │           │                  │          │  Table       │
│ file_N.json   │──────────>│  (checkpoint)    │─────────>│              │
└───────────────┘           └──────────────────┘          └──────────────┘
                                    │
                                    v
                              Checkpoint Dir
                              (tracks which files
                               have been processed)
```

### Why Not Just Use `spark.read`?

| Concern | spark.read (batch) | Auto Loader (streaming) |
|---------|-------------------|------------------------|
| New files | Must re-scan entire directory | Discovers only new files |
| Cost at scale | O(n) listing on every run | O(1) with file notification mode |
| Exactly-once | Must implement yourself | Built-in via checkpoint |
| Schema changes | Manual handling | Automatic evolution / rescue |
| Scalability | Slows as file count grows | Constant cost per trigger |

---

## File Discovery Modes

Auto Loader supports two mechanisms for finding new files:

### 1. Directory Listing Mode (default)

- Spark lists the directory on each trigger
- Compares against the checkpoint to find new files
- **Pros:** No cloud infrastructure setup required; works everywhere
- **Cons:** Listing cost grows with the total number of files in the directory
- **Best for:** Directories with fewer than ~10,000 files or infrequent loads

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "false")  # default
    .load("/landing/events/")
```

### 2. File Notification Mode

- Auto Loader creates cloud-native event subscriptions (AWS SNS+SQS, Azure
  Event Grid, GCP Pub/Sub) to receive notifications when files land
- **Pros:** Near-instant detection; O(1) cost regardless of directory size
- **Cons:** Requires permissions to create/manage cloud resources
- **Best for:** High-volume landing zones with millions of files

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useNotifications", "true")
    .load("/landing/events/")
```

### Decision Matrix

```
How many files will accumulate in the landing directory over time?

├── < 10,000 files       --> Directory Listing (simple, no setup)
├── 10,000 - 1,000,000   --> Either mode works; notification is more efficient
└── > 1,000,000 files    --> File Notification (mandatory for cost control)

Can you grant Auto Loader permissions to create cloud event subscriptions?
├── YES --> File Notification preferred at any scale
└── NO  --> Directory Listing (the only option)
```

---

## Schema Inference and Evolution

### Automatic Schema Inference

Auto Loader can infer the schema from the first batch of files. The inferred
schema is stored in a schema location (separate from the checkpoint) so that
it does not need to be re-inferred on every restart.

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .load("/landing/events/")
```

### Schema Evolution

When new columns appear in incoming files, Auto Loader can evolve the schema
automatically. Set `cloudFiles.schemaEvolutionMode`:

| Mode | Behavior |
|------|----------|
| `addNewColumns` (default for JSON/CSV) | Adds new columns; restarts the stream to pick them up |
| `rescue` | Puts unrecognized fields into `_rescued_data` column (no restart needed) |
| `failOnNewColumns` | Fails the stream so you can review and approve the change |
| `none` | Ignores new columns entirely |

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .load("/landing/events/")
```

### The Rescued Data Column

Even without explicit schema evolution, you can enable `_rescued_data` to
capture any fields that do not match the current schema:

```python
spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/events_schema/")
    .option("rescuedDataColumn", "_rescued_data")
    .load("/landing/events/")
```

The `_rescued_data` column contains a JSON string with all mismatched or
unexpected fields. This is invaluable for debugging data quality issues
without losing any data.

---

## Configuration Reference

| Option | Default | Description |
|--------|---------|-------------|
| `cloudFiles.format` | (required) | Source file format: csv, json, parquet, avro, text, binaryFile |
| `cloudFiles.useNotifications` | `false` | Enable file-notification mode |
| `cloudFiles.schemaLocation` | (required for inference) | Path to store inferred schema |
| `cloudFiles.schemaEvolutionMode` | `addNewColumns` | How to handle new columns |
| `cloudFiles.maxFilesPerTrigger` | 1000 | Max files to process per micro-batch |
| `cloudFiles.maxBytesPerTrigger` | (none) | Max bytes to process per micro-batch |
| `cloudFiles.includeExistingFiles` | `true` | Process files that existed before the stream started |
| `cloudFiles.validateOptions` | `true` | Validate that all options are recognized |
| `rescuedDataColumn` | (none) | Column name for rescued data |
| `pathGlobFilter` | (none) | Only process files matching this glob |

### Format-Specific Options

Auto Loader passes through format-specific options. Prefix them with the
format name or use the generic option names:

```python
# These are equivalent for CSV
.option("header", "true")
.option("cloudFiles.format", "csv")
```

---

## Checkpoint Location

The checkpoint is the backbone of exactly-once semantics. It tracks:

1. Which files have been discovered
2. Which files have been committed (written to the sink)
3. The current schema (if using schema inference)

```python
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", "/checkpoints/events/schema/")
    .load("/landing/events/")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/checkpoints/events/stream/")
    .trigger(availableNow=True)
    .toTable("bronze.events")
)
```

**Rules for checkpoints:**

- One checkpoint per stream (never share checkpoints between different streams)
- Never delete a checkpoint unless you want to reprocess all files
- Store checkpoints in a durable location (cloud storage, not local disk)
- Checkpoint and schema locations should be different directories

---

## Trigger Modes

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `trigger(processingTime="10 seconds")` | Micro-batch every 10s | Near-real-time |
| `trigger(availableNow=True)` | Process all new files then stop | Scheduled batch jobs (recommended) |
| `trigger(once=True)` | Process one micro-batch then stop | Legacy (use `availableNow` instead) |
| No trigger (default) | Continuous micro-batches | Always-on streaming |

**Best practice:** Use `availableNow=True` in scheduled Databricks Jobs. It
processes ALL available files (possibly in multiple micro-batches for
parallelism) and then stops the stream cleanly.

---

## Monitoring

### Streaming Query Progress

```python
query = (spark.readStream.format("cloudFiles") ...
    .writeStream ...
    .start())

# Check progress
query.lastProgress
query.status
```

### Key Metrics

- `numInputRows`: number of rows in the current micro-batch
- `inputRowsPerSecond`: ingestion rate
- `processedRowsPerSecond`: processing rate
- `sources[0].numFilesOutstanding`: files waiting to be processed

### Streaming Query Listener

For production monitoring, register a `StreamingQueryListener` to push metrics
to your observability platform.

---

## Auto Loader vs. COPY INTO

| Feature | Auto Loader | COPY INTO |
|---------|------------|-----------|
| Engine | Structured Streaming | SQL command (batch) |
| File tracking | Checkpoint (automatic) | Target table metadata |
| Scalability | Billions of files | Millions of files |
| Schema evolution | Built-in | Limited |
| Rescued data | Built-in | No |
| Idempotency | Exactly-once via checkpoint | Idempotent by file path |
| Trigger modes | Multiple (availableNow, etc.) | Run manually or via job |
| Cloud event support | Yes (notification mode) | No |
| Recommended for | Most production pipelines | Simple, low-volume loads |

**Databricks recommendation:** Use Auto Loader for almost all file ingestion.
Use COPY INTO only when you need a pure-SQL solution for simple use cases.

---

## Hands-On Walkthrough

Open the companion notebook `02-auto-loader_notebook.py` and work through
each cell. The notebook:

1. Writes sample JSON files to a temp landing directory
2. Sets up an Auto Loader stream with directory-listing mode
3. Demonstrates processing new files incrementally
4. Shows schema evolution by adding a new field
5. Inspects checkpoint contents
6. Cleans up all resources

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Notification service | SNS + SQS | Event Grid + Queue Storage | Pub/Sub |
| Required permissions | `s3:GetBucketNotificationConfiguration`, `sqs:*`, `sns:*` | Storage account contributor | `pubsub.*` |
| Notification setup | Automatic (or manual) | Automatic (or manual) | Automatic (or manual) |
| File event latency | Seconds | Seconds | Seconds |

Auto Loader creates and manages the cloud resources automatically when
`cloudFiles.useNotifications=true`. On teardown, it cleans them up if the
stream is properly stopped.

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam heavily tests Auto
Loader. Key points:

- Know the difference between directory listing and file notification modes
- Understand that `cloudFiles.schemaLocation` stores the inferred schema
- Know that `trigger(availableNow=True)` is the recommended trigger for batch jobs
- Understand `_rescued_data` column and schema evolution modes
- Be able to compare Auto Loader with COPY INTO (when to use which)

The **Professional** exam goes deeper into notification mode setup, checkpoint
management, and production monitoring patterns.

---

## Key Takeaways

- Auto Loader is the recommended way to ingest files in Databricks -- use it instead of `spark.read` for incremental pipelines
- Directory listing mode is the simplest to set up; file notification mode scales to billions of files
- Schema inference + evolution + rescued data give you a safety net for changing source schemas
- Always use `trigger(availableNow=True)` for scheduled job-based ingestion
- One checkpoint per stream, never shared, never deleted (unless you want full reprocessing)

---

## Next Steps

Proceed to [03 -- COPY INTO](03-copy-into.md) to learn the SQL-based
alternative for idempotent file loading.
