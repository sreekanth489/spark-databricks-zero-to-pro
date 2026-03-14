# Structured Streaming & Auto Loader
> Module: Data Engineering Pipelines | Day 19 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Build streaming pipelines with Spark Structured Streaming
- Use Auto Loader to incrementally ingest files from AWS S3
- Choose between the three Auto Loader modes: directory listing, managed file events, and classic notifications
- Configure schema inference, evolution, and rescue columns
- Select the right trigger strategy for your use case
- Troubleshoot common Auto Loader errors on AWS

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
   (via directory listing, managed events, or classic notifications)
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

## Auto Loader: Three Modes on AWS

Auto Loader supports three file discovery mechanisms on AWS. The choice depends on your Databricks edition, infrastructure, and scale.

### Mode 1: Directory Listing (Recommended Starter Path)

**The simplest setup and most reliable for learning and development.**

Auto Loader scans the S3 directory for new files, comparing against previously processed files tracked in the checkpoint.

**Configuration**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")          # explicit: directory listing
    .option("cloudFiles.includeExistingFiles", "true")       # process existing files on first run
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/orders")
    .load("s3://bucket/raw/orders/")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))   # Unity Catalog safe
    .withColumn("source_file_name", col("_metadata.file_name"))
```

**How it discovers files**:
1. On first run: performs a full LIST of the S3 prefix
2. On subsequent runs: uses incremental listing with lexicographic ordering
3. Tracks processed files in the checkpoint directory (RocksDB state store)
4. Only reads files that haven't been processed before

**When to use**:
- Learning and development
- Moderate file volumes (up to tens of thousands per directory)
- When you want zero infrastructure setup
- Scheduled batch-style ingestion with `trigger(availableNow=True)`
- Works on both Free and Premium editions

**Advantages**: Zero setup, most reliable, works everywhere
**Limitations**: Higher S3 LIST API costs, not suitable for sub-second latency

**Advanced options**:
```python
.option("cloudFiles.maxFilesPerTrigger", "1000")       # limit files per micro-batch
.option("cloudFiles.maxBytesPerTrigger", "10g")        # limit bytes per micro-batch
.option("pathGlobFilter", "*.json")                    # file extension filter
.option("recursiveFileLookup", "true")                 # scan subdirectories
```

---

### Mode 2: Managed File Events (Recommended Production Path)

**The modern, recommended direction on Databricks AWS Premium.**

Databricks manages the notification infrastructure (SNS/SQS) behind the scenes through Unity Catalog external locations.

**Configuration**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useManagedFileEvents", "true")       # managed file events
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/events")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("s3://bucket/raw/events/")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_file_name", col("_metadata.file_name"))
```

**Do NOT combine** with `cloudFiles.useNotifications = true` -- they are mutually exclusive.

**Prerequisites** (one-time setup):

```sql
-- 1. Create storage credential (Admin)
CREATE STORAGE CREDENTIAL my_s3_credential
WITH (AWS_IAM_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/databricks-runtime-role');

-- 2. Create external location for the S3 prefix
CREATE EXTERNAL LOCATION streaming_lab_location
URL 's3://databricks-zero-to-pro/streaming_lab/'
WITH (STORAGE CREDENTIAL my_s3_credential);

-- 3. Enable file events on the external location
ALTER EXTERNAL LOCATION streaming_lab_location
ENABLE FILE EVENTS;
```

**When to use**:
- Production workloads on Databricks Premium
- When you have Unity Catalog external locations configured
- High-volume ingestion requiring near-real-time detection
- When you want Databricks to manage notification lifecycle

**Advantages**: Modern pattern, cleaner than classic, Databricks manages lifecycle
**Limitations**: Requires Premium edition, Unity Catalog, external location with file events

---

### Mode 3: Classic File Notifications (Legacy / Appendix)

**The older approach where Auto Loader auto-manages S3 bucket notifications plus SNS/SQS per stream.** This has more moving parts and more AWS-side failure modes than the other two modes.

**Configuration**:
```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")           # classic notifications
    .option("cloudFiles.region", "us-east-1")                # MUST match S3 bucket region
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/orders")
    .load("s3://bucket/raw/orders/")
```

**What Databricks does behind the scenes**:
1. Creates an SNS topic for S3 event notifications
2. Creates an SQS queue subscribed to the SNS topic
3. Configures S3 bucket event notifications to publish to SNS
4. Polls the SQS queue for new file events

**Pre-configured SQS** (production variant):
```python
.option("cloudFiles.queueUrl",
        "https://sqs.us-east-1.amazonaws.com/123456789012/my-autoloader-queue")
```

**When to use**:
- When you cannot use managed file events (no Unity Catalog external location)
- When you need fine-grained control over SNS/SQS resources
- On non-Premium workspaces without Unity Catalog

**Advantages**: Near-real-time detection, works without Unity Catalog
**Limitations**: Requires broad IAM permissions, per-stream SNS/SQS resources, cleanup needed

---

## Comparison: Three Auto Loader Modes

| Feature | Directory Listing | Managed File Events | Classic Notifications |
|---------|-------------------|--------------------|-----------------------|
| **Option** | `useNotifications=false` | `useManagedFileEvents=true` | `useNotifications=true` |
| **Setup complexity** | None | External location + file events | IAM for SNS/SQS + bucket policy |
| **Infrastructure** | None | Databricks-managed | Per-stream SNS/SQS |
| **File detection** | Polls S3 directory | Near-real-time events | Near-real-time events |
| **Scale** | Moderate (< 100K files) | Millions+ | Millions+ |
| **Databricks edition** | Free + Premium | Premium only | Free + Premium |
| **Unity Catalog** | Optional | Required | Optional |
| **Cleanup** | Nothing | Databricks manages | Must teardown SNS/SQS |
| **Recommendation** | Starter / Dev | Production | Legacy / Appendix |

---

## IAM and Bucket Policy for AWS

### IAM Policy for the Databricks Runtime Role

Attach this to the IAM role that your Databricks workspace actually uses (e.g., `databricks-s3-ingest-XXXXX-db_s3_iam`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowBucketNotificationOps",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketNotification",
        "s3:PutBucketNotification",
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::databricks-zero-to-pro"
    },
    {
      "Sid": "AllowObjectAccess",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject"
      ],
      "Resource": "arn:aws:s3:::databricks-zero-to-pro/*"
    },
    {
      "Sid": "AllowSNSOps",
      "Effect": "Allow",
      "Action": [
        "sns:CreateTopic", "sns:DeleteTopic", "sns:GetTopicAttributes",
        "sns:SetTopicAttributes", "sns:ListSubscriptionsByTopic",
        "sns:Subscribe", "sns:Unsubscribe", "sns:Publish"
      ],
      "Resource": "*"
    },
    {
      "Sid": "AllowSQSOps",
      "Effect": "Allow",
      "Action": [
        "sqs:CreateQueue", "sqs:DeleteQueue", "sqs:GetQueueAttributes",
        "sqs:SetQueueAttributes", "sqs:ListQueueTags", "sqs:TagQueue",
        "sqs:UntagQueue", "sqs:ReceiveMessage", "sqs:DeleteMessage",
        "sqs:SendMessage"
      ],
      "Resource": "*"
    }
  ]
}
```

**Note**: This is intentionally broad for labs. In production, scope SNS/SQS resources more tightly.

### S3 Bucket Policy

Put this on the S3 bucket itself, granting the Databricks role bucket-level notification access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowDatabricksRoleBucketNotificationAccess",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::ACCOUNT_ID:role/databricks-s3-ingest-XXXXX-db_s3_iam"
      },
      "Action": [
        "s3:GetBucketNotification",
        "s3:PutBucketNotification",
        "s3:GetBucketLocation",
        "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::databricks-zero-to-pro"
    }
  ]
}
```

---

## Metadata Columns: `_metadata` vs `input_file_name()`

In Unity Catalog, use `_metadata.file_path` and `_metadata.file_name` instead of `input_file_name()`:

```python
# Unity Catalog safe (recommended)
.withColumn("source_file", col("_metadata.file_path"))
.withColumn("source_file_name", col("_metadata.file_name"))

# Older approach (may not work with Unity Catalog)
.withColumn("source_file", input_file_name())
```

The `_metadata` column is automatically available on all file-based sources and provides:
- `_metadata.file_path`: full path to the source file
- `_metadata.file_name`: file name only
- `_metadata.file_size`: file size in bytes
- `_metadata.file_modification_time`: last modified timestamp

---

## Schema Handling

### Schema Inference

Auto Loader infers the schema from source files and persists it to `schemaLocation`:

```python
.option("cloudFiles.inferColumnTypes", "true")      # infer types (not just strings)
.option("cloudFiles.schemaLocation", "s3://bucket/schemas/my_stream")
```

The schema is only inferred once (from the first batch of files), making restarts fast.

### Schema Evolution

When source data adds new columns:

| Mode | Behavior |
|------|----------|
| `addNewColumns` | New columns are automatically added to the schema |
| `rescue` | New columns go into `_rescued_data` (default) |
| `failOnNewColumns` | Stream fails if new columns are detected |
| `none` | New columns are silently ignored |

### Rescue Column

Data that doesn't match the expected schema is captured in `_rescued_data`:
```python
.option("cloudFiles.schemaEvolutionMode", "rescue")  # default
# _rescued_data column is automatically added
```

---

## Trigger Strategies

| Trigger | Behavior | Use Case |
|---------|----------|----------|
| `trigger(availableNow=True)` | Process all available data then stop | Scheduled batch jobs |
| `trigger(processingTime="30 seconds")` | Micro-batch every 30 seconds | Near-real-time dashboards |
| `trigger(processingTime="0 seconds")` | Process as fast as possible | Low-latency requirements |
| `trigger(once=True)` | Process one micro-batch then stop | **Deprecated**, use `availableNow` |

**Production recommendation**: Use `trigger(availableNow=True)` for scheduled Workflows.

---

## Output Modes

| Mode | Behavior | Use |
|------|----------|-----|
| **append** | Only new rows written to sink | File ingestion (most common) |
| **complete** | Entire result table rewritten | Aggregations without watermark |
| **update** | Only changed rows written | Aggregations with watermark |

---

## Checkpoint Management

```
s3://bucket/checkpoints/my_stream/
    commits/       # completed micro-batch IDs
    offsets/       # what data was read in each micro-batch
    sources/       # source-specific state (processed file list)
    state/         # aggregation state (if applicable)
    metadata       # stream metadata
```

**Best practices**:
- Store checkpoints on S3 (same region as data)
- Never share a checkpoint directory between different streams
- Never delete a checkpoint unless you want to reprocess all data
- Use consistent naming: `s3://bucket/checkpoints/{table_name}`

---

## Common Errors and Troubleshooting

### `PermanentRedirect` Error
Your bucket region does not match. Set the correct AWS region:
```python
.option("cloudFiles.region", "us-east-1")  # must match your S3 bucket region
```

### `GetBucketNotification AccessDenied`
The Databricks runtime role needs `s3:GetBucketNotification` permission. Add it to:
1. The IAM role policy attached to the Databricks runtime role
2. The S3 bucket policy granting the role access to that bucket-level action

### Managed File Events: "no matching external location found"
The S3 path is not inside a Unity Catalog external location with file events enabled. Steps:
1. Create the storage credential
2. Create the external location for the S3 prefix
3. Enable file events on that external location

### Managed File Events: fails during `sns.subscribe`
The external location was found, but Databricks could not finish SNS/SQS subscription. Check SNS and SQS permissions and inspect CloudTrail for the exact failing API.

### CloudTrail shows `anonymous` identity with `AccessDenied`
This does **not** mean public internet access. It means S3 did not recognize the request as an authorized principal. Re-check:
- Which IAM role the Databricks runtime actually uses (check CloudTrail for the assumed role)
- The bucket policy principal ARN
- Whether you are using classic notifications vs managed file events

---

## Recommended Learning Progression

| Step | Topic | Mode |
|------|-------|------|
| 1 | Auto Loader basics | Directory listing |
| 2 | Unity Catalog metadata columns | `_metadata.file_path` |
| 3 | Premium workspace IAM role setup | AWS IAM |
| 4 | Storage credential + external location | Unity Catalog |
| 5 | Managed file events | Production |
| Appendix | Classic `useNotifications=true` | Legacy reference |

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Object Storage | S3 | ADLS Gen2 | GCS |
| Notification Service | S3 Events -> SQS | Event Grid -> Queue Storage | Pub/Sub |
| Managed file events | External location + file events | External location + file events | External location + file events |
| Classic notifications | S3 + SNS + SQS | ADLS + Event Grid + Queue | GCS + Pub/Sub |
| Directory listing | Supported | Supported | Supported |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- Auto Loader configuration: `cloudFiles.format`, `cloudFiles.schemaLocation`
- Difference between notification modes and directory listing
- Checkpoint directories and exactly-once guarantees
- Trigger modes: `availableNow`, `processingTime`, `once` (deprecated)
- Schema evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`
- Output modes: `append`, `complete`, `update`

---

## Key Takeaways

1. **Start with directory listing** (`useNotifications=false`) -- it always works, zero setup
2. **Graduate to managed file events** (`useManagedFileEvents=true`) for production on Premium
3. **Avoid classic notifications** as your main path -- more moving parts, more failure modes
4. Use `_metadata.file_path` instead of `input_file_name()` in Unity Catalog
5. **Schema inference** is persisted to `schemaLocation` and only runs once
6. **Schema evolution** with `addNewColumns` automatically adapts to new fields
7. **Checkpoints** enable exactly-once processing -- never share or delete them
8. **`trigger(availableNow=True)`** is the recommended trigger for scheduled Workflows
9. Always set `cloudFiles.region` when using classic notifications

---

## Hands-On Walkthrough

See the accompanying notebook: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py)

The lab covers:
- **Track A**: Directory listing mode (zero-setup, always works)
- **Track B**: Managed file events (modern production, Unity Catalog)
- **Track C**: Classic notifications (appendix/legacy)
- Incremental ingestion with new file batches
- Schema evolution with new columns
- Trigger strategies comparison
- Full Bronze -> Silver streaming pipeline
- Troubleshooting common AWS errors

## Next Steps

- [Day 20: Advanced Streaming](../day20-advanced-streaming/)
