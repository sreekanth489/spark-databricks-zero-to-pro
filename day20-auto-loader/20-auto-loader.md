# Auto Loader
> Module: Data Engineering Pipelines | Day 20 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain Auto Loader as a file ingestion source built on Structured Streaming
- Choose between the three Auto Loader modes on AWS
- Configure schema inference, evolution, and rescue columns
- Set up IAM and bucket policies for notification modes
- Troubleshoot common Auto Loader errors on AWS

---

## What is Auto Loader?

**Auto Loader is NOT a streaming engine.** It is a specialized file ingestion SOURCE built by Databricks on top of Spark Structured Streaming.

```
S3 / ADLS / GCS
      |
      v
Auto Loader (cloudFiles)       <-- SOURCE (this session)
      |
      v
Spark Structured Streaming     <-- ENGINE (Day 19)
      |
      v
Transformations
      |
      v
Delta Lake
```

Instead of using the standard file source:
```python
spark.readStream.format("json").schema(my_schema).load("/data")
```

You use Auto Loader:
```python
spark.readStream.format("cloudFiles").option("cloudFiles.format", "json").load("s3://bucket/data")
```

Both use the same Structured Streaming engine underneath. Auto Loader adds optimized file discovery, schema inference, and schema evolution.

---

## Why Auto Loader?

Standard Spark file streaming has major limitations for cloud storage:

| Problem | Standard File Source | Auto Loader |
|---------|---------------------|-------------|
| Directory scans | Expensive (full LIST every trigger) | Optimized (incremental listing or notifications) |
| Millions of files | Slow / fails | Handles well |
| Schema changes | Manual, painful | Automatic inference and evolution |
| Duplicate file detection | Manual | Built-in via checkpoint |
| File tracking | Re-lists everything | Incremental, persisted state |
| Schema inference | Not available | Built-in, persisted to `schemaLocation` |
| Rescue column | Not available | `_rescued_data` for mismatched data |

---

## Three Modes on AWS

### Mode 1: Directory Listing (Recommended Starter Path)

The simplest setup. Auto Loader scans the S3 directory for new files.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "false")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/orders")
    .load("s3://bucket/raw/orders/")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
```

| Aspect | Detail |
|--------|--------|
| Setup | None |
| IAM | S3 read/write only |
| Latency | Depends on trigger interval |
| Scale | Moderate (< 100K files per directory) |
| Edition | Free + Premium |
| Best for | Learning, dev, scheduled batch ingestion |

### Mode 2: Managed File Events (Recommended Production)

The modern production path. Databricks manages notification infrastructure via Unity Catalog.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.useManagedFileEvents", "true")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/events")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .load("s3://bucket/raw/events/")
    .withColumn("load_time", current_timestamp())
    .withColumn("source_file", col("_metadata.file_path"))
    .withColumn("source_file_name", col("_metadata.file_name"))
```

**Do NOT combine** with `cloudFiles.useNotifications = true`.

**Prerequisites** (one-time setup):
```sql
-- 1. Create storage credential
CREATE STORAGE CREDENTIAL my_s3_credential
WITH (AWS_IAM_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/databricks-runtime-role');

-- 2. Create external location
CREATE EXTERNAL LOCATION my_location
URL 's3://databricks-zero-to-pro/autoloader_lab/'
WITH (STORAGE CREDENTIAL my_s3_credential);

-- 3. Enable file events
ALTER EXTERNAL LOCATION my_location ENABLE FILE EVENTS;
```

| Aspect | Detail |
|--------|--------|
| Setup | Storage credential + external location + file events |
| Latency | Near-real-time |
| Scale | Millions+ |
| Edition | Premium only |
| Best for | Production, high volume |

### Mode 3: Classic File Notifications (Legacy / Appendix)

The older approach. Auto Loader auto-manages S3 notifications + SNS/SQS per stream.

```python
spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.useNotifications", "true")
    .option("cloudFiles.region", "us-east-1")         # MUST match S3 bucket region
    .option("cloudFiles.schemaLocation", "s3://bucket/schemas/orders")
    .load("s3://bucket/raw/orders/")
```

| Aspect | Detail |
|--------|--------|
| Setup | IAM for SNS/SQS + bucket policy |
| Latency | Near-real-time |
| Scale | Millions+ |
| Edition | Free + Premium |
| Best for | Legacy setups, no Unity Catalog |

---

## Three-Mode Comparison

| Feature | Directory Listing | Managed File Events | Classic Notifications |
|---------|-------------------|--------------------|-----------------------|
| **Option** | `useNotifications=false` | `useManagedFileEvents=true` | `useNotifications=true` |
| **Setup** | None | External location + file events | IAM for SNS/SQS + bucket policy |
| **Infrastructure** | None | Databricks-managed | Per-stream SNS/SQS |
| **Latency** | Trigger interval | Near-real-time | Near-real-time |
| **Scale** | < 100K files | Millions+ | Millions+ |
| **Edition** | Free + Premium | Premium only | Free + Premium |
| **Cleanup** | Nothing | Databricks manages | Must teardown SNS/SQS |
| **Recommendation** | Starter / Dev | Production | Legacy |

---

## Schema Handling

### Schema Inference

```python
.option("cloudFiles.inferColumnTypes", "true")
.option("cloudFiles.schemaLocation", "s3://bucket/schemas/my_stream")
```

Schema is inferred once from the first batch of files and persisted. Subsequent runs reuse the persisted schema.

### Schema Evolution Modes

| Mode | Behavior |
|------|----------|
| `addNewColumns` | New columns auto-added to schema |
| `rescue` | New columns captured in `_rescued_data` (default) |
| `failOnNewColumns` | Stream fails for review |
| `none` | New columns silently dropped |

### Rescue Column

```python
.option("cloudFiles.schemaEvolutionMode", "rescue")
# _rescued_data column automatically added with mismatched data as JSON
```

---

## Metadata Columns

Use `_metadata` instead of `input_file_name()` in Unity Catalog:

```python
.withColumn("source_file", col("_metadata.file_path"))
.withColumn("source_file_name", col("_metadata.file_name"))
```

Available fields: `_metadata.file_path`, `_metadata.file_name`, `_metadata.file_size`, `_metadata.file_modification_time`.

---

## IAM and Bucket Policies for AWS

### IAM Policy (attach to Databricks runtime role)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3BucketOps",
      "Effect": "Allow",
      "Action": [
        "s3:GetBucketNotification", "s3:PutBucketNotification",
        "s3:GetBucketLocation", "s3:ListBucket"
      ],
      "Resource": "arn:aws:s3:::databricks-zero-to-pro"
    },
    {
      "Sid": "S3ObjectOps",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::databricks-zero-to-pro/*"
    },
    {
      "Sid": "SNSOps",
      "Effect": "Allow",
      "Action": [
        "sns:CreateTopic", "sns:DeleteTopic", "sns:GetTopicAttributes",
        "sns:SetTopicAttributes", "sns:Subscribe", "sns:Unsubscribe", "sns:Publish"
      ],
      "Resource": "*"
    },
    {
      "Sid": "SQSOps",
      "Effect": "Allow",
      "Action": [
        "sqs:CreateQueue", "sqs:DeleteQueue", "sqs:GetQueueAttributes",
        "sqs:SetQueueAttributes", "sqs:ReceiveMessage", "sqs:DeleteMessage",
        "sqs:SendMessage"
      ],
      "Resource": "*"
    }
  ]
}
```

### S3 Bucket Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "AWS": "arn:aws:iam::ACCOUNT_ID:role/databricks-runtime-role"
    },
    "Action": [
      "s3:GetBucketNotification", "s3:PutBucketNotification",
      "s3:GetBucketLocation", "s3:ListBucket"
    ],
    "Resource": "arn:aws:s3:::databricks-zero-to-pro"
  }]
}
```

---

## Common Errors and Troubleshooting

### `PermanentRedirect`
Bucket region mismatch. Set: `.option("cloudFiles.region", "us-east-1")`

### `GetBucketNotification AccessDenied`
Databricks role needs `s3:GetBucketNotification`. Add to both IAM role policy AND bucket policy.

### Managed file events: "no matching external location found"
The S3 path is not inside a Unity Catalog external location with file events enabled.

### Managed file events: fails during `sns.subscribe`
External location found, but SNS/SQS subscription failed. Check SNS/SQS permissions in CloudTrail.

### CloudTrail shows `anonymous` with `AccessDenied`
S3 did not recognize the request as an authorized principal. Does NOT mean public access. Re-check the Databricks runtime role ARN and bucket policy principal.

---

## Where Auto Loader Fits in Medallion Architecture

```
S3 Raw Files
     |
     v
Auto Loader (cloudFiles)      <-- Bronze layer ingestion
     |
     v
Bronze Table
     |
     v
Structured Streaming           <-- Silver/Gold transformations
     |
     v
Silver → Gold
```

Auto Loader is typically used only at the **Bronze layer** for raw file ingestion. Downstream layers (Bronze -> Silver -> Gold) use standard Structured Streaming from Delta tables.

---

## Recommended Learning Progression

| Step | Topic | What to Learn |
|------|-------|---------------|
| 1 | Directory listing | Auto Loader basics, zero setup |
| 2 | Metadata columns | `_metadata.file_path` in Unity Catalog |
| 3 | IAM role setup | Premium workspace, Databricks runtime role |
| 4 | Storage credential + external location | Unity Catalog prerequisites |
| 5 | Managed file events | Production ingestion |
| Appendix | Classic `useNotifications=true` | Legacy reference |

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:
- Auto Loader configuration: `cloudFiles.format`, `cloudFiles.schemaLocation`
- Difference between notification modes and directory listing
- Schema evolution modes and rescue column
- How Auto Loader relates to Structured Streaming (source vs engine)
- Checkpoint directories and exactly-once guarantees

---

## Key Takeaways

1. **Auto Loader is a SOURCE, not an engine** -- it uses Structured Streaming for execution
2. **Start with directory listing** (`useNotifications=false`) -- zero setup, always works
3. **Graduate to managed file events** (`useManagedFileEvents=true`) for production
4. **Classic notifications** (`useNotifications=true`) are legacy -- more failure modes
5. Use `_metadata.file_path` instead of `input_file_name()` in Unity Catalog
6. **Schema inference** is persisted to `schemaLocation` and only runs once
7. Always set `cloudFiles.region` when using classic notifications
8. Auto Loader is typically used only at the **Bronze layer**

---

## Hands-On Walkthrough

See the accompanying notebook: [`20-auto-loader_notebook.py`](20-auto-loader_notebook.py)

## Next Steps

- [Day 21: Delta Live Tables](../day21-delta-live-tables/)
