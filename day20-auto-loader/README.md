# Day 20: Auto Loader

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand Auto Loader as a specialized file ingestion SOURCE built on Structured Streaming
- Implement three Auto Loader modes: directory listing, managed file events, classic notifications
- Configure schema inference, evolution, and rescue columns
- Set up IAM policies and bucket policies for notification modes on AWS
- Troubleshoot common Auto Loader errors on AWS
- Integrate Auto Loader with Medallion Architecture (Bronze layer ingestion)

## Key Concepts

- **Auto Loader** (`cloudFiles`) -- Databricks-native file ingestion source that incrementally processes new files from cloud storage, built on top of Structured Streaming
- **Directory Listing Mode** -- scans S3 directory for new files (zero setup, recommended starter)
- **Managed File Events** -- uses Unity Catalog external locations with file events enabled (recommended production)
- **Classic File Notifications** -- auto-manages S3 bucket notifications + SNS/SQS per stream (legacy)
- **Schema Inference** -- Auto Loader infers schema from source files and persists it to `schemaLocation`
- **Schema Evolution** -- automatically handles new columns with `schemaEvolutionMode`
- **Rescue Column** (`_rescued_data`) -- captures data that does not match the expected schema

## Topics Covered

- Auto Loader vs standard file streaming: why Auto Loader exists
- Relationship: Auto Loader (source) uses Structured Streaming (engine)
- Three modes: directory listing, managed file events, classic notifications
- Schema inference with `cloudFiles.schemaLocation` and `inferColumnTypes`
- Schema evolution modes: `addNewColumns`, `rescue`, `failOnNewColumns`, `none`
- Metadata columns: `_metadata.file_path` vs `input_file_name()`
- IAM policies and S3 bucket policies for AWS notification modes
- Common errors and troubleshooting (PermanentRedirect, AccessDenied, CloudTrail)
- Full pipeline: Auto Loader -> Bronze -> Silver
- Rate limiting and advanced configuration options

## Hands-On

- **Guide**: [`20-auto-loader.md`](20-auto-loader.md) -- theory, three modes, IAM setup, and troubleshooting
- **Notebook**: [`20-auto-loader_notebook.py`](20-auto-loader_notebook.py) -- runnable Databricks lab with all three modes, schema evolution, and Bronze -> Silver pipeline

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- Auto Loader configuration: `cloudFiles.format`, `cloudFiles.schemaLocation`
- Difference between `useNotifications`, `useManagedFileEvents`, and directory listing
- Schema evolution modes and rescue column behavior
- How Auto Loader relates to Structured Streaming
- Checkpoint directories and exactly-once guarantees

## Next Steps

- [Day 21: Delta Live Tables](../day21-delta-live-tables/)
