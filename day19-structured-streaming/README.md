# Day 19: Structured Streaming & Auto Loader

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand Spark Structured Streaming fundamentals: sources, sinks, triggers, and checkpoints
- Master Auto Loader (`cloudFiles`) for incremental file ingestion from AWS S3
- Compare and implement three Auto Loader modes: **directory listing**, **managed file events**, and **classic notifications**
- Build end-to-end streaming pipelines that feed into Medallion Architecture layers
- Handle schema inference, evolution, and rescue columns in streaming workloads
- Use triggers (`availableNow`, `processingTime`) to control micro-batch behavior

## Key Concepts

- **Structured Streaming** -- Spark's stream processing engine that treats a live data stream as a continuously appended table
- **Auto Loader** (`cloudFiles`) -- Databricks-native streaming source that incrementally ingests new files from cloud storage
- **Directory Listing Mode** -- Auto Loader polls the S3 directory to discover new files (zero setup, recommended starter)
- **Managed File Events** -- Auto Loader uses Unity Catalog external locations with file events for near-real-time detection (recommended production)
- **Classic File Notifications** -- Auto Loader auto-manages SNS/SQS per stream (legacy, more moving parts)
- **Checkpointing** -- persists streaming progress to S3 so pipelines can resume exactly where they left off
- **Schema Inference & Evolution** -- Auto Loader can infer and adapt to schema changes in source files
- **Trigger Modes** -- `availableNow` (process all pending then stop), `processingTime` (fixed intervals), continuous
- **Rescue Column** (`_rescued_data`) -- captures data that does not match the expected schema

## Topics Covered

- Structured Streaming architecture: input sources, query plans, output sinks
- Auto Loader vs traditional `spark.readStream.format("parquet")`
- Directory listing mode: polling-based file discovery (zero setup)
- Managed file events: Unity Catalog external locations with file events (modern production)
- Classic file notification mode: S3 -> SNS -> SQS -> Auto Loader (legacy)
- IAM policies and bucket policies for notification modes
- Common errors and troubleshooting (PermanentRedirect, AccessDenied, CloudTrail)
- Schema inference with `cloudFiles.schemaLocation`
- Schema evolution with `cloudFiles.schemaEvolutionMode`
- Rescue column for handling unexpected data
- Trigger strategies: `availableNow=True`, `processingTime="30 seconds"`, continuous
- Output modes: append, complete, update
- Checkpoint management and exactly-once processing
- Stream monitoring with `spark.streams.active`
- Integration with Medallion Architecture (Bronze layer ingestion)
- Production best practices: error handling, idempotency, monitoring

## Hands-On

See the accompanying guide and notebook:

- **Guide**: [`19-structured-streaming.md`](19-structured-streaming.md) -- theory, architecture diagrams, Auto Loader internals, and production patterns
- **Notebook**: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py) -- runnable Databricks lab with three Auto Loader modes (directory listing, managed file events, classic notifications), schema evolution, and Bronze -> Silver pipeline

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- Difference between batch and streaming reads in Spark
- Auto Loader configuration options (`cloudFiles.format`, `cloudFiles.schemaLocation`)
- Understanding checkpoint directories and exactly-once guarantees
- Trigger modes and when to use each
- How Auto Loader handles schema evolution
- Output modes (append vs complete vs update)

## Next Steps

- [Day 20: Advanced Streaming](../day20-advanced-streaming/)
