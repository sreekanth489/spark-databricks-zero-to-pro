# Day 19: Structured Streaming & Auto Loader

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand Spark Structured Streaming fundamentals: sources, sinks, triggers, and checkpoints
- Master Auto Loader (`cloudFiles`) for incremental file ingestion from AWS S3
- Compare and implement both Auto Loader modes: **file notification** (S3 + SQS) and **directory listing**
- Build end-to-end streaming pipelines that feed into Medallion Architecture layers
- Handle schema inference, evolution, and rescue columns in streaming workloads
- Use triggers (`availableNow`, `processingTime`) to control micro-batch behavior

## Key Concepts

- **Structured Streaming** -- Spark's stream processing engine that treats a live data stream as a continuously appended table
- **Auto Loader** (`cloudFiles`) -- Databricks-native streaming source that incrementally ingests new files from cloud storage
- **File Notification Mode** -- Auto Loader uses S3 event notifications (via SQS) for near-real-time file discovery
- **Directory Listing Mode** -- Auto Loader polls the S3 directory to discover new files (no infrastructure setup needed)
- **Checkpointing** -- persists streaming progress to S3 so pipelines can resume exactly where they left off
- **Schema Inference & Evolution** -- Auto Loader can infer and adapt to schema changes in source files
- **Trigger Modes** -- `availableNow` (process all pending then stop), `processingTime` (fixed intervals), continuous
- **Rescue Column** (`_rescued_data`) -- captures data that does not match the expected schema

## Topics Covered

- Structured Streaming architecture: input sources, query plans, output sinks
- Auto Loader vs traditional `spark.readStream.format("parquet")`
- File notification mode: how S3 -> SNS -> SQS -> Auto Loader works
- Directory listing mode: polling-based file discovery
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
- **Notebook**: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py) -- runnable Databricks lab with both Auto Loader modes on AWS S3

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
