# Day 19: Structured Streaming

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand Spark Structured Streaming as a stream processing engine
- Distinguish Structured Streaming (the engine) from Auto Loader (a specialized source)
- Stream data from Delta tables and file sources
- Perform stream-static joins to enrich streaming data
- Apply watermarking for late data handling
- Compare trigger strategies and output modes
- Monitor and manage streaming queries in production

## Key Concepts

- **Structured Streaming** -- Spark's stream processing engine that treats live data as a continuously appended unbounded table
- **Micro-batch Execution** -- Structured Streaming processes data in small batches, each producing exactly-once results
- **Checkpointing** -- persists streaming progress to S3 so pipelines can resume exactly where they left off
- **Stream-Static Join** -- joining a streaming DataFrame with a static (batch) DataFrame for enrichment
- **Watermarking** -- defines how long to wait for late-arriving data before finalizing aggregation results
- **Trigger Modes** -- `availableNow` (process all then stop), `processingTime` (fixed intervals), continuous
- **Output Modes** -- append (new rows only), complete (full result), update (changed rows only)

## Topics Covered

- Structured Streaming architecture: unbounded table model
- How Structured Streaming relates to Auto Loader (engine vs source)
- Streaming from Delta tables (`readStream.format("delta")`)
- Streaming from file sources (`readStream.format("parquet")`)
- Stream-static joins for data enrichment
- Trigger strategies: `availableNow`, `processingTime`, `once` (deprecated)
- Output modes: append, complete, update
- Watermarking for late data and window aggregations
- Checkpointing and exactly-once semantics
- Stream monitoring: `spark.streams.active`, `lastProgress`, `status`
- Graceful shutdown patterns

## Hands-On

- **Guide**: [`19-structured-streaming.md`](19-structured-streaming.md) -- theory, architecture, and production patterns
- **Notebook**: [`19-structured-streaming_notebook.py`](19-structured-streaming_notebook.py) -- runnable Databricks lab covering Delta streaming, joins, triggers, watermarks, and monitoring

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- Difference between batch and streaming reads (`spark.read` vs `spark.readStream`)
- Checkpoint directories and exactly-once guarantees
- Trigger modes: `availableNow`, `processingTime`, `once` (deprecated)
- Output modes: append, complete, update
- Watermarking syntax and behavior
- How Structured Streaming relates to Auto Loader

## Next Steps

- [Day 20: Auto Loader](../day20-auto-loader/) -- the optimized file ingestion source built on Structured Streaming
