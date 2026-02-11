# Module 07: Streaming & Real-Time

> Build real-time data pipelines with Spark Structured Streaming, Auto Loader, and Kafka on Databricks.

## Why This Module Matters

Batch processing gets you yesterday's answers. Streaming gets you answers *now*. Modern
data platforms increasingly demand near-real-time ingestion, continuous ETL, and event-driven
architectures. Whether you are processing clickstream data, IoT sensor feeds, financial
transactions, or CDC events from operational databases, Structured Streaming is the engine
that powers it all on Databricks.

This module takes you from the fundamentals of Spark's streaming model through production-grade
patterns including watermarking, stream-stream joins, Auto Loader ingestion, and Kafka
integration. By the end, you will be able to design, build, and operate streaming pipelines
that run 24/7 in production.

## Prerequisites

- **Modules 00-06** completed (Python/Spark fundamentals, data ingestion, Delta Lake,
  transformations, performance optimization, orchestration)
- A Databricks workspace (Community Edition works for most exercises; Auto Loader file
  notification mode and Kafka integration require a paid tier or external services)
- Familiarity with Delta Lake (Module 03) — Delta is the primary streaming sink

## Topics

| # | Topic | Focus | Time |
|---|-------|-------|------|
| 01 | [Structured Streaming Fundamentals](01-structured-streaming-fundamentals.md) | Streaming vs batch, micro-batch model, sources, sinks, readStream/writeStream | 55 min |
| 02 | [Triggers & Output Modes](02-triggers-output-modes.md) | Append/complete/update modes, trigger types, availableNow vs once | 45 min |
| 03 | [Watermarks & Late Data](03-watermarks-late-data.md) | Event time, late data handling, withWatermark(), state management | 50 min |
| 04 | [Stream-Stream Joins](04-stream-stream-joins.md) | Inner/outer stream joins, stream-static joins, state cleanup | 50 min |
| 05 | [Auto Loader Streaming](05-auto-loader-streaming.md) | cloudFiles, directory listing vs notification, schema evolution, rescued data | 55 min |
| 06 | [Kafka Integration](06-kafka-integration.md) | Kafka source/sink, deserialization, Schema Registry, exactly-once semantics | 55 min |

**Total estimated time: ~5 hours 10 minutes**

## Learning Path

```
01-Fundamentals ──> 02-Triggers ──> 03-Watermarks ──> 04-Joins
   (foundations)     (control)      (time/state)      (combine)
                                                         |
                                                         v
                                    06-Kafka <────── 05-Auto-Loader
                                   (external)        (file ingest)
```

Topics 01-04 build a progressive understanding of Structured Streaming internals and API.
Topic 05 introduces Auto Loader, the Databricks-specific file ingestion engine that you
will use in nearly every production pipeline. Topic 06 extends to external message brokers
with Kafka, the most common streaming source in enterprise architectures.

## Key Themes

- **Unbounded table model** -- Spark treats streams as tables that grow continuously
- **Micro-batch execution** -- Low-latency processing in small batches (not row-by-row)
- **Exactly-once semantics** -- Checkpointing + Delta Lake idempotent writes = reliable pipelines
- **State management** -- Aggregations maintain state; watermarks prevent unbounded growth
- **Auto Loader** -- The preferred Databricks ingestion method for file-based sources
- **Production readiness** -- Checkpointing, monitoring, graceful shutdown, and failure recovery

## Companion Notebooks

Each topic includes a `_notebook.py` file in Databricks source format. These notebooks
are self-contained: they generate their own sample data, run transformations, and clean
up after themselves. Import them into any Databricks workspace by navigating to
**Workspace > Import > File** and selecting the `.py` file.

Notebooks for Topics 05 (Auto Loader) and 06 (Kafka) include simulated alternatives
for environments without access to cloud storage events or a Kafka cluster.

## Next Module

**Module 08 -- Governance & Security**: Unity Catalog, data access control, lineage
tracking, Lakehouse Federation, and PII protection strategies.
