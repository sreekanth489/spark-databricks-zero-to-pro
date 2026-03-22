# Day 22: Spark Declarative Pipelines (Lakeflow SDP)

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand the shift from imperative to declarative pipeline development
- Build end-to-end Spark Declarative Pipelines with Bronze, Silver, and Gold layers
- Configure expectations for automated data quality enforcement (warn, drop, fail)
- Implement Change Data Capture (CDC) with Auto CDC flows (SCD Type 1 and Type 2)
- Choose between Streaming Tables, Materialized Views, and Views for each use case
- Configure pipeline modes (triggered vs continuous) and cluster strategies
- Inspect pipeline DAGs, event logs, and data quality metrics

## Key Concepts

- **Spark Declarative Pipelines (SDP)** -- formerly Delta Live Tables (DLT), rebranded as Lakeflow Declarative Pipelines; you declare datasets and queries, the framework handles execution
- **Streaming Table** -- append-only table populated by a streaming query, ideal for ingestion/Bronze layer
- **Materialized View** -- table populated by a batch query, fully recomputed on each run, ideal for Silver/Gold layers
- **Expectations** -- declarative data quality constraints that can warn, drop bad rows, or fail the pipeline
- **Auto CDC Flow** -- built-in change data capture that handles inserts, updates, and deletes with SCD Type 1 or Type 2
- **Append Flow** -- streaming insert-only flow for logs and events
- **Pipeline DAG** -- visual dependency graph automatically derived from your dataset definitions
- **Triggered vs Continuous** -- triggered mode runs once and shuts down; continuous mode keeps running for low-latency

## Prerequisites

- [Day 18: Medallion Architecture](../day18-medallion-architecture/) -- Bronze/Silver/Gold layering
- [Day 19: Structured Streaming](../day19-structured-streaming/) -- streaming fundamentals
- [Day 20: Auto Loader](../day20-auto-loader/) -- file ingestion with cloudFiles
- [Day 21: Change Data Capture](../day21-change-data-capture/) -- CDC concepts and MERGE patterns

## Hands-On

- **Guide**: [`22-spark-declarative-pipelines.md`](22-spark-declarative-pipelines.md) -- comprehensive theory, architecture diagrams, and cloud-specific notes
- **Notebook**: [`22-spark-declarative-pipelines_notebook.py`](22-spark-declarative-pipelines_notebook.py) -- interactive learning notebook covering SDP concepts, syntax, and pipeline configuration
- **Lab Scripts**: [`lab-scripts/`](lab-scripts/) -- production-grade pipeline source files (Bronze, Silver, Gold) ready to run as a Databricks pipeline

## Certification Tip

Spark Declarative Pipelines (still referenced as "Delta Live Tables" or "DLT" in many exam materials) is heavily tested on the **Databricks Certified Data Engineer Professional** exam. Expect questions on:
- Expectation syntax and enforcement actions (`expect`, `expect_or_drop`, `expect_or_fail`)
- Choosing between Streaming Tables and Materialized Views
- Pipeline deployment modes (triggered vs continuous, development vs production)
- Auto CDC flow configuration (KEYS, SEQUENCE BY, SCD types)
- Reading pipeline event logs for troubleshooting

## Next Steps

- [Day 23: SCD Type 2 Pipelines](../day23-scd-type-2-pipelines/)
