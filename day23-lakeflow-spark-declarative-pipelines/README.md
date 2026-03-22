# Day 23: Lakeflow Spark Declarative Pipelines

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand the evolution from Traditional Spark to DLT to Lakeflow Spark Declarative Pipelines
- Build end-to-end Lakeflow Spark Declarative Pipelines with Bronze, Silver, and Gold layers
- Configure expectations for automated data quality enforcement (warn, drop, fail)
- Implement Change Data Capture (CDC) with Auto CDC flows (SCD Type 1 and Type 2)
- Choose between Streaming Tables, Materialized Views, and Views for each use case
- Configure pipeline modes (triggered vs continuous) and cluster strategies
- Inspect pipeline DAGs, event logs, and data quality metrics

## Key Concepts

- **Lakeflow Spark Declarative Pipelines (SDP)** -- formerly Delta Live Tables (DLT); you declare datasets and queries, the framework handles execution
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
- [Day 22: Lakeflow Connect](../day22-lakeflow-connect/) -- data ingestion into the Lakehouse

## Hands-On

### Evolution Notebooks (Traditional Spark → DLT → Lakeflow SDP)

These three notebooks build the **same e-commerce pipeline** using three approaches, showing how each generation solved the problems of the previous one:

1. **[`23-traditional-spark-pipeline_notebook.py`](23-traditional-spark-pipeline_notebook.py)** -- Manual Spark pipeline with explicit orchestration, checkpoints, and error handling (the pain)
2. **[`23-dlt-pipeline_notebook.py`](23-dlt-pipeline_notebook.py)** -- Delta Live Tables pipeline with `@dlt.table` decorators, expectations, and auto-DAG (the improvement)
3. **[`23-lakeflow-sdp-pipeline_notebook.py`](23-lakeflow-sdp-pipeline_notebook.py)** -- Lakeflow SDP pipeline with `@dp.table`, Auto CDC, and full platform integration (the evolution)

### Core Content

- **Guide**: [`23-lakeflow-spark-declarative-pipelines.md`](23-lakeflow-spark-declarative-pipelines.md) -- comprehensive theory, architecture diagrams, evolution comparison, and cloud-specific notes
- **Notebook**: [`23-lakeflow-spark-declarative-pipelines_notebook.py`](23-lakeflow-spark-declarative-pipelines_notebook.py) -- interactive learning notebook covering SDP concepts, syntax, and pipeline configuration
- **Lab Scripts**: [`lab-scripts/`](lab-scripts/) -- production-grade pipeline source files (Bronze, Silver, Gold) ready to run as a Databricks pipeline

## Certification Tip

Lakeflow Spark Declarative Pipelines (still referenced as "Delta Live Tables" or "DLT" in many exam materials) is heavily tested on the **Databricks Certified Data Engineer Professional** exam. Expect questions on:
- Expectation syntax and enforcement actions (`expect`, `expect_or_drop`, `expect_or_fail`)
- Choosing between Streaming Tables and Materialized Views
- Pipeline deployment modes (triggered vs continuous, development vs production)
- Auto CDC flow configuration (KEYS, SEQUENCE BY, SCD types)
- Reading pipeline event logs for troubleshooting

## Next Steps

- [Day 24: Lakeflow Jobs](../day24-lakeflow-jobs/)
