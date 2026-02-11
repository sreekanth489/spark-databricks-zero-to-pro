# Module 02: Data Ingestion

> Learn every method for getting data into the Databricks Lakehouse.

## Prerequisites

- **Module 01** completed (DataFrames and Spark SQL basics)
- A running Databricks cluster (DBR 13.3 LTS or later recommended)
- Basic familiarity with file formats (CSV, JSON, Parquet)

## Why This Module Matters

Data ingestion is the front door of every analytics platform. A poorly designed
ingestion layer leads to data loss, duplicates, stale dashboards, and runaway
cloud costs. This module walks you through every tool Databricks provides --
from the simplest `spark.read` call to production-grade Auto Loader pipelines --
so you can pick the right approach for each use case.

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 01 | Reading Files (CSV / JSON / Parquet / Avro) | [Guide](01-reading-files.md) | [Notebook](01-reading-files_notebook.py) | 45 min |
| 02 | Auto Loader (cloudFiles) | [Guide](02-auto-loader.md) | [Notebook](02-auto-loader_notebook.py) | 50 min |
| 03 | COPY INTO | [Guide](03-copy-into.md) | [Notebook](03-copy-into_notebook.py) | 35 min |
| 04 | External Sources (JDBC, Kafka, Cloud Storage) | [Guide](04-external-sources.md) | [Notebook](04-external-sources_notebook.py) | 50 min |
| 05 | Multi-Hop Ingestion Patterns (Bronze / Silver / Gold) | [Guide](05-multi-hop-ingestion.md) | [Notebook](05-multi-hop-ingestion_notebook.py) | 55 min |

**Total estimated time: ~4 hours**

## Learning Path

```
01-Reading Files ──> 02-Auto Loader ──> 03-COPY INTO
                                              │
                         04-External Sources <─┘
                                │
                         05-Multi-Hop Ingestion
```

Start with Topic 01 to understand the fundamentals, then work through Auto
Loader and COPY INTO (the two production ingestion mechanisms). Topic 04
broadens your toolkit to JDBC, Kafka, and cloud storage. Topic 05 ties
everything together into the Medallion Architecture pattern.

## How to Use These Materials

Each topic has two companion files:

| File type | Purpose |
|-----------|---------|
| `NN-topic-name.md` | Conceptual guide -- read this first for theory, diagrams, and decision frameworks |
| `NN-topic-name_notebook.py` | Databricks-compatible notebook -- import into your workspace and run cell by cell |

**Importing notebooks into Databricks:**

1. In the Databricks workspace sidebar, click **Workspace**.
2. Navigate to your target folder.
3. Right-click and select **Import**.
4. Choose **File** and upload the `.py` file.
5. Databricks will recognize the `# Databricks notebook source` header and render it as a notebook.

## Key Concepts Covered

- Batch file reading with `spark.read` (CSV, JSON, Parquet, Avro, ORC, text, binary)
- Schema inference vs. explicit schema definition
- Corrupt-record handling modes (PERMISSIVE, DROPMALFORMED, FAILFAST)
- Incremental ingestion with Auto Loader (`cloudFiles`)
- Schema evolution and the rescued data column
- Idempotent loading with `COPY INTO`
- JDBC connections with predicate pushdown and partitioned reads
- Kafka streaming integration
- Cloud storage access patterns (S3, ADLS Gen2, GCS)
- The Medallion Architecture (Bronze / Silver / Gold)
- Exactly-once ingestion semantics

## Next Module

Once you have mastered data ingestion, move on to **Module 03: Data
Transformations** to learn how to clean, reshape, and enrich your ingested data.
