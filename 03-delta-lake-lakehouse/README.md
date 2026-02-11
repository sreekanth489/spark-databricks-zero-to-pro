# Module 03: Delta Lake & Lakehouse

> Master Delta Lake -- the foundation of the Databricks Lakehouse.

Delta Lake is the storage layer that makes the Databricks Lakehouse possible.
It brings ACID transactions, scalable metadata handling, and time travel to
data lakes built on Apache Parquet. This module covers everything from
fundamentals through advanced optimization, culminating in the Medallion
Architecture pattern used in production Lakehouse deployments.

## Prerequisites

- **Module 01** -- DataFrames and Spark SQL (comfortable writing transformations)
- **Module 02** -- Data Ingestion basics (reading/writing files in Spark)
- A Databricks workspace (Community Edition works for most topics)

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 01 | Delta Lake Fundamentals | [Guide](01-delta-lake-fundamentals.md) | [Notebook](01-delta-lake-fundamentals_notebook.py) | 45 min |
| 02 | CRUD & MERGE Operations | [Guide](02-crud-operations.md) | [Notebook](02-crud-operations_notebook.py) | 40 min |
| 03 | Time Travel | [Guide](03-time-travel.md) | [Notebook](03-time-travel_notebook.py) | 35 min |
| 04 | Schema Evolution | [Guide](04-schema-evolution.md) | [Notebook](04-schema-evolution_notebook.py) | 35 min |
| 05 | Optimization | [Guide](05-optimization.md) | [Notebook](05-optimization_notebook.py) | 50 min |
| 06 | Change Data Feed | [Guide](06-change-data-feed.md) | [Notebook](06-change-data-feed_notebook.py) | 35 min |
| 07 | Medallion Architecture | [Guide](07-medallion-architecture.md) | [Notebook](07-medallion-architecture_notebook.py) | 50 min |
| 08 | Delta Sharing | [Guide](08-delta-sharing.md) | [Notebook](08-delta-sharing_notebook.py) | 30 min |

**Total estimated time: ~5.5 hours**

## How to Use This Module

1. **Read the Guide first** -- each `.md` file explains concepts, shows diagrams,
   and maps content to certification exam domains.
2. **Run the Notebook** -- import the companion `_notebook.py` file into your
   Databricks workspace and execute cells interactively.
3. **Experiment** -- modify the notebook code, break things, and observe how
   Delta Lake behaves.

## Certification Alignment

| Exam | Relevant Domains |
|------|-----------------|
| Databricks Certified Data Engineer Associate | Delta Lake (25%), ELT with Spark SQL and Python (20%) |
| Databricks Certified Data Engineer Professional | Incremental Processing (25%), Data Modeling (20%) |
| Databricks Certified Associate Developer for Apache Spark | DataFrames API (applied to Delta tables) |

## Key Concepts Map

```
                    +---------------------------+
                    |     Databricks Lakehouse  |
                    +---------------------------+
                               |
              +----------------+----------------+
              |                                 |
     +--------v--------+            +-----------v-----------+
     |   Delta Lake     |            |   Unity Catalog       |
     |   (Storage Layer) |            |   (Governance Layer)  |
     +--------+---------+            +-----------------------+
              |
    +---------+---------+---------+---------+
    |         |         |         |         |
 +--v--+  +--v--+  +--v--+  +--v--+  +---v----+
 |ACID |  |Time |  |Schema|  |Audit|  |Optimize|
 |Txns |  |Travel| |Evolve|  | CDF |  |Z-Order |
 +-----+  +-----+  +------+  +-----+  +--------+
```

## Next Module

**[Module 04: Data Engineering Pipelines](../04-data-engineering-pipelines/)** --
Build production ETL pipelines using Delta Live Tables and Structured Streaming.
