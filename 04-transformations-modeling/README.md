# Module 04: Transformations & Data Modeling

> Master Spark transformations and learn data modeling patterns for the Lakehouse.

## Prerequisites

- Module 01 (DataFrames and Spark SQL)
- Module 03 (Delta Lake basics)
- Basic understanding of SQL joins and aggregations
- Familiarity with Python functions and type hints

## Why This Module Matters

Transformations are the core of every Spark job. Whether you are joining customer
records across tables, computing rolling averages over time windows, or reshaping
nested JSON payloads, every pipeline depends on the concepts covered here.

This module also bridges the gap between raw transformation logic and production
data modeling. You will learn how to organize tables using the medallion
architecture, implement slowly changing dimensions, and choose between managed
and external tables in Databricks.

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 01 | Joins Deep Dive | [Guide](01-joins-deep-dive.md) | [Notebook](01-joins-deep-dive_notebook.py) | 50 min |
| 02 | Aggregations | [Guide](02-aggregations.md) | [Notebook](02-aggregations_notebook.py) | 40 min |
| 03 | Window Functions | [Guide](03-window-functions.md) | [Notebook](03-window-functions_notebook.py) | 45 min |
| 04 | Complex Types | [Guide](04-complex-types.md) | [Notebook](04-complex-types_notebook.py) | 40 min |
| 05 | UDFs and Pandas UDFs | [Guide](05-udfs-and-pandas-udfs.md) | [Notebook](05-udfs-and-pandas-udfs_notebook.py) | 45 min |
| 06 | Higher-Order Functions | [Guide](06-higher-order-functions.md) | [Notebook](06-higher-order-functions_notebook.py) | 35 min |
| 07 | Data Modeling Patterns | [Guide](07-data-modeling-patterns.md) | [Notebook](07-data-modeling-patterns_notebook.py) | 55 min |

**Total estimated time: ~5 hours**

## Learning Path

```
01-Joins Deep Dive
  |
  v
02-Aggregations -----> 03-Window Functions
                            |
                            v
                       04-Complex Types
                            |
                            v
              05-UDFs & Pandas UDFs ----> 06-Higher-Order Functions
                                              |
                                              v
                                    07-Data Modeling Patterns
```

Topics 01-03 build on each other (joins, then grouping, then windowing).
Topics 04-06 cover advanced expression techniques.
Topic 07 ties everything together with production modeling patterns.

## Key Concepts Across This Module

- **Lazy Evaluation** -- In Pandas, calling a transformation immediately creates
  a new DataFrame. In Spark, transformations only build a logical plan. The plan
  is not executed until an action (show, collect, write) triggers it. This is why
  Spark can optimize across an entire chain of transformations.

- **Narrow vs Wide Transformations** -- Narrow transformations (select, filter,
  map) process each partition independently. Wide transformations (join, groupBy,
  repartition) require shuffling data across the network. Understanding this
  distinction is critical for performance tuning.

- **Immutability** -- In Spark, DataFrames are immutable. Once you create one
  you cannot change it; you can only create a new one by applying a
  transformation. This design enables safe parallel execution and lineage
  tracking.

## Datasets Used

| Dataset | Used In | Description |
|---------|---------|-------------|
| Movies & Studios | Topics 01, 03 | Film metadata with studios, revenue, ratings |
| E-commerce Orders | Topics 02, 06 | Order transactions with products and customers |
| Nested Events JSON | Topic 04 | Web analytics events with nested payloads |
| Text Processing | Topic 05 | Product reviews for UDF demonstrations |
| Retail Sales | Topic 07 | Full star schema for data modeling |

All datasets are synthetically generated inside each notebook -- no external
downloads required.

## Running the Notebooks

1. Import each `_notebook.py` file into your Databricks workspace.
2. Attach to a cluster running **Databricks Runtime 13.3 LTS** or later.
3. Run cells sequentially -- each notebook is self-contained.
4. The final cell in every notebook cleans up temporary tables and views.

## What Comes Next

After completing this module, proceed to:
- **Module 05** -- Performance Tuning & Optimization
- **Module 06** -- Structured Streaming
