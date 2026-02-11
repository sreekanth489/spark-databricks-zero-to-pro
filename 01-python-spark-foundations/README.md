# Module 01: Python & Spark Foundations

> Master the core concepts of Apache Spark and the Python APIs that power Databricks.

## Prerequisites

- Basic Python knowledge (variables, functions, control flow)
- Completed Module 00 (or equivalent Databricks workspace access)
- A running Databricks cluster (Community Edition is sufficient for all exercises)

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 01 | Python Essentials for Spark | [Guide](01-python-essentials.md) | [Notebook](01-python-essentials_notebook.py) | 45 min |
| 02 | Spark Architecture | [Guide](02-spark-architecture.md) | [Notebook](02-spark-architecture_notebook.py) | 50 min |
| 03 | Distributed Computing | [Guide](03-distributed-computing.md) | [Notebook](03-distributed-computing_notebook.py) | 50 min |
| 04 | RDDs Fundamentals | [Guide](04-rdds-fundamentals.md) | [Notebook](04-rdds-fundamentals_notebook.py) | 45 min |
| 05 | DataFrames Introduction | [Guide](05-dataframes-introduction.md) | [Notebook](05-dataframes-introduction_notebook.py) | 60 min |
| 06 | Spark SQL | [Guide](06-spark-sql.md) | [Notebook](06-spark-sql_notebook.py) | 55 min |
| 07 | Catalyst Optimizer | [Guide](07-catalyst-optimizer.md) | [Notebook](07-catalyst-optimizer_notebook.py) | 50 min |

**Total estimated time: ~6 hours**

## Learning Path

```
01 Python Essentials ──> 02 Spark Architecture ──> 03 Distributed Computing
                                                          |
                                                          v
        06 Spark SQL <── 05 DataFrames Intro <── 04 RDDs Fundamentals
              |
              v
        07 Catalyst Optimizer
```

Work through the topics in order. Each guide introduces concepts, and its companion notebook
lets you practice hands-on in a Databricks environment.

## How to Use This Module

1. **Read the guide** (.md file) for each topic to understand the concepts.
2. **Import the notebook** (.py file) into your Databricks workspace.
3. **Run each cell** in order, reading the markdown explanations between code cells.
4. **Experiment** -- modify the code, change parameters, and observe what happens.

## What You Will Be Able to Do After This Module

- Write Pythonic code that leverages the patterns Spark relies on (lambdas, generators, type hints)
- Explain Spark's driver-executor architecture and how jobs, stages, and tasks relate
- Reason about data partitioning, shuffles, and narrow vs. wide transformations
- Create and transform RDDs, understanding when they are appropriate vs. DataFrames
- Build DataFrames from multiple sources, define schemas, and perform column operations
- Write Spark SQL queries using temp views, CTEs, and built-in functions
- Read query execution plans and understand how the Catalyst optimizer rewrites your queries

## Next Module

[Module 02: Data Ingestion & Transformation](../02-data-ingestion-transformation/README.md)
