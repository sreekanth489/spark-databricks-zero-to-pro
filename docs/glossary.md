# Glossary — Spark & Databricks Terms

Quick reference for terminology used throughout this repository, organized by category.

---

## Apache Spark Core

| Term | Definition |
|------|-----------|
| **SparkSession** | Entry point for Spark functionality. Created with `spark = SparkSession.builder.getOrCreate()`. In Databricks, `spark` is pre-configured. |
| **Driver** | The JVM process that runs your `main()` function, creates the SparkSession, and coordinates work across the cluster. |
| **Executor** | Worker JVM processes that run tasks and store data. Each executor runs on a cluster node. |
| **Task** | The smallest unit of work sent to an executor — operates on one data partition. |
| **Stage** | A group of tasks that can run in parallel without shuffling data. Stages are separated by shuffle boundaries. |
| **Job** | A complete computation triggered by an action (e.g., `.count()`, `.show()`). A job consists of one or more stages. |
| **Partition** | A chunk of data processed by a single task. More partitions = more parallelism (up to available cores). |
| **Shuffle** | Redistribution of data across partitions — happens during joins, `groupBy`, `repartition`. Expensive operation. |
| **Transformation** | A lazy operation that defines a computation but doesn't execute it (e.g., `.filter()`, `.select()`, `.groupBy()`). |
| **Action** | An operation that triggers execution and returns results (e.g., `.count()`, `.collect()`, `.write`). |
| **Lazy Evaluation** | Spark doesn't execute transformations until an action is called. This allows the optimizer to plan the most efficient execution. |
| **DAG** | Directed Acyclic Graph — Spark's execution plan showing the sequence of operations. |
| **Catalyst Optimizer** | Spark SQL's query optimizer that transforms logical plans into optimized physical plans. |
| **Tungsten** | Spark's execution engine for CPU and memory optimization — manages off-heap memory and code generation. |
| **AQE** | Adaptive Query Execution — runtime optimization that adjusts plans based on actual data statistics. |

## DataFrames & SQL

| Term | Definition |
|------|-----------|
| **DataFrame** | A distributed collection of rows organized into named columns — like a table in a database. |
| **Dataset** | Typed version of DataFrame (Scala/Java only). In Python, DataFrame and Dataset are the same. |
| **RDD** | Resilient Distributed Dataset — Spark's low-level API. DataFrames are built on top of RDDs. |
| **Schema** | The structure of a DataFrame — column names and data types (`StructType` / `StructField`). |
| **Temp View** | A named DataFrame registered so it can be queried with SQL. Lives only for the SparkSession duration. |
| **UDF** | User-Defined Function — custom Python/Scala function applied to DataFrame columns. |
| **Window Function** | A function that operates on a group of rows related to the current row (e.g., `ROW_NUMBER`, `LAG`, `LEAD`). |

## Delta Lake

| Term | Definition |
|------|-----------|
| **Delta Lake** | Open-source storage layer that adds ACID transactions, schema enforcement, and time travel to data lakes. |
| **Transaction Log** | The `_delta_log/` directory containing JSON files that record every change to a Delta table. |
| **ACID** | Atomicity, Consistency, Isolation, Durability — guarantees for reliable data transactions. |
| **Time Travel** | Querying or restoring previous versions of a Delta table using `VERSION AS OF` or `TIMESTAMP AS OF`. |
| **MERGE** | SQL operation that combines INSERT, UPDATE, and DELETE in a single atomic transaction. |
| **Schema Evolution** | Automatically adding new columns when writing data with `mergeSchema` option. |
| **Schema Enforcement** | Rejecting writes that don't match the table's existing schema (Delta Lake default behavior). |
| **OPTIMIZE** | Compacts small files into larger ones for better read performance. |
| **VACUUM** | Removes old data files no longer referenced by the transaction log. |
| **Z-Order** | Co-locates related data in the same files for faster filtered queries. |
| **Liquid Clustering** | Databricks feature that replaces partitioning and Z-ordering with automatic, incremental data layout optimization. |
| **Change Data Feed (CDF)** | Records row-level changes (inserts, updates, deletes) for downstream processing. |
| **Delta Sharing** | Open protocol for securely sharing data across organizations without copying it. |

## Databricks Platform

| Term | Definition |
|------|-----------|
| **Workspace** | A Databricks deployment — contains notebooks, clusters, jobs, and data objects. |
| **Cluster** | A set of compute resources (driver + workers) that runs notebooks and jobs. |
| **All-Purpose Cluster** | Interactive cluster for development and ad-hoc analysis. |
| **Job Cluster** | Ephemeral cluster created for a specific job run, then terminated. |
| **Databricks Runtime (DBR)** | The set of software (Spark, Delta Lake, libraries) installed on cluster nodes. LTS = Long Term Support. |
| **Photon** | Databricks' native C++ execution engine — faster than standard Spark for SQL and DataFrame workloads. |
| **Serverless** | Databricks-managed compute that starts in seconds with no cluster configuration. |
| **DBFS** | Databricks File System — an abstraction over cloud object storage accessible at `/dbfs/`. |
| **Unity Catalog** | Centralized governance layer for data and AI assets — manages permissions, lineage, and discovery. |
| **Metastore** | A top-level container in Unity Catalog that stores metadata about databases, tables, and permissions. |
| **Catalog** | A namespace grouping within Unity Catalog (e.g., `prod`, `dev`). Structure: `catalog.schema.table`. |
| **Volumes** | Unity Catalog-managed storage locations for non-tabular files (CSVs, images, models). |
| **Repos** | Git integration in Databricks — sync notebooks and code with remote Git repositories. |
| **Workflows** | Databricks job orchestration — schedule and chain notebooks, Python scripts, and JARs. |
| **DLT** | Delta Live Tables — declarative framework for building reliable data pipelines. |
| **Auto Loader** | Incrementally ingests new files from cloud storage using Structured Streaming (`cloudFiles`). |
| **COPY INTO** | SQL command for idempotent batch ingestion of files into Delta tables. |
| **Asset Bundles (DABs)** | Infrastructure-as-code for Databricks — define jobs, pipelines, and resources in YAML. |
| **Databricks Connect** | Library for running Spark code from your local IDE against a Databricks cluster. |
| **Medallion Architecture** | Data design pattern with Bronze (raw), Silver (cleaned), and Gold (aggregated) layers. |

## Data Ingestion

| Term | Definition |
|------|-----------|
| **Batch Ingestion** | Loading a complete dataset at once (e.g., reading a directory of Parquet files). |
| **Streaming Ingestion** | Continuously processing new data as it arrives (e.g., Auto Loader, Kafka). |
| **Parquet** | Columnar file format — efficient for analytics. Default format for Spark. |
| **Avro** | Row-based file format — efficient for streaming and write-heavy workloads. |
| **ORC** | Optimized Row Columnar format — similar to Parquet, common in Hive ecosystems. |
| **JSON (newline-delimited)** | Each line is a separate JSON record. Spark reads this with `format("json")`. |
| **Multi-Hop** | Pattern where data flows through multiple processing stages (typically Bronze → Silver → Gold). |

## ML / AI (Preview)

| Term | Definition |
|------|-----------|
| **MLflow** | Open-source platform for ML lifecycle management — tracking, packaging, and deployment. |
| **Model Registry** | Central repository for managing ML model versions, stages, and approvals. |
| **Feature Store** | Centralized repository for storing and serving ML features. |
| **Vector Search** | Databricks service for similarity search over embedding vectors (used in RAG). |
| **RAG** | Retrieval-Augmented Generation — combining LLMs with document retrieval for grounded answers. |
| **Model Serving** | Hosting ML/AI models as REST API endpoints on Databricks. |
