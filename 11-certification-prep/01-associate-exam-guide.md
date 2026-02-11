# Associate Data Engineer Exam Guide

> Module 11 -- Topic 01 | Level: Intermediate | Time: 90 min

## Learning Objectives

- Understand the Databricks Certified Data Engineer Associate exam format and logistics
- Know the five exam domains and their relative weight
- Identify the key topics within each domain and map them to course modules
- Recognize common question patterns and how to approach them
- Build confidence through targeted review of the highest-weight domains

## Exam Overview

### Format and Logistics

| Attribute | Detail |
|-----------|--------|
| Certification name | Databricks Certified Data Engineer Associate |
| Number of questions | 45 multiple choice |
| Time limit | 90 minutes (~2 minutes per question) |
| Passing score | 70% (approximately 32 out of 45 correct) |
| Question types | Single-answer multiple choice (4 options: A, B, C, D) |
| Delivery | Online proctored (Kryterion) |
| Cost | $200 USD |
| Retake policy | 14-day wait after first attempt |
| Validity period | 2 years from passing date |

### Who Should Take This Exam

This exam is designed for data engineers with at least 6 months of hands-on experience using Databricks and Apache Spark. You should be comfortable writing PySpark and Spark SQL code, working with Delta Lake tables, and setting up basic data pipelines.

---

## Domain 1: Databricks Lakehouse Platform (24%)

**Approximate questions: 10-11 out of 45**

This domain tests your understanding of the Lakehouse architecture, Databricks workspace components, and cluster management.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Lakehouse architecture | Combines data lake storage with data warehouse management; Delta Lake provides the reliability layer | Module 03 |
| Data lake vs. data warehouse vs. lakehouse | Trade-offs of each; why lakehouse unifies analytics and ML | Module 03 |
| Clusters | All-purpose vs. job clusters; autoscaling; cluster policies; Databricks Runtime versions | Module 00 |
| Notebooks | Cell types (Python, SQL, Scala, R, Markdown); magic commands (%sql, %md, %run); notebook workflows | Module 00, 01 |
| Repos (Git integration) | Connecting to Git providers; branching; pull requests from Databricks | Module 06 |
| Databricks SQL | SQL warehouses; dashboards; alerts; query history | Module 01 |
| Delta Lake fundamentals | ACID transactions; time travel; schema enforcement; schema evolution | Module 03 |
| DBFS and cloud storage | Default storage; mounting; Unity Catalog external locations | Module 00 |

### Concepts You Must Know

- **SparkSession** is the entry point for all Spark functionality. In Databricks, the `spark` variable is pre-configured and available in every notebook.
- **Cluster types**: All-purpose clusters are for interactive workloads (development, ad-hoc queries). Job clusters are created and terminated automatically for scheduled production jobs and are more cost-effective.
- **Databricks Runtime (DBR)**: Includes Apache Spark, Delta Lake, and curated libraries. DBR ML adds machine learning libraries.
- **Delta Lake** is the default storage format in Databricks. It stores data as Parquet files plus a transaction log (`_delta_log/`).
- **DBFS** (Databricks File System) provides a unified interface to cloud storage. Paths look like `/mnt/data/` or `dbfs:/mnt/data/`.

### Study Tips for This Domain

- Focus on understanding "why" the lakehouse architecture exists (combines best of data lake and warehouse).
- Know the difference between managed and external tables.
- Be able to explain time travel syntax: `VERSION AS OF` and `TIMESTAMP AS OF`.

---

## Domain 2: ELT with Spark SQL and Python (29%)

**Approximate questions: 13 out of 45**

This is the highest-weight domain. It covers reading and writing data, DataFrame transformations, and SQL operations.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Reading data | `spark.read.format().option().load()`; CSV, JSON, Parquet, Delta formats | Module 02 |
| Writing data | `df.write.format().mode().save()`; overwrite, append, errorIfExists, ignore modes | Module 02 |
| Schema definition | StructType/StructField; inferSchema; DDL strings | Module 01, 02 |
| Column operations | `select`, `withColumn`, `drop`, `alias`, `cast` | Module 04 |
| Filtering | `filter`, `where`, `between`, `isin`, `isNull`, `isNotNull` | Module 04 |
| Joins | Inner, left, right, full outer, cross, anti, semi joins | Module 04 |
| Aggregations | `groupBy`, `agg`, `count`, `sum`, `avg`, `min`, `max` | Module 04 |
| Window functions | `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `over` | Module 04 |
| SQL operations | CTEs, subqueries, CASE WHEN, UNION, PIVOT, UNPIVOT | Module 01, 04 |
| Built-in functions | `pyspark.sql.functions`: `col`, `lit`, `when`, `coalesce`, `concat`, date functions | Module 04 |
| UDFs | `@udf` decorator; registered SQL UDFs; performance implications | Module 04 |
| Delta operations | `CREATE TABLE`, `INSERT INTO`, `UPDATE`, `DELETE`, `MERGE` | Module 03 |

### Concepts You Must Know

- **Lazy evaluation**: Transformations (select, filter, join) build a logical plan but do not execute. Actions (show, count, collect, write) trigger execution.
- **Narrow vs. wide transformations**: Narrow (map, filter) do not require shuffles. Wide (groupBy, join, repartition) require shuffles across the cluster.
- **Write modes**: `overwrite` replaces all data, `append` adds data, `errorIfExists` (default) fails if data exists, `ignore` silently skips.
- **MERGE INTO**: Combines INSERT, UPDATE, and DELETE in one atomic operation. Syntax: `MERGE INTO target USING source ON condition WHEN MATCHED THEN ... WHEN NOT MATCHED THEN ...`
- **cache() vs. persist()**: `cache()` stores DataFrame in memory. `persist()` accepts a storage level parameter (MEMORY_ONLY, MEMORY_AND_DISK, etc.).

### Study Tips for This Domain

- This domain carries 29% of the exam weight -- spend extra time here.
- Practice writing both PySpark and Spark SQL versions of the same transformations.
- Know the most common `pyspark.sql.functions` by heart: `col`, `lit`, `when`, `coalesce`, `concat`, `substring`, `date_format`, `datediff`, `current_timestamp`.
- Understand the difference between `select(col("name"))` and `select("name")` -- both work but have different implications for complex expressions.

---

## Domain 3: Incremental Data Processing (22%)

**Approximate questions: 10 out of 45**

This domain covers Structured Streaming, Auto Loader, COPY INTO, and Delta Live Tables.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Structured Streaming | `readStream`, `writeStream`, triggers, output modes (append, complete, update) | Module 07 |
| Auto Loader | `cloudFiles` format; schema inference and evolution; checkpoint location | Module 02, 07 |
| COPY INTO | SQL-based file ingestion; idempotent; good for occasional batch loads | Module 02 |
| Auto Loader vs. COPY INTO | Auto Loader preferred for most cases (scales better, schema evolution); COPY INTO for simple one-time loads | Module 02 |
| Triggers | `trigger(availableNow=True)` processes all data then stops; `trigger(processingTime="10 seconds")` for continuous; `trigger(once=True)` is deprecated | Module 07 |
| Checkpointing | Required for fault tolerance; stores offsets and state; must be unique per stream | Module 07 |
| Delta Live Tables (DLT) | Declarative pipeline framework; `@dlt.table`, `@dlt.view`; live vs. streaming tables | Module 07 |
| DLT expectations | Data quality rules: `EXPECT` (warn), `EXPECT OR DROP` (filter bad rows), `EXPECT OR FAIL` (halt pipeline) | Module 07 |
| Medallion architecture | Bronze (raw), Silver (cleansed), Gold (aggregated) layers in a lakehouse | Module 03 |

### Concepts You Must Know

- **Auto Loader** uses `spark.readStream.format("cloudFiles")` with `.option("cloudFiles.format", "json")` (or csv, parquet, etc.). It automatically detects new files and tracks which files have been processed.
- **trigger(availableNow=True)** is the preferred replacement for `trigger(once=True)`. It processes all available data in multiple micro-batches (better for large backlogs) and then stops.
- **Output modes**: `append` (new rows only, default), `complete` (full result table, used with aggregations), `update` (only changed rows).
- **DLT expectations** enforce data quality declaratively:
  - `EXPECT ("valid_id" ON id IS NOT NULL)` -- logs violations but keeps rows
  - `EXPECT OR DROP ("valid_id" ON id IS NOT NULL)` -- drops violating rows
  - `EXPECT OR FAIL ("valid_id" ON id IS NOT NULL)` -- fails the pipeline

### Study Tips for This Domain

- The exam distinguishes between Auto Loader and COPY INTO -- know when to use each.
- Understand that checkpoints are essential and must not be shared between different streams.
- Know the three DLT expectation levels and their behavior.
- Be comfortable with the medallion architecture (Bronze/Silver/Gold) as a common pattern.

---

## Domain 4: Production Pipelines (16%)

**Approximate questions: 7 out of 45**

This domain covers jobs, workflows, orchestration, and monitoring.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Databricks Jobs | Creating multi-task jobs; task types (notebook, Python, SQL, DLT pipeline, JAR) | Module 06 |
| Job clusters vs. all-purpose | Job clusters are ephemeral, cheaper, auto-terminate; preferred for production | Module 06 |
| Task orchestration | Task dependencies; linear and fan-out/fan-in patterns; task values for passing data | Module 06 |
| Scheduling | Cron expressions; continuous scheduling; manual triggers; pausing | Module 06 |
| Retries and alerts | Retry policies; email/webhook alerts on failure, success, or duration thresholds | Module 06 |
| DLT pipelines | Creating and managing DLT pipelines in production; development vs. production mode | Module 07 |
| Monitoring | Job run history; run duration tracking; identifying failed tasks | Module 09 |

### Concepts You Must Know

- **Multi-task jobs** allow you to define a DAG of tasks with dependencies. Tasks can use different cluster types and even different languages.
- **Job clusters** are created when a job starts and terminated when it finishes. They are more cost-effective than all-purpose clusters for production workloads.
- **Task values** (`dbutils.jobs.taskValues.set()` and `dbutils.jobs.taskValues.get()`) pass small amounts of data between tasks in a multi-task job.
- **DLT pipeline modes**: Development mode reuses clusters and does not retry on failure (faster iteration). Production mode creates new clusters and retries on failure.

### Study Tips for This Domain

- Focus on understanding the job orchestration UI and how task dependencies work.
- Know the difference between development and production mode for DLT pipelines.
- Understand how to set up alerts and monitor job health.

---

## Domain 5: Data Governance (9%)

**Approximate questions: 4 out of 45**

This is the lowest-weight domain but still important -- a few questions wrong here could mean failing.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Unity Catalog | Three-level namespace: `catalog.schema.table`; centralized governance | Module 08 |
| Access control | `GRANT`, `REVOKE`; privileges (SELECT, MODIFY, CREATE TABLE, ALL PRIVILEGES, USAGE) | Module 08 |
| Table ownership | Owner can grant privileges; `ALTER TABLE ... SET OWNER TO` | Module 08 |
| Data discovery | Column-level tags; data lineage; search | Module 08 |
| Dynamic views | Row-level and column-level security using `current_user()` and `is_member()` | Module 08 |
| Managed vs. external tables | Managed tables: Unity Catalog controls storage; External tables: you manage storage | Module 03, 08 |

### Concepts You Must Know

- **Unity Catalog namespace**: `catalog.schema.table` (three levels). This replaces the legacy hive_metastore which used `database.table` (two levels).
- **GRANT SELECT ON TABLE catalog.schema.table TO group_name** -- standard syntax for granting read access.
- **USAGE privilege** is required at the catalog and schema level before any table-level privilege takes effect.
- **Dynamic views** can mask columns or filter rows based on the querying user: `CASE WHEN is_member('admins') THEN ssn ELSE '***' END`.

### Study Tips for This Domain

- Although only 9%, getting all 4 questions right is easy if you know the basics.
- Memorize the three-level namespace and common GRANT/REVOKE syntax.
- Understand the difference between managed and external tables in Unity Catalog.

---

## Exam Strategy

### Time Management

- You have 90 minutes for 45 questions = 2 minutes per question.
- Flag difficult questions and move on. Come back to them after completing easier questions.
- Do not spend more than 3 minutes on any single question in your first pass.

### Question Approach

1. **Read the entire question** before looking at answers.
2. **Eliminate obviously wrong answers** first (usually 1-2 are clearly incorrect).
3. **Look for absolute words** like "always", "never", "only" -- these are often (but not always) incorrect.
4. **Choose the most specific correct answer** when multiple options seem partially correct.
5. **When in doubt, pick the Databricks-native solution** (e.g., Auto Loader over custom file listing, Unity Catalog over legacy access control).

### Common Traps

- Confusing `trigger(once=True)` (deprecated) with `trigger(availableNow=True)` (preferred).
- Mixing up write modes: `overwrite` vs. `append` vs. `ignore`.
- Forgetting that `cache()` is lazy -- it does not execute until an action triggers it.
- Thinking COPY INTO and Auto Loader are interchangeable -- Auto Loader is generally preferred.
- Confusing MERGE syntax -- remember it uses `WHEN MATCHED` and `WHEN NOT MATCHED`.

---

## Quick Reference: Module-to-Domain Mapping

| Course Module | Associate Domain(s) |
|--------------|---------------------|
| Module 00: Setup & Basics | Domain 1 (Platform) |
| Module 01: Python & Spark Foundations | Domain 1 (Platform), Domain 2 (ELT) |
| Module 02: Data Ingestion | Domain 2 (ELT), Domain 3 (Incremental) |
| Module 03: Delta Lake & Lakehouse | Domain 1 (Platform), Domain 2 (ELT) |
| Module 04: Transformations & Modeling | Domain 2 (ELT) |
| Module 05: Performance Optimization | Domain 2 (ELT) |
| Module 06: Orchestration & CI/CD | Domain 4 (Production Pipelines) |
| Module 07: Streaming & Real-Time | Domain 3 (Incremental) |
| Module 08: Governance & Security | Domain 5 (Governance) |
| Module 09: Testing & Monitoring | Domain 4 (Production Pipelines) |
| Module 10: Real-World Projects | All domains |

---

## Recommended Study Order (by domain weight)

1. **Domain 2 -- ELT with Spark SQL and Python (29%)**: Spend the most time here. Practice DataFrame transformations, SQL queries, MERGE operations.
2. **Domain 1 -- Lakehouse Platform (24%)**: Review Lakehouse architecture, cluster types, Delta Lake fundamentals.
3. **Domain 3 -- Incremental Data Processing (22%)**: Study Auto Loader, Structured Streaming triggers, DLT expectations.
4. **Domain 4 -- Production Pipelines (16%)**: Understand multi-task jobs, job clusters, scheduling.
5. **Domain 5 -- Data Governance (9%)**: Learn Unity Catalog namespace, GRANT/REVOKE syntax, dynamic views.

---

## Next Steps

After reviewing this guide:
1. Work through the companion notebook (`01-associate-exam-guide_notebook.py`) for hands-on practice.
2. Take the practice questions in Topic 02 under timed conditions.
3. Score yourself and identify weak domains.
4. Go back to the relevant course modules for deeper review on weak areas.
5. Repeat the practice questions until you consistently score above 80%.
