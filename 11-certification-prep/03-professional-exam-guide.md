# Professional Data Engineer Exam Guide

> Module 11 -- Topic 03 | Level: Advanced | Time: 90 min

## Learning Objectives

- Understand the Databricks Certified Data Engineer Professional exam format and logistics
- Know the six exam domains and their relative weight
- Identify the advanced topics that distinguish Professional from Associate
- Recognize architecture and design decision patterns tested on the exam
- Build confidence through targeted review of the highest-weight domains

## Exam Overview

### Format and Logistics

| Attribute | Detail |
|-----------|--------|
| Certification name | Databricks Certified Data Engineer Professional |
| Number of questions | 60 multiple choice |
| Time limit | 120 minutes (~2 minutes per question) |
| Passing score | 70% (approximately 42 out of 60 correct) |
| Question types | Single-answer multiple choice (4 options: A, B, C, D) |
| Delivery | Online proctored (Kryterion) |
| Cost | $300 USD |
| Retake policy | 14-day wait after first attempt |
| Validity period | 2 years from passing date |

### How Professional Differs from Associate

The Professional exam tests not just knowledge of features but your ability to make design decisions and troubleshoot production issues. Key differences:

| Aspect | Associate | Professional |
|--------|-----------|-------------|
| Question style | "What does X do?" | "Given this scenario, what is the best approach?" |
| Depth | Know the syntax | Know the why, trade-offs, and edge cases |
| Architecture | Use features | Design solutions using features |
| Troubleshooting | Basic error handling | Diagnose production issues from symptoms |
| Optimization | Know that optimization exists | Know specific tuning strategies and when to apply them |

---

## Domain 1: Databricks Tooling (20%)

**Approximate questions: 12 out of 60**

This domain covers the developer experience: IDE integration, Repos, CLI, REST API, Databricks Connect, and Asset Bundles.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Databricks Repos | Git integration, branch management, pull requests, .gitignore patterns | Module 06 |
| Databricks CLI | Installation, authentication (PAT, OAuth), workspace commands, jobs commands, cluster commands | Module 06 |
| REST API | Jobs API (create, run, list), Clusters API, DBFS API, common endpoints and response formats | Module 06 |
| Databricks Connect | Running Spark code from local IDE against remote cluster; setup and limitations | Module 06 |
| Asset Bundles (DABs) | Infrastructure-as-code for Databricks resources; `databricks.yml` configuration; deployment targets | Module 06 |
| Secrets | `dbutils.secrets.get()`; secret scopes; integrating with key vaults | Module 08 |
| Notebook workflows | `dbutils.notebook.run()`; passing parameters; return values; limitations vs. Jobs | Module 06 |
| Widgets | `dbutils.widgets.text()`, `dropdown()`, `combobox()`, `multiselect()`; parameterizing notebooks | Module 06 |

### Advanced Concepts

- **Databricks Connect** allows running Spark code from VS Code, PyCharm, or any Python environment against a remote Databricks cluster. The compute happens on the cluster, not locally. Limitations include no support for Structured Streaming, no RDD operations, and no custom JVM code.
- **Asset Bundles (DABs)** define jobs, pipelines, and resources in YAML files that are version-controlled. They use `databricks bundle deploy` to deploy and `databricks bundle run` to execute. This replaces older approaches using the REST API or Terraform for Databricks resource management.
- **REST API pagination**: The Jobs API returns paginated results. You must handle `has_more` and `next_page_token` in your API client code.
- **Secret scopes** backed by Databricks or Azure Key Vault / AWS Secrets Manager. Secrets are redacted in notebook output (displayed as `[REDACTED]`).

### Common Gotchas

- Databricks Connect does NOT support Structured Streaming -- this is a frequent exam trap.
- `dbutils.notebook.run()` runs a child notebook but is limited to returning a single string value. For complex orchestration, use Databricks Jobs.
- CLI and API authentication requires a personal access token (PAT) or OAuth M2M token -- know the difference and when each is appropriate.

---

## Domain 2: Data Processing (30%)

**Approximate questions: 18 out of 60**

This is the highest-weight domain. It covers complex transformations, UDFs, performance tuning, and advanced streaming.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Complex joins | Broadcast joins, skew joins, range joins, self-joins | Module 04, 05 |
| Broadcast joins | Small table (<10MB default) broadcast to all executors, avoiding shuffle; `broadcast()` hint | Module 05 |
| Adaptive Query Execution (AQE) | Dynamic partition coalescing, broadcast join conversion, skew join optimization | Module 05 |
| Photon engine | C++ vectorized execution engine; faster for SQL/DataFrame; uses more DBUs but net cost often lower | Module 05 |
| Window functions | Advanced frames (ROWS BETWEEN, RANGE BETWEEN), running totals, session windows | Module 04 |
| UDFs | Python UDFs, Pandas UDFs (vectorized), performance differences; SQL UDFs | Module 04 |
| Higher-order functions | `transform()`, `filter()`, `aggregate()`, `exists()` on arrays | Module 04 |
| Complex types | Arrays, maps, structs; nested data operations; `explode`, `collect_list`, `collect_set` | Module 04 |
| Streaming with watermarks | `withWatermark()` to control state growth; late data handling | Module 07 |
| Streaming joins | Stream-stream joins (require watermarks on both sides); stream-static joins | Module 07 |
| foreachBatch | Custom sink logic; writing to multiple tables from a single stream | Module 07 |
| Idempotent writes | Ensuring exactly-once semantics in batch and streaming pipelines | Module 07 |

### Advanced Concepts

- **Broadcast join threshold**: Controlled by `spark.sql.autoBroadcastJoinThreshold` (default 10MB). Tables below this size are automatically broadcast. Use `broadcast(df)` to force broadcast regardless of size.
- **AQE** (enabled by default in DBR 13+) dynamically optimizes at runtime:
  - Coalesces small post-shuffle partitions (reduces task overhead)
  - Converts sort-merge joins to broadcast hash joins when one side is small after filtering
  - Handles skewed partitions by splitting them into smaller tasks
- **Photon** is Databricks' native vectorized query engine written in C++. It accelerates SQL and DataFrame operations (especially scans, filters, aggregations, and joins). It uses more DBUs per hour but often reduces total cost because queries finish faster.
- **Pandas UDFs** (vectorized UDFs) are 10-100x faster than row-at-a-time Python UDFs because they use Apache Arrow for data transfer and operate on Pandas Series/DataFrames.
- **Watermarks** define how long the system waits for late data. Without watermarks, stateful streaming operations (aggregations, joins) keep ALL state forever, which can cause out-of-memory errors.

### Common Gotchas

- A Python UDF forces data serialization from JVM to Python and back -- always prefer built-in functions or Pandas UDFs.
- `foreachBatch` receives a static DataFrame, not a streaming one -- you can use any DataFrame operation inside it, including MERGE.
- Stream-stream joins require watermarks on BOTH sides; stream-static joins do not need watermarks.
- `trigger(availableNow=True)` is preferred over `trigger(once=True)` because it processes all data in multiple micro-batches rather than one.

---

## Domain 3: Data Modeling (20%)

**Approximate questions: 12 out of 60**

This domain covers star schema design, slowly changing dimensions, medallion architecture, and Delta optimization.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Star schema | Fact tables (events/metrics), dimension tables (descriptive attributes), surrogate keys | Module 04 |
| Snowflake schema | Normalized dimensions; when to use vs. star schema | Module 04 |
| SCD Type 1 | Overwrite old value with new; no history maintained | Module 04 |
| SCD Type 2 | Maintain full history with `is_current`, `effective_date`, `end_date` columns; implemented with MERGE | Module 04 |
| Medallion architecture | Bronze (raw), Silver (cleansed), Gold (aggregated); incremental processing between layers | Module 03 |
| Z-ORDER | Co-locates related data on disk for better data skipping; specify high-cardinality filter columns | Module 05 |
| Liquid Clustering | Replaces partitioning + Z-ordering; automatic optimization; `CLUSTER BY` clause | Module 05 |
| Partitioning | Hash/range partitioning in files; when to partition (high-cardinality key with common filters) | Module 05 |
| OPTIMIZE | Compacts small files into larger ones; reduces metadata overhead; can be scheduled | Module 05 |
| VACUUM | Removes old data files no longer referenced by the transaction log; default retention 7 days | Module 03 |

### Advanced Concepts

- **SCD Type 2 with MERGE**: The pattern uses `MERGE INTO` with conditions that close the current record (set `is_current = false`, `end_date = current_date`) when a change is detected, and then insert a new record with `is_current = true`. This requires a carefully constructed MERGE with `WHEN MATCHED AND source.value != target.value THEN UPDATE` plus `WHEN NOT MATCHED THEN INSERT`.
- **Z-ORDER** is most effective on columns used frequently in `WHERE` clauses with high cardinality. Do NOT Z-ORDER on low-cardinality columns (like boolean flags). Z-ORDER runs during `OPTIMIZE`.
- **Liquid Clustering** is the newer approach that replaces manual partitioning and Z-ORDER. It uses `ALTER TABLE ... CLUSTER BY (col1, col2)` and automatically reorganizes data during writes. It supports incremental clustering, meaning only new/modified data is re-clustered.
- **VACUUM** removes files older than the retention period (default 168 hours = 7 days). Running `VACUUM` with a retention shorter than 7 days requires setting `spark.databricks.delta.retentionDurationCheck.enabled = false` (not recommended for production). After VACUUM, time travel to versions older than the retention period will fail.

### Common Gotchas

- Z-ORDER and Liquid Clustering serve similar purposes, but Liquid Clustering is the newer, recommended approach. The exam may test when each is appropriate.
- Partitioning on a high-cardinality column (e.g., user_id) creates too many small files -- this is an anti-pattern. Partition on low-cardinality columns (e.g., date, region) with at least 1 GB per partition.
- VACUUM and time travel interact: if you VACUUM with retention = 0 hours, you lose all time travel capability.
- `OPTIMIZE` does not delete old files immediately -- it creates new compacted files and marks old ones for deletion. `VACUUM` actually removes the old files.

---

## Domain 4: Security and Governance (10%)

**Approximate questions: 6 out of 60**

This domain covers Unity Catalog, access control, encryption, and PII handling.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Unity Catalog | Three-level namespace, metastore, data lineage, table access control | Module 08 |
| Row-level security | Dynamic views with `current_user()`, `is_member()`; row filters | Module 08 |
| Column masking | Dynamic views or column masks that hide sensitive data from unauthorized users | Module 08 |
| Encryption | Encryption at rest (default in cloud storage); encryption in transit (TLS); customer-managed keys | Module 08 |
| PII handling | Tokenization, hashing, masking strategies; GDPR right to deletion | Module 08 |
| Service principals | Non-human identities for automation; used in CI/CD pipelines and scheduled jobs | Module 08 |
| Audit logging | Unity Catalog audit logs; tracking who accessed what data and when | Module 08 |
| External locations | Unity Catalog external locations and storage credentials; managing access to cloud storage | Module 08 |

### Advanced Concepts

- **Service principals** are identities used for automated processes (CI/CD, scheduled jobs). They authenticate using OAuth M2M tokens or personal access tokens. Best practice: use service principals (not personal accounts) for production workloads.
- **Column masks** (Unity Catalog feature) allow you to define a function that is automatically applied when a user queries a column. Different from dynamic views because the mask is applied transparently without requiring users to query a specific view.
- **Row filters** (Unity Catalog feature) transparently filter rows based on the querying user's identity. Like column masks, they are applied at the table level rather than requiring a view.
- **Data lineage** in Unity Catalog automatically tracks the flow of data from source to destination, including which notebooks and jobs processed the data. This is useful for impact analysis and compliance.

### Common Gotchas

- USAGE privilege must be granted at BOTH the catalog and schema level before table-level grants take effect.
- External locations require a storage credential to be configured first.
- Audit logs capture read access, but there is a delay (not real-time).

---

## Domain 5: Monitoring and Logging (10%)

**Approximate questions: 6 out of 60**

This domain covers Spark UI, alerting, cost management, and observability.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Spark UI | Jobs, stages, tasks, DAG visualization; identifying bottlenecks | Module 05, 09 |
| Query profiles | SQL query execution plans; understanding scan, filter, join, and aggregate operators | Module 05 |
| Ganglia metrics | Cluster-level CPU, memory, network metrics; identifying resource contention | Module 09 |
| Alerting | Job failure alerts (email, webhook, PagerDuty); SQL alert conditions | Module 09 |
| Cost management | Job clusters + spot instances for savings; cluster policies to limit resources; DBU tracking | Module 05, 09 |
| Streaming monitoring | Streaming query progress; processing rate; batch duration; latency metrics | Module 07, 09 |
| Log4j configuration | Custom log levels; directing logs to cloud storage; structured logging | Module 09 |

### Advanced Concepts

- **Spark UI interpretation**: The Stages tab shows shuffle read/write sizes, task duration distribution, and skew. If one task takes 10x longer than others, you have data skew. The Storage tab shows cached RDDs/DataFrames. The SQL tab shows physical plans with metrics.
- **Cost optimization strategies**:
  - Use job clusters instead of all-purpose clusters (~30-50% savings on DBU rates)
  - Use spot/preemptible instances for workers (~60-70% savings on cloud compute)
  - Right-size clusters using autoscaling (min to max workers)
  - Use cluster policies to prevent over-provisioning
  - Schedule OPTIMIZE and VACUUM to reduce storage costs
  - Combined: job clusters + spot instances can save ~70% compared to all-purpose on-demand
- **Streaming metrics**: `query.lastProgress` returns a JSON object with `inputRowsPerSecond`, `processedRowsPerSecond`, `batchDuration`, and `stateOperators` (state size, rows dropped by watermark).

### Common Gotchas

- Spark UI metrics are per-job, not per-query. A single Spark action may create multiple jobs.
- Spot instances can be reclaimed by the cloud provider -- do not use them for the driver node, only workers.
- Streaming metrics show "rows per second" but this includes overhead -- actual throughput may differ.

---

## Domain 6: Testing and Deployment (10%)

**Approximate questions: 6 out of 60**

This domain covers CI/CD, testing frameworks, and deployment patterns.

### Key Topics

| Topic | What to Know | Course Module |
|-------|-------------|---------------|
| Unit testing | Testing pure Python functions; mocking SparkSession; pytest patterns | Module 09 |
| Integration testing | Testing with real Spark (local mode); testing Delta operations end-to-end | Module 09 |
| CI/CD pipelines | GitHub Actions, Azure DevOps, Jenkins; linting, testing, deployment stages | Module 06 |
| Asset Bundles | Infrastructure-as-code; `databricks.yml` for defining jobs, pipelines, dashboards | Module 06 |
| Deployment patterns | Blue/green deployment; canary deployment; feature flags; rollback strategies | Module 06 |
| Environment promotion | Dev -> Staging -> Production; using the same code with different configurations | Module 06 |
| Nutter framework | Databricks testing framework for notebooks; `NutterFixture` class | Module 09 |
| Code review | Best practices for reviewing Spark code; common performance anti-patterns | Module 09 |

### Advanced Concepts

- **Testing pyramid for data engineering**: Unit tests (fast, many) for pure functions -> Integration tests (medium speed, some) for Spark operations -> End-to-end tests (slow, few) for full pipeline runs.
- **Mocking SparkSession**: For unit testing helper functions that use Spark, you can either mock the SparkSession or use a local SparkSession. Mocking is faster but less realistic.
- **Asset Bundles deployment**: `databricks bundle validate` checks configuration, `databricks bundle deploy` pushes to workspace, `databricks bundle run` executes resources. Targets (dev, staging, prod) use different configurations from the same codebase.
- **Environment promotion pattern**: Code is promoted through environments (dev -> staging -> prod) using the same artifacts but different configurations (cluster sizes, table names, notification channels).

### Common Gotchas

- Notebook tests (using Nutter or similar) run on a Databricks cluster and are slower than local tests -- use them for integration testing, not unit testing.
- Asset Bundles and Terraform serve different purposes: DABs are for Databricks-specific resources, Terraform is for broader cloud infrastructure.
- Testing streaming pipelines is harder than batch -- consider using `trigger(availableNow=True)` in test environments to process all data and then verify.

---

## Exam Strategy

### Time Management

- You have 120 minutes for 60 questions = 2 minutes per question.
- Flag scenario-based questions that require careful reading and come back to them.
- Aim to complete your first pass in 90 minutes, leaving 30 minutes for review.

### Professional-Level Question Approach

1. **Read the full scenario** -- Professional questions often have 3-4 sentence setups with important details.
2. **Identify what is being asked**: "Best practice", "most efficient", "would cause an error", or "root cause".
3. **Eliminate the clearly wrong answer first** -- there is usually one obviously incorrect option.
4. **Choose the Databricks-recommended approach** when multiple options work but one aligns with best practices.
5. **Consider cost and performance together** -- the "best" answer often balances both.

### What Distinguishes a Professional Answer

- Associate: "Use a broadcast join for small tables."
- Professional: "Use a broadcast join when one table is under 10MB (or adjust the threshold), and consider AQE's automatic broadcast conversion for dynamically small tables post-filter."

---

## Quick Reference: Domain Weight Summary

| Domain | Weight | Key Focus Areas |
|--------|--------|----------------|
| Data Processing | 30% | Complex transforms, UDFs, streaming, performance |
| Databricks Tooling | 20% | CLI, API, Repos, Connect, Asset Bundles |
| Data Modeling | 20% | Star schema, SCD, medallion, Delta optimization |
| Security & Governance | 10% | Unity Catalog, row/column security, PII |
| Monitoring & Logging | 10% | Spark UI, cost management, streaming metrics |
| Testing & Deployment | 10% | CI/CD, testing patterns, environment promotion |

---

## Recommended Study Order (by domain weight and difficulty)

1. **Domain 2 -- Data Processing (30%)**: Highest weight. Master complex joins, UDFs, streaming with watermarks, and AQE.
2. **Domain 3 -- Data Modeling (20%)**: Study SCD Type 2, Z-ORDER vs. Liquid Clustering, OPTIMIZE/VACUUM.
3. **Domain 1 -- Databricks Tooling (20%)**: Practice CLI commands, understand REST API patterns, know Asset Bundles.
4. **Domains 4, 5, 6 (10% each)**: Study these together -- security, monitoring, and testing are often interrelated.

---

## Next Steps

After reviewing this guide:
1. Work through the companion notebook (`03-professional-exam-guide_notebook.py`) for advanced hands-on exercises.
2. Take the 50 practice questions in Topic 04 under timed conditions.
3. Score yourself and identify weak domains.
4. Review the Study Plan (Topic 05) for a structured preparation schedule.
