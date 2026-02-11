# Study Plan and Exam Tips

> Module 11 -- Topic 05 | Level: All Levels | Time: 30 min (read), ongoing (execution)

## Learning Objectives

- Follow a structured study plan for Associate (4 weeks) or Professional (8 weeks) certification
- Review a compact cheat sheet of key concepts from every module (00-09)
- Avoid the most common mistakes and misconceptions on the exam
- Apply effective strategies on exam day for timing, question approach, and confidence

---

## Part 1: Four-Week Associate Study Schedule

This plan assumes 1-2 hours of study per weekday and 2-3 hours on weekends.

### Week 1: Foundations and Spark Core

| Day | Topics | Resources |
|-----|--------|-----------|
| Mon | Databricks workspace navigation, notebooks, clusters, DBFS | Module 00 |
| Tue | SparkSession, transformations vs actions, lazy evaluation | Module 01 |
| Wed | DataFrames: select, filter, withColumn, drop, alias | Module 01 |
| Thu | Spark SQL: SELECT, WHERE, GROUP BY, HAVING, ORDER BY | Module 01 |
| Fri | Reading data: CSV, JSON, Parquet options; write modes | Module 02 |
| Sat | Reading data: schemas (inferred vs explicit), format options | Module 02 |
| Sun | Review Week 1; run Module 01 and 02 notebooks hands-on | All Week 1 |

### Week 2: Delta Lake and Transformations

| Day | Topics | Resources |
|-----|--------|-----------|
| Mon | Delta Lake: ACID transactions, _delta_log, schema enforcement | Module 03 |
| Tue | Delta: time travel (VERSION AS OF, TIMESTAMP AS OF), RESTORE | Module 03 |
| Wed | Delta: OPTIMIZE, VACUUM, retention, schema evolution | Module 03 |
| Thu | Medallion architecture: Bronze, Silver, Gold layers | Module 03 |
| Fri | Joins: inner, left, right, full, semi, anti; join syntax | Module 04 |
| Sat | Window functions: row_number, rank, lag, lead, frames | Module 04 |
| Sun | Aggregations, complex types (arrays, structs, maps), null handling | Module 04 |

### Week 3: Incremental Processing and Production

| Day | Topics | Resources |
|-----|--------|-----------|
| Mon | Structured Streaming: readStream, writeStream, output modes | Module 07 |
| Tue | Triggers: processingTime, once, availableNow; checkpointing | Module 07 |
| Wed | Auto Loader (cloudFiles), COPY INTO, schema evolution | Module 02, 07 |
| Thu | Delta Live Tables: LIVE tables, expectations (EXPECT, DROP, FAIL) | Module 07 |
| Fri | Jobs: scheduling, multi-task, job clusters, task values | Module 06 |
| Sat | Repos, Git integration, notebook workflows | Module 06 |
| Sun | Review Week 3; run Module 06 and 07 notebooks hands-on | All Week 3 |

### Week 4: Governance, Review, and Practice Exam

| Day | Topics | Resources |
|-----|--------|-----------|
| Mon | Unity Catalog: three-level namespace, GRANT/REVOKE, USAGE | Module 08 |
| Tue | Dynamic views, managed vs external tables, data lineage | Module 08 |
| Wed | Full practice exam (40 questions, timed at 90 minutes) | Topic 02 |
| Thu | Review incorrect answers; revisit weak modules | Topic 02 answers |
| Fri | Re-do Topic 02 practice notebook exercises | Topic 02 notebook |
| Sat | Second practice exam attempt; target improvement | Topic 02 |
| Sun | Final review: cheat sheet below, exam day tips | This guide |

---

## Part 2: Eight-Week Professional Study Schedule

This plan assumes 1-2 hours of study per weekday and 2-3 hours on weekends. Weeks 1-4 overlap with Associate content but go deeper.

### Weeks 1-2: Core Foundations (Deeper Review)

| Week | Focus | Key Topics |
|------|-------|-----------|
| 1 | Spark internals and Delta Lake | Catalyst optimizer, execution plans, partitioning strategies, Delta internals (_delta_log, checkpoints), OPTIMIZE, VACUUM + time travel interaction |
| 2 | Advanced transformations | Complex joins (broadcast, skew), window frames (ROWS BETWEEN, RANGE BETWEEN), higher-order functions (transform, filter, aggregate), UDFs vs Pandas UDFs |

### Weeks 3-4: Streaming and Data Modeling

| Week | Focus | Key Topics |
|------|-------|-----------|
| 3 | Streaming deep dive | Watermarks, stream-stream joins, stream-static joins, foreachBatch with MERGE, trigger modes (availableNow vs once), output modes, state management |
| 4 | Data modeling patterns | Star schema design, SCD Type 1 and Type 2 with MERGE, medallion architecture (incremental processing between layers), Z-ORDER vs Liquid Clustering |

### Weeks 5-6: Tooling and Performance

| Week | Focus | Key Topics |
|------|-------|-----------|
| 5 | Databricks tooling | CLI commands, REST API (Jobs API, Clusters API), Databricks Connect limitations, Asset Bundles (databricks.yml, targets, deploy/run), widgets, secret scopes |
| 6 | Performance tuning | AQE (coalescing, broadcast conversion, skew handling), Photon engine, broadcast join threshold, execution plan analysis (Spark UI: stages, tasks, shuffle, spill) |

### Weeks 7-8: Security, Testing, and Practice Exams

| Week | Focus | Key Topics |
|------|-------|-----------|
| 7 | Security, monitoring, testing | Unity Catalog (USAGE grants, column masks, row filters), service principals, cost optimization (job clusters + spot), streaming metrics, testing pyramid, CI/CD |
| 8 | Practice and review | Take 50-question practice exam (Topic 04) timed; review incorrect answers; re-do Topic 04 notebook; retake exam; final review of cheat sheet and weak areas |

---

## Part 3: Key Concepts Cheat Sheet

### Module 00: Environment Setup

- Databricks workspace has: Workspace (notebooks), Repos (Git), Catalog (Unity), Compute (clusters)
- Cluster types: All-purpose (interactive, higher DBU), Job (ephemeral, lower DBU), SQL warehouse (SQL queries)
- DBFS: distributed file system abstraction over cloud storage (S3, ADLS, GCS)
- Magic commands: `%sql`, `%python`, `%scala`, `%r`, `%md`, `%run`, `%fs`

### Module 01: Spark Foundations

- SparkSession is the unified entry point (`spark` variable in notebooks)
- Transformations are lazy (build a plan); Actions trigger execution (show, count, collect, write)
- DataFrame is immutable -- operations return a new DataFrame
- `explain(True)` shows full plan: parsed -> analyzed -> optimized -> physical
- Catalyst optimizer: predicate pushdown, projection pruning, constant folding

### Module 02: Data Ingestion

- Read: `spark.read.format("csv").option("header", "true").load(path)`
- Write modes: `overwrite`, `append`, `ignore`, `errorIfExists` (default)
- Schema: inferred (`inferSchema=true`) or explicit (`StructType/StructField`)
- Auto Loader: `format("cloudFiles")`, `.option("cloudFiles.format", "json")`
- COPY INTO: simple batch ingestion, less scalable than Auto Loader
- Schema evolution: `cloudFiles.schemaEvolutionMode` + `mergeSchema` on write side

### Module 03: Delta Lake

- ACID transactions via `_delta_log` (JSON + checkpoint Parquet files)
- Schema enforcement: rejects writes with mismatched schemas
- Schema evolution: `.option("mergeSchema", "true")` or `ALTER TABLE ADD COLUMNS`
- Time travel: `VERSION AS OF n`, `TIMESTAMP AS OF t`, `RESTORE TABLE TO VERSION AS OF n`
- OPTIMIZE: compacts small files; VACUUM: removes old files (default retention = 7 days)
- VACUUM + time travel: after VACUUM, cannot time travel beyond retention period
- Managed table: drop deletes data; External table: drop keeps data

### Module 04: Transformations

- `withColumn`, `select`, `filter`, `groupBy`, `agg`, `join`, `orderBy`
- Joins: inner, left, right, full, cross, semi (rows with match, left cols only), anti (rows without match)
- Window functions: `row_number`, `rank`, `dense_rank`, `lag`, `lead`, `ntile`
- Window frames: `rowsBetween(unboundedPreceding, currentRow)` for running totals
- `when/otherwise` for conditional logic (equivalent to SQL CASE WHEN)
- `coalesce(col_a, col_b, lit("default"))` returns first non-null
- `explode` flattens arrays; `collect_list`/`collect_set` aggregates into arrays
- Higher-order functions: `transform`, `filter`, `aggregate` operate on arrays in-place

### Module 05: Performance Tuning

- AQE: dynamic partition coalescing, broadcast conversion, skew handling (DBR 13+ default)
- Broadcast joins: `broadcast(df)` hint; threshold: `spark.sql.autoBroadcastJoinThreshold` (10MB)
- Photon: C++ vectorized engine; faster for SQL/DataFrame; more DBUs but lower net cost
- Z-ORDER: co-locates data by column values for data skipping; runs during OPTIMIZE
- Liquid Clustering: replaces partitioning + Z-ORDER; incremental; `CLUSTER BY` clause
- Partitioning: use low-cardinality columns; target >= 1 GB per partition
- `cache()` = `persist(MEMORY_AND_DISK)`; `persist()` allows custom storage levels
- Shuffle: most expensive operation; minimize by filtering early, using broadcast joins

### Module 06: Databricks Workflows

- Jobs: single-task or multi-task; job clusters (ephemeral) vs all-purpose
- Scheduling: cron expressions (e.g., `0 0 8 ? * MON-FRI`)
- Task values: `dbutils.jobs.taskValues.set/get` for passing small values between tasks
- `dbutils.notebook.run()`: returns a single string; same cluster; limited orchestration
- Widgets: `text`, `dropdown`, `combobox`, `multiselect`; parameterize notebooks
- Repos: Git integration; branch, commit, push/pull from Databricks
- Databricks Connect: local IDE -> remote cluster; no streaming, no RDD, no custom JVM
- Asset Bundles: `databricks.yml`; `databricks bundle validate/deploy/run`; targets for env promotion

### Module 07: Streaming

- `spark.readStream.format(...).load()` -> `df.writeStream.format(...).start()`
- Output modes: append (default), complete (full results), update (changed rows only)
- Triggers: default (continuous micro-batch), `processingTime`, `once`, `availableNow` (preferred)
- Checkpoints: stores offsets and state; unique per stream; enables exactly-once
- Watermarks: `withWatermark("event_time", "10 minutes")` bounds state; drops late data
- Stream-stream joins: require watermarks on BOTH sides
- Stream-static joins: no watermarks needed; static side re-read each micro-batch
- `foreachBatch`: receives a static DataFrame per micro-batch; supports MERGE, multi-sink
- DLT expectations: `EXPECT` (warn), `EXPECT OR DROP` (filter), `EXPECT OR FAIL` (halt)

### Module 08: Security and Governance

- Unity Catalog: `catalog.schema.table` (three-level namespace)
- USAGE must be granted at catalog AND schema level before table grants work
- GRANT SELECT/MODIFY/ALL PRIVILEGES ON TABLE ... TO group
- Dynamic views: use `current_user()`, `is_member()` for row/column security
- Column masks: transparent function applied on read; no view required
- Row filters: transparent predicate applied on read; no view required
- Service principals: non-human identities; OAuth M2M tokens preferred for production
- Secrets: `dbutils.secrets.get(scope, key)`; output is auto-redacted as `[REDACTED]`

### Module 09: Production and Testing

- Testing pyramid: many unit tests (pure Python) > fewer integration tests (local Spark) > few E2E tests (Databricks)
- Cost optimization: job clusters + spot instances can save ~70% vs all-purpose on-demand
- Cluster policies: limit instance types, max workers, auto-termination
- Spark UI: Jobs -> Stages -> Tasks; look for shuffle spill, skew, long-running tasks
- Streaming metrics: `query.lastProgress` -> inputRowsPerSecond, processedRowsPerSecond, stateOperators
- CI/CD: lint -> test -> deploy; Asset Bundles for deployment; env promotion (dev -> staging -> prod)

---

## Part 4: Common Mistakes and Misconceptions

### 1. Confusing transformations and actions
Transformations (filter, select, groupBy) are lazy. Actions (count, show, collect, write) trigger execution. A pipeline that only calls transformations does nothing until an action is called.

### 2. Thinking DataFrames are mutable
Every operation on a DataFrame returns a NEW DataFrame. `df.withColumn(...)` does not modify `df`; you must assign the result: `df = df.withColumn(...)`.

### 3. Using VACUUM too aggressively
Running `VACUUM RETAIN 0 HOURS` destroys all time travel capability. Always keep at least 7 days (the default) unless there is a specific reason.

### 4. Forgetting USAGE grants in Unity Catalog
Granting `SELECT` on a table is not enough. Users also need `USAGE` on the parent catalog and schema. This is the most common Unity Catalog issue on the exam.

### 5. Confusing trigger(once) with trigger(availableNow)
`trigger(once=True)` processes all data in a SINGLE micro-batch. `trigger(availableNow=True)` processes all data across MULTIPLE micro-batches and then stops. The latter is preferred for large backlogs.

### 6. Sharing checkpoint locations between streams
Each streaming query must have its own unique checkpoint location. Sharing checkpoints causes state corruption and incorrect results.

### 7. Not using watermarks for stateful streaming
Without watermarks, stateful operations (aggregations, stream-stream joins) keep all state forever, eventually causing OOM errors.

### 8. Using Python UDFs when built-in functions exist
Python UDFs are 10-100x slower than built-in functions due to per-row serialization. Always prefer built-in functions. If you must use a UDF, use a Pandas UDF.

### 9. Partitioning on high-cardinality columns
Partitioning on columns like `user_id` (millions of distinct values) creates millions of tiny files. Partition on low-cardinality columns (date, region) and use Z-ORDER or Liquid Clustering for high-cardinality columns.

### 10. Confusing OPTIMIZE and VACUUM
OPTIMIZE compacts small files into larger ones but does not delete old files. VACUUM deletes old files after the retention period. They serve different purposes and should both be scheduled.

### 11. Thinking Databricks Connect supports everything
Databricks Connect does NOT support Structured Streaming, RDD operations, or custom JVM code. This is a common exam trap.

### 12. Confusing output modes in streaming
`append` only writes new rows (default). `complete` rewrites the full result (required for aggregations without watermarks). `update` writes only changed rows. Using the wrong mode causes errors or incorrect results.

### 13. Forgetting that MERGE INTO is atomic
A MERGE statement atomically handles INSERT, UPDATE, and DELETE in a single transaction. You do not need to wrap it in a try/except for atomicity.

### 14. Confusing managed and external tables
Dropping a managed table deletes both metadata AND data files. Dropping an external table deletes only metadata; data files remain.

### 15. Ignoring the cost difference between cluster types
Job clusters have a ~30-50% lower DBU rate than all-purpose clusters. Combined with spot instances (~60-70% savings on cloud compute), job clusters + spot can save ~70% compared to all-purpose on-demand.

### 16. Misunderstanding schema enforcement vs evolution
Schema enforcement (default) REJECTS writes that do not match the existing schema. Schema evolution ALLOWS adding new columns. They serve opposite purposes and must be configured explicitly.

### 17. Not caching DataFrames used multiple times in foreachBatch
Inside `foreachBatch`, each action triggers a full recomputation of the micro-batch DataFrame. Call `batch_df.cache()` at the start and `batch_df.unpersist()` at the end if performing multiple actions.

### 18. Confusing Z-ORDER and Liquid Clustering
Z-ORDER requires running `OPTIMIZE ... ZORDER BY` and processes entire files. Liquid Clustering uses `CLUSTER BY` and incrementally clusters new data. Liquid Clustering is the newer, recommended approach.

---

## Part 5: Exam Day Tips

### Before the Exam

1. **Confirm your setup**: Ensure your webcam, microphone, and internet connection work. Online proctoring requires a stable connection.
2. **Clear your desk**: Proctored exams require a clean workspace with no notes, books, or additional monitors.
3. **Close other applications**: The proctoring software may flag background applications.
4. **Have your ID ready**: Government-issued photo ID is required for identity verification.
5. **Start 15 minutes early**: Check-in can take 5-10 minutes. Do not cut it close.

### During the Exam

1. **First pass (60-70% of time)**: Answer all questions you are confident about. Flag uncertain ones.
2. **Second pass (30-40% of time)**: Return to flagged questions with fresh eyes.
3. **Read every word**: Professional questions have long scenarios -- critical details are often in the last sentence.
4. **Eliminate wrong answers**: Usually one option is clearly wrong. Narrow to 2-3 choices.
5. **When two answers seem correct**: Choose the one that aligns with Databricks best practices, not just a technically valid approach.
6. **Do not change answers**: Unless you find a clear error in your reasoning, your first instinct is usually correct.
7. **Manage your clock**:
   - Associate: 45 questions / 90 minutes = 2 min per question
   - Professional: 60 questions / 120 minutes = 2 min per question
8. **Never leave a question blank**: There is no penalty for guessing. Eliminate what you can and guess.

### Time Allocation Strategy

| Phase | Associate (90 min) | Professional (120 min) |
|-------|-------------------|----------------------|
| First pass | 60 minutes | 80 minutes |
| Review flagged | 20 minutes | 30 minutes |
| Final review | 10 minutes | 10 minutes |

### Mental Shortcuts for Common Question Types

- **"Which is the BEST approach?"** -> Choose the Databricks-recommended practice, not just a valid option.
- **"What is the MOST LIKELY cause?"** -> Eliminate edge cases; choose the most common root cause.
- **"What happens when...?"** -> Trace through the operation step by step.
- **"Which configuration...?"** -> Know the exact property name and default value.
- **"Which of the following is TRUE?"** -> Treat it as "find the false statements" -- it is often easier to eliminate.

---

## Part 6: Resources

### Official Databricks Documentation

- [Databricks Certification Overview](https://www.databricks.com/learn/certification)
- [Associate Data Engineer Exam Guide (official)](https://www.databricks.com/learn/certification/data-engineer-associate)
- [Professional Data Engineer Exam Guide (official)](https://www.databricks.com/learn/certification/data-engineer-professional)
- [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
- [Structured Streaming Guide](https://docs.databricks.com/structured-streaming/index.html)
- [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
- [Databricks CLI Documentation](https://docs.databricks.com/dev-tools/cli/index.html)
- [Asset Bundles Documentation](https://docs.databricks.com/dev-tools/bundles/index.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)

### Databricks Academy (Free Courses)

- Data Engineering with Databricks (self-paced)
- Apache Spark Programming with Databricks
- Advanced Data Engineering with Databricks (for Professional)
- Databricks Lakehouse Fundamentals

### Apache Spark Documentation

- [Spark SQL Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)
- [Structured Streaming Programming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [PySpark API Reference](https://spark.apache.org/docs/latest/api/python/index.html)

### Community Resources

- [Databricks Community Edition](https://community.cloud.databricks.com/) -- Free cluster for practice
- [Databricks Blog](https://www.databricks.com/blog) -- Deep dives on features and best practices
- [Stack Overflow: databricks tag](https://stackoverflow.com/questions/tagged/databricks)
- [r/databricks subreddit](https://www.reddit.com/r/databricks/)
- [Databricks Community Forum](https://community.databricks.com/)

### Books

- *Learning Spark, 2nd Edition* (O'Reilly) -- Comprehensive Spark fundamentals
- *Spark: The Definitive Guide* (O'Reilly) -- Deep dive into Spark internals
- *Delta Lake: The Definitive Guide* (O'Reilly) -- Delta Lake architecture and best practices

---

## Final Checklist: Am I Ready?

### Associate Readiness Check

- [ ] I can explain what a lakehouse is and how it differs from a data warehouse and data lake
- [ ] I can write PySpark code to read, transform, and write data
- [ ] I understand Delta Lake: ACID, time travel, schema enforcement, OPTIMIZE, VACUUM
- [ ] I can write MERGE statements for upserts
- [ ] I know the difference between Structured Streaming triggers and output modes
- [ ] I understand Auto Loader and COPY INTO
- [ ] I can explain DLT expectations (EXPECT, EXPECT OR DROP, EXPECT OR FAIL)
- [ ] I know job cluster vs all-purpose cluster trade-offs
- [ ] I understand Unity Catalog: namespace hierarchy, GRANT/REVOKE, USAGE
- [ ] I scored 70%+ on the practice exam (Topic 02)

### Professional Readiness Check

- [ ] All Associate items above, plus:
- [ ] I can explain AQE's three optimizations and when each applies
- [ ] I know broadcast join threshold configuration (static and AQE)
- [ ] I can implement SCD Type 2 with MERGE
- [ ] I understand watermarks and their role in streaming state management
- [ ] I know stream-stream vs stream-static join requirements
- [ ] I can explain Databricks Connect limitations
- [ ] I can describe Asset Bundle structure (databricks.yml, targets)
- [ ] I understand the testing pyramid for data engineering
- [ ] I know cost optimization strategies (job clusters, spot, cluster policies)
- [ ] I can read and interpret Spark execution plans
- [ ] I scored 70%+ on the practice exam (Topic 04)

---

## Next Steps

1. Follow the study schedule appropriate for your certification level.
2. Work through the companion cheat sheet notebook for quick-reference code patterns.
3. Take the practice exams under timed conditions.
4. Schedule your exam when you consistently score 80%+ on practice exams (giving yourself a margin above the 70% passing score).
5. Good luck!
