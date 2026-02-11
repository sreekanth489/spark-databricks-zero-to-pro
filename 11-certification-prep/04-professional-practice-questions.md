# Professional Data Engineer Practice Questions

> Module 11 -- Topic 04 | Level: Advanced | Time: 150 min

## Learning Objectives

- Test your knowledge across all six Professional exam domains
- Practice answering scenario-based questions that test design decisions and trade-offs
- Identify weak areas that require deeper study before the exam
- Understand the reasoning behind correct and incorrect answers at a Professional level

## How to Use These Practice Questions

1. Set a timer for 120 minutes (matching the real exam).
2. Answer all 50 questions without looking at the answers.
3. Score yourself: each correct answer = 1 point. Passing = 35/50 (70%).
4. Read the detailed explanations for every question, even ones you got right.
5. Note which domains you scored lowest in and revisit those modules.
6. Pay special attention to the "why" in each explanation -- the Professional exam rewards understanding of trade-offs.

---

## Domain 1: Data Processing (15 questions)

### Question 1
A data engineer has a 500 GB fact table that is regularly joined with a 5 MB lookup table. The join is taking 30 minutes due to a shuffle. Which approach will most effectively reduce the join time?

A) Repartition both tables by the join key before the join
B) Use `broadcast(lookup_df)` to broadcast the small table to all executors
C) Increase the number of shuffle partitions using `spark.sql.shuffle.partitions`
D) Cache both tables in memory before the join

**Answer: B**

**Explanation:** Broadcasting the 5 MB lookup table sends a copy to every executor, eliminating the expensive shuffle of the 500 GB fact table entirely. Repartitioning (A) still requires a shuffle. Increasing shuffle partitions (C) only changes partition granularity but does not eliminate the shuffle. Caching (D) speeds up repeated reads but does not avoid the shuffle on the first join. The broadcast threshold default is 10 MB, so this table would likely be auto-broadcast, but an explicit `broadcast()` hint guarantees it. (Module 05)

---

### Question 2
A streaming pipeline uses a windowed aggregation (`groupBy(window(col("event_time"), "1 hour"))`) without a watermark. After running for two days, the pipeline crashes with an OutOfMemoryError. What is the root cause?

A) The rate source is producing data too fast for the cluster to process
B) Without a watermark, Spark keeps ALL window state in memory indefinitely, causing unbounded state growth
C) The windowed aggregation requires `outputMode("complete")` which buffers all results
D) The checkpoint directory ran out of disk space

**Answer: B**

**Explanation:** Without a watermark, Spark cannot determine when a window is "closed" and will never discard state for old windows. Over two days, the state accumulates every window partition in memory. Adding `.withWatermark("event_time", "2 hours")` tells Spark it can discard state for windows older than the watermark threshold. Output mode (C) affects output behavior but does not cause the state growth issue itself. (Module 07)

---

### Question 3
A data engineer observes that a Spark job with 200 tasks completes in 45 minutes, but the Spark UI shows that 198 tasks finish in under 2 minutes while 2 tasks take 40 minutes each. What is the most likely cause?

A) The cluster has insufficient memory allocated to executors
B) Data skew -- two partitions contain significantly more data than the others
C) The driver node is a bottleneck because it collects too much data
D) Network bandwidth between executors is saturated

**Answer: B**

**Explanation:** When a small number of tasks take dramatically longer than the rest, data skew is the most likely cause. Some partitions contain far more data than others, causing the corresponding tasks to run much longer. Solutions include AQE skew join optimization (automatic in DBR 13+), salting the join key, or repartitioning by a different key. Insufficient memory (A) would typically cause spill or OOM errors across many tasks, not just two. (Module 05)

---

### Question 4
Which configuration parameter controls whether AQE automatically converts a sort-merge join into a broadcast hash join at runtime?

A) `spark.sql.adaptive.enabled`
B) `spark.sql.adaptive.autoBroadcastJoinThreshold`
C) `spark.sql.autoBroadcastJoinThreshold`
D) `spark.sql.adaptive.localShuffleReader.enabled`

**Answer: B**

**Explanation:** `spark.sql.adaptive.autoBroadcastJoinThreshold` is the AQE-specific threshold that determines whether a join side that is discovered to be small at runtime (after filtering and aggregation) should be converted from a sort-merge join to a broadcast hash join. This is different from `spark.sql.autoBroadcastJoinThreshold` (C) which is the static compile-time threshold used during initial query planning. `spark.sql.adaptive.enabled` (A) enables/disables AQE overall. (Module 05)

---

### Question 5
A Pandas UDF processes 10 million rows in 15 seconds. A standard Python UDF performing the same logic processes 10 million rows in 12 minutes. What architectural difference explains this performance gap?

A) Pandas UDFs run on the driver node with more memory, while standard UDFs run on executors
B) Pandas UDFs use Apache Arrow for zero-copy data transfer and operate on columnar batches, avoiding per-row serialization overhead
C) Pandas UDFs are compiled to native code by the JVM, while Python UDFs are interpreted
D) Pandas UDFs bypass the Catalyst optimizer and execute directly on raw files

**Answer: B**

**Explanation:** Standard Python UDFs serialize each row from the JVM to a Python process and back, creating massive serialization overhead for large datasets. Pandas UDFs use Apache Arrow for efficient columnar data transfer (zero-copy when possible) and operate on Pandas Series/DataFrames in vectorized batches. They run on executors (not the driver), are not compiled to native code, and still go through the query plan. (Module 04, 05)

---

### Question 6
A streaming job uses `trigger(once=True)` and processes a backlog of 50 million records. The job runs for 2 hours, processing all records in a single micro-batch, and the cluster is under-utilized during the final compaction phase. What trigger mode would improve this behavior?

A) `trigger(processingTime="1 minute")`
B) `trigger(availableNow=True)`
C) `trigger(continuous="1 second")`
D) Remove the trigger to use the default micro-batch mode

**Answer: B**

**Explanation:** `trigger(availableNow=True)` processes all available data but splits it across multiple micro-batches instead of one. This improves resource utilization and allows better parallelism during processing. It also provides a clean stop after all data is processed. `trigger(once=True)` loads everything into a single micro-batch which can cause uneven resource usage. Continuous trigger (C) is for ultra-low-latency and does not stop after processing. (Module 07)

---

### Question 7
A data engineer needs to join two streaming DataFrames: one with user click events and one with ad impression events. Both have an `event_time` column. What is required for the join to succeed?

A) Only the click events stream needs a watermark
B) Both streams must have watermarks defined on their respective event time columns
C) A watermark is not needed for stream-stream joins, only for aggregations
D) The join must use `outputMode("complete")` to handle late data

**Answer: B**

**Explanation:** Stream-stream joins require watermarks on BOTH sides to allow the engine to bound the state it must maintain. Without watermarks on both streams, Spark would need to buffer all historical events from both streams indefinitely, eventually causing out-of-memory errors. Stream-static joins, by contrast, do not require watermarks. (Module 07)

---

### Question 8
A pipeline includes `foreachBatch` to write streaming micro-batches to a Delta table and also send metrics to an external monitoring system. Inside `foreachBatch`, the engineer calls `batch_df.count()` twice: once for logging and once for the monitoring payload. What is the performance issue?

A) `foreachBatch` does not support calling actions more than once
B) Each call to `count()` triggers a full re-execution of the micro-batch computation because the batch DataFrame is not cached
C) The `foreachBatch` function runs on the driver node which cannot handle two actions
D) Calling `count()` inside `foreachBatch` blocks the next micro-batch from starting

**Answer: B**

**Explanation:** Inside `foreachBatch`, the `batch_df` is a standard (non-streaming) DataFrame. Each action (like `count()`) triggers a full computation from the source. The fix is to call `batch_df.cache()` at the start of the function and `batch_df.unpersist()` at the end, so the computation is only performed once. The `foreachBatch` function does block the next micro-batch (D is partially true), but the core performance issue is the redundant computation. (Module 07)

---

### Question 9
Which Photon statement is true?

A) Photon replaces the entire Spark execution engine, including the Catalyst optimizer
B) Photon is a C++ vectorized execution engine that accelerates SQL and DataFrame operations, consuming more DBUs per hour but often reducing total cost due to faster execution
C) Photon requires code changes to use -- engineers must add `.enablePhoton()` to their queries
D) Photon only accelerates SQL queries; DataFrame API operations are not affected

**Answer: B**

**Explanation:** Photon is Databricks' native vectorized query engine written in C++. It accelerates scans, filters, aggregations, joins, and other operations for both SQL and DataFrame API workloads. It does NOT replace the Catalyst optimizer (A) -- Catalyst still plans the query; Photon executes the physical plan. It requires no code changes (C) -- it is enabled at the cluster level. It works with the DataFrame API (D), not just SQL. (Module 05)

---

### Question 10
A data engineer uses `spark.sql("SELECT * FROM my_table").explain(True)` and sees that the optimized logical plan includes a `PartitionFilters` entry. What does this indicate?

A) The query will only read partitions matching the filter, skipping irrelevant partition directories entirely
B) The query will read all data and then filter at the partition level
C) The table has too many partitions and needs to be repartitioned
D) The optimizer failed to push the filter down to the scan level

**Answer: A**

**Explanation:** `PartitionFilters` in the execution plan indicates partition pruning: Spark will only read data files from partitions that match the filter predicate. This is one of the most important performance optimizations for partitioned tables, as it eliminates I/O for irrelevant partitions entirely. If the filter appears as `DataFilters` instead, it means the filter is applied after reading (not as efficiently). (Module 05)

---

### Question 11
A data engineer has a complex query that reads from three Delta tables, applies several joins, and writes the result. The query takes 30 minutes. After enabling AQE, the same query takes 18 minutes. Which AQE feature most likely contributed to this improvement?

A) AQE cached the intermediate results between joins
B) AQE dynamically coalesced small post-shuffle partitions and converted an eligible join to broadcast, reducing shuffle overhead
C) AQE rewrote the SQL query to use more efficient syntax
D) AQE increased the number of executor cores available to the query

**Answer: B**

**Explanation:** AQE's primary performance improvements come from three runtime optimizations: (1) coalescing small post-shuffle partitions to reduce task scheduling overhead, (2) converting sort-merge joins to broadcast hash joins when one side is discovered to be small at runtime, and (3) optimizing skewed join partitions. AQE does not cache results (A), rewrite SQL (C), or change cluster resources (D). (Module 05)

---

### Question 12
A data engineer uses `collect_list` in an aggregation and finds that some groups return arrays with millions of elements, causing driver OOM errors. What is the best approach to handle this?

A) Increase the driver memory to accommodate the large arrays
B) Use `collect_set` instead of `collect_list` to deduplicate elements
C) Limit the aggregation output using a window function with `row_number` to cap elements per group, or use `approx_count_distinct` if only counts are needed
D) Switch from DataFrame API to RDD API for better memory management

**Answer: C**

**Explanation:** `collect_list` and `collect_set` aggregate all values into a single array on one executor, which can be problematic for large groups. The best approach depends on the use case: if you only need a count, use `approx_count_distinct`; if you need the top N items per group, use a window function to limit rows before collecting. Increasing driver memory (A) is a band-aid that does not scale. Using `collect_set` (B) helps with duplicates but still fails for groups with many distinct values. RDDs (D) have worse performance than DataFrames. (Module 04, 05)

---

### Question 13
A pipeline reads JSON files with Auto Loader (`cloudFiles` format). A new field `loyalty_status` appears in recent files. The pipeline fails with a schema mismatch error. What configuration enables automatic handling of new columns?

A) `.option("cloudFiles.inferColumnTypes", "true")`
B) `.option("cloudFiles.schemaEvolutionMode", "addNewColumns")`
C) `.option("mergeSchema", "true")` on the write side
D) Both B and C are required together

**Answer: D**

**Explanation:** Two settings work together: on the read side, `cloudFiles.schemaEvolutionMode = "addNewColumns"` tells Auto Loader to detect and add new columns to the inferred schema. On the write side, `.option("mergeSchema", "true")` (or `spark.databricks.delta.schema.autoMerge.enabled = true`) tells Delta Lake to accept the new column in the target table schema. Without both, either the read or the write will fail on the new column. (Module 02, 07)

---

### Question 14
A data engineer uses higher-order functions to process an array column containing order items. Which expression correctly calculates the total cost of all items where the item price exceeds 100?

A) `aggregate(filter(items, x -> x.price > 100), 0, (acc, x) -> acc + x.price * x.qty)`
B) `transform(items, x -> x.price * x.qty).filter(x -> x > 100).sum()`
C) `explode(items).filter(price > 100).agg(sum(price * qty))`
D) `reduce(items, 0, (acc, x) -> IF(x.price > 100, acc + x.price * x.qty, acc))`

**Answer: A**

**Explanation:** The correct approach uses `filter` to keep only items with price > 100, then `aggregate` (also called `reduce` in some contexts) to sum the cost. In PySpark SQL syntax, `aggregate(array, initial_value, merge_function)` accumulates a result across all array elements. Option B uses chained methods that do not exist on Column objects. Option C uses `explode`, which is valid but not a higher-order function. Option D uses `reduce` which is the Spark SQL name but the syntax shown is incorrect. (Module 04)

---

### Question 15
A streaming pipeline writes to a Delta table using `foreachBatch`. Inside the `foreachBatch` function, the engineer performs a MERGE operation. Under what condition can this produce duplicate records?

A) If the MERGE key has null values in the source data
B) If the streaming query restarts after a failure and the `foreachBatch` function is not idempotent -- the same micro-batch may be reprocessed without the MERGE detecting duplicates correctly
C) MERGE inside `foreachBatch` always produces duplicates because streaming and MERGE are incompatible
D) If the checkpoint location is on local disk instead of cloud storage

**Answer: B**

**Explanation:** Structured Streaming guarantees exactly-once processing through checkpointing, but `foreachBatch` can re-execute a micro-batch on failure recovery. If the MERGE operation is not idempotent (for example, if it inserts unconditionally in the `WHEN NOT MATCHED` clause without checking for the specific batch), reprocessing can produce duplicates. The fix is to make the MERGE idempotent by including the batch ID or using a natural key that prevents double-insertion. (Module 07)

---

## Domain 2: Databricks Tooling (10 questions)

### Question 16
A data engineer wants to run PySpark code from their local VS Code against a remote Databricks cluster. They set up Databricks Connect v2. Which limitation will they encounter?

A) Databricks Connect does not support reading Delta tables
B) Databricks Connect does not support Structured Streaming, RDD operations, or custom JVM code
C) Databricks Connect only works with SQL queries, not DataFrame API
D) Databricks Connect requires a dedicated single-node cluster

**Answer: B**

**Explanation:** Databricks Connect v2 supports DataFrame API and Spark SQL against a remote cluster, but it does NOT support Structured Streaming, RDD operations, SparkContext methods, or custom JVM code. It works with multi-node clusters (D is wrong), supports Delta (A is wrong), and supports both SQL and DataFrame API (C is wrong). (Module 06)

---

### Question 17
An Asset Bundle (`databricks.yml`) defines a job with two deployment targets: `dev` and `prod`. The prod target overrides the cluster configuration to use 8 workers instead of 2. Which command deploys to the production target?

A) `databricks bundle deploy --target prod`
B) `databricks bundle deploy -e prod`
C) `databricks bundle run -t prod`
D) `databricks deploy --env prod`

**Answer: A**

**Explanation:** The correct command is `databricks bundle deploy --target prod` (or `-t prod` as a shorthand). The `--target` flag (also abbreviated `-t`) selects which target configuration to use from the `targets` section of `databricks.yml`. Option C (`run`) executes a resource but does not deploy. The `-e` flag (B) and `databricks deploy` (D) are not valid CLI commands for Asset Bundles. (Module 06)

---

### Question 18
A data engineer uses `dbutils.notebook.run("./child_notebook", timeout_seconds=600, arguments={"date": "2024-01-01"})` in a parent notebook. The child notebook completes and returns the string `"processed 5000 rows"`. Which statement about this approach is correct?

A) The child notebook runs on a separate cluster, isolating failures
B) `dbutils.notebook.run()` can only return a single string value, which limits its usefulness for passing complex data between notebooks
C) The arguments dictionary is available as Spark configuration properties in the child notebook
D) If the child notebook fails, the parent notebook continues execution normally

**Answer: B**

**Explanation:** `dbutils.notebook.run()` executes a child notebook on the same cluster and can return a single string via `dbutils.notebook.exit("result_string")`. This is a significant limitation for complex orchestration -- for passing structured data, you should use Delta tables, task values in Jobs, or Databricks Jobs with proper task dependencies. The child runs on the SAME cluster (A is wrong). Arguments are accessed via `dbutils.widgets.get()` (C is wrong). If the child fails, an exception is raised in the parent (D is wrong). (Module 06)

---

### Question 19
A production pipeline uses the Databricks REST API to trigger a job and poll for completion. The engineer's polling code calls the "get run" endpoint every 5 seconds. After a recent platform update, the code starts receiving HTTP 429 responses. What does this indicate?

A) The job failed and the API is returning an error
B) The API is rate-limiting the client because it is making too many requests per minute
C) The authentication token has expired
D) The run ID is invalid

**Answer: B**

**Explanation:** HTTP 429 means "Too Many Requests" -- the API is rate-limiting the client. The fix is to implement exponential backoff (increase the polling interval after each request) or reduce the polling frequency. A 5-second interval can be too aggressive for the Jobs API, especially if multiple pipelines are polling simultaneously. HTTP 401 would indicate an expired token (C), HTTP 404 would indicate an invalid run ID (D). (Module 06)

---

### Question 20
A data engineer wants to parameterize a notebook so that the same code can process different date ranges when run as part of a job. Which approach is recommended for production?

A) Hard-code dates and create a separate notebook for each date range
B) Use `dbutils.widgets` to define parameters, which can be overridden by job task configuration at runtime
C) Read dates from a configuration file stored in DBFS
D) Use environment variables set on the cluster

**Answer: B**

**Explanation:** `dbutils.widgets` is the standard Databricks mechanism for notebook parameterization. When the notebook runs as a job task, parameters defined with widgets can be overridden by the task configuration (base_parameters or notebook_params in the API). This allows the same notebook to be reused with different configurations. Hard-coding (A) does not scale. DBFS config files (C) add unnecessary complexity. Environment variables (D) require cluster restarts to change. (Module 06)

---

### Question 21
A CI/CD pipeline uses Asset Bundles to deploy Databricks resources. The `databricks.yml` file has targets for `dev`, `staging`, and `prod`. What is the correct deployment sequence?

A) Deploy to all three targets simultaneously to save time
B) Deploy to dev, run tests, promote to staging for integration tests, then deploy to prod
C) Deploy directly to prod and roll back if tests fail
D) Deploy to staging first since it mirrors production

**Answer: B**

**Explanation:** The recommended environment promotion pattern is dev -> staging -> prod. Deploy to dev first for development and unit testing. After passing tests, deploy to staging for integration testing against production-like data. Finally, deploy to prod. Simultaneous deployment (A) risks deploying untested code to production. Direct-to-prod (C) risks production failures. Skipping dev (D) misses early detection of issues. (Module 06, 09)

---

### Question 22
A secret scope is created in Databricks backed by Azure Key Vault. A notebook accesses a secret using `dbutils.secrets.get(scope="my_scope", key="db_password")`. What happens when this value is printed in a notebook cell?

A) The raw password is displayed in the cell output
B) The value is displayed as `[REDACTED]` to prevent accidental exposure in notebook output
C) An error is thrown because secrets cannot be printed
D) The value is displayed only if the user has `MANAGE` permission on the scope

**Answer: B**

**Explanation:** Databricks automatically redacts secret values in notebook output, displaying them as `[REDACTED]`. This prevents accidental exposure of sensitive values in logs, cell output, or shared notebooks. The secret value is fully accessible in the code itself (for use in connection strings, API calls, etc.), just not in displayed output. This applies regardless of permissions. (Module 08)

---

### Question 23
A data engineer creates a multi-task job with the following dependencies: Task A -> Task B -> Task D, and Task A -> Task C -> Task D. Task B fails after 3 retries. What happens to Tasks C and D?

A) Both Task C and Task D are skipped
B) Task C runs (it depends only on Task A), but Task D is skipped (it depends on Task B which failed)
C) Task C runs, and Task D runs with only the output from Task C
D) All tasks are retried from the beginning

**Answer: B**

**Explanation:** In a multi-task job, each task runs when ALL of its upstream dependencies succeed. Task C depends only on Task A (which succeeded), so it runs. Task D depends on BOTH Task B and Task C. Since Task B failed, Task D is skipped regardless of whether Task C succeeds. This is the fan-in pattern: a task with multiple dependencies requires all of them to succeed. (Module 06)

---

### Question 24
A data engineer needs to configure a notebook widget that allows users to select multiple values from a predefined list. Which widget type should they use?

A) `dbutils.widgets.text("regions", "US")`
B) `dbutils.widgets.dropdown("regions", "US", ["US", "EU", "APAC"])`
C) `dbutils.widgets.multiselect("regions", "US", ["US", "EU", "APAC"])`
D) `dbutils.widgets.combobox("regions", "US", ["US", "EU", "APAC"])`

**Answer: C**

**Explanation:** `multiselect` allows users to choose one or more values from a predefined list, returning a comma-separated string of selected values. `dropdown` (B) allows selecting only one value. `text` (A) is a free-form text input. `combobox` (D) allows selecting from a list or typing a custom value, but only one value at a time. (Module 06)

---

### Question 25
An engineer runs `databricks bundle validate` and gets an error: "resource 'daily_etl' references cluster key 'etl_cluster' which is not defined in job_clusters". What needs to be fixed?

A) The cluster must be created manually in the workspace before deploying
B) A `job_clusters` section must be added to the job definition in `databricks.yml` that defines the `etl_cluster` key with a cluster configuration
C) The `etl_cluster` name must match an existing all-purpose cluster in the workspace
D) The `validate` command has a bug; use `deploy` directly instead

**Answer: B**

**Explanation:** Asset Bundles require that any `job_cluster_key` referenced by a task is defined in the `job_clusters` section of the same job resource. This section specifies the cluster configuration (node type, number of workers, Spark version, etc.) that will be used when the job runs. The cluster is created and destroyed automatically for each job run -- it does not need to exist beforehand (A, C are wrong). The validate command is working correctly (D is wrong). (Module 06)

---

## Domain 3: Data Modeling (10 questions)

### Question 26
A data engineer implements SCD Type 2 using MERGE INTO for a customer dimension table. The table has columns: `customer_id`, `name`, `tier`, `effective_date`, `end_date`, `is_current`. After a MERGE that closes an old record and inserts a new version, the engineer notices that the closed record has `end_date = current_date()` but the new record also has `effective_date = current_date()`. What potential issue does this create?

A) No issue -- this is the standard SCD Type 2 pattern
B) A gap-free history requires that `end_date` of the old record equals `effective_date - 1` of the new record, or the convention must be documented so queries handle the overlap correctly
C) The MERGE will fail because two records with the same `customer_id` cannot exist
D) The `is_current` flag becomes unreliable

**Answer: B**

**Explanation:** When both the closed record's `end_date` and the new record's `effective_date` are `current_date()`, there is an overlap on that date: both records are "effective" on the same day. This is acceptable if the convention is documented and queries use `is_current = true` for the latest version. However, if queries use date range filtering (`WHERE effective_date <= query_date AND (end_date IS NULL OR end_date > query_date)`), the overlap could return two records. The standard practice is either to use `end_date = effective_date - 1 day` for the old record, or to use `end_date` as exclusive (the record is valid up to but not including `end_date`). (Module 04)

---

### Question 27
A Delta table has 10,000 small files (1 MB each) created by frequent streaming micro-batch writes. Queries against this table are slow. Which operation sequence will resolve the performance issue?

A) Run `VACUUM` to remove old files, then `OPTIMIZE` to compact remaining files
B) Run `OPTIMIZE` to compact small files into larger ones, then `VACUUM` after the retention period to remove the old small files
C) Drop and recreate the table with fewer partitions
D) Increase the `spark.sql.shuffle.partitions` setting

**Answer: B**

**Explanation:** `OPTIMIZE` compacts the 10,000 small files into fewer, larger files (targeting ~1 GB each). After OPTIMIZE, the old small files still exist on disk (for time travel). `VACUUM` removes files older than the retention period (default 7 days). Running VACUUM first (A) would not help because the small files are the current version. Dropping the table (C) loses history. Shuffle partitions (D) affect query execution, not file layout. (Module 03, 05)

---

### Question 28
A data architect is designing a lakehouse with a medallion architecture. The Silver layer contains cleansed, deduplicated customer records. The Gold layer needs a customer dimension suitable for analytical queries. Which approach is correct?

A) Copy the Silver table directly to Gold without modifications -- deduplication is sufficient
B) Apply business logic (deriving tiers, aggregating metrics, computing lifetime value), create surrogate keys, and structure the data as a star schema dimension in the Gold layer
C) Store raw JSON documents in the Gold layer for maximum flexibility
D) The Gold layer should only contain materialized views, never tables

**Answer: B**

**Explanation:** The Gold layer serves business-ready, analytics-optimized data. It should apply business rules (tier calculations, aggregations), use surrogate keys for efficient joins in star schema patterns, and be structured for the consumption use case. Simply copying Silver data (A) does not add business value. Raw JSON (C) belongs in Bronze. The Gold layer can contain both tables and views (D). (Module 03)

---

### Question 29
A data engineer needs to choose between Z-ORDER and Liquid Clustering for a large Delta table that is filtered by both `region` and `event_date` columns. The table receives continuous streaming inserts. Which approach is better and why?

A) Z-ORDER is better because it is a mature feature with better documentation
B) Liquid Clustering is better because it supports incremental clustering on new data, while Z-ORDER requires running OPTIMIZE on the entire table or specified partitions
C) Both are identical in behavior -- choose either one
D) Neither is needed because Delta Lake's data skipping statistics already handle this case

**Answer: B**

**Explanation:** Liquid Clustering (`ALTER TABLE ... CLUSTER BY (region, event_date)`) is the newer, recommended approach. It clusters data incrementally during writes and during `OPTIMIZE`, meaning only new or modified data is re-clustered. Z-ORDER requires explicitly running `OPTIMIZE ... ZORDER BY (region, event_date)`, which processes entire files (or partitions), making it more expensive for tables with continuous streaming ingestion. Data skipping statistics (D) help but are most effective when combined with clustering. (Module 05)

---

### Question 30
A data engineer runs `VACUUM my_table RETAIN 0 HOURS` and receives an error. What must they do to execute this command, and what is the consequence?

A) Set `spark.databricks.delta.retentionDurationCheck.enabled = false`; the consequence is that all historical versions are permanently deleted and time travel to any prior version will fail
B) No additional configuration is needed; VACUUM with 0 hours is always allowed
C) Set `spark.databricks.delta.vacuum.enabled = true`; there is no consequence
D) Run `ALTER TABLE my_table SET TBLPROPERTIES ('delta.enableTimeTravel' = 'false')` first

**Answer: A**

**Explanation:** By default, Delta Lake prevents VACUUM with retention less than 7 days to protect time travel. To override this safety check, set `spark.databricks.delta.retentionDurationCheck.enabled = false`. With `RETAIN 0 HOURS`, all files not referenced by the current version are deleted, making time travel to any historical version impossible. This is destructive and generally not recommended for production. (Module 03)

---

### Question 31
A fact table has 500 million rows and is partitioned by `event_date`. Queries almost always filter by `customer_id` (high cardinality: 10 million distinct values). Should the engineer add partitioning by `customer_id`?

A) Yes, partitioning by `customer_id` will speed up all queries that filter by it
B) No, partitioning by `customer_id` would create 10 million partition directories with tiny files, which is an anti-pattern; use Z-ORDER or Liquid Clustering on `customer_id` instead
C) Yes, but only if combined with bucketing on `event_date`
D) No, because Delta Lake does not support partitioning on integer columns

**Answer: B**

**Explanation:** Partitioning on a high-cardinality column like `customer_id` (10M distinct values) creates millions of tiny partition directories, each with very small files. This leads to excessive metadata overhead and poor performance (the "small files problem"). Instead, use Z-ORDER or Liquid Clustering to co-locate data by `customer_id` within the existing date partitions. This gives data skipping benefits without the small files problem. (Module 05)

---

### Question 32
A data engineer builds a SCD Type 2 customer dimension and needs to query "what was each customer's tier on January 15, 2024?" Which WHERE clause correctly retrieves the point-in-time snapshot?

A) `WHERE effective_date = '2024-01-15'`
B) `WHERE effective_date <= '2024-01-15' AND (end_date IS NULL OR end_date > '2024-01-15')`
C) `WHERE is_current = true AND effective_date <= '2024-01-15'`
D) `WHERE end_date = '2024-01-15'`

**Answer: B**

**Explanation:** A point-in-time SCD Type 2 query requires finding the record that was active on the target date. The record must have started on or before the target date (`effective_date <= '2024-01-15'`) AND must not have ended before the target date (`end_date IS NULL` for current records, or `end_date > '2024-01-15'` for records that were later superseded). Option A only finds records that started on exactly that date. Option C (is_current = true) only returns the current version, not the historical one valid on Jan 15. (Module 04)

---

### Question 33
A Gold-layer table needs to be refreshed daily. The table is a complete aggregation of Silver-layer data (total sales by region by month). Which refresh strategy is most efficient?

A) Truncate and reload the entire Gold table every day
B) Use an incremental approach: identify which months in Silver changed since the last run, and MERGE only those aggregated months into the Gold table
C) Append new daily aggregations and let consumers filter for the latest version
D) Use a materialized view so that no explicit refresh is needed

**Answer: B**

**Explanation:** Incremental processing (B) is the most efficient approach for most Gold-layer tables. By tracking which source data changed (using Delta table versioning, timestamps, or watermarks), you only recompute and MERGE the affected aggregations. Full truncate-and-reload (A) is simple but wasteful for large tables. Appending without deduplication (C) creates duplicates. Materialized views (D) are a valid option in some cases but may not be supported for all aggregation patterns and have their own refresh costs. (Module 03, 07)

---

### Question 34
A table uses partitioning by `year` and `month`. Running `DESCRIBE DETAIL my_table` shows `numFiles = 50000`. The engineer runs `OPTIMIZE my_table` and the file count drops to 2400. What happened?

A) OPTIMIZE deleted 47,600 files from the table
B) OPTIMIZE compacted 50,000 small files into 2,400 larger files; the original small files still exist on disk until VACUUM removes them
C) OPTIMIZE repartitioned the data by year and month
D) OPTIMIZE deduplicated the rows, removing 47,600 files worth of duplicate data

**Answer: B**

**Explanation:** OPTIMIZE creates new, larger compacted files and marks the old small files as "tombstoned" in the Delta log. The old files remain on disk and are accessible for time travel. The `numFiles` in DESCRIBE DETAIL shows only the files referenced by the current table version. Running VACUUM after the retention period will physically remove the old small files. OPTIMIZE does not delete files, repartition, or deduplicate. (Module 03, 05)

---

### Question 35
An analytics team queries a Gold-layer table that joins a 100 million row fact table with a 5,000 row dimension table. The query plan shows a SortMergeJoin. What change would improve performance?

A) Add an index on the join key in both tables
B) The small dimension table should be broadcast joined; either reduce the `autoBroadcastJoinThreshold` or use an explicit `broadcast()` hint to force a BroadcastHashJoin
C) Increase the cluster size to add more executors
D) Repartition both tables by the join key before the join

**Answer: B**

**Explanation:** A 5,000 row dimension table is very small and should be broadcast to all executors, avoiding the expensive sort and shuffle of the 100 million row fact table. If AQE is enabled, it may automatically convert this to a broadcast join. However, if the table's estimated size exceeds the `autoBroadcastJoinThreshold` (10 MB default), an explicit `broadcast()` hint forces the optimization. Delta tables do not support traditional indexes (A). Adding executors (C) helps but does not eliminate the shuffle. Repartitioning (D) still requires a shuffle. (Module 05)

---

## Domain 4: Security and Governance (5 questions)

### Question 36
In Unity Catalog, an engineer grants `SELECT` on a table to the `analysts` group, but the analysts report "Access Denied" when querying the table. What is the most likely cause?

A) The `SELECT` grant syntax was incorrect
B) The `analysts` group has not been granted `USAGE` on the parent catalog and schema -- USAGE is required before table-level privileges take effect
C) Unity Catalog does not support group-level permissions
D) The table is encrypted and the analysts do not have the decryption key

**Answer: B**

**Explanation:** Unity Catalog requires the `USAGE` privilege at every level of the namespace hierarchy. Even if `SELECT` is granted on a table, users cannot access it unless they also have `USAGE` on the parent schema and `USAGE` on the parent catalog. The correct fix is: `GRANT USAGE ON CATALOG x TO analysts; GRANT USAGE ON SCHEMA x.y TO analysts; GRANT SELECT ON TABLE x.y.z TO analysts;`. (Module 08)

---

### Question 37
A company must comply with GDPR "right to erasure" requirements. Customer PII is stored across Bronze, Silver, and Gold Delta layers. What is the recommended approach to delete a specific customer's data?

A) Run `DELETE FROM` on each table in all three layers where `customer_id = <id>`, then `VACUUM` all tables to physically remove the data from storage
B) Drop and recreate all three tables without the customer's data
C) Overwrite the customer's PII columns with null values instead of deleting rows
D) Simply run `VACUUM` on all tables -- it automatically removes PII data

**Answer: A**

**Explanation:** GDPR's right to erasure requires that the customer's data be physically removed from storage. Step 1: Run `DELETE FROM` on every Delta table containing the customer's data across all layers. Step 2: Run `VACUUM` with a retention of 0 hours (after disabling the retention check) to physically remove the old data files that still contain the customer's records. Without VACUUM, the deleted data remains accessible via time travel. Nullifying (C) is not full erasure. Dropping tables (B) is destructive and unnecessary. VACUUM alone (D) does not target specific records. (Module 08)

---

### Question 38
A data engineer sets up a production pipeline using a service principal for authentication. Which statement about service principals in Databricks is correct?

A) Service principals can only authenticate using personal access tokens (PATs)
B) Service principals support OAuth M2M (machine-to-machine) tokens, which are preferred over PATs for production because they have built-in expiration and do not depend on a specific user account
C) Service principals have full admin privileges by default
D) Service principals cannot be used with Unity Catalog

**Answer: B**

**Explanation:** Service principals are non-human identities designed for automated processes. They support both PATs and OAuth M2M tokens, but OAuth M2M is preferred for production because tokens expire automatically and do not tie to an individual user's account. Service principals do NOT have admin privileges by default (C) -- they follow the same privilege model as regular users. They work fully with Unity Catalog (D). (Module 08)

---

### Question 39
A Unity Catalog column mask function is defined as:

```sql
CREATE FUNCTION email_mask(email STRING)
RETURNS STRING
RETURN CASE
  WHEN is_member('pii_readers') THEN email
  ELSE CONCAT(LEFT(email, 2), '****@****.com')
END;
```

The mask is applied to the `email` column. A user who is NOT in the `pii_readers` group queries the table. What do they see for the email `alice@example.com`?

A) `alice@example.com` (the mask is not applied for SELECT queries)
B) `al****@****.com`
C) `NULL`
D) An access denied error

**Answer: B**

**Explanation:** Column masks are applied transparently on every query. The function checks group membership using `is_member()`. For users not in `pii_readers`, the ELSE branch runs, returning the first 2 characters followed by the mask pattern. So `alice@example.com` becomes `al****@****.com`. The user sees the masked value without any error -- column masks are transparent, not access-blocking. (Module 08)

---

### Question 40
An auditor asks: "Who accessed the `revenue_summary` table in the last 30 days?" Where can this information be found?

A) In the Delta transaction log (`_delta_log`) of the table
B) In Unity Catalog audit logs, which track data access events including the user identity, timestamp, and operation type
C) In the Spark driver logs on the cluster
D) This information is not tracked by Databricks

**Answer: B**

**Explanation:** Unity Catalog audit logs capture data access events with details including the user or service principal identity, the operation performed (SELECT, INSERT, etc.), the timestamp, and the resources accessed. The Delta transaction log (A) records write operations (INSERT, UPDATE, DELETE, MERGE) but does not log read operations (SELECT). Spark driver logs (C) may contain query text but are not structured for audit purposes and are not retained long-term. (Module 08)

---

## Domain 5: Monitoring and Logging (5 questions)

### Question 41
A Spark job runs nightly and processes 100 GB of data. The job previously took 30 minutes but now takes 2 hours. The Spark UI shows a large amount of "Shuffle Spill (Disk)" in the Stages tab. What is the most likely cause and fix?

A) The data volume increased; add more workers to the cluster
B) Executor memory is insufficient for the shuffle operation, causing data to spill from memory to disk; increase `spark.executor.memory` or reduce the data per partition by increasing `spark.sql.shuffle.partitions`
C) The DBFS storage is slow; switch to a faster storage tier
D) The Delta transaction log is too large; run VACUUM

**Answer: B**

**Explanation:** "Shuffle Spill (Disk)" means executors ran out of memory during a shuffle and had to write intermediate data to local disk, which is much slower. The fixes are: increase executor memory, increase shuffle partitions (so each partition is smaller), or reduce the amount of data being shuffled (by filtering earlier or using broadcast joins). Data volume increase (A) could be a contributing factor but the specific symptom points to memory pressure during shuffle. (Module 05, 09)

---

### Question 42
A streaming pipeline processes events with an average latency of 5 seconds. After a deployment change, latency increases to 60 seconds. The `query.lastProgress` metrics show `processedRowsPerSecond` dropped from 10,000 to 500. What should the engineer investigate first?

A) Whether the source system is sending data more slowly
B) Whether the deployment introduced a less efficient transformation (such as replacing a built-in function with a Python UDF) that reduced processing throughput
C) Whether the checkpoint directory was accidentally deleted
D) Whether the output Delta table ran out of storage

**Answer: B**

**Explanation:** A dramatic drop in `processedRowsPerSecond` after a deployment change strongly suggests the new code introduced a processing bottleneck. Common culprits include replacing built-in functions with Python UDFs (10-100x slower), adding unnecessary shuffles, or introducing a complex join. If the source was slower (A), `inputRowsPerSecond` would also drop. A deleted checkpoint (C) would cause a restart from scratch, not slower processing. Storage issues (D) would cause write failures, not slow processing. (Module 07, 09)

---

### Question 43
A data engineer wants to monitor the cost of Databricks jobs and identify the most expensive workflows. Which approach provides the most accurate cost tracking?

A) Estimate cost by multiplying cluster uptime by the on-demand instance price
B) Use the Databricks account console or system tables (`system.billing.usage`) to track DBU consumption by job, cluster, and workspace
C) Monitor the cloud provider bill and divide total compute cost by the number of jobs
D) Count the number of tasks in each job as a proxy for cost

**Answer: B**

**Explanation:** Databricks system tables (specifically `system.billing.usage`) provide granular DBU consumption data broken down by workspace, cluster, job, and user. This gives the most accurate view of cost attribution. The account console provides dashboards for this data. Cloud provider bills (C) show infrastructure cost but do not attribute it to specific Databricks jobs. Cluster uptime (A) does not account for different instance types and DBU rates. Task count (D) is not correlated with cost. (Module 09)

---

### Question 44
In the Spark UI, a stage shows 200 tasks with the following distribution: min duration = 0.5s, median = 2s, 75th percentile = 3s, max = 120s. What does this indicate?

A) The stage is performing well with normal variation
B) Severe data skew -- one or a few partitions contain significantly more data, causing outlier task durations at 120 seconds while most finish in 2-3 seconds
C) The cluster is too small for the workload
D) The max duration task had a hardware failure

**Answer: B**

**Explanation:** When the max task duration (120s) is 60x the median (2s), this is a clear signal of data skew. Most partitions are small (processed in 2-3 seconds), but one or a few partitions are extremely large (taking 2 minutes). AQE's skew join optimization, key salting, or repartitioning can address this. A hardware failure (D) would typically show as a task failure and retry, not a long-running task. (Module 05, 09)

---

### Question 45
A data engineer configures alerts for a production Databricks job. Which alert configuration provides the most comprehensive monitoring?

A) Email notification on job failure only
B) Email on failure, webhook to PagerDuty on failure, duration alert if runtime exceeds 2x the historical average, and a success notification to a Slack channel
C) Log monitoring using grep on cluster log files
D) Manual dashboard checking every morning

**Answer: B**

**Explanation:** Comprehensive job monitoring includes multiple alert channels and conditions: failure alerts (email + PagerDuty for immediate response), duration alerts (catching performance degradation before failure), and success notifications (confirming normal operation). Email-only (A) misses performance degradation. Log monitoring (C) is reactive and manual. Dashboard checking (D) introduces delay in detecting issues. (Module 09)

---

## Domain 6: Testing and Deployment (5 questions)

### Question 46
A data engineer writes a function that applies business rules to a DataFrame. They want to unit test it using pytest. Which approach is correct?

A) Write the function as a standalone Python function that accepts a DataFrame as input and returns a DataFrame, then test it using a local SparkSession in pytest
B) Test the function by running the notebook in Databricks and visually inspecting the output
C) Skip testing because Spark DataFrames cannot be compared in unit tests
D) Use print statements instead of assertions to verify correctness

**Answer: A**

**Explanation:** The best approach is to write business logic as standalone functions (not embedded in notebooks) that accept DataFrames as input and return DataFrames. In pytest, create a local SparkSession fixture, build small test DataFrames, pass them through the function, and assert on the output using `collect()` or `toPandas()`. Visual inspection (B) is error-prone and not automated. DataFrame comparison is straightforward with `collect()` (C is wrong). Print statements (D) do not fail the test suite. (Module 09)

---

### Question 47
A CI/CD pipeline for a Databricks project has three stages: lint, test, and deploy. The test stage creates a local SparkSession and runs pytest. A test uses `spark.createDataFrame()` to build test data. What is a potential issue in the CI/CD environment?

A) There is no issue -- local SparkSession works in any Python environment
B) The CI/CD runner must have Java installed and sufficient memory for a local Spark instance; without Java, the SparkSession will fail to initialize
C) pytest cannot import PySpark functions
D) Local SparkSession does not support `createDataFrame`

**Answer: B**

**Explanation:** PySpark requires a Java Runtime Environment (JRE) to create a SparkSession, even in local mode. CI/CD runners (GitHub Actions, Azure DevOps agents, etc.) may not have Java installed by default. The pipeline must include a step to install Java and ensure adequate memory (at least 2-4 GB) for the Spark JVM. PySpark can be imported (C is wrong) and `createDataFrame` works in local mode (D is wrong). (Module 09)

---

### Question 48
A data engineer needs to test a streaming pipeline that reads from Kafka and writes to Delta. What is the recommended testing approach?

A) Connect to the production Kafka cluster from the test environment
B) Use `trigger(availableNow=True)` with test data written to a temporary Delta table as the source, verify the output, and clean up after the test
C) Streaming pipelines cannot be tested -- only validate in production
D) Mock the entire Spark streaming framework

**Answer: B**

**Explanation:** For integration testing of streaming pipelines, replace the real source (Kafka) with a test source (a temporary Delta table or memory source) containing known test data. Use `trigger(availableNow=True)` to process all test data in one run and then stop. Verify the output against expected results. This approach tests the actual streaming logic without depending on external systems. Connecting to production Kafka (A) is risky and unreliable for testing. (Module 07, 09)

---

### Question 49
A deployment uses a blue/green strategy for a critical pipeline. The "blue" version is currently in production. The new "green" version is deployed alongside it. What determines when to switch traffic from blue to green?

A) The green version is switched immediately after deployment
B) The green version runs against a sample of production data; after validation confirms correct output and acceptable performance, traffic is switched; the blue version is kept available for quick rollback
C) Both versions run simultaneously in production forever
D) The green version replaces the blue version after a 24-hour waiting period regardless of test results

**Answer: B**

**Explanation:** Blue/green deployment involves running both versions simultaneously, validating the green version against production data (or a representative sample), and switching traffic only after confidence in correctness and performance. The blue version remains available for instant rollback if issues are discovered. Immediate switching (A) defeats the purpose. Running both forever (C) doubles cost. Time-based switching (D) ignores validation results. (Module 06, 09)

---

### Question 50
A data engineering team adopts the testing pyramid for their Databricks project. They have limited CI/CD compute budget. How should they allocate their testing effort?

A) Focus entirely on end-to-end tests that run complete pipelines on a Databricks cluster
B) Write many fast unit tests for pure Python functions (local, no Spark), fewer integration tests with local SparkSession, and a small number of end-to-end tests on Databricks
C) Only write unit tests because they are the cheapest to run
D) Skip automated testing and rely on code reviews

**Answer: B**

**Explanation:** The testing pyramid recommends: many unit tests at the base (fast, cheap, test business logic), fewer integration tests in the middle (test Spark operations with local SparkSession), and a small number of end-to-end tests at the top (test full pipeline on Databricks, expensive). Unit tests alone (C) miss Spark-specific issues. End-to-end only (A) is slow and expensive. Skipping tests (D) leads to production failures. (Module 09)

---

## Scoring Guide

| Score | Assessment | Recommendation |
|-------|-----------|----------------|
| 45-50 (90-100%) | Excellent -- ready for the exam | Schedule your exam within 1-2 weeks |
| 40-44 (80-89%) | Strong -- minor gaps to address | Review missed topics, retake in 1 week |
| 35-39 (70-79%) | Passing -- but thin margin | Study weak domains for 2 more weeks |
| 25-34 (50-69%) | Needs significant work | Revisit Modules 01-09, retake in 3-4 weeks |
| Below 25 (<50%) | Not ready | Complete all course modules before retaking |

### Score by Domain

Track your score per domain to identify weaknesses:

| Domain | Questions | Your Score | Passing (70%) |
|--------|-----------|------------|---------------|
| 1: Data Processing | 1-15 (15 Qs) | __/15 | 11 |
| 2: Databricks Tooling | 16-25 (10 Qs) | __/10 | 7 |
| 3: Data Modeling | 26-35 (10 Qs) | __/10 | 7 |
| 4: Security & Governance | 36-40 (5 Qs) | __/5 | 4 |
| 5: Monitoring & Logging | 41-45 (5 Qs) | __/5 | 4 |
| 6: Testing & Deployment | 46-50 (5 Qs) | __/5 | 4 |
| **Total** | **50 Qs** | **__/50** | **35** |

---

## Domain Comparison: Where to Focus

| If You Scored Low In... | Revisit These Modules | Key Concepts to Review |
|------------------------|----------------------|----------------------|
| Data Processing | 04, 05, 07 | Broadcast joins, AQE, watermarks, UDFs, foreachBatch |
| Databricks Tooling | 06 | CLI, REST API, Asset Bundles, Databricks Connect, widgets |
| Data Modeling | 03, 04, 05 | SCD Type 2, Z-ORDER vs Liquid Clustering, OPTIMIZE/VACUUM |
| Security & Governance | 08 | Unity Catalog USAGE grants, column masks, service principals |
| Monitoring & Logging | 05, 09 | Spark UI interpretation, streaming metrics, cost tracking |
| Testing & Deployment | 06, 09 | Testing pyramid, CI/CD, blue/green deployment, Asset Bundles |

---

## Next Steps

- If scoring below 70% in any domain, revisit the corresponding course modules.
- Work through the companion notebook for hands-on code challenges.
- Review the Study Plan (Topic 05) for a structured preparation schedule.
- Retake this practice exam after 1-2 weeks of targeted study.
