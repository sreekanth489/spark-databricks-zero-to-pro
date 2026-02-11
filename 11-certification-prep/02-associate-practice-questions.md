# Associate Data Engineer Practice Questions

> Module 11 -- Topic 02 | Level: Intermediate | Time: 120 min

## Learning Objectives

- Test your knowledge across all five Associate exam domains
- Practice answering multiple-choice questions under exam-like conditions
- Identify weak areas that need further study
- Understand the reasoning behind correct and incorrect answers

## How to Use These Practice Questions

1. Set a timer for 90 minutes (matching the real exam).
2. Answer all 40 questions without looking at the answers.
3. Score yourself: each correct answer = 1 point. Passing = 28/40 (70%).
4. Read the detailed explanations for every question, even ones you got right.
5. Note which domains you scored lowest in and revisit those modules.

---

## Domain 1: Databricks Lakehouse Platform (10 questions)

### Question 1
What is the primary advantage of a lakehouse architecture over a traditional data warehouse?

A) It only supports structured data
B) It combines the reliability of a data warehouse with the flexibility and cost-efficiency of a data lake
C) It eliminates the need for ETL pipelines
D) It requires proprietary storage formats

**Answer: B**

**Explanation:** The lakehouse architecture unifies the best features of data lakes (low-cost storage, schema-on-read flexibility, support for unstructured data) with data warehouse capabilities (ACID transactions, schema enforcement, governance). Delta Lake provides the transaction layer that makes this possible. (Module 03)

---

### Question 2
In Databricks, the `spark` variable available in every notebook is an instance of which class?

A) SparkContext
B) SQLContext
C) SparkSession
D) DatabricksSession

**Answer: C**

**Explanation:** `SparkSession` is the unified entry point for all Spark functionality since Spark 2.0. It encapsulates `SparkContext`, `SQLContext`, and `HiveContext`. In Databricks notebooks, a pre-configured `SparkSession` instance named `spark` is automatically available. (Module 01)

---

### Question 3
Which type of cluster should be used for scheduled production jobs to minimize cost?

A) All-purpose cluster with autoscaling
B) Job cluster
C) SQL warehouse
D) Single-node cluster

**Answer: B**

**Explanation:** Job clusters are created automatically when a job starts and terminated when it finishes. They have a lower DBU rate than all-purpose clusters and do not incur costs when idle. All-purpose clusters are designed for interactive development and remain running until manually stopped or auto-terminated. (Module 06)

---

### Question 4
What does the `_delta_log` directory contain?

A) The actual data files in Parquet format
B) Transaction log entries (JSON files) that record every change to the table
C) Cached query results for faster reads
D) Schema metadata for the Hive metastore

**Answer: B**

**Explanation:** The `_delta_log` directory is the transaction log for a Delta table. It contains JSON files (and periodic checkpoint Parquet files) that record every operation (insert, update, delete, merge) performed on the table. This log enables ACID transactions, time travel, and audit history. (Module 03)

---

### Question 5
Which SQL command retrieves data from a Delta table as it existed at version 3?

A) `SELECT * FROM my_table ROLLBACK TO VERSION 3`
B) `SELECT * FROM my_table VERSION AS OF 3`
C) `SELECT * FROM my_table AT VERSION 3`
D) `SELECT * FROM my_table RESTORE VERSION 3`

**Answer: B**

**Explanation:** Delta Lake time travel uses the syntax `VERSION AS OF <version_number>` or `TIMESTAMP AS OF <timestamp>`. `RESTORE` is a different operation that actually reverts the table to a previous version (creating a new version). (Module 03)

---

### Question 6
In Databricks, what is the difference between a managed table and an external table?

A) Managed tables are stored in DBFS; external tables are stored in memory
B) Managed tables are controlled by the metastore (dropping the table deletes the data); external tables have user-managed storage (dropping the table keeps the data)
C) External tables support Delta format; managed tables do not
D) There is no difference; the terms are interchangeable

**Answer: B**

**Explanation:** When you drop a managed table, both the metadata and the underlying data files are deleted. When you drop an external table, only the metadata is removed -- the data files remain in their storage location. This distinction is important for data lifecycle management. (Module 03, 08)

---

### Question 7
Which magic command runs a SQL query in a Python notebook cell?

A) `%run sql`
B) `%sql`
C) `%%sql`
D) `spark.sql()`

**Answer: B**

**Explanation:** The `%sql` magic command at the beginning of a cell switches the cell language to SQL. Note that `spark.sql()` is a Python method call (not a magic command) that can also execute SQL but returns a DataFrame. `%%sql` is Jupyter syntax, not Databricks syntax. (Module 00, 01)

---

### Question 8
What does schema enforcement in Delta Lake do?

A) Automatically converts data types to match the table schema
B) Rejects writes that do not match the table's existing schema
C) Drops columns that are not in the table schema
D) Adds new columns automatically when new data has extra fields

**Answer: B**

**Explanation:** Schema enforcement (also called schema validation) prevents writes with mismatched schemas from corrupting the table. If incoming data has extra columns or incompatible types, the write fails. Schema evolution (a separate feature enabled with `.option("mergeSchema", "true")`) allows adding new columns. (Module 03)

---

### Question 9
Which statement about Databricks Repos is true?

A) Repos can only connect to GitHub, not other Git providers
B) Repos allow you to clone a Git repository, create branches, commit changes, and push/pull from within Databricks
C) Repos automatically deploy code to production when you push
D) Repos replace the need for notebooks in Databricks

**Answer: B**

**Explanation:** Databricks Repos supports integration with multiple Git providers (GitHub, GitLab, Bitbucket, Azure DevOps). It allows standard Git operations within the Databricks workspace. It does not automatically deploy code (that requires CI/CD pipelines) and works alongside notebooks, not as a replacement. (Module 06)

---

### Question 10
What is the default file format when you create a table in Databricks using `CREATE TABLE`?

A) Parquet
B) CSV
C) Delta
D) ORC

**Answer: C**

**Explanation:** In Databricks, Delta is the default format for all table creation operations. When you run `CREATE TABLE` without specifying a format, the table is created as a Delta table. This is different from open-source Apache Spark where Parquet is the default. (Module 03)

---

## Domain 2: ELT with Spark SQL and Python (12 questions)

### Question 11
Which of the following is an action (not a transformation) in Spark?

A) `df.filter(col("age") > 30)`
B) `df.select("name", "age")`
C) `df.groupBy("department")`
D) `df.count()`

**Answer: D**

**Explanation:** `count()` is an action because it triggers execution and returns a result to the driver. `filter()`, `select()`, and `groupBy()` are transformations that build up a logical plan but do not execute until an action is called. Other common actions include `show()`, `collect()`, `write()`, and `first()`. (Module 01)

---

### Question 12
What is the result of `df.write.mode("ignore").saveAsTable("my_table")` when `my_table` already exists?

A) The existing table is overwritten
B) New data is appended to the existing table
C) An error is thrown
D) The write operation is silently skipped

**Answer: D**

**Explanation:** Write mode `ignore` silently skips the operation if the table or path already exists. Mode `overwrite` replaces the data, `append` adds to it, and `errorIfExists` (the default) throws an error. (Module 02)

---

### Question 13
Given the following code, what does `result` contain?

```python
df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "letter"])
result = df.filter(col("id") > 1).select("letter")
```

A) A DataFrame with one column ("letter") and two rows ("b", "c")
B) A list containing ["b", "c"]
C) A DataFrame with two columns ("id", "letter") and two rows
D) Nothing -- the code has a syntax error

**Answer: A**

**Explanation:** `filter(col("id") > 1)` keeps rows where id is 2 and 3. `select("letter")` projects only the "letter" column. The result is a DataFrame (not a list) with one column and two rows. No action has been called, so the computation has not yet been executed -- but the result type is still a DataFrame. (Module 01, 04)

---

### Question 14
What is the correct syntax for a MERGE operation in Spark SQL?

A) `MERGE my_table SET ... WHERE ...`
B) `MERGE INTO target USING source ON condition WHEN MATCHED THEN UPDATE SET ... WHEN NOT MATCHED THEN INSERT ...`
C) `UPDATE target MERGE source ON condition`
D) `INSERT OR UPDATE INTO target FROM source`

**Answer: B**

**Explanation:** The MERGE statement follows the SQL standard syntax: `MERGE INTO` target table, `USING` source, `ON` join condition, with `WHEN MATCHED` and `WHEN NOT MATCHED` clauses. It atomically combines INSERT, UPDATE, and optionally DELETE operations. (Module 03)

---

### Question 15
Which function creates a new column with conditional logic?

A) `col("x").if_else(condition, true_val, false_val)`
B) `when(condition, value).otherwise(default)`
C) `case_when(condition, value)`
D) `iif(condition, true_val, false_val)`

**Answer: B**

**Explanation:** `pyspark.sql.functions.when()` creates a Column expression with conditional logic. You can chain multiple `.when()` calls and end with `.otherwise()` for a default value. This is equivalent to SQL's CASE WHEN expression. (Module 04)

---

### Question 16
What is the difference between `cache()` and `persist()` on a DataFrame?

A) They are identical in every way
B) `cache()` stores in memory only; `persist()` allows choosing a storage level (MEMORY_ONLY, MEMORY_AND_DISK, etc.)
C) `cache()` is for DataFrames; `persist()` is for RDDs only
D) `persist()` writes to disk; `cache()` writes to a Delta table

**Answer: B**

**Explanation:** `cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`. `persist()` accepts a `StorageLevel` parameter that controls where data is stored (MEMORY_ONLY, MEMORY_AND_DISK, DISK_ONLY, etc.). Both are lazy -- the actual caching happens when the next action is triggered. (Module 05)

---

### Question 17
Which join type returns only rows from the left DataFrame that have NO match in the right DataFrame?

A) Left outer join
B) Left semi join
C) Left anti join
D) Cross join

**Answer: C**

**Explanation:** Left anti join returns rows from the left table that do NOT have a matching row in the right table. Left semi join returns rows from the left that DO have a match (but without columns from the right). Left outer join returns all left rows with NULLs for non-matching right columns. (Module 04)

---

### Question 18
What does `coalesce(col("a"), col("b"), lit("default"))` return?

A) The sum of columns a and b
B) The first non-null value among columns a, b, and the literal "default"
C) Column a if it equals column b, otherwise "default"
D) A new column concatenating a, b, and "default"

**Answer: B**

**Explanation:** `coalesce()` returns the first non-null value from its arguments. If column a is null, it checks column b. If both are null, it returns the literal "default". This is commonly used for handling null values in data pipelines. (Module 04)

---

### Question 19
Which PySpark method adds a new column (or replaces an existing one) to a DataFrame?

A) `df.addColumn("new_col", expression)`
B) `df.withColumn("new_col", expression)`
C) `df.setColumn("new_col", expression)`
D) `df.createColumn("new_col", expression)`

**Answer: B**

**Explanation:** `withColumn(name, expression)` returns a new DataFrame with the specified column added or replaced. If a column with the same name exists, it is replaced. DataFrames are immutable -- `withColumn` returns a new DataFrame rather than modifying the original. (Module 04)

---

### Question 20
What is the output of the following Spark SQL query?

```sql
SELECT department, COUNT(*) as cnt
FROM employees
GROUP BY department
HAVING cnt > 2
ORDER BY cnt DESC
```

A) All departments with their employee counts, sorted descending
B) Only departments with more than 2 employees, sorted by count descending
C) An error because `cnt` cannot be used in HAVING
D) All employees grouped by department where their ID > 2

**Answer: B**

**Explanation:** The query groups employees by department, counts them, filters to only departments with more than 2 employees (HAVING clause), and sorts by count descending. Note: In Spark SQL, you CAN reference aliases in HAVING (unlike some other SQL engines). (Module 01, 04)

---

### Question 21
Which read option is used to specify a custom delimiter when reading a CSV file?

A) `.option("fieldSeparator", "|")`
B) `.option("delimiter", "|")`
C) `.option("sep", "|")`
D) Both B and C are valid

**Answer: D**

**Explanation:** Both `delimiter` and `sep` are accepted options for specifying the field delimiter when reading CSV files with `spark.read.format("csv")`. The default delimiter is a comma. Other common CSV options include `header`, `inferSchema`, `quote`, and `nullValue`. (Module 02)

---

### Question 22
What does `df.explain(True)` display?

A) The DataFrame schema
B) The physical execution plan only
C) All plan stages: parsed logical plan, analyzed logical plan, optimized logical plan, and physical plan
D) Statistics about the DataFrame (row count, column count)

**Answer: C**

**Explanation:** `explain(True)` (or `explain("extended")`) shows the complete query plan lifecycle: the parsed logical plan (what you wrote), the analyzed plan (resolved references), the optimized plan (after Catalyst optimization), and the physical plan (how Spark will actually execute it). `explain()` without arguments shows only the physical plan. (Module 01, 05)

---

## Domain 3: Incremental Data Processing (9 questions)

### Question 23
What format does Auto Loader use in `spark.readStream`?

A) `autoLoader`
B) `cloudFiles`
C) `fileStream`
D) `incrementalLoad`

**Answer: B**

**Explanation:** Auto Loader uses the `cloudFiles` format: `spark.readStream.format("cloudFiles")`. The actual file format (JSON, CSV, Parquet) is specified separately via `.option("cloudFiles.format", "json")`. (Module 02, 07)

---

### Question 24
When should you use `COPY INTO` instead of Auto Loader?

A) When you need to process millions of files continuously
B) When you need automatic schema evolution
C) For simple, infrequent batch loads with a small number of files
D) When you need exactly-once processing guarantees

**Answer: C**

**Explanation:** COPY INTO is simpler but less scalable. It is best for occasional batch loads with relatively few files. Auto Loader is preferred for most production workloads because it scales to millions of files, supports automatic schema evolution, and uses efficient file notification mechanisms. Both provide exactly-once guarantees. (Module 02)

---

### Question 25
What is the purpose of a checkpoint location in Structured Streaming?

A) To cache intermediate results for faster processing
B) To store the stream's progress (offsets) and state for fault tolerance and exactly-once processing
C) To log errors for debugging
D) To define the output file path

**Answer: B**

**Explanation:** The checkpoint location stores the stream's progress (which offsets have been processed) and any aggregation state. If a stream fails and restarts, it reads the checkpoint to resume from where it left off without reprocessing or losing data. Each stream must have a unique checkpoint location. (Module 07)

---

### Question 26
Which trigger mode processes all available data and then stops the stream?

A) `trigger(once=True)`
B) `trigger(availableNow=True)`
C) `trigger(processingTime="0 seconds")`
D) `trigger(continuous="1 second")`

**Answer: B**

**Explanation:** `trigger(availableNow=True)` processes all available data in multiple micro-batches and then stops. It is the preferred replacement for `trigger(once=True)`, which processes only one micro-batch. `availableNow` handles large backlogs more efficiently by splitting work across multiple batches. (Module 07)

---

### Question 27
In Delta Live Tables, what does `EXPECT OR DROP` do?

A) Drops the entire pipeline if any row violates the expectation
B) Drops rows that violate the expectation and continues processing valid rows
C) Logs a warning and keeps all rows
D) Drops the expectation rule if it causes too many violations

**Answer: B**

**Explanation:** DLT expectations have three behaviors: `EXPECT` logs violations but keeps all rows (warn). `EXPECT OR DROP` filters out violating rows and processes only valid ones. `EXPECT OR FAIL` halts the pipeline if any row violates the rule. (Module 07)

---

### Question 28
What is the medallion architecture in a lakehouse?

A) A security model with gold, silver, and bronze access levels
B) A data organization pattern with Bronze (raw), Silver (cleansed/validated), and Gold (aggregated/business-ready) layers
C) A storage optimization technique using three levels of compression
D) A cluster configuration pattern with three tiers of compute resources

**Answer: B**

**Explanation:** The medallion architecture organizes data into three layers: Bronze (raw ingestion, minimal transformation), Silver (cleansed, deduplicated, validated), and Gold (business-level aggregates, feature tables, curated datasets). This pattern is commonly implemented using Delta Lake tables. (Module 03)

---

### Question 29
Which Structured Streaming output mode writes only new rows since the last trigger?

A) `complete`
B) `append`
C) `update`
D) `incremental`

**Answer: B**

**Explanation:** `append` mode (the default) writes only new rows to the output. `complete` mode writes the full result table (required for aggregations without watermarks). `update` mode writes only rows that changed since the last trigger. There is no `incremental` mode. (Module 07)

---

### Question 30
What happens if you use the same checkpoint location for two different streams?

A) The streams share state efficiently, reducing storage
B) Both streams process the same data in parallel
C) One or both streams may fail or produce incorrect results due to conflicting state
D) The second stream automatically creates a subdirectory

**Answer: C**

**Explanation:** Each stream must have a unique checkpoint location. Sharing checkpoints between different streams can cause state corruption, incorrect offset tracking, and processing failures. Always use a distinct path for each stream's checkpoint. (Module 07)

---

### Question 31
Which statement about Auto Loader schema evolution is correct?

A) Auto Loader cannot handle schema changes
B) Auto Loader can detect new columns in the source data and automatically add them to the target table when schema evolution is enabled
C) Schema evolution requires manually restarting the stream after each schema change
D) Auto Loader only supports schema inference at stream creation time

**Answer: B**

**Explanation:** Auto Loader stores the inferred schema in the `cloudFiles.schemaLocation` directory. When new columns appear in source files, Auto Loader can automatically evolve the schema (adding new columns). This is controlled by the `cloudFiles.schemaEvolutionMode` option (addNewColumns, rescue, failOnNewColumns, none). (Module 02, 07)

---

## Domain 4: Production Pipelines (6 questions)

### Question 32
In a Databricks multi-task job, how do you pass a small value (like a row count) from Task A to Task B?

A) Write the value to a Delta table and read it in Task B
B) Use `dbutils.jobs.taskValues.set()` in Task A and `dbutils.jobs.taskValues.get()` in Task B
C) Use shared variables in the SparkContext
D) Save the value to a notebook widget

**Answer: B**

**Explanation:** `dbutils.jobs.taskValues` is the built-in mechanism for passing small values between tasks in a multi-task job. Task A calls `.set(key="row_count", value=12345)` and Task B calls `.get(taskKey="task_a", key="row_count")`. This is designed for metadata, not large datasets. (Module 06)

---

### Question 33
Which cluster type is automatically created and destroyed by a Databricks job?

A) All-purpose cluster
B) SQL warehouse
C) Job cluster
D) High-concurrency cluster

**Answer: C**

**Explanation:** Job clusters are ephemeral -- they are created when a job run starts and terminated when it completes. This makes them more cost-effective than all-purpose clusters for production workloads because you only pay for the compute time actually used. (Module 06)

---

### Question 34
What is the difference between DLT Development mode and Production mode?

A) Development mode uses Python; Production mode uses SQL
B) Development mode reuses clusters and does not retry on failure; Production mode creates new clusters and retries on failure
C) Development mode processes a sample of data; Production mode processes all data
D) There is no difference; they are the same

**Answer: B**

**Explanation:** Development mode reuses the same cluster across pipeline runs (faster iteration) and does not retry failed tasks. Production mode creates a new cluster for each run and automatically retries failed tasks. Both modes process the full dataset. (Module 07)

---

### Question 35
Which scheduling method allows you to define a job that runs at 8:00 AM every weekday?

A) Continuous scheduling
B) Cron expression: `0 0 8 ? * MON-FRI`
C) Time-based trigger in the notebook
D) `dbutils.scheduler.cron("0 8 * * 1-5")`

**Answer: B**

**Explanation:** Databricks Jobs use cron expressions for scheduling. The expression `0 0 8 ? * MON-FRI` means "at 8:00:00 AM, every Monday through Friday." Cron scheduling is configured in the Jobs UI or via the API, not within the notebook code. (Module 06)

---

### Question 36
What should you do if a production job fails due to a transient error (like a cloud provider API timeout)?

A) Manually restart the job every time it fails
B) Configure automatic retries in the job settings
C) Switch to an all-purpose cluster to avoid failures
D) Add try/except blocks around every line of code

**Answer: B**

**Explanation:** Databricks Jobs support automatic retries for failed tasks. You can configure the maximum number of retries and the interval between attempts in the job or task settings. This handles transient failures (network issues, temporary service unavailability) without manual intervention. (Module 06, 09)

---

### Question 37
In a multi-task job, Task C depends on both Task A and Task B. When does Task C start?

A) As soon as either Task A or Task B completes
B) Only after both Task A and Task B complete successfully
C) Immediately when the job starts
D) After a configurable delay

**Answer: B**

**Explanation:** In a multi-task job with dependency configuration, a task starts only when ALL of its upstream dependencies complete successfully. If either Task A or Task B fails (and is not retried or retries are exhausted), Task C is skipped. This is the fan-in pattern. (Module 06)

---

## Domain 5: Data Governance (3 questions)

### Question 38
What is the correct namespace hierarchy in Unity Catalog?

A) `database.table`
B) `schema.table`
C) `catalog.schema.table`
D) `workspace.catalog.schema.table`

**Answer: C**

**Explanation:** Unity Catalog uses a three-level namespace: `catalog.schema.table` (also called `catalog.schema.object` since it applies to tables, views, and functions). This is different from the legacy Hive metastore which used a two-level namespace (`database.table`). (Module 08)

---

### Question 39
Which SQL statement grants read access to a table for a specific group?

A) `ALLOW SELECT ON TABLE my_table FOR analysts`
B) `GRANT SELECT ON TABLE catalog.schema.my_table TO analysts`
C) `PERMIT READ ON catalog.schema.my_table TO analysts`
D) `ADD PRIVILEGE SELECT ON my_table FOR GROUP analysts`

**Answer: B**

**Explanation:** The standard SQL `GRANT` syntax is used: `GRANT <privilege> ON <object_type> <object_name> TO <principal>`. Common privileges include SELECT, MODIFY, CREATE TABLE, ALL PRIVILEGES, and USAGE. Remember that USAGE must also be granted at the catalog and schema levels. (Module 08)

---

### Question 40
What is a dynamic view in the context of Unity Catalog security?

A) A view that automatically refreshes when underlying data changes
B) A view that uses functions like `current_user()` or `is_member()` to filter or mask data based on the querying user
C) A view that dynamically selects columns based on query parameters
D) A materialized view that caches results

**Answer: B**

**Explanation:** Dynamic views implement row-level and column-level security by using identity functions (`current_user()`, `is_member()`) in the view definition. Different users see different data when querying the same view. For example, `CASE WHEN is_member('admins') THEN ssn ELSE '***' END` masks the SSN column for non-admin users. (Module 08)

---

## Scoring Guide

| Score | Assessment | Recommendation |
|-------|-----------|----------------|
| 36-40 (90-100%) | Excellent -- ready for the exam | Schedule your exam within 1-2 weeks |
| 32-35 (80-89%) | Strong -- minor gaps to address | Review missed topics, retake in 1 week |
| 28-31 (70-79%) | Passing -- but thin margin | Study weak domains for 1-2 more weeks |
| 20-27 (50-69%) | Needs work | Revisit Modules 01-08, retake in 2-3 weeks |
| Below 20 (<50%) | Significant gaps | Complete Modules 01-08 before retaking |

### Score by Domain

Track your score per domain to identify weaknesses:

| Domain | Questions | Your Score | Passing (70%) |
|--------|-----------|------------|---------------|
| 1: Lakehouse Platform | 1-10 (10 Qs) | __/10 | 7 |
| 2: ELT with Spark SQL/Python | 11-22 (12 Qs) | __/12 | 8 |
| 3: Incremental Processing | 23-31 (9 Qs) | __/9 | 6 |
| 4: Production Pipelines | 32-37 (6 Qs) | __/6 | 4 |
| 5: Data Governance | 38-40 (3 Qs) | __/3 | 2 |
| **Total** | **40 Qs** | **__/40** | **28** |

---

## Next Steps

- If scoring below 70% in any domain, revisit the corresponding course modules.
- Work through the companion notebook for hands-on code practice.
- After improving, try the Professional exam guide (Topic 03) if pursuing that certification.
