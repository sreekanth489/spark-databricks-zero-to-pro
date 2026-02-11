# Catalyst Optimizer

> Module 01 -- Topic 07 | Level: Beginner | Time: 50 min

## Learning Objectives

- Explain the role of the Catalyst optimizer in Spark's execution pipeline
- Trace a query through logical and physical planning
- Identify key optimization rules: predicate pushdown, column pruning, constant folding, join reorder
- Read and interpret `explain()` output
- Understand Adaptive Query Execution (AQE) and when it changes plans at runtime
- Describe whole-stage code generation and Photon (Databricks)

## Conceptual Overview

### What Is Catalyst?

Catalyst is Spark SQL's query optimizer. It takes your DataFrame operations or SQL queries
and transforms them into an efficient execution plan before any data is processed. This
is why DataFrames and Spark SQL are faster than raw RDD operations -- you write the "what"
and Catalyst figures out the "how."

```
  Your Code
  (DataFrame API / SQL)
       │
       ▼
  ┌──────────────────────┐
  │   Unresolved Logical  │   Column names, table references (not yet validated)
  │   Plan                │
  └──────────┬───────────┘
             │ Analysis (resolve names, types)
             ▼
  ┌──────────────────────┐
  │   Resolved Logical    │   All names resolved, types checked
  │   Plan                │
  └──────────┬───────────┘
             │ Logical Optimization (rule-based)
             ▼
  ┌──────────────────────┐
  │   Optimized Logical   │   Predicate pushdown, column pruning, etc.
  │   Plan                │
  └──────────┬───────────┘
             │ Physical Planning (cost-based)
             ▼
  ┌──────────────────────┐
  │   Physical Plan       │   Concrete execution strategy chosen
  │   (Selected)          │
  └──────────┬───────────┘
             │ Code Generation
             ▼
  ┌──────────────────────┐
  │   RDD Execution       │   Actual data processing on the cluster
  └──────────────────────┘
```

### Logical Plan

The logical plan describes **what** operations to perform, without specifying **how**.

- **Unresolved plan**: Raw translation of your code. Column names and table references are
  not yet validated.
- **Resolved plan**: The analyzer resolves column names, table references, and data types
  using the catalog.
- **Optimized plan**: Optimization rules are applied to rewrite the plan for efficiency.

### Physical Plan

The physical plan describes **how** to execute the query. It includes:

- Specific algorithms for joins (broadcast hash join, sort-merge join, etc.)
- Scan strategies (file scan, in-memory scan)
- Exchange (shuffle) operators
- Sort operators

Spark may generate multiple physical plans and use a cost model to pick the best one.

### Key Optimization Rules

#### 1. Predicate Pushdown

Filters are pushed as close to the data source as possible, reducing the amount of data
read and processed.

```
  BEFORE (no pushdown):               AFTER (predicate pushdown):
  ┌────────────┐                      ┌────────────┐
  │   Filter   │                      │   Project  │
  │ age > 30   │                      │ name, age  │
  └─────┬──────┘                      └─────┬──────┘
        │                                   │
  ┌─────▼──────┐                      ┌─────▼──────┐
  │  Project   │                      │   Scan     │
  │ name, age  │                      │ (age > 30) │ ◄── filter pushed into scan
  └─────┬──────┘                      └────────────┘
        │
  ┌─────▼──────┐
  │   Scan     │ ◄── reads ALL rows
  │ (full scan)│
  └────────────┘
```

For Parquet and Delta files, predicate pushdown can skip entire row groups or files,
dramatically reducing I/O.

#### 2. Column Pruning

Only the columns needed for the final result are read from the source. Unnecessary columns
are dropped early.

```
  Table has: id, name, age, salary, department, address, phone, email

  Query: SELECT name, salary FROM employees WHERE age > 30

  Without pruning: reads all 8 columns
  With pruning:    reads only name, age, salary (age for filter, name + salary for output)
```

For columnar formats like Parquet, this means entire column chunks are skipped during I/O.

#### 3. Constant Folding

Expressions with constants are evaluated at plan time, not at runtime for every row.

```
  BEFORE:  col("price") * (1 + 0.08)    <-- 1 + 0.08 computed per row
  AFTER:   col("price") * 1.08           <-- constant folded at compile time
```

#### 4. Join Reorder

When multiple tables are joined, Catalyst reorders them to minimize shuffle and
intermediate data sizes. Smaller tables are joined first.

```
  BEFORE (user wrote):          AFTER (optimizer reorders):
  A JOIN B JOIN C               C JOIN A JOIN B
  (A=10GB, B=5GB, C=100KB)     (C=100KB first, reduces intermediate)
```

#### 5. Boolean Expression Simplification

```
  BEFORE: WHERE (age > 30 AND true) OR false
  AFTER:  WHERE age > 30
```

### Reading explain() Output

The `explain()` method shows the execution plan without running the query.

**Basic explain:**
```python
df.filter(col("age") > 30).select("name", "salary").explain()
```

**Extended explain (Spark 3.0+):**
```python
df.explain(mode="extended")    # shows all 4 plan stages
df.explain(mode="formatted")   # formatted with section headers
df.explain(mode="cost")        # includes cost estimates
df.explain(mode="codegen")     # shows generated Java code
```

**Explain output structure (formatted mode):**
```
== Physical Plan ==
* Project [name, salary]
+- * Filter (age > 30)
   +- * Scan [name, age, salary]

== Optimized Logical Plan ==
Project [name, salary]
+- Filter (age > 30)
   +- Relation [name, age, salary, ...] parquet

== Analyzed Logical Plan ==
name: string, salary: double
Project [name, salary]
+- Filter (age > 30)
   +- Relation [name, age, salary, department, ...] parquet
```

**Key operators to recognize:**

| Operator | Meaning |
|----------|---------|
| `Scan` / `FileScan` | Reading data from source |
| `Filter` | Row filtering |
| `Project` | Column selection |
| `Exchange` | Shuffle (data redistribution) |
| `HashAggregate` | Aggregation using hash tables |
| `SortMergeJoin` | Join via sort-merge algorithm |
| `BroadcastHashJoin` | Join by broadcasting small table |
| `Sort` | Sorting data |
| `WholeStageCodegen` | Fused code generation block (marked with `*`) |

### Adaptive Query Execution (AQE)

AQE (introduced in Spark 3.0, enabled by default in Spark 3.2+) allows Spark to re-
optimize the plan **at runtime** based on actual data statistics collected during
execution.

```
  ┌──────────────────────────────────────────────────────────┐
  │                    Without AQE                           │
  │  Plan is fixed at compile time                           │
  │  Estimates may be wrong ──> suboptimal plan              │
  └──────────────────────────────────────────────────────────┘

  ┌──────────────────────────────────────────────────────────┐
  │                     With AQE                             │
  │  Plan compiles ──> Stage 1 runs ──> collect stats ──>    │
  │  re-optimize remaining stages based on actual data       │
  └──────────────────────────────────────────────────────────┘
```

**AQE capabilities:**

1. **Coalesce shuffle partitions**: Combines small partitions after a shuffle to reduce
   task overhead. Instead of 200 partitions with 1 KB each, AQE merges them into fewer,
   larger partitions.

2. **Convert sort-merge join to broadcast join**: If a join input turns out to be small
   (after filtering), AQE can switch to a more efficient broadcast join at runtime.

3. **Optimize skew joins**: Detects skewed partitions (one key has disproportionately
   many rows) and splits them into smaller sub-partitions.

```
  Without AQE (skew):              With AQE (skew handling):
  ┌───────┐                        ┌───────┐
  │Part 0 │ 100 rows               │Part 0 │ 100 rows
  └───────┘                        └───────┘
  ┌───────┐                        ┌───────┐
  │Part 1 │ 1M rows ◄── skew!     │Part 1a│ 500K rows
  └───────┘                        └───────┘
  ┌───────┐                        ┌───────┐
  │Part 2 │ 100 rows               │Part 1b│ 500K rows  ◄── split!
  └───────┘                        └───────┘
                                   ┌───────┐
                                   │Part 2 │ 100 rows
                                   └───────┘
```

Enable/configure AQE:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")                    # default in 3.2+
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Whole-Stage Code Generation

Instead of interpreting the plan operator-by-operator, Spark generates optimized Java
bytecode that fuses multiple operators into a single function. This eliminates virtual
function call overhead and enables CPU-efficient processing.

In `explain()` output, operators prefixed with `*` are part of a whole-stage codegen
block:

```
*(1) Project [name, salary]
+- *(1) Filter (age > 30)
   +- *(1) FileScan parquet [name, age, salary]
```

The `(1)` indicates these three operators are fused into a single generated function.

### Photon (Databricks)

Photon is Databricks' proprietary native vectorized engine written in C++. It replaces
Spark's JVM-based execution for supported operations with faster native code.

```
  Standard Spark:     JVM bytecode (whole-stage codegen)
  Databricks Photon:  Native C++ vectorized execution

  Photon accelerates:
  - Scans (Parquet, Delta)
  - Filters
  - Aggregations
  - Joins
  - Shuffles
```

Photon is enabled on Photon-accelerated clusters in Databricks. You do not need to change
your code -- the same SQL and DataFrame operations automatically use Photon when available.

## Hands-On Walkthrough

Open the companion notebook `07-catalyst-optimizer_notebook.py` in Databricks. You will:

- Use `explain()` with different modes to read execution plans
- Observe predicate pushdown and column pruning
- Compare plans with and without optimizations
- See AQE in action by checking post-execution plan changes
- Examine whole-stage code generation markers

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|-------------------|----------------|
| Catalyst | Available (all Spark) | Available | Available (all Spark) |
| AQE | Spark 3.0+ (manual enable pre-3.2) | Enabled by default | Spark 3.0+ (manual enable pre-3.2) |
| Photon | Not available | Available on Photon clusters | Not available |
| explain() | Available | Available + visual plans in UI | Available |

Databricks provides a visual query plan in the SQL tab of the Spark UI that is easier to
read than the text-based `explain()` output.

## Certification Tip

The Databricks certification tests your ability to:

- Read `explain()` output and identify whether predicate pushdown occurred
- Know which operations trigger shuffles (look for `Exchange` in the plan)
- Understand that DataFrame and SQL produce identical plans
- Know what AQE does (coalesce partitions, convert join types, handle skew)
- Recognize that `BroadcastHashJoin` is used when one side is small

When a question asks about performance optimization, think about Catalyst first:
- Can a filter be pushed down?
- Are unnecessary columns being read?
- Is a broadcast join possible?

## Key Takeaways

- **Catalyst** transforms your code into an optimized execution plan through four phases:
  analysis, logical optimization, physical planning, and code generation
- **Predicate pushdown** filters data at the source, reducing I/O dramatically
- **Column pruning** reads only needed columns, especially powerful with Parquet
- **AQE** re-optimizes at runtime based on actual data statistics -- handles skew,
  coalesces partitions, and converts join strategies
- **Whole-stage code generation** fuses operators into efficient JVM bytecode
- **Photon** (Databricks) replaces JVM execution with native C++ for even faster processing
- Use `explain(mode="formatted")` to understand what Spark actually does with your code
- DataFrames and SQL go through the same optimizer -- choose whichever API you prefer

## Next Steps

You have completed Module 01. Continue to
[Module 02: Data Ingestion & Transformation](../02-data-ingestion-transformation/README.md)
to learn how to read, write, and transform data at scale.
