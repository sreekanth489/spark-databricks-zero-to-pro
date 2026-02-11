# Photon & Serverless
> Module 05 — Topic 07 | Level: Intermediate-Advanced | Time: 40 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain what the Photon Query Engine is and why Databricks built it
2. Identify which operations Photon accelerates (and which it does not)
3. Understand the full query pipeline: SQL Query -> Catalyst -> Photon Executor
4. Evaluate when to use Photon-enabled clusters vs standard Spark
5. Describe Databricks Serverless compute and its cost/performance trade-offs
6. Apply a cost optimization mindset to cluster and warehouse selection

---

## Conceptual Overview

### What Is Photon?

**Photon Query Engine is a high-performance vectorized query engine written in C++.**
Databricks rewrote everything from Scala to C++ so that it could be faster and more
performant.

Why C++? The JVM (Java/Scala) introduces overhead from garbage collection, object
headers, and memory management. C++ gives direct control over memory layout, enabling
vectorized (SIMD) operations that process batches of values in a single CPU instruction.

```
Traditional Spark (Scala/JVM):                Photon (C++):
+----------------------------------+          +----------------------------------+
| JVM Executor                     |          | Photon Executor                  |
|                                  |          |                                  |
| Row 1: process -> GC pause       |          | Batch of 1024 rows:              |
| Row 2: process -> GC pause       |          |   process ALL in one CPU op      |
| Row 3: process -> GC pause       |          |                                  |
| ...                              |          | Next batch of 1024 rows:         |
| Row N: process -> GC pause       |          |   process ALL in one CPU op      |
|                                  |          |                                  |
| Overhead: GC, object headers,    |          | No GC, no object overhead,       |
| row-at-a-time processing         |          | vectorized batch processing      |
+----------------------------------+          +----------------------------------+
```

### The Full Query Pipeline with Photon

```
+------------------+
| SQL Query /      |
| DataFrame API    |
+------------------+
        |
        v
+------------------+
| Catalyst         |    Compile-time optimization:
| Optimizer        |    predicate pushdown, column pruning,
|                  |    join reordering, constant folding
+------------------+
        |
        v
+------------------+
| Optimized Plan   |    AQE may refine this at runtime
+------------------+
        |
        v
+------------------+
| Photon Executor  |    The C++ execution engine:
|                  |    - Goes to your table (Parquet on cloud storage)
|                  |    - Scans ONLY columns that are required
|                  |    - Applies the filter
|                  |    - Executes aggregations, joins, sorts
|                  |    - Returns results
+------------------+
        |
        v
+------------------+
| Results          |
+------------------+
```

**Photon is your actual executor.** It goes to your table where the data is stored in
Parquet format, scans only the columns that are required, then applies the filter. It
handles the physical execution of the plan that Catalyst optimized.

### What Photon Accelerates

| Operation | Photon Benefit | Why |
|-----------|---------------|-----|
| **Scans** (Parquet/Delta reads) | High | Vectorized column reads, predicate pushdown |
| **Filters** | High | SIMD comparisons on column batches |
| **Aggregations** (SUM, AVG, COUNT) | High | Vectorized accumulation |
| **Joins** (hash, sort-merge) | High | Optimized hash tables in C++ |
| **Sorts** | High | Cache-friendly sorting algorithms |
| **String operations** | High | Optimized string processing |
| **UDFs (Python)** | None | Python UDFs run in Python, not Photon |
| **UDFs (Scala)** | None | Scala UDFs run in JVM, not Photon |
| **RDD operations** | None | Photon only handles DataFrame/SQL API |
| **ML training** | Limited | Model training is mostly custom code |

### Performance: Photon vs Standard Spark

Typical benchmarks show 2x-8x improvement for SQL/DataFrame workloads:

```
Benchmark: TPC-DS 10TB

                    Standard Spark    Photon          Improvement
                    ──────────────    ──────          ───────────
Scan + Filter       120 sec           30 sec          4.0x
Aggregations        85 sec            25 sec          3.4x
Joins               200 sec           55 sec          3.6x
Complex Queries     450 sec           110 sec         4.1x
                    ──────────────    ──────          ───────────
Total               855 sec           220 sec         3.9x
```

### Enabling Photon

Photon is enabled at the cluster level, not at the query level:

```
Cluster Configuration:
  Runtime: Databricks Runtime 12.x+ (Photon-enabled)
  OR
  Access mode: Serverless (Photon included)

  Check in cluster config:
    "Photon Acceleration" toggle -> ON

  Verify in notebook:
    spark.conf.get("spark.databricks.photon.enabled")
    # "true"
```

### Common Performance Tuning Scenarios

When you encounter performance problems, think about three categories:

```
+-------------------------------------------------------+
|           PERFORMANCE TUNING CHECKLIST                 |
+-------------------------------------------------------+
| 1. SKEW                                               |
|    - One partition has much more data than others      |
|    - Fix: salt keys, AQE skew join, repartition       |
|                                                       |
| 2. NEED MORE MEMORY                                   |
|    - Spill to disk, OOM errors, high GC               |
|    - Fix: increase executor memory, reduce partition   |
|           count, optimize data types                   |
|                                                       |
| 3. NEED TO BE MOVED TO PHOTON                         |
|    - Running on standard Spark for SQL/DF workloads   |
|    - Fix: switch to Photon-enabled cluster            |
|    - Immediate 2-8x improvement for supported ops     |
+-------------------------------------------------------+
```

### Databricks Serverless Compute

Serverless eliminates cluster management entirely. No waiting for clusters to start,
no over-provisioning, no under-provisioning.

```
TRADITIONAL CLUSTERS:                    SERVERLESS:
+---------------------------+            +---------------------------+
| You choose:               |            | Databricks manages:       |
|   - Node type             |            |   - Node type (automatic) |
|   - Number of workers     |            |   - Worker count (elastic)|
|   - Autoscaling range     |            |   - Scaling (instant)     |
|   - Cluster start/stop    |            |   - Start/stop (instant)  |
|                           |            |                           |
| Time to first query:      |            | Time to first query:      |
|   2-8 minutes             |            |   seconds                 |
|                           |            |                           |
| Idle cost: YES            |            | Idle cost: NO             |
| (cluster running, no work)|            | (pay only when running)   |
+---------------------------+            +---------------------------+
```

Think of it like the self-hosting vs managed service analogy:
- **Self-hosting (traditional clusters)**: Like being the caterer, photographer, and event
  planner. You provision nodes, configure settings, manage lifecycle.
- **Managed service (serverless)**: Like hiring an event planner. You describe what you
  want, Databricks handles everything.

### Serverless SQL Warehouses

For BI and SQL analytics workloads, Serverless SQL Warehouses provide:

```
+---------------------------------------------------+
| Serverless SQL Warehouse                           |
|                                                    |
| - Instant start (no cold-start waiting)            |
| - Auto-scaling (0 to N based on query load)        |
| - Photon included (always-on)                      |
| - Optimized for BI tools (Tableau, Power BI)       |
| - Pay per query second (no idle cost)              |
| - Built-in query caching                           |
+---------------------------------------------------+
```

### Cost Comparison

| Dimension | Standard Cluster | Photon Cluster | Serverless |
|-----------|-----------------|----------------|------------|
| **Startup time** | 2-8 min | 2-8 min | Seconds |
| **Idle cost** | Full cluster cost | Full cluster cost | None |
| **Per-hour cost** | Lowest DBU rate | ~2x DBU rate | ~3x DBU rate |
| **Query speed** | Baseline | 2-8x faster | 2-8x faster (Photon) |
| **Total cost** | High for bursty | Lower (faster = shorter) | Lowest for bursty |
| **Management** | Full | Full | None |

**Key insight on cost**: Photon clusters cost more per hour but finish queries faster.
If a query takes 4x less time on Photon, you pay 2x the rate but for 1/4 the time =
50% less total cost. The same logic applies to serverless for bursty workloads.

### Cost Optimization Mindset

```
COST OPTIMIZATION = minimize (compute_time x hourly_rate + storage_cost)

Strategies:
1. How long your servers are running -- minimize with Photon (faster queries)
2. How much compute they are using -- minimize with serverless (no idle cost)
3. How much storage you use -- minimize with VACUUM, Z-ORDER compression
4. How much data you scan -- minimize with partition pruning, data skipping
```

---

## Hands-On Walkthrough

See the companion notebook `07-photon-serverless_notebook.py` for:

1. Checking if your cluster has Photon enabled
2. Running benchmark queries and comparing execution plans
3. Observing Photon-specific metrics in the Spark UI SQL tab
4. Demonstrating the pipeline: SQL -> Catalyst -> Photon Executor -> Results
5. Cost analysis template for cluster selection

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR | Google Dataproc |
|---------|-----------|---------|-----------------|
| Photon | Databricks exclusive | Not available | Not available |
| Serverless compute | Serverless clusters + SQL warehouses | EMR Serverless (different) | Dataproc Serverless (different) |
| Vectorized execution | Photon (C++) | Spark 3.x codegen (JVM) | Spark 3.x codegen (JVM) |
| Auto-scaling | Built-in (enhanced) | EMR auto-scaling | Dataproc auto-scaling |

**Note**: Photon is exclusively a Databricks feature. Open-source Spark uses whole-stage
code generation (codegen) in the JVM, which is good but not as fast as native C++
vectorized execution.

---

## Certification Tip

**Exam question pattern**: "What is Photon?"
Answer: A high-performance vectorized query engine **written in C++** that replaces the
Spark JVM executor for supported operations. Key phrase: "native C++ execution engine."

**Also tested**: "Which operations benefit from Photon?"
Answer: Scans, filters, aggregations, joins, sorts, string operations. NOT Python/Scala
UDFs, NOT RDD operations.

**Serverless question**: "What is the main advantage of serverless SQL warehouses?"
Answer: Instant startup, no idle cost, automatic scaling. Photon is always enabled.

---

## Key Takeaways

1. **Photon is a C++ vectorized query engine** -- Databricks rewrote the executor from
   Scala to C++ for 2-8x performance improvement
2. **The pipeline**: SQL Query -> Catalyst Optimizer -> Optimized Plan -> Photon Executor
   -> Results
3. **Photon accelerates**: scans, filters, aggregations, joins, sorts. It does NOT help
   Python UDFs or RDD operations.
4. **Performance tuning triad**: check for Skew, check if you need more memory, check if
   you need to move to Photon
5. **Serverless** eliminates cluster management, provides instant start, and has no idle cost
6. **Cost optimization**: faster queries on Photon = less total cost despite higher hourly
   rate. Serverless = no idle cost for bursty workloads.

---

## Module Complete

Congratulations! You have completed Module 05: Performance Optimization. You now
understand the full performance stack from Spark internals (plans, partitions, cache) to
Databricks-specific features (AQE, file layout, Photon, serverless).

**Next Module**: [Module 06 - Databricks Platform Features](../06-databricks-platform/README.md)
