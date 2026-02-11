# Distributed Computing

> Module 01 -- Topic 03 | Level: Beginner | Time: 50 min

## Learning Objectives

- Explain why distributed computing is necessary for big data workloads
- Describe how Spark partitions data across a cluster
- Differentiate between narrow and wide transformations
- Understand shuffle operations and their cost
- Explain data locality levels and their impact on performance
- Describe Spark's fault tolerance model based on lineage
- Understand serialization formats (Pickle vs Arrow) and speculative execution

## Conceptual Overview

### Why Distributed Computing?

A single machine has finite resources: CPU cores, memory, disk bandwidth, and network
throughput. When data grows beyond what one machine can process in a reasonable time, you
must distribute the work.

```
  Single Machine                    Distributed System
  ┌─────────────┐                   ┌──────┐ ┌──────┐ ┌──────┐
  │  100 TB of  │                   │ 33TB │ │ 33TB │ │ 33TB │
  │    data      │   ──────────>    │Node 1│ │Node 2│ │Node 3│
  │  1 CPU       │                   │ 8CPU │ │ 8CPU │ │ 8CPU │
  │  ~days       │                   │~hours│ │~hours│ │~hours│
  └─────────────┘                   └──────┘ └──────┘ └──────┘
```

Spark makes distributed computing accessible by:
1. Abstracting away the complexity of distributed coordination
2. Providing high-level APIs (DataFrames, SQL) that look like single-machine code
3. Automatically handling data partitioning, fault tolerance, and scheduling

### Partitioning Fundamentals

A **partition** is a chunk of data that Spark processes as a unit. Each partition is
handled by exactly one task on one executor.

```
  ┌───────────────────────────────────────────────┐
  │              Original Dataset                 │
  │  [row1, row2, row3, row4, row5, row6, ...]   │
  └───────────────────┬───────────────────────────┘
                      │ partition
       ┌──────────────┼──────────────┐
       ▼              ▼              ▼
  ┌─────────┐   ┌─────────┐   ┌─────────┐
  │ Part. 0 │   │ Part. 1 │   │ Part. 2 │
  │ row1    │   │ row3    │   │ row5    │
  │ row2    │   │ row4    │   │ row6    │
  └─────────┘   └─────────┘   └─────────┘
      │              │              │
   Task 0         Task 1         Task 2
  (Executor 1)   (Executor 2)   (Executor 1)
```

**Key principles:**

- More partitions = more parallelism (up to the number of available cores)
- Too few partitions = some cores sit idle; data skew is magnified
- Too many partitions = excessive overhead from scheduling and coordination
- **Rule of thumb**: 2-4 partitions per CPU core; each partition 128 MB - 200 MB

**What determines partition count?**

| Source | Default Partitions |
|--------|-------------------|
| `spark.range()` | `spark.default.parallelism` |
| `sc.parallelize()` | `spark.default.parallelism` |
| Reading files | Number of file blocks (128 MB each for HDFS/S3) |
| After shuffle | `spark.sql.shuffle.partitions` (default 200) |
| `repartition(n)` | Exactly `n` |
| `coalesce(n)` | Exactly `n` (can only decrease, no shuffle) |

### Data Locality

Spark tries to schedule tasks on the node where the data already resides to minimize
network transfer. The locality levels, from best to worst:

| Level | Meaning | Latency |
|-------|---------|---------|
| `PROCESS_LOCAL` | Data is in the same JVM (cached in memory) | Fastest |
| `NODE_LOCAL` | Data is on the same physical node (different JVM or disk) | Fast |
| `RACK_LOCAL` | Data is on a different node in the same rack | Moderate |
| `ANY` | Data must be fetched from a remote rack | Slowest |

Spark waits a configurable amount of time (`spark.locality.wait`, default 3s) before
falling back to a less-local placement. In Databricks, this is mostly managed for you.

### Shuffle Operations

A **shuffle** is the redistribution of data across partitions. It is the most expensive
operation in Spark because it involves:

1. Each task writes its output to local disk (shuffle write)
2. Data is transferred across the network to new partitions
3. Receiving tasks read the shuffled data (shuffle read)

```
  STAGE 1 (before shuffle)              STAGE 2 (after shuffle)
  ┌────────────┐                        ┌────────────┐
  │ Partition 0 │ ──── key A ────────> │ Partition 0 │  (all key A)
  │ A:1, B:2   │ ──── key B ──┐       │ A:1, A:3   │
  └────────────┘               │       └────────────┘
  ┌────────────┐               │       ┌────────────┐
  │ Partition 1 │ ──── key A ──│────> │ Partition 1 │  (all key B)
  │ A:3, B:4   │ ──── key B ──┘       │ B:2, B:4   │
  └────────────┘                       └────────────┘
```

**Operations that cause shuffles:**

- `groupBy()` / `groupByKey()` / `reduceByKey()`
- `join()` (unless one side is broadcast)
- `distinct()`
- `repartition()`
- `orderBy()` / `sort()`

### Narrow vs. Wide Transformations

This is one of the most important concepts for understanding Spark performance.

**Narrow transformations** -- each input partition contributes to at most one output
partition. No data movement between partitions. Can be pipelined.

```
  Narrow (map, filter, select, withColumn)

  Input          Output
  ┌──────┐       ┌──────┐
  │Part 0│ ────> │Part 0│   1:1 mapping
  └──────┘       └──────┘
  ┌──────┐       ┌──────┐
  │Part 1│ ────> │Part 1│   1:1 mapping
  └──────┘       └──────┘
  ┌──────┐       ┌──────┐
  │Part 2│ ────> │Part 2│   1:1 mapping
  └──────┘       └──────┘
```

**Wide transformations** -- each input partition may contribute to many output partitions.
Requires a shuffle. Creates a new stage.

```
  Wide (groupBy, join, repartition, orderBy)

  Input          Output
  ┌──────┐    ╱─ ┌──────┐
  │Part 0│ ──╱──>│Part 0│
  └──────┘  ╱ ╲  └──────┘
  ┌──────┐ ╱   ╲ ┌──────┐
  │Part 1│╱─────>│Part 1│
  └──────┘╲   ╱  └──────┘
  ┌──────┐ ╲ ╱   ┌──────┐
  │Part 2│──╲──> │Part 2│
  └──────┘   ╲   └──────┘
             All-to-all
```

**Performance implication**: Chain as many narrow transformations as possible before
triggering a wide transformation. Spark pipelines narrow transformations into a single
stage and processes them partition-by-partition without any network I/O.

### Repartition vs. Coalesce

| Method | Direction | Shuffle? | Use Case |
|--------|-----------|----------|----------|
| `repartition(n)` | Increase or decrease | Yes (full shuffle) | Rebalance after a skewed join; increase parallelism |
| `coalesce(n)` | Decrease only | No (moves partitions, no shuffle) | Reduce partitions before writing to fewer files |

```
  coalesce(2):  Merge partitions locally
  ┌──────┐
  │Part 0│ ─┐   ┌──────┐
  └──────┘  ├─> │Part 0│   (Part 0 + Part 1 combined)
  ┌──────┐  │   └──────┘
  │Part 1│ ─┘
  └──────┘       ┌──────┐
  ┌──────┐ ────> │Part 1│   (Part 2 stays as-is)
  │Part 2│       └──────┘
  └──────┘
```

### Serialization: Pickle vs. Arrow

When data moves between the JVM (Spark) and Python processes:

| Format | Speed | Use Case |
|--------|-------|----------|
| **Pickle** | Slower; row-by-row serialization | Default for RDD operations and older UDFs |
| **Apache Arrow** | Faster; columnar, zero-copy | `toPandas()`, `createDataFrame(pandas_df)`, Pandas UDFs |

Enable Arrow-based conversion:
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
```

In Databricks DBR 10+, Arrow is enabled by default for most operations.

### Fault Tolerance: Lineage

Spark achieves fault tolerance through **lineage** rather than data replication. Every
RDD/DataFrame remembers the sequence of transformations that created it.

```
  textFile("data.txt") ──> filter(contains "error") ──> map(parse_line) ──> groupByKey()
       │                        │                           │                  │
       └────────────────────────┴───────────────────────────┴──────────────────┘
                                     LINEAGE GRAPH

  If Partition 2 of the groupByKey result is lost:
  1. Spark traces the lineage back
  2. Re-reads Partition 2 of data.txt
  3. Re-applies filter and map
  4. Re-computes only the lost partition
```

Benefits:
- No need to replicate data across nodes (unlike HDFS replication)
- Only the lost partition is recomputed, not the entire dataset
- Works automatically -- no user intervention needed

### Speculative Execution

Sometimes a task runs slowly due to hardware issues, GC pauses, or data skew. Spark can
launch a **speculative copy** of the slow task on another executor. Whichever finishes
first wins; the other is killed.

```
  Normal:     Task A ────────────────────> done (10s)

  Straggler:  Task B ─────────────────────────────────> done (60s)
  Speculative:            Task B' ──────> done (12s)  ← wins!
```

Enable with:
```python
spark.conf.set("spark.speculation", "true")
```

Speculative execution is disabled by default because it can cause issues with
non-idempotent operations (e.g., writing to external databases).

## Hands-On Walkthrough

Open the companion notebook `03-distributed-computing_notebook.py` in Databricks. You will:

- Observe partition counts and control them with `repartition()` and `coalesce()`
- Run `explain()` to see shuffle stages in execution plans
- Identify narrow vs. wide transformations in real queries
- Measure the cost difference between narrow and wide operations

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|-------------------|----------------|
| Default block size | 128 MB (S3/HDFS) | 128 MB (DBFS/ADLS) | 128 MB (GCS/HDFS) |
| Arrow enabled by default | No (must set manually) | Yes (DBR 10+) | No (must set manually) |
| Speculative execution | Disabled by default | Disabled by default | Disabled by default |
| Shuffle service | External shuffle service | Databricks optimized shuffle | External shuffle service |

## Certification Tip

Exam questions frequently test:

- **Narrow vs. wide**: "Which of these transformations causes a shuffle?" -- Answer:
  `groupBy`, `join`, `repartition`, `distinct`, `orderBy`. Not: `map`, `filter`, `select`,
  `withColumn`, `coalesce`.
- **Partition count**: "After a shuffle, what determines partition count?" -- Answer:
  `spark.sql.shuffle.partitions` (default 200).
- **Fault tolerance**: "How does Spark recover from a lost partition?" -- Answer:
  It recomputes it using the lineage (DAG of transformations).

## Key Takeaways

- **Partitions** are the fundamental unit of parallelism in Spark
- **Narrow transformations** (map, filter) process partitions independently -- no shuffle
- **Wide transformations** (groupBy, join) require a shuffle -- data moves across the network
- **Shuffles** are expensive: minimize them by designing your data flow carefully
- **Coalesce** reduces partitions without a shuffle; **repartition** requires one
- **Lineage** provides fault tolerance: Spark can recompute lost partitions from the DAG
- **Arrow** significantly speeds up Python-JVM data transfers (especially toPandas)

## Next Steps

Continue to [04 - RDDs Fundamentals](04-rdds-fundamentals.md) to learn about Spark's
original data abstraction and when it is still the right tool.
