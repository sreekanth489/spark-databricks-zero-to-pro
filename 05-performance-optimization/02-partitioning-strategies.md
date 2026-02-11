# Partitioning Strategies
> Module 05 — Topic 02 | Level: Intermediate-Advanced | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain why "partition is a unit of parallelism" and what that means in practice
2. Choose the right number of partitions for your workload
3. Use `repartition(n)` (round-robin) vs `repartition(n, col)` (hash) correctly
4. Use `coalesce()` to reduce partitions with minimum data movement
5. Apply partition pruning when reading partitioned Delta/Parquet data
6. Configure `spark.sql.shuffle.partitions` for your cluster size

---

## Conceptual Overview

### Partition = Unit of Parallelism

This is the single most important concept in Spark performance.

A **partition** is a chunk of your data that lives on one machine and is processed by
one CPU core. The number of partitions determines how many tasks run in parallel. If
you have 8 cores and 8 partitions, all cores work simultaneously. If you have 8 cores
and 2 partitions, 6 cores sit idle.

```
Cluster: 3 Nodes, 4 cores each = 12 total cores

  Node 1              Node 2              Node 3
  +----+----+         +----+----+         +----+----+
  | P0 | P1 |         | P4 | P5 |         | P8 | P9 |
  +----+----+         +----+----+         +----+----+
  | P2 | P3 |         | P6 | P7 |         | P10| P11|
  +----+----+         +----+----+         +----+----+

  Your DataFrame is distributed across all machines and partitions.
  Spark provides abstractions so you don't have to worry about these details.
  But when it comes to performance, these details matter.
```

**The number of partitions that you have -- or the number of parallel tasks that you are
running -- is a number that you have to decide with a lot of care. You need to keep a
right balance. You don't want too many workers, and you don't want too few workers.**

| Problem | Symptom | Fix |
|---------|---------|-----|
| Too few partitions | Some cores idle, slow execution | Increase partitions |
| Too many partitions | Excessive task scheduling overhead, tiny files | Decrease partitions |
| Right-sized | Each partition ~128 MB, all cores utilized | Target this |

### The 128 MB Rule of Thumb

Aim for partitions of approximately 128 MB each. This balances parallelism with overhead:

```
Total Data Size: 10 GB
Target Partition Size: 128 MB
Ideal Partitions: 10,000 MB / 128 MB = ~80 partitions
```

### repartition() vs coalesce()

These are your two tools for controlling partition count. They behave very differently.

#### repartition(n) -- Round Robin

When you call `repartition(n)` without specifying a key column, Spark distributes
records across `n` partitions in a round-robin fashion. This is a **full shuffle** --
every record may move to a different node.

```
BEFORE: 4 partitions                 AFTER: repartition(6)
+--------+--------+--------+--------+   FULL SHUFFLE (round-robin)
| AABBCC | DDDDEE | FFFFFF | GGHH   |   ----->
+--------+--------+--------+--------+

+------+------+------+------+------+------+
| AD F | B EF | CDFG | AE G | BD H | CF H|
+------+------+------+------+------+------+
Records distributed evenly but randomly across 6 partitions
```

#### repartition(n, col) -- Hash Partitioning

When you specify a key column, Spark uses a **hash function** to determine which
partition each record goes to. Every record with the same key value ends up in the
same partition.

```
df.repartition(6, "studio")

Hash("Warner Bros") = 2   -->  Partition 2
Hash("Disney")      = 5   -->  Partition 5
Hash("Universal")   = 0   -->  Partition 0

RESULT:
+------------+------------+------------+------------+------------+------------+
| Universal  | Paramount  | Warner Bros| Sony       | Lionsgate  | Disney     |
| MGM        |            | Fox        |            |            |            |
+------------+------------+------------+------------+------------+------------+
  Partition 0  Partition 1  Partition 2  Partition 3  Partition 4  Partition 5

Every record for one studio is in one partition.
When you groupBy("studio"), Spark does NOT have to move data across the network
because records for a single studio are already on the same node.
```

**This is where the skill of data engineering comes into play.** If you know you are
going to do multiple operations based on "studio" as the key -- groupBy, joins, window
functions -- then it is better to create a DataFrame that is optimized for studio-based
operations by hash-partitioning on that key.

Use `.explain()` to prove the difference:

```python
# Without pre-partitioning: plan shows Exchange (shuffle)
movies_df.groupBy("studio").count().explain()
# == Physical Plan ==
# HashAggregate -> Exchange hashpartitioning(studio) -> HashAggregate -> Scan

# With pre-partitioning: NO Exchange (no shuffle!)
optimized_df = movies_df.repartition(6, "studio")
optimized_df.groupBy("studio").count().explain()
# == Physical Plan ==
# HashAggregate -> HashAggregate -> Scan   <-- no Exchange!
```

#### coalesce(n) -- Reduce Partitions with Minimum Data Movement

Coalesce is used to **reduce** the number of partitions with **minimum data movement**.
It minimizes data movement; it does not prevent it completely. It will only reduce
partitions -- it will never increase them.

```
BEFORE: 6 partitions
+----+----+----+----+----+----+
| P0 | P1 | P2 | P3 | P4 | P5 |
+----+----+----+----+----+----+

coalesce(3):  merges adjacent partitions (minimal movement)
+----------+----------+----------+
| P0 + P1  | P2 + P3  | P4 + P5  |
+----------+----------+----------+

repartition(3): full shuffle (all data may move)
+-----------+-----------+-----------+
| shuffled  | shuffled  | shuffled  |
+-----------+-----------+-----------+
```

**Benefits of Coalesce:**
1. **Eliminates task overhead from too many small partitions** -- fewer tasks to schedule
2. **Optimizes file output** -- writing 3 large files instead of 1000 tiny files
3. **Minimizes data movement** -- unlike repartition, it avoids a full shuffle

**When to use which:**

| Scenario | Use | Why |
|----------|-----|-----|
| Reduce partition count | `coalesce(n)` | Minimum data movement |
| Increase partition count | `repartition(n)` | Coalesce cannot increase |
| Partition by column | `repartition(n, col)` | Hash-based co-location |
| Write fewer output files | `coalesce(n)` | Optimizes file output |

### Partition Pruning

When your data on disk is organized by a partition column (e.g., `partitionBy("year")`),
Spark can skip entire directories of data that do not match your filter:

```
/data/movies/
  year=2020/    <-- only this directory is read
  year=2021/    <-- skipped
  year=2022/    <-- skipped
  year=2023/    <-- skipped

SELECT * FROM movies WHERE year = 2020
-- Spark reads 1 directory instead of 4 = 75% less I/O
```

### The Cluster Architecture Connection

```
+---------------------------+
|       Driver Program      |  -- divides data into partitions
|  (SparkContext, plans)    |  -- assigns tasks to executors
+---------------------------+
            |
   +--------+--------+
   |                  |
   v                  v
+----------+    +----------+
| Executor |    | Executor |    Cluster Manager (Databricks-managed,
| Node 1   |    | Node 2   |    YARN, Kubernetes, etc.) handles node
| [P0][P1] |    | [P2][P3] |    lifecycle: creating, replacing failed
+----------+    +----------+    nodes. But task assignment is done
                                by the Driver Program.
```

Think of it like event planning:
- **Self-hosting** (YARN, Kubernetes): You are the caterer, photographer, and event
  planner. You provision the physical/VM nodes, install Spark, manage configs.
- **Managed Service** (Databricks): The event planner handles everything. You just
  specify how many guests (workers) you need and the type of event (workload).

### spark.sql.shuffle.partitions

After any wide transformation (groupBy, join), Spark creates this many partitions:

```python
# Default: 200 partitions after every shuffle
spark.conf.set("spark.sql.shuffle.partitions", "200")

# For small datasets, 200 is way too many -- you get tiny partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")

# For large datasets, 200 may be too few
spark.conf.set("spark.sql.shuffle.partitions", "2000")

# Best practice: let AQE handle it (Databricks default)
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

---

## Hands-On Walkthrough

See the companion notebook `02-partitioning-strategies_notebook.py` for:

1. Checking default partition count on read
2. Repartitioning round-robin vs by key (using movies/studios dataset)
3. Proving shuffle elimination with `.explain()` after hash repartition
4. Using coalesce to optimize file output
5. Writing and reading partitioned data with partition pruning
6. Tuning `spark.sql.shuffle.partitions`

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR | Google Dataproc |
|---------|-----------|---------|-----------------|
| Default shuffle partitions | 200 (AQE adjusts) | 200 | 200 |
| AQE partition coalescing | Enabled by default | Must enable AQE | Must enable AQE |
| Auto-optimize writes | `optimizedWrite` setting | Not available | Not available |
| Delta partitionBy | Full support | Open-source Delta | Open-source Delta |

---

## Certification Tip

**Frequently tested**: "What is the difference between `repartition()` and `coalesce()`?"

Key distinctions the exam expects:
- `coalesce(n)` can only **reduce** partitions; `repartition(n)` can increase or decrease
- `coalesce` minimizes data movement (no full shuffle); `repartition` does a full shuffle
- `repartition(n, col)` uses hash partitioning; `coalesce` has no column option
- After `repartition(n, "studio")`, a subsequent `groupBy("studio")` avoids a shuffle

---

## Key Takeaways

1. **Partition is a unit of parallelism** -- get the count right for your cluster
2. **repartition(n)** distributes round-robin (full shuffle); **repartition(n, col)**
   uses hash to co-locate records by key (eliminates shuffles on subsequent operations)
3. **coalesce(n)** reduces partitions with minimum data movement -- use it to optimize
   file output and eliminate small-partition overhead
4. **Hash repartitioning** is a data engineering skill: if you know your downstream
   operations use a specific key, pre-partition on that key
5. **128 MB per partition** is the target; use `spark.sql.shuffle.partitions` or AQE
6. **Partition pruning** on disk-partitioned data can skip 90%+ of I/O

---

## Next Steps

Now that you control how data is distributed, learn when to keep it in memory:
[03 - Caching & Persistence](03-caching-persistence.md)
