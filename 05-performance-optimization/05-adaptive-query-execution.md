# Adaptive Query Execution (AQE)
> Module 05 — Topic 05 | Level: Intermediate-Advanced | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain what AQE is and why it exists (runtime vs compile-time optimization)
2. Describe AQE's three core features: partition coalescing, join switching, skew handling
3. Configure AQE parameters for your workload
4. Compare execution plans before and after AQE
5. Understand how AQE fits into the Catalyst pipeline and cost model

---

## Conceptual Overview

### Why AQE Exists

Traditional query optimization happens at **compile time** -- before any data is read.
The optimizer makes decisions based on table statistics, which can be stale, missing, or
inaccurate. AQE fixes this by **re-optimizing at runtime**, using real data statistics
observed during execution.

### The Full Pipeline (with AQE)

```
SQL Query / DataFrame API
        |
        v
+-------------------------------+
| Catalyst Query Optimizer       |
| (Compile-time optimization)    |
|                                |
| Unresolved Logical Plan        |
|        |                       |
|        v  (Catalog)            |
| Resolved Logical Plan          |
|        |                       |
|        v  (Logical Optimizer)  |
| Optimized Logical Plan         |
|        |                       |
|        v  (Physical Planning)  |
| Physical Plans (candidates)    |
+-------------------------------+
        |
        v
+-------------------------------+
| Cost Model + AQE               |  <-- THIS IS THE KEY
| (Runtime re-optimization)      |
|                                |
| After each shuffle stage:      |
| 1. Observe actual data sizes   |
| 2. Re-optimize remaining plan  |
| 3. Adjust partitions/joins     |
+-------------------------------+
        |
        v
+-------------------------------+
| Best Physical Plan             |
| (continuously refined)         |
+-------------------------------+
        |
        v
+-------------------------------+
| Cluster Execution              |
+-------------------------------+
```

The cost model uses AQE to figure out which plan is going to be the best in terms of
performance and compute cost. AQE operates at shuffle boundaries -- after a shuffle
stage completes, Spark has real statistics about the data and can adjust the plan.

### AQE Feature 1: Dynamically Coalescing Shuffle Partitions

**Problem**: `spark.sql.shuffle.partitions` defaults to 200. For small datasets, this
creates 200 tiny partitions. For large datasets, it may be too few.

**AQE Solution**: After a shuffle, AQE examines actual partition sizes and merges small
partitions together.

```
BEFORE AQE coalescing (200 shuffle partitions):
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+   ...   +--+
|1K|2K|1K|0 |1K|0 |2K|1K|0 |1K|0 |1K|2K|0 |1K|   ...   |1K|
+--+--+--+--+--+--+--+--+--+--+--+--+--+--+--+   ...   +--+
  200 partitions, most nearly empty = 200 tasks with tiny work

AFTER AQE coalescing (automatically reduced to 5 partitions):
+------------+------------+------------+------------+------------+
|   ~2.5 MB  |   ~2.5 MB  |   ~2.5 MB  |   ~2.5 MB  |   ~2.5 MB |
+------------+------------+------------+------------+------------+
  5 partitions, right-sized = 5 tasks with meaningful work
```

**Configuration**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionSize", "1m")
# AQE will merge adjacent post-shuffle partitions until they reach target size
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
```

### AQE Feature 2: Dynamically Switching Join Strategies

**Problem**: At compile time, Spark estimated a table at 500 MB (too large to broadcast).
After filters execute, the actual data is only 5 MB.

**AQE Solution**: At the shuffle boundary, AQE observes the real size and switches from
SortMergeJoin to BroadcastHashJoin.

```
COMPILE TIME plan:                      AQE RUNTIME switch:
SortMergeJoin (estimated 500MB)         BroadcastHashJoin (actual 5MB)
  :- Exchange (shuffle table A)           :- Scan table A (no shuffle!)
  +- Exchange (shuffle table B)           +- BroadcastExchange (table B, 5MB)

  Both sides shuffled                     Only 5MB broadcast, no shuffle!
```

**Configuration**:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
# AQE uses the same threshold as autoBroadcastJoinThreshold
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
```

### AQE Feature 3: Dynamically Optimizing Skew Joins

**Problem**: One partition has 10x more data than others (data skew). The task processing
that partition takes 10x longer, and everyone waits.

**AQE Solution**: Detect skewed partitions and split them into smaller sub-partitions.
The other side is duplicated to match.

```
BEFORE (skewed):
Table A:  | P0: 1GB | P1: 1GB | P2: 10GB (SKEW!) | P3: 1GB |
Table B:  | P0: 1GB | P1: 1GB | P2: 1GB           | P3: 1GB |

AFTER AQE skew optimization:
Table A:  | P0: 1GB | P1: 1GB | P2a: 3GB | P2b: 3GB | P2c: 4GB | P3: 1GB |
Table B:  | P0: 1GB | P1: 1GB | P2 copy  | P2 copy  | P2 copy   | P3: 1GB |
                                  ^^^        ^^^        ^^^
                                  Table B's P2 is replicated to match A's splits
```

**Configuration**:
```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# A partition is skewed if it is N times the median AND larger than this threshold
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256m")
```

### AQE Configuration Summary

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `spark.sql.adaptive.enabled` | true (DBR) | Master switch for AQE |
| `spark.sql.adaptive.coalescePartitions.enabled` | true | Merge small partitions |
| `spark.sql.adaptive.coalescePartitions.minPartitionSize` | 1m | Minimum post-coalesce size |
| `spark.sql.adaptive.advisoryPartitionSizeInBytes` | 128m (DBR) | Target partition size |
| `spark.sql.adaptive.skewJoin.enabled` | true | Handle skewed partitions |
| `spark.sql.adaptive.skewJoin.skewedPartitionFactor` | 5 | Skew detection multiplier |
| `spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256m | Minimum size to be skewed |

### Real-World Analogy: The GPS Rerouting

Traditional optimization is like printing directions from MapQuest before your trip.
AQE is like using Google Maps with live traffic -- it continuously reroutes based on
actual road conditions. You might start on the highway (sort-merge join), but if traffic
clears on a side road (table turns out to be small), the GPS reroutes you there
(broadcast join).

---

## Hands-On Walkthrough

See the companion notebook `05-adaptive-query-execution_notebook.py` for:

1. Toggling AQE on/off and comparing partition counts after a shuffle
2. Observing dynamic join strategy switching in execution plans
3. Creating artificial skew and watching AQE handle it
4. Tuning AQE parameters for different workload sizes

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR (Spark 3.x) | Google Dataproc |
|---------|-----------|---------------------|-----------------|
| AQE default | Enabled | Must enable | Must enable |
| Advisory partition size | 128 MB (tuned) | 64 MB (Spark default) | 64 MB |
| Skew join optimization | Enhanced in DBR | Standard Spark | Standard Spark |
| Integration with Photon | AQE feeds into Photon executor | N/A | N/A |

**Databricks-specific**: DBR enables AQE by default and tunes the advisory partition size
to 128 MB. Combined with Photon, AQE decisions feed directly into the vectorized C++
executor for optimal performance.

---

## Certification Tip

**Exam question pattern**: "What are the three main features of AQE?"
Answer: (1) Dynamically coalescing shuffle partitions, (2) Dynamically switching join
strategies, (3) Dynamically optimizing skew joins.

**Also tested**: "Is AQE enabled by default in Databricks?" Yes. "In open-source Spark 3.x?"
It depends on the version -- Spark 3.2+ defaults to true.

---

## Key Takeaways

1. **AQE re-optimizes at runtime** using real data statistics, not stale estimates
2. **Partition coalescing** automatically merges small post-shuffle partitions
3. **Join switching** changes SortMergeJoin to BroadcastHashJoin when actual data is small
4. **Skew optimization** splits large partitions and replicates the other side
5. **AQE fits into the Catalyst pipeline**: Cost Model + AQE selects the best physical plan
6. **Enabled by default** on Databricks; must enable on open-source Spark
7. AQE operates at **shuffle boundaries** -- it cannot help within a single stage

---

## Next Steps

AQE optimizes query execution at runtime. But what about the data sitting on disk?
[06 - File Layout Optimization](06-file-layout-optimization.md)
