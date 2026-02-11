# Broadcast Joins
> Module 05 — Topic 04 | Level: Intermediate | Time: 40 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain how broadcast joins eliminate shuffle for small-large table joins
2. Identify when Spark auto-broadcasts (autoBroadcastJoinThreshold = 10 MB)
3. Use the `broadcast()` hint to force a broadcast join
4. Compare broadcast vs sort-merge vs shuffle-hash join strategies
5. Know when NOT to broadcast (and what happens if you do)
6. Use broadcast variables for non-join lookups

---

## Conceptual Overview

### The Shuffle Problem with Joins

When you join two large DataFrames, Spark must ensure that matching keys are on the
same node. This means shuffling both tables across the network:

```
Sort-Merge Join (default for large-large joins):

  Table A (500 GB)                    Table B (200 GB)
  +-------+-------+-------+          +-------+-------+-------+
  | P0    | P1    | P2    |          | P0    | P1    | P2    |
  +-------+-------+-------+          +-------+-------+-------+
      |       |       |                  |       |       |
      +-------+-------+------ SHUFFLE ---+-------+-------+
      |       |       |                  |       |       |
      v       v       v                  v       v       v
  +-------+-------+-------+-------+-------+-------+
  |   Sort + Merge on join key across all partitions |
  +-------+-------+-------+-------+-------+-------+

  Both tables are shuffled. 700 GB of data moves across the network.
  This is EXPENSIVE.
```

### The Broadcast Solution

If one table is small enough to fit in each executor's memory, Spark can **broadcast**
it -- send a complete copy to every executor. The large table stays in place. No shuffle.

```
Broadcast Hash Join:

  Table A (500 GB)                    Table B (5 MB)
  stays in place                      broadcast to all executors
  +-------+-------+-------+          +-------+
  | P0    | P1    | P2    |          | small |
  +-------+-------+-------+          +-------+
      |       |       |              /    |    \
      |       |       |            /      |      \
      v       v       v          v        v        v
  +-------+-------+-------+
  | P0+B  | P1+B  | P2+B  |    B = full copy of small table
  +-------+-------+-------+    on every executor

  Only the small table moves (5 MB x num_executors).
  The large table does NOT shuffle. Massive performance win.
```

### Auto-Broadcast Threshold

Spark automatically broadcasts a table when its size is below the threshold:

```python
# Default: 10 MB (10485760 bytes)
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# "10485760"

# Increase threshold to broadcast larger tables (e.g., 100 MB)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600")

# Disable auto-broadcast entirely
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### The broadcast() Hint

When you know a table is small enough but Spark's size estimate is wrong, you can
force a broadcast:

```python
from pyspark.sql.functions import broadcast

# Force broadcast of the small table
result = large_df.join(broadcast(small_df), on="key", how="inner")
```

In the execution plan, you will see `BroadcastHashJoin` instead of `SortMergeJoin`:

```
# Without broadcast hint:
== Physical Plan ==
SortMergeJoin [key], [key]
  :- Sort [key]
  :     +- Exchange hashpartitioning(key)    <-- SHUFFLE large table
  +- Sort [key]
        +- Exchange hashpartitioning(key)    <-- SHUFFLE small table

# With broadcast hint:
== Physical Plan ==
BroadcastHashJoin [key], [key]
  :- Scan large_table                        <-- NO shuffle
  +- BroadcastExchange                       <-- small table sent to all nodes
        +- Scan small_table
```

### Join Strategy Comparison

| Strategy | When Used | Shuffle? | Memory Req | Best For |
|----------|-----------|----------|------------|----------|
| **Broadcast Hash** | One side < threshold | No | Small table in each executor | Small-large joins |
| **Sort-Merge** | Both sides large | Yes (both) | Moderate | Large-large joins |
| **Shuffle Hash** | Medium tables, no sort | Yes (both) | Hash table in memory | Medium joins |

### Real-World Analogy: The Phone Book

Imagine joining customer transactions (billions of rows) with a country lookup table
(200 rows).

- **Sort-Merge Join**: Ship both the transaction ledgers AND the phone book to a central
  sorting room. Sort both, then match. Absurdly expensive for a tiny phone book.
- **Broadcast Join**: Photocopy the phone book and put one copy on every desk. Each
  clerk matches their transactions locally. No need to move the ledgers.

### When NOT to Broadcast

Broadcasting a table that is too large causes **OutOfMemoryError** on executors:

```
WARNING: Do NOT broadcast when:
  - Table is larger than executor memory (will crash)
  - Table is larger than ~1 GB (even if it fits, broadcast overhead is high)
  - Both tables are large (use sort-merge join instead)
  - Table size is unknown or growing (a "small" table today may be large tomorrow)
```

### Broadcast Variables for Lookups

Beyond joins, you can use broadcast variables for efficient key-value lookups:

```python
# Create a lookup dictionary
studio_regions = {
    "Warner Bros": "North America",
    "Disney": "North America",
    "Sony": "Asia",
    "Universal": "North America",
}

# Broadcast it -- one copy sent to each executor
bc_lookup = spark.sparkContext.broadcast(studio_regions)

# Use in a UDF
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def get_region(studio):
    return bc_lookup.value.get(studio, "Unknown")

df_with_region = df.withColumn("region", get_region(F.col("studio")))
```

This avoids sending the lookup dictionary with every task (which happens without
broadcast) and instead sends it once per executor.

---

## Hands-On Walkthrough

See the companion notebook `04-broadcast-joins_notebook.py` for:

1. Comparing sort-merge vs broadcast join performance on the movies dataset
2. Reading execution plans to identify join strategies
3. Using the `broadcast()` hint when auto-broadcast misestimates size
4. Demonstrating what happens when you broadcast a table that is too large
5. Using broadcast variables for lookup enrichment

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR | Google Dataproc |
|---------|-----------|---------|-----------------|
| Default threshold | 10 MB | 10 MB | 10 MB |
| AQE broadcast switch | Dynamically switches to broadcast at runtime | Requires AQE enabled | Requires AQE enabled |
| Photon broadcast | Optimized C++ broadcast execution | N/A | N/A |
| Max broadcast size | Limited by driver memory | Limited by driver memory | Limited by driver memory |

**Databricks-specific**: With AQE enabled (default on Databricks), Spark can dynamically
switch from sort-merge to broadcast at runtime if it discovers one side is smaller than
expected. This is one of AQE's most powerful features.

---

## Certification Tip

**Frequently tested**: "What is the default value of `spark.sql.autoBroadcastJoinThreshold`?"
Answer: **10 MB** (10485760 bytes).

**Also tested**: "How do you force a broadcast join?"
Answer: Use `broadcast()` hint: `large_df.join(broadcast(small_df), ...)`.

**Tricky question**: "What happens if you broadcast a 50 GB table?"
Answer: OutOfMemoryError on executors (or driver, depending on implementation).

---

## Key Takeaways

1. **Broadcast joins eliminate shuffle** for the large table -- only the small table moves
2. **Auto-broadcast** kicks in when a table is < 10 MB (configurable)
3. **broadcast() hint** forces a broadcast when Spark's estimate is wrong
4. **Sort-merge join** is the default for large-large joins (shuffles both sides)
5. **Never broadcast large tables** -- it causes OOM errors
6. **AQE can dynamically switch** to broadcast at runtime if actual sizes are small
7. **Broadcast variables** are useful for non-join lookups in UDFs

---

## Next Steps

Broadcast joins handle the small-large case. But what about dynamic runtime optimization
for all query types? [05 - Adaptive Query Execution](05-adaptive-query-execution.md)
