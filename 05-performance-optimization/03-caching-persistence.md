# Caching & Persistence
> Module 05 — Topic 03 | Level: Intermediate | Time: 40 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain the difference between `cache()` and `persist()` and when to use each
2. Choose the right storage level for your workload
3. Know when caching helps and when it hurts
4. Monitor cached data in the Spark UI Storage tab
5. Properly invalidate caches with `unpersist()`
6. Understand Delta Cache vs Spark Cache in Databricks

---

## Conceptual Overview

### Why Caching Matters

Spark is lazy -- it recomputes a DataFrame from scratch every time you call an action
on it. If you use the same DataFrame in multiple downstream operations, Spark will
re-read the source, re-apply all transformations, and re-shuffle data for every single
action.

```
Without caching:                         With caching:

Action 1: df.count()                     df.cache()
  -> read source -> filter -> group      Action 1: df.count()
                                           -> read source -> filter -> group
Action 2: df.show()                          -> STORE IN MEMORY
  -> read source -> filter -> group
       (entire pipeline re-executed!)    Action 2: df.show()
                                           -> READ FROM MEMORY (instant!)
Action 3: df.write(...)
  -> read source -> filter -> group      Action 3: df.write(...)
       (and again!)                        -> READ FROM MEMORY (instant!)
```

### cache() vs persist()

They are almost the same thing. `cache()` is a shortcut for `persist(MEMORY_AND_DISK)`.

```python
# These two lines are equivalent:
df.cache()
df.persist(StorageLevel.MEMORY_AND_DISK)

# persist() lets you choose the storage level:
df.persist(StorageLevel.MEMORY_ONLY)
df.persist(StorageLevel.DISK_ONLY)
df.persist(StorageLevel.MEMORY_AND_DISK_SER)
```

### Storage Levels Explained

| Level | Where | Serialized? | Replicated? | Use When |
|-------|-------|-------------|-------------|----------|
| `MEMORY_ONLY` | RAM only | No | No | Data fits in memory, speed critical |
| `MEMORY_AND_DISK` | RAM, spill to disk | No | No | **Default for cache()**, safest option |
| `DISK_ONLY` | Disk only | Yes | No | Data too large for memory |
| `MEMORY_ONLY_SER` | RAM only | Yes | No | Save memory at CPU cost |
| `MEMORY_AND_DISK_SER` | RAM + disk | Yes | No | Balance memory + reliability |
| `MEMORY_ONLY_2` | RAM only | No | Yes (2x) | Fault-tolerant caching |

**Serialized** means data is stored in a compact binary format. Uses less memory but
requires CPU to serialize/deserialize. Good trade-off when memory is tight.

**Replicated** (the `_2` variants) stores two copies on different nodes. If one node
fails, the cache survives. Rarely needed because Spark can recompute from lineage.

### Real-World Analogy: The Prep Kitchen

Think of caching like a restaurant prep kitchen:
- **No caching**: Every time a customer orders a salad, the chef washes lettuce, chops
  vegetables, and makes dressing from scratch
- **MEMORY_ONLY**: Pre-chopped vegetables sit on the counter -- fast to grab but limited
  counter space (if the counter is full, vegetables fall on the floor and are lost)
- **MEMORY_AND_DISK**: Pre-chopped vegetables on the counter, overflow goes in the fridge
  (slower but nothing is lost)
- **DISK_ONLY**: Everything in the fridge -- always available but slower to retrieve

### When to Cache

Cache when:
- The same DataFrame is used in **multiple downstream actions**
- The DataFrame is **expensive to compute** (complex joins, heavy aggregations)
- The data **fits reasonably in memory** (or you accept spill to disk)

Do NOT cache when:
- The DataFrame is used only once
- The DataFrame is very large relative to available memory
- The source data changes frequently (cache becomes stale)
- You are reading from Delta (Delta Cache may be better)

### Cache Invalidation with unpersist()

Cached data stays in memory until you explicitly remove it or the cluster is terminated.
Always clean up when you are done:

```python
# Cache the DataFrame
df.cache()
df.count()  # triggers materialization

# ... use df in multiple operations ...

# Release memory when done
df.unpersist()

# Blocking unpersist (waits until memory is freed)
df.unpersist(blocking=True)
```

### Lazy Materialization

`cache()` and `persist()` are lazy -- they do not immediately store data. The data is
cached only when the first action triggers computation:

```python
df.cache()        # marks df for caching, nothing happens yet
df.count()        # NOW data is computed and stored in cache
df.show()         # reads from cache (fast!)
```

### Delta Cache vs Spark Cache

In Databricks, there are two separate caching mechanisms:

```
+--------------------------------------------+
|               Spark Cache                   |
|  - Explicit: df.cache() / df.persist()     |
|  - Caches computed results (post-transform)|
|  - Uses executor memory (JVM heap)         |
|  - You manage lifecycle (unpersist)        |
+--------------------------------------------+

+--------------------------------------------+
|               Delta Cache                   |
|  - Automatic on Delta-enabled clusters     |
|  - Caches raw Parquet files from storage   |
|  - Uses local SSD (not executor memory)    |
|  - Managed automatically by Databricks     |
|  - Survives across queries                 |
+--------------------------------------------+
```

**Key difference**: Spark Cache stores transformed data in executor memory. Delta Cache
stores raw Parquet file data on local SSDs, freeing executor memory for computation.
On Databricks, Delta Cache is often the better choice because it is automatic and does
not compete with executor memory.

### Monitoring Cache in the Spark UI

Go to **Spark UI -> Storage Tab** to see:
- Which DataFrames are cached
- Storage level being used
- Size in memory vs size on disk
- Fraction of data cached (if it did not all fit in memory)

```
+----------------------------------------------------------------+
| Storage Tab                                                     |
|                                                                 |
| RDD Name         | Storage Level   | Size in Memory | % Cached |
|------------------|-----------------|----------------|----------|
| movies_df        | Memory + Disk   | 256 MB         | 100%     |
| joined_df        | Memory Only     | 1.2 GB         | 73%      |
|                    ^^^ only 73% fit in memory, rest was dropped |
+----------------------------------------------------------------+
```

---

## Hands-On Walkthrough

See the companion notebook `03-caching-persistence_notebook.py` for:

1. Timing queries with and without caching
2. Comparing storage levels (MEMORY_ONLY vs MEMORY_AND_DISK)
3. Monitoring cache in the Spark UI Storage tab
4. Demonstrating lazy materialization
5. Proper cleanup with unpersist()

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR | Google Dataproc |
|---------|-----------|---------|-----------------|
| Delta Cache | Automatic on Delta clusters | Not available | Not available |
| Cache local storage | Local SSDs (Delta) + memory (Spark) | Instance store + memory | Local SSDs + memory |
| Cache persistence | Survives query restarts (Delta) | Lost on cluster termination | Lost on cluster termination |
| Disk cache UI | Enhanced Storage tab | Standard Spark UI | Standard Spark UI |

---

## Certification Tip

**Exam question pattern**: "Which storage level does `cache()` use by default?"
Answer: `MEMORY_AND_DISK` (deserialized, no replication).

**Also tested**: Understanding that `cache()` is lazy and requires an action to
materialize. The data is not stored until you call `.count()`, `.show()`, `.collect()`,
or `.write()`.

---

## Key Takeaways

1. **cache() = persist(MEMORY_AND_DISK)** -- cache is just a convenience shortcut
2. **Cache is lazy** -- data is not stored until an action triggers computation
3. **Cache when reused** -- only cache DataFrames used in multiple downstream actions
4. **Always unpersist()** -- cached data stays in memory until explicitly released
5. **Delta Cache vs Spark Cache** -- on Databricks, Delta Cache uses local SSDs and is
   automatic; Spark Cache uses executor memory and is manual
6. **Monitor in Spark UI** -- the Storage tab shows what is cached and how much fits

---

## Next Steps

Caching keeps data in memory. But what about eliminating shuffles for joins?
[04 - Broadcast Joins](04-broadcast-joins.md)
