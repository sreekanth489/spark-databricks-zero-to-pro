# Spark UI & Debugging
> Module 05 — Topic 01 | Level: Intermediate | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:
1. Navigate every tab of the Spark UI (Jobs, Stages, Storage, SQL, Environment)
2. Read execution plans using all four `.explain()` modes
3. Trace the full Catalyst Optimizer pipeline from SQL to cluster execution
4. Identify shuffle bottlenecks, data skew, and spill from stage details
5. Use the event timeline to spot stragglers and scheduling delays

---

## Conceptual Overview

### The Catalyst Optimizer Pipeline

Every query you write -- whether DataFrame API or SQL -- goes through the same pipeline
before a single byte of data is touched. Understanding this pipeline is the single most
important debugging skill in Spark.

```
 YOUR QUERY (SQL or DataFrame API)
        |
        v
 +-------------------------------+
 | Unresolved Logical Plan       |  -- parsed, but column names/types unknown
 +-------------------------------+
        |
        v  (Catalog lookup: resolve table names, column types, schemas)
 +-------------------------------+
 | Resolved Logical Plan         |  -- all references validated against catalog
 +-------------------------------+
        |
        v  (Catalyst Query Optimizer: predicate pushdown, constant folding, etc.)
 +-------------------------------+
 | Optimized Logical Plan        |  -- logically equivalent but more efficient
 +-------------------------------+
        |
        v  (Physical Planning: generate multiple physical strategies)
 +-------------------------------+
 | Physical Plans (candidates)   |  -- e.g., SortMergeJoin vs BroadcastHashJoin
 +-------------------------------+
        |
        v  (Cost Model + AQE: pick cheapest strategy)
 +-------------------------------+
 | Best Physical Plan            |  -- the plan that actually runs
 +-------------------------------+
        |
        v
 +-------------------------------+
 | Cluster Execution             |  -- tasks distributed to executors
 +-------------------------------+
```

**Key insight**: The Catalyst Query Optimizer creates the optimized logical plan. It
applies rule-based transformations like pushing filters closer to the data source,
eliminating unnecessary columns, and folding constant expressions. Then the cost model
(enhanced by AQE at runtime) selects the best physical plan from multiple candidates.

### Reading Execution Plans with .explain()

Spark provides four modes for inspecting plans:

| Mode | What It Shows | Usage |
|------|---------------|-------|
| `simple` (default) | Physical plan only | Quick checks |
| `extended` | Both logical and physical plans | Debugging optimization |
| `formatted` | Physical plan with per-operator details | Production debugging |
| `codegen` | Generated Java code | Deep performance analysis |

```python
# Simple: print only the physical plan
df.explain("simple")

# Extended: print both logical and physical plans
df.explain("extended")

# Formatted: structured physical plan with details
df.explain("formatted")

# Codegen: the actual generated Java bytecode
df.explain("codegen")
```

**Read plans bottom-up**: The lowest node in the plan tree executes first (scanning data),
and the topmost node produces the final result.

### The Spark UI: Your Performance Dashboard

Think of the Spark UI as the cockpit instruments of an airplane. You can fly without
looking at them, but you will crash eventually.

```
+------------------------------------------------------------------+
|  SPARK UI                                                        |
|                                                                  |
|  [Jobs] [Stages] [Storage] [Environment] [Executors] [SQL]      |
|                                                                  |
|  Jobs Tab          -- one job per action (count, collect, write) |
|    |                                                             |
|    +-- Stages Tab  -- one stage per shuffle boundary             |
|         |                                                        |
|         +-- Tasks  -- one task per partition                     |
+------------------------------------------------------------------+
```

**Jobs Tab**: Each action (`.count()`, `.write()`, `.collect()`) triggers a job. Jobs
break into stages at shuffle boundaries.

**Stages Tab**: This is where you spend most of your debugging time. Look for:
- **Shuffle Read/Write sizes** -- large shuffles mean expensive wide transformations
- **Task duration distribution** -- one slow task means data skew
- **Spill (Memory/Disk)** -- data that did not fit in memory

**Storage Tab**: Shows cached/persisted DataFrames. Verify your cache is actually in
memory and check the fraction cached.

**SQL Tab**: Shows the physical plan as a visual DAG. Click any query to see per-operator
metrics (rows output, time spent). This is the best place to find bottlenecks.

**Executors Tab**: Memory usage, GC time, shuffle I/O per executor. High GC time means
your executors need more memory.

### Anatomy of a Stage

```
Stage 2: groupBy("studio").agg(sum("revenue"))
+----------------------------------------------------+
| Task 0: Partition 0 | 1.2 GB | 3.4s   <-- normal  |
| Task 1: Partition 1 | 1.1 GB | 3.2s   <-- normal  |
| Task 2: Partition 2 | 8.7 GB | 45.1s  <-- SKEW!   |
| Task 3: Partition 3 | 1.0 GB | 3.1s   <-- normal  |
+----------------------------------------------------+
  Summary: min=3.1s, median=3.3s, max=45.1s
           ^^^ when max >> median, you have skew
```

### Real-World Analogy: The Restaurant Kitchen

Imagine a restaurant kitchen with four chefs (executors). The head chef (driver) divides
a large catering order into tasks. If three chefs each get 10 dishes but one chef gets
80 dishes, the entire order waits for that one overwhelmed chef. That is data skew --
and the Spark UI stage details will show you exactly which "chef" is overloaded.

### Narrow vs Wide Transformations in the UI

Every shuffle boundary creates a new stage in the Spark UI:

```
Stage 1 (narrow)          Stage 2 (wide)           Stage 3 (narrow)
+----------------+        +----------------+        +----------------+
| map, filter    | -----> | groupBy, join  | -----> | select, map    |
| (no shuffle)   | SHUFFLE| (shuffle)      | -----> | (no shuffle)   |
+----------------+        +----------------+        +----------------+
```

- **Narrow transformations** (map, filter, select): each input partition produces exactly
  one output partition. No data movement. Same stage.
- **Wide transformations** (groupBy, join, repartition): data must move between partitions.
  Expensive. Creates a new stage. Visible as shuffle read/write in the UI.

---

## Hands-On Walkthrough

See the companion notebook `01-spark-ui-debugging_notebook.py` for interactive exercises:

1. Generate a sample dataset and trigger actions to create jobs
2. Use `.explain()` in all four modes on the same query
3. Inspect the Spark UI to find shuffle sizes and task distributions
4. Introduce artificial skew and observe the impact in stage details
5. Compare plans before and after adding a filter (predicate pushdown)

---

## Cloud Provider Notes

| Feature | Databricks | AWS EMR | Google Dataproc |
|---------|-----------|---------|-----------------|
| Spark UI Access | Built into workspace UI | YARN ResourceManager proxy | Component Gateway |
| Persisted History | Spark UI persists across cluster restarts | Requires Spark History Server config | Auto-configured |
| SQL Tab | Enhanced with Photon metrics | Standard Spark SQL tab | Standard Spark SQL tab |
| Ganglia Metrics | Not available (use Databricks metrics) | Available on EMR | Available on Dataproc |

**Databricks-specific**: The Databricks SQL tab shows additional Photon-specific metrics
when running on Photon-enabled clusters. The Driver Logs tab provides stdout/stderr from
the driver, which is invaluable for debugging.

---

## Certification Tip

**Exam favorite**: "Which `.explain()` mode shows both logical and physical plans?"
Answer: `extended`. The default (`simple`) shows only the physical plan. This is a
frequently tested distinction on both the Associate and Professional exams.

**Also tested**: Understanding that one job = one action, stages are separated by
shuffles, and tasks = partitions within a stage.

---

## Key Takeaways

1. **Every query** goes through: Unresolved Plan -> Resolved Plan (Catalog) ->
   Optimized Plan (Catalyst) -> Physical Plans -> Cost Model + AQE -> Execution
2. **Read plans bottom-up**: the lowest operator executes first
3. **Stages = shuffle boundaries**: minimize wide transformations to reduce stages
4. **Data skew** shows up as one task taking much longer than others in stage details
5. **The SQL tab** is the most useful tab for understanding query performance
6. **Narrow transformations** stay within a partition; **wide transformations** shuffle
   data across the network and are expensive

---

## Next Steps

Now that you can read what Spark is doing, the next topic teaches you how to control
it: [02 - Partitioning Strategies](02-partitioning-strategies.md)
