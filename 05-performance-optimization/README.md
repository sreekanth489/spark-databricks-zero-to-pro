# Module 05: Performance Optimization

> Tune your Spark workloads for maximum throughput and minimum cost.

## Prerequisites

- **Module 01-02**: Spark architecture fundamentals (Driver, Executors, Cluster Manager)
- **Module 03**: Delta Lake basics (Delta tables, ACID transactions)
- **Module 04**: Transformations (narrow vs wide, joins, aggregations)
- A running Databricks workspace (Community Edition or paid tier)

## Why This Module Matters

Understanding Spark internals is what separates a beginner from a production-ready data
engineer. You can write correct Spark code without any of this knowledge, but you cannot
write *fast* or *cost-efficient* Spark code without it. Every topic in this module directly
impacts your cloud bill and pipeline SLAs.

## Topics

| # | Topic | Key Concept | Time |
|---|-------|-------------|------|
| 01 | [Spark UI & Debugging](01-spark-ui-debugging.md) | Reading execution plans, the Catalyst pipeline, identifying bottlenecks | 50 min |
| 02 | [Partitioning Strategies](02-partitioning-strategies.md) | Partition = unit of parallelism, repartition vs coalesce, hash partitioning | 55 min |
| 03 | [Caching & Persistence](03-caching-persistence.md) | cache() vs persist(), storage levels, Delta cache | 40 min |
| 04 | [Broadcast Joins](04-broadcast-joins.md) | Broadcast vs sort-merge joins, autoBroadcastJoinThreshold | 40 min |
| 05 | [Adaptive Query Execution](05-adaptive-query-execution.md) | Runtime re-optimization, dynamic partition coalescing, skew handling | 45 min |
| 06 | [File Layout Optimization](06-file-layout-optimization.md) | OPTIMIZE, Z-ORDER, Liquid Clustering, VACUUM | 45 min |
| 07 | [Photon & Serverless](07-photon-serverless.md) | C++ vectorized engine, serverless compute, cost optimization | 40 min |

## Learning Path

```
01 Spark UI & Debugging         -- understand what Spark is doing
  |
  v
02 Partitioning Strategies      -- control how data is distributed
  |
  v
03 Caching & Persistence        -- keep hot data in memory
  |
  v
04 Broadcast Joins              -- eliminate shuffles for small tables
  |
  v
05 Adaptive Query Execution     -- let Spark optimize at runtime
  |
  v
06 File Layout Optimization     -- optimize data on disk (Delta)
  |
  v
07 Photon & Serverless          -- maximize engine performance & minimize cost
```

## Notebooks

Each topic includes a companion Databricks notebook (`*_notebook.py`) that you can
import directly into your workspace. The notebooks are self-contained with sample data
generation, demonstrations, and cleanup cells.

## How to Use This Module

1. **Read the concept guide** (`.md` file) for each topic first
2. **Import the notebook** into Databricks and run it cell by cell
3. **Inspect the Spark UI** after each action -- this is where real learning happens
4. **Experiment**: change partition counts, toggle AQE, compare with/without Photon

## Certification Relevance

This module covers material heavily tested on:
- **Databricks Certified Data Engineer Associate** (30-40% of exam)
- **Databricks Certified Data Engineer Professional** (performance tuning scenarios)
- **Spark Certification (Scala/Python)** (execution plans, partitioning, joins)

## Key Configuration Parameters

| Parameter | Default | Purpose |
|-----------|---------|---------|
| `spark.sql.shuffle.partitions` | 200 | Number of partitions after a shuffle |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Max size for auto-broadcast |
| `spark.sql.adaptive.enabled` | true (DBR) | Enable AQE |
| `spark.databricks.delta.optimizeWrite.enabled` | false | Auto bin-pack on write |
| `spark.databricks.photon.enabled` | true (Photon clusters) | Enable Photon engine |

---

**Next Module**: [Module 06 - Databricks Platform Features](../06-databricks-platform/README.md)
