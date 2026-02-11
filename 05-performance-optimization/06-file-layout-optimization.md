# File Layout Optimization
> Module 05 — Topic 06 | Level: Intermediate-Advanced | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:
1. Use OPTIMIZE to bin-pack small files into larger ones
2. Apply Z-ORDER to co-locate data for faster filtered queries and lower cloud bills
3. Understand Liquid Clustering as the next evolution beyond Z-ORDER
4. Configure auto-optimize (optimizedWrite + autoCompact)
5. Use VACUUM to remove stale data and reduce storage costs
6. Leverage data skipping and bloom filters for scan optimization

---

## Conceptual Overview

### The Small File Problem

Every time you write to a Delta table -- whether from streaming, small batch appends,
or concurrent writes -- you create new Parquet files. Over time, you accumulate
thousands of small files:

```
BEFORE OPTIMIZE:
/delta_table/
  part-00001.parquet  (2 MB)
  part-00002.parquet  (500 KB)
  part-00003.parquet  (1 MB)
  part-00004.parquet  (100 KB)
  ... (5,000 more small files)

  Problem: 5,000 file opens, metadata reads, and seeks.
  Slow scans. High cloud storage API costs.

AFTER OPTIMIZE:
/delta_table/
  part-00001.parquet  (1 GB)   -- target file size
  part-00002.parquet  (1 GB)
  part-00003.parquet  (800 MB)

  Result: 3 file opens instead of 5,000. Much faster.
```

### OPTIMIZE: Bin-Packing

OPTIMIZE compacts small files into larger, right-sized files:

```sql
-- Compact all files in the table
OPTIMIZE movies_delta;

-- Compact only files matching a predicate (faster for large tables)
OPTIMIZE movies_delta WHERE release_year = 2024;
```

Target file size defaults to 1 GB. You can tune it:

```python
spark.conf.set("spark.databricks.delta.optimize.maxFileSize", "134217728")  # 128 MB
```

### Z-ORDER: Co-locate Data for Faster Queries

Z-ORDER rearranges data within files so that rows with similar column values are stored
together. This dramatically improves data skipping -- Spark can skip entire files that
do not contain the values you are filtering on.

```
WITHOUT Z-ORDER:
File 1: studios = [Disney, Sony, Warner, Universal, Fox, ...]
File 2: studios = [Disney, Paramount, Sony, MGM, Warner, ...]
File 3: studios = [Universal, Fox, Lionsgate, Disney, Sony, ...]

Query: WHERE studio = 'Disney'
  -> Must scan ALL 3 files (Disney is in every file)

WITH Z-ORDER BY studio:
File 1: studios = [Disney, Disney, Disney, Disney, ...]     <-- read
File 2: studios = [Fox, Lionsgate, MGM, Paramount, ...]     <-- SKIP
File 3: studios = [Sony, Universal, Warner, Warner, ...]    <-- SKIP

Query: WHERE studio = 'Disney'
  -> Reads only File 1, skips Files 2 and 3 = 66% less I/O!
```

```sql
-- Z-ORDER when running OPTIMIZE
OPTIMIZE movies_delta ZORDER BY (studio);

-- Z-ORDER on multiple columns (order matters: most filtered first)
OPTIMIZE movies_delta ZORDER BY (studio, release_year);
```

**Delta tables with Z-ORDER compress smaller and take less on your cloud bill.** Files
are better organized, which improves both compression ratios and query performance. This
directly reduces how much you pay for storage and compute.

### How Z-ORDER Works: The Z-Curve

Z-ORDER interleaves the bits of multiple columns to create a single sort order that
preserves locality in multiple dimensions:

```
Traditional sort (by studio only):
  studio    | year
  ----------|------
  Disney    | 2020    Files sorted by studio, but years are scattered
  Disney    | 1995    within each studio. Filtering on year still
  Disney    | 2023    scans many files.
  Fox       | 2001
  Fox       | 2022

Z-ORDER (by studio, year):
  Data is arranged so that nearby values in BOTH dimensions
  are stored together. Filtering on studio OR year (or both)
  benefits from data skipping.

  +--------+--------+--------+
  |  File 1         |  File 2|
  |  Disney 1990s   |  Disney|
  |  Disney 2000s   |  2010s+|
  +--------+--------+--------+
  |  File 3         |  File 4|
  |  Fox, Lionsgate |  MGM+  |
  |  1990s-2000s    |  Sony+ |
  +--------+--------+--------+
```

### Liquid Clustering (Databricks)

Liquid Clustering is the next evolution. Unlike Z-ORDER (which requires manual
OPTIMIZE runs), Liquid Clustering incrementally reorganizes data during writes:

```sql
-- Create a table with Liquid Clustering
CREATE TABLE movies_lc (
    movie_id INT,
    title STRING,
    studio STRING,
    release_year INT
)
USING DELTA
CLUSTER BY (studio, release_year);

-- Data is automatically clustered on write -- no manual OPTIMIZE needed
INSERT INTO movies_lc SELECT * FROM raw_movies;
```

| Feature | Z-ORDER | Liquid Clustering |
|---------|---------|-------------------|
| Trigger | Manual (OPTIMIZE) | Automatic on write |
| Change columns | Requires full rewrite | Alter table, incremental |
| Write overhead | None (separate step) | Slight overhead on write |
| Availability | All Delta Lake | Databricks only |

### VACUUM: Remove Stale Data

Delta Lake keeps old file versions for time travel. VACUUM removes files older than the
retention period, reducing storage costs:

```sql
-- Remove files older than 7 days (default retention)
VACUUM movies_delta;

-- Remove files older than 1 day (minimum with safety check)
VACUUM movies_delta RETAIN 24 HOURS;
```

```
BEFORE VACUUM:
/delta_table/
  part-00001.parquet  (current)
  part-00002.parquet  (current)
  part-00003.parquet  (old version - 10 days ago)  <-- VACUUM removes
  part-00004.parquet  (old version - 15 days ago)  <-- VACUUM removes

AFTER VACUUM:
/delta_table/
  part-00001.parquet  (current)
  part-00002.parquet  (current)

  Storage cost reduced. Time travel limited to retention period.
```

**Delta Table Techniques: OPTIMIZE and VACUUM** -- use OPTIMIZE to compact files for
performance and VACUUM to remove data that you no longer need for cost savings.

### Auto-Optimize: optimizedWrite + autoCompact

Instead of running OPTIMIZE manually, enable automatic optimization:

```python
# Enable on table creation
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

# Or set per-table
ALTER TABLE movies_delta SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

| Feature | What It Does | When |
|---------|-------------|------|
| `optimizedWrite` | Repartitions data before write to reduce small files | During write |
| `autoCompact` | Runs a mini-OPTIMIZE after each write | After write |

### Data Skipping and Bloom Filters

Delta Lake automatically collects **min/max statistics** for the first 32 columns of each
file. When you filter, Spark skips files where the filter value falls outside the
min/max range.

**Bloom filters** provide probabilistic data skipping for high-cardinality columns:

```sql
-- Create a bloom filter index on movie_id (high cardinality)
CREATE BLOOMFILTER INDEX ON TABLE movies_delta FOR COLUMNS(movie_id);
```

### Cost Optimization Mindset

Every optimization in this topic directly impacts your cloud bill:

```
+-------------------------------------------+
|         COST OPTIMIZATION                  |
+-------------------------------------------+
| 1. OPTIMIZE: fewer files = fewer API calls |
| 2. Z-ORDER: skip files = less data scanned |
| 3. VACUUM: remove old data = less storage  |
| 4. Auto-optimize: prevent small files      |
+-------------------------------------------+
  How long your servers are running, how much
  compute they are using -- minimize that.
```

---

## Hands-On Walkthrough

See the companion notebook `06-file-layout-optimization_notebook.py` for:

1. Creating a Delta table with many small files
2. Running OPTIMIZE and measuring scan performance before/after
3. Applying Z-ORDER and demonstrating data skipping
4. Running VACUUM to reclaim storage
5. Configuring auto-optimize settings

---

## Cloud Provider Notes

| Feature | Databricks | Open-Source Delta | Apache Iceberg |
|---------|-----------|-------------------|----------------|
| OPTIMIZE | Full support | Supported | `rewrite_data_files` |
| Z-ORDER | Full support | Supported (limited) | Sort orders |
| Liquid Clustering | Databricks only | Not available | Not available |
| VACUUM | Full support | Supported | `expire_snapshots` |
| Auto-optimize | Full support | Not available | Not available |
| Bloom Filters | Full support | Community support | Supported |

---

## Certification Tip

**Exam favorite**: "What does OPTIMIZE do?" Answer: Compacts small files into larger ones
(bin-packing). It does NOT change the data -- only the physical file layout.

**Also tested**: "What is the difference between Z-ORDER and partition pruning?"
- Partition pruning skips entire **directories** based on the partition column
- Z-ORDER enables skipping entire **files** based on min/max statistics within files
- They are complementary: partition by coarse-grain (year), Z-ORDER by fine-grain (studio)

---

## Key Takeaways

1. **OPTIMIZE** compacts small files into larger ones -- fewer file opens, faster scans
2. **Z-ORDER** co-locates data for multi-dimensional data skipping -- reduces I/O and
   cloud storage costs
3. **Liquid Clustering** is the automatic, incremental successor to Z-ORDER
4. **VACUUM** removes stale files beyond the retention period -- reclaims storage
5. **Auto-optimize** (optimizedWrite + autoCompact) prevents small files at write time
6. **Cost optimization** = fewer files + skip more data + remove what you do not need

---

## Next Steps

Files are optimized on disk. Now maximize the engine that reads them:
[07 - Photon & Serverless](07-photon-serverless.md)
