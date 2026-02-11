# Optimization

> Module 03 -- Topic 05 | Level: Intermediate | Time: 50 min

## Learning Objectives

- Use OPTIMIZE to compact small files (bin-packing)
- Understand and run VACUUM to remove stale data files
- Apply Z-ORDER BY for multi-dimensional data skipping
- Use Liquid Clustering as a modern replacement for partitioning + Z-ORDER
- Configure target file sizes and data skipping statistics
- Make informed decisions using the optimization decision tree

## Conceptual Overview

### Why Optimization Matters

Delta tables accumulate **small files** over time from streaming ingestion,
frequent appends, and DML operations. Small files create two problems:

1. **Metadata overhead** -- listing thousands of files is slow
2. **I/O overhead** -- reading many small files is less efficient than reading
   fewer large files

Delta Lake provides several tools to address this:

```
  The Optimization Toolkit:
  =========================

  OPTIMIZE      -->  Compact small files into larger ones (bin-packing)
  Z-ORDER BY    -->  Co-locate related data for multi-dimensional skipping
  VACUUM        -->  Remove old, unreferenced data files
  Liquid Cluster-->  Automatic, incremental clustering (replaces partitions)
  ANALYZE TABLE -->  Compute column statistics for the query optimizer
```

### OPTIMIZE (Bin-Packing)

OPTIMIZE rewrites small files into larger, more efficient files:

```sql
-- Optimize the entire table
OPTIMIZE my_table;

-- Optimize only specific partitions
OPTIMIZE my_table WHERE date = '2025-01-15';
```

**How it works**:

```
  Before OPTIMIZE:                 After OPTIMIZE:
  ================                 ===============

  file_001.parquet (2 MB)
  file_002.parquet (1 MB)
  file_003.parquet (3 MB)          file_new_001.parquet (128 MB)
  file_004.parquet (5 MB)   --->   file_new_002.parquet (128 MB)
  file_005.parquet (1 MB)
  ...                              (old files remain until VACUUM)
  file_050.parquet (4 MB)
  Total: 50 files, ~150 MB         Total: 2 files, ~150 MB
```

**Default target file size**: 1 GB on Databricks (configurable).

```sql
-- Change target file size
ALTER TABLE my_table
SET TBLPROPERTIES ('delta.targetFileSize' = '128mb');
```

**When to OPTIMIZE**:

- After batch ingestion of many small files
- After many DML operations (UPDATE/DELETE create new small files)
- On a regular schedule for streaming tables
- Before running expensive analytical queries

### Z-ORDER BY (Multi-Dimensional Data Skipping)

Z-ORDER co-locates rows with similar values for specified columns within the
same files. This dramatically improves data skipping for queries that filter
on those columns.

```sql
-- Z-ORDER by a single column
OPTIMIZE my_table ZORDER BY (customer_id);

-- Z-ORDER by multiple columns (up to 4 recommended)
OPTIMIZE my_table ZORDER BY (date, region, product_id);
```

**How Z-ORDER improves query performance**:

```
  Without Z-ORDER:
  =================
  Query: WHERE region = 'US-West'

  file_1: regions [US-West, US-East, EU, Asia]    <-- must read
  file_2: regions [US-West, EU, Asia, US-East]    <-- must read
  file_3: regions [US-East, US-West, EU, Asia]    <-- must read
  file_4: regions [Asia, US-West, EU, US-East]    <-- must read
  --> Must read ALL 4 files (no skipping possible)


  With Z-ORDER BY (region):
  =========================
  Query: WHERE region = 'US-West'

  file_1: regions [US-West]                       <-- must read
  file_2: regions [US-East]                       <-- SKIP
  file_3: regions [EU]                            <-- SKIP
  file_4: regions [Asia]                          <-- SKIP
  --> Read only 1 out of 4 files (75% skipping!)
```

**Z-ORDER best practices**:

| Practice | Reason |
|----------|--------|
| Choose high-cardinality columns | Low cardinality (boolean) gives little benefit |
| Limit to 3-4 columns | Effectiveness decreases with more columns |
| Pick columns used in WHERE/JOIN | Only helps queries that filter on Z-ORDER columns |
| Re-run after large writes | New files are not Z-ordered automatically |
| Combine with OPTIMIZE | Z-ORDER is an option of the OPTIMIZE command |

### VACUUM (Removing Old Files)

When Delta rewrites files (OPTIMIZE, UPDATE, DELETE), old files remain on disk.
VACUUM removes files no longer referenced by any version within the retention
period:

```sql
-- Remove files older than the default retention (7 days)
VACUUM my_table;

-- Remove files older than 24 hours (minimum safe value)
VACUUM my_table RETAIN 24 HOURS;

-- Dry run: see what would be deleted without actually deleting
VACUUM my_table DRY RUN;
```

**Safety interval**: VACUUM will not delete files newer than the retention
threshold to prevent breaking concurrent readers.

```
  VACUUM Safety Model:
  ====================

  |<-------- retention period -------->|
  |                                    |
  v                                    v  now
  +---------+---------+---------+------+
  | removed | removed | removed | SAFE |
  | by      | by      | by      |      |
  | VACUUM  | VACUUM  | VACUUM  |      |
  +---------+---------+---------+------+

  Files referenced by current version are NEVER vacuumed.
  Files removed (by DML/OPTIMIZE) more than retention period ago ARE vacuumed.
```

**Important warnings**:

- Setting retention below 7 days requires:
  `spark.databricks.delta.retentionDurationCheck.enabled = false`
- After VACUUM, you **cannot time-travel** to versions that reference
  deleted files
- VACUUM is irreversible -- deleted files cannot be recovered

### Liquid Clustering

Liquid Clustering is the **modern replacement** for partitioning and Z-ORDER
on Databricks. It provides:

- **Incremental clustering** -- only new/modified data is reorganized
- **No partition boundaries** -- avoids the small-file problem of over-partitioning
- **Automatic optimization** -- no need to run OPTIMIZE ZORDER manually
- **Flexible column changes** -- change clustering columns without rewriting data

```sql
-- Create a table with Liquid Clustering
CREATE TABLE my_table (
  id INT,
  region STRING,
  event_date DATE,
  amount DOUBLE
)
USING DELTA
CLUSTER BY (region, event_date);

-- Change clustering columns (no data rewrite!)
ALTER TABLE my_table CLUSTER BY (event_date, region, id);

-- Trigger optimization (or let it happen automatically)
OPTIMIZE my_table;
```

**Liquid Clustering vs Partitioning vs Z-ORDER**:

```
  Partitioning:
  - Physically separates files by column value
  - Works well for low-cardinality columns (date, region)
  - Too many partitions = small file problem
  - Cannot change partition columns after creation

  Z-ORDER:
  - Reorders data within files for data skipping
  - Works with high-cardinality columns
  - Must re-run OPTIMIZE ZORDER after each write
  - Combined with partitioning for best results

  Liquid Clustering:
  - Replaces BOTH partitioning and Z-ORDER
  - Incremental (only touches new data)
  - Column changes are metadata-only operations
  - Recommended for all new Delta tables on Databricks
```

### Data Skipping Statistics

Delta Lake stores per-file statistics in the transaction log:

- **numRecords** -- row count per file
- **minValues** -- minimum value per column (first 32 columns)
- **maxValues** -- maximum value per column (first 32 columns)
- **nullCount** -- null count per column

The query engine uses these stats to skip files that cannot contain matching
rows:

```sql
-- Query: WHERE price > 1000
-- File stats: maxValues.price = 500
-- Result: SKIP this file (max is 500, no rows can be > 1000)
```

Configure statistics collection:

```sql
-- Collect stats for more columns (default is 32)
ALTER TABLE my_table
SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 50);
```

### Bloom Filters

Bloom filters provide fast point-lookup filtering for high-cardinality columns:

```sql
-- Create a bloom filter index
CREATE BLOOMFILTER INDEX ON TABLE my_table
FOR COLUMNS (user_id OPTIONS (fpp = 0.01, numItems = 10000000));
```

Bloom filters are probabilistic: they can tell you a value **definitely does
not exist** in a file, or that it **might exist**. False positives are possible
but false negatives are not.

### ANALYZE TABLE

Compute detailed column-level statistics for the Spark optimizer:

```sql
-- Analyze all columns
ANALYZE TABLE my_table COMPUTE STATISTICS FOR ALL COLUMNS;

-- Analyze specific columns
ANALYZE TABLE my_table COMPUTE STATISTICS FOR COLUMNS id, date, region;
```

This helps the optimizer choose better join strategies and filter ordering.

### Optimization Decision Tree

```
  Choosing the Right Optimization Strategy:
  ==========================================

  New table on Databricks?
  |
  +-- YES --> Use Liquid Clustering (CLUSTER BY)
  |           No partitioning needed.
  |           OPTIMIZE runs incrementally.
  |
  +-- NO (existing table or OSS Delta)
      |
      +-- Low cardinality filter column (e.g., date)?
      |   +-- Use PARTITIONED BY
      |   +-- Combine with Z-ORDER on other filter columns
      |
      +-- High cardinality filter column?
      |   +-- Use Z-ORDER BY (via OPTIMIZE)
      |
      +-- Many small files from streaming?
      |   +-- Schedule regular OPTIMIZE
      |   +-- Configure auto-optimize:
      |       delta.autoOptimize.optimizeWrite = true
      |       delta.autoOptimize.autoCompact = true
      |
      +-- Old files consuming storage?
          +-- Schedule VACUUM (e.g., weekly)
          +-- Set appropriate retention period
```

## Hands-On Walkthrough

Open the companion notebook `05-optimization_notebook.py` in your Databricks
workspace. You will:

1. Create a table with many small files
2. Run OPTIMIZE and observe file compaction
3. Apply Z-ORDER and measure query improvement
4. Run VACUUM (dry run) to see stale files
5. Demonstrate Liquid Clustering syntax
6. Inspect data skipping stats

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| OPTIMIZE | All clouds | Same | Same |
| Z-ORDER | All clouds | Same | Same |
| VACUUM | All clouds | Same | Same |
| Liquid Clustering | DBR 13.3+ (all clouds) | Same | Same |
| Auto-Optimize | All clouds | Same | Same |
| Predictive I/O | Databricks-managed (all clouds) | Same | Same |

Storage costs for retaining old files before VACUUM vary by cloud provider.

## Certification Tip

Optimization is a **major exam domain** for both Associate and Professional:

- "What does OPTIMIZE do?" -- Compacts small files into larger ones (bin-packing)
- "What is Z-ORDER?" -- Reorders data within files for multi-dimensional data skipping
- "What does VACUUM do?" -- Removes unreferenced files older than retention period
- "What is the default VACUUM retention?" -- 7 days
- "Can you time-travel after VACUUM?" -- Only to versions whose files were not removed
- "What is Liquid Clustering?" -- Modern replacement for partitioning + Z-ORDER
- "When should you use Liquid Clustering vs partitioning?" -- Liquid Clustering for new tables; partitioning for legacy or OSS scenarios

Expect 3-5 questions on optimization in the Associate exam.

## Key Takeaways

1. OPTIMIZE compacts small files into larger, more efficient ones (bin-packing).
2. Z-ORDER reorders data within files to co-locate similar values, enabling
   data skipping for filtered queries.
3. VACUUM removes unreferenced data files older than the retention period --
   it is irreversible and affects time travel.
4. Liquid Clustering is the recommended approach for new tables on Databricks,
   replacing both partitioning and Z-ORDER.
5. Data skipping statistics (min/max/null per column) are stored in the
   transaction log and used automatically by the query engine.

## Next Steps

Proceed to [06 - Change Data Feed](06-change-data-feed.md) to learn how to
track row-level changes for incremental processing.
