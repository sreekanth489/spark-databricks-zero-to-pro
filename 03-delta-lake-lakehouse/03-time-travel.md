# Time Travel

> Module 03 -- Topic 03 | Level: Intermediate | Time: 35 min

## Learning Objectives

- Query previous versions of a Delta table by version number
- Query previous versions by timestamp
- Use DESCRIBE HISTORY to audit table changes
- Restore a table to a previous version
- Configure data retention settings
- Understand the limitations and costs of time travel

## Conceptual Overview

### What Is Time Travel?

Every time you write to a Delta table, a new **version** is created in the
transaction log. Delta Lake keeps a complete history of all versions, which
means you can:

- **Query any past version** of the data
- **Audit** who changed what and when
- **Restore** the table to a previous state
- **Reproduce** results from a specific point in time

```
  Version Timeline:
  =================

  v0          v1          v2          v3          v4
  |           |           |           |           |
  CREATE      INSERT      UPDATE      DELETE      MERGE
  TABLE       100 rows    price col   old rows    upsert
  |           |           |           |           |
  +-----------+-----------+-----------+-----------+----->  time

  You can query ANY of these versions at any time!
```

### Querying by Version Number

Every commit to the transaction log has an incrementing version number (0, 1,
2, ...). You can query any version:

**SQL Syntax**

```sql
-- Read version 3 of the table
SELECT * FROM my_table VERSION AS OF 3;

-- Alternative syntax
SELECT * FROM my_table@v3;
```

**DataFrame API**

```python
df = (spark.read
    .format("delta")
    .option("versionAsOf", 3)
    .load("/path/to/table"))
```

### Querying by Timestamp

You can also travel to the version that was current at a specific timestamp:

**SQL Syntax**

```sql
SELECT * FROM my_table TIMESTAMP AS OF '2025-01-15T10:30:00Z';

-- Also works with date strings
SELECT * FROM my_table TIMESTAMP AS OF '2025-01-15';
```

**DataFrame API**

```python
df = (spark.read
    .format("delta")
    .option("timestampAsOf", "2025-01-15")
    .load("/path/to/table"))
```

Delta finds the **latest version committed at or before** the given timestamp.

### DESCRIBE HISTORY

The HISTORY command shows a complete audit trail:

```sql
DESCRIBE HISTORY my_table;

-- Limit to recent history
DESCRIBE HISTORY my_table LIMIT 10;
```

**Output columns**:

| Column | Description |
|--------|-------------|
| `version` | Version number (0, 1, 2, ...) |
| `timestamp` | When the commit occurred |
| `userId` | Who made the change |
| `userName` | Human-readable name |
| `operation` | WRITE, UPDATE, DELETE, MERGE, etc. |
| `operationParameters` | Details (predicates, modes) |
| `operationMetrics` | Row counts (numOutputRows, numTargetRowsUpdated, etc.) |
| `readVersion` | The version read before this write |
| `isolationLevel` | Serializable or WriteSerializable |

**DataFrame API**:

```python
from delta.tables import DeltaTable

dt = DeltaTable.forPath(spark, "/path/to/table")
history_df = dt.history()
history_df.show()
```

### RESTORE TABLE

You can roll back a table to a previous version:

```sql
-- Restore to version 3
RESTORE TABLE my_table TO VERSION AS OF 3;

-- Restore to a timestamp
RESTORE TABLE my_table TO TIMESTAMP AS OF '2025-01-15';
```

**Important**: RESTORE creates a **new version** (it does not delete history).
If the table is at version 5 and you restore to version 3, the table is now at
version 6 with the same data as version 3.

```
  v0  v1  v2  v3  v4  v5  v6
  |   |   |   |   |   |   |
                              RESTORE TO v3
                              v6 data = v3 data
                              History preserved!
```

### Retention Settings

Delta Lake retains old data files for a configurable period:

| Setting | Default | Description |
|---------|---------|-------------|
| `delta.logRetentionDuration` | 30 days | How long transaction log entries are kept |
| `delta.deletedFileRetentionDuration` | 7 days | How long deleted data files are kept before VACUUM can remove them |

```sql
-- Configure retention on a table
ALTER TABLE my_table
SET TBLPROPERTIES (
  'delta.logRetentionDuration' = '60 days',
  'delta.deletedFileRetentionDuration' = '14 days'
);
```

**Relationship between time travel and VACUUM**:

```
  Time travel depends on TWO things:
  ===================================

  1. Transaction log entries (JSON/checkpoint files)
     - Controlled by delta.logRetentionDuration
     - Without log entries, Delta cannot reconstruct past versions

  2. Data files on disk
     - Old files are only removed by VACUUM
     - VACUUM respects delta.deletedFileRetentionDuration
     - Until VACUUM runs, old files remain available

  Time travel works ONLY if both the log entry AND the data files exist.
```

### Limitations of Time Travel

| Limitation | Details |
|-----------|---------|
| VACUUM removes old files | After VACUUM, you cannot time-travel to versions that reference those files |
| Log retention | After log entries expire, metadata for old versions is lost |
| Storage cost | Keeping many versions increases storage usage |
| Not a backup | Time travel is not a substitute for proper backups -- VACUUM or accidental table drops can destroy data |
| Performance | Very old versions may require replaying many log entries |

### Use Cases

1. **Audit and compliance** -- show regulators what the data looked like on a
   specific date
2. **Rollback after bad writes** -- restore the table after an erroneous UPDATE
   or DELETE
3. **Reproducibility** -- re-run an ML training pipeline on the exact data
   version used previously
4. **Debugging** -- compare current data with a past version to find what
   changed
5. **Point-in-time reporting** -- generate reports as of a specific date

## Hands-On Walkthrough

Open the companion notebook `03-time-travel_notebook.py` in your Databricks
workspace. You will:

1. Create a Delta table and make several modifications
2. Use DESCRIBE HISTORY to see the version timeline
3. Query specific versions using VERSION AS OF
4. Query by timestamp using TIMESTAMP AS OF
5. Restore the table to a previous version
6. Verify that RESTORE creates a new version (non-destructive)

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Time Travel | Available on all clouds | Same | Same |
| Default Retention | 7 days (data files), 30 days (log) | Same | Same |
| Storage Cost Impact | S3 pricing (old files) | ADLS pricing | GCS pricing |
| Cross-region Time Travel | Not directly supported | Not directly | Not directly |

Time travel behavior is identical across clouds. The only difference is storage
cost for retaining old file versions.

## Certification Tip

Common exam questions on time travel:

- "How do you query a previous version?" -- `VERSION AS OF n` or `TIMESTAMP AS OF`
- "Does RESTORE delete history?" -- No, it creates a new version
- "What controls time travel availability?" -- `deletedFileRetentionDuration` + VACUUM
- "What is the default file retention?" -- 7 days
- "What is the default log retention?" -- 30 days
- "Can you time travel after VACUUM?" -- Only to versions whose files were not vacuumed

This topic appears on both Associate and Professional exams.

## Key Takeaways

1. Delta Lake time travel lets you query any previous version of a table by
   version number or timestamp.
2. DESCRIBE HISTORY provides a full audit trail of all operations.
3. RESTORE TABLE rolls back to a previous version by creating a new commit --
   it never destroys history.
4. Time travel availability depends on both log retention and whether VACUUM
   has removed old data files.
5. Retention settings should be configured based on your compliance and
   debugging needs, balanced against storage costs.

## Next Steps

Proceed to [04 - Schema Evolution](04-schema-evolution.md) to learn how Delta
Lake handles schema changes gracefully.
