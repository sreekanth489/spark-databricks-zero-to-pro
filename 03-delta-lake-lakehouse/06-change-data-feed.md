# Change Data Feed

> Module 03 -- Topic 06 | Level: Intermediate | Time: 35 min

## Learning Objectives

- Explain what Change Data Feed (CDF) is and when to use it
- Enable CDF on a Delta table
- Read the change feed and interpret `_change_type`, `_commit_version`, and
  `_commit_timestamp` columns
- Implement CDC (Change Data Capture) patterns using CDF
- Use CDF with streaming for incremental processing
- Identify real-world use cases for CDF

## Conceptual Overview

### What Is Change Data Feed?

Change Data Feed (CDF) is a Delta Lake feature that records **row-level changes**
made to a table. When enabled, every INSERT, UPDATE, and DELETE operation
generates change records that downstream consumers can read.

```
  Normal Delta Read:            Change Data Feed Read:
  ==================            =====================

  Returns: current state        Returns: what CHANGED
  (all rows as they are now)    (inserts, updates, deletes)

  +----+-------+-----+         +----+-------+-----+--------------+
  | id | name  | val |         | id | name  | val | _change_type |
  +----+-------+-----+         +----+-------+-----+--------------+
  | 1  | Alice | 100 |         | 3  | Carol | 300 | insert       |
  | 2  | Bob   | 250 |         | 2  | Bob   | 200 | update_preimage |
  | 3  | Carol | 300 |         | 2  | Bob   | 250 | update_postimage|
  +----+-------+-----+         | 4  | David | 150 | delete       |
                                +----+-------+-----+--------------+
```

### Enabling CDF

CDF must be explicitly enabled on a table:

```sql
-- On table creation
CREATE TABLE my_table (id INT, name STRING, value DOUBLE)
USING DELTA
TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

-- On an existing table
ALTER TABLE my_table
SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
```

You can also enable CDF for all new tables in a session:

```sql
SET spark.databricks.delta.properties.defaults.enableChangeDataFeed = true;
```

**Important**: CDF only records changes made **after** it is enabled. It does
not retroactively generate change records for existing data.

### Reading the Change Feed

**SQL Syntax**

```sql
-- Read changes between two versions
SELECT * FROM table_changes('my_table', 2, 5);

-- Read changes from a version to the latest
SELECT * FROM table_changes('my_table', 2);

-- Read changes between timestamps
SELECT * FROM table_changes('my_table', '2025-01-15', '2025-01-20');
```

**DataFrame API**

```python
# By version range
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 2)
    .option("endingVersion", 5)
    .table("my_table"))

# By timestamp range
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingTimestamp", "2025-01-15")
    .option("endingTimestamp", "2025-01-20")
    .table("my_table"))
```

### CDF Metadata Columns

Every change feed record includes three metadata columns:

| Column | Type | Description |
|--------|------|-------------|
| `_change_type` | STRING | The type of change: `insert`, `update_preimage`, `update_postimage`, `delete` |
| `_commit_version` | LONG | The Delta version where this change was committed |
| `_commit_timestamp` | TIMESTAMP | When the commit occurred |

**Change types explained**:

```
  INSERT operation:
    _change_type = 'insert'           (the new row)

  UPDATE operation:
    _change_type = 'update_preimage'  (the row BEFORE the update)
    _change_type = 'update_postimage' (the row AFTER the update)

  DELETE operation:
    _change_type = 'delete'           (the row that was deleted)
```

The **preimage/postimage** pair for updates is extremely valuable -- you can
see both the old and new values without having to compare full table snapshots.

### CDC Patterns with CDF

**Pattern 1: Incremental ETL**

Instead of reprocessing an entire table, read only the changes since the last
processed version:

```python
# Track the last processed version
last_version = get_last_processed_version()  # from checkpoint/state

# Read only new changes
changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_version + 1)
    .table("source_table"))

# Process changes
process_changes(changes)

# Update checkpoint
save_last_processed_version(current_version)
```

**Pattern 2: Propagate Changes Downstream**

```python
changes = read_change_feed(source_table, start_version)

# Apply inserts
inserts = changes.filter("_change_type = 'insert'")
inserts.write.format("delta").mode("append").save(downstream_table)

# Apply updates
updates = changes.filter("_change_type = 'update_postimage'")
apply_updates(downstream_table, updates)

# Apply deletes
deletes = changes.filter("_change_type = 'delete'")
apply_deletes(downstream_table, deletes)
```

**Pattern 3: Audit Trail**

```python
# Capture all changes for compliance
all_changes = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("sensitive_table"))

all_changes.write.format("delta").mode("append").save("audit_log")
```

### CDF with Streaming

CDF integrates with Structured Streaming for real-time change processing:

```python
# Stream changes from a Delta table with CDF enabled
stream = (spark.readStream
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", 0)
    .table("my_table"))

# Process the stream
query = (stream
    .filter("_change_type != 'update_preimage'")
    .writeStream
    .format("delta")
    .option("checkpointLocation", "/tmp/cdf_checkpoint")
    .trigger(availableNow=True)
    .toTable("downstream_table"))
```

**Streaming + CDF flow**:

```
  Source Table          Structured Streaming        Downstream Table
  (CDF enabled)        (readChangeFeed=true)       (receives changes)
  +-----------+        +------------------+        +---------------+
  | INSERT    |------->| _change_type     |------->| new rows      |
  | UPDATE    |------->| = insert         |        | updated rows  |
  | DELETE    |------->| = update_post*   |        | deletions     |
  +-----------+        | = delete         |        +---------------+
                       +------------------+
                       Checkpoint tracks
                       last processed version
```

### Use Cases

| Use Case | How CDF Helps |
|----------|--------------|
| **Incremental ETL** | Process only changed rows instead of full table scans |
| **Audit trail** | Record every change with before/after values |
| **Downstream refresh** | Push changes to dependent tables without full recompute |
| **ML feature updates** | Update feature store with only changed features |
| **Data replication** | Replicate changes to another system (e.g., external DB) |
| **Real-time dashboards** | Stream changes to a dashboard aggregation table |

### CDF Storage Overhead

CDF data is stored in a `_change_data/` directory alongside the regular data:

```
  my_table/
  +-- _delta_log/
  +-- _change_data/           <-- CDF records stored here
  |   +-- cdc-00000-...parquet
  +-- part-00000-...parquet   <-- regular data files
```

The storage overhead depends on how many changes occur. For tables with heavy
DML activity, CDF can add 10-30% storage overhead.

## Hands-On Walkthrough

Open the companion notebook `06-change-data-feed_notebook.py` in your
Databricks workspace. You will:

1. Create a Delta table with CDF enabled
2. Perform INSERT, UPDATE, and DELETE operations
3. Read the change feed and inspect all metadata columns
4. Filter changes by type (insert, update, delete)
5. Demonstrate incremental processing using version ranges

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| CDF Availability | All clouds, DBR 8.4+ | Same | Same |
| CDF with Streaming | All clouds | Same | Same |
| CDF Storage Location | `_change_data/` under table path | Same | Same |
| CDF + Unity Catalog | Fully supported | Same | Same |

CDF behavior is identical across all cloud providers.

## Certification Tip

CDF questions appear on both Associate and Professional exams:

- "How do you enable Change Data Feed?" -- `delta.enableChangeDataFeed = true`
- "What are the _change_type values?" -- `insert`, `update_preimage`,
  `update_postimage`, `delete`
- "Does CDF work retroactively?" -- No, only records changes after enablement
- "How do you read the change feed in SQL?" -- `table_changes('table', start, end)`
- "What extra columns does CDF add?" -- `_change_type`, `_commit_version`,
  `_commit_timestamp`

The Professional exam specifically tests CDF in the context of incremental
processing pipelines.

## Key Takeaways

1. Change Data Feed records row-level changes (insert, update, delete) with
   before and after images.
2. CDF must be explicitly enabled and only captures changes made after
   enablement.
3. Three metadata columns are added: `_change_type`, `_commit_version`, and
   `_commit_timestamp`.
4. CDF is essential for incremental ETL -- process only what changed instead
   of rescanning the entire table.
5. CDF integrates with Structured Streaming for real-time change propagation.

## Next Steps

Proceed to [07 - Medallion Architecture](07-medallion-architecture.md) to
learn the Bronze/Silver/Gold pattern for building production Lakehouse
pipelines.
