# CRUD & MERGE Operations

> Module 03 -- Topic 02 | Level: Intermediate | Time: 40 min

## Learning Objectives

- Perform INSERT, UPDATE, and DELETE operations on Delta tables
- Write MERGE (upsert) statements with matched and not-matched clauses
- Implement SCD Type 1 and Type 2 patterns using MERGE
- Use the DataFrame API equivalents for DML operations
- Apply performance best practices for large-scale MERGE operations

## Conceptual Overview

### Why DML on a Data Lake Matters

Traditional data lakes built on Parquet or CSV are **append-only**. If you need
to update a single row, you must:

1. Read the entire dataset
2. Filter out the old row
3. Union with the new row
4. Overwrite the entire dataset

Delta Lake provides **row-level DML operations** that handle this efficiently
through the transaction log:

```
  Without Delta:                      With Delta:
  ==============                      ===========
  Read entire dataset                 UPDATE table
  Filter out target rows              SET col = value
  Union with new data                 WHERE condition
  Overwrite all files                 --> Only affected files rewritten
                                      --> Atomic commit to _delta_log
```

### INSERT Operations

**SQL -- INSERT INTO**

```sql
INSERT INTO my_table VALUES (1, 'Alice', 100.00);

INSERT INTO my_table
SELECT * FROM staging_table;
```

**DataFrame API -- Append Mode**

```python
df.write.format("delta").mode("append").save("/path/to/table")

df.write.format("delta").mode("append").saveAsTable("my_table")
```

**Key behavior**: INSERT (append) adds new files. It does not read or modify
existing files, making it the fastest write operation.

### UPDATE Operations

**SQL Syntax**

```sql
UPDATE my_table
SET status = 'shipped', updated_at = current_timestamp()
WHERE order_id = 42;
```

**DataFrame API (Delta Lake Python API)**

```python
from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, "/path/to/table")
delta_table.update(
    condition="order_id = 42",
    set={"status": "'shipped'", "updated_at": "current_timestamp()"}
)
```

**How UPDATE works internally**:

```
  1. Scan files using data skipping stats
  2. Identify files containing matching rows
  3. Read those files, apply updates
  4. Write NEW Parquet files with updated rows
  5. Commit: add(new files) + remove(old files) in _delta_log
  6. Old files remain on disk until VACUUM
```

### DELETE Operations

**SQL Syntax**

```sql
DELETE FROM my_table WHERE order_date < '2024-01-01';
```

**DataFrame API**

```python
delta_table = DeltaTable.forPath(spark, "/path/to/table")
delta_table.delete(condition="order_date < '2024-01-01'")
```

**Internal mechanism**: Same as UPDATE -- affected files are rewritten without
the deleted rows. The old files are marked as `remove` in the log.

### MERGE (Upsert) -- The Most Powerful DML

MERGE is the flagship DML operation for Delta Lake. It handles:

- **Upsert**: Insert new rows, update existing ones
- **SCD Type 1**: Overwrite old values with new
- **SCD Type 2**: Keep history by inserting new versions
- **Deduplication**: Merge incoming data without creating duplicates

**Full MERGE Syntax**

```sql
MERGE INTO target_table AS t
USING source_table AS s
ON t.id = s.id                          -- merge condition

WHEN MATCHED AND s.op = 'DELETE' THEN
  DELETE

WHEN MATCHED AND s.op = 'UPDATE' THEN
  UPDATE SET
    t.name = s.name,
    t.value = s.value,
    t.updated_at = current_timestamp()

WHEN NOT MATCHED THEN
  INSERT (id, name, value, updated_at)
  VALUES (s.id, s.name, s.value, current_timestamp())

WHEN NOT MATCHED BY SOURCE THEN
  DELETE                                -- optional: remove orphans
```

**Clause Breakdown**

```
  MERGE INTO target USING source ON <condition>
  |
  +-- WHEN MATCHED [AND <extra_condition>] THEN
  |   +-- UPDATE SET ...    (update existing rows)
  |   +-- DELETE            (remove matched rows)
  |
  +-- WHEN NOT MATCHED [AND <extra_condition>] THEN
  |   +-- INSERT ...        (insert new rows from source)
  |
  +-- WHEN NOT MATCHED BY SOURCE [AND <extra_condition>] THEN
      +-- UPDATE SET ...    (update target rows not in source)
      +-- DELETE            (delete target rows not in source)
```

**DataFrame API for MERGE**

```python
from delta.tables import DeltaTable

target = DeltaTable.forPath(spark, "/path/to/target")
source = spark.read.format("delta").load("/path/to/source")

target.alias("t").merge(
    source.alias("s"),
    "t.id = s.id"
).whenMatchedUpdate(
    set={"name": "s.name", "value": "s.value"}
).whenNotMatchedInsert(
    values={"id": "s.id", "name": "s.name", "value": "s.value"}
).execute()
```

### SCD Type 1 Pattern (Overwrite)

SCD Type 1 simply overwrites old values with new ones:

```sql
MERGE INTO customers AS t
USING updates AS s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN
  UPDATE SET *           -- overwrite all columns
WHEN NOT MATCHED THEN
  INSERT *               -- insert new customers
```

### SCD Type 2 Pattern (History Preservation)

SCD Type 2 keeps a history of changes by closing old records and inserting new
ones:

```sql
-- Step 1: Close existing records
MERGE INTO customers_scd2 AS t
USING updates AS s
ON t.customer_id = s.customer_id AND t.is_current = true
WHEN MATCHED THEN
  UPDATE SET t.is_current = false, t.end_date = current_date()

-- Step 2: Insert new current records
INSERT INTO customers_scd2
SELECT customer_id, name, email, current_date(), null, true
FROM updates
```

### MERGE with Schema Evolution

As of Delta Lake 3.x, MERGE supports automatic schema evolution:

```python
target.alias("t").merge(
    source.alias("s"),
    "t.id = s.id"
).whenMatchedUpdateAll(
).whenNotMatchedInsertAll(
).execute()

# With schema evolution enabled:
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

When `autoMerge` is enabled, new columns in the source are automatically added
to the target table schema.

### Performance Tips for MERGE

| Tip | Why |
|-----|-----|
| Ensure the merge condition is selective | Broad conditions scan more files |
| Use partition pruning in the condition | `ON t.date = s.date AND t.id = s.id` limits files scanned |
| Z-ORDER on merge key columns | Improves data skipping during the match phase |
| Reduce source data before MERGE | Pre-filter, deduplicate source to minimize comparisons |
| Avoid `WHEN NOT MATCHED BY SOURCE` on large tables | This scans the entire target |
| Use `INSERT *` and `UPDATE SET *` for wide tables | Reduces boilerplate and errors |

## Hands-On Walkthrough

Open the companion notebook `02-crud-operations_notebook.py` in your Databricks
workspace. You will:

1. Create a Delta table with sample product data
2. INSERT new rows using both SQL and DataFrame API
3. UPDATE rows with conditions
4. DELETE rows
5. Implement a full MERGE with SCD Type 1 logic
6. Explore MERGE output metrics

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| DML Performance | Standard | Standard | Standard |
| Concurrent MERGE | Optimistic concurrency (conflict resolution) | Same | Same |
| Row-level Concurrency | Databricks Runtime 14.2+ (all clouds) | Same | Same |
| Deletion Vectors | Databricks Runtime 14.0+ (all clouds) | Same | Same |

**Deletion Vectors** (Databricks-only): Instead of rewriting entire files for
UPDATE/DELETE, Databricks marks individual rows as deleted in a separate file.
This makes DML on large files significantly faster.

## Certification Tip

The **Data Engineer Associate** exam heavily tests MERGE syntax:

- Know all three clause types: `MATCHED`, `NOT MATCHED`, `NOT MATCHED BY SOURCE`
- Know that the merge condition determines which rows are paired
- Understand that `UPDATE SET *` and `INSERT *` use all columns
- Know that MERGE is a single atomic transaction
- Practice writing MERGE statements from scratch -- expect 2-3 questions on this

## Key Takeaways

1. Delta Lake provides full DML support: INSERT, UPDATE, DELETE, and MERGE.
2. Internally, UPDATE and DELETE rewrite affected Parquet files and commit
   changes atomically to the transaction log.
3. MERGE is the most versatile operation, supporting upsert, SCD Type 1/2,
   and conditional logic in a single atomic statement.
4. Performance depends on selective merge conditions, partition pruning, and
   Z-ORDER on join key columns.
5. Schema evolution during MERGE is supported when `autoMerge` is enabled.

## Next Steps

Proceed to [03 - Time Travel](03-time-travel.md) to learn how to query and
restore previous versions of your Delta tables.
