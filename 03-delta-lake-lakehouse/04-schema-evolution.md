# Schema Evolution

> Module 03 -- Topic 04 | Level: Intermediate | Time: 35 min

## Learning Objectives

- Understand schema enforcement as Delta Lake's default behavior
- Enable and use schema evolution with `mergeSchema`
- Distinguish between `mergeSchema` and `overwriteSchema`
- Handle type widening for safe type changes
- Evolve nested (struct) schemas
- Use Delta column mapping for DROP COLUMN and RENAME COLUMN

## Conceptual Overview

### Schema Enforcement (Default Behavior)

By default, Delta Lake **rejects** writes that do not match the table's schema.
This is a safety feature that prevents data corruption from schema drift.

```
  Schema Enforcement Flow:
  ========================

  DataFrame with columns [A, B, C, D]
       |
       v
  Delta Table with schema [A, B, C]
       |
       v
  REJECTED!  AnalysisException:
  "A schema mismatch detected when writing to the Delta table"
  Column D is not in the target table schema.
```

Schema enforcement checks:

| Check | Behavior |
|-------|----------|
| Extra columns in write | **Rejected** (column D not in target) |
| Missing columns in write | Allowed (nulls inserted for missing columns) |
| Data type mismatch | **Rejected** (e.g., writing STRING to INT column) |
| Column order difference | Allowed (matched by name, not position) |

### Schema Evolution with `mergeSchema`

When your data legitimately has new columns, you enable schema evolution:

**DataFrame API**

```python
# Option 1: Per-write option
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("my_table")

# Option 2: Global setting
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
```

**SQL (with INSERT)**

```sql
SET spark.databricks.delta.schema.autoMerge.enabled = true;
INSERT INTO my_table SELECT * FROM source_with_new_columns;
```

When `mergeSchema` is enabled:

```
  DataFrame [A, B, C, D]
       |
       v (mergeSchema = true)
  Delta Table [A, B, C]
       |
       v
  Delta Table [A, B, C, D]    <-- schema evolved!
  (existing rows have D = null)
```

### `mergeSchema` vs `overwriteSchema`

| Behavior | `mergeSchema` | `overwriteSchema` |
|----------|--------------|-------------------|
| Adds new columns | Yes | Yes |
| Removes missing columns | No | Yes |
| Changes data types | No (error) | Yes |
| Preserves existing data | Yes | No (full overwrite required) |
| Use case | Additive evolution | Complete schema replacement |

```python
# overwriteSchema: replaces the schema entirely
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("my_table")
```

**Warning**: `overwriteSchema` requires `mode("overwrite")` and replaces all
existing data. Use it only when you intentionally want a complete table
replacement with a new schema.

### Type Widening

Delta Lake supports **safe type promotions** (type widening) in certain cases:

| Original Type | Widened To | Safe? |
|--------------|-----------|-------|
| BYTE | SHORT, INT, LONG | Yes |
| SHORT | INT, LONG | Yes |
| INT | LONG | Yes |
| FLOAT | DOUBLE | Yes |
| DATE | TIMESTAMP | Yes |
| INT | STRING | Requires overwrite |
| STRING | INT | Requires overwrite |

To enable type widening:

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.enableTypeWidening' = 'true'
);
```

With type widening enabled, existing data files are not rewritten -- Delta
tracks the type change in the metadata and handles conversion at read time.

### Handling Nested (Struct) Schemas

Delta tables often have nested structures. Schema evolution works at any level:

```python
# Original schema
# root
#  |-- id: integer
#  |-- address: struct
#  |    |-- street: string
#  |    |-- city: string

# New data adds address.zip_code
# root
#  |-- id: integer
#  |-- address: struct
#  |    |-- street: string
#  |    |-- city: string
#  |    |-- zip_code: string    <-- new nested field

# With mergeSchema=true, the nested field is added
df.write.format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .save("/path/to/table")
```

Nested schema evolution supports:

- Adding new fields to a struct
- Adding new elements to a map type
- Nested structs within structs

It does **not** support:

- Removing nested fields (use column mapping)
- Changing nested field types (without type widening)

### Delta Column Mapping

Column mapping decouples **logical column names** from **physical Parquet
column names**. This enables:

- **DROP COLUMN**: physically remove a column from the schema
- **RENAME COLUMN**: change a column's name without rewriting data

```sql
-- Enable column mapping (required for DROP/RENAME)
ALTER TABLE my_table SET TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name'
);

-- Now you can drop columns
ALTER TABLE my_table DROP COLUMN temp_column;

-- And rename columns
ALTER TABLE my_table RENAME COLUMN old_name TO new_name;
```

**How it works**:

```
  Without column mapping:
    Logical name "revenue" --> Parquet column "revenue"
    (1:1 mapping, can't rename without rewriting all files)

  With column mapping (mode = 'name'):
    Logical name "revenue" --> physical ID abc-123
    Parquet column abc-123 --> stored data
    (rename only changes the metadata mapping, no file rewrite)
```

Column mapping modes:

| Mode | Description |
|------|-------------|
| `none` (default) | Logical names = physical names |
| `name` | Logical names mapped via internal IDs |
| `id` | Column mapping by physical column ID (future) |

### Schema Evolution Decision Tree

```
  Need to change table schema?
  |
  +-- Adding new columns?
  |   +-- Use mergeSchema = true
  |
  +-- Removing columns?
  |   +-- Enable column mapping (mode = 'name')
  |   +-- ALTER TABLE DROP COLUMN
  |
  +-- Renaming columns?
  |   +-- Enable column mapping (mode = 'name')
  |   +-- ALTER TABLE RENAME COLUMN
  |
  +-- Changing data types?
  |   +-- Safe widening (INT -> LONG)?
  |   |   +-- Enable type widening property
  |   +-- Unsafe change (STRING -> INT)?
  |       +-- Use overwriteSchema with mode("overwrite")
  |
  +-- Complete schema replacement?
      +-- Use overwriteSchema with mode("overwrite")
```

## Hands-On Walkthrough

Open the companion notebook `04-schema-evolution_notebook.py` in your Databricks
workspace. You will:

1. See schema enforcement reject a mismatched write
2. Enable `mergeSchema` and successfully add columns
3. Demonstrate nested schema evolution
4. Use column mapping to DROP and RENAME columns
5. Compare `mergeSchema` vs `overwriteSchema` behavior

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Schema enforcement | All clouds (default) | Same | Same |
| mergeSchema | All clouds | Same | Same |
| Column mapping | DBR 10.2+ (all clouds) | Same | Same |
| Type widening | DBR 15.2+ (all clouds) | Same | Same |
| DROP / RENAME COLUMN | Requires column mapping on all clouds | Same | Same |

Schema evolution features are cloud-agnostic. The only variable is the
Databricks Runtime version required for newer features.

## Certification Tip

Schema evolution questions on the Data Engineer Associate exam:

- "What happens when you write a DataFrame with extra columns to a Delta table?"
  -- Schema enforcement rejects it by default
- "How do you enable automatic schema evolution?" -- `mergeSchema` option or
  `spark.databricks.delta.schema.autoMerge.enabled`
- "What is the difference between mergeSchema and overwriteSchema?" -- merge
  adds columns, overwrite replaces the entire schema
- "How do you drop a column from a Delta table?" -- Enable column mapping,
  then ALTER TABLE DROP COLUMN

Know the distinction between enforcement and evolution cold.

## Key Takeaways

1. Delta Lake enforces schema by default -- writes with mismatched schemas are
   rejected to prevent data corruption.
2. `mergeSchema` enables additive schema evolution (new columns, nested fields).
3. `overwriteSchema` replaces the entire schema and requires overwrite mode.
4. Column mapping (`delta.columnMapping.mode = 'name'`) enables DROP COLUMN
   and RENAME COLUMN without rewriting data files.
5. Type widening supports safe promotions (INT to LONG, FLOAT to DOUBLE)
   without rewriting existing data.

## Next Steps

Proceed to [05 - Optimization](05-optimization.md) to learn how to tune Delta
table performance with OPTIMIZE, Z-ORDER, and Liquid Clustering.
