# COPY INTO

> Module 02 -- Topic 03 | Level: Beginner-Intermediate | Time: 35 min

---

## Learning Objectives

- Write a `COPY INTO` statement to load files into a Delta table
- Explain idempotent loading and how COPY INTO tracks processed files
- Configure format options, file filtering, and credential management
- Compare COPY INTO with Auto Loader and choose the right tool
- Use COPY INTO with schema evolution

---

## Conceptual Overview

### What Is COPY INTO?

`COPY INTO` is a SQL command that loads data from files in cloud storage into a
Delta table. It is **idempotent** -- running the same command multiple times
will NOT duplicate data, because Databricks tracks which files have already
been loaded.

```
Cloud Storage                COPY INTO                     Delta Table
┌───────────────┐           ┌──────────────────┐          ┌──────────────┐
│ file_001.csv  │──────────>│                  │─────────>│              │
│ file_002.csv  │──────────>│  Parse + Filter  │─────────>│   Target     │
│ file_003.csv  │    skip   │  + Append        │          │   Table      │
│ file_001.csv  │────X─────>│  (already loaded)│          │              │
└───────────────┘           └──────────────────┘          └──────────────┘
                                    │
                                    v
                            File tracking metadata
                            stored in Delta table's
                            transaction log
```

### How Does Idempotency Work?

COPY INTO stores a record of each loaded file (path + size + modification
time) in the Delta table's transaction log. On subsequent runs, it checks
incoming files against this record and skips any that have already been
processed. This means:

- Running `COPY INTO` twice with the same files loads data only once
- New files added to the source directory are picked up on the next run
- Modified files (same path, different size or timestamp) are loaded again

---

## Syntax

### Basic Syntax

```sql
COPY INTO target_table
FROM '/path/to/source/files'
FILEFORMAT = CSV
FORMAT_OPTIONS (
    'header' = 'true',
    'inferSchema' = 'true'
)
COPY_OPTIONS (
    'mergeSchema' = 'true'
);
```

### Full Syntax Reference

```sql
COPY INTO target_table
FROM [ source_path | (SELECT ... FROM source_path) ]
FILEFORMAT = { CSV | JSON | PARQUET | AVRO | ORC | TEXT | BINARYFILE }
[ FILES = ( 'file1', 'file2', ... ) ]
[ PATTERN = 'glob_pattern' ]
[ FORMAT_OPTIONS ( key = value, ... ) ]
[ COPY_OPTIONS ( key = value, ... ) ]
```

### Source with Transformation (Subquery)

You can apply transformations during loading:

```sql
COPY INTO target_table
FROM (
    SELECT
        _c0::INT       AS id,
        _c1::STRING    AS name,
        _c2::DOUBLE    AS amount,
        current_timestamp() AS load_ts
    FROM '/path/to/data/'
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'false');
```

This is useful for:
- Renaming columns (`_c0` -> `id`)
- Casting types
- Adding metadata columns (load timestamp, source file name)
- Filtering rows during ingestion

---

## FORMAT_OPTIONS

These control how Spark parses the source files. They are the same options you
would pass to `spark.read`:

### CSV Format Options

| Option | Default | Description |
|--------|---------|-------------|
| `header` | `false` | First row is column names |
| `delimiter` / `sep` | `,` | Field separator |
| `inferSchema` | `false` | Infer column types |
| `quote` | `"` | Quote character |
| `escape` | `\` | Escape character |
| `multiLine` | `false` | Fields can span lines |
| `dateFormat` | `yyyy-MM-dd` | Date parse format |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parse format |
| `nullValue` | `""` | Null representation |
| `encoding` | `UTF-8` | Character encoding |
| `mode` | `PERMISSIVE` | Parse mode |

### JSON Format Options

| Option | Default | Description |
|--------|---------|-------------|
| `multiLine` | `false` | Parse multi-line JSON |
| `primitivesAsString` | `false` | All primitives as strings |
| `dateFormat` | `yyyy-MM-dd` | Date parse format |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parse format |

### Parquet / Avro

Usually no format options needed -- schemas are embedded.

---

## COPY_OPTIONS

| Option | Default | Description |
|--------|---------|-------------|
| `mergeSchema` | `false` | Allow schema evolution (add new columns) |
| `force` | `false` | Reload ALL files, even previously loaded ones (breaks idempotency) |

**Warning:** `force = true` defeats the purpose of COPY INTO's idempotency.
Use it only for reprocessing scenarios (e.g., after fixing a data quality bug
in source files).

---

## File Selection

### Load All Files in a Directory

```sql
COPY INTO my_table
FROM '/landing/events/'
FILEFORMAT = JSON;
```

### Load Specific Files

```sql
COPY INTO my_table
FROM '/landing/events/'
FILEFORMAT = JSON
FILES = ('events_001.json', 'events_002.json');
```

### Load Files Matching a Pattern

```sql
COPY INTO my_table
FROM '/landing/events/'
FILEFORMAT = JSON
PATTERN = 'events_2024*.json';
```

---

## COPY INTO with Credentials

When loading from external cloud storage that is not mounted or configured via
Unity Catalog external locations, you can pass credentials inline:

```sql
-- AWS S3
COPY INTO my_table
FROM 's3://my-bucket/data/'
  WITH (CREDENTIAL (
    AWS_ACCESS_KEY = '...',
    AWS_SECRET_KEY = '...'
  ))
FILEFORMAT = PARQUET;

-- Azure ADLS Gen2
COPY INTO my_table
FROM 'abfss://container@account.dfs.core.windows.net/data/'
  WITH (CREDENTIAL (
    AZURE_SAS_TOKEN = '...'
  ))
FILEFORMAT = PARQUET;
```

**Best practice:** Avoid inline credentials. Instead, use Unity Catalog
external locations or storage credentials for centralized access control.

---

## COPY INTO vs. Auto Loader -- When to Use Which

| Factor | COPY INTO | Auto Loader |
|--------|-----------|-------------|
| Interface | SQL only | Python / Scala / SQL (via DLT) |
| File tracking | Delta transaction log | Streaming checkpoint |
| Max files | Millions | Billions |
| Schema evolution | `mergeSchema` option | Built-in with multiple modes |
| Rescued data | No | Yes |
| Incremental discovery | Re-lists directory each run | Checkpoint or notification |
| Near-real-time | No (batch only) | Yes (streaming triggers) |
| Complexity | Very simple | Moderate |
| Best for | Simple SQL-first teams, low volume | Production pipelines, high volume |

### Decision Tree

```
Is your team SQL-first with no Python/Scala expertise?
├── YES --> COPY INTO (simple, familiar syntax)
└── NO
    │
    Will the landing directory accumulate > 1 million files?
    ├── YES --> Auto Loader (much more scalable)
    └── NO
        │
        Do you need near-real-time ingestion?
        ├── YES --> Auto Loader (streaming triggers)
        └── NO
            │
            Do you need schema evolution or rescued data?
            ├── YES --> Auto Loader (richer schema handling)
            └── NO  --> Either works; COPY INTO is simpler
```

---

## Schema Evolution with COPY INTO

COPY INTO supports schema evolution via the `mergeSchema` copy option:

```sql
COPY INTO my_table
FROM '/landing/events/'
FILEFORMAT = JSON
COPY_OPTIONS ('mergeSchema' = 'true');
```

When `mergeSchema` is true and incoming files have new columns, COPY INTO will
add those columns to the target Delta table automatically.

**Limitation:** COPY INTO does not support the `_rescued_data` column. If you
need to capture unexpected fields, use Auto Loader instead.

---

## Common Patterns

### Pattern 1: Scheduled Idempotent Load

Run this in a Databricks Job on a schedule (e.g., every hour):

```sql
COPY INTO bronze.sales
FROM '/landing/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');
```

Each run picks up only new files. Safe to retry on failure.

### Pattern 2: Load with Metadata Columns

```sql
COPY INTO bronze.events
FROM (
    SELECT
        *,
        _metadata.file_path     AS source_file,
        _metadata.file_name     AS source_file_name,
        _metadata.file_size     AS source_file_size,
        current_timestamp()     AS ingested_at
    FROM '/landing/events/'
)
FILEFORMAT = JSON;
```

### Pattern 3: Reprocessing After Bug Fix

```sql
-- Force reloads ALL files, ignoring previous tracking
COPY INTO bronze.sales
FROM '/landing/sales/'
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true')
COPY_OPTIONS ('force' = 'true');
```

---

## Hands-On Walkthrough

Open the companion notebook `03-copy-into_notebook.py`. The notebook:

1. Creates a Delta target table
2. Writes sample CSV files to a temp landing directory
3. Runs `COPY INTO` to load the files
4. Demonstrates idempotency by running `COPY INTO` again
5. Adds new files and shows incremental loading
6. Shows the `force` option for reprocessing
7. Cleans up all resources

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Source path format | `s3://bucket/prefix/` | `abfss://container@account.dfs.core.windows.net/path/` | `gs://bucket/prefix/` |
| Inline credentials | AWS_ACCESS_KEY, AWS_SECRET_KEY, AWS_SESSION_TOKEN | AZURE_SAS_TOKEN | GCS_SERVICE_ACCOUNT_KEY |
| Recommended auth | Unity Catalog external location | Unity Catalog external location | Unity Catalog external location |

---

## Certification Tip

For the **Databricks Certified Data Engineer Associate** exam:

- Know the `COPY INTO` syntax (FILEFORMAT, FORMAT_OPTIONS, COPY_OPTIONS)
- Understand idempotency -- COPY INTO will not reload already-processed files
- Know the `force=true` option and when it is used
- Be able to compare COPY INTO with Auto Loader (the exam asks "when would you choose one over the other?")
- Remember that COPY INTO is SQL-only -- it does not use `spark.readStream`

---

## Key Takeaways

- COPY INTO is the simplest way to do idempotent file loading using pure SQL
- It tracks loaded files in the Delta transaction log -- no external state
- Use `FORMAT_OPTIONS` for parse settings and `COPY_OPTIONS` for load behavior
- `force=true` breaks idempotency -- use only for intentional reprocessing
- For high-volume, schema-evolving, near-real-time pipelines, prefer Auto Loader
- For simple, SQL-first, low-volume loads, COPY INTO is a great fit

---

## Next Steps

Proceed to [04 -- External Sources](04-external-sources.md) to learn how to
ingest data from JDBC databases, Kafka, and cloud storage APIs.
