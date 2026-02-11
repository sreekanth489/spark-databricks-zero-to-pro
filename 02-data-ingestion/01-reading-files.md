# Reading Files (CSV / JSON / Parquet / Avro)

> Module 02 -- Topic 01 | Level: Beginner | Time: 45 min

---

## Learning Objectives

- Use `spark.read` to load CSV, JSON, Parquet, Avro, ORC, text, and binary files
- Compare schema inference with explicit schema definition and choose the right approach
- Configure format-specific options (header, delimiter, multiLine, mergeSchema, etc.)
- Handle corrupt records with the three parse modes (PERMISSIVE, DROPMALFORMED, FAILFAST)
- Read single files, directories, and glob patterns efficiently

---

## Conceptual Overview

### The DataFrameReader API

Every batch file read in Spark begins with `spark.read`. The call chain looks
like this:

```
spark.read               --> returns a DataFrameReader
     .format("csv")      --> sets the file format
     .option(k, v)       --> sets reader options (repeatable)
     .schema(s)          --> applies an explicit schema
     .load("/path")      --> triggers the read and returns a DataFrame
```

There are also shorthand helpers -- `spark.read.csv(path)`,
`spark.read.json(path)`, and `spark.read.parquet(path)` -- that combine
`.format()` and `.load()` into a single call.

### File Format Landscape

Not all formats are equal. The table below summarizes the trade-offs:

| Feature | CSV | JSON | Parquet | Avro | ORC | Delta |
|---------|-----|------|---------|------|-----|-------|
| Type | Row | Row | Columnar | Row | Columnar | Columnar (Parquet-based) |
| Human-readable | Yes | Yes | No | No | No | No |
| Schema embedded | No | Partial | Yes | Yes | Yes | Yes |
| Compression | External | External | Built-in (Snappy, Gzip, Zstd) | Built-in (Snappy, Deflate) | Built-in (Zlib, Snappy) | Built-in |
| Splittable | Yes* | Yes* | Yes | Yes | Yes | Yes |
| Column pruning | No | No | Yes | No | Yes | Yes |
| Predicate pushdown | No | No | Yes | No | Yes | Yes |
| Schema evolution | Manual | Manual | Limited | Good | Limited | Full |
| Typical use case | Data exchange | APIs, logs | Analytics | Streaming | Hive warehouse | Lakehouse |

*CSV and JSON are splittable only when uncompressed or compressed with a
splittable codec (e.g., bzip2). Gzip-compressed CSV/JSON files are NOT
splittable, meaning a single task must read the entire file.

### Decision Tree: Which Format?

```
Is the data from an external system you don't control?
├── YES --> Accept whatever format they provide (CSV, JSON, etc.)
│          and convert to Delta/Parquet at ingestion time
└── NO  --> Are you optimizing for analytics (SELECT with filters)?
            ├── YES --> Parquet or Delta
            └── NO  --> Is schema evolution critical?
                        ├── YES --> Avro or Delta
                        └── NO  --> Parquet or Delta
```

**Rule of thumb:** Ingest in the source format, then immediately convert to
Delta for all downstream processing.

---

## Reading CSV Files

CSV is the most common ingestion format and also the most error-prone. Key
options you should know:

| Option | Default | Purpose |
|--------|---------|---------|
| `header` | `false` | First row contains column names |
| `inferSchema` | `false` | Infer column types (requires extra pass over data) |
| `sep` (or `delimiter`) | `,` | Field delimiter |
| `quote` | `"` | Quote character for fields containing the delimiter |
| `escape` | `\` | Escape character inside quoted fields |
| `multiLine` | `false` | Allow fields to span multiple lines |
| `dateFormat` | `yyyy-MM-dd` | Parse format for date columns |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Parse format for timestamp columns |
| `nullValue` | `""` | String representation of null |
| `nanValue` | `NaN` | String representation of NaN |
| `mode` | `PERMISSIVE` | Parse mode for corrupt records |
| `columnNameOfCorruptRecord` | `_corrupt_record` | Column name to store bad rows |
| `encoding` | `UTF-8` | Character encoding |

### Schema Inference vs. Explicit Schema

**Schema inference** (`inferSchema=true`) reads through the data an extra time
to guess column types. Problems:

1. Doubles the I/O cost on large files
2. Can guess wrong (e.g., a ZIP code column "07024" becomes integer 7024)
3. Non-deterministic across data samples

**Explicit schema** -- always preferred in production:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("id", IntegerType(), nullable=False),
    StructField("name", StringType(), nullable=True),
    StructField("amount", DoubleType(), nullable=True),
])

df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("/path/to/data.csv")
```

You can also use DDL-style strings:

```python
schema = "id INT NOT NULL, name STRING, amount DOUBLE"
```

---

## Reading JSON Files

JSON files come in two flavors:

| Variant | Description | Spark default |
|---------|-------------|---------------|
| JSON Lines (JSONL) | One JSON object per line | Yes |
| Multi-line JSON | Pretty-printed, array of objects | Requires `multiLine=true` |

Key options for JSON:

| Option | Default | Purpose |
|--------|---------|---------|
| `multiLine` | `false` | Parse multi-line JSON documents |
| `primitivesAsString` | `false` | Read all primitives as strings |
| `allowUnquotedFieldNames` | `false` | Tolerate unquoted keys |
| `allowComments` | `false` | Tolerate Java/C-style comments |
| `dateFormat` | `yyyy-MM-dd` | Date parsing format |
| `timestampFormat` | `yyyy-MM-dd'T'HH:mm:ss` | Timestamp parsing format |
| `mode` | `PERMISSIVE` | Corrupt record handling |

### Nested JSON

Spark maps JSON objects to `StructType` and JSON arrays to `ArrayType`
automatically. Use dot notation or `getField()` to traverse nested structures:

```python
df.select("address.city", "tags[0]")
```

For deeply nested or semi-structured JSON, consider using `from_json()`,
`json_tuple()`, or `explode()` after reading the file as a single string column.

---

## Reading Parquet Files

Parquet is Spark's native format. Reading is straightforward:

```python
df = spark.read.parquet("/path/to/data.parquet")
```

Key options:

| Option | Default | Purpose |
|--------|---------|---------|
| `mergeSchema` | `false` | Merge schemas from all Parquet files in the path |
| `datetimeRebaseMode` | `EXCEPTION` | Handle Julian-to-Proleptic Gregorian calendar rebase |

Because Parquet embeds its schema, there is no need for `inferSchema` or
`header`. Column pruning and predicate pushdown happen automatically.

### mergeSchema

When Parquet files in the same directory were written with different schemas
(e.g., a new column was added), use `mergeSchema`:

```python
df = spark.read.option("mergeSchema", "true").parquet("/data/events/")
```

Spark will take the union of all schemas. Missing columns in older files become
null.

---

## Reading Avro Files

Avro is popular in streaming ecosystems (Kafka, Confluent Schema Registry).
Databricks includes Avro support out of the box:

```python
df = spark.read.format("avro").load("/path/to/data.avro")
```

Key option:

| Option | Purpose |
|--------|---------|
| `avroSchema` | Override the embedded Avro schema with a custom one (useful for schema evolution) |

---

## Reading Other Formats

### Text Files

```python
df = spark.read.text("/path/to/file.txt")
# Returns a DataFrame with a single column named "value"
```

Use `wholetext=true` to read each file as a single row.

### Binary Files

```python
df = spark.read.format("binaryFile").load("/path/to/images/")
# Returns columns: path, modificationTime, length, content (binary)
```

### ORC Files

```python
df = spark.read.format("orc").load("/path/to/data.orc")
```

---

## Handling Corrupt Records

Spark provides three parse modes that control what happens when a row cannot be
parsed according to the schema:

| Mode | Behavior | Use case |
|------|----------|----------|
| `PERMISSIVE` (default) | Puts the entire bad row into `_corrupt_record` column; sets other columns to null | Debugging, data quality analysis |
| `DROPMALFORMED` | Silently drops bad rows | Quick-and-dirty loads where some loss is acceptable |
| `FAILFAST` | Throws an exception on the first bad row | Production pipelines that must not silently lose data |

```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("columnNameOfCorruptRecord", "_bad_row") \
    .schema("id INT, name STRING, amount DOUBLE, _bad_row STRING") \
    .load("/path/to/data.csv")

# Inspect bad rows
df.filter("_bad_row IS NOT NULL").show(truncate=False)
```

**Important:** When using PERMISSIVE mode with an explicit schema, you must
include the corrupt-record column in your schema definition, or the bad rows
will be silently ignored.

---

## Reading Directories and Glob Patterns

### Entire directory

```python
df = spark.read.parquet("/data/events/")
# Reads all Parquet files in the directory (non-recursive by default)
```

### Recursive directory

```python
spark.conf.set("spark.sql.sources.recursive", "true")
df = spark.read.parquet("/data/events/")
```

### Glob patterns

```python
# All CSV files across year=2024 partitions
df = spark.read.csv("/data/events/year=2024/month=*/")

# Specific months
df = spark.read.csv("/data/events/year=2024/month=0[1-3]/")

# Files matching a name pattern
df = spark.read.csv("/data/events/part-*.csv")
```

### Partition discovery

When your directory layout follows Hive-style partitioning
(`/key=value/key=value/`), Spark automatically discovers partition columns
and adds them to the DataFrame:

```
/data/events/
  year=2023/month=01/part-00000.parquet
  year=2023/month=02/part-00000.parquet
  year=2024/month=01/part-00000.parquet
```

```python
df = spark.read.parquet("/data/events/")
df.printSchema()
# root
#  |-- col1: string
#  |-- col2: integer
#  |-- year: integer  <-- auto-discovered partition column
#  |-- month: integer <-- auto-discovered partition column
```

Use `basePath` option to control the root of partition discovery.

---

## Hands-On Walkthrough

Open the companion notebook `01-reading-files_notebook.py` and work through
each cell. The notebook generates sample data inline (no external files needed)
and demonstrates:

1. Writing and reading CSV with header / delimiter / schema options
2. Writing and reading JSON (single-line and multi-line)
3. Writing and reading Parquet with mergeSchema
4. Corrupt-record handling in all three modes
5. Glob pattern reads
6. Text and binary file reads
7. Cleanup of all temp files

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Default storage | S3 (`s3://`) | ADLS Gen2 (`abfss://`) | GCS (`gs://`) |
| Path format | `s3://bucket/key` | `abfss://container@account.dfs.core.windows.net/path` | `gs://bucket/path` |
| Authentication | Instance profile / IAM role | Service principal / managed identity | Service account |
| DBFS mount | `dbutils.fs.mount` (S3) | `dbutils.fs.mount` (ADLS) | `dbutils.fs.mount` (GCS) |
| Unity Catalog external location | Supported | Supported | Supported |

In Databricks, you can always use `dbfs:/` paths or Unity Catalog volumes
(`/Volumes/catalog/schema/volume/`) regardless of cloud provider.

---

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam tests:

- Knowing the `spark.read` API and format-specific options (header, inferSchema, multiLine, mergeSchema)
- Choosing between schema inference and explicit schemas
- Understanding corrupt-record parse modes (PERMISSIVE / DROPMALFORMED / FAILFAST)
- Recognizing when to use Parquet vs. CSV vs. JSON vs. Delta

Focus on the option names and their default values -- the exam often asks "What
happens if you do NOT set header=true on a CSV read?"

---

## Key Takeaways

- Always define an explicit schema in production -- never rely on `inferSchema`
- Use columnar formats (Parquet, Delta) for analytics workloads; row formats (CSV, JSON) only at the ingestion boundary
- Understand the three corrupt-record modes; PERMISSIVE + explicit schema is the best practice for data quality pipelines
- Glob patterns and partition discovery let you read large, partitioned datasets with a single `spark.read` call
- Converting from source format to Delta as early as possible gives you ACID transactions, time travel, and column pruning

---

## Next Steps

Proceed to [02 -- Auto Loader](02-auto-loader.md) to learn how to continuously
and incrementally ingest files as they arrive.
