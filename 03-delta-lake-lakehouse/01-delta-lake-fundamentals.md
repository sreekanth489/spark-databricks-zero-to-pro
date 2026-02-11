# Delta Lake Fundamentals

> Module 03 -- Topic 01 | Level: Intermediate | Time: 45 min

## Learning Objectives

- Explain what Delta Lake is and how it differs from raw Parquet storage
- Describe the Lakehouse architecture and why it exists
- Understand ACID transactions on a data lake
- Walk through the Delta transaction log (`_delta_log`) in detail
- Create Delta tables using multiple methods
- Compare Delta Lake with Apache Iceberg and Apache Hudi

## Conceptual Overview

### What Is Delta Lake?

Delta Lake is an open-source storage layer that brings reliability to data lakes.
It runs on top of your existing data lake storage (S3, ADLS, GCS) and provides:

- **ACID transactions** -- every write is atomic; readers never see partial results
- **Scalable metadata** -- handles billions of files without choking the metastore
- **Time travel** -- query or roll back to any previous version of your data
- **Schema enforcement** -- prevents bad data from corrupting your tables
- **Unified batch and streaming** -- the same table serves both workloads

Delta Lake is both an **open-source project** (delta.io, Apache 2.0 license) and
a core component of the **Databricks platform** where it receives additional
proprietary optimizations such as Liquid Clustering, Predictive I/O, and
UniForm compatibility.

```
  Open-Source Delta Lake            Databricks Delta Lake
  =====================            =====================
  ACID transactions                ACID transactions
  Time travel                      Time travel
  Schema enforcement               Schema enforcement
  DML (MERGE/UPDATE/DELETE)        DML (MERGE/UPDATE/DELETE)
  Change Data Feed                 Change Data Feed
  --                               Liquid Clustering
  --                               Predictive I/O
  --                               UniForm (Iceberg compat)
  --                               Deletion Vectors
  --                               Row-level concurrency
```

### The Lakehouse Architecture

Before Lakehouse, organizations chose between two architectures:

```
  Traditional Data Warehouse          Traditional Data Lake
  ==========================          =====================
  + Structured, governed              + Cheap, scalable storage
  + ACID, schema enforcement          + All data formats
  + Fast BI queries                   + ML/AI workloads
  - Expensive storage                 - No transactions
  - No unstructured data              - No schema enforcement
  - Vendor lock-in                    - "Data swamp" risk
```

The **Lakehouse** combines both:

```
  +---------------------------------------------------------------+
  |                      Lakehouse Platform                        |
  |---------------------------------------------------------------|
  |  BI / SQL Analytics  |  Data Engineering  |  ML / AI          |
  |---------------------------------------------------------------|
  |                     Query Engine (Photon)                      |
  |---------------------------------------------------------------|
  |                     Delta Lake (Storage Layer)                 |
  |  +----------+  +-----------+  +----------+  +--------------+  |
  |  |   ACID   |  |  Schema   |  |  Time    |  | Unified      |  |
  |  |   Txns   |  |  Enforce  |  |  Travel  |  | Batch+Stream |  |
  |  +----------+  +-----------+  +----------+  +--------------+  |
  |---------------------------------------------------------------|
  |              Cloud Object Storage (S3 / ADLS / GCS)           |
  +---------------------------------------------------------------+
```

Key insight: the Lakehouse stores data in **open formats** (Parquet) on cheap
cloud storage, but uses Delta Lake's transaction log to provide the reliability
guarantees you expect from a data warehouse.

### ACID Transactions Explained

ACID stands for:

| Property | Meaning | Delta Lake Implementation |
|----------|---------|--------------------------|
| **Atomicity** | A write either fully succeeds or fully fails | Commit written to `_delta_log` only on success |
| **Consistency** | Data always meets defined constraints | Schema enforcement, CHECK constraints |
| **Isolation** | Concurrent readers/writers do not interfere | Optimistic concurrency control, snapshot isolation |
| **Durability** | Committed data survives system failures | Data in cloud object storage; log is the source of truth |

Without ACID on a data lake, you face:

- **Partial writes** -- a job fails halfway, leaving corrupt data
- **Dirty reads** -- a query reads data mid-write and returns wrong results
- **Lost updates** -- two concurrent writes overwrite each other

Delta Lake solves all three through its transaction log.

### The Transaction Log (`_delta_log`) Deep Dive

Every Delta table has a `_delta_log/` directory. This is the **single source of
truth** for the table's state.

```
  my_table/
  +-- _delta_log/
  |   +-- 00000000000000000000.json    <-- version 0 (initial commit)
  |   +-- 00000000000000000001.json    <-- version 1
  |   +-- 00000000000000000002.json    <-- version 2
  |   +-- ...
  |   +-- 00000000000000000010.checkpoint.parquet  <-- checkpoint at v10
  |   +-- _last_checkpoint                          <-- pointer to latest ckpt
  +-- part-00000-...snappy.parquet     <-- data files
  +-- part-00001-...snappy.parquet
  +-- part-00002-...snappy.parquet
```

**JSON commit files** contain actions:

- `add` -- a new Parquet file is part of the table
- `remove` -- a Parquet file is logically deleted (still on disk until VACUUM)
- `metaData` -- schema or configuration change
- `commitInfo` -- who, when, what operation
- `protocol` -- reader/writer protocol version

Example commit file (simplified):

```json
{
  "add": {
    "path": "part-00000-abc123.snappy.parquet",
    "size": 1048576,
    "partitionValues": {"date": "2025-01-15"},
    "modificationTime": 1705334400000,
    "dataChange": true,
    "stats": "{\"numRecords\":50000,\"minValues\":{\"id\":1},\"maxValues\":{\"id\":50000}}"
  }
}
{
  "commitInfo": {
    "operation": "WRITE",
    "operationParameters": {"mode": "Append"},
    "readVersion": 0,
    "timestamp": 1705334400000
  }
}
```

**Checkpoint files** are written every 10 commits (by default). They aggregate
all actions up to that version into a single Parquet file for fast reads.
Without checkpoints, reading version 1000 would require replaying 1000 JSON
files.

```
  How Delta Reads Table State:
  ============================

  1. Read _last_checkpoint  -->  "version 990"
  2. Read checkpoint.parquet at version 990
  3. Read JSON commits 991, 992, ..., 1000
  4. Merge checkpoint + incremental JSONs = current table state
  5. Use data skipping stats to prune files
  6. Read only relevant Parquet data files
```

### Parquet + Transaction Log = Delta

A Delta table is fundamentally just:

```
  Delta Table  =  Parquet data files  +  _delta_log (transaction log)
```

This means:

- The actual data is stored in standard **Apache Parquet** columnar format
- The `_delta_log` adds transactional guarantees on top
- Any engine that understands the Delta protocol can read the table
- You can even read the raw Parquet files (but you lose ACID guarantees)

### Creating Delta Tables

There are several ways to create a Delta table:

**Method 1: DataFrame write (most common in ETL)**

```python
df.write.format("delta").mode("overwrite").save("/path/to/table")
```

**Method 2: DataFrame write to managed table**

```python
df.write.format("delta").saveAsTable("my_catalog.my_schema.my_table")
```

**Method 3: CREATE TABLE AS SELECT (CTAS)**

```sql
CREATE TABLE my_table
USING DELTA
AS SELECT * FROM source_table
```

**Method 4: CREATE TABLE with schema**

```sql
CREATE TABLE my_table (
  id INT,
  name STRING,
  created_at TIMESTAMP
)
USING DELTA
LOCATION '/path/to/table'
```

**Method 5: Convert existing Parquet**

```sql
CONVERT TO DELTA parquet.`/path/to/parquet_table`
```

### Delta vs Parquet vs Iceberg vs Hudi

| Feature | Delta Lake | Plain Parquet | Apache Iceberg | Apache Hudi |
|---------|-----------|---------------|----------------|-------------|
| ACID Transactions | Yes | No | Yes | Yes |
| Time Travel | Yes | No | Yes | Yes |
| Schema Evolution | Yes | No (manual) | Yes | Yes |
| MERGE / Upsert | Yes | No | Yes | Yes |
| Streaming Support | Yes | Limited | Limited | Yes |
| Transaction Log | JSON + checkpoint | None | Avro manifest | Timeline |
| Open Source | Apache 2.0 | Apache 2.0 | Apache 2.0 | Apache 2.0 |
| Databricks Native | Yes (primary) | Read/write | Via UniForm | No |
| Spark Integration | Deep | Basic | Good | Good |
| Partition Evolution | Liquid Clustering | Manual | Yes (hidden) | No |

Delta Lake is the default and best-supported format on Databricks. With
**UniForm**, Delta tables can also be read as Iceberg tables, providing cross-
engine compatibility.

## Hands-On Walkthrough

Open the companion notebook `01-delta-lake-fundamentals_notebook.py` in your
Databricks workspace. The notebook walks you through:

1. Creating a Delta table from scratch
2. Inspecting the `_delta_log` directory
3. Reading individual commit JSON files
4. Understanding file-level statistics for data skipping
5. Comparing Delta and Parquet behavior under concurrent operations

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Storage Backend | S3 | ADLS Gen2 | GCS |
| Default Delta Location | `s3://bucket/...` | `abfss://container@account.dfs.core.windows.net/...` | `gs://bucket/...` |
| Log Store | S3 (multi-cluster via DynamoDB) | ADLS (atomic rename) | GCS (atomic rename) |
| Unity Catalog Metastore | AWS Glue or UC-managed | UC-managed | UC-managed |
| Concurrent Write Safety | Requires DynamoDB for multi-cluster | Native (ADLS atomic ops) | Native (GCS atomic ops) |

## Certification Tip

The **Databricks Data Engineer Associate** exam frequently asks:

- "What does the transaction log contain?" -- know `add`, `remove`, `metaData`, `commitInfo`
- "How does Delta ensure atomicity?" -- commit-level writes to the log
- "What is the relationship between Delta and Parquet?" -- Delta = Parquet + log
- "How often are checkpoint files created?" -- every 10 commits by default

These are high-frequency questions. Understand the log mechanics cold.

## Key Takeaways

1. Delta Lake adds ACID transactions to data lakes by maintaining a transaction
   log alongside Parquet data files.
2. The `_delta_log` directory contains JSON commit files and periodic Parquet
   checkpoint files -- it is the single source of truth for table state.
3. The Lakehouse architecture combines the low cost and flexibility of data
   lakes with the reliability and governance of data warehouses.
4. Delta Lake is both open-source and Databricks-native, with additional
   optimizations available on the Databricks platform.
5. Multiple methods exist to create Delta tables: DataFrame API, SQL CTAS,
   explicit CREATE TABLE, or converting existing Parquet.

## Next Steps

Proceed to [02 - CRUD & MERGE Operations](02-crud-operations.md) to learn how
to insert, update, delete, and merge data in Delta tables.
