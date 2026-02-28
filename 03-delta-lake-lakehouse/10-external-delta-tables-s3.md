# External Delta Tables on S3 & Deletion Vectors

> Module 03 -- Topic 10 | Level: Intermediate to Advanced | Time: 75 min

## Learning Objectives

- Create external Delta tables that store data on AWS S3
- Understand the complete anatomy of the Delta transaction log (`_delta_log`)
- Decode every field in commit JSON files: `commitInfo`, `metaData`, `protocol`, `add`, `remove`
- Explain how deletion vectors work and why they are the default in Databricks
- Compare deletion vectors ON vs OFF behavior for UPDATE and DELETE operations
- Understand how OPTIMIZE interacts with deletion vectors
- Configure auto-optimize settings and checkpoint intervals
- Use VACUUM correctly with both modes

## Conceptual Overview

### External Tables vs Managed Tables

```
  MANAGED TABLE                          EXTERNAL TABLE
  =============                          ==============

  CREATE TABLE t (...)                   CREATE TABLE t (...)
                                         LOCATION 's3://bucket/path'
        |                                       |
        v                                       v
  +------------------+                  +------------------+
  | Unity Catalog    |                  | Unity Catalog    |
  | stores metadata  |                  | stores metadata  |
  | AND owns data    |                  | but YOU own data |
  +------------------+                  +------------------+
        |                                       |
        v                                       v
  +------------------+                  +------------------+
  | Managed storage  |                  | YOUR S3 bucket   |
  | (catalog root)   |                  | s3://bucket/path |
  +------------------+                  +------------------+
        |                                       |
        v                                       v
  DROP TABLE =                           DROP TABLE =
  Deletes data + metadata               Deletes metadata ONLY
                                         Data remains on S3!
```

External tables are essential when:

- Data must be **shared across multiple Databricks workspaces** or tools
- You need **full control** over the storage location and lifecycle
- Data is produced by **external systems** (Kafka, Airflow, other engines)
- You want to **decouple compute from storage** governance

### Creating External Delta Tables on S3

There are three approaches to create an external Delta table on S3:

**Approach 1: Write data first, then register as a table**

```python
# Step 1: Write Delta data to S3
df.write.format("delta").mode("overwrite").save("s3://bucket/orders")

# Step 2: Register as a table in Unity Catalog
spark.sql("""
  CREATE TABLE orders_external
  USING DELTA
  LOCATION 's3://bucket/orders'
""")
```

**Approach 2: Create table with CTAS (copies data)**

```sql
CREATE TABLE orders_external
USING DELTA
AS SELECT * FROM source_table;
```

**Approach 3: Create empty table then load data**

```sql
CREATE TABLE orders_external (
  order_id INT, customer_name STRING, product STRING,
  quantity INT, price DOUBLE, order_date STRING
)
USING DELTA
LOCATION 's3://bucket/orders';

INSERT INTO orders_external VALUES (...);
```

## The Delta Transaction Log: Field-by-Field Deep Dive

Every Delta table has a `_delta_log/` directory containing JSON commit files.
Understanding every field in these files is critical for debugging, auditing,
and mastering Delta internals.

### Directory Structure on S3

```
  s3://databricks-zero-to-pro/orders/
  |
  +-- _delta_log/
  |   +-- 00000000000000000000.json    <-- Version 0 (initial write)
  |   +-- 00000000000000000001.json    <-- Version 1 (UPDATE)
  |   +-- 00000000000000000002.json    <-- Version 2 (DELETE)
  |   +-- 00000000000000000003.json    <-- Version 3 (auto-OPTIMIZE)
  |
  +-- part-00000-19111d98-...snappy.parquet    <-- Original data file
  +-- part-00000-fa3ea6f3-...snappy.parquet    <-- Updated row file
  +-- part-00000-cab3ba72-...snappy.parquet    <-- Compacted file (after OPTIMIZE)
  +-- deletion_vector_....bin                   <-- Deletion vector files
```

### Version 0 (0000.json): Initial WRITE

Version 0 is the initial write. It contains **four actions**: `commitInfo`,
`metaData`, `protocol`, and `add`.

#### Action 1: `commitInfo` -- Who Did What, When

```json
{
  "commitInfo": {
    "timestamp": 1772232863914,
    "userId": "71002250154182",
    "userName": "sreekanthdatabricks@gmail.com",
    "operation": "WRITE",
    "operationParameters": {
      "mode": "Overwrite",
      "statsOnLoad": false,
      "partitionBy": "[]"
    },
    "notebook": { "notebookId": "817341971342573" },
    "clusterId": "0227-225046-ibqyq0bf-v2n",
    "isolationLevel": "WriteSerializable",
    "isBlindAppend": false,
    "operationMetrics": {
      "numFiles": "1",
      "numRemovedFiles": "0",
      "numRemovedBytes": "0",
      "numDeletionVectorsRemoved": "0",
      "numOutputRows": "10",
      "numOutputBytes": "2176"
    },
    "engineInfo": "Databricks-Runtime/18.0.x-aarch64-photon-scala2.13",
    "txnId": "668a5c57-4be8-463a-9a86-070e9a66e2d7"
  }
}
```

| Field | Description |
|-------|-------------|
| `timestamp` | Unix epoch (ms) when the commit was made |
| `userId` / `userName` | Who executed the operation (audit trail) |
| `operation` | Type of DML: WRITE, UPDATE, DELETE, MERGE, OPTIMIZE, etc. |
| `operationParameters.mode` | Write mode: Overwrite, Append, ErrorIfExists |
| `operationParameters.partitionBy` | Partition columns (empty = unpartitioned) |
| `notebook.notebookId` | Which Databricks notebook triggered this |
| `clusterId` | Which compute cluster executed the operation |
| `isolationLevel` | Transaction isolation: WriteSerializable or Serializable |
| `isBlindAppend` | `true` = no existing data read; `false` = read existing data |
| `operationMetrics.numFiles` | Number of Parquet files written |
| `operationMetrics.numOutputRows` | Total rows written |
| `operationMetrics.numOutputBytes` | Total bytes written |
| `operationMetrics.numDeletionVectorsRemoved` | DVs cleaned up in this operation |
| `engineInfo` | Runtime version (e.g., Databricks 18.0 with Photon) |
| `txnId` | Unique transaction ID (idempotency key) |

#### Action 2: `metaData` -- Table Schema and Configuration

```json
{
  "metaData": {
    "id": "6b160c91-2e6c-474b-a5a2-eea475991b06",
    "format": { "provider": "parquet", "options": {} },
    "schemaString": "{\"type\":\"struct\",\"fields\":[...]}",
    "partitionColumns": [],
    "configuration": {
      "delta.enableDeletionVectors": "true"
    },
    "createdTime": 1772232860968
  }
}
```

| Field | Description |
|-------|-------------|
| `id` | Unique table identifier (UUID, never changes after creation) |
| `format.provider` | Underlying data format (always `parquet` for Delta) |
| `schemaString` | Full schema as JSON -- column names, types, nullable flags |
| `partitionColumns` | Columns used for partitioning (empty = unpartitioned) |
| `configuration` | Table properties set by user or Databricks defaults |
| `configuration.delta.enableDeletionVectors` | `true` = DVs enabled (Databricks default) |
| `createdTime` | Unix epoch (ms) when the table was first created |

The `schemaString` deserializes to:

```
  Column Name      Type      Nullable
  -----------      ----      --------
  order_id         integer   true
  customer_name    string    true
  product          string    true
  quantity         integer   true
  price            double    true
  order_date       string    true
```

#### Action 3: `protocol` -- Reader/Writer Version Requirements

```json
{
  "protocol": {
    "minReaderVersion": 3,
    "minWriterVersion": 7,
    "readerFeatures": ["deletionVectors"],
    "writerFeatures": ["deletionVectors", "appendOnly", "invariants"]
  }
}
```

| Field | Description |
|-------|-------------|
| `minReaderVersion` | Minimum Delta reader protocol version required to read this table |
| `minWriterVersion` | Minimum Delta writer protocol version required to write to this table |
| `readerFeatures` | Reader must support these features (e.g., `deletionVectors`) |
| `writerFeatures` | Writer must support these features |

```
  Protocol Version Matrix:
  ========================

  Reader v1: Basic Delta reading
  Reader v2: Column mapping
  Reader v3: Deletion vectors, table features    <-- Our table

  Writer v2: Append-only tables
  Writer v3: CHECK constraints
  Writer v4: Change Data Feed
  Writer v5: Column mapping
  Writer v7: Table features (deletion vectors,   <-- Our table
             row tracking, etc.)

  Higher versions = more features, but older engines
  may not be able to read/write the table.
```

#### Action 4: `add` -- A New Data File

```json
{
  "add": {
    "path": "part-00000-19111d98-...snappy.parquet",
    "partitionValues": {},
    "size": 2176,
    "modificationTime": 1772232863000,
    "dataChange": true,
    "stats": "{\"numRecords\":10,\"minValues\":{...},\"maxValues\":{...},\"nullCount\":{...},\"tightBounds\":true}",
    "tags": {
      "INSERTION_TIME": "1772232863000000",
      "MIN_INSERTION_TIME": "1772232863000000",
      "MAX_INSERTION_TIME": "1772232863000000",
      "OPTIMIZE_TARGET_SIZE": "268435456"
    }
  }
}
```

| Field | Description |
|-------|-------------|
| `path` | Relative path to the Parquet file within the table directory |
| `partitionValues` | Partition column values (empty `{}` = unpartitioned) |
| `size` | File size in bytes (2,176 bytes = ~2 KB) |
| `modificationTime` | When the file was last modified (epoch ms) |
| `dataChange` | `true` = this file changes query results; `false` = maintenance only (OPTIMIZE) |
| `stats` | Per-file statistics for data skipping (see below) |
| `tags.INSERTION_TIME` | Microsecond timestamp for row-level ordering |
| `tags.OPTIMIZE_TARGET_SIZE` | Target file size for OPTIMIZE (268,435,456 = 256 MB) |

**Data Skipping Statistics (`stats`):**

```json
{
  "numRecords": 10,
  "minValues": {
    "order_id": 1,
    "customer_name": "Alice Johnson",
    "product": "Headphones",
    "price": 9.99,
    "order_date": "2025-01-15"
  },
  "maxValues": {
    "order_id": 10,
    "customer_name": "Jack Anderson",
    "product": "Webcam",
    "price": 1299.99,
    "order_date": "2025-01-19"
  },
  "nullCount": {
    "order_id": 0,
    "customer_name": 0,
    "product": 0,
    "quantity": 0,
    "price": 0,
    "order_date": 0
  },
  "tightBounds": true
}
```

| Stats Field | Description |
|-------------|-------------|
| `numRecords` | Total number of rows in this file |
| `minValues` | Minimum value per column (used for data skipping) |
| `maxValues` | Maximum value per column (used for data skipping) |
| `nullCount` | Number of null values per column |
| `tightBounds` | `true` = stats reflect exact live rows; `false` = stats include logically deleted rows |

```
  Data Skipping with Stats:
  =========================

  Query: SELECT * FROM orders WHERE order_id = 15

  File stats say: minValues.order_id=1, maxValues.order_id=10
  Decision: SKIP this file (15 is outside [1, 10] range)

  This avoids reading the file entirely!
  For tables with 1000s of files, this is a massive speedup.
```

### Version 1 (0001.json): UPDATE with Deletion Vectors ON

An UPDATE with deletion vectors enabled produces a **very different** pattern
than traditional copy-on-write:

```json
{
  "commitInfo": {
    "operation": "UPDATE",
    "operationParameters": {
      "predicate": "[\"(order_id#13177 = 1)\"]"
    },
    "operationMetrics": {
      "numRemovedFiles": "0",
      "numCopiedRows": "0",
      "numDeletionVectorsAdded": "1",
      "numAddedFiles": "1",
      "numUpdatedRows": "1",
      "numDeletionVectorsRemoved": "0",
      "numAddedBytes": "1706"
    }
  }
}
```

Key metrics to understand:

| Metric | Value | Meaning |
|--------|-------|---------|
| `numRemovedFiles` | **0** | NO files were deleted -- the original stays! |
| `numCopiedRows` | **0** | NO rows were copied (unlike copy-on-write) |
| `numDeletionVectorsAdded` | **1** | A deletion vector was created to mark the old row |
| `numAddedFiles` | **1** | One new file with just the updated row |
| `numUpdatedRows` | **1** | Only 1 row was affected |
| `numAddedBytes` | **1706** | Only 1,706 bytes written (just the updated row) |

The commit contains three file-level actions:

```
  Action 1: remove (logically remove the ORIGINAL file)
  +---------------------------------------------------------+
  | remove: part-00000-19111d98-...snappy.parquet            |
  | - This file is no longer valid as-is                     |
  | - But the PHYSICAL file stays on disk                    |
  +---------------------------------------------------------+

  Action 2: add (new file with ONLY the updated row)
  +---------------------------------------------------------+
  | add: part-00000-fa3ea6f3-...snappy.parquet               |
  | - Contains 1 record (the updated row with price=1000.99) |
  | - size: 1,706 bytes                                      |
  | - tightBounds: true (stats are exact)                    |
  +---------------------------------------------------------+

  Action 3: add (re-add original file WITH a deletion vector)
  +---------------------------------------------------------+
  | add: part-00000-19111d98-...snappy.parquet               |
  | - Same file! But now with a deletion vector attached     |
  | - deletionVector.cardinality: 1 (1 row marked deleted)  |
  | - tightBounds: false (stats no longer exact due to DV)  |
  +---------------------------------------------------------+
```

**The deletion vector on the re-added file:**

```json
{
  "deletionVector": {
    "storageType": "u",
    "pathOrInlineDv": "soljSmI4&[X=W8/KB5eZ",
    "offset": 1,
    "sizeInBytes": 34,
    "cardinality": 1
  }
}
```

| DV Field | Description |
|----------|-------------|
| `storageType` | `u` = UUID-named file in table directory; `i` = inline (in the JSON); `p` = absolute path |
| `pathOrInlineDv` | Reference to the DV file or inline bitmap data |
| `offset` | Starting byte offset within the DV file |
| `sizeInBytes` | Size of the deletion vector data (34 bytes -- very small!) |
| `cardinality` | Number of rows marked as deleted (1 row in this case) |

### Version 2 (0002.json): DELETE with Deletion Vectors ON

Deleting `order_id = 2` updates the existing deletion vector:

```json
{
  "commitInfo": {
    "operation": "DELETE",
    "operationMetrics": {
      "numRemovedFiles": "0",
      "numCopiedRows": "0",
      "numDeletionVectorsAdded": "1",
      "numDeletionVectorsUpdated": "1",
      "numDeletionVectorsRemoved": "1",
      "numDeletedRows": "1",
      "numAddedFiles": "0",
      "numAddedBytes": "0"
    }
  }
}
```

| Metric | Value | Meaning |
|--------|-------|---------|
| `numRemovedFiles` | **0** | No files deleted (DV handles it) |
| `numCopiedRows` | **0** | No rows copied |
| `numDeletionVectorsAdded` | **1** | New DV with 2 rows marked |
| `numDeletionVectorsUpdated` | **1** | The previous DV was updated |
| `numDeletionVectorsRemoved` | **1** | The old DV (cardinality=1) was replaced |
| `numDeletedRows` | **1** | 1 row deleted |
| `numAddedFiles` | **0** | NO new data files at all! |
| `numAddedBytes` | **0** | ZERO bytes written -- just a DV update |

The file is re-added with an updated deletion vector:

```json
{
  "deletionVector": {
    "storageType": "u",
    "pathOrInlineDv": "bNf0[)AFwgYi1-t@b3^r",
    "offset": 1,
    "sizeInBytes": 36,
    "cardinality": 2
  }
}
```

Notice `cardinality: 2` -- now two rows are marked as deleted (the previously
updated row and the newly deleted row).

```
  Deletion Vector State After Version 2:
  =======================================

  Original file: part-00000-19111d98-...snappy.parquet (10 rows)
  DV marks:      row for order_id=1 (updated in v1) + order_id=2 (deleted in v2)
  Live rows:     8 rows from original file
  Plus:          1 new file with updated order_id=1 data

  Total live rows: 8 + 1 = 9 rows
```

### Version 3 (0003.json): Auto-OPTIMIZE Compaction

Databricks automatically triggers OPTIMIZE to resolve deletion vectors and
compact files:

```json
{
  "commitInfo": {
    "operation": "OPTIMIZE",
    "operationParameters": {
      "auto": true
    },
    "operationMetrics": {
      "numRemovedFiles": "2",
      "numRemovedBytes": "3882",
      "numDeletionVectorsRemoved": "1",
      "numAddedFiles": "1",
      "numAddedBytes": "2163"
    }
  }
}
```

| Metric | Value | Meaning |
|--------|-------|---------|
| `auto` | **true** | Databricks triggered this automatically |
| `numRemovedFiles` | **2** | Both old files removed (original + update) |
| `numRemovedBytes` | **3882** | 3,882 bytes freed from stale files |
| `numDeletionVectorsRemoved` | **1** | The DV is no longer needed |
| `numAddedFiles` | **1** | One clean compacted file |
| `numAddedBytes` | **2163** | Final file is 2,163 bytes |

The compacted file has **9 records** (the 10 original minus the deleted row,
with the updated row merged in) and `tightBounds: true` (stats are exact again).

Notice the `remove` actions contain compaction metadata:

```json
{
  "tags": {
    "compactedInto": "[\"part-00000-cab3ba72-...snappy.parquet\"]"
  }
}
```

And the `remove` actions have `dataChange: false` -- meaning this is a
**maintenance operation** that does not change query results.

## Deletion Vectors: The Complete Picture

### What Are Deletion Vectors?

Deletion vectors (DVs) are a **Databricks-specific optimization** that avoids
rewriting entire Parquet files for UPDATE and DELETE operations.

```
  Traditional Copy-on-Write              Deletion Vectors
  (delta.enableDeletionVectors=false)    (delta.enableDeletionVectors=true)
  ====================================  ====================================

  UPDATE 1 row in a 1 GB file:          UPDATE 1 row in a 1 GB file:

  1. Read entire 1 GB file              1. Write new file with updated row
  2. Modify the 1 row                   2. Create DV (34 bytes!) marking
  3. Write NEW 1 GB file                   old row as deleted
  4. Remove old file from log           3. Re-add original file with DV

  Cost: ~1 GB read + ~1 GB write        Cost: ~1 KB write + 34 bytes DV
  Time: Minutes                         Time: Seconds
```

### Deletion Vectors ON vs OFF: Side-by-Side Comparison

| Aspect | DV ON (Default) | DV OFF (Copy-on-Write) |
|--------|-----------------|------------------------|
| **UPDATE 1 row in 1 GB file** | Writes ~1 KB + DV | Reads + rewrites ~1 GB |
| **DELETE 1 row** | Updates DV (0 data bytes!) | Copies all other rows to new file |
| **Write speed** | Very fast (small writes) | Slow (full file rewrite) |
| **Read speed** | Slightly slower (must check DVs) | Fast (no DV overhead) |
| **File count growth** | Moderate (new files for updated rows) | High (full rewrites create new files) |
| **Storage overhead** | Small (DVs are tiny, ~34 bytes) | Large (duplicate data during rewrites) |
| **OPTIMIZE resolves** | Merges data + applies DVs | Just compacts files |
| **Protocol requirement** | Reader v3, Writer v7 | Reader v1, Writer v2 |
| **Compatibility** | Databricks-only (without UniForm) | Any Delta reader |

### Creating Tables with Deletion Vectors OFF

To disable deletion vectors, set the table property:

```sql
-- Method 1: At table creation
CREATE TABLE orders_deltaoff
USING DELTA
TBLPROPERTIES ('delta.enableDeletionVectors' = 'false')
LOCATION 's3://bucket/orders-deltaoff';

-- Method 2: On existing table
ALTER TABLE orders_deltaoff
SET TBLPROPERTIES ('delta.enableDeletionVectors' = 'false');

-- Method 3: During DataFrame write
df.write \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .mode("overwrite") \
  .save("s3://bucket/orders-deltaoff")
```

### What Happens During UPDATE with DV OFF

```
  UPDATE orders SET price = price + 1 WHERE order_id = 1

  With DV OFF (copy-on-write):
  ============================

  Step 1: Scan files to find order_id = 1
          Found in: part-00000-19111d98-...snappy.parquet (10 rows)

  Step 2: Read ALL 10 rows from the file

  Step 3: Modify row with order_id = 1 (price + 1)

  Step 4: Write ALL 10 rows to a NEW file
          New file: part-00000-abc123-...snappy.parquet (10 rows)

  Step 5: Transaction log:
          - remove: part-00000-19111d98-...snappy.parquet
          - add: part-00000-abc123-...snappy.parquet

  Result: 10 rows copied just to update 1 row!
```

```
  UPDATE orders SET price = price + 1 WHERE order_id = 1

  With DV ON (deletion vectors):
  ==============================

  Step 1: Scan files to find order_id = 1
          Found in: part-00000-19111d98-...snappy.parquet (10 rows)

  Step 2: Read ONLY the 1 matching row

  Step 3: Write ONLY the updated row to a new file
          New file: part-00000-fa3ea6f3-...snappy.parquet (1 row)

  Step 4: Create deletion vector marking old row as deleted
          DV: 34 bytes, cardinality = 1

  Step 5: Transaction log:
          - remove: part-00000-19111d98-...snappy.parquet (logical)
          - add: part-00000-fa3ea6f3-...snappy.parquet (1 row)
          - add: part-00000-19111d98-...snappy.parquet + DV (9 live rows)

  Result: Only 1 row written! Original file untouched!
```

### tightBounds: Understanding Stats Accuracy

The `tightBounds` field in `stats` is critical for understanding data skipping
accuracy:

```
  tightBounds = true
  ==================
  Stats are EXACT. Every row counted in numRecords, minValues,
  maxValues is a live row. Data skipping is fully accurate.

  tightBounds = false
  ===================
  Stats include logically deleted rows (marked by DV).
  numRecords may be HIGHER than actual live rows.
  minValues/maxValues may be WIDER than actual live range.
  Data skipping is still safe but may read extra files.

  Example:
    File has 10 physical rows, 2 deleted by DV
    tightBounds=false: numRecords=10 (includes deleted)
    After OPTIMIZE:    numRecords=8  (tightBounds=true again)
```

## Auto-Optimize and Configuration

### Auto-Optimize Settings

Databricks can automatically optimize tables:

```sql
-- Table-level auto-optimize settings
ALTER TABLE orders_external
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',   -- Coalesce small files on write
  'delta.autoOptimize.autoCompact'   = 'true'    -- Auto-compact after writes
);

-- Disable auto-optimize (for manual control)
ALTER TABLE orders_external
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'false',
  'delta.autoOptimize.autoCompact'   = 'false'
);

-- Session-level settings
SET spark.databricks.delta.optimizeWrite.enabled = false;
SET spark.databricks.delta.autoCompact.enabled = false;
```

| Setting | Default | Purpose |
|---------|---------|---------|
| `optimizeWrite` | true (Databricks) | Coalesces small partitions during writes |
| `autoCompact` | true (Databricks) | Automatically runs OPTIMIZE after writes |

### Checkpoint Interval

Checkpoints aggregate all actions into a Parquet file for fast state reconstruction:

```python
# Default: checkpoint every 10 commits
# Change to checkpoint every commit (useful for debugging):
spark.conf.set("spark.databricks.delta.checkpointInterval", "1")
```

### VACUUM Behavior with Deletion Vectors

VACUUM works the same regardless of DV on/off, but interacts differently:

```
  With DV ON:
  ===========
  VACUUM removes:
  - Stale Parquet files (no longer referenced)
  - Old deletion vector files
  - Files compacted by OPTIMIZE

  With DV OFF:
  ============
  VACUUM removes:
  - Old Parquet files replaced by copy-on-write rewrites
  - No DV files exist to clean up
```

```sql
-- Default retention: 7 days (168 hours)
VACUUM orders_external;

-- Aggressive: 0 hours (breaks time travel for old versions!)
-- Requires safety check disabled:
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM orders_external RETAIN 0 HOURS;
```

## When to Use DV ON vs OFF

| Scenario | Recommendation | Rationale |
|----------|---------------|-----------|
| General-purpose tables | **DV ON** (default) | Fastest writes, reasonable reads |
| Heavy UPDATE/DELETE workloads | **DV ON** | Avoids constant file rewrites |
| Read-heavy / minimal DML | **DV OFF** | No DV overhead on reads |
| Cross-engine compatibility | **DV OFF** | Not all engines support DVs |
| Very large files (>1 GB) | **DV ON** | Avoids rewriting multi-GB files |
| Streaming append-only | Either | DVs are not used for appends |

## Hands-On Walkthrough

Open the companion notebook `10-external-delta-tables-s3_notebook.py` in your
Databricks workspace. The notebook walks you through:

1. Writing Delta data to S3 and inspecting the directory structure
2. Reading and decoding transaction log JSON files field-by-field
3. Creating an external table on S3
4. UPDATE and DELETE operations with deletion vectors ON (default)
5. Inspecting how deletion vectors appear in the log
6. Creating a second table with deletion vectors OFF
7. The same UPDATE/DELETE with copy-on-write behavior
8. OPTIMIZE and VACUUM on both tables
9. Side-by-side comparison of DV ON vs OFF

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| External Table Location | `s3://bucket/path` | `abfss://container@account.dfs.core.windows.net/path` | `gs://bucket/path` |
| IAM for External Tables | S3 bucket policy + IAM role | Storage account RBAC | GCS IAM |
| Deletion Vector Support | Full (Databricks) | Full (Databricks) | Full (Databricks) |
| Cross-workspace Access | Via Unity Catalog external locations | Via Unity Catalog | Via Unity Catalog |

## Certification Tip

The **Databricks Data Engineer Professional** exam tests:

- "What is the difference between managed and external tables?" -- Know DROP behavior
- "What are deletion vectors?" -- Lightweight bitmaps that mark rows as deleted
  without rewriting files
- "How does OPTIMIZE interact with deletion vectors?" -- OPTIMIZE resolves DVs by
  rewriting files with deleted rows excluded
- "What does `dataChange: false` mean in a commit?" -- Maintenance operation
  (OPTIMIZE, VACUUM) that does not change query results
- "What happens to time travel after VACUUM?" -- Old versions that depend on
  removed files become inaccessible

## Key Takeaways

1. **External tables** decouple storage from compute -- data lives on S3 and
   survives DROP TABLE. Use them for shared or externally-produced data.
2. The Delta transaction log contains four key actions: `commitInfo` (audit),
   `metaData` (schema), `protocol` (version requirements), and `add`/`remove`
   (file tracking).
3. **Deletion vectors** (default ON in Databricks) avoid rewriting entire files
   for UPDATE/DELETE. They create tiny bitmaps (~34 bytes) marking deleted rows.
4. With DVs ON, `tightBounds: false` means file statistics include logically
   deleted rows -- data skipping is still safe but slightly less precise until
   OPTIMIZE resolves the DVs.
5. **Auto-OPTIMIZE** automatically compacts files and resolves deletion vectors,
   restoring `tightBounds: true` and cleaning up small files.
6. With DVs OFF (copy-on-write), every UPDATE/DELETE rewrites the entire affected
   file, which is slower but simpler and more compatible.
7. Use **VACUUM** to reclaim storage from stale files, but be aware it breaks
   time travel for old versions.

## Next Steps

You've now mastered both managed and external Delta tables, including the
internal mechanics of the transaction log and deletion vectors. Return to the
[Module README](README.md) for the complete topic list, or proceed to build
production pipelines in **[Module 04: Data Engineering Pipelines](../04-data-engineering-pipelines/)**.
