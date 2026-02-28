# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # External Delta Tables on S3 & Deletion Vectors
# MAGIC > Module 03 -- Topic 10 | Companion Notebook
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC
# MAGIC 1. Writing Delta data to S3 and inspecting the `_delta_log`
# MAGIC 2. Decoding every field in transaction log JSON files
# MAGIC 3. Creating external tables on S3
# MAGIC 4. UPDATE/DELETE behavior with **deletion vectors ON** (default)
# MAGIC 5. UPDATE/DELETE behavior with **deletion vectors OFF** (copy-on-write)
# MAGIC 6. OPTIMIZE resolving deletion vectors
# MAGIC 7. VACUUM, auto-optimize, and checkpoint configuration
# MAGIC
# MAGIC **Prerequisites:** A Databricks workspace with an S3 bucket configured
# MAGIC as an external location in Unity Catalog.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG databricks_pro;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS databricks_pro.default;
# MAGIC USE SCHEMA default;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DoubleType, TimestampType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Generate Sample Data
# MAGIC
# MAGIC We create a small e-commerce orders dataset. The same data will be written
# MAGIC to two separate S3 paths -- one with deletion vectors ON, one with OFF.

# COMMAND ----------

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("price", DoubleType(), True),
    StructField("order_date", StringType(), True),
])

data = [
    (1, "Alice Johnson", "Laptop", 1, 999.99, "2025-01-15"),
    (2, "Bob Smith", "Mouse", 2, 29.99, "2025-01-15"),
    (3, "Carol White", "Keyboard", 1, 79.99, "2025-01-16"),
    (4, "David Brown", "Monitor", 1, 349.99, "2025-01-16"),
    (5, "Eve Davis", "USB Cable", 5, 9.99, "2025-01-17"),
    (6, "Frank Miller", "Webcam", 1, 59.99, "2025-01-17"),
    (7, "Grace Lee", "Headphones", 2, 149.99, "2025-01-18"),
    (8, "Hank Wilson", "Laptop", 1, 1299.99, "2025-01-18"),
    (9, "Iris Taylor", "Mouse", 3, 24.99, "2025-01-19"),
    (10, "Jack Anderson", "SSD Drive", 1, 89.99, "2025-01-19"),
]

df = spark.createDataFrame(data, schema=schema)
print(f"Row count: {df.count()}")
df.display(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART A: Deletion Vectors ON (Default Behavior)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Write Delta Data to S3 (DV ON)
# MAGIC
# MAGIC By default, Databricks enables deletion vectors. When we write Delta data
# MAGIC to S3, the `_delta_log/0000.json` will contain `delta.enableDeletionVectors: true`
# MAGIC in the `metaData` action.

# COMMAND ----------

TABLE_PATH = "s3://databricks-zero-to-pro/orders"

# Remove any previous data at this path
dbutils.fs.rm(TABLE_PATH, recurse=True)

# Write as Delta table (deletion vectors ON by default)
df.write.format("delta").mode("overwrite").save(TABLE_PATH)

print(f"Delta data written to: {TABLE_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Inspect the Directory Structure
# MAGIC
# MAGIC The S3 path now contains:
# MAGIC - `_delta_log/` directory with transaction log JSON files
# MAGIC - Parquet data file(s)

# COMMAND ----------

files = dbutils.fs.ls(TABLE_PATH)
for f in files:
    print(f"{'[DIR] ' if f.isDir() else '      '}{f.name:50s}  {f.size:>10} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Read the Transaction Log (Version 0)
# MAGIC
# MAGIC Version 0 contains **four actions**: `commitInfo`, `metaData`, `protocol`, `add`.
# MAGIC Each line in the JSON file is a separate action.

# COMMAND ----------

log_path = f"{TABLE_PATH}/_delta_log"
log_files = dbutils.fs.ls(log_path)

print("Transaction log files:")
print("-" * 60)
for f in log_files:
    print(f"  {f.name:50s}  {f.size:>8} bytes")

# COMMAND ----------

# Read version 0 as raw text to see each action
commit_df = spark.read.text(f"{log_path}/00000000000000000000.json")
commit_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decode Version 0 Field-by-Field
# MAGIC
# MAGIC Let's parse each action and understand every field:
# MAGIC
# MAGIC - **commitInfo**: Audit trail (who, what, when, how many rows/bytes)
# MAGIC - **metaData**: Table schema, partition columns, configuration (DV enabled)
# MAGIC - **protocol**: Min reader/writer versions, required features
# MAGIC - **add**: New data file with path, size, and data skipping statistics

# COMMAND ----------

import json

commit_lines = commit_df.collect()
print("=" * 70)
print("ACTIONS IN VERSION 0 (Initial WRITE)")
print("=" * 70)
for row in commit_lines:
    parsed = json.loads(row[0])
    action_type = list(parsed.keys())[0]
    print(f"\n{'─' * 70}")
    print(f"ACTION: {action_type}")
    print(f"{'─' * 70}")
    print(json.dumps(parsed[action_type], indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Understanding the `add` Action Statistics
# MAGIC
# MAGIC The `stats` field in each `add` action enables **data skipping**.
# MAGIC Delta uses min/max values per column to skip entire files that
# MAGIC cannot contain matching rows.

# COMMAND ----------

add_actions = []
for row in commit_lines:
    parsed = json.loads(row[0])
    if "add" in parsed:
        add_actions.append(parsed["add"])

for i, action in enumerate(add_actions):
    print(f"\n{'=' * 60}")
    print(f"File {i}: {action['path']}")
    print(f"{'=' * 60}")
    print(f"  Size:              {action['size']:,} bytes")
    print(f"  Modification Time: {action['modificationTime']}")
    print(f"  Data Change:       {action['dataChange']}")
    if "stats" in action and action["stats"]:
        stats = json.loads(action["stats"])
        print(f"  Num Records:       {stats.get('numRecords', 'N/A')}")
        print(f"  Tight Bounds:      {stats.get('tightBounds', 'N/A')}")
        if "minValues" in stats:
            print(f"  Min Values:        {json.dumps(stats['minValues'])}")
        if "maxValues" in stats:
            print(f"  Max Values:        {json.dumps(stats['maxValues'])}")
        if "nullCount" in stats:
            print(f"  Null Count:        {json.dumps(stats['nullCount'])}")
    if "deletionVector" in action:
        dv = action["deletionVector"]
        print(f"  Deletion Vector:")
        print(f"    Storage Type:    {dv['storageType']}")
        print(f"    Size (bytes):    {dv['sizeInBytes']}")
        print(f"    Cardinality:     {dv['cardinality']} rows marked deleted")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Register as External Table
# MAGIC
# MAGIC Now that Delta data exists on S3, we register it as a table in Unity Catalog.
# MAGIC The `LOCATION` clause makes this an **external table** -- Databricks stores
# MAGIC metadata but YOU own the data on S3.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS orders_external
# MAGIC USING DELTA
# MAGIC LOCATION 's3://databricks-zero-to-pro/orders';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify the table reads correctly
# MAGIC SELECT * FROM orders_external;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check table properties -- notice delta.enableDeletionVectors = true
# MAGIC SHOW TBLPROPERTIES orders_external;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: UPDATE with Deletion Vectors ON
# MAGIC
# MAGIC With DV ON, UPDATE does NOT rewrite the entire file. Instead:
# MAGIC 1. A small new file is written with ONLY the updated row
# MAGIC 2. A deletion vector (34 bytes) marks the old row as deleted
# MAGIC 3. The original file stays on disk untouched
# MAGIC
# MAGIC Check `numCopiedRows: 0` and `numDeletionVectorsAdded: 1` in the metrics.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_external SET price = price + 1 WHERE order_id = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Version 1 Log -- See the Deletion Vector in Action

# COMMAND ----------

# Read the version 1 commit to see how DV handles the UPDATE
v1_df = spark.read.json(f"{TABLE_PATH}/_delta_log/00000000000000000001.json")
display(v1_df)

# COMMAND ----------

# Parse version 1 in detail
v1_text = spark.read.text(f"{TABLE_PATH}/_delta_log/00000000000000000001.json")
v1_lines = v1_text.collect()

print("=" * 70)
print("ACTIONS IN VERSION 1 (UPDATE with DV ON)")
print("=" * 70)
for row in v1_lines:
    parsed = json.loads(row[0])
    action_type = list(parsed.keys())[0]
    print(f"\n{'─' * 70}")
    print(f"ACTION: {action_type}")
    print(f"{'─' * 70}")
    content = parsed[action_type]
    # Highlight key DV-related fields
    if action_type == "commitInfo":
        metrics = content.get("operationMetrics", {})
        print(f"  Operation:                {content.get('operation')}")
        print(f"  numRemovedFiles:          {metrics.get('numRemovedFiles')} (0 = no file rewrite!)")
        print(f"  numCopiedRows:            {metrics.get('numCopiedRows')} (0 = no rows copied!)")
        print(f"  numDeletionVectorsAdded:  {metrics.get('numDeletionVectorsAdded')} (DV created)")
        print(f"  numUpdatedRows:           {metrics.get('numUpdatedRows')}")
        print(f"  numAddedFiles:            {metrics.get('numAddedFiles')} (new file for updated row)")
        print(f"  numAddedBytes:            {metrics.get('numAddedBytes')} bytes (tiny!)")
    elif action_type == "add" and "deletionVector" in content:
        dv = content["deletionVector"]
        print(f"  File: {content['path']}")
        print(f"  DELETION VECTOR ATTACHED:")
        print(f"    storageType:   {dv['storageType']} (u=UUID file, i=inline, p=absolute path)")
        print(f"    sizeInBytes:   {dv['sizeInBytes']} bytes (tiny bitmap!)")
        print(f"    cardinality:   {dv['cardinality']} row(s) marked as deleted")
        stats = json.loads(content.get("stats", "{}"))
        print(f"    tightBounds:   {stats.get('tightBounds')} (false = stats include deleted rows)")
    else:
        print(json.dumps(content, indent=2)[:500])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: DELETE with Deletion Vectors ON
# MAGIC
# MAGIC DELETE with DV ON is even more efficient -- it writes ZERO new data bytes.
# MAGIC It simply updates the deletion vector to mark another row as deleted.
# MAGIC
# MAGIC Check `numAddedFiles: 0` and `numAddedBytes: 0` in the metrics.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM orders_external WHERE order_id = 2;

# COMMAND ----------

# Parse version 2 (DELETE)
v2_text = spark.read.text(f"{TABLE_PATH}/_delta_log/00000000000000000002.json")
v2_lines = v2_text.collect()

print("=" * 70)
print("ACTIONS IN VERSION 2 (DELETE with DV ON)")
print("=" * 70)
for row in v2_lines:
    parsed = json.loads(row[0])
    action_type = list(parsed.keys())[0]
    print(f"\n{'─' * 70}")
    print(f"ACTION: {action_type}")
    print(f"{'─' * 70}")
    content = parsed[action_type]
    if action_type == "commitInfo":
        metrics = content.get("operationMetrics", {})
        print(f"  Operation:                 {content.get('operation')}")
        print(f"  numRemovedFiles:           {metrics.get('numRemovedFiles')} (0 = no file rewrite!)")
        print(f"  numCopiedRows:             {metrics.get('numCopiedRows')} (0 = no rows copied!)")
        print(f"  numAddedFiles:             {metrics.get('numAddedFiles')} (0 = NO new data files!)")
        print(f"  numAddedBytes:             {metrics.get('numAddedBytes')} (ZERO bytes written!)")
        print(f"  numDeletedRows:            {metrics.get('numDeletedRows')}")
        print(f"  numDeletionVectorsAdded:   {metrics.get('numDeletionVectorsAdded')}")
        print(f"  numDeletionVectorsUpdated: {metrics.get('numDeletionVectorsUpdated')}")
        print(f"  numDeletionVectorsRemoved: {metrics.get('numDeletionVectorsRemoved')} (old DV replaced)")
    elif action_type == "add" and "deletionVector" in content:
        dv = content["deletionVector"]
        print(f"  File: {content['path']}")
        print(f"  UPDATED DELETION VECTOR:")
        print(f"    cardinality: {dv['cardinality']} rows now marked as deleted")
        print(f"    (was 1 after UPDATE, now 2 after DELETE)")
    else:
        print(json.dumps(content, indent=2)[:500])

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Table history shows all operations
# MAGIC DESCRIBE HISTORY orders_external;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Inspect Physical Files After DV Operations
# MAGIC
# MAGIC Let's see what files actually exist on S3. With DV ON, the original file
# MAGIC is still there -- it has NOT been rewritten.

# COMMAND ----------

display(dbutils.fs.ls(TABLE_PATH))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Auto-OPTIMIZE Resolves Deletion Vectors
# MAGIC
# MAGIC Databricks auto-OPTIMIZE (or manual `OPTIMIZE`) resolves deletion vectors by:
# MAGIC 1. Reading all live rows (excluding DV-marked rows)
# MAGIC 2. Writing them into a new compacted file
# MAGIC 3. Removing old files and DVs from the log
# MAGIC
# MAGIC After OPTIMIZE, `tightBounds` returns to `true` and stats are exact again.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check if auto-optimize already ran (look for OPTIMIZE in history)
# MAGIC DESCRIBE HISTORY orders_external;

# COMMAND ----------

# If auto-OPTIMIZE hasn't run, trigger it manually:
# spark.sql("OPTIMIZE orders_external")

# Check the OPTIMIZE version log (version 3 if auto-OPTIMIZE ran)
try:
    v3_text = spark.read.text(f"{TABLE_PATH}/_delta_log/00000000000000000003.json")
    v3_lines = v3_text.collect()
    print("=" * 70)
    print("ACTIONS IN VERSION 3 (OPTIMIZE)")
    print("=" * 70)
    for row in v3_lines:
        parsed = json.loads(row[0])
        action_type = list(parsed.keys())[0]
        print(f"\n{'─' * 70}")
        print(f"ACTION: {action_type}")
        print(f"{'─' * 70}")
        content = parsed[action_type]
        if action_type == "commitInfo":
            metrics = content.get("operationMetrics", {})
            print(f"  Operation:                 {content.get('operation')}")
            print(f"  auto:                      {content.get('operationParameters', {}).get('auto')}")
            print(f"  numRemovedFiles:           {metrics.get('numRemovedFiles')} (old files removed)")
            print(f"  numDeletionVectorsRemoved: {metrics.get('numDeletionVectorsRemoved')} (DVs cleaned up)")
            print(f"  numAddedFiles:             {metrics.get('numAddedFiles')} (new compacted file)")
            print(f"  numAddedBytes:             {metrics.get('numAddedBytes')}")
        elif action_type == "add":
            stats = json.loads(content.get("stats", "{}"))
            print(f"  File: {content['path']}")
            print(f"  numRecords: {stats.get('numRecords')} (clean, no deleted rows)")
            print(f"  tightBounds: {stats.get('tightBounds')} (stats are exact again!)")
            print(f"  dataChange: {content.get('dataChange')} (false = maintenance only)")
        else:
            print(json.dumps(content, indent=2)[:400])
except Exception as e:
    print(f"Version 3 not yet available. Run OPTIMIZE manually:")
    print(f"  spark.sql('OPTIMIZE orders_external')")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC # PART B: Deletion Vectors OFF (Copy-on-Write Behavior)
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Write Delta Data with DV OFF
# MAGIC
# MAGIC Now we create the same table but with deletion vectors **disabled**.
# MAGIC This uses the traditional **copy-on-write** approach: every UPDATE or DELETE
# MAGIC rewrites the entire affected Parquet file.

# COMMAND ----------

TABLE_PATH_OFF = "s3://databricks-zero-to-pro/orders-deltaoff"

# Remove any previous data at this path
dbutils.fs.rm(TABLE_PATH_OFF, recurse=True)

# Write with deletion vectors DISABLED
df.write \
  .format("delta") \
  .option("delta.enableDeletionVectors", "false") \
  .mode("overwrite") \
  .save(TABLE_PATH_OFF)

print(f"Delta data (DV OFF) written to: {TABLE_PATH_OFF}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Register External Table (DV OFF)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS orders_deltaoff;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE orders_deltaoff
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableDeletionVectors' = 'false'
# MAGIC )
# MAGIC LOCATION 's3://databricks-zero-to-pro/orders-deltaoff';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify: notice delta.enableDeletionVectors = false
# MAGIC SHOW TBLPROPERTIES orders_deltaoff;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_deltaoff;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: UPDATE with Deletion Vectors OFF (Copy-on-Write)
# MAGIC
# MAGIC With DV OFF, the same UPDATE (`price + 1 WHERE order_id = 1`) will:
# MAGIC 1. Read the entire file containing order_id = 1
# MAGIC 2. Copy ALL rows to a new file (with the 1 row modified)
# MAGIC 3. Mark the old file as removed
# MAGIC
# MAGIC Compare the metrics: `numCopiedRows > 0`, `numDeletionVectorsAdded: 0`

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE orders_deltaoff SET price = price + 1 WHERE order_id = 1;

# COMMAND ----------

# Read the update commit log
v1_off_text = spark.read.text(f"{TABLE_PATH_OFF}/_delta_log/00000000000000000001.json")
v1_off_lines = v1_off_text.collect()

print("=" * 70)
print("UPDATE with DV OFF (Copy-on-Write)")
print("=" * 70)
for row in v1_off_lines:
    parsed = json.loads(row[0])
    action_type = list(parsed.keys())[0]
    content = parsed[action_type]
    if action_type == "commitInfo":
        metrics = content.get("operationMetrics", {})
        print(f"\n  Operation:          {content.get('operation')}")
        print(f"  numRemovedFiles:    {metrics.get('numRemovedFiles')} (old file removed)")
        print(f"  numCopiedRows:      {metrics.get('numCopiedRows')} (ALL other rows copied!)")
        print(f"  numUpdatedRows:     {metrics.get('numUpdatedRows')}")
        print(f"  numAddedFiles:      {metrics.get('numAddedFiles')}")
        print(f"  numAddedBytes:      {metrics.get('numAddedBytes')}")
        dv_added = metrics.get('numDeletionVectorsAdded', '0')
        print(f"  numDeletionVectorsAdded: {dv_added} (no DVs -- full rewrite!)")
    elif action_type == "add":
        stats = json.loads(content.get("stats", "{}"))
        print(f"\n  ADD: {content['path']}")
        print(f"  numRecords: {stats.get('numRecords')} (all rows rewritten)")
        print(f"  tightBounds: {stats.get('tightBounds')} (always true without DVs)")
        if "deletionVector" in content:
            print("  HAS deletion vector (unexpected!)")
        else:
            print("  No deletion vector (as expected)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 13: Side-by-Side Comparison
# MAGIC
# MAGIC Let's compare what happened with the same UPDATE on both tables:
# MAGIC
# MAGIC | Metric | DV ON | DV OFF |
# MAGIC |--------|-------|--------|
# MAGIC | numCopiedRows | 0 | 9 |
# MAGIC | numDeletionVectorsAdded | 1 | 0 |
# MAGIC | numRemovedFiles | 0 | 1 |
# MAGIC | numAddedFiles | 1 (just updated row) | 1 (all 10 rows) |
# MAGIC | Bytes written | ~1,706 | ~2,176 |
# MAGIC | Original file | Kept (with DV) | Replaced entirely |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 14: Physical File Comparison
# MAGIC
# MAGIC Let's look at the actual files on S3 for both tables.

# COMMAND ----------

print("=" * 60)
print("FILES ON S3 -- DV ON (orders)")
print("=" * 60)
for f in dbutils.fs.ls(TABLE_PATH):
    if not f.isDir():
        print(f"  {f.name:60s}  {f.size:>8} bytes")

print(f"\n{'=' * 60}")
print("FILES ON S3 -- DV OFF (orders-deltaoff)")
print("=" * 60)
for f in dbutils.fs.ls(TABLE_PATH_OFF):
    if not f.isDir():
        print(f"  {f.name:60s}  {f.size:>8} bytes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 15: Read Individual Parquet Files
# MAGIC
# MAGIC You can read the raw Parquet files to see exactly what rows they contain.
# MAGIC This helps understand the difference between DV ON (updated row in separate
# MAGIC file) vs DV OFF (all rows rewritten into one file).

# COMMAND ----------

# List parquet files for DV ON table
dv_on_files = [f for f in dbutils.fs.ls(TABLE_PATH) if f.name.endswith(".parquet")]
for f in dv_on_files:
    print(f"\nFile: {f.name} ({f.size} bytes)")
    df_file = spark.read.parquet(f.path)
    df_file.display()

# COMMAND ----------

# List parquet files for DV OFF table
dv_off_files = [f for f in dbutils.fs.ls(TABLE_PATH_OFF) if f.name.endswith(".parquet")]
for f in dv_off_files:
    print(f"\nFile: {f.name} ({f.size} bytes)")
    df_file = spark.read.parquet(f.path)
    df_file.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 16: VACUUM on Both Tables
# MAGIC
# MAGIC VACUUM removes stale files no longer referenced by the current table version.
# MAGIC - **DV ON**: Removes old DV files and pre-OPTIMIZE data files
# MAGIC - **DV OFF**: Removes old Parquet files replaced by copy-on-write rewrites

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Default VACUUM (7-day retention, safe)
# MAGIC -- VACUUM orders_external;
# MAGIC -- VACUUM orders_deltaoff;
# MAGIC
# MAGIC -- Aggressive VACUUM (0 hours, breaks time travel):
# MAGIC -- SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC -- VACUUM orders_external RETAIN 0 HOURS;
# MAGIC -- VACUUM orders_deltaoff RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 17: Auto-Optimize Configuration
# MAGIC
# MAGIC Databricks auto-optimize settings control whether files are automatically
# MAGIC compacted and coalesced. You can configure these at the table level or
# MAGIC session level.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Disable auto-optimize for manual control (useful for benchmarking)
# MAGIC ALTER TABLE orders_external
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.autoOptimize.optimizeWrite' = 'false',
# MAGIC   'delta.autoOptimize.autoCompact'   = 'false'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Session-level settings (apply to all tables in this session)
# MAGIC SET spark.databricks.delta.optimizeWrite.enabled = false;
# MAGIC SET spark.databricks.delta.autoCompact.enabled = false;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify settings
# MAGIC SHOW TBLPROPERTIES orders_external;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 18: Checkpoint Configuration
# MAGIC
# MAGIC Checkpoints aggregate all transaction log actions into a single Parquet file
# MAGIC for fast state reconstruction. Default is every 10 commits.

# COMMAND ----------

# Change checkpoint interval (useful for debugging -- checkpoint every commit)
spark.conf.set("spark.databricks.delta.checkpointInterval", "1")

# Trigger a read to force checkpoint creation
spark.read.format("delta").load(TABLE_PATH).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 19: Summary -- DV ON vs OFF Decision Guide
# MAGIC
# MAGIC | Scenario | Recommendation |
# MAGIC |----------|---------------|
# MAGIC | General-purpose tables | **DV ON** (default) |
# MAGIC | Heavy UPDATE/DELETE workloads | **DV ON** (avoids file rewrites) |
# MAGIC | Read-heavy, minimal DML | **DV OFF** (no DV overhead on reads) |
# MAGIC | Cross-engine compatibility | **DV OFF** (not all engines support DVs) |
# MAGIC | Very large files (>1 GB) | **DV ON** (avoids rewriting multi-GB files) |
# MAGIC | Append-only streaming | Either (DVs not used for appends) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Uncomment and run to clean up external tables and S3 data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove table metadata (data stays on S3 for external tables)
# MAGIC -- DROP TABLE IF EXISTS orders_external;
# MAGIC -- DROP TABLE IF EXISTS orders_deltaoff;

# COMMAND ----------

# Remove data from S3 (uncomment to actually delete)
# dbutils.fs.rm("s3://databricks-zero-to-pro/orders", recurse=True)
# dbutils.fs.rm("s3://databricks-zero-to-pro/orders-deltaoff", recurse=True)
# print("S3 data cleaned up.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC You now understand the full lifecycle of external Delta tables on S3 and
# MAGIC how deletion vectors change UPDATE/DELETE behavior. Return to the
# MAGIC [Module README](README.md) for the complete topic list.
