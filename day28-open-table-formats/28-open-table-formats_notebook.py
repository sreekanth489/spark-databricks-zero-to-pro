# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 28: Open Table Formats — Delta Lake, Apache Iceberg, Apache Hudi
# MAGIC
# MAGIC **Objective**: Understand and compare the three dominant open table formats through hands-on exploration
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Feel the pain of raw Parquet (no ACID, no deletes) — the world before open table formats
# MAGIC 2. Explore Delta Lake: transaction log, ACID commits, time travel, MERGE, OPTIMIZE
# MAGIC 3. Explore Apache Iceberg: metadata hierarchy, hidden partitioning, partition evolution, branches
# MAGIC 4. Explore Apache Hudi: CoW vs MoR storage types, indexed upserts, incremental reads
# MAGIC 5. Compare all three side-by-side on the same use case
# MAGIC 6. Enable Delta UniForm to expose a Delta table as Iceberg metadata
# MAGIC
# MAGIC **The Three Formats at a Glance**:
# MAGIC ```
# MAGIC ┌──────────────────────┬───────────────────┬──────────────────────┐
# MAGIC │    DELTA LAKE        │  APACHE ICEBERG   │    APACHE HUDI       │
# MAGIC │                      │                   │                      │
# MAGIC │  Created: Databricks │ Created: Netflix  │ Created: Uber        │
# MAGIC │  Year: 2019          │ Year: 2018        │ Year: 2017           │
# MAGIC │                      │                   │                      │
# MAGIC │  Best for:           │ Best for:         │ Best for:            │
# MAGIC │  Databricks-first    │ Multi-engine /    │ High-frequency CDC   │
# MAGIC │  Spark pipelines     │ multi-cloud       │ upserts, streaming   │
# MAGIC │  Unity Catalog       │ Snowflake + Spark │ low write latency    │
# MAGIC └──────────────────────┴───────────────────┴──────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks Runtime 13.0+ with Unity Catalog
# MAGIC **Prerequisites**: `CREATE TABLE` privilege in a UC catalog/schema

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Create sample data used throughout this notebook

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType
)

# Use the current user's default catalog/schema or set explicitly
CATALOG = spark.sql("SELECT current_catalog()").collect()[0][0]
SCHEMA  = "day28_open_table_formats"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Working in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate sample data: ride-sharing trips
# MAGIC
# MAGIC We use a ride-sharing dataset because it maps perfectly to all three format use cases:
# MAGIC - Trips are continuously inserted (streaming ingestion → Hudi strength)
# MAGIC - Trip status updates are frequent upserts (CDC → Hudi strength)
# MAGIC - Analytics scan billions of trips (read performance → Delta / Iceberg strength)
# MAGIC - GDPR: delete all trips for user_id X (row-level deletes → all three)

# COMMAND ----------

def generate_trips(n=50000, seed=42):
    random.seed(seed)
    statuses = ["REQUESTED", "ACCEPTED", "IN_PROGRESS", "COMPLETED", "CANCELLED"]
    cities   = ["New York", "Los Angeles", "Chicago", "San Francisco", "Seattle",
                "Austin", "Boston", "Denver", "Miami", "Atlanta"]

    base_time = datetime(2024, 1, 1)
    records = []
    for i in range(n):
        trip_id   = f"TRIP-{i:08d}"
        user_id   = f"USER-{random.randint(1, 5000):06d}"
        driver_id = f"DRV-{random.randint(1, 1000):05d}"
        city      = random.choice(cities)
        status    = statuses[random.randint(0, 4)]
        fare      = round(random.uniform(5.0, 120.0), 2)
        distance  = round(random.uniform(0.5, 30.0), 2)
        event_ts  = base_time + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        records.append((trip_id, user_id, driver_id, city, status, fare, distance, event_ts))

    schema = StructType([
        StructField("trip_id",   StringType(),    False),
        StructField("user_id",   StringType(),    False),
        StructField("driver_id", StringType(),    False),
        StructField("city",      StringType(),    True),
        StructField("status",    StringType(),    True),
        StructField("fare_usd",  DoubleType(),    True),
        StructField("distance_miles", DoubleType(), True),
        StructField("event_ts",  TimestampType(), True),
    ])
    return spark.createDataFrame(records, schema)

trips_df = generate_trips(50000)
print(f"Generated {trips_df.count():,} trips")
trips_df.printSchema()
trips_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: The World Without Open Table Formats
# MAGIC
# MAGIC Before we use Delta/Iceberg/Hudi, let's recreate the pain of raw Parquet.
# MAGIC This is what every data team dealt with before 2019.

# COMMAND ----------

# Write raw Parquet — no table format, just files
trips_df.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(f"/tmp/day28_raw_parquet/trips/")

print("Raw Parquet written to /tmp/day28_raw_parquet/trips/")

# COMMAND ----------

# Problem 1: No ACID — try to "delete" a user's data (GDPR compliance)
# With raw Parquet, you must:
#   1. Read the entire partition
#   2. Filter out the user
#   3. Rewrite the entire partition
#   4. Hope no one reads during the rewrite (no isolation!)

user_to_delete = "USER-001234"

# Step 1: Read the partition that contains the user
city_of_user = trips_df.filter(F.col("user_id") == user_to_delete) \
                        .select("city").first()

if city_of_user:
    city = city_of_user["city"]
    print(f"User {user_to_delete} found in city: {city}")
    print("To delete: must rewrite the entire city partition")
    print("While rewriting: readers see inconsistent data (old + new mixed)")
    print("If the job fails halfway: data is corrupted")
    print()
    print("This is the GDPR nightmare that open table formats solve.")
else:
    print("User not found in sample")

# COMMAND ----------

# Problem 2: No time travel
# With raw Parquet, once you overwrite a partition — history is gone.

# Simulate overwrite (write more trips with mode=overwrite)
new_batch = generate_trips(5000, seed=99)
new_batch.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(f"/tmp/day28_raw_parquet/trips/")

print("Overwrote Parquet data.")
print("The original 50,000 rows are GONE.")
print("No 'AS OF TIMESTAMP', no rollback, no history.")
print()
print("With Delta Lake / Iceberg / Hudi: the original version is still accessible.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: Delta Lake Deep Dive
# MAGIC
# MAGIC Delta Lake solves every problem we just saw by adding a **transaction log**
# MAGIC (`_delta_log/`) that tracks every change to the table as a series of JSON commits.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Create a Delta Table and Inspect the Transaction Log

# COMMAND ----------

# Write as Delta
trips_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("delta_trips")

print("Delta table created: delta_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the transaction log: every commit is a JSON file in _delta_log/
# MAGIC DESCRIBE HISTORY delta_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the physical storage structure (Delta-specific metadata)
# MAGIC DESCRIBE DETAIL delta_trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 ACID Transactions: INSERT, UPDATE, DELETE

# COMMAND ----------

# Insert a new batch of trips
new_trips = generate_trips(10000, seed=200)
new_trips.write.format("delta").mode("append").saveAsTable("delta_trips")

print(f"After append, total rows: {spark.table('delta_trips').count():,}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Update: mark all IN_PROGRESS trips older than 2 hours as COMPLETED
# MAGIC -- (In real life: trip reconciliation job)
# MAGIC UPDATE delta_trips
# MAGIC SET status = 'COMPLETED'
# MAGIC WHERE status = 'IN_PROGRESS'
# MAGIC   AND event_ts < '2024-02-01';
# MAGIC
# MAGIC SELECT status, COUNT(*) AS count
# MAGIC FROM delta_trips
# MAGIC GROUP BY status
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GDPR delete: remove all trips for a specific user
# MAGIC -- This is a single SQL statement — no partition rewrite needed!
# MAGIC DELETE FROM delta_trips WHERE user_id = 'USER-001234';
# MAGIC
# MAGIC SELECT COUNT(*) AS remaining_trips,
# MAGIC        SUM(CASE WHEN user_id = 'USER-001234' THEN 1 ELSE 0 END) AS deleted_user_rows
# MAGIC FROM delta_trips;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Time Travel: Query Previous Versions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See the full history of changes
# MAGIC DESCRIBE HISTORY delta_trips

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel: read version 0 (the original 50,000 rows)
# MAGIC SELECT COUNT(*) AS row_count_at_version_0
# MAGIC FROM delta_trips VERSION AS OF 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- The deleted user's data is still accessible via time travel
# MAGIC SELECT COUNT(*) AS deleted_user_rows_in_history
# MAGIC FROM delta_trips VERSION AS OF 0
# MAGIC WHERE user_id = 'USER-001234';
# MAGIC -- Note: VACUUM removes old versions after retention period (default 7 days)
# MAGIC -- Until VACUUM runs, all history is accessible

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.4 MERGE INTO: Upsert Pattern

# COMMAND ----------

# MAGIC %md
# MAGIC MERGE INTO is how you implement CDC (Change Data Capture) in Delta Lake.
# MAGIC A source system sends a changelog: some rows are inserts, some are updates.
# MAGIC MERGE applies both in one atomic operation.

# COMMAND ----------

# Simulate a CDC batch: some new trips, some status updates for existing trips
from pyspark.sql import Row

cdc_updates = spark.createDataFrame([
    # Updates to existing trips
    Row(trip_id="TRIP-00000001", user_id="USER-000042", driver_id="DRV-00123",
        city="New York", status="COMPLETED", fare_usd=45.50, distance_miles=8.2,
        event_ts=datetime(2024, 1, 1, 10, 30)),
    Row(trip_id="TRIP-00000002", user_id="USER-000099", driver_id="DRV-00456",
        city="Chicago", status="CANCELLED", fare_usd=0.0, distance_miles=0.0,
        event_ts=datetime(2024, 1, 1, 11, 0)),
    # New trips not in the table yet
    Row(trip_id="TRIP-99990001", user_id="USER-009999", driver_id="DRV-00999",
        city="Seattle", status="COMPLETED", fare_usd=22.75, distance_miles=4.1,
        event_ts=datetime(2024, 3, 31, 23, 59)),
], schema=trips_df.schema)

# Register as a temp view for the MERGE SQL
cdc_updates.createOrReplaceTempView("cdc_batch")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE: update if trip_id exists, insert if it doesn't
# MAGIC MERGE INTO delta_trips AS target
# MAGIC USING cdc_batch AS source
# MAGIC ON target.trip_id = source.trip_id
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     status       = source.status,
# MAGIC     fare_usd     = source.fare_usd,
# MAGIC     distance_miles = source.distance_miles
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT (trip_id, user_id, driver_id, city, status, fare_usd, distance_miles, event_ts)
# MAGIC   VALUES (source.trip_id, source.user_id, source.driver_id, source.city,
# MAGIC           source.status, source.fare_usd, source.distance_miles, source.event_ts);
# MAGIC
# MAGIC -- Confirm the new trip was inserted
# MAGIC SELECT * FROM delta_trips WHERE trip_id = 'TRIP-99990001';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.5 OPTIMIZE: Compact Small Files + Z-Order

# COMMAND ----------

# MAGIC %sql
# MAGIC -- OPTIMIZE: compact many small files into larger ones (improves read performance)
# MAGIC -- Z-ORDER BY: co-locate records with the same city value in the same files
# MAGIC -- This dramatically improves performance for queries that filter by city
# MAGIC OPTIMIZE delta_trips
# MAGIC ZORDER BY (city, status);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After OPTIMIZE, data skipping uses min/max stats per file
# MAGIC -- Queries like WHERE city = 'New York' skip files that don't contain New York
# MAGIC EXPLAIN EXTENDED
# MAGIC SELECT * FROM delta_trips WHERE city = 'New York' AND status = 'COMPLETED';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.6 Change Data Feed (CDF): Track Row-Level Changes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable Change Data Feed on the table
# MAGIC ALTER TABLE delta_trips
# MAGIC SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Make some changes that CDF will capture
# MAGIC UPDATE delta_trips SET fare_usd = fare_usd * 1.05
# MAGIC WHERE city = 'San Francisco' AND status = 'COMPLETED';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Read the change feed: what changed between version 4 and now?
# MAGIC -- _change_type: insert, update_preimage, update_postimage, delete
# MAGIC SELECT _change_type, trip_id, city, fare_usd, _commit_version, _commit_timestamp
# MAGIC FROM table_changes('delta_trips', 4)
# MAGIC ORDER BY _commit_version, trip_id
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.7 Schema Evolution

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add a new column without rewriting any data
# MAGIC ALTER TABLE delta_trips ADD COLUMN tip_usd DOUBLE;
# MAGIC
# MAGIC -- Existing rows get NULL for the new column
# MAGIC SELECT trip_id, fare_usd, tip_usd FROM delta_trips LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Apache Iceberg Deep Dive
# MAGIC
# MAGIC Iceberg is the multi-engine format — built for teams that need Snowflake,
# MAGIC Trino, Athena, AND Spark to read/write the same table without coordination.
# MAGIC
# MAGIC Key architectural difference: Iceberg uses a METADATA TREE (manifest list →
# MAGIC manifest files → data files) instead of a flat transaction log.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create a Native Iceberg Table on Databricks

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an Iceberg table using Iceberg v2 format (enables row-level deletes)
# MAGIC CREATE TABLE IF NOT EXISTS iceberg_trips (
# MAGIC   trip_id       STRING NOT NULL,
# MAGIC   user_id       STRING NOT NULL,
# MAGIC   driver_id     STRING NOT NULL,
# MAGIC   city          STRING,
# MAGIC   status        STRING,
# MAGIC   fare_usd      DOUBLE,
# MAGIC   distance_miles DOUBLE,
# MAGIC   event_ts      TIMESTAMP
# MAGIC )
# MAGIC USING iceberg
# MAGIC TBLPROPERTIES (
# MAGIC   'format-version'                  = '2',
# MAGIC   'write.delete.mode'               = 'merge-on-read',
# MAGIC   'write.update.mode'               = 'merge-on-read',
# MAGIC   'write.merge.mode'                = 'merge-on-read'
# MAGIC )
# MAGIC COMMENT 'Apache Iceberg v2 table — row-level deletes via delete files';

# COMMAND ----------

# Insert data into Iceberg table
trips_df.write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("iceberg_trips")

print(f"Inserted {spark.table('iceberg_trips').count():,} rows into Iceberg table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Iceberg Metadata Hierarchy Inspection

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect Iceberg snapshots (equivalent to Delta versions)
# MAGIC SELECT snapshot_id, committed_at, operation, summary
# MAGIC FROM iceberg_trips.snapshots;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the manifest files that belong to the current snapshot
# MAGIC SELECT path, length, partition_spec_id, added_files_count, existing_files_count
# MAGIC FROM iceberg_trips.manifests;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Inspect the actual data files tracked by Iceberg
# MAGIC SELECT file_path, file_format, record_count, file_size_in_bytes, partition
# MAGIC FROM iceberg_trips.files
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Iceberg Time Travel via Snapshots

# COMMAND ----------

# Make some changes to create additional snapshots
spark.sql("""
    INSERT INTO iceberg_trips
    SELECT trip_id, user_id, driver_id, city, 'COMPLETED' AS status,
           fare_usd, distance_miles, event_ts
    FROM iceberg_trips
    WHERE status = 'IN_PROGRESS'
    LIMIT 1000
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all snapshots
# MAGIC SELECT snapshot_id, committed_at, operation
# MAGIC FROM iceberg_trips.snapshots
# MAGIC ORDER BY committed_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Time travel by snapshot ID (Iceberg-style)
# MAGIC -- Replace <snapshot_id> with an actual ID from the query above
# MAGIC -- SELECT COUNT(*) FROM iceberg_trips VERSION AS OF <snapshot_id>;
# MAGIC
# MAGIC -- Time travel by timestamp
# MAGIC SELECT COUNT(*) AS count_yesterday
# MAGIC FROM iceberg_trips TIMESTAMP AS OF '2024-01-01 00:00:00';

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.4 Iceberg Hidden Partitioning
# MAGIC
# MAGIC This is one of Iceberg's most powerful features.
# MAGIC Users never need to specify partition columns in WHERE clauses —
# MAGIC Iceberg derives the partition from the data column automatically.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create an Iceberg table with HIDDEN partitioning
# MAGIC -- Users query by event_ts (the raw timestamp)
# MAGIC -- Iceberg auto-derives the day partition — users never see it
# MAGIC CREATE TABLE IF NOT EXISTS iceberg_trips_partitioned (
# MAGIC   trip_id       STRING,
# MAGIC   user_id       STRING,
# MAGIC   city          STRING,
# MAGIC   status        STRING,
# MAGIC   fare_usd      DOUBLE,
# MAGIC   event_ts      TIMESTAMP
# MAGIC )
# MAGIC USING iceberg
# MAGIC PARTITIONED BY (days(event_ts))   -- hidden partition: users query by event_ts
# MAGIC TBLPROPERTIES ('format-version' = '2');

# COMMAND ----------

trips_df.select("trip_id","user_id","city","status","fare_usd","event_ts") \
    .write \
    .format("iceberg") \
    .mode("append") \
    .saveAsTable("iceberg_trips_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query by event_ts — Iceberg automatically applies partition pruning
# MAGIC -- No need to write WHERE date_part = '2024-01-15' explicitly
# MAGIC -- No accidental full table scans possible
# MAGIC SELECT city, COUNT(*) AS trips, SUM(fare_usd) AS revenue
# MAGIC FROM iceberg_trips_partitioned
# MAGIC WHERE event_ts BETWEEN '2024-01-01' AND '2024-01-31'
# MAGIC GROUP BY city
# MAGIC ORDER BY revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.5 Iceberg Row-Level Deletes (v2 Feature)
# MAGIC
# MAGIC Iceberg v2 introduced DELETE FILES — instead of rewriting the data file,
# MAGIC it writes a small "delete file" that says which rows to exclude at read time.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- GDPR delete: remove a user's data from Iceberg
# MAGIC -- With format-version=2 and merge-on-read mode:
# MAGIC --   → Writes a small equality delete file (not a full file rewrite)
# MAGIC DELETE FROM iceberg_trips WHERE user_id = 'USER-001234';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After the delete, inspect the files — you'll see a delete file was added
# MAGIC SELECT file_path, content, record_count, file_size_in_bytes
# MAGIC FROM iceberg_trips.files
# MAGIC ORDER BY content;  -- content=0: data file, content=1: position delete, content=2: equality delete

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.6 Iceberg Table Maintenance

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Expire old snapshots (equivalent to Delta VACUUM)
# MAGIC -- Removes snapshots older than the specified timestamp
# MAGIC CALL system.expire_snapshots('iceberg_trips',
# MAGIC   TIMESTAMP '2024-06-01 00:00:00');

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rewrite data files: compact small files + merge delete files into base files
# MAGIC -- (physically removes deleted rows)
# MAGIC CALL system.rewrite_data_files('iceberg_trips');

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Apache Hudi Deep Dive
# MAGIC
# MAGIC Hudi was built for the CDC use case: high-frequency upserts from operational
# MAGIC databases. Its key innovation is the INDEXED UPSERT — it knows exactly which
# MAGIC file contains each record key, so it can update one record without scanning
# MAGIC all files.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Copy-on-Write (CoW) Table
# MAGIC
# MAGIC CoW: every write rewrites the entire affected Parquet file.
# MAGIC Best for tables that are mostly read, rarely written.

# COMMAND ----------

# Hudi requires specifying the record key and precombine field
hudi_options_cow = {
    "hoodie.table.name":                  "hudi_trips_cow",
    "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
    "hoodie.datasource.write.recordkey.field":    "trip_id",
    "hoodie.datasource.write.precombine.field":   "event_ts",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.upsert.shuffle.parallelism":  2,
    "hoodie.insert.shuffle.parallelism":  2,
}

trips_df.write \
    .format("hudi") \
    .options(**hudi_options_cow) \
    .mode("overwrite") \
    .save(f"/tmp/day28_hudi/trips_cow/")

print("Hudi CoW table written to /tmp/day28_hudi/trips_cow/")

# COMMAND ----------

# Read back the Hudi CoW table
hudi_cow_df = spark.read \
    .format("hudi") \
    .load("/tmp/day28_hudi/trips_cow/")

print(f"Rows in Hudi CoW table: {hudi_cow_df.count():,}")
# Notice Hudi adds metadata columns: _hoodie_commit_time, _hoodie_record_key, etc.
hudi_cow_df.select(
    "_hoodie_commit_time",
    "_hoodie_record_key",
    "trip_id", "city", "status", "fare_usd"
).show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Upsert into CoW Table
# MAGIC
# MAGIC Hudi's upsert uses the index to find which file contains each record key,
# MAGIC then rewrites only the affected files (CoW) — not the entire table.

# COMMAND ----------

# Simulate a CDC batch: updates to existing trips + new trips
cdc_batch = spark.createDataFrame([
    # Updates: change status of existing trips
    ("TRIP-00000001", "USER-000042", "DRV-00123", "New York",
     "COMPLETED", 45.50, 8.2, datetime(2024, 1, 1, 10, 30)),
    ("TRIP-00000005", "USER-000099", "DRV-00456", "Chicago",
     "CANCELLED", 0.0, 0.0, datetime(2024, 1, 1, 11, 0)),
    # New record
    ("TRIP-99990001", "USER-009999", "DRV-00999", "Seattle",
     "COMPLETED", 22.75, 4.1, datetime(2024, 3, 31, 23, 59)),
], schema=trips_df.schema)

# Upsert: Hudi matches on trip_id (recordkey), uses event_ts (precombine) to resolve ties
cdc_batch.write \
    .format("hudi") \
    .options(**hudi_options_cow) \
    .mode("append") \
    .save("/tmp/day28_hudi/trips_cow/")

print("Upsert complete.")
print(f"Total rows after upsert: {spark.read.format('hudi').load('/tmp/day28_hudi/trips_cow/').count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Merge-on-Read (MoR) Table
# MAGIC
# MAGIC MoR: new/updated records go to a small Avro LOG file.
# MAGIC The base Parquet file is NOT rewritten.
# MAGIC Best for high-frequency writes where low write latency matters.

# COMMAND ----------

hudi_options_mor = {
    "hoodie.table.name":                  "hudi_trips_mor",
    "hoodie.datasource.write.table.type": "MERGE_ON_READ",   # ← key change
    "hoodie.datasource.write.recordkey.field":    "trip_id",
    "hoodie.datasource.write.precombine.field":   "event_ts",
    "hoodie.datasource.write.partitionpath.field": "city",
    "hoodie.datasource.write.hive_style_partitioning": "true",
    "hoodie.compact.inline":              "false",  # async compaction
    "hoodie.compact.inline.max.delta.commits": "5",  # compact after 5 delta commits
    "hoodie.upsert.shuffle.parallelism":  2,
    "hoodie.insert.shuffle.parallelism":  2,
}

trips_df.write \
    .format("hudi") \
    .options(**hudi_options_mor) \
    .mode("overwrite") \
    .save("/tmp/day28_hudi/trips_mor/")

print("Hudi MoR table written to /tmp/day28_hudi/trips_mor/")

# COMMAND ----------

# MoR: apply several upserts quickly (in a real scenario, these come from Kafka/CDC)
for batch_num in range(3):
    seed = 300 + batch_num
    updates = generate_trips(1000, seed=seed)
    updates.write \
        .format("hudi") \
        .options(**hudi_options_mor) \
        .mode("append") \
        .save("/tmp/day28_hudi/trips_mor/")
    print(f"Upsert batch {batch_num + 1} written (MoR: only log files updated, base files untouched)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 MoR Read Modes: Snapshot vs Read-Optimized

# COMMAND ----------

# Snapshot query: merges base files + log files — fully current
snapshot_df = spark.read \
    .format("hudi") \
    .option("hoodie.datasource.query.type", "snapshot") \
    .load("/tmp/day28_hudi/trips_mor/")

# Read-optimized query: reads only base Parquet files — fast but slightly stale
read_opt_df = spark.read \
    .format("hudi") \
    .option("hoodie.datasource.query.type", "read_optimized") \
    .load("/tmp/day28_hudi/trips_mor/")

print(f"Snapshot read (fully current):    {snapshot_df.count():,} rows")
print(f"Read-optimized (base files only): {read_opt_df.count():,} rows")
print()
print("Difference = rows in log files not yet compacted into base files")
print("After compaction, both counts will be equal")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.5 Incremental Queries: Hudi's Kafka-Style Pull
# MAGIC
# MAGIC Incremental reads are Hudi's superpower for downstream consumers.
# MAGIC Instead of "give me the whole table", consumers say "give me only what
# MAGIC changed since commit time X". This is like a Kafka consumer group offset.

# COMMAND ----------

# Find the first and last commit times on the MoR table
commits = spark.read \
    .format("hudi") \
    .load("/tmp/day28_hudi/trips_mor/") \
    .select("_hoodie_commit_time") \
    .distinct() \
    .orderBy("_hoodie_commit_time")

commit_times = [row["_hoodie_commit_time"] for row in commits.collect()]
if len(commit_times) >= 2:
    begin_time = commit_times[0]
    end_time   = commit_times[-1]
    print(f"Reading incremental changes from {begin_time} to {end_time}")

    incremental_df = spark.read \
        .format("hudi") \
        .option("hoodie.datasource.query.type",           "incremental") \
        .option("hoodie.datasource.read.begin.instanttime", begin_time) \
        .option("hoodie.datasource.read.end.instanttime",   end_time) \
        .load("/tmp/day28_hudi/trips_mor/")

    print(f"Rows that changed in this range: {incremental_df.count():,}")
    incremental_df.select(
        "_hoodie_commit_time", "trip_id", "city", "status"
    ).orderBy("_hoodie_commit_time").show(10, truncate=False)
else:
    print("Need at least 2 commits for incremental read demo")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Side-by-Side Comparison — Same Operation, Three Formats

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 GDPR Delete — Compare all three approaches

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC GDPR Delete: Remove all data for USER-000042
# MAGIC
# MAGIC FORMAT        | MECHANISM                          | RESULT
# MAGIC ─────────────────────────────────────────────────────────────────────
# MAGIC Delta Lake    | DELETE FROM delta_trips            | Adds a "remove" entry to
# MAGIC               | WHERE user_id = 'USER-000042'      | _delta_log/; no file rewrite
# MAGIC               |                                    | until OPTIMIZE + VACUUM
# MAGIC               |                                    |
# MAGIC Apache Iceberg | DELETE FROM iceberg_trips         | v1: rewrites file
# MAGIC               | WHERE user_id = 'USER-000042'      | v2 MoR: writes equality
# MAGIC               |                                    | delete file (tiny)
# MAGIC               |                                    | Physical removal after
# MAGIC               |                                    | rewrite_data_files()
# MAGIC               |                                    |
# MAGIC Apache Hudi   | Upsert with delete marker          | MoR: writes tombstone record
# MAGIC               | or HoodieWriteClient.delete()      | to log file (instant)
# MAGIC               |                                    | CoW: rewrites affected file
# MAGIC ```

# COMMAND ----------

# Delta: execute the delete
spark.sql("DELETE FROM delta_trips WHERE user_id = 'USER-000042'")
delta_count = spark.sql(
    "SELECT COUNT(*) AS c FROM delta_trips WHERE user_id = 'USER-000042'"
).collect()[0]["c"]

# Iceberg: execute the delete
spark.sql("DELETE FROM iceberg_trips WHERE user_id = 'USER-000042'")
iceberg_count = spark.sql(
    "SELECT COUNT(*) AS c FROM iceberg_trips WHERE user_id = 'USER-000042'"
).collect()[0]["c"]

print(f"After DELETE — Delta  : {delta_count} rows remaining for USER-000042")
print(f"After DELETE — Iceberg: {iceberg_count} rows remaining for USER-000042")
print()
print("Both are 0 — but the mechanisms differ:")
print("  Delta: soft-remove in transaction log, physical removal deferred to VACUUM")
print("  Iceberg v2: writes equality delete file, physical removal deferred to compaction")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Write Performance Comparison
# MAGIC
# MAGIC This demo illustrates write amplification difference between CoW and MoR.

# COMMAND ----------

import time

# Generate a small targeted update batch (updates 500 specific trips)
# In real CDC: this simulates a database binlog batch
target_trips = trips_df.limit(500)
updates_only = target_trips.withColumn("status", F.lit("COMPLETED")) \
                            .withColumn("fare_usd", F.col("fare_usd") * 1.1)

# --- Delta (always CoW, optimized by Z-ORDER file layout) ---
t0 = time.time()
updates_only.write.format("delta").mode("append").saveAsTable("delta_trips")
delta_write_ms = int((time.time() - t0) * 1000)

# --- Hudi CoW ---
t0 = time.time()
updates_only.write \
    .format("hudi") \
    .options(**hudi_options_cow) \
    .mode("append") \
    .save("/tmp/day28_hudi/trips_cow/")
hudi_cow_ms = int((time.time() - t0) * 1000)

# --- Hudi MoR ---
t0 = time.time()
updates_only.write \
    .format("hudi") \
    .options(**hudi_options_mor) \
    .mode("append") \
    .save("/tmp/day28_hudi/trips_mor/")
hudi_mor_ms = int((time.time() - t0) * 1000)

print("Write time for 500-record update batch:")
print(f"  Delta Lake (CoW, MERGE-based): {delta_write_ms:,} ms")
print(f"  Hudi CoW:                      {hudi_cow_ms:,} ms")
print(f"  Hudi MoR:                      {hudi_mor_ms:,} ms")
print()
print("At scale (millions of records), MoR write time stays near-constant")
print("because it only writes the delta records, not the entire affected file.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6: Delta UniForm — One Table, Multiple Metadata Views

# COMMAND ----------

# MAGIC %md
# MAGIC UniForm (Universal Format) is Delta Lake 3.0+'s answer to multi-engine access.
# MAGIC Enable it on any Delta table and Snowflake / Trino / Athena can read it as Iceberg —
# MAGIC using the same physical Parquet files, no data copy.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Enable UniForm on the Delta trips table
# MAGIC -- This makes Databricks auto-generate Iceberg metadata alongside Delta log
# MAGIC ALTER TABLE delta_trips
# MAGIC SET TBLPROPERTIES (
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg',
# MAGIC   'delta.enableIcebergCompatV2'          = 'true'
# MAGIC );
# MAGIC
# MAGIC DESCRIBE EXTENDED delta_trips;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- After enabling UniForm, check that Iceberg metadata is being generated
# MAGIC SELECT key, value
# MAGIC FROM (DESCRIBE EXTENDED delta_trips)
# MAGIC WHERE key IN (
# MAGIC   'delta.universalFormat.enabledFormats',
# MAGIC   'delta.enableIcebergCompatV2',
# MAGIC   'delta.minReaderVersion',
# MAGIC   'delta.minWriterVersion'
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ### How UniForm Works Internally
# MAGIC
# MAGIC ```
# MAGIC DELTA UNIFORM ARCHITECTURE
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                                                                      │
# MAGIC │  Same physical Parquet files on cloud storage                       │
# MAGIC │                                                                      │
# MAGIC │  ┌──────────────────────────┐   ┌──────────────────────────────┐   │
# MAGIC │  │  _delta_log/             │   │  metadata/                   │   │
# MAGIC │  │  00000001.json           │   │  v1.metadata.json            │   │
# MAGIC │  │  00000002.json  ─────────┼──▶│  snap-001-manifest-list.avro │   │
# MAGIC │  │  (Delta commits)        │   │  a1b2-manifest.avro          │   │
# MAGIC │  │                          │   │  (Iceberg metadata —         │   │
# MAGIC │  │                          │   │   auto-generated by UniForm) │   │
# MAGIC │  └──────────────────────────┘   └──────────────────────────────┘   │
# MAGIC │           │                                    │                    │
# MAGIC │           ▼                                    ▼                    │
# MAGIC │   Databricks / Spark              Snowflake (reads Iceberg natively)│
# MAGIC │   reads Delta natively            Trino / Athena / Starburst        │
# MAGIC │   (full Delta feature set)        (reads Iceberg natively)          │
# MAGIC │                                                                      │
# MAGIC │  Result: Write once (Delta), read from any engine (Iceberg view)    │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrating UniForm: Iceberg APIs on a Delta Table

# COMMAND ----------

# After enabling UniForm, you can use Iceberg APIs to read the Delta table
# This is what Snowflake or Trino would do when pointed at the Iceberg metadata

# Read the Delta table using Iceberg format (as an external engine would)
uniform_as_iceberg = spark.read \
    .format("iceberg") \
    .load(f"{CATALOG}.{SCHEMA}.delta_trips")

print(f"Reading Delta table via Iceberg API: {uniform_as_iceberg.count():,} rows")
print()
print("This is exactly how Snowflake reads a UniForm-enabled Delta table:")
print("  1. Snowflake catalog integration points to Unity Catalog REST API")
print("  2. UC returns the Iceberg metadata path")
print("  3. Snowflake reads the Parquet files directly from S3/ADLS")
print("  4. No Databricks cluster involved — no data copy")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Format Decision Guide — Code Version

# COMMAND ----------

def recommend_format(
    primary_engine: str,
    multi_engine_reads: bool,
    cdc_upserts_per_hour: int,
    table_size_gb: float,
    needs_partition_evolution: bool,
) -> str:
    """
    Returns a format recommendation with reasoning.
    This mirrors the decision tree in the guide.
    """
    reasons = []

    if primary_engine == "databricks":
        score_delta   = 100
        score_iceberg = 60
        score_hudi    = 40
        reasons.append("Primary engine is Databricks → Delta Lake has best native support")
    else:
        score_delta   = 40
        score_iceberg = 80
        score_hudi    = 60
        reasons.append("Non-Databricks primary engine → Delta loses native advantage")

    if multi_engine_reads:
        score_iceberg += 40
        score_delta   -= 20
        reasons.append("Multi-engine reads needed → Iceberg is engine-agnostic by design")

    if cdc_upserts_per_hour > 500_000:
        score_hudi    += 50
        score_delta   -= 10
        score_iceberg -= 10
        reasons.append(f"High CDC frequency ({cdc_upserts_per_hour:,}/hr) → Hudi MoR minimizes write amplification")
    elif cdc_upserts_per_hour > 100_000:
        score_hudi    += 20
        reasons.append(f"Moderate CDC frequency ({cdc_upserts_per_hour:,}/hr) → Hudi is competitive")

    if needs_partition_evolution:
        score_iceberg += 30
        score_delta   -= 10
        score_hudi    -= 5
        reasons.append("Partition evolution needed → Iceberg handles this natively without data rewrites")

    if table_size_gb > 10_000:
        score_iceberg += 20
        reasons.append(f"Very large table ({table_size_gb:,.0f} GB) → Iceberg manifest tree scales better than flat log")

    scores = {"Delta Lake": score_delta, "Apache Iceberg": score_iceberg, "Apache Hudi": score_hudi}
    winner = max(scores, key=scores.get)

    print(f"Recommendation: {winner}")
    print(f"Scores: {scores}")
    print("Reasoning:")
    for r in reasons:
        print(f"  • {r}")
    return winner

# COMMAND ----------

print("=== Scenario 1: Databricks-first analytics team ===")
recommend_format(
    primary_engine="databricks",
    multi_engine_reads=False,
    cdc_upserts_per_hour=10_000,
    table_size_gb=500,
    needs_partition_evolution=False
)

# COMMAND ----------

print("=== Scenario 2: Multi-cloud team (Spark + Snowflake + Athena) ===")
recommend_format(
    primary_engine="spark",
    multi_engine_reads=True,
    cdc_upserts_per_hour=50_000,
    table_size_gb=2_000,
    needs_partition_evolution=True
)

# COMMAND ----------

print("=== Scenario 3: High-frequency CDC from MySQL (Uber-style) ===")
recommend_format(
    primary_engine="spark",
    multi_engine_reads=False,
    cdc_upserts_per_hour=2_000_000,
    table_size_gb=5_000,
    needs_partition_evolution=False
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 8: Cleanup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS delta_trips;
# MAGIC DROP TABLE IF EXISTS iceberg_trips;
# MAGIC DROP TABLE IF EXISTS iceberg_trips_partitioned;
# MAGIC DROP SCHEMA IF EXISTS day28_open_table_formats CASCADE;

# COMMAND ----------

import shutil
for path in ["/tmp/day28_raw_parquet", "/tmp/day28_hudi"]:
    try:
        dbutils.fs.rm(path, recurse=True)
        print(f"Removed {path}")
    except Exception:
        try:
            shutil.rmtree(path)
            print(f"Removed {path} (local)")
        except Exception as e:
            print(f"Could not remove {path}: {e}")

print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: What You Learned
# MAGIC
# MAGIC ```
# MAGIC OPEN TABLE FORMAT RECAP
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                                                                      │
# MAGIC │  Problem solved: Raw Parquet has no ACID, no upserts, no deletes,  │
# MAGIC │  no time travel, no schema evolution — open table formats add all   │
# MAGIC │  of these on top of cloud object storage.                           │
# MAGIC │                                                                      │
# MAGIC │  DELTA LAKE                                                         │
# MAGIC │    • Transaction log (_delta_log/) tracks every commit as JSON     │
# MAGIC │    • ACID via optimistic concurrency control                        │
# MAGIC │    • Best Databricks / Spark integration                           │
# MAGIC │    • UniForm: expose Delta table as Iceberg to external engines     │
# MAGIC │    • Choose when: Databricks-first, Unity Catalog, Spark pipelines  │
# MAGIC │                                                                      │
# MAGIC │  APACHE ICEBERG                                                     │
# MAGIC │    • Metadata tree: snapshot → manifest list → manifests → files    │
# MAGIC │    • Engine-agnostic: Spark, Flink, Trino, Snowflake, Athena       │
# MAGIC │    • Hidden partitioning + partition evolution (no data rewrites)   │
# MAGIC │    • v2 delete files: efficient row-level deletes                   │
# MAGIC │    • Choose when: multi-engine reads/writes, petabyte scale        │
# MAGIC │                                                                      │
# MAGIC │  APACHE HUDI                                                        │
# MAGIC │    • CoW: rewrite files on every update (read-optimized)           │
# MAGIC │    • MoR: append to log files, compact periodically (write-fast)   │
# MAGIC │    • Indexed upserts: finds record location without full scan       │
# MAGIC │    • Incremental queries: "give me only changes since commit X"     │
# MAGIC │    • Choose when: high-frequency CDC, low write latency, Amazon EMR │
# MAGIC │                                                                      │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
