# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Project 03: CDC Pipeline (Change Data Capture)
# MAGIC > Module 10 -- Capstone Project | Level: Advanced | Time: 3-4 hours
# MAGIC
# MAGIC ## What You Will Build
# MAGIC
# MAGIC A complete CDC pipeline that processes database change events:
# MAGIC - **Bronze**: Raw CDC events as an immutable audit trail
# MAGIC - **Silver**: Current-state table (SCD Type 1) + full history table (SCD Type 2)
# MAGIC - **Gold**: Change frequency analytics, churn detection, data freshness monitoring
# MAGIC
# MAGIC The pipeline handles inserts, updates, and deletes -- including out-of-order
# MAGIC events -- using Delta Lake MERGE and Change Data Feed.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Setup and Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, BooleanType
)
from pyspark.sql.window import Window
import random
from datetime import datetime, timedelta

# Configuration
DATABASE = "module10_cdc"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
spark.sql(f"USE {DATABASE}")

# Clean up prior runs
existing_tables = [row.tableName for row in spark.sql("SHOW TABLES").collect()]
for t in existing_tables:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")

print(f"Database '{DATABASE}' ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Data Generation
# MAGIC
# MAGIC We generate:
# MAGIC 1. Initial customer load (1000 records)
# MAGIC 2. CDC events: 500 inserts, 300 updates, 100 deletes
# MAGIC 3. ~5% of events are intentionally out-of-order

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 Generate Initial Customer Records

# COMMAND ----------

random.seed(42)

first_names = [
    "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Hank",
    "Ivy", "Jack", "Karen", "Leo", "Mia", "Noah", "Olivia", "Peter",
    "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor", "Wendy", "Xander",
    "Yara", "Zach", "Bella", "Carlos", "Dara", "Ethan"
]
last_names = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Rodriguez", "Martinez", "Anderson", "Taylor", "Thomas",
    "Jackson", "White", "Harris", "Martin", "Thompson", "Moore", "Allen"
]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix",
          "Seattle", "Denver", "Boston", "Atlanta", "Portland",
          "Austin", "Miami", "Dallas", "San Francisco", "Nashville"]
states = ["NY", "CA", "IL", "TX", "AZ", "WA", "CO", "MA", "GA", "OR",
          "TX", "FL", "TX", "CA", "TN"]
tiers = ["Bronze", "Silver", "Gold", "Platinum"]
tier_weights = [0.4, 0.3, 0.2, 0.1]

# Generate initial customer load
initial_customers = []
base_time = datetime(2024, 1, 1, 0, 0, 0)

for i in range(1, 1001):
    first = random.choice(first_names)
    last = random.choice(last_names)
    city_idx = random.randint(0, len(cities) - 1)
    signup = base_time - timedelta(days=random.randint(30, 730))
    created = base_time - timedelta(days=random.randint(1, 30))

    initial_customers.append({
        "customer_id": f"C-{i:05d}",
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "city": cities[city_idx],
        "state": states[city_idx],
        "tier": random.choices(tiers, weights=tier_weights, k=1)[0],
        "signup_date": signup.strftime("%Y-%m-%d"),
        "created_at": created.strftime("%Y-%m-%d %H:%M:%S"),
        "last_updated": created.strftime("%Y-%m-%d %H:%M:%S"),
    })

initial_df = spark.createDataFrame(initial_customers)
print(f"Initial customers: {initial_df.count()}")
initial_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 Generate CDC Events

# COMMAND ----------

random.seed(42)

cdc_events = []
event_counter = 0
cdc_start = datetime(2024, 1, 1, 0, 0, 0)

# Lookup for current customer state (for building before_images)
customer_state = {c["customer_id"]: dict(c) for c in initial_customers}
existing_ids = list(customer_state.keys())

# --- Generate 500 INSERT events (new customers) ---
for i in range(1001, 1501):
    event_counter += 1
    ts = cdc_start + timedelta(hours=random.randint(1, 720))
    first = random.choice(first_names)
    last = random.choice(last_names)
    city_idx = random.randint(0, len(cities) - 1)

    new_customer = {
        "customer_id": f"C-{i:05d}",
        "name": f"{first} {last}",
        "email": f"{first.lower()}.{last.lower()}{i}@example.com",
        "phone": f"+1-{random.randint(200,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}",
        "city": cities[city_idx],
        "state": states[city_idx],
        "tier": random.choices(tiers, weights=tier_weights, k=1)[0],
        "signup_date": ts.strftime("%Y-%m-%d"),
    }

    cdc_events.append({
        "event_id": f"EVT-{event_counter:06d}",
        "operation": "I",
        "customer_id": new_customer["customer_id"],
        "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "after_name": new_customer["name"],
        "after_email": new_customer["email"],
        "after_phone": new_customer["phone"],
        "after_city": new_customer["city"],
        "after_state": new_customer["state"],
        "after_tier": new_customer["tier"],
        "after_signup_date": new_customer["signup_date"],
        "before_name": None, "before_email": None, "before_phone": None,
        "before_city": None, "before_state": None, "before_tier": None,
        "source_table": "customers",
        "transaction_id": f"TXN-{random.randint(100000, 999999)}",
    })
    customer_state[new_customer["customer_id"]] = new_customer
    existing_ids.append(new_customer["customer_id"])

# --- Generate 300 UPDATE events ---
for _ in range(300):
    event_counter += 1
    cust_id = random.choice(existing_ids[:1000])  # Update existing customers
    ts = cdc_start + timedelta(hours=random.randint(1, 720))
    current = customer_state.get(cust_id, {})

    # Decide what to update
    update_field = random.choice(["name", "email", "city", "tier"])
    before_values = {
        "name": current.get("name", ""),
        "email": current.get("email", ""),
        "city": current.get("city", ""),
        "state": current.get("state", ""),
        "tier": current.get("tier", ""),
    }
    after_values = dict(before_values)

    if update_field == "name":
        after_values["name"] = f"{random.choice(first_names)} {random.choice(last_names)}"
    elif update_field == "email":
        after_values["email"] = f"updated{event_counter}@example.com"
    elif update_field == "city":
        new_city_idx = random.randint(0, len(cities) - 1)
        after_values["city"] = cities[new_city_idx]
        after_values["state"] = states[new_city_idx]
    elif update_field == "tier":
        after_values["tier"] = random.choice(tiers)

    cdc_events.append({
        "event_id": f"EVT-{event_counter:06d}",
        "operation": "U",
        "customer_id": cust_id,
        "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "after_name": after_values["name"],
        "after_email": after_values["email"],
        "after_phone": current.get("phone"),
        "after_city": after_values["city"],
        "after_state": after_values["state"],
        "after_tier": after_values["tier"],
        "after_signup_date": current.get("signup_date"),
        "before_name": before_values["name"],
        "before_email": before_values["email"],
        "before_phone": current.get("phone"),
        "before_city": before_values["city"],
        "before_state": before_values["state"],
        "before_tier": before_values["tier"],
        "source_table": "customers",
        "transaction_id": f"TXN-{random.randint(100000, 999999)}",
    })

# --- Generate 100 DELETE events ---
delete_candidates = random.sample(existing_ids[:1000], 100)
for cust_id in delete_candidates:
    event_counter += 1
    ts = cdc_start + timedelta(hours=random.randint(360, 720))  # Deletes happen later
    current = customer_state.get(cust_id, {})

    cdc_events.append({
        "event_id": f"EVT-{event_counter:06d}",
        "operation": "D",
        "customer_id": cust_id,
        "event_timestamp": ts.strftime("%Y-%m-%d %H:%M:%S"),
        "after_name": None, "after_email": None, "after_phone": None,
        "after_city": None, "after_state": None, "after_tier": None,
        "after_signup_date": None,
        "before_name": current.get("name"),
        "before_email": current.get("email"),
        "before_phone": current.get("phone"),
        "before_city": current.get("city"),
        "before_state": current.get("state"),
        "before_tier": current.get("tier"),
        "source_table": "customers",
        "transaction_id": f"TXN-{random.randint(100000, 999999)}",
    })

# --- Inject out-of-order events (~5%) ---
# Move 5% of event timestamps backward to simulate late arrival
for i in range(len(cdc_events)):
    if random.random() < 0.05:
        original_ts = datetime.strptime(cdc_events[i]["event_timestamp"], "%Y-%m-%d %H:%M:%S")
        delayed_ts = original_ts - timedelta(hours=random.randint(1, 48))
        cdc_events[i]["event_timestamp"] = delayed_ts.strftime("%Y-%m-%d %H:%M:%S")

cdc_df = spark.createDataFrame(cdc_events)
cdc_df = cdc_df.withColumn("event_timestamp", F.to_timestamp("event_timestamp", "yyyy-MM-dd HH:mm:ss"))

print(f"Total CDC events: {cdc_df.count()}")
print("\nEvent distribution:")
cdc_df.groupBy("operation").count().show()
cdc_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: BRONZE LAYER -- Immutable CDC Event Store
# MAGIC
# MAGIC The Bronze layer stores every CDC event exactly as received. This is the
# MAGIC **immutable audit trail** -- it is never modified after ingestion.

# COMMAND ----------

# Write initial customer load to a reference table
initial_df.write.format("delta").mode("overwrite").saveAsTable("bronze_initial_load")
print(f"Bronze initial load: {spark.table('bronze_initial_load').count()} customers")

# Write CDC events with metadata
bronze_cdc = (cdc_df
    .withColumn("_ingest_timestamp", F.current_timestamp())
    .withColumn("_source", F.lit("source_db_binlog"))
    .withColumn("_batch_id", F.lit("cdc_batch_001"))
)

bronze_cdc.write.format("delta").mode("overwrite").saveAsTable("bronze_cdc_events")
bronze_count = spark.table("bronze_cdc_events").count()
print(f"Bronze CDC events: {bronze_count} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Bronze: CDC events with all operation types
# MAGIC SELECT event_id, operation, customer_id, event_timestamp,
# MAGIC        after_name, before_name, _ingest_timestamp
# MAGIC FROM bronze_cdc_events
# MAGIC ORDER BY event_timestamp
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: SILVER LAYER -- SCD Type 1 (Current State)
# MAGIC
# MAGIC Apply CDC events using MERGE to maintain a current-state-only table.
# MAGIC The most recent state of each customer is preserved; historical changes
# MAGIC are overwritten.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Seed the Current-State Table from Initial Load

# COMMAND ----------

# Create current-state table with Change Data Feed enabled
spark.sql("""
    CREATE OR REPLACE TABLE silver_customers_current (
        customer_id STRING,
        name STRING,
        email STRING,
        phone STRING,
        city STRING,
        state STRING,
        tier STRING,
        signup_date STRING,
        last_updated TIMESTAMP,
        cdc_operation STRING
    )
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
""")

# Seed with initial load
initial_with_ts = (spark.table("bronze_initial_load")
    .withColumn("last_updated",
        F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("cdc_operation", F.lit("INITIAL_LOAD"))
    .select("customer_id", "name", "email", "phone", "city", "state",
            "tier", "signup_date", "last_updated", "cdc_operation")
)

initial_with_ts.write.format("delta").mode("append").saveAsTable("silver_customers_current")
initial_count = spark.table("silver_customers_current").count()
print(f"Silver current-state seeded: {initial_count} customers")

# Record the initial version for time travel comparison later
initial_version = spark.sql("DESCRIBE HISTORY silver_customers_current").select(
    F.max("version")).collect()[0][0]
print(f"Initial version: {initial_version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Apply CDC Events with MERGE (SCD Type 1)

# COMMAND ----------

# Prepare CDC source: deduplicate to keep latest event per customer
# This handles cases where multiple events exist for the same customer
cdc_source = spark.table("bronze_cdc_events")

window_latest = Window.partitionBy("customer_id").orderBy(F.desc("event_timestamp"))

cdc_deduped = (cdc_source
    .withColumn("rn", F.row_number().over(window_latest))
    .filter("rn = 1")
    .drop("rn")
)

# Register as temp view for SQL MERGE
cdc_deduped.createOrReplaceTempView("cdc_latest_events")

print(f"CDC events (deduplicated to latest per customer): {cdc_deduped.count()}")
print("\nOperation distribution after dedup:")
cdc_deduped.groupBy("operation").count().show()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MERGE: Apply CDC events to current-state table
# MAGIC -- This is the core SCD Type 1 pattern
# MAGIC MERGE INTO silver_customers_current AS target
# MAGIC USING cdc_latest_events AS source
# MAGIC ON target.customer_id = source.customer_id
# MAGIC
# MAGIC -- DELETE: Remove customers marked for deletion
# MAGIC WHEN MATCHED AND source.operation = 'D' THEN DELETE
# MAGIC
# MAGIC -- UPDATE: Apply changes only if the event is newer than current state
# MAGIC WHEN MATCHED AND source.operation = 'U'
# MAGIC   AND source.event_timestamp > target.last_updated
# MAGIC THEN UPDATE SET
# MAGIC   target.name = source.after_name,
# MAGIC   target.email = source.after_email,
# MAGIC   target.phone = source.after_phone,
# MAGIC   target.city = source.after_city,
# MAGIC   target.state = source.after_state,
# MAGIC   target.tier = source.after_tier,
# MAGIC   target.last_updated = source.event_timestamp,
# MAGIC   target.cdc_operation = 'UPDATE'
# MAGIC
# MAGIC -- INSERT: Add new customers
# MAGIC WHEN NOT MATCHED AND source.operation = 'I' THEN INSERT (
# MAGIC   customer_id, name, email, phone, city, state, tier,
# MAGIC   signup_date, last_updated, cdc_operation
# MAGIC ) VALUES (
# MAGIC   source.customer_id, source.after_name, source.after_email,
# MAGIC   source.after_phone, source.after_city, source.after_state,
# MAGIC   source.after_tier, source.after_signup_date,
# MAGIC   source.event_timestamp, 'INSERT'
# MAGIC )

# COMMAND ----------

post_merge_count = spark.table("silver_customers_current").count()
print(f"Silver current-state after MERGE: {post_merge_count} customers")
print(f"  Initial load:  {initial_count}")
print(f"  After MERGE:   {post_merge_count}")
print(f"  Net change:    {post_merge_count - initial_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Query Change Data Feed
# MAGIC
# MAGIC Delta Lake Change Data Feed captures exactly what MERGE changed.

# COMMAND ----------

# Query the Change Data Feed to see what the MERGE operation did
cdf_df = (spark.read
    .format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", initial_version + 1)
    .table("silver_customers_current")
)

print("=== CHANGE DATA FEED -- What MERGE Changed ===")
cdf_df.groupBy("_change_type").count().show()
print("\nSample CDF records:")
cdf_df.select(
    "customer_id", "name", "city", "tier",
    "_change_type", "_commit_version", "_commit_timestamp"
).show(15, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.4 Time Travel -- Compare Before and After

# COMMAND ----------

# Version before MERGE (initial load)
before_merge = (spark.read.format("delta")
    .option("versionAsOf", initial_version)
    .table("silver_customers_current")
)

# Current version (after MERGE)
after_merge = spark.table("silver_customers_current")

print(f"Before MERGE (version {initial_version}): {before_merge.count()} rows")
print(f"After MERGE (current):                    {after_merge.count()} rows")

# Find customers that were updated (existed before and after but changed)
updated_customers = (before_merge.alias("before")
    .join(after_merge.alias("after"), on="customer_id", how="inner")
    .filter(
        (F.col("before.name") != F.col("after.name")) |
        (F.col("before.city") != F.col("after.city")) |
        (F.col("before.tier") != F.col("after.tier"))
    )
    .select(
        F.col("customer_id"),
        F.col("before.name").alias("old_name"),
        F.col("after.name").alias("new_name"),
        F.col("before.city").alias("old_city"),
        F.col("after.city").alias("new_city"),
        F.col("before.tier").alias("old_tier"),
        F.col("after.tier").alias("new_tier"),
    )
)

print(f"\nCustomers with changed attributes: {updated_customers.count()}")
updated_customers.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: SILVER LAYER -- SCD Type 2 (Full History)
# MAGIC
# MAGIC SCD Type 2 preserves the complete history of every customer. Each change
# MAGIC creates a new version record while closing the previous one.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Create and Seed the History Table

# COMMAND ----------

# Create SCD Type 2 table
spark.sql("""
    CREATE OR REPLACE TABLE silver_customers_history (
        surrogate_key STRING,
        customer_id STRING,
        name STRING,
        email STRING,
        phone STRING,
        city STRING,
        state STRING,
        tier STRING,
        signup_date STRING,
        is_current BOOLEAN,
        effective_date TIMESTAMP,
        end_date TIMESTAMP,
        version INT,
        cdc_operation STRING
    )
    USING DELTA
""")

# Seed with initial load -- all records are version 1 and current
initial_history = (spark.table("bronze_initial_load")
    .withColumn("surrogate_key",
        F.concat(F.col("customer_id"), F.lit("_v1")))
    .withColumn("is_current", F.lit(True))
    .withColumn("effective_date",
        F.to_timestamp("created_at", "yyyy-MM-dd HH:mm:ss"))
    .withColumn("end_date", F.lit(None).cast("timestamp"))
    .withColumn("version", F.lit(1))
    .withColumn("cdc_operation", F.lit("INITIAL_LOAD"))
    .select("surrogate_key", "customer_id", "name", "email", "phone",
            "city", "state", "tier", "signup_date",
            "is_current", "effective_date", "end_date", "version", "cdc_operation")
)

initial_history.write.format("delta").mode("append").saveAsTable("silver_customers_history")
history_count = spark.table("silver_customers_history").count()
print(f"SCD Type 2 history seeded: {history_count} records (all version 1)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 Apply CDC Events for SCD Type 2
# MAGIC
# MAGIC For each event we:
# MAGIC 1. **INSERT (I)**: Add new record with is_current=true
# MAGIC 2. **UPDATE (U)**: Close current record (set end_date, is_current=false),
# MAGIC    then insert new version
# MAGIC 3. **DELETE (D)**: Close current record (set end_date, is_current=false)

# COMMAND ----------

# Process CDC events in timestamp order for correct history
cdc_ordered = (spark.table("bronze_cdc_events")
    .orderBy("event_timestamp")
    .collect()
)

print(f"Processing {len(cdc_ordered)} CDC events in timestamp order...")

# Process in batches for efficiency
batch_size = 100
processed = 0

for batch_start in range(0, len(cdc_ordered), batch_size):
    batch = cdc_ordered[batch_start:batch_start + batch_size]
    batch_df = spark.createDataFrame(batch)
    batch_df.createOrReplaceTempView("cdc_batch")

    # Step 1: Close current records for customers being updated or deleted
    spark.sql("""
        MERGE INTO silver_customers_history AS target
        USING (
            SELECT DISTINCT customer_id, event_timestamp
            FROM cdc_batch
            WHERE operation IN ('U', 'D')
        ) AS source
        ON target.customer_id = source.customer_id
           AND target.is_current = true
           AND source.event_timestamp > target.effective_date
        WHEN MATCHED THEN UPDATE SET
            target.is_current = false,
            target.end_date = source.event_timestamp
    """)

    # Step 2: Insert new version records for inserts and updates
    inserts_and_updates = batch_df.filter("operation IN ('I', 'U')")

    if inserts_and_updates.count() > 0:
        # Get current max version for each customer
        max_versions = (spark.table("silver_customers_history")
            .groupBy("customer_id")
            .agg(F.max("version").alias("max_version"))
        )

        new_records = (inserts_and_updates
            .join(max_versions, on="customer_id", how="left")
            .withColumn("new_version", F.coalesce(F.col("max_version"), F.lit(0)) + 1)
            .withColumn("surrogate_key",
                F.concat(F.col("customer_id"), F.lit("_v"),
                         F.col("new_version").cast("string")))
            .select(
                F.col("surrogate_key"),
                F.col("customer_id"),
                F.col("after_name").alias("name"),
                F.col("after_email").alias("email"),
                F.col("after_phone").alias("phone"),
                F.col("after_city").alias("city"),
                F.col("after_state").alias("state"),
                F.col("after_tier").alias("tier"),
                F.col("after_signup_date").alias("signup_date"),
                F.lit(True).alias("is_current"),
                F.col("event_timestamp").alias("effective_date"),
                F.lit(None).cast("timestamp").alias("end_date"),
                F.col("new_version").alias("version"),
                F.col("operation").alias("cdc_operation"),
            )
        )

        new_records.write.format("delta").mode("append").saveAsTable("silver_customers_history")

    processed += len(batch)
    if (batch_start // batch_size + 1) % 3 == 0:
        print(f"  Processed {processed}/{len(cdc_ordered)} events...")

total_history = spark.table("silver_customers_history").count()
current_records = spark.table("silver_customers_history").filter("is_current = true").count()
historical_records = spark.table("silver_customers_history").filter("is_current = false").count()

print(f"\nSCD Type 2 processing complete:")
print(f"  Total history records: {total_history}")
print(f"  Current (is_current=true): {current_records}")
print(f"  Historical (is_current=false): {historical_records}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 Verify SCD Type 2 History

# COMMAND ----------

# Show a customer with multiple versions
multi_version_customer = (spark.table("silver_customers_history")
    .groupBy("customer_id")
    .count()
    .filter("count > 2")
    .orderBy(F.desc("count"))
    .first()
)

if multi_version_customer:
    example_id = multi_version_customer["customer_id"]
    print(f"=== HISTORY FOR {example_id} ({multi_version_customer['count']} versions) ===")
    spark.sql(f"""
        SELECT customer_id, version, name, city, tier,
               is_current, effective_date, end_date, cdc_operation
        FROM silver_customers_history
        WHERE customer_id = '{example_id}'
        ORDER BY version
    """).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.4 Validate CDC Events -- Out-of-Order Detection

# COMMAND ----------

# Mark each event as valid/out-of-order based on timestamp comparison
cdc_all = spark.table("bronze_cdc_events")

# For each customer, check if events arrive in timestamp order
event_window = Window.partitionBy("customer_id").orderBy("_ingest_timestamp")

validated_events = (cdc_all
    .withColumn("prev_event_ts",
        F.lag("event_timestamp").over(event_window))
    .withColumn("is_out_of_order",
        F.when(F.col("prev_event_ts").isNull(), False)
         .otherwise(F.col("event_timestamp") < F.col("prev_event_ts")))
    .withColumn("is_valid",
        F.when(F.col("customer_id").isNull(), False)
         .when(F.col("operation") == "I", F.col("after_name").isNotNull())
         .when(F.col("operation") == "U", F.col("after_name").isNotNull())
         .when(F.col("operation") == "D", F.col("before_name").isNotNull())
         .otherwise(False))
    .withColumn("applied_action",
        F.when(~F.col("is_valid"), "error")
         .when(F.col("is_out_of_order"), "skipped_out_of_order")
         .otherwise("applied"))
)

validated_events.write.format("delta").mode("overwrite").saveAsTable("silver_cdc_events_validated")

print("=== CDC EVENT VALIDATION SUMMARY ===")
validated_events.groupBy("applied_action").count().orderBy("applied_action").show()

ooo_count = validated_events.filter("is_out_of_order = true").count()
total_events = validated_events.count()
print(f"Out-of-order rate: {ooo_count}/{total_events} = {ooo_count/total_events*100:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.5 Silver Layer Data Quality Report

# COMMAND ----------

print("=" * 65)
print("SILVER LAYER DATA QUALITY REPORT")
print("=" * 65)

s_current = spark.table("silver_customers_current").count()
s_history = spark.table("silver_customers_history").count()
s_validated = spark.table("silver_cdc_events_validated").count()
s_history_current = spark.table("silver_customers_history").filter("is_current = true").count()
s_history_closed = spark.table("silver_customers_history").filter("is_current = false").count()

print(f"\n  silver_customers_current:        {s_current:>6} rows (SCD Type 1)")
print(f"  silver_customers_history:        {s_history:>6} rows (SCD Type 2)")
print(f"    - Current records:             {s_history_current:>6}")
print(f"    - Historical records:          {s_history_closed:>6}")
print(f"  silver_cdc_events_validated:     {s_validated:>6} rows")
print(f"\n  Consistency check:")
print(f"    SCD1 current count:            {s_current}")
print(f"    SCD2 is_current=true count:    {s_history_current}")
print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: GOLD LAYER -- CDC Analytics

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Customer Change Frequency

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_customer_change_frequency AS
# MAGIC WITH change_stats AS (
# MAGIC   SELECT
# MAGIC     customer_id,
# MAGIC     COUNT(*) AS total_changes,
# MAGIC     SUM(CASE WHEN operation = 'I' THEN 1 ELSE 0 END) AS inserts,
# MAGIC     SUM(CASE WHEN operation = 'U' THEN 1 ELSE 0 END) AS updates,
# MAGIC     SUM(CASE WHEN operation = 'D' THEN 1 ELSE 0 END) AS deletes,
# MAGIC     MIN(event_timestamp) AS first_event,
# MAGIC     MAX(event_timestamp) AS last_event
# MAGIC   FROM bronze_cdc_events
# MAGIC   GROUP BY customer_id
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   CASE
# MAGIC     WHEN total_changes > 1
# MAGIC     THEN ROUND(
# MAGIC       CAST(DATEDIFF(last_event, first_event) AS DOUBLE) / (total_changes - 1), 1
# MAGIC     )
# MAGIC     ELSE NULL
# MAGIC   END AS avg_days_between_changes,
# MAGIC   CASE
# MAGIC     WHEN updates >= 3 THEN 'Highly Active'
# MAGIC     WHEN updates >= 1 THEN 'Moderately Active'
# MAGIC     WHEN deletes > 0 THEN 'Churned'
# MAGIC     ELSE 'Stable'
# MAGIC   END AS activity_segment
# MAGIC FROM change_stats
# MAGIC ORDER BY total_changes DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Change frequency distribution
# MAGIC SELECT activity_segment, COUNT(*) AS customer_count,
# MAGIC        ROUND(AVG(total_changes), 1) AS avg_changes,
# MAGIC        ROUND(AVG(updates), 1) AS avg_updates
# MAGIC FROM gold_customer_change_frequency
# MAGIC GROUP BY activity_segment
# MAGIC ORDER BY avg_changes DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Churn Analysis

# COMMAND ----------

# Identify churned customers (those with delete events)
# and analyze their characteristics
churned = (spark.table("bronze_cdc_events")
    .filter("operation = 'D'")
    .select("customer_id", "event_timestamp",
            "before_name", "before_city", "before_tier")
)

# Join with initial load to get signup info
initial = spark.table("bronze_initial_load")

churn_analysis = (churned.alias("c")
    .join(initial.alias("i"), on="customer_id", how="left")
    .withColumn("tenure_days",
        F.datediff(F.col("c.event_timestamp"),
                   F.to_timestamp(F.col("i.created_at"), "yyyy-MM-dd HH:mm:ss")))
    .select(
        F.col("c.customer_id"),
        F.col("c.before_name").alias("name"),
        F.col("c.before_city").alias("last_city"),
        F.col("c.before_tier").alias("tier_at_deletion"),
        F.col("c.event_timestamp").alias("deletion_date"),
        F.col("i.signup_date"),
        F.col("tenure_days"),
    )
)

churn_analysis.write.format("delta").mode("overwrite").saveAsTable("gold_churn_analysis")
churn_count = spark.table("gold_churn_analysis").count()
print(f"Gold churn analysis: {churn_count} churned customers")

# COMMAND ----------

# Churn summary
print("=== CHURN ANALYSIS SUMMARY ===")
spark.sql("""
    SELECT
        tier_at_deletion,
        COUNT(*) AS churned_count,
        ROUND(AVG(tenure_days), 0) AS avg_tenure_days,
        MIN(tenure_days) AS min_tenure,
        MAX(tenure_days) AS max_tenure
    FROM gold_churn_analysis
    GROUP BY tier_at_deletion
    ORDER BY churned_count DESC
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Data Freshness Monitoring

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_data_freshness AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('hour', event_timestamp) AS event_hour,
# MAGIC   COUNT(*) AS events_in_hour,
# MAGIC   COUNT(DISTINCT customer_id) AS unique_customers,
# MAGIC   SUM(CASE WHEN operation = 'I' THEN 1 ELSE 0 END) AS inserts,
# MAGIC   SUM(CASE WHEN operation = 'U' THEN 1 ELSE 0 END) AS updates,
# MAGIC   SUM(CASE WHEN operation = 'D' THEN 1 ELSE 0 END) AS deletes,
# MAGIC   -- Latency: time between event and ingestion
# MAGIC   ROUND(AVG(
# MAGIC     CAST(UNIX_TIMESTAMP(_ingest_timestamp) - UNIX_TIMESTAMP(event_timestamp) AS DOUBLE)
# MAGIC   ), 1) AS avg_latency_seconds,
# MAGIC   -- Cumulative totals
# MAGIC   SUM(COUNT(*)) OVER (ORDER BY DATE_TRUNC('hour', event_timestamp)
# MAGIC     ROWS UNBOUNDED PRECEDING) AS cumulative_events
# MAGIC FROM bronze_cdc_events
# MAGIC GROUP BY DATE_TRUNC('hour', event_timestamp)
# MAGIC ORDER BY event_hour

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_data_freshness LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.4 CDC Audit Dashboard

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold_cdc_audit_dashboard AS
# MAGIC SELECT
# MAGIC   DATE_TRUNC('hour', event_timestamp) AS audit_hour,
# MAGIC   operation,
# MAGIC   applied_action,
# MAGIC   COUNT(*) AS event_count,
# MAGIC   SUM(CASE WHEN is_out_of_order THEN 1 ELSE 0 END) AS out_of_order_count,
# MAGIC   SUM(CASE WHEN NOT is_valid THEN 1 ELSE 0 END) AS invalid_count,
# MAGIC   ROUND(SUM(CASE WHEN is_out_of_order THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS ooo_rate_pct,
# MAGIC   COUNT(DISTINCT customer_id) AS affected_customers
# MAGIC FROM silver_cdc_events_validated
# MAGIC GROUP BY DATE_TRUNC('hour', event_timestamp), operation, applied_action
# MAGIC ORDER BY audit_hour, operation

# COMMAND ----------

# Audit summary
print("=== CDC AUDIT SUMMARY ===")
spark.sql("""
    SELECT
        operation,
        SUM(event_count) AS total_events,
        SUM(out_of_order_count) AS total_ooo,
        SUM(invalid_count) AS total_invalid,
        ROUND(SUM(out_of_order_count) * 100.0 / SUM(event_count), 1) AS ooo_rate_pct
    FROM gold_cdc_audit_dashboard
    GROUP BY operation
    ORDER BY operation
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Cross-Layer Data Quality Validation

# COMMAND ----------

print("=" * 70)
print("CDC PIPELINE -- CROSS-LAYER DATA QUALITY REPORT")
print("=" * 70)

b_initial = spark.table("bronze_initial_load").count()
b_cdc = spark.table("bronze_cdc_events").count()
s_current = spark.table("silver_customers_current").count()
s_history = spark.table("silver_customers_history").count()
s_validated = spark.table("silver_cdc_events_validated").count()
g_change = spark.table("gold_customer_change_frequency").count()
g_churn = spark.table("gold_churn_analysis").count()
g_fresh = spark.table("gold_data_freshness").count()
g_audit = spark.table("gold_cdc_audit_dashboard").count()

print(f"\n{'Layer':<12} {'Table':<40} {'Rows':>8}")
print("-" * 65)
print(f"{'BRONZE':<12} {'bronze_initial_load':<40} {b_initial:>8}")
print(f"{'BRONZE':<12} {'bronze_cdc_events':<40} {b_cdc:>8}")
print("-" * 65)
print(f"{'SILVER':<12} {'silver_customers_current (SCD1)':<40} {s_current:>8}")
print(f"{'SILVER':<12} {'silver_customers_history (SCD2)':<40} {s_history:>8}")
print(f"{'SILVER':<12} {'silver_cdc_events_validated':<40} {s_validated:>8}")
print("-" * 65)
print(f"{'GOLD':<12} {'gold_customer_change_frequency':<40} {g_change:>8}")
print(f"{'GOLD':<12} {'gold_churn_analysis':<40} {g_churn:>8}")
print(f"{'GOLD':<12} {'gold_data_freshness':<40} {g_fresh:>8}")
print(f"{'GOLD':<12} {'gold_cdc_audit_dashboard':<40} {g_audit:>8}")
print("=" * 70)

# Accountability check
inserts_in_cdc = spark.table("bronze_cdc_events").filter("operation = 'I'").count()
updates_in_cdc = spark.table("bronze_cdc_events").filter("operation = 'U'").count()
deletes_in_cdc = spark.table("bronze_cdc_events").filter("operation = 'D'").count()

print(f"\nCDC Event Breakdown:")
print(f"  Inserts:  {inserts_in_cdc}")
print(f"  Updates:  {updates_in_cdc}")
print(f"  Deletes:  {deletes_in_cdc}")
print(f"  Total:    {b_cdc}")
print(f"\nExpected SCD1 count: {b_initial} + {inserts_in_cdc} - {deletes_in_cdc} = {b_initial + inserts_in_cdc - deletes_in_cdc}")
print(f"Actual SCD1 count:   {s_current}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Business Insights Summary

# COMMAND ----------

# Insight 1: Customer base growth
print("=== CUSTOMER BASE GROWTH ===")
print(f"Initial customers:     {b_initial}")
print(f"New registrations:     {inserts_in_cdc}")
print(f"Deletions (churn):     {deletes_in_cdc}")
print(f"Current customer base: {s_current}")
growth_rate = (s_current - b_initial) / b_initial * 100
print(f"Net growth rate:       {growth_rate:.1f}%")

# COMMAND ----------

# Insight 2: Most changed customers
print("=== TOP 10 MOST FREQUENTLY CHANGED CUSTOMERS ===")
spark.sql("""
    SELECT customer_id, total_changes, inserts, updates, deletes,
           avg_days_between_changes, activity_segment
    FROM gold_customer_change_frequency
    ORDER BY total_changes DESC
    LIMIT 10
""").show(truncate=False)

# COMMAND ----------

# Insight 3: Churn by tier
print("=== CHURN RATE BY TIER ===")
spark.sql("""
    SELECT
        tier_at_deletion,
        COUNT(*) AS churned,
        ROUND(AVG(tenure_days), 0) AS avg_tenure_days,
        ROUND(COUNT(*) * 100.0 / (
            SELECT COUNT(*) FROM gold_churn_analysis
        ), 1) AS pct_of_all_churn
    FROM gold_churn_analysis
    GROUP BY tier_at_deletion
    ORDER BY churned DESC
""").show(truncate=False)

# COMMAND ----------

# Insight 4: Pipeline health
print("=== PIPELINE HEALTH (Data Freshness) ===")
spark.sql("""
    SELECT
        COUNT(*) AS total_hours_tracked,
        ROUND(AVG(events_in_hour), 1) AS avg_events_per_hour,
        ROUND(AVG(avg_latency_seconds), 1) AS avg_latency_sec,
        MAX(cumulative_events) AS total_events_processed
    FROM gold_data_freshness
""").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: Cleanup

# COMMAND ----------

# Drop all tables and views
spark.sql("DROP VIEW IF EXISTS cdc_latest_events")
spark.sql("DROP VIEW IF EXISTS cdc_batch")

tables_to_drop = [row.tableName for row in spark.sql(f"SHOW TABLES IN {DATABASE}").collect()]
for t in tables_to_drop:
    spark.sql(f"DROP TABLE IF EXISTS {DATABASE}.{t}")
    print(f"  Dropped: {DATABASE}.{t}")

spark.sql(f"DROP DATABASE IF EXISTS {DATABASE}")
print(f"\nDatabase '{DATABASE}' dropped. Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary
# MAGIC
# MAGIC In this project you built a complete CDC Pipeline:
# MAGIC
# MAGIC | Layer | Tables | Purpose |
# MAGIC |-------|--------|---------|
# MAGIC | **Bronze** | 2 tables | Initial customer load + raw CDC events (immutable audit trail) |
# MAGIC | **Silver** | 3 tables | SCD Type 1 current state, SCD Type 2 full history, validated events |
# MAGIC | **Gold** | 4 tables | Change frequency, churn analysis, data freshness, audit dashboard |
# MAGIC
# MAGIC **Key techniques practiced**:
# MAGIC - Delta Lake MERGE with INSERT/UPDATE/DELETE actions
# MAGIC - SCD Type 1 (overwrite) and SCD Type 2 (history tracking) patterns
# MAGIC - Out-of-order event detection and handling via timestamps
# MAGIC - Change Data Feed to audit what MERGE operations changed
# MAGIC - Time travel to compare table states before and after CDC processing
# MAGIC - Surrogate keys and version numbering for SCD Type 2
# MAGIC - Churn analysis from delete events
# MAGIC - Data freshness and pipeline health monitoring
