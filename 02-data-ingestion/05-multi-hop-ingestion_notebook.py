# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Multi-Hop Ingestion: Bronze -> Silver -> Gold
# MAGIC
# MAGIC **Module 02 -- Topic 05 | Databricks Zero-to-Pro**
# MAGIC
# MAGIC This notebook builds a complete Medallion Architecture pipeline from
# MAGIC generated CSV files through Bronze (raw), Silver (cleaned), and Gold
# MAGIC (aggregated) Delta tables. All data is generated inline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql.functions import (
    col, current_timestamp, input_file_name, lit,
    to_date, trim, upper, when, count, sum as _sum, avg, min as _min, max as _max,
    round as _round
)
from pyspark.sql.types import StructType, StructField, StringType

# Configuration
TMP_DIR = "/tmp/m02_multi_hop"
LANDING_DIR = f"{TMP_DIR}/landing"
BRONZE_TABLE = "m02_bronze_orders"
SILVER_TABLE = "m02_silver_orders"
SILVER_QUARANTINE = "m02_silver_quarantine"
GOLD_TABLE = "m02_gold_daily_sales"

# Clean up from any prior run
dbutils.fs.rm(TMP_DIR, recurse=True)
for table in [BRONZE_TABLE, SILVER_TABLE, SILVER_QUARANTINE, GOLD_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")

dbutils.fs.mkdirs(LANDING_DIR)
print("Setup complete. All temp resources cleaned.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Generate Sample Source Data (CSV Files)
# MAGIC
# MAGIC We simulate an order management system exporting CSV files to a landing
# MAGIC zone. We intentionally include some bad data to demonstrate quality checks.

# COMMAND ----------

random.seed(42)
products = ["Widget A", "Widget B", "Widget C", "Gadget X", "Gadget Y"]
regions = ["US", "EU", "APAC", "  us  ", "eu"]  # Some messy casing/whitespace
customers = [f"CUST-{i:03d}" for i in range(1, 21)]
base_date = datetime(2024, 1, 1)

def generate_order_csv(file_name, num_orders, start_id, include_bad_rows=False):
    """Generate a CSV file with order data, optionally including bad rows."""
    header = "order_id,customer_id,product,amount,order_date,region\n"
    rows = []

    for i in range(num_orders):
        oid = start_id + i
        cust = random.choice(customers)
        prod = random.choice(products)
        amt = round(random.uniform(5.0, 500.0), 2)
        dt = (base_date + timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d")
        reg = random.choice(regions)
        rows.append(f"{oid},{cust},{prod},{amt},{dt},{reg}")

    if include_bad_rows:
        # Bad row 1: negative amount
        rows.append(f"{start_id + num_orders},CUST-001,Widget A,-50.00,2024-01-15,US")
        # Bad row 2: missing customer_id
        rows.append(f"{start_id + num_orders + 1},,Widget B,25.00,2024-01-16,EU")
        # Bad row 3: future date
        rows.append(f"{start_id + num_orders + 2},CUST-005,Gadget X,100.00,2099-12-31,US")
        # Bad row 4: completely malformed
        rows.append("GARBAGE,ROW,HERE")

    content = header + "\n".join(rows) + "\n"
    dbutils.fs.put(f"{LANDING_DIR}/{file_name}", content, overwrite=True)
    total = num_orders + (4 if include_bad_rows else 0)
    print(f"  {file_name}: {total} rows ({num_orders} good + {4 if include_bad_rows else 0} bad)")

print("Generating source CSV files:")
generate_order_csv("orders_batch_001.csv", num_orders=30, start_id=1, include_bad_rows=True)
generate_order_csv("orders_batch_002.csv", num_orders=25, start_id=100, include_bad_rows=False)
generate_order_csv("orders_batch_003.csv", num_orders=20, start_id=200, include_bad_rows=True)

print(f"\nFiles in landing zone:")
for f in dbutils.fs.ls(LANDING_DIR):
    print(f"  {f.name}  ({f.size} bytes)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. BRONZE Layer -- Raw Ingestion
# MAGIC
# MAGIC **Goal:** Land data exactly as-is from the source, adding only metadata
# MAGIC columns for lineage. No transformations, no filtering.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronze Schema
# MAGIC
# MAGIC We read everything as STRING to preserve source fidelity (no casting errors).

# COMMAND ----------

bronze_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("order_date", StringType(), True),
    StructField("region", StringType(), True),
])

# Read all CSV files from the landing zone
df_raw = (
    spark.read.format("csv")
    .option("header", "true")
    .schema(bronze_schema)
    .load(f"{LANDING_DIR}/*.csv")
)

# Add metadata columns
df_bronze = (
    df_raw
    .withColumn("_ingest_ts", current_timestamp())
    .withColumn("_source_file", input_file_name())
)

# Write to Bronze Delta table
(df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(BRONZE_TABLE)
)

bronze_count = spark.table(BRONZE_TABLE).count()
print(f"Bronze table created: {BRONZE_TABLE}")
print(f"Total rows in Bronze: {bronze_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Bronze Data

# COMMAND ----------

df_bronze_check = spark.table(BRONZE_TABLE)
print("Bronze schema (all STRING to preserve source fidelity):")
df_bronze_check.printSchema()

print("\nSample Bronze rows (note: no type casting, no cleaning):")
df_bronze_check.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SILVER Layer -- Cleaning & Validation
# MAGIC
# MAGIC **Goal:** Cast types, standardize values, deduplicate, and enforce data
# MAGIC quality rules. Bad records go to a quarantine table.

# COMMAND ----------

# Read from Bronze
df_from_bronze = spark.table(BRONZE_TABLE)

# Step 1: Type casting
df_typed = (
    df_from_bronze
    .withColumn("order_id_int", col("order_id").cast("int"))
    .withColumn("amount_dbl", col("amount").cast("double"))
    .withColumn("order_date_dt", to_date(col("order_date"), "yyyy-MM-dd"))
)

# Step 2: Standardize region (trim whitespace, uppercase)
df_standardized = (
    df_typed
    .withColumn("region_clean", upper(trim(col("region"))))
)

# Step 3: Data quality checks -- flag bad records
df_quality = (
    df_standardized
    .withColumn("_dq_flags",
        when(col("order_id_int").isNull(), lit("INVALID_ORDER_ID"))
        .when(col("customer_id").isNull() | (trim(col("customer_id")) == ""),
              lit("MISSING_CUSTOMER_ID"))
        .when(col("amount_dbl").isNull(), lit("INVALID_AMOUNT"))
        .when(col("amount_dbl") < 0, lit("NEGATIVE_AMOUNT"))
        .when(col("order_date_dt").isNull(), lit("INVALID_DATE"))
        .when(col("order_date_dt") > current_timestamp().cast("date"),
              lit("FUTURE_DATE"))
        .otherwise(lit("PASS"))
    )
)

print("Data quality flag distribution:")
df_quality.groupBy("_dq_flags").count().orderBy("_dq_flags").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split: Clean Records vs. Quarantine

# COMMAND ----------

# Clean records -> Silver table
df_silver_clean = (
    df_quality
    .filter(col("_dq_flags") == "PASS")
    .select(
        col("order_id_int").alias("order_id"),
        col("customer_id"),
        col("product"),
        col("amount_dbl").alias("amount"),
        col("order_date_dt").alias("order_date"),
        col("region_clean").alias("region"),
        col("_ingest_ts"),
    )
    # Deduplicate by order_id (keep first occurrence)
    .dropDuplicates(["order_id"])
)

# Quarantine records -> Quarantine table
df_quarantine = (
    df_quality
    .filter(col("_dq_flags") != "PASS")
    .select(
        col("order_id"),
        col("customer_id"),
        col("product"),
        col("amount"),
        col("order_date"),
        col("region"),
        col("_dq_flags"),
        col("_ingest_ts"),
        col("_source_file"),
    )
)

# Write Silver table
(df_silver_clean.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_TABLE)
)

# Write Quarantine table
(df_quarantine.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(SILVER_QUARANTINE)
)

silver_count = spark.table(SILVER_TABLE).count()
quarantine_count = spark.table(SILVER_QUARANTINE).count()
print(f"Silver table: {silver_count} clean rows")
print(f"Quarantine table: {quarantine_count} bad rows")
print(f"Total: {silver_count + quarantine_count} (should match Bronze: {bronze_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Silver Data

# COMMAND ----------

df_silver_check = spark.table(SILVER_TABLE)
print("Silver schema (proper types, standardized values):")
df_silver_check.printSchema()

print("\nSample Silver rows:")
df_silver_check.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Quarantined Records

# COMMAND ----------

df_quarantine_check = spark.table(SILVER_QUARANTINE)
print("Quarantined records (failed quality checks):")
df_quarantine_check.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. GOLD Layer -- Business Aggregation
# MAGIC
# MAGIC **Goal:** Create pre-aggregated tables ready for dashboards and analytics.

# COMMAND ----------

# Read from Silver
df_silver = spark.table(SILVER_TABLE)

# Gold table: Daily sales summary by region and product
df_gold = (
    df_silver
    .groupBy("order_date", "region", "product")
    .agg(
        count("order_id").alias("order_count"),
        _round(_sum("amount"), 2).alias("total_revenue"),
        _round(avg("amount"), 2).alias("avg_order_value"),
        _round(_min("amount"), 2).alias("min_order_value"),
        _round(_max("amount"), 2).alias("max_order_value"),
    )
    .orderBy("order_date", "region", "product")
)

# Write Gold table
(df_gold.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(GOLD_TABLE)
)

gold_count = spark.table(GOLD_TABLE).count()
print(f"Gold table created: {GOLD_TABLE}")
print(f"Aggregated rows: {gold_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspect Gold Data

# COMMAND ----------

df_gold_check = spark.table(GOLD_TABLE)
print("Gold schema (pre-aggregated business metrics):")
df_gold_check.printSchema()

print("\nGold table -- Daily Sales Summary:")
df_gold_check.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pipeline Summary
# MAGIC
# MAGIC Let us review the row counts at each layer to see the full picture.

# COMMAND ----------

summary_data = [
    ("Landing (CSV files)", "3 files", "Raw CSV", "Source system"),
    ("Bronze", str(bronze_count) + " rows", "Delta (all STRING)", "Append-only, metadata enriched"),
    ("Silver", str(silver_count) + " rows", "Delta (typed)", "Cleaned, validated, deduplicated"),
    ("Silver Quarantine", str(quarantine_count) + " rows", "Delta", "Failed quality checks"),
    ("Gold", str(gold_count) + " rows", "Delta (aggregated)", "Daily sales by region & product"),
]

df_summary = spark.createDataFrame(
    summary_data,
    schema="layer STRING, volume STRING, format STRING, description STRING"
)

print("=== Pipeline Summary ===")
df_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Querying the Gold Layer
# MAGIC
# MAGIC Gold tables are designed for fast, simple queries by analysts.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Top 5 Products by Revenue

# COMMAND ----------

top_products = (
    spark.table(GOLD_TABLE)
    .groupBy("product")
    .agg(
        _round(_sum("total_revenue"), 2).alias("grand_total_revenue"),
        _sum("order_count").alias("total_orders"),
    )
    .orderBy(col("grand_total_revenue").desc())
    .limit(5)
)

print("Top 5 Products by Revenue:")
top_products.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Revenue by Region

# COMMAND ----------

region_revenue = (
    spark.table(GOLD_TABLE)
    .groupBy("region")
    .agg(
        _round(_sum("total_revenue"), 2).alias("total_revenue"),
        _sum("order_count").alias("total_orders"),
        _round(avg("avg_order_value"), 2).alias("avg_order_value"),
    )
    .orderBy(col("total_revenue").desc())
)

print("Revenue by Region:")
region_revenue.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Data Lineage Visualization
# MAGIC
# MAGIC ```
# MAGIC Landing Zone          Bronze              Silver              Gold
# MAGIC ┌──────────┐     ┌─────────────┐    ┌─────────────┐    ┌──────────────┐
# MAGIC │ CSV files│────>│ Raw rows    │───>│ Clean rows  │───>│ Aggregated   │
# MAGIC │ (3 files)│     │ + metadata  │    │ Typed cols  │    │ Daily summary│
# MAGIC └──────────┘     │ All STRING  │    │ Deduped     │    │ By region    │
# MAGIC                  └─────────────┘    │ Validated   │    │ By product   │
# MAGIC                                    └──────┬──────┘    └──────────────┘
# MAGIC                                           │
# MAGIC                                    ┌──────┴──────┐
# MAGIC                                    │ Quarantine  │
# MAGIC                                    │ Bad records │
# MAGIC                                    └─────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Bonus: Delta Time Travel
# MAGIC
# MAGIC Because we used Delta format at every layer, we get time travel for free.
# MAGIC This means we can query any prior version of the data.

# COMMAND ----------

# View the history of the Bronze table
print("Bronze table history:")
spark.sql(f"DESCRIBE HISTORY {BRONZE_TABLE}").select(
    "version", "timestamp", "operation", "operationMetrics"
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove all tables and temp files created by this notebook.

# COMMAND ----------

for table in [BRONZE_TABLE, SILVER_TABLE, SILVER_QUARANTINE, GOLD_TABLE]:
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    print(f"  Dropped {table}")

dbutils.fs.rm(TMP_DIR, recurse=True)
print(f"\nRemoved {TMP_DIR}")
print("Cleanup complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Congratulations -- you have completed **Module 02: Data Ingestion**!
# MAGIC
# MAGIC Proceed to **Module 03: Data Transformations** to learn how to clean,
# MAGIC reshape, enrich, and join your ingested data using Spark and SQL.
