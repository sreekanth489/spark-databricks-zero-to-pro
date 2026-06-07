# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Bronze Layer: E-Commerce Orders Ingestion
# MAGIC
# MAGIC **Task**: `ingest_bronze` in the `ecommerce_medallion_pipeline` job
# MAGIC
# MAGIC This notebook:
# MAGIC 1. Reads bundle parameters (`catalog`, `schema`, `environment`)
# MAGIC 2. Generates synthetic e-commerce orders (simulating a source system extract)
# MAGIC 3. Writes a raw Delta table: `{catalog}.{schema}.bronze_orders`
# MAGIC
# MAGIC In production the data generator would be replaced by a real source read
# MAGIC (S3 file, Kafka topic, JDBC connection, etc.).

# COMMAND ----------

# Parameters injected by the job task at runtime.
# In the bundle: base_parameters: { catalog: ${var.catalog}, schema: ${var.schema} }
# Locally (interactive): defaults are used.

dbutils.widgets.text("catalog",     "dev_catalog",  "Catalog")
dbutils.widgets.text("schema",      "ecommerce",    "Schema")
dbutils.widgets.text("environment", "dev",          "Environment")

CATALOG     = dbutils.widgets.get("catalog")
SCHEMA      = dbutils.widgets.get("schema")
ENVIRONMENT = dbutils.widgets.get("environment")

TABLE = f"{CATALOG}.{SCHEMA}.bronze_orders"

print(f"Environment : {ENVIRONMENT}")
print(f"Target table: {TABLE}")

# COMMAND ----------

import random
from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType
)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Synthetic Orders Data
# MAGIC
# MAGIC Simulates a daily extract from an operational database.
# MAGIC Row count and date range vary by environment to keep dev costs low.

# COMMAND ----------

ROW_COUNTS = {"dev": 10_000, "staging": 100_000, "prod": 500_000}
n_rows = ROW_COUNTS.get(ENVIRONMENT, 10_000)

def generate_orders(n: int, seed: int = 42):
    random.seed(seed)
    statuses   = ["PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"]
    categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys", "Beauty"]
    channels   = ["web", "mobile", "in-store", "phone"]
    regions    = ["US-West", "US-East", "US-Central", "EU", "APAC", "LATAM"]

    base_ts = datetime(2024, 1, 1)
    records = []
    for i in range(n):
        order_id   = f"ORD-{i+1:010d}"
        customer_id = f"CUST-{random.randint(1, 50000):07d}"
        product_id  = f"PROD-{random.randint(1, 5000):06d}"
        category    = random.choice(categories)
        status      = random.choices(statuses, weights=[5, 15, 20, 45, 10, 5])[0]
        quantity    = random.randint(1, 10)
        unit_price  = round(random.uniform(5.0, 999.99), 2)
        discount_pct = round(random.uniform(0, 0.30), 2) if random.random() < 0.3 else 0.0
        channel     = random.choice(channels)
        region      = random.choice(regions)
        order_ts    = base_ts + timedelta(
            days=random.randint(0, 89),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        # Introduce realistic data quality issues for silver to handle
        customer_id = None if random.random() < 0.02 else customer_id  # 2% nulls
        unit_price  = -unit_price if random.random() < 0.01 else unit_price  # 1% negatives

        records.append((
            order_id, customer_id, product_id, category, status,
            quantity, unit_price, discount_pct, channel, region, order_ts
        ))

    schema = StructType([
        StructField("order_id",     StringType(),    False),
        StructField("customer_id",  StringType(),    True),
        StructField("product_id",   StringType(),    False),
        StructField("category",     StringType(),    True),
        StructField("status",       StringType(),    True),
        StructField("quantity",     IntegerType(),   True),
        StructField("unit_price",   DoubleType(),    True),
        StructField("discount_pct", DoubleType(),    True),
        StructField("channel",      StringType(),    True),
        StructField("region",       StringType(),    True),
        StructField("order_ts",     TimestampType(), True),
    ])
    return spark.createDataFrame(records, schema)

orders_df = generate_orders(n_rows)
print(f"Generated {orders_df.count():,} orders for environment: {ENVIRONMENT}")
orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Bronze Table
# MAGIC
# MAGIC Bronze tables store raw data exactly as received — no transformations,
# MAGIC no quality filtering. We add ingestion metadata columns only.

# COMMAND ----------

bronze_df = orders_df \
    .withColumn("_ingested_at",   F.current_timestamp()) \
    .withColumn("_source",        F.lit("synthetic-generator")) \
    .withColumn("_pipeline_run",  F.lit(dbutils.notebook.entry_point.getDbutils()
                                        .notebook().getContext()
                                        .currentRunId().get()
                                        if hasattr(dbutils.notebook.entry_point, 'getDbutils') else "local"))

(bronze_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE))

count = spark.table(TABLE).count()
print(f"Wrote {count:,} rows to {TABLE}")
spark.sql(f"DESCRIBE EXTENDED {TABLE}").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Summary
# MAGIC
# MAGIC Report quality issues so the silver layer knows what to expect.

# COMMAND ----------

total = spark.table(TABLE).count()
issues = spark.sql(f"""
    SELECT
        COUNT(*) FILTER (WHERE customer_id IS NULL) AS null_customer_ids,
        COUNT(*) FILTER (WHERE unit_price < 0)      AS negative_prices,
        COUNT(*) FILTER (WHERE quantity <= 0)        AS invalid_quantities
    FROM {TABLE}
""").collect()[0]

print("Bronze Data Quality Report:")
print(f"  Total rows         : {total:,}")
print(f"  Null customer_id   : {issues['null_customer_ids']:,}  ({issues['null_customer_ids']/total*100:.1f}%)")
print(f"  Negative prices    : {issues['negative_prices']:,}  ({issues['negative_prices']/total*100:.1f}%)")
print(f"  Invalid quantities : {issues['invalid_quantities']:,}  ({issues['invalid_quantities']/total*100:.1f}%)")
print()
print(f"Bronze table {TABLE} ready for silver transformation.")
