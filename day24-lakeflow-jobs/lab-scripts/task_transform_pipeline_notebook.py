# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Task: Transform Pipeline (Bronze -> Silver -> Gold)
# MAGIC
# MAGIC **Objective**: Demonstrate a multi-layer transformation task designed to run inside
# MAGIC a Lakeflow Job. This notebook reads raw data from Bronze, cleans it into Silver,
# MAGIC and aggregates it into Gold -- all in a single task.
# MAGIC
# MAGIC **Key Insight**: In production, this would be an SDP (Spark Declarative Pipeline).
# MAGIC This notebook simulates the same Bronze -> Silver -> Gold flow so you can test
# MAGIC the DAG orchestration without deploying a full SDP pipeline.
# MAGIC
# MAGIC **Usage in a Lakeflow Job**:
# MAGIC ```
# MAGIC Task type:   Notebook
# MAGIC Depends on:  ingest_orders
# MAGIC Parameters:  catalog, run_date
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup: Parameters and Configuration
# MAGIC
# MAGIC This task accepts parameters from the parent Lakeflow Job via widgets.
# MAGIC When the job passes `run_date=2025-06-15`, the widget receives that value.
# MAGIC When running interactively, the default value is used instead.

# COMMAND ----------

dbutils.widgets.text("catalog", "ecommerce_demo", "Catalog")
dbutils.widgets.text("run_date", "2025-06-15", "Run Date")
dbutils.widgets.text("job_run_id", "interactive", "Job Run ID")

catalog = dbutils.widgets.get("catalog")
run_date = dbutils.widgets.get("run_date")
job_run_id = dbutils.widgets.get("job_run_id")

print(f"Transform Pipeline Task")
print(f"  Catalog:    {catalog}")
print(f"  Run Date:   {run_date}")
print(f"  Job Run ID: {job_run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 1: Generate Sample Bronze Data
# MAGIC
# MAGIC In production, the upstream `ingest_orders` task populates Bronze.
# MAGIC Here we generate sample data so this notebook runs standalone.

# COMMAND ----------

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.functions import (
    col,
    when,
    lower,
    trim,
    regexp_replace,
    sum as _sum,
    count,
    avg,
    round as _round,
    current_timestamp,
    lit,
    to_date,
    countDistinct,
)
from datetime import datetime, timedelta
import random

random.seed(42)

# Simulate raw order records (messy data, as it would arrive from source)
bronze_data = []
stores = ["Store_NYC", "store_la", "STORE_CHI", "Store_NYC", "store_la"]
statuses = ["completed", "completed", "completed", "cancelled", "COMPLETED", "pending"]

for i in range(1, 201):
    bronze_data.append((
        f"ORD-{i:05d}",
        f"CUST-{random.randint(1, 50):04d}",
        random.choice(stores),
        random.choice(statuses),
        round(random.uniform(10.0, 500.0), 2),
        round(random.uniform(0.0, 50.0), 2),
        random.randint(1, 10),
        datetime(2025, 6, 15, random.randint(0, 23), random.randint(0, 59)),
        datetime.utcnow(),
        f"s3://ecommerce-lakehouse/raw/orders/batch_{random.randint(1,5)}.json",
    ))

bronze_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), True),
    StructField("store_name", StringType(), True),
    StructField("status", StringType(), True),
    StructField("order_total", DoubleType(), True),
    StructField("discount_amount", DoubleType(), True),
    StructField("item_count", IntegerType(), True),
    StructField("order_timestamp", TimestampType(), True),
    StructField("_ingested_at", TimestampType(), True),
    StructField("_source_file", StringType(), True),
])

bronze_df = spark.createDataFrame(bronze_data, schema=bronze_schema)
bronze_df.createOrReplaceTempView("bronze_orders")

print(f"Bronze records loaded: {bronze_df.count()}")
bronze_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 2: Bronze -> Silver (Clean and Standardize)
# MAGIC
# MAGIC Silver layer transformations:
# MAGIC - Standardize store names to lowercase
# MAGIC - Normalize status values
# MAGIC - Remove records with null order totals
# MAGIC - Add data quality flags
# MAGIC - Deduplicate by order_id

# COMMAND ----------

silver_df = (
    bronze_df
    # Standardize store names
    .withColumn("store_name_clean", lower(trim(col("store_name"))))
    # Normalize status to lowercase
    .withColumn("status_clean", lower(trim(col("status"))))
    # Flag records with potential quality issues
    .withColumn(
        "dq_flag",
        when(col("order_total").isNull(), "missing_total")
        .when(col("order_total") < 0, "negative_total")
        .when(col("customer_id").isNull(), "missing_customer")
        .otherwise("pass"),
    )
    # Filter out null totals
    .filter(col("order_total").isNotNull())
    # Deduplicate by order_id (keep first occurrence)
    .dropDuplicates(["order_id"])
    # Add processing metadata
    .withColumn("_processed_at", current_timestamp())
    .withColumn("_job_run_id", lit(job_run_id))
    # Select final Silver columns
    .select(
        "order_id",
        "customer_id",
        "store_name_clean",
        "status_clean",
        "order_total",
        "discount_amount",
        "item_count",
        "order_timestamp",
        "dq_flag",
        "_ingested_at",
        "_processed_at",
        "_job_run_id",
    )
    .withColumnRenamed("store_name_clean", "store_name")
    .withColumnRenamed("status_clean", "status")
)

silver_df.createOrReplaceTempView("silver_orders")

total_silver = silver_df.count()
dq_passed = silver_df.filter(col("dq_flag") == "pass").count()
dq_failed = total_silver - dq_passed

print(f"Silver records: {total_silver}")
print(f"  DQ passed: {dq_passed}")
print(f"  DQ flagged: {dq_failed}")
silver_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 3: Silver -> Gold (Aggregate for Analytics)
# MAGIC
# MAGIC Gold layer produces two business-ready tables:
# MAGIC 1. **Daily revenue by store** -- used by the downstream SQL report task
# MAGIC 2. **Order status summary** -- used for operational dashboards

# COMMAND ----------

# Gold Table 1: Daily Revenue by Store
gold_revenue_df = (
    silver_df
    .filter(col("status") == "completed")
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "store_name")
    .agg(
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        _round(_sum("order_total"), 2).alias("gross_revenue"),
        _round(_sum("discount_amount"), 2).alias("total_discounts"),
        _round(_sum(col("order_total") - col("discount_amount")), 2).alias("net_revenue"),
        _round(avg("order_total"), 2).alias("avg_order_value"),
        _round(avg("item_count"), 1).alias("avg_items_per_order"),
    )
    .orderBy("order_date", "store_name")
)

gold_revenue_df.createOrReplaceTempView("gold_daily_revenue")

print("Gold: Daily Revenue by Store")
gold_revenue_df.show(truncate=False)

# COMMAND ----------

# Gold Table 2: Order Status Summary
gold_status_df = (
    silver_df
    .withColumn("order_date", to_date(col("order_timestamp")))
    .groupBy("order_date", "status")
    .agg(
        count("order_id").alias("order_count"),
        _round(_sum("order_total"), 2).alias("total_value"),
    )
    .orderBy("order_date", "status")
)

gold_status_df.createOrReplaceTempView("gold_order_status")

print("Gold: Order Status Summary")
gold_status_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Step 4: Task Output Values
# MAGIC
# MAGIC Lakeflow Jobs supports passing values between tasks using `dbutils.jobs.taskValues`.
# MAGIC The downstream notification task can read these values to include in its alert.

# COMMAND ----------

# Set task output values for downstream tasks to consume
# These are accessible via: dbutils.jobs.taskValues.get("transform_pipeline", "key")
dbutils.jobs.taskValues.set(key="bronze_count", value=bronze_df.count())
dbutils.jobs.taskValues.set(key="silver_count", value=total_silver)
dbutils.jobs.taskValues.set(key="dq_failed_count", value=dq_failed)
dbutils.jobs.taskValues.set(key="gold_revenue_rows", value=gold_revenue_df.count())
dbutils.jobs.taskValues.set(key="run_date", value=run_date)

print("Task values set for downstream consumption:")
print(f"  bronze_count:     {bronze_df.count()}")
print(f"  silver_count:     {total_silver}")
print(f"  dq_failed_count:  {dq_failed}")
print(f"  gold_revenue_rows: {gold_revenue_df.count()}")
print(f"  run_date:         {run_date}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Verification: Query Gold Tables
# MAGIC
# MAGIC Quick sanity check that the Gold layer is ready for the downstream SQL report task.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Gold daily revenue view
# MAGIC SELECT
# MAGIC     store_name,
# MAGIC     total_orders,
# MAGIC     unique_customers,
# MAGIC     net_revenue,
# MAGIC     avg_order_value
# MAGIC FROM gold_daily_revenue
# MAGIC ORDER BY net_revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Verify Gold order status view
# MAGIC SELECT
# MAGIC     status,
# MAGIC     order_count,
# MAGIC     total_value
# MAGIC FROM gold_order_status
# MAGIC ORDER BY order_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup

# COMMAND ----------

# Drop temp views (tables in production would persist)
spark.catalog.dropTempView("bronze_orders")
spark.catalog.dropTempView("silver_orders")
spark.catalog.dropTempView("gold_daily_revenue")
spark.catalog.dropTempView("gold_order_status")

dbutils.widgets.removeAll()

print("Transform pipeline task complete -- temp views cleaned up")
print("In production, Gold tables would persist for the downstream SQL task")
