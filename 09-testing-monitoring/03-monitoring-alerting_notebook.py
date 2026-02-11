# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Monitoring & Alerting
# MAGIC > Module 09 -- Topic 03 | Access Spark metrics, monitor streaming, and build alert thresholds
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Access SparkContext metrics and job/stage information programmatically
# MAGIC 2. Trigger jobs and inspect execution plan details
# MAGIC 3. Simulate streaming query progress monitoring
# MAGIC 4. Build a custom monitoring dashboard with summary statistics
# MAGIC 5. Create alerting threshold checks for pipeline metrics
# MAGIC 6. Show Databricks SQL Alert configuration templates

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BooleanType
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import random
import time
import json

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Transactions Dataset
# MAGIC Large enough to produce meaningful Spark UI metrics.

# COMMAND ----------

num_transactions = 500_000
merchants = [f"Merchant_{i}" for i in range(100)]
categories = ["Retail", "Food", "Travel", "Entertainment", "Healthcare", "Education", "Utilities"]
payment_methods = ["credit_card", "debit_card", "bank_transfer", "digital_wallet"]

txn_data = [
    (
        i + 1,
        f"CUST_{random.randint(1, 5000):05d}",
        random.choice(merchants),
        random.choice(categories),
        round(random.uniform(1.0, 10000.0), 2),
        random.choice(payment_methods),
        datetime(2024, 1, 1) + timedelta(seconds=random.randint(0, 31536000)),
    )
    for i in range(num_transactions)
]

txn_schema = StructType([
    StructField("txn_id", IntegerType(), False),
    StructField("customer_id", StringType(), False),
    StructField("merchant", StringType(), False),
    StructField("category", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("payment_method", StringType(), False),
    StructField("txn_timestamp", TimestampType(), False),
])

txn_df = spark.createDataFrame(txn_data, schema=txn_schema)
txn_df.cache()
print(f"Transaction dataset: {txn_df.count()} rows")
txn_df.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Accessing Spark Metrics Programmatically
# MAGIC
# MAGIC The SparkContext provides a status tracker for active and completed jobs/stages.

# COMMAND ----------

# Access the SparkContext status tracker
sc = spark.sparkContext
tracker = sc.statusTracker()

# Trigger a job to have something to inspect
category_summary = (
    txn_df
    .groupBy("category")
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_amount"),
        F.avg("amount").alias("avg_amount"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
)
category_summary.collect()  # Triggers execution

# Check active jobs (likely empty since collect() completed)
active_jobs = tracker.getActiveJobIds()
print(f"Active jobs: {list(active_jobs)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inspecting Execution Plans
# MAGIC
# MAGIC The `.explain()` method reveals what Spark will actually do with your query.
# MAGIC Four modes are available: `simple`, `extended`, `codegen`, and `formatted`.

# COMMAND ----------

# A complex query involving joins, filters, and aggregations
merchant_txn = txn_df.filter(F.col("amount") > 100)
top_merchants = (
    merchant_txn
    .groupBy("merchant", "category")
    .agg(
        F.count("*").alias("txn_count"),
        F.sum("amount").alias("total_revenue"),
    )
    .filter(F.col("txn_count") > 10)
    .orderBy(F.desc("total_revenue"))
)

print("=== SIMPLE PLAN (Physical plan only) ===")
top_merchants.explain("simple")

# COMMAND ----------

print("=== FORMATTED PLAN (Readable with operators and attributes) ===")
top_merchants.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC **How to read the plan:**
# MAGIC - Read bottom-up: data flows from the leaf (Scan/InMemoryRelation) to the root
# MAGIC - Look for `Exchange` nodes -- these are shuffles (expensive)
# MAGIC - `HashAggregate` usually appears twice: partial aggregation before shuffle, final after
# MAGIC - `Filter` appearing close to the scan means predicate pushdown is working
# MAGIC - `Sort` nodes for ORDER BY add a full data shuffle

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Measuring Job and Stage Performance

# COMMAND ----------

def time_query(name, query_fn):
    """Execute a query function and measure wall-clock time."""
    start = time.time()
    result = query_fn()
    duration = time.time() - start
    return {"query_name": name, "duration_seconds": round(duration, 3), "result_preview": str(result)[:100]}


# Time several operations to compare performance
metrics = []

metrics.append(time_query(
    "Full count",
    lambda: txn_df.count()
))

metrics.append(time_query(
    "Filtered count (amount > 1000)",
    lambda: txn_df.filter(F.col("amount") > 1000).count()
))

metrics.append(time_query(
    "GroupBy category",
    lambda: txn_df.groupBy("category").count().collect()
))

metrics.append(time_query(
    "GroupBy merchant (100 groups)",
    lambda: txn_df.groupBy("merchant").count().collect()
))

metrics.append(time_query(
    "GroupBy customer_id (5000 groups)",
    lambda: txn_df.groupBy("customer_id").count().collect()
))

metrics.append(time_query(
    "Window function (rank by amount per category)",
    lambda: txn_df.withColumn(
        "rank", F.row_number().over(Window.partitionBy("category").orderBy(F.desc("amount")))
    ).filter(F.col("rank") <= 10).collect()
))

# Display results
perf_schema = StructType([
    StructField("query_name", StringType()),
    StructField("duration_seconds", DoubleType()),
    StructField("result_preview", StringType()),
])
perf_df = spark.createDataFrame(
    [(m["query_name"], m["duration_seconds"], m["result_preview"]) for m in metrics],
    schema=perf_schema
)
print("Query Performance Comparison:")
perf_df.select("query_name", "duration_seconds").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Simulated Streaming Query Monitoring
# MAGIC
# MAGIC We simulate streaming progress data to demonstrate monitoring patterns.
# MAGIC In production, you would read this from `query.recentProgress`.

# COMMAND ----------

# Simulate streaming progress snapshots
streaming_progress_data = []
base_time = datetime(2024, 6, 15, 10, 0, 0)

for i in range(60):  # 60 micro-batches
    ts = base_time + timedelta(seconds=i * 10)
    input_rows = random.randint(800, 1200)
    processing_ms = random.randint(3000, 8000)

    # Simulate a backlog scenario starting at batch 40
    if i >= 40:
        input_rows = random.randint(1500, 2500)
        processing_ms = random.randint(8000, 15000)

    processed_rows_per_sec = input_rows / (processing_ms / 1000.0)
    input_rows_per_sec = input_rows / 10.0  # 10-second trigger interval

    streaming_progress_data.append((
        i + 1,
        ts,
        input_rows,
        round(input_rows_per_sec, 1),
        round(processed_rows_per_sec, 1),
        processing_ms,
        random.randint(10000, 50000 + i * 500),  # state store size (grows over time)
    ))

stream_schema = StructType([
    StructField("batch_id", IntegerType()),
    StructField("timestamp", TimestampType()),
    StructField("num_input_rows", IntegerType()),
    StructField("input_rows_per_sec", DoubleType()),
    StructField("processed_rows_per_sec", DoubleType()),
    StructField("batch_duration_ms", IntegerType()),
    StructField("state_rows_total", IntegerType()),
])

stream_df = spark.createDataFrame(streaming_progress_data, schema=stream_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Health Analysis

# COMMAND ----------

stream_health = (
    stream_df
    .withColumn(
        "health_status",
        F.when(
            F.col("processed_rows_per_sec") >= F.col("input_rows_per_sec") * 1.2,
            F.lit("HEALTHY")
        ).when(
            F.col("processed_rows_per_sec") >= F.col("input_rows_per_sec"),
            F.lit("WARNING")
        ).otherwise(
            F.lit("CRITICAL")
        )
    )
    .withColumn(
        "throughput_ratio",
        F.round(F.col("processed_rows_per_sec") / F.col("input_rows_per_sec"), 2)
    )
)

print("Streaming Health Status (last 20 batches showing the backlog scenario):")
stream_health.filter(F.col("batch_id") >= 35).select(
    "batch_id", "input_rows_per_sec", "processed_rows_per_sec",
    "throughput_ratio", "batch_duration_ms", "health_status"
).show(25, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Health Summary

# COMMAND ----------

health_summary = (
    stream_health
    .groupBy("health_status")
    .agg(
        F.count("*").alias("batch_count"),
        F.avg("throughput_ratio").alias("avg_throughput_ratio"),
        F.avg("batch_duration_ms").alias("avg_batch_duration_ms"),
        F.avg("input_rows_per_sec").alias("avg_input_rate"),
        F.avg("processed_rows_per_sec").alias("avg_process_rate"),
    )
)

print("Streaming Health Summary:")
health_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Pipeline Monitoring Dashboard
# MAGIC
# MAGIC Build a monitoring view from simulated pipeline run metadata.

# COMMAND ----------

# Simulate pipeline run history
pipeline_runs = []
pipelines = ["orders_bronze", "orders_silver", "orders_gold", "customers_sync", "inventory_update"]

for day_offset in range(30):
    run_date = datetime(2024, 6, 1) + timedelta(days=day_offset)
    for pipeline in pipelines:
        start_time = run_date.replace(hour=random.randint(1, 6))
        duration_min = random.randint(5, 120)
        end_time = start_time + timedelta(minutes=duration_min)
        records = random.randint(10000, 500000)

        # Simulate occasional failures
        status = "SUCCESS"
        error_msg = None
        if random.random() < 0.08:
            status = "FAILED"
            error_msg = random.choice([
                "Connection timeout to source database",
                "Schema mismatch: unexpected column 'new_field'",
                "Out of memory on executor node",
                "Delta table version conflict",
            ])
            records = random.randint(0, records // 2)

        pipeline_runs.append((
            pipeline, run_date, start_time, end_time,
            duration_min, records, status, error_msg
        ))

run_schema = StructType([
    StructField("pipeline_name", StringType()),
    StructField("run_date", TimestampType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("duration_minutes", IntegerType()),
    StructField("records_processed", IntegerType()),
    StructField("status", StringType()),
    StructField("error_message", StringType()),
])

runs_df = spark.createDataFrame(pipeline_runs, schema=run_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard: Pipeline Health Overview

# COMMAND ----------

# Overall success rate per pipeline
pipeline_health = (
    runs_df
    .groupBy("pipeline_name")
    .agg(
        F.count("*").alias("total_runs"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successful_runs"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failed_runs"),
        F.avg("duration_minutes").alias("avg_duration_min"),
        F.max("duration_minutes").alias("max_duration_min"),
        F.avg("records_processed").alias("avg_records"),
    )
    .withColumn("success_rate", F.round(F.col("successful_runs") / F.col("total_runs"), 3))
    .orderBy("pipeline_name")
)

print("Pipeline Health Overview (Last 30 Days):")
pipeline_health.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard: Recent Failures

# COMMAND ----------

recent_failures = (
    runs_df
    .filter(F.col("status") == "FAILED")
    .orderBy(F.desc("run_date"))
    .select("pipeline_name", "run_date", "duration_minutes", "records_processed", "error_message")
)

print("Recent Pipeline Failures:")
recent_failures.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dashboard: Error Distribution

# COMMAND ----------

error_distribution = (
    runs_df
    .filter(F.col("status") == "FAILED")
    .groupBy("error_message")
    .agg(
        F.count("*").alias("occurrence_count"),
        F.collect_set("pipeline_name").alias("affected_pipelines"),
    )
    .orderBy(F.desc("occurrence_count"))
)

print("Error Distribution:")
error_distribution.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Alerting Threshold Checks
# MAGIC
# MAGIC Define thresholds and check current metrics against them.

# COMMAND ----------

# Define alerting thresholds
alert_thresholds = {
    "success_rate_min": 0.90,           # Alert if success rate drops below 90%
    "max_duration_minutes": 90,          # Alert if any run exceeds 90 minutes
    "min_records_per_run": 1000,         # Alert if a successful run processes too few records
    "max_consecutive_failures": 2,       # Alert if a pipeline fails 2+ times in a row
    "data_freshness_hours": 24,          # Alert if data is older than 24 hours
}

print("Alerting Thresholds:")
for metric, threshold in alert_thresholds.items():
    print(f"  {metric}: {threshold}")

# COMMAND ----------

# Check thresholds against actual metrics
alerts = []

# Check 1: Success rate per pipeline
for row in pipeline_health.collect():
    if row["success_rate"] < alert_thresholds["success_rate_min"]:
        alerts.append({
            "alert_type": "LOW_SUCCESS_RATE",
            "pipeline": row["pipeline_name"],
            "severity": "WARNING",
            "details": f"Success rate {row['success_rate']:.1%} < threshold {alert_thresholds['success_rate_min']:.1%}",
        })

# Check 2: Duration exceeded
long_runs = (
    runs_df
    .filter(F.col("duration_minutes") > alert_thresholds["max_duration_minutes"])
    .filter(F.col("status") == "SUCCESS")
)
for row in long_runs.collect():
    alerts.append({
        "alert_type": "LONG_RUNNING",
        "pipeline": row["pipeline_name"],
        "severity": "WARNING",
        "details": f"Run on {row['run_date'].date()} took {row['duration_minutes']} min "
                   f"(threshold: {alert_thresholds['max_duration_minutes']} min)",
    })

# Check 3: Low record count
low_records = (
    runs_df
    .filter(F.col("status") == "SUCCESS")
    .filter(F.col("records_processed") < alert_thresholds["min_records_per_run"])
)
for row in low_records.collect():
    alerts.append({
        "alert_type": "LOW_RECORD_COUNT",
        "pipeline": row["pipeline_name"],
        "severity": "CRITICAL",
        "details": f"Only {row['records_processed']} records on {row['run_date'].date()} "
                   f"(min: {alert_thresholds['min_records_per_run']})",
    })

# Display all alerts
if alerts:
    alert_schema = StructType([
        StructField("alert_type", StringType()),
        StructField("pipeline", StringType()),
        StructField("severity", StringType()),
        StructField("details", StringType()),
    ])
    alerts_df = spark.createDataFrame(
        [(a["alert_type"], a["pipeline"], a["severity"], a["details"]) for a in alerts],
        schema=alert_schema
    )
    print(f"ALERTS TRIGGERED: {alerts_df.count()}")
    alerts_df.show(50, truncate=False)
else:
    print("No alerts triggered. All metrics within thresholds.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Databricks SQL Alert Templates
# MAGIC
# MAGIC These SQL queries can be used directly in Databricks SQL Alerts.
# MAGIC They are shown as reference -- they require Databricks SQL warehouse to execute.

# COMMAND ----------

# MAGIC %md
# MAGIC ```sql
# MAGIC -- Alert 1: Data Freshness Check
# MAGIC -- Trigger: Alert when value > 0
# MAGIC -- Schedule: Every 15 minutes
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   TIMESTAMPDIFF(HOUR, MAX(updated_at), CURRENT_TIMESTAMP()) as hours_since_update
# MAGIC FROM pipeline_metadata.table_freshness
# MAGIC WHERE TIMESTAMPDIFF(HOUR, MAX(updated_at), CURRENT_TIMESTAMP()) > 24
# MAGIC GROUP BY table_name;
# MAGIC
# MAGIC -- Alert 2: Pipeline Failure Detection
# MAGIC -- Trigger: Alert when value > 0
# MAGIC -- Schedule: Every 5 minutes
# MAGIC SELECT
# MAGIC   COUNT(*) as recent_failures
# MAGIC FROM pipeline_metadata.run_log
# MAGIC WHERE status = 'FAILED'
# MAGIC   AND start_time > CURRENT_TIMESTAMP() - INTERVAL 1 HOUR;
# MAGIC
# MAGIC -- Alert 3: Data Quality Degradation
# MAGIC -- Trigger: Alert when value > 0
# MAGIC -- Schedule: Every 30 minutes
# MAGIC SELECT
# MAGIC   COUNT(*) as failing_checks
# MAGIC FROM pipeline_metadata.quality_metrics
# MAGIC WHERE check_date = CURRENT_DATE()
# MAGIC   AND pass_rate < threshold;
# MAGIC
# MAGIC -- Alert 4: Row Count Anomaly Detection
# MAGIC -- Trigger: Alert when value > 0
# MAGIC -- Schedule: Daily
# MAGIC WITH daily_counts AS (
# MAGIC   SELECT
# MAGIC     table_name,
# MAGIC     run_date,
# MAGIC     record_count,
# MAGIC     AVG(record_count) OVER (
# MAGIC       PARTITION BY table_name
# MAGIC       ORDER BY run_date
# MAGIC       ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
# MAGIC     ) as rolling_avg,
# MAGIC     STDDEV(record_count) OVER (
# MAGIC       PARTITION BY table_name
# MAGIC       ORDER BY run_date
# MAGIC       ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
# MAGIC     ) as rolling_stddev
# MAGIC   FROM pipeline_metadata.daily_row_counts
# MAGIC )
# MAGIC SELECT
# MAGIC   table_name,
# MAGIC   record_count,
# MAGIC   rolling_avg,
# MAGIC   rolling_stddev,
# MAGIC   ABS(record_count - rolling_avg) / NULLIF(rolling_stddev, 0) as z_score
# MAGIC FROM daily_counts
# MAGIC WHERE run_date = CURRENT_DATE()
# MAGIC   AND ABS(record_count - rolling_avg) / NULLIF(rolling_stddev, 0) > 2.0;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: SparkContext Configuration Inspection
# MAGIC
# MAGIC Understanding your runtime configuration is crucial for debugging and optimization.

# COMMAND ----------

# Display key Spark configuration values
key_configs = [
    "spark.sql.shuffle.partitions",
    "spark.default.parallelism",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
    "spark.sql.adaptive.skewJoin.enabled",
    "spark.executor.memory",
    "spark.driver.memory",
    "spark.sql.autoBroadcastJoinThreshold",
]

print("Key Spark Configuration:")
print("-" * 60)
for config in key_configs:
    try:
        value = spark.conf.get(config, "not set")
    except Exception:
        value = "not accessible"
    print(f"  {config}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

txn_df.unpersist()
print("Notebook 03-monitoring-alerting complete.")
print("Cached DataFrames unpersisted. No tables or temp views were created.")
