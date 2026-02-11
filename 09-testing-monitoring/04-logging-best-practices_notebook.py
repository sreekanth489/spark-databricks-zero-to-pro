# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 - Logging Best Practices
# MAGIC > Module 09 -- Topic 04 | Structured logging, pipeline tracking, and dead letter patterns
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Set up Python logging in a Databricks notebook
# MAGIC 2. Implement structured logging with JSON format
# MAGIC 3. Build a pipeline run tracker (start time, end time, records, status)
# MAGIC 4. Implement error handling with try/except and dead letter pattern
# MAGIC 5. Create a dead letter table for failed records
# MAGIC 6. Query simulated pipeline run history for trend analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, TimestampType, LongType, BooleanType
)
from datetime import datetime, timedelta
import logging
import json
import random
import uuid
import traceback

random.seed(42)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Python Logging Setup
# MAGIC
# MAGIC Standard Python logging works on the driver side. This is where your orchestration
# MAGIC code runs (the code between Spark transformations).

# COMMAND ----------

# Basic logging setup for notebooks
# Remove existing handlers to avoid duplicate output
root_logger = logging.getLogger()
for handler in root_logger.handlers[:]:
    root_logger.removeHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("pipeline.demo")
logger.setLevel(logging.INFO)

# Demonstrate log levels
logger.debug("This is a DEBUG message (not visible at INFO level)")
logger.info("This is an INFO message -- pipeline status updates")
logger.warning("This is a WARNING -- something unexpected but recoverable")
logger.error("This is an ERROR -- something broke")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Structured JSON Logging
# MAGIC
# MAGIC JSON-formatted logs are essential for production because they can be parsed
# MAGIC by log aggregation tools (Splunk, ELK, Datadog, Azure Log Analytics).

# COMMAND ----------

class JsonFormatter(logging.Formatter):
    """Format log records as JSON for machine-readable log aggregation."""

    def __init__(self, pipeline_name="unknown", run_id="unknown"):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.run_id = run_id

    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline": self.pipeline_name,
            "run_id": self.run_id,
        }
        # Add extra fields if provided
        for key in ["records_read", "records_written", "duration_seconds", "table_name"]:
            if hasattr(record, key):
                log_entry[key] = getattr(record, key)
        # Add exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["exception_type"] = record.exc_info[0].__name__
            log_entry["exception_message"] = str(record.exc_info[1])
        return json.dumps(log_entry, default=str)


# Create a JSON logger
json_handler = logging.StreamHandler()
json_handler.setFormatter(JsonFormatter(
    pipeline_name="orders_silver",
    run_id=f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
))

json_logger = logging.getLogger("pipeline.json")
json_logger.handlers = [json_handler]
json_logger.setLevel(logging.INFO)
json_logger.propagate = False

# Demonstrate structured logging
json_logger.info("Pipeline started")
json_logger.info("Reading source data", extra={"table_name": "bronze.orders"})
json_logger.info("Transform complete", extra={"records_read": 50000, "records_written": 48500})
json_logger.warning("Dropped 1500 records due to null order_id")

# Demonstrate error logging
try:
    result = 1 / 0
except ZeroDivisionError:
    json_logger.error("Division by zero in metric calculation", exc_info=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Pipeline Run Tracker
# MAGIC
# MAGIC A run tracker records every pipeline execution with metrics. This is the
# MAGIC foundation for SLA monitoring and debugging.

# COMMAND ----------

class PipelineRunTracker:
    """Track pipeline execution metadata for monitoring and debugging."""

    def __init__(self, pipeline_name, logger=None):
        self.pipeline_name = pipeline_name
        self.run_id = str(uuid.uuid4())[:8]
        self.start_time = datetime.now()
        self.end_time = None
        self.metrics = {}
        self.status = "RUNNING"
        self.error_message = None
        self.logger = logger or logging.getLogger(f"pipeline.{pipeline_name}")

        self.logger.info(
            f"[{self.run_id}] Pipeline '{self.pipeline_name}' STARTED "
            f"at {self.start_time.isoformat()}"
        )

    def log_metric(self, key, value):
        """Record a numeric metric for this run."""
        self.metrics[key] = value
        self.logger.info(f"[{self.run_id}] Metric: {key} = {value}")

    def log_step(self, step_name, detail=""):
        """Log a pipeline step completion."""
        self.logger.info(f"[{self.run_id}] Step completed: {step_name}. {detail}")

    def complete(self, status="SUCCESS", error_message=None):
        """Mark the pipeline run as complete."""
        self.end_time = datetime.now()
        self.status = status
        self.error_message = error_message
        duration = (self.end_time - self.start_time).total_seconds()

        level = logging.INFO if status == "SUCCESS" else logging.ERROR
        self.logger.log(
            level,
            f"[{self.run_id}] Pipeline '{self.pipeline_name}' {status} "
            f"in {duration:.1f}s. Metrics: {self.metrics}"
        )
        if error_message:
            self.logger.error(f"[{self.run_id}] Error: {error_message}")

        return self.to_dict()

    def to_dict(self):
        """Return run metadata as a dictionary."""
        duration = None
        if self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        return {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "status": self.status,
            "error_message": self.error_message,
            **self.metrics
        }

print("PipelineRunTracker class defined.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Demonstrate the Run Tracker with a Sample Pipeline

# COMMAND ----------

# Generate sample data for our pipeline
raw_data = []
for i in range(1000):
    order_id = i + 1
    customer_id = random.randint(1, 200)
    amount_str = str(round(random.uniform(10.0, 5000.0), 2))
    product = random.choice(["Laptop", "Phone", "Tablet", "Monitor", "Cable"])
    order_date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 364))

    # Inject some bad data
    if i % 40 == 0:
        amount_str = "INVALID"
    if i % 60 == 0:
        amount_str = ""
    if i % 80 == 0:
        amount_str = None

    raw_data.append((order_id, customer_id, amount_str, product, order_date))

raw_schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("customer_id", IntegerType(), False),
    StructField("amount_raw", StringType(), True),
    StructField("product", StringType(), False),
    StructField("order_date", TimestampType(), False),
])

raw_df = spark.createDataFrame(raw_data, schema=raw_schema)
print(f"Raw data: {raw_df.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Pipeline with Tracking

# COMMAND ----------

tracker = PipelineRunTracker("orders_bronze_to_silver", logger=logger)

try:
    # Step 1: Read raw data
    input_count = raw_df.count()
    tracker.log_metric("records_read", input_count)
    tracker.log_step("read_source", f"{input_count} records from bronze")

    # Step 2: Parse amount column with inline error handling
    parsed_df = (
        raw_df
        .withColumn(
            "amount",
            F.when(
                F.col("amount_raw").isNotNull() & F.col("amount_raw").rlike(r"^\d+\.?\d*$"),
                F.col("amount_raw").cast("double")
            ).otherwise(F.lit(None).cast("double"))
        )
        .withColumn(
            "_parse_error",
            F.when(
                F.col("amount").isNull() & F.col("amount_raw").isNotNull() & (F.col("amount_raw") != ""),
                F.lit("invalid_amount_format")
            ).when(
                F.col("amount_raw").isNull() | (F.col("amount_raw") == ""),
                F.lit("missing_amount")
            ).otherwise(F.lit(None))
        )
    )
    tracker.log_step("parse_amount", "Parsed amount column with error flags")

    # Step 3: Separate good and bad records
    good_records = parsed_df.filter(F.col("_parse_error").isNull())
    bad_records = parsed_df.filter(F.col("_parse_error").isNotNull())

    good_count = good_records.count()
    bad_count = bad_records.count()

    tracker.log_metric("records_valid", good_count)
    tracker.log_metric("records_rejected", bad_count)
    tracker.log_step("quality_filter", f"{good_count} valid, {bad_count} rejected")

    # Step 4: Apply business transformation on good records
    silver_df = (
        good_records
        .select("order_id", "customer_id", "amount", "product", "order_date")
        .withColumn("processed_at", F.current_timestamp())
    )

    output_count = silver_df.count()
    tracker.log_metric("records_written", output_count)
    tracker.log_step("transform", f"Silver output: {output_count} records")

    # Complete successfully
    run_result = tracker.complete(status="SUCCESS")

except Exception as e:
    run_result = tracker.complete(
        status="FAILED",
        error_message=f"{type(e).__name__}: {str(e)}"
    )
    # In production, you would re-raise after logging:
    # raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### View Run Result

# COMMAND ----------

print("Pipeline Run Summary:")
print("-" * 50)
for key, value in run_result.items():
    print(f"  {key}: {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Dead Letter Table Pattern
# MAGIC
# MAGIC Bad records are preserved with error context for investigation.

# COMMAND ----------

# Create dead letter DataFrame from the bad records identified above
dead_letter_df = (
    bad_records
    .select(
        "order_id",
        "customer_id",
        "amount_raw",
        "product",
        "order_date",
        F.col("_parse_error").alias("error_reason"),
    )
    .withColumn("pipeline_name", F.lit("orders_bronze_to_silver"))
    .withColumn("run_id", F.lit(tracker.run_id))
    .withColumn("quarantined_at", F.current_timestamp())
)

print(f"Dead Letter Records: {dead_letter_df.count()}")
dead_letter_df.show(20, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dead Letter Analysis
# MAGIC
# MAGIC Analyze dead letter records to understand failure patterns.

# COMMAND ----------

dead_letter_summary = (
    dead_letter_df
    .groupBy("error_reason")
    .agg(
        F.count("*").alias("record_count"),
        F.collect_set("product").alias("affected_products"),
    )
    .orderBy(F.desc("record_count"))
)

print("Dead Letter Summary by Error Reason:")
dead_letter_summary.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Error Handling with Retry Pattern

# COMMAND ----------

import time as time_module

def retry_with_backoff(fn, max_retries=3, base_delay=1, logger=None):
    """
    Retry a function with exponential backoff.
    Useful for transient failures (network timeouts, rate limits).
    """
    log = logger or logging.getLogger("retry")
    for attempt in range(max_retries + 1):
        try:
            result = fn()
            if attempt > 0:
                log.info(f"Succeeded on attempt {attempt + 1}")
            return result
        except Exception as e:
            if attempt == max_retries:
                log.error(f"All {max_retries + 1} attempts failed. Last error: {e}")
                raise
            delay = base_delay * (2 ** attempt)
            log.warning(
                f"Attempt {attempt + 1} failed: {type(e).__name__}: {e}. "
                f"Retrying in {delay}s..."
            )
            time_module.sleep(delay)


# Demonstrate retry pattern
call_count = 0

def flaky_function():
    """Simulates a function that fails twice then succeeds."""
    global call_count
    call_count += 1
    if call_count <= 2:
        raise ConnectionError(f"Timeout on attempt {call_count}")
    return "Success!"


call_count = 0
result = retry_with_backoff(flaky_function, max_retries=3, base_delay=0.1, logger=logger)
print(f"Final result: {result}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Simulated Pipeline Run History
# MAGIC
# MAGIC In production, run tracker records would be persisted to a Delta table.
# MAGIC Here we simulate historical data for analysis.

# COMMAND ----------

# Generate 30 days of pipeline run history for 4 pipelines
pipeline_names = [
    "orders_bronze_to_silver",
    "customers_sync",
    "inventory_refresh",
    "analytics_gold_build",
]

run_history = []
for day_offset in range(30):
    run_date = datetime(2024, 6, 1) + timedelta(days=day_offset)
    for pipeline in pipeline_names:
        run_id = str(uuid.uuid4())[:8]
        start = run_date.replace(hour=random.randint(1, 5), minute=random.randint(0, 59))
        duration = random.randint(60, 1800)  # 1-30 minutes in seconds
        end = start + timedelta(seconds=duration)
        records_read = random.randint(10000, 200000)

        status = "SUCCESS"
        error_msg = None
        records_written = int(records_read * random.uniform(0.95, 1.0))
        records_rejected = records_read - records_written

        # 8% failure rate
        if random.random() < 0.08:
            status = "FAILED"
            error_msg = random.choice([
                "ConnectionTimeout: source database unreachable",
                "AnalysisException: column 'legacy_field' not found",
                "OutOfMemoryError: GC overhead limit exceeded",
                "DeltaConcurrentModificationException: version conflict",
            ])
            records_written = 0
            records_rejected = 0
            duration = random.randint(30, 300)
            end = start + timedelta(seconds=duration)

        run_history.append((
            pipeline, run_id, start, end, float(duration),
            status, records_read, records_written, records_rejected, error_msg
        ))

history_schema = StructType([
    StructField("pipeline_name", StringType()),
    StructField("run_id", StringType()),
    StructField("start_time", TimestampType()),
    StructField("end_time", TimestampType()),
    StructField("duration_seconds", DoubleType()),
    StructField("status", StringType()),
    StructField("records_read", IntegerType()),
    StructField("records_written", IntegerType()),
    StructField("records_rejected", IntegerType()),
    StructField("error_message", StringType()),
])

history_df = spark.createDataFrame(run_history, schema=history_schema)
print(f"Pipeline run history: {history_df.count()} runs across {len(pipeline_names)} pipelines")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Run History: Success Rates

# COMMAND ----------

success_rates = (
    history_df
    .groupBy("pipeline_name")
    .agg(
        F.count("*").alias("total_runs"),
        F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("successes"),
        F.sum(F.when(F.col("status") == "FAILED", 1).otherwise(0)).alias("failures"),
        F.avg("duration_seconds").alias("avg_duration_sec"),
        F.sum("records_read").alias("total_records_read"),
        F.sum("records_written").alias("total_records_written"),
    )
    .withColumn("success_rate", F.round(F.col("successes") / F.col("total_runs"), 3))
    .orderBy("pipeline_name")
)

print("Pipeline Success Rates (Last 30 Days):")
success_rates.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Run History: Error Patterns

# COMMAND ----------

error_patterns = (
    history_df
    .filter(F.col("status") == "FAILED")
    .groupBy("error_message")
    .agg(
        F.count("*").alias("occurrences"),
        F.collect_set("pipeline_name").alias("affected_pipelines"),
        F.min("start_time").alias("first_occurrence"),
        F.max("start_time").alias("last_occurrence"),
    )
    .orderBy(F.desc("occurrences"))
)

print("Error Pattern Analysis:")
error_patterns.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Run History: Duration Trends

# COMMAND ----------

from pyspark.sql.window import Window

daily_metrics = (
    history_df
    .filter(F.col("status") == "SUCCESS")
    .withColumn("run_date", F.to_date("start_time"))
    .groupBy("pipeline_name", "run_date")
    .agg(
        F.avg("duration_seconds").alias("avg_duration"),
        F.sum("records_written").alias("total_records"),
    )
)

window_spec = (
    Window
    .partitionBy("pipeline_name")
    .orderBy("run_date")
    .rowsBetween(-6, 0)
)

duration_trends = (
    daily_metrics
    .withColumn("rolling_avg_duration", F.round(F.avg("avg_duration").over(window_spec), 1))
    .withColumn(
        "duration_trend",
        F.when(
            F.col("avg_duration") > F.col("rolling_avg_duration") * 1.5,
            F.lit("SLOW")
        ).when(
            F.col("avg_duration") < F.col("rolling_avg_duration") * 0.5,
            F.lit("FAST")
        ).otherwise(F.lit("NORMAL"))
    )
)

print("Duration Trends (showing anomalies):")
(
    duration_trends
    .filter(F.col("duration_trend") != "NORMAL")
    .orderBy("pipeline_name", "run_date")
    .show(20, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Querying Cluster Logs (Reference)
# MAGIC
# MAGIC When cluster log delivery is configured, logs are stored in cloud storage.
# MAGIC Here is how to query them with Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # Reference: Reading cluster logs from cloud storage
# MAGIC # (requires cluster log delivery to be configured)
# MAGIC
# MAGIC # AWS
# MAGIC log_path = "s3://my-bucket/cluster-logs/<cluster-id>/driver/log4j-active.log"
# MAGIC
# MAGIC # Azure
# MAGIC log_path = "abfss://logs@storage.dfs.core.windows.net/cluster-logs/<cluster-id>/driver/"
# MAGIC
# MAGIC # GCP
# MAGIC log_path = "gs://my-bucket/cluster-logs/<cluster-id>/driver/"
# MAGIC
# MAGIC # Read as text and parse
# MAGIC logs_df = (
# MAGIC     spark.read.text(log_path)
# MAGIC     .withColumn("log_line", F.col("value"))
# MAGIC     .withColumn("is_error", F.col("value").contains("ERROR"))
# MAGIC     .withColumn("is_warning", F.col("value").contains("WARN"))
# MAGIC )
# MAGIC
# MAGIC # Filter for errors
# MAGIC errors = logs_df.filter(F.col("is_error"))
# MAGIC errors.show(20, truncate=False)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC This notebook used only in-memory DataFrames. No tables, temp views, or files were created.

# COMMAND ----------

print("Notebook 04-logging-best-practices complete.")
print(f"Pipeline run tracker demo: {run_result['status']} in {run_result['duration_seconds']:.1f}s")
print(f"Dead letter records captured: {dead_letter_df.count()}")
