# Logging Best Practices
> Module 09 -- Topic 04 | Level: Intermediate | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:
1. Configure Python logging for Spark on both driver and executors
2. Implement structured logging with JSON format for machine readability
3. Build a pipeline run tracker with start/end time, records processed, and status
4. Design error handling patterns with try/except and dead letter tables
5. Understand log4j configuration on Databricks clusters
6. Create audit logging for data pipeline operations
7. Query and analyze cluster logs effectively

---

## Conceptual Overview

### Driver vs Executor Logging

The most confusing aspect of logging in Spark is the distributed execution model.
Your code runs in two places, and logging behaves differently in each:

```
  +---------------------------+      +---------------------------+
  |       DRIVER              |      |       EXECUTOR (x N)      |
  +---------------------------+      +---------------------------+
  | - Orchestrates the job    |      | - Runs tasks (partitions) |
  | - Python logging works    |      | - Separate JVM process    |
  |   normally (stdout/stderr)|      | - Python logging goes to  |
  | - log4j controls Spark    |      |   executor stderr (log4j) |
  |   framework messages      |      | - UDF print() goes here   |
  | - print() appears in      |      | - Output visible in Spark |
  |   notebook or driver logs |      |   UI > Executors > stderr  |
  +---------------------------+      +---------------------------+
```

**Key insight**: `print()` inside a UDF or `foreachPartition` runs on executors,
not the driver. The output goes to executor stderr logs, not your notebook.

### What to Log

| Level | What to Log | Example |
|-------|-------------|---------|
| INFO | Pipeline start/end, record counts, checkpoints | "Pipeline orders_silver started. Input: 50,000 rows" |
| WARNING | Unexpected but recoverable conditions | "Column 'legacy_id' missing; using default value" |
| ERROR | Failures that stop a pipeline stage | "Failed to read from source: ConnectionTimeout" |
| DEBUG | Detailed transformation steps (development only) | "Applying dedup on columns: [order_id, timestamp]" |

### What NOT to Log

- Individual row data (privacy risk, log volume explosion)
- Credentials or connection strings
- Full stack traces for expected exceptions
- Metrics that belong in a metrics system (use monitoring instead)

---

## Python Logging Configuration

### Basic Setup for Databricks Notebooks

```python
import logging

# Configure logging for the notebook (driver-side)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

logger = logging.getLogger("my_pipeline")
logger.setLevel(logging.INFO)

# Usage
logger.info("Pipeline started")
logger.warning("Missing column, using default")
logger.error("Failed to process batch", exc_info=True)
```

### Structured Logging (JSON Format)

Structured logs are machine-parseable and work well with log aggregation tools
(Splunk, ELK Stack, Datadog Logs, Azure Log Analytics):

```python
import logging
import json
from datetime import datetime

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "pipeline": getattr(record, "pipeline", "unknown"),
            "run_id": getattr(record, "run_id", "unknown"),
        }
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_entry)

# Attach to handler
handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger = logging.getLogger("pipeline")
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

Output:
```json
{"timestamp": "2024-06-15T10:30:00Z", "level": "INFO", "logger": "pipeline",
 "message": "Pipeline started", "pipeline": "orders_silver", "run_id": "run_20240615_001"}
```

---

## log4j Configuration on Databricks

Spark's internal logging (and JVM-level logging) uses log4j. On Databricks,
configure it through cluster spark config or init scripts:

### Cluster Spark Configuration

```
# In cluster Advanced Options > Spark Config:
spark.driver.extraJavaOptions -Dlog4j.configuration=file:/path/to/log4j.properties
spark.executor.extraJavaOptions -Dlog4j.configuration=file:/path/to/log4j.properties
```

### Common log4j Tuning

```properties
# Reduce noise from chatty libraries
log4j.logger.org.apache.spark=WARN
log4j.logger.org.apache.hadoop=WARN
log4j.logger.org.apache.hive=WARN
log4j.logger.org.apache.kafka=WARN

# Enable debug for your own code
log4j.logger.com.mycompany.pipeline=DEBUG
```

### Databricks Cluster Log Delivery

Databricks can deliver cluster logs to cloud storage:

```
Cluster Config > Advanced Options > Logging
  - Log destination: s3://my-bucket/cluster-logs/ (AWS)
                     abfss://container@storage.dfs.core.windows.net/logs/ (Azure)
                     gs://my-bucket/cluster-logs/ (GCP)
  - Log types: driver logs, executor logs, init script logs
  - Delivery: every 5 minutes
```

---

## Pipeline Run Tracker

A pipeline run tracker table records every execution with metadata for debugging,
SLA monitoring, and trend analysis:

### Schema Design

```
  Pipeline Run Tracker Table
  ==========================

  pipeline_name       STRING      -- e.g., "orders_bronze_to_silver"
  run_id              STRING      -- Unique identifier for this run
  start_time          TIMESTAMP   -- When the run started
  end_time            TIMESTAMP   -- When the run completed (null if running)
  duration_seconds    DOUBLE      -- Computed: end_time - start_time
  status              STRING      -- RUNNING / SUCCESS / FAILED
  records_read        LONG        -- Number of input records
  records_written     LONG        -- Number of output records
  records_rejected    LONG        -- Number of records sent to quarantine
  error_message       STRING      -- Error details (null on success)
  input_path          STRING      -- Source table/path
  output_path         STRING      -- Destination table/path
  spark_job_ids       STRING      -- Comma-separated Spark job IDs
  cluster_id          STRING      -- Databricks cluster ID
  notebook_path       STRING      -- Path to the notebook that ran
```

### Implementation Pattern

```python
class PipelineRunTracker:
    def __init__(self, spark, pipeline_name):
        self.spark = spark
        self.pipeline_name = pipeline_name
        self.run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.start_time = datetime.now()
        self.metrics = {}

    def log_metric(self, key, value):
        self.metrics[key] = value

    def complete(self, status="SUCCESS", error_message=None):
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()
        # Write to run tracker table (or log)
        record = {
            "pipeline_name": self.pipeline_name,
            "run_id": self.run_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "status": status,
            "error_message": error_message,
            **self.metrics
        }
        return record
```

---

## Error Handling Patterns

### Pattern 1: Try/Except with Logging

```python
try:
    result_df = transform(input_df)
    result_df.write.mode("overwrite").saveAsTable("silver.orders")
    tracker.log_metric("records_written", result_df.count())
    tracker.complete(status="SUCCESS")
except AnalysisException as e:
    logger.error(f"Schema or table error: {e}")
    tracker.complete(status="FAILED", error_message=str(e))
    raise
except Exception as e:
    logger.error(f"Unexpected error: {e}", exc_info=True)
    tracker.complete(status="FAILED", error_message=str(e))
    raise
```

### Pattern 2: Dead Letter Table

The dead letter pattern routes records that fail transformation to a separate
table instead of failing the entire pipeline:

```python
def process_with_dead_letter(df, transform_fn, dead_letter_table):
    """Apply a transformation and route failures to a dead letter table."""
    try:
        # Try transformation on all records
        result = transform_fn(df)
        return result
    except Exception:
        # If bulk fails, process row by row (expensive, last resort)
        good_records = []
        bad_records = []
        for row in df.collect():
            try:
                single_df = spark.createDataFrame([row], schema=df.schema)
                transformed = transform_fn(single_df)
                good_records.append(transformed.first())
            except Exception as e:
                bad_records.append((*row, str(e), datetime.now()))
        # Write bad records to dead letter table
        if bad_records:
            write_to_dead_letter(bad_records, dead_letter_table)
        return spark.createDataFrame(good_records)
```

**Better approach**: Use column-level error handling to avoid collect():

```python
def safe_transform(df):
    """Transform with inline error handling -- no collect() needed."""
    return (
        df
        .withColumn("parsed_amount",
            F.when(F.col("raw_amount").rlike(r"^\d+\.?\d*$"),
                   F.col("raw_amount").cast("double"))
            .otherwise(F.lit(None)))
        .withColumn("_parse_error",
            F.when(F.col("parsed_amount").isNull() & F.col("raw_amount").isNotNull(),
                   F.lit("invalid_amount_format"))
            .otherwise(F.lit(None)))
    )
```

### Pattern 3: Retry with Exponential Backoff

```python
import time

def retry_with_backoff(fn, max_retries=3, base_delay=5):
    """Retry a function with exponential backoff."""
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries:
                logger.error(f"All {max_retries} retries exhausted: {e}")
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s")
            time.sleep(delay)
```

---

## Audit Logging

Audit logs answer: "Who did what, when, and to which data?"

### What to Audit

| Event | Fields to Capture |
|-------|-------------------|
| Table write | table_name, write_mode, record_count, user, timestamp |
| Schema change | table_name, old_schema, new_schema, user, timestamp |
| Access grant/revoke | principal, privilege, object, user, timestamp |
| Pipeline run | pipeline_name, status, duration, user, timestamp |
| Data deletion | table_name, filter_condition, rows_deleted, user, timestamp |

### Unity Catalog Audit Logs

Databricks Unity Catalog automatically captures audit events and delivers them
to cloud storage. These can be queried with Spark SQL:

```sql
SELECT
  event_time,
  user_identity.email as user_email,
  action_name,
  request_params.full_name_arg as table_name
FROM system.access.audit
WHERE action_name IN ('createTable', 'deleteTable', 'alterTable')
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
```

---

## Hands-On Walkthrough

Open `04-logging-best-practices_notebook.py` to practice:
- Setting up Python logging in a notebook
- Structured logging with JSON format
- Building a pipeline run tracker
- Implementing error handling with try/except and dead letter pattern
- Querying simulated pipeline run history

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Cluster log delivery | S3 bucket | ADLS Gen2 / Blob Storage | GCS bucket |
| Log aggregation | CloudWatch Logs | Azure Log Analytics | Cloud Logging |
| Audit logs (UC) | Delivered to S3 | Delivered to ADLS | Delivered to GCS |
| Structured log search | CloudWatch Insights | Kusto Query Language (KQL) | Cloud Logging query |
| Log retention | Configurable (S3 lifecycle) | Configurable (retention policy) | Configurable (retention policy) |
| Init script logs | Stored in DBFS/cloud storage | Stored in DBFS/cloud storage | Stored in DBFS/cloud storage |

---

## Certification Tip

> **Databricks Certified Data Engineer Associate**: Expect questions about how
> to handle errors in Spark pipelines without losing data. Key concepts:
> - Dead letter tables preserve failed records for investigation
> - Pipeline metadata tables track run history for SLA monitoring
> - Databricks cluster logs are delivered to cloud storage
> - Unity Catalog audit logs track data access and schema changes
>
> You may also see questions about the difference between driver and executor
> logging, and how to access executor logs through the Spark UI.

---

## Key Takeaways

1. **Driver and executor logging are separate.** Python logging on the driver
   goes to notebook output; logging on executors goes to executor stderr (visible
   in Spark UI or cluster log delivery).
2. **Use structured (JSON) logging** for production pipelines. It enables
   machine-parseable log search and aggregation.
3. **Build a pipeline run tracker** that records every execution with start/end
   time, record counts, status, and error details. This is essential for SLA
   monitoring and debugging.
4. **Use dead letter tables** to preserve failed records instead of failing entire
   pipelines. Column-level error handling avoids expensive collect() operations.
5. **Implement retry with exponential backoff** for transient failures
   (connection timeouts, rate limits, temporary unavailability).
6. **Enable cluster log delivery** to cloud storage for post-mortem analysis.
   Cluster-local logs are lost when the cluster terminates.
7. **Audit logging** answers "who did what, when" -- Unity Catalog provides this
   automatically for all data operations.

---

## Next Steps

- Proceed to **Topic 05: Cost Management** to learn DBU pricing, cluster sizing,
  and optimization strategies.
- Apply the pipeline run tracker pattern to your own pipelines from Modules 04-07.
