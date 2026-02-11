# Monitoring & Alerting
> Module 09 -- Topic 03 | Level: Intermediate | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:
1. Navigate all tabs of the Spark UI and extract actionable insights
2. Identify shuffle bottlenecks, data skew, and spill from stage metrics
3. Monitor streaming queries programmatically using progress and recentProgress
4. Build custom pipeline monitoring dashboards using display/displayHTML
5. Configure alerting thresholds for data pipeline metrics
6. Understand Databricks SQL Alerts and external monitoring integrations

---

## Conceptual Overview

### The Monitoring Stack

Production Spark pipelines require monitoring at multiple layers:

```
  +--------------------------------------------------------------+
  |                    Business Metrics                          |
  |  Record counts, SLA adherence, data freshness, quality      |
  +--------------------------------------------------------------+
                              |
  +--------------------------------------------------------------+
  |                    Application Metrics                       |
  |  Job duration, stage timing, task distribution, error rates  |
  +--------------------------------------------------------------+
                              |
  +--------------------------------------------------------------+
  |                    Infrastructure Metrics                    |
  |  CPU, memory, disk I/O, network, JVM heap, GC pauses        |
  +--------------------------------------------------------------+
                              |
  +--------------------------------------------------------------+
  |                    Cluster Metrics                           |
  |  Node count, autoscaling events, spot terminations, DBU usage|
  +--------------------------------------------------------------+
```

### The Spark UI: Your Primary Debugging Tool

The Spark UI is organized into tabs, each revealing different aspects of
execution:

```
  Spark UI Tabs
  =============

  +----------+   +----------+   +----------+   +----------+   +----------+
  |   Jobs   |   |  Stages  |   | Storage  |   |   SQL    |   |  Environ |
  +----------+   +----------+   +----------+   +----------+   +----------+
  | One row  |   | Tasks per|   | Cached   |   | Query    |   | Config   |
  | per      |   | stage,   |   | RDDs and |   | plans,   |   | values,  |
  | action   |   | shuffle  |   | DataFrames| | execution|   | JARs,    |
  | called   |   | read/    |   | with size|   | DAGs     |   | classpaths|
  |          |   | write,   |   | and      |   |          |   |          |
  |          |   | duration |   | partitions|  |          |   |          |
  +----------+   +----------+   +----------+   +----------+   +----------+
```

#### Jobs Tab
- One entry per Spark action (count, collect, save, show, etc.)
- Shows total duration, number of stages, and number of tasks
- **What to look for**: Jobs that take disproportionately long

#### Stages Tab
- Breaks each job into stages (separated by shuffle boundaries)
- Shows input/output size, shuffle read/write, and task metrics
- **What to look for**:
  - **Data skew**: Large variance in task durations within a stage
  - **Spill**: Memory spill to disk indicates insufficient partition memory
  - **Shuffle**: Large shuffle read/write sizes suggest optimization opportunities

#### Stage Detail: Task Metrics

```
  Task Metric Distribution (Example -- skewed stage)
  ===================================================

  Task Duration:
    Min:    0.1s     <-- Many tasks finish quickly
    25th:   0.3s
    Median: 0.5s
    75th:   1.2s
    Max:    45.2s    <-- One task is 90x slower (SKEW!)

  Input Size:
    Min:    2 MB
    Max:    850 MB   <-- This task got most of the data

  Diagnosis: Data skew on the join key. Consider salting or
  using AQE skew join optimization.
```

#### SQL Tab
- Shows logical and physical plans for SQL queries
- Includes node-level metrics (rows output, time spent)
- **What to look for**: Scan operators reading more data than expected
  (missing predicate pushdown), BroadcastHashJoin vs SortMergeJoin selection

#### Storage Tab
- Shows cached DataFrames/RDDs with size and partition count
- **What to look for**: Unexpectedly large cached data, or cache that should
  have been unpersisted

---

## Streaming Query Monitoring

Structured Streaming provides built-in monitoring through the StreamingQuery
object:

### Key Metrics

| Metric | What It Tells You | Healthy Range |
|--------|-------------------|---------------|
| `inputRowsPerSecond` | Data arrival rate | Stable (no sudden drops) |
| `processedRowsPerSecond` | Processing throughput | >= inputRowsPerSecond |
| `batchDuration` | Time per micro-batch | < trigger interval |
| `numInputRows` | Rows in current batch | Consistent with expectations |
| `stateOperators.numRowsTotal` | State store size | Growing predictably |
| `stateOperators.numRowsDroppedByWatermark` | Late data drops | Low is good |

### Critical Ratio: Input Rate vs Processing Rate

```
  Healthy:     inputRate < processedRate
               Pipeline keeps up; no growing lag

  Warning:     inputRate ~ processedRate
               Pipeline at capacity; no headroom

  Critical:    inputRate > processedRate
               Backlog growing; pipeline falling behind
               Action: scale out, optimize query, increase trigger interval
```

### Programmatic Access

```python
# Get the active streaming query
query = streaming_df.writeStream.format("delta").start(...)

# Access latest progress
progress = query.lastProgress
print(f"Input rate: {progress['inputRowsPerSecond']}")
print(f"Process rate: {progress['processedRowsPerSecond']}")
print(f"Batch duration: {progress['batchDuration']}ms")

# Access recent progress history
for p in query.recentProgress:
    print(p["timestamp"], p["inputRowsPerSecond"])
```

---

## Custom Metrics with Spark Listeners

Spark provides a listener API that fires events on job, stage, and task
completion:

```python
from pyspark import SparkContext

class PipelineMetricsListener(object):
    def onJobEnd(self, jobEnd):
        print(f"Job {jobEnd.jobId()} completed: {jobEnd.jobResult()}")

    def onStageCompleted(self, stageCompleted):
        info = stageCompleted.stageInfo()
        print(f"Stage {info.stageId()} completed in {info.taskMetrics().executorRunTime()}ms")
```

In practice, custom listeners are more commonly used in Scala. Python users
typically access metrics through:
- `spark.sparkContext.statusTracker()` for active job/stage info
- SparkUI REST API at port 4040
- Streaming query progress objects

---

## Databricks SQL Alerts

Databricks SQL Alerts monitor query results and trigger notifications when
conditions are met:

```
  Alert Configuration
  ===================

  1. Write a SQL query that returns a numeric value
     SELECT COUNT(*) as failed_rows
     FROM quality_metrics
     WHERE pass_rate < 0.95 AND check_date = CURRENT_DATE

  2. Set the condition:
     "Alert when value > 0"

  3. Set the schedule:
     "Every 15 minutes"

  4. Configure destinations:
     - Email
     - Slack webhook
     - PagerDuty
     - Microsoft Teams
```

### Alert Query Patterns

| Use Case | Query Template |
|----------|----------------|
| Data freshness | `SELECT TIMESTAMPDIFF(HOUR, MAX(updated_at), NOW()) FROM table` |
| Row count anomaly | `SELECT ABS(today.cnt - avg.cnt) / avg.cnt FROM ...` |
| Quality threshold | `SELECT COUNT(*) FROM checks WHERE pass_rate < threshold` |
| Pipeline failure | `SELECT COUNT(*) FROM run_log WHERE status = 'FAILED' AND date = TODAY` |
| Late arriving data | `SELECT COUNT(*) FROM events WHERE event_time < NOW() - INTERVAL 1 HOUR` |

---

## External Monitoring Integration

### Datadog

```python
from datadog import statsd

# Send custom metrics from your pipeline
statsd.gauge("spark.pipeline.records_processed", record_count, tags=["pipeline:orders"])
statsd.gauge("spark.pipeline.duration_seconds", duration, tags=["pipeline:orders"])
statsd.increment("spark.pipeline.errors", tags=["pipeline:orders", f"error:{error_type}"])
```

### Grafana + Prometheus

Databricks exposes Ganglia metrics that can be scraped by Prometheus. For custom
metrics, push from your pipeline to a Prometheus Pushgateway:

```python
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

registry = CollectorRegistry()
records_gauge = Gauge("pipeline_records_processed", "Records processed", registry=registry)
records_gauge.set(record_count)
push_to_gateway("prometheus-pushgateway:9091", job="spark_pipeline", registry=registry)
```

### PagerDuty

Route critical alerts through PagerDuty for on-call escalation:

```python
import requests

def trigger_pagerduty_alert(routing_key, summary, severity="critical"):
    payload = {
        "routing_key": routing_key,
        "event_action": "trigger",
        "payload": {
            "summary": summary,
            "severity": severity,
            "source": "databricks-pipeline",
        }
    }
    requests.post("https://events.pagerduty.com/v2/enqueue", json=payload)
```

---

## Hands-On Walkthrough

Open `03-monitoring-alerting_notebook.py` to practice:
- Accessing SparkContext metrics programmatically
- Simulating streaming query progress monitoring
- Building a custom monitoring dashboard
- Creating alerting threshold checks
- Databricks SQL alert configuration templates

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Spark UI access | Databricks workspace UI | Databricks workspace UI | Databricks workspace UI |
| Cluster metrics | CloudWatch + Ganglia | Azure Monitor + Ganglia | Cloud Monitoring + Ganglia |
| Log aggregation | CloudWatch Logs, S3 | Log Analytics, ADLS | Cloud Logging, GCS |
| Native alerting | CloudWatch Alarms + SNS | Azure Monitor Alerts | Cloud Monitoring Alerts |
| External integration | Datadog, Grafana, PagerDuty | Datadog, Grafana, PagerDuty | Datadog, Grafana, PagerDuty |
| SQL Alerts | Databricks SQL (all clouds) | Databricks SQL (all clouds) | Databricks SQL (all clouds) |

---

## Certification Tip

> **Databricks Certified Data Engineer Associate**: You should know how to use
> the Spark UI to identify performance bottlenecks. Key concepts:
> - Jobs are created by actions (count, collect, write)
> - Stages are separated by shuffle boundaries (groupBy, join, repartition)
> - Data skew appears as high variance in task durations within a stage
> - Spill to disk indicates memory pressure
>
> For streaming, know how to check whether a query is keeping up with its input
> rate by comparing `inputRowsPerSecond` to `processedRowsPerSecond`.

---

## Key Takeaways

1. **The Spark UI is the number-one debugging tool.** Learn to read the DAG
   visualization, identify shuffle stages, and spot data skew in task metrics.
2. **Monitor at multiple layers**: business metrics (record counts, SLA), application
   metrics (job duration, errors), and infrastructure metrics (CPU, memory).
3. **Streaming queries** expose progress metrics programmatically. The critical
   ratio is input rate versus processing rate -- if input exceeds processing, the
   pipeline is falling behind.
4. **Databricks SQL Alerts** provide a low-code way to monitor query results and
   trigger notifications on schedule.
5. **External tools** (Datadog, Grafana, PagerDuty) integrate through standard
   APIs and are essential for centralized monitoring across multiple pipelines.
6. **Automate alert responses** where possible -- autoscaling, retry logic, and
   automated ticket creation reduce mean time to resolution.

---

## Next Steps

- Proceed to **Topic 04: Logging Best Practices** to learn structured logging,
  pipeline run tracking, and dead letter patterns.
- Revisit Module 05 (Performance Optimization) with your new Spark UI knowledge
  to identify optimization opportunities in your own pipelines.
