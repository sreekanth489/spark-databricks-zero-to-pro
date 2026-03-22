# Lakeflow Jobs
> Module: Data Engineering Orchestration | Day 24 | Level: Intermediate | Time: 60 min

![Lakeflow Jobs: Smart orchestration with triggers, control flow, observability, and serverless compute](images/lakeflow-jobs.png)
<p align="center"><em>Image credit: <a href="https://www.databricks.com/product/lakeflow">Databricks</a></em></p>

## Learning Objectives

After completing this session, you will be able to:
- Explain where Lakeflow Jobs fits in the Lakeflow ecosystem (Connect, SDP, Jobs)
- Design multi-task jobs with sequential, parallel, and conditional DAG patterns
- Choose the right trigger mode for each workload (manual, scheduled, continuous, file arrival)
- Configure retry policies, repair runs, and notifications for production reliability
- Deploy jobs across environments using Databricks Asset Bundles (DAB)
- Apply RBAC permissions and cluster strategies to optimize cost and security

---

## Conceptual Overview

### The Lakeflow Ecosystem

Lakeflow is Databricks' unified platform for data engineering. It has three components, each handling a distinct phase of the data lifecycle:

![Lakeflow Ecosystem: Connect → Spark Declarative Pipelines → Jobs](images/lakeflow-ecosystem.excalidraw.png)

> The Excalidraw source file is available at [`images/lakeflow-ecosystem.excalidraw`](images/lakeflow-ecosystem.excalidraw) for editing.

| Component | Day | Role |
|-----------|-----|------|
| **Lakeflow Connect** | Day 22 | "Get the data in" -- ingest from databases, SaaS, files, APIs |
| **Spark Declarative Pipelines** | Day 23 | "Make the data right" -- transform Bronze → Silver → Gold |
| **Lakeflow Jobs** | Day 24 | "Run it all reliably" -- orchestrate the entire workflow |

**Lakeflow Jobs** is the orchestration layer. It does not move or transform data itself -- it coordinates the tasks that do. A single job can combine:
- A Lakeflow Connect ingestion task
- An SDP pipeline for transformation
- A SQL query for analytics
- A Python script for notifications
- A dbt model for downstream consumers

### What Makes Lakeflow Jobs Different?

Traditional schedulers (Airflow, cron, ADF) require you to manage infrastructure, define task serialization, and build retry logic. Lakeflow Jobs provides:

| Capability | Traditional Scheduler | Lakeflow Jobs |
|---|---|---|
| Cluster management | You provision and manage | Serverless or job clusters auto-managed |
| Retry logic | You code it | Built-in per-task retry policies |
| Failure recovery | Re-run entire pipeline | Repair runs: re-run only failed tasks |
| DAG definition | Code-based (Python/YAML) | UI + code + DAB |
| Trigger modes | Cron only (usually) | Cron, continuous, file arrival, manual |
| Monitoring | External dashboards | Built-in run history, metrics, alerts |
| CI/CD | Custom scripts | Databricks Asset Bundles |

---

## Multi-Task Job Architecture

A multi-task job is a collection of tasks arranged as a DAG. Each task runs a specific workload type.

### Supported Task Types

| Task Type | Use Case | Example |
|---|---|---|
| **Notebook** | Interactive data processing | Bronze ingestion notebook |
| **Python script** | Standalone scripts | Data validation script |
| **SQL** | Analytics queries | Daily revenue aggregation |
| **SDP pipeline** | Declarative transformations | Bronze -> Silver -> Gold |
| **JAR** | Java/Scala applications | Legacy ETL jobs |
| **dbt** | dbt model runs | Downstream analytics |
| **Spark Submit** | Custom Spark applications | ML training jobs |
| **Run Job** | Trigger another job | Cross-team dependencies |
| **If/Else condition** | Conditional branching | Route based on data quality |
| **For Each** | Parameterized iteration | Process each region separately |

### DAG Patterns

#### Pattern 1: Simple Linear

```
Task A ──> Task B ──> Task C
(ingest)   (transform) (report)
```

Use when: Steps must run strictly in order. Each depends on the output of the previous.

#### Pattern 2: Fan-Out / Fan-In

```
                ┌──> Task B (US orders) ──┐
                │                         │
Task A ────────>├──> Task C (EU orders) ──├──> Task E
(ingest all)    │                         │    (aggregate)
                └──> Task D (APAC orders)─┘
```

Use when: Independent processing can run in parallel, followed by a consolidation step.

#### Pattern 3: Conditional

```
                 ┌── success ──> Task B (publish report)
Task A ──────────┤
(data quality)   └── failure ──> Task C (send alert)
```

Use when: Downstream behavior depends on the outcome of a previous task.

#### Pattern 4: Mixed (End-to-End Lakeflow)

```
Task 1: Connect          Task 2: SDP Pipeline
(ingest from S3)  ──────> (bronze->silver->gold)
                                    │
                          ┌─────────┴─────────┐
                          │                   │
                    Task 3: SQL           Task 4: Python
                    (daily report)        (send notification)
```

This is the most common production pattern -- combining all three Lakeflow components.

---

## Trigger Modes

| Mode | When it runs | Best for | Configuration |
|---|---|---|---|
| **Manual** | On-demand via UI, API, or CLI | Ad-hoc analysis, testing | No trigger config needed |
| **Scheduled** | Cron-based schedule | Nightly ETL, hourly refreshes | Cron expression + timezone |
| **Continuous** | Immediately after previous run ends | Near-real-time processing | `continuous` flag |
| **File Arrival** | New files land in cloud storage | Event-driven ingestion | S3/ADLS/GCS path + wait time |

### Scheduled Triggers (Cron)

```
# Every day at 2 AM UTC
0 0 2 * * ?

# Every hour on weekdays
0 0 * * * 1-5

# Every 15 minutes
0 */15 * * * ?
```

### File Arrival Triggers

File arrival triggers watch a cloud storage path for new files. When files appear, the job starts automatically.

```
Trigger Configuration:
  Path:       s3://ecommerce-lakehouse/raw/orders/
  Min files:  1
  Wait time:  60 seconds (debounce period)
```

The wait time prevents the job from triggering on every single file during a bulk upload. After the first file arrives, Databricks waits for the specified duration before starting the job.

### Continuous Triggers

A continuous job starts a new run immediately after the previous run completes. This provides near-real-time processing without the complexity of streaming.

```
Run 1 starts ──> Run 1 ends ──> Run 2 starts ──> Run 2 ends ──> ...
  (5 min)          (immediate)    (5 min)          (immediate)
```

**Warning**: Continuous jobs incur compute costs 24/7. Use streaming within SDP for true real-time needs.

---

## Cluster Strategies

| Strategy | Description | Cost | Use Case |
|---|---|---|---|
| **Job cluster** | Created for the job run, terminated after | Low | Production batch jobs |
| **Shared job cluster** | One cluster shared across tasks in a job | Medium | Multi-task jobs with similar compute needs |
| **All-purpose cluster** | Existing interactive cluster | High | Development and debugging |
| **Serverless** | Databricks manages everything | Variable | No cluster management overhead |

### Shared Job Clusters

Multiple tasks can share a single job cluster, reducing startup time and cost:

```
Job: ecommerce_daily_pipeline
├── Cluster Pool: "shared_etl"
├── Task 1: ingest_orders      (uses shared_etl)
├── Task 2: ingest_customers   (uses shared_etl)
├── Task 3: transform_silver   (uses shared_etl)
└── Task 4: build_gold         (uses separate ML cluster)
```

---

## Retry Policies and Error Handling

### Per-Task Retry Configuration

Each task can have its own retry policy:

```python
retry_policy = {
    "max_retries": 3,              # Number of retry attempts
    "min_retry_interval_millis": 30000,   # 30 seconds between retries
    "unlimited_retries": False
}
```

### Repair Runs

Repair runs are one of the most powerful features of Lakeflow Jobs. When a job fails partway through, you can re-run only the failed tasks:

```
Original Run:
  Task A (ingest)      -- SUCCESS  (took 30 min)
  Task B (transform)   -- SUCCESS  (took 45 min)
  Task C (aggregate)   -- FAILED   (error after 10 min)
  Task D (report)      -- SKIPPED  (depends on Task C)

Repair Run:
  Task A (ingest)      -- SKIPPED  (reuses previous result)
  Task B (transform)   -- SKIPPED  (reuses previous result)
  Task C (aggregate)   -- RE-RUN   (after fixing the issue)
  Task D (report)      -- RE-RUN   (depends on Task C)
```

This saves significant compute time and cost. Without repair runs, you would need to re-run the entire 85-minute pipeline.

---

## Parameters and Widgets

### Job Parameters

Job-level parameters are passed to all tasks:

```json
{
    "run_date": "2025-01-15",
    "environment": "production",
    "catalog": "ecommerce"
}
```

### Task Parameters

Task-level parameters override job parameters for specific tasks:

```json
{
    "task_key": "ingest_orders",
    "parameters": {
        "source_path": "s3://ecommerce-lakehouse/raw/orders/",
        "batch_size": "10000"
    }
}
```

### Dynamic Value References

Lakeflow Jobs supports dynamic values that resolve at runtime:

| Reference | Resolves To |
|---|---|
| `{{job.id}}` | The job ID |
| `{{job.run_id}}` | The current run ID |
| `{{job.start_time.iso_date}}` | Run start date (YYYY-MM-DD) |
| `{{task.task_key}}` | The current task name |
| `{{job.parameters.run_date}}` | Value of the run_date parameter |

### Databricks Widgets

In notebooks, use widgets to receive parameters:

```python
dbutils.widgets.text("run_date", "2025-01-01", "Run Date")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"])

run_date = dbutils.widgets.get("run_date")
environment = dbutils.widgets.get("environment")
```

---

## Notifications

### Notification Types

| Channel | Configuration | Use Case |
|---|---|---|
| **Email** | Email addresses | Team alerts |
| **Slack** | Webhook URL | Channel notifications |
| **Webhook** | HTTP endpoint | Custom integrations (PagerDuty, Opsgenie) |
| **System** | Databricks notifications | In-platform alerts |

### Notification Events

You can configure notifications for:
- `on_start` -- job run started
- `on_success` -- job run completed successfully
- `on_failure` -- job run failed
- `on_duration_warning_threshold_exceeded` -- run exceeds expected duration

---

## RBAC and Permissions

| Permission | Can do |
|---|---|
| **No permissions** | Nothing |
| **Can View** | See job configuration and run history |
| **Can Manage Run** | View + trigger runs, cancel runs |
| **Can Manage** | Full control: edit, delete, change permissions |
| **Is Owner** | Manage + transfer ownership |

Best practices:
- Data engineers: **Can Manage** on their team's jobs
- Data analysts: **Can View** on upstream jobs they depend on
- Service principals: **Is Owner** for production jobs (not personal accounts)

---

## Databricks Asset Bundles (DAB)

### What Are DABs?

Databricks Asset Bundles are a CI/CD tool for deploying Databricks resources (jobs, pipelines, notebooks) across environments. They use a YAML configuration file (`databricks.yml`) and the Databricks CLI.

### Why DABs?

| Without DAB | With DAB |
|---|---|
| Manual job creation in UI | Jobs defined as code |
| Copy/paste between environments | Automated deployment across dev/staging/prod |
| No version control for job configs | Full git history for all configurations |
| Error-prone manual updates | Validated before deployment |

### DAB Project Structure

```
my-project/
├── databricks.yml              # Main configuration
├── resources/
│   └── job_config.yml          # Job definitions
├── src/
│   ├── ingest_orders.py        # Task notebooks/scripts
│   ├── transform_pipeline.py
│   └── daily_report.sql
└── tests/
    └── test_pipeline.py
```

### Key CLI Commands

```bash
# Initialize a new bundle project
databricks bundle init

# Validate configuration
databricks bundle validate

# Deploy to a target environment
databricks bundle deploy --target dev
databricks bundle deploy --target prod

# Run a job defined in the bundle
databricks bundle run ecommerce_pipeline_job --target dev

# Destroy deployed resources
databricks bundle destroy --target dev
```

### Environment Targets

```yaml
# databricks.yml
targets:
  dev:
    workspace:
      host: https://dbc-abc123.cloud.databricks.com
    default: true
  staging:
    workspace:
      host: https://dbc-def456.cloud.databricks.com
  prod:
    workspace:
      host: https://dbc-ghi789.cloud.databricks.com
    run_as:
      service_principal_name: "prod-service-principal"
```

---

## Monitoring and Observability

### Run History

Every job maintains a full run history with:
- Start time, duration, and status for each task
- Cluster utilization and cost metrics
- Error messages and stack traces for failed tasks
- Repair run lineage (which tasks were repaired)

### System Tables

Databricks provides system tables for programmatic monitoring:

```sql
-- Recent job runs with duration and status
SELECT
    job_id,
    run_id,
    result_state,
    ROUND((end_time - start_time) / 1000 / 60, 1) AS duration_minutes
FROM system.lakeflow.job_run_timeline
WHERE start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
ORDER BY start_time DESC;
```

### Health Rules

Configure health rules to detect degradation:
- **Duration threshold**: Alert if a run takes longer than expected
- **Consecutive failures**: Alert after N consecutive failures
- **Cost threshold**: Alert if compute cost exceeds budget

---

## Hands-On Walkthrough

See the companion notebook [`25-lakeflow-jobs_notebook.py`](25-lakeflow-jobs_notebook.py) for interactive exercises covering:
1. Creating jobs programmatically via the Databricks SDK
2. Building multi-task DAGs with fan-out/fan-in patterns
3. Configuring triggers (scheduled, file arrival)
4. Passing parameters to tasks
5. Repair runs for failed tasks
6. End-to-end Lakeflow workflow (Connect + SDP + Jobs)

See [`lab-scripts/`](lab-scripts/) for production-ready examples:
- `create_job_api.py` -- create a multi-task job via the SDK
- `databricks_bundle/` -- complete DAB configuration for CI/CD
- `task_ingest_orders.py` -- sample ingestion task
- `task_daily_report.sql` -- sample SQL analytics task

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---|---|---|---|
| File arrival trigger path | `s3://bucket/path/` | `abfss://container@storage.dfs.core.windows.net/path/` | `gs://bucket/path/` |
| Serverless compute | GA | GA | GA |
| Notification integration | SNS, SES, Slack | Azure Monitor, Slack | Pub/Sub, Slack |
| DAB CLI | `databricks bundle deploy` | Same | Same |
| Service principal for prod | IAM role / SP | Azure AD SP | GCP SA |
| System tables | `system.lakeflow.*` | Same | Same |

---

## Certification Tip

Lakeflow Jobs (still referenced as "Workflows" or "Jobs" in many exam questions) is tested on the **Databricks Certified Data Engineer Professional** exam. Focus on:

1. **DAG design**: Know when to use fan-out/fan-in vs linear vs conditional patterns
2. **Repair runs**: Understand that only failed and downstream tasks are re-executed
3. **Trigger modes**: Be able to match scenarios to the correct trigger (e.g., "files land hourly" = file arrival trigger)
4. **Cluster strategies**: Job clusters for production (cost), all-purpose for dev (convenience), serverless for simplicity
5. **DAB**: Know the purpose and basic commands (`bundle validate`, `bundle deploy`, `bundle run`)
6. **Parameters**: Understand job vs task parameters and dynamic value references

---

## Key Takeaways

1. **Lakeflow Jobs is the orchestration layer** -- it coordinates Connect (ingestion), SDP (transformation), and any other task type into reliable production workflows
2. **Multi-task DAGs** support sequential, parallel, conditional, and for-each patterns
3. **Repair runs** save compute by re-running only failed tasks, not the entire pipeline
4. **Four trigger modes** cover every scheduling need: manual, cron, continuous, and file arrival
5. **Databricks Asset Bundles** bring CI/CD best practices to job deployment
6. **Serverless compute** eliminates cluster management for simpler operations

---

## Next Steps

- [Day 26: Performance Engineering](../day26-performance-engineering/) -- optimize Spark job performance with partitioning, caching, and adaptive query execution
