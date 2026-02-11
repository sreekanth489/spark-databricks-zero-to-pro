# Databricks Workflows
> Module 06 — Topic 01 | Level: Intermediate-Advanced | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:

1. Create multi-task Databricks Workflows with task dependencies
2. Choose between job clusters and all-purpose clusters for cost optimization
3. Configure scheduling, retries, and alerting for production jobs
4. Pass parameters between tasks using widgets and task values
5. Use the Workflows REST API to manage jobs programmatically

## Conceptual Overview

### What Are Databricks Workflows?

Databricks Workflows is the native orchestration engine built into the platform.
It replaces external schedulers (Airflow, ADF) for many use cases by letting you
define, schedule, and monitor multi-step data pipelines directly in the workspace.

```
  +------------------------------------------------------+
  |              Databricks Workflow (Job)                |
  |                                                      |
  |   +-----------+     +-----------+     +-----------+  |
  |   | Task A    |---->| Task B    |---->| Task D    |  |
  |   | (ingest)  |     | (transform)|    | (publish) |  |
  |   +-----------+     +-----------+     +-----------+  |
  |         |                                  ^         |
  |         |           +-----------+          |         |
  |         +---------->| Task C    |----------+         |
  |                     | (quality) |                    |
  |                     +-----------+                    |
  |                                                      |
  |   Cluster: Job Cluster (auto-terminates)             |
  |   Schedule: Daily 02:00 UTC (cron)                   |
  +------------------------------------------------------+
```

### Task Types

| Task Type | Use Case |
|-----------|----------|
| Notebook | Most common — runs a Databricks notebook |
| Python script | Standalone `.py` files from Repos or DBFS |
| JAR | Java/Scala compiled jobs |
| SQL | Run SQL queries or dashboards |
| dbt | dbt Core transformations |
| Delta Live Tables | Trigger a DLT pipeline |
| Run Job | Chain one workflow into another |

### Dependency Patterns

```
Linear:          A --> B --> C --> D

Fan-out:         A --> B
                 A --> C
                 A --> D

Fan-in:          B --> D
                 C --> D

Diamond:         A --> B --> D
                 A --> C --> D
```

### Cost Optimization: Job Clusters vs All-Purpose Clusters

This is one of the most impactful decisions for production cost:

```
  All-Purpose Cluster              Job Cluster
  +------------------+             +------------------+
  | Runs 24/7 or     |             | Spins up when    |
  | until manually   |             | job starts,      |
  | terminated       |             | terminates when  |
  |                  |             | job completes    |
  | $$$$ per month   |             | $ per run        |
  +------------------+             +------------------+
  Best for: interactive            Best for: scheduled
  development, ad-hoc              production jobs,
  exploration                      automated pipelines
```

**Rule of thumb**: Use all-purpose clusters for development. Use job clusters for
anything that runs on a schedule. The savings can be 60-80% of your compute cost.

As the instructor emphasizes: *"Cost Optimization — How long your servers are running,
how much compute they are using, minimize that."*

## Hands-On Walkthrough

### Step 1: Create a Simple Multi-Task Workflow

Navigate to **Workflows > Create Job** in the Databricks UI.

1. **Name the job**: `etl-daily-sales`
2. **Add Task A — Ingest**:
   - Type: Notebook
   - Path: `/Repos/team/project/01_ingest`
   - Cluster: New Job Cluster (select instance type + Spark version)
3. **Add Task B — Transform** (depends on Task A):
   - Type: Notebook
   - Path: `/Repos/team/project/02_transform`
   - Depends on: `ingest`
4. **Add Task C — Quality Check** (depends on Task A):
   - Type: Notebook
   - Path: `/Repos/team/project/03_quality`
   - Depends on: `ingest`
5. **Add Task D — Publish** (depends on B and C):
   - Type: Notebook
   - Path: `/Repos/team/project/04_publish`
   - Depends on: `transform`, `quality_check`

### Step 2: Configure Scheduling

Use cron syntax for precise control:

| Expression | Meaning |
|------------|---------|
| `0 2 * * *` | Daily at 2:00 AM |
| `0 */6 * * *` | Every 6 hours |
| `0 8 * * 1-5` | Weekdays at 8:00 AM |
| `0 0 1 * *` | First day of each month |

### Step 3: Parameters and Widgets

Pass parameters to notebooks at runtime:

```python
# In the notebook, define a widget
dbutils.widgets.text("run_date", "2024-01-01", "Run Date")
run_date = dbutils.widgets.get("run_date")

# Use it in your query
df = spark.sql(f"""
    SELECT * FROM sales.transactions
    WHERE transaction_date = '{run_date}'
""")
```

In the Workflow UI, set task parameters as key-value pairs.

### Step 4: Task Values (Passing Data Between Tasks)

```python
# In Task A: set a value
dbutils.jobs.taskValues.set(key="row_count", value=df.count())

# In Task B: read the value from Task A
row_count = dbutils.jobs.taskValues.get(
    taskKey="ingest",
    key="row_count",
    default=0
)
```

### Step 5: Retries and Alerts

- **Retries**: Set max retries (e.g., 2) with a delay between attempts
- **Alerts**: Configure email or webhook notifications on:
  - Job start / success / failure
  - Duration exceeds threshold
  - Specific task failures

### Step 6: Workflows REST API

```python
import requests

# List all jobs
response = requests.get(
    f"{workspace_url}/api/2.1/jobs/list",
    headers={"Authorization": f"Bearer {token}"}
)

# Create a job
job_config = {
    "name": "api-created-job",
    "tasks": [
        {
            "task_key": "ingest",
            "notebook_task": {
                "notebook_path": "/Repos/team/project/01_ingest"
            },
            "new_cluster": {
                "spark_version": "14.3.x-scala2.12",
                "num_workers": 2,
                "node_type_id": "i3.xlarge"
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 2 * * ?",
        "timezone_id": "America/New_York"
    }
}
response = requests.post(
    f"{workspace_url}/api/2.1/jobs/create",
    headers={"Authorization": f"Bearer {token}"},
    json=job_config
)
```

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Instance types | i3.xlarge, m5.xlarge | Standard_DS3_v2 | n1-standard-4 |
| Spot/preemptible | Spot Instances | Azure Spot VMs | Preemptible VMs |
| Cluster pools | Supported | Supported | Supported |
| Serverless jobs | Preview | Preview | Preview |

Spot instances on job clusters can reduce cost by an additional 40-70%. Configure
a fallback to on-demand to avoid job failures.

## Certification Tip

The Databricks Data Engineer Associate exam frequently tests:
- Difference between job clusters and all-purpose clusters
- How to configure task dependencies in a multi-task workflow
- When to use retries vs alerts
- Widget parameter passing between notebooks

## Key Takeaways

1. **Workflows** is the native orchestrator — prefer it over external tools when possible
2. **Job clusters** are essential for cost optimization in production
3. **Task dependencies** support linear, fan-out, fan-in, and diamond patterns
4. **Task values** pass data between tasks without writing to storage
5. **The REST API** enables programmatic job management for CI/CD integration

## Next Steps

Proceed to [02 - Delta Live Tables](02-delta-live-tables.md) to learn how to define
declarative data pipelines that can be orchestrated as Workflow tasks.
