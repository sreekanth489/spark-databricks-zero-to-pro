# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 25: Lakeflow Jobs
# MAGIC
# MAGIC **Objective**: Understand Lakeflow Jobs -- the orchestration layer that ties together
# MAGIC Lakeflow Connect (ingestion), Spark Declarative Pipelines (transformation), and any other
# MAGIC task type into reliable, production-grade workflows.
# MAGIC
# MAGIC **Key Insight**: Lakeflow Jobs does not move or transform data itself. It coordinates the
# MAGIC tasks that do -- arranging them as a DAG with dependencies, retries, triggers, and monitoring.
# MAGIC
# MAGIC ```
# MAGIC Lakeflow Platform:
# MAGIC   Connect (Day 22)    --> Ingest data from external sources
# MAGIC   SDP (Day 24)        --> Transform data (Bronze -> Silver -> Gold)
# MAGIC   Jobs (Day 25)       --> Orchestrate everything reliably
# MAGIC ```
# MAGIC
# MAGIC **Platform**: Databricks on AWS with Unity Catalog
# MAGIC
# MAGIC **Prerequisites**:
# MAGIC - [Day 22: Lakeflow Connect](../day22-lakeflow-connect/)
# MAGIC - [Day 24: Lakeflow Spark Declarative Pipelines](../day24-lakeflow-spark-declarative-pipelines/)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 1: Job Concepts Overview
# MAGIC
# MAGIC ### What is a Lakeflow Job?
# MAGIC
# MAGIC A Lakeflow Job is a **multi-task workflow** that runs on Databricks. Each task in the job
# MAGIC performs a specific action (run a notebook, execute SQL, trigger a pipeline), and tasks
# MAGIC are connected by **dependencies** that define the execution order.
# MAGIC
# MAGIC ### Core Components
# MAGIC
# MAGIC | Component | Description |
# MAGIC |---|---|
# MAGIC | **Job** | A named workflow containing one or more tasks |
# MAGIC | **Task** | A single unit of work (notebook, SQL, script, pipeline, etc.) |
# MAGIC | **DAG** | The dependency graph connecting tasks |
# MAGIC | **Run** | A single execution of the job |
# MAGIC | **Trigger** | What starts the job (manual, scheduled, continuous, file arrival) |
# MAGIC | **Cluster** | The compute resource that executes tasks |
# MAGIC
# MAGIC ### Supported Task Types
# MAGIC
# MAGIC | Type | Example |
# MAGIC |---|---|
# MAGIC | Notebook | Data ingestion notebook |
# MAGIC | Python script | Validation script |
# MAGIC | SQL | Revenue aggregation query |
# MAGIC | SDP pipeline | Bronze -> Silver -> Gold |
# MAGIC | JAR | Legacy Java ETL |
# MAGIC | dbt | Analytics models |
# MAGIC | If/Else | Conditional branching |
# MAGIC | For Each | Iterate over regions |
# MAGIC | Run Job | Trigger another job |

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 2: Creating a Job via the UI
# MAGIC
# MAGIC ### Step-by-Step Walkthrough
# MAGIC
# MAGIC 1. Navigate to **Workflows** in the left sidebar
# MAGIC 2. Click **Create Job**
# MAGIC 3. Name the job (e.g., `ecommerce_daily_pipeline`)
# MAGIC 4. Add tasks:
# MAGIC    - **Task 1**: Notebook task for ingestion
# MAGIC    - **Task 2**: SDP pipeline task for transformation (depends on Task 1)
# MAGIC    - **Task 3**: SQL task for reporting (depends on Task 2)
# MAGIC    - **Task 4**: Python script for notification (depends on Task 3)
# MAGIC 5. Configure the trigger (e.g., daily at 2 AM UTC)
# MAGIC 6. Set up notifications (email on failure)
# MAGIC 7. Click **Create**
# MAGIC
# MAGIC ### DAG Visualization
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────────┐     ┌─────────────────┐
# MAGIC │  ingest_orders  │────>│  sdp_pipeline    │
# MAGIC │  (Notebook)     │     │  (SDP Pipeline)  │
# MAGIC └─────────────────┘     └────────┬─────────┘
# MAGIC                                  │
# MAGIC                         ┌────────┴─────────┐
# MAGIC                         │                  │
# MAGIC                  ┌──────▼──────┐    ┌──────▼──────────┐
# MAGIC                  │ daily_report│    │ send_notification│
# MAGIC                  │ (SQL)       │    │ (Python)         │
# MAGIC                  └─────────────┘    └─────────────────┘
# MAGIC ```
# MAGIC
# MAGIC The UI provides a visual DAG editor where you can drag-and-drop tasks and draw
# MAGIC dependency arrows between them.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 3: Multi-Task Job DAG Patterns
# MAGIC
# MAGIC ### Pattern 1: Simple Linear
# MAGIC ```
# MAGIC Task A ──> Task B ──> Task C
# MAGIC ```
# MAGIC Each task runs only after the previous one succeeds.
# MAGIC
# MAGIC ### Pattern 2: Fan-Out / Fan-In
# MAGIC ```
# MAGIC              ┌──> Task B (US) ──┐
# MAGIC Task A ──────├──> Task C (EU) ──├──> Task E (aggregate)
# MAGIC              └──> Task D (AP) ──┘
# MAGIC ```
# MAGIC Tasks B, C, D run in parallel. Task E waits for all three.
# MAGIC
# MAGIC ### Pattern 3: Conditional
# MAGIC ```
# MAGIC              ┌── success ──> Task B (publish)
# MAGIC Task A ──────┤
# MAGIC              └── failure ──> Task C (alert)
# MAGIC ```
# MAGIC Downstream path depends on the outcome of the upstream task.
# MAGIC
# MAGIC ### Pattern 4: For Each
# MAGIC ```
# MAGIC Task A ──> ForEach(regions) ──> Task B(region) ──> Task C
# MAGIC ```
# MAGIC Task B runs once per element in the list, then Task C aggregates.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 4: Creating Jobs Programmatically (Databricks SDK)
# MAGIC
# MAGIC The Databricks SDK for Python (`databricks-sdk`) provides a programmatic interface
# MAGIC for creating, running, and managing jobs. This is the preferred approach for
# MAGIC production deployments.

# COMMAND ----------

# Import the Databricks SDK
# The SDK is pre-installed in Databricks Runtime 13.3+
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    SqlTask,
    TaskDependency,
    CronSchedule,
    JobEmailNotifications,
    PauseStatus,
    Source,
    SqlTaskQuery,
)

# Initialize the workspace client
# When running in a Databricks notebook, authentication is automatic
w = WorkspaceClient()

print("Databricks SDK initialized successfully")
print(f"Workspace URL: {w.config.host}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create a Multi-Task Job
# MAGIC
# MAGIC This example creates a job with four tasks arranged in a DAG:
# MAGIC ```
# MAGIC ingest_orders ──> transform_pipeline ──> daily_report
# MAGIC                                     └──> send_notification
# MAGIC ```

# COMMAND ----------

# Define the job with multiple tasks
job_name = "ecommerce_daily_pipeline_demo"

# Check if a demo job already exists and clean it up
existing_jobs = w.jobs.list(name=job_name)
for job in existing_jobs:
    print(f"Deleting existing demo job: {job.job_id}")
    w.jobs.delete(job_id=job.job_id)

# COMMAND ----------

# Create the multi-task job
created_job = w.jobs.create(
    name=job_name,
    tasks=[
        # Task 1: Ingest orders (notebook task)
        Task(
            task_key="ingest_orders",
            description="Ingest raw order data from S3 into Bronze layer",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/task_ingest_orders",
                source=Source.WORKSPACE,
                base_parameters={
                    "catalog": "ecommerce",
                    "source_path": "s3://ecommerce-lakehouse/raw/orders/",
                },
            ),
            # Use serverless compute
            environment_key="Default",
            max_retries=2,
            min_retry_interval_millis=60000,  # 1 minute between retries
        ),
        # Task 2: Run SDP pipeline (depends on Task 1)
        Task(
            task_key="transform_pipeline",
            description="Run SDP pipeline: Bronze -> Silver -> Gold",
            depends_on=[TaskDependency(task_key="ingest_orders")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/transform_pipeline",
                source=Source.WORKSPACE,
            ),
            environment_key="Default",
        ),
        # Task 3: SQL analytics (depends on Task 2)
        Task(
            task_key="daily_report",
            description="Generate daily revenue report from Gold layer",
            depends_on=[TaskDependency(task_key="transform_pipeline")],
            sql_task=SqlTask(
                warehouse_id="your_warehouse_id",
                query=SqlTaskQuery(
                    query_id="your_query_id",
                ),
            ),
        ),
        # Task 4: Notification (depends on Task 2)
        Task(
            task_key="send_notification",
            description="Send completion notification",
            depends_on=[TaskDependency(task_key="transform_pipeline")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/send_notification",
                source=Source.WORKSPACE,
            ),
            environment_key="Default",
        ),
    ],
    # Email notifications on failure
    email_notifications=JobEmailNotifications(
        on_failure=["data-engineering-team@company.com"],
    ),
    # Tags for organization
    tags={"team": "data-engineering", "domain": "ecommerce", "env": "dev"},
)

print(f"Created job: {created_job.job_id}")
print(f"Job URL: {w.config.host}/#job/{created_job.job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Run the Job and Check Status

# COMMAND ----------

# Trigger a job run (this is a non-blocking call)
# NOTE: This will fail if the notebook paths do not exist.
# In a real scenario, you would deploy the notebooks first.
# Uncomment the following lines to trigger the job:

# run = w.jobs.run_now(job_id=created_job.job_id)
# print(f"Started run: {run.run_id}")

# COMMAND ----------

# List recent runs for the job
runs = w.jobs.list_runs(job_id=created_job.job_id, limit=5)

print(f"Recent runs for job {created_job.job_id}:")
print("-" * 60)
for run in runs:
    print(f"  Run ID: {run.run_id}")
    print(f"  State:  {run.state.life_cycle_state}")
    print(f"  Result: {run.state.result_state}")
    print(f"  Start:  {run.start_time}")
    print()

# COMMAND ----------

# Get detailed info about the job
job_details = w.jobs.get(job_id=created_job.job_id)

print(f"Job Name: {job_details.settings.name}")
print(f"Tasks:    {len(job_details.settings.tasks)}")
print(f"Tags:     {job_details.settings.tags}")
print()
print("Task DAG:")
for task in job_details.settings.tasks:
    deps = [d.task_key for d in (task.depends_on or [])]
    dep_str = f" (depends on: {', '.join(deps)})" if deps else " (root task)"
    print(f"  {task.task_key}{dep_str}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 5: Configuring Triggers
# MAGIC
# MAGIC Jobs can be triggered in four ways: manual, scheduled, continuous, or file arrival.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scheduled Trigger (Cron)
# MAGIC
# MAGIC The most common trigger for batch ETL pipelines.

# COMMAND ----------

from databricks.sdk.service.jobs import CronSchedule, PauseStatus

# Update the job to run daily at 2 AM UTC
w.jobs.update(
    job_id=created_job.job_id,
    new_settings={
        "schedule": CronSchedule(
            quartz_cron_expression="0 0 2 * * ?",  # Daily at 2 AM
            timezone_id="UTC",
            pause_status=PauseStatus.PAUSED,  # Start paused for safety
        ),
    },
)

print("Schedule configured: Daily at 2:00 AM UTC (PAUSED)")
print("Unpause in the UI or via API when ready for production")

# COMMAND ----------

# MAGIC %md
# MAGIC ### File Arrival Trigger
# MAGIC
# MAGIC File arrival triggers start a job when new files land in cloud storage.
# MAGIC This is ideal for event-driven ingestion pipelines.
# MAGIC
# MAGIC ```python
# MAGIC # Example: Trigger when files arrive in S3
# MAGIC from databricks.sdk.service.jobs import FileArrivalTriggerConfiguration
# MAGIC
# MAGIC w.jobs.update(
# MAGIC     job_id=created_job.job_id,
# MAGIC     new_settings={
# MAGIC         "trigger": {
# MAGIC             "file_arrival": FileArrivalTriggerConfiguration(
# MAGIC                 url="s3://ecommerce-lakehouse/raw/orders/",
# MAGIC                 min_time_between_triggers_seconds=300,  # 5 min debounce
# MAGIC                 wait_after_last_change_seconds=60,      # Wait 60s after last file
# MAGIC             ),
# MAGIC         },
# MAGIC     },
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **How it works**:
# MAGIC 1. Databricks monitors the S3 path for new files
# MAGIC 2. When a file arrives, a 60-second timer starts
# MAGIC 3. If more files arrive, the timer resets (debouncing)
# MAGIC 4. After 60 seconds of no new files, the job starts
# MAGIC 5. Minimum 5 minutes between consecutive triggers

# COMMAND ----------

# MAGIC %md
# MAGIC ### Continuous Trigger
# MAGIC
# MAGIC A continuous job restarts immediately after each run completes.
# MAGIC
# MAGIC ```python
# MAGIC from databricks.sdk.service.jobs import Continuous, PauseStatus
# MAGIC
# MAGIC w.jobs.update(
# MAGIC     job_id=created_job.job_id,
# MAGIC     new_settings={
# MAGIC         "continuous": Continuous(
# MAGIC             pause_status=PauseStatus.UNPAUSED,
# MAGIC         ),
# MAGIC     },
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC **Use with caution**: Continuous jobs run 24/7 and incur constant compute costs.
# MAGIC For near-real-time needs, consider using Structured Streaming within an SDP pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 6: Parameters and Widgets
# MAGIC
# MAGIC Parameters let you pass dynamic values to tasks at runtime. Widgets provide an
# MAGIC interactive UI for parameter input in notebooks.

# COMMAND ----------

# Using Databricks widgets to receive parameters from a job
# These create input fields in the notebook UI and accept job parameters

dbutils.widgets.text("run_date", "2025-01-15", "Run Date")
dbutils.widgets.text("catalog", "ecommerce", "Catalog")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")

# Retrieve parameter values
run_date = dbutils.widgets.get("run_date")
catalog = dbutils.widgets.get("catalog")
environment = dbutils.widgets.get("environment")

print(f"Run Date:    {run_date}")
print(f"Catalog:     {catalog}")
print(f"Environment: {environment}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Value References
# MAGIC
# MAGIC When configuring task parameters in a job, you can use dynamic value references
# MAGIC that resolve at runtime:
# MAGIC
# MAGIC | Reference | Resolves To |
# MAGIC |---|---|
# MAGIC | `{{job.id}}` | The job ID |
# MAGIC | `{{job.run_id}}` | The current run ID |
# MAGIC | `{{job.start_time.iso_date}}` | Run start date (YYYY-MM-DD) |
# MAGIC | `{{task.task_key}}` | The current task name |
# MAGIC | `{{job.parameters.run_date}}` | Value of the run_date parameter |
# MAGIC
# MAGIC Example in job configuration:
# MAGIC ```python
# MAGIC Task(
# MAGIC     task_key="daily_report",
# MAGIC     notebook_task=NotebookTask(
# MAGIC         notebook_path="/Workspace/reports/daily_revenue",
# MAGIC         base_parameters={
# MAGIC             "run_date": "{{job.start_time.iso_date}}",
# MAGIC             "job_run_id": "{{job.run_id}}",
# MAGIC         },
# MAGIC     ),
# MAGIC )
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 7: Repair Runs
# MAGIC
# MAGIC Repair runs let you re-execute only the failed tasks in a job run, preserving
# MAGIC the results of successful tasks. This saves significant time and compute cost.

# COMMAND ----------

# MAGIC %md
# MAGIC ### How Repair Runs Work
# MAGIC
# MAGIC ```
# MAGIC Original Run (run_id: 12345):
# MAGIC   Task A (ingest)      -- SUCCESS   (30 min, $2.50)
# MAGIC   Task B (transform)   -- SUCCESS   (45 min, $3.75)
# MAGIC   Task C (aggregate)   -- FAILED    (error at 10 min)
# MAGIC   Task D (report)      -- SKIPPED   (depends on C)
# MAGIC
# MAGIC Repair Run (same run_id: 12345):
# MAGIC   Task A (ingest)      -- SKIPPED   (reuses result)
# MAGIC   Task B (transform)   -- SKIPPED   (reuses result)
# MAGIC   Task C (aggregate)   -- RE-RUN    (after bug fix)
# MAGIC   Task D (report)      -- RE-RUN    (depends on C)
# MAGIC
# MAGIC Savings: 75 min and $6.25 in compute costs
# MAGIC ```

# COMMAND ----------

# Repair a failed run using the SDK
# Replace with an actual run_id from a failed run

# run_id_to_repair = 12345  # Example run ID
# rerun_tasks = ["aggregate_gold", "daily_report"]  # Only failed + downstream

# repaired = w.jobs.repair_run(
#     run_id=run_id_to_repair,
#     rerun_tasks=rerun_tasks,
# )
# print(f"Repair run started: {repaired.repair_id}")

# You can also repair via the UI:
# 1. Go to the job run page
# 2. Click "Repair Run" in the top-right
# 3. Select which tasks to re-run
# 4. Click "Repair"

print("Repair run example (commented out -- requires a real failed run ID)")
print("In the UI: Job Run Page -> Repair Run -> Select Tasks -> Repair")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 8: Databricks Asset Bundles (DAB)
# MAGIC
# MAGIC Databricks Asset Bundles bring CI/CD best practices to Databricks. They let you
# MAGIC define jobs, pipelines, and resources as code and deploy them across environments.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example `databricks.yml`
# MAGIC
# MAGIC ```yaml
# MAGIC # databricks.yml -- main configuration file for a Databricks Asset Bundle
# MAGIC bundle:
# MAGIC   name: ecommerce-pipeline
# MAGIC
# MAGIC # Include resource definitions from separate files
# MAGIC include:
# MAGIC   - resources/*.yml
# MAGIC
# MAGIC # Environment targets
# MAGIC targets:
# MAGIC   dev:
# MAGIC     workspace:
# MAGIC       host: https://dbc-abc123.cloud.databricks.com
# MAGIC     default: true
# MAGIC     variables:
# MAGIC       catalog: ecommerce_dev
# MAGIC       warehouse_id: abc123
# MAGIC
# MAGIC   staging:
# MAGIC     workspace:
# MAGIC       host: https://dbc-def456.cloud.databricks.com
# MAGIC     variables:
# MAGIC       catalog: ecommerce_staging
# MAGIC       warehouse_id: def456
# MAGIC
# MAGIC   prod:
# MAGIC     workspace:
# MAGIC       host: https://dbc-ghi789.cloud.databricks.com
# MAGIC     run_as:
# MAGIC       service_principal_name: prod-sp
# MAGIC     variables:
# MAGIC       catalog: ecommerce
# MAGIC       warehouse_id: ghi789
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Key DAB CLI Commands
# MAGIC
# MAGIC ```bash
# MAGIC # Install the Databricks CLI (if not already installed)
# MAGIC pip install databricks-cli
# MAGIC
# MAGIC # Initialize a new bundle project from a template
# MAGIC databricks bundle init
# MAGIC
# MAGIC # Validate the bundle configuration
# MAGIC databricks bundle validate
# MAGIC
# MAGIC # Deploy resources to the dev environment
# MAGIC databricks bundle deploy --target dev
# MAGIC
# MAGIC # Run a specific job in the dev environment
# MAGIC databricks bundle run ecommerce_pipeline_job --target dev
# MAGIC
# MAGIC # Deploy to production
# MAGIC databricks bundle deploy --target prod
# MAGIC
# MAGIC # Tear down deployed resources
# MAGIC databricks bundle destroy --target dev
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### DAB in CI/CD Pipeline
# MAGIC
# MAGIC ```
# MAGIC Git Push ──> CI Pipeline ──> databricks bundle validate
# MAGIC                          ──> databricks bundle deploy --target staging
# MAGIC                          ──> Run integration tests
# MAGIC                          ──> databricks bundle deploy --target prod
# MAGIC ```
# MAGIC
# MAGIC This ensures every change to your pipeline code goes through validation and
# MAGIC testing before reaching production.

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 9: End-to-End Lakeflow Workflow
# MAGIC
# MAGIC This section brings together all three Lakeflow components into a single workflow:
# MAGIC 1. **Lakeflow Connect** -- Ingest orders from S3
# MAGIC 2. **SDP Pipeline** -- Transform Bronze -> Silver -> Gold
# MAGIC 3. **SQL Analytics** -- Generate daily revenue report
# MAGIC 4. **Notification** -- Send completion alert
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────────────────────────────────────┐
# MAGIC │                    LAKEFLOW JOB: ecommerce_daily                    │
# MAGIC ├──────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                     │
# MAGIC │  ┌─────────────┐     ┌─────────────────┐     ┌─────────────────┐   │
# MAGIC │  │  CONNECT    │     │  SDP PIPELINE    │     │  SQL REPORT     │   │
# MAGIC │  │  (ingest)   │────>│  (transform)     │────>│  (analytics)    │   │
# MAGIC │  │             │     │                  │     │                 │   │
# MAGIC │  │  S3 orders  │     │  Bronze->Silver  │     │  Daily revenue  │   │
# MAGIC │  │  -> Bronze  │     │  ->Gold tables   │     │  by store       │   │
# MAGIC │  └─────────────┘     └─────────────────┘     └────────┬────────┘   │
# MAGIC │                                                       │            │
# MAGIC │                                              ┌────────▼────────┐   │
# MAGIC │                                              │  NOTIFICATION   │   │
# MAGIC │                                              │  (Python)       │   │
# MAGIC │                                              │                 │   │
# MAGIC │                                              │  Email + Slack  │   │
# MAGIC │                                              └─────────────────┘   │
# MAGIC │                                                                     │
# MAGIC │  Trigger: File arrival on s3://ecommerce-lakehouse/raw/orders/      │
# MAGIC │  Retry:   2 attempts per task, 60s backoff                          │
# MAGIC │  Notify:  Email on failure, Slack on success                        │
# MAGIC └──────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# Build the end-to-end Lakeflow job programmatically
from databricks.sdk.service.jobs import (
    Task,
    NotebookTask,
    SqlTask,
    TaskDependency,
    JobEmailNotifications,
    Source,
    SqlTaskQuery,
    PipelineTask,
)

e2e_job_name = "ecommerce_daily_e2e_demo"

# Clean up any existing demo job
existing = w.jobs.list(name=e2e_job_name)
for j in existing:
    w.jobs.delete(job_id=j.job_id)

# Create the end-to-end job
e2e_job = w.jobs.create(
    name=e2e_job_name,
    tasks=[
        # Task 1: Ingest orders from S3 (simulates Lakeflow Connect)
        Task(
            task_key="ingest_orders",
            description="Ingest raw orders from S3 into ecommerce.bronze.orders",
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/task_ingest_orders",
                source=Source.WORKSPACE,
                base_parameters={
                    "source_path": "s3://ecommerce-lakehouse/raw/orders/",
                    "target_table": "ecommerce.bronze.orders",
                },
            ),
            environment_key="Default",
            max_retries=2,
            min_retry_interval_millis=60000,
        ),
        # Task 2: SDP Pipeline (Bronze -> Silver -> Gold)
        # In production, use PipelineTask to reference an SDP pipeline by ID
        Task(
            task_key="transform_pipeline",
            description="Run SDP pipeline: Bronze -> Silver -> Gold",
            depends_on=[TaskDependency(task_key="ingest_orders")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/transform_pipeline",
                source=Source.WORKSPACE,
            ),
            environment_key="Default",
            max_retries=1,
        ),
        # Task 3: Daily revenue report (SQL)
        Task(
            task_key="daily_revenue_report",
            description="Generate daily revenue by store from Gold layer",
            depends_on=[TaskDependency(task_key="transform_pipeline")],
            sql_task=SqlTask(
                warehouse_id="your_warehouse_id",
                query=SqlTaskQuery(query_id="your_query_id"),
            ),
        ),
        # Task 4: Send notification
        Task(
            task_key="send_completion_alert",
            description="Notify team of pipeline completion via email and Slack",
            depends_on=[TaskDependency(task_key="daily_revenue_report")],
            notebook_task=NotebookTask(
                notebook_path="/Workspace/Users/your-user/day25/send_notification",
                source=Source.WORKSPACE,
                base_parameters={
                    "channel": "#data-engineering",
                    "message": "E-commerce daily pipeline completed successfully",
                },
            ),
            environment_key="Default",
        ),
    ],
    email_notifications=JobEmailNotifications(
        on_failure=["data-engineering-team@company.com"],
        on_success=["data-engineering-team@company.com"],
    ),
    tags={"team": "data-engineering", "domain": "ecommerce", "pipeline": "daily"},
)

print(f"Created end-to-end job: {e2e_job.job_id}")
print(f"Job URL: {w.config.host}/#job/{e2e_job.job_id}")

# COMMAND ----------

# Display the job's task DAG
e2e_details = w.jobs.get(job_id=e2e_job.job_id)

print("=" * 60)
print(f"Job: {e2e_details.settings.name}")
print("=" * 60)
print()
print("Task DAG:")
print("-" * 40)
for task in e2e_details.settings.tasks:
    deps = [d.task_key for d in (task.depends_on or [])]
    dep_str = f" -> depends on [{', '.join(deps)}]" if deps else " (root)"
    retry_str = f" (retries: {task.max_retries})" if task.max_retries else ""
    print(f"  {task.task_key}{dep_str}{retry_str}")
print()
print(f"Tags: {e2e_details.settings.tags}")
print(f"Notifications: on_failure -> {e2e_details.settings.email_notifications.on_failure}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Section 10: Monitoring with System Tables
# MAGIC
# MAGIC Databricks provides system tables for programmatic monitoring of job runs.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View recent job runs from system tables
# MAGIC -- Note: system.lakeflow tables require admin access
# MAGIC -- Uncomment the following query in your workspace:

# MAGIC -- SELECT
# MAGIC --     job_id,
# MAGIC --     run_id,
# MAGIC --     result_state,
# MAGIC --     ROUND((end_time - start_time) / 1000 / 60, 1) AS duration_minutes
# MAGIC -- FROM system.lakeflow.job_run_timeline
# MAGIC -- WHERE start_time > CURRENT_TIMESTAMP() - INTERVAL 7 DAYS
# MAGIC -- ORDER BY start_time DESC
# MAGIC -- LIMIT 20;

# MAGIC SELECT 'System table query shown above -- uncomment in your workspace' AS note

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove the demo jobs created in this notebook.

# COMMAND ----------

# Delete demo jobs
for job_name in ["ecommerce_daily_pipeline_demo", "ecommerce_daily_e2e_demo"]:
    jobs = w.jobs.list(name=job_name)
    for job in jobs:
        w.jobs.delete(job_id=job.job_id)
        print(f"Deleted job: {job_name} (ID: {job.job_id})")

# Remove widgets
dbutils.widgets.removeAll()

print("Cleanup complete")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC 1. **Lakeflow Jobs orchestrates everything** -- Connect, SDP, SQL, Python, dbt, and more
# MAGIC 2. **Multi-task DAGs** support linear, fan-out/fan-in, conditional, and for-each patterns
# MAGIC 3. **Repair runs** save time and money by re-running only failed tasks
# MAGIC 4. **Four trigger modes**: manual, scheduled (cron), continuous, file arrival
# MAGIC 5. **Databricks SDK** enables programmatic job management (create, run, monitor, repair)
# MAGIC 6. **Databricks Asset Bundles** bring CI/CD to job deployment across environments
# MAGIC 7. **Parameters and widgets** make jobs flexible and reusable
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC - [Day 26: Performance Engineering](../day26-performance-engineering/)
