# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Workflows — Hands-On Notebook
# MAGIC > Module 06 — Topic 01 | Orchestration & CI/CD
# MAGIC
# MAGIC This notebook demonstrates:
# MAGIC 1. Widget parameters for task configuration
# MAGIC 2. Task values for passing data between workflow tasks
# MAGIC 3. Workflows REST API calls to create, list, and trigger jobs
# MAGIC 4. Job cluster configuration for cost optimization
# MAGIC
# MAGIC **Note**: Some cells contain reference code (REST API calls) that require
# MAGIC a configured workspace URL and access token. Interactive cells are marked
# MAGIC with `[RUNNABLE]`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: Widget Parameters [RUNNABLE]
# MAGIC Widgets are how Workflows passes parameters into notebook tasks at runtime.

# COMMAND ----------

# Define widgets — these appear as input fields at the top of the notebook
dbutils.widgets.text("run_date", "2024-01-15", "Run Date")
dbutils.widgets.dropdown("environment", "dev", ["dev", "staging", "prod"], "Environment")
dbutils.widgets.text("batch_size", "10000", "Batch Size")

# Retrieve widget values
run_date = dbutils.widgets.get("run_date")
environment = dbutils.widgets.get("environment")
batch_size = int(dbutils.widgets.get("batch_size"))

print(f"Run Date    : {run_date}")
print(f"Environment : {environment}")
print(f"Batch Size  : {batch_size}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Simulate an Ingest Task [RUNNABLE]
# MAGIC This simulates what Task A (ingest) would do in a multi-task workflow.

# COMMAND ----------

from pyspark.sql import functions as F

# Simulate ingesting sales data for the given run_date
raw_data = [
    (1, "2024-01-15", "Widget A", 29.99, 10),
    (2, "2024-01-15", "Widget B", 49.99, 5),
    (3, "2024-01-15", "Widget C", 19.99, 25),
    (4, "2024-01-15", "Widget D", 99.99, 2),
    (5, "2024-01-15", "Widget E", 14.99, 50),
]
columns = ["order_id", "order_date", "product", "price", "quantity"]
df_raw = spark.createDataFrame(raw_data, schema=columns)

# Filter by the parameterized run_date
df_filtered = df_raw.filter(F.col("order_date") == run_date)

row_count = df_filtered.count()
print(f"Ingested {row_count} records for {run_date}")
df_filtered.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: Task Values — Passing Data Between Tasks [RUNNABLE]
# MAGIC
# MAGIC `dbutils.jobs.taskValues` lets tasks communicate metadata without writing
# MAGIC to Delta tables. This only works inside a Workflow run. Outside a workflow,
# MAGIC we wrap it in a try/except for safe local testing.

# COMMAND ----------

# Set task values (works inside a Workflow run)
try:
    dbutils.jobs.taskValues.set(key="row_count", value=row_count)
    dbutils.jobs.taskValues.set(key="status", value="success")
    print(f"Task values set: row_count={row_count}, status=success")
except Exception as e:
    print(f"Not running in a Workflow context (expected during interactive use).")
    print(f"In a Workflow, this would set: row_count={row_count}, status=success")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Reading Task Values (Downstream Task) [REFERENCE]
# MAGIC This code would run in Task B, reading values set by Task A.

# COMMAND ----------

# -- REFERENCE CODE (runs in a downstream task) --
# In Task B, read values set by Task A ("ingest" is the task_key of Task A):
#
# upstream_count = dbutils.jobs.taskValues.get(
#     taskKey="ingest",
#     key="row_count",
#     default=0
# )
# upstream_status = dbutils.jobs.taskValues.get(
#     taskKey="ingest",
#     key="status",
#     default="unknown"
# )
# print(f"Upstream ingested {upstream_count} rows with status: {upstream_status}")
#
# if upstream_status != "success" or upstream_count == 0:
#     dbutils.notebook.exit("SKIP: upstream ingest did not succeed")

print("Cell 4 is reference code. See comments above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: Job Cluster vs All-Purpose Cluster Comparison [RUNNABLE]
# MAGIC Cost optimization is critical: job clusters auto-terminate after the run.

# COMMAND ----------

# Cost comparison model
hours_per_day = 24
days_per_month = 30
dbu_rate = 0.55  # $/DBU-hour for Jobs Compute (example rate)
all_purpose_rate = 0.40  # $/DBU-hour for All-Purpose (example rate)
dbu_per_hour = 4  # DBUs consumed by the cluster per hour

# Scenario: Job runs for 2 hours/day
job_hours_per_day = 2

# All-purpose cluster left running 12 hours/day (common anti-pattern)
all_purpose_hours = 12

job_cluster_monthly = job_hours_per_day * days_per_month * dbu_per_hour * dbu_rate
all_purpose_monthly = all_purpose_hours * days_per_month * dbu_per_hour * all_purpose_rate
savings = all_purpose_monthly - job_cluster_monthly
savings_pct = (savings / all_purpose_monthly) * 100

print("=== Monthly Cost Comparison ===")
print(f"Job Cluster (2 hrs/day)         : ${job_cluster_monthly:,.2f}")
print(f"All-Purpose Cluster (12 hrs/day): ${all_purpose_monthly:,.2f}")
print(f"Monthly Savings                 : ${savings:,.2f} ({savings_pct:.0f}%)")
print()
print("Lesson: ALWAYS use job clusters for scheduled production workloads.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: Workflows REST API — List Jobs [REFERENCE]
# MAGIC Use the Jobs API to programmatically manage workflows.

# COMMAND ----------

# -- REFERENCE CODE (requires workspace URL and token) --
#
# import requests
#
# WORKSPACE_URL = "https://<your-workspace>.cloud.databricks.com"
# TOKEN = dbutils.secrets.get(scope="admin", key="api-token")
#
# # List all jobs
# response = requests.get(
#     f"{WORKSPACE_URL}/api/2.1/jobs/list",
#     headers={"Authorization": f"Bearer {TOKEN}"},
#     params={"limit": 25, "offset": 0}
# )
# jobs = response.json().get("jobs", [])
# for job in jobs:
#     print(f"Job ID: {job['job_id']}  Name: {job['settings']['name']}")

print("Cell 6 is reference code. Configure WORKSPACE_URL and TOKEN to run.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Workflows REST API — Create a Job [REFERENCE]
# MAGIC Defining a complete multi-task workflow via the API.

# COMMAND ----------

# -- REFERENCE CODE --
#
# job_config = {
#     "name": "etl-daily-sales",
#     "tags": {"team": "data-engineering", "env": "prod"},
#     "schedule": {
#         "quartz_cron_expression": "0 0 2 * * ?",
#         "timezone_id": "America/New_York",
#         "pause_status": "UNPAUSED"
#     },
#     "tasks": [
#         {
#             "task_key": "ingest",
#             "notebook_task": {
#                 "notebook_path": "/Repos/prod/project/01_ingest",
#                 "base_parameters": {"run_date": "{{start_date}}"}
#             },
#             "new_cluster": {
#                 "spark_version": "14.3.x-scala2.12",
#                 "num_workers": 2,
#                 "node_type_id": "i3.xlarge",
#                 "aws_attributes": {"availability": "SPOT_WITH_FALLBACK"}
#             },
#             "max_retries": 2,
#             "min_retry_interval_millis": 60000
#         },
#         {
#             "task_key": "transform",
#             "depends_on": [{"task_key": "ingest"}],
#             "notebook_task": {
#                 "notebook_path": "/Repos/prod/project/02_transform"
#             },
#             "new_cluster": {
#                 "spark_version": "14.3.x-scala2.12",
#                 "num_workers": 4,
#                 "node_type_id": "i3.xlarge"
#             }
#         },
#         {
#             "task_key": "publish",
#             "depends_on": [{"task_key": "transform"}],
#             "notebook_task": {
#                 "notebook_path": "/Repos/prod/project/03_publish"
#             },
#             "new_cluster": {
#                 "spark_version": "14.3.x-scala2.12",
#                 "num_workers": 2,
#                 "node_type_id": "i3.xlarge"
#             }
#         }
#     ],
#     "email_notifications": {
#         "on_failure": ["team@company.com"],
#         "on_success": ["team@company.com"]
#     }
# }
#
# response = requests.post(
#     f"{WORKSPACE_URL}/api/2.1/jobs/create",
#     headers={"Authorization": f"Bearer {TOKEN}"},
#     json=job_config
# )
# print(f"Created job: {response.json()}")

print("Cell 7 is reference code. See the full job_config in comments above.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Workflows REST API — Trigger a Run [REFERENCE]

# COMMAND ----------

# -- REFERENCE CODE --
#
# # Trigger a one-time run with parameter overrides
# run_response = requests.post(
#     f"{WORKSPACE_URL}/api/2.1/jobs/run-now",
#     headers={"Authorization": f"Bearer {TOKEN}"},
#     json={
#         "job_id": 12345,
#         "notebook_params": {"run_date": "2024-01-20"}
#     }
# )
# run_id = run_response.json()["run_id"]
# print(f"Triggered run: {run_id}")
#
# # Poll run status
# import time
# while True:
#     status_resp = requests.get(
#         f"{WORKSPACE_URL}/api/2.1/jobs/runs/get",
#         headers={"Authorization": f"Bearer {TOKEN}"},
#         params={"run_id": run_id}
#     )
#     state = status_resp.json()["state"]["life_cycle_state"]
#     print(f"  Run {run_id} state: {state}")
#     if state in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
#         break
#     time.sleep(30)

print("Cell 8 is reference code for triggering and monitoring job runs.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Scheduling Cheat-Sheet [RUNNABLE]
# MAGIC Common cron expressions you will use in production.

# COMMAND ----------

cron_examples = [
    ("0 0 2 * * ?",     "Daily at 2:00 AM"),
    ("0 0 */6 * * ?",   "Every 6 hours"),
    ("0 0 8 ? * MON-FRI", "Weekdays at 8:00 AM"),
    ("0 0 0 1 * ?",     "First day of each month at midnight"),
    ("0 30 7 ? * MON",  "Every Monday at 7:30 AM"),
    ("0 0 */2 * * ?",   "Every 2 hours"),
    ("0 0 22 ? * SUN",  "Every Sunday at 10:00 PM"),
]

print(f"{'Cron Expression':<25} {'Description'}")
print("-" * 60)
for expr, desc in cron_examples:
    print(f"{expr:<25} {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Cleanup [RUNNABLE]

# COMMAND ----------

# Remove widgets created in this notebook
dbutils.widgets.removeAll()
print("Cleanup complete. Widgets removed.")
