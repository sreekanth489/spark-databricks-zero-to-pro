# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC # Day 31: Databricks Asset Bundles — DevOps for Data Engineers
# MAGIC
# MAGIC **Objective**: Understand Databricks Asset Bundles (DAB) end-to-end through hands-on exploration
# MAGIC
# MAGIC In this lab we will:
# MAGIC 1. Feel the pain of manual deployments (the world before DAB)
# MAGIC 2. Walk through a complete `databricks.yml` anatomy — every section, explained
# MAGIC 3. Use the Databricks SDK to deploy and manage jobs programmatically (what DAB does under the hood)
# MAGIC 4. Simulate environment-specific configuration (dev vs prod settings)
# MAGIC 5. Inspect deployed resources and their state via the API
# MAGIC 6. Build a full CI/CD pipeline pattern using DAB commands
# MAGIC
# MAGIC **Note on DAB and Notebooks**:
# MAGIC DAB is a CLI tool (`databricks bundle deploy`) — it runs outside Databricks.
# MAGIC This notebook teaches the *concepts and mechanics* of DAB by using the Databricks
# MAGIC SDK (available in DBR 13.3+) to interact with the same APIs that DAB uses internally.
# MAGIC The notebook also contains the exact YAML you would put in `databricks.yml`.
# MAGIC
# MAGIC **Platform**: Databricks Runtime 13.3+ (for Databricks SDK)
# MAGIC **Prerequisites**: `CREATE` privilege on a catalog schema

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Setup

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs, compute, iam
from databricks.sdk.service.jobs import (
    Task, NotebookTask, PythonWheelTask, JobCluster,
    JobSettings, CronSchedule, JobEmailNotifications,
    RunNow, SparkPythonTask
)
import json, time, textwrap
from datetime import datetime

# WorkspaceClient uses the current cluster's auth automatically
w = WorkspaceClient()

# Workspace info
me = w.current_user.me()
WORKSPACE_HOST = spark.conf.get("spark.databricks.workspaceUrl")

print(f"Workspace : https://{WORKSPACE_HOST}")
print(f"User      : {me.user_name}")
print(f"SDK ready : {w is not None}")

# COMMAND ----------

CATALOG = spark.sql("SELECT current_catalog()").collect()[0][0]
SCHEMA  = "day31_devops_lab"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

print(f"Working catalog/schema: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 1: The Pain of Manual Deployments
# MAGIC
# MAGIC Before DAB, teams managed Databricks job configurations either via the UI
# MAGIC or by writing custom REST API scripts. Let's recreate both approaches to
# MAGIC understand exactly what problem DAB solves.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.1 Manual Approach: Raw REST API JSON
# MAGIC
# MAGIC Below is what a team would maintain to create ONE job in prod.
# MAGIC This JSON would live in a file like `prod-job-config.json`.
# MAGIC A parallel `dev-job-config.json` would be nearly identical but with different
# MAGIC cluster sizes and schedules — maintained manually in sync.

# COMMAND ----------

# This is what the "before DAB" world looked like:
# A raw JSON payload for the /api/2.1/jobs/create endpoint
manual_job_config = {
    "name": "ecommerce-ingestion-prod",
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/main/my-pipeline/src/bronze_ingestion",
                "base_parameters": {
                    "catalog": "prod_catalog",
                    "env": "prod"
                }
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.4xlarge",   # HARDCODED: different from dev (i3.xlarge)
                "num_workers": 8,                 # HARDCODED: different from dev (2)
                "spark_conf": {
                    "spark.databricks.delta.optimizeWrite.enabled": "true"
                }
            }
        },
        {
            "task_key": "transform_silver",
            "depends_on": [{"task_key": "ingest_bronze"}],
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/main/my-pipeline/src/silver_transform",
                "base_parameters": {
                    "catalog": "prod_catalog"
                }
            },
            "job_cluster_key": "shared_cluster"
        }
    ],
    "job_clusters": [
        {
            "job_cluster_key": "shared_cluster",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.4xlarge",     # HARDCODED again
                "num_workers": 4
            }
        }
    ],
    "schedule": {
        "quartz_cron_expression": "0 0 1 * * ?",   # HARDCODED: nightly at 1am in prod
        "timezone_id": "America/Los_Angeles",
        "pause_status": "UNPAUSED"
    },
    "email_notifications": {
        "on_failure": ["ops-team@company.com"]
    }
}

# A second file would exist for dev — nearly identical, with different values:
manual_job_config_dev = {
    "name": "ecommerce-ingestion-dev",
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "notebook_task": {
                "notebook_path": "/Workspace/Repos/dev/my-pipeline/src/bronze_ingestion",
                "base_parameters": {
                    "catalog": "dev_catalog",
                    "env": "dev"
                }
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",    # ← manually kept in sync with prod
                "num_workers": 2,               # ← manually kept in sync with prod
            }
        }
        # ... rest of config duplicated ...
    ]
}

print("PROBLEM ANALYSIS:")
print("=" * 60)
prod_keys = set(json.dumps(manual_job_config, sort_keys=True).split())
dev_keys  = set(json.dumps(manual_job_config_dev, sort_keys=True).split())

print(f"Prod config tokens: {len(prod_keys)}")
print(f"Dev config tokens : {len(dev_keys)}")
print()
print("Pain points with this approach:")
problems = [
    "1. Two near-identical JSON files to maintain in sync",
    "2. Cluster sizes and schedules are hardcoded strings scattered everywhere",
    "3. No validation — a typo in node_type_id fails silently until deploy",
    "4. 'What is different between dev and prod?' = manual diff of two large JSON blobs",
    "5. Adding a new task = update dev JSON + prod JSON + staging JSON + ...",
    "6. No CI gate: nobody validates before deploying to prod",
    "7. Who deployed what and when? The UI history, not version control",
]
for p in problems:
    print(f"  {p}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1.2 The Config Drift Problem
# MAGIC
# MAGIC Over time, manual configs drift. Dev gets a fix that never makes it to prod.
# MAGIC Prod gets a cluster type upgrade that dev never gets. After 6 months, they
# MAGIC are so different that "works in dev, fails in prod" becomes the norm.

# COMMAND ----------

# Simulate config drift — what happens to manual configs over 6 months
print("CONFIG DRIFT SIMULATION: Same job, 6 months apart")
print("=" * 60)

drift_examples = [
    ("node_type_id", "i3.xlarge", "m5.2xlarge",
     "Prod was upgraded for a perf fix, dev never got the memo"),

    ("spark_version", "13.3.x-scala2.12", "15.4.x-scala2.12",
     "Dev tested new DBR, never updated prod config"),

    ("schedule quartz_cron", "0 0 1 * * ? (1am)", "0 30 1 * * ? (1:30am)",
     "Prod time was shifted to avoid cluster contention, never documented"),

    ("num_workers",  "2 → 4 in dev", "still 2 in prod",
     "Dev team doubled workers for a large backfill, forgot to revert"),

    ("libraries",
     "pandas==2.0 added in dev job",
     "pandas==1.5 still in prod (default)",
     "Library version mismatch causing subtle numeric differences"),
]

for field, dev_val, prod_val, reason in drift_examples:
    print(f"\nField: {field}")
    print(f"  Dev : {dev_val}")
    print(f"  Prod: {prod_val}")
    print(f"  Why : {reason}")

print()
print("Result: 'works in dev, fails in prod' is the most common incident cause")
print("Solution: DAB makes dev and prod identical by design (same YAML, different vars)")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 2: DAB `databricks.yml` Anatomy
# MAGIC
# MAGIC Now let's look at the DAB solution. The same two-environment config above
# MAGIC becomes a single `databricks.yml` with variables.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.1 The Full `databricks.yml` — Annotated

# COMMAND ----------

dab_yml = textwrap.dedent("""
# ── bundle identity ────────────────────────────────────────────────────
bundle:
  name: ecommerce-data-platform

# ── include additional resource files ─────────────────────────────────
# Large bundles split into multiple files for readability
include:
  - resources/jobs.yml
  - resources/pipelines.yml

# ── variables: single source of truth for env-specific values ─────────
variables:
  environment:
    description: Deployment environment
    default: dev

  catalog:
    description: Unity Catalog catalog name
    default: dev_catalog

  schedule_cron:
    description: Quartz CRON for job schedule
    default: "0 0 */6 * * ?"    # every 6 hours in dev

  cluster_node_type:
    description: Worker node instance type
    default: "i3.xlarge"

  num_workers:
    description: Number of workers for job clusters
    default: "2"

# ── resources: what to deploy ─────────────────────────────────────────
resources:
  jobs:
    ecommerce_ingestion:                        # identifier used in 'bundle run'
      name: "ecommerce-ingestion-${var.environment}"

      tasks:
        - task_key: ingest_bronze
          notebook_task:
            notebook_path: ./src/bronze_ingestion.py
            base_parameters:
              catalog: ${var.catalog}
              env:     ${var.environment}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id:  ${var.cluster_node_type}   # ← single variable, not duplicated
            num_workers:   ${var.num_workers}          # ← single variable

        - task_key: transform_silver
          depends_on: [{task_key: ingest_bronze}]
          notebook_task:
            notebook_path: ./src/silver_transform.py
            base_parameters:
              catalog: ${var.catalog}
          job_cluster_key: shared_cluster

      job_clusters:
        - job_cluster_key: shared_cluster
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id:  ${var.cluster_node_type}
            num_workers:   4

      schedule:
        quartz_cron_expression: ${var.schedule_cron}
        timezone_id: America/Los_Angeles
        pause_status: UNPAUSED

      email_notifications:
        on_failure: [ops-team@company.com]

      permissions:
        - level: CAN_MANAGE_RUN
          group_name: data-engineers

# ── targets: one per environment ──────────────────────────────────────
targets:
  dev:
    mode: development     # ← prefixes resource names with [dev username]
    default: true
    workspace:
      host: https://my-workspace.azuredatabricks.net
    variables:
      catalog:            dev_catalog
      schedule_cron:      "0 0 */6 * * ?"
      cluster_node_type:  "i3.xlarge"
      num_workers:        "2"

  staging:
    workspace:
      host: https://my-workspace.azuredatabricks.net
    variables:
      catalog:            staging_catalog
      schedule_cron:      "0 0 2 * * ?"     # nightly at 2am
      cluster_node_type:  "i3.2xlarge"
      num_workers:        "4"

  prod:
    workspace:
      host: https://prod-workspace.azuredatabricks.net
    run_as:
      service_principal_name: "prod-etl-sp"   # job runs as SP, not the deployer
    variables:
      catalog:            prod_catalog
      schedule_cron:      "0 0 1 * * ?"       # nightly at 1am
      cluster_node_type:  "i3.4xlarge"
      num_workers:        "8"
""").strip()

print("=== COMPLETE databricks.yml ===")
print(dab_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.2 What `mode: development` Does
# MAGIC
# MAGIC This is one of DAB's most powerful features for team workflows.
# MAGIC When `mode: development` is set on a target, DAB transforms resource names
# MAGIC and file paths so every developer gets their own isolated copy.

# COMMAND ----------

def simulate_development_mode(bundle_name: str, job_name: str, username: str):
    """Show how DAB transforms names in development mode."""
    short_name = username.split("@")[0]

    print("WITHOUT mode: development:")
    print(f"  Resource name : {job_name}")
    print(f"  File sync path: /Workspace/Users/{username}/.bundle/{bundle_name}/prod/files/")
    print()
    print("WITH mode: development (dev target):")
    dev_job_name = f"[dev {short_name}] {job_name}"
    dev_path     = f"/Workspace/Users/{username}/.bundle/{bundle_name}/dev/files/"
    print(f"  Resource name : {dev_job_name}")
    print(f"  File sync path: {dev_path}")
    print()
    print("If a second developer (alice@company.com) deploys the same bundle:")
    alice_name = "alice"
    print(f"  Resource name : [dev {alice_name}] {job_name}")
    print(f"  File sync path: /Workspace/Users/alice@company.com/.bundle/{bundle_name}/dev/files/")
    print()
    print("Result: Two developers can deploy the same bundle simultaneously")
    print("        without any name collision or file overwrite.")

simulate_development_mode(
    bundle_name="ecommerce-data-platform",
    job_name="ecommerce-ingestion",
    username=me.user_name
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2.3 Variable Resolution at Deploy Time
# MAGIC
# MAGIC When you run `databricks bundle deploy --target prod`, DAB resolves
# MAGIC every `${var.name}` reference using the target's variable overrides.
# MAGIC Let's simulate this resolution process.

# COMMAND ----------

def resolve_bundle_variables(target: str) -> dict:
    """Simulate what 'bundle validate' does: resolve all variables for a target."""
    # Base defaults
    variables = {
        "environment":        "dev",
        "catalog":            "dev_catalog",
        "schedule_cron":      "0 0 */6 * * ?",
        "cluster_node_type":  "i3.xlarge",
        "num_workers":        "2",
    }

    # Target-specific overrides (from targets: section in databricks.yml)
    overrides = {
        "dev": {
            "environment": "dev",
            "catalog": "dev_catalog",
            "schedule_cron": "0 0 */6 * * ?",
            "cluster_node_type": "i3.xlarge",
            "num_workers": "2",
        },
        "staging": {
            "environment": "staging",
            "catalog": "staging_catalog",
            "schedule_cron": "0 0 2 * * ?",
            "cluster_node_type": "i3.2xlarge",
            "num_workers": "4",
        },
        "prod": {
            "environment": "prod",
            "catalog": "prod_catalog",
            "schedule_cron": "0 0 1 * * ?",
            "cluster_node_type": "i3.4xlarge",
            "num_workers": "8",
        }
    }

    variables.update(overrides.get(target, {}))
    return variables

def resolve_job_config(target: str) -> dict:
    """Generate the fully-resolved job config for a target."""
    v = resolve_bundle_variables(target)
    return {
        "name": f"ecommerce-ingestion-{v['environment']}",
        "tasks": [
            {
                "task_key": "ingest_bronze",
                "notebook_path": "./src/bronze_ingestion.py",
                "base_parameters": {"catalog": v["catalog"], "env": v["environment"]},
                "node_type_id": v["cluster_node_type"],
                "num_workers": int(v["num_workers"]),
            }
        ],
        "schedule_cron": v["schedule_cron"],
        "cluster_node_type": v["cluster_node_type"],
        "num_workers": int(v["num_workers"]),
    }

print("VARIABLE RESOLUTION PER TARGET")
print("=" * 60)
for target in ["dev", "staging", "prod"]:
    config = resolve_job_config(target)
    print(f"\nTarget: {target}")
    print(f"  Job name       : {config['name']}")
    print(f"  Node type      : {config['cluster_node_type']}")
    print(f"  Num workers    : {config['num_workers']}")
    print(f"  Schedule CRON  : {config['schedule_cron']}")
    print(f"  Catalog        : {config['tasks'][0]['base_parameters']['catalog']}")

print()
print("Same databricks.yml → three completely different, correctly configured deployments.")
print("No JSON duplication. No manual sync required.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 3: Deploying Resources via Databricks SDK
# MAGIC
# MAGIC DAB uses the Databricks REST API internally to create and update resources.
# MAGIC Here we use the Python SDK directly to perform the same operations DAB would do,
# MAGIC giving you visibility into what happens under the hood.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Create a Job Programmatically (what `bundle deploy` does)

# COMMAND ----------

# This mirrors what DAB does when you run: databricks bundle deploy --target dev
# DAB calls the Jobs API with a fully-resolved job settings payload.

LAB_JOB_NAME = f"[lab-{me.user_name.split('@')[0]}] day31-ecommerce-ingestion"

# Check if this job already exists (idempotent behavior — DAB updates, not re-creates)
existing_jobs = list(w.jobs.list(name=LAB_JOB_NAME))
existing_job_id = existing_jobs[0].job_id if existing_jobs else None

if existing_job_id:
    print(f"Job already exists (ID: {existing_job_id}) — will update it (DAB idempotency)")
else:
    print(f"Job does not exist yet — will create it")

# COMMAND ----------

# Define the job settings (equivalent to the resources.jobs section in databricks.yml)
# Using the Databricks SDK's typed objects instead of raw JSON

notebook_source = """
# Databricks notebook source
# COMMAND ----------
# This is a demo notebook deployed by the Day 31 lab bundle
dbutils.widgets.text("catalog", "dev_catalog")
dbutils.widgets.text("env", "dev")

catalog = dbutils.widgets.get("catalog")
env     = dbutils.widgets.get("env")

print(f"Running bronze ingestion in environment: {env}")
print(f"Using catalog: {catalog}")

# Simulate some work
import time
time.sleep(2)
print("Bronze ingestion complete.")
"""

# Upload the demo notebook to workspace
notebook_path = f"/Users/{me.user_name}/.bundle/day31-lab/dev/files/src/bronze_ingestion"
try:
    import base64
    w.workspace.import_(
        path=notebook_path,
        format=w.workspace.ImportFormat.SOURCE,
        language=w.workspace.Language.PYTHON,
        content=base64.b64encode(notebook_source.encode()).decode(),
        overwrite=True,
    )
    print(f"Uploaded demo notebook to: {notebook_path}")
except Exception as e:
    print(f"Note: {e}")
    print("Using a notebook path that will be resolved at runtime.")

# COMMAND ----------

# Create (or update) the job via the SDK
# DAB does exactly this: resolves variables, then calls jobs.create() or jobs.reset()

from databricks.sdk.service.jobs import (
    JobSettings, Task, NotebookTask, CreateJob,
    JobCluster, ClusterSpec, CronSchedule, JobEmailNotifications,
    TaskDependency, QueueSettings
)
from databricks.sdk.service.compute import AutoScale

job_settings = {
    "name": LAB_JOB_NAME,
    "tasks": [
        {
            "task_key": "ingest_bronze",
            "description": "Bronze layer ingestion",
            "notebook_task": {
                "notebook_path": notebook_path,
                "base_parameters": {
                    "catalog": "dev_catalog",
                    "env":     "dev"
                }
            },
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id":  "i3.xlarge",
                "num_workers":   1,
                "spark_conf":    {
                    "spark.databricks.cluster.profile": "singleNode",
                    "spark.master": "local[*, 4]"
                },
                "custom_tags":   {"env": "dev", "bundle": "day31-lab"}
            },
            "timeout_seconds": 300,
        }
    ],
    "tags":   {"env": "dev", "bundle": "day31-lab", "managed_by": "dab"},
    "queue":  {"enabled": True},
    "max_concurrent_runs": 1,
}

if existing_job_id:
    # DAB behavior: update (reset) existing job rather than creating a new one
    w.jobs.reset(job_id=existing_job_id, new_settings=job_settings)
    job_id = existing_job_id
    print(f"Updated existing job: {LAB_JOB_NAME} (ID: {job_id})")
else:
    # DAB behavior: create new job
    created = w.jobs.create(**job_settings)
    job_id = created.job_id
    print(f"Created new job: {LAB_JOB_NAME} (ID: {job_id})")

print(f"\nJob URL: https://{WORKSPACE_HOST}/#job/{job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.2 Inspect the Deployed Job (what `bundle summary` shows)
# MAGIC
# MAGIC After deploying, DAB's `bundle summary` command lists all deployed resources
# MAGIC with their workspace URLs and IDs. Let's do the same via the SDK.

# COMMAND ----------

# Retrieve the deployed job configuration
deployed_job = w.jobs.get(job_id=job_id)

print("=== BUNDLE SUMMARY (SDK equivalent) ===")
print(f"\nResource: jobs.ecommerce_ingestion")
print(f"  Name  : {deployed_job.settings.name}")
print(f"  ID    : {deployed_job.job_id}")
print(f"  URL   : https://{WORKSPACE_HOST}/#job/{deployed_job.job_id}")
print(f"  Tasks : {[t.task_key for t in deployed_job.settings.tasks]}")
print(f"  Tags  : {deployed_job.settings.tags}")

# Show the creator and modification history
print(f"\nCreator            : {deployed_job.creator_user_name}")
print(f"Created time       : {datetime.fromtimestamp(deployed_job.created_time / 1000)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.3 Trigger a Job Run (what `bundle run` does)
# MAGIC
# MAGIC `databricks bundle run <resource_key>` triggers the job and streams output
# MAGIC to the terminal, exiting non-zero if the run fails. This makes it useful
# MAGIC as a post-deploy smoke test in CI.

# COMMAND ----------

# Trigger a manual run — equivalent to: databricks bundle run ecommerce_ingestion
run = w.jobs.run_now(job_id=job_id)
run_id = run.run_id
print(f"Triggered run: {run_id}")
print(f"Run URL: https://{WORKSPACE_HOST}/#job/{job_id}/run/{run_id}")

# Poll until the run completes (DAB does this with --wait flag)
print("\nWaiting for run to complete...")
run_result = w.jobs.wait_get_run_job_terminated_or_skipped(run_id=run_id)

print(f"\nRun result: {run_result.state.result_state}")
print(f"Run state : {run_result.state.life_cycle_state}")

# Non-zero exit when failed — this is how CI knows to fail the build
from databricks.sdk.service.jobs import RunResultState
if run_result.state.result_state == RunResultState.SUCCESS:
    print("✓ Smoke test passed — safe to mark deployment successful in CI")
else:
    print(f"✗ Smoke test FAILED — CI should block the deployment")
    print(f"  State message: {run_result.state.state_message}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 4: Environment-Specific Configuration Patterns
# MAGIC
# MAGIC One of DAB's core problems to solve: how do you make the same bundle
# MAGIC behave differently in dev vs prod without duplicating config?
# MAGIC The answer is variables + target overrides.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Simulate Multi-Environment Deployment

# COMMAND ----------

# Show what the job configuration looks like in each environment
# This is what DAB produces when it resolves variables per target

ENVIRONMENT_CONFIGS = {
    "dev": {
        "job_name_suffix":   "[dev sreekanth] ecommerce-ingestion-dev",
        "catalog":           "dev_catalog",
        "node_type":         "i3.xlarge",
        "num_workers":       2,
        "schedule":          "Every 6 hours",
        "cluster_cost_hr":   "$0.68",
        "monthly_cost_est":  "$98",
        "dlt_dev_mode":      True,      # DLT development mode (no quarantine, cheaper)
        "tags":              {"env": "dev", "cost_center": "engineering"},
    },
    "staging": {
        "job_name_suffix":   "ecommerce-ingestion-staging",
        "catalog":           "staging_catalog",
        "node_type":         "i3.2xlarge",
        "num_workers":       4,
        "schedule":          "Nightly at 2am",
        "cluster_cost_hr":   "$1.36",
        "monthly_cost_est":  "$41",
        "dlt_dev_mode":      False,
        "tags":              {"env": "staging", "cost_center": "data-platform"},
    },
    "prod": {
        "job_name_suffix":   "ecommerce-ingestion-prod",
        "catalog":           "prod_catalog",
        "node_type":         "i3.4xlarge",
        "num_workers":       8,
        "schedule":          "Nightly at 1am",
        "cluster_cost_hr":   "$2.72",
        "monthly_cost_est":  "$83",
        "dlt_dev_mode":      False,
        "tags":              {"env": "prod", "cost_center": "data-platform"},
    }
}

print("ENVIRONMENT COMPARISON — Same bundle, three targets")
print("=" * 80)
print(f"{'Property':<22} {'DEV':<30} {'STAGING':<30} {'PROD':<30}")
print("-" * 80)

props = [
    ("Job name",       "job_name_suffix"),
    ("Catalog",        "catalog"),
    ("Node type",      "node_type"),
    ("Num workers",    "num_workers"),
    ("Schedule",       "schedule"),
    ("Cost/hr",        "cluster_cost_hr"),
    ("Monthly est.",   "monthly_cost_est"),
    ("DLT dev mode",   "dlt_dev_mode"),
]

for label, key in props:
    dev_val     = str(ENVIRONMENT_CONFIGS["dev"][key])
    staging_val = str(ENVIRONMENT_CONFIGS["staging"][key])
    prod_val    = str(ENVIRONMENT_CONFIGS["prod"][key])
    print(f"{label:<22} {dev_val:<30} {staging_val:<30} {prod_val:<30}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Target Overrides for Resource Properties
# MAGIC
# MAGIC Beyond variables, targets can override specific resource properties.
# MAGIC This is useful when you want to disable a schedule in staging or
# MAGIC add extra permissions in prod.

# COMMAND ----------

target_override_example = textwrap.dedent("""
# In resources/jobs.yml:
resources:
  jobs:
    ecommerce_ingestion:
      name: "ecommerce-ingestion-${var.environment}"
      schedule:
        quartz_cron_expression: ${var.schedule_cron}
        pause_status: UNPAUSED

# In databricks.yml targets section:
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://my-workspace.azuredatabricks.net
    variables:
      schedule_cron: "0 0 */6 * * ?"
    # TARGET OVERRIDE: pause the schedule in dev
    # (developers trigger manually, no automated runs to pollute dev)
    resources:
      jobs:
        ecommerce_ingestion:
          schedule:
            pause_status: PAUSED      # ← Override: dev schedule is paused

  prod:
    workspace:
      host: https://prod-workspace.azuredatabricks.net
    variables:
      schedule_cron: "0 0 1 * * ?"
    # TARGET OVERRIDE: add prod-only notification
    resources:
      jobs:
        ecommerce_ingestion:
          schedule:
            pause_status: UNPAUSED    # ← Prod runs on schedule
          email_notifications:
            on_failure:
              - ops-pagerduty@company.com
              - data-lead@company.com  # extra contact in prod only
          health:                     # ← Prod-only health check
            rules:
              - metric: RUN_DURATION_SECONDS
                op: GREATER_THAN
                value: 3600          # alert if run takes > 1 hour
""").strip()

print("TARGET-LEVEL RESOURCE OVERRIDES:")
print(target_override_example)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 5: Bundle Validation and Testing Patterns

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 What `bundle validate` Checks

# COMMAND ----------

# Simulate the checks that 'databricks bundle validate' performs
# In real usage: run this in CI on every PR before any deployment

def simulate_bundle_validate(bundle_config: dict) -> list:
    """Return a list of validation errors (empty = valid)."""
    errors = []

    # Check: all tasks in a job have unique task_key
    job_tasks_seen = {}
    for job_name, job_def in bundle_config.get("jobs", {}).items():
        task_keys = [t["task_key"] for t in job_def.get("tasks", [])]
        dupes = [k for k in task_keys if task_keys.count(k) > 1]
        if dupes:
            errors.append(f"Job '{job_name}': duplicate task keys: {set(dupes)}")

        # Check: depends_on references valid task keys
        for task in job_def.get("tasks", []):
            for dep in task.get("depends_on", []):
                if dep["task_key"] not in task_keys:
                    errors.append(
                        f"Job '{job_name}', task '{task['task_key']}': "
                        f"depends_on unknown task '{dep['task_key']}'"
                    )

    # Check: all variables referenced with ${var.x} are declared
    import re
    def find_var_refs(obj, path=""):
        refs = set()
        if isinstance(obj, str):
            for match in re.findall(r"\$\{var\.([a-zA-Z_]+)\}", obj):
                refs.add(match)
        elif isinstance(obj, dict):
            for k, v in obj.items():
                refs.update(find_var_refs(v, f"{path}.{k}"))
        elif isinstance(obj, list):
            for item in obj:
                refs.update(find_var_refs(item, path))
        return refs

    declared_vars = set(bundle_config.get("variables", {}).keys())
    used_vars = find_var_refs(bundle_config.get("jobs", {}))
    undeclared = used_vars - declared_vars
    if undeclared:
        errors.append(f"Undeclared variables used: {undeclared}")

    return errors

# Test with a valid bundle
valid_bundle = {
    "variables": {"catalog": {}, "environment": {}, "node_type": {}},
    "jobs": {
        "my_job": {
            "name": "my-job-${var.environment}",
            "tasks": [
                {"task_key": "step1", "notebook_task": {"notebook_path": "./src/step1.py"}},
                {"task_key": "step2", "depends_on": [{"task_key": "step1"}],
                 "notebook_task": {"notebook_path": "./src/step2.py"}},
            ]
        }
    }
}

# Test with an invalid bundle
invalid_bundle = {
    "variables": {"catalog": {}},                      # missing 'environment' var
    "jobs": {
        "my_job": {
            "name": "my-job-${var.environment}",       # references undeclared var
            "tasks": [
                {"task_key": "step1", "notebook_task": {"notebook_path": "./step1.py"}},
                {"task_key": "step1", "notebook_task": {"notebook_path": "./step1b.py"}},  # duplicate!
                {"task_key": "step2", "depends_on": [{"task_key": "step99"}],  # bad dep
                 "notebook_task": {"notebook_path": "./step2.py"}},
            ]
        }
    }
}

print("Validating VALID bundle:")
errors = simulate_bundle_validate(valid_bundle)
if errors:
    for e in errors:
        print(f"  ERROR: {e}")
else:
    print("  ✓ Bundle is valid — safe to deploy")

print()
print("Validating INVALID bundle:")
errors = simulate_bundle_validate(invalid_bundle)
if errors:
    for e in errors:
        print(f"  ERROR: {e}")
else:
    print("  ✓ No errors")

print()
print("In CI, 'bundle validate' catches these BEFORE touching the workspace.")
print("Failed validation exits with code 1 → CI blocks the PR immediately.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 CI/CD Pipeline Structure

# COMMAND ----------

ci_cd_stages = textwrap.dedent("""
  CI/CD PIPELINE FOR A DAB-MANAGED PROJECT
  ┌─────────────────────────────────────────────────────────────────────────────┐
  │                                                                              │
  │  PULL REQUEST (developer pushes code change):                               │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │  [1] Unit tests                                                             │
  │      pytest tests/                                                          │
  │      → Tests Python transform logic without Spark                          │
  │                                                                              │
  │  [2] Bundle validation (no workspace required!)                            │
  │      databricks bundle validate --target dev                               │
  │      databricks bundle validate --target prod                              │
  │      → Catches YAML errors, undeclared variables, bad deps                 │
  │      → Fast: zero API calls                                                │
  │                                                                              │
  │  [3] Deploy to dev (optional integration gate)                             │
  │      databricks bundle deploy --target dev                                 │
  │      databricks bundle run ecommerce_ingestion --target dev                │
  │      → Real end-to-end test on a dev workspace                            │
  │                                                                              │
  │  MERGE TO MAIN (PR approved and merged):                                   │
  │  ─────────────────────────────────────────────────────────────────────────  │
  │  [4] Deploy to staging                                                      │
  │      databricks bundle deploy --target staging                             │
  │      databricks bundle run ecommerce_ingestion --target staging            │
  │                                                                              │
  │  [5] Staging smoke test passes → manual approval gate                      │
  │                                                                              │
  │  [6] Deploy to prod (requires approval)                                    │
  │      databricks bundle deploy --target prod                                │
  │      → Runs as prod service principal (run_as in target)                  │
  │                                                                              │
  │  [7] Post-deploy smoke test                                                 │
  │      databricks bundle run ecommerce_ingestion --target prod \             │
  │        --python-params='["--date", "'"${YESTERDAY}"'"]'                  │
  │      → Verifies prod deployment is live and healthy                       │
  │                                                                              │
  │  ON FAILURE: notify via Slack/PagerDuty, block further merges             │
  │  ON SUCCESS: update deployment dashboard, tag git commit                   │
  │                                                                              │
  └─────────────────────────────────────────────────────────────────────────────┘
""").strip()

print(ci_cd_stages)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.3 GitHub Actions Workflow (complete, production-ready)

# COMMAND ----------

github_actions_pr_yaml = textwrap.dedent("""
# .github/workflows/pr-validate.yml
name: "PR — Bundle Validate"

on:
  pull_request:
    branches: [main]

env:
  # Dev workspace auth (read-only: validate only, no deploy)
  DATABRICKS_HOST:  ${{ secrets.DEV_DATABRICKS_HOST }}
  DATABRICKS_TOKEN: ${{ secrets.DEV_DATABRICKS_TOKEN }}

jobs:
  unit-test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: '3.11' }
      - run: pip install -r requirements-dev.txt
      - run: pytest tests/ -v --tb=short

  bundle-validate:
    runs-on: ubuntu-latest
    needs: unit-test
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main      # installs 'databricks' CLI

      - name: Validate dev target
        run: databricks bundle validate --target dev

      - name: Validate prod target
        run: databricks bundle validate --target prod
        env:
          DATABRICKS_HOST:  ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_DATABRICKS_TOKEN }}
""").strip()

github_actions_deploy_yaml = textwrap.dedent("""
# .github/workflows/deploy.yml
name: "Deploy to Production"

on:
  push:
    branches: [main]

jobs:
  deploy-staging:
    runs-on: ubuntu-latest
    environment: staging
    env:
      DATABRICKS_HOST:  ${{ secrets.STAGING_DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.STAGING_DATABRICKS_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target staging
      - run: |
          databricks bundle run ecommerce_ingestion \\
            --target staging \\
            --python-params='["--validate-only","true"]'

  deploy-prod:
    runs-on: ubuntu-latest
    needs: deploy-staging
    environment: production               # requires manual approval in GitHub
    env:
      DATABRICKS_HOST:  ${{ secrets.PROD_DATABRICKS_HOST }}
      DATABRICKS_TOKEN: ${{ secrets.PROD_SERVICE_PRINCIPAL_TOKEN }}
    steps:
      - uses: actions/checkout@v4
      - uses: databricks/setup-cli@main
      - run: databricks bundle deploy --target prod
      - name: Post-deploy smoke test
        run: |
          YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
          databricks bundle run ecommerce_ingestion \\
            --target prod \\
            --python-params="[\\"--date\\",\\"${YESTERDAY}\\"]"
""").strip()

print("PR VALIDATION WORKFLOW:")
print(github_actions_pr_yaml)
print()
print("=" * 70)
print()
print("PRODUCTION DEPLOY WORKFLOW:")
print(github_actions_deploy_yaml)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 6: Exploring Deployed Resources (Bundle State)
# MAGIC
# MAGIC After deploying, you need visibility into what is deployed and whether it
# MAGIC is healthy. `bundle summary` gives you URLs; the SDK gives you full detail.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 List All Jobs Managed by a Bundle (using tags)
# MAGIC
# MAGIC DAB tags all resources it creates with `{"bundle": "<bundle_name>"}`
# MAGIC This makes it easy to find all bundle-managed jobs in the workspace.

# COMMAND ----------

# List all jobs in the workspace that have our lab bundle tag
print(f"Searching for jobs with bundle tag 'day31-lab'...")
all_jobs = list(w.jobs.list())

bundle_jobs = [
    j for j in all_jobs
    if j.settings and j.settings.tags and
       j.settings.tags.get("bundle") == "day31-lab"
]

print(f"\nFound {len(bundle_jobs)} job(s) managed by bundle 'day31-lab':")
for j in bundle_jobs:
    print(f"  ID   : {j.job_id}")
    print(f"  Name : {j.settings.name}")
    print(f"  URL  : https://{WORKSPACE_HOST}/#job/{j.job_id}")
    print(f"  Tags : {j.settings.tags}")
    print()

if not bundle_jobs:
    print("  (none found — the job created in Part 3 may use a different tag)")
    print(f"  Looking for our lab job directly: {LAB_JOB_NAME}")
    our_job = next((j for j in all_jobs if j.settings.name == LAB_JOB_NAME), None)
    if our_job:
        print(f"  Found: ID={our_job.job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Inspect Run History

# COMMAND ----------

# Get the run history for our lab job
run_history = list(w.jobs.list_runs(job_id=job_id, limit=5))

print(f"Recent runs for job: {LAB_JOB_NAME}")
print(f"{'Run ID':<12} {'Status':<20} {'Start Time':<25} {'Duration':<12}")
print("-" * 70)

for run in run_history:
    status     = str(run.state.result_state or run.state.life_cycle_state)
    start_time = datetime.fromtimestamp(run.start_time / 1000) if run.start_time else "N/A"
    end_time   = datetime.fromtimestamp(run.end_time / 1000) if run.end_time else None
    duration   = f"{(run.end_time - run.start_time) / 1000:.1f}s" if run.end_time else "running"
    print(f"{run.run_id:<12} {status:<20} {str(start_time):<25} {duration:<12}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.3 Compare Bundle State vs Expected State (Drift Detection)
# MAGIC
# MAGIC A powerful DevOps pattern: compare what is deployed against what is
# MAGIC defined in the bundle. Any discrepancy is "configuration drift" —
# MAGIC something was changed directly in the UI or API, bypassing the bundle.

# COMMAND ----------

def detect_config_drift(job_id: int, expected_config: dict) -> list:
    """
    Compare deployed job config against expected config.
    Returns a list of drift items (empty = no drift).
    """
    deployed = w.jobs.get(job_id=job_id)
    drift = []

    # Check job name
    if deployed.settings.name != expected_config.get("name"):
        drift.append({
            "field": "name",
            "expected": expected_config.get("name"),
            "actual": deployed.settings.name,
            "severity": "HIGH"
        })

    # Check task count
    expected_tasks = len(expected_config.get("tasks", []))
    actual_tasks   = len(deployed.settings.tasks or [])
    if expected_tasks != actual_tasks:
        drift.append({
            "field": "task_count",
            "expected": expected_tasks,
            "actual": actual_tasks,
            "severity": "HIGH"
        })

    return drift

# Test drift detection
expected = {
    "name": LAB_JOB_NAME,
    "tasks": [{"task_key": "ingest_bronze"}]
}

drift = detect_config_drift(job_id=job_id, expected_config=expected)

if drift:
    print("DRIFT DETECTED — deployed config differs from bundle definition:")
    for item in drift:
        print(f"  [{item['severity']}] {item['field']}: "
              f"expected={item['expected']!r}, actual={item['actual']!r}")
    print()
    print("Fix: run 'databricks bundle deploy' to restore the intended state.")
else:
    print("✓ No drift detected — deployed config matches bundle definition")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 7: Key DAB Patterns for Production Use

# COMMAND ----------

production_patterns = {
    "1. Never share PATs in CI": textwrap.dedent("""
        Bad:  DATABRICKS_TOKEN=dapi123... (personal access token in CI secrets)
        Good: Use an OAuth M2M service principal token.
              Create a service principal in the admin console,
              grant it 'Can run' on specific jobs,
              use its OAuth client_id + client_secret in CI.
        Why:  PATs expire, are tied to a person, and have broad permissions.
              Service principals have scoped permissions and rotate via OAuth.
    """),

    "2. run_as for job execution identity": textwrap.dedent("""
        targets:
          prod:
            run_as:
              service_principal_name: "prod-etl-sp"

        Why: The deploying identity (CI bot) has broad workspace permissions.
             The job execution identity should be minimal-privilege.
             run_as separates 'who deployed' from 'who the job runs as'.
             Critical for Unity Catalog audit logs.
    """),

    "3. bundle validate in PR checks, not just deploy": textwrap.dedent("""
        Why: validate makes ZERO API calls — it is instant and free.
             Running it on every PR catches YAML errors before any human
             reviews the code. Don't save validation for the deploy step.
    """),

    "4. Pin Databricks CLI version in CI": textwrap.dedent("""
        Bad:  - uses: databricks/setup-cli@main   (gets latest, may break)
        Good: - uses: databricks/setup-cli@v0.230.0   (pinned version)
        Why:  Databricks CLI is actively developed. A breaking change in CLI
              behavior can break your deployment overnight if you track main.
    """),

    "5. Use bundle destroy carefully": textwrap.dedent("""
        databricks bundle destroy --target prod

        This DELETES all jobs, pipelines, and dashboards the bundle manages.
        It does NOT delete data in Unity Catalog.
        In prod: always require --auto-approve to be set explicitly in CI
                 and require a manual approval gate before running destroy.
        Never add destroy to the same pipeline as deploy.
    """),
}

print("PRODUCTION PATTERNS FOR DATABRICKS ASSET BUNDLES")
print("=" * 70)
for title, content in production_patterns.items():
    print(f"\n{'─' * 70}")
    print(f"PATTERN {title}")
    print(content.strip())

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Part 8: Cleanup

# COMMAND ----------

# Delete the lab job we created
try:
    w.jobs.delete(job_id=job_id)
    print(f"Deleted lab job: {LAB_JOB_NAME} (ID: {job_id})")
except Exception as e:
    print(f"Could not delete job: {e}")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP SCHEMA IF EXISTS day31_devops_lab CASCADE;

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Summary: What You Learned
# MAGIC
# MAGIC ```
# MAGIC DATABRICKS ASSET BUNDLES RECAP
# MAGIC ┌─────────────────────────────────────────────────────────────────────┐
# MAGIC │                                                                      │
# MAGIC │  PROBLEM SOLVED:                                                    │
# MAGIC │    Manual deployments → config drift, no version control,          │
# MAGIC │    no CI/CD, "works in dev fails in prod"                          │
# MAGIC │                                                                      │
# MAGIC │  DAB SOLUTION:                                                      │
# MAGIC │    databricks.yml = single source of truth for all Databricks      │
# MAGIC │    resources. One file. Checked into git. Version-controlled.       │
# MAGIC │                                                                      │
# MAGIC │  KEY CONCEPTS:                                                      │
# MAGIC │    bundle:     Project identity and name                           │
# MAGIC │    variables:  Parameterize everything (${var.name} syntax)        │
# MAGIC │    resources:  What to deploy (jobs, pipelines, dashboards)        │
# MAGIC │    targets:    Where and how to deploy (dev/staging/prod)          │
# MAGIC │    include:    Split large bundles into multiple YAML files        │
# MAGIC │                                                                      │
# MAGIC │  LIFECYCLE COMMANDS:                                                │
# MAGIC │    bundle validate   → Check YAML, zero API calls, fast           │
# MAGIC │    bundle deploy     → Create/update resources in workspace        │
# MAGIC │    bundle run        → Trigger a job and stream output             │
# MAGIC │    bundle summary    → List deployed resources with URLs           │
# MAGIC │    bundle destroy    → Delete all managed resources               │
# MAGIC │                                                                      │
# MAGIC │  CRITICAL FEATURES:                                                 │
# MAGIC │    mode: development → personal namespace per developer            │
# MAGIC │    run_as            → separate deploy identity from run identity  │
# MAGIC │    target overrides  → pause schedule in dev, extra alerts in prod │
# MAGIC │                                                                      │
# MAGIC │  CI/CD PATTERN:                                                     │
# MAGIC │    PR  → unit tests + bundle validate (no workspace needed)        │
# MAGIC │    Merge → deploy staging → smoke test → approve → deploy prod     │
# MAGIC │                                                                      │
# MAGIC └─────────────────────────────────────────────────────────────────────┘
# MAGIC ```
