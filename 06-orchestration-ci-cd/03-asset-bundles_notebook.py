# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Databricks Asset Bundles (DABs) — Hands-On Notebook
# MAGIC > Module 06 — Topic 03 | Orchestration & CI/CD
# MAGIC
# MAGIC This notebook covers:
# MAGIC 1. Bundle project structure and `databricks.yml` anatomy
# MAGIC 2. Resource definitions for jobs and DLT pipelines
# MAGIC 3. Environment management with targets
# MAGIC 4. CLI workflow from init to destroy
# MAGIC
# MAGIC **Note**: Asset Bundles are managed from the CLI, not from notebooks.
# MAGIC This notebook provides reference YAML examples and explains each concept.
# MAGIC Cells marked `[REFERENCE]` show YAML/CLI syntax. Cells marked `[RUNNABLE]`
# MAGIC demonstrate supporting concepts in Python.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 1: What Are Asset Bundles? [RUNNABLE]
# MAGIC Quick overview of the problem DABs solve.

# COMMAND ----------

print("""
PROBLEM: Manual configuration in the Databricks UI leads to:
  - Configuration drift between dev, staging, and prod
  - No audit trail for who changed what
  - Impossible to reproduce an environment from scratch
  - No code review process for infrastructure changes

SOLUTION: Databricks Asset Bundles (DABs)
  - Define jobs, pipelines, and dashboards as YAML
  - Store alongside source code in Git
  - Deploy via CLI: databricks bundle deploy --target prod
  - Destroy and recreate environments on demand

Think of it as: "Terraform for Databricks resources"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 2: Bundle Project Structure [RUNNABLE]
# MAGIC The standard directory layout for a DABs project.

# COMMAND ----------

project_structure = """
my-etl-project/
|
+-- databricks.yml              # Main bundle config (required)
|
+-- src/                        # Source code
|   +-- ingest.py               # Notebook or Python script
|   +-- transform.py
|   +-- quality_checks.py
|   +-- dlt_definitions.py      # DLT pipeline code
|
+-- resources/                  # Resource YAML definitions
|   +-- etl_job.yml             # Workflow job
|   +-- dlt_pipeline.yml        # DLT pipeline config
|   +-- dashboard.yml           # SQL dashboard
|
+-- tests/                      # Unit and integration tests
|   +-- test_transform.py
|   +-- conftest.py
|
+-- fixtures/                   # Test data
|   +-- sample_data.json
|
+-- .databricks/                # Local CLI state (GITIGNORED)
|   +-- bundle/
|       +-- dev/
"""

print(project_structure)
print("Key rules:")
print("  1. databricks.yml MUST be at the project root")
print("  2. .databricks/ should be in .gitignore")
print("  3. Resources can be defined inline or in included files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 3: The databricks.yml File [REFERENCE]
# MAGIC The central configuration file that defines the entire bundle.

# COMMAND ----------

databricks_yml = """
# ========================================================
# databricks.yml — Main Bundle Configuration
# ========================================================

bundle:
  name: sales-etl                    # Unique bundle identifier

workspace:
  host: https://myworkspace.cloud.databricks.com

# Pull in resource definitions from separate files
include:
  - resources/*.yml

# Variables that can be overridden per target
variables:
  catalog:
    description: Unity Catalog name
    default: dev_catalog
  warehouse_size:
    description: SQL warehouse size
    default: "2X-Small"

# ========================================================
# Targets = Environments
# ========================================================
targets:
  dev:
    mode: development               # Prefixes resource names with [dev]
    default: true                    # Default when no --target specified
    workspace:
      host: https://dev.cloud.databricks.com
    variables:
      catalog: dev_catalog

  staging:
    workspace:
      host: https://staging.cloud.databricks.com
    variables:
      catalog: staging_catalog
      warehouse_size: "Small"

  prod:
    mode: production                 # Strict mode: locks permissions
    workspace:
      host: https://prod.cloud.databricks.com
    run_as:
      service_principal_name: "sp-etl-prod"  # NEVER run as a user in prod
    variables:
      catalog: prod_catalog
      warehouse_size: "Medium"
"""

print(databricks_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 4: Job Resource Definition [REFERENCE]
# MAGIC Defining a multi-task Workflow job in YAML.

# COMMAND ----------

job_yml = """
# ========================================================
# resources/etl_job.yml — Workflow Job Definition
# ========================================================

resources:
  jobs:
    sales_etl_job:
      name: "sales-etl-${bundle.target}"    # Dynamic naming per target

      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "America/New_York"

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            num_workers: 4
            node_type_id: "i3.xlarge"
            data_security_mode: "SINGLE_USER"

      tasks:
        - task_key: ingest
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../src/ingest.py
            base_parameters:
              catalog: "${var.catalog}"
          max_retries: 2
          min_retry_interval_millis: 60000

        - task_key: transform
          depends_on:
            - task_key: ingest
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../src/transform.py

        - task_key: quality_check
          depends_on:
            - task_key: transform
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../src/quality_checks.py

      email_notifications:
        on_failure:
          - data-team@company.com
"""

print(job_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 5: DLT Pipeline Resource Definition [REFERENCE]

# COMMAND ----------

dlt_yml = """
# ========================================================
# resources/dlt_pipeline.yml — Delta Live Tables Pipeline
# ========================================================

resources:
  pipelines:
    sales_dlt_pipeline:
      name: "sales-dlt-${bundle.target}"
      target: "${var.catalog}.sales_dlt"

      libraries:
        - notebook:
            path: ../src/dlt_definitions.py

      clusters:
        - label: default
          autoscale:
            min_workers: 1
            max_workers: 5
            mode: ENHANCED

      continuous: false           # Triggered (batch) mode
      development: true           # Override per target
      channel: CURRENT            # Use current DLT runtime

      configuration:
        "spark.databricks.delta.preview.enabled": "true"
"""

print(dlt_yml)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 6: CLI Workflow [RUNNABLE]
# MAGIC The complete lifecycle of a bundle from creation to teardown.

# COMMAND ----------

cli_commands = [
    ("1. Initialize",  "databricks bundle init",
     "Create a new project from a template"),
    ("2. Validate",    "databricks bundle validate",
     "Check YAML syntax, verify references, catch errors early"),
    ("3. Deploy",      "databricks bundle deploy --target dev",
     "Sync code + create/update resources in the workspace"),
    ("4. Run",         "databricks bundle run sales_etl_job",
     "Trigger a specific job or pipeline"),
    ("5. Run (params)","databricks bundle run sales_etl_job --params run_date=2024-01-20",
     "Trigger with parameter overrides"),
    ("6. Destroy",     "databricks bundle destroy --target dev",
     "Remove ALL resources created by this bundle"),
]

print("=== Databricks Bundle CLI Workflow ===\n")
for step, command, description in cli_commands:
    print(f"{step}")
    print(f"  $ {command}")
    print(f"  {description}\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 7: Variable Substitution Patterns [RUNNABLE]
# MAGIC Understanding how variables resolve across targets.

# COMMAND ----------

targets = {
    "dev": {"catalog": "dev_catalog", "warehouse_size": "2X-Small", "mode": "development"},
    "staging": {"catalog": "staging_catalog", "warehouse_size": "Small", "mode": "default"},
    "prod": {"catalog": "prod_catalog", "warehouse_size": "Medium", "mode": "production"},
}

template_job_name = "sales-etl-{target}"
template_target_schema = "{catalog}.sales_dlt"

print("=== Variable Resolution Per Target ===\n")
for target, vars in targets.items():
    print(f"Target: {target}")
    print(f"  Job name     : {template_job_name.format(target=target)}")
    print(f"  DLT target   : {template_target_schema.format(**vars)}")
    print(f"  Warehouse    : {vars['warehouse_size']}")
    print(f"  Mode         : {vars['mode']}")
    if target == "prod":
        print(f"  run_as       : sp-etl-prod (service principal)")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 8: Development vs Production Mode [RUNNABLE]
# MAGIC The `mode` setting changes how bundles behave.

# COMMAND ----------

print("=== Bundle Modes ===\n")
print("DEVELOPMENT mode (mode: development):")
print("  - Resource names prefixed with [dev <user>]")
print("  - Jobs are paused by default (no accidental runs)")
print("  - Cluster policies may be relaxed")
print("  - Resources created under the deploying user's identity")
print()
print("PRODUCTION mode (mode: production):")
print("  - Resource names are used as-is (no prefix)")
print("  - Jobs can be scheduled (unpaused)")
print("  - Must specify run_as (service principal recommended)")
print("  - Permissions are locked — only the run_as identity can modify")
print("  - Requires CAN_MANAGE permission for the deploying principal")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 9: Integrating Bundles into CI/CD [REFERENCE]
# MAGIC A GitHub Actions snippet that deploys a bundle on merge to main.

# COMMAND ----------

github_actions_snippet = """
# .github/workflows/deploy-prod.yml (REFERENCE)

name: Deploy to Production
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Validate Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_URL }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: databricks bundle validate --target prod

      - name: Deploy Bundle
        env:
          DATABRICKS_HOST: ${{ secrets.PROD_WORKSPACE_URL }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_TOKEN }}
        run: databricks bundle deploy --target prod
"""

print(github_actions_snippet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cell 10: Cleanup [RUNNABLE]

# COMMAND ----------

print("No persistent resources created in this notebook.")
print()
print("To practice with Asset Bundles:")
print("  1. Install the Databricks CLI locally")
print("  2. Run: databricks bundle init")
print("  3. Edit databricks.yml and resource files")
print("  4. Run: databricks bundle validate")
print("  5. Run: databricks bundle deploy --target dev")
print("  6. Verify resources in your workspace UI")
print("  7. Run: databricks bundle destroy --target dev")
