# Databricks Asset Bundles (DABs)
> Module 06 — Topic 03 | Level: Intermediate-Advanced | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:

1. Explain what Databricks Asset Bundles are and why they matter
2. Define a `databricks.yml` bundle configuration for jobs and pipelines
3. Use the Databricks CLI to init, validate, deploy, and destroy bundles
4. Manage multiple environments (dev, staging, prod) in a single bundle
5. Integrate Asset Bundles into CI/CD pipelines

## Conceptual Overview

### What Are Databricks Asset Bundles?

Asset Bundles (DABs) are Databricks' native infrastructure-as-code solution. They
let you define Workflows, DLT pipelines, dashboards, and ML models as YAML files
that live alongside your source code in Git. Think of them as "Terraform for
Databricks resources."

```
  Without DABs:                    With DABs:
  +-------------------+            +-------------------+
  | Click in UI to    |            | databricks.yml    |
  | create jobs       |            | defines everything|
  | Manual config     |            | Version-controlled|
  | Drift between     |            | Consistent across |
  | environments      |            | all environments  |
  +-------------------+            +-------------------+
       Fragile                          Reliable
```

This directly supports the goal of having a platform that is **"agile — meaning
you should be able to upgrade and downgrade infrastructure at any point of time
without worrying about the cost."** With DABs, your infrastructure is code that
can be reviewed, tested, and rolled back.

### Bundle Project Structure

```
my-etl-project/
+-- databricks.yml              # Main bundle configuration
+-- src/
|   +-- ingest.py               # Source code
|   +-- transform.py
|   +-- quality_checks.py
+-- resources/
|   +-- etl_job.yml             # Job definition (included by databricks.yml)
|   +-- dlt_pipeline.yml        # DLT pipeline definition
+-- tests/
|   +-- test_transform.py
+-- fixtures/
|   +-- sample_data.json
+-- .databricks/                # Local state (gitignored)
```

### How Bundles Work

```
  Developer                   Databricks CLI              Workspace
  +--------+                  +------------+              +---------+
  |  Edit  | -- validate -->  | Parse YAML |              |         |
  |  YAML  |                  | Check refs |              |         |
  |  +     | -- deploy --->   | Sync files | -- API -->   | Create/ |
  |  Code  |                  | Create res |              | Update  |
  |        | -- run ------>   | Trigger    | -- API -->   | Execute |
  |        | -- destroy --->  | Tear down  | -- API -->   | Delete  |
  +--------+                  +------------+              +---------+
```

## Hands-On Walkthrough

### Step 1: Install the Databricks CLI

```bash
# Install via pip (Python 3.8+)
pip install databricks-cli

# Or install the new Go-based CLI
# macOS
brew tap databricks/tap
brew install databricks

# Verify
databricks --version

# Configure authentication
databricks configure --token
# Enter: workspace URL and personal access token
```

### Step 2: Initialize a Bundle from a Template

```bash
# Create a new bundle from the default Python template
databricks bundle init

# Or specify a template
databricks bundle init default-python \
    --project-name my-etl-project

# The generated structure:
# my-etl-project/
# +-- databricks.yml
# +-- src/
# +-- resources/
# +-- tests/
# +-- README.md
```

### Step 3: Define the Bundle Configuration

```yaml
# databricks.yml
bundle:
  name: sales-etl

# Shared settings
workspace:
  host: https://myworkspace.cloud.databricks.com

# Include resource definitions from separate files
include:
  - resources/*.yml

# Environment overrides
targets:
  dev:
    mode: development
    default: true
    workspace:
      host: https://dev-workspace.cloud.databricks.com
    variables:
      catalog: dev_catalog
      warehouse_size: "2X-Small"

  staging:
    workspace:
      host: https://staging-workspace.cloud.databricks.com
    variables:
      catalog: staging_catalog
      warehouse_size: "Small"

  prod:
    mode: production
    workspace:
      host: https://prod-workspace.cloud.databricks.com
    run_as:
      service_principal_name: "sp-etl-prod"
    variables:
      catalog: prod_catalog
      warehouse_size: "Medium"
```

### Step 4: Define a Workflow Job

```yaml
# resources/etl_job.yml
resources:
  jobs:
    sales_etl_job:
      name: "sales-etl-${bundle.target}"
      schedule:
        quartz_cron_expression: "0 0 2 * * ?"
        timezone_id: "America/New_York"

      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: "14.3.x-scala2.12"
            num_workers: 4
            node_type_id: "i3.xlarge"
            aws_attributes:
              availability: SPOT_WITH_FALLBACK

      tasks:
        - task_key: ingest
          job_cluster_key: etl_cluster
          notebook_task:
            notebook_path: ../src/ingest.py
          max_retries: 2

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
```

### Step 5: Define a DLT Pipeline

```yaml
# resources/dlt_pipeline.yml
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
      continuous: false
      development: true   # overridden per target
```

### Step 6: CLI Commands

```bash
# Validate the bundle (syntax + reference checks)
databricks bundle validate

# Deploy to the default target (dev)
databricks bundle deploy

# Deploy to a specific target
databricks bundle deploy --target staging

# Run a specific resource
databricks bundle run sales_etl_job

# Run with parameter overrides
databricks bundle run sales_etl_job \
    --params run_date=2024-01-20

# Destroy all resources created by the bundle
databricks bundle destroy --target dev
```

### Step 7: Environment Promotion Workflow

```
  dev branch           staging branch         main branch
  +----------+         +-----------+          +----------+
  | bundle   |         | bundle    |          | bundle   |
  | deploy   | ------> | deploy    | -------> | deploy   |
  | --target |  merge  | --target  |  merge   | --target |
  | dev      |         | staging   |          | prod     |
  +----------+         +-----------+          +----------+
       |                     |                      |
       v                     v                      v
  Dev Workspace        Staging Workspace      Prod Workspace
```

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Node types | i3.xlarge, m5.xlarge | Standard_DS3_v2 | n1-standard-4 |
| Auth method | PAT, OAuth, AWS profiles | PAT, Azure AD SP | PAT, OAuth |
| workspace.host | *.cloud.databricks.com | *.azuredatabricks.net | *.gcp.databricks.com |
| Spot config | `aws_attributes.availability` | `azure_attributes.availability` | `gcp_attributes.availability` |

Asset Bundles work identically across clouds. Only cluster configurations and
authentication methods differ — the bundle structure itself is cloud-agnostic.
This supports the multicloud strategy: **"Don't marry to a single cloud."**

## Certification Tip

While Asset Bundles are not heavily tested on current Databricks exams, understanding
the concept of infrastructure-as-code for Databricks resources is increasingly
important. Know:
- The purpose of `databricks.yml`
- How targets map to environments
- The relationship between bundles and the Workflows/DLT features you deploy

## Key Takeaways

1. **Asset Bundles** bring infrastructure-as-code to Databricks (jobs, pipelines, dashboards)
2. **`databricks.yml`** is the central configuration file that defines everything
3. **Targets** (dev/staging/prod) enable environment-specific overrides
4. **CLI workflow**: `validate` -> `deploy` -> `run` -> `destroy`
5. **Version control** your bundles alongside source code for full traceability
6. **Service principals** should own production deployments (`run_as`)

## Next Steps

Proceed to [04 - CI/CD Patterns](04-cicd-patterns.md) to learn how to automate
bundle deployments with GitHub Actions, Azure DevOps, and GitLab CI.
