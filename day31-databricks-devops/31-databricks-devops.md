# Databricks Asset Bundles
> Module: DevOps & Platform Engineering | Day 31 | Level: Intermediate | Time: 90 min

## Learning Objectives

After completing this session, you will be able to:
- Explain what Databricks Asset Bundles are and the specific problem they solve
- Understand the anatomy of a `databricks.yml` bundle file and every key section
- Define resources (jobs, DLT pipelines) as code and promote them across environments
- Use variables and target overrides to configure environment-specific behavior
- Integrate DAB into a CI/CD pipeline using GitHub Actions or Azure DevOps
- Execute the full DAB lifecycle: `validate → deploy → run → destroy`

---

## The World Before Asset Bundles

To understand why Asset Bundles exist, you need to feel the pain they replaced.

### The Manual Databricks Deployment Era

Before 2023, deploying Databricks resources meant one of two approaches:

```
  THE MANUAL DEPLOYMENT PROBLEM
  ┌─────────────────────────────────────────────────────────────────────────┐
  │                                                                          │
  │  APPROACH 1: UI-ONLY DEPLOYMENT                                         │
  │  ─────────────────────────────                                          │
  │  Developer clicks through Databricks UI to create a job                │
  │    → Uploads notebook manually                                          │
  │    → Configures cluster (type, runtime, libraries) in the UI           │
  │    → Sets schedule by hand                                              │
  │    → Repeats for dev, staging, prod workspaces (manually again)        │
  │                                                                          │
  │  Problems:                                                               │
  │    ✗ No version control on job configuration                           │
  │    ✗ Prod config drifts from dev config over time                      │
  │    ✗ Impossible to review "what changed" in a PR                       │
  │    ✗ One person's manual mistake deletes a prod schedule               │
  │    ✗ "Works in dev, broken in prod" = invisible config difference      │
  │                                                                          │
  │  APPROACH 2: CUSTOM API SCRIPTS                                         │
  │  ──────────────────────────────                                         │
  │  Team writes a deploy.py script using the Databricks REST API          │
  │    → Calls /api/2.1/jobs/create with a JSON payload                   │
  │    → Maintains separate JSON files per environment                     │
  │    → Script grows to 500 lines; only one person understands it        │
  │                                                                          │
  │  Problems:                                                               │
  │    ✗ Bespoke tooling — every team reinvents the wheel                  │
  │    ✗ No standard structure; different teams do it differently          │
  │    ✗ No built-in validation before deployment                          │
  │    ✗ Environment promotion (dev→staging→prod) is fully manual          │
  │    ✗ Secret management is ad-hoc                                       │
  │                                                                          │
  └─────────────────────────────────────────────────────────────────────────┘
```

### Four Root Causes of Manual Deployment Pain

**1. Configuration is not code**
Job definitions lived in the Databricks UI or in ad-hoc JSON blobs. Nobody could answer "what changed in prod last Tuesday?" without clicking through the UI history.

**2. No environment promotion standard**
Every team had a different answer to "how do we get this from dev to prod?" Some used REST API scripts; some used Terraform; some just manually recreated things in prod.

**3. CI/CD was bolted on, not built in**
Continuous integration tested the Python/SQL code but not the job configuration. You could catch a bug in a notebook but not a misconfigured cluster policy or a missing library.

**4. Scale collapse**
A single data platform team supporting 50 engineers across 10 projects with 200 jobs and 30 pipelines — the manual approach collapses completely. There is no way to manage that surface area without a structured deployment system.

---

## What Are Databricks Asset Bundles?

A **Databricks Asset Bundle (DAB)** is a project structure that lets you define, deploy, and run Databricks resources as code.

```
  DATABRICKS ASSET BUNDLE = CODE + CONFIGURATION + DEPLOYMENT TARGETS
  ┌──────────────────────────────────────────────────────────────────────┐
  │                                                                       │
  │  Your project directory:                                             │
  │                                                                       │
  │  my-pipeline/                                                        │
  │  ├── databricks.yml          ← THE BUNDLE: defines everything        │
  │  ├── resources/                                                      │
  │  │   ├── jobs.yml            ← Job definitions                      │
  │  │   └── pipelines.yml       ← DLT pipeline definitions             │
  │  ├── src/                                                            │
  │  │   ├── bronze_ingestion.py ← Notebook / Python code               │
  │  │   ├── silver_transform.py                                         │
  │  │   └── gold_aggregates.py                                         │
  │  └── tests/                                                          │
  │      └── test_transforms.py  ← Unit tests                           │
  │                                                                       │
  │  DAB CLI commands:                                                   │
  │  $ databricks bundle validate   ← Check YAML is correct             │
  │  $ databricks bundle deploy     ← Create/update resources in target │
  │  $ databricks bundle run <job>  ← Trigger a job run                 │
  │  $ databricks bundle destroy    ← Delete all deployed resources     │
  │                                                                       │
  └──────────────────────────────────────────────────────────────────────┘
```

Key insight: **the bundle is the single source of truth**. Everything about your Databricks project — what jobs exist, how they are configured, what clusters they run on, how they differ between dev and prod — lives in YAML files that are checked into git.

---

## The databricks.yml File: Complete Anatomy

The `databricks.yml` file is the entry point for every bundle. It defines the bundle identity, workspace connections, resource references, and deployment targets.

```yaml
# ── Top-level bundle metadata ──────────────────────────────────────────
bundle:
  name: ecommerce-data-platform     # Must be unique within the workspace

# ── Include additional YAML files ─────────────────────────────────────
# Split large bundles into multiple files for readability
include:
  - resources/jobs.yml
  - resources/pipelines.yml

# ── Variables: parameterize the bundle ────────────────────────────────
variables:
  environment:
    description: "Deployment environment: dev, staging, or prod"
    default: dev

  catalog:
    description: "Unity Catalog catalog to use"
    default: dev_catalog

  schedule_cron:
    description: "CRON expression for job schedule"
    default: "0 */6 * * *"     # every 6 hours in dev

  cluster_node_type:
    description: "Worker node type"
    default: "i3.xlarge"

# ── Resources: what to deploy ─────────────────────────────────────────
resources:
  jobs:
    ecommerce_ingestion:             # key = job identifier in this bundle
      name: "ecommerce-ingestion-${var.environment}"
      description: "Bronze layer ingestion from S3 to Delta"
      tasks:
        - task_key: ingest_orders
          notebook_task:
            notebook_path: ./src/bronze_ingestion.py
            base_parameters:
              catalog: ${var.catalog}
              env:     ${var.environment}
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id:  ${var.cluster_node_type}
            num_workers:   2
            spark_conf:
              "spark.databricks.delta.optimizeWrite.enabled": "true"

        - task_key: transform_silver
          depends_on:
            - task_key: ingest_orders
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

      email_notifications:
        on_failure:
          - ops-team@company.com

# ── Targets: one per environment ──────────────────────────────────────
targets:
  dev:
    mode: development              # adds [dev username] prefix to resource names
    default: true                  # used when no --target flag is passed
    workspace:
      host: https://my-workspace.azuredatabricks.net
    variables:
      catalog: dev_catalog
      schedule_cron: "0 */6 * * *"
      cluster_node_type: "i3.xlarge"

  staging:
    workspace:
      host: https://my-workspace.azuredatabricks.net
    variables:
      catalog: staging_catalog
      schedule_cron: "0 2 * * *"    # nightly at 2am
      cluster_node_type: "i3.2xlarge"

  prod:
    workspace:
      host: https://prod-workspace.azuredatabricks.net
    variables:
      catalog: prod_catalog
      schedule_cron: "0 1 * * *"    # nightly at 1am
      cluster_node_type: "i3.4xlarge"
    run_as:
      service_principal_name: "prod-deploy-sp"
```

### Breaking Down Every Section

```
  DATABRICKS.YML SECTION MAP
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │  bundle:        Identity. Name must be unique per workspace.        │
  │                                                                      │
  │  include:       Modularity. Pull in other YAML files.              │
  │                 Resources can live in resources/jobs.yml,          │
  │                 resources/pipelines.yml, etc.                      │
  │                 DAB merges all included files at deploy time.       │
  │                                                                      │
  │  variables:     Parameterization. Define a variable once,          │
  │                 reference it with ${var.name} syntax.              │
  │                 Each target can override the default value.        │
  │                                                                      │
  │  resources:     What to deploy. Supported types:                   │
  │                   jobs          → Databricks Workflows             │
  │                   pipelines     → Lakeflow/DLT pipelines           │
  │                   dashboards    → Lakeview dashboards              │
  │                   model_serving → Model serving endpoints          │
  │                   experiments   → MLflow experiments               │
  │                   schemas       → Unity Catalog schemas            │
  │                                                                      │
  │  targets:       Environments. Each target is a named deployment    │
  │                 context with its own workspace, variables,         │
  │                 permissions, and resource overrides.               │
  │                                                                      │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## Resource Types in Depth

### Jobs (Workflows)

Jobs are the most commonly bundled resource. A job in a bundle maps directly to a Databricks Workflow.

```yaml
resources:
  jobs:
    medallion_pipeline:
      name: "medallion-pipeline-${var.environment}"
      tasks:
        - task_key: bronze
          notebook_task:
            notebook_path: ./src/bronze.py
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 2

        - task_key: silver
          depends_on: [{task_key: bronze}]
          python_wheel_task:           # deploy a Python wheel, not a notebook
            package_name: my_transforms
            entry_point: run_silver
            parameters: ["--env", "${var.environment}"]

        - task_key: gold
          depends_on: [{task_key: silver}]
          sql_task:
            query:
              query_id: ${var.gold_query_id}   # Databricks SQL query
            warehouse_id: ${var.warehouse_id}

      continuous:
        pause_status: UNPAUSED          # always-on streaming job
```

**Task types you can use in a job:**
- `notebook_task` — run a Databricks notebook
- `python_wheel_task` — run a Python wheel entry point
- `spark_jar_task` — run a Spark JAR
- `sql_task` — run a Databricks SQL query or dashboard refresh
- `dbt_task` — run a dbt project
- `run_job_task` — trigger another job (job chaining)
- `pipeline_task` — trigger a DLT pipeline run

### DLT / Lakeflow Pipelines

```yaml
resources:
  pipelines:
    orders_pipeline:
      name: "orders-dlt-${var.environment}"
      target: ${var.catalog}.silver
      libraries:
        - notebook:
            path: ./src/dlt/bronze_orders.py
        - notebook:
            path: ./src/dlt/silver_orders.py
      configuration:
        catalog: ${var.catalog}
        env:     ${var.environment}
      clusters:
        - label: default
          node_type_id: ${var.cluster_node_type}
          num_workers: 4
      continuous: false              # triggered mode (not continuous)
      development: ${bundle.is_development}  # dev mode in non-prod targets
```

The `${bundle.is_development}` variable is automatically set to `true` when `mode: development` is present on the target, enabling DLT development mode (no automatic data quality enforcement, cheaper clusters).

---

## Targets and Environment Management

Targets are the heart of DAB's environment promotion model. Each target is a named context — dev, staging, prod — that can override any variable, resource property, or workspace.

```
  TARGET OVERRIDE HIERARCHY
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │  Base definition in resources/jobs.yml:                            │
  │    cluster_node_type: ${var.cluster_node_type}                     │
  │    num_workers: 2                                                   │
  │    schedule: ${var.schedule_cron}                                  │
  │                                                                      │
  │  dev target:                                                        │
  │    cluster_node_type = "i3.xlarge"    (small)                      │
  │    schedule_cron = "0 */6 * * *"      (every 6 hours)             │
  │    mode = development → job name becomes "[dev sreekanth] my-job" │
  │                                                                      │
  │  staging target:                                                    │
  │    cluster_node_type = "i3.2xlarge"   (medium)                    │
  │    schedule_cron = "0 2 * * *"        (nightly)                   │
  │                                                                      │
  │  prod target:                                                       │
  │    cluster_node_type = "i3.4xlarge"   (large)                     │
  │    schedule_cron = "0 1 * * *"        (nightly at off-peak)       │
  │    run_as: service_principal_name = "prod-sp"                      │
  │                                                                      │
  │  Same bundle.yml → three completely different deployments          │
  │  One command: databricks bundle deploy --target prod               │
  │                                                                      │
  └─────────────────────────────────────────────────────────────────────┘
```

### Development Mode

When a target has `mode: development`, DAB:
1. Prefixes all resource names with `[dev <username>]` — so `my-job` becomes `[dev sreekanth] my-job`
2. Deploys code to a personal path: `/Users/<username>/.bundle/<bundle>/<target>/`
3. Does not overwrite another developer's deployment (everyone gets their own isolated copy)

This means every engineer on a team can have their own dev deployment of the same bundle without conflicts.

```yaml
targets:
  dev:
    mode: development     # ← enables personal workspace prefix
    default: true
    workspace:
      host: https://my-workspace.azuredatabricks.net
```

---

## Variables and Parameterization

DAB variables let you define a value once and reference it everywhere with `${var.name}` syntax. This eliminates copy-paste errors when configuring environment-specific values.

```yaml
variables:
  # Simple variable with a default
  num_workers:
    default: 2

  # Variable with no default (must be set in each target or passed via CLI)
  warehouse_id:
    description: "SQL Warehouse ID for BI queries"

  # Complex variable: reference a Databricks resource attribute
  cluster_policy_id:
    lookup:
      cluster_policy: "Shared Compute Policy"   # look up policy by name

targets:
  prod:
    variables:
      num_workers: 8            # override for prod
      warehouse_id: abc123def   # prod warehouse ID
```

### Built-in Bundle Variables

DAB provides pre-defined variables you can reference without declaring them:

| Variable | Value |
|----------|-------|
| `${bundle.name}` | Bundle name from `bundle.name` |
| `${bundle.target}` | Current target name (dev/staging/prod) |
| `${bundle.is_development}` | `true` if `mode: development`, else `false` |
| `${workspace.host}` | Workspace URL of the current target |
| `${workspace.current_user.userName}` | Username of the deploying user |
| `${workspace.current_user.shortName}` | Short username (first part before @) |

### Secrets

Never put secret values directly in YAML. Reference Databricks Secrets:

```yaml
# In your job task's environment variables:
tasks:
  - task_key: my_task
    notebook_task:
      notebook_path: ./src/ingest.py
    new_cluster:
      spark_conf:
        "spark.databricks.delta.optimizeWrite.enabled": "true"
    environment_key: prod_env

environments:
  prod_env:
    environment_variables:
      DB_PASSWORD: "{{secrets/prod-scope/db-password}}"   # Databricks Secrets
      API_KEY:     "{{secrets/prod-scope/api-key}}"
```

---

## DAB CLI: The Full Lifecycle

```
  BUNDLE LIFECYCLE COMMANDS
  ─────────────────────────────────────────────────────────────────────────

  1. INITIALIZE (start a new bundle from a template)
  ──────────────────────────────────────────────────
  $ databricks bundle init
    → Interactive wizard: choose template (default, dbt, mlops)
    → Generates databricks.yml and project structure

  2. VALIDATE (check YAML correctness before deploying)
  ──────────────────────────────────────────────────────
  $ databricks bundle validate
  $ databricks bundle validate --target staging
    → Parses YAML, resolves all variables and lookups
    → Outputs the fully-resolved bundle as JSON (no API calls made)
    → ALWAYS run this in CI before deploy

  3. DEPLOY (create or update resources in the workspace)
  ────────────────────────────────────────────────────────
  $ databricks bundle deploy
  $ databricks bundle deploy --target prod
  $ databricks bundle deploy --var="num_workers=8"   # override a variable
    → Syncs code files to workspace file system
    → Creates or updates jobs, pipelines, etc. via Databricks APIs
    → Idempotent: safe to run multiple times

  4. RUN (trigger a job or pipeline run after deploy)
  ────────────────────────────────────────────────────
  $ databricks bundle run ecommerce_ingestion
  $ databricks bundle run ecommerce_ingestion --target prod
  $ databricks bundle run ecommerce_ingestion --python-params='["--date","2024-01-01"]'
    → Triggers a run of the named resource
    → Streams job output to terminal
    → Exits with non-zero code if run fails (useful in CI)

  5. SUMMARY (inspect what has been deployed)
  ────────────────────────────────────────────
  $ databricks bundle summary
  $ databricks bundle summary --target prod
    → Lists all deployed resources with their workspace URLs
    → Shows job IDs, pipeline IDs — useful for monitoring/debugging

  6. DESTROY (delete all bundle-managed resources)
  ──────────────────────────────────────────────────
  $ databricks bundle destroy
  $ databricks bundle destroy --target staging --auto-approve
    → Deletes all resources created by this bundle in the target
    → Does NOT delete files in Unity Catalog (data is preserved)
    → Requires confirmation unless --auto-approve is passed
```

---

## Modular Bundle Structure

Large projects should split resource definitions across multiple files using `include:`.

```
  RECOMMENDED PROJECT STRUCTURE FOR A LARGE BUNDLE
  ┌─────────────────────────────────────────────────────────────────────┐
  │                                                                      │
  │  my-platform/                                                       │
  │  ├── databricks.yml              ← Entry point: targets, variables  │
  │  │                                                                   │
  │  ├── resources/                                                     │
  │  │   ├── ingestion_jobs.yml      ← Bronze layer jobs               │
  │  │   ├── transform_jobs.yml      ← Silver/Gold jobs                │
  │  │   ├── dlt_pipelines.yml       ← DLT pipeline definitions        │
  │  │   └── sql_dashboards.yml      ← Databricks SQL dashboards       │
  │  │                                                                   │
  │  ├── src/                                                           │
  │  │   ├── ingestion/                                                 │
  │  │   │   ├── bronze_orders.py                                      │
  │  │   │   └── bronze_events.py                                      │
  │  │   ├── transforms/                                                │
  │  │   │   ├── silver_orders.py                                      │
  │  │   │   └── gold_revenue.py                                       │
  │  │   └── utils/                                                     │
  │  │       └── common.py                                              │
  │  │                                                                   │
  │  ├── tests/                                                         │
  │  │   └── test_transforms.py                                        │
  │  │                                                                   │
  │  └── .github/                                                       │
  │      └── workflows/                                                 │
  │          ├── pr-validate.yml     ← Run bundle validate on PR       │
  │          └── deploy-prod.yml     ← Deploy on merge to main        │
  │                                                                      │
  │  databricks.yml:                                                    │
  │    bundle:                                                          │
  │      name: my-platform                                             │
  │    include:                                                         │
  │      - resources/*.yml           ← Glob includes all resource files│
  │    targets:                                                         │
  │      dev: ...                                                       │
  │      prod: ...                                                      │
  │                                                                      │
  └─────────────────────────────────────────────────────────────────────┘
```

---

## CI/CD Integration

### GitHub Actions: PR Validation + Prod Deploy

```yaml
# .github/workflows/pr-validate.yml
name: Bundle PR Validation

on:
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: databricks/setup-cli@main    # Install Databricks CLI

      - name: Validate bundle (dev target)
        run: databricks bundle validate --target dev
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

      - name: Validate bundle (prod target)
        run: databricks bundle validate --target prod
        env:
          DATABRICKS_HOST:  ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_DATABRICKS_TOKEN }}
```

```yaml
# .github/workflows/deploy-prod.yml
name: Deploy to Production

on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    environment: production           # requires manual approval in GitHub
    steps:
      - uses: actions/checkout@v4

      - uses: databricks/setup-cli@main

      - name: Deploy to prod
        run: databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST:  ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_SERVICE_PRINCIPAL_TOKEN }}

      - name: Run smoke test
        run: |
          databricks bundle run ecommerce_ingestion \
            --target prod \
            --python-params='["--date","${YESTERDAY}"]'
        env:
          DATABRICKS_HOST:  ${{ secrets.PROD_DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.PROD_SERVICE_PRINCIPAL_TOKEN }}
          YESTERDAY: $(date -d "yesterday" +%Y-%m-%d)
```

### Azure DevOps Pipeline

```yaml
# azure-pipelines.yml
trigger:
  branches:
    include: [main]

pr:
  branches:
    include: [main]

stages:
  - stage: Validate
    displayName: "Validate Bundle"
    jobs:
      - job: ValidateBundle
        pool:
          vmImage: ubuntu-latest
        steps:
          - script: |
              curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
            displayName: "Install Databricks CLI"

          - script: |
              databricks bundle validate --target dev
              databricks bundle validate --target prod
            displayName: "Validate all targets"
            env:
              DATABRICKS_HOST:  $(DATABRICKS_HOST)
              DATABRICKS_TOKEN: $(DATABRICKS_TOKEN)

  - stage: DeployProd
    displayName: "Deploy to Production"
    condition: and(succeeded(), eq(variables['Build.SourceBranch'], 'refs/heads/main'))
    dependsOn: Validate
    jobs:
      - deployment: DeployToProduction
        environment: Production      # requires approval gate in Azure DevOps
        pool:
          vmImage: ubuntu-latest
        strategy:
          runOnce:
            deploy:
              steps:
                - script: |
                    curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
                    databricks bundle deploy --target prod --auto-approve
                  displayName: "Deploy bundle to prod"
                  env:
                    DATABRICKS_HOST:  $(PROD_DATABRICKS_HOST)
                    DATABRICKS_TOKEN: $(PROD_SERVICE_PRINCIPAL_TOKEN)
```

---

## Authentication for CI/CD

DAB reads credentials from environment variables:

```
  AUTHENTICATION METHODS (in priority order)
  ─────────────────────────────────────────────────────────────────────────

  1. DATABRICKS_TOKEN + DATABRICKS_HOST (most common in CI)
     export DATABRICKS_HOST=https://my-workspace.azuredatabricks.net
     export DATABRICKS_TOKEN=dapi1234...
     → Use a Service Principal token, never a personal access token in CI

  2. OAuth (M2M) — recommended for production CI/CD
     Configure in databricks.yml:
       targets:
         prod:
           workspace:
             host: https://...
             auth_type: oauth-m2m
             client_id: ${DATABRICKS_CLIENT_ID}
             client_secret: ${DATABRICKS_CLIENT_SECRET}

  3. Azure Managed Identity (Azure DevOps agents)
     If your runner has an Azure Managed Identity, DAB can use it
     automatically via the Azure CLI credential chain.

  4. run_as (for the job execution identity, not the deployer)
     targets:
       prod:
         run_as:
           service_principal_name: "prod-etl-sp"
     → Jobs run as this service principal in prod (not the deployer)
     → Critical for audit trails: "who ran this job?"
```

---

## Python Artifacts: Deploying Wheels

DAB can build and deploy Python wheels as part of the bundle:

```yaml
# databricks.yml
artifacts:
  my_transforms:
    type: whl
    path: ./python/        # directory containing setup.py or pyproject.toml
    build: "pip install wheel && python setup.py bdist_wheel"

resources:
  jobs:
    transform_job:
      tasks:
        - task_key: run_transform
          python_wheel_task:
            package_name: my_transforms
            entry_point: run_silver
          libraries:
            - whl: ./python/dist/*.whl   # DAB resolves the built artifact path
          new_cluster:
            spark_version: "15.4.x-scala2.12"
            node_type_id: "i3.xlarge"
            num_workers: 4
```

When you run `databricks bundle deploy`, DAB:
1. Runs the build command to produce the wheel
2. Uploads the wheel to the bundle's file path in DBFS or Unity Catalog volumes
3. Configures the job task to use that exact wheel path

This means every deploy gets the fresh wheel — no stale library versions.

---

## Permissions and Access Control

You can define permissions on bundle resources:

```yaml
resources:
  jobs:
    ecommerce_ingestion:
      name: "ecommerce-ingestion-${var.environment}"
      permissions:
        - level: CAN_MANAGE_RUN
          group_name: data-engineers
        - level: CAN_VIEW
          group_name: data-analysts
        - level: IS_OWNER
          service_principal_name: prod-etl-sp
```

Permission levels for jobs:
- `CAN_VIEW` — see job config and run history
- `CAN_MANAGE_RUN` — trigger manual runs
- `CAN_MANAGE` — edit config, delete job
- `IS_OWNER` — full ownership (only one entity can own a job)

---

## What DAB Does Not Replace

Understanding DAB's scope prevents misuse:

```
  DAB IS FOR:                          DAB IS NOT FOR:
  ─────────────────────────────────    ─────────────────────────────────
  ✓ Jobs (Workflows)                   ✗ Unity Catalog schemas/tables
  ✓ DLT / Lakeflow Pipelines          ✗ Cluster policies (use Terraform)
  ✓ Lakeview Dashboards               ✗ Identity/SCIM user management
  ✓ MLflow experiments                ✗ Workspace-level network config
  ✓ Model Serving Endpoints           ✗ Instance pool management
  ✓ UC Schemas (limited support)      ✗ Complex IAM (use Terraform)
  ✓ Notebooks (as code files)         ✗ Cloud infrastructure (VPCs, storage)
```

For infrastructure (VPCs, storage accounts, IAM roles), use **Terraform** with the Databricks Terraform provider. DAB and Terraform complement each other: Terraform provisions the platform, DAB deploys the workloads on top of it.

---

## Cloud Provider Notes

| Aspect | AWS | Azure | GCP |
|--------|-----|-------|-----|
| **Authentication** | Databricks PAT or OAuth M2M | Azure Service Principal (OAuth) or PAT | Google Service Account or PAT |
| **Secret store** | Databricks Secrets (backed by AWS Secrets Manager) | Databricks Secrets (backed by Azure Key Vault) | Databricks Secrets (backed by GCP Secret Manager) |
| **CI runner auth** | IAM Role for EC2 runner (instance profile) | Azure Managed Identity for Azure DevOps agent | Workload Identity Federation for GCP runner |
| **Cluster node types** | `i3.xlarge`, `i3.2xlarge`, etc. | `Standard_DS3_v2`, `Standard_DS4_v2`, etc. | `n2-standard-4`, `n2-standard-8`, etc. |
| **DAB availability** | Full support | Full support | Full support |
| **GitHub Actions integration** | Standard; use IAM role for OIDC | Standard; use Azure federated credentials | Standard; use GCP Workload Identity |

---

## Certification Tip

**Databricks Certified Data Engineer Professional** is the exam most likely to test DAB:
- Understand the purpose and structure of `databricks.yml`
- Know what `mode: development` does to resource names and file paths
- Know that `bundle validate` makes no API calls — it only checks YAML correctness
- Know that `run_as` controls the job execution identity (not the deployer)
- Understand that DAB is built on top of the Databricks REST API
- Know that `bundle destroy` removes resources but does not delete data

Questions often test:
- "How do you ensure the same bundle deploys differently in dev vs prod?" → targets + variables
- "How do you prevent developers' deployments from conflicting?" → `mode: development` adds username prefix
- "What is the correct order of DAB commands in CI?" → validate → deploy → run

---

## Key Takeaways

1. **DAB is infrastructure-as-code for Databricks workloads.** Job configs, pipeline definitions, permissions, and schedules live in YAML files that are versioned in git — just like application code.

2. **`databricks.yml` is the single source of truth.** Everything about a deployment — what code runs, on what cluster, on what schedule — is defined in one place and environment-specific via targets.

3. **Targets enable environment promotion without copy-paste.** The same bundle deploys to dev, staging, and prod by overriding variables and resource properties per target. No parallel JSON files, no manual reconfiguration.

4. **`mode: development` gives every developer their own isolated deployment.** Resources get a personal prefix; code syncs to a personal path. Teams of 50 engineers can each deploy from the same bundle without colliding.

5. **`bundle validate` is your first CI gate.** It resolves all variables and checks correctness without touching the workspace — run it on every PR before any deployment.

6. **DAB complements Terraform.** Terraform provisions cloud infrastructure and Unity Catalog. DAB deploys the Databricks workloads (jobs, pipelines) that run on top of that infrastructure.

---

## Next Steps

- **Day 32**: [Monitoring & Observability](../day32-monitoring-observability/README.md) — instrument your deployed jobs with metrics, alerts, and dashboards
- **Day 09**: [Git Integration](../day09-git-integration-collaboration/README.md) — Repos and Git integration in Databricks workspace
- **Day 23**: [Lakeflow Spark Declarative Pipelines](../day23-lakeflow-spark-declarative-pipelines/README.md) — DLT/Lakeflow pipelines that DAB can deploy
- **Day 24**: [Lakeflow Jobs](../day24-lakeflow-jobs/README.md) — Job orchestration that DAB manages as code
