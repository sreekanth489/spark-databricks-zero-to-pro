# Day 31 — Bundle Demo: E-Commerce Medallion Pipeline

A complete, runnable Databricks Asset Bundle that deploys a three-task
Bronze → Silver → Gold pipeline across dev, staging, and prod environments.

## Project Structure

```
bundle-demo/
├── databricks.yml                  ← Entry point: variables + targets
├── resources/
│   ├── jobs.yml                    ← Medallion pipeline job definition
│   └── pipelines.yml              ← DLT pipeline definition
├── src/
│   ├── bronze_ingestion.py        ← Task 1: generate + write bronze Delta table
│   ├── silver_transform.py        ← Task 2: clean + enrich → silver
│   ├── gold_aggregates.py         ← Task 3: aggregate → gold (BI-ready)
│   └── dlt_silver_pipeline.py     ← DLT version of the silver layer
└── .github/workflows/
    ├── pr-validate.yml             ← Validate all targets on every PR
    └── deploy.yml                  ← dev → staging → prod (with approval gate)
```

## Prerequisites

```bash
# Install the Databricks CLI
pip install databricks-cli
# or
brew install databricks

# Authenticate to your workspace
databricks configure
# Enter: host URL, personal access token
```

## Quick Start

```bash
cd day31-databricks-devops/bundle-demo

# 1. Replace workspace URLs in databricks.yml
#    targets.dev.workspace.host    → your dev workspace URL
#    targets.staging.workspace.host → your staging workspace URL
#    targets.prod.workspace.host   → your prod workspace URL

# 2. Validate (no API calls — pure YAML check)
databricks bundle validate

# 3. Deploy to dev (your personal isolated copy)
databricks bundle deploy

# 4. Run the pipeline
databricks bundle run ecommerce_medallion_pipeline

# 5. Check what was deployed
databricks bundle summary
```

## Environment Comparison

| Config | dev | staging | prod |
|--------|-----|---------|------|
| Resource name prefix | `[dev <username>]` | none | none |
| Catalog | `dev_catalog` | `staging_catalog` | `prod_catalog` |
| Node type | `i3.xlarge` | `i3.2xlarge` | `i3.4xlarge` |
| Workers | 2 | 4 | 8 |
| Schedule | every 6h (PAUSED) | nightly 3am | nightly 1am |
| Job runs as | deploying user | deploying user | service principal |
| DLT dev mode | `true` | `false` | `false` |

## Deploying to a Specific Target

```bash
# Deploy to dev (default)
databricks bundle deploy

# Deploy to staging
databricks bundle deploy --target staging

# Deploy to prod
databricks bundle deploy --target prod

# Override a variable at deploy time
databricks bundle deploy --target dev --var="job_num_workers=4"
```

## Running the Pipeline

```bash
# Trigger the full pipeline (all three tasks)
databricks bundle run ecommerce_medallion_pipeline --target dev

# Run a single task only (useful for debugging)
databricks bundle run ecommerce_medallion_pipeline --target dev --task ingest_bronze

# Run the DLT pipeline
databricks bundle run orders_dlt_pipeline --target dev
```

## Tear Down

```bash
# Remove all deployed resources (keeps data in Unity Catalog)
databricks bundle destroy --target dev
```

## CI/CD

Two GitHub Actions workflows are included:

- **pr-validate.yml** — runs `bundle validate` on every PR (no API calls, instant)
- **deploy.yml** — on merge to main: deploys dev → staging → prod with a manual approval gate before prod

Set these secrets in your GitHub repository:
- `DEV_DATABRICKS_HOST`, `DEV_DATABRICKS_TOKEN`
- `STAGING_DATABRICKS_HOST`, `STAGING_DATABRICKS_TOKEN`
- `PROD_DATABRICKS_HOST`, `PROD_DATABRICKS_TOKEN`
