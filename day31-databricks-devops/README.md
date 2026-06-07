# Day 31: Databricks Asset Bundles

CI/CD pipelines and infrastructure-as-code for Databricks workloads using Databricks Asset Bundles (DAB).

## Topics Covered

- The manual deployment problem: config drift, no version control, no CI/CD
- DAB architecture: `databricks.yml`, `bundle:`, `variables:`, `resources:`, `targets:`
- Resource types: jobs (Workflows), DLT/Lakeflow pipelines, dashboards, model serving endpoints
- Variable substitution (`${var.name}`) and environment-specific overrides
- Development mode: per-developer isolated deployments with name prefixes
- Full DAB CLI lifecycle: `validate → deploy → run → summary → destroy`
- Modular bundles: `include:` to split resources across files
- Python artifact deployment: building and deploying wheels with `artifacts:`
- Authentication: service principals, OAuth M2M, `run_as` for job execution identity
- CI/CD integration: complete GitHub Actions and Azure DevOps pipeline examples
- Configuration drift detection: comparing deployed state vs bundle definition

## Guide

[31-databricks-devops.md](31-databricks-devops.md) — 90 min read

## Notebook

[31-databricks-devops_notebook.py](31-databricks-devops_notebook.py) — hands-on lab

### What the notebook covers
1. Manual deployment pain: raw REST API JSON configs, config drift simulation
2. `databricks.yml` anatomy: complete annotated example with all sections explained
3. `mode: development`: simulate personal namespace prefixes for every developer
4. Variable resolution: how `${var.name}` resolves differently per target
5. Databricks SDK: create/update/run/delete jobs (what DAB does under the hood)
6. Inspect deployed resources: get job config, run history, resource URLs
7. Configuration drift detection: compare deployed state vs expected state
8. CI/CD workflow: full GitHub Actions YAML for PR validation and prod deploy
9. Production patterns: service principals, `run_as`, pinned CLI versions

## Prerequisites

- Databricks Runtime 13.3+ (for Databricks SDK)
- `CREATE TABLE` privilege in a catalog schema
- The hands-on DAB CLI lab (outside the notebook) requires:
  - Databricks CLI installed (`pip install databricks-cli` or `brew install databricks`)
  - A personal access token or service principal configured

## Time Estimate

- Guide: 90 min
- Notebook: 45 min
- Optional: DAB CLI lab (create a real bundle and deploy it) — 60 min
