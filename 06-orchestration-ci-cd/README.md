# Module 06: Orchestration & CI/CD

> Productionize your Spark pipelines with scheduling, monitoring, and deployment automation.

## Why This Module Matters

Building a Spark transformation is only half the story. In production, pipelines must
run on schedule, recover from failures, enforce data quality, and deploy safely across
environments. This module bridges the gap between *"it works on my cluster"* and
*"it runs reliably every day at 2 AM and pages me if something breaks."*

As the domain expert puts it: **"How Cloud Data Engineering will drive real business
value"** depends entirely on whether your pipelines are automated, monitored, and
cost-optimized in production.

## Prerequisites

- **Modules 01-05** completed (Spark fundamentals, Delta Lake, transformations,
  performance tuning, Structured Streaming)
- A Databricks workspace (Community Edition works for most exercises; some features
  like Delta Live Tables require a paid tier)
- Basic familiarity with Git and YAML syntax

## Topics

| # | Topic | Focus | Time |
|---|-------|-------|------|
| 01 | [Databricks Workflows](01-databricks-workflows.md) | Job orchestration, scheduling, cost optimization | 50 min |
| 02 | [Delta Live Tables](02-delta-live-tables.md) | Declarative pipelines, data quality, medallion architecture | 55 min |
| 03 | [Asset Bundles (DABs)](03-asset-bundles.md) | Infrastructure-as-code for Databricks resources | 45 min |
| 04 | [CI/CD Patterns](04-cicd-patterns.md) | Testing, branching, promotion across environments | 50 min |
| 05 | [Databricks Connect](05-databricks-connect.md) | Local IDE development against remote clusters | 40 min |

## Learning Path

```
01-Workflows ──> 02-DLT ──> 03-Asset-Bundles ──> 04-CI/CD ──> 05-DB-Connect
   (run)         (declare)     (package)          (deploy)      (develop)
```

Topics 01 and 02 cover *what* runs in production. Topic 03 teaches you to *package*
those resources as code. Topic 04 shows how to *deploy* them safely. Topic 05 closes
the loop by improving the *development* experience.

## Key Themes

- **Cost optimization** — Job clusters vs all-purpose clusters; right-sizing compute
- **Data quality** — DLT expectations; testing patterns for Spark code
- **Environment promotion** — dev / staging / prod with consistent deployments
- **Multicloud readiness** — Patterns that work on AWS, Azure, and GCP

## Companion Notebooks

Each topic includes a `_notebook.py` file that can be imported directly into a
Databricks workspace. Some topics (Workflows, DLT, Asset Bundles) contain reference
code rather than directly executable cells, since those features require specific
platform configuration. The notebooks clearly mark which cells are executable and
which are reference examples.

## Next Module

**Module 07 — Unity Catalog & Governance**: Data access control, lineage, Lakehouse
Federation, and PII protection strategies.
