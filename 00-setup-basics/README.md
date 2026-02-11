# Module 00: Setup and Basics

> Get your Databricks environment ready and learn the platform fundamentals.

## Prerequisites

- None (this is the starting module)
- A Databricks workspace (Community Edition works for most exercises)
- A web browser with access to your Databricks workspace URL

## Topics

| # | Topic | Guide | Notebook | Time |
|---|-------|-------|----------|------|
| 1 | Databricks Overview | [Guide](01-databricks-overview.md) | [Notebook](01-databricks-overview_notebook.py) | 30 min |
| 2 | Cluster Management | [Guide](02-cluster-management.md) | [Notebook](02-cluster-management_notebook.py) | 35 min |
| 3 | Notebook Fundamentals | [Guide](03-notebook-fundamentals.md) | [Notebook](03-notebook-fundamentals_notebook.py) | 30 min |
| 4 | Databricks Repos & Git | [Guide](04-databricks-repos-git.md) | [Notebook](04-databricks-repos-git_notebook.py) | 25 min |
| 5 | DBFS and Volumes | [Guide](05-dbfs-and-volumes.md) | [Notebook](05-dbfs-and-volumes_notebook.py) | 30 min |

## Learning Path

Start with topic 1 and proceed sequentially. Each topic builds on concepts from the
previous one:

```
01 Databricks Overview
 |
 v
02 Cluster Management
 |
 v
03 Notebook Fundamentals
 |
 v
04 Databricks Repos & Git
 |
 v
05 DBFS and Volumes
```

Each topic has two companion files:

- **Guide (.md)** — conceptual explanation with diagrams, cloud provider notes,
  and certification tips. Read this first.
- **Notebook (.py)** — hands-on Databricks notebook in source format. Import into
  your workspace and run cell by cell.

## How to Import Notebooks

1. Open your Databricks workspace.
2. Navigate to your user folder in the Workspace browser.
3. Right-click and select **Import**.
4. Choose **File** and upload the `_notebook.py` file.
5. Databricks will recognize the `# Databricks notebook source` header and render
   it as a multi-cell notebook.

## What You Will Be Able to Do After This Module

- Explain the Databricks Lakehouse architecture and how it differs from standalone Spark
- Create, configure, and manage clusters with cost-effective settings
- Write and run multi-language notebooks using magic commands, widgets, and dbutils
- Connect a Databricks workspace to a Git repository and manage version control
- Navigate DBFS, create and read files, and understand the transition to Unity Catalog Volumes
- Feel confident moving into Module 01 (DataFrames and Transformations)

## Estimated Total Time

~2.5 hours (reading guides + running notebooks)
