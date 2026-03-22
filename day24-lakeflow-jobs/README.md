# Day 24: Lakeflow Jobs

> Module: Data Engineering Orchestration | Level: Intermediate | Time: 60 min

## Learning Objectives

- Understand how Lakeflow Jobs orchestrate multi-task workflows on Databricks
- Build DAGs with sequential, parallel, and conditional task dependencies
- Configure trigger modes: manual, scheduled, continuous, and file arrival
- Use repair runs to re-execute only failed tasks without reprocessing successful ones
- Pass dynamic parameters to tasks using widgets and job parameters
- Deploy jobs across environments with Databricks Asset Bundles (DAB)
- Set up notifications (email, Slack, webhooks) and RBAC permissions

## Key Concepts

- **Lakeflow Jobs** -- the orchestration layer of the Lakeflow platform; composes notebooks, Python scripts, SQL, SDP pipelines, JARs, and dbt tasks into a single workflow
- **Multi-task Job** -- a job containing multiple tasks arranged as a directed acyclic graph (DAG)
- **Task Dependencies** -- define execution order: sequential, fan-out/fan-in, or conditional
- **Trigger Modes** -- manual (on-demand), scheduled (cron), continuous (always running), file arrival (event-driven)
- **Repair Runs** -- re-run only the failed tasks in a job run, preserving successful task results
- **Smart Retries** -- configurable retry policies per task with backoff
- **Databricks Asset Bundles (DAB)** -- CI/CD tool for packaging and deploying jobs, pipelines, and notebooks across environments
- **Serverless Compute** -- run jobs without managing clusters; Databricks provisions and scales infrastructure automatically

## Prerequisites

- [Day 22: Lakeflow Connect](../day22-lakeflow-connect/) -- data ingestion into the Lakehouse
- [Day 23: Lakeflow Spark Declarative Pipelines](../day23-lakeflow-spark-declarative-pipelines/) -- declarative pipeline development
- [Day 18: Medallion Architecture](../day18-medallion-architecture/) -- Bronze/Silver/Gold layering

## Hands-On

- **Guide**: [`24-lakeflow-jobs.md`](24-lakeflow-jobs.md) -- comprehensive theory, architecture diagrams, and cloud-specific notes
- **Notebook**: [`24-lakeflow-jobs_notebook.py`](24-lakeflow-jobs_notebook.py) -- interactive notebook covering job creation, DAG patterns, triggers, and the Databricks SDK
- **Lab Scripts**: [`lab-scripts/`](lab-scripts/) -- production-ready scripts for job creation, DAB configuration, and sample tasks

## Certification Tip

Lakeflow Jobs (referenced as "Workflows" or "Jobs" in exam materials) is tested on the **Databricks Certified Data Engineer Professional** exam. Expect questions on:
- Multi-task job DAG design (sequential, fan-out/fan-in, conditional)
- Choosing the right trigger mode for a given scenario
- Repair runs vs full re-runs for failure recovery
- Cluster strategies: job clusters vs all-purpose vs serverless
- Databricks Asset Bundles for CI/CD deployment

## Next Steps

- [Day 25: SCD Type 2 Pipelines](../day25-scd-type-2-pipelines/)
