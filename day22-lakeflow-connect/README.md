# Day 22: Lakeflow Connect

> Module: Data Engineering Pipelines | Level: Intermediate | Time: 60 min

## Learning Objectives

- Understand Lakeflow Connect as the ingestion layer of the Lakeflow ecosystem
- Differentiate between Manual File Uploads, Standard Connectors, and Managed Connectors
- Implement Standard Connectors using Auto Loader, batch reads, and Kafka streaming
- Configure Managed Connectors via the Databricks UI for database and SaaS ingestion
- Integrate Lakeflow Connect with Unity Catalog for governance and observability
- Choose the right ingestion method for a given use case

## Key Concepts

- **Lakeflow Connect** -- the ingestion component of Databricks Lakeflow; brings data INTO the Lakehouse from external sources
- **Manual File Upload** -- upload local files directly to a Unity Catalog volume or table via the Databricks UI
- **Standard Connectors** -- code-based ingestion from cloud storage (Auto Loader), Kafka, JDBC, and other Spark-supported sources
- **Managed Connectors** -- no-code, serverless, UI-driven connectors for databases (PostgreSQL, MySQL, SQL Server, Oracle) and SaaS apps (Salesforce, Workday, etc.)
- **Ingestion Modes** -- batch (full load), incremental batch (new/changed rows only), streaming (continuous real-time)
- **CDC-based Ingestion** -- Managed Connectors use Change Data Capture for efficient incremental reads from databases
- **Serverless Compute** -- Managed Connectors run on serverless infrastructure with automatic scaling

## Topics Covered

- The Lakeflow ecosystem: Connect -> Spark Declarative Pipelines -> Jobs
- Three ingestion types and when to use each
- Standard Connector patterns: Auto Loader, JDBC batch, Kafka streaming
- Managed Connector architecture: no-code setup, schema inference, CDC
- Auto Loader as the standard connector for cloud file ingestion
- Unity Catalog integration for access control and lineage
- Serverless compute and auto-scaling for Managed Connectors
- Comparing ingestion methods across latency, complexity, and governance

## Prerequisites

- [Day 10: Unity Catalog Fundamentals](../day10-unity-catalog-fundamentals/)
- [Day 19: Structured Streaming](../day19-structured-streaming/)
- [Day 20: Auto Loader](../day20-auto-loader/)

## Hands-On

- **Guide**: [`22-lakeflow-connect.md`](22-lakeflow-connect.md) -- theory, architecture, ingestion patterns, and cloud notes
- **Notebook**: [`22-lakeflow-connect_notebook.py`](22-lakeflow-connect_notebook.py) -- runnable Databricks lab with Standard Connectors, Managed Connector walkthrough, and manual upload
- **Lab Scripts**:
  - [`lab-scripts/standard_connector_autoloader.py`](lab-scripts/standard_connector_autoloader.py) -- Auto Loader ingestion pattern
  - [`lab-scripts/standard_connector_batch.py`](lab-scripts/standard_connector_batch.py) -- JDBC batch ingestion pattern
  - [`lab-scripts/standard_connector_kafka.py`](lab-scripts/standard_connector_kafka.py) -- Kafka streaming ingestion pattern
  - [`lab-scripts/managed_connector_setup.sql`](lab-scripts/managed_connector_setup.sql) -- SQL commands for Managed Connector setup

## Certification Tip

The Databricks Certified Data Engineer Associate exam tests:
- Knowing that Lakeflow Connect is the ingestion component of Lakeflow
- Differences between Standard Connectors and Managed Connectors
- Auto Loader as a Standard Connector for cloud file ingestion
- Managed Connector capabilities: serverless, no-code, CDC-based
- Unity Catalog governance over ingested data

## Next Steps

- [Day 23: SCD Type 2 Pipelines](../day23-scd-type-2-pipelines/)
