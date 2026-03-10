# Databricks Pro Series: From Lakehouse Engineering to Generative AI

Databricks zero to pro repo for data engineers and AI practitioners to learn Databricks in 55 sessions. This repo includes conceptual guides, runnable notebooks, real-time examples and hands-on projects. Each session provides practical knowledge on Databricks services, allowing you to apply what you've learned and gain production-ready skills.

Course phases: **Foundations → Data Engineering → DevOps → AI**

Alongside the sessions, I'm building this open-source repository and writing structured **Medium articles** based on our discussions so the content is accessible to a broader audience.

Every topic includes a **conceptual guide** (`.md`) and a **runnable Databricks notebook** (`_notebook.py`) that you can import directly into any Databricks workspace (including the free Community Edition).

**Next Session:** March 15, 2026 at 9:00 AM EST — Delta Lake Internals & Production Use Cases

---

## Medium Articles

| # | Topic | Article |
|---|-------|---------|
| 1 | Big Data Evolution | [Why Hadoop, Spark, and Databricks Exist — And Why We Even Need Delta Lake](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148) |
| 14 | Delta Lake Fundamentals | [From Data Lakes to Delta Lake: A Practical Guide for Beginners to Experienced Data Engineers](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30) |
| 15 | Delta Lake Internals | [Inside the Delta Log (Part 1): How _delta_log Builds ACID Transactions](https://medium.com/@sreekanth489/inside-the-delta-log-part-1-how-delta-log-builds-acid-transactions-9a09995fe1fa) |
| 15 | Delta Lake Internals | [Inside the Delta Log (Part 2): What Really Happens When You UPDATE and DELETE](https://medium.com/@sreekanth489/inside-the-delta-log-part-2-what-really-happens-when-you-update-and-delete-330ec8539fc0) |
| 16 | Delta Lake Advanced Features | [Inside the Delta Log (Part 3): Deletion Vectors Internals & Production Tradeoffs](https://medium.com/@sreekanth489/inside-the-delta-log-part-3-deletion-vectors-internals-production-tradeoffs-a3a92d17bbb0) |
| 16 | Delta Lake Advanced Features | [Inside the Delta Log (Part 4): Snapshot Reconstruction, Checkpoints & Time Travel Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-4-snapshot-reconstruction-checkpoints-time-travel-internals-a84210ff0f51) |
| 16 | Delta Lake Advanced Features | [Inside the Delta Log (Part 5): Concurrency, Isolation & Conflict Detection Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-4-snapshot-reconstruction-checkpoints-time-travel-internals-a84210ff0f51) |
| 17 | Data Skipping & Z-ORDER | [Inside the Delta Log (Part 6): Stats, Data Skipping & Z-ORDER Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-6-stats-data-skipping-z-order-internals-59cb7f1c89c7) |

*More articles will be added as the series progresses.*

---

## <img src="https://img.shields.io/badge/Phase_1-Big_Data_Evolution_&_Spark_Fundamentals-E34F26?style=for-the-badge&logo=apachespark&logoColor=white" />

## Day 1: Big Data Evolution

RDBMS limitations, Hadoop ecosystem, Spark distributed computing, emergence of Databricks Lakehouse.

**Article:** [Why Hadoop, Spark, and Databricks Exist — And Why We Even Need Delta Lake](https://medium.com/@sreekanth489/why-hadoop-spark-and-databricks-exist-and-why-we-even-need-delta-lake-235441d5f148)

## Day 2: Distributed Data Systems

HDFS, S3, ADLS, object storage principles, partitioning and distributed file storage.

## Day 3: Apache Spark Architecture

Driver, executors, cluster managers, DAG scheduler, stages, tasks.

## Day 4: Spark Programming Model

RDD vs DataFrame vs Dataset, transformations, actions, lazy execution.

## Day 5: Spark SQL & Catalyst Optimizer

Logical plan, optimized plan, physical plan, cost-based optimization.

## Day 6: Spark Performance Basics

Shuffle, partitioning, skew, repartition vs coalesce.

## <img src="https://img.shields.io/badge/Phase_2-Databricks_Platform-FF3621?style=for-the-badge&logo=databricks&logoColor=white" />

## Day 7: Databricks Platform Overview

Workspace, notebooks, clusters, jobs, repos, collaborative development.

## Day 8: DBFS & Storage Architecture

DBFS abstraction layer, underlying cloud storage mapping.

## Day 9: Git Integration & Collaboration

Repos, GitHub/Bitbucket integration, version control workflows.

## <img src="https://img.shields.io/badge/Phase_3-Unity_Catalog_&_Data_Governance-1B998B?style=for-the-badge&logo=databricks&logoColor=white" />

## Day 10: Unity Catalog Fundamentals

Catalog → Schema → Table hierarchy, governance model.

## Day 11: Unity Catalog Security

RBAC, row-level security, column masking, dynamic views.

## Day 12: Managed vs External Tables

Storage management, lifecycle behavior, table metadata.

## Day 13: Volumes in Databricks

File governance using volumes and Unity Catalog.

## <img src="https://img.shields.io/badge/Phase_4-Delta_Lake-00ADD8?style=for-the-badge&logo=delta&logoColor=white" />

## Day 14: Delta Lake Fundamentals

ACID transactions, schema enforcement, schema evolution.

**Article:** [From Data Lakes to Delta Lake: A Practical Guide for Beginners to Experienced Data Engineers](https://medium.com/@sreekanth489/from-data-lakes-to-delta-lake-a-practical-guide-for-beginners-to-experienced-data-engineers-4571ff129f30)

## Day 15: Delta Lake Internals

Transaction log (_delta_log), commits, checkpoints.

**Articles:** [Inside the Delta Log (Part 1): How _delta_log Builds ACID Transactions](https://medium.com/@sreekanth489/inside-the-delta-log-part-1-how-delta-log-builds-acid-transactions-9a09995fe1fa) | [Part 2: What Really Happens When You UPDATE and DELETE](https://medium.com/@sreekanth489/inside-the-delta-log-part-2-what-really-happens-when-you-update-and-delete-330ec8539fc0)

## Day 16: Delta Lake Advanced Features

Deletion vectors, time travel, vacuum, optimize.

**Articles:** [Inside the Delta Log (Part 3): Deletion Vectors Internals & Production Tradeoffs](https://medium.com/@sreekanth489/inside-the-delta-log-part-3-deletion-vectors-internals-production-tradeoffs-a3a92d17bbb0) | [Part 4: Snapshot Reconstruction, Checkpoints & Time Travel Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-4-snapshot-reconstruction-checkpoints-time-travel-internals-a84210ff0f51) | [Part 5: Concurrency, Isolation & Conflict Detection Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-4-snapshot-reconstruction-checkpoints-time-travel-internals-a84210ff0f51)

## Day 17: Data Skipping & Z-ORDER

Statistics collection, data skipping optimization.

**Article:** [Inside the Delta Log (Part 6): Stats, Data Skipping & Z-ORDER Internals](https://medium.com/@sreekanth489/inside-the-delta-log-part-6-stats-data-skipping-z-order-internals-59cb7f1c89c7)

## <img src="https://img.shields.io/badge/Phase_5-Data_Engineering_Pipelines:_Batch_&_Streaming-FF6F00?style=for-the-badge&logo=apachekafka&logoColor=white" />

## Day 18: Medallion Architecture

Bronze, Silver, Gold layered pipeline architecture.

## Day 19: Structured Streaming

Continuous data processing with Spark Structured Streaming.

## Day 20: Auto Loader

Incremental ingestion using cloudFiles and schema inference.

## Day 21: Change Data Capture

CDC patterns, Auto CDC APIs, change tracking.

## Day 22: SCD Type 2 Pipelines

Historical data management patterns.

## Day 23: Lakeflow Declarative Pipelines

Modern pipeline orchestration using Lakeflow.

## Day 24: Lakeflow Connect

Data ingestion from operational databases.

## <img src="https://img.shields.io/badge/Phase_6-Performance_&_Cost_Optimization-7B2FF7?style=for-the-badge&logo=speedtest&logoColor=white" />

## Day 25: Performance Engineering

Photon engine, AQE, query optimization.

## Day 26: Cluster & Cost Optimization

Cluster sizing, autoscaling, job clusters vs interactive clusters.

## <img src="https://img.shields.io/badge/Phase_7-Interoperability_&_Open_Table_Formats-0D47A1?style=for-the-badge&logo=apacheiceberg&logoColor=white" />

## Day 27: Open Table Formats

Delta vs Iceberg vs Hudi, interoperability patterns.

## Day 28: Lakehouse Federation

Querying external systems without copying data.

## Day 29: Delta Sharing

Secure cross-organization data sharing.

## <img src="https://img.shields.io/badge/Phase_8-Databricks_DevOps-2E7D32?style=for-the-badge&logo=githubactions&logoColor=white" />

## Day 30: Databricks DevOps

CI/CD pipelines, Databricks Asset Bundles, automated deployments.

## Day 31: Monitoring & Observability

Jobs monitoring, logging, alerts, debugging pipelines.

## <img src="https://img.shields.io/badge/Phase_9-Databricks_Analytics_Layer-F9A825?style=for-the-badge&logo=databricks&logoColor=white" />

## Day 32: Databricks SQL & BI

Dashboards, SQL warehouses, query acceleration.

## <img src="https://img.shields.io/badge/Phase_10-Databricks_Machine_Learning_Platform-AD1457?style=for-the-badge&logo=mlflow&logoColor=white" />

## Day 33: MLflow Fundamentals

Experiment tracking, model registry.

## Day 34: Model Serving

Deploying models and Interface endpoints.

## <img src="https://img.shields.io/badge/Phase_11-Databricks_Generative_AI-6A1B9A?style=for-the-badge&logo=openai&logoColor=white" />

## Day 35: Introduction to Generative AI

LLM fundamentals, prompt engineering concepts.

## Day 36: Data Preparation for AI

Document ingestion, preprocessing, chunking strategies.

## Day 37: Embeddings & Vector Databases

Embedding generation, cosine similarity, vector indexing.

## Day 38: Databricks Vector Search

Managed vector database for RAG applications.

## Day 39: RAG Architecture

Retrieval Augmented Generation pipelines.

## Day 40: Building Knowledge Assistants

Agent frameworks and AI assistants.

## Day 41: Genie Space

Natural language analytics interface for business users.

## Day 42: Model Access in Databricks

Using hosted models and external models.

## Day 43: Context Window Optimization

Prompt structuring, batching and caching.

## Day 44: LLM Cost Optimization

Model selection, open source vs proprietary models.

## Day 45: AI Safety & Guardrails

Hallucination mitigation, evaluation frameworks.

## Day 46: AI Governance & Compliance

Handling PII, security, SOC2/HIPAA considerations.

## Day 47: Distributed Model Inference

Serving models using GPUs and scalable inference. Scaling model inference with GPUs.

## Day 48: Building End-to-End AI Applications

Combining data pipelines, vector search and LLMs.

## <img src="https://img.shields.io/badge/Phase_12-Databricks_Agentic_AI-00695C?style=for-the-badge&logo=robot&logoColor=white" />

## Day 49: Agentic AI Concepts

Autonomous agents, planning, reasoning.

## Day 50: Agent Tools & Function Calling

Tool usage and external system integration.

## Day 51: Building Agents on Databricks

Agent orchestration with data tools and models.

## Day 52: Multi-Agent Systems

Coordinator agents and worker agents.

## Day 53: Agent Observability

Monitoring agent workflows and debugging.

## Day 54: Building End-to-End AI Applications

Combining data pipelines, vector search and LLMs. Building a lakehouse-powered AI platform.

## Day 55: Capstone Project

End-to-end lakehouse pipeline with RAG application built on Databricks. End-to-end pipeline: ingestion → Delta → vector search → AI agent.

---

## Quick Start

### 1. Set up a Databricks workspace

Use the free **Community Edition** — no cloud account required.
See [docs/setup-community-edition.md](docs/setup-community-edition.md) for step-by-step instructions.

### 2. Import notebooks

Each `_notebook.py` file uses [Databricks source format](https://docs.databricks.com/en/notebooks/notebook-format.html) and can be imported directly:

1. In your workspace, click **Import** in the sidebar
2. Choose **File** and upload any `_notebook.py` file
3. The notebook opens ready to run — all sample data is generated in-cell

See [docs/importing-notebooks.md](docs/importing-notebooks.md) for detailed instructions.

### 3. Start learning

Begin with Day 1 and follow the roadmap, or jump to any session that matches your level.

---

## Repository Structure

```
spark-databricks-zero-to-pro/
├── 00-setup-basics/               # Module directories
│   ├── README.md                  #   Module overview & table of contents
│   ├── 01-topic-name.md           #   Conceptual guide
│   └── 01-topic-name_notebook.py  #   Runnable Databricks notebook
├── ...
├── docs/                          # Setup guides, glossary
│   ├── setup-community-edition.md
│   ├── importing-notebooks.md
│   └── glossary.md
├── resources/
│   └── data-generators/           # Shared data generation utilities
│       ├── generator_utils.py
│       └── generate_ecommerce.py
├── CLAUDE.md                      # AI assistant conventions
├── .gitignore
└── README.md                      # This file
```

---

## Design Principles

- **Self-contained notebooks** — every notebook generates its own sample data; no dependency on prior notebooks
- **Cloud-agnostic** — generic code with AWS / Azure / GCP difference tables where relevant
- **Zero external dependencies** — only libraries in Databricks Runtime (PySpark, pandas, numpy)
- **Progressive difficulty** — within and across modules, from beginner to advanced
- **Certification-aligned** — every guide maps to Databricks exam domains

---

## Contributing

1. Follow the naming conventions in [CLAUDE.md](CLAUDE.md)
2. Every topic needs both a `.md` guide and a `_notebook.py` notebook
3. Notebooks must use Databricks source format and be self-contained
4. Test notebooks on Databricks Community Edition before submitting

## License

This project is for educational purposes.
