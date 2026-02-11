# Databricks Overview
> Module 00 — Topic 01 | Level: Beginner | Time: 30 min

## Learning Objectives
- Explain what Databricks is and why it exists
- Describe the Lakehouse architecture and its advantages over data lakes and data warehouses
- Identify the core components of a Databricks workspace
- Understand how Databricks relates to and extends Apache Spark
- Recognize Databricks Runtime versions and their purpose

## Conceptual Overview

### What Is Databricks?

Databricks is a unified analytics platform built on top of Apache Spark. It was
founded by the original creators of Spark, Delta Lake, and MLflow. The platform
provides a managed environment for data engineering, data science, machine
learning, and analytics — all on a single collaborative surface.

Think of Databricks as the "managed Spark plus much more" offering. While
open-source Spark gives you a distributed compute engine, Databricks layers on:

- An interactive notebook environment
- Managed cluster lifecycle (provisioning, scaling, termination)
- Delta Lake for reliable data storage
- Unity Catalog for data governance
- MLflow for experiment tracking and model deployment
- SQL warehouses for BI workloads
- Workflow orchestration (Jobs)

### The Lakehouse Architecture

Before the Lakehouse, organizations chose between two architectures:

```
  DATA WAREHOUSE (Traditional)          DATA LAKE (Big Data Era)
  +-------------------------+           +-------------------------+
  |  Structured data only   |           |  All data formats       |
  |  Schema-on-write        |           |  Schema-on-read         |
  |  Great for BI/SQL       |           |  Great for ML/DS        |
  |  Expensive at scale     |           |  Cheap storage          |
  |  Strong governance      |           |  Weak governance        |
  |  No ML support          |           |  Reliability issues     |
  +-------------------------+           +-------------------------+
            |                                     |
            +------ PROBLEMS WITH BOTH -----------+
            |  - Data duplication                  |
            |  - ETL complexity                    |
            |  - Stale data in warehouse           |
            |  - Data swamps in lakes              |
            +--------------------------------------+
```

The Lakehouse combines the best of both:

```
  LAKEHOUSE ARCHITECTURE
  +=====================================================+
  |                   UNITY CATALOG                      |
  |            (Governance & Access Control)              |
  +=====================================================+
  |                                                      |
  |   +-------------+  +-------------+  +-------------+  |
  |   |  SQL / BI   |  | Data Science|  |    Data     |  |
  |   | Warehousing |  |  & ML       |  | Engineering |  |
  |   +------+------+  +------+------+  +------+------+  |
  |          |                |                |          |
  |   +------+----------------+----------------+------+   |
  |   |          Databricks Runtime (Spark)           |   |
  |   +------+----------------+----------------+------+   |
  |          |                |                |          |
  |   +------+----------------+----------------+------+   |
  |   |               DELTA LAKE                      |   |
  |   |   (ACID Transactions, Time Travel, Schema)    |   |
  |   +------+----------------+----------------+------+   |
  |          |                |                |          |
  |   +------+----------------+----------------+------+   |
  |   |          Cloud Object Storage                 |   |
  |   |     (S3 / ADLS Gen2 / GCS)                    |   |
  |   +-----------------------------------------------+   |
  +========================================================+
```

Key Lakehouse principles:
- **Open formats** — data stored as Delta Lake (Parquet + transaction log), not proprietary
- **ACID transactions** — reliable reads and writes, even with concurrent users
- **Schema enforcement** — prevents bad data from entering your tables
- **Time travel** — query previous versions of your data
- **Unified access** — SQL analysts, data engineers, and data scientists all work on the same data

### Workspace Components

A Databricks workspace is your home base. It contains:

```
  DATABRICKS WORKSPACE
  +----------------------------------------------------------+
  |                                                          |
  |  +------------+  +------------+  +--------------------+  |
  |  | Notebooks  |  |  Repos     |  |  Workflows (Jobs)  |  |
  |  +------------+  +------------+  +--------------------+  |
  |                                                          |
  |  +------------+  +------------+  +--------------------+  |
  |  | Clusters   |  | SQL        |  |  Machine Learning  |  |
  |  |            |  | Warehouses |  |  (MLflow, Models)  |  |
  |  +------------+  +------------+  +--------------------+  |
  |                                                          |
  |  +------------+  +------------+  +--------------------+  |
  |  |  DBFS      |  | Unity      |  |  Delta Live Tables |  |
  |  |            |  | Catalog    |  |  (DLT Pipelines)   |  |
  |  +------------+  +------------+  +--------------------+  |
  |                                                          |
  +----------------------------------------------------------+
```

| Component | Purpose |
|-----------|---------|
| **Notebooks** | Interactive code documents supporting Python, SQL, Scala, R |
| **Clusters** | Managed Spark clusters that run your code |
| **Repos** | Git-integrated folder for version-controlled projects |
| **DBFS** | Databricks File System — distributed storage abstraction |
| **SQL Warehouses** | Serverless or classic compute optimized for SQL/BI queries |
| **Workflows/Jobs** | Scheduled and triggered execution of notebooks and pipelines |
| **Unity Catalog** | Centralized governance for data and AI assets |
| **Delta Live Tables** | Declarative ETL framework for building reliable pipelines |
| **MLflow** | Experiment tracking, model registry, and serving |

### Databricks vs. Standalone Spark

| Aspect | Standalone Spark | Databricks |
|--------|-----------------|------------|
| Cluster setup | Manual (YARN, Mesos, K8s) | One-click, fully managed |
| Auto-scaling | Requires manual config | Built-in autoscaling |
| Storage | HDFS / S3 (raw files) | Delta Lake (ACID on object store) |
| Notebooks | Jupyter / Zeppelin (separate) | Native, collaborative |
| Governance | None built-in | Unity Catalog |
| Performance | Vanilla Spark | Photon engine (C++ vectorized) |
| Cost management | You manage infra | Auto-termination, spot instances |
| Collaboration | Git repos + shared drives | Real-time co-editing, comments |

### Databricks Runtime (DBR) Versions

The Databricks Runtime is a curated set of libraries and optimizations that run on
every cluster. Versions follow the pattern `Major.Minor.xLTS`:

| Runtime | Includes | Use Case |
|---------|----------|----------|
| **Standard** (e.g., 15.4 LTS) | Spark, Delta Lake, Python, R, Scala | General workloads |
| **ML Runtime** (e.g., 15.4 ML LTS) | Standard + PyTorch, TensorFlow, Hugging Face, MLflow | Machine learning |
| **Photon** | Standard + Photon engine | High-performance SQL/ETL |
| **GPU Runtime** | ML Runtime + GPU drivers (CUDA) | Deep learning |

LTS (Long Term Support) versions are supported for 2+ years and recommended for
production. Non-LTS versions are supported for approximately 6 months.

**Best practice**: Always choose the latest LTS runtime unless you need a
specific feature from a newer release.

### Pricing Model Overview

Databricks uses a **DBU (Databricks Unit)** model:

```
  Total Cost = Cloud Infrastructure Cost + DBU Cost

  Cloud Infrastructure Cost  -->  Paid to AWS / Azure / GCP
  (VMs, storage, networking)

  DBU Cost                   -->  Paid to Databricks
  (per DBU-hour, varies by workload type and tier)
```

DBU rates vary by:
- **Workload type** — Jobs compute is cheaper than all-purpose (interactive) compute
- **Tier** — Premium tier (required for Unity Catalog) costs more than Standard
- **Cloud provider** — Slight pricing differences across AWS, Azure, GCP
- **Commitment** — Reserved capacity discounts available for steady workloads

## Hands-On Walkthrough

Import the companion notebook `01-databricks-overview_notebook.py` into your
Databricks workspace. The notebook will guide you through:

1. Exploring the SparkSession object
2. Checking runtime version and configuration
3. Listing available databases and tables
4. Creating a simple DataFrame to verify the cluster is running
5. Examining the Spark UI

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Workspace deployment | AWS account + Databricks account | Azure subscription + resource group | GCP project |
| Object storage | S3 | ADLS Gen2 | GCS |
| Identity provider | AWS IAM + SCIM | Azure AD (Entra ID) | Google Identity |
| Networking | VPC with NAT | VNet injection | VPC with Private Google Access |
| Marketplace | AWS Marketplace | Azure Marketplace | GCP Marketplace |
| Serverless SQL | Available | Available | Available |
| Unity Catalog | Supported | Supported | Supported |

## Certification Tip

The **Databricks Certified Data Engineer Associate** exam covers:
- Databricks Lakehouse Platform (24% of exam)
  - Describe the relationship between the data lakehouse and the data warehouse
  - Identify the improvement in data quality that the data lakehouse provides over the data lake
  - Describe the relationship between Databricks and Apache Spark

Know the Lakehouse architecture diagram cold. Be ready to explain why Delta Lake
is essential to the Lakehouse (ACID transactions on object storage).

## Key Takeaways

- Databricks is a unified platform for data engineering, data science, and analytics
- The Lakehouse architecture combines the reliability of warehouses with the flexibility of data lakes
- Delta Lake provides ACID transactions on cloud object storage — it is the foundation of the Lakehouse
- Databricks Runtime versions bundle Spark with optimized libraries; always prefer LTS for production
- Pricing is based on cloud infrastructure (paid to your cloud provider) plus DBUs (paid to Databricks)
- A workspace contains notebooks, clusters, repos, DBFS, jobs, and governance tools

## Next Steps

Proceed to [02 — Cluster Management](02-cluster-management.md) to learn how to
create and configure the compute resources that power your Spark workloads.
