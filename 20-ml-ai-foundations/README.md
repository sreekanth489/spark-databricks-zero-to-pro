# Module 20 -- ML/AI Foundations

> Build end-to-end machine learning workflows on Databricks using MLflow,
> Feature Store, AutoML, Model Serving, and PySpark MLlib.

---

## Why This Module Matters

Machine learning on Databricks is not just about training models -- it is about
managing the entire ML lifecycle from experiment tracking to production serving.
Databricks provides an integrated platform where data engineers and data scientists
share the same infrastructure, the same governance layer (Unity Catalog), and the
same operational tooling (MLflow). This module teaches you to use every component
in that stack so you can build ML systems that are reproducible, auditable, and
production-ready.

Whether you are a data engineer who needs to understand how ML pipelines fit into
your lakehouse architecture, or a data scientist preparing for the Databricks
Machine Learning Professional certification, these six topics give you the
foundational skills you need.

---

## Prerequisites

| Requirement | Why |
|-------------|-----|
| **Modules 00-09 completed** | You need working knowledge of PySpark, Delta Lake, Spark SQL, and Databricks workflows |
| **Full Databricks workspace (recommended)** | MLflow UI, Model Registry, Feature Store, and Model Serving require a full workspace -- Community Edition has limited ML support |
| **Basic ML familiarity** | Understanding of train/test splits, classification vs regression, and evaluation metrics (accuracy, RMSE) helps but is not strictly required |

> **Community Edition users**: Every notebook generates its own sample data and
> runs core demonstrations using sklearn and pandas locally. Databricks-specific
> features (Model Registry UI, Feature Store tables, Serving endpoints) are
> provided as commented templates with explanatory output so you can follow along.

---

## Table of Contents

| # | Topic | Guide | Notebook | Time | Level |
|---|-------|-------|----------|------|-------|
| 01 | MLflow Tracking & Experiments | [Guide](01-mlflow-tracking.md) | [Notebook](01-mlflow-tracking_notebook.py) | 55 min | Intermediate |
| 02 | Model Registry | [Guide](02-model-registry.md) | [Notebook](02-model-registry_notebook.py) | 50 min | Intermediate |
| 03 | Feature Store | [Guide](03-feature-store.md) | [Notebook](03-feature-store_notebook.py) | 55 min | Intermediate |
| 04 | AutoML | [Guide](04-automl.md) | [Notebook](04-automl_notebook.py) | 45 min | Intermediate |
| 05 | Model Serving | [Guide](05-model-serving.md) | [Notebook](05-model-serving_notebook.py) | 50 min | Advanced |
| 06 | PySpark ML Pipeline | [Guide](06-pyspark-ml-pipeline.md) | [Notebook](06-pyspark-ml-pipeline_notebook.py) | 60 min | Intermediate |

**Total estimated time: ~5.5 hours**

---

## Learning Path

```
  Module 20 Learning Flow
  ========================

  01-MLflow Tracking & Experiments
    |
    |  Learn to track experiments, log parameters, metrics, and
    |  artifacts. Understand autologging and the MLflow UI.
    |  Foundation for everything that follows.
    |
    v
  02-Model Registry
    |
    |  Register trained models, manage versions, promote through
    |  lifecycle stages, and use Unity Catalog model registry.
    |  Builds on MLflow tracking from Topic 01.
    |
    v
  03-Feature Store
    |
    |  Create and manage feature tables. Ensure consistency
    |  between training and serving. Point-in-time lookups.
    |  Uses models registered in Topic 02.
    |
    v
  04-AutoML
    |
    |  Let Databricks AutoML generate baseline models and
    |  notebooks. Understand the generated code and customize it.
    |  Leverages MLflow tracking from Topic 01.
    |
    v
  05-Model Serving
    |
    |  Deploy models to real-time REST endpoints. Configure
    |  scaling, A/B testing, and monitoring. Uses models from
    |  the registry (Topic 02).
    |
    v
  06-PySpark ML Pipeline
    |
    |  Build distributed ML pipelines with Spark MLlib.
    |  Feature engineering at scale, cross-validation, and
    |  pipeline persistence. The Spark-native ML approach.
```

---

## Key Concepts at a Glance

- **MLflow Tracking** -- The open-source standard for experiment tracking. Databricks
  integrates it natively so every notebook has access to `mlflow` without installation.
  Tracks parameters, metrics, artifacts (models, plots, data samples) organized by
  experiments and runs.

- **Model Registry** -- Central hub for model versioning and lifecycle management.
  Unity Catalog model registry (the modern approach) provides three-level namespace
  governance (`catalog.schema.model`). Supports aliases like "Champion" and
  "Challenger" for deployment workflows.

- **Feature Store** -- Ensures the same feature computation is used in training and
  serving, eliminating training-serving skew. Feature tables are Delta tables with
  metadata. Supports point-in-time lookups for time-series features.

- **AutoML** -- Democratizes ML by automatically exploring features, trying multiple
  algorithms, tuning hyperparameters, and generating editable notebooks. Think of it
  as a senior data scientist's first pass that you can then customize.

- **Model Serving** -- Provides low-latency REST endpoints for real-time predictions.
  Supports serverless compute, GPU acceleration, A/B testing with traffic splitting,
  and integration with MLflow model signatures.

- **PySpark MLlib** -- Spark's distributed ML library for training at scale. The
  Pipeline API (Transformers, Estimators, Pipeline) ensures reproducible workflows.
  Better than sklearn when data exceeds single-node memory.

---

## Important Notes

1. **Self-contained notebooks** -- Every notebook generates its own sample data and
   cleans up after itself. No external datasets are required.

2. **Databricks-specific features** are shown with both runnable local alternatives
   (using sklearn/pandas) and Databricks API templates (as commented code blocks).
   This dual approach lets you learn the concepts on any environment while seeing
   the exact Databricks APIs.

3. **Unity Catalog** is the recommended governance layer for models and features.
   Workspace-level model registry still works but is considered legacy. All examples
   show both approaches where applicable.

4. **Certification relevance** -- MLflow tracking, model registry, Feature Store,
   and MLlib pipelines are tested on the Databricks Machine Learning Professional
   exam. Look for "Certification Tip" callouts in each guide.

---

## Next Steps

After completing this module, proceed to:
- **Module 21** -- GenAI & LLM Use Cases (apply ML foundations to large language models)
- **Module 22** -- Agentic AI on Databricks (build AI agents using Databricks tooling)
