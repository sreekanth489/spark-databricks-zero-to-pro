# MLflow Tracking & Experiments
> Module 20 -- Topic 01 | Level: Intermediate | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain the core MLflow concepts: experiments, runs, parameters, metrics, artifacts
2. Use `mlflow.log_param()`, `mlflow.log_metric()`, and `mlflow.log_artifact()` for manual tracking
3. Enable autologging with `mlflow.autolog()` and framework-specific autologgers
4. Organize experiments and compare runs in the MLflow UI
5. Understand the MLflow Tracking Server architecture and backend/artifact stores
6. Search and query past runs programmatically with the MLflow client API

---

## Conceptual Overview

### What Is MLflow?

MLflow is an open-source platform for managing the end-to-end machine learning
lifecycle. It was created by Databricks and donated to the Linux Foundation. On
Databricks, MLflow is fully managed -- no server setup, no database configuration,
no artifact storage provisioning. Every workspace has MLflow built in.

MLflow has four components, but this topic focuses on the first:

```
  MLflow Components
  =================

  +-------------------+     +-------------------+
  | MLflow Tracking   |     | Model Registry    |  <-- Topic 02
  | (This topic)      |     | (versions, stages)|
  +-------------------+     +-------------------+
  | Experiments       |     | Models            |
  | Runs              |     | Versions          |
  | Parameters        |     | Aliases           |
  | Metrics           |     | Tags              |
  | Artifacts         |     +-------------------+
  +-------------------+

  +-------------------+     +-------------------+
  | MLflow Models     |     | MLflow Projects   |
  | (packaging)       |     | (reproducibility) |
  +-------------------+     +-------------------+
```

### Core Concepts

**Experiment**: A named container that groups related runs. Think of it as a folder
for a specific modeling task (e.g., "customer-churn-prediction"). Every workspace
has a "Default" experiment. Best practice is to create one experiment per project
or model objective.

**Run**: A single execution of ML code. Each run records:
- **Parameters** -- Input configuration (learning rate, max depth, number of trees)
- **Metrics** -- Output measurements (accuracy, RMSE, F1 score). Metrics can be
  logged at steps for tracking training curves.
- **Artifacts** -- Files produced by the run (trained model, feature importance
  plots, confusion matrix images, data samples)
- **Tags** -- Key-value metadata (author, git commit, environment)
- **Source** -- The notebook or script that created the run

```
  Experiment: customer-churn-v2
  ============================
  |
  +-- Run: 2024-03-15_rf_baseline
  |   params: {n_estimators: 100, max_depth: 10}
  |   metrics: {accuracy: 0.87, f1: 0.82, auc: 0.91}
  |   artifacts: [model/, confusion_matrix.png, feature_importance.csv]
  |
  +-- Run: 2024-03-15_rf_tuned
  |   params: {n_estimators: 500, max_depth: 15, min_samples_leaf: 5}
  |   metrics: {accuracy: 0.89, f1: 0.85, auc: 0.93}
  |   artifacts: [model/, confusion_matrix.png]
  |
  +-- Run: 2024-03-16_xgboost
      params: {n_estimators: 200, learning_rate: 0.1, max_depth: 6}
      metrics: {accuracy: 0.91, f1: 0.88, auc: 0.95}
      artifacts: [model/, shap_summary.png]
```

### Manual Logging vs Autologging

**Manual logging** gives you full control over what gets tracked:

```python
import mlflow

with mlflow.start_run(run_name="rf_baseline"):
    mlflow.log_param("n_estimators", 100)
    mlflow.log_param("max_depth", 10)
    # ... train model ...
    mlflow.log_metric("accuracy", 0.87)
    mlflow.log_metric("f1_score", 0.82)
    mlflow.log_artifact("confusion_matrix.png")
    mlflow.sklearn.log_model(model, "model")
```

**Autologging** captures everything automatically:

```python
mlflow.autolog()  # or mlflow.sklearn.autolog()
# Just train -- MLflow captures params, metrics, model, and more
model.fit(X_train, y_train)
```

Autologging supports: sklearn, TensorFlow, Keras, PyTorch, XGBoost, LightGBM,
Spark MLlib, and more. It captures:
- All constructor parameters
- Training metrics (accuracy, loss curves)
- The trained model artifact
- Feature importance (where applicable)
- Training dataset metadata

### MLflow Tracking Server Architecture

```
  Tracking Architecture on Databricks
  ====================================

  +------------------+       +-----------------------+
  | Notebook / Job   | ----> | MLflow Tracking       |
  | mlflow.log_*()   |       | Server (managed)      |
  +------------------+       +-----------+-----------+
                                         |
                       +-----------------+-----------------+
                       |                                   |
              +--------v--------+               +----------v----------+
              | Backend Store   |               | Artifact Store      |
              | (run metadata)  |               | (models, files)     |
              | MySQL / Postgres|               | DBFS / cloud storage|
              +-----------------+               +---------------------+
              | - run_id        |               | - model binaries    |
              | - params        |               | - plots (PNG, HTML) |
              | - metrics       |               | - data samples      |
              | - tags          |               | - requirements.txt  |
              +-----------------+               +---------------------+

  On Databricks: Both stores are fully managed. No setup required.
  Self-hosted: You configure the backend DB and artifact location.
```

### Experiment Organization Best Practices

| Strategy | When to Use | Example |
|----------|-------------|---------|
| One experiment per model objective | Most common | `/Users/team/churn-prediction` |
| Sub-experiments by approach | Large research projects | `/Users/team/churn/tree-models`, `/Users/team/churn/neural-nets` |
| Tags for metadata | Cross-cutting concerns | `{"team": "marketing", "dataset_version": "v3"}` |
| Nested runs | Hyperparameter sweeps | Parent run = sweep config, child runs = individual trials |

### Querying Past Runs

The MLflow client API lets you search runs programmatically:

```python
import mlflow

client = mlflow.tracking.MlflowClient()
runs = client.search_runs(
    experiment_ids=["123"],
    filter_string="metrics.accuracy > 0.85 AND params.max_depth = '10'",
    order_by=["metrics.f1_score DESC"],
    max_results=10
)
for run in runs:
    print(f"Run {run.info.run_id}: accuracy={run.data.metrics['accuracy']}")
```

---

## Hands-On Walkthrough

Open `01-mlflow-tracking_notebook.py` to practice:
- Creating an experiment and organizing runs
- Training sklearn models with manual parameter/metric logging
- Using autologging to capture everything automatically
- Comparing multiple runs with different hyperparameters
- Logging custom artifacts (plots, data samples)
- Querying past runs with the MLflow client API

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| MLflow Tracking | Managed, included | Managed, included | Managed, included |
| Artifact storage | S3 via DBFS | ADLS Gen2 via DBFS | GCS via DBFS |
| Experiment sharing | Workspace-level ACLs | Workspace-level ACLs | Workspace-level ACLs |
| External MLflow server | EC2 + RDS + S3 | VM + Azure SQL + Blob | GCE + Cloud SQL + GCS |
| Unity Catalog integration | Supported (2023+) | Supported (2023+) | Supported (2023+) |

---

## Certification Tip

> **Databricks ML Professional**: Expect questions about MLflow experiment
> organization, the difference between `log_param` and `log_metric`, when to use
> autologging vs manual logging, and how to query runs using `search_runs()`.
>
> **Key concept**: `mlflow.autolog()` enables automatic logging for all supported
> frameworks. Framework-specific autologgers (e.g., `mlflow.sklearn.autolog()`)
> give finer control. Autologging captures constructor parameters, not
> `fit()` parameters -- manual logging is needed for data preprocessing choices.

---

## Key Takeaways

1. **MLflow Tracking** is the foundation of ML lifecycle management on Databricks.
   Every experiment, run, parameter, metric, and artifact is versioned and searchable.
2. **Autologging** is the fastest way to start tracking. Enable it at the top of
   your notebook and MLflow captures everything from supported frameworks.
3. **Manual logging** is essential for custom metrics, preprocessing parameters,
   and artifacts that autologging does not capture (business metrics, data quality
   scores, custom plots).
4. **Organize experiments** by model objective, not by date or person. Use tags for
   cross-cutting metadata like team, dataset version, or git commit.
5. **The MLflow client API** lets you query and compare runs programmatically --
   essential for automated model selection in CI/CD pipelines.

---

## Next Steps

- Proceed to **Topic 02: Model Registry** to learn how to register your best models,
  manage versions, and promote models through lifecycle stages.
- Apply MLflow tracking to your own models from Modules 04-06.
