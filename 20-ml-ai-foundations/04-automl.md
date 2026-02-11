# AutoML
> Module 20 -- Topic 04 | Level: Intermediate | Time: 45 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain how Databricks AutoML works and when to use it
2. Describe the three problem types AutoML supports: classification, regression, forecasting
3. Understand what AutoML generates: trial notebooks, a best model, and a leaderboard
4. Configure AutoML runs with custom metrics, feature exclusions, and time budgets
5. Interpret AutoML results and customize the generated notebooks
6. Integrate AutoML outputs with MLflow tracking and the Model Registry

---

## Conceptual Overview

### What Is AutoML?

Databricks AutoML is a glass-box automated machine learning system. Given a dataset
and a target column, it:
1. Explores the data (statistics, correlations, null rates)
2. Selects candidate algorithms (LightGBM, XGBoost, sklearn models)
3. Engineers features automatically (imputation, encoding, scaling)
4. Tunes hyperparameters (random search or Bayesian optimization)
5. Evaluates models with cross-validation
6. Ranks results on a leaderboard

The "glass-box" part is critical: AutoML generates **editable source notebooks**
for every trial. You can open, read, modify, and run these notebooks yourself.
This is not a black box -- it is a senior data scientist's first pass that you
can customize.

```
  AutoML Workflow
  ================

  +------------------+     +-------------------+     +------------------+
  | Input Dataset    | --> | AutoML Engine     | --> | Outputs          |
  | (Delta table or  |     | 1. Data profiling |     | - Leaderboard    |
  |  pandas DataFrame|     | 2. Feature eng    |     | - Best model     |
  |  + target column)|     | 3. Model training |     | - Trial notebooks|
  +------------------+     | 4. Hyperparam     |     | - Data profile   |
                           |    tuning         |     | - MLflow runs    |
                           | 5. Evaluation     |     +------------------+
                           +-------------------+

  Each trial is a full MLflow run with params, metrics, and model artifacts.
  Each trial notebook is a standalone, runnable Python notebook.
```

### Problem Types

| Problem Type | Target Column | Algorithms Tried | Primary Metric |
|-------------|---------------|------------------|----------------|
| **Classification** | Categorical (binary or multiclass) | LightGBM, XGBoost, sklearn (LogReg, RF, DT) | F1 (binary), log loss (multiclass) |
| **Regression** | Numeric (continuous) | LightGBM, XGBoost, sklearn (RF, ElasticNet, DT) | RMSE |
| **Forecasting** | Numeric (time series) | Prophet, ARIMA | SMAPE |

### What AutoML Generates

For each AutoML run, you get:

```
  AutoML Outputs
  ===============

  Experiment (MLflow)
  |
  +-- Data Exploration Notebook
  |   - Dataset statistics, null counts, correlations
  |   - Target variable distribution
  |   - Feature type detection
  |
  +-- Trial 01 Notebook (LightGBM)
  |   - Full preprocessing pipeline
  |   - Model training with specific hyperparameters
  |   - Evaluation metrics and plots
  |   - MLflow logging (automatic)
  |
  +-- Trial 02 Notebook (XGBoost)
  |   - Same structure, different algorithm/hyperparameters
  |
  +-- Trial 03 Notebook (sklearn RandomForest)
  |   ...
  |
  +-- Leaderboard (sorted by primary metric)
  |
  +-- Best Model (ready to register)
```

### How to Launch AutoML

**From the UI**: Experiments -> Create AutoML Experiment -> Select table/target

**From code (API)**:

```python
from databricks import automl

# Classification
summary = automl.classify(
    dataset=df,                          # Spark or pandas DataFrame
    target_col="churned",                # target column name
    primary_metric="f1",                 # optimization metric
    timeout_minutes=30,                  # max runtime
    max_trials=20,                       # max number of models to try
    exclude_columns=["customer_id"],     # columns to ignore
    experiment_name="/Users/<email>/churn-automl"
)

# Regression
summary = automl.regress(
    dataset=df,
    target_col="price",
    primary_metric="rmse",
    timeout_minutes=30
)

# Forecasting
summary = automl.forecast(
    dataset=df,
    target_col="sales",
    time_col="date",
    frequency="D",
    horizon=30,
    timeout_minutes=30
)
```

### Interpreting the Results

The AutoML summary object provides programmatic access to results:

```python
# Best model
best_model = summary.best_trial
print(f"Best run ID: {best_model.mlflow_run_id}")
print(f"Best metric: {best_model.metrics}")

# Load the best model
import mlflow
model = mlflow.sklearn.load_model(f"runs:/{best_model.mlflow_run_id}/model")

# Leaderboard
leaderboard = summary.output_table_name  # Delta table with all results
```

### Customizing Generated Notebooks

The generated trial notebooks are fully editable. Common customizations:

1. **Feature engineering** -- Add domain-specific features that AutoML cannot infer
2. **Hyperparameter ranges** -- Narrow the search space based on domain knowledge
3. **Custom preprocessing** -- Replace generic imputation with domain-specific logic
4. **Evaluation** -- Add business metrics beyond standard ML metrics
5. **Ensemble** -- Combine top-K models from the leaderboard

### When to Use AutoML

| Use Case | AutoML Fit |
|----------|-----------|
| Quick baseline for a new problem | Excellent |
| Explore which algorithms work best | Excellent |
| Citizen data scientists (low-code) | Excellent |
| Production model with custom features | Start with AutoML, then customize |
| Deep learning / NLP / computer vision | Not supported (use custom training) |
| Very large datasets (billions of rows) | Use PySpark MLlib instead (Topic 06) |

---

## Hands-On Walkthrough

Open `04-automl_notebook.py` to practice:
- Generating a classification dataset
- Simulating the AutoML workflow (data profiling, multi-algorithm training, hyperparameter tuning)
- Building a leaderboard from multiple model trials
- Interpreting results and selecting the best model
- Databricks AutoML API reference

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| AutoML availability | Full workspace (not Community) | Full workspace (not Community) | Full workspace (not Community) |
| Compute | Cluster with ML Runtime | Cluster with ML Runtime | Cluster with ML Runtime |
| Supported runtimes | DBR ML 12.0+ | DBR ML 12.0+ | DBR ML 12.0+ |
| GPU trials | Supported (if GPU cluster) | Supported (if GPU cluster) | Supported (if GPU cluster) |
| Generated notebooks | Python, stored in workspace | Python, stored in workspace | Python, stored in workspace |

---

## Certification Tip

> **Databricks ML Professional**: Understand that AutoML generates editable
> notebooks (glass-box, not black-box). Know the three problem types
> (classify, regress, forecast). Expect questions about when AutoML is
> appropriate vs when to use custom training.
>
> **Key concept**: AutoML uses MLflow for all tracking. Every trial is a run
> in an experiment. The best model can be registered directly to the Model
> Registry. AutoML respects the same governance rules as manual training.

---

## Key Takeaways

1. **AutoML is a glass-box system** -- it generates editable notebooks, not opaque
   models. Every decision is visible and customizable.
2. **Three problem types**: classification, regression, and forecasting. Each tries
   appropriate algorithms and metrics.
3. **Start with AutoML**, then customize. It provides a strong baseline and shows
   you which algorithms and hyperparameters work for your data.
4. **Every trial is an MLflow run** with full tracking. The leaderboard is built
   from MLflow metrics, so you can extend it with custom evaluations.
5. **AutoML is not a replacement** for domain expertise. It handles the mechanical
   parts (algorithm selection, hyperparameter tuning) so you can focus on feature
   engineering and business context.
6. **Use AutoML for tabular data**. For deep learning, NLP, computer vision, or
   very large datasets, use custom training pipelines.

---

## Next Steps

- Proceed to **Topic 05: Model Serving** to learn how to deploy your best model
  (from AutoML or manual training) to a real-time REST endpoint.
- Take the best model from your AutoML run and register it in the Model Registry
  (Topic 02).
