# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 01 - MLflow Tracking & Experiments
# MAGIC > Module 20 -- Topic 01 | Track experiments, log parameters/metrics/artifacts, and compare runs
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate a synthetic classification dataset
# MAGIC 2. Create an MLflow experiment and log runs manually
# MAGIC 3. Train multiple sklearn models with different hyperparameters
# MAGIC 4. Use autologging to capture everything automatically
# MAGIC 5. Log custom artifacts (confusion matrix plot, feature importance)
# MAGIC 6. Query and compare past runs programmatically
# MAGIC 7. Demonstrate step-based metric logging for training curves

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data Generation

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score,
    roc_auc_score, confusion_matrix, classification_report
)
import mlflow
import mlflow.sklearn
import warnings
import json
import tempfile
import os

warnings.filterwarnings("ignore")
np.random.seed(42)

print("Libraries imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate a Synthetic Customer Churn Dataset
# MAGIC We create a binary classification problem: predict whether a customer will churn.
# MAGIC The dataset is generated in-memory -- no external files needed.

# COMMAND ----------

# Generate synthetic data
X, y = make_classification(
    n_samples=2000,
    n_features=12,
    n_informative=8,
    n_redundant=2,
    n_clusters_per_class=2,
    weights=[0.7, 0.3],
    random_state=42
)

feature_names = [
    "tenure_months", "monthly_charges", "total_charges", "num_support_tickets",
    "contract_length", "payment_delay_days", "num_products", "usage_minutes",
    "satisfaction_score", "age", "num_referrals", "engagement_index"
]

df = pd.DataFrame(X, columns=feature_names)
df["churned"] = y

print(f"Dataset shape: {df.shape}")
print(f"Churn rate: {df['churned'].mean():.2%}")
print(f"\nFeature summary:")
print(df.describe().round(2))

# COMMAND ----------

# Split into train and test sets
X_train, X_test, y_train, y_test = train_test_split(
    df[feature_names], df["churned"],
    test_size=0.25,
    random_state=42,
    stratify=df["churned"]
)

print(f"Training set: {X_train.shape[0]} samples")
print(f"Test set:     {X_test.shape[0]} samples")
print(f"Train churn rate: {y_train.mean():.2%}")
print(f"Test churn rate:  {y_test.mean():.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Create an MLflow Experiment
# MAGIC
# MAGIC On Databricks, experiments are created automatically when you set a name.
# MAGIC In a notebook, the default experiment is tied to the notebook path.
# MAGIC We explicitly set one for clarity.

# COMMAND ----------

# Set experiment name
# On Databricks: mlflow.set_experiment("/Users/<your-email>/churn-prediction-demo")
# Locally or in Community Edition, the default experiment is used
EXPERIMENT_NAME = "/tmp/module20-churn-prediction"

try:
    mlflow.set_experiment(EXPERIMENT_NAME)
    print(f"Experiment set to: {EXPERIMENT_NAME}")
except Exception as e:
    print(f"Using default experiment (set_experiment not available: {e})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Manual Logging -- Train a RandomForest
# MAGIC
# MAGIC Manual logging gives you full control. You decide exactly what to track.
# MAGIC This is the recommended approach when you need to log custom metrics,
# MAGIC preprocessing decisions, or business-specific measurements.

# COMMAND ----------

def evaluate_model(model, X_test, y_test):
    """Evaluate a trained model and return a dictionary of metrics."""
    y_pred = model.predict(X_test)
    y_proba = model.predict_proba(X_test)[:, 1] if hasattr(model, "predict_proba") else None

    metrics = {
        "accuracy": accuracy_score(y_test, y_pred),
        "f1_score": f1_score(y_test, y_pred),
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
    }
    if y_proba is not None:
        metrics["auc_roc"] = roc_auc_score(y_test, y_proba)

    return metrics, y_pred

print("Evaluation helper defined.")

# COMMAND ----------

# Run 1: RandomForest with manual logging
with mlflow.start_run(run_name="rf_baseline") as run:
    # Log parameters manually
    params = {
        "model_type": "RandomForest",
        "n_estimators": 100,
        "max_depth": 10,
        "min_samples_split": 2,
        "random_state": 42,
        "test_size": 0.25
    }
    for key, value in params.items():
        mlflow.log_param(key, value)

    # Train
    rf_model = RandomForestClassifier(
        n_estimators=params["n_estimators"],
        max_depth=params["max_depth"],
        min_samples_split=params["min_samples_split"],
        random_state=params["random_state"]
    )
    rf_model.fit(X_train, y_train)

    # Evaluate and log metrics
    metrics, y_pred = evaluate_model(rf_model, X_test, y_test)
    for metric_name, metric_value in metrics.items():
        mlflow.log_metric(metric_name, metric_value)

    # Log the model
    mlflow.sklearn.log_model(rf_model, "model")

    # Log a custom tag
    mlflow.set_tag("author", "module20-demo")
    mlflow.set_tag("stage", "baseline")

    run_id_rf = run.info.run_id
    print(f"Run ID: {run_id_rf}")
    print(f"Metrics: {json.dumps({k: round(v, 4) for k, v in metrics.items()}, indent=2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Log Custom Artifacts
# MAGIC
# MAGIC Artifacts are files attached to a run: plots, data samples, configuration
# MAGIC files, or any file your future self will want to reference.

# COMMAND ----------

# Run 2: RandomForest tuned -- with custom artifacts
with mlflow.start_run(run_name="rf_tuned") as run:
    params = {
        "model_type": "RandomForest",
        "n_estimators": 300,
        "max_depth": 15,
        "min_samples_split": 5,
        "min_samples_leaf": 3,
        "random_state": 42,
    }
    for key, value in params.items():
        mlflow.log_param(key, value)

    rf_tuned = RandomForestClassifier(
        n_estimators=params["n_estimators"],
        max_depth=params["max_depth"],
        min_samples_split=params["min_samples_split"],
        min_samples_leaf=params["min_samples_leaf"],
        random_state=params["random_state"]
    )
    rf_tuned.fit(X_train, y_train)

    metrics, y_pred = evaluate_model(rf_tuned, X_test, y_test)
    for metric_name, metric_value in metrics.items():
        mlflow.log_metric(metric_name, metric_value)

    # --- Custom artifact: feature importance CSV ---
    feature_imp = pd.DataFrame({
        "feature": feature_names,
        "importance": rf_tuned.feature_importances_
    }).sort_values("importance", ascending=False)

    with tempfile.TemporaryDirectory() as tmpdir:
        # Save feature importance as CSV
        imp_path = os.path.join(tmpdir, "feature_importance.csv")
        feature_imp.to_csv(imp_path, index=False)
        mlflow.log_artifact(imp_path, "analysis")

        # Save confusion matrix as text
        cm = confusion_matrix(y_test, y_pred)
        cm_path = os.path.join(tmpdir, "confusion_matrix.txt")
        with open(cm_path, "w") as f:
            f.write("Confusion Matrix\n")
            f.write(f"TN={cm[0][0]}  FP={cm[0][1]}\n")
            f.write(f"FN={cm[1][0]}  TP={cm[1][1]}\n")
        mlflow.log_artifact(cm_path, "analysis")

        # Save classification report
        report_path = os.path.join(tmpdir, "classification_report.txt")
        with open(report_path, "w") as f:
            f.write(classification_report(y_test, y_pred))
        mlflow.log_artifact(report_path, "analysis")

    mlflow.sklearn.log_model(rf_tuned, "model")
    mlflow.set_tag("stage", "tuned")

    run_id_rf_tuned = run.info.run_id
    print(f"Run ID: {run_id_rf_tuned}")
    print(f"Metrics: {json.dumps({k: round(v, 4) for k, v in metrics.items()}, indent=2)}")
    print(f"\nTop 5 features:")
    print(feature_imp.head(5).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Autologging
# MAGIC
# MAGIC Autologging captures constructor parameters, metrics, and the model artifact
# MAGIC automatically. Enable it once -- then just train as usual.

# COMMAND ----------

# Enable sklearn autologging
mlflow.sklearn.autolog(log_models=True, log_input_examples=True)

# Run 3: GradientBoosting with autologging -- no manual log calls needed
with mlflow.start_run(run_name="gbt_autolog") as run:
    gbt_model = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.1,
        max_depth=5,
        random_state=42
    )
    gbt_model.fit(X_train, y_train)

    # Autolog captures params, metrics, and model automatically.
    # We can still add custom metrics on top.
    metrics, _ = evaluate_model(gbt_model, X_test, y_test)
    mlflow.log_metric("custom_auc_roc", metrics.get("auc_roc", 0))
    mlflow.set_tag("stage", "autolog-demo")

    run_id_gbt = run.info.run_id
    print(f"Run ID: {run_id_gbt}")
    print(f"Autologging captured params, metrics, model, and input example.")
    print(f"Custom AUC-ROC: {metrics.get('auc_roc', 'N/A'):.4f}")

# Disable autologging for remaining cells
mlflow.sklearn.autolog(disable=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Step-Based Metric Logging (Training Curves)
# MAGIC
# MAGIC You can log the same metric name at different steps to create training curves.
# MAGIC This is essential for monitoring convergence in iterative algorithms.

# COMMAND ----------

# Run 4: LogisticRegression with step-based logging to simulate training curves
with mlflow.start_run(run_name="lr_with_training_curve") as run:
    mlflow.log_param("model_type", "LogisticRegression")
    mlflow.log_param("max_iter", 100)
    mlflow.log_param("C", 1.0)

    # Simulate a training curve by training on increasing data fractions
    fractions = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    for i, frac in enumerate(fractions):
        n_samples = int(len(X_train) * frac)
        X_partial = X_train.iloc[:n_samples]
        y_partial = y_train.iloc[:n_samples]

        lr = LogisticRegression(max_iter=100, C=1.0, random_state=42)
        lr.fit(X_partial, y_partial)

        train_acc = lr.score(X_partial, y_partial)
        test_acc = lr.score(X_test, y_test)

        # Log at each step
        mlflow.log_metric("train_accuracy", train_acc, step=i)
        mlflow.log_metric("test_accuracy", test_acc, step=i)
        mlflow.log_metric("data_fraction", frac, step=i)

    # Log final model
    mlflow.sklearn.log_model(lr, "model")
    mlflow.set_tag("stage", "learning-curve")

    run_id_lr = run.info.run_id
    print(f"Run ID: {run_id_lr}")
    print(f"Logged {len(fractions)} steps of training/test accuracy")
    print(f"Final test accuracy: {test_acc:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Comparing Runs with Different Algorithms
# MAGIC
# MAGIC A common workflow: try multiple algorithms with the same data, then compare.

# COMMAND ----------

# Run a series of models and collect results
model_configs = [
    {
        "name": "rf_shallow",
        "model": RandomForestClassifier(n_estimators=50, max_depth=5, random_state=42),
    },
    {
        "name": "rf_deep",
        "model": RandomForestClassifier(n_estimators=200, max_depth=20, random_state=42),
    },
    {
        "name": "gbt_conservative",
        "model": GradientBoostingClassifier(n_estimators=100, learning_rate=0.05, max_depth=3, random_state=42),
    },
    {
        "name": "gbt_aggressive",
        "model": GradientBoostingClassifier(n_estimators=300, learning_rate=0.2, max_depth=7, random_state=42),
    },
]

comparison_results = []

for config in model_configs:
    with mlflow.start_run(run_name=config["name"]) as run:
        model = config["model"]
        model_params = model.get_params()

        # Log all constructor parameters
        for key, value in model_params.items():
            mlflow.log_param(key, value)

        model.fit(X_train, y_train)
        metrics, _ = evaluate_model(model, X_test, y_test)

        for metric_name, metric_value in metrics.items():
            mlflow.log_metric(metric_name, metric_value)

        mlflow.sklearn.log_model(model, "model")
        mlflow.set_tag("comparison_group", "algorithm_sweep")

        comparison_results.append({
            "run_name": config["name"],
            "run_id": run.info.run_id,
            **{k: round(v, 4) for k, v in metrics.items()}
        })

# Display comparison table
comparison_df = pd.DataFrame(comparison_results)
print("Model Comparison Results:")
print("=" * 80)
print(comparison_df.to_string(index=False))
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Querying Runs Programmatically
# MAGIC
# MAGIC The MLflow client API lets you search, filter, and sort runs. This is
# MAGIC essential for automated model selection in production pipelines.

# COMMAND ----------

# Search for the best run by AUC-ROC
client = mlflow.tracking.MlflowClient()

try:
    experiment = client.get_experiment_by_name(EXPERIMENT_NAME)
    if experiment:
        exp_id = experiment.experiment_id
    else:
        exp_id = "0"  # default experiment
except Exception:
    exp_id = "0"

# Search all runs in this experiment, ordered by accuracy
runs = client.search_runs(
    experiment_ids=[exp_id],
    filter_string="",
    order_by=["metrics.accuracy DESC"],
    max_results=10
)

print(f"Found {len(runs)} runs in experiment {exp_id}")
print("\nTop runs by accuracy:")
print("-" * 90)
print(f"{'Run Name':<25} {'Run ID':<35} {'Accuracy':>10} {'F1':>10} {'AUC-ROC':>10}")
print("-" * 90)
for run in runs:
    name = run.info.run_name or "unnamed"
    accuracy = run.data.metrics.get("accuracy", 0)
    f1 = run.data.metrics.get("f1_score", 0)
    auc = run.data.metrics.get("auc_roc", run.data.metrics.get("custom_auc_roc", 0))
    print(f"{name:<25} {run.info.run_id:<35} {accuracy:>10.4f} {f1:>10.4f} {auc:>10.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Loading a Model from a Run
# MAGIC
# MAGIC Once you find the best run, you can load its model directly.

# COMMAND ----------

# Load the best model by run ID
if runs:
    best_run = runs[0]
    best_run_id = best_run.info.run_id
    print(f"Loading model from best run: {best_run.info.run_name} ({best_run_id})")

    loaded_model = mlflow.sklearn.load_model(f"runs:/{best_run_id}/model")

    # Verify it works
    predictions = loaded_model.predict(X_test[:5])
    print(f"\nSample predictions from loaded model: {predictions}")
    print(f"Actual labels:                        {y_test.values[:5]}")
    print(f"\nModel type: {type(loaded_model).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Nested Runs for Hyperparameter Sweeps
# MAGIC
# MAGIC Nested runs organize hyperparameter searches under a parent run.
# MAGIC The parent holds the sweep configuration; children hold individual trials.

# COMMAND ----------

# Hyperparameter sweep using nested runs
with mlflow.start_run(run_name="hyperparam_sweep") as parent_run:
    mlflow.log_param("sweep_type", "grid_search")
    mlflow.log_param("model_type", "RandomForest")

    best_accuracy = 0
    best_child_run_id = None

    n_estimators_options = [50, 100, 200]
    max_depth_options = [5, 10, 15]

    for n_est in n_estimators_options:
        for depth in max_depth_options:
            with mlflow.start_run(
                run_name=f"rf_n{n_est}_d{depth}",
                nested=True
            ) as child_run:
                mlflow.log_param("n_estimators", n_est)
                mlflow.log_param("max_depth", depth)

                model = RandomForestClassifier(
                    n_estimators=n_est, max_depth=depth, random_state=42
                )
                model.fit(X_train, y_train)
                metrics, _ = evaluate_model(model, X_test, y_test)

                for k, v in metrics.items():
                    mlflow.log_metric(k, v)

                if metrics["accuracy"] > best_accuracy:
                    best_accuracy = metrics["accuracy"]
                    best_child_run_id = child_run.info.run_id

    # Log best result on parent
    mlflow.log_metric("best_accuracy", best_accuracy)
    mlflow.set_tag("best_child_run", best_child_run_id)
    print(f"Sweep complete: {len(n_estimators_options) * len(max_depth_options)} trials")
    print(f"Best accuracy: {best_accuracy:.4f} (run: {best_child_run_id})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: MLflow UI Reference
# MAGIC
# MAGIC On Databricks, navigate to the Experiments sidebar (flask icon) to see:
# MAGIC - **Experiment list**: All experiments in your workspace
# MAGIC - **Run table**: Sortable/filterable table of all runs in an experiment
# MAGIC - **Run detail**: Parameters, metrics, artifacts, and tags for a single run
# MAGIC - **Compare view**: Side-by-side comparison of selected runs
# MAGIC - **Chart view**: Parallel coordinates, scatter plots for metric comparison
# MAGIC
# MAGIC ```
# MAGIC   MLflow UI Layout
# MAGIC   ================
# MAGIC
# MAGIC   +-------+----------------------------------------------+
# MAGIC   | Exps  |  Experiment: customer-churn-prediction        |
# MAGIC   |       |                                                |
# MAGIC   | exp1  |  [Run Table]                                   |
# MAGIC   | exp2  |  | Run Name   | Accuracy | F1   | AUC  | ... |
# MAGIC   | exp3  |  |------------|----------|------|------|-----|
# MAGIC   | ...   |  | gbt_auto   | 0.912    | 0.88 | 0.95 |     |
# MAGIC   |       |  | rf_tuned   | 0.893    | 0.85 | 0.93 |     |
# MAGIC   |       |  | rf_base    | 0.871    | 0.82 | 0.91 |     |
# MAGIC   |       |                                                |
# MAGIC   |       |  [Compare] [Chart] [Delete]                   |
# MAGIC   +-------+----------------------------------------------+
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: Databricks-Specific Features (Reference)
# MAGIC
# MAGIC These features work on full Databricks workspaces. Shown as reference.

# COMMAND ----------

# --- Databricks-specific: Notebook experiment linking ---
# On Databricks, each notebook automatically gets an experiment.
# You can also link to a shared experiment:
#
# mlflow.set_experiment("/Shared/team-experiments/churn-model")
#
# --- Databricks-specific: Unity Catalog experiment tracking ---
# With Unity Catalog, experiments can be governed at the catalog level:
#
# mlflow.set_registry_uri("databricks-uc")
# mlflow.set_experiment("/Users/<email>/my-experiment")
#
# --- Databricks-specific: Cluster-level autologging ---
# Admins can enable autologging at the cluster level so every notebook
# on that cluster automatically tracks experiments:
#
# Cluster settings > Advanced > Spark Config:
#   spark.databricks.mlflow.trackMLlib.enabled true
#
# --- Databricks-specific: Experiment permissions ---
# On Databricks, experiments support ACLs:
#   - Can View: See runs and metrics
#   - Can Edit: Create runs, log metrics
#   - Can Manage: Change permissions, delete experiment

print("Databricks-specific features shown as comments above.")
print("On a full workspace, uncomment and modify paths as needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary of All Runs

# COMMAND ----------

# Final summary
print("\n" + "=" * 80)
print("  MODULE 20, TOPIC 01: MLflow Tracking -- Run Summary")
print("=" * 80)
print(f"\nExperiment: {EXPERIMENT_NAME}")
print(f"Total runs logged: {len(runs)} (from search)")
print(f"\nKey techniques demonstrated:")
print(f"  - Manual logging:     mlflow.log_param(), log_metric(), log_artifact()")
print(f"  - Autologging:        mlflow.sklearn.autolog()")
print(f"  - Step-based metrics: mlflow.log_metric('acc', value, step=i)")
print(f"  - Nested runs:        mlflow.start_run(nested=True)")
print(f"  - Run search:         client.search_runs()")
print(f"  - Model loading:      mlflow.sklearn.load_model('runs:/<id>/model')")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC This notebook used only in-memory data and MLflow logging.
# MAGIC MLflow runs are stored in the tracking server and do not need cleanup
# MAGIC for learning purposes. In production, you would periodically archive
# MAGIC old experiments.

# COMMAND ----------

# Optional: delete the experiment if you want a clean slate
# mlflow.delete_experiment(exp_id)

print("Notebook 01-mlflow-tracking complete.")
