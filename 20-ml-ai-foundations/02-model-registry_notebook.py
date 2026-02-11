# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 02 - Model Registry
# MAGIC > Module 20 -- Topic 02 | Register models, manage versions, and promote through lifecycle stages
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Train multiple model versions on a synthetic dataset
# MAGIC 2. Register models into the MLflow Model Registry
# MAGIC 3. Manage model versions and transitions (staging, production, archived)
# MAGIC 4. Work with aliases (Champion/Challenger) for Unity Catalog patterns
# MAGIC 5. Load models by name, version, and alias
# MAGIC 6. Compare model versions side-by-side
# MAGIC 7. Simulate an approval workflow

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
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import warnings
import time

warnings.filterwarnings("ignore")
np.random.seed(42)

print("Libraries imported successfully.")

# COMMAND ----------

# Generate synthetic loan default prediction dataset
X, y = make_classification(
    n_samples=1500,
    n_features=10,
    n_informative=7,
    n_redundant=2,
    weights=[0.75, 0.25],
    random_state=42
)

feature_names = [
    "credit_score", "annual_income", "debt_to_income", "num_accounts",
    "credit_history_years", "loan_amount", "interest_rate",
    "employment_years", "num_late_payments", "utilization_ratio"
]

df = pd.DataFrame(X, columns=feature_names)
df["default"] = y

X_train, X_test, y_train, y_test = train_test_split(
    df[feature_names], df["default"],
    test_size=0.25, random_state=42, stratify=df["default"]
)

print(f"Dataset: {df.shape[0]} rows, {df.shape[1]} columns")
print(f"Default rate: {df['default'].mean():.2%}")
print(f"Train: {X_train.shape[0]} | Test: {X_test.shape[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Train Multiple Model Versions
# MAGIC
# MAGIC We train three different models that will become three versions
# MAGIC in the model registry. Each represents an iteration of improvement.

# COMMAND ----------

# Set experiment
EXPERIMENT_NAME = "/tmp/module20-model-registry-demo"
try:
    mlflow.set_experiment(EXPERIMENT_NAME)
except Exception:
    pass

client = MlflowClient()

def evaluate_and_log(model, model_name, X_train, y_train, X_test, y_test):
    """Train, evaluate, and log a model to MLflow. Return run_id and metrics."""
    with mlflow.start_run(run_name=model_name) as run:
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "f1_score": f1_score(y_test, y_pred),
            "auc_roc": roc_auc_score(y_test, y_proba),
        }

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        mlflow.sklearn.log_model(model, "model")
        mlflow.set_tag("model_type", type(model).__name__)

        return run.info.run_id, metrics

print("Training helper defined.")

# COMMAND ----------

# Version 1: Simple Logistic Regression (baseline)
lr_model = LogisticRegression(max_iter=200, random_state=42)
run_id_v1, metrics_v1 = evaluate_and_log(
    lr_model, "v1_logistic_regression",
    X_train, y_train, X_test, y_test
)
print(f"Version 1 (LogisticRegression):")
print(f"  Run ID: {run_id_v1}")
print(f"  Metrics: {', '.join(f'{k}={v:.4f}' for k, v in metrics_v1.items())}")

# COMMAND ----------

# Version 2: Random Forest (improvement)
rf_model = RandomForestClassifier(n_estimators=200, max_depth=12, random_state=42)
run_id_v2, metrics_v2 = evaluate_and_log(
    rf_model, "v2_random_forest",
    X_train, y_train, X_test, y_test
)
print(f"Version 2 (RandomForest):")
print(f"  Run ID: {run_id_v2}")
print(f"  Metrics: {', '.join(f'{k}={v:.4f}' for k, v in metrics_v2.items())}")

# COMMAND ----------

# Version 3: Gradient Boosting (best)
gbt_model = GradientBoostingClassifier(
    n_estimators=300, learning_rate=0.1, max_depth=5, random_state=42
)
run_id_v3, metrics_v3 = evaluate_and_log(
    gbt_model, "v3_gradient_boosting",
    X_train, y_train, X_test, y_test
)
print(f"Version 3 (GradientBoosting):")
print(f"  Run ID: {run_id_v3}")
print(f"  Metrics: {', '.join(f'{k}={v:.4f}' for k, v in metrics_v3.items())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Register Models into the Registry
# MAGIC
# MAGIC Registration creates a named model entry with version tracking.
# MAGIC Each registration from a different run creates a new version.

# COMMAND ----------

MODEL_NAME = "loan_default_predictor"

# Register version 1
mv1 = mlflow.register_model(
    model_uri=f"runs:/{run_id_v1}/model",
    name=MODEL_NAME
)
print(f"Registered {MODEL_NAME} version {mv1.version} from run {run_id_v1}")

# COMMAND ----------

# Register version 2
mv2 = mlflow.register_model(
    model_uri=f"runs:/{run_id_v2}/model",
    name=MODEL_NAME
)
print(f"Registered {MODEL_NAME} version {mv2.version} from run {run_id_v2}")

# COMMAND ----------

# Register version 3
mv3 = mlflow.register_model(
    model_uri=f"runs:/{run_id_v3}/model",
    name=MODEL_NAME
)
print(f"Registered {MODEL_NAME} version {mv3.version} from run {run_id_v3}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: List and Inspect Model Versions

# COMMAND ----------

# List all versions of our model
print(f"All versions of '{MODEL_NAME}':")
print("-" * 80)

try:
    versions = client.search_model_versions(f"name='{MODEL_NAME}'")
    for v in versions:
        print(f"  Version {v.version}:")
        print(f"    Stage:       {v.current_stage}")
        print(f"    Run ID:      {v.run_id}")
        print(f"    Status:      {v.status}")
        print(f"    Created:     {v.creation_timestamp}")
        print()
except Exception as e:
    print(f"  (search_model_versions not available in this environment: {e})")
    print(f"  Versions registered: v1={mv1.version}, v2={mv2.version}, v3={mv3.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Lifecycle Stage Transitions (Workspace Registry)
# MAGIC
# MAGIC In the workspace registry, models move through stages:
# MAGIC None -> Staging -> Production -> Archived.
# MAGIC
# MAGIC NOTE: On Unity Catalog, aliases replace stages (see Section 7).

# COMMAND ----------

# Transition version 1 to Staging (for validation)
try:
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=mv1.version,
        stage="Staging"
    )
    print(f"Version {mv1.version} -> Staging")
except Exception as e:
    print(f"Stage transition not available: {e}")
    print("(This is expected in environments without full registry support)")

# COMMAND ----------

# After validation, promote version 2 to Production
try:
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=mv2.version,
        stage="Production"
    )
    print(f"Version {mv2.version} -> Production")
except Exception as e:
    print(f"Stage transition not available: {e}")

# COMMAND ----------

# Version 3 is our new candidate -- promote to Staging first
try:
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=mv3.version,
        stage="Staging"
    )
    print(f"Version {mv3.version} -> Staging (for validation)")
except Exception as e:
    print(f"Stage transition not available: {e}")

# Archive version 1 (no longer needed)
try:
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=mv1.version,
        stage="Archived"
    )
    print(f"Version {mv1.version} -> Archived")
except Exception as e:
    print(f"Stage transition not available: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Loading Models from the Registry

# COMMAND ----------

# Load by specific version number
print("Loading model by version number...")
model_v2 = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/2")
preds_v2 = model_v2.predict(X_test[:5])
print(f"  Version 2 predictions: {preds_v2}")

# Load by version number (latest)
model_v3 = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/3")
preds_v3 = model_v3.predict(X_test[:5])
print(f"  Version 3 predictions: {preds_v3}")

# Load by stage (workspace registry)
try:
    model_prod = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/Production")
    preds_prod = model_prod.predict(X_test[:5])
    print(f"  Production predictions: {preds_prod}")
except Exception as e:
    print(f"  Loading by stage: {e}")
    print(f"  (Loading by stage requires workspace registry with stage transitions)")

print(f"\nActual labels:           {y_test.values[:5]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Aliases (Unity Catalog Pattern)
# MAGIC
# MAGIC Unity Catalog uses aliases instead of stages. Aliases are mutable labels
# MAGIC that you attach to a specific version. This is more flexible because:
# MAGIC - You can define custom aliases (not just 4 fixed stages)
# MAGIC - Moving an alias is atomic and instant
# MAGIC - Serving endpoints reference aliases, so promotion = moving a label

# COMMAND ----------

# Simulate alias management
# On Databricks with Unity Catalog:
#   client.set_registered_model_alias(
#       name="catalog.schema.loan_default_predictor",
#       alias="Champion",
#       version=2
#   )
#   client.set_registered_model_alias(
#       name="catalog.schema.loan_default_predictor",
#       alias="Challenger",
#       version=3
#   )
#   # Load by alias:
#   model = mlflow.sklearn.load_model(
#       "models:/catalog.schema.loan_default_predictor@Champion"
#   )

# Simulating aliases with a dictionary for demonstration
aliases = {}

def set_alias(model_name, alias, version):
    """Simulate setting an alias on a model version."""
    aliases[(model_name, alias)] = version
    print(f"Set alias '{alias}' on {model_name} version {version}")

def get_model_by_alias(model_name, alias):
    """Simulate loading a model by alias."""
    version = aliases.get((model_name, alias))
    if version is None:
        raise ValueError(f"Alias '{alias}' not found for {model_name}")
    return mlflow.sklearn.load_model(f"models:/{model_name}/{version}")

# Set initial aliases
set_alias(MODEL_NAME, "Champion", mv2.version)
set_alias(MODEL_NAME, "Challenger", mv3.version)

print(f"\nCurrent aliases for '{MODEL_NAME}':")
for (name, alias), version in aliases.items():
    print(f"  @{alias} -> version {version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Promote Challenger to Champion
# MAGIC
# MAGIC After validation, promoting a model is just moving the alias.

# COMMAND ----------

# Compare Champion vs Challenger
champion_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{aliases[(MODEL_NAME, 'Champion')]}")
challenger_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{aliases[(MODEL_NAME, 'Challenger')]}")

champion_acc = accuracy_score(y_test, champion_model.predict(X_test))
challenger_acc = accuracy_score(y_test, challenger_model.predict(X_test))
champion_auc = roc_auc_score(y_test, champion_model.predict_proba(X_test)[:, 1])
challenger_auc = roc_auc_score(y_test, challenger_model.predict_proba(X_test)[:, 1])

print("Champion vs Challenger Comparison:")
print("=" * 50)
print(f"{'Metric':<20} {'Champion':>12} {'Challenger':>12}")
print("-" * 50)
print(f"{'Accuracy':<20} {champion_acc:>12.4f} {challenger_acc:>12.4f}")
print(f"{'AUC-ROC':<20} {champion_auc:>12.4f} {challenger_auc:>12.4f}")
print("=" * 50)

if challenger_auc > champion_auc:
    print("\nChallenger outperforms Champion -- promoting!")
    set_alias(MODEL_NAME, "Champion", aliases[(MODEL_NAME, "Challenger")])
    # In a real workflow, you would also remove the Challenger alias:
    # client.delete_registered_model_alias(name=MODEL_NAME, alias="Challenger")
    print(f"New Champion: version {aliases[(MODEL_NAME, 'Champion')]}")
else:
    print("\nChampion still leads. No promotion.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Model Version Descriptions and Tags

# COMMAND ----------

# Add descriptions and tags to model versions for documentation
try:
    client.update_model_version(
        name=MODEL_NAME,
        version=mv1.version,
        description="Baseline logistic regression. Simple and interpretable."
    )
    client.update_model_version(
        name=MODEL_NAME,
        version=mv2.version,
        description="Random forest with 200 trees. Significant improvement over baseline."
    )
    client.update_model_version(
        name=MODEL_NAME,
        version=mv3.version,
        description="Gradient boosting with 300 trees. Best AUC-ROC in evaluation."
    )
    print("Model version descriptions updated.")
except Exception as e:
    print(f"Version update: {e}")

# Set tags on model versions
try:
    client.set_model_version_tag(MODEL_NAME, mv1.version, "validation_status", "passed")
    client.set_model_version_tag(MODEL_NAME, mv2.version, "validation_status", "passed")
    client.set_model_version_tag(MODEL_NAME, mv3.version, "validation_status", "passed")
    client.set_model_version_tag(MODEL_NAME, mv3.version, "approved_by", "ml-platform-team")
    print("Model version tags set.")
except Exception as e:
    print(f"Version tags: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Simulated Approval Workflow
# MAGIC
# MAGIC In production, model promotion follows an approval process. Here we
# MAGIC simulate the key steps that would involve human review and automated checks.

# COMMAND ----------

class ModelApprovalWorkflow:
    """Simulates a model promotion approval workflow."""

    def __init__(self, model_name):
        self.model_name = model_name
        self.checks = []

    def check_accuracy_threshold(self, model, X_test, y_test, threshold=0.80):
        """Check if model meets minimum accuracy requirement."""
        acc = accuracy_score(y_test, model.predict(X_test))
        passed = acc >= threshold
        self.checks.append(("accuracy_threshold", passed, f"accuracy={acc:.4f}, threshold={threshold}"))
        return passed

    def check_auc_threshold(self, model, X_test, y_test, threshold=0.85):
        """Check if model meets minimum AUC-ROC requirement."""
        auc = roc_auc_score(y_test, model.predict_proba(X_test)[:, 1])
        passed = auc >= threshold
        self.checks.append(("auc_threshold", passed, f"auc={auc:.4f}, threshold={threshold}"))
        return passed

    def check_prediction_latency(self, model, X_sample, max_ms=100):
        """Check if single prediction is fast enough."""
        start = time.time()
        for _ in range(100):
            model.predict(X_sample[:1])
        avg_ms = (time.time() - start) / 100 * 1000
        passed = avg_ms <= max_ms
        self.checks.append(("prediction_latency", passed, f"avg={avg_ms:.2f}ms, max={max_ms}ms"))
        return passed

    def check_no_data_leakage(self, train_metrics, test_metrics, max_gap=0.10):
        """Check for large train/test metric gap (data leakage indicator)."""
        gap = abs(train_metrics - test_metrics)
        passed = gap <= max_gap
        self.checks.append(("data_leakage", passed, f"gap={gap:.4f}, max={max_gap}"))
        return passed

    def report(self):
        """Print approval report."""
        all_passed = all(p for _, p, _ in self.checks)
        print(f"\n{'=' * 60}")
        print(f"  Model Approval Report: {self.model_name}")
        print(f"  Decision: {'APPROVED' if all_passed else 'REJECTED'}")
        print(f"{'=' * 60}")
        for name, passed, detail in self.checks:
            status = "PASS" if passed else "FAIL"
            print(f"  [{status}] {name}: {detail}")
        print(f"{'=' * 60}")
        return all_passed


# Run the approval workflow on our best model (version 3)
workflow = ModelApprovalWorkflow(MODEL_NAME)

best_model = mlflow.sklearn.load_model(f"models:/{MODEL_NAME}/{mv3.version}")

workflow.check_accuracy_threshold(best_model, X_test, y_test, threshold=0.80)
workflow.check_auc_threshold(best_model, X_test, y_test, threshold=0.85)
workflow.check_prediction_latency(best_model, X_test)

train_acc = accuracy_score(y_train, best_model.predict(X_train))
test_acc = accuracy_score(y_test, best_model.predict(X_test))
workflow.check_no_data_leakage(train_acc, test_acc, max_gap=0.10)

approved = workflow.report()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Databricks-Specific Registry Features (Reference)

# COMMAND ----------

# --- Unity Catalog Model Registry ---
# The UC registry is the recommended approach for production Databricks.
#
# Setup:
#   mlflow.set_registry_uri("databricks-uc")
#
# Register:
#   mlflow.register_model(
#       model_uri=f"runs:/{run_id}/model",
#       name="catalog.schema.loan_default_predictor"
#   )
#
# Aliases:
#   client.set_registered_model_alias(
#       name="catalog.schema.loan_default_predictor",
#       alias="Champion",
#       version=3
#   )
#
# Load by alias:
#   model = mlflow.pyfunc.load_model(
#       "models:/catalog.schema.loan_default_predictor@Champion"
#   )
#
# --- Webhooks for automation ---
# Trigger actions when registry events occur:
#
# from databricks.sdk import WorkspaceClient
# w = WorkspaceClient()
# w.model_registry.create_webhook(
#     events=["MODEL_VERSION_CREATED"],
#     model_name="loan_default_predictor",
#     http_url_spec={"url": "https://your-ci-cd/validate"}
# )
#
# --- Model lineage in Unity Catalog ---
# UC automatically tracks:
#   - Which notebook/job created the model
#   - Which tables were read during training
#   - Which experiment and run produced the model

print("Unity Catalog registry features shown as comments above.")
print("On a full Databricks workspace, uncomment and configure as needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: Version Comparison Summary

# COMMAND ----------

# Final comparison of all registered versions
comparison = pd.DataFrame([
    {
        "version": mv1.version,
        "model_type": "LogisticRegression",
        "accuracy": metrics_v1["accuracy"],
        "f1_score": metrics_v1["f1_score"],
        "auc_roc": metrics_v1["auc_roc"],
        "run_id": run_id_v1[:12] + "...",
    },
    {
        "version": mv2.version,
        "model_type": "RandomForest",
        "accuracy": metrics_v2["accuracy"],
        "f1_score": metrics_v2["f1_score"],
        "auc_roc": metrics_v2["auc_roc"],
        "run_id": run_id_v2[:12] + "...",
    },
    {
        "version": mv3.version,
        "model_type": "GradientBoosting",
        "accuracy": metrics_v3["accuracy"],
        "f1_score": metrics_v3["f1_score"],
        "auc_roc": metrics_v3["auc_roc"],
        "run_id": run_id_v3[:12] + "...",
    },
])

print(f"\nModel Registry Summary: '{MODEL_NAME}'")
print("=" * 90)
print(comparison.to_string(index=False))
print("=" * 90)
print(f"\nCurrent Champion: version {aliases.get((MODEL_NAME, 'Champion'), 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC Remove the registered model if you want a clean slate.

# COMMAND ----------

# Clean up: delete model versions and the registered model
try:
    for version in [mv1.version, mv2.version, mv3.version]:
        client.delete_model_version(name=MODEL_NAME, version=version)
    client.delete_registered_model(name=MODEL_NAME)
    print(f"Deleted registered model '{MODEL_NAME}' and all versions.")
except Exception as e:
    print(f"Cleanup: {e}")
    print("(Some registry operations may not be available in this environment)")

print("Notebook 02-model-registry complete.")
