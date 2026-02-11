# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 04 - AutoML
# MAGIC > Module 20 -- Topic 04 | Automated model selection, hyperparameter tuning, and leaderboard comparison
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate a synthetic classification dataset (customer churn)
# MAGIC 2. Perform automated data profiling (mimicking AutoML's first step)
# MAGIC 3. Train multiple algorithms with hyperparameter variations
# MAGIC 4. Build a leaderboard ranking all trials
# MAGIC 5. Select and evaluate the best model
# MAGIC 6. Show how to customize an AutoML-generated notebook
# MAGIC 7. Demonstrate the Databricks AutoML API (as reference)
# MAGIC
# MAGIC **Note:** Databricks AutoML requires a full workspace with ML Runtime.
# MAGIC This notebook simulates the AutoML workflow using sklearn so it runs
# MAGIC anywhere (including Community Edition and local environments).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data Generation

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.ensemble import (
    RandomForestClassifier, GradientBoostingClassifier, ExtraTreesClassifier
)
from sklearn.linear_model import LogisticRegression
from sklearn.tree import DecisionTreeClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score,
    roc_auc_score, log_loss
)
import mlflow
import mlflow.sklearn
import warnings
import time

warnings.filterwarnings("ignore")
np.random.seed(42)

print("Libraries imported successfully.")

# COMMAND ----------

# Generate a synthetic customer churn dataset with realistic feature names
X, y = make_classification(
    n_samples=3000,
    n_features=15,
    n_informative=10,
    n_redundant=3,
    n_clusters_per_class=3,
    weights=[0.72, 0.28],
    flip_y=0.05,
    random_state=42
)

feature_names = [
    "tenure_months", "monthly_charges", "total_charges", "num_support_tickets",
    "contract_months_remaining", "payment_delay_days", "num_products",
    "avg_usage_hours", "satisfaction_score", "age", "num_referrals",
    "engagement_index", "promo_response_rate", "login_frequency",
    "last_interaction_days"
]

df = pd.DataFrame(X, columns=feature_names)
df["churned"] = y

# Inject some nulls to make it realistic (AutoML handles nulls automatically)
null_mask = np.random.random(df.shape) < 0.03
null_mask[:, -1] = False  # don't null the target
for i, col in enumerate(feature_names):
    df.loc[null_mask[:, i], col] = np.nan

print(f"Dataset: {df.shape[0]} rows, {df.shape[1]} columns")
print(f"Churn rate: {df['churned'].mean():.2%}")
print(f"Null values: {df.isnull().sum().sum()} ({df.isnull().mean().mean():.2%} per column)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Data Profiling (AutoML Step 1)
# MAGIC
# MAGIC AutoML starts by profiling the dataset: statistics, distributions,
# MAGIC correlations, and null patterns. This informs feature engineering choices.

# COMMAND ----------

def automl_data_profile(df, target_col):
    """Simulate AutoML's data exploration step."""
    print("=" * 70)
    print("  AutoML Data Profile")
    print("=" * 70)

    # Basic info
    print(f"\n  Shape: {df.shape[0]} rows x {df.shape[1]} columns")
    print(f"  Target: '{target_col}' ({df[target_col].dtype})")
    print(f"  Task type: {'Classification' if df[target_col].nunique() <= 20 else 'Regression'}")

    # Target distribution
    print(f"\n  Target distribution:")
    for val, count in df[target_col].value_counts().items():
        pct = count / len(df) * 100
        bar = "#" * int(pct / 2)
        print(f"    {val}: {count:>5} ({pct:>5.1f}%) {bar}")

    # Feature summary
    print(f"\n  Feature types:")
    numeric_cols = df.select_dtypes(include=[np.number]).columns.drop(target_col, errors="ignore")
    print(f"    Numeric: {len(numeric_cols)}")

    # Null analysis
    null_counts = df.isnull().sum()
    cols_with_nulls = null_counts[null_counts > 0]
    print(f"\n  Columns with nulls: {len(cols_with_nulls)}")
    if len(cols_with_nulls) > 0:
        for col, count in cols_with_nulls.items():
            print(f"    {col}: {count} ({count/len(df)*100:.1f}%)")

    # Correlation with target
    print(f"\n  Top feature correlations with '{target_col}':")
    correlations = df[numeric_cols].corrwith(df[target_col]).abs().sort_values(ascending=False)
    for feat, corr in correlations.head(8).items():
        bar = "#" * int(corr * 40)
        print(f"    {feat:<30} {corr:.4f} {bar}")

    print("=" * 70)


automl_data_profile(df, "churned")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Prepare Data (AutoML Step 2)
# MAGIC
# MAGIC AutoML handles preprocessing automatically: imputation, encoding, scaling.
# MAGIC Here we replicate those steps explicitly.

# COMMAND ----------

# Split data
X = df.drop(columns=["churned"])
y = df["churned"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

# Impute nulls (AutoML uses median for numeric)
from sklearn.impute import SimpleImputer
imputer = SimpleImputer(strategy="median")
X_train_imputed = pd.DataFrame(
    imputer.fit_transform(X_train),
    columns=feature_names, index=X_train.index
)
X_test_imputed = pd.DataFrame(
    imputer.transform(X_test),
    columns=feature_names, index=X_test.index
)

print(f"Training set: {X_train_imputed.shape[0]} rows (nulls imputed)")
print(f"Test set:     {X_test_imputed.shape[0]} rows (nulls imputed)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Multi-Algorithm Training (AutoML Step 3)
# MAGIC
# MAGIC AutoML tries multiple algorithms with different hyperparameters.
# MAGIC We simulate this by running a set of trials and logging each to MLflow.

# COMMAND ----------

# Set experiment
EXPERIMENT_NAME = "/tmp/module20-automl-demo"
try:
    mlflow.set_experiment(EXPERIMENT_NAME)
except Exception:
    pass

# Define the trial configurations (AutoML generates these automatically)
trials = [
    # Logistic Regression trials
    {"name": "lr_default", "model": LogisticRegression(max_iter=500, random_state=42)},
    {"name": "lr_l1", "model": LogisticRegression(max_iter=500, penalty="l1", solver="saga", C=0.1, random_state=42)},
    {"name": "lr_l2_strong", "model": LogisticRegression(max_iter=500, C=0.01, random_state=42)},

    # Decision Tree trials
    {"name": "dt_shallow", "model": DecisionTreeClassifier(max_depth=5, random_state=42)},
    {"name": "dt_medium", "model": DecisionTreeClassifier(max_depth=10, min_samples_leaf=10, random_state=42)},

    # Random Forest trials
    {"name": "rf_100_d10", "model": RandomForestClassifier(n_estimators=100, max_depth=10, random_state=42)},
    {"name": "rf_200_d15", "model": RandomForestClassifier(n_estimators=200, max_depth=15, random_state=42)},
    {"name": "rf_300_d20", "model": RandomForestClassifier(n_estimators=300, max_depth=20, min_samples_leaf=5, random_state=42)},

    # Extra Trees trials
    {"name": "et_200_d15", "model": ExtraTreesClassifier(n_estimators=200, max_depth=15, random_state=42)},

    # Gradient Boosting trials
    {"name": "gbt_100_lr01", "model": GradientBoostingClassifier(n_estimators=100, learning_rate=0.1, max_depth=3, random_state=42)},
    {"name": "gbt_200_lr005", "model": GradientBoostingClassifier(n_estimators=200, learning_rate=0.05, max_depth=5, random_state=42)},
    {"name": "gbt_300_lr01", "model": GradientBoostingClassifier(n_estimators=300, learning_rate=0.1, max_depth=5, random_state=42)},
]

print(f"Total trials to run: {len(trials)}")
print(f"Algorithms: LogisticRegression, DecisionTree, RandomForest, ExtraTrees, GradientBoosting")

# COMMAND ----------

# Run all trials
leaderboard = []

for trial in trials:
    start_time = time.time()

    with mlflow.start_run(run_name=trial["name"]) as run:
        model = trial["model"]
        model_params = model.get_params()

        # Log all parameters
        for k, v in model_params.items():
            if v is not None:
                mlflow.log_param(k, v)
        mlflow.log_param("algorithm", type(model).__name__)

        # Train
        model.fit(X_train_imputed, y_train)

        # Predict
        y_pred = model.predict(X_test_imputed)
        y_proba = model.predict_proba(X_test_imputed)[:, 1]

        # Cross-validation score
        cv_scores = cross_val_score(model, X_train_imputed, y_train, cv=5, scoring="f1")

        # Compute metrics
        metrics = {
            "accuracy": accuracy_score(y_test, y_pred),
            "f1_score": f1_score(y_test, y_pred),
            "precision": precision_score(y_test, y_pred),
            "recall": recall_score(y_test, y_pred),
            "auc_roc": roc_auc_score(y_test, y_proba),
            "log_loss": log_loss(y_test, y_proba),
            "cv_f1_mean": cv_scores.mean(),
            "cv_f1_std": cv_scores.std(),
        }

        for k, v in metrics.items():
            mlflow.log_metric(k, v)

        mlflow.sklearn.log_model(model, "model")
        mlflow.set_tag("automl_trial", "true")

        elapsed = time.time() - start_time

        leaderboard.append({
            "rank": 0,  # will be set after sorting
            "trial_name": trial["name"],
            "algorithm": type(model).__name__,
            "f1_score": metrics["f1_score"],
            "accuracy": metrics["accuracy"],
            "auc_roc": metrics["auc_roc"],
            "cv_f1_mean": metrics["cv_f1_mean"],
            "training_time_s": round(elapsed, 2),
            "run_id": run.info.run_id,
        })

print(f"\nAll {len(trials)} trials complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: AutoML Leaderboard

# COMMAND ----------

# Build and display the leaderboard
leaderboard_df = pd.DataFrame(leaderboard)
leaderboard_df = leaderboard_df.sort_values("f1_score", ascending=False).reset_index(drop=True)
leaderboard_df["rank"] = range(1, len(leaderboard_df) + 1)

# Reorder columns for display
display_cols = ["rank", "trial_name", "algorithm", "f1_score", "accuracy", "auc_roc", "cv_f1_mean", "training_time_s"]
leaderboard_display = leaderboard_df[display_cols].copy()

print("AutoML Leaderboard (sorted by F1 Score)")
print("=" * 100)
print(leaderboard_display.to_string(index=False, float_format="%.4f"))
print("=" * 100)

best_trial = leaderboard_df.iloc[0]
print(f"\nBest model: {best_trial['trial_name']} ({best_trial['algorithm']})")
print(f"  F1 Score:  {best_trial['f1_score']:.4f}")
print(f"  Accuracy:  {best_trial['accuracy']:.4f}")
print(f"  AUC-ROC:   {best_trial['auc_roc']:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Analyze the Best Model

# COMMAND ----------

# Load the best model
best_model = mlflow.sklearn.load_model(f"runs:/{best_trial['run_id']}/model")

# Feature importance (for tree-based models)
if hasattr(best_model, "feature_importances_"):
    importance = pd.DataFrame({
        "feature": feature_names,
        "importance": best_model.feature_importances_
    }).sort_values("importance", ascending=False)

    print(f"Feature Importance ({best_trial['algorithm']}):")
    print("-" * 55)
    for _, row in importance.iterrows():
        bar = "#" * int(row["importance"] * 60)
        print(f"  {row['feature']:<30} {row['importance']:.4f} {bar}")

# COMMAND ----------

# Detailed evaluation of the best model
from sklearn.metrics import confusion_matrix, classification_report

y_pred_best = best_model.predict(X_test_imputed)
y_proba_best = best_model.predict_proba(X_test_imputed)[:, 1]

print("Classification Report (Best Model):")
print(classification_report(y_test, y_pred_best, target_names=["Not Churned", "Churned"]))

cm = confusion_matrix(y_test, y_pred_best)
print("Confusion Matrix:")
print(f"  Predicted:    Not Churned  Churned")
print(f"  Not Churned:  {cm[0][0]:>8}    {cm[0][1]:>7}")
print(f"  Churned:      {cm[1][0]:>8}    {cm[1][1]:>7}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Customizing AutoML Output
# MAGIC
# MAGIC AutoML generates a trial notebook for each model. Here is what a
# MAGIC customized version looks like. You would typically:
# MAGIC 1. Start with the generated notebook for the best trial
# MAGIC 2. Add domain-specific features
# MAGIC 3. Adjust hyperparameter ranges
# MAGIC 4. Add custom evaluation metrics

# COMMAND ----------

# Example: Customizing the best trial with additional feature engineering
print("Customizing AutoML output: Adding interaction features\n")

# Create interaction features (domain knowledge that AutoML cannot infer)
X_train_custom = X_train_imputed.copy()
X_test_custom = X_test_imputed.copy()

# Feature interactions
X_train_custom["charge_per_tenure"] = X_train_custom["monthly_charges"] / (X_train_custom["tenure_months"].abs() + 1)
X_train_custom["ticket_rate"] = X_train_custom["num_support_tickets"] / (X_train_custom["tenure_months"].abs() + 1)
X_train_custom["usage_engagement"] = X_train_custom["avg_usage_hours"] * X_train_custom["engagement_index"]

X_test_custom["charge_per_tenure"] = X_test_custom["monthly_charges"] / (X_test_custom["tenure_months"].abs() + 1)
X_test_custom["ticket_rate"] = X_test_custom["num_support_tickets"] / (X_test_custom["tenure_months"].abs() + 1)
X_test_custom["usage_engagement"] = X_test_custom["avg_usage_hours"] * X_test_custom["engagement_index"]

# Retrain the best algorithm with additional features
best_algo = type(best_model)
custom_model = best_algo(**best_model.get_params())
custom_model.fit(X_train_custom, y_train)

y_pred_custom = custom_model.predict(X_test_custom)
y_proba_custom = custom_model.predict_proba(X_test_custom)[:, 1]

print(f"Original best model F1:  {f1_score(y_test, y_pred_best):.4f}")
print(f"Customized model F1:     {f1_score(y_test, y_pred_custom):.4f}")
print(f"Original best model AUC: {roc_auc_score(y_test, y_proba_best):.4f}")
print(f"Customized model AUC:    {roc_auc_score(y_test, y_proba_custom):.4f}")

improvement = f1_score(y_test, y_pred_custom) - f1_score(y_test, y_pred_best)
if improvement > 0:
    print(f"\nCustomization improved F1 by {improvement:.4f}")
else:
    print(f"\nCustomization did not improve F1 (delta: {improvement:.4f})")
    print("This is normal -- AutoML already found good features for this dataset.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Algorithm Comparison by Family

# COMMAND ----------

# Group leaderboard by algorithm family
algo_summary = leaderboard_df.groupby("algorithm").agg(
    best_f1=("f1_score", "max"),
    avg_f1=("f1_score", "mean"),
    best_auc=("auc_roc", "max"),
    num_trials=("trial_name", "count"),
    avg_time_s=("training_time_s", "mean"),
).sort_values("best_f1", ascending=False)

print("Algorithm Family Comparison:")
print("=" * 80)
print(algo_summary.to_string(float_format="%.4f"))
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Databricks AutoML API Reference
# MAGIC
# MAGIC On a full Databricks workspace with ML Runtime, AutoML is available
# MAGIC through the `databricks.automl` module.

# COMMAND ----------

# --- Databricks AutoML API Reference ---
#
# from databricks import automl
#
# # Classification
# summary = automl.classify(
#     dataset=spark_df,                        # Spark DataFrame or table name
#     target_col="churned",                    # target column
#     primary_metric="f1",                     # metric to optimize
#     timeout_minutes=30,                      # max runtime
#     max_trials=20,                           # max models to try
#     exclude_columns=["customer_id", "date"], # columns to exclude
#     experiment_name="/Users/me/churn-automl", # MLflow experiment
#     exclude_frameworks=["sklearn"],           # e.g., only try LightGBM/XGBoost
# )
#
# # Access results
# print(summary.best_trial)                    # best trial info
# print(summary.best_trial.mlflow_run_id)      # run ID
# print(summary.best_trial.metrics)            # all metrics
# print(summary.best_trial.model_path)         # artifact path
#
# # Load best model
# best_model = mlflow.sklearn.load_model(
#     f"runs:/{summary.best_trial.mlflow_run_id}/model"
# )
#
# # Register best model
# mlflow.register_model(
#     f"runs:/{summary.best_trial.mlflow_run_id}/model",
#     name="catalog.schema.churn_predictor"
# )
#
# # Regression
# summary = automl.regress(
#     dataset=spark_df,
#     target_col="price",
#     primary_metric="rmse",
#     timeout_minutes=30
# )
#
# # Forecasting
# summary = automl.forecast(
#     dataset=spark_df,
#     target_col="daily_sales",
#     time_col="date",
#     frequency="D",                # daily
#     horizon=30,                   # predict 30 days ahead
#     timeout_minutes=60,
#     output_database="catalog.schema"
# )

print("Databricks AutoML API reference shown as comments above.")
print("These require a full Databricks workspace with ML Runtime.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: When to Use AutoML vs Manual Training

# COMMAND ----------

print("""
AutoML Decision Guide
======================

USE AutoML WHEN:
  - Starting a new ML problem (get a quick baseline)
  - Exploring which algorithms work best for your data
  - You have tabular data with clear features and target
  - Time is limited and you need a good-enough model fast
  - You want to generate starter code for customization

USE MANUAL TRAINING WHEN:
  - You need deep learning, NLP, or computer vision
  - You have very large data (billions of rows) -> use PySpark MLlib
  - You need custom loss functions or training loops
  - You need specific preprocessing that AutoML does not support
  - You are building ensemble models across multiple data sources

RECOMMENDED WORKFLOW:
  1. Start with AutoML to get a baseline
  2. Review the generated notebooks to understand the data
  3. Customize the best trial notebook with domain features
  4. Register the final model in the Model Registry
  5. Monitor and iterate
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC This notebook used only in-memory data and MLflow logging.
# MAGIC No tables, temp views, or files were created.

# COMMAND ----------

print("Notebook 04-automl complete.")
