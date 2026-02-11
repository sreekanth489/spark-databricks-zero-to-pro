# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 - Model Serving
# MAGIC > Module 20 -- Topic 05 | Deploy models for batch and real-time inference, A/B testing, and monitoring
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Train and register a model suitable for serving
# MAGIC 2. Demonstrate batch inference patterns (pandas locally, spark_udf reference)
# MAGIC 3. Build REST API request/response structures for real-time serving
# MAGIC 4. Implement A/B testing with traffic splitting logic
# MAGIC 5. Monitor prediction distributions and detect drift
# MAGIC 6. Show Databricks Model Serving endpoint configuration templates
# MAGIC
# MAGIC **Note:** Creating actual serving endpoints requires a full Databricks workspace.
# MAGIC This notebook demonstrates the patterns and APIs using local computation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Model Training

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.datasets import make_classification
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient
import json
import time
import warnings

warnings.filterwarnings("ignore")
np.random.seed(42)

print("Libraries imported successfully.")

# COMMAND ----------

# Generate a fraud detection dataset
X, y = make_classification(
    n_samples=5000,
    n_features=12,
    n_informative=9,
    n_redundant=2,
    weights=[0.95, 0.05],  # 5% fraud rate
    random_state=42
)

feature_names = [
    "transaction_amount", "merchant_risk_score", "distance_from_home",
    "time_since_last_txn_hours", "txn_frequency_24h", "card_age_months",
    "num_failed_attempts", "is_international", "device_risk_score",
    "avg_txn_amount_30d", "amount_deviation", "velocity_score"
]

df = pd.DataFrame(X, columns=feature_names)
df["is_fraud"] = y

X_train, X_test, y_train, y_test = train_test_split(
    df[feature_names], df["is_fraud"],
    test_size=0.25, random_state=42, stratify=df["is_fraud"]
)

print(f"Dataset: {df.shape[0]} transactions, {df['is_fraud'].mean():.2%} fraud rate")
print(f"Train: {X_train.shape[0]} | Test: {X_test.shape[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Train Two Model Versions for A/B Testing

# COMMAND ----------

EXPERIMENT_NAME = "/tmp/module20-model-serving-demo"
try:
    mlflow.set_experiment(EXPERIMENT_NAME)
except Exception:
    pass

MODEL_NAME = "fraud_detector"

# Version A: Champion (Random Forest)
with mlflow.start_run(run_name="champion_rf") as run_a:
    model_a = RandomForestClassifier(
        n_estimators=200, max_depth=12, class_weight="balanced", random_state=42
    )
    model_a.fit(X_train, y_train)

    y_pred_a = model_a.predict(X_test)
    y_proba_a = model_a.predict_proba(X_test)[:, 1]

    metrics_a = {
        "accuracy": accuracy_score(y_test, y_pred_a),
        "f1_score": f1_score(y_test, y_pred_a),
        "auc_roc": roc_auc_score(y_test, y_proba_a),
    }
    for k, v in metrics_a.items():
        mlflow.log_metric(k, v)
    mlflow.sklearn.log_model(model_a, "model")
    run_id_a = run_a.info.run_id

print(f"Champion (RF):   F1={metrics_a['f1_score']:.4f}, AUC={metrics_a['auc_roc']:.4f}")

# Version B: Challenger (Gradient Boosting)
with mlflow.start_run(run_name="challenger_gbt") as run_b:
    model_b = GradientBoostingClassifier(
        n_estimators=300, learning_rate=0.1, max_depth=5, random_state=42
    )
    model_b.fit(X_train, y_train)

    y_pred_b = model_b.predict(X_test)
    y_proba_b = model_b.predict_proba(X_test)[:, 1]

    metrics_b = {
        "accuracy": accuracy_score(y_test, y_pred_b),
        "f1_score": f1_score(y_test, y_pred_b),
        "auc_roc": roc_auc_score(y_test, y_proba_b),
    }
    for k, v in metrics_b.items():
        mlflow.log_metric(k, v)
    mlflow.sklearn.log_model(model_b, "model")
    run_id_b = run_b.info.run_id

print(f"Challenger (GBT): F1={metrics_b['f1_score']:.4f}, AUC={metrics_b['auc_roc']:.4f}")

# Register both
mv_a = mlflow.register_model(f"runs:/{run_id_a}/model", MODEL_NAME)
mv_b = mlflow.register_model(f"runs:/{run_id_b}/model", MODEL_NAME)
print(f"\nRegistered {MODEL_NAME}: Champion=v{mv_a.version}, Challenger=v{mv_b.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Batch Inference Pattern
# MAGIC
# MAGIC Score a large dataset using the model. Locally, we use pandas.
# MAGIC On Databricks, you would use `mlflow.pyfunc.spark_udf()` for distributed scoring.

# COMMAND ----------

# Simulate a batch of new transactions to score
batch_size = 1000
new_transactions = pd.DataFrame(
    np.random.randn(batch_size, len(feature_names)),
    columns=feature_names
)
new_transactions["transaction_id"] = [f"TXN{str(i).zfill(6)}" for i in range(batch_size)]

print(f"New transactions to score: {batch_size}")

# Batch inference with pandas (local pattern)
start_time = time.time()
predictions = model_a.predict(new_transactions[feature_names])
probabilities = model_a.predict_proba(new_transactions[feature_names])[:, 1]
elapsed = time.time() - start_time

new_transactions["fraud_prediction"] = predictions
new_transactions["fraud_probability"] = probabilities
new_transactions["risk_level"] = pd.cut(
    probabilities,
    bins=[0, 0.1, 0.5, 0.8, 1.0],
    labels=["LOW", "MEDIUM", "HIGH", "CRITICAL"]
)

print(f"Scoring time: {elapsed:.3f}s ({batch_size/elapsed:.0f} txns/sec)")
print(f"\nPrediction distribution:")
print(new_transactions["risk_level"].value_counts().to_string())

print(f"\nSample scored transactions:")
print(new_transactions[["transaction_id", "fraud_probability", "risk_level"]].head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark UDF Batch Inference (Databricks Reference)

# COMMAND ----------

# --- Databricks spark_udf pattern ---
# On Databricks, use mlflow.pyfunc.spark_udf() for distributed batch scoring:
#
# import mlflow
# from pyspark.sql import functions as F
#
# # Load the model as a Spark UDF
# predict_udf = mlflow.pyfunc.spark_udf(
#     spark,
#     model_uri="models:/catalog.schema.fraud_detector@Champion",
#     result_type="double"  # or "string" for classification labels
# )
#
# # Score a Delta table (distributed across all executors)
# feature_cols = [F.col(c) for c in feature_names]
# scored_df = (
#     spark.read.table("catalog.schema.new_transactions")
#     .withColumn("fraud_probability", predict_udf(*feature_cols))
#     .withColumn("risk_level",
#         F.when(F.col("fraud_probability") > 0.8, "CRITICAL")
#         .when(F.col("fraud_probability") > 0.5, "HIGH")
#         .when(F.col("fraud_probability") > 0.1, "MEDIUM")
#         .otherwise("LOW")
#     )
# )
#
# # Write predictions to a Delta table
# scored_df.write.mode("overwrite").saveAsTable(
#     "catalog.schema.fraud_predictions"
# )
#
# print(f"Scored {scored_df.count()} transactions")

print("Spark UDF batch inference pattern shown as comments above.")
print("This distributes prediction across all cluster executors.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Real-Time Serving API Simulation
# MAGIC
# MAGIC Model Serving endpoints accept JSON payloads and return predictions.
# MAGIC We simulate the request/response format here.

# COMMAND ----------

class ModelServingSimulator:
    """
    Simulates a Databricks Model Serving endpoint.
    In production, this is a managed REST service.
    """

    def __init__(self, models, traffic_config):
        """
        models: dict of {"model_name": sklearn_model}
        traffic_config: dict of {"model_name": traffic_percentage}
        """
        self.models = models
        self.traffic_config = traffic_config
        self.request_log = []

    def invoke(self, payload):
        """Process a prediction request (simulates REST API call)."""
        start_time = time.time()

        # Route to a model based on traffic split
        model_name = self._route_request()
        model = self.models[model_name]

        # Parse input
        records = payload.get("dataframe_records", [])
        if not records:
            return {"error": "No records provided"}, 400

        input_df = pd.DataFrame(records)
        missing_cols = set(feature_names) - set(input_df.columns)
        if missing_cols:
            return {"error": f"Missing features: {missing_cols}"}, 400

        # Predict
        probabilities = model.predict_proba(input_df[feature_names])[:, 1]
        predictions = model.predict(input_df[feature_names])

        latency_ms = (time.time() - start_time) * 1000

        # Log the request
        self.request_log.append({
            "timestamp": time.time(),
            "model": model_name,
            "num_records": len(records),
            "latency_ms": latency_ms,
            "predictions": predictions.tolist(),
        })

        return {
            "predictions": [
                {
                    "fraud_probability": round(float(p), 4),
                    "is_fraud": int(pred),
                }
                for p, pred in zip(probabilities, predictions)
            ],
            "metadata": {
                "model_name": model_name,
                "latency_ms": round(latency_ms, 2),
            }
        }, 200

    def _route_request(self):
        """Route based on traffic percentages."""
        rand = np.random.random() * 100
        cumulative = 0
        for model_name, pct in self.traffic_config.items():
            cumulative += pct
            if rand <= cumulative:
                return model_name
        return list(self.models.keys())[-1]


# Create the serving simulator
endpoint = ModelServingSimulator(
    models={
        "champion_v1": model_a,
        "challenger_v2": model_b,
    },
    traffic_config={
        "champion_v1": 90,
        "challenger_v2": 10,
    }
)

print("Model Serving endpoint simulator created.")
print("Traffic split: champion_v1=90%, challenger_v2=10%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Making Predictions via the Serving Endpoint

# COMMAND ----------

# Single prediction request
single_request = {
    "dataframe_records": [
        {
            "transaction_amount": 2.5,
            "merchant_risk_score": 1.8,
            "distance_from_home": 3.2,
            "time_since_last_txn_hours": -0.5,
            "txn_frequency_24h": 0.3,
            "card_age_months": -1.2,
            "num_failed_attempts": 2.1,
            "is_international": 1.5,
            "device_risk_score": 0.8,
            "avg_txn_amount_30d": -0.3,
            "amount_deviation": 1.9,
            "velocity_score": 0.7,
        }
    ]
}

response, status = endpoint.invoke(single_request)
print(f"Status: {status}")
print(f"Response: {json.dumps(response, indent=2)}")

# COMMAND ----------

# Batch request (multiple records)
batch_request = {
    "dataframe_records": X_test.head(5).to_dict(orient="records")
}

response, status = endpoint.invoke(batch_request)
print(f"Batch request ({len(batch_request['dataframe_records'])} records):")
print(f"Status: {status}")
for i, pred in enumerate(response["predictions"]):
    actual = y_test.values[i]
    print(f"  Record {i+1}: prob={pred['fraud_probability']:.4f}, "
          f"pred={pred['is_fraud']}, actual={actual}")
print(f"Served by: {response['metadata']['model_name']}")
print(f"Latency: {response['metadata']['latency_ms']:.2f}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: A/B Testing Simulation
# MAGIC
# MAGIC Send many requests and track which model serves each one.
# MAGIC Compare the performance of Champion vs Challenger in production traffic.

# COMMAND ----------

# Send 500 requests to simulate production traffic
np.random.seed(42)
num_requests = 500

for i in range(num_requests):
    # Pick a random test sample
    idx = np.random.randint(0, len(X_test))
    request = {
        "dataframe_records": [X_test.iloc[idx].to_dict()]
    }
    endpoint.invoke(request)

# Analyze traffic distribution
traffic_log = pd.DataFrame(endpoint.request_log)
traffic_by_model = traffic_log["model"].value_counts()

print(f"A/B Test Traffic Analysis ({num_requests} requests)")
print("=" * 50)
for model_name, count in traffic_by_model.items():
    pct = count / num_requests * 100
    print(f"  {model_name}: {count} requests ({pct:.1f}%)")

# Compare prediction distributions
print(f"\nPrediction Distribution by Model:")
print("-" * 50)
for model_name in traffic_log["model"].unique():
    model_preds = traffic_log[traffic_log["model"] == model_name]["predictions"]
    all_preds = [p[0] for p in model_preds]
    fraud_rate = np.mean(all_preds)
    print(f"  {model_name}: predicted fraud rate = {fraud_rate:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Latency Monitoring

# COMMAND ----------

# Analyze latency distribution
print("Latency Distribution:")
print("=" * 50)
latencies = traffic_log["latency_ms"]
print(f"  P50: {latencies.quantile(0.50):.2f}ms")
print(f"  P75: {latencies.quantile(0.75):.2f}ms")
print(f"  P90: {latencies.quantile(0.90):.2f}ms")
print(f"  P95: {latencies.quantile(0.95):.2f}ms")
print(f"  P99: {latencies.quantile(0.99):.2f}ms")
print(f"  Max: {latencies.max():.2f}ms")
print(f"  Mean: {latencies.mean():.2f}ms")

# Latency by model
print(f"\nLatency by Model:")
for model_name in traffic_log["model"].unique():
    model_latencies = traffic_log[traffic_log["model"] == model_name]["latency_ms"]
    print(f"  {model_name}: P50={model_latencies.quantile(0.50):.2f}ms, "
          f"P99={model_latencies.quantile(0.99):.2f}ms")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Prediction Drift Detection
# MAGIC
# MAGIC Monitor how prediction distributions change over time. Drift in
# MAGIC predictions often indicates data drift or model degradation.

# COMMAND ----------

def detect_prediction_drift(baseline_predictions, current_predictions, threshold=0.05):
    """
    Compare prediction distributions between baseline and current.
    Uses a simple mean/std comparison. In production, use KS-test or PSI.
    """
    baseline_mean = np.mean(baseline_predictions)
    current_mean = np.mean(current_predictions)
    baseline_std = np.std(baseline_predictions)
    current_std = np.std(current_predictions)

    mean_shift = abs(current_mean - baseline_mean)
    std_shift = abs(current_std - baseline_std)

    drift_detected = mean_shift > threshold or std_shift > threshold

    return {
        "baseline_mean": round(baseline_mean, 4),
        "current_mean": round(current_mean, 4),
        "mean_shift": round(mean_shift, 4),
        "baseline_std": round(baseline_std, 4),
        "current_std": round(current_std, 4),
        "std_shift": round(std_shift, 4),
        "drift_detected": drift_detected,
        "threshold": threshold,
    }


# Baseline: predictions on the test set
baseline_probs = model_a.predict_proba(X_test)[:, 1]

# Simulate "current" traffic with slight distribution shift
X_current_shifted = X_test.copy()
X_current_shifted["transaction_amount"] += np.random.normal(0.5, 0.3, len(X_test))
X_current_shifted["device_risk_score"] += np.random.normal(0.3, 0.2, len(X_test))
current_probs = model_a.predict_proba(X_current_shifted[feature_names])[:, 1]

# Check for drift
drift_result = detect_prediction_drift(baseline_probs, current_probs, threshold=0.02)

print("Prediction Drift Analysis:")
print("=" * 50)
for key, value in drift_result.items():
    print(f"  {key}: {value}")
if drift_result["drift_detected"]:
    print("\n  WARNING: Prediction drift detected!")
    print("  Action: Investigate input data distribution changes.")
    print("  Consider retraining the model with recent data.")
else:
    print("\n  OK: No significant prediction drift detected.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Serving Endpoint Configuration Templates

# COMMAND ----------

# --- Databricks Model Serving Endpoint Configuration ---
#
# from databricks.sdk import WorkspaceClient
# from databricks.sdk.service.serving import (
#     EndpointCoreConfigInput, ServedEntityInput, TrafficConfig, Route
# )
#
# w = WorkspaceClient()
#
# # Create a serving endpoint
# endpoint = w.serving_endpoints.create_and_wait(
#     name="fraud-detector",
#     config=EndpointCoreConfigInput(
#         served_entities=[
#             ServedEntityInput(
#                 entity_name="catalog.schema.fraud_detector",
#                 entity_version="3",
#                 workload_size="Small",
#                 scale_to_zero_enabled=True,
#             )
#         ]
#     )
# )
#
# # A/B test with traffic splitting
# w.serving_endpoints.update_config_and_wait(
#     name="fraud-detector",
#     served_entities=[
#         ServedEntityInput(
#             entity_name="catalog.schema.fraud_detector",
#             entity_version="3",
#             workload_size="Small",
#             scale_to_zero_enabled=True,
#         ),
#         ServedEntityInput(
#             entity_name="catalog.schema.fraud_detector",
#             entity_version="4",
#             workload_size="Small",
#             scale_to_zero_enabled=True,
#         ),
#     ],
#     traffic_config=TrafficConfig(
#         routes=[
#             Route(served_model_name="fraud_detector-3", traffic_percentage=90),
#             Route(served_model_name="fraud_detector-4", traffic_percentage=10),
#         ]
#     )
# )
#
# # Query the endpoint
# import requests
#
# url = f"https://{workspace_url}/serving-endpoints/fraud-detector/invocations"
# headers = {
#     "Authorization": f"Bearer {token}",
#     "Content-Type": "application/json"
# }
# payload = {
#     "dataframe_records": [
#         {"transaction_amount": 500, "merchant_risk_score": 0.8, ...}
#     ]
# }
# response = requests.post(url, headers=headers, json=payload)
# print(response.json())
#
# # Delete endpoint when done
# w.serving_endpoints.delete(name="fraud-detector")

print("Databricks Serving Endpoint configuration templates shown as comments above.")
print("These require a full Databricks workspace with Model Serving enabled.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: REST API Request/Response Reference

# COMMAND ----------

# Document the expected API format
api_reference = {
    "endpoint_url": "https://<workspace>.cloud.databricks.com/serving-endpoints/<endpoint-name>/invocations",
    "method": "POST",
    "headers": {
        "Authorization": "Bearer <personal-access-token>",
        "Content-Type": "application/json"
    },
    "request_body_format": {
        "dataframe_records": [
            {"feature_1": "value", "feature_2": "value", "...": "..."}
        ]
    },
    "response_format": {
        "predictions": [0.85, 0.12, 0.67]
    },
    "alternative_input_formats": {
        "dataframe_split": {
            "columns": ["feature_1", "feature_2"],
            "data": [[1.0, 2.0], [3.0, 4.0]]
        },
        "instances": [
            {"feature_1": 1.0, "feature_2": 2.0},
            {"feature_1": 3.0, "feature_2": 4.0}
        ]
    }
}

print("REST API Reference:")
print(json.dumps(api_reference, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Serving Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("  MODULE 20, TOPIC 05: Model Serving -- Summary")
print("=" * 70)
print(f"""
  Inference Patterns:
    Batch:     mlflow.pyfunc.spark_udf() for distributed scoring
    Real-time: REST API endpoint (JSON in, JSON out)

  A/B Testing:
    Champion (v{mv_a.version}): 90% traffic, AUC={metrics_a['auc_roc']:.4f}
    Challenger (v{mv_b.version}): 10% traffic, AUC={metrics_b['auc_roc']:.4f}

  Monitoring:
    Latency: P50={latencies.quantile(0.50):.2f}ms, P99={latencies.quantile(0.99):.2f}ms
    Drift: {'Detected' if drift_result['drift_detected'] else 'Not detected'}

  Key APIs:
    mlflow.pyfunc.spark_udf()       -- batch inference
    requests.post(url, json=payload) -- real-time serving
    WorkspaceClient().serving_endpoints -- endpoint management
""")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up registered model
client = MlflowClient()
try:
    for v in [mv_a.version, mv_b.version]:
        client.delete_model_version(name=MODEL_NAME, version=v)
    client.delete_registered_model(name=MODEL_NAME)
    print(f"Cleaned up registered model '{MODEL_NAME}'.")
except Exception as e:
    print(f"Cleanup: {e}")

print("Notebook 05-model-serving complete.")
