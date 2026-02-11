# Model Serving
> Module 20 -- Topic 05 | Level: Advanced | Time: 50 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain the difference between batch inference and real-time serving
2. Describe how Databricks Model Serving endpoints work
3. Configure a serving endpoint with scaling, compute, and traffic rules
4. Implement A/B testing with traffic splitting between model versions
5. Use `mlflow.pyfunc.spark_udf()` for batch inference at scale
6. Call model serving REST APIs for real-time predictions
7. Monitor served models for latency, errors, and data drift

---

## Conceptual Overview

### Batch Inference vs Real-Time Serving

Machine learning predictions come in two flavors:

```
  Inference Patterns
  ====================

  BATCH INFERENCE                      REAL-TIME SERVING
  +-------------------+                +-------------------+
  | Scheduled Job     |                | REST Endpoint     |
  | (hourly/daily)    |                | (always on)       |
  +-------------------+                +-------------------+
        |                                     |
        v                                     v
  +-------------------+                +-------------------+
  | Read from         |                | Receive single    |
  | Delta table       |                | request (JSON)    |
  | (millions of rows)|                | (1-100 records)   |
  +-------------------+                +-------------------+
        |                                     |
        v                                     v
  +-------------------+                +-------------------+
  | Score all rows    |                | Score instantly    |
  | with spark_udf()  |                | (~10-100ms)       |
  +-------------------+                +-------------------+
        |                                     |
        v                                     v
  +-------------------+                +-------------------+
  | Write predictions |                | Return JSON       |
  | to Delta table    |                | response           |
  +-------------------+                +-------------------+

  USE BATCH WHEN:                      USE REAL-TIME WHEN:
  - Predictions needed periodically    - Predictions needed immediately
  - Large volumes (millions of rows)   - User-facing applications
  - Latency not critical (minutes OK)  - Low latency required (<100ms)
  - Cost efficiency is priority        - Individual predictions
```

### Databricks Model Serving Architecture

```
  Model Serving Endpoint
  ========================

  Client Application
        |
        | HTTPS REST API
        v
  +-------------------------------------+
  | Serving Endpoint                     |
  | "churn-predictor"                    |
  |                                      |
  |  Traffic Routing                     |
  |  +------------+    +------------+    |
  |  | Version A  |    | Version B  |    |
  |  | 90% traffic|    | 10% traffic|    |
  |  | (Champion) |    | (Challenger)|   |
  |  +------------+    +------------+    |
  |                                      |
  |  Auto-scaling                        |
  |  min: 0 (scale to zero)             |
  |  max: 4 (concurrent instances)       |
  |  target: 100 QPS per instance        |
  +-------------------------------------+
        |
        v
  Model loaded from
  Unity Catalog / Model Registry
```

### Creating a Serving Endpoint

On Databricks, endpoints are created via UI, API, or SDK:

```python
import requests

# Create endpoint via REST API
endpoint_config = {
    "name": "churn-predictor",
    "config": {
        "served_entities": [
            {
                "entity_name": "catalog.schema.churn_predictor",
                "entity_version": "3",
                "workload_size": "Small",
                "scale_to_zero_enabled": True
            }
        ],
        "traffic_config": {
            "routes": [
                {
                    "served_model_name": "churn_predictor-3",
                    "traffic_percentage": 100
                }
            ]
        }
    }
}
```

### Workload Sizes

| Size | CPU | Memory | Concurrency | Use Case |
|------|-----|--------|-------------|----------|
| Small | 4 cores | 16 GB | ~4 QPS | Development, low traffic |
| Medium | 8 cores | 32 GB | ~16 QPS | Medium traffic |
| Large | 16 cores | 64 GB | ~64 QPS | High traffic, large models |
| GPU Small | 1 GPU | 16 GB | Varies | Deep learning models |

### A/B Testing with Traffic Splitting

Traffic splitting lets you safely roll out new model versions:

```
  A/B Test Configuration
  ========================

  Endpoint: churn-predictor
  |
  +-- Route A: churn_predictor-v2 (Champion)
  |   Traffic: 90%
  |   Model: GradientBoosting, AUC=0.93
  |
  +-- Route B: churn_predictor-v3 (Challenger)
      Traffic: 10%
      Model: XGBoost, AUC=0.95 (needs validation)

  After confirming v3 performs well in production:
  - Move to 50/50 split
  - Then to 100% on v3
  - Archive v2
```

### REST API for Predictions

```python
import requests
import json

# Databricks serving endpoint URL
url = "https://<workspace>.cloud.databricks.com/serving-endpoints/churn-predictor/invocations"

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Single prediction
payload = {
    "dataframe_records": [
        {
            "tenure_months": 24,
            "monthly_charges": 79.99,
            "num_support_tickets": 3,
            "satisfaction_score": 6.5
        }
    ]
}

response = requests.post(url, headers=headers, json=payload)
predictions = response.json()
```

### Batch Inference with spark_udf

For scoring large datasets, `mlflow.pyfunc.spark_udf()` converts any MLflow
model into a Spark UDF that runs distributed across the cluster:

```python
import mlflow

# Load model as a Spark UDF
predict_udf = mlflow.pyfunc.spark_udf(
    spark,
    model_uri="models:/catalog.schema.churn_predictor@Champion"
)

# Score a Delta table (distributed)
scored_df = (
    spark.read.table("catalog.schema.customers")
    .withColumn("churn_prediction", predict_udf(*feature_columns))
)

scored_df.write.mode("overwrite").saveAsTable("catalog.schema.customer_predictions")
```

### Monitoring Served Models

Key metrics to monitor:

| Metric | What It Tells You | Alert Threshold |
|--------|------------------|----------------|
| Request latency (P50, P99) | Model speed | P99 > 500ms |
| Error rate | Model health | > 1% |
| Request volume | Usage pattern | Sudden drop or spike |
| Input data distribution | Data drift | Significant shift from training |
| Prediction distribution | Model drift | Shift in output probabilities |

```
  Monitoring Dashboard
  =====================

  Requests/min:  [=========>         ] 450 RPM
  P50 Latency:   [====>              ] 23ms
  P99 Latency:   [==========>        ] 87ms
  Error Rate:    [>                  ] 0.1%
  Uptime:        [==================>] 99.97%
```

---

## Hands-On Walkthrough

Open `05-model-serving_notebook.py` to practice:
- Training and registering a model for serving
- Simulating batch inference with pandas (and spark_udf reference)
- Building REST API request/response patterns
- Implementing A/B testing logic with traffic splitting
- Monitoring predictions and detecting drift
- Databricks Serving Endpoint configuration templates

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Serverless serving | Supported | Supported | Supported |
| GPU serving | p4d, g5 instances | NC-series | A2, T4 instances |
| Scale to zero | Supported | Supported | Supported |
| Custom containers | Supported | Supported | Supported |
| External endpoints | SageMaker compatible | AzureML compatible | Vertex AI compatible |
| Authentication | PAT or OAuth | PAT or OAuth | PAT or OAuth |

---

## Certification Tip

> **Databricks ML Professional**: Know the difference between batch inference
> (spark_udf) and real-time serving (REST endpoints). Understand traffic splitting
> for A/B testing and how to configure endpoint scaling. Expect questions about
> when to use scale-to-zero vs always-on endpoints.
>
> **Key concept**: `mlflow.pyfunc.spark_udf()` converts any MLflow model into a
> Spark UDF for distributed batch scoring. The model runs on each executor, not
> on the driver. This is the standard pattern for scoring large Delta tables.

---

## Key Takeaways

1. **Batch inference** (spark_udf) is for scoring large datasets periodically.
   **Real-time serving** (REST endpoints) is for immediate, individual predictions.
2. **Databricks Model Serving** provides managed, auto-scaling REST endpoints.
   Scale-to-zero eliminates cost when there is no traffic.
3. **Traffic splitting** enables A/B testing. Route a small percentage of traffic
   to a new model version, validate it works, then gradually increase.
4. **`mlflow.pyfunc.spark_udf()`** is the standard tool for batch inference on
   Spark. It distributes prediction across all executors.
5. **Monitor everything**: latency, error rate, data drift, and prediction drift.
   A model that was good at training time can degrade silently in production.
6. **Use aliases** (Champion/Challenger) so serving endpoints reference a stable
   label. Promoting a new model version = moving the alias.

---

## Next Steps

- Proceed to **Topic 06: PySpark ML Pipeline** to learn how to build distributed
  ML pipelines with Spark MLlib for large-scale training.
- Deploy your best model from Topics 01-04 to a serving endpoint.
