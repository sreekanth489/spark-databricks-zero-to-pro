# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 03 - Feature Store
# MAGIC > Module 20 -- Topic 03 | Create feature tables, perform lookups, and prevent training-serving skew
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate synthetic customer transaction data
# MAGIC 2. Engineer features from raw data (aggregations, ratios, time-based)
# MAGIC 3. Create a feature table with primary keys and timestamp keys
# MAGIC 4. Build a training dataset using feature lookups
# MAGIC 5. Demonstrate point-in-time correctness
# MAGIC 6. Train a model using Feature Store-managed features
# MAGIC 7. Show Databricks Feature Engineering API patterns (as reference)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Raw Data Generation

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, f1_score, roc_auc_score
import warnings

warnings.filterwarnings("ignore")
np.random.seed(42)

print("Libraries imported successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Synthetic Customer Transaction Data
# MAGIC We create raw transaction data, then engineer features from it.
# MAGIC This mirrors a real workflow: raw events -> feature engineering -> feature table.

# COMMAND ----------

# Generate 200 customers with varying behaviors
num_customers = 200
customer_ids = [f"C{str(i).zfill(4)}" for i in range(1, num_customers + 1)]

# Customer base attributes
customers = pd.DataFrame({
    "customer_id": customer_ids,
    "signup_date": [
        datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, 365))
        for _ in range(num_customers)
    ],
    "segment": np.random.choice(["premium", "standard", "basic"], num_customers, p=[0.2, 0.5, 0.3]),
})

print(f"Customers: {len(customers)}")
print(customers.head(10).to_string(index=False))

# COMMAND ----------

# Generate transactions over 12 months
transactions = []
for _, customer in customers.iterrows():
    cid = customer["customer_id"]
    seg = customer["segment"]

    # Number of transactions varies by segment
    base_txns = {"premium": 50, "standard": 30, "basic": 15}
    n_txns = np.random.poisson(base_txns[seg])

    for _ in range(n_txns):
        txn_date = datetime(2024, 1, 1) + timedelta(days=np.random.randint(0, 365))
        amount_mean = {"premium": 150, "standard": 80, "basic": 40}
        amount = max(5, np.random.normal(amount_mean[seg], amount_mean[seg] * 0.4))
        category = np.random.choice(
            ["electronics", "clothing", "food", "entertainment", "home"],
            p=[0.15, 0.25, 0.30, 0.15, 0.15]
        )
        transactions.append({
            "customer_id": cid,
            "transaction_date": txn_date,
            "amount": round(amount, 2),
            "category": category,
        })

transactions_df = pd.DataFrame(transactions)
print(f"\nTransactions: {len(transactions_df)}")
print(transactions_df.head(10).to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Feature Engineering
# MAGIC
# MAGIC We compute features from the raw transactions. In a production Feature Store,
# MAGIC this logic runs as a scheduled pipeline that updates the feature table.

# COMMAND ----------

def compute_customer_features(transactions_df, as_of_date):
    """
    Compute customer features using transactions up to (but not after) as_of_date.
    This is the feature engineering logic that would run in a scheduled pipeline.
    """
    # Filter transactions up to the as_of_date
    mask = transactions_df["transaction_date"] <= as_of_date
    txns = transactions_df[mask].copy()

    features = txns.groupby("customer_id").agg(
        total_spend=("amount", "sum"),
        avg_transaction_amount=("amount", "mean"),
        transaction_count=("amount", "count"),
        max_single_transaction=("amount", "max"),
        min_single_transaction=("amount", "min"),
        std_transaction_amount=("amount", "std"),
        num_categories=("category", "nunique"),
        last_transaction_date=("transaction_date", "max"),
    ).reset_index()

    # Derived features
    features["days_since_last_transaction"] = (
        as_of_date - features["last_transaction_date"]
    ).dt.days

    features["spend_volatility"] = (
        features["std_transaction_amount"] / features["avg_transaction_amount"]
    ).fillna(0)

    # Add the computation timestamp
    features["feature_timestamp"] = as_of_date

    # Drop intermediate columns
    features = features.drop(columns=["last_transaction_date"])

    # Fill NaN for customers with single transactions (std is NaN)
    features["std_transaction_amount"] = features["std_transaction_amount"].fillna(0)

    return features


# Compute features as of different dates (simulates periodic updates)
feature_snapshots = []
snapshot_dates = [
    datetime(2024, 3, 31),
    datetime(2024, 6, 30),
    datetime(2024, 9, 30),
    datetime(2024, 12, 31),
]

for snap_date in snapshot_dates:
    snapshot = compute_customer_features(transactions_df, snap_date)
    feature_snapshots.append(snapshot)
    print(f"Snapshot {snap_date.date()}: {len(snapshot)} customers, "
          f"avg spend=${snapshot['total_spend'].mean():.2f}")

# Combine all snapshots (this simulates a feature table with history)
feature_table = pd.concat(feature_snapshots, ignore_index=True)
print(f"\nFull feature table: {len(feature_table)} rows "
      f"({len(feature_table['customer_id'].unique())} customers x {len(snapshot_dates)} snapshots)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Examine the Feature Table

# COMMAND ----------

# Show feature table structure
print("Feature Table Schema:")
print("-" * 60)
for col in feature_table.columns:
    print(f"  {col:<35} {feature_table[col].dtype}")

print(f"\nPrimary Key: customer_id")
print(f"Timestamp Key: feature_timestamp")

print(f"\nSample rows (customer C0001 across all snapshots):")
c0001 = feature_table[feature_table["customer_id"] == "C0001"].sort_values("feature_timestamp")
print(c0001.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Create Labels (Churn Prediction)
# MAGIC
# MAGIC We generate churn labels at specific dates. The feature lookup must
# MAGIC use features computed BEFORE the label date (point-in-time correctness).

# COMMAND ----------

# Generate churn labels at two different dates
label_dates = [datetime(2024, 7, 15), datetime(2024, 10, 15)]
labels = []

for cid in customer_ids:
    for label_date in label_dates:
        # Simulate churn probability based on customer behavior
        cust_txns = transactions_df[
            (transactions_df["customer_id"] == cid) &
            (transactions_df["transaction_date"] <= label_date)
        ]
        if len(cust_txns) == 0:
            churn_prob = 0.8  # No transactions -> likely churned
        else:
            days_inactive = (label_date - cust_txns["transaction_date"].max()).days
            avg_spend = cust_txns["amount"].mean()
            churn_prob = min(0.95, max(0.05, 0.3 + days_inactive * 0.005 - avg_spend * 0.001))

        churned = 1 if np.random.random() < churn_prob else 0
        labels.append({
            "customer_id": cid,
            "label_date": label_date,
            "churned": churned,
        })

labels_df = pd.DataFrame(labels)
print(f"Labels: {len(labels_df)} rows")
print(f"Churn rate: {labels_df['churned'].mean():.2%}")
print(f"\nLabel dates: {[d.date() for d in label_dates]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Point-in-Time Feature Lookup
# MAGIC
# MAGIC This is the critical operation. For each label, we look up features
# MAGIC using the most recent snapshot BEFORE the label date.
# MAGIC
# MAGIC - Label date 2024-07-15 -> Use features from 2024-06-30 (not 2024-09-30!)
# MAGIC - Label date 2024-10-15 -> Use features from 2024-09-30 (not 2024-12-31!)

# COMMAND ----------

def point_in_time_lookup(labels_df, feature_table, lookup_key, timestamp_key, label_timestamp_key):
    """
    Perform a point-in-time join: for each label row, find the most recent
    feature snapshot that is on or before the label timestamp.

    This is what Databricks Feature Engineering does automatically with
    FeatureLookup + timestamp_lookup_key.
    """
    result_rows = []

    for _, label_row in labels_df.iterrows():
        cid = label_row[lookup_key]
        label_ts = label_row[label_timestamp_key]

        # Find features for this customer with timestamp <= label timestamp
        candidate_features = feature_table[
            (feature_table[lookup_key] == cid) &
            (feature_table[timestamp_key] <= label_ts)
        ]

        if len(candidate_features) > 0:
            # Take the most recent snapshot
            best_match = candidate_features.sort_values(timestamp_key).iloc[-1]
            row = label_row.to_dict()
            for col in feature_table.columns:
                if col not in [lookup_key, timestamp_key]:
                    row[f"feature_{col}"] if col == "feature_timestamp" else None
                    row[col] = best_match[col]
            row["_feature_timestamp_used"] = best_match[timestamp_key]
            result_rows.append(row)
        else:
            # No features available before label date
            row = label_row.to_dict()
            row["_feature_timestamp_used"] = None
            result_rows.append(row)

    return pd.DataFrame(result_rows)


# Perform the point-in-time lookup
training_data = point_in_time_lookup(
    labels_df=labels_df,
    feature_table=feature_table,
    lookup_key="customer_id",
    timestamp_key="feature_timestamp",
    label_timestamp_key="label_date"
)

print(f"Training dataset: {len(training_data)} rows")
print(f"\nPoint-in-time correctness check:")
pit_check = training_data[["customer_id", "label_date", "_feature_timestamp_used"]].drop_duplicates()
for label_date in label_dates:
    subset = pit_check[pit_check["label_date"] == label_date]
    feature_ts = subset["_feature_timestamp_used"].dropna().unique()
    print(f"  Label date {label_date.date()} -> Feature timestamps used: {[str(t.date()) if pd.notna(t) else 'None' for t in feature_ts]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Verify Point-in-Time Correctness
# MAGIC
# MAGIC The critical assertion: no feature timestamp should be AFTER the label date.

# COMMAND ----------

# Verify no future leakage
violations = training_data[
    training_data["_feature_timestamp_used"] > training_data["label_date"]
]
print(f"Point-in-time violations (future leakage): {len(violations)}")
assert len(violations) == 0, "TEMPORAL LEAKAGE DETECTED!"
print("PASSED: All feature lookups use only past data. No temporal leakage.")

# Show the temporal relationship
print(f"\nTemporal alignment summary:")
for label_date in label_dates:
    subset = training_data[training_data["label_date"] == label_date]
    max_feature_ts = subset["_feature_timestamp_used"].max()
    print(f"  Label: {label_date.date()} | Latest feature used: {max_feature_ts.date() if pd.notna(max_feature_ts) else 'N/A'} | "
          f"Gap: {(label_date - max_feature_ts).days if pd.notna(max_feature_ts) else 'N/A'} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Train a Model Using Feature Store Data

# COMMAND ----------

# Prepare the training dataset
feature_columns = [
    "total_spend", "avg_transaction_amount", "transaction_count",
    "max_single_transaction", "min_single_transaction", "std_transaction_amount",
    "num_categories", "days_since_last_transaction", "spend_volatility"
]

# Drop rows where features are missing (customers with no transactions before label date)
train_ready = training_data.dropna(subset=feature_columns)
print(f"Training rows (after dropping missing): {len(train_ready)}")

X = train_ready[feature_columns]
y = train_ready["churned"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

# Train a model
model = RandomForestClassifier(n_estimators=200, max_depth=10, random_state=42)
model.fit(X_train, y_train)

# Evaluate
y_pred = model.predict(X_test)
y_proba = model.predict_proba(X_test)[:, 1]

print(f"\nModel Performance (trained on Feature Store features):")
print(f"  Accuracy: {accuracy_score(y_test, y_pred):.4f}")
print(f"  F1 Score: {f1_score(y_test, y_pred):.4f}")
print(f"  AUC-ROC:  {roc_auc_score(y_test, y_proba):.4f}")

# Feature importance
importance = pd.DataFrame({
    "feature": feature_columns,
    "importance": model.feature_importances_
}).sort_values("importance", ascending=False)

print(f"\nFeature Importance:")
for _, row in importance.iterrows():
    bar = "#" * int(row["importance"] * 50)
    print(f"  {row['feature']:<30} {row['importance']:.4f} {bar}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Simulating Online vs Offline Serving

# COMMAND ----------

# Offline serving: batch prediction using the full feature table
print("=== Offline Serving (Batch Inference) ===")
print("Use the latest feature snapshot for all customers.")

latest_features = feature_table[
    feature_table["feature_timestamp"] == feature_table["feature_timestamp"].max()
]
print(f"Latest snapshot date: {latest_features['feature_timestamp'].iloc[0].date()}")
print(f"Customers in snapshot: {len(latest_features)}")

X_batch = latest_features[feature_columns].fillna(0)
batch_predictions = model.predict_proba(X_batch)[:, 1]
latest_features = latest_features.copy()
latest_features["churn_probability"] = batch_predictions

high_risk = latest_features[latest_features["churn_probability"] > 0.6]
print(f"High-risk customers (>60% churn prob): {len(high_risk)}")

# COMMAND ----------

# Online serving: single customer lookup
print("=== Online Serving (Real-Time Inference) ===")
print("Look up a single customer's features and predict.\n")

# Simulate an online store lookup
def online_predict(customer_id, feature_store_latest, model, feature_columns):
    """Simulate an online feature lookup and prediction."""
    customer_features = feature_store_latest[
        feature_store_latest["customer_id"] == customer_id
    ]
    if len(customer_features) == 0:
        return {"error": f"Customer {customer_id} not found in online store"}

    features = customer_features[feature_columns].fillna(0)
    probability = model.predict_proba(features)[:, 1][0]

    return {
        "customer_id": customer_id,
        "churn_probability": round(probability, 4),
        "risk_level": "HIGH" if probability > 0.6 else "MEDIUM" if probability > 0.3 else "LOW",
        "features_used": {col: round(features[col].values[0], 2) for col in feature_columns[:5]},
    }


# Predict for a few customers
for cid in ["C0001", "C0050", "C0100", "C0150"]:
    result = online_predict(cid, latest_features, model, feature_columns)
    print(f"  {result['customer_id']}: prob={result.get('churn_probability', 'N/A')}, "
          f"risk={result.get('risk_level', 'N/A')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Feature Table Metadata

# COMMAND ----------

# In a Feature Store, metadata is essential for discovery and governance
feature_metadata = {
    "table_name": "catalog.schema.customer_features",
    "primary_keys": ["customer_id"],
    "timestamp_keys": ["feature_timestamp"],
    "description": "Customer behavioral features derived from transaction data",
    "features": {
        "total_spend": "Sum of all transaction amounts",
        "avg_transaction_amount": "Mean transaction amount",
        "transaction_count": "Number of transactions",
        "max_single_transaction": "Largest single transaction",
        "min_single_transaction": "Smallest single transaction",
        "std_transaction_amount": "Standard deviation of transaction amounts",
        "num_categories": "Number of distinct product categories purchased",
        "days_since_last_transaction": "Days between last transaction and snapshot date",
        "spend_volatility": "Coefficient of variation (std/mean) of transaction amounts",
    },
    "update_frequency": "Monthly (end of each quarter)",
    "owner": "data-science-team",
    "consumers": ["churn-prediction-model", "marketing-segmentation"],
}

print("Feature Table Metadata:")
print("=" * 60)
print(f"  Table: {feature_metadata['table_name']}")
print(f"  Primary Keys: {feature_metadata['primary_keys']}")
print(f"  Timestamp Keys: {feature_metadata['timestamp_keys']}")
print(f"  Update Frequency: {feature_metadata['update_frequency']}")
print(f"  Owner: {feature_metadata['owner']}")
print(f"\nFeatures ({len(feature_metadata['features'])}):")
for feat, desc in feature_metadata["features"].items():
    print(f"  - {feat}: {desc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Databricks Feature Engineering API (Reference)
# MAGIC
# MAGIC On a full Databricks workspace, the Feature Engineering in Unity Catalog
# MAGIC client provides managed feature tables, automatic lineage, and
# MAGIC integrated serving.

# COMMAND ----------

# --- Databricks Feature Engineering API Reference ---
#
# from databricks.feature_engineering import FeatureEngineeringClient, FeatureLookup
#
# fe = FeatureEngineeringClient()
#
# # 1. Create a feature table
# fe.create_table(
#     name="catalog.schema.customer_features",
#     primary_keys=["customer_id"],
#     timestamp_keys=["feature_timestamp"],
#     df=spark.createDataFrame(feature_table),  # Spark DataFrame
#     description="Customer behavioral features for churn prediction"
# )
#
# # 2. Update (write) features to the table
# fe.write_table(
#     name="catalog.schema.customer_features",
#     df=new_features_df,
#     mode="merge"  # or "overwrite"
# )
#
# # 3. Create a training set with feature lookups
# training_set = fe.create_training_set(
#     df=labels_spark_df,
#     feature_lookups=[
#         FeatureLookup(
#             table_name="catalog.schema.customer_features",
#             lookup_key="customer_id",
#             timestamp_lookup_key="label_date",  # enables point-in-time
#             feature_names=["total_spend", "transaction_count", "spend_volatility"]
#         ),
#     ],
#     label="churned",
#     exclude_columns=["label_date"]
# )
# training_df = training_set.load_df()
#
# # 4. Train and log model WITH feature metadata
# import mlflow
# with mlflow.start_run():
#     model = RandomForestClassifier(n_estimators=200)
#     model.fit(training_df.drop("churned").toPandas(), training_df.select("churned").toPandas())
#
#     # This logs the model AND the feature lookup spec
#     fe.log_model(
#         model=model,
#         artifact_path="model",
#         flavor=mlflow.sklearn,
#         training_set=training_set,
#         registered_model_name="catalog.schema.churn_predictor"
#     )
#
# # 5. Batch inference (automatically looks up features)
# predictions = fe.score_batch(
#     model_uri="models:/catalog.schema.churn_predictor@Champion",
#     df=new_customers_df  # only needs primary keys
# )
#
# # 6. Publish to online store (for real-time serving)
# from databricks.feature_engineering.online_store_spec import AmazonDynamoDBSpec
#
# online_spec = AmazonDynamoDBSpec(
#     region="us-east-1",
#     table_name="customer_features_online"
# )
# fe.publish_table(
#     name="catalog.schema.customer_features",
#     online_store_spec=online_spec
# )

print("Databricks Feature Engineering API patterns shown as comments above.")
print("On a full workspace, uncomment and configure table names as needed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Feature Store Best Practices Summary

# COMMAND ----------

print("""
Feature Store Best Practices
=============================

1. NAMING: Use descriptive, namespaced table names
   GOOD:  catalog.ml_features.customer_behavioral_features
   BAD:   features_v2_final_FINAL

2. PRIMARY KEYS: Always define them. They enable lookups.
   - Single entity: ["customer_id"]
   - Composite entity: ["customer_id", "product_id"]

3. TIMESTAMP KEYS: Add them for time-series features.
   - Enables point-in-time lookups (prevents temporal leakage)
   - Required for features that change over time

4. FEATURE GRANULARITY: One feature table per entity.
   - customer_features (keyed by customer_id)
   - product_features (keyed by product_id)
   - NOT: combined_features (keyed by customer_id + product_id)

5. UPDATE FREQUENCY: Match your business needs.
   - Real-time: streaming feature computation
   - Hourly: high-frequency behavioral features
   - Daily/Weekly: aggregate features (most common)

6. DOCUMENTATION: Describe every feature.
   - What does it measure?
   - How is it computed?
   - What are its valid ranges?

7. MONITORING: Track feature drift and freshness.
   - Alert if features stop updating
   - Monitor distribution shifts
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC This notebook used only in-memory pandas DataFrames.
# MAGIC No tables, temp views, or files were created.

# COMMAND ----------

print("Notebook 03-feature-store complete.")
