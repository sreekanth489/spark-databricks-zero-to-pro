# Feature Store
> Module 20 -- Topic 03 | Level: Intermediate | Time: 55 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain what a Feature Store is and why it matters for ML systems
2. Understand the training-serving skew problem and how Feature Store solves it
3. Create feature tables with primary keys and timestamp keys
4. Perform point-in-time lookups for time-series features
5. Distinguish between online and offline feature stores
6. Use Feature Engineering in Unity Catalog (the modern Databricks approach)
7. Build a training dataset using feature lookups

---

## Conceptual Overview

### What Is a Feature Store?

A feature is a measurable property used as input to a machine learning model. A
Feature Store is a centralized repository that manages the creation, storage,
discovery, and serving of these features across the organization.

Without a Feature Store, teams duplicate feature engineering logic. The same
"customer_lifetime_value" might be computed differently in the training notebook
and the serving pipeline, leading to **training-serving skew** -- the most insidious
bug in production ML.

```
  The Training-Serving Skew Problem
  ===================================

  WITHOUT Feature Store:

  Training Notebook              Serving Pipeline
  +--------------------+        +--------------------+
  | def calc_ltv(df):  |        | def get_ltv(cust): |
  |   # uses 12 months |        |   # uses 6 months  |  <-- DIFFERENT LOGIC!
  |   return df.sum()  |        |   return sum(...)   |
  +--------------------+        +--------------------+
        |                              |
        v                              v
  Model trains on                Model predicts on
  12-month LTV                   6-month LTV
                                 --> WRONG PREDICTIONS

  WITH Feature Store:

  Feature Store (single source of truth)
  +------------------------------------------+
  | Feature Table: customer_features          |
  | - customer_id (PK)                        |
  | - lifetime_value (computed once, same way)|
  | - avg_order_value                         |
  | - tenure_months                           |
  +------------------------------------------+
        |                    |
        v                    v
  Training reads         Serving reads
  SAME features          SAME features
  --> CONSISTENT         --> CONSISTENT
```

### Core Components

```
  Feature Store Architecture
  ============================

  Feature Engineering        Feature Tables          Feature Serving
  +------------------+      +------------------+     +------------------+
  | PySpark / SQL    |      | Delta Tables     |     | Online Store     |
  | Batch or Stream  | ---> | (Offline Store)  | --> | (Low-latency)    |
  | Transformations  |      | In Lakehouse     |     | DynamoDB / CosmosDB
  +------------------+      +------------------+     +------------------+
                                    |
                                    v
                            +------------------+
                            | Training Dataset |
                            | Feature Lookups  |
                            | Point-in-time    |
                            +------------------+
```

**Feature Table**: A Delta table with metadata (primary keys, timestamp keys,
description) registered in the Feature Store. It is both a regular table you
can query with SQL and a managed feature source for ML.

**Offline Store**: The lakehouse itself (Delta tables in Unity Catalog). Used for
batch feature lookups during training and batch inference. Supports full history.

**Online Store**: A low-latency key-value store (DynamoDB on AWS, CosmosDB on Azure,
Cloud Bigtable on GCP) synchronized from the offline store. Used for real-time
serving when single-digit millisecond latency is required.

**Point-in-Time Lookup**: When features change over time, you need the feature value
as it existed at a specific timestamp -- not the current value. This prevents
**temporal leakage** (using future information to predict the past).

### Point-in-Time Lookups

```
  Point-in-Time Feature Lookup
  ===============================

  Feature Table (customer_features):
  +-------------+---------------+------------+
  | customer_id | monthly_spend | updated_at |
  +-------------+---------------+------------+
  | C001        | 500           | 2024-01-15 |
  | C001        | 600           | 2024-02-15 |  <-- feature updated
  | C001        | 550           | 2024-03-15 |  <-- feature updated
  +-------------+---------------+------------+

  Training Label (predict churn at a specific date):
  +-------------+------------+---------+
  | customer_id | label_date | churned |
  +-------------+------------+---------+
  | C001        | 2024-02-20 | 0       |
  +-------------+------------+---------+

  Point-in-time lookup for C001 at 2024-02-20:
  --> Uses monthly_spend = 600 (from 2024-02-15)
  --> NOT 550 (from 2024-03-15, which is in the future)

  This prevents temporal leakage in training data.
```

### Feature Engineering in Unity Catalog

The modern Databricks approach uses Unity Catalog for feature governance:

```python
from databricks.feature_engineering import FeatureEngineeringClient

fe = FeatureEngineeringClient()

# Create a feature table
fe.create_table(
    name="catalog.schema.customer_features",
    primary_keys=["customer_id"],
    timestamp_keys=["updated_at"],
    df=feature_df,
    description="Customer behavioral features for churn prediction"
)

# Create a training set with feature lookups
from databricks.feature_engineering import FeatureLookup

training_set = fe.create_training_set(
    df=labels_df,
    feature_lookups=[
        FeatureLookup(
            table_name="catalog.schema.customer_features",
            lookup_key="customer_id",
            timestamp_lookup_key="label_date",
            feature_names=["monthly_spend", "avg_order_value"]
        )
    ],
    label="churned"
)
training_df = training_set.load_df()
```

### Online vs Offline Feature Serving

| Aspect | Offline Store | Online Store |
|--------|--------------|-------------|
| Storage | Delta Lake (lakehouse) | DynamoDB / CosmosDB / Bigtable |
| Latency | Seconds to minutes | Single-digit milliseconds |
| Use case | Training, batch inference | Real-time serving endpoints |
| History | Full version history | Latest value only |
| Cost | Storage cost only | Storage + provisioned throughput |
| Sync | Source of truth | Synced from offline store |

### Feature Discovery and Reuse

The Feature Store UI in Databricks lets you:
- Browse all feature tables across the organization
- Search features by name, description, or tag
- View feature lineage (what pipeline produces this feature)
- See which models consume each feature
- Track feature freshness and quality metrics

---

## Hands-On Walkthrough

Open `03-feature-store_notebook.py` to practice:
- Creating feature tables from generated customer data
- Computing features with PySpark transformations
- Building a training dataset with feature lookups
- Simulating point-in-time correctness
- Understanding online vs offline serving patterns
- Databricks Feature Engineering API reference

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Offline store | Delta Lake on S3 | Delta Lake on ADLS Gen2 | Delta Lake on GCS |
| Online store | DynamoDB | CosmosDB | Cloud Bigtable |
| Feature Engineering client | `databricks-feature-engineering` | `databricks-feature-engineering` | `databricks-feature-engineering` |
| Unity Catalog integration | Supported | Supported | Supported |
| Online store publish | `fe.publish_table()` | `fe.publish_table()` | `fe.publish_table()` |

---

## Certification Tip

> **Databricks ML Professional**: Understand why Feature Store prevents training-
> serving skew. Know the difference between offline and online stores. Expect
> questions about point-in-time lookups and when to use timestamp keys.
>
> **Key concept**: Feature tables are Delta tables with extra metadata (primary keys,
> timestamp keys). They are queryable with SQL like any other table, but the
> Feature Engineering client provides ML-specific operations like feature lookups
> and point-in-time joins.

---

## Key Takeaways

1. **Feature Store eliminates training-serving skew** by ensuring the same feature
   computation is used everywhere -- training, batch inference, and real-time serving.
2. **Feature tables are Delta tables** with metadata (primary keys, timestamp keys).
   They live in your lakehouse and are governed by Unity Catalog.
3. **Point-in-time lookups** prevent temporal leakage by fetching feature values as
   they existed at a specific timestamp, not the current value.
4. **Offline stores** (Delta Lake) support batch operations. **Online stores**
   (DynamoDB, CosmosDB) provide low-latency lookups for real-time serving.
5. **Feature discovery** across the organization is a key benefit -- teams can
   find and reuse features instead of recomputing them.
6. **Unity Catalog Feature Engineering** is the modern approach, replacing the
   legacy workspace-level Feature Store client.

---

## Next Steps

- Proceed to **Topic 04: AutoML** to learn how Databricks can automatically
  explore features and build baseline models.
- Create feature tables for your own datasets and practice feature lookups.
