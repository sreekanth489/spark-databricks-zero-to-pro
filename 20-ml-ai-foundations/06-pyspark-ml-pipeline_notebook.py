# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 06 - PySpark ML Pipeline
# MAGIC > Module 20 -- Topic 06 | Build distributed ML pipelines with feature engineering, training, and evaluation
# MAGIC
# MAGIC **What you will do in this notebook:**
# MAGIC 1. Generate a synthetic e-commerce dataset (predict product returns)
# MAGIC 2. Build feature engineering stages: StringIndexer, OneHotEncoder, VectorAssembler
# MAGIC 3. Train multiple classifiers: RandomForest, GBT, LogisticRegression
# MAGIC 4. Evaluate models with BinaryClassificationEvaluator
# MAGIC 5. Perform cross-validation with ParamGridBuilder
# MAGIC 6. Save and load the fitted pipeline
# MAGIC 7. Integrate the pipeline with MLflow tracking

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 1: Setup and Sample Data Generation

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DoubleType, BooleanType
)
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler,
    StandardScaler, Imputer
)
from pyspark.ml.classification import (
    RandomForestClassifier, GBTClassifier, LogisticRegression
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator, MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import mlflow
import mlflow.spark
import numpy as np
import random

random.seed(42)
np.random.seed(42)

print("All imports successful.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Synthetic E-Commerce Returns Dataset
# MAGIC Predict whether a customer will return a product based on order
# MAGIC characteristics. This is a common classification problem in retail.

# COMMAND ----------

num_orders = 5000

categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books", "Toys"]
payment_methods = ["credit_card", "debit_card", "paypal", "gift_card"]
shipping_methods = ["standard", "express", "overnight"]
customer_segments = ["new", "regular", "vip"]

orders_data = []
for i in range(num_orders):
    category = random.choice(categories)
    payment = random.choice(payment_methods)
    shipping = random.choice(shipping_methods)
    segment = random.choice(customer_segments)

    price = round(random.uniform(10, 500), 2)
    quantity = random.randint(1, 5)
    discount_pct = round(random.uniform(0, 30), 1)
    customer_age = random.randint(18, 75)
    days_since_last_order = random.randint(1, 365)
    num_past_returns = random.randint(0, 10)
    avg_review_rating = round(random.uniform(1.0, 5.0), 1)

    # Simulate return probability based on features
    return_prob = 0.15  # base rate
    if category == "Clothing":
        return_prob += 0.12
    if category == "Electronics":
        return_prob += 0.05
    if discount_pct > 20:
        return_prob += 0.08
    if num_past_returns > 5:
        return_prob += 0.15
    if avg_review_rating < 3.0:
        return_prob += 0.10
    if shipping == "overnight":
        return_prob -= 0.05

    # Introduce some nulls for realism
    if random.random() < 0.03:
        avg_review_rating = None
    if random.random() < 0.02:
        days_since_last_order = None

    is_returned = 1 if random.random() < min(return_prob, 0.90) else 0

    orders_data.append((
        i + 1, category, payment, shipping, segment,
        price, quantity, discount_pct, customer_age,
        days_since_last_order, num_past_returns,
        avg_review_rating, is_returned
    ))

schema = StructType([
    StructField("order_id", IntegerType(), False),
    StructField("category", StringType(), False),
    StructField("payment_method", StringType(), False),
    StructField("shipping_method", StringType(), False),
    StructField("customer_segment", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("discount_pct", DoubleType(), False),
    StructField("customer_age", IntegerType(), False),
    StructField("days_since_last_order", IntegerType(), True),
    StructField("num_past_returns", IntegerType(), False),
    StructField("avg_review_rating", DoubleType(), True),
    StructField("is_returned", IntegerType(), False),
])

orders_df = spark.createDataFrame(orders_data, schema=schema)
print(f"Orders dataset: {orders_df.count()} rows")
print(f"Return rate: {orders_df.agg(F.mean('is_returned')).collect()[0][0]:.2%}")
orders_df.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 2: Exploratory Data Analysis

# COMMAND ----------

# Return rate by category
print("Return Rate by Category:")
print("-" * 40)
orders_df.groupBy("category").agg(
    F.mean("is_returned").alias("return_rate"),
    F.count("*").alias("count")
).orderBy("return_rate", ascending=False).show()

# Return rate by shipping method
print("Return Rate by Shipping Method:")
orders_df.groupBy("shipping_method").agg(
    F.mean("is_returned").alias("return_rate"),
    F.count("*").alias("count")
).orderBy("return_rate", ascending=False).show()

# Null counts
print("Null Counts:")
for col_name in orders_df.columns:
    null_count = orders_df.filter(F.col(col_name).isNull()).count()
    if null_count > 0:
        print(f"  {col_name}: {null_count} ({null_count/orders_df.count()*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 3: Train-Test Split

# COMMAND ----------

# Cast label to double (required by MLlib)
orders_df = orders_df.withColumn("label", F.col("is_returned").cast("double"))

# Split into train and test
train_df, test_df = orders_df.randomSplit([0.75, 0.25], seed=42)
print(f"Training set: {train_df.count()} rows")
print(f"Test set:     {test_df.count()} rows")
print(f"Train return rate: {train_df.agg(F.mean('label')).collect()[0][0]:.2%}")
print(f"Test return rate:  {test_df.agg(F.mean('label')).collect()[0][0]:.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 4: Build the Feature Engineering Pipeline
# MAGIC
# MAGIC MLlib requires all features in a single Vector column. Categorical
# MAGIC features must be indexed (StringIndexer) and optionally one-hot encoded
# MAGIC (OneHotEncoder) before assembly.

# COMMAND ----------

# Stage 1: Handle nulls with Imputer
# Imputer requires double type columns
imputer = Imputer(
    inputCols=["avg_review_rating", "days_since_last_order"],
    outputCols=["avg_review_rating_imputed", "days_since_last_order_imputed"],
    strategy="median"
)

# Stage 2: Index categorical columns (string -> numeric)
categorical_cols = ["category", "payment_method", "shipping_method", "customer_segment"]
indexers = [
    StringIndexer(
        inputCol=col, outputCol=f"{col}_idx",
        handleInvalid="keep"  # handle unseen categories at prediction time
    )
    for col in categorical_cols
]

# Stage 3: One-hot encode indexed columns
encoders = [
    OneHotEncoder(
        inputCol=f"{col}_idx", outputCol=f"{col}_vec"
    )
    for col in categorical_cols
]

# Stage 4: Assemble all features into a single vector
numeric_cols = [
    "price", "quantity", "discount_pct", "customer_age",
    "days_since_last_order_imputed", "num_past_returns", "avg_review_rating_imputed"
]
vector_cols = [f"{col}_vec" for col in categorical_cols]
all_feature_cols = numeric_cols + vector_cols

assembler = VectorAssembler(
    inputCols=all_feature_cols,
    outputCol="features_raw",
    handleInvalid="keep"
)

# Stage 5: Scale features
scaler = StandardScaler(
    inputCol="features_raw",
    outputCol="features",
    withStd=True,
    withMean=False  # False for sparse vectors
)

# Combine all preprocessing stages
preprocessing_stages = [imputer] + indexers + encoders + [assembler, scaler]

print(f"Preprocessing pipeline: {len(preprocessing_stages)} stages")
for i, stage in enumerate(preprocessing_stages):
    print(f"  Stage {i+1}: {type(stage).__name__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 5: Train a RandomForest Classifier

# COMMAND ----------

# Add the classifier as the final stage
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    numTrees=100,
    maxDepth=10,
    seed=42
)

# Build the complete pipeline
rf_pipeline = Pipeline(stages=preprocessing_stages + [rf])

# Fit the pipeline (trains everything)
print("Training RandomForest pipeline...")
rf_model = rf_pipeline.fit(train_df)
print("Training complete.")

# Make predictions
rf_predictions = rf_model.transform(test_df)

# Evaluate
binary_evaluator = BinaryClassificationEvaluator(
    labelCol="label", metricName="areaUnderROC"
)
multi_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", metricName="accuracy"
)
f1_evaluator = MulticlassClassificationEvaluator(
    labelCol="label", metricName="f1"
)

rf_auc = binary_evaluator.evaluate(rf_predictions)
rf_accuracy = multi_evaluator.evaluate(rf_predictions)
rf_f1 = f1_evaluator.evaluate(rf_predictions)

print(f"\nRandomForest Results:")
print(f"  AUC-ROC:  {rf_auc:.4f}")
print(f"  Accuracy: {rf_accuracy:.4f}")
print(f"  F1 Score: {rf_f1:.4f}")

# COMMAND ----------

# Show predictions
print("Sample Predictions:")
rf_predictions.select(
    "order_id", "category", "price", "num_past_returns",
    "label", "prediction", "probability"
).show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 6: Train a GBT Classifier

# COMMAND ----------

gbt = GBTClassifier(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    maxDepth=5,
    stepSize=0.1,
    seed=42
)

gbt_pipeline = Pipeline(stages=preprocessing_stages + [gbt])

print("Training GBT pipeline...")
gbt_model = gbt_pipeline.fit(train_df)

gbt_predictions = gbt_model.transform(test_df)
gbt_auc = binary_evaluator.evaluate(gbt_predictions)
gbt_accuracy = multi_evaluator.evaluate(gbt_predictions)
gbt_f1 = f1_evaluator.evaluate(gbt_predictions)

print(f"GBT Results:")
print(f"  AUC-ROC:  {gbt_auc:.4f}")
print(f"  Accuracy: {gbt_accuracy:.4f}")
print(f"  F1 Score: {gbt_f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 7: Train a LogisticRegression Classifier

# COMMAND ----------

lr = LogisticRegression(
    featuresCol="features",
    labelCol="label",
    maxIter=100,
    regParam=0.01,
    elasticNetParam=0.5
)

lr_pipeline = Pipeline(stages=preprocessing_stages + [lr])

print("Training LogisticRegression pipeline...")
lr_model = lr_pipeline.fit(train_df)

lr_predictions = lr_model.transform(test_df)
lr_auc = binary_evaluator.evaluate(lr_predictions)
lr_accuracy = multi_evaluator.evaluate(lr_predictions)
lr_f1 = f1_evaluator.evaluate(lr_predictions)

print(f"LogisticRegression Results:")
print(f"  AUC-ROC:  {lr_auc:.4f}")
print(f"  Accuracy: {lr_accuracy:.4f}")
print(f"  F1 Score: {lr_f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 8: Model Comparison

# COMMAND ----------

# Compare all three models
print("Model Comparison:")
print("=" * 60)
print(f"{'Model':<25} {'AUC-ROC':>10} {'Accuracy':>10} {'F1':>10}")
print("-" * 60)
print(f"{'RandomForest':<25} {rf_auc:>10.4f} {rf_accuracy:>10.4f} {rf_f1:>10.4f}")
print(f"{'GBT':<25} {gbt_auc:>10.4f} {gbt_accuracy:>10.4f} {gbt_f1:>10.4f}")
print(f"{'LogisticRegression':<25} {lr_auc:>10.4f} {lr_accuracy:>10.4f} {lr_f1:>10.4f}")
print("=" * 60)

best_auc = max(rf_auc, gbt_auc, lr_auc)
best_model_name = (
    "RandomForest" if rf_auc == best_auc
    else "GBT" if gbt_auc == best_auc
    else "LogisticRegression"
)
print(f"\nBest model by AUC-ROC: {best_model_name} ({best_auc:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 9: Feature Importance (RandomForest)

# COMMAND ----------

# Extract feature importances from the RandomForest model
# The last stage of the PipelineModel is the classifier
rf_classifier_model = rf_model.stages[-1]

if hasattr(rf_classifier_model, "featureImportances"):
    importances = rf_classifier_model.featureImportances.toArray()

    # Map indices back to feature names
    # This requires understanding the VectorAssembler output ordering
    feature_importance_list = list(zip(all_feature_cols, range(len(all_feature_cols))))
    importance_df = []

    # For numeric features, it is a 1:1 mapping
    idx = 0
    for col in numeric_cols:
        if idx < len(importances):
            importance_df.append((col, float(importances[idx])))
        idx += 1

    # For one-hot encoded features, sum the importances of all encoded columns
    for col in categorical_cols:
        # Each encoded column contributes multiple indices
        # Get the number of categories from the model
        n_categories = len([s for s in rf_model.stages if hasattr(s, "labels") and s.getInputCol() == col])
        # Use a simpler approach: report the aggregate
        category_imp = sum(importances[idx:idx+5]) if idx + 5 <= len(importances) else 0
        importance_df.append((f"{col} (encoded)", category_imp))
        idx += 5  # approximate

    # Sort and display
    importance_df.sort(key=lambda x: x[1], reverse=True)
    print("Feature Importance (RandomForest):")
    print("-" * 55)
    for name, imp in importance_df:
        bar = "#" * int(imp * 80)
        print(f"  {name:<35} {imp:.4f} {bar}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 10: Cross-Validation with Hyperparameter Tuning

# COMMAND ----------

# Define a simpler pipeline for cross-validation (to keep runtime reasonable)
rf_cv = RandomForestClassifier(
    featuresCol="features",
    labelCol="label",
    seed=42
)
cv_pipeline = Pipeline(stages=preprocessing_stages + [rf_cv])

# Define the parameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(rf_cv.numTrees, [50, 100, 200])
    .addGrid(rf_cv.maxDepth, [5, 10])
    .build()
)

print(f"Parameter grid: {len(paramGrid)} combinations")
for i, params in enumerate(paramGrid):
    param_str = ", ".join(f"{k.name}={v}" for k, v in params.items())
    print(f"  Combo {i+1}: {param_str}")

# COMMAND ----------

# Run cross-validation
crossval = CrossValidator(
    estimator=cv_pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=binary_evaluator,
    numFolds=3,
    parallelism=2,
    seed=42
)

print("Running 3-fold cross-validation...")
print(f"Total fits: {len(paramGrid)} combos x 3 folds = {len(paramGrid) * 3}")
cv_model = crossval.fit(train_df)
print("Cross-validation complete.")

# Display results
avg_metrics = cv_model.avgMetrics
print(f"\nCross-Validation Results (AUC-ROC):")
print("-" * 50)
for i, (params, metric) in enumerate(zip(paramGrid, avg_metrics)):
    param_str = ", ".join(f"{k.name}={v}" for k, v in params.items())
    marker = " <-- BEST" if metric == max(avg_metrics) else ""
    print(f"  {param_str}: {metric:.4f}{marker}")

# Evaluate the best model on the test set
best_predictions = cv_model.transform(test_df)
best_auc_cv = binary_evaluator.evaluate(best_predictions)
best_accuracy_cv = multi_evaluator.evaluate(best_predictions)
print(f"\nBest CV Model on Test Set:")
print(f"  AUC-ROC:  {best_auc_cv:.4f}")
print(f"  Accuracy: {best_accuracy_cv:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 11: Save and Load the Pipeline

# COMMAND ----------

# Save the best pipeline model
PIPELINE_PATH = "/tmp/module20_ecommerce_pipeline"

try:
    cv_model.bestModel.write().overwrite().save(PIPELINE_PATH)
    print(f"Pipeline saved to: {PIPELINE_PATH}")

    # Load it back
    loaded_pipeline = PipelineModel.load(PIPELINE_PATH)
    print(f"Pipeline loaded successfully. Stages: {len(loaded_pipeline.stages)}")

    # Verify it produces the same predictions
    loaded_predictions = loaded_pipeline.transform(test_df)
    loaded_auc = binary_evaluator.evaluate(loaded_predictions)
    print(f"Loaded pipeline AUC-ROC: {loaded_auc:.4f} (original: {best_auc_cv:.4f})")
    assert abs(loaded_auc - best_auc_cv) < 0.001, "Loaded model produces different results!"
    print("VERIFIED: Loaded pipeline produces identical results.")

except Exception as e:
    print(f"Pipeline save/load: {e}")
    print("(File-based operations may not be available in all environments)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 12: Integrate with MLflow

# COMMAND ----------

EXPERIMENT_NAME = "/tmp/module20-pyspark-ml-pipeline"
try:
    mlflow.set_experiment(EXPERIMENT_NAME)
except Exception:
    pass

# Log the best pipeline model to MLflow
with mlflow.start_run(run_name="best_rf_pipeline") as run:
    # Log parameters from the best model
    best_rf_model = cv_model.bestModel.stages[-1]
    mlflow.log_param("algorithm", "RandomForestClassifier")
    mlflow.log_param("numTrees", best_rf_model.getNumTrees)
    mlflow.log_param("maxDepth", best_rf_model.getOrDefault("maxDepth"))
    mlflow.log_param("preprocessing_stages", len(preprocessing_stages))
    mlflow.log_param("num_cv_folds", 3)

    # Log metrics
    mlflow.log_metric("test_auc_roc", best_auc_cv)
    mlflow.log_metric("test_accuracy", best_accuracy_cv)

    # Log the Spark ML pipeline model
    try:
        mlflow.spark.log_model(cv_model.bestModel, "spark_pipeline")
        print("Spark ML pipeline logged to MLflow.")
    except Exception as e:
        print(f"mlflow.spark.log_model: {e}")
        print("(This requires a Spark-enabled MLflow environment)")

    mlflow.set_tag("pipeline_type", "full_ml_pipeline")
    mlflow.set_tag("domain", "ecommerce_returns")

    print(f"Run ID: {run.info.run_id}")
    print(f"Logged AUC-ROC: {best_auc_cv:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 13: Prediction Analysis

# COMMAND ----------

# Analyze prediction distribution
pred_summary = best_predictions.groupBy("prediction").agg(
    F.count("*").alias("count"),
    F.avg("label").alias("actual_positive_rate")
)
print("Prediction Distribution:")
pred_summary.show()

# Confusion matrix
tp = best_predictions.filter((F.col("prediction") == 1) & (F.col("label") == 1)).count()
fp = best_predictions.filter((F.col("prediction") == 1) & (F.col("label") == 0)).count()
tn = best_predictions.filter((F.col("prediction") == 0) & (F.col("label") == 0)).count()
fn = best_predictions.filter((F.col("prediction") == 0) & (F.col("label") == 1)).count()

print("Confusion Matrix:")
print(f"  Predicted:    Not Returned  Returned")
print(f"  Not Returned: {tn:>10}    {fp:>8}")
print(f"  Returned:     {fn:>10}    {tp:>8}")
print(f"\n  Precision: {tp/(tp+fp) if (tp+fp) > 0 else 0:.4f}")
print(f"  Recall:    {tp/(tp+fn) if (tp+fn) > 0 else 0:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Section 14: Pipeline Architecture Summary

# COMMAND ----------

print("ML Pipeline Architecture:")
print("=" * 65)
for i, stage in enumerate(cv_model.bestModel.stages):
    stage_name = type(stage).__name__
    if hasattr(stage, "getInputCol"):
        detail = f"({stage.getInputCol()} -> {stage.getOutputCol()})"
    elif hasattr(stage, "getInputCols"):
        n_inputs = len(stage.getInputCols())
        detail = f"({n_inputs} inputs -> {stage.getOutputCol()})"
    elif hasattr(stage, "getNumTrees"):
        detail = f"(trees={stage.getNumTrees}, depth={stage.getOrDefault('maxDepth')})"
    elif hasattr(stage, "getMaxIter"):
        detail = f"(maxIter={stage.getOrDefault('maxIter')})"
    else:
        detail = ""
    print(f"  Stage {i+1:>2}: {stage_name:<30} {detail}")
print("=" * 65)

print(f"""
Pipeline Summary:
  - Preprocessing stages: {len(preprocessing_stages)}
  - Classifier: RandomForestClassifier
  - Cross-validation: 3 folds, {len(paramGrid)} param combos
  - Best test AUC-ROC: {best_auc_cv:.4f}
  - Best test Accuracy: {best_accuracy_cv:.4f}
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup

# COMMAND ----------

# Clean up saved pipeline
import shutil
try:
    shutil.rmtree(PIPELINE_PATH, ignore_errors=True)
    print(f"Cleaned up: {PIPELINE_PATH}")
except Exception:
    pass

# Remove temp views if any were created
try:
    spark.catalog.dropTempView("orders")
except Exception:
    pass

print("Notebook 06-pyspark-ml-pipeline complete.")
