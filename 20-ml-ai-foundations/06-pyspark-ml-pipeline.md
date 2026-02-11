# PySpark ML Pipeline
> Module 20 -- Topic 06 | Level: Intermediate | Time: 60 min

## Learning Objectives

By the end of this topic you will be able to:
1. Explain the Spark MLlib Pipeline API: Transformers, Estimators, Pipelines
2. Use feature engineering stages: VectorAssembler, StringIndexer, OneHotEncoder, StandardScaler
3. Train classification and regression models with Spark MLlib (RandomForest, GBT, LogisticRegression)
4. Evaluate models with BinaryClassificationEvaluator and MulticlassClassificationEvaluator
5. Perform cross-validation and hyperparameter tuning with CrossValidator and ParamGridBuilder
6. Save, load, and reuse ML pipelines for reproducible workflows
7. Compare MLlib (distributed) with sklearn (single-node) for different data scales

---

## Conceptual Overview

### Why PySpark MLlib?

sklearn works beautifully when your data fits in memory on a single machine. But
when your training data is tens of gigabytes or more, you need distributed training.
PySpark MLlib runs on the same Spark infrastructure as your data pipelines, so you
can go from data engineering to model training without moving data.

```
  When to Use Each Framework
  ============================

  Data Size      |  Framework          |  Why
  ---------------+---------------------+---------------------------
  < 10 GB        |  sklearn / XGBoost  |  Faster, more algorithms
  10-100 GB      |  PySpark MLlib      |  Distributed, same cluster
  > 100 GB       |  PySpark MLlib      |  Only option without sampling
  Any size       |  AutoML             |  Let Databricks choose
```

### Pipeline API: Transformers, Estimators, Pipeline

The MLlib Pipeline API follows a clean design pattern:

```
  MLlib Pipeline Architecture
  ============================

  +-------------------+     +-------------------+     +-------------------+
  | Transformer       |     | Estimator         |     | Pipeline          |
  | .transform(df)    |     | .fit(df) -> Model |     | Sequence of       |
  | Input -> Output   |     | (a Transformer)   |     | stages            |
  +-------------------+     +-------------------+     +-------------------+
  | VectorAssembler   |     | StringIndexer     |     | [StringIndexer,   |
  | SQLTransformer    |     | StandardScaler    |     |  OneHotEncoder,   |
  | Tokenizer         |     | RandomForest      |     |  VectorAssembler, |
  | StopWordsRemover  |     | LogisticRegression|     |  RandomForest]    |
  +-------------------+     +-------------------+     +-------------------+

  Key Insight:
  - An Estimator's .fit() method produces a Transformer (the fitted model)
  - A Pipeline's .fit() calls .fit() on each Estimator stage in sequence
  - The fitted PipelineModel's .transform() applies all stages in sequence
```

**Transformer**: Takes a DataFrame, returns a DataFrame with additional/modified
columns. It has a `.transform(df)` method. Example: `VectorAssembler` combines
multiple columns into a single vector column.

**Estimator**: Takes a DataFrame, learns something, and produces a Transformer.
It has a `.fit(df)` method that returns a Model (which is a Transformer). Example:
`StandardScaler.fit(df)` learns mean/std, returns a `StandardScalerModel` that
can `.transform()` new data.

**Pipeline**: A sequence of Transformers and Estimators. When you call
`pipeline.fit(df)`, it fits each Estimator in order, producing a `PipelineModel`.

### Feature Engineering Stages

| Stage | Purpose | Input | Output |
|-------|---------|-------|--------|
| `StringIndexer` | Convert strings to numeric indices | String column | Double column (indices) |
| `OneHotEncoder` | One-hot encode indexed columns | Index column | Sparse vector column |
| `VectorAssembler` | Combine features into a single vector | Multiple columns | Single vector column |
| `StandardScaler` | Normalize features (mean=0, std=1) | Vector column | Scaled vector column |
| `MinMaxScaler` | Scale features to [0, 1] range | Vector column | Scaled vector column |
| `Bucketizer` | Bin continuous values into categories | Double column | Double column (bins) |
| `Imputer` | Fill missing values (mean, median) | Double column | Imputed double column |
| `SQLTransformer` | Apply SQL expression | DataFrame | DataFrame |

### Complete Pipeline Example

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler
)
from pyspark.ml.classification import RandomForestClassifier

# Define stages
category_indexer = StringIndexer(inputCol="category", outputCol="category_idx")
category_encoder = OneHotEncoder(inputCol="category_idx", outputCol="category_vec")
assembler = VectorAssembler(
    inputCols=["age", "income", "category_vec"],
    outputCol="features"
)
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
classifier = RandomForestClassifier(
    featuresCol="scaled_features", labelCol="label"
)

# Build the pipeline
pipeline = Pipeline(stages=[
    category_indexer, category_encoder, assembler, scaler, classifier
])

# Fit (trains the entire pipeline)
model = pipeline.fit(train_df)

# Predict (applies all stages in sequence)
predictions = model.transform(test_df)
```

### Cross-Validation and Hyperparameter Tuning

```python
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Define the parameter grid
paramGrid = (
    ParamGridBuilder()
    .addGrid(classifier.numTrees, [100, 200, 300])
    .addGrid(classifier.maxDepth, [5, 10, 15])
    .build()
)

# Define the evaluator
evaluator = BinaryClassificationEvaluator(
    labelCol="label", metricName="areaUnderROC"
)

# Cross-validate
crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=paramGrid,
    evaluator=evaluator,
    numFolds=3,
    parallelism=4  # run 4 parameter combinations in parallel
)

cv_model = crossval.fit(train_df)
best_model = cv_model.bestModel
```

### Saving and Loading Pipelines

```python
# Save the fitted pipeline
model.save("/path/to/model")

# Load it later
from pyspark.ml import PipelineModel
loaded_model = PipelineModel.load("/path/to/model")
predictions = loaded_model.transform(new_data)
```

---

## Hands-On Walkthrough

Open `06-pyspark-ml-pipeline_notebook.py` to practice:
- Building a complete ML pipeline on generated e-commerce data
- Feature engineering: StringIndexer, OneHotEncoder, VectorAssembler
- Training RandomForest, GBT, and LogisticRegression classifiers
- Model evaluation with BinaryClassificationEvaluator
- Cross-validation with ParamGridBuilder
- Saving and loading pipelines
- Integrating MLlib pipelines with MLflow

---

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| MLlib availability | All runtimes | All runtimes | All runtimes |
| Distributed training | Multi-node clusters | Multi-node clusters | Multi-node clusters |
| GPU-accelerated MLlib | Spark Rapids (limited) | Spark Rapids (limited) | Spark Rapids (limited) |
| Pipeline persistence | DBFS / S3 | DBFS / ADLS Gen2 | DBFS / GCS |
| MLflow integration | Native | Native | Native |

---

## Certification Tip

> **Databricks ML Professional**: Understand the Pipeline API deeply:
> Transformer vs Estimator, how Pipeline.fit() works, and how PipelineModel
> .transform() applies all stages. Know how to use VectorAssembler (required
> for all MLlib algorithms), StringIndexer + OneHotEncoder for categoricals,
> and CrossValidator for hyperparameter tuning.
>
> **Key concept**: Every MLlib algorithm requires features in a single Vector
> column. VectorAssembler is almost always the last feature engineering stage
> before the algorithm. Numeric features go directly into VectorAssembler;
> categorical features must be indexed and encoded first.

---

## Key Takeaways

1. **PySpark MLlib** is for distributed ML training when data exceeds single-node
   memory. It uses the same Spark cluster as your data pipelines.
2. **Pipeline API** (Transformer, Estimator, Pipeline) ensures reproducible
   workflows. A fitted PipelineModel applies all preprocessing and prediction
   steps in sequence.
3. **VectorAssembler** is mandatory -- MLlib algorithms require all features in a
   single Vector column. String features must be indexed and encoded first.
4. **CrossValidator** with ParamGridBuilder automates hyperparameter tuning with
   k-fold cross-validation. Use `parallelism` to run trials concurrently.
5. **Save pipelines**, not just models. A saved PipelineModel includes all
   preprocessing stages, so new data goes through the exact same transformations.
6. **Integrate with MLflow**: Log MLlib pipelines with `mlflow.spark.log_model()`
   to get versioning, registry, and serving capabilities.

---

## Next Steps

- Revisit **Module 10: Real-World Projects** to apply ML pipelines to end-to-end
  scenarios combining data engineering and machine learning.
- Explore **Module 21: GenAI & LLM Use Cases** to see how ML foundations support
  large language model applications on Databricks.
