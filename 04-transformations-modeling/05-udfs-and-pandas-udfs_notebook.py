# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # 05 -- UDFs and Pandas UDFs
# MAGIC
# MAGIC **Module 04 | Topic 05 | Level: Intermediate | Time: 45 min**
# MAGIC
# MAGIC In this notebook you will:
# MAGIC - Write a Python UDF and understand its serialization overhead
# MAGIC - Register UDFs for SQL queries
# MAGIC - Write Pandas UDFs (vectorized) for better performance
# MAGIC - Compare performance: Python UDF vs Pandas UDF vs built-in
# MAGIC - Use Grouped Map and Grouped Aggregate Pandas UDFs
# MAGIC - Apply type annotations with the modern API

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 -- Setup: Create Product Reviews Data

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, udf, pandas_udf, upper, length, when, lit, avg, count,
    round as _round
)
from pyspark.sql.types import (
    StringType, IntegerType, DoubleType, StructType, StructField, ArrayType
)
import pandas as pd
import time

spark = SparkSession.builder.getOrCreate()

reviews_data = [
    (1, "P001", "Excellent laptop, blazing fast!", 5, 1200.00, "electronics"),
    (2, "P002", "Good phone but battery drains quickly", 3, 800.00, "electronics"),
    (3, "P003", "Great book on Spark, highly recommend", 5, 45.00, "books"),
    (4, "P004", "Tablet is OK, nothing special", 3, 450.00, "electronics"),
    (5, "P005", "Terrible quality jacket, fell apart", 1, 90.00, "clothing"),
    (6, "P006", "Decent Python book for beginners", 4, 35.00, "books"),
    (7, "P007", "Amazing monitor, crystal clear", 5, 350.00, "electronics"),
    (8, "P008", "Comfortable sneakers, good value", 4, 120.00, "clothing"),
    (9, "P009", "Average SQL book, could be better", 3, 40.00, "books"),
    (10, "P010", "Premium headphones, worth every penny", 5, 150.00, "electronics"),
    (11, "P011", "Cheap camera, photos are blurry", 2, 700.00, "electronics"),
    (12, "P012", "Soft t-shirt, love the material", 4, 25.00, "clothing"),
]

reviews_schema = StructType([
    StructField("review_id", IntegerType(), False),
    StructField("product_id", StringType(), False),
    StructField("review_text", StringType(), False),
    StructField("rating", IntegerType(), False),
    StructField("price", DoubleType(), False),
    StructField("category", StringType(), False),
])

reviews_df = spark.createDataFrame(data=reviews_data, schema=reviews_schema)
reviews_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 -- Python UDF: Sentiment Classification
# MAGIC
# MAGIC A regular Python UDF processes rows one at a time. Spark serializes each
# MAGIC row from JVM to Python using **Pickle**, executes the function, then
# MAGIC sends the result back. This is the slowest approach.

# COMMAND ----------

# Define a Python UDF that classifies sentiment based on keywords
def classify_sentiment(text):
    """Simple keyword-based sentiment classifier."""
    if text is None:
        return "neutral"
    text_lower = text.lower()
    positive = ["excellent", "great", "amazing", "love", "premium", "good", "comfortable"]
    negative = ["terrible", "cheap", "blurry", "drains", "average"]
    pos_count = sum(1 for word in positive if word in text_lower)
    neg_count = sum(1 for word in negative if word in text_lower)
    if pos_count > neg_count:
        return "positive"
    elif neg_count > pos_count:
        return "negative"
    return "neutral"

# Register as UDF with explicit return type
classify_sentiment_udf = udf(classify_sentiment, StringType())

# Apply the UDF
print("Python UDF -- sentiment classification:")
reviews_df.select(
    "review_id",
    "review_text",
    "rating",
    classify_sentiment_udf(col("review_text")).alias("sentiment"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 -- Register UDF for SQL Queries
# MAGIC
# MAGIC You can run SQL queries on top of a DataFrame using `createOrReplaceTempView`.
# MAGIC Register the UDF so it is callable from SQL.

# COMMAND ----------

# Register the Python UDF for SQL use
spark.udf.register("classify_sentiment_sql", classify_sentiment, StringType())

reviews_df.createOrReplaceTempView("reviews")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the registered UDF in a SQL query
# MAGIC SELECT
# MAGIC     review_id,
# MAGIC     review_text,
# MAGIC     rating,
# MAGIC     classify_sentiment_sql(review_text) AS sentiment
# MAGIC FROM reviews
# MAGIC ORDER BY review_id;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 -- Pandas UDF (Vectorized): Much Faster
# MAGIC
# MAGIC Pandas UDFs use **Apache Arrow** to transfer data in columnar batches
# MAGIC instead of row-by-row Pickle serialization. This is 10-100x faster.
# MAGIC
# MAGIC Arrow serialization:
# MAGIC - Transfers thousands of rows per batch
# MAGIC - Uses zero-copy columnar binary format
# MAGIC - Leverages NumPy/Pandas vectorized operations

# COMMAND ----------

# Scalar Pandas UDF using type annotations (modern API)
@pandas_udf(StringType())
def classify_sentiment_pandas(texts: pd.Series) -> pd.Series:
    """Vectorized sentiment classification using Pandas operations."""
    positive = ["excellent", "great", "amazing", "love", "premium", "good", "comfortable"]
    negative = ["terrible", "cheap", "blurry", "drains", "average"]

    def _classify(text):
        if text is None:
            return "neutral"
        text_lower = text.lower()
        pos_count = sum(1 for w in positive if w in text_lower)
        neg_count = sum(1 for w in negative if w in text_lower)
        if pos_count > neg_count:
            return "positive"
        elif neg_count > pos_count:
            return "negative"
        return "neutral"

    return texts.apply(_classify)

print("Pandas UDF -- sentiment classification:")
reviews_df.select(
    "review_id",
    "review_text",
    classify_sentiment_pandas(col("review_text")).alias("sentiment"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 -- Performance Comparison: Built-in vs Pandas UDF vs Python UDF
# MAGIC
# MAGIC Built-in functions always win. Let us compare a simple transformation
# MAGIC across the three approaches.

# COMMAND ----------

# Generate a larger dataset for timing
large_df = spark.range(0, 100000).withColumn(
    "value", (col("id") % 100).cast(DoubleType())
)
large_df.cache()
large_df.count()  # force cache materialization

# Approach 1: Built-in function (fastest, runs in JVM)
start = time.time()
result_builtin = large_df.withColumn("doubled", col("value") * 2)
result_builtin.collect()
builtin_time = time.time() - start

# Approach 2: Pandas UDF (Arrow serialization)
@pandas_udf(DoubleType())
def double_pandas(s: pd.Series) -> pd.Series:
    return s * 2

start = time.time()
result_pandas = large_df.withColumn("doubled", double_pandas(col("value")))
result_pandas.collect()
pandas_time = time.time() - start

# Approach 3: Python UDF (Pickle serialization -- slowest)
@udf(DoubleType())
def double_python(val):
    return val * 2 if val is not None else None

start = time.time()
result_python = large_df.withColumn("doubled", double_python(col("value")))
result_python.collect()
python_time = time.time() - start

print(f"Built-in function: {builtin_time:.3f}s")
print(f"Pandas UDF (Arrow): {pandas_time:.3f}s")
print(f"Python UDF (Pickle): {python_time:.3f}s")
print(f"\nPandas UDF is ~{python_time/max(pandas_time, 0.001):.1f}x faster than Python UDF")
print(f"Built-in is ~{python_time/max(builtin_time, 0.001):.1f}x faster than Python UDF")

large_df.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 -- Pandas UDF: Price Tier Classification
# MAGIC
# MAGIC Another scalar Pandas UDF example using pd.cut for bucketing.

# COMMAND ----------

@pandas_udf(StringType())
def price_tier(prices: pd.Series) -> pd.Series:
    """Classify prices into tiers using Pandas cut."""
    bins = [0, 50, 200, 500, float("inf")]
    labels = ["budget", "mid-range", "premium", "luxury"]
    return pd.cut(prices, bins=bins, labels=labels).astype(str)

reviews_df.select(
    "product_id",
    "price",
    "category",
    price_tier(col("price")).alias("tier"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 -- Grouped Map Pandas UDF
# MAGIC
# MAGIC Operates on each group independently. Input: full pd.DataFrame for the
# MAGIC group. Output: a pd.DataFrame (can be a different shape).
# MAGIC
# MAGIC Use case: normalize prices within each category.

# COMMAND ----------

# Define the output schema (must match the returned DataFrame columns)
normalized_schema = StructType([
    StructField("product_id", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("price_z_score", DoubleType()),
])

@pandas_udf(normalized_schema, functionType="GROUPED_MAP")
def normalize_prices(pdf: pd.DataFrame) -> pd.DataFrame:
    """Z-score normalization of prices within each category."""
    mean_price = pdf["price"].mean()
    std_price = pdf["price"].std()
    if std_price is None or std_price == 0:
        pdf["price_z_score"] = 0.0
    else:
        pdf["price_z_score"] = ((pdf["price"] - mean_price) / std_price).round(3)
    return pdf[["product_id", "category", "price", "price_z_score"]]

print("Grouped Map -- Z-score normalized prices per category:")
reviews_df.groupBy("category").apply(normalize_prices).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 -- Grouped Aggregate Pandas UDF
# MAGIC
# MAGIC Like a custom aggregation function. Input: pd.Series per group.
# MAGIC Output: a single scalar value per group.

# COMMAND ----------

@pandas_udf(DoubleType())
def weighted_avg_rating(ratings: pd.Series, prices: pd.Series) -> float:
    """Compute price-weighted average rating."""
    if prices.sum() == 0:
        return float(ratings.mean())
    return float((ratings * prices).sum() / prices.sum())

print("Grouped Aggregate -- Price-weighted average rating per category:")
reviews_df.groupBy("category").agg(
    weighted_avg_rating(col("rating"), col("price")).alias("weighted_avg_rating"),
    _round(avg("rating"), 2).alias("simple_avg_rating"),
    count("review_id").alias("review_count"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9 -- Using a Third-Party Library in a UDF
# MAGIC
# MAGIC UDFs can call any Python library available on the cluster.
# MAGIC Here we use the built-in `re` module for regex extraction.

# COMMAND ----------

import re

@pandas_udf(IntegerType())
def count_adjectives(texts: pd.Series) -> pd.Series:
    """Count common adjective patterns in review text."""
    adjective_pattern = re.compile(
        r"\b(excellent|great|amazing|good|terrible|cheap|bad|"
        r"comfortable|soft|premium|decent|average|crystal|blazing)\b",
        re.IGNORECASE
    )
    return texts.apply(lambda t: len(adjective_pattern.findall(t)) if t else 0)

reviews_df.select(
    "review_id",
    "review_text",
    count_adjectives(col("review_text")).alias("adjective_count"),
).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10 -- Key Performance Tips
# MAGIC
# MAGIC **Performance hierarchy** (prefer options at the top):
# MAGIC 1. Built-in functions -- fastest, runs in JVM
# MAGIC 2. Higher-order functions (TRANSFORM, FILTER) -- Catalyst native
# MAGIC 3. Pandas UDFs -- Arrow serialization, batch processing
# MAGIC 4. Python UDFs -- Pickle serialization, row-by-row (slowest)
# MAGIC
# MAGIC **Remember**:
# MAGIC - UDFs are opaque to Catalyst: the optimizer cannot push predicates
# MAGIC   or prune columns through a UDF
# MAGIC - Pickle (Python UDF) is row-at-a-time; Arrow (Pandas UDF) is batch
# MAGIC - Always benchmark before choosing a UDF approach

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11 -- Cleanup

# COMMAND ----------

spark.sql("DROP VIEW IF EXISTS reviews")
print("Cleanup complete.")
