# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # RDDs Fundamentals -- Hands-On
# MAGIC
# MAGIC Practice creating RDDs, applying transformations and actions, working with
# MAGIC pair RDDs, and building the classic word count pipeline.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Creating RDDs

# COMMAND ----------

# Method 1: parallelize -- from an in-memory Python list
numbers = sc.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], numSlices=4)
print(f"Type: {type(numbers)}")
print(f"Partitions: {numbers.getNumPartitions()}")
print(f"Elements: {numbers.collect()}")

# COMMAND ----------

# Method 2: from a DataFrame
from pyspark.sql.functions import col, rand

df = spark.range(10).withColumn("value", rand(seed=42))
rdd_from_df = df.rdd
print(f"Type: {type(rdd_from_df)}")
print(f"First element: {rdd_from_df.first()}")
print(f"Element type: {type(rdd_from_df.first())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic Transformations: map, filter

# COMMAND ----------

# map -- apply a function to each element
rdd = sc.parallelize([1, 2, 3, 4, 5])

squared = rdd.map(lambda x: x ** 2)
print("Squared:", squared.collect())

# filter -- keep elements matching a condition
evens = rdd.filter(lambda x: x % 2 == 0)
print("Evens:", evens.collect())

# Chain transformations (lazy until action)
result = rdd.filter(lambda x: x > 2).map(lambda x: x * 10)
print("Chained:", result.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. flatMap -- Flatten Results
# MAGIC
# MAGIC `map` wraps each result in a list. `flatMap` flattens nested results.

# COMMAND ----------

sentences = sc.parallelize([
    "hello world",
    "apache spark is fast",
    "rdds are fundamental"
])

# map returns a list of lists
mapped = sentences.map(lambda s: s.split(" "))
print("map result:", mapped.collect())

# flatMap flattens into a single list of words
flat_mapped = sentences.flatMap(lambda s: s.split(" "))
print("flatMap result:", flat_mapped.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Actions: collect, count, take, reduce, first

# COMMAND ----------

rdd = sc.parallelize([10, 20, 30, 40, 50])

print("collect():", rdd.collect())           # all elements
print("count():", rdd.count())               # number of elements
print("first():", rdd.first())               # first element
print("take(3):", rdd.take(3))               # first 3 elements
print("reduce(+):", rdd.reduce(lambda a, b: a + b))  # sum all

# top and takeOrdered
print("top(2):", rdd.top(2))                 # largest 2
print("takeOrdered(3):", rdd.takeOrdered(3)) # smallest 3

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Pair RDDs (Key-Value)
# MAGIC
# MAGIC Many RDD operations require a pair format: `(key, value)`.

# COMMAND ----------

# Create a pair RDD
sales_data = sc.parallelize([
    ("Electronics", 1200),
    ("Clothing", 350),
    ("Electronics", 800),
    ("Food", 150),
    ("Clothing", 420),
    ("Food", 90),
    ("Electronics", 950),
    ("Food", 200),
])

print("Sample pair:", sales_data.first())
print("Keys:", sales_data.keys().collect())
print("Values:", sales_data.values().collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. reduceByKey vs. groupByKey

# COMMAND ----------

# reduceByKey -- combines locally first, then shuffles (preferred)
totals = sales_data.reduceByKey(lambda a, b: a + b)
print("reduceByKey totals:", totals.collect())

# groupByKey -- shuffles everything, then groups
grouped = sales_data.groupByKey()
grouped_list = grouped.mapValues(list).collect()
print("groupByKey result:", grouped_list)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Why reduceByKey is Better
# MAGIC
# MAGIC `reduceByKey` does a **local reduction** on each partition before shuffling.
# MAGIC This means less data crosses the network.

# COMMAND ----------

# Demonstrate with a larger dataset
import random
random.seed(42)

large_pairs = sc.parallelize(
    [(f"key_{i % 100}", random.randint(1, 100)) for i in range(100000)],
    numSlices=8
)

# reduceByKey: pre-aggregates locally
reduce_result = large_pairs.reduceByKey(lambda a, b: a + b)
print(f"reduceByKey result count: {reduce_result.count()}")

# groupByKey: shuffles all values
group_result = large_pairs.groupByKey().mapValues(sum)
print(f"groupByKey result count: {group_result.count()}")

print("\nBoth produce the same result, but reduceByKey shuffles less data.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. More Pair RDD Operations

# COMMAND ----------

# countByKey -- count occurrences of each key
category_counts = sales_data.countByKey()
print("countByKey:", dict(category_counts))

# sortByKey
sorted_rdd = sales_data.sortByKey()
print("sortByKey:", sorted_rdd.collect())

# mapValues -- transform only the values
doubled = sales_data.mapValues(lambda v: v * 2)
print("mapValues (x2):", doubled.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Joining Pair RDDs

# COMMAND ----------

# Two pair RDDs to join
categories = sc.parallelize([
    ("Electronics", "Tech"),
    ("Clothing", "Fashion"),
    ("Food", "Grocery"),
])

category_budgets = sc.parallelize([
    ("Electronics", 50000),
    ("Clothing", 20000),
    ("Food", 15000),
    ("Home", 30000),  # no match in categories
])

# Inner join
inner = categories.join(category_budgets)
print("Inner join:", inner.collect())

# Left outer join
left = categories.leftOuterJoin(category_budgets)
print("Left outer:", left.collect())

# Full outer join
full = categories.fullOuterJoin(category_budgets)
print("Full outer:", full.collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. The Classic Word Count
# MAGIC
# MAGIC The "Hello, World!" of distributed computing.

# COMMAND ----------

# Create sample text
text_rdd = sc.parallelize([
    "Apache Spark is a distributed computing framework",
    "Spark provides RDDs DataFrames and SQL APIs",
    "RDDs are the original Spark data abstraction",
    "DataFrames are built on top of RDDs",
    "Spark SQL lets you query DataFrames with SQL",
    "Apache Spark is fast and scalable",
    "Spark runs on clusters of machines",
])

# Word count pipeline
word_counts = (
    text_rdd
    .flatMap(lambda line: line.lower().split(" "))   # split into words
    .map(lambda word: (word, 1))                     # pair (word, 1)
    .reduceByKey(lambda a, b: a + b)                 # sum per word
    .sortBy(lambda pair: pair[1], ascending=False)   # sort by count
)

print("=== Top 10 Words ===")
for word, count in word_counts.take(10):
    print(f"  {word:20s} {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. mapPartitions -- Efficient Per-Partition Processing

# COMMAND ----------

rdd = sc.parallelize(range(20), numSlices=4)

def partition_stats(iterator):
    """Compute stats for each partition without materializing the whole list."""
    values = list(iterator)
    if values:
        yield {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "sum": sum(values),
        }

stats = rdd.mapPartitions(partition_stats).collect()
for i, s in enumerate(stats):
    print(f"Partition {i}: {s}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. RDD vs. DataFrame Performance
# MAGIC
# MAGIC Let us compare the same operation implemented with RDDs vs. DataFrames.

# COMMAND ----------

import time

# Generate data
n = 1000000
rdd_data = sc.parallelize(
    [(i % 100, i * 1.5) for i in range(n)],
    numSlices=8
)

# RDD version: sum by key
start = time.time()
rdd_result = rdd_data.reduceByKey(lambda a, b: a + b).collect()
rdd_time = time.time() - start

# DataFrame version: same aggregation
from pyspark.sql.functions import sum as _sum

df_data = spark.createDataFrame(
    [(i % 100, i * 1.5) for i in range(n)],
    ["key", "value"]
)

start = time.time()
df_result = df_data.groupBy("key").agg(_sum("value")).collect()
df_time = time.time() - start

print(f"RDD time:       {rdd_time:.3f}s")
print(f"DataFrame time: {df_time:.3f}s")
print(f"DataFrame is {rdd_time / max(df_time, 0.001):.1f}x faster")
print("\nDataFrame wins due to Catalyst optimization and Tungsten memory management.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC No temp views or persistent objects were created. All data was generated inline.

# COMMAND ----------

print("Notebook complete. All RDD exercises finished.")
