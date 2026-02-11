# Databricks notebook source

# COMMAND ----------

# MAGIC %md
# MAGIC # Python Essentials for Spark
# MAGIC
# MAGIC This notebook covers the Python features you will use most when writing PySpark code:
# MAGIC list comprehensions, lambdas, generators, decorators, type hints, and common patterns.
# MAGIC
# MAGIC **Cluster requirement:** Any Databricks runtime (DBR 13.x+ recommended).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List Comprehensions
# MAGIC
# MAGIC List comprehensions let you create lists in a single, readable expression.
# MAGIC They are heavily used in PySpark for dynamic column selection and data generation.

# COMMAND ----------

# Basic list comprehension
squares = [x ** 2 for x in range(10)]
print("Squares:", squares)

# Filtered comprehension
even_squares = [x ** 2 for x in range(10) if x % 2 == 0]
print("Even squares:", even_squares)

# Nested comprehension -- flatten a matrix
matrix = [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
flat = [val for row in matrix for val in row]
print("Flattened:", flat)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Spark Use Case: Dynamic Column Selection

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

# Create a sample DataFrame
data = [
    ("Alice", 34, "Engineering", 95000),
    ("Bob", 28, "Marketing", 72000),
    ("Charlie", 45, "Engineering", 120000),
    ("Diana", 31, "Marketing", 68000),
]
df = spark.createDataFrame(data, ["name", "age", "department", "salary"])
df.show()

# Use list comprehension to select only string columns
string_cols = [c for c, dtype in df.dtypes if dtype == "string"]
print("String columns:", string_cols)
df.select(string_cols).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Lambda Functions
# MAGIC
# MAGIC Lambdas are anonymous single-expression functions. They are the standard way
# MAGIC to pass inline logic to RDD transformations like `map`, `filter`, and `reduceByKey`.

# COMMAND ----------

# Basic lambdas
double = lambda x: x * 2
print("Double 5:", double(5))

# Sorting with a lambda key
employees = [("Alice", 95000), ("Bob", 72000), ("Charlie", 120000)]
by_salary = sorted(employees, key=lambda emp: emp[1], reverse=True)
print("Sorted by salary:", by_salary)

# COMMAND ----------

# Lambda with RDD operations
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

evens = rdd.filter(lambda x: x % 2 == 0).collect()
doubled = rdd.map(lambda x: x * 2).collect()
total = rdd.reduce(lambda a, b: a + b)

print("Evens:", evens)
print("Doubled:", doubled)
print("Total:", total)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generators and Iterators
# MAGIC
# MAGIC Generators produce values lazily, one at a time. They are the natural fit for
# MAGIC Spark's `mapPartitions`, which expects a function returning an iterator.

# COMMAND ----------

# Generator function
def fibonacci(n):
    """Yield the first n Fibonacci numbers."""
    a, b = 0, 1
    for _ in range(n):
        yield a
        a, b = b, a + b

fib_list = list(fibonacci(10))
print("Fibonacci:", fib_list)

# Generator expression (like a list comprehension, but lazy)
gen = (x ** 2 for x in range(5))
print("Type:", type(gen))
print("Values:", list(gen))

# COMMAND ----------

# Using a generator with mapPartitions
rdd = spark.sparkContext.parallelize(range(20), numSlices=4)

def process_partition(iterator):
    """Process each partition: yield doubled values, skip negatives."""
    for value in iterator:
        if value >= 0:
            yield value * 2

result = rdd.mapPartitions(process_partition).collect()
print("Processed:", result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Decorators
# MAGIC
# MAGIC Decorators wrap functions with additional behaviour. In PySpark, you will see
# MAGIC them used for UDF registration, retry logic, and logging.

# COMMAND ----------

import time
from functools import wraps

# A simple timing decorator
def timer(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        elapsed = time.time() - start
        print(f"{func.__name__} took {elapsed:.4f}s")
        return result
    return wrapper

@timer
def heavy_computation():
    return sum(range(1_000_000))

result = heavy_computation()
print("Result:", result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### UDF Decorator Pattern

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def initials(name):
    """Extract initials from a full name."""
    if name is None:
        return None
    return "".join(word[0].upper() for word in name.split() if word)

df_with_initials = df.withColumn("initials", initials(col("name")))
df_with_initials.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Type Hints
# MAGIC
# MAGIC Type hints improve readability and IDE support. They are especially valuable
# MAGIC in data engineering where schema mismatches cause hard-to-debug failures.

# COMMAND ----------

from typing import List, Dict, Optional
from pyspark.sql import DataFrame

def filter_by_department(df: DataFrame, dept: str) -> DataFrame:
    """Return rows matching the given department."""
    return df.filter(col("department") == dept)

def get_column_stats(df: DataFrame, columns: List[str]) -> Dict[str, float]:
    """Compute mean for the specified numeric columns."""
    stats: Dict[str, float] = {}
    for c in columns:
        row = df.agg({c: "mean"}).first()
        stats[c] = row[0]
    return stats

eng_df = filter_by_department(df, "Engineering")
eng_df.show()

stats = get_column_stats(df, ["age", "salary"])
print("Column stats:", stats)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. The `collections` Module

# COMMAND ----------

from collections import namedtuple, defaultdict, Counter

# namedtuple -- lightweight row objects
Employee = namedtuple("Employee", ["name", "dept", "salary"])
emps = [Employee("Alice", "Eng", 95000), Employee("Bob", "Mkt", 72000)]
print("First employee department:", emps[0].dept)

# defaultdict -- safe key access with defaults
dept_totals = defaultdict(float)
for emp in emps:
    dept_totals[emp.dept] += emp.salary
print("Dept totals:", dict(dept_totals))

# Counter -- quick frequency counts
words = ["spark", "python", "spark", "data", "spark", "python"]
print("Word counts:", Counter(words))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Working with Dates

# COMMAND ----------

from datetime import datetime, timedelta, date

today = date.today()
print("Today:", today)

# Generate a list of the last 7 days
last_week = [today - timedelta(days=i) for i in range(7)]
print("Last 7 days:", last_week)

# Build partition paths
paths = [f"data/year={d.year}/month={d.month:02d}/day={d.day:02d}" for d in last_week]
for p in paths:
    print(p)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. f-Strings and String Formatting

# COMMAND ----------

# Basic f-string
table_name = "sales_transactions"
year = 2024
query = f"SELECT * FROM {table_name} WHERE year = {year}"
print(query)

# Expressions inside f-strings
price = 49.99
print(f"Price with tax: ${price * 1.08:.2f}")

# Multi-line f-strings for SQL
sql = f"""
SELECT
    department,
    COUNT(*) AS emp_count,
    AVG(salary) AS avg_salary
FROM employees
WHERE year = {year}
GROUP BY department
ORDER BY avg_salary DESC
"""
print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Common PySpark Patterns

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, avg, count, when, lit

# Pattern 1: Chained transformations
result = (
    df
    .filter(col("salary") > 70000)
    .withColumn("salary_band", when(col("salary") > 100000, "senior").otherwise("mid"))
    .groupBy("department", "salary_band")
    .agg(
        count("*").alias("headcount"),
        avg("salary").alias("avg_salary")
    )
)
result.show()

# COMMAND ----------

# Pattern 2: Schema construction
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

schema = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=True),
    StructField("price", DoubleType(), nullable=True),
    StructField("quantity", IntegerType(), nullable=True),
])

products_data = [
    ("P001", "Widget", 9.99, 100),
    ("P002", "Gadget", 24.99, 50),
    ("P003", "Doohickey", 4.99, 200),
]

products_df = spark.createDataFrame(products_data, schema=schema)
products_df.printSchema()
products_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Serialization Awareness
# MAGIC
# MAGIC When a Python function is sent to executors, Python pickles the function and
# MAGIC any variables it closes over. Keep closures small to avoid bloated tasks.

# COMMAND ----------

# Good: capture only the value needed
tax_rate = 1.08

rdd_prices = spark.sparkContext.parallelize([10.0, 20.0, 30.0, 40.0])
# The lambda captures only 'tax_rate' (a small float)
with_tax = rdd_prices.map(lambda price: price * tax_rate).collect()
print("Prices with tax:", with_tax)

# Bad pattern (avoid): capturing a large object
# class HugeConfig:
#     data = list(range(1_000_000))
# config = HugeConfig()
# rdd.map(lambda x: x + config.data[0])  # ships entire config!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC
# MAGIC No temporary views or tables were created in this notebook. All data was generated
# MAGIC inline using `createDataFrame` and `parallelize`.

# COMMAND ----------

print("Notebook complete. All exercises finished successfully.")
