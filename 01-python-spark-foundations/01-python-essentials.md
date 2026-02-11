# Python Essentials for Spark

> Module 01 -- Topic 01 | Level: Beginner | Time: 45 min

## Learning Objectives

- Write list comprehensions, lambdas, and generator expressions used throughout PySpark code
- Use Python's `collections` module and date/time utilities in data-engineering contexts
- Apply type hints and f-strings for cleaner, self-documenting Spark pipelines
- Recognize the Python patterns that appear most often in PySpark transformations
- Understand how Python closures and serialization affect distributed execution

## Conceptual Overview

### Why Python Matters for Spark

PySpark wraps the Spark JVM engine with a Python API. While Spark does the heavy lifting,
the glue code you write -- UDFs, schema definitions, configuration, orchestration -- is
pure Python. Writing idiomatic Python makes your Spark code shorter, faster to read, and
less error-prone.

### List Comprehensions

List comprehensions are the Swiss Army knife of Python data work. They replace verbose
`for` loops with a single readable expression.

```python
# Verbose
columns = []
for c in df.columns:
    if c.startswith("sales_"):
        columns.append(c)

# Pythonic
columns = [c for c in df.columns if c.startswith("sales_")]
```

Spark usage: dynamically selecting columns, building lists of `Column` expressions,
generating test data.

### Lambda Functions

Lambdas are anonymous, single-expression functions. In PySpark they are the primary way
to define inline transformations for RDD operations.

```python
rdd.map(lambda row: row[0].upper())
rdd.filter(lambda x: x > 100)
rdd.reduceByKey(lambda a, b: a + b)
```

Key rules:
- One expression only (no statements, no assignments)
- Implicit return of the expression value
- Keep them short -- if logic grows, extract to a named function

### Generators and Iterators

Generators produce values lazily, one at a time, without loading everything into memory.
This aligns with Spark's own lazy-evaluation model.

```python
def date_range(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
```

Spark usage: `mapPartitions` expects a function that returns an iterator. Generators are
the natural fit.

```python
def process_partition(iterator):
    for row in iterator:
        yield transform(row)

rdd.mapPartitions(process_partition)
```

### Decorators

Decorators wrap functions with additional behaviour. In Spark you encounter them with UDF
registration and testing frameworks.

```python
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def clean_name(name):
    return name.strip().title()
```

### Type Hints

Type hints make your code self-documenting and enable IDE autocompletion. They are
especially valuable in data engineering where schema mismatches are a top source of bugs.

```python
from pyspark.sql import DataFrame, SparkSession

def load_sales(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.parquet(path)
```

### The `collections` Module

| Class | Spark Use Case |
|-------|---------------|
| `namedtuple` | Lightweight row objects for RDD operations |
| `defaultdict` | Accumulating results in driver-side logic |
| `Counter` | Quick frequency counts during prototyping |
| `OrderedDict` | Preserving column order in schema builders |

### Working with Dates

Date manipulation is central to data engineering. Python's `datetime` module pairs with
PySpark's date functions.

```python
from datetime import datetime, timedelta

# Partition path generation
today = datetime.now()
path = f"data/year={today.year}/month={today.month:02d}/day={today.day:02d}"

# Date arithmetic
seven_days_ago = today - timedelta(days=7)
```

### f-Strings

f-strings (Python 3.6+) are the preferred way to build dynamic strings -- column
expressions, file paths, SQL queries, and log messages.

```python
table = "sales"
year = 2024
query = f"SELECT * FROM {table} WHERE year = {year}"
```

### Common PySpark Patterns in Python

**Pattern 1 -- Chained transformations:**
```python
result = (
    df
    .filter(col("status") == "active")
    .withColumn("revenue", col("price") * col("quantity"))
    .groupBy("region")
    .agg(sum("revenue").alias("total_revenue"))
)
```

**Pattern 2 -- Dynamic column selection:**
```python
numeric_cols = [c for c, t in df.dtypes if t in ("int", "double", "float")]
df.select(numeric_cols)
```

**Pattern 3 -- Schema construction:**
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
])
```

**Pattern 4 -- UDF with closure:**
```python
multiplier = 1.08  # tax rate

@udf(returnType=DoubleType())
def add_tax(price):
    return price * multiplier  # captures 'multiplier' from enclosing scope
```

### Serialization Awareness

When Python functions are sent to Spark executors, they are serialized (pickled). This
means:

1. The function and any variables it closes over must be picklable.
2. Large objects captured in closures bloat the serialized task.
3. Referencing `self` in a class method ships the entire object -- use local variables.

```
  Driver                          Executors
  ┌────────────┐   pickle        ┌────────────┐
  │ lambda x:  │ ──────────────> │ lambda x:  │
  │   x + tax  │   (+ tax val)   │   x + tax  │
  └────────────┘                 └────────────┘
```

## Hands-On Walkthrough

Open the companion notebook `01-python-essentials_notebook.py` in Databricks. It contains
interactive exercises covering:

- List comprehensions for column selection
- Lambda functions with RDD map/filter
- Generator-based partition processing
- Building schemas with type hints
- Date utilities for partition paths
- f-string patterns for dynamic queries

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|-------------------|----------------|
| Python version | Configured per cluster | Configured per cluster runtime | Configured per cluster image |
| Package install | `bootstrap_actions` | `%pip install` / init scripts | `initialization_actions` |
| Secrets access | AWS Secrets Manager | Azure Key Vault via `dbutils` | GCP Secret Manager |

All Python language features described in this guide work identically across providers.
The differences are limited to environment setup and external service integration.

## Certification Tip

The **Databricks Certified Associate Developer for Apache Spark** exam tests your ability
to read and write PySpark transformations. Questions frequently present code with lambdas,
list comprehensions, and chained DataFrame operations. Make sure you can:

- Predict the output of `map` and `filter` with lambdas
- Identify correct schema definitions using `StructType`
- Recognize when a UDF is needed vs. a built-in function

## Key Takeaways

- List comprehensions and lambdas are the most common Python patterns in PySpark code
- Generators align naturally with Spark's `mapPartitions` for memory-efficient processing
- Type hints and f-strings improve readability and reduce bugs in pipeline code
- Be conscious of serialization: closures are pickled and sent to executors
- The `collections` module and `datetime` are essential utilities for data engineering
- Always prefer Spark built-in functions over Python UDFs for performance

## Next Steps

Continue to [02 - Spark Architecture](02-spark-architecture.md) to understand how Spark
executes the Python code you write across a distributed cluster.
