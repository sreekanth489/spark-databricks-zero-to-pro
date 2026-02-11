# DataFrames Introduction

> Module 01 -- Topic 05 | Level: Beginner | Time: 60 min

## Learning Objectives

- Create DataFrames from multiple sources: Python lists, dictionaries, CSV, JSON, Parquet
- Define and enforce schemas using StructType and StructField
- Perform column operations: select, withColumn, filter, drop, rename
- Use the Column class, col(), and expr() for column expressions
- Compare Spark DataFrames with Pandas DataFrames
- Inspect DataFrames with show(), display(), printSchema(), describe()

## Conceptual Overview

### What Is a DataFrame?

A Spark DataFrame is a distributed collection of data organized into named columns. It is
conceptually equivalent to a table in a relational database or a Pandas DataFrame, but it
is distributed across a cluster and can handle petabyte-scale data.

```
  ┌──────────────────────────────────────────────────────────┐
  │                    Spark DataFrame                       │
  │                                                          │
  │  Schema: name (string) | age (int) | salary (double)    │
  │  ┌────────────────────────────────────────────────┐      │
  │  │          Partition 0 (Executor 1)              │      │
  │  │  ("Alice", 34, 95000.0)                        │      │
  │  │  ("Bob",   28, 72000.0)                        │      │
  │  └────────────────────────────────────────────────┘      │
  │  ┌────────────────────────────────────────────────┐      │
  │  │          Partition 1 (Executor 2)              │      │
  │  │  ("Charlie", 45, 120000.0)                     │      │
  │  │  ("Diana",   31, 68000.0)                      │      │
  │  └────────────────────────────────────────────────┘      │
  │                                                          │
  │  Backed by: Catalyst Optimizer + Tungsten Memory         │
  └──────────────────────────────────────────────────────────┘
```

### DataFrames vs. RDDs

| Feature | RDD | DataFrame |
|---------|-----|----------|
| Schema | None (opaque objects) | Named, typed columns |
| Optimization | None | Catalyst + Tungsten |
| API style | Functional (map/filter) | Declarative (select/groupBy) |
| Performance | Slower (Python serialization) | Faster (JVM-optimized) |
| Interop with SQL | No | Yes (register as views) |

### Creating DataFrames

**Method 1: From a Python list of tuples**

```python
data = [("Alice", 34), ("Bob", 28)]
df = spark.createDataFrame(data, ["name", "age"])
```

**Method 2: From a list of Row objects**

```python
from pyspark.sql import Row
data = [Row(name="Alice", age=34), Row(name="Bob", age=28)]
df = spark.createDataFrame(data)
```

**Method 3: From a list of dictionaries**

```python
data = [{"name": "Alice", "age": 34}, {"name": "Bob", "age": 28}]
df = spark.createDataFrame(data)
```

**Method 4: From a Pandas DataFrame**

```python
import pandas as pd
pdf = pd.DataFrame({"name": ["Alice", "Bob"], "age": [34, 28]})
df = spark.createDataFrame(pdf)
```

**Method 5: Reading from files**

```python
df = spark.read.csv("path/to/file.csv", header=True, inferSchema=True)
df = spark.read.json("path/to/file.json")
df = spark.read.parquet("path/to/file.parquet")
```

### Schema Definition

Schemas define the structure of your data: column names, data types, and nullability.

**Implicit schema (inferred):**
```python
df = spark.read.csv("data.csv", header=True, inferSchema=True)
# Spark reads a sample of the data to guess types -- can be slow and wrong
```

**Explicit schema (recommended for production):**
```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

schema = StructType([
    StructField("name", StringType(), nullable=False),
    StructField("age", IntegerType(), nullable=True),
    StructField("salary", DoubleType(), nullable=True),
    StructField("active", BooleanType(), nullable=True),
])

df = spark.createDataFrame(data, schema=schema)
```

**DDL-style schema string:**
```python
schema = "name STRING NOT NULL, age INT, salary DOUBLE, active BOOLEAN"
df = spark.createDataFrame(data, schema=schema)
```

**Why explicit schemas matter:**
1. **Faster reads**: No need to scan data for type inference
2. **Correctness**: You control the types, not Spark's heuristics
3. **Documentation**: The schema is self-documenting
4. **Early errors**: Schema mismatches fail fast instead of producing wrong results

### Common Data Types

| PySpark Type | Python Equivalent | SQL Name |
|-------------|------------------|----------|
| `StringType()` | `str` | STRING |
| `IntegerType()` | `int` | INT |
| `LongType()` | `int` (large) | BIGINT |
| `DoubleType()` | `float` | DOUBLE |
| `FloatType()` | `float` | FLOAT |
| `BooleanType()` | `bool` | BOOLEAN |
| `DateType()` | `datetime.date` | DATE |
| `TimestampType()` | `datetime.datetime` | TIMESTAMP |
| `ArrayType(T)` | `list` | ARRAY<T> |
| `MapType(K, V)` | `dict` | MAP<K,V> |
| `StructType([...])` | nested object | STRUCT<...> |

### Column Operations

**Selecting columns:**
```python
df.select("name", "age")                    # by string name
df.select(col("name"), col("age"))          # by Column object
df.select(df.name, df.age)                  # by DataFrame attribute
df.select(df["name"], df["age"])            # by bracket notation
```

**Adding or modifying columns:**
```python
df.withColumn("age_next_year", col("age") + 1)
df.withColumn("name_upper", upper(col("name")))
```

**Renaming columns:**
```python
df.withColumnRenamed("name", "full_name")
df.select(col("name").alias("full_name"), "age")
```

**Dropping columns:**
```python
df.drop("salary")
df.drop("salary", "department")
```

**Filtering rows:**
```python
df.filter(col("age") > 30)
df.filter("age > 30")               # SQL expression string
df.where(col("age") > 30)           # where is an alias for filter
df.filter((col("age") > 30) & (col("salary") > 80000))  # AND
df.filter((col("age") > 30) | (col("salary") > 80000))  # OR
```

### The Column Class

The `Column` class represents a column expression. It supports operators:

```
  col("price") * col("quantity")        # arithmetic
  col("status") == "active"             # comparison
  col("name").startswith("A")           # string methods
  col("value").isNull()                 # null checks
  col("value").cast("double")           # type casting
  col("name").alias("full_name")        # renaming
```

### col() vs. expr()

| Function | Use When |
|----------|---------|
| `col("name")` | Simple column references and operations |
| `expr("price * 1.08")` | Complex SQL-like expressions |

```python
from pyspark.sql.functions import col, expr

# Equivalent operations
df.withColumn("tax", col("price") * 0.08)
df.withColumn("tax", expr("price * 0.08"))

# expr shines with complex SQL expressions
df.withColumn("bucket", expr("CASE WHEN age < 30 THEN 'young' ELSE 'senior' END"))
```

### Inspecting DataFrames

| Method | Purpose |
|--------|---------|
| `df.show(n)` | Print first `n` rows in a formatted table |
| `df.display()` | Databricks-only rich visual display |
| `df.printSchema()` | Print the schema tree |
| `df.schema` | Return the schema as a StructType object |
| `df.dtypes` | Return list of (column_name, data_type) tuples |
| `df.columns` | Return list of column names |
| `df.describe()` | Summary statistics (count, mean, stddev, min, max) |
| `df.summary()` | Extended statistics (adds percentiles) |
| `df.count()` | Number of rows |
| `df.head(n)` | Return first `n` Row objects |

### DataFrame vs. Pandas DataFrame

```
  ┌──────────────────────────────────┬──────────────────────────────────┐
  │         Spark DataFrame          │        Pandas DataFrame          │
  ├──────────────────────────────────┼──────────────────────────────────┤
  │  Distributed across cluster      │  Single machine memory           │
  │  Lazy evaluation                 │  Eager evaluation                │
  │  Immutable (new DF per op)       │  Mutable (in-place operations)   │
  │  Handles TB/PB scale             │  Limited to GB scale             │
  │  No row index                    │  Row index (0, 1, 2, ...)       │
  │  Catalyst optimizer              │  No query optimization           │
  │  .show() / .display()           │  Just print the object           │
  └──────────────────────────────────┴──────────────────────────────────┘
```

**Converting between them:**
```python
# Spark to Pandas (careful: all data comes to driver)
pdf = df.toPandas()

# Pandas to Spark
df = spark.createDataFrame(pdf)
```

## Hands-On Walkthrough

Open the companion notebook `05-dataframes-introduction_notebook.py` in Databricks. You
will:

- Create DataFrames from Python lists, dictionaries, and Rows
- Define schemas with StructType and DDL strings
- Practice select, withColumn, filter, and drop operations
- Use col() and expr() for column expressions
- Inspect DataFrames with show, printSchema, and describe
- Convert between Spark and Pandas DataFrames

## Cloud Provider Notes

| Feature | AWS | Azure | GCP |
|---------|-----|-------|-----|
| Parquet read | S3: `s3a://bucket/path` | ADLS: `abfss://container@acct.dfs.core.windows.net/` | GCS: `gs://bucket/path` |
| CSV/JSON read | Same S3 paths | Same ADLS paths | Same GCS paths |
| Databricks display() | Available | Available | Available |
| Arrow optimization | Set `spark.sql.execution.arrow.pyspark.enabled=true` | Enabled by default (DBR 10+) | Set manually |

## Certification Tip

The Databricks Associate exam heavily tests DataFrame operations. Key topics:

- Creating DataFrames with explicit schemas vs. inferred schemas
- Knowing the difference between `select` and `withColumn`
- Understanding that `filter` and `where` are aliases
- Recognizing that DataFrame operations are lazy until an action is called
- Schema enforcement: what happens when data does not match the schema

Expect questions like: "What does this code return?" followed by a chain of
select/filter/withColumn operations.

## Key Takeaways

- DataFrames are Spark's primary API for structured data -- use them over RDDs
- Always define schemas explicitly in production for speed and correctness
- `col()` for column references; `expr()` for complex SQL-like expressions
- `withColumn` adds or replaces a column; `select` picks columns
- `filter` and `where` are interchangeable
- DataFrames are immutable -- every operation returns a new DataFrame
- `toPandas()` collects everything to the driver -- use only on small datasets
- `display()` in Databricks provides rich visualizations

## Next Steps

Continue to [06 - Spark SQL](06-spark-sql.md) to learn how to query DataFrames using
SQL syntax.
