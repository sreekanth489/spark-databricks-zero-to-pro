# UDFs and Pandas UDFs

> Module 04 -- Topic 05 | Level: Intermediate | Time: 45 min

## Learning Objectives

- Understand when and why you need User-Defined Functions (UDFs)
- Write Python UDFs and register them for SQL use
- Explain the serialization overhead: Pickle vs Arrow
- Use Pandas UDFs (vectorized UDFs) for dramatically better performance
- Distinguish between scalar, grouped map, and grouped aggregate UDF types
- Apply type annotations with the modern pandas_udf decorator

## Conceptual Overview

### When Do You Need a UDF?

Spark provides hundreds of built-in functions (see `pyspark.sql.functions`).
These run natively inside the JVM via the Catalyst optimizer and Tungsten
execution engine. They are always the fastest option.

When your logic cannot be expressed with built-in functions -- for example, a
custom text scoring algorithm, a call to a third-party Python library, or a
complex business rule -- you write a UDF.

### Python UDFs: How They Work (and Why They Are Slow)

A regular Python UDF runs in a separate Python process. For every row, Spark
must:

1. **Serialize** the row from JVM to Python (using Pickle)
2. Execute your Python function
3. **Deserialize** the result back to JVM

```
  JVM (Executor)                   Python Worker
  +------------------+             +------------------+
  | Row 1            | -- Pickle-> | udf(row1)        |
  | Row 2            | -- Pickle-> | udf(row2)        |
  | Row 3            | -- Pickle-> | udf(row3)        |
  |   ...            |             |   ...            |
  | Row N            | -- Pickle-> | udf(rowN)        |
  +------------------+             +------------------+
        <--- Pickle (result) ---

  Pickle serialization: row-at-a-time, slow, high overhead
```

This row-by-row serialization is the main bottleneck. On a dataset with
millions of rows, a Python UDF can be 10-100x slower than an equivalent
built-in function.

### Pandas UDFs: The Vectorized Alternative

Pandas UDFs (also called vectorized UDFs) use **Apache Arrow** to transfer
data in columnar batches instead of row-by-row Pickle serialization:

```
  JVM (Executor)                   Python Worker
  +------------------+             +------------------+
  | Batch 1          |             |                  |
  |   1000 rows      | -- Arrow -> | pandas_udf(batch)|
  |   columnar       |             | operates on      |
  |                  |             | pd.Series        |
  +------------------+             +------------------+
        <--- Arrow (result) ---

  Arrow serialization: batch-at-a-time, columnar, 10-100x faster
```

Key advantages of Pandas UDFs:
- **Batch processing**: operates on thousands of rows at once
- **Arrow serialization**: zero-copy columnar format, no Pickle overhead
- **NumPy/Pandas operations**: leverage vectorized operations under the hood
- **Type annotations**: modern API uses Python type hints

### Pickle vs Arrow: The Core Difference

```
  +--------------------+-----------------------------+
  | Pickle (Python UDF)| Arrow (Pandas UDF)          |
  +--------------------+-----------------------------+
  | Row-at-a-time      | Batch (thousands of rows)   |
  | Python objects      | Columnar binary format      |
  | High serialization  | Near zero-copy transfer     |
  |   overhead          |                             |
  | Works with any      | Requires pd.Series /        |
  |   Python type       |   pd.DataFrame types        |
  | 10-100x slower      | Near-native performance     |
  +--------------------+-----------------------------+
```

### UDF Types

| Type | Input | Output | Use Case |
|------|-------|--------|----------|
| **Scalar** (Python) | One row | One value | Simple row transforms |
| **Scalar** (Pandas) | pd.Series batch | pd.Series batch | Vectorized row transforms |
| **Grouped Map** | pd.DataFrame per group | pd.DataFrame | Complex per-group transforms (e.g., normalize within group) |
| **Grouped Aggregate** | pd.Series per group | Scalar per group | Custom aggregation (like a custom sum) |

### Scalar UDF Example (Python vs Pandas)

```python
# Python UDF -- row-at-a-time, uses Pickle
@udf(returnType=StringType())
def classify_python(revenue):
    if revenue is None:
        return "unknown"
    return "blockbuster" if revenue > 1000 else "regular"

# Pandas UDF -- batch, uses Arrow, 10-100x faster
@pandas_udf(StringType())
def classify_pandas(revenue: pd.Series) -> pd.Series:
    return revenue.apply(
        lambda r: "blockbuster" if r and r > 1000 else "regular"
    )
```

### Registering UDFs for SQL

```python
# Register so you can call from SQL
spark.udf.register("classify_sql", classify_python)

# Now available in SQL
# SELECT classify_sql(revenue) FROM movies
```

Or register a Pandas UDF:

```python
spark.udf.register("classify_pandas_sql", classify_pandas)
```

### Performance Hierarchy

Always prefer options higher in this list:

```
  1. Built-in functions (fastest, runs in JVM)
  2. Higher-order functions (TRANSFORM, FILTER -- runs in Catalyst)
  3. Pandas UDFs (vectorized, Arrow serialization)
  4. Python UDFs (slowest, Pickle serialization)
```

## Hands-On Walkthrough

Open the companion notebook `05-udfs-and-pandas-udfs_notebook.py` which covers:

1. Writing a Python UDF for text classification
2. Registering the UDF for SQL queries
3. Writing the equivalent Pandas UDF (scalar)
4. Performance comparison: Python UDF vs Pandas UDF vs built-in
5. Grouped Map Pandas UDF for per-group normalization
6. Grouped Aggregate Pandas UDF for custom aggregation
7. Type annotations with the modern pandas_udf decorator
8. Using third-party libraries inside a UDF

## Cloud Provider Notes

| Feature | AWS (EMR) | Azure (Databricks) | GCP (Dataproc) |
|---------|-----------|---------------------|----------------|
| Python UDFs | All Spark versions | All DBR | All Spark versions |
| Pandas UDFs | Spark 2.3+ (needs PyArrow) | Built-in on DBR | Spark 2.3+ (needs PyArrow) |
| Arrow optimization | `spark.sql.execution.arrow.pyspark.enabled` | Enabled by default on DBR | Config required |
| Photon + UDFs | N/A | Photon cannot optimize UDFs; falls back to non-Photon | N/A |
| Grouped Map | Spark 2.3+ | All DBR | Spark 2.3+ |

## Certification Tip

Expect questions on:
- Knowing that Python UDFs use Pickle serialization and are slow
- Knowing that Pandas UDFs use Arrow and are faster
- Understanding that UDFs disable Catalyst optimizations for that column
- Being able to register a UDF for SQL use with `spark.udf.register()`
- Knowing that built-in functions are always preferred over UDFs

## Key Takeaways

1. Use built-in functions whenever possible -- they are the fastest option.
2. Python UDFs serialize data row-by-row with Pickle -- expensive on large data.
3. Pandas UDFs use Arrow for batch columnar transfer -- 10-100x faster.
4. Three Pandas UDF types: scalar, grouped map, grouped aggregate.
5. Register UDFs with `spark.udf.register()` to use them in SQL queries.
6. UDFs are opaque to Catalyst -- the optimizer cannot push predicates through them.
7. Modern Pandas UDFs use type annotations (`pd.Series -> pd.Series`).
8. Always profile before and after switching from Python UDF to Pandas UDF.

## Next Steps

Proceed to **Topic 06 -- Higher-Order Functions** to learn how TRANSFORM,
FILTER, and AGGREGATE can often replace UDFs entirely with native Catalyst
execution.
