# RDDs Fundamentals

> Module 01 -- Topic 04 | Level: Beginner | Time: 45 min

## Learning Objectives

- Define what an RDD is and its core properties
- Create RDDs using `parallelize()` and `textFile()`
- Apply common transformations: map, filter, flatMap, reduceByKey, groupByKey
- Execute actions: collect, count, take, reduce, first
- Work with pair RDDs for key-value operations
- Decide when to use RDDs vs. DataFrames

## Conceptual Overview

### What Is an RDD?

RDD stands for **Resilient Distributed Dataset**. It is Spark's original data abstraction,
introduced in the 2012 research paper that started it all. An RDD is:

- **Resilient**: Fault-tolerant via lineage -- if a partition is lost, Spark recomputes it
- **Distributed**: Data is split across partitions on multiple nodes
- **Dataset**: A collection of elements (rows, objects, tuples, etc.)

```
  ┌─────────────────────────────────────────────┐
  │                   RDD                       │
  │                                             │
  │  ┌─────────┐  ┌─────────┐  ┌─────────┐    │
  │  │ Part. 0 │  │ Part. 1 │  │ Part. 2 │    │
  │  │ elem 1  │  │ elem 4  │  │ elem 7  │    │
  │  │ elem 2  │  │ elem 5  │  │ elem 8  │    │
  │  │ elem 3  │  │ elem 6  │  │ elem 9  │    │
  │  └─────────┘  └─────────┘  └─────────┘    │
  │                                             │
  │  Properties:                                │
  │  - Immutable (create new RDD on transform)  │
  │  - Lazily evaluated (nothing until action)  │
  │  - Typed (Python objects)                   │
  └─────────────────────────────────────────────┘
```

### RDD Properties

1. **Immutable**: Once created, an RDD cannot be changed. Transformations create new RDDs.
2. **Lazy**: Transformations are recorded but not executed until an action is called.
3. **Partitioned**: Data is split into partitions that can be processed in parallel.
4. **Typed**: In Python, RDD elements can be any Python object (strings, tuples, dicts).

### Creating RDDs

**Method 1: `parallelize()`** -- create an RDD from an in-memory Python collection.

```python
rdd = sc.parallelize([1, 2, 3, 4, 5], numSlices=3)
```

**Method 2: `textFile()`** -- read a text file (local, HDFS, S3, ADLS, GCS).

```python
rdd = sc.textFile("dbfs:/data/logs.txt")
```

**Method 3: From a DataFrame** -- convert using `.rdd`.

```python
rdd = df.rdd  # Each element is a Row object
```

### Transformations

Transformations are **lazy** operations that define a new RDD from an existing one. They
are not executed until an action is called.

| Transformation | Description | Narrow/Wide |
|---------------|-------------|-------------|
| `map(f)` | Apply `f` to each element | Narrow |
| `filter(f)` | Keep elements where `f` returns True | Narrow |
| `flatMap(f)` | Like map, but `f` returns an iterable; results are flattened | Narrow |
| `mapPartitions(f)` | Apply `f` to each partition (iterator in, iterator out) | Narrow |
| `distinct()` | Remove duplicates | Wide |
| `union(other)` | Combine two RDDs | Narrow |
| `reduceByKey(f)` | Merge values for each key using `f` (pair RDD) | Wide |
| `groupByKey()` | Group values by key (pair RDD) | Wide |
| `sortByKey()` | Sort by key (pair RDD) | Wide |
| `join(other)` | Inner join two pair RDDs by key | Wide |

**map vs. flatMap:**

```
  map(lambda x: x.split(" "))
  "hello world" ──> ["hello", "world"]     # list inside list
  "foo bar"     ──> ["foo", "bar"]

  flatMap(lambda x: x.split(" "))
  "hello world" ──> "hello", "world"       # flattened
  "foo bar"     ──> "foo", "bar"
```

**reduceByKey vs. groupByKey:**

```
  Data: [("a",1), ("b",2), ("a",3), ("b",4)]

  reduceByKey(lambda x, y: x + y)
  ──> combines locally first, then shuffles partial results
  ──> ("a", 4), ("b", 6)
  ──> EFFICIENT: less data shuffled

  groupByKey()
  ──> shuffles ALL values, then groups
  ──> ("a", [1, 3]), ("b", [2, 4])
  ──> EXPENSIVE: all data is shuffled, then summed
```

```
  reduceByKey                      groupByKey
  ┌──────────┐                     ┌──────────┐
  │ Part 0   │                     │ Part 0   │
  │ a:1, a:3 │ local              │ a:1, a:3 │ shuffle
  │ ──> a:4  │ reduce             │ ──────── │ everything
  └────┬─────┘    │               └────┬─────┘    │
       │ shuffle  │                    │ shuffle  │
       ▼ (a:4)   │                    ▼ (a:1,a:3)│
  ┌──────────┐   │               ┌──────────┐   │
  │ Result   │ ◄─┘               │ Result   │ ◄─┘
  │ a:4+...  │                   │ a:[1,3..]│
  └──────────┘                   └──────────┘
```

**Best practice**: Prefer `reduceByKey` over `groupByKey` whenever possible. It does a
local pre-aggregation (like a combiner in MapReduce) before shuffling.

### Actions

Actions **trigger execution** and return a result to the driver or write data to storage.

| Action | Description | Returns |
|--------|-------------|---------|
| `collect()` | Return all elements to the driver | List |
| `count()` | Count the number of elements | Integer |
| `first()` | Return the first element | Element |
| `take(n)` | Return the first `n` elements | List |
| `reduce(f)` | Aggregate all elements using `f` | Single value |
| `foreach(f)` | Apply `f` to each element (no return) | None |
| `saveAsTextFile(path)` | Write elements to a text file | None |
| `countByKey()` | Count elements per key (pair RDD) | Dict |

**Warning about `collect()`**: It brings ALL data to the driver. On large datasets, this
will cause an `OutOfMemoryError`. Use `take(n)` or write to storage instead.

### Pair RDDs

A pair RDD is an RDD of key-value tuples: `(key, value)`. Many operations (reduceByKey,
groupByKey, join, sortByKey) only work on pair RDDs.

```python
# Create a pair RDD
pair_rdd = rdd.map(lambda line: (line.split(",")[0], line.split(",")[1]))

# Operations
pair_rdd.reduceByKey(lambda a, b: a + b)
pair_rdd.groupByKey()
pair_rdd.sortByKey()
pair_rdd.join(other_pair_rdd)
pair_rdd.countByKey()
pair_rdd.keys()
pair_rdd.values()
```

### The Classic Word Count

The "Hello, World!" of distributed computing:

```python
text_rdd = sc.textFile("data.txt")

word_counts = (
    text_rdd
    .flatMap(lambda line: line.split(" "))     # split into words
    .map(lambda word: (word.lower(), 1))       # create pair (word, 1)
    .reduceByKey(lambda a, b: a + b)           # sum counts per word
    .sortBy(lambda pair: pair[1], ascending=False)
)

word_counts.take(10)
```

### When to Use RDDs vs. DataFrames

| Criterion | RDDs | DataFrames |
|-----------|------|-----------|
| Optimization | None -- Spark runs exactly what you write | Catalyst optimizer rewrites and optimizes |
| Type safety | Python objects (no schema) | Schema-based (StructType) |
| Performance | Slower (no Tungsten, no code generation) | Faster (Tungsten memory, whole-stage codegen) |
| API style | Functional (map/filter/reduce) | Declarative (select/filter/groupBy) |
| Serialization | Python pickle (slow) | Tungsten binary (fast) |
| Use when | Unstructured data, custom objects, low-level control | Structured/semi-structured data (99% of cases) |

**Rule of thumb**: Use DataFrames for everything unless you have a specific reason to
drop to the RDD level (custom partitioning, unstructured data, legacy code).

## Hands-On Walkthrough

Open the companion notebook `04-rdds-fundamentals_notebook.py` in Databricks. You will:

- Create RDDs with `parallelize()` and from DataFrames
- Practice map, filter, flatMap transformations
- Work with pair RDDs (reduceByKey, groupByKey, join)
- Build the classic word count pipeline
- Compare RDD vs. DataFrame performance

## Cloud Provider Notes

RDD operations work identically across all cloud providers. The only difference is the
file path scheme used with `textFile()`:

| Provider | Path Format |
|----------|------------|
| AWS S3 | `s3a://bucket/path/file.txt` |
| Azure ADLS | `abfss://container@account.dfs.core.windows.net/path/file.txt` |
| GCP GCS | `gs://bucket/path/file.txt` |
| Databricks | `dbfs:/path/file.txt` |

## Certification Tip

The Databricks Associate exam includes questions about RDDs to test foundational
knowledge. You should be able to:

- Identify which operations are transformations vs. actions
- Know that `collect()` is dangerous on large datasets
- Explain why `reduceByKey` is preferred over `groupByKey`
- Understand that RDD transformations are lazy

The Professional exam is more focused on DataFrames and Spark SQL, but understanding
RDDs helps with debugging and performance questions.

## Key Takeaways

- RDDs are Spark's original, low-level data abstraction: resilient, distributed, immutable
- **Transformations** are lazy; **actions** trigger execution
- `flatMap` flattens results; `map` does not
- Prefer `reduceByKey` over `groupByKey` for better shuffle performance
- `collect()` pulls all data to the driver -- use with caution
- DataFrames outperform RDDs in almost all structured data scenarios
- Use RDDs only when you need low-level control or work with unstructured data

## Next Steps

Continue to [05 - DataFrames Introduction](05-dataframes-introduction.md) to learn
Spark's primary API for structured data processing.
